package docker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

const (
	guestReadinessTimeout = 10 * time.Second
	guestReadinessOutput  = int64(64 << 10)
	containerCleanupGrace = 10 * time.Second
)

type Config struct {
	Build  BuildConfig
	Engine Engine
	Now    func() time.Time
}

type Driver struct {
	build       BuildConfig
	engine      Engine
	now         func() time.Time
	lifecycleMu sync.Mutex
	mu          sync.Mutex
	workspaces  map[string]workspaceRecord
	idempotency map[string]string
}

type workspaceRecord struct {
	plan          ports.AgentWorkspacePlan
	containerPlan ContainerPlan
	containerID   string
	status        ports.AgentWorkspaceStatus
}

func New(config Config) (*Driver, error) {
	if config.Engine == nil {
		return nil, fmt.Errorf("Docker engine is required")
	}
	if config.Build.WorkspaceRoot == "" || config.Build.ImageRepository == "" {
		return nil, fmt.Errorf("workspace root and image repository are required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Driver{build: config.Build, engine: config.Engine, now: config.Now, workspaces: make(map[string]workspaceRecord), idempotency: make(map[string]string)}, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

func (d *Driver) Probe(ctx context.Context) (domain.CapabilityFingerprint, error) {
	if err := requireContext(ctx, "docker.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	capabilities, err := d.engine.Probe(ctx)
	if err != nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "docker.probe", "engine", "Docker Engine probe failed", err)
	}
	supported, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{"api_version": capabilities.APIVersion, "cgroup_version": capabilities.CgroupVersion}, map[string]string{"engine_version": capabilities.EngineVersion})
	isolation, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{"privileged": "false", "host_namespaces": "false", "runtime_socket": "false"}, nil)
	return domain.NewCapabilityFingerprint(map[string]domain.Capability{"agent.docker": supported, "agent.hardened-isolation": isolation}, map[string]string{"os": capabilities.OSType, "architecture": capabilities.Architecture})
}

func (d *Driver) Provision(ctx context.Context, input ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	if err := requireContext(ctx, "docker.provision"); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	containerPlan, err := BuildContainerPlan(input, d.build)
	if err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	key := workspaceKey(containerPlan.AgentWorkspaceID, containerPlan.Generation)
	d.mu.Lock()
	if existingKey, found := d.idempotency[input.IdempotencyKey]; found {
		record, exists := d.workspaces[existingKey]
		d.mu.Unlock()
		if !exists || existingKey != key {
			return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeConflict, "docker.provision", "idempotency_key", "was used for a different workspace generation", nil)
		}
		return ports.AgentWorkspaceResult{Status: record.status, Created: false}, nil
	}
	if _, exists := d.workspaces[key]; exists {
		d.mu.Unlock()
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeAlreadyExists, "docker.provision", "generation", "workspace generation already exists under another idempotency key", nil)
	}
	d.mu.Unlock()
	if err := prepareWorkspaceAccess(containerPlan.ExpectedWorkspaceSource, containerPlan.User); err != nil {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeFailedPrecondition, "docker.provision", "workspace_access", "workspace could not be restricted to the configured container identity", err)
	}

	containerID, err := d.engine.Create(ctx, containerPlan)
	if err != nil {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeUnavailable, "docker.provision", "create", "Docker create failed", err)
	}
	if err := d.engine.Start(ctx, containerID); err != nil {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeUnavailable, "docker.provision", "start", "Docker start failed", d.removeFailedContainer(containerID, err))
	}
	state, err := d.engine.Inspect(ctx, containerID)
	if err != nil || !state.Running {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeFailedPrecondition, "docker.provision", "readiness", "agent container did not become running", d.removeFailedContainer(containerID, err))
	}
	if err := validateContainerIdentity(state, containerPlan); err != nil {
		return ports.AgentWorkspaceResult{}, errors.Join(err, d.removeFailedContainer(containerID, nil))
	}
	if err := d.requireGuestReadiness(ctx, containerID, containerPlan); err != nil {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeFailedPrecondition, "docker.provision", "guest_protocol", "world-guest did not complete its framed readiness probe", d.removeFailedContainer(containerID, err))
	}
	status := ports.AgentWorkspaceStatus{AgentWorkspaceID: containerPlan.AgentWorkspaceID, Generation: containerPlan.Generation, State: domain.AgentGenerationReady, Ready: true, ContainerID: containerID, CgroupID: state.CgroupID, GuestProtocol: uint32(transport.ProtocolVersion), ObservedAt: d.now().UTC()}
	d.mu.Lock()
	d.workspaces[key] = workspaceRecord{plan: input, containerPlan: containerPlan, containerID: containerID, status: status}
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()
	return ports.AgentWorkspaceResult{Status: status, Created: true}, nil
}

func (d *Driver) OpenExec(ctx context.Context, plan ports.ExecPlan) (ports.ExecTransport, error) {
	if err := requireContext(ctx, "docker.open_exec"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	record, err := d.requireWorkspace(plan.AgentWorkspaceID, plan.AgentGeneration)
	if err != nil {
		return nil, err
	}
	if plan.LeaseID != record.containerPlan.LeaseID {
		return nil, domain.NewError(domain.CodeForbidden, "docker.open_exec", "lease_id", "exec does not belong to this workspace", nil)
	}
	if !record.status.Ready {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "docker.open_exec", "workspace", "workspace is not ready", nil)
	}
	return d.engine.OpenExec(ctx, record.containerID, record.containerPlan.Entrypoint[0], plan)
}

func (d *Driver) requireGuestReadiness(ctx context.Context, containerID string, plan ContainerPlan) error {
	readinessCtx, cancel := context.WithTimeout(ctx, guestReadinessTimeout)
	defer cancel()
	execPlan, err := guestReadinessPlan(readinessCtx, plan)
	if err != nil {
		return err
	}
	session, err := d.engine.OpenExec(readinessCtx, containerID, plan.Entrypoint[0], execPlan)
	if err != nil {
		return err
	}
	probeErr := receiveGuestReadiness(readinessCtx, session)
	return errors.Join(probeErr, session.Close())
}

func guestReadinessPlan(ctx context.Context, plan ContainerPlan) (ports.ExecPlan, error) {
	execID, err := domain.NewExecID()
	if err != nil {
		return ports.ExecPlan{}, fmt.Errorf("create readiness exec identity: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return ports.ExecPlan{}, fmt.Errorf("readiness context has no deadline")
	}
	guestBinary := plan.Entrypoint[0]
	argv := []string{transport.GuestSelfTestArgument}
	exec, err := domain.NewExec(domain.ExecSpec{
		ID: execID, LeaseID: plan.LeaseID, AgentWorkspaceID: plan.AgentWorkspaceID, AgentGeneration: plan.Generation,
		Kind: domain.ExecTool, Executable: guestBinary, Argv: argv, WorkingDirectory: ".", CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return ports.ExecPlan{}, err
	}
	return ports.ExecPlan{
		LeaseID: plan.LeaseID, AgentWorkspaceID: plan.AgentWorkspaceID, AgentGeneration: plan.Generation, Exec: exec,
		Start: transport.ExecStart{
			ExecID: execID.String(), IdempotencyKey: "world-guest-readiness-" + execID.String(),
			Executable: guestBinary, Argv: argv, WorkingDirectory: ".", Deadline: deadline,
			MaxOutputBytes: guestReadinessOutput, CleanupGrace: time.Second,
		},
	}, nil
}

func receiveGuestReadiness(ctx context.Context, session ports.ExecTransport) error {
	var outputBytes int64
	var lifecycle transport.ProcessLifecycle
	for {
		frame, err := session.Receive(ctx)
		if err != nil {
			return err
		}
		switch frame.Kind {
		case transport.KindStdout, transport.KindStderr:
			if int64(len(frame.Data)) > guestReadinessOutput-outputBytes {
				return transport.ErrOutputLimit
			}
			outputBytes += int64(len(frame.Data))
		case transport.KindProcess:
			if err := lifecycle.Observe(frame); err != nil {
				return fmt.Errorf("invalid readiness process event: %w", err)
			}
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			if err != nil {
				return err
			}
			if err := lifecycle.ValidateTerminal(terminal); err != nil {
				return fmt.Errorf("readiness process lifecycle: %w", err)
			}
			if terminal.ExitCode != 0 || !terminal.CleanupConfirmed || terminal.Error != "" {
				return fmt.Errorf("readiness outcome is not authoritative: exit=%d cleanup=%t error=%q", terminal.ExitCode, terminal.CleanupConfirmed, terminal.Error)
			}
			return nil
		default:
			return fmt.Errorf("unexpected readiness frame kind %d", frame.Kind)
		}
	}
}

func (d *Driver) removeFailedContainer(containerID string, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), containerCleanupGrace)
	defer cancel()
	return errors.Join(cause, d.engine.Remove(cleanupCtx, containerID))
}

func (d *Driver) Inspect(ctx context.Context, ref ports.AgentWorkspaceRef) (ports.AgentWorkspaceStatus, error) {
	if err := requireContext(ctx, "docker.inspect"); err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	if err := ref.Validate(); err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	record, err := d.requireWorkspace(ref.ID, ref.Generation)
	if err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	state, err := d.engine.Inspect(ctx, record.containerID)
	if err != nil {
		return ports.AgentWorkspaceStatus{}, domain.NewError(domain.CodeUnavailable, "docker.inspect", "engine", "Docker inspect failed", err)
	}
	if err := validateContainerIdentity(state, record.containerPlan); err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	status := record.status
	status.Ready = state.Running
	status.ObservedAt = d.now().UTC()
	status.CgroupID = state.CgroupID
	if !state.Running && status.State != domain.AgentGenerationSealed {
		status.State = domain.AgentGenerationFailed
	}
	d.mu.Lock()
	record.status = status
	d.workspaces[workspaceKey(ref.ID, ref.Generation)] = record
	d.mu.Unlock()
	return status, nil
}

func (d *Driver) Stop(ctx context.Context, ref ports.AgentWorkspaceRef, mode ports.StopMode) error {
	if err := requireContext(ctx, "docker.stop"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if !mode.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "docker.stop", "mode", "is not recognized", nil)
	}
	record, err := d.requireWorkspace(ref.ID, ref.Generation)
	if err != nil {
		return err
	}
	if !record.status.Ready && record.status.State == domain.AgentGenerationSealed {
		return nil
	}
	if err := d.engine.Stop(ctx, record.containerID, mode); err != nil {
		return domain.NewError(domain.CodeUnavailable, "docker.stop", "engine", "Docker stop failed", err)
	}
	d.mu.Lock()
	record.status.Ready = false
	record.status.State = domain.AgentGenerationSealed
	record.status.ObservedAt = d.now().UTC()
	d.workspaces[workspaceKey(ref.ID, ref.Generation)] = record
	d.mu.Unlock()
	return nil
}

func (d *Driver) Destroy(ctx context.Context, ref ports.AgentWorkspaceRef) error {
	if err := requireContext(ctx, "docker.destroy"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	key := workspaceKey(ref.ID, ref.Generation)
	d.mu.Lock()
	record, found := d.workspaces[key]
	d.mu.Unlock()
	containerID, absent, err := d.resolveAgentDestroy(ctx, ref, record, found)
	if err != nil {
		return err
	}
	if absent {
		if found {
			d.mu.Lock()
			delete(d.workspaces, key)
			delete(d.idempotency, record.plan.IdempotencyKey)
			d.mu.Unlock()
		}
		return nil
	}
	if err := d.engine.Remove(ctx, containerID); err != nil {
		return domain.NewError(domain.CodeUnavailable, "docker.destroy", "engine", "Docker remove failed", err)
	}
	if inventory, supported := d.engine.(EngineInventory); supported {
		states, err := inventory.ListContainers(ctx)
		if err != nil {
			return domain.NewError(domain.CodeUnavailable, "docker.destroy", "inventory", "could not prove container absence after removal", err)
		}
		if candidates := agentRefCandidates(states, ref); len(candidates) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "docker.destroy", "absence", "container removal did not produce authoritative absence", nil)
		}
	} else if !found {
		return domain.NewError(domain.CodeCapabilityUnavailable, "docker.destroy", "inventory", "cannot prove absence after restart", nil)
	}
	d.mu.Lock()
	delete(d.workspaces, key)
	if found {
		delete(d.idempotency, record.plan.IdempotencyKey)
	}
	d.mu.Unlock()
	return nil
}

func (d *Driver) requireWorkspace(id domain.AgentWorkspaceID, generation domain.AgentGeneration) (workspaceRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.workspaces[workspaceKey(id, generation)]
	if !found {
		return workspaceRecord{}, domain.NewError(domain.CodeNotFound, "docker.workspace", "generation", "workspace generation was not provisioned by this driver", nil)
	}
	return record, nil
}

func workspaceKey(id domain.AgentWorkspaceID, generation domain.AgentGeneration) string {
	return id.String() + "/" + strconv.FormatUint(uint64(generation), 10)
}

func requireContext(ctx context.Context, operation string) error {
	return ports.RequireDeadline(ctx, operation)
}

var _ ports.AgentWorkspaceDriver = (*Driver)(nil)
