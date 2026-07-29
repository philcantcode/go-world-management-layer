package docker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
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
	cleanupOnly map[string]workspaceRecord
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
	return &Driver{
		build: config.Build, engine: config.Engine, now: config.Now,
		workspaces: make(map[string]workspaceRecord), cleanupOnly: make(map[string]workspaceRecord), idempotency: make(map[string]string),
	}, nil
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
	if err := dockercli.RequireSupportedCgroupVersion(capabilities.CgroupVersion); err != nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "docker.probe", "cgroup_version", "Docker Engine cannot provide a supported resource-controller contract", err)
	}
	supported, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{
		"api_version":               capabilities.APIVersion,
		"cgroup_identity_authority": dockercli.ContainerCgroupIdentityAuthority(),
		"cgroup_version":            capabilities.CgroupVersion,
	}, map[string]string{"engine_version": capabilities.EngineVersion})
	isolationFacts := dockercli.RestrictedNamespaceFacts(capabilities.SecurityOptions)
	isolationFacts["privileged"] = "false"
	isolationFacts["runtime_socket"] = "false"
	isolation, _ := domain.NewCapability(domain.CapabilitySupported, isolationFacts, nil)
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
	if result, complete, err := d.existingProvisionResult(input, key); err != nil || complete {
		return result, err
	}
	containerID, found, err := d.findProvisionContainer(ctx, input, containerPlan)
	if err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	if err := prepareWorkspaceAccess(containerPlan.ExpectedWorkspaceSource, containerPlan.User); err != nil {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeFailedPrecondition, "docker.provision", "workspace_access", "workspace could not be restricted to the configured container identity", err)
	}
	created := false
	if !found {
		containerID, err = d.engine.Create(ctx, containerPlan)
		if err != nil {
			return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeUnavailable, "docker.provision", "create", "Docker create failed", err)
		}
		if err := dockercli.RequireCanonicalContainerID(containerID); err != nil {
			return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeIntegrityViolation, "docker.provision", "container_id", "Docker create returned a non-canonical container identity", err)
		}
		created = true
	}
	state, err := d.inspectProvisionContainer(ctx, containerID, containerPlan)
	if err != nil {
		return ports.AgentWorkspaceResult{}, d.failProvisionContainer(containerID, err)
	}
	state, err = d.establishFreshAgentReadiness(ctx, containerID, containerPlan, state, "docker.provision")
	if err != nil {
		return ports.AgentWorkspaceResult{}, d.failProvisionContainer(containerID, err)
	}
	status := readyAgentStatus(containerPlan, state, d.now().UTC())
	d.mu.Lock()
	d.workspaces[key] = workspaceRecord{plan: input, containerPlan: containerPlan, containerID: containerID, status: status}
	delete(d.cleanupOnly, key)
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()
	return ports.AgentWorkspaceResult{Status: status, Created: created}, nil
}

func (d *Driver) existingProvisionResult(input ports.AgentWorkspacePlan, key string) (ports.AgentWorkspaceResult, bool, error) {
	d.mu.Lock()
	existingKey, found := d.idempotency[input.IdempotencyKey]
	record, exists := d.workspaces[existingKey]
	_, generationExists := d.workspaces[key]
	d.mu.Unlock()
	if !found {
		if generationExists {
			return ports.AgentWorkspaceResult{}, true, domain.NewError(domain.CodeAlreadyExists, "docker.provision", "generation", "workspace generation already exists under another idempotency key", nil)
		}
		return ports.AgentWorkspaceResult{}, false, nil
	}
	if !exists || existingKey != key {
		return ports.AgentWorkspaceResult{}, true, domain.NewError(domain.CodeConflict, "docker.provision", "idempotency_key", "was used for a different workspace generation", nil)
	}
	samePlan, err := sameAgentWorkspacePlanIdentity(record.plan, input)
	if err != nil {
		return ports.AgentWorkspaceResult{}, true, domain.NewError(domain.CodeInternal, "docker.provision", "idempotency_key", "could not compare the existing and requested workspace plans", err)
	}
	if !samePlan {
		return ports.AgentWorkspaceResult{}, true, domain.NewError(domain.CodeConflict, "docker.provision", "idempotency_key", "was reused with a different workspace plan", nil)
	}
	if record.status.State.Terminal() {
		return ports.AgentWorkspaceResult{Status: record.status, Created: false}, true, nil
	}
	// A Docker container can be stopped and restarted without changing its ID
	// or immutable configuration. Because ContainerState has no authoritative
	// start identity, even an in-process Ready record must be inspected and
	// complete a fresh framed guest probe before a Provision replay reports it
	// ready.
	record.status.Ready = false
	record.status.State = domain.AgentGenerationProvisioning
	record.status.GuestProtocol = 0
	record.status.ObservedAt = d.now().UTC()
	d.mu.Lock()
	d.workspaces[key] = record
	d.mu.Unlock()
	return ports.AgentWorkspaceResult{}, false, nil
}

func (d *Driver) findProvisionContainer(ctx context.Context, input ports.AgentWorkspacePlan, plan ContainerPlan) (string, bool, error) {
	inventory, supported := d.engine.(EngineInventory)
	if !supported {
		return "", false, domain.NewError(domain.CodeCapabilityUnavailable, "docker.provision", "inventory", "Docker engine does not provide authoritative inventory required for restart convergence", nil)
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return "", false, domain.NewError(domain.CodeUnavailable, "docker.provision", "inventory", "Docker inventory failed", err)
	}
	if err := validateContainerInventory(states); err != nil {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "docker.provision", "inventory", "Docker inventory is ambiguous", err)
	}
	ref := ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}
	candidates := agentCandidates(states, expectedAgentContainer{input: input, plan: plan, ref: ref})
	switch len(candidates) {
	case 0:
		return "", false, nil
	case 1:
		state := states[candidates[0]]
		if err := validateContainerIdentity(state, plan); err != nil {
			return "", false, err
		}
		return state.ID, true, nil
	default:
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "docker.provision", "identity", "multiple Docker resources claim the workspace generation", nil)
	}
}

func (d *Driver) inspectProvisionContainer(ctx context.Context, containerID string, plan ContainerPlan) (ContainerState, error) {
	return d.inspectOwnedContainer(ctx, containerID, plan, "docker.provision")
}

func (d *Driver) inspectOwnedContainer(ctx context.Context, containerID string, plan ContainerPlan, operation string) (ContainerState, error) {
	if err := dockercli.RequireCanonicalContainerID(containerID); err != nil {
		return ContainerState{}, domain.NewError(domain.CodeIntegrityViolation, operation, "container_id", "stored Docker container identity is non-canonical", err)
	}
	state, err := d.engine.Inspect(ctx, containerID)
	if err != nil {
		return ContainerState{}, domain.NewError(domain.CodeUnavailable, operation, "inspect", "Docker inspect failed", err)
	}
	if state.ID != containerID {
		return ContainerState{}, domain.NewError(domain.CodeIntegrityViolation, operation, "container_id", "Docker inspect returned a different container", nil)
	}
	if err := validateContainerIdentity(state, plan); err != nil {
		return ContainerState{}, err
	}
	return state, nil
}

func (d *Driver) requireOwnedContainerStopped(ctx context.Context, containerID string, plan ContainerPlan, mode ports.StopMode, operation string) (ContainerState, error) {
	state, err := d.inspectOwnedContainer(ctx, containerID, plan, operation)
	if err != nil {
		return ContainerState{}, err
	}
	if !state.Running {
		if err := requireStoppedContainerState(state, operation, dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
			return ContainerState{}, err
		}
		return state, nil
	}
	stopErr := d.engine.Stop(ctx, containerID, mode)
	stopped, inspectErr := d.inspectOwnedContainer(ctx, containerID, plan, operation)
	if inspectErr != nil {
		if stopErr != nil {
			stopErr = domain.NewError(domain.CodeUnavailable, operation, "engine", "Docker stop failed", stopErr)
		}
		return ContainerState{}, errors.Join(stopErr, inspectErr)
	}
	if stopped.Running {
		return stopped, errors.Join(stopErr, domain.NewError(domain.CodeFailedPrecondition, operation, "running", "container remained running after stop", nil))
	}
	if err := requireStoppedContainerState(stopped, operation, dockercli.StoppedStatusExited); err != nil {
		return ContainerState{}, errors.Join(stopErr, err)
	}
	if stopErr != nil {
		return stopped, domain.NewError(domain.CodeUnavailable, operation, "engine", "Docker reported a stop failure despite the stopped observation", stopErr)
	}
	return stopped, nil
}

func (d *Driver) requireRunningProvisionContainer(ctx context.Context, containerID string, plan ContainerPlan) (ContainerState, error) {
	state, err := d.inspectProvisionContainer(ctx, containerID, plan)
	if err != nil {
		return ContainerState{}, err
	}
	if err := requireLiveContainerState(state, "docker.provision"); err != nil {
		return ContainerState{}, err
	}
	return state, nil
}

func requireLiveContainerState(state ContainerState, operation string) error {
	if err := dockercli.RequireExactRunningState(state.Running, state.Paused, state.Restarting, state.Dead, state.Status); err != nil {
		return domain.NewError(domain.CodeFailedPrecondition, operation, "readiness", "agent container is not in the exact running state", err)
	}
	return nil
}

func requireStoppedContainerState(state ContainerState, operation string, allowedStatuses ...string) error {
	if err := dockercli.RequireExactStoppedState(state.Running, state.Paused, state.Restarting, state.Dead, state.Status, allowedStatuses...); err != nil {
		return domain.NewError(domain.CodeFailedPrecondition, operation, "stopped_state", "agent container is not in an allowed exact stopped state", err)
	}
	return nil
}

func requireCoherentContainerState(state ContainerState, operation string) error {
	if state.Running {
		return requireLiveContainerState(state, operation)
	}
	return requireStoppedContainerState(state, operation, dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited)
}

func (d *Driver) failProvisionContainer(containerID string, cause error) error {
	if domain.IsCode(cause, domain.CodeIntegrityViolation) {
		return cause
	}
	return errors.Join(cause, d.removeFailedContainer(containerID, nil))
}

func readyAgentStatus(plan ContainerPlan, state ContainerState, observedAt time.Time) ports.AgentWorkspaceStatus {
	return ports.AgentWorkspaceStatus{
		AgentWorkspaceID: plan.AgentWorkspaceID, Generation: plan.Generation, State: domain.AgentGenerationReady,
		Ready: true, ContainerID: state.ID, CgroupID: state.CgroupID, GuestProtocol: uint32(transport.ProtocolVersion), ObservedAt: observedAt,
	}
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
	state, err := d.inspectOwnedContainer(ctx, record.containerID, record.containerPlan, "docker.inspect")
	if err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	status := record.status
	status.ObservedAt = d.now().UTC()
	status.CgroupID = state.CgroupID
	if status.State == domain.AgentGenerationSealed {
		if err := requireStoppedContainerState(state, "docker.inspect", dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
			return ports.AgentWorkspaceStatus{}, err
		}
	} else if requireLiveContainerState(state, "docker.inspect") != nil {
		status.Ready = false
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
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	record, cleanupOnly, err := d.requireWorkspaceForCleanup(ref.ID, ref.Generation)
	if err != nil {
		return err
	}
	if _, err := d.requireOwnedContainerStopped(ctx, record.containerID, record.containerPlan, mode, "docker.stop"); err != nil {
		return err
	}
	d.mu.Lock()
	record.status.Ready = false
	record.status.State = domain.AgentGenerationSealed
	record.status.ObservedAt = d.now().UTC()
	key := workspaceKey(ref.ID, ref.Generation)
	if cleanupOnly {
		d.cleanupOnly[key] = record
	} else {
		d.workspaces[key] = record
	}
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
	if !found {
		record, found = d.cleanupOnly[key]
	}
	d.mu.Unlock()
	containerID, absent, err := d.resolveAgentDestroy(ctx, ref, record, found)
	if err != nil {
		return err
	}
	if absent {
		if found {
			d.mu.Lock()
			delete(d.workspaces, key)
			delete(d.cleanupOnly, key)
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
	delete(d.cleanupOnly, key)
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

func (d *Driver) requireWorkspaceForCleanup(id domain.AgentWorkspaceID, generation domain.AgentGeneration) (workspaceRecord, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := workspaceKey(id, generation)
	if record, found := d.workspaces[key]; found {
		return record, false, nil
	}
	if record, found := d.cleanupOnly[key]; found {
		return record, true, nil
	}
	return workspaceRecord{}, false, domain.NewError(domain.CodeNotFound, "docker.workspace", "generation", "workspace generation was not proven by this driver", nil)
}

func workspaceKey(id domain.AgentWorkspaceID, generation domain.AgentGeneration) string {
	return id.String() + "/" + strconv.FormatUint(uint64(generation), 10)
}

func requireContext(ctx context.Context, operation string) error {
	return ports.RequireDeadline(ctx, operation)
}

var _ ports.AgentWorkspaceDriver = (*Driver)(nil)
