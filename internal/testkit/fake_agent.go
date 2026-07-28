package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type FakeAgentWorkspaceDriver struct {
	mu           sync.Mutex
	capabilities domain.CapabilityFingerprint
	clock        *Clock
	faults       *FaultInjector
	tracker      *OwnershipTracker
	workspaces   map[ports.AgentWorkspaceRef]ports.AgentWorkspaceStatus
	owners       map[ports.AgentWorkspaceRef]string
	requests     map[string]string
}

func NewFakeAgentWorkspaceDriver(capabilities domain.CapabilityFingerprint, clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeAgentWorkspaceDriver {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeAgentWorkspaceDriver{capabilities: capabilities, clock: clock, faults: faults, tracker: tracker, workspaces: make(map[ports.AgentWorkspaceRef]ports.AgentWorkspaceStatus), owners: make(map[ports.AgentWorkspaceRef]string), requests: make(map[string]string)}
}

func (d *FakeAgentWorkspaceDriver) Probe(ctx context.Context) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "fake_agent.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := d.faults.Check("agent.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	return d.capabilities, nil
}

func (d *FakeAgentWorkspaceDriver) Provision(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_agent.provision"); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	if err := d.faults.Check("agent.provision.before"); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	ref := ports.AgentWorkspaceRef{ID: plan.Generation.Spec().AgentWorkspaceID, Generation: plan.Generation.Spec().Generation}
	signature := fmt.Sprintf("%s/%d/%s/%s", ref.ID, ref.Generation, plan.Workspace.ID(), plan.ImageDigest)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, exists := d.requests[plan.IdempotencyKey]; exists {
		if previous != signature {
			return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeConflict, "fake_agent.provision", "idempotency_key", "was reused with a different plan", nil)
		}
		return ports.AgentWorkspaceResult{Status: d.workspaces[ref]}, nil
	}
	if _, exists := d.workspaces[ref]; exists {
		return ports.AgentWorkspaceResult{}, domain.NewError(domain.CodeAlreadyExists, "fake_agent.provision", "generation", "already exists", nil)
	}
	if err := d.tracker.Acquire("agent_workspace", ref.ID.String()+fmt.Sprintf("/%d", ref.Generation), plan.LeaseID.String()); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	status := ports.AgentWorkspaceStatus{AgentWorkspaceID: ref.ID, Generation: ref.Generation, State: domain.AgentGenerationReady, Ready: true, ContainerID: "container-" + ref.ID.String(), CgroupID: "cgroup-" + ref.ID.String(), GuestProtocol: 1, ObservedAt: d.clock.Now()}
	d.workspaces[ref] = status
	d.owners[ref] = plan.LeaseID.String()
	d.requests[plan.IdempotencyKey] = signature
	if err := d.faults.Check("agent.provision.after"); err != nil {
		return ports.AgentWorkspaceResult{}, err
	}
	return ports.AgentWorkspaceResult{Status: status, Created: true}, nil
}

func (d *FakeAgentWorkspaceDriver) OpenExec(ctx context.Context, plan ports.ExecPlan) (ports.ExecTransport, error) {
	if err := ports.RequireDeadline(ctx, "fake_agent.open_exec"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if err := d.faults.Check("agent.open_exec"); err != nil {
		return nil, err
	}
	ref := ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.AgentGeneration}
	d.mu.Lock()
	status, exists := d.workspaces[ref]
	d.mu.Unlock()
	if !exists || !status.Ready {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "fake_agent.open_exec", "workspace", "is not ready", nil)
	}
	id := plan.Exec.ID().String()
	if err := d.tracker.Acquire("agent_exec", id, ref.ID.String()); err != nil {
		return nil, err
	}
	return NewFakeExecTransport(func() { _ = d.tracker.Release("agent_exec", id, ref.ID.String()) }), nil
}

func (d *FakeAgentWorkspaceDriver) Inspect(ctx context.Context, ref ports.AgentWorkspaceRef) (ports.AgentWorkspaceStatus, error) {
	if err := ports.RequireDeadline(ctx, "fake_agent.inspect"); err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	if err := ref.Validate(); err != nil {
		return ports.AgentWorkspaceStatus{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	status, exists := d.workspaces[ref]
	if !exists {
		return ports.AgentWorkspaceStatus{}, domain.NewError(domain.CodeNotFound, "fake_agent.inspect", "workspace", "not found", nil)
	}
	return status, nil
}

func (d *FakeAgentWorkspaceDriver) Stop(ctx context.Context, ref ports.AgentWorkspaceRef, mode ports.StopMode) error {
	if err := ports.RequireDeadline(ctx, "fake_agent.stop"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if !mode.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "fake_agent.stop", "mode", "is not recognized", nil)
	}
	if err := d.faults.Check("agent.stop"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	status, exists := d.workspaces[ref]
	if !exists {
		return domain.NewError(domain.CodeNotFound, "fake_agent.stop", "workspace", "not found", nil)
	}
	status.Ready = false
	status.State = domain.AgentGenerationQuiescing
	status.ObservedAt = d.clock.Now()
	d.workspaces[ref] = status
	return nil
}

func (d *FakeAgentWorkspaceDriver) Destroy(ctx context.Context, ref ports.AgentWorkspaceRef) error {
	if err := ports.RequireDeadline(ctx, "fake_agent.destroy"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if err := d.faults.Check("agent.destroy"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.workspaces[ref]; !exists {
		return nil
	}
	owner := d.owners[ref]
	delete(d.workspaces, ref)
	delete(d.owners, ref)
	return d.tracker.Release("agent_workspace", ref.ID.String()+fmt.Sprintf("/%d", ref.Generation), owner)
}

var _ ports.AgentWorkspaceDriver = (*FakeAgentWorkspaceDriver)(nil)
