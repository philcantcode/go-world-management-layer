package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type fakeWorkspaceRecord struct {
	plan    ports.WorkspacePlan
	handle  ports.WorkspaceHandle
	preview ports.WorkspacePreviewResult
	seal    ports.WorkspaceSealResult
	owner   string
}

type FakeWorkspaceDriver struct {
	mu         sync.Mutex
	clock      *Clock
	faults     *FaultInjector
	tracker    *OwnershipTracker
	requests   map[string]string
	workspaces map[domain.WorkspaceID]*fakeWorkspaceRecord
}

func NewFakeWorkspaceDriver(clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeWorkspaceDriver {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeWorkspaceDriver{
		clock: clock, faults: faults, tracker: tracker,
		requests:   make(map[string]string),
		workspaces: make(map[domain.WorkspaceID]*fakeWorkspaceRecord),
	}
}

func (d *FakeWorkspaceDriver) Prepare(ctx context.Context, plan ports.WorkspacePlan) (ports.WorkspaceHandle, error) {
	if err := ports.RequireDeadline(ctx, "fake_workspace.prepare"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if err := d.faults.Check("workspace.prepare.before"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	spec := plan.Workspace.Spec()
	signature := fmt.Sprintf("%s/%s/%s/%d/%s/%s/%d/%d", spec.ID, spec.LeaseID, spec.AgentWorkspaceID, spec.AgentGeneration, plan.InputView.ID(), plan.Construction, plan.UpperByteLimit, plan.UpperInodeLimit)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, found := d.requests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.WorkspaceHandle{}, idempotencyConflict("fake_workspace.prepare")
		}
		record, found := d.workspaces[spec.ID]
		if !found {
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeConflict, "fake_workspace.prepare", "idempotency_key", "refers to a workspace that is no longer present", nil)
		}
		return record.handle, nil
	}
	if _, found := d.workspaces[spec.ID]; found {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeAlreadyExists, "fake_workspace.prepare", "workspace_id", "already exists", nil)
	}
	if err := d.tracker.Acquire("workspace", spec.ID.String(), spec.LeaseID.String()); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	handle := ports.WorkspaceHandle{
		WorkspaceID: spec.ID, State: domain.WorkspaceReady,
		MergedPath: "memory://workspace/" + spec.ID.String(), ObservedAt: d.clock.Now(),
	}
	d.workspaces[spec.ID] = &fakeWorkspaceRecord{plan: plan, handle: handle, owner: spec.LeaseID.String()}
	d.requests[plan.IdempotencyKey] = signature
	if err := d.faults.Check("workspace.prepare.after"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	return handle, nil
}

func (d *FakeWorkspaceDriver) Mount(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	if err := ports.RequireDeadline(ctx, "fake_workspace.mount"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if err := d.faults.Check("workspace.mount"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireWorkspace(workspaceID, "fake_workspace.mount")
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if record.handle.State != domain.WorkspaceReady && record.handle.State != domain.WorkspaceMounted {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeFailedPrecondition, "fake_workspace.mount", "state", "workspace cannot be mounted", nil)
	}
	record.handle.State = domain.WorkspaceMounted
	record.handle.ObservedAt = d.clock.Now()
	return record.handle, nil
}

func (d *FakeWorkspaceDriver) Inspect(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	if err := ports.RequireDeadline(ctx, "fake_workspace.inspect"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if err := d.faults.Check("workspace.inspect"); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireWorkspace(workspaceID, "fake_workspace.inspect")
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	return record.handle, nil
}

func (d *FakeWorkspaceDriver) Preview(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspacePreviewResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_workspace.preview"); err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	if err := d.faults.Check("workspace.preview"); err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireWorkspace(workspaceID, "fake_workspace.preview")
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	if record.handle.State == domain.WorkspaceSealed {
		return ports.WorkspacePreviewResult{ChangeSet: record.seal.ChangeSet, ObservedAt: record.seal.SealedAt}, nil
	}
	if record.handle.State != domain.WorkspaceMounted {
		return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeFailedPrecondition, "fake_workspace.preview", "state", "workspace must be mounted", nil)
	}
	if record.preview.ChangeSet.WorkspaceRevision().IsValid() {
		return record.preview, nil
	}
	observedAt := d.clock.Now()
	changes, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, nil, domain.InitialRevision, observedAt)
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	record.preview = ports.WorkspacePreviewResult{ChangeSet: changes, ObservedAt: observedAt}
	return record.preview, nil
}

func (d *FakeWorkspaceDriver) Seal(ctx context.Context, workspaceID domain.WorkspaceID, revision domain.Revision) (ports.WorkspaceSealResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_workspace.seal"); err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if !revision.IsValid() {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeInvalidArgument, "fake_workspace.seal", "revision", "must be positive", nil)
	}
	if err := d.faults.Check("workspace.seal.before"); err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireWorkspace(workspaceID, "fake_workspace.seal")
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if record.handle.State == domain.WorkspaceSealed {
		if record.seal.ChangeSet.WorkspaceRevision() != revision {
			return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeConflict, "fake_workspace.seal", "revision", "workspace was sealed at another revision", nil)
		}
		return record.seal, nil
	}
	if record.handle.State != domain.WorkspaceMounted {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeFailedPrecondition, "fake_workspace.seal", "state", "workspace must be mounted", nil)
	}
	if !record.preview.ChangeSet.WorkspaceRevision().IsValid() {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeFailedPrecondition, "fake_workspace.seal", "revision", "workspace must be previewed before sealing", nil)
	}
	if record.preview.ChangeSet.WorkspaceRevision() != revision {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeConflict, "fake_workspace.seal", "revision", "does not match the latest preview", nil)
	}
	sealedAt := d.clock.Now()
	changes, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, nil, revision, sealedAt)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	record.seal = ports.WorkspaceSealResult{ChangeSet: changes, SealedAt: sealedAt}
	record.handle.State = domain.WorkspaceSealed
	record.handle.ObservedAt = sealedAt
	if err := d.faults.Check("workspace.seal.after"); err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	return record.seal, nil
}

func (d *FakeWorkspaceDriver) Release(ctx context.Context, workspaceID domain.WorkspaceID) error {
	if err := ports.RequireDeadline(ctx, "fake_workspace.release"); err != nil {
		return err
	}
	if err := d.faults.Check("workspace.release"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.workspaces[workspaceID]
	if !found {
		return nil
	}
	if record.handle.State != domain.WorkspaceSealed {
		return domain.NewError(domain.CodeFailedPrecondition, "fake_workspace.release", "state", "workspace must be sealed", nil)
	}
	delete(d.workspaces, workspaceID)
	return d.tracker.Release("workspace", workspaceID.String(), record.owner)
}

func (d *FakeWorkspaceDriver) requireWorkspace(workspaceID domain.WorkspaceID, operation string) (*fakeWorkspaceRecord, error) {
	if workspaceID.IsZero() {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "workspace_id", "must be set", nil)
	}
	record, found := d.workspaces[workspaceID]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, operation, "workspace_id", "not found", nil)
	}
	return record, nil
}

var _ ports.WorkspaceDriver = (*FakeWorkspaceDriver)(nil)
