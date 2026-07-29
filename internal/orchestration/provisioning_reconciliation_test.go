package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

type startupRecoveryCallLog struct {
	calls []string
}

func (l *startupRecoveryCallLog) record(call string) {
	l.calls = append(l.calls, call)
}

type startupRecoveryWorkspaceDriver struct {
	*testkit.FakeWorkspaceDriver
	calls          *startupRecoveryCallLog
	prepared       []ports.WorkspacePlan
	preparedStates []domain.WorkspaceState
}

func (d *startupRecoveryWorkspaceDriver) Prepare(ctx context.Context, plan ports.WorkspacePlan) (ports.WorkspaceHandle, error) {
	d.calls.record("workspace.prepare")
	d.prepared = append(d.prepared, plan)
	handle, err := d.FakeWorkspaceDriver.Prepare(ctx, plan)
	if err == nil {
		d.preparedStates = append(d.preparedStates, handle.State)
	}
	return handle, err
}

type startupRecoveryAgentDriver struct {
	*reconciliationAgentDriver
	calls           *startupRecoveryCallLog
	provisioned     []ports.AgentWorkspacePlan
	beforeInventory func()
}

func (d *startupRecoveryAgentDriver) Provision(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	d.calls.record("agent.provision")
	d.provisioned = append(d.provisioned, plan)
	return d.FakeAgentWorkspaceDriver.Provision(ctx, plan)
}

func (d *startupRecoveryAgentDriver) ReconcileAgentWorkspaces(ctx context.Context, request ports.AgentWorkspaceReconciliationRequest) (ports.AgentWorkspaceReconciliationReport, error) {
	d.calls.record("agent.inventory")
	if d.beforeInventory != nil {
		d.beforeInventory()
	}
	return d.reconciliationAgentDriver.ReconcileAgentWorkspaces(ctx, request)
}

type startupRecoveryTargetDriver struct {
	*reconciliationTargetDriver
	calls           *startupRecoveryCallLog
	created         []ports.TargetPlan
	beforeInventory func()
}

func (d *startupRecoveryTargetDriver) Create(ctx context.Context, plan ports.TargetPlan) (ports.TargetResult, error) {
	d.calls.record("target.create")
	d.created = append(d.created, plan)
	return d.FakeTargetDriver.Create(ctx, plan)
}

func (d *startupRecoveryTargetDriver) ReconcileTargets(ctx context.Context, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	d.calls.record("target.inventory")
	if d.beforeInventory != nil {
		d.beforeInventory()
	}
	return d.reconciliationTargetDriver.ReconcileTargets(ctx, request)
}

func TestControllerReconcilePhysicalResourcesRecoversUnboundInitialAgentProvisioning(t *testing.T) {
	fixture, harness, request, view := newStartupRecoveryProvisioningAgentFixture(t, "startup-unbound-agent")

	calls := &startupRecoveryCallLog{}
	workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: calls}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	agent.beforeInventory = func() {
		requireStartupRecoveryAgentState(t, fixture, view.Session.ID, domain.AgentGenerationReady)
	}
	target := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	controller := newStartupRecoveryController(t, fixture, harness, agent, target, workspace)
	report := reconcileStartupRecovery(t, controller)

	requireStartupRecoveryCallOrder(t, calls.calls, "workspace.prepare", "agent.provision")
	requireStartupRecoveryCallOrder(t, calls.calls, "agent.provision", "agent.inventory")
	if len(workspace.prepared) != 1 || len(agent.provisioned) != 1 {
		t.Fatalf("physical recovery calls: workspace prepares=%d agent provisions=%d", len(workspace.prepared), len(agent.provisioned))
	}
	wantWorkspaceKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/workspace")
	wantAgentKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/agent")
	if got := workspace.prepared[0].IdempotencyKey; got != wantWorkspaceKey {
		t.Fatalf("workspace provisioning key = %q, want %q", got, wantWorkspaceKey)
	}
	if got := agent.provisioned[0].IdempotencyKey; got != wantAgentKey {
		t.Fatalf("agent provisioning key = %q, want %q", got, wantAgentKey)
	}

	persisted, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(persisted.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if generation.State != domain.AgentGenerationReady || generation.ProvisioningPlanDigest == "" ||
		generation.WorkspaceProvisioningKey != wantWorkspaceKey || generation.AgentProvisioningKey != wantAgentKey {
		t.Fatalf("recovered agent generation = %#v", generation)
	}
	wantRef := ports.AgentWorkspaceRef{ID: agent.provisioned[0].Generation.Spec().AgentWorkspaceID, Generation: domain.AgentGeneration(generation.Generation)}
	requireSingleStartupRecoveryAgentRef(t, report.RecoveredAgentProvisionings, wantRef)
}

func TestControllerReconcilePhysicalResourcesRecoversAgentWithMountedWorkspaceReplay(t *testing.T) {
	fixture, harness, request, view := newStartupRecoveryProvisioningAgentFixture(t, "startup-mounted-agent")
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	defer cancel()
	resolved, err := harness.resolver.ResolveAcquisition(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := bindAgentProvisioning(request, resolved, view)
	if err != nil {
		t.Fatal(err)
	}
	view, err = harness.controller.bindAgentProvisioningPlan(ctx, request.Meta, view, plan)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := harness.workspace.Prepare(ctx, plan.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := harness.workspace.Mount(ctx, prepared.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if mounted.State != domain.WorkspaceMounted {
		t.Fatalf("pre-restart workspace state = %s, want mounted", mounted.State)
	}

	calls := &startupRecoveryCallLog{}
	workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: calls}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	agent.beforeInventory = func() {
		requireStartupRecoveryAgentState(t, fixture, view.Session.ID, domain.AgentGenerationReady)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, workspace)
	report := reconcileStartupRecovery(t, controller)

	requireStartupRecoveryCallOrder(t, calls.calls, "workspace.prepare", "agent.provision")
	if len(workspace.preparedStates) != 1 || workspace.preparedStates[0] != domain.WorkspaceMounted {
		t.Fatalf("replayed workspace prepare states = %v, want [mounted]", workspace.preparedStates)
	}
	if len(agent.provisioned) != 1 {
		t.Fatalf("agent provisioning calls = %d, want 1", len(agent.provisioned))
	}
	spec := plan.Agent.Generation.Spec()
	requireSingleStartupRecoveryAgentRef(t, report.RecoveredAgentProvisionings, ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation})
}

func TestControllerReconcilePhysicalResourcesRecoversUnboundInitialTargetProvisioning(t *testing.T) {
	fixture, harness, view, _ := newStartupRecoveryReadyTargetFixture(t, false)
	creationMeta := fixture.meta("startup-unbound-target")
	target, err := fixture.core.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: creationMeta, LeaseID: view.Lease.ID, Template: "linux-visible", Kind: domain.TargetLinuxContainer,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.CreationIdempotencyKey != creationMeta.IdempotencyKey {
		t.Fatalf("persisted target creation key = %q, want %q", target.CreationIdempotencyKey, creationMeta.IdempotencyKey)
	}
	requireStartupRecoveryTargetState(t, fixture, target.ID, domain.TargetGenerationProvisioning)

	calls := &startupRecoveryCallLog{}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	targetDriver := &startupRecoveryTargetDriver{
		reconciliationTargetDriver: &reconciliationTargetDriver{FakeTargetDriver: harness.target},
		calls:                      calls,
	}
	targetDriver.beforeInventory = func() {
		requireStartupRecoveryTargetState(t, fixture, target.ID, domain.TargetGenerationReady)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, targetDriver, harness.workspace)
	report := reconcileStartupRecovery(t, controller)

	requireStartupRecoveryCallOrder(t, calls.calls, "target.create", "target.inventory")
	if len(targetDriver.created) != 1 {
		t.Fatalf("target create calls = %d, want 1", len(targetDriver.created))
	}
	wantKey := domain.DeriveIdempotencyKey(creationMeta.IdempotencyKey, "physical/target")
	if got := targetDriver.created[0].IdempotencyKey; got != wantKey {
		t.Fatalf("target provisioning key = %q, want %q", got, wantKey)
	}
	persisted, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if generation.State != domain.TargetGenerationReady || generation.ProvisioningPlanDigest == "" || generation.ProvisioningKey != wantKey {
		t.Fatalf("recovered target generation = %#v", generation)
	}
	wantRef := ports.TargetRef{ID: targetDriver.created[0].Target.ID(), Generation: domain.TargetGeneration(generation.Generation)}
	requireSingleStartupRecoveryTargetRef(t, report.RecoveredTargetProvisionings, wantRef)
}

func TestControllerReconcilePhysicalResourcesPreservesPendingUnboundTargetResetForClientRetry(t *testing.T) {
	fixture, harness, _, target := newStartupRecoveryReadyTargetFixture(t, true)
	request, pending := beginStartupRecoveryTargetReset(t, fixture, target, "startup-unbound-reset")
	previousRef := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
	currentRef := ports.TargetRef{ID: previousRef.ID, Generation: domain.TargetGeneration(pending.CurrentGeneration)}

	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	controller := newStartupRecoveryController(
		t, fixture, harness,
		&reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		targetDriver, harness.workspace,
	)
	report := reconcileStartupRecovery(t, controller)

	requireSingleStartupRecoveryTargetRef(t, report.PendingTargetProvisionings, currentRef)
	if len(targetDriver.expected) != 1 || targetDriver.expected[0].Generation.Spec().Generation != previousRef.Generation {
		t.Fatalf("pending reset inventory plans = %#v, want predecessor %s", targetDriver.expected, targetRefKey(previousRef))
	}
	if len(targetDriver.destroyed) != 0 {
		t.Fatalf("pending reset predecessor was destroyed during startup: %v", targetDriver.destroyed)
	}
	requireStartupRecoveryTargetState(t, fixture, pending.ID, domain.TargetGenerationProvisioning)

	retried, err := controller.ResetTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(retried)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset")
	if generation.State != domain.TargetGenerationReady || generation.ProvisioningPlanDigest == "" || generation.ProvisioningKey != wantKey {
		t.Fatalf("client retry did not complete the exact pending reset: %#v", generation)
	}
	if len(targetDriver.destroyed) != 0 {
		t.Fatalf("client retry destroyed the predecessor explicitly: %v", targetDriver.destroyed)
	}
}

func TestControllerReconcilePhysicalResourcesAcceptsSafeBoundPendingTargetResetOrientations(t *testing.T) {
	tests := []struct {
		name               string
		predecessorPresent bool
		cleanupResidue     bool
	}{
		{name: "predecessor_adopted_successor_missing", predecessorPresent: true},
		{name: "predecessor_missing_successor_adopted"},
		{name: "predecessor_missing_successor_adopted_with_local_residue", cleanupResidue: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, harness, _, target := newStartupRecoveryReadyTargetFixture(t, true)
			request, pending := beginStartupRecoveryTargetReset(t, fixture, target, "startup-bound-reset")
			pending, plan := bindStartupRecoveryTargetReset(t, harness, request, pending)
			previousRef := ports.TargetRef{ID: plan.Target.ID(), Generation: domain.TargetGeneration(target.CurrentGeneration)}
			currentRef := ports.TargetRef{ID: previousRef.ID, Generation: domain.TargetGeneration(pending.CurrentGeneration)}
			if !test.predecessorPresent {
				resetPlan, err := bindTargetResetPlan(request, pending)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
				_, err = harness.target.Reset(ctx, previousRef.ID, resetPlan)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}

			cleanupResidue := test.cleanupResidue
			targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
			targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
				report := ports.TargetReconciliationReport{ObservedAt: fixture.clock.Now()}
				for _, expectedPlan := range expected {
					spec := expectedPlan.Generation.Spec()
					ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}
					present := ref == previousRef && test.predecessorPresent || ref == currentRef && !test.predecessorPresent
					observation := ports.TargetReconciliation{Ref: ref}
					if present {
						observation.RuntimeID = "runtime-" + targetRefKey(ref)
						observation.Classification = ports.PhysicalResourceAdopted
						observation.PlanMatched = true
					} else {
						observation.Classification = ports.PhysicalResourceMissing
						observation.Diagnostic = "authoritative absence"
						observation.CleanupRequired = ref == previousRef && cleanupResidue
					}
					report.Expected = append(report.Expected, observation)
				}
				return report
			}
			targetDriver.destroy = func(ref ports.TargetRef) error {
				if ref != previousRef {
					t.Fatalf("destroyed unexpected pending reset target %s", targetRefKey(ref))
				}
				cleanupResidue = false
				return nil
			}
			controller := newStartupRecoveryController(
				t, fixture, harness,
				&reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
				targetDriver, harness.workspace,
			)
			report := reconcileStartupRecovery(t, controller)

			requireSingleStartupRecoveryTargetRef(t, report.PendingTargetProvisionings, currentRef)
			if len(targetDriver.expected) != 2 {
				t.Fatalf("bound pending reset inventory plans = %d, want predecessor and successor", len(targetDriver.expected))
			}
			wantDestroyed := 0
			if test.cleanupResidue {
				wantDestroyed = 1
			}
			if len(targetDriver.destroyed) != wantDestroyed {
				t.Fatalf("safe pending reset cleanup calls = %v, want %d", targetDriver.destroyed, wantDestroyed)
			}
			requireStartupRecoveryTargetState(t, fixture, pending.ID, domain.TargetGenerationProvisioning)
		})
	}
}

func newStartupRecoveryProvisioningAgentFixture(t *testing.T, prefix string) (*integrationFixture, controllerHarness, application.AcquireRequest, application.ResearchSessionView) {
	t.Helper()
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	request := application.AcquireRequest{
		Meta: fixture.meta(prefix), OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	}
	view, err := fixture.core.AcquireResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if view.Session.AcquisitionIdempotencyKey != request.Meta.IdempotencyKey {
		t.Fatalf("persisted acquisition key = %q, want %q", view.Session.AcquisitionIdempotencyKey, request.Meta.IdempotencyKey)
	}
	requireStartupRecoveryAgentState(t, fixture, view.Session.ID, domain.AgentGenerationProvisioning)
	return fixture, harness, request, view
}

func newStartupRecoveryReadyTargetFixture(t *testing.T, createTarget bool) (*integrationFixture, controllerHarness, application.ResearchSessionView, application.TargetRecord) {
	t.Helper()
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	if !createTarget {
		return fixture, harness, view, application.TargetRecord{}
	}
	return fixture, harness, view, harness.createTarget(t, fixture, view)
}

func beginStartupRecoveryTargetReset(t *testing.T, fixture *integrationFixture, target application.TargetRecord, prefix string) (application.ResetTargetRequest, application.TargetRecord) {
	t.Helper()
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta(prefix + "-resettable"), TargetID: target.ID, Generation: target.CurrentGeneration,
		ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := application.ResetTargetRequest{
		Meta: fixture.meta(prefix), TargetID: target.ID, ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
	}
	pending, err := fixture.core.ResetTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	current, err := targetGeneration(pending)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.TargetGenerationProvisioning || current.Previous != target.CurrentGeneration ||
		current.ProvisioningPlanDigest != "" || current.ProvisioningKey != "" {
		t.Fatalf("logical reset did not leave an unbound successor: %#v", current)
	}
	return request, pending
}

func bindStartupRecoveryTargetReset(t *testing.T, harness controllerHarness, request application.ResetTargetRequest, pending application.TargetRecord) (application.TargetRecord, ports.TargetPlan) {
	t.Helper()
	physicalKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset")
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	defer cancel()
	plan, err := harness.controller.resolvePersistedTargetProvisioningPlan(ctx, request.Meta, pending, physicalKey)
	if err != nil {
		t.Fatal(err)
	}
	pending, err = harness.controller.bindTargetProvisioningPlan(ctx, request.Meta, pending, plan)
	if err != nil {
		t.Fatal(err)
	}
	return pending, plan
}

func newStartupRecoveryController(t *testing.T, fixture *integrationFixture, harness controllerHarness, agent ports.AgentWorkspaceDriver, target ports.TargetDriver, workspace ports.WorkspaceDriver) *Controller {
	t.Helper()
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agent, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: target},
		Workspace: workspace, Resolver: harness.resolver, Capabilities: harness.capabilities, Observers: harness.controller.observers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func reconcileStartupRecovery(t *testing.T, controller *Controller) PhysicalReconciliationReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func requireStartupRecoveryAgentState(t *testing.T, fixture *integrationFixture, sessionID string, want domain.AgentGenerationState) {
	t.Helper()
	view, err := fixture.core.GetResearchSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if generation.State != want {
		t.Fatalf("agent generation state = %s, want %s", generation.State, want)
	}
}

func requireStartupRecoveryTargetState(t *testing.T, fixture *integrationFixture, targetID string, want domain.TargetGenerationState) {
	t.Helper()
	target, err := fixture.core.GetTarget(context.Background(), targetID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	if generation.State != want {
		t.Fatalf("target generation state = %s, want %s", generation.State, want)
	}
}

func requireStartupRecoveryCallOrder(t *testing.T, calls []string, before, after string) {
	t.Helper()
	beforeIndex, afterIndex := -1, -1
	for index, call := range calls {
		if call == before && beforeIndex < 0 {
			beforeIndex = index
		}
		if call == after && afterIndex < 0 {
			afterIndex = index
		}
	}
	if beforeIndex < 0 || afterIndex < 0 || beforeIndex >= afterIndex {
		t.Fatalf("startup call order = %v, want %q before %q", calls, before, after)
	}
}

func requireSingleStartupRecoveryAgentRef(t *testing.T, got []ports.AgentWorkspaceRef, want ports.AgentWorkspaceRef) {
	t.Helper()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("agent refs = %v, want [%v]", got, want)
	}
}

func requireSingleStartupRecoveryTargetRef(t *testing.T, got []ports.TargetRef, want ports.TargetRef) {
	t.Helper()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("target refs = %v, want [%v]", got, want)
	}
}
