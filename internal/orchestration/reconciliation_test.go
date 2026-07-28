package orchestration

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

type reconciliationAgentDriver struct {
	*testkit.FakeAgentWorkspaceDriver
	reconcile func([]ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport
	expected  []ports.AgentWorkspacePlan
	destroyed []ports.AgentWorkspaceRef
}

func (d *reconciliationAgentDriver) ReconcileAgentWorkspaces(_ context.Context, expected []ports.AgentWorkspacePlan) (ports.AgentWorkspaceReconciliationReport, error) {
	d.expected = append([]ports.AgentWorkspacePlan(nil), expected...)
	if d.reconcile != nil {
		return d.reconcile(expected), nil
	}
	report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		report.Expected = append(report.Expected, ports.AgentWorkspaceReconciliation{
			Ref:         ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation},
			ContainerID: "agent-runtime-" + spec.AgentWorkspaceID.String(), Classification: ports.PhysicalResourceAdopted,
		})
	}
	return report, nil
}

func (d *reconciliationAgentDriver) Destroy(ctx context.Context, ref ports.AgentWorkspaceRef) error {
	d.destroyed = append(d.destroyed, ref)
	return d.FakeAgentWorkspaceDriver.Destroy(ctx, ref)
}

type reconciliationTargetDriver struct {
	*testkit.FakeTargetDriver
	expected     []ports.TargetPlan
	recover      func(ports.TargetRunPlan) (ports.PreparedTargetRun, error)
	stop         func(domain.TargetRunID, ports.StopMode) (ports.TargetRunStopReceipt, error)
	recoverCalls int
	startCalls   int
	stopCalls    int
}

func (d *reconciliationTargetDriver) ReconcileTargets(_ context.Context, expected []ports.TargetPlan) (ports.TargetReconciliationReport, error) {
	d.expected = append([]ports.TargetPlan(nil), expected...)
	report := ports.TargetReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		report.Expected = append(report.Expected, ports.TargetReconciliation{
			Ref: ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}, RuntimeID: "target-runtime-" + spec.TargetID.String(),
			Classification: ports.PhysicalResourceAdopted,
		})
	}
	return report, nil
}

func (d *reconciliationTargetDriver) RecoverInterruptedRun(_ context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	d.recoverCalls++
	if d.recover != nil {
		return d.recover(plan)
	}
	return d.FakeTargetDriver.PrepareRun(context.Background(), plan)
}

func (d *reconciliationTargetDriver) StartRun(ctx context.Context, runID domain.TargetRunID) error {
	d.startCalls++
	return d.FakeTargetDriver.StartRun(ctx, runID)
}

func (d *reconciliationTargetDriver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	d.stopCalls++
	if d.stop != nil {
		return d.stop(runID, mode)
	}
	return d.FakeTargetDriver.StopRun(ctx, runID, mode)
}

type reconciliationResolverSpy struct {
	ProvisioningResolver
	persistedAgentCalls int
	targetCalls         int
	agentAdmissions     int
	forgetTargetKey     bool
}

func (r *reconciliationResolverSpy) ResolvePersistedAgent(ctx context.Context, view application.ResearchSessionView) (ResolvedAcquisition, error) {
	r.persistedAgentCalls++
	return r.ProvisioningResolver.ResolvePersistedAgent(ctx, view)
}

func (r *reconciliationResolverSpy) ResolveTarget(ctx context.Context, request application.CreateTargetRequest, target application.TargetRecord) (ports.TargetPlan, error) {
	r.targetCalls++
	plan, err := r.ProvisioningResolver.ResolveTarget(ctx, request, target)
	if err == nil && r.forgetTargetKey {
		plan.IdempotencyKey = "resolver-reconstructed-key"
	}
	return plan, err
}

func (r *reconciliationResolverSpy) AdmitAgentWorkspacePlan(context.Context, ports.AgentWorkspacePlan) error {
	r.agentAdmissions++
	return nil
}

func TestAssessReconciliationAcceptsExactInventoryAndTerminalOrphan(t *testing.T) {
	safe, err := assessReconciliation(
		"agent workspace", []string{"active/1"}, map[string]bool{"active/1": false, "terminal/1": true}, time.Now(),
		[]reconciliationObservation{{key: "active/1", runtimeID: "runtime-active", classification: ports.PhysicalResourceAdopted}},
		[]reconciliationObservation{{key: "terminal/1", runtimeID: "runtime-terminal", classification: ports.PhysicalResourceOrphan}}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(safe) != 1 || safe[0] != "terminal/1" {
		t.Fatalf("safe orphans = %v", safe)
	}
}

func TestAssessReconciliationFailsClosedOnMissingUncertainConflictAndUnsafeOrphan(t *testing.T) {
	_, err := assessReconciliation(
		"target", []string{"missing/1", "uncertain/1"}, map[string]bool{"live-orphan/1": false}, time.Now(),
		[]reconciliationObservation{{key: "uncertain/1", classification: ports.PhysicalResourceUncertain, diagnostic: "duplicate claims"}},
		[]reconciliationObservation{{key: "live-orphan/1", runtimeID: "runtime-orphan", classification: ports.PhysicalResourceOrphan}},
		[]ports.PhysicalResourceConflict{{ResourceID: "foreign", Name: "world-target-bad", Classification: ports.PhysicalResourceForeign, Diagnostic: "malformed labels"}},
	)
	if err == nil {
		t.Fatal("unsafe inventory unexpectedly accepted")
	}
	for _, fragment := range []string{"missing from the inventory", "uncertain", "unsafe target orphan", "inventory conflict"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q does not contain %q", err, fragment)
		}
	}
}

func TestAssessReconciliationRejectsIncompleteDriverReport(t *testing.T) {
	_, err := assessReconciliation("agent", []string{"expected/1"}, nil, time.Time{}, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no observation time") || !strings.Contains(err.Error(), "missing from the inventory") {
		t.Fatalf("incomplete report error = %v", err)
	}
}

func TestCompleteProvisioningBindingRejectsZeroDigestSentinel(t *testing.T) {
	bound, err := completeProvisioningBinding("terminal generation", "sha256:"+strings.Repeat("0", 64), "physical/key")
	if bound || err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("zero digest binding = bound %t, error %v", bound, err)
	}
}

func TestControllerReconcilePhysicalResourcesRestoresBoundPlansAndReadmits(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	resolver := &reconciliationResolverSpy{ProvisioningResolver: harness.resolver, forgetTargetKey: true}
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agent, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
		Workspace: harness.workspace, Resolver: resolver, Capabilities: harness.capabilities, Observers: harness.controller.observers,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Agent.Expected) != 1 || len(report.Targets[domain.TargetLinuxContainer].Expected) != 1 {
		t.Fatalf("reconciliation report = %#v", report)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if len(agent.expected) != 1 || agent.expected[0].IdempotencyKey != generation.AgentProvisioningKey {
		t.Fatalf("agent plan did not restore persisted identity: %#v", agent.expected)
	}
	targetGen, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(targetDriver.expected) != 1 || targetDriver.expected[0].Target.ID().String() != target.ID ||
		targetDriver.expected[0].IdempotencyKey != targetGen.ProvisioningKey {
		t.Fatalf("target plans = %#v", targetDriver.expected)
	}
	if resolver.persistedAgentCalls != 1 || resolver.targetCalls != 1 || resolver.agentAdmissions != 1 {
		t.Fatalf("resolver calls persisted_agent=%d target=%d agent_admission=%d", resolver.persistedAgentCalls, resolver.targetCalls, resolver.agentAdmissions)
	}
}

func TestControllerReconcilePhysicalResourcesRemovesOnlyProvenTerminalOrphan(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.core.TransitionAgentGeneration(context.Background(), application.TransitionAgentRequest{
		Meta: fixture.meta("terminal-agent-orphan"), AgentWorkspaceID: view.Agent.ID, Generation: generation.Generation,
		ExpectedRevision: generation.Revision, State: domain.AgentGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := agentGenerationRef(failed.ID, failed.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	inventories := 0
	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		inventories++
		if len(expected) != 0 {
			t.Fatalf("terminal generation was treated as expected: %#v", expected)
		}
		report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
		if inventories == 1 {
			report.Unclaimed = []ports.AgentWorkspaceReconciliation{{
				Ref: ref, ContainerID: "terminal-runtime", Classification: ports.PhysicalResourceOrphan,
				Diagnostic: "valid world-owned generation is absent from active plans",
			}}
		}
		return report
	}
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agent, Workspace: harness.workspace, Resolver: harness.resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inventories != 2 || len(agent.destroyed) != 1 || agent.destroyed[0] != ref || len(report.RemovedAgentOrphans) != 1 {
		t.Fatalf("inventories=%d destroyed=%v report=%#v", inventories, agent.destroyed, report)
	}
}

func TestControllerReconcilePhysicalResourcesRejectsUnsafeOrphanWithoutDestroy(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	harness.acquire(t, fixture)
	unknownID, err := fixture.ids.AgentWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	unsafeRef := ports.AgentWorkspaceRef{ID: unknownID, Generation: 1}
	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
		for _, plan := range expected {
			spec := plan.Generation.Spec()
			report.Expected = append(report.Expected, ports.AgentWorkspaceReconciliation{
				Ref: ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation}, ContainerID: "active-runtime",
				Classification: ports.PhysicalResourceAdopted,
			})
		}
		report.Unclaimed = []ports.AgentWorkspaceReconciliation{{Ref: unsafeRef, ContainerID: "unsafe-runtime", Classification: ports.PhysicalResourceOrphan}}
		return report
	}
	controller, err := NewController(ControllerConfig{Core: fixture.core, Agent: agent, Workspace: harness.workspace, Resolver: harness.resolver})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = controller.ReconcilePhysicalResources(ctx)
	if err == nil || !strings.Contains(err.Error(), "unsafe agent workspace orphan") {
		t.Fatalf("unsafe orphan error = %v", err)
	}
	if len(agent.destroyed) != 0 {
		t.Fatalf("unsafe orphan was destroyed: %v", agent.destroyed)
	}
}

func TestControllerReconcilePhysicalResourcesRejectsDriverDowngradeWithBoundHistory(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	harness.acquire(t, fixture)
	logical, err := NewController(ControllerConfig{Core: fixture.core})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = logical.ReconcilePhysicalResources(ctx)
	if err == nil || !strings.Contains(err.Error(), "persisted physical agent history") {
		t.Fatalf("driver downgrade error = %v", err)
	}
}

func TestControllerReconcilePhysicalResourcesFinalizesInterruptedRunAndLosesActiveOperation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the sealed-bundle safe-path implementation requires the dedicated Linux host filesystem")
	}
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("crash-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := fixture.core.CreateTargetOperation(context.Background(), application.CreateTargetOperationRequest{
		Meta: fixture.meta("crash-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationExec, CommandDisplay: "active specimen exec",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = fixture.core.TransitionTargetOperation(context.Background(), application.TransitionTargetOperationRequest{
		Meta: fixture.meta("crash-operation-running"), TargetID: target.ID, OperationID: operation.ID,
		ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldObserverRecord := harness.controller.observers.records[run.ID]
	if oldObserverRecord == nil || oldObserverRecord.timer == nil {
		t.Fatal("test run did not own the pre-crash maximum-duration timer")
	}
	observerStateRoot := harness.controller.observers.stateRoot
	fixture.clock.Advance(10 * time.Second)
	restartedObservers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: fixture.ledger, IDs: fixture.ids, Clock: fixture.clock.Now, StateRoot: observerStateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	freshTarget := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, testkit.NewOwnershipTracker())
	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: freshTarget}
	var recovered ports.PreparedTargetRun
	targetDriver.recover = func(plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
		spec := plan.Run.Spec()
		recovered = ports.PreparedTargetRun{
			RunID: spec.ID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
			MaterializationDigest: spec.MaterializationDigest, RequiredCoverage: append([]string(nil), plan.RequiredCoverage...),
			Attachment: oldObserverRecord.start.Prepared.Attachment, PreparedAt: fixture.clock.Now(),
		}
		return recovered, nil
	}
	targetDriver.stop = func(runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
		if mode != ports.StopForce || runID.String() != run.ID {
			t.Fatalf("recovery stop run=%s mode=%s", runID, mode)
		}
		stoppedAt := fixture.clock.Now()
		changes, changeErr := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
		if changeErr != nil {
			return ports.TargetRunStopReceipt{}, changeErr
		}
		return ports.TargetRunStopReceipt{
			RunID: runID, Outcome: ports.RunFailed, FailureKind: ports.TargetRunFailureNeverStarted,
			StoppedAt: stoppedAt, TargetChanges: changes,
			Observations: []ports.TargetRunObservation{
				{Kind: "target.run.control-plane-loss", ObservedAt: recovered.PreparedAt, Payload: []byte(`{"continuity":false}`)},
				{Kind: "target.run.never-started", ObservedAt: stoppedAt, Payload: []byte(`{"resumed":false}`)},
			},
		}, nil
	}
	agentDriver := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	harness.capabilities.targets[domain.TargetLinuxContainer] = targetDriver
	harness.capabilities.observers = restartedObservers
	finalization, err := application.NewRunFinalizationService(fixture.core, inMemoryBundleFinalizer{}, testkit.NewFakeMaterialAuthority(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	harness.capabilities.finalization = finalization
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agentDriver, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
		Workspace: harness.workspace, Resolver: harness.resolver, Capabilities: harness.capabilities, Observers: restartedObservers,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if targetDriver.recoverCalls != 1 || targetDriver.stopCalls != 1 || targetDriver.startCalls != 0 {
		t.Fatalf("recovery calls recover=%d stop=%d start=%d", targetDriver.recoverCalls, targetDriver.stopCalls, targetDriver.startCalls)
	}
	if len(report.RecoveredRuns) != 1 || report.RecoveredRuns[0] != run.ID || len(report.LostTargetOperations) != 1 || report.LostTargetOperations[0] != operation.ID {
		t.Fatalf("recovery report = %#v", report)
	}
	recoveredRecord := restartedObservers.records[run.ID]
	if recoveredRecord == nil || recoveredRecord.timer != nil {
		t.Fatalf("recovery reset the maximum-duration timer: %#v", recoveredRecord)
	}
	latestTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	latestRun, err := targetRun(latestTarget, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	latestOperation, err := targetOperation(latestTarget, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latestRun.State != domain.TargetRunFailed || latestRun.BundleID == "" || latestRun.BundleArtifact == "" || latestRun.BundleDigest == "" || len(latestRun.IncidentIDs) == 0 {
		t.Fatalf("interrupted run was not sealed failed with an incident: %#v", latestRun)
	}
	if latestOperation.State != domain.TargetOperationLost {
		t.Fatalf("active target exec survived recovery: %#v", latestOperation)
	}
	bundle, err := harness.capabilities.GetObservationBundle(context.Background(), &worldv1.GetObservationBundleRequest{TargetRunId: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.State != string(domain.ObservationBundleSealed) || len(bundle.IncidentIds) == 0 || len(bundle.Gaps) == 0 {
		t.Fatalf("interrupted run bundle = %#v", bundle)
	}
	foundControlPlaneLoss := false
	for _, gap := range bundle.Gaps {
		foundControlPlaneLoss = foundControlPlaneLoss || strings.Contains(gap.Detail, "control-plane loss")
	}
	if !foundControlPlaneLoss {
		t.Fatalf("bundle omitted explicit control-plane-loss gap: %#v", bundle.Gaps)
	}

	revision, bundleID := latestRun.Revision, latestRun.BundleID
	second, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	latestTarget, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	latestRun, err = targetRun(latestTarget, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.RecoveredRuns) != 0 || targetDriver.recoverCalls != 1 || targetDriver.stopCalls != 1 || latestRun.Revision != revision || latestRun.BundleID != bundleID {
		t.Fatalf("terminal run was touched by retry: report=%#v run=%#v", second, latestRun)
	}
}

type inMemoryBundleFinalizer struct{}

func (inMemoryBundleFinalizer) Finalize(_ context.Context, request observationbundle.FinalizeRequest) (observationbundle.Result, error) {
	result := request.Result
	bundle, err := domain.NewObservationBundle(domain.ObservationBundleSpec{
		ID: request.BundleID, TargetRunID: result.RunID, TargetID: request.TargetID,
		TargetGeneration: request.TargetGeneration, AgentWorkspaceID: request.AgentWorkspaceID, AgentGeneration: request.AgentGeneration,
		FirstCursor: result.FirstCursor, LastCursor: result.LastCursor, RawArtifacts: result.RawArtifacts,
		NormalizedEvents: result.NormalizedEvents, Metrics: result.Metrics, Coverage: result.Coverage, Gaps: result.Gaps,
		TargetChanges: result.TargetChanges, IncidentIDs: result.IncidentIDs, Summary: result.Summary, CreatedAt: request.CreatedAt,
	})
	if err != nil {
		return observationbundle.Result{}, err
	}
	bundle, err = bundle.Seal(bundle.Revision(), request.FinalizedAt)
	if err != nil {
		return observationbundle.Result{}, err
	}
	content := testkit.NewMemoryContentSource([]byte("sealed in-memory observation bundle"))
	return observationbundle.Result{Bundle: bundle, Content: content, Created: true}, nil
}

func terminalizeFixtureAgent(t *testing.T, fixture *integrationFixture) {
	t.Helper()
	generation, err := currentAgentGeneration(fixture.view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.core.TransitionAgentGeneration(context.Background(), application.TransitionAgentRequest{
		Meta: fixture.meta("retire-logical-fixture-agent"), AgentWorkspaceID: fixture.view.Agent.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision, State: domain.AgentGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
}
