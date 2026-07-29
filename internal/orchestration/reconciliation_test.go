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
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
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

func (d *reconciliationAgentDriver) ReconcileAgentWorkspaces(_ context.Context, request ports.AgentWorkspaceReconciliationRequest) (ports.AgentWorkspaceReconciliationReport, error) {
	expected := allAgentInventoryPlans(request)
	d.expected = append([]ports.AgentWorkspacePlan(nil), expected...)
	if d.reconcile != nil {
		return d.reconcile(expected), nil
	}
	report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		ref := ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation}
		if containsAgentRef(d.destroyed, ref) {
			report.Expected = append(report.Expected, ports.AgentWorkspaceReconciliation{Ref: ref, Classification: ports.PhysicalResourceMissing})
			continue
		}
		report.Expected = append(report.Expected, ports.AgentWorkspaceReconciliation{
			Ref:         ref,
			ContainerID: "agent-runtime-" + spec.AgentWorkspaceID.String(), Classification: ports.PhysicalResourceAdopted, PlanMatched: true,
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
	reconcile    func([]ports.TargetPlan) ports.TargetReconciliationReport
	expected     []ports.TargetPlan
	destroy      func(ports.TargetRef) error
	destroyed    []ports.TargetRef
	recover      func(ports.TargetRunPlan) (ports.PreparedTargetRun, error)
	stop         func(domain.TargetRunID, ports.StopMode) (ports.TargetRunStopReceipt, error)
	recoverCalls int
	startCalls   int
	stopCalls    int
}

func (d *reconciliationTargetDriver) ReconcileTargets(_ context.Context, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	expected := allTargetInventoryPlans(request)
	d.expected = append([]ports.TargetPlan(nil), expected...)
	if d.reconcile != nil {
		return d.reconcile(expected), nil
	}
	report := ports.TargetReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}
		if containsTargetRef(d.destroyed, ref) {
			report.Expected = append(report.Expected, ports.TargetReconciliation{Ref: ref, Classification: ports.PhysicalResourceMissing})
			continue
		}
		report.Expected = append(report.Expected, ports.TargetReconciliation{
			Ref: ref, RuntimeID: "target-runtime-" + spec.TargetID.String(),
			Classification: ports.PhysicalResourceAdopted, PlanMatched: true,
		})
	}
	return report, nil
}

func containsAgentRef(refs []ports.AgentWorkspaceRef, expected ports.AgentWorkspaceRef) bool {
	for _, ref := range refs {
		if ref == expected {
			return true
		}
	}
	return false
}

func containsTargetRef(refs []ports.TargetRef, expected ports.TargetRef) bool {
	for _, ref := range refs {
		if ref == expected {
			return true
		}
	}
	return false
}

func (d *reconciliationTargetDriver) Destroy(ctx context.Context, ref ports.TargetRef) error {
	d.destroyed = append(d.destroyed, ref)
	if d.destroy != nil {
		return d.destroy(ref)
	}
	return d.FakeTargetDriver.Destroy(ctx, ref)
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

func TestAssessReconciliationAcceptsOnlyExactPlanMatchedTerminalCleanup(t *testing.T) {
	safe, err := assessReconciliation(
		"agent workspace", []string{"active/1", "terminal/1"}, map[string]bool{"active/1": false, "terminal/1": true}, time.Now(),
		[]reconciliationObservation{
			{key: "active/1", runtimeID: "runtime-active", classification: ports.PhysicalResourceAdopted},
			{key: "terminal/1", runtimeID: "runtime-terminal", classification: ports.PhysicalResourceUncertain, planMatched: true},
		}, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(safe) != 1 || safe[0] != "terminal/1" {
		t.Fatalf("safe orphans = %v", safe)
	}
}

func TestAssessReconciliationAllowsExactStoppedRuntimeOnlyForInterruptedRunRecovery(t *testing.T) {
	expected := []reconciliationObservation{{
		key: "target/2", runtimeID: "world-emulator-5556",
		classification: ports.PhysicalResourceUncertain, planMatched: true,
		diagnostic: "exact plan-owned runtime is stopped and preserved",
	}}
	if _, err := assessReconciliationWithAllowedMissing(
		"android target", []string{"target/2"}, nil, nil, nil, time.Now().UTC(), expected, nil, nil,
	); err == nil || !strings.Contains(err.Error(), "is uncertain") {
		t.Fatalf("unapproved stopped runtime assessment error = %v", err)
	}
	safe, err := assessReconciliationWithAllowedMissing(
		"android target", []string{"target/2"}, nil, nil, map[string]bool{"target/2": true}, time.Now().UTC(), expected, nil, nil,
	)
	if err != nil || len(safe) != 0 {
		t.Fatalf("approved interrupted-run stopped runtime assessment = safe=%v err=%v", safe, err)
	}

	withoutPlanMatch := append([]reconciliationObservation(nil), expected...)
	withoutPlanMatch[0].planMatched = false
	if _, err := assessReconciliationWithAllowedMissing(
		"android target", []string{"target/2"}, nil, nil, map[string]bool{"target/2": true}, time.Now().UTC(), withoutPlanMatch, nil, nil,
	); err == nil {
		t.Fatal("interrupted-run authority accepted a stopped runtime without an exact plan match")
	}
	if _, err := assessReconciliationWithAllowedMissing(
		"android target", []string{"target/2"}, nil, nil, map[string]bool{"foreign/9": true}, time.Now().UTC(), expected, nil, nil,
	); err == nil || !strings.Contains(err.Error(), "unexpected identity") {
		t.Fatalf("foreign stopped-run recovery authorization error = %v", err)
	}
}

func TestAssessReconciliationAcceptsMissingTerminalCleanupResidue(t *testing.T) {
	safe, err := assessReconciliation(
		"target", []string{"terminal/1"}, map[string]bool{"terminal/1": true}, time.Now(),
		[]reconciliationObservation{{
			key: "terminal/1", classification: ports.PhysicalResourceMissing, cleanupRequired: true,
			diagnostic: "runtime absent; exact local allocation remains",
		}}, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(safe) != 1 || safe[0] != "terminal/1" {
		t.Fatalf("safe cleanup candidates = %v", safe)
	}
}

func TestAssessReconciliationRejectsInvalidCleanupRequiredEvidence(t *testing.T) {
	_, err := assessReconciliation(
		"target", []string{"terminal/1"}, map[string]bool{"terminal/1": true}, time.Now(),
		[]reconciliationObservation{{key: "terminal/1", runtimeID: "runtime", classification: ports.PhysicalResourceAdopted, planMatched: true, cleanupRequired: true}}, nil, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid cleanup-required evidence") {
		t.Fatalf("invalid cleanup evidence error = %v", err)
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
	for _, fragment := range []string{"missing from the inventory", "uncertain", "unsafe target unclaimed resource", "inventory conflict"} {
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

func TestControllerReconcilePhysicalResourcesPreservesCurrentQuarantinedTarget(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	quarantined, err := fixture.core.QuarantineTarget(context.Background(), application.QuarantineTargetRequest{
		Meta: fixture.meta("quarantined-before-restart"), TargetID: target.ID, ExpectedRevision: target.Revision,
		Reason: "preserve evidence across restart",
		Evidence: ports.TargetQuarantineEvidence{
			Target:    ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)},
			RuntimeID: "target-runtime-" + target.ID, ExecutionStopped: true, NetworkUnreachable: true,
			StatePreserved: true, ObservedAt: fixture.clock.Now(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
		Workspace: harness.workspace, Resolver: harness.resolver, Capabilities: harness.capabilities,
		Observers: harness.controller.observers,
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
	if len(targetDriver.expected) != 1 || len(targetDriver.destroyed) != 0 || len(report.RemovedTargetOrphans[domain.TargetLinuxContainer]) != 0 {
		t.Fatalf("quarantined target was not preserved: expected=%d destroyed=%v report=%#v", len(targetDriver.expected), targetDriver.destroyed, report)
	}
	latest, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(latest)
	if err != nil || generation.State != domain.TargetGenerationQuarantined || latest.Revision != quarantined.Revision {
		t.Fatalf("quarantined logical state changed: generation=%#v target_revision=%d want=%d err=%v", generation, latest.Revision, quarantined.Revision, err)
	}
}

func TestControllerReconcilePhysicalResourcesResumesExactDurableTargetDestruction(t *testing.T) {
	tests := []struct {
		name            string
		makeResettable  bool
		physicalPresent bool
		stickyCleanup   bool
	}{
		{name: "reservation_before_logical_boundary", physicalPresent: true},
		{name: "resettable_before_physical_destroy", makeResettable: true, physicalPresent: true},
		{name: "physical_destroyed_before_logical_commit", makeResettable: true},
		{name: "local_cleanup_not_retired", makeResettable: true, stickyCleanup: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			generation, err := targetGeneration(target)
			if err != nil {
				t.Fatal(err)
			}
			reservationKey := "destroy-crash-window/" + test.name
			if err := harness.capabilities.reserveOperation(context.Background(), "destroy_target", target.ID, reservationKey, "signature-"+test.name, ledger.Identity{
				ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
				TargetGeneration: target.CurrentGeneration,
			}); err != nil {
				t.Fatal(err)
			}
			if test.makeResettable {
				target, err = fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
					Meta: fixture.meta("destroy-crash-resettable"), TargetID: target.ID, Generation: target.CurrentGeneration,
					ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			physicalPresent := test.physicalPresent
			cleanupRequired := !physicalPresent
			inventories := 0
			targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
			targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
				inventories++
				report := ports.TargetReconciliationReport{ObservedAt: time.Now().UTC()}
				for _, plan := range expected {
					spec := plan.Generation.Spec()
					item := ports.TargetReconciliation{Ref: ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}}
					if physicalPresent {
						item.RuntimeID, item.Classification = "runtime-before-destroy", ports.PhysicalResourceAdopted
						item.PlanMatched = true
					} else {
						item.Classification, item.Diagnostic = ports.PhysicalResourceMissing, "authoritative absence"
						item.CleanupRequired = cleanupRequired
					}
					report.Expected = append(report.Expected, item)
				}
				return report
			}
			targetDriver.destroy = func(ports.TargetRef) error {
				physicalPresent = false
				if !test.stickyCleanup {
					cleanupRequired = false
				}
				return nil
			}
			reloadedCapabilities := fixture.service(Config{
				Finalization: harness.capabilities.finalization, Agent: harness.agent,
				Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
				Workspace: harness.workspace, WorkspaceScope: harness.capabilities.workspaceScope,
				Material: harness.capabilities.material, Captures: harness.capture,
				Observers: harness.controller.observers, ActionEvidence: harness.capabilities.actionEvidence,
			})
			newController := func() *Controller {
				controller, err := NewController(ControllerConfig{
					Core: fixture.core, Agent: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
					Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
					Workspace: harness.workspace, Resolver: harness.resolver, Capabilities: reloadedCapabilities,
					Observers: harness.controller.observers,
				})
				if err != nil {
					t.Fatal(err)
				}
				return controller
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			report, err := newController().ReconcilePhysicalResources(ctx)
			cancel()
			if test.stickyCleanup {
				if err == nil || !strings.Contains(err.Error(), "physical cleanup remains") {
					t.Fatalf("stale local cleanup error = %v", err)
				}
				latest, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				latestGeneration, generationErr := targetGeneration(latest)
				if generationErr != nil || latestGeneration.State != domain.TargetGenerationResettable {
					t.Fatalf("failed cleanup committed logical destruction: %#v, %v", latestGeneration, generationErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ref := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
			if physicalPresent || inventories != 2 || len(targetDriver.destroyed) != 1 || targetDriver.destroyed[0] != ref || len(report.RecoveredTargetDestructions) != 1 || report.RecoveredTargetDestructions[0] != ref {
				t.Fatalf("destroy recovery mismatch: present=%t inventories=%d destroyed=%v report=%#v", physicalPresent, inventories, targetDriver.destroyed, report)
			}
			latest, err := fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			latestGeneration, err := targetGeneration(latest)
			if err != nil || latestGeneration.State != domain.TargetGenerationDestroyed {
				t.Fatalf("logical destruction was not committed: %#v, %v", latestGeneration, err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			second, err := newController().ReconcilePhysicalResources(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if len(targetDriver.destroyed) != 1 || len(second.RecoveredTargetDestructions) != 0 || len(targetDriver.expected) != 1 || len(second.Targets[target.Kind].Expected) != 1 || second.Targets[target.Kind].Expected[0].Classification != ports.PhysicalResourceMissing {
				t.Fatalf("completed destruction replayed: destroyed=%v expected=%v report=%#v", targetDriver.destroyed, targetDriver.expected, second)
			}
		})
	}
}

func mustTargetID(t *testing.T, value string) domain.TargetID {
	t.Helper()
	id, err := domain.ParseTargetID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
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
		report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
		if inventories == 1 {
			if len(expected) != 1 || agentPlanKey(expected[0]) != agentRefKey(ref) {
				t.Fatalf("terminal generation did not carry its exact cleanup plan: %#v", expected)
			}
			report.Expected = []ports.AgentWorkspaceReconciliation{{
				Ref: ref, ContainerID: "terminal-runtime", Classification: ports.PhysicalResourceUncertain, PlanMatched: true,
				Diagnostic: "exact cleanup plan matches without adoption",
			}}
		} else {
			if len(expected) != 1 || agentPlanKey(expected[0]) != agentRefKey(ref) {
				t.Fatalf("terminal cleanup verification lost its exact plan: %#v", expected)
			}
			report.Expected = []ports.AgentWorkspaceReconciliation{{Ref: ref, Classification: ports.PhysicalResourceMissing}}
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
	workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if hasOwnership(harness.tracker.Snapshot(), "workspace", workspaceID.String()) {
		t.Fatalf("terminal agent orphan left its workspace owned: %#v", harness.tracker.Snapshot())
	}
}

func TestControllerReconcilePhysicalResourcesRemovesWorkspaceWhenTerminalContainerIsAlreadyMissing(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.core.TransitionAgentGeneration(context.Background(), application.TransitionAgentRequest{
		Meta: fixture.meta("terminal-agent-missing-runtime"), AgentWorkspaceID: view.Agent.ID, Generation: generation.Generation,
		ExpectedRevision: generation.Revision, State: domain.AgentGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := agentGenerationRef(failed.ID, failed.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	physicalCtx, physicalCancel := context.WithTimeout(context.Background(), time.Second)
	if err := harness.agent.Stop(physicalCtx, ref, ports.StopForce); err != nil {
		physicalCancel()
		t.Fatal(err)
	}
	if err := harness.agent.Destroy(physicalCtx, ref); err != nil {
		physicalCancel()
		t.Fatal(err)
	}
	physicalCancel()

	inventories := 0
	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		inventories++
		report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
		if inventories == 1 {
			if len(expected) != 1 || agentPlanKey(expected[0]) != agentRefKey(ref) {
				t.Fatalf("terminal generation did not carry its exact cleanup plan: %#v", expected)
			}
			report.Expected = []ports.AgentWorkspaceReconciliation{{Ref: ref, Classification: ports.PhysicalResourceMissing}}
		} else {
			if len(expected) != 1 || agentPlanKey(expected[0]) != agentRefKey(ref) {
				t.Fatalf("terminal cleanup verification lost its exact plan: %#v", expected)
			}
			report.Expected = []ports.AgentWorkspaceReconciliation{{Ref: ref, Classification: ports.PhysicalResourceMissing}}
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
	report, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if inventories != 2 || len(agent.destroyed) != 1 || agent.destroyed[0] != ref || len(report.RemovedAgentOrphans) != 1 {
		t.Fatalf("inventories=%d destroyed=%v report=%#v", inventories, agent.destroyed, report)
	}
	workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), time.Second)
	_, inspectErr := harness.workspace.Inspect(inspectCtx, workspaceID)
	inspectCancel()
	if !domain.IsCode(inspectErr, domain.CodeNotFound) {
		t.Fatalf("terminal missing-runtime workspace still exists: %v", inspectErr)
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
	if err == nil || !strings.Contains(err.Error(), "unsafe agent workspace unclaimed resource") {
		t.Fatalf("unsafe orphan error = %v", err)
	}
	if len(agent.destroyed) != 0 {
		t.Fatalf("unsafe orphan was destroyed: %v", agent.destroyed)
	}
}

func TestControllerReconcilePhysicalResourcesRemovesOnlyPlanMatchedTerminalTarget(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta("terminal-target-cleanup"), TargetID: target.ID, Generation: target.CurrentGeneration,
		ExpectedRevision: generation.Revision, State: domain.TargetGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := ports.TargetRef{ID: mustTargetID(t, failed.ID), Generation: domain.TargetGeneration(failed.CurrentGeneration)}
	driver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	driver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		return targetOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, driver, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	report, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(driver.destroyed) != 1 || driver.destroyed[0] != ref || len(report.RemovedTargetOrphans[target.Kind]) != 1 {
		t.Fatalf("terminal target cleanup = destroyed %v report %#v", driver.destroyed, report)
	}
}

func TestControllerReconcilePhysicalResourcesRemovesMissingTerminalTargetCleanupResidue(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta("terminal-target-missing-runtime"), TargetID: target.ID, Generation: target.CurrentGeneration,
		ExpectedRevision: generation.Revision, State: domain.TargetGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := ports.TargetRef{ID: mustTargetID(t, failed.ID), Generation: domain.TargetGeneration(failed.CurrentGeneration)}
	inventories := 0
	driver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	driver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		inventories++
		report := ports.TargetReconciliationReport{ObservedAt: time.Now().UTC()}
		if inventories == 1 {
			if len(expected) != 1 || targetPlanKey(expected[0]) != targetRefKey(ref) {
				t.Fatalf("terminal target did not carry its exact cleanup plan: %#v", expected)
			}
			report.Expected = []ports.TargetReconciliation{{
				Ref: ref, Classification: ports.PhysicalResourceMissing, CleanupRequired: true,
				Diagnostic: "runtime absent; exact local allocation remains",
			}}
		} else {
			if len(expected) != 1 || targetPlanKey(expected[0]) != targetRefKey(ref) {
				t.Fatalf("terminal cleanup verification lost its exact plan: %#v", expected)
			}
			report.Expected = []ports.TargetReconciliation{{Ref: ref, Classification: ports.PhysicalResourceMissing}}
		}
		return report
	}
	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, driver, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	report, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if inventories != 2 || len(driver.destroyed) != 1 || driver.destroyed[0] != ref || len(report.RemovedTargetOrphans[target.Kind]) != 1 {
		t.Fatalf("inventories=%d destroyed=%v report=%#v", inventories, driver.destroyed, report)
	}
}

func TestControllerReconcilePhysicalResourcesPreservesTerminalTargetWithoutPlanMatch(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta("terminal-target-mismatch"), TargetID: target.ID, Generation: target.CurrentGeneration,
		ExpectedRevision: generation.Revision, State: domain.TargetGenerationFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := ports.TargetRef{ID: mustTargetID(t, failed.ID), Generation: domain.TargetGeneration(failed.CurrentGeneration)}
	driver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	driver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		report := ports.TargetReconciliationReport{ObservedAt: fixture.clock.Now()}
		for _, plan := range expected {
			itemRef := ports.TargetRef{ID: plan.Target.ID(), Generation: plan.Generation.Spec().Generation}
			if itemRef == ref {
				report.Expected = append(report.Expected, ports.TargetReconciliation{Ref: ref, RuntimeID: "foreign-runtime", Classification: ports.PhysicalResourceForeign, Diagnostic: "physical configuration differs from persisted plan"})
				continue
			}
			report.Expected = append(report.Expected, ports.TargetReconciliation{Ref: itemRef, RuntimeID: "runtime-" + targetRefKey(itemRef), Classification: ports.PhysicalResourceAdopted, PlanMatched: true})
		}
		return report
	}
	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, driver, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "lacks an exact cleanup-plan match") {
		t.Fatalf("terminal target mismatch error = %v", err)
	}
	if len(driver.destroyed) != 0 || !hasOwnership(harness.tracker.Snapshot(), "target", targetRefKey(ref)) {
		t.Fatalf("mismatched terminal target was mutated: destroyed=%v ownership=%#v", driver.destroyed, harness.tracker.Snapshot())
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
	targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		report := ports.TargetReconciliationReport{ObservedAt: fixture.clock.Now()}
		for _, plan := range expected {
			spec := plan.Generation.Spec()
			ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}
			report.Expected = append(report.Expected, ports.TargetReconciliation{
				Ref: ref, RuntimeID: "stopped-runtime-" + spec.TargetID.String(),
				Classification: ports.PhysicalResourceUncertain, PlanMatched: true,
				Diagnostic: "exact plan-owned runtime is stopped and preserved",
			})
		}
		return report
	}
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
