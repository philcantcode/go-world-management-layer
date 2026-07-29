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

func TestStartupUnboundAgentRecoveryRetainsPredecessorForExactClientRetry(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, pending := beginPersistedAgentRecovery(t, fixture, view)
	previous := ports.AgentWorkspaceRef{ID: mustAgentWorkspaceID(t, view.Agent.ID), Generation: domain.AgentGeneration(view.Agent.CurrentGeneration)}
	current := ports.AgentWorkspaceRef{ID: previous.ID, Generation: domain.AgentGeneration(pending.Agent.CurrentGeneration)}

	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	report := reconcileStartupRecovery(t, controller)

	requireSingleStartupRecoveryAgentRef(t, report.PendingAgentProvisionings, current)
	if len(agent.destroyed) != 0 {
		t.Fatalf("unbound recovery destroyed predecessor: %v", agent.destroyed)
	}
	if !hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(previous)) {
		t.Fatal("unbound recovery did not retain predecessor ownership")
	}
	incident, err := fixture.core.GetIncident(context.Background(), request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if incident.State != domain.IncidentRecovering {
		t.Fatalf("incident state = %s, want recovering", incident.State)
	}

	request.Meta.Deadline = request.Meta.Deadline.Add(time.Minute)
	outcome, err := controller.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Incident.State != domain.IncidentResolved || outcome.Agent == nil || outcome.Agent.CurrentGeneration != uint64(current.Generation) {
		t.Fatalf("exact client retry outcome = %#v", outcome)
	}
	if hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(previous)) ||
		!hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(current)) {
		t.Fatalf("ownership after exact retry = %#v", harness.tracker.Snapshot())
	}
}

func TestStartupBoundAgentRecoveryRetiresBeforeProvisionAndResolvesIncident(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, outcome := beginPersistedAgentRecovery(t, fixture, view)
	pending, _ := bindPersistedAgentRecovery(t, harness, request, outcome)
	previous := ports.AgentWorkspaceRef{ID: mustAgentWorkspaceID(t, view.Agent.ID), Generation: domain.AgentGeneration(view.Agent.CurrentGeneration)}
	current := ports.AgentWorkspaceRef{ID: previous.ID, Generation: domain.AgentGeneration(pending.Agent.CurrentGeneration)}

	calls := &startupRecoveryCallLog{}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		calls.record("agent.inventory.observed")
		return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	report := reconcileStartupRecovery(t, controller)

	if len(agent.destroyed) != 1 || agent.destroyed[0] != previous {
		t.Fatalf("destroyed agent generations = %v, want [%v]", agent.destroyed, previous)
	}
	requireStartupRecoveryCallOrder(t, calls.calls, "agent.inventory", "agent.provision")
	if !hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(current)) ||
		hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(previous)) {
		t.Fatalf("recovered ownership = %#v", harness.tracker.Snapshot())
	}
	requireSingleStartupRecoveryAgentRef(t, report.RecoveredAgentRecoveries, current)
	incident, err := fixture.core.GetIncident(context.Background(), request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if incident.State != domain.IncidentResolved || !containsExactString(incident.RecoveryActions, "physical-agent:recreate") {
		t.Fatalf("completed recovery incident = %#v", incident)
	}
	requireStartupRecoveryAgentState(t, fixture, view.Session.ID, domain.AgentGenerationReady)
}

func TestStartupBoundAgentRecoveryClosesPostRetirementCrashWindows(t *testing.T) {
	tests := []struct {
		name             string
		provisionCurrent bool
		advanceReady     bool
	}{
		{name: "predecessor_and_successor_absent"},
		{name: "successor_physical_before_logical_ready", provisionCurrent: true},
		{name: "successor_ready_before_incident_completion", provisionCurrent: true, advanceReady: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			request, outcome := beginPersistedAgentRecovery(t, fixture, view)
			pending, plan := bindPersistedAgentRecovery(t, harness, request, outcome)
			previousResource, err := requirePreviousAgentResource(pending.Agent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
			if err := harness.controller.destroyAgentAndWorkspace(ctx, previousResource.ref, previousResource.workspaceID, ports.StopForce); err != nil {
				cancel()
				t.Fatal(err)
			}
			if test.provisionCurrent {
				if err := harness.controller.provisionAgentPhysical(ctx, plan); err != nil {
					cancel()
					t.Fatal(err)
				}
			}
			if test.advanceReady {
				pending.Agent, err = harness.controller.advanceAgentReady(ctx, request.Meta, pending.Agent)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
			}
			cancel()

			agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
			agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
				return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
			}
			controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
			report := reconcileStartupRecovery(t, controller)
			current := ports.AgentWorkspaceRef{ID: mustAgentWorkspaceID(t, pending.Agent.ID), Generation: domain.AgentGeneration(pending.Agent.CurrentGeneration)}
			requireSingleStartupRecoveryAgentRef(t, report.RecoveredAgentRecoveries, current)
			incident, err := fixture.core.GetIncident(context.Background(), request.IncidentID)
			if err != nil || incident.State != domain.IncidentResolved {
				t.Fatalf("recovered incident = %#v, %v", incident, err)
			}
			if !hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(current)) {
				t.Fatalf("successor ownership missing: %#v", harness.tracker.Snapshot())
			}
		})
	}
}

func TestStartupBoundAgentRecoveryRejectsBothPhysicalGenerations(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, outcome := beginPersistedAgentRecovery(t, fixture, view)
	pending, plan := bindPersistedAgentRecovery(t, harness, request, outcome)
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	if err := harness.controller.provisionAgentPhysical(ctx, plan); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	previous := ports.AgentWorkspaceRef{ID: mustAgentWorkspaceID(t, view.Agent.ID), Generation: domain.AgentGeneration(view.Agent.CurrentGeneration)}
	current := ports.AgentWorkspaceRef{ID: previous.ID, Generation: domain.AgentGeneration(pending.Agent.CurrentGeneration)}

	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), time.Second)
	_, err := controller.ReconcilePhysicalResources(startupCtx)
	startupCancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("both-generation recovery error = %v", err)
	}
	if len(agent.destroyed) != 0 || !hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(previous)) ||
		!hasOwnership(harness.tracker.Snapshot(), "agent_workspace", agentRefKey(current)) {
		t.Fatalf("unsafe pair was mutated: destroyed=%v ownership=%#v", agent.destroyed, harness.tracker.Snapshot())
	}
}

func TestStartupResolvedAgentRecoveryRejectsIncompleteSuccessor(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, outcome := beginPersistedAgentRecovery(t, fixture, view)
	bindPersistedAgentRecovery(t, harness, request, outcome)
	completeRecoveryIncidentForTest(t, fixture, outcome.Incident.ID, "physical-agent:recreate")

	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("incomplete resolved recovery error = %v", err)
	}
	if len(agent.destroyed) != 0 {
		t.Fatalf("incomplete resolved recovery mutated physical state: %v", agent.destroyed)
	}
}

func TestStartupResolvedAgentRecoveryRequiresExactCompletedPhysicalPair(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, outcome := beginPersistedAgentRecovery(t, fixture, view)
	pending, plan := bindPersistedAgentRecovery(t, harness, request, outcome)
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	if err := harness.controller.provisionAgentPhysical(ctx, plan); err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err := harness.controller.advanceAgentReady(ctx, request.Meta, pending.Agent); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	completeRecoveryIncidentForTest(t, fixture, outcome.Incident.ID, "physical-agent:recreate")

	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("resolved recovery with both generations error = %v", err)
	}
	if len(agent.destroyed) != 0 {
		t.Fatalf("resolved recovery pair verification mutated state: %v", agent.destroyed)
	}
}

func TestStartupResolvedAgentRecoveryAdmitsExactCompletedPhysicalPair(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	request, outcome := beginPersistedAgentRecovery(t, fixture, view)
	pending, plan := bindPersistedAgentRecovery(t, harness, request, outcome)
	previous, err := requirePreviousAgentResource(pending.Agent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	if err = harness.controller.destroyAgentAndWorkspace(ctx, previous.ref, previous.workspaceID, ports.StopForce); err == nil {
		err = harness.controller.provisionAgentPhysical(ctx, plan)
	}
	if err == nil {
		_, err = harness.controller.advanceAgentReady(ctx, request.Meta, pending.Agent)
	}
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	completeRecoveryIncidentForTest(t, fixture, outcome.Incident.ID, "physical-agent:recreate")
	agent := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	agent.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
		return agentOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	reconcileStartupRecovery(t, controller)
	if len(agent.destroyed) != 0 {
		t.Fatalf("already-completed recovery replayed destruction: %v", agent.destroyed)
	}
	if !containsAgentPlanRef(agent.expected, previous.ref) {
		t.Fatalf("resolved predecessor disappeared from converged cleanup inventory: %#v", agent.expected)
	}
}

func TestStartupReadyTargetRecoveryCompletesLinkedIncidentAfterExactPairProof(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident, pending := prepareReadyPersistedTargetRecovery(t, fixture, harness, view, target)
	stillRecovering, err := fixture.core.GetIncident(context.Background(), incident.ID)
	if err != nil || stillRecovering.State != domain.IncidentRecovering {
		t.Fatalf("pre-startup incident = %#v, %v", stillRecovering, err)
	}

	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		return targetOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(
		t, fixture, harness,
		&reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, targetDriver, harness.workspace,
	)
	report := reconcileStartupRecovery(t, controller)
	want := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(pending.CurrentGeneration)}
	requireSingleStartupRecoveryTargetRef(t, report.CompletedTargetRecoveries, want)
	completed, err := fixture.core.GetIncident(context.Background(), incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != domain.IncidentResolved || !containsExactString(completed.RecoveryActions, "physical-target:baseline") {
		t.Fatalf("completed target incident = %#v", completed)
	}
}

func TestStartupReadyTargetRecoveryRejectsBothPhysicalGenerations(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident, pending := prepareReadyPersistedTargetRecovery(t, fixture, harness, view, target)
	previous := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
	current := ports.TargetRef{ID: previous.ID, Generation: domain.TargetGeneration(pending.CurrentGeneration)}
	if err := harness.tracker.Acquire("target", targetRefKey(previous), view.Lease.ID); err != nil {
		t.Fatal(err)
	}

	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		return targetOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(
		t, fixture, harness,
		&reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, targetDriver, harness.workspace,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("both-generation target recovery error = %v", err)
	}
	if !hasOwnership(harness.tracker.Snapshot(), "target", targetRefKey(previous)) ||
		!hasOwnership(harness.tracker.Snapshot(), "target", targetRefKey(current)) {
		t.Fatalf("unsafe target pair was mutated: ownership=%#v", harness.tracker.Snapshot())
	}
	stillRecovering, loadErr := fixture.core.GetIncident(context.Background(), incident.ID)
	if loadErr != nil || stillRecovering.State != domain.IncidentRecovering {
		t.Fatalf("unsafe target pair changed incident: %#v, %v", stillRecovering, loadErr)
	}
}

func TestStartupResolvedTargetRecoveryRejectsIncompleteSuccessor(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentLinuxTargetFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		TargetID: target.ID, TargetGeneration: target.CurrentGeneration, Trigger: "premature resolved reset", LastKnownState: "ready",
	})
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("startup-incomplete-target-recovery"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceTarget,
		Strategy: string(ports.ResetBaseline), VisibilityAcknowledgement: "sealed evidence visible",
	}
	outcome, err := fixture.core.RecoverIncident(context.Background(), request)
	if err != nil || outcome.Target == nil {
		t.Fatalf("begin target recovery: %#v, %v", outcome, err)
	}
	completeRecoveryIncidentForTest(t, fixture, incident.ID, "physical-target:baseline")

	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("incomplete resolved target recovery error = %v", err)
	}
}

func TestStartupResolvedTargetRecoveryRequiresExactCompletedPhysicalPair(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident, pending := prepareReadyPersistedTargetRecovery(t, fixture, harness, view, target)
	previous := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
	if err := harness.tracker.Acquire("target", targetRefKey(previous), view.Lease.ID); err != nil {
		t.Fatal(err)
	}
	completeRecoveryIncidentForTest(t, fixture, incident.ID, "physical-target:baseline")

	targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	targetDriver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
		return targetOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
	}
	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, targetDriver, harness.workspace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("resolved target recovery with both generations error = %v", err)
	}
	current := ports.TargetRef{ID: previous.ID, Generation: domain.TargetGeneration(pending.CurrentGeneration)}
	if !hasOwnership(harness.tracker.Snapshot(), "target", targetRefKey(previous)) || !hasOwnership(harness.tracker.Snapshot(), "target", targetRefKey(current)) {
		t.Fatalf("resolved target pair verification mutated ownership: %#v", harness.tracker.Snapshot())
	}
}

func TestStartupResolvedTargetRecoveryAdmitsExactCompletedPhysicalPair(t *testing.T) {
	tests := []struct {
		name           string
		cleanupResidue bool
	}{
		{name: "no_residue"},
		{name: "local_residue", cleanupResidue: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			previous := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
			incident, _ := prepareReadyPersistedTargetRecovery(t, fixture, harness, view, target)
			completeRecoveryIncidentForTest(t, fixture, incident.ID, "physical-target:baseline")
			cleanupResidue := test.cleanupResidue
			driver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
			driver.reconcile = func(expected []ports.TargetPlan) ports.TargetReconciliationReport {
				report := targetOwnershipInventory(fixture.clock.Now(), harness.tracker.Snapshot(), expected)
				for index := range report.Expected {
					if report.Expected[index].Ref == previous && report.Expected[index].Classification == ports.PhysicalResourceMissing {
						report.Expected[index].CleanupRequired = cleanupResidue
					}
				}
				return report
			}
			driver.destroy = func(ref ports.TargetRef) error {
				if ref != previous {
					t.Fatalf("destroyed unexpected resolved recovery target %s", targetRefKey(ref))
				}
				cleanupResidue = false
				return nil
			}
			controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, driver, harness.workspace)
			reconcileStartupRecovery(t, controller)
			wantDestroyed := 0
			if test.cleanupResidue {
				wantDestroyed = 1
			}
			if len(driver.destroyed) != wantDestroyed {
				t.Fatalf("resolved recovery cleanup calls = %v, want %d", driver.destroyed, wantDestroyed)
			}
			if !containsTargetPlanRef(driver.expected, previous) {
				t.Fatalf("resolved target predecessor disappeared from converged cleanup inventory: %#v", driver.expected)
			}
		})
	}
}

func containsAgentPlanRef(plans []ports.AgentWorkspacePlan, expected ports.AgentWorkspaceRef) bool {
	for _, plan := range plans {
		if agentPlanKey(plan) == agentRefKey(expected) {
			return true
		}
	}
	return false
}

func containsTargetPlanRef(plans []ports.TargetPlan, expected ports.TargetRef) bool {
	for _, plan := range plans {
		if targetPlanKey(plan) == targetRefKey(expected) {
			return true
		}
	}
	return false
}

func completeRecoveryIncidentForTest(t *testing.T, fixture *integrationFixture, incidentID, physicalAction string) {
	t.Helper()
	incident, err := fixture.core.GetIncident(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	actions := append([]string(nil), incident.RecoveryActions...)
	if physicalAction != "" {
		actions = appendUniqueString(actions, physicalAction)
	}
	_, err = fixture.core.CompleteIncidentRecovery(context.Background(), application.TransitionIncidentRequest{
		Meta: fixture.meta("premature-complete-" + incidentID), IncidentID: incident.ID, ExpectedRevision: incident.Revision,
		State: domain.IncidentResolved, RecoveryActions: actions,
		VisibilityAcknowledgements: append([]string(nil), incident.VisibilityAcknowledgements...),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func prepareReadyPersistedTargetRecovery(
	t *testing.T,
	fixture *integrationFixture,
	harness controllerHarness,
	view application.ResearchSessionView,
	target application.TargetRecord,
) (application.IncidentRecord, application.TargetRecord) {
	t.Helper()
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentLinuxTargetFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		TargetID: target.ID, TargetGeneration: target.CurrentGeneration, Trigger: "target restart completion crash", LastKnownState: "ready",
	})
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("startup-ready-target-recovery"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceTarget,
		Strategy: string(ports.ResetBaseline), VisibilityAcknowledgement: "sealed evidence visible",
	}
	outcome, err := fixture.core.RecoverIncident(context.Background(), request)
	if err != nil || outcome.Target == nil {
		t.Fatalf("begin target recovery: %#v, %v", outcome, err)
	}
	pending := *outcome.Target
	reset := application.ResetTargetRequest{
		Meta: request.Meta, TargetID: pending.ID, ExpectedRevision: pending.Revision,
		Mode: ports.ResetBaseline, RecoveryIncidentID: incident.ID,
	}
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	physicalKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset")
	plan, err := harness.controller.resolveAndAdmitTargetReset(ctx, reset, pending, view, &outcome.Incident, physicalKey)
	if err == nil {
		pending, err = harness.controller.bindTargetProvisioningPlan(ctx, request.Meta, pending, plan)
	}
	if err == nil {
		pending, _, err = harness.controller.resetTargetPhysical(ctx, reset, pending, harness.target)
	}
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	currentGeneration, err := targetGeneration(pending)
	if err != nil || currentGeneration.State != domain.TargetGenerationReady {
		t.Fatalf("physical recovery target = %#v, %v", pending, err)
	}
	return incident, pending
}

func beginPersistedAgentRecovery(t *testing.T, fixture *integrationFixture, view application.ResearchSessionView) (application.RecoverIncidentRequest, application.RecoveryOutcome) {
	t.Helper()
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentAgentWorkspaceFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		AgentWorkspaceID: view.Agent.ID, AgentGeneration: view.Agent.CurrentGeneration,
		Trigger: "agent restart recovery crash", LastKnownState: "ready",
	})
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("startup-agent-incident-recovery"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceAgent,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence visible",
	}
	outcome, err := fixture.core.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return request, outcome
}

func bindPersistedAgentRecovery(t *testing.T, harness controllerHarness, request application.RecoverIncidentRequest, pending application.RecoveryOutcome) (application.ResearchSessionView, AgentProvisioningPlan) {
	t.Helper()
	if pending.Agent == nil {
		t.Fatal("agent recovery outcome has no agent")
	}
	view, err := harness.controller.Core.GetResearchSession(context.Background(), pending.Agent.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	defer cancel()
	resolved, err := harness.resolver.ResolveAgentRecovery(ctx, request, view)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := bindAgentProvisioning(application.AcquireRequest{Meta: request.Meta}, resolved, view)
	if err != nil {
		t.Fatal(err)
	}
	view, err = harness.controller.bindAgentProvisioningPlan(ctx, request.Meta, view, plan)
	if err != nil {
		t.Fatal(err)
	}
	return view, plan
}

func agentOwnershipInventory(now time.Time, ownership []testkit.Ownership, expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
	report := ports.AgentWorkspaceReconciliationReport{ObservedAt: now}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		ref := ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation}
		observation := ports.AgentWorkspaceReconciliation{Ref: ref}
		if hasOwnership(ownership, "agent_workspace", agentRefKey(ref)) {
			observation.ContainerID = "runtime-" + agentRefKey(ref)
			observation.Classification = ports.PhysicalResourceAdopted
			observation.PlanMatched = true
		} else {
			observation.Classification = ports.PhysicalResourceMissing
			observation.Diagnostic = "authoritative absence"
		}
		report.Expected = append(report.Expected, observation)
	}
	return report
}

func targetOwnershipInventory(now time.Time, ownership []testkit.Ownership, expected []ports.TargetPlan) ports.TargetReconciliationReport {
	report := ports.TargetReconciliationReport{ObservedAt: now}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}
		observation := ports.TargetReconciliation{Ref: ref}
		if hasOwnership(ownership, "target", targetRefKey(ref)) {
			observation.RuntimeID = "runtime-" + targetRefKey(ref)
			observation.Classification = ports.PhysicalResourceAdopted
			observation.PlanMatched = true
		} else {
			observation.Classification = ports.PhysicalResourceMissing
			observation.Diagnostic = "authoritative absence"
		}
		report.Expected = append(report.Expected, observation)
	}
	return report
}

func hasOwnership(ownership []testkit.Ownership, kind, id string) bool {
	for _, item := range ownership {
		if item.Kind == kind && item.ID == id {
			return true
		}
	}
	return false
}

func mustAgentWorkspaceID(t *testing.T, value string) domain.AgentWorkspaceID {
	t.Helper()
	id, err := domain.ParseAgentWorkspaceID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
