package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestStartupRejectsUnboundAgentBeyondProvisioning(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view, err := fixture.core.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("startup-invalid-unbound-agent"), OwnerSubject: integrationOwner,
		InputViewID: harness.inputViewID, PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.TransitionAgentGeneration(context.Background(), application.TransitionAgentRequest{
		Meta: fixture.meta("startup-invalid-unbound-agent-booting"), AgentWorkspaceID: view.Agent.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision, State: domain.AgentGenerationBooting,
	}); err != nil {
		t.Fatal(err)
	}

	calls := &startupRecoveryCallLog{}
	agent := &startupRecoveryAgentDriver{reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, calls: calls}
	workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: calls}
	controller := newStartupRecoveryController(t, fixture, harness, agent, &reconciliationTargetDriver{FakeTargetDriver: harness.target}, workspace)
	requireStartupIntegrityFailure(t, controller)
	if len(agent.provisioned) != 0 || len(workspace.prepared) != 0 {
		t.Fatalf("invalid unbound agent reached physical mutation: agent=%d workspace=%d", len(agent.provisioned), len(workspace.prepared))
	}
}

func TestStartupRejectsUnboundTargetResetBeyondProvisioning(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta("startup-invalid-target-resettable"), TargetID: target.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable,
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := fixture.core.ResetTarget(context.Background(), application.ResetTargetRequest{
		Meta: fixture.meta("startup-invalid-unbound-target-reset"), TargetID: target.ID,
		ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := targetGeneration(pending)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{
		Meta: fixture.meta("startup-invalid-unbound-target-instrumenting"), TargetID: pending.ID,
		Generation: current.Generation, ExpectedRevision: current.Revision, State: domain.TargetGenerationInstrumenting,
	}); err != nil {
		t.Fatal(err)
	}

	targetDriver := &startupRecoveryTargetDriver{reconciliationTargetDriver: &reconciliationTargetDriver{FakeTargetDriver: harness.target}, calls: &startupRecoveryCallLog{}}
	controller := newStartupRecoveryController(t, fixture, harness, &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}, targetDriver, harness.workspace)
	requireStartupIntegrityFailure(t, controller)
	if len(targetDriver.created) != 0 {
		t.Fatalf("invalid unbound target reset reached Create: %d", len(targetDriver.created))
	}
}

func TestStartupRejectsUnboundTargetRunBeyondRequested(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := fixture.core.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("startup-invalid-unbound-run"), TargetID: target.ID,
		MaterializationDigest: harness.materialDigest, SpecimenOccurrenceRefs: append([]string(nil), harness.specimenRefs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.TransitionTargetRun(context.Background(), application.TransitionTargetRunRequest{
		Meta: fixture.meta("startup-invalid-unbound-run-preparing"), TargetID: target.ID,
		RunID: run.ID, ExpectedRevision: run.Revision, State: domain.TargetRunPreparing,
	}); err != nil {
		t.Fatal(err)
	}

	controller := newStartupRecoveryController(
		t, fixture, harness,
		&reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		&reconciliationTargetDriver{FakeTargetDriver: harness.target}, harness.workspace,
	)
	requireStartupIntegrityFailure(t, controller)
}

func requireStartupIntegrityFailure(t *testing.T, controller *Controller) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := controller.ReconcilePhysicalResources(ctx)
	if err == nil || !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("startup error = %v, want integrity violation", err)
	}
}
