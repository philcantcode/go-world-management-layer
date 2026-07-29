package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestReconcilePhysicalResourcesGatesDueUnboundInitialAgentBeforeProvisioning(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	calls := &startupRecoveryCallLog{}
	workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: calls}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	target := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
	controller := newStartupRecoveryController(t, fixture, harness, agent, target, workspace)

	fixture.clock.Set(fixture.view.Lease.ExpiresAt)
	report := reconcileStartupRecovery(t, controller)
	if len(workspace.prepared) != 0 || len(agent.provisioned) != 0 {
		t.Fatalf("due unbound agent was physically provisioned: workspace prepares=%d agent provisions=%d calls=%v", len(workspace.prepared), len(agent.provisioned), calls.calls)
	}
	if len(report.RecoveredAgentProvisionings) != 0 || len(report.PendingAgentProvisionings) != 0 {
		t.Fatalf("due unbound agent appeared as startup provisioning work: %#v", report)
	}
	requireDurableStartupExpiryGate(t, fixture, fixture.view.Session.ID)
	completeStartupExpiries(t, fixture, controller, fixture.view.Session.ID, 1)
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilePhysicalResourcesGatesDueUnboundInitialTargetBeforeCreation(t *testing.T) {
	fixture, harness, view, _ := newStartupRecoveryReadyTargetFixture(t, false)
	boundTarget := harness.createTarget(t, fixture, view)
	target, err := fixture.core.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: fixture.meta("startup-due-unbound-target"), LeaseID: view.Lease.ID,
		Template: "linux-visible", Kind: domain.TargetLinuxContainer,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireStartupRecoveryTargetState(t, fixture, target.ID, domain.TargetGenerationProvisioning)

	calls := &startupRecoveryCallLog{}
	workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: calls}
	agent := &startupRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		calls:                     calls,
	}
	targetDriver := &startupRecoveryTargetDriver{
		reconciliationTargetDriver: &reconciliationTargetDriver{FakeTargetDriver: harness.target},
		calls:                      calls,
	}
	controller := newStartupRecoveryController(t, fixture, harness, agent, targetDriver, workspace)

	fixture.clock.Set(view.Lease.ExpiresAt)
	report := reconcileStartupRecovery(t, controller)
	if len(workspace.prepared) != 0 || len(agent.provisioned) != 0 || len(targetDriver.created) != 0 {
		t.Fatalf("due resources were physically provisioned: workspace prepares=%d agent provisions=%d target creates=%d calls=%v", len(workspace.prepared), len(agent.provisioned), len(targetDriver.created), calls.calls)
	}
	if len(report.RecoveredTargetProvisionings) != 0 || len(report.PendingTargetProvisionings) != 0 {
		t.Fatalf("due unbound target appeared as startup provisioning work: %#v", report)
	}
	requireDurableStartupExpiryGate(t, fixture, view.Session.ID)
	// newIntegrationFixture also owns one baseline lease with the same fake
	// clock deadline, so the reaper completes both durable expiry gates.
	completeStartupExpiries(t, fixture, controller, view.Session.ID, 2)
	if !containsDestroyedTarget(targetDriver.destroyed, boundTarget.ID) {
		t.Fatalf("bound target %s was not retired by expiry cleanup: %v", boundTarget.ID, targetDriver.destroyed)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func containsDestroyedTarget(refs []ports.TargetRef, targetID string) bool {
	for _, ref := range refs {
		if ref.ID.String() == targetID {
			return true
		}
	}
	return false
}

func requireDurableStartupExpiryGate(t *testing.T, fixture *integrationFixture, sessionID string) {
	t.Helper()
	view, err := fixture.core.GetResearchSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Session.State != domain.ResearchSessionReleasing || view.Lease.State != domain.LeaseActive ||
		view.Lease.Termination.Kind != application.LeaseTerminationExpiry ||
		view.Lease.Termination.State != application.LeaseTerminationExpiring {
		t.Fatalf("startup expiry gate = session %s lease %s termination %#v", view.Session.State, view.Lease.State, view.Lease.Termination)
	}
	work, err := fixture.core.ListLeaseTerminationWork(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range work {
		if item.LeaseID == view.Lease.ID {
			if item.NeedsBegin || item.Kind != application.LeaseTerminationExpiry || item.State != application.LeaseTerminationExpiring {
				t.Fatalf("persisted startup expiry work = %#v", item)
			}
			return
		}
	}
	t.Fatalf("lease %s is absent from durable termination work", view.Lease.ID)
}

func completeStartupExpiries(t *testing.T, fixture *integrationFixture, controller *Controller, sessionID string, wantExamined int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := controller.ReconcileLeaseTerminations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Examined != wantExamined || report.Begun != 0 || report.Completed != wantExamined {
		t.Fatalf("startup expiry completion report = %#v, want examined/completed %d and no new begins", report, wantExamined)
	}
	assertTerminationState(t, fixture, sessionID, domain.LeaseExpired, application.LeaseTerminationExpired)
}
