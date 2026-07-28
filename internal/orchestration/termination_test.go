package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestControllerReleaseDrainsActiveLeaseWorkAndReplaysExactly(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("termination-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution := createRunningTerminationExec(t, fixture, view.Lease.ID)
	operation := createRunningTerminationTargetOperation(t, fixture, target.ID, run.ID)
	capture, err := harness.capabilities.StartCapture(context.Background(), &worldv1.StartCaptureRequest{
		Mutation: fixture.wireMeta("termination-capture"), LeaseId: view.Lease.ID,
		CaptureSpec: &worldv1.CaptureSpec{
			Profile: "termination", SignalFamilies: []string{"process"},
			Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("termination-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "integration complete",
	}
	outcome, err := harness.controller.ReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := harness.controller.ReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatalf("exact release replay failed: %v", err)
	}
	if replayed != outcome {
		t.Fatalf("release replay = %#v, want %#v", replayed, outcome)
	}

	assertTerminationState(t, fixture, view.Session.ID, domain.LeaseReleased, application.LeaseTerminationReleased)
	assertTerminationWorkFinalized(t, fixture, target.ID, run.ID, execution.ID, operation.ID)
	assertCaptureCompleted(t, harness, capture.CaptureId)
	if harness.capture.stopCalls != 1 {
		t.Fatalf("capture stop calls = %d, want exactly 1", harness.capture.stopCalls)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseTerminationResumesExistingCaptureStopReservation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	capture, err := harness.capabilities.StartCapture(context.Background(), &worldv1.StartCaptureRequest{
		Mutation: fixture.wireMeta("reserved-capture-start"), LeaseId: view.Lease.ID,
		CaptureSpec: &worldv1.CaptureSpec{
			Profile: "termination", SignalFamilies: []string{"process"},
			Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	physicalFailure := errors.New("capture stop response was lost")
	harness.capture.stopError = physicalFailure
	stop := &worldv1.StopCaptureRequest{
		Mutation: fixture.wireMeta("reserved-capture-stop"), LeaseId: view.Lease.ID,
		CaptureId: capture.CaptureId, ExpectedRevision: capture.Revision,
	}
	if _, err := harness.capabilities.StopCapture(context.Background(), stop); !errors.Is(err, physicalFailure) {
		t.Fatalf("first stop error = %v, want injected failure", err)
	}
	request := application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("reserved-capture-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "resume capture cleanup",
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), request); err != nil {
		t.Fatalf("termination did not resume the reserved capture stop: %v", err)
	}
	assertCaptureCompleted(t, harness, capture.CaptureId)
	if harness.capture.stopCalls != 2 {
		t.Fatalf("capture stop calls = %d, want failed attempt plus one exact retry", harness.capture.stopCalls)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExpiryReaperResumesDurableIntentAndFinalizesLostRun(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)

	// Persist a nonterminal run without entering the target driver. This models
	// a restart where durable control state survived but the runtime's
	// in-memory run record did not.
	run, err := fixture.core.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("lost-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.clock.Set(view.Lease.ExpiresAt)
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), application.BeginLeaseExpiryRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Kind != application.LeaseTerminationExpiry {
		t.Fatalf("termination kind = %s, want expiry", prepared.Kind)
	}

	report, err := harness.controller.ReconcileLeaseTerminations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// newIntegrationFixture owns one baseline logical lease in addition to the
	// physically provisioned lease. Both share the deterministic clock and are
	// therefore due; only the provisioned lease already has a persisted intent.
	if report.Examined != 2 || report.Begun != 1 || report.Completed != 2 {
		t.Fatalf("resume report = %#v", report)
	}
	assertTerminationState(t, fixture, view.Session.ID, domain.LeaseExpired, application.LeaseTerminationExpired)
	current, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	finalRun, err := targetRun(current, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !finalRun.State.Terminal() || finalRun.BundleID == "" || len(finalRun.IncidentIDs) == 0 {
		t.Fatalf("lost run was not explicitly finalized with evidence and incident: %#v", finalRun)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
	empty, err := harness.controller.ReconcileLeaseTerminations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if empty != (LeaseTerminationScanReport{}) {
		t.Fatalf("terminal reaper replay = %#v, want no work", empty)
	}
}

func createRunningTerminationExec(t *testing.T, fixture *integrationFixture, leaseID string) application.ExecRecord {
	t.Helper()
	execution, err := fixture.core.CreateExec(context.Background(), application.CreateExecRequest{
		Meta: fixture.meta("termination-exec"), LeaseID: leaseID,
		Kind: domain.ExecTool, Executable: "bin/tool",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.ExecState{domain.ExecStarting, domain.ExecRunning} {
		execution, err = fixture.core.TransitionExec(context.Background(), application.TransitionExecRequest{
			Meta: fixture.meta("termination-exec-transition"), ExecID: execution.ID,
			ExpectedRevision: execution.Revision, State: state,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return execution
}

func createRunningTerminationTargetOperation(t *testing.T, fixture *integrationFixture, targetID, runID string) application.TargetOperationRecord {
	t.Helper()
	operation, err := fixture.core.CreateTargetOperation(context.Background(), application.CreateTargetOperationRequest{
		Meta: fixture.meta("termination-operation"), TargetID: targetID, RunID: runID,
		Kind: domain.TargetOperationExec, CommandDisplay: "specimen operation",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = fixture.core.TransitionTargetOperation(context.Background(), application.TransitionTargetOperationRequest{
		Meta: fixture.meta("termination-operation-running"), TargetID: targetID,
		OperationID: operation.ID, ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func assertTerminationState(t *testing.T, fixture *integrationFixture, sessionID string, leaseState domain.LeaseState, terminationState application.LeaseTerminationState) {
	t.Helper()
	view, err := fixture.core.GetResearchSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Lease.State != leaseState || view.Lease.Termination.State != terminationState {
		t.Fatalf("lease termination = %#v, want %s/%s", view.Lease, leaseState, terminationState)
	}
}

func assertTerminationWorkFinalized(t *testing.T, fixture *integrationFixture, targetID, runID, execID, operationID string) {
	t.Helper()
	target, err := fixture.core.GetTarget(context.Background(), targetID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := targetRun(target, runID)
	if err != nil {
		t.Fatal(err)
	}
	if !run.State.Terminal() || run.BundleID == "" {
		t.Fatalf("run was not evidence-finalized: %#v", run)
	}
	execution, err := fixture.core.GetExec(context.Background(), execID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.State != domain.ExecCancelled || !execution.CleanupConfirmed {
		t.Fatalf("exec was not cleanup-confirmed cancelled: %#v", execution)
	}
	operation, err := targetOperation(target, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != domain.TargetOperationCancelled {
		t.Fatalf("target operation state = %s, want cancelled", operation.State)
	}
}

func assertCaptureCompleted(t *testing.T, harness controllerHarness, captureID string) {
	t.Helper()
	harness.capabilities.mu.RLock()
	record, found := harness.capabilities.captureState[captureID]
	harness.capabilities.mu.RUnlock()
	if !found || record.Capture.State != captureStateCompleted {
		t.Fatalf("capture was not completed: %#v", record.Capture)
	}
}
