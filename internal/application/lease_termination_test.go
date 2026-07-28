package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

func TestLeaseExpiryDeadlineBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		offset  time.Duration
		wantDue bool
	}{
		{name: "before", offset: -time.Nanosecond},
		{name: "at", wantDue: true},
		{name: "after", offset: time.Nanosecond, wantDue: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCoreFixture(t)
			view, _ := fixture.acquire(t)
			fixture.now = view.Lease.ExpiresAt.Add(test.offset)

			work, err := fixture.core.ListLeaseTerminationWork(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := len(work) == 1 && work[0].NeedsBegin; got != test.wantDue {
				t.Fatalf("due work = %#v, want due %v", work, test.wantDue)
			}
			preparation, err := fixture.core.BeginDueLeaseExpiry(context.Background(), BeginLeaseExpiryRequest{
				LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
			})
			if !test.wantDue {
				if err == nil {
					t.Fatal("expiry began before the exact lease deadline")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if preparation.Kind != LeaseTerminationExpiry || preparation.View.Lease.Termination.State != LeaseTerminationExpiring {
				t.Fatalf("expiry preparation = %#v", preparation)
			}
		})
	}
}

func TestLeaseExpirySurvivesRestartAndCompletesIdempotently(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.now = view.Lease.ExpiresAt
	begin := BeginLeaseExpiryRequest{LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision}
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), begin)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.View.Lease.State != domain.LeaseActive || prepared.View.Lease.Termination.State != LeaseTerminationExpiring ||
		prepared.View.Session.State != domain.ResearchSessionReleasing {
		t.Fatalf("active-to-expiring gate = %#v", prepared.View)
	}

	// Exact retry replays the durable begin. Changing the old scan revision under
	// the deterministic lease key must conflict rather than create a second intent.
	replayed, err := fixture.core.BeginDueLeaseExpiry(context.Background(), begin)
	if err != nil || replayed.TerminatingLeaseRevision != prepared.TerminatingLeaseRevision {
		t.Fatalf("exact expiry begin replay = %#v, %v", replayed, err)
	}
	changed := begin
	changed.ExpectedRevision++
	if _, err := fixture.core.BeginDueLeaseExpiry(context.Background(), changed); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changed expiry begin error = %v, want idempotency conflict", err)
	}

	reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	work, err := reopened.ListLeaseTerminationWork(context.Background())
	if err != nil || len(work) != 1 || work[0].NeedsBegin || work[0].State != LeaseTerminationExpiring {
		t.Fatalf("restart work = %#v, %v", work, err)
	}
	resumed, err := reopened.ResumeLeaseTermination(context.Background(), view.Lease.ID)
	if err != nil || resumed.Kind != LeaseTerminationExpiry || resumed.TerminatingLeaseRevision != prepared.TerminatingLeaseRevision {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}

	complete := CompleteLeaseTerminationRequest{LeaseID: view.Lease.ID, ExpectedRevision: resumed.TerminatingLeaseRevision}
	outcome, err := reopened.CompleteLeaseTermination(context.Background(), complete)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != LeaseTerminationExpiry || outcome.LeaseState != domain.LeaseExpired {
		t.Fatalf("expiry outcome = %#v", outcome)
	}
	after, err := reopened.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Lease.State != domain.LeaseExpired || after.Lease.Termination.State != LeaseTerminationExpired ||
		after.Session.State != domain.ResearchSessionReleased {
		t.Fatalf("expiring-to-expired projection = %#v", after)
	}
	replayedOutcome, err := reopened.CompleteLeaseTermination(context.Background(), complete)
	if err != nil || replayedOutcome != outcome {
		t.Fatalf("exact completion replay = %#v, %v", replayedOutcome, err)
	}
	changedComplete := complete
	changedComplete.ExpectedRevision++
	if _, err := reopened.CompleteLeaseTermination(context.Background(), changedComplete); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changed completion error = %v, want idempotency conflict", err)
	}
	work, err = reopened.ListLeaseTerminationWork(context.Background())
	if err != nil || len(work) != 0 {
		t.Fatalf("terminal expiry remained in work queue: %#v, %v", work, err)
	}
}

func TestManualReleaseUsesStableDeadlineIndependentSignatures(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	request := ReleaseResearchSessionRequest{
		Meta: fixture.meta(t, "stable-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "research complete",
	}
	prepared, err := fixture.core.BeginReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.View.Lease.State != domain.LeaseReleasing || prepared.View.Lease.Termination.State != LeaseTerminationReleasing {
		t.Fatalf("release gate = %#v", prepared.View.Lease)
	}

	deadlineRetry := request
	deadlineRetry.Meta.Deadline = deadlineRetry.Meta.Deadline.Add(30 * time.Second)
	replayed, err := fixture.core.BeginReleaseResearchSession(context.Background(), deadlineRetry)
	if err != nil || replayed.ReleasingLeaseRevision != prepared.ReleasingLeaseRevision {
		t.Fatalf("deadline-independent begin replay = %#v, %v", replayed, err)
	}
	changedBegin := request
	changedBegin.Reason = "different reason"
	if _, err := fixture.core.BeginReleaseResearchSession(context.Background(), changedBegin); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changed begin payload error = %v, want idempotency conflict", err)
	}
	newKey := request
	newKey.Meta = fixture.meta(t, "new-release-key")
	newKey.ExpectedRevision = prepared.ReleasingLeaseRevision
	if _, err := fixture.core.BeginReleaseResearchSession(context.Background(), newKey); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("new begin key error = %v, want idempotency conflict", err)
	}

	renew := RenewLeaseRequest{
		Meta: fixture.meta(t, "renew-after-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: prepared.ReleasingLeaseRevision, TTL: 2 * time.Hour,
	}
	if _, err := fixture.core.RenewLease(context.Background(), renew); err == nil {
		t.Fatal("renewal succeeded after termination began")
	}

	complete := request
	complete.Meta = fixture.meta(t, "complete-stable-release")
	complete.ExpectedRevision = prepared.ReleasingLeaseRevision
	outcome, err := fixture.core.CompleteReleaseResearchSession(context.Background(), complete)
	if err != nil {
		t.Fatal(err)
	}
	deadlineCompleteRetry := complete
	deadlineCompleteRetry.Meta.Deadline = deadlineCompleteRetry.Meta.Deadline.Add(45 * time.Second)
	replayedOutcome, err := fixture.core.CompleteReleaseResearchSession(context.Background(), deadlineCompleteRetry)
	if err != nil || replayedOutcome != outcome {
		t.Fatalf("deadline-independent completion replay = %#v, %v", replayedOutcome, err)
	}
	changedPayload := complete
	changedPayload.Reason = "different reason"
	if _, err := fixture.core.CompleteReleaseResearchSession(context.Background(), changedPayload); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changed completion payload error = %v, want idempotency conflict", err)
	}
	newCompleteKey := complete
	newCompleteKey.Meta = fixture.meta(t, "new-complete-key")
	if _, err := fixture.core.CompleteReleaseResearchSession(context.Background(), newCompleteKey); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("new completion key error = %v, want idempotency conflict", err)
	}
	after, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Lease.State != domain.LeaseReleased || after.Lease.Termination.State != LeaseTerminationReleased {
		t.Fatalf("release-to-released projection = %#v", after.Lease)
	}
}

func TestManualReleaseCanResumeUnderTrustedDeadlineAfterRestart(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	request := ReleaseResearchSessionRequest{
		Meta: fixture.meta(t, "restart-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "research complete",
	}
	prepared, err := fixture.core.BeginReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = request.Meta.Deadline.Add(time.Second)
	reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.BeginReleaseResearchSession(context.Background(), request); err == nil {
		t.Fatal("expired caller deadline replayed through the public begin API")
	}
	work, err := reopened.ListLeaseTerminationWork(context.Background())
	if err != nil || len(work) != 1 || work[0].Kind != LeaseTerminationRelease || work[0].State != LeaseTerminationReleasing || work[0].NeedsBegin {
		t.Fatalf("restart release work = %#v, %v", work, err)
	}
	resumed, err := reopened.ResumeLeaseTermination(context.Background(), view.Lease.ID)
	if err != nil || resumed.Kind != LeaseTerminationRelease {
		t.Fatalf("trusted release resume = %#v, %v", resumed, err)
	}
	outcome, err := reopened.CompleteLeaseTermination(context.Background(), CompleteLeaseTerminationRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: prepared.ReleasingLeaseRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != LeaseTerminationRelease || outcome.LeaseState != domain.LeaseReleased {
		t.Fatalf("trusted release outcome = %#v", outcome)
	}
}

func TestReleaseBeginTransactionFaultsAreRollbackSafeAndRestartRecoverable(t *testing.T) {
	t.Run("before commit rolls back", func(t *testing.T) {
		faults := &oneShotStoreFault{}
		fixture := newCoreFixtureWithFaults(t, faults)
		view, _ := fixture.acquire(t)
		request := ReleaseResearchSessionRequest{
			Meta: fixture.meta(t, "release-before-commit"), LeaseID: view.Lease.ID,
			ExpectedRevision: view.Lease.Revision, Reason: "done",
		}
		faults.arm("store.before_commit")
		if _, err := fixture.core.BeginReleaseResearchSession(context.Background(), request); err == nil {
			t.Fatal("injected pre-commit fault did not fail begin")
		}
		after, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Lease.State != domain.LeaseActive || !after.Lease.Termination.Empty() || after.Session.State != domain.ResearchSessionLeased {
			t.Fatalf("rolled-back begin leaked projection changes: %#v", after)
		}
		if _, err := fixture.core.BeginReleaseResearchSession(context.Background(), request); err != nil {
			t.Fatalf("exact retry after rollback: %v", err)
		}
	})

	t.Run("after commit resumes", func(t *testing.T) {
		faults := &oneShotStoreFault{}
		fixture := newCoreFixtureWithFaults(t, faults)
		view, _ := fixture.acquire(t)
		request := ReleaseResearchSessionRequest{
			Meta: fixture.meta(t, "release-after-commit"), LeaseID: view.Lease.ID,
			ExpectedRevision: view.Lease.Revision, Reason: "done",
		}
		faults.arm("store.after_commit")
		if _, err := fixture.core.BeginReleaseResearchSession(context.Background(), request); err == nil {
			t.Fatal("injected post-commit fault did not obscure response")
		}
		reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
		if err != nil {
			t.Fatal(err)
		}
		work, err := reopened.ListLeaseTerminationWork(context.Background())
		if err != nil || len(work) != 1 || work[0].Kind != LeaseTerminationRelease || work[0].NeedsBegin {
			t.Fatalf("post-commit restart work = %#v, %v", work, err)
		}
		if _, err := reopened.BeginReleaseResearchSession(context.Background(), request); err != nil {
			t.Fatalf("exact replay after post-commit fault: %v", err)
		}
	})
}

func TestExpiryGatesBeforeDrainAndRequiresTerminalWorkToComplete(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.readyAgent(t, view.Agent)
	execution, err := fixture.core.CreateExec(context.Background(), CreateExecRequest{
		Meta: fixture.meta(t, "expiry-active-exec"), LeaseID: view.Lease.ID,
		Kind: domain.ExecTool, Executable: "bin/tool",
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = view.Lease.ExpiresAt
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), BeginLeaseExpiryRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
	})
	if err != nil {
		t.Fatalf("expiry did not close admission before drain: %v", err)
	}
	complete := CompleteLeaseTerminationRequest{LeaseID: view.Lease.ID, ExpectedRevision: prepared.TerminatingLeaseRevision}
	if _, err := fixture.core.CompleteLeaseTermination(context.Background(), complete); err == nil {
		t.Fatal("expiry completed with a nonterminal exec")
	}
	finalizeMeta := fixture.meta(t, "expiry-finalize-exec")
	finalizeMeta.Deadline = fixture.now.Add(time.Minute)
	if _, err := fixture.core.FinalizeExec(context.Background(), FinalizeExecRequest{
		Meta: finalizeMeta, ExecID: execution.ID, ExpectedRevision: execution.Revision,
		State: domain.ExecCancelled, CleanupConfirmed: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.CompleteLeaseTermination(context.Background(), complete); err != nil {
		t.Fatalf("expiry did not complete after drain: %v", err)
	}
}

func TestExpiryAllowsTargetOperationAndRunDrainButNoNewWork(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.readyAgent(t, view.Agent)
	target := fixture.readyTarget(t, view)
	run := fixture.runningRun(t, target)
	operation, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
		Meta: fixture.meta(t, "expiry-target-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationShell, CommandDisplay: "write output",
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = view.Lease.ExpiresAt
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), BeginLeaseExpiryRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutationMeta := func(key string) MutationMeta {
		meta := fixture.meta(t, key)
		meta.Deadline = fixture.now.Add(time.Minute)
		return meta
	}
	if _, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
		Meta: mutationMeta("expiry-new-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationShell, CommandDisplay: "late write",
	}); err == nil {
		t.Fatal("expiring lease admitted a new target operation")
	}
	operation, err = fixture.core.TransitionTargetOperation(context.Background(), TransitionTargetOperationRequest{
		Meta: mutationMeta("expiry-cancel-operation"), TargetID: target.ID, OperationID: operation.ID,
		ExpectedRevision: operation.Revision, State: domain.TargetOperationCancelled,
	})
	if err != nil {
		t.Fatalf("cancel target operation while expiring: %v", err)
	}
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	run, err = fixture.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{
		Meta: mutationMeta("expiry-finalize-run"), TargetID: target.ID, RunID: run.ID,
		ExpectedRevision: run.Revision, Failed: true, BundleID: bundleID.String(),
	})
	if err != nil {
		t.Fatalf("finalize target run while expiring: %v", err)
	}
	if !operation.State.Terminal() || !run.State.Terminal() {
		t.Fatalf("drain did not reach terminal states: operation=%s run=%s", operation.State, run.State)
	}
	if _, err := fixture.core.CompleteLeaseTermination(context.Background(), CompleteLeaseTerminationRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: prepared.TerminatingLeaseRevision,
	}); err != nil {
		t.Fatalf("complete expiry after target drain: %v", err)
	}
}

func TestExpiryIntentBlocksRenewalBeforeCompletion(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.now = view.Lease.ExpiresAt
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), BeginLeaseExpiryRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = fixture.now.Add(time.Nanosecond)
	renewMeta := fixture.meta(t, "renew-expiring")
	renewMeta.Deadline = fixture.now.Add(time.Minute)
	if _, err := fixture.core.RenewLease(context.Background(), RenewLeaseRequest{
		Meta: renewMeta, LeaseID: view.Lease.ID,
		ExpectedRevision: prepared.TerminatingLeaseRevision, TTL: 2 * time.Hour,
	}); err == nil {
		t.Fatal("renewal succeeded while the lease was durably expiring")
	}
}
