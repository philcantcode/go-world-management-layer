package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type coreFixture struct {
	core  *Core
	store *store.Store
	now   time.Time
	seq   int
}

func newCoreFixture(t *testing.T) *coreFixture {
	return newCoreFixtureWithFaults(t, nil)
}

func newCoreFixtureWithFaults(t *testing.T, faults store.FaultInjector) *coreFixture {
	t.Helper()
	fixture := &coreFixture{now: time.Now().UTC().Truncate(time.Millisecond)}
	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "world.db"), Faults: faults, Now: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	entropy := make([]byte, 4096)
	for index := range entropy {
		entropy[index] = byte(index*31 + 7)
	}
	ids, err := domain.NewIDGenerator(func() time.Time { return fixture.now }, bytes.NewReader(entropy))
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(context.Background(), CoreOptions{Store: controlStore, IDs: ids, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	fixture.core, fixture.store = core, controlStore
	return fixture
}

func TestAuthorizationRejectsExpiredLeaseMutationsButAllowsHistoricalReads(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	mutation := AuthorizationRequest{
		Subject: "test-owner", PolicyReference: view.Session.PolicyDigest, LeaseID: view.Lease.ID,
	}
	if err := f.core.Authorize(context.Background(), mutation); err != nil {
		t.Fatalf("authorize active lease: %v", err)
	}
	f.now = view.Lease.ExpiresAt
	if err := f.core.Authorize(context.Background(), mutation); !errors.Is(err, ErrScope) {
		t.Fatalf("expired mutation authorization error = %v, want ErrScope", err)
	}
	if err := f.core.Authorize(context.Background(), AuthorizationRequest{Subject: "test-owner", SessionID: view.Session.ID}); err != nil {
		t.Fatalf("historical read authorization after expiry: %v", err)
	}
}

func TestListResearchSessionsReturnsStableCompleteViews(t *testing.T) {
	f := newCoreFixture(t)
	first, _ := f.acquire(t)
	second, _ := f.acquire(t)
	f.readyTarget(t, first)

	views, err := f.core.ListResearchSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 {
		t.Fatalf("session count = %d, want 2", len(views))
	}
	if views[0].Session.ID > views[1].Session.ID {
		t.Fatalf("sessions are not sorted: %q then %q", views[0].Session.ID, views[1].Session.ID)
	}
	byID := map[string]ResearchSessionView{views[0].Session.ID: views[0], views[1].Session.ID: views[1]}
	if len(byID[first.Session.ID].Targets) != 1 || byID[first.Session.ID].Lease.ID != first.Lease.ID {
		t.Fatalf("first session view is incomplete: %#v", byID[first.Session.ID])
	}
	if len(byID[second.Session.ID].Targets) != 0 || byID[second.Session.ID].Agent.ID != second.Agent.ID {
		t.Fatalf("second session view is incomplete: %#v", byID[second.Session.ID])
	}
}

func TestCreateTargetFailsClosedOnInvalidPersistedIdentity(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	f.core.mu.Lock()
	lease := f.core.leases[view.Lease.ID]
	lease.SessionID = "corrupt-session-id"
	f.core.leases[view.Lease.ID] = lease
	f.core.mu.Unlock()

	_, err := f.core.CreateTarget(context.Background(), CreateTargetRequest{
		Meta: f.meta(t, "invalid-stored-id"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest,
	})
	if !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("CreateTarget() error = %v, want integrity violation", err)
	}
}

func (f *coreFixture) meta(t *testing.T, prefix string) MutationMeta {
	t.Helper()
	f.seq++
	correlation, err := domain.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	return MutationMeta{IdempotencyKey: prefix + "-" + time.Now().Format("150405.000000000") + "-" + string(rune(f.seq+64)), CorrelationID: correlation.String(), AuthorizedPolicyReference: "policy:test", Deadline: time.Now().Add(time.Minute)}
}

func (f *coreFixture) acquire(t *testing.T) (ResearchSessionView, AcquireRequest) {
	t.Helper()
	request := AcquireRequest{Meta: f.meta(t, "acquire"), OwnerSubject: "test-owner", InputViewID: domain.NewInputViewID([]byte("manifest")).String(), PolicyDigest: domain.NewDigest([]byte("policy")).String(), CapabilityDigest: domain.NewDigest([]byte("capability")).String(), TTL: time.Hour}
	view, err := f.core.AcquireResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return view, request
}

func (f *coreFixture) readyAgent(t *testing.T, agent AgentWorkspaceRecord) AgentWorkspaceRecord {
	t.Helper()
	for _, state := range []domain.AgentGenerationState{domain.AgentGenerationBooting, domain.AgentGenerationReady} {
		generation := agent.Generations[len(agent.Generations)-1]
		var err error
		agent, err = f.core.TransitionAgentGeneration(context.Background(), TransitionAgentRequest{Meta: f.meta(t, "agent"), AgentWorkspaceID: agent.ID, Generation: agent.CurrentGeneration, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	return agent
}

func (f *coreFixture) readyTarget(t *testing.T, view ResearchSessionView) TargetRecord {
	t.Helper()
	target, err := f.core.CreateTarget(context.Background(), CreateTargetRequest{Meta: f.meta(t, "target"), LeaseID: view.Lease.ID, Template: "linux-visible", Kind: domain.TargetLinuxContainer, PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.TargetGenerationState{domain.TargetGenerationInstrumenting, domain.TargetGenerationReady} {
		generation := target.Generations[len(target.Generations)-1]
		target, err = f.core.TransitionTargetGeneration(context.Background(), TransitionTargetGenerationRequest{Meta: f.meta(t, "target-generation"), TargetID: target.ID, Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	return target
}

func (f *coreFixture) runningRun(t *testing.T, target TargetRecord) TargetRunRecord {
	t.Helper()
	run, err := f.core.StartTargetRun(context.Background(), StartTargetRunRequest{Meta: f.meta(t, "run"), TargetID: target.ID, MaterializationDigest: domain.NewDigest([]byte("specimen")).String()})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.TargetRunState{domain.TargetRunPreparing, domain.TargetRunObserving, domain.TargetRunRunning} {
		run, err = f.core.TransitionTargetRun(context.Background(), TransitionTargetRunRequest{Meta: f.meta(t, "run-transition"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	return run
}

func TestStartTargetRunRequiresFreshGenerationAndPreservesIdempotentReplay(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	request := StartTargetRunRequest{
		Meta:                  f.meta(t, "run-once-per-generation"),
		TargetID:              target.ID,
		MaterializationDigest: domain.NewDigest([]byte("specimen")).String(),
	}
	first, err := f.core.StartTargetRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := f.core.StartTargetRun(context.Background(), request)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replayed.ID != first.ID {
		t.Fatalf("idempotent replay returned run %q, want %q", replayed.ID, first.ID)
	}
	second := request
	second.Meta = f.meta(t, "second-run-same-generation")
	if _, err := f.core.StartTargetRun(context.Background(), second); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("second run in generation error = %v, want failed precondition", err)
	}
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{
		Meta: f.meta(t, "finalize-first-run"), TargetID: target.ID, RunID: first.ID,
		ExpectedRevision: first.Revision, Failed: true, BundleID: bundleID.String(),
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewCore(context.Background(), CoreOptions{Store: f.store, Clock: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	second.Meta = f.meta(t, "second-run-after-restart")
	if _, err := reopened.StartTargetRun(context.Background(), second); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("post-restart second run in generation error = %v, want failed precondition", err)
	}
	recovered, err := reopened.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	var recoveredTarget TargetRecord
	for _, candidate := range recovered.Targets {
		if candidate.ID == target.ID {
			recoveredTarget = candidate
			break
		}
	}
	if recoveredTarget.ID == "" {
		t.Fatal("restarted core lost target")
	}
	recoveredTarget, err = reopened.ResetTarget(context.Background(), ResetTargetRequest{
		Meta: f.meta(t, "reset-for-next-run"), TargetID: target.ID,
		ExpectedRevision: recoveredTarget.Revision, Mode: ports.ResetRecreate,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.TargetGenerationState{domain.TargetGenerationInstrumenting, domain.TargetGenerationReady} {
		generation := recoveredTarget.Generations[len(recoveredTarget.Generations)-1]
		recoveredTarget, err = reopened.TransitionTargetGeneration(context.Background(), TransitionTargetGenerationRequest{
			Meta: f.meta(t, "ready-reset-generation"), TargetID: target.ID,
			Generation: recoveredTarget.CurrentGeneration, ExpectedRevision: generation.Revision, State: state,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	second.Meta = f.meta(t, "run-after-reset")
	if _, err := reopened.StartTargetRun(context.Background(), second); err != nil {
		t.Fatalf("fresh generation rejected first run: %v", err)
	}
}

func TestFullTargetLifecycleResetKeepsAgentGeneration(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	if view.Session.State != domain.ResearchSessionLeased || view.Agent.CurrentGeneration != 1 {
		t.Fatalf("unexpected acquired view: %#v", view)
	}
	f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	var err error
	run := f.runningRun(t, target)
	operation, err := f.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{Meta: f.meta(t, "operation"), TargetID: target.ID, RunID: run.ID, Kind: domain.TargetOperationShell, CommandDisplay: "arbitrary guest shell", ContentDigest: domain.NewDigest([]byte("command bytes")).String()})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.TargetOperationState{domain.TargetOperationRunning, domain.TargetOperationCompleted} {
		operation, err = f.core.TransitionTargetOperation(context.Background(), TransitionTargetOperationRequest{Meta: f.meta(t, "operation-transition"), TargetID: target.ID, OperationID: operation.ID, ExpectedRevision: operation.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	run, err = f.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{Meta: f.meta(t, "finalize"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision, BundleID: bundleID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if run.State != domain.TargetRunCompleted || run.BundleID == "" {
		t.Fatalf("run not finalized: %#v", run)
	}
	records, err := f.store.Records(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	foundFinalizing := false
	for _, record := range records {
		if record.Kind != "target_run.finalizing" {
			continue
		}
		var journaled TargetRecord
		if err := json.Unmarshal(record.Payload, &journaled); err != nil {
			t.Fatal(err)
		}
		journaledRun, err := findRun(&journaled, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if journaledRun.State != domain.TargetRunFinalizing {
			t.Fatalf("finalizing record contains %s state", journaledRun.State)
		}
		foundFinalizing = true
	}
	if !foundFinalizing {
		t.Fatal("finalizing journal record not found")
	}
	target, err = f.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	resetRequest := ResetTargetRequest{Meta: f.meta(t, "reset"), TargetID: target.ID, ExpectedRevision: target.Revision, Mode: ports.ResetRecreate}
	target, err = f.core.ResetTarget(context.Background(), resetRequest)
	if err != nil {
		t.Fatal(err)
	}
	if target.CurrentGeneration != 2 || len(target.Generations) != 2 {
		t.Fatalf("target reset did not create generation two: %#v", target)
	}
	resetRequest.Mode = ports.ResetBaseline
	if _, err := f.core.ResetTarget(context.Background(), resetRequest); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changing the reset mode under one idempotency key returned %v, want idempotency conflict", err)
	}
	after, err := f.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Agent.CurrentGeneration != 1 || after.Lease.AgentGeneration != 1 {
		t.Fatalf("target reset changed agent generation: %#v", after.Agent)
	}
}

func TestQuarantineTargetAtomicallyTerminatesCurrentGenerationWork(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	run := f.runningRun(t, target)
	operation, err := f.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
		Meta: f.meta(t, "quarantine-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationShell, CommandDisplay: "suspect process",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = f.core.TransitionTargetOperation(context.Background(), TransitionTargetOperationRequest{
		Meta: f.meta(t, "quarantine-operation-running"), TargetID: target.ID, OperationID: operation.ID,
		ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := domain.ParseTargetID(before.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := QuarantineTargetRequest{
		Meta: f.meta(t, "quarantine-target"), TargetID: before.ID,
		ExpectedRevision: before.Revision, Reason: "backend confirmed containment",
		Evidence: ports.TargetQuarantineEvidence{
			Target:    ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(before.CurrentGeneration)},
			RuntimeID: "runtime-exact", ExecutionStopped: true, NetworkUnreachable: true, StatePreserved: true, ObservedAt: f.now,
		},
	}
	contained, err := f.core.QuarantineTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := findTargetGeneration(&contained, contained.CurrentGeneration)
	containedRun, _ := findRun(&contained, run.ID)
	containedOperation, _ := findOperation(&contained, operation.ID)
	if generation.State != domain.TargetGenerationQuarantined || containedRun.State != domain.TargetRunQuarantined || containedOperation.State != domain.TargetOperationCancelled {
		t.Fatalf("atomic quarantine result = generation %s, run %s, operation %s", generation.State, containedRun.State, containedOperation.State)
	}
	if contained.Revision != before.Revision+1 {
		t.Fatalf("quarantine target revision = %d, want %d", contained.Revision, before.Revision+1)
	}
	replay, err := f.core.QuarantineTarget(context.Background(), request)
	if err != nil || replay.Revision != contained.Revision {
		t.Fatalf("quarantine replay = %#v, %v", replay, err)
	}
	conflict := request
	conflict.Reason = "different reason"
	if _, err := f.core.QuarantineTarget(context.Background(), conflict); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("quarantine idempotency conflict = %v", err)
	}
}

func TestGetResearchSessionByLeaseResolvesExactProjection(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	resolved, err := f.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Session.ID != view.Session.ID || resolved.Lease.ID != view.Lease.ID || resolved.Agent.ID != view.Agent.ID {
		t.Fatalf("resolved wrong projection: %#v", resolved)
	}
	other, _ := domain.NewLeaseID()
	if _, err := f.core.GetResearchSessionByLease(context.Background(), other.String()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown lease error = %v, want ErrNotFound", err)
	}
}

func TestResetTargetRejectsInvalidModeSelectionBeforeMutation(t *testing.T) {
	f := newCoreFixture(t)
	invalid := []ResetTargetRequest{
		{Mode: "unknown"},
		{Mode: ports.ResetSnapshot},
		{Mode: ports.ResetRecreate, SnapshotName: "ignored"},
	}
	for index, request := range invalid {
		request.Meta = f.meta(t, "invalid-reset")
		request.TargetID = "missing"
		if _, err := f.core.ResetTarget(context.Background(), request); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Errorf("case %d returned %v, want invalid argument", index, err)
		}
	}
}

func TestFinalizeFailedTargetRunFromEveryActiveState(t *testing.T) {
	states := []domain.TargetRunState{
		domain.TargetRunRequested,
		domain.TargetRunPreparing,
		domain.TargetRunObserving,
		domain.TargetRunRunning,
		domain.TargetRunFinalizing,
	}
	for _, desiredState := range states {
		t.Run(desiredState.String(), func(t *testing.T) {
			f := newCoreFixture(t)
			view, _ := f.acquire(t)
			f.readyAgent(t, view.Agent)
			target := f.readyTarget(t, view)
			run, err := f.core.StartTargetRun(context.Background(), StartTargetRunRequest{
				Meta:                  f.meta(t, "run"),
				TargetID:              target.ID,
				MaterializationDigest: domain.NewDigest([]byte("specimen")).String(),
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, state := range states[1:] {
				if run.State == desiredState {
					break
				}
				run, err = f.core.TransitionTargetRun(context.Background(), TransitionTargetRunRequest{
					Meta:             f.meta(t, "run-transition"),
					TargetID:         target.ID,
					RunID:            run.ID,
					ExpectedRevision: run.Revision,
					State:            state,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			beforeRevision := run.Revision
			bundleID, err := domain.NewObservationBundleID()
			if err != nil {
				t.Fatal(err)
			}
			run, err = f.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{
				Meta:             f.meta(t, "failed-run"),
				TargetID:         target.ID,
				RunID:            run.ID,
				ExpectedRevision: run.Revision,
				Failed:           true,
				BundleID:         bundleID.String(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if run.State != domain.TargetRunFailed || run.BundleID != bundleID.String() {
				t.Fatalf("run not failed with its evidence bundle: %#v", run)
			}
			if run.Revision != beforeRevision+1 {
				t.Fatalf("failure should be one authoritative transition: got revision %d from %d", run.Revision, beforeRevision)
			}
		})
	}
}

func TestAcquireIdempotencyAndReplay(t *testing.T) {
	f := newCoreFixture(t)
	first, request := f.acquire(t)
	second, err := f.core.AcquireResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if second.Session.ID != first.Session.ID || second.Lease.ID != first.Lease.ID {
		t.Fatal("idempotent acquire created new identities")
	}
	if first.Session.AcquisitionIdempotencyKey != request.Meta.IdempotencyKey || second.Session.AcquisitionIdempotencyKey != request.Meta.IdempotencyKey {
		t.Fatal("logical acquisition did not retain its immutable idempotency identity")
	}
	conflict := request
	conflict.TTL = 2 * time.Hour
	if _, err := f.core.AcquireResearchSession(context.Background(), conflict); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("changed idempotency payload error = %v", err)
	}
	reopened, err := NewCore(context.Background(), CoreOptions{Store: f.store, Clock: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.GetResearchSession(context.Background(), first.Session.ID)
	if err != nil || replayed.Session.Revision != first.Session.Revision || replayed.Session.AcquisitionIdempotencyKey != request.Meta.IdempotencyKey {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
}

func TestReleaseResearchSessionSealsResources(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	outcome, err := f.core.ReleaseResearchSession(context.Background(), ReleaseResearchSessionRequest{Meta: f.meta(t, "release"), LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision, Reason: "research complete"})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.SessionID != view.Session.ID || outcome.LeaseID != view.Lease.ID {
		t.Fatalf("unexpected release outcome: %#v", outcome)
	}
	after, err := f.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Session.State != domain.ResearchSessionReleased || after.Lease.State != domain.LeaseReleased {
		t.Fatalf("session or lease was not released: %#v", after)
	}
	if after.Agent.Generations[0].State != domain.AgentGenerationSealed {
		t.Fatalf("agent generation was not sealed: %#v", after.Agent)
	}
	releasedTarget, err := f.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if releasedTarget.Generations[0].State != domain.TargetGenerationDestroyed {
		t.Fatalf("target generation was not destroyed: %#v", releasedTarget)
	}
}

func TestReleaseResearchSessionTwoPhaseGatesNewWork(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	request := ReleaseResearchSessionRequest{
		Meta: f.meta(t, "prepare-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "research complete",
	}
	preparation, err := f.core.BeginReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.View.Lease.State != domain.LeaseReleasing || preparation.View.Session.State != domain.ResearchSessionReleasing {
		t.Fatalf("release was not durably gated: %#v", preparation.View)
	}
	preparedAgent, err := findAgentGeneration(&preparation.View.Agent, agent.CurrentGeneration)
	if err != nil || preparedAgent.State != domain.AgentGenerationReady {
		t.Fatalf("begin release retired the agent before physical cleanup: %#v, %v", preparedAgent, err)
	}
	preparedTarget, err := findTargetGeneration(&preparation.View.Targets[0], target.CurrentGeneration)
	if err != nil || preparedTarget.State != domain.TargetGenerationReady {
		t.Fatalf("begin release retired the target before physical cleanup: %#v, %v", preparedTarget, err)
	}
	if _, err := f.core.CreateTarget(context.Background(), CreateTargetRequest{
		Meta: f.meta(t, "target-after-release-gate"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest,
	}); err == nil {
		t.Fatal("releasing lease accepted a new target")
	}
	complete := request
	complete.Meta = f.meta(t, "complete-release")
	complete.ExpectedRevision = preparation.ReleasingLeaseRevision
	if _, err := f.core.CompleteReleaseResearchSession(context.Background(), complete); err != nil {
		t.Fatal(err)
	}
	after, err := f.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Lease.State != domain.LeaseReleased || after.Session.State != domain.ResearchSessionReleased {
		t.Fatalf("release did not complete: %#v", after)
	}
	completedAgent, _ := findAgentGeneration(&after.Agent, agent.CurrentGeneration)
	completedTarget, _ := findTargetGeneration(&after.Targets[0], target.CurrentGeneration)
	if completedAgent.State != domain.AgentGenerationSealed || completedTarget.State != domain.TargetGenerationDestroyed {
		t.Fatalf("physical completion was not reflected logically: agent=%s target=%s", completedAgent.State, completedTarget.State)
	}
	// Replaying Begin returns the current terminal view while retaining the
	// originally committed releasing revision for deterministic completion.
	replayed, err := f.core.BeginReleaseResearchSession(context.Background(), request)
	if err != nil || replayed.View.Lease.State != domain.LeaseReleased || replayed.ReleasingLeaseRevision != preparation.ReleasingLeaseRevision {
		t.Fatalf("BeginRelease(replay) = %#v, %v", replayed, err)
	}
}

func TestTargetRecoveryRequiresSealedEvidenceAndKeepsAgentGeneration(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	run := f.runningRun(t, target)
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	run, err = f.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{Meta: f.meta(t, "failed-run"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision, Failed: true, BundleID: bundleID.String()})
	if err != nil {
		t.Fatal(err)
	}
	incident, err := f.core.CreateIncident(context.Background(), CreateIncidentRequest{Meta: f.meta(t, "incident"), Classification: domain.IncidentLinuxTargetFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID, AgentGeneration: agent.CurrentGeneration, TargetID: target.ID, TargetGeneration: target.CurrentGeneration, TargetRunID: run.ID, Trigger: "container exited", LastKnownState: "running", Cause: CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0}, Artifacts: []IncidentArtifactRecord{incidentArtifact("target-run-final")}})
	if err != nil {
		t.Fatal(err)
	}
	request := RecoverIncidentRequest{Meta: f.meta(t, "recover"), IncidentID: incident.ID, ExpectedIncidentRevision: incident.Revision, Resource: RecoveryResourceTarget, Strategy: "cold recreate", VisibilityAcknowledgement: "incident streamed"}
	if _, err := f.core.RecoverIncident(context.Background(), request); err == nil {
		t.Fatal("recovery started before evidence was sealed")
	}
	incident, err = f.core.TransitionIncident(context.Background(), TransitionIncidentRequest{Meta: f.meta(t, "seal-incident"), IncidentID: incident.ID, ExpectedRevision: incident.Revision, State: domain.IncidentEvidenceSealed, VisibilityAcknowledgements: []string{"evidence indexed"}})
	if err != nil {
		t.Fatal(err)
	}
	request.Meta = f.meta(t, "recover")
	request.ExpectedIncidentRevision = incident.Revision
	outcome, err := f.core.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Target == nil || outcome.Target.CurrentGeneration != 2 || outcome.Target.Generations[1].RecoveryIncident != incident.ID {
		t.Fatalf("target recovery did not create linked generation: %#v", outcome)
	}
	if outcome.Incident.State != domain.IncidentRecovering {
		t.Fatalf("incident did not enter recovering: %#v", outcome.Incident)
	}
	completion := TransitionIncidentRequest{
		Meta: f.meta(t, "complete-recovery"), IncidentID: outcome.Incident.ID,
		ExpectedRevision: outcome.Incident.Revision, State: domain.IncidentResolved,
		RecoveryActions:            append(append([]string(nil), outcome.Incident.RecoveryActions...), "physical-target:cold recreate"),
		VisibilityAcknowledgements: append([]string(nil), outcome.Incident.VisibilityAcknowledgements...),
	}
	if _, err := f.core.TransitionIncident(context.Background(), completion); err == nil || !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("ordinary transition completed physical recovery: %v", err)
	}
	wrongCompletion := completion
	wrongCompletion.Meta = f.meta(t, "complete-recovery-wrong-action")
	wrongCompletion.RecoveryActions = append(append([]string(nil), outcome.Incident.RecoveryActions...), "physical-agent:cold recreate")
	if _, err := f.core.CompleteIncidentRecovery(context.Background(), wrongCompletion); err == nil || !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("trusted recovery accepted a different physical action: %v", err)
	}
	resolved, err := f.core.CompleteIncidentRecovery(context.Background(), completion)
	if err != nil || resolved.State != domain.IncidentResolved {
		t.Fatalf("trusted recovery completion = %#v, %v", resolved, err)
	}
	after, err := f.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Agent.CurrentGeneration != 1 || after.Lease.AgentGeneration != 1 {
		t.Fatalf("target recovery changed agent generation: %#v", after.Agent)
	}
}

func TestSealRunIncidentsBindsExactBundleAtomicallyAndReplays(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	run := f.runningRun(t, target)
	incident, err := f.core.CreateIncident(context.Background(), CreateIncidentRequest{
		Meta: f.meta(t, "bundle-incident"), Classification: domain.IncidentLinuxTargetFailure,
		SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID, AgentGeneration: agent.CurrentGeneration,
		TargetID: target.ID, TargetGeneration: target.CurrentGeneration, TargetRunID: run.ID,
		Trigger: "container exited", LastKnownState: "running",
		Cause: CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	artifact := IncidentArtifactRecord{
		Reference: "artifact://bundles/" + bundleID.String(), Digest: domain.NewDigest([]byte("sealed bundle")).String(),
		Size: 13, Role: "observation-bundle", Sensitivity: domain.SensitivityRestricted,
	}
	run, err = f.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{
		Meta: f.meta(t, "failed-run-with-bundle"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision,
		Failed: true, BundleID: bundleID.String(), BundleArtifact: artifact.Reference, BundleDigest: artifact.Digest,
		IncidentIDs: []string{incident.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := SealRunIncidentsRequest{
		Meta: f.meta(t, "seal-run-incidents"), TargetID: target.ID, RunID: run.ID,
		BundleID: bundleID.String(), BundleArtifact: artifact, IncidentIDs: []string{incident.ID},
	}
	sealed, err := f.core.SealRunIncidents(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1 || sealed[0].State != domain.IncidentEvidenceSealed || !incidentHasBundleEvidence(sealed[0], bundleID.String(), artifact) {
		t.Fatalf("sealed incidents = %#v", sealed)
	}
	replayed, err := f.core.SealRunIncidents(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].Revision != sealed[0].Revision {
		t.Fatalf("idempotent replay = %#v, want revision %d", replayed, sealed[0].Revision)
	}

	restarted, err := NewCore(context.Background(), CoreOptions{Store: f.store, IDs: f.core.ids, Clock: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := restarted.GetIncident(context.Background(), incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.State != domain.IncidentEvidenceSealed || !incidentHasBundleEvidence(restored, bundleID.String(), artifact) {
		t.Fatalf("replayed incident = %#v", restored)
	}
}

func TestAgentRecoveryRollsOnlyAgentAndLeaseGeneration(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	current := agent.Generations[0]
	agent, err := f.core.TransitionAgentGeneration(context.Background(), TransitionAgentRequest{Meta: f.meta(t, "fail-agent"), AgentWorkspaceID: agent.ID, Generation: agent.CurrentGeneration, ExpectedRevision: current.Revision, State: domain.AgentGenerationFailed})
	if err != nil {
		t.Fatal(err)
	}
	incident, err := f.core.CreateIncident(context.Background(), CreateIncidentRequest{Meta: f.meta(t, "agent-incident"), Classification: domain.IncidentAgentWorkspaceFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID, AgentGeneration: agent.CurrentGeneration, Trigger: "supervisor exited", LastKnownState: "ready", Cause: CauseRecord{Kind: domain.CauseProven, Summary: "supervisor exit observed", Confidence: 1}, Artifacts: []IncidentArtifactRecord{incidentArtifact("agent-supervisor-exit")}})
	if err != nil {
		t.Fatal(err)
	}
	incident, err = f.core.TransitionIncident(context.Background(), TransitionIncidentRequest{Meta: f.meta(t, "seal-agent-incident"), IncidentID: incident.ID, ExpectedRevision: incident.Revision, State: domain.IncidentEvidenceSealed})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := f.core.RecoverIncident(context.Background(), RecoverIncidentRequest{Meta: f.meta(t, "recover-agent"), IncidentID: incident.ID, ExpectedIncidentRevision: incident.Revision, Resource: RecoveryResourceAgent, Strategy: "new provider invocation", VisibilityAcknowledgement: "failure delivered"})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Agent == nil || outcome.Lease == nil || outcome.Agent.CurrentGeneration != 2 || outcome.Lease.AgentGeneration != 2 {
		t.Fatalf("agent recovery did not update agent and lease: %#v", outcome)
	}
	if len(outcome.Agent.Generations) != 2 || outcome.Agent.Generations[1].RecoveryIncident != incident.ID {
		t.Fatalf("agent recovery provenance missing: %#v", outcome.Agent)
	}
}

func TestExecLifecyclePreservesTerminalZeroAndIncidentLinks(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	execution, err := f.core.CreateExec(context.Background(), CreateExecRequest{Meta: f.meta(t, "exec"), LeaseID: view.Lease.ID, Kind: domain.ExecProvider, Executable: "bin/provider", Argv: []string{"run"}, WorkingDirectory: "analysis"})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.ExecState{domain.ExecStarting, domain.ExecRunning} {
		execution, err = f.core.TransitionExec(context.Background(), TransitionExecRequest{Meta: f.meta(t, "exec-transition"), ExecID: execution.ID, ExpectedRevision: execution.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	zero := 0
	execution, err = f.core.FinalizeExec(context.Background(), FinalizeExecRequest{Meta: f.meta(t, "exec-finalize"), ExecID: execution.ID, ExpectedRevision: execution.Revision, State: domain.ExecCompleted, ExitCode: &zero, CleanupConfirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if execution.ExitCode == nil || *execution.ExitCode != 0 || !execution.CleanupConfirmed {
		t.Fatalf("terminal zero or cleanup state was lost: %#v", execution)
	}
	after, err := f.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Agent.Generations[0].State != domain.AgentGenerationRunning || len(after.Execs) != 1 {
		t.Fatalf("exec did not propagate to session and agent: %#v", after)
	}

	failed, err := f.core.CreateExec(context.Background(), CreateExecRequest{Meta: f.meta(t, "failed-exec"), LeaseID: view.Lease.ID, Kind: domain.ExecTool, Executable: "bin/tool", Argv: []string{"inspect"}})
	if err != nil {
		t.Fatal(err)
	}
	failed, err = f.core.TransitionExec(context.Background(), TransitionExecRequest{Meta: f.meta(t, "failed-start"), ExecID: failed.ID, ExpectedRevision: failed.Revision, State: domain.ExecStarting})
	if err != nil {
		t.Fatal(err)
	}
	exit := 17
	failed, err = f.core.FinalizeExec(context.Background(), FinalizeExecRequest{Meta: f.meta(t, "failed-finalize"), ExecID: failed.ID, ExpectedRevision: failed.Revision, State: domain.ExecFailed, ExitCode: &exit, Error: "transport disconnected"})
	if err != nil {
		t.Fatal(err)
	}
	incident, err := f.core.CreateIncident(context.Background(), CreateIncidentRequest{Meta: f.meta(t, "exec-incident"), Classification: domain.IncidentAgentExecFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID, AgentGeneration: agent.CurrentGeneration, ExecID: failed.ID, Trigger: "transport disconnected", LastKnownState: "starting", Cause: CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0}, Artifacts: []IncidentArtifactRecord{incidentArtifact("exec-terminal")}})
	if err != nil {
		t.Fatal(err)
	}
	linked, err := f.core.GetExec(context.Background(), failed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(linked.IncidentIDs) != 1 || linked.IncidentIDs[0] != incident.ID {
		t.Fatalf("incident was not linked to exec: %#v", linked)
	}
}

func TestReleaseRejectsActiveExec(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	f.readyAgent(t, view.Agent)
	if _, err := f.core.CreateExec(context.Background(), CreateExecRequest{Meta: f.meta(t, "active-exec"), LeaseID: view.Lease.ID, Kind: domain.ExecTool, Executable: "bin/tool"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ReleaseResearchSession(context.Background(), ReleaseResearchSessionRequest{Meta: f.meta(t, "release-active"), LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision, Reason: "done"}); err == nil {
		t.Fatal("release accepted an active exec")
	}
}

func incidentArtifact(label string) IncidentArtifactRecord {
	content := []byte(label)
	return IncidentArtifactRecord{Reference: "artifact://incident/" + label, Digest: domain.NewDigest(content).String(), Size: int64(len(content)), Role: "incident-evidence", Sensitivity: domain.SensitivityInternal}
}
