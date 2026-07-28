package application

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestRunFinalizationSealsPublishesAndCommitsEvidenceIdempotently(t *testing.T) {
	f := newCoreFixture(t)
	view, _ := f.acquire(t)
	agent := f.readyAgent(t, view.Agent)
	target := f.readyTarget(t, view)
	run := f.runningRun(t, target)
	evidence := completedRunEvidence(t, f.now, view, agent, target, run)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := testkit.NewFakeMaterialAuthority(nil, nil)
	service, err := NewRunFinalizationService(f.core, finalizer, authority)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := FinalizeRunEvidenceRequest{Meta: f.meta(t, "evidence-finalize"), TargetID: target.ID, ExpectedRunRevision: run.Revision, Evidence: evidence}
	prepared, err := service.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	beforeCommit, err := f.core.GetTarget(ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	stillRunning, err := findRun(&beforeCommit, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillRunning.State.Terminal() || stillRunning.BundleID != "" {
		t.Fatalf("prepare crossed the terminal control boundary: %#v", stillRunning)
	}
	first, err := service.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if first.Run.State != domain.TargetRunCompleted || first.Run.BundleID == "" || first.Run.BundleArtifact == "" || first.Run.BundleDigest == "" {
		t.Fatalf("run evidence was not fully committed: %#v", first.Run)
	}
	if first.Bundle.State() != domain.ObservationBundleSealed || first.Artifact.Spec().Digest.String() != first.Run.BundleDigest {
		t.Fatalf("bundle publication mismatch: %#v", first)
	}
	replay, err := service.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Run.BundleID != first.Run.BundleID || replay.Artifact.Spec().Reference != first.Artifact.Spec().Reference {
		t.Fatalf("idempotent replay changed evidence identity: %#v %#v", first, replay)
	}
}

func completedRunEvidence(t *testing.T, now time.Time, view ResearchSessionView, agent AgentWorkspaceRecord, target TargetRecord, run TargetRunRecord) observationbundle.FinalizeRequest {
	t.Helper()
	sessionID, _ := domain.ParseResearchSessionID(view.Session.ID)
	leaseID, _ := domain.ParseLeaseID(view.Lease.ID)
	agentID, _ := domain.ParseAgentWorkspaceID(agent.ID)
	targetID, _ := domain.ParseTargetID(target.ID)
	runID, _ := domain.ParseTargetRunID(run.ID)
	bundleID, err := domain.NewObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := domain.NewEventID()
	if err != nil {
		t.Fatal(err)
	}
	collectorID, err := domain.NewCollectorID()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: "artifact://raw/process", Digest: domain.NewDigest([]byte("raw")), Size: 3, Role: "raw-process", Sensitivity: domain.SensitivityRestricted})
	if err != nil {
		t.Fatal(err)
	}
	event, err := domain.NewEventEnvelope(domain.EventEnvelopeParams{SchemaVersion: 1, EventID: eventID, Kind: "process.exec", ResearchSessionID: sessionID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(agent.CurrentGeneration), TargetID: targetID, TargetGeneration: domain.TargetGeneration(target.CurrentGeneration), TargetRunID: runID, Source: "test-collector", SourceInstance: "collector-1", SourceSequence: 1, SourceCursor: 1, CollectorID: collectorID, CollectorPlacement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelComplete, ObservedWallTime: now.Add(time.Second), Payload: json.RawMessage(`{"pid":42}`), Sensitivity: domain.SensitivityRestricted, Completeness: domain.CompletenessComplete, Confidence: 1, Origin: domain.OriginSystem})
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := domain.NewCollectorCoverage(domain.CollectorCoverageSpec{CollectorID: collectorID, SignalFamily: "process", Placement: domain.CollectorPlacementHost, Level: domain.CoverageLevelComplete, Status: domain.CoverageAvailable, Required: true, StartedAt: now, EndedAt: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	change, err := domain.NewChangeEntry(domain.ChangeEntrySpec{Kind: domain.ChangeModified, Path: "state/runtime.db", BeforeDigest: domain.NewDigest([]byte("before")), AfterDigest: domain.NewDigest([]byte("after"))})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, []domain.ChangeEntry{change}, 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	summary, err := domain.NewDerivedSummary(domain.DerivedSummarySpec{Text: "process execution observed", Citations: []domain.EvidenceCitation{{FirstCursor: 1, LastCursor: 1, Artifact: artifact}}})
	if err != nil {
		t.Fatal(err)
	}
	result := ports.TargetRunResult{RunID: runID, Outcome: ports.RunCompleted, FirstCursor: 1, LastCursor: 1, RawArtifacts: []domain.ArtifactReference{artifact}, NormalizedEvents: []domain.EventEnvelope{event}, Coverage: []domain.CollectorCoverage{coverage}, TargetChanges: changes, Summary: summary, StoppedAt: now.Add(2 * time.Second)}
	return observationbundle.FinalizeRequest{BundleID: bundleID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(target.CurrentGeneration), AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(agent.CurrentGeneration), RequiredCoverage: []string{"process"}, Result: result, CreatedAt: now, FinalizedAt: now.Add(3 * time.Second)}
}
