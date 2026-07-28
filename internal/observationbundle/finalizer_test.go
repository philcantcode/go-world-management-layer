package observationbundle_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestFinalizePublishesExactlyOneCanonicalBundleAndReplays(t *testing.T) {
	request := completedRequest(t)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := finalizer.Finalize(context.Background(), request); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Finalize(without deadline) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Bundle.State() != domain.ObservationBundleSealed {
		t.Fatalf("first result = %#v", first)
	}
	replay, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Created || replay.Metadata.ContentDigest != first.Metadata.ContentDigest || replay.Path != first.Path {
		t.Fatalf("replay = %#v, first = %#v", replay, first)
	}
	encoded, err := os.ReadFile(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata observationbundle.Metadata
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, canonical) {
		t.Fatalf("sealed metadata is not canonical JSON: %q", encoded)
	}
	if first.Content == nil || first.Content.Digest() != domain.NewDigest(encoded) || first.Content.Size() != int64(len(encoded)) {
		t.Fatalf("sealed content identity does not describe published bytes")
	}
	reader, err := first.Content.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamed, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(streamed, encoded) {
		t.Fatalf("sealed content stream mismatch: read=%v close=%v", err, closeErr)
	}
	marker, err := os.ReadFile(filepath.Join(filepath.Dir(first.Path), "committed"))
	if err != nil {
		t.Fatal(err)
	}
	if string(marker) != first.Content.Digest().String()+"\n" {
		t.Fatalf("commit marker = %q", marker)
	}
	entries, err := os.ReadDir(filepath.Dir(first.Path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("run directory contains %d entries, want sealed metadata and marker", len(entries))
	}
}

func TestFinalizeConcurrentRetryCreatesOneBundle(t *testing.T) {
	request := completedRequest(t)
	root := t.TempDir()
	finalizer, err := observationbundle.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const callers = 24
	results := make(chan observationbundle.Result, callers)
	errors := make(chan error, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, finalizeErr := finalizer.Finalize(ctx, request)
			if finalizeErr != nil {
				errors <- finalizeErr
				return
			}
			results <- result
		}()
	}
	group.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("Finalize() error = %v", err)
	}
	created := 0
	digest := ""
	count := 0
	for result := range results {
		count++
		if result.Created {
			created++
		}
		if digest == "" {
			digest = result.Metadata.ContentDigest
		} else if result.Metadata.ContentDigest != digest {
			t.Fatalf("retry digest = %q, want %q", result.Metadata.ContentDigest, digest)
		}
	}
	if count != callers || created != 1 {
		t.Fatalf("successful callers = %d, created = %d; want %d and 1", count, created, callers)
	}
	objects, err := os.ReadDir(filepath.Join(root, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("object count = %d, want 1", len(objects))
	}
}

func TestFinalizeDetectsConflictingRetry(t *testing.T) {
	request := completedRequest(t)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := finalizer.Finalize(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.FinalizedAt = request.FinalizedAt.Add(time.Second)
	if _, err := finalizer.Finalize(ctx, request); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("conflicting Finalize() error = %v, want conflict", err)
	}
}

func TestFinalizeSealsFailedRunWithCoverageLossEvidence(t *testing.T) {
	request := failedRequest(t)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Metadata.Bundle.Outcome != ports.RunFailed || len(result.Metadata.Bundle.Gaps) != 1 || len(result.Metadata.Bundle.IncidentIDs) != 1 {
		t.Fatalf("failed bundle metadata = %#v", result.Metadata.Bundle)
	}
}

func TestFinalizeRejectsIncompleteEvidence(t *testing.T) {
	tests := map[string]struct {
		mutate func(*testing.T, *observationbundle.FinalizeRequest)
		code   domain.ErrorCode
	}{
		"missing required coverage": {
			mutate: func(_ *testing.T, request *observationbundle.FinalizeRequest) { request.Result.Coverage = nil },
			code:   domain.CodeInvalidArgument,
		},
		"failed run without incident": {
			mutate: func(_ *testing.T, request *observationbundle.FinalizeRequest) { request.Result.IncidentIDs = nil },
			code:   domain.CodeIntegrityViolation,
		},
		"summary cites absent raw evidence": {
			mutate: func(t *testing.T, request *observationbundle.FinalizeRequest) {
				rogue := mustArtifact(t, "artifact://rogue", "rogue")
				summary, err := domain.NewDerivedSummary(domain.DerivedSummarySpec{
					Text: "Unsupported citation", Citations: []domain.EvidenceCitation{{FirstCursor: 1, LastCursor: 1, Artifact: rogue}},
				})
				if err != nil {
					t.Fatal(err)
				}
				request.Result.Summary = summary
			},
			code: domain.CodeConflict,
		},
		"event cursor outside sealed range": {
			mutate: func(t *testing.T, request *observationbundle.FinalizeRequest) {
				params := request.Result.NormalizedEvents[0].Params()
				params.SourceCursor = request.Result.LastCursor + 1
				event, err := domain.NewEventEnvelope(params)
				if err != nil {
					t.Fatal(err)
				}
				request.Result.NormalizedEvents = []domain.EventEnvelope{event}
			},
			code: domain.CodeInvalidArgument,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			request := failedRequest(t)
			test.mutate(t, &request)
			finalizer, err := observationbundle.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := finalizer.Finalize(ctx, request); !domain.IsCode(err, test.code) {
				t.Fatalf("Finalize() error = %v, want %s", err, test.code)
			}
		})
	}
}

func TestFinalizeRejectsMalformedTrailingMetadataOnRetry(t *testing.T) {
	request := completedRequest(t)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(result.Path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("{")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := finalizer.Finalize(ctx, request); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("Finalize(corrupt retry) error = %v, want integrity_violation", err)
	}
}

type bundleFixture struct {
	request    observationbundle.FinalizeRequest
	incidentID domain.IncidentID
}

func completedRequest(t *testing.T) observationbundle.FinalizeRequest {
	return newBundleFixture(t).request
}

func failedRequest(t *testing.T) observationbundle.FinalizeRequest {
	fixture := newBundleFixture(t)
	request := fixture.request
	gap, err := domain.NewGap(domain.GapSpec{
		Kind: domain.GapDropped, Source: "process-collector", SourceInstance: "collector-1",
		FirstSourceSequence: 2, LastSourceSequence: 5, FirstCursor: 2, LastCursor: 2,
		StartedAt: request.CreatedAt.Add(time.Second), EndedAt: request.Result.StoppedAt,
		LostRecords: 4, Reason: "collector buffer overflow during target failure",
	})
	if err != nil {
		t.Fatal(err)
	}
	coverageSpec := request.Result.Coverage[0].Spec()
	coverageSpec.Level = domain.CoverageLevelPartial
	coverageSpec.Status = domain.CoverageLost
	coverageSpec.DroppedRecords = 4
	coverageSpec.Gaps = []domain.Gap{gap}
	coverage, err := domain.NewCollectorCoverage(coverageSpec)
	if err != nil {
		t.Fatal(err)
	}
	request.Result.Outcome = ports.RunFailed
	request.Result.Coverage[0] = coverage
	request.Result.Gaps = []domain.Gap{gap}
	request.Result.IncidentIDs = []domain.IncidentID{fixture.incidentID}
	return request
}

func newBundleFixture(t *testing.T) bundleFixture {
	t.Helper()
	clock := testkit.NewClock(time.Unix(1_900_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	sessionID := mustGenerate(t, ids.ResearchSessionID)
	leaseID := mustGenerate(t, ids.LeaseID)
	agentID := mustGenerate(t, ids.AgentWorkspaceID)
	targetID := mustGenerate(t, ids.TargetID)
	runID := mustGenerate(t, ids.TargetRunID)
	bundleID := mustGenerate(t, ids.ObservationBundleID)
	eventID := mustGenerate(t, ids.EventID)
	collectorID := mustGenerate(t, ids.CollectorID)
	subjectID := mustGenerate(t, ids.SubjectID)
	incidentID := mustGenerate(t, ids.IncidentID)
	createdAt := clock.Now()
	stoppedAt := createdAt.Add(4 * time.Second)
	finalizedAt := createdAt.Add(5 * time.Second)
	artifact := mustArtifact(t, "artifact://raw/process-events", "raw process evidence")
	event, err := domain.NewEventEnvelope(domain.EventEnvelopeParams{
		SchemaVersion: 1, EventID: eventID, Kind: "process.exec", ResearchSessionID: sessionID,
		LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration, TargetRunID: runID,
		Source: "process-collector", SourceInstance: "collector-1", SourceSequence: 1, SourceCursor: 1,
		CollectorID: collectorID, CollectorPlacement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelComplete,
		ObservedWallTime: createdAt.Add(time.Second), Payload: json.RawMessage(`{"pid":42}`),
		Sensitivity: domain.SensitivityRestricted, Completeness: domain.CompletenessComplete,
		Confidence: 1, Origin: domain.OriginSystem,
	})
	if err != nil {
		t.Fatal(err)
	}
	value := 12.5
	metric, err := domain.NewMetricSample(domain.MetricSampleSpec{
		SubjectID: subjectID, SubjectKind: domain.SubjectLinuxTarget, Name: "cpu.utilization", Unit: "percent",
		Kind: domain.MetricGauge, Availability: domain.MetricAvailable, NumericValue: &value,
		CollectedAt: createdAt.Add(2 * time.Second), PublishedAt: createdAt.Add(3 * time.Second),
		Cursor: 2, Labels: map[string]string{"source": "fake"}, TargetRunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := domain.NewCollectorCoverage(domain.CollectorCoverageSpec{
		CollectorID: collectorID, SignalFamily: "process", Placement: domain.CollectorPlacementHost,
		Level: domain.CoverageLevelComplete, Status: domain.CoverageAvailable, Required: true,
		StartedAt: createdAt, EndedAt: stoppedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	change, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
		Kind: domain.ChangeModified, Path: "state/runtime.db", BeforeDigest: domain.NewDigest([]byte("before")),
		AfterDigest: domain.NewDigest([]byte("after")), Metadata: map[string]string{"source": "target"},
	})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, []domain.ChangeEntry{change}, domain.InitialRevision, stoppedAt)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := domain.NewDerivedSummary(domain.DerivedSummarySpec{
		Text:       "The target executed a process and changed its runtime state.",
		Citations:  []domain.EvidenceCitation{{FirstCursor: 1, LastCursor: 2, Artifact: artifact}},
		Inferences: []string{"runtime state changed after process execution"},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := observationbundle.FinalizeRequest{
		BundleID: bundleID, TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration,
		AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		RequiredCoverage: []string{"process"}, CreatedAt: createdAt, FinalizedAt: finalizedAt,
		Result: ports.TargetRunResult{
			RunID: runID, Outcome: ports.RunCompleted, FirstCursor: 1, LastCursor: 2,
			RawArtifacts: []domain.ArtifactReference{artifact}, NormalizedEvents: []domain.EventEnvelope{event},
			Metrics: []domain.MetricSample{metric}, Coverage: []domain.CollectorCoverage{coverage},
			TargetChanges: changes, Summary: summary, StoppedAt: stoppedAt,
		},
	}
	return bundleFixture{request: request, incidentID: incidentID}
}

func mustArtifact(t *testing.T, reference, content string) domain.ArtifactReference {
	t.Helper()
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: reference, Digest: domain.NewDigest([]byte(content)), Size: int64(len(content)),
		Role: "raw-observation", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func mustGenerate[T any](t *testing.T, generate func() (T, error)) T {
	t.Helper()
	value, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
