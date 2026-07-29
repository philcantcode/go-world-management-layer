package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestBundleStopPreparationStoresFailureEvidenceOnce(t *testing.T) {
	preparation := validStopPreparation(t)
	preparation.Result.Outcome = ports.RunFailed
	uniqueReference := "artifact://unique/evidence-reference"
	uniqueGapReason := "unique gap reason retained only in the result"
	preparation.Result.Artifacts = []persistedArtifact{{
		Reference: uniqueReference, Digest: domain.NewDigest([]byte("artifact")).String(), Size: 8,
		Role: "collector-output", Sensitivity: domain.SensitivityInternal,
	}}
	preparation.Result.Gaps = []persistedGap{{Kind: domain.GapUnavailable, Source: "collector", Reason: uniqueGapReason}}
	preparation.Incident = &runFailureIncidentIntent{
		Classification: domain.IncidentObserverFailure, Trigger: "required collector failed",
		Cause: application.CauseRecord{Kind: domain.CauseProven, Summary: "collector failed", Confidence: 1},
	}
	preparation.ObserverDigest = mustStoppedResultDigest(t, preparation.Result)
	encoded, err := json.Marshal(preparation)
	if err != nil {
		t.Fatal(err)
	}
	for label, unique := range map[string]string{"artifact": uniqueReference, "gap": uniqueGapReason} {
		if count := bytes.Count(encoded, []byte(unique)); count != 1 {
			t.Fatalf("%s evidence appears %d times in the stop preparation, want exactly once", label, count)
		}
	}
}

func TestBundleStopPreparationEnforcesExactSerializedByteBoundary(t *testing.T) {
	preparation := validStopPreparation(t)
	preparation.Result.Summary.Text = strings.Repeat("x", 4096)
	preparation.ObserverDigest = mustStoppedResultDigest(t, preparation.Result)
	service := &Service{maxTransferBytes: 1 << 20, maxStateBytes: 1}
	encoded, err := service.validateAndEncodeBundleStopPreparation(preparation)
	if err != nil {
		t.Fatal(err)
	}
	service.maxTransferBytes = int64(len(encoded)) - int64(service.maxStateBytes)
	if _, err := service.validateAndEncodeBundleStopPreparation(preparation); err != nil {
		t.Fatalf("exact byte limit rejected: %v", err)
	}
	service.maxTransferBytes--
	if _, err := service.validateAndEncodeBundleStopPreparation(preparation); err == nil {
		t.Fatal("one-byte stop-preparation overflow was accepted")
	}
}

func TestObserverMarkerAcceptsExactLimitAndRejectsOneByteOverflowWithoutReplacement(t *testing.T) {
	stateRoot, config := observerNamespaceSecurityConfig(t)
	coordinator, err := NewRunObserverCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := config.IDs.TargetRunID()
	if err != nil {
		t.Fatal(err)
	}
	marker := observerStateMarker{
		Version: observerStateVersion, RunID: runID.String(), PlanDigest: domain.NewDigest([]byte("plan")).String(),
		Phase: "active", Collectors: []ports.InterruptedCollectorBinding{}, UpdatedAt: config.Clock().UTC(),
	}
	low, high := 1, int(maximumObserverStateMarkerBytes)+1
	for low+1 < high {
		middle := low + (high-low)/2
		marker.Signature = strings.Repeat("x", middle)
		encoded, marshalErr := json.Marshal(marker)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if int64(len(encoded)) <= maximumObserverStateMarkerBytes {
			low = middle
		} else {
			high = middle
		}
	}
	marker.Signature = strings.Repeat("x", low)
	want, err := json.Marshal(marker)
	if err != nil || int64(len(want)) != maximumObserverStateMarkerBytes {
		t.Fatalf("boundary marker size = %d, want %d: %v", len(want), maximumObserverStateMarkerBytes, err)
	}
	if err := coordinator.writeMarker(marker); err != nil {
		t.Fatalf("exact marker byte limit rejected: %v", err)
	}
	marker.Signature += "x"
	if err := coordinator.writeMarker(marker); !domain.IsCode(err, domain.CodeResourceExhausted) {
		t.Fatalf("one-byte marker overflow error = %v, want resource exhausted", err)
	}
	stored, err := os.ReadFile(filepath.Join(stateRoot, "runs", runID.String()+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, want) {
		t.Fatal("oversized marker attempt replaced the exact-limit durable marker")
	}
}

func TestObserverMarkerVersionSixContract(t *testing.T) {
	_, config := observerNamespaceSecurityConfig(t)
	coordinator, err := NewRunObserverCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := config.IDs.TargetRunID()
	if err != nil {
		t.Fatal(err)
	}
	marker := observerStateMarker{
		Version: 6, RunID: runID.String(), PlanDigest: domain.NewDigest([]byte("plan")).String(),
		Signature: "signature", Phase: "active", Collectors: []ports.InterruptedCollectorBinding{}, UpdatedAt: config.Clock().UTC(),
	}
	if err := coordinator.writeMarker(marker); err != nil {
		t.Fatal(err)
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 1 || markers[0].Version != 6 {
		t.Fatalf("load literal v6 marker = %#v, %v", markers, err)
	}

	marker.Version = 5
	if err := coordinator.writeMarker(marker); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.loadMarkers(); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("load literal v5 marker error = %v, want integrity violation", err)
	}
}

func TestObserverMarkerBindsEntireStopPreparationDigest(t *testing.T) {
	_, config := observerNamespaceSecurityConfig(t)
	coordinator, err := NewRunObserverCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := config.IDs.TargetRunID()
	if err != nil {
		t.Fatal(err)
	}
	resultDigest := domain.NewDigest([]byte("stopped result")).String()
	preparationDigest := domain.NewDigest([]byte("complete preparation")).String()
	marker := observerStateMarker{
		Version: observerStateVersion, RunID: runID.String(), PlanDigest: domain.NewDigest([]byte("plan")).String(),
		Signature: "signature", Phase: "stopped", Collectors: []ports.InterruptedCollectorBinding{},
		StoppedResultDigest: resultDigest, StopPreparationDigest: preparationDigest, UpdatedAt: config.Clock().UTC(),
	}
	if err := coordinator.writeMarker(marker); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RequireStoppedPreparation(runID.String(), resultDigest, preparationDigest); err != nil {
		t.Fatalf("exact preparation binding rejected: %v", err)
	}
	tamperedDigest := domain.NewDigest([]byte("tampered preparation")).String()
	if err := coordinator.RequireStoppedPreparation(runID.String(), resultDigest, tamperedDigest); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("tampered preparation digest error = %v, want integrity violation", err)
	}
	marker.StopPreparationDigest = ""
	if err := coordinator.writeMarker(marker); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(context.Background()); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("partially bound marker error = %v, want integrity violation", err)
	}
}

func validStopPreparation(t *testing.T) stagedBundleStopPreparation {
	t.Helper()
	clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	leaseID, err := ids.LeaseID()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := ids.TargetID()
	if err != nil {
		t.Fatal(err)
	}
	runID, err := ids.TargetRunID()
	if err != nil {
		t.Fatal(err)
	}
	bundleID, err := ids.ObservationBundleID()
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := ids.AgentWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := ids.CorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	result := persistedTargetRunResult{RunID: runID.String(), Outcome: ports.RunCompleted, StoppedAt: clock.Now()}
	return stagedBundleStopPreparation{
		Version:         bundleStopPreparationVersion,
		Reservation:     bundleReservation{Namespace: "stop_target_run", LeaseID: leaseID.String(), TargetID: targetID.String(), RunID: runID.String(), BundleID: bundleID.String(), IdempotencyKey: "stop", Signature: "signature"},
		Meta:            application.MutationMeta{IdempotencyKey: "stop", CorrelationID: correlationID.String(), AuthorizedPolicyReference: domain.NewDigest([]byte("policy")).String()},
		InitialRunState: domain.TargetRunRunning, InitialRevision: 1, TargetGeneration: 1,
		AgentWorkspaceID: agentID.String(), AgentGeneration: 1, RunCreatedAt: clock.Now(),
		RequiredCoverage: []string{ports.TargetLifecycleSignal}, Result: result, ObserverDigest: mustStoppedResultDigest(t, result),
	}
}

func mustStoppedResultDigest(t *testing.T, result persistedTargetRunResult) string {
	t.Helper()
	digest, err := stoppedResultDigest(result)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
