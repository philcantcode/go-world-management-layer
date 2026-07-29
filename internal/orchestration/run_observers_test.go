package orchestration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestRunObserverCoordinatorMergesExternalAndTargetEvidenceAuthoritatively(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	fingerprint := observerTestFingerprint(t)
	driver := testkit.NewFakeObserverDriver(fingerprint, clock, nil, nil)
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now,
		StateRoot: stateRoot, CleanupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	readiness, err := NewLedgerCollectorReadiness(observations)
	if err != nil {
		t.Fatal(err)
	}
	if err := readiness.AwaitReady(ctx, input.Plan.Run.ID(), []ports.ObservationRequirement{input.Plan.Collectors[0].Requirement}); err != nil {
		t.Fatalf("authoritative readiness: %v", err)
	}

	clock.Advance(2 * time.Second)
	receipt := observerTestReceipt(t, input, clock.Now())
	evidence, err := coordinator.Finalize(ctx, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.FirstCursor == 0 || evidence.LastCursor < evidence.FirstCursor || len(evidence.Events) < 4 || len(evidence.Artifacts) < 2 || len(evidence.Failures) != 0 {
		t.Fatalf("incomplete merged evidence: %#v", evidence)
	}
	wantedFamilies := map[string]bool{"process": false, ports.TargetLifecycleSignal: false}
	for _, coverage := range evidence.Coverage {
		spec := coverage.Spec()
		if _, wanted := wantedFamilies[spec.SignalFamily]; wanted && spec.Required && spec.Status == domain.CoverageAvailable && spec.Level == domain.CoverageLevelComplete && len(spec.Gaps) == 0 {
			wantedFamilies[spec.SignalFamily] = true
		}
	}
	for family, available := range wantedFamilies {
		if !available {
			t.Fatalf("required family %q did not have authoritative final coverage: %#v", family, evidence.Coverage)
		}
	}
	result, err := assembleTargetRunResult(receipt, evidence)
	if err != nil || result.Outcome != ports.RunCompleted || len(result.NormalizedEvents) != len(evidence.Events) {
		t.Fatalf("assembled result = %#v, %v", result, err)
	}
	if err := coordinator.Commit(ctx, receipt.RunID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Commit(ctx, receipt.RunID); err != nil {
		t.Fatalf("commit replay: %v", err)
	}
	journalDirectory := filepath.Join(stateRoot, "journals")
	journalNames, err := os.ReadDir(journalDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(journalNames) != 1 {
		t.Fatalf("durable journal files after repeated checkpoints = %d, want 1", len(journalNames))
	}
	planDigest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		t.Fatal(err)
	}
	orphan := observerEvidenceJournal{
		Version: observerEvidenceJournalVersion, RunID: receipt.RunID.String(), PlanDigest: planDigest.String(),
		Completed: []observerJournalCompletion{},
	}
	orphanBytes, err := encodeObserverJournal(orphan)
	if err != nil {
		t.Fatal(err)
	}
	orphanDigest := domain.NewDigest(orphanBytes)
	orphanName := receipt.RunID.String() + "-" + strings.TrimPrefix(orphanDigest.String(), "sha256:") + ".json"
	if orphanName == journalNames[0].Name() {
		t.Fatal("test orphan unexpectedly matches the referenced terminal checkpoint")
	}
	orphanPath := filepath.Join(journalDirectory, orphanName)
	if err := os.WriteFile(orphanPath, orphanBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now,
		StateRoot: stateRoot, CleanupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatalf("clean committed restart: %v", err)
	}
	if _, err := os.Lstat(orphanPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced crash-window journal remains after startup reconciliation: %v", err)
	}
}

func TestRunObserverCoordinatorRetriesUntilPhysicalTeardownIsConfirmed(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	faults := testkit.NewFaultInjector()
	tracker := testkit.NewOwnershipTracker()
	driver := testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, faults, tracker)
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now,
		StateRoot: stateRoot, CleanupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	receipt := observerTestReceipt(t, input, clock.Now())
	faults.FailNext("observer.stop.before", errors.New("physical stop unavailable"))
	if _, err := coordinator.Finalize(ctx, receipt); err == nil {
		t.Fatal("finalization succeeded without confirmed collector teardown")
	}
	if len(tracker.Snapshot()) == 0 {
		t.Fatal("failed stop incorrectly released physical collector ownership")
	}
	restarted, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now,
		StateRoot: stateRoot, CleanupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(ctx); err == nil {
		t.Fatal("startup adopted an interrupted physical collector lifecycle")
	}
	evidence, err := coordinator.Finalize(ctx, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if len(tracker.Snapshot()) != 0 {
		t.Fatalf("confirmed retry leaked collector ownership: %#v", tracker.Snapshot())
	}
	result, err := assembleTargetRunResult(receipt, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != ports.RunFailed || len(evidence.Failures) == 0 {
		t.Fatalf("lost stop interval was not retained as failed evidence: %#v", evidence)
	}
}

func TestRunObserverCoordinatorPersistsExactBindingsBeforeCollectorStart(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_050_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	driver := &crashSafeObserverDriver{inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, nil, nil)}
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	inspected := false
	driver.startHook = func(plan ports.CollectorPlan) error {
		markers, err := coordinator.loadMarkers()
		if err != nil {
			return err
		}
		if len(markers) != 1 || markers[0].Phase != "starting" || len(markers[0].Collectors) != 1 || markers[0].Collectors[0].StartCommitted || markers[0].Collectors[0].Plan.CollectorID != plan.CollectorID || markers[0].Collectors[0].Plan.StartedAt != plan.StartedAt || markers[0].IntrinsicID == "" {
			return errors.New("exact uncommitted bindings were not durable before observer start")
		}
		inspected = true
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	if !inspected {
		t.Fatal("collector start did not inspect its durable pre-start marker")
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 1 || !markers[0].Collectors[0].StartCommitted {
		t.Fatalf("post-start committed binding = %#v, %v", markers, err)
	}
}

func TestRunObserverCoordinatorRemovesInterruptedAtomicMarkerStaging(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_060_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(stateRoot, "runs", ".staging-interrupted-write")
	if err := os.WriteFile(staging, []byte(`{"version":3,"run_id":"truncated`), 0o600); err != nil {
		t.Fatal(err)
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 0 {
		t.Fatalf("marker staging reconciliation = %#v, %v", markers, err)
	}
	if _, err := os.Lstat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted atomic marker staging remains: %v", err)
	}
}

func TestRunObserverCoordinatorRejectsUnclaimedJournalInsteadOfDeletingIt(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_065_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(stateRoot, "journals", "foreign.json")
	if err := os.WriteFile(foreign, []byte(`{"not":"a journal"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Reconcile(context.Background()); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("unclaimed journal reconciliation error = %v", err)
	}
	if _, err := os.Lstat(foreign); err != nil {
		t.Fatalf("unclaimed journal was deleted instead of failing closed: %v", err)
	}
}

func TestRunObserverCoordinatorRejectsTamperedPersistedCollectorBinding(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_070_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	driver := &crashSafeObserverDriver{inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, nil, nil)}
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	input.TargetKind = domain.TargetAndroidVirtualDevice
	input.Prepared.Attachment = ports.ObservationAttachment{
		TargetKind: domain.TargetAndroidVirtualDevice, RuntimeID: "world-android-generation-2",
		ADBDevice: ports.ADBDeviceSelection{Server: ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5041}, Serial: "emulator-5578"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 1 {
		t.Fatalf("load marker = %#v, %v", markers, err)
	}
	markers[0].Collectors[0].Plan.Attachment.ADBDevice.Serial = "emulator-5580"
	if err := coordinator.writeMarker(markers[0]); err != nil {
		t.Fatal(err)
	}
	digest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcilePersistedRuns(ctx, []PersistedRunObserverBinding{{RunID: input.Plan.Run.ID(), PlanDigest: digest, State: domain.TargetRunRunning}}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverInterrupted(ctx, input, domain.TargetRunRunning); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("tampered collector binding recovery error = %v", err)
	}
}

func TestObserverStartSignatureBindsExactADBObservationAuthority(t *testing.T) {
	input := observerTestStart(t, time.Unix(1_800_075_000, 0).UTC())
	input.TargetKind = domain.TargetAndroidVirtualDevice
	input.Prepared.Attachment = ports.ObservationAttachment{
		TargetKind: domain.TargetAndroidVirtualDevice, RuntimeID: "world-android-generation-2",
		ADBDevice: ports.ADBDeviceSelection{Server: ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5041}, Serial: "emulator-5578"},
	}
	if err := validateRunObserverStart(input); err != nil {
		t.Fatal(err)
	}
	base, err := observerStartSignature(input)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RunObserverStart){
		"server": func(value *RunObserverStart) { value.Prepared.Attachment.ADBDevice.Server.Port = 5043 },
		"serial": func(value *RunObserverStart) { value.Prepared.Attachment.ADBDevice.Serial = "emulator-5580" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := input
			mutate(&changed)
			signature, err := observerStartSignature(changed)
			if err != nil {
				t.Fatal(err)
			}
			if signature == base {
				t.Fatalf("%s mutation did not change observer signature %s", name, signature)
			}
		})
	}
}

func TestRunObserverCoordinatorRecoversInterruptedRunWithoutResumingCollectorsOrDuration(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	driver := &crashSafeObserverDriver{inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, nil, nil)}
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	digest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		t.Fatal(err)
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 1 || markers[0].Version != observerStateVersion || markers[0].PlanDigest != digest.String() || len(markers[0].Collectors) != 1 || !markers[0].Collectors[0].StartCommitted || markers[0].IntrinsicID == "" || markers[0].IntrinsicStartedAt.IsZero() {
		t.Fatalf("persisted marker = %#v, %v", markers, err)
	}
	persistedCollector := markers[0].Collectors[0].Plan
	persistedIntrinsic := markers[0].IntrinsicID

	clock.Advance(10 * time.Second)
	restarted, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := PersistedRunObserverBinding{RunID: input.Plan.Run.ID(), PlanDigest: digest, State: domain.TargetRunRunning}
	if err := restarted.ReconcilePersistedRuns(ctx, []PersistedRunObserverBinding{binding}); err != nil {
		t.Fatal(err)
	}
	recovered := input
	recovered.Prepared.PreparedAt = clock.Now()
	if err := restarted.RecoverInterrupted(ctx, recovered, domain.TargetRunRunning); err != nil {
		t.Fatal(err)
	}
	if driver.starts != 1 || driver.cleanups != 1 {
		t.Fatalf("collector starts=%d cleanup proofs=%d, want 1 and 1", driver.starts, driver.cleanups)
	}
	record := restarted.records[input.Plan.Run.ID().String()]
	if record == nil || record.timer != nil || len(record.failures) != 2 {
		t.Fatalf("prepared-only recovered observer record = %#v", record)
	}
	if len(record.plans) != 1 || record.plans[0].CollectorID != persistedCollector.CollectorID || record.plans[0].StartedAt != persistedCollector.StartedAt || record.intrinsicID.String() != persistedIntrinsic || record.intrinsicStartedAt != markers[0].IntrinsicStartedAt {
		t.Fatalf("recovery did not retain exact persisted collector bindings: %#v", record)
	}
	for collectorID, started := range record.started {
		if started {
			t.Fatalf("recovery resumed collector %s", collectorID)
		}
	}
	clock.Advance(time.Second)
	receipt := observerNeverStartedReceipt(t, recovered, clock.Now())
	evidence, err := restarted.Finalize(ctx, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Failures) != 2 || len(evidence.Gaps) != 2 {
		t.Fatalf("control-plane-loss evidence = %#v", evidence)
	}
	for _, failure := range evidence.Failures {
		if !strings.Contains(failure.Reason, "control-plane loss") || strings.Contains(failure.Reason, "resumed successfully") {
			t.Fatalf("recovery failure reason = %q", failure.Reason)
		}
	}
	if err := restarted.Commit(ctx, receipt.RunID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunObserverCoordinatorRetainsRecoveredFinalizedOutputWithoutClaimingContinuity(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_100_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	driver := &crashSafeObserverDriver{inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, nil, nil)}
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}
	digest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		t.Fatal(err)
	}
	driver.reconcileFunc = func(request ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
		artifacts := recoveredTestArtifacts(t, request.Collectors[0].Plan.CollectorID, request.Collectors[0].Plan.MaximumBytes)
		return ports.InterruptedCollectorReconciliationReport{
			TargetRunID: request.TargetRunID,
			Outputs: []ports.InterruptedCollectorOutput{{
				CollectorID: request.Collectors[0].Plan.CollectorID, State: ports.InterruptedCollectorOutputFinalized,
				Artifacts: artifacts, CaptureLimitExceeded: true,
			}},
		}, nil
	}

	clock.Advance(time.Minute)
	restarted, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: ids, Clock: clock.Now, StateRoot: stateRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcilePersistedRuns(ctx, []PersistedRunObserverBinding{{RunID: input.Plan.Run.ID(), PlanDigest: digest, State: domain.TargetRunRunning}}); err != nil {
		t.Fatal(err)
	}
	recovered := input
	recovered.Prepared.PreparedAt = clock.Now()
	if err := restarted.RecoverInterrupted(ctx, recovered, domain.TargetRunRunning); err != nil {
		t.Fatal(err)
	}
	if len(driver.lastRecovery.Collectors) != 1 || driver.lastRecovery.Collectors[0].Plan.CollectorID.IsZero() {
		t.Fatalf("exact recovery request = %#v", driver.lastRecovery)
	}
	clock.Advance(time.Second)
	evidence, err := restarted.Finalize(ctx, observerNeverStartedReceipt(t, recovered, clock.Now()))
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range recoveredTestArtifacts(t, driver.lastRecovery.Collectors[0].Plan.CollectorID, driver.lastRecovery.Collectors[0].Plan.MaximumBytes) {
		found := false
		for _, artifact := range evidence.Artifacts {
			if artifact.Spec() == wanted.Spec() {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("recovered immutable artifact missing from evidence: %#v", evidence.Artifacts)
		}
	}
	for _, failure := range evidence.Failures {
		if failure.Family == "process" && !strings.Contains(failure.Reason, "byte-limit-truncated prefix") {
			t.Fatalf("recovered truncation boundary missing from failure: %#v", failure)
		}
	}
	for _, coverage := range evidence.Coverage {
		if coverage.Spec().SignalFamily == "process" && (coverage.Spec().Status != domain.CoverageLost || coverage.Level() != domain.CoverageLevelNone) {
			t.Fatalf("recovered output incorrectly claimed continuity: %#v", coverage.Spec())
		}
	}
}

func recoveredTestArtifacts(t *testing.T, collectorID domain.CollectorID, maximumBytes int64) []domain.ArtifactReference {
	t.Helper()
	result := make([]domain.ArtifactReference, 0, 2)
	for index, value := range []struct {
		role, content string
	}{{ports.CollectorStdoutArtifactRole, "recovered stdout"}, {ports.CollectorStderrArtifactRole, "recovered stderr"}} {
		content := []byte(value.content)
		digest := domain.NewDigest(content)
		size := int64(1)
		if index == 0 {
			size = maximumBytes - 1
		}
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
			Reference: "observer://collectors/" + collectorID.String() + "/" + strings.TrimPrefix(value.role, "collector.") + "/" + digest.String(),
			Digest:    digest, Size: size, Role: value.role, Sensitivity: domain.SensitivityInternal,
		})
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, artifact)
	}
	return result
}

func TestRunObserverCoordinatorRejectsForeignOrMismatchedMarkers(t *testing.T) {
	for _, test := range []struct {
		name     string
		bindings func(RunObserverStart, domain.Digest) []PersistedRunObserverBinding
	}{
		{name: "foreign", bindings: func(RunObserverStart, domain.Digest) []PersistedRunObserverBinding { return nil }},
		{name: "plan mismatch", bindings: func(input RunObserverStart, _ domain.Digest) []PersistedRunObserverBinding {
			return []PersistedRunObserverBinding{{RunID: input.Plan.Run.ID(), PlanDigest: domain.NewDigest([]byte("different-plan")), State: domain.TargetRunRunning}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
			observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer observations.Close()
			coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
				Driver: &crashSafeObserverDriver{inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, nil, nil)},
				Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now, StateRoot: filepath.Join(t.TempDir(), "state"),
			})
			if err != nil {
				t.Fatal(err)
			}
			input := observerTestStart(t, clock.Now())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := coordinator.Start(ctx, input); err != nil {
				t.Fatal(err)
			}
			digest, err := TargetRunProvisioningPlanDigest(input.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ReconcilePersistedRuns(ctx, test.bindings(input, digest)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("marker reconciliation error = %v", err)
			}
		})
	}
}

func observerNeverStartedReceipt(t *testing.T, input RunObserverStart, stoppedAt time.Time) ports.TargetRunStopReceipt {
	t.Helper()
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetRunStopReceipt{
		RunID: input.Plan.Run.ID(), Outcome: ports.RunFailed, FailureKind: ports.TargetRunFailureNeverStarted,
		StoppedAt: stoppedAt, TargetChanges: changes,
		Observations: []ports.TargetRunObservation{{Kind: "target.run.never-started", ObservedAt: stoppedAt, Payload: []byte(`{"control_plane_loss":true}`)}},
	}
}

type crashSafeObserverDriver struct {
	inner         ports.ObserverDriver
	starts        int
	cleanups      int
	lastRecovery  ports.InterruptedCollectorReconciliation
	startHook     func(ports.CollectorPlan) error
	reconcileFunc func(ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error)
}

func (d *crashSafeObserverDriver) Probe(ctx context.Context, requirement ports.ObservationRequirement) (domain.CapabilityFingerprint, error) {
	return d.inner.Probe(ctx, requirement)
}

func (d *crashSafeObserverDriver) Start(ctx context.Context, plan ports.CollectorPlan) (ports.Collector, error) {
	d.starts++
	if d.startHook != nil {
		if err := d.startHook(plan); err != nil {
			return ports.Collector{}, err
		}
	}
	return d.inner.Start(ctx, plan)
}

func (d *crashSafeObserverDriver) PrepareStop(ctx context.Context, id domain.CollectorID) error {
	return d.inner.PrepareStop(ctx, id)
}

func (d *crashSafeObserverDriver) CancelStopPreparation(ctx context.Context, id domain.CollectorID) error {
	return d.inner.CancelStopPreparation(ctx, id)
}

func (d *crashSafeObserverDriver) Stop(ctx context.Context, id domain.CollectorID) (ports.CollectorResult, error) {
	return d.inner.Stop(ctx, id)
}

func (d *crashSafeObserverDriver) Coverage(ctx context.Context, id domain.CollectorID) (domain.CollectorCoverage, error) {
	return d.inner.Coverage(ctx, id)
}

func (d *crashSafeObserverDriver) InterruptedCollectorCleanupGuaranteed() bool { return true }

func (d *crashSafeObserverDriver) ReconcileInterruptedCollectors(_ context.Context, request ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
	d.cleanups++
	d.lastRecovery = request
	if d.reconcileFunc != nil {
		return d.reconcileFunc(request)
	}
	report := ports.InterruptedCollectorReconciliationReport{TargetRunID: request.TargetRunID}
	for _, binding := range request.Collectors {
		report.Outputs = append(report.Outputs, ports.InterruptedCollectorOutput{CollectorID: binding.Plan.CollectorID, State: ports.InterruptedCollectorOutputAborted})
	}
	return report, nil
}

func observerTestReceipt(t *testing.T, input RunObserverStart, stoppedAt time.Time) ports.TargetRunStopReceipt {
	t.Helper()
	startedAt := input.Prepared.PreparedAt.Add(time.Second)
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetRunStopReceipt{
		RunID: input.Plan.Run.ID(), Outcome: ports.RunCompleted, FailureKind: ports.TargetRunFailureNone,
		StartedAt: startedAt, StoppedAt: stoppedAt, TargetChanges: changes,
		Observations: []ports.TargetRunObservation{
			{Kind: "target.run.started", ObservedAt: startedAt, Payload: []byte(`{"runtime":"exact"}`)},
			{Kind: "target.run.stopped", ObservedAt: stoppedAt, Payload: []byte(`{"cleanup":true}`)},
		},
	}
}

func observerTestStart(t *testing.T, now time.Time) RunObserverStart {
	t.Helper()
	sessionID, _ := domain.NewResearchSessionID()
	leaseID, _ := domain.NewLeaseID()
	agentID, _ := domain.NewAgentWorkspaceID()
	targetID, _ := domain.NewTargetID()
	runID, _ := domain.NewTargetRunID()
	content := []byte("specimen")
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "artifact://observer-test/specimen", Digest: domain.NewDigest(content), Size: int64(len(content)),
		Role: "specimen", Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	material := []ports.TargetMaterialPlan{{Artifact: artifact, LogicalPath: "bin/specimen", Mode: 0o555, Content: observerBytes(content)}}
	materialDigest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.NewTargetRun(domain.TargetRunSpec{
		ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration,
		AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		MaterializationDigest: materialDigest, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	requirement := ports.ObservationRequirement{
		SignalFamily: "process", Placement: domain.CollectorPlacementHost,
		MinimumLevel: domain.CoverageLevelComplete, Required: true,
	}
	plan := ports.TargetRunPlan{
		IdempotencyKey: "observer-run", Run: run,
		RequiredCoverage: []string{ports.TargetLifecycleSignal, requirement.SignalFamily},
		Collectors: []ports.CollectorSpec{{
			Name: "process-trace", Requirement: requirement, Adapter: "trace", Version: "v1",
			ConfigurationDigest: domain.NewDigest([]byte("trace-config")), MaximumBytes: 1 << 20,
		}},
		Material: material, MaximumDuration: time.Minute,
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	return RunObserverStart{
		Plan: plan,
		Prepared: ports.PreparedTargetRun{
			RunID: runID, TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration,
			MaterializationDigest: materialDigest, RequiredCoverage: append([]string(nil), plan.RequiredCoverage...),
			Attachment: ports.ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: "runtime-exact"}, PreparedAt: now,
		},
		TargetKind: domain.TargetLinuxContainer, ResearchSessionID: sessionID,
		PolicyDigest: domain.NewDigest([]byte("policy")), CapabilityFingerprintDigest: domain.NewDigest([]byte("capability")),
	}
}

func observerTestFingerprint(t *testing.T) domain.CapabilityFingerprint {
	t.Helper()
	capability, err := domain.NewCapability(domain.CapabilitySupported, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := domain.NewCapabilityFingerprint(map[string]domain.Capability{"observer.trace": capability}, map[string]string{"probe": "test"})
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

type observerBytes []byte

func (b observerBytes) Digest() domain.Digest { return domain.NewDigest(b) }
func (b observerBytes) Size() int64           { return int64(len(b)) }
func (b observerBytes) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b)), nil
}

var _ ports.ContentSource = observerBytes(nil)
