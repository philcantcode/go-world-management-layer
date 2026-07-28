package linuxcontainer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestIntrinsicLifecycleCompletesAndStopReplaysExactly(t *testing.T) {
	collectorCalled := false
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
		collectorCalled = true
		return fmt.Errorf("intrinsic coverage must not use external readiness")
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{IntrinsicSignalFamily})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	if collectorCalled {
		t.Fatal("intrinsic-only run consulted external collector readiness")
	}
	opened, err := driver.OpenTransport(ctx, authority.RunID)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("intrinsic transfer")
	transfer := transferPlanWithDigest(t, authority, domain.TargetOperationPush, "result.bin", 128, domain.NewDigest(content))
	if _, err := opened.PushFile(ctx, transfer, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunCompleted, ports.TargetRunFailureNone)
	wantKinds := []string{"target.run.started", "target.transport.opened", "target.transfer.opened", "target.transfer.succeeded", "target.run.stopped"}
	if len(result.Observations) != len(wantKinds) {
		t.Fatalf("lifecycle observations = %d, want %d", len(result.Observations), len(wantKinds))
	}
	for index, want := range wantKinds {
		if got := result.Observations[index].Kind; got != want {
			t.Fatalf("event[%d] kind = %q, want %q", index, got, want)
		}
	}
	replay, err := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil || !reflect.DeepEqual(replay, result) {
		t.Fatalf("idempotent stop replay = %#v, %v; want %#v", replay, err, result)
	}
	replay.Observations[0].Payload[0] = 'x'
	secondReplay, err := driver.StopRun(ctx, authority.RunID, ports.StopImmediate)
	if err != nil || !reflect.DeepEqual(secondReplay, result) {
		t.Fatalf("mutating a replay changed stored receipt: %#v, %v", secondReplay, err)
	}
}

func TestMixedLifecycleDelegatesExternalCoverageWithoutFakingProcessEvidence(t *testing.T) {
	factory := &manualTimerFactory{}
	var requested []ports.ObservationRequirement
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(_ context.Context, _ domain.TargetRunID, requirements []ports.ObservationRequirement) error {
		requested = append([]ports.ObservationRequirement(nil), requirements...)
		return nil
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{IntrinsicSignalFamily, "process"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(requested, []ports.ObservationRequirement{{SignalFamily: "process", Placement: domain.CollectorPlacementHost, MinimumLevel: domain.CoverageLevelComplete, Required: true}}) {
		t.Fatalf("external readiness families = %#v", requested)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunCompleted, ports.TargetRunFailureNone)
	for _, observation := range result.Observations {
		if observation.Kind == "process" {
			t.Fatalf("driver manufactured process evidence: %#v", observation)
		}
	}
}

func TestUnsupportedExternalReadinessFailureLeavesRunNeverStarted(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(_ context.Context, _ domain.TargetRunID, requirements []ports.ObservationRequirement) error {
		if !reflect.DeepEqual(requirements, []ports.ObservationRequirement{{SignalFamily: "unsupported.signal", Placement: domain.CollectorPlacementHost, MinimumLevel: domain.CoverageLevelComplete, Required: true}}) {
			t.Fatalf("readiness requirements = %#v", requirements)
		}
		return fmt.Errorf("collector unavailable")
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{IntrinsicSignalFamily, "unsupported.signal"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("unsupported readiness error = %v", err)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopImmediate)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunFailed, ports.TargetRunFailureNeverStarted)
	if got := result.Observations[len(result.Observations)-1].Kind; got != "target.run.never-started" {
		t.Fatalf("never-started observation = %q", got)
	}
	if factory.Last() != nil {
		t.Fatal("failed readiness armed a run timer")
	}
}

func TestIntrinsicLifecycleTimeoutPreservesCompleteCoverageAndFailsRun(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
		return fmt.Errorf("unexpected external readiness")
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{IntrinsicSignalFamily})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	factory.Last().Fire()
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunFailed, ports.TargetRunFailureDurationExceeded)
	if result.Observations[len(result.Observations)-1].Kind != "target.run.duration-exceeded" {
		t.Fatalf("intrinsic timeout result = %#v", result)
	}
}

func TestStartRunRejectsMaterialProjectionDriftBeforeActivation(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
		return nil
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{IntrinsicSignalFamily})
	record := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	materialRoot := record.plan.materialRoot()
	if err := os.Chmod(materialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(materialRoot, "unplanned.bin"), []byte("drift"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("material drift start error = %v", err)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunFailed, ports.TargetRunFailureNeverStarted)
	if factory.Last() != nil {
		t.Fatal("material drift armed a run timer")
	}
}

func assertStopReceipt(t *testing.T, result ports.TargetRunStopReceipt, outcome ports.RunOutcome, failure ports.TargetRunFailureKind) {
	t.Helper()
	if result.Outcome != outcome || result.FailureKind != failure || result.Validate() != nil {
		t.Fatalf("target stop receipt = %#v", result)
	}
}

func setLifecycleCoverage(driver *Driver, runID domain.TargetRunID, families []string) {
	run := driver.runs[runID.String()]
	run.plan.RequiredCoverage = append([]string(nil), families...)
	run.prepared.RequiredCoverage = append([]string(nil), families...)
	run.plan.Collectors = nil
	for index, family := range families {
		if SupportsIntrinsicCoverage(family) {
			continue
		}
		run.plan.Collectors = append(run.plan.Collectors, ports.CollectorSpec{
			Name:    fmt.Sprintf("test-collector-%d", index),
			Adapter: "test",
			Version: "1",
			ConfigurationDigest: domain.NewDigest(
				[]byte("test-collector/" + family),
			),
			MaximumBytes: 1024,
			Requirement: ports.ObservationRequirement{
				SignalFamily: family,
				Placement:    domain.CollectorPlacementHost,
				MinimumLevel: domain.CoverageLevelComplete,
				Required:     true,
			},
		})
	}
}

func TestRunDeadlineFailsRunAndRevokesTransport(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	transport := &targetTransport{root: driver.targets[targetKey(authority.TargetID, authority.Generation)].plan.writableRoot(), authority: authority}
	driver.runs[authority.RunID.String()].transports[transport] = struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	content := []byte("deadline snapshot")
	if err := os.WriteFile(filepath.Join(transport.root, "deadline.bin"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	pull, err := transport.PullFile(ctx, transferPlanWithDigest(t, authority, domain.TargetOperationPull, "deadline.bin", 64, domain.NewDigest(content)))
	if err != nil {
		t.Fatal(err)
	}
	timer := factory.Last()
	if timer == nil || factory.Duration() != 3*time.Second {
		t.Fatalf("deadline timer = %#v / %s", timer, factory.Duration())
	}
	timer.Fire()
	if err := transport.requireOpen(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("expired transport remained open: %v", err)
	}
	if _, err := io.ReadAll(pull); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("expired transport left a pull reader usable: %v", err)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunFailed, ports.TargetRunFailureDurationExceeded)
	if _, err := driver.OpenTransport(ctx, authority.RunID); err == nil {
		t.Fatal("expired run opened a new transport")
	}
}

func TestProbeFingerprintDistinguishesPtraceAuthority(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: "runc",
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "visibility-first",
	}
	probe := func(allowPtrace bool) domain.CapabilityFingerprint {
		driver, err := New(Config{
			Build:      BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target", AllowPtrace: allowPtrace},
			Runtime:    ptraceProbeRuntime{noopRuntime{}},
			Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fingerprint, err := driver.Probe(ctx, template)
		if err != nil {
			t.Fatal(err)
		}
		visibility, found := fingerprint.Capability("target.visibility-first")
		if !found || visibility.Constraints()["ptrace"] != fmt.Sprint(allowPtrace) {
			t.Fatalf("ptrace=%t capability = %#v", allowPtrace, visibility.Constraints())
		}
		return fingerprint
	}
	without := probe(false)
	with := probe(true)
	if without.Digest() == with.Digest() {
		t.Fatal("ptrace authority did not change the capability fingerprint")
	}
}

type ptraceProbeRuntime struct{ noopRuntime }

func (ptraceProbeRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{Version: "29.0", APIVersion: "1.52", CgroupVersion: "2", OSType: "linux"}, nil
}

func TestManualStopCancelsDeadline(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, ports.RunCompleted, ports.TargetRunFailureNone)
	timer := factory.Last()
	if timer == nil || !timer.Stopped() {
		t.Fatal("manual stop did not cancel the run deadline")
	}
	timer.Fire()
	replay, err := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil || !reflect.DeepEqual(replay, result) {
		t.Fatalf("canceled timer changed result: %#v, %v", replay, err)
	}
}

func TestSynchronousDeadlineCallbackDoesNotDeadlockStart(t *testing.T) {
	after := func(_ time.Duration, callback func()) RunTimer {
		timer := &manualRunTimer{callback: callback}
		timer.Fire()
		return timer
	}
	driver, authority := lifecycleTestDriver(t, after, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- driver.StartRun(ctx, authority.RunID) }()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("synchronous deadline callback deadlocked StartRun")
	}
	result, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil || result.Outcome != ports.RunFailed || result.FailureKind != ports.TargetRunFailureDurationExceeded {
		t.Fatalf("synchronously expired result = %#v, %v", result, err)
	}
}

func TestStopWhileCollectorsWaitPreventsLateStart(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(ctx context.Context, _ domain.TargetRunID, _ []ports.ObservationRequirement) error {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}))
	setLifecycleCoverage(driver, authority.RunID, []string{"process"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	startResult := make(chan error, 1)
	go func() { startResult <- driver.StartRun(ctx, authority.RunID) }()
	<-entered
	stopped, err := driver.StopRun(ctx, authority.RunID, ports.StopImmediate)
	if err != nil || stopped.Outcome != ports.RunFailed || stopped.FailureKind != ports.TargetRunFailureNeverStarted {
		t.Fatalf("stop while preparing = %#v, %v", stopped, err)
	}
	close(release)
	if err := <-startResult; !domain.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("late start error = %v", err)
	}
	if factory.Last() != nil {
		t.Fatal("late start armed a deadline after the run was stopped")
	}
}

func TestQuarantineConfirmsContainmentRevokesAccessAndPreservesState(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	driver.afterFunc = factory.AfterFunc
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	record := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	transport := &targetTransport{root: record.plan.writableRoot(), authority: authority}
	driver.runs[authority.RunID.String()].transports[transport] = struct{}{}
	plan := ports.TargetQuarantinePlan{
		IdempotencyKey: "quarantine-linux", Target: ports.TargetRef{ID: authority.TargetID, Generation: authority.Generation},
		Reason: "contain suspicious workload",
	}
	evidence, err := driver.Quarantine(ctx, plan)
	if err != nil || evidence.Validate(plan.Target) != nil {
		t.Fatalf("Quarantine() = %#v, %v", evidence, err)
	}
	if err := transport.requireOpen(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("quarantine left transport open: %v", err)
	}
	if timer := factory.Last(); timer == nil || !timer.Stopped() {
		t.Fatal("quarantine did not cancel the run deadline")
	}
	if _, err := os.Stat(record.plan.TargetDirectory); err != nil {
		t.Fatalf("quarantine removed retained state: %v", err)
	}
	stored := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	if stored.status.State != domain.TargetGenerationQuarantined || stored.status.Ready || !driver.runs[authority.RunID.String()].quarantined {
		t.Fatalf("contained driver state is incomplete: %#v / %#v", stored.status, driver.runs[authority.RunID.String()])
	}
	actions := runtime.Actions()
	replay, err := driver.Quarantine(ctx, plan)
	if err != nil || replay != evidence || len(runtime.Actions()) != len(actions) {
		t.Fatalf("quarantine replay = %#v, %v; actions %v -> %v", replay, err, actions, runtime.Actions())
	}
	conflict := plan
	conflict.Reason = "different containment request"
	if _, err := driver.Quarantine(ctx, conflict); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("quarantine idempotency conflict = %v", err)
	}
}

func TestQuarantineDoesNotAdvanceDriverStateWithoutBackendProof(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan), unconfirmedContainment: true}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := ports.TargetQuarantinePlan{IdempotencyKey: "quarantine-unconfirmed", Target: ports.TargetRef{ID: authority.TargetID, Generation: authority.Generation}, Reason: "test failure"}
	if _, err := driver.Quarantine(ctx, plan); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("unconfirmed containment error = %v", err)
	}
	record := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	if record.quarantine != nil || record.status.State == domain.TargetGenerationQuarantined || driver.runs[authority.RunID.String()].quarantined {
		t.Fatal("unconfirmed containment changed authoritative driver state")
	}
}

func TestResetProvesReplacementBeforeStoppingPreviousAndCleansStoppedRuns(t *testing.T) {
	runtime := &recordingRuntime{failCreateGeneration: 2, plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	run := driver.runs[authority.RunID.String()]
	run.stopped = true
	run.result = &ports.TargetRunStopReceipt{RunID: authority.RunID, Outcome: ports.RunFailed, FailureKind: ports.TargetRunFailureNeverStarted, StoppedAt: time.Unix(20, 0).UTC()}
	reset := resetPlan(t, authority, "reset-retry")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.Reset(ctx, authority.TargetID, reset); err == nil {
		t.Fatal("replacement creation failure was accepted")
	}
	if runtime.HasAction("stop:runtime-1") || runtime.HasAction("remove:runtime-1") {
		t.Fatalf("previous runtime was destroyed before replacement readiness: %v", runtime.Actions())
	}
	if _, found := driver.targets[targetKey(authority.TargetID, authority.Generation)]; !found {
		t.Fatal("failed reset removed previous generation bookkeeping")
	}

	runtime.failCreateGeneration = 0
	result, err := driver.Reset(ctx, authority.TargetID, reset)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status.Generation != 2 || !runtime.ActionBefore("inspect:runtime-2", "stop:runtime-1") {
		t.Fatalf("reset did not prove replacement first: %#v / %v", result, runtime.Actions())
	}
	if _, found := driver.runs[authority.RunID.String()]; found {
		t.Fatal("successful reset retained a previous-generation run")
	}
	if _, found := driver.idempotency[run.plan.IdempotencyKey]; found {
		t.Fatal("successful reset retained previous-run idempotency bookkeeping")
	}
}

func TestResetRejectsPreparedUnstoppedRun(t *testing.T) {
	driver, authority := lifecycleTestDriverWithRuntime(t, &recordingRuntime{plans: make(map[string]ContainerPlan)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.Reset(ctx, authority.TargetID, resetPlan(t, authority, "reset-prepared")); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("prepared run reset error = %v", err)
	}
}

func TestResetReplaysCommittedCleanupFailureWithoutRepeatingDestruction(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	run := driver.runs[authority.RunID.String()]
	run.stopped = true
	run.result = &ports.TargetRunStopReceipt{RunID: authority.RunID, Outcome: ports.RunFailed, FailureKind: ports.TargetRunFailureNeverStarted, StoppedAt: time.Unix(20, 0).UTC()}
	previousDirectory := driver.targets[targetKey(authority.TargetID, authority.Generation)].plan.TargetDirectory
	// Materialization seals the managed tree; restore write bits before Replace.
	if err := filepath.WalkDir(previousDirectory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return os.Chmod(path, 0o700)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(previousDirectory); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previousDirectory, []byte("tampered target directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan := resetPlan(t, authority, "reset-committed-cleanup-failure")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, firstErr := driver.Reset(ctx, authority.TargetID, plan)
	if !domain.IsCode(firstErr, domain.CodeUnavailable) || result.Status.Generation != plan.NextGeneration {
		t.Fatalf("committed cleanup result = %#v, %v", result, firstErr)
	}
	actions := runtime.Actions()
	replay, replayErr := driver.Reset(ctx, authority.TargetID, plan)
	if replayErr != firstErr || replay.Status.RuntimeID != result.Status.RuntimeID || replay.Status.Generation != result.Status.Generation {
		t.Fatalf("reset replay = %#v, %v; want %#v, %v", replay, replayErr, result, firstErr)
	}
	if got := runtime.Actions(); len(got) != len(actions) {
		t.Fatalf("reset replay repeated destructive runtime work: before=%v after=%v", actions, got)
	}
}

func lifecycleTestDriver(t *testing.T, after func(time.Duration, func()) RunTimer, collectors CollectorReadiness) (*Driver, RunAuthority) {
	t.Helper()
	driver, authority := lifecycleTestDriverWithRuntime(t, &recordingRuntime{plans: make(map[string]ContainerPlan)})
	driver.afterFunc = after
	driver.collectors = collectors
	return driver, authority
}

func lifecycleTestDriverWithRuntime(t *testing.T, runtime Runtime) (*Driver, RunAuthority) {
	t.Helper()
	root := writableTempDir(t)
	lease, _ := domain.NewLeaseID()
	session, _ := domain.NewResearchSessionID()
	target, _ := domain.NewTargetID()
	agent, _ := domain.NewAgentWorkspaceID()
	runID, _ := domain.NewTargetRunID()
	createdAt := time.Unix(5, 0).UTC()
	targetModel, err := domain.NewTarget(target, session, domain.TargetLinuxContainer, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("lifecycle input")
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: "artifact://linux/lifecycle-input", Digest: domain.NewDigest(content), Size: int64(len(content)), Role: "target-input", Sensitivity: domain.SensitivityInternal})
	if err != nil {
		t.Fatal(err)
	}
	material := []ports.TargetMaterialPlan{{Artifact: artifact, LogicalPath: "input.bin", Mode: 0o600, Content: lifecycleContent(content)}}
	materializationDigest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	runModel, err := domain.NewTargetRun(domain.TargetRunSpec{ID: runID, LeaseID: lease, TargetID: target, TargetGeneration: 1, AgentWorkspaceID: agent, AgentGeneration: 1, MaterializationDigest: materializationDigest, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	plan := validLifecycleContainerPlan(t, root, lease, target, 1)
	// Production targets hand off to 65532:65532 and require root on Linux.
	// Lifecycle unit tests only need a consistent identity the process can own.
	if goruntime.GOOS == "linux" && os.Geteuid() != 0 {
		plan.User = fmt.Sprintf("%d:%d", os.Geteuid(), os.Getegid())
	}
	if err := prepareTargetDirectories(root, plan); err != nil {
		t.Fatal(err)
	}
	if err := materializeTarget(context.Background(), root, plan, material); err != nil {
		t.Fatal(err)
	}
	if recording, ok := runtime.(*recordingRuntime); ok {
		recording.mu.Lock()
		recording.plans["runtime-1"] = plan
		recording.mu.Unlock()
	}
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 1, RunID: runID}
	return &Driver{
		build: BuildConfig{TargetRoot: root, ImageRepository: "world-target"}, runtime: runtime,
		collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
		now:        func() time.Time { return time.Unix(30, 0).UTC() },
		afterFunc:  func(time.Duration, func()) RunTimer { return &manualRunTimer{} },
		targets:    map[string]targetRecord{targetKey(target, 1): {input: ports.TargetPlan{Target: targetModel}, plan: plan, runtimeID: "runtime-1"}},
		runs: map[string]*runRecord{runID.String(): {
			plan: ports.TargetRunPlan{IdempotencyKey: "run-key", Run: runModel, RequiredCoverage: []string{"process"}, Material: material, MaximumDuration: 3 * time.Second}, authority: authority,
			prepared: ports.PreparedTargetRun{
				RunID: runID, TargetID: target, TargetGeneration: 1, MaterializationDigest: materializationDigest,
				RequiredCoverage: []string{"process"}, Attachment: ports.ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: "runtime-1"},
				PreparedAt: time.Unix(10, 0).UTC(),
			},
			transports: make(map[*targetTransport]struct{}),
		}},
		idempotency: map[string]string{"run-key": runID.String()}, resetResults: make(map[string]resetOutcome), materialized: make(map[string]*materializationState),
	}, authority
}

type lifecycleContent []byte

func (s lifecycleContent) Digest() domain.Digest { return domain.NewDigest(s) }
func (s lifecycleContent) Size() int64           { return int64(len(s)) }
func (s lifecycleContent) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s)), nil
}

func validLifecycleContainerPlan(t *testing.T, root string, lease domain.LeaseID, target domain.TargetID, generation domain.TargetGeneration) ContainerPlan {
	t.Helper()
	directory := filepath.Join(root, target.String(), "generations", fmt.Sprint(generation))
	policy := domain.NewDigest([]byte("policy"))
	capability := domain.NewDigest([]byte("capability"))
	plan := ContainerPlan{
		Name: targetContainerName(target, generation), LeaseID: lease, TargetID: target, Generation: generation,
		Image: "world-target@" + domain.NewDigest([]byte("image")).String(), Runtime: dockercli.RuncRuntime, TargetDirectory: directory,
		PolicyDigest: policy, CapabilityDigest: capability, Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, PIDs: 64},
		User: defaultTargetUser, ReadOnlyRoot: true, NoNewPrivileges: true, SeccompProfile: dockercli.RuntimeDefaultSeccompProfile,
		Labels: map[string]string{"world.role": targetRoleLabel, "world.lease": lease.String(), "world.target": target.String(), "world.target-generation": fmt.Sprint(generation), "world.policy-digest": policy.String(), "world.capability-digest": capability.String()},
	}
	plan.MountSources = []string{plan.writableRoot(), plan.materialRoot()}
	if err := setPlanDigest(&plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func resetPlan(t *testing.T, authority RunAuthority, key string) ports.ResetPlan {
	t.Helper()
	return ports.ResetPlan{IdempotencyKey: key, LeaseID: authority.LeaseID, Previous: ports.TargetRef{ID: authority.TargetID, Generation: authority.Generation}, NextGeneration: authority.Generation + 1, Mode: ports.ResetRecreate}
}

type manualTimerFactory struct {
	mu       sync.Mutex
	duration time.Duration
	last     *manualRunTimer
}

func (f *manualTimerFactory) AfterFunc(duration time.Duration, callback func()) RunTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.duration = duration
	f.last = &manualRunTimer{callback: callback}
	return f.last
}

func (f *manualTimerFactory) Last() *manualRunTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

func (f *manualTimerFactory) Duration() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.duration
}

type manualRunTimer struct {
	mu       sync.Mutex
	callback func()
	stopped  bool
	fired    bool
}

func (t *manualRunTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (t *manualRunTimer) Fire() {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return
	}
	t.fired = true
	callback := t.callback
	t.mu.Unlock()
	callback()
}

func (t *manualRunTimer) Stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

type recordingRuntime struct {
	mu                     sync.Mutex
	failCreateGeneration   domain.TargetGeneration
	failContainment        bool
	unconfirmedContainment bool
	plans                  map[string]ContainerPlan
	actions                []string
}

func (r *recordingRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{}, nil
}

func (r *recordingRuntime) Create(_ context.Context, plan ContainerPlan) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, "create:"+fmt.Sprint(plan.Generation))
	if plan.Generation == r.failCreateGeneration {
		return "", fmt.Errorf("injected create failure")
	}
	id := "runtime-" + fmt.Sprint(plan.Generation)
	r.plans[id] = plan
	return id, nil
}

func (r *recordingRuntime) Start(_ context.Context, id string) error {
	r.record("start:" + id)
	return nil
}

func (r *recordingRuntime) Inspect(_ context.Context, id string) (RuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, "inspect:"+id)
	plan, found := r.plans[id]
	if !found {
		return RuntimeState{}, fmt.Errorf("runtime %s not found", id)
	}
	return RuntimeState{ID: id, Name: plan.Name, Running: true, Labels: cloneStrings(plan.Labels), CgroupID: "cgroup/" + id, Configuration: expectedTargetConfiguration(plan)}, nil
}

func (r *recordingRuntime) Stop(_ context.Context, id string, _ ports.StopMode) error {
	r.record("stop:" + id)
	return nil
}

func (r *recordingRuntime) Quarantine(_ context.Context, id string) (RuntimeContainmentEvidence, error) {
	r.record("quarantine:" + id)
	if r.failContainment {
		return RuntimeContainmentEvidence{}, fmt.Errorf("injected containment failure")
	}
	return RuntimeContainmentEvidence{
		RuntimeID: id, ExecutionStopped: true, NetworkUnreachable: !r.unconfirmedContainment,
		StatePreserved: true, ObservedAt: time.Unix(40, 0).UTC(),
	}, nil
}

func (r *recordingRuntime) Remove(_ context.Context, id string) error {
	r.record("remove:" + id)
	return nil
}

func (r *recordingRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	return nil, fmt.Errorf("not used")
}

func (r *recordingRuntime) record(value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, value)
}

func (r *recordingRuntime) Actions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.actions...)
}

func (r *recordingRuntime) HasAction(expected string) bool {
	for _, action := range r.Actions() {
		if action == expected {
			return true
		}
	}
	return false
}

func (r *recordingRuntime) ActionBefore(first, second string) bool {
	firstIndex, secondIndex := -1, -1
	for index, action := range r.Actions() {
		if action == first && firstIndex < 0 {
			firstIndex = index
		}
		if action == second && secondIndex < 0 {
			secondIndex = index
		}
	}
	return firstIndex >= 0 && secondIndex > firstIndex
}

var _ Runtime = (*recordingRuntime)(nil)
var _ RuntimeContainment = (*recordingRuntime)(nil)
