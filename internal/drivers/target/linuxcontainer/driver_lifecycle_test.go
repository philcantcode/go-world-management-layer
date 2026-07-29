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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestIntrinsicLifecycleCompletesAndStopReplaysExactly(t *testing.T) {
	collectorCalled := false
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
		collectorCalled = true
		return fmt.Errorf("intrinsic coverage must not use external readiness")
	}))
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
	// The timer is manually fired; this context only bounds cleanup. Keep a
	// generous wall-clock budget so repository-wide parallel package load does
	// not turn the assertion into an unrelated context-expiry failure.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	changes := result.TargetChanges.Entries()
	if len(changes) != 1 || changes[0].Kind() != domain.ChangeAdded || changes[0].Path() != "result.bin" || changes[0].Spec().AfterDigest != domain.NewDigest(content) {
		t.Fatalf("target changes = %#v", changes)
	}
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
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily, "process"})
	ctx := targetDeadline(t)
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

func TestStopRunEnforcesWritableTreeBoundsAndCanRetrySealing(t *testing.T) {
	driver, authority := lifecycleTestDriver(t, func(time.Duration, func()) RunTimer { return &manualRunTimer{} }, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	record := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	record.plan.Resources.StorageBytes = 4
	driver.targets[targetKey(authority.TargetID, authority.Generation)] = record
	ctx := targetDeadline(t)
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(record.plan.writableRoot(), "bounded.bin")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful); !domain.IsCode(err, domain.CodeResourceExhausted) {
		t.Fatalf("oversized writable tree error = %v", err)
	}
	if _, err := driver.OpenTransport(ctx, authority.RunID); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("run reopened authority while sealing was incomplete: %v", err)
	}
	if err := os.WriteFile(path, []byte("1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	changes := receipt.TargetChanges.Entries()
	if len(changes) != 1 || changes[0].Path() != "bounded.bin" || changes[0].Spec().AfterDigest != domain.NewDigest([]byte("1234")) {
		t.Fatalf("bounded retry changes = %#v", changes)
	}
}

func TestStopRunRejectsUnsafeWritableTree(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("creating a symlink requires a developer-mode privilege on Windows")
	}
	driver, authority := lifecycleTestDriver(t, func(time.Duration, func()) RunTimer { return &manualRunTimer{} }, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	ctx := targetDeadline(t)
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	root := driver.targets[targetKey(authority.TargetID, authority.Generation)].plan.writableRoot()
	unsafePath := filepath.Join(root, "redirect")
	if err := os.Symlink("outside", unsafePath); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, authority.RunID, ports.StopForce); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("unsafe writable tree error = %v", err)
	}
	if err := os.Remove(unsafePath); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, authority.RunID, ports.StopForce); err != nil {
		t.Fatalf("retry after removing unsafe entry: %v", err)
	}
}

func TestStopRunUsesStoppedContainerAsAuthoritativeTransportCleanupBoundary(t *testing.T) {
	driver, authority := lifecycleTestDriver(t, func(time.Duration, func()) RunTimer { return &manualRunTimer{} }, CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	flaky := &flakyCloseExecTransport{failures: 1}
	scoped := &targetTransport{execs: []ports.ExecTransport{flaky}}
	driver.runs[authority.RunID.String()].transports[scoped] = struct{}{}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, receipt, ports.RunCompleted, ports.TargetRunFailureNone)
	if flaky.closeCalls != 1 {
		t.Fatalf("transport close calls = %d, want 1", flaky.closeCalls)
	}
}

func TestRunExpiryStopsContainerBeforeBlockingTransportDrain(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := &blockingCloseExecTransport{entered: entered, release: release}
	driver.runs[authority.RunID.String()].transports[&targetTransport{execs: []ports.ExecTransport{blocking}}] = struct{}{}
	finished := make(chan struct{})
	go func() {
		driver.expireRun(authority.RunID)
		close(finished)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("expiry did not reach transport drain")
	}
	runningWhileDrainBlocked := runtime.IsRunning(testRuntimeID("runtime-1"))
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("expiry did not finish after transport drain was released")
	}
	if runningWhileDrainBlocked {
		t.Fatal("expiry drained a blocking transport before stopping the exact container")
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, receipt, ports.RunFailed, ports.TargetRunFailureDurationExceeded)
}

func TestStopRunEstablishesDurableStoppedContainerBoundary(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopImmediate)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != ports.RunCompleted || runtime.IsRunning(testRuntimeID("runtime-1")) {
		t.Fatalf("stopped receipt/runtime = %#v, running=%t", receipt, runtime.IsRunning(testRuntimeID("runtime-1")))
	}
	target := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	if target.status.State != domain.TargetGenerationResettable || target.status.Ready {
		t.Fatalf("stopped target status = %#v", target.status)
	}
	run := driver.runs[authority.RunID.String()]
	claim, err := requireTargetGenerationRunClaim(target.plan.TargetDirectory, run.plan)
	if err != nil {
		t.Fatal(err)
	}
	boundary, found, err := loadStoppedRunBoundary(run.directory, claim, target.runtimeID)
	if err != nil || !found || boundary.Mode != ports.StopImmediate || boundary.StoppedAt.After(receipt.StoppedAt) {
		t.Fatalf("stopped boundary = %#v, found=%t, err=%v", boundary, found, err)
	}
}

func TestStopRunRetriesAfterRuntimeReportsFailureAtProvenBoundary(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan), stopFailures: 1}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
	ctx := targetDeadline(t)
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, authority.RunID, ports.StopForce); !domain.IsCode(err, domain.CodeUnavailable) {
		t.Fatalf("first stop error = %v", err)
	}
	if runtime.IsRunning(testRuntimeID("runtime-1")) {
		t.Fatal("failed stop report left the proven container running")
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopGraceful)
	if err != nil || receipt.Outcome != ports.RunCompleted {
		t.Fatalf("retry stop = %#v, %v", receipt, err)
	}
	if calls := runtime.ActionCount("stop:" + testRuntimeID("runtime-1")); calls != 1 {
		t.Fatalf("physical stop calls = %d, want 1", calls)
	}
}

func TestPrepareRunDurableGenerationClaimSurvivesDriverStateLoss(t *testing.T) {
	driver, authority := lifecycleTestDriverWithRuntime(t, &recordingRuntime{plans: make(map[string]ContainerPlan)})
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
	first := driver.runs[authority.RunID.String()].plan
	driver.runs = make(map[string]*runRecord)
	driver.idempotency = make(map[string]string)
	driver.materialized = make(map[string]*materializationState)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if prepared, err := driver.PrepareRun(ctx, first); err != nil || prepared.RunID != authority.RunID {
		t.Fatalf("exact incomplete preparation recovery = %#v, %v", prepared, err)
	}
	driver.runs = make(map[string]*runRecord)
	driver.idempotency = make(map[string]string)
	second := replacementLifecycleRunPlan(t, first, "different-run-key")
	if _, err := driver.PrepareRun(ctx, second); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("second same-generation run error = %v", err)
	}
}

func TestDirectDriverIdempotencyRejectsChangedTargetAndRunPlans(t *testing.T) {
	driver, authority := lifecycleTestDriverWithRuntime(t, &recordingRuntime{plans: make(map[string]ContainerPlan)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	target := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	driver.idempotency[target.input.IdempotencyKey] = targetKey(authority.TargetID, authority.Generation)
	targetCases := map[string]func(ports.TargetPlan) ports.TargetPlan{
		"physical resources": func(plan ports.TargetPlan) ports.TargetPlan {
			plan.Resources.CPUMilli++
			return plan
		},
		"template semantic name": func(plan ports.TargetPlan) ports.TargetPlan {
			plan.Template.Name += "-changed"
			return plan
		},
		"generation creation time": func(plan ports.TargetPlan) ports.TargetPlan {
			spec := plan.Generation.Spec()
			spec.CreatedAt = spec.CreatedAt.Add(time.Second)
			generation, err := domain.NewTargetGeneration(spec)
			if err != nil {
				t.Fatal(err)
			}
			plan.Generation = generation
			return plan
		},
		"target model time": func(plan ports.TargetPlan) ports.TargetPlan {
			target, err := domain.NewTarget(plan.Target.ID(), plan.Target.ResearchSessionID(), plan.Target.Kind(), plan.Target.CurrentGeneration(), plan.Target.UpdatedAt().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			plan.Target = target
			return plan
		},
	}
	for name, change := range targetCases {
		t.Run("target/"+name, func(t *testing.T) {
			changed := change(target.input)
			if err := changed.Validate(); err != nil {
				t.Fatalf("changed target plan is not a valid comparison: %v", err)
			}
			if _, err := driver.Create(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed target-plan replay error = %v, want conflict", err)
			}
		})
	}

	setLifecycleCoverage(t, driver, authority.RunID, []string{"process", "network"})
	run := driver.runs[authority.RunID.String()]
	runCases := map[string]func(ports.TargetRunPlan) ports.TargetRunPlan{
		"maximum duration": func(plan ports.TargetRunPlan) ports.TargetRunPlan {
			plan.MaximumDuration++
			return plan
		},
		"coverage order": func(plan ports.TargetRunPlan) ports.TargetRunPlan {
			plan.RequiredCoverage[0], plan.RequiredCoverage[1] = plan.RequiredCoverage[1], plan.RequiredCoverage[0]
			return plan
		},
		"collector version": func(plan ports.TargetRunPlan) ports.TargetRunPlan {
			plan.Collectors[0].Version += ".changed"
			return plan
		},
		"material role": func(plan ports.TargetRunPlan) ports.TargetRunPlan {
			spec := plan.Material[0].Artifact.Spec()
			spec.Role += "-changed"
			artifact, err := domain.NewArtifactReference(spec)
			if err != nil {
				t.Fatal(err)
			}
			plan.Material[0].Artifact = artifact
			return bindRunMaterialization(t, plan)
		},
		"material mode": func(plan ports.TargetRunPlan) ports.TargetRunPlan {
			plan.Material[0].Mode = 0o640
			return bindRunMaterialization(t, plan)
		},
	}
	for name, change := range runCases {
		t.Run("run/"+name, func(t *testing.T) {
			changed := change(cloneTargetRunPlanForTest(run.plan))
			if err := changed.Validate(); err != nil {
				t.Fatalf("changed run plan is not a valid comparison: %v", err)
			}
			if _, err := driver.PrepareRun(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed run-plan replay error = %v, want conflict", err)
			}
		})
	}
}

func cloneTargetRunPlanForTest(plan ports.TargetRunPlan) ports.TargetRunPlan {
	plan.RequiredCoverage = append([]string(nil), plan.RequiredCoverage...)
	plan.Collectors = append([]ports.CollectorSpec(nil), plan.Collectors...)
	for index := range plan.Collectors {
		plan.Collectors[index].Resources = plan.Collectors[index].Resources.Clone()
	}
	plan.Material = append([]ports.TargetMaterialPlan(nil), plan.Material...)
	return plan
}

func bindRunMaterialization(t *testing.T, plan ports.TargetRunPlan) ports.TargetRunPlan {
	t.Helper()
	digest, err := ports.TargetMaterializationDigest(plan.Material)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Run.Spec()
	spec.MaterializationDigest = digest
	run, err := domain.NewTargetRun(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan.Run = run
	return plan
}

func TestUnsupportedExternalReadinessFailureLeavesRunNeverStarted(t *testing.T) {
	factory := &manualTimerFactory{}
	driver, authority := lifecycleTestDriver(t, factory.AfterFunc, CollectorReadinessFunc(func(_ context.Context, _ domain.TargetRunID, requirements []ports.ObservationRequirement) error {
		if !reflect.DeepEqual(requirements, []ports.ObservationRequirement{{SignalFamily: "unsupported.signal", Placement: domain.CollectorPlacementHost, MinimumLevel: domain.CoverageLevelComplete, Required: true}}) {
			t.Fatalf("readiness requirements = %#v", requirements)
		}
		return fmt.Errorf("collector unavailable")
	}))
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily, "unsupported.signal"})
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
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
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
	setLifecycleCoverage(t, driver, authority.RunID, []string{IntrinsicSignalFamily})
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

func setLifecycleCoverage(t *testing.T, driver *Driver, runID domain.TargetRunID, families []string) {
	t.Helper()
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
	target := driver.targets[targetKey(run.authority.TargetID, run.authority.Generation)]
	if err := os.Remove(filepath.Join(target.plan.TargetDirectory, generationRunClaimFile)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := claimTargetGenerationRun(target.plan.TargetDirectory, run.plan); err != nil {
		t.Fatal(err)
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
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
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

func TestProbeAcceptsOnlyRuncTemplateRuntime(t *testing.T) {
	driver, err := New(Config{
		Build:      BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target"},
		Runtime:    ptraceProbeRuntime{noopRuntime{}},
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fingerprint, err := driver.Probe(ctx, template)
	if err != nil {
		t.Fatal(err)
	}
	visibility, found := fingerprint.Capability("target.visibility-first")
	if !found || visibility.Constraints()["runtime"] != dockercli.RuncRuntime {
		t.Fatalf("runtime capability = %#v", visibility.Constraints())
	}
	runtimeCapability, found := fingerprint.Capability("target.linux-container")
	if !found || runtimeCapability.Constraints()["cgroup_identity_authority"] != dockercli.ContainerCgroupIdentityAuthority() {
		t.Fatalf("cgroup identity authority capability = %#v", runtimeCapability.Constraints())
	}
	invalid := template
	invalid.Driver = "foreign"
	if _, err := driver.Probe(ctx, invalid); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
		t.Fatalf("unsupported physical template error = %v", err)
	}
	for _, runtimeName := range []string{"gvisor", "kata"} {
		unsupported := template
		unsupported.Runtime = runtimeName
		unsupported.IsolationProfile = "sandboxed-kernel"
		if _, err := driver.Probe(ctx, unsupported); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("unsupported runtime %q error = %v", runtimeName, err)
		}
	}
}

func TestTargetProbeReportsDaemonUserNamespaceFact(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
	}
	for _, test := range []struct {
		name     string
		options  []string
		wantUser string
	}{
		{name: "host user namespace", options: []string{}, wantUser: "host"},
		{name: "remapped user namespace", options: []string{"name=userns"}, wantUser: "remapped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, err := New(Config{
				Build:      BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target"},
				Runtime:    namespaceProbeRuntime{noopRuntime: noopRuntime{}, securityOptions: test.options},
				Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
			})
			if err != nil {
				t.Fatal(err)
			}
			fingerprint, err := driver.Probe(targetDeadline(t), template)
			if err != nil {
				t.Fatal(err)
			}
			visibility, found := fingerprint.Capability("target.visibility-first")
			if !found || visibility.Constraints()["user_namespace"] != test.wantUser {
				t.Fatalf("target namespace facts = %#v", visibility.Constraints())
			}
		})
	}
}

func TestProbeAcceptsCgroupV1ResourceControllers(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
	}
	driver, err := New(Config{
		Build:      BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target"},
		Runtime:    cgroupProbeRuntime{noopRuntime: noopRuntime{}, version: "1"},
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
	capability, found := fingerprint.Capability("target.linux-container")
	if !found || capability.Constraints()["cgroup_version"] != "1" {
		t.Fatalf("cgroup v1 capability = %#v", capability.Constraints())
	}
}

func TestProbeRejectsUnreportedOrUnsupportedCgroupVersion(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
	}
	for _, version := range []string{"", "3"} {
		t.Run("version_"+version, func(t *testing.T) {
			driver, err := New(Config{
				Build:      BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target"},
				Runtime:    cgroupProbeRuntime{noopRuntime: noopRuntime{}, version: version},
				Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := driver.Probe(ctx, template); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
				t.Fatalf("Probe() error = %v, want capability unavailable", err)
			}
		})
	}
}

func TestCreateRequiresFramedGuestReadinessAndSurfacesCleanupFailure(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan), failGuest: true, failRemove: true}
	driver, input := newTargetCreateTestDriver(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.Create(ctx, input); !domain.IsCode(err, domain.CodeFailedPrecondition) || !strings.Contains(err.Error(), "self-test failed") || !strings.Contains(err.Error(), "could not remove failed target runtime") {
		t.Fatalf("Create() error = %v", err)
	}
	plan, err := BuildContainerPlan(input, driver.build)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.TargetDirectory); err != nil {
		t.Fatalf("failed cleanup removed directory still referenced by runtime: %v", err)
	}
	if _, err := driver.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("unready target became Ready: %v", err)
	}
	runtime.failRemove = false
	if err := driver.cleanupFailedRuntime(testRuntimeID("runtime-1"), plan.TargetDirectory); err != nil {
		t.Fatal(err)
	}
}

func TestCreateInspectsExactStoppedConfigurationBeforeStart(t *testing.T) {
	t.Run("event order", func(t *testing.T) {
		runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
		driver, input := newTargetCreateTestDriver(t, runtime)
		result, err := driver.Create(targetDeadline(t), input)
		if err != nil {
			t.Fatal(err)
		}
		id := result.Status.RuntimeID
		want := strings.Join([]string{"create:1", "inspect:" + id, "start:" + id, "inspect:" + id, "exec:" + id}, ",")
		if got := strings.Join(runtime.Actions(), ","); got != want {
			t.Fatalf("fresh create events = %s; want %s", got, want)
		}
	})

	t.Run("poisoned stopped configuration never starts", func(t *testing.T) {
		runtime := &recordingRuntime{plans: make(map[string]ContainerPlan), poisonCreatedConfiguration: true}
		driver, input := newTargetCreateTestDriver(t, runtime)
		if _, err := driver.Create(targetDeadline(t), input); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("poisoned Create() error = %v, want integrity violation", err)
		}
		actions := strings.Join(runtime.Actions(), ",")
		if strings.Contains(actions, "start:") || strings.Contains(actions, "exec:") {
			t.Fatalf("poisoned stopped configuration crossed the start boundary: %s", actions)
		}
		if !strings.Contains(actions, "create:1,inspect:") || !strings.Contains(actions, ",remove:") {
			t.Fatalf("poisoned create did not inspect then clean up: %s", actions)
		}
	})
}

func newTargetCreateTestDriver(t *testing.T, runtime Runtime) (*Driver, ports.TargetPlan) {
	t.Helper()
	containerUser := defaultTargetUser
	if goruntime.GOOS == "linux" {
		containerUser = fmt.Sprintf("%d:%d", os.Geteuid(), os.Getegid())
	}
	driver, err := New(Config{
		Build: BuildConfig{TargetRoot: writableTempDir(t), ImageRepository: "world-target", ContainerUser: containerUser}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("readiness-image")))
	return driver, input
}

type ptraceProbeRuntime struct{ noopRuntime }

func (ptraceProbeRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{Version: "29.0", APIVersion: "1.52", CgroupVersion: "2", OSType: "linux", DefaultRuntime: "runc", Runtimes: []string{"runc"}}, nil
}

type cgroupProbeRuntime struct {
	noopRuntime
	version string
}

type namespaceProbeRuntime struct {
	noopRuntime
	securityOptions []string
}

func (r namespaceProbeRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{
		Version: "29.0", APIVersion: "1.52", CgroupVersion: "2", OSType: "linux",
		SecurityOptions: append([]string(nil), r.securityOptions...), DefaultRuntime: dockercli.RuncRuntime, Runtimes: []string{dockercli.RuncRuntime},
	}, nil
}

func (r cgroupProbeRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{
		Version: "29.0", APIVersion: "1.52", CgroupVersion: r.version, OSType: "linux",
		DefaultRuntime: dockercli.RuncRuntime, Runtimes: []string{dockercli.RuncRuntime},
	}, nil
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
	setLifecycleCoverage(t, driver, authority.RunID, []string{"process"})
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

func TestQuarantineRetriesFailedTransportCleanup(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	flaky := &flakyCloseExecTransport{failures: 1}
	scoped := &targetTransport{execs: []ports.ExecTransport{flaky}}
	driver.runs[authority.RunID.String()].transports[scoped] = struct{}{}
	plan := ports.TargetQuarantinePlan{
		IdempotencyKey: "quarantine-cleanup-retry", Target: ports.TargetRef{ID: authority.TargetID, Generation: authority.Generation},
		Reason: "exercise cleanup retry",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	evidence, err := driver.Quarantine(ctx, plan)
	if !domain.IsCode(err, domain.CodeUnavailable) || !evidence.ExecutionStopped {
		t.Fatalf("first quarantine = %#v, %v", evidence, err)
	}
	replay, err := driver.Quarantine(ctx, plan)
	if err != nil || replay != evidence {
		t.Fatalf("quarantine cleanup replay = %#v, %v", replay, err)
	}
	if flaky.closeCalls != 2 || driver.runs[authority.RunID.String()].transports != nil {
		t.Fatalf("cleanup calls/state = %d / %#v", flaky.closeCalls, driver.runs[authority.RunID.String()].transports)
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
	if runtime.HasAction("stop:"+testRuntimeID("runtime-1")) || runtime.HasAction("remove:"+testRuntimeID("runtime-1")) {
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
	if result.Status.Generation != 2 || !runtime.ActionBefore("inspect:"+testRuntimeID("runtime-2"), "stop:"+testRuntimeID("runtime-1")) {
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
		if err := setPlanDigest(&plan); err != nil {
			t.Fatal(err)
		}
	}
	generationModel, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: target, Generation: 1, PolicyDigest: plan.PolicyDigest,
		CapabilityFingerprintDigest: plan.CapabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	targetInput := ports.TargetPlan{
		IdempotencyKey: "target-key", LeaseID: lease, Target: targetModel, Generation: generationModel,
		Template: ports.TargetTemplate{
			Name: "lifecycle", Kind: domain.TargetLinuxContainer, Driver: "docker",
			Runtime: dockercli.RuncRuntime, ImageDigest: domain.NewDigest([]byte("image")), IsolationProfile: "observable-container",
		},
		PolicyDigest: plan.PolicyDigest, CapabilityFingerprintDigest: plan.CapabilityDigest, Resources: plan.Resources,
	}
	if err := targetInput.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := prepareTargetDirectories(root, plan); err != nil {
		t.Fatal(err)
	}
	if err := materializeTarget(context.Background(), root, plan, material); err != nil {
		t.Fatal(err)
	}
	baseline, err := scanTargetWritable(context.Background(), plan, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Join(plan.TargetDirectory, "runs", runID.String())
	runPlan := ports.TargetRunPlan{IdempotencyKey: "run-key", Run: runModel, RequiredCoverage: []string{"process"}, Material: material, MaximumDuration: 3 * time.Second}
	if _, _, err := claimTargetGenerationRun(plan.TargetDirectory, runPlan); err != nil {
		t.Fatal(err)
	}
	if err := prepareManagedDirectory(root, runDirectory); err != nil {
		t.Fatal(err)
	}
	if err := persistRunBaseline(runDirectory, baseline); err != nil {
		t.Fatal(err)
	}
	if recording, ok := runtime.(*recordingRuntime); ok {
		recording.mu.Lock()
		recording.plans[testRuntimeID("runtime-1")] = plan
		if recording.running == nil {
			recording.running = make(map[string]bool)
		}
		recording.running[testRuntimeID("runtime-1")] = true
		recording.mu.Unlock()
	}
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 1, RunID: runID}
	return &Driver{
		build: BuildConfig{TargetRoot: root, ImageRepository: "world-target", ContainerUser: plan.User}, runtime: runtime,
		collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
		now:        func() time.Time { return time.Unix(30, 0).UTC() },
		afterFunc:  func(time.Duration, func()) RunTimer { return &manualRunTimer{} },
		random:     bytes.NewReader(bytes.Repeat([]byte{0x5a}, 256)),
		targets: map[string]targetRecord{targetKey(target, 1): {
			input: targetInput, plan: plan, runtimeID: testRuntimeID("runtime-1"),
			status: ports.TargetStatus{TargetID: target, Generation: 1, Kind: domain.TargetLinuxContainer, State: domain.TargetGenerationReady, Ready: true, RuntimeID: testRuntimeID("runtime-1"), CgroupID: "cgroup/" + testRuntimeID("runtime-1"), ObservedAt: time.Unix(10, 0).UTC()},
		}},
		runs: map[string]*runRecord{runID.String(): {
			plan: runPlan, authority: authority,
			directory: runDirectory, baseline: baseline,
			prepared: ports.PreparedTargetRun{
				RunID: runID, TargetID: target, TargetGeneration: 1, MaterializationDigest: materializationDigest,
				RequiredCoverage: []string{"process"}, Attachment: ports.ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: testRuntimeID("runtime-1")},
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

type flakyCloseExecTransport struct {
	closeCalls int
	failures   int
}

type blockingCloseExecTransport struct {
	entered chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (*blockingCloseExecTransport) Send(context.Context, transport.Kind, []byte) (transport.Frame, error) {
	return transport.Frame{}, io.ErrClosedPipe
}

func (*blockingCloseExecTransport) Receive(context.Context) (transport.Frame, error) {
	return transport.Frame{}, io.ErrClosedPipe
}

func (t *blockingCloseExecTransport) Close() error {
	t.once.Do(func() { close(t.entered) })
	<-t.release
	return nil
}

func (*flakyCloseExecTransport) Send(context.Context, transport.Kind, []byte) (transport.Frame, error) {
	return transport.Frame{}, io.ErrClosedPipe
}

func (*flakyCloseExecTransport) Receive(context.Context) (transport.Frame, error) {
	return transport.Frame{}, io.ErrClosedPipe
}

func (t *flakyCloseExecTransport) Close() error {
	t.closeCalls++
	if t.closeCalls <= t.failures {
		return fmt.Errorf("injected close failure")
	}
	return nil
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

func replacementLifecycleRunPlan(t *testing.T, first ports.TargetRunPlan, key string) ports.TargetRunPlan {
	t.Helper()
	spec := first.Run.Spec()
	runID, err := domain.NewTargetRunID()
	if err != nil {
		t.Fatal(err)
	}
	spec.ID = runID
	spec.CreatedAt = spec.CreatedAt.Add(time.Second)
	run, err := domain.NewTargetRun(spec)
	if err != nil {
		t.Fatal(err)
	}
	first.IdempotencyKey = key
	first.Run = run
	return first
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
	mu                         sync.Mutex
	failCreateGeneration       domain.TargetGeneration
	failContainment            bool
	unconfirmedContainment     bool
	failGuest                  bool
	failRemove                 bool
	stopFailures               int
	poisonCreatedConfiguration bool
	plans                      map[string]ContainerPlan
	running                    map[string]bool
	statuses                   map[string]string
	actions                    []string
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
	id := testRuntimeID("runtime-" + fmt.Sprint(plan.Generation))
	r.plans[id] = plan
	if r.running == nil {
		r.running = make(map[string]bool)
	}
	r.running[id] = false
	if r.statuses == nil {
		r.statuses = make(map[string]string)
	}
	r.statuses[id] = dockercli.StoppedStatusCreated
	return id, nil
}

func (r *recordingRuntime) Start(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, "start:"+id)
	if r.running == nil {
		r.running = make(map[string]bool)
	}
	r.running[id] = true
	if r.statuses == nil {
		r.statuses = make(map[string]string)
	}
	r.statuses[id] = "running"
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
	running := r.running[id]
	status := r.statuses[id]
	if status == "" {
		status = dockercli.StoppedStatusExited
		if running {
			status = "running"
		}
	}
	configuration := expectedTargetConfiguration(plan)
	if r.poisonCreatedConfiguration && status == dockercli.StoppedStatusCreated {
		configuration.MemoryBytes++
	}
	return RuntimeState{ID: id, Name: plan.Name, Running: running, Status: status, Labels: cloneStrings(plan.Labels), CgroupID: "cgroup/" + id, Configuration: configuration}, nil
}

func (r *recordingRuntime) Stop(_ context.Context, id string, _ ports.StopMode) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, "stop:"+id)
	r.running[id] = false
	if r.statuses == nil {
		r.statuses = make(map[string]string)
	}
	r.statuses[id] = dockercli.StoppedStatusExited
	if r.stopFailures > 0 {
		r.stopFailures--
		return fmt.Errorf("injected stop failure")
	}
	return nil
}

func (r *recordingRuntime) Quarantine(_ context.Context, id string) (RuntimeContainmentEvidence, error) {
	r.record("quarantine:" + id)
	if r.failContainment {
		return RuntimeContainmentEvidence{}, fmt.Errorf("injected containment failure")
	}
	r.mu.Lock()
	r.running[id] = false
	if r.statuses == nil {
		r.statuses = make(map[string]string)
	}
	r.statuses[id] = dockercli.StoppedStatusExited
	r.mu.Unlock()
	return RuntimeContainmentEvidence{
		RuntimeID: id, ExecutionStopped: true, NetworkUnreachable: !r.unconfirmedContainment,
		StatePreserved: true, ObservedAt: time.Unix(40, 0).UTC(),
	}, nil
}

func (r *recordingRuntime) Remove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, "remove:"+id)
	if r.failRemove {
		return fmt.Errorf("injected remove failure")
	}
	delete(r.plans, id)
	delete(r.running, id)
	delete(r.statuses, id)
	return nil
}

func (r *recordingRuntime) OpenExec(_ context.Context, id string, _ ports.TargetExecPlan) (ports.ExecTransport, error) {
	r.record("exec:" + id)
	if r.failGuest {
		return &targetReadinessTransport{frames: []transport.Frame{
			targetJSONFrame(1, transport.KindTerminal, transport.Terminal{ExitCode: 1, CleanupConfirmed: true, Error: "injected guest failure"}),
		}}, nil
	}
	return successfulTargetReadinessTransport(), nil
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

func (r *recordingRuntime) ActionCount(expected string) int {
	count := 0
	for _, action := range r.Actions() {
		if action == expected {
			count++
		}
	}
	return count
}

func (r *recordingRuntime) IsRunning(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running[id]
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
