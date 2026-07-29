package cuttlefish

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestResetCreatesReachableNextBeforeRetiringPrevious(t *testing.T) {
	for _, mode := range []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline} {
		t.Run(string(mode), func(t *testing.T) {
			driver, backend, allocator, targetID, leaseID, previous := resetTestDriver(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			plan := ports.ResetPlan{IdempotencyKey: "reset-" + string(mode), LeaseID: leaseID, Previous: ports.TargetRef{ID: targetID, Generation: 1}, NextGeneration: 2, Mode: mode}
			result, err := driver.Reset(ctx, targetID, plan)
			if err != nil {
				t.Fatal(err)
			}
			if result.Status.Generation != 2 || result.Status.DeviceSerial == previous.Allocation.Serial || !backend.Reachable(result.Status.DeviceSerial) {
				t.Fatalf("reset returned a metadata-only or unreachable replacement: %#v", result)
			}
			if backend.Reachable(previous.Allocation.Serial) {
				t.Fatal("previous instance remained reachable after reset committed")
			}
			if !backend.ActionBefore("ready:"+result.Status.RuntimeID, "destroy:"+previous.RuntimeID) {
				t.Fatalf("previous instance retired before Next readiness: %v", backend.Actions())
			}
			if _, found := driver.targets[deviceKey(targetID, 1)]; found {
				t.Fatal("successful reset retained previous generation bookkeeping")
			}
			if record, found := driver.targets[deviceKey(targetID, 2)]; !found || record.instance.Allocation.Serial != result.Status.DeviceSerial {
				t.Fatalf("Next bookkeeping does not identify the reachable instance: %#v", record)
			}
			beforeReplay := backend.Actions()
			replay, err := driver.Reset(ctx, targetID, plan)
			if err != nil || replay.Status.DeviceSerial != result.Status.DeviceSerial || len(backend.Actions()) != len(beforeReplay) {
				t.Fatalf("idempotent reset replay changed physical state: %#v, %v, %v", replay, err, backend.Actions())
			}
			conflicting := plan
			if mode == ports.ResetRecreate {
				conflicting.Mode = ports.ResetBaseline
			} else {
				conflicting.Mode = ports.ResetRecreate
			}
			if _, err := driver.Reset(ctx, targetID, conflicting); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("idempotency key reuse error = %v", err)
			}
			if allocatorGenerationCount(allocator, targetID) != 1 {
				t.Fatal("successful reset did not release the previous allocation")
			}
		})
	}
}

func TestResetMakesASpentAndroidGenerationUsableOnlyAsFreshNextGeneration(t *testing.T) {
	driver, _, _, targetID, leaseID, previous := resetTestDriver(t)
	files := newRecordingFileGateway(previous.Allocation.Serial)
	driver.files = files
	driver.collectors = CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil })
	driver.random = bytes.NewReader(bytes.Repeat([]byte{0x51}, 256))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := targetRunPlanForGeneration(t, leaseID, targetID, 1, nil, "spent-generation-first")
	if _, err := driver.PrepareRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, first.Run.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, first.Run.ID(), ports.StopGraceful); err != nil {
		t.Fatal(err)
	}
	blocked := targetRunPlanForGeneration(t, leaseID, targetID, 1, nil, "spent-generation-blocked")
	if _, err := driver.PrepareRun(ctx, blocked); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("spent generation accepted another run: %v", err)
	}
	reset, err := driver.Reset(ctx, targetID, ports.ResetPlan{
		IdempotencyKey: "spent-generation-reset", LeaseID: leaseID,
		Previous: ports.TargetRef{ID: targetID, Generation: 1}, NextGeneration: 2, Mode: ports.ResetBaseline,
	})
	if err != nil || reset.Status.Generation != 2 || reset.Status.DeviceSerial == previous.Allocation.Serial {
		t.Fatalf("fresh reset generation = %#v, %v", reset, err)
	}
	files.serial = reset.Status.DeviceSerial
	second := targetRunPlanForGeneration(t, leaseID, targetID, 2, nil, "fresh-generation-run")
	if _, err := driver.PrepareRun(ctx, second); err != nil {
		t.Fatalf("fresh reset generation rejected first run: %v", err)
	}
	if _, err := driver.StopRun(ctx, second.Run.ID(), ports.StopGraceful); err != nil {
		t.Fatal(err)
	}
}

func TestResetRetainsProvenNextWhenPreviousCannotBeRestored(t *testing.T) {
	driver, backend, allocator, targetID, leaseID, previous := resetTestDriver(t)
	backend.failDestroyRuntime = previous.RuntimeID
	backend.failRestoreRuntime = previous.RuntimeID
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := ports.ResetPlan{IdempotencyKey: "reset-retain-next", LeaseID: leaseID, Previous: ports.TargetRef{ID: targetID, Generation: 1}, NextGeneration: 2, Mode: ports.ResetRecreate}
	result, err := driver.Reset(ctx, targetID, plan)
	if !domain.IsCode(err, domain.CodeUnavailable) || result.Status.Generation != 2 || !backend.Reachable(result.Status.DeviceSerial) {
		t.Fatalf("uncertain previous did not retain the proven replacement: %#v, %v", result, err)
	}
	if _, found := driver.targets[deviceKey(targetID, 2)]; !found {
		t.Fatal("proven replacement was not committed")
	}
	if allocatorGenerationCount(allocator, targetID) != 2 {
		t.Fatal("uncertain previous allocation was released and could collide")
	}
	replay, replayErr := driver.Reset(ctx, targetID, plan)
	if !domain.IsCode(replayErr, domain.CodeUnavailable) || replay.Status.DeviceSerial != result.Status.DeviceSerial {
		t.Fatalf("partial reset outcome was not replayed exactly: %#v, %v", replay, replayErr)
	}
}

func TestResetRetirementFailureRollsBackReachableNext(t *testing.T) {
	driver, backend, allocator, targetID, leaseID, previous := resetTestDriver(t)
	backend.failDestroyRuntime = previous.RuntimeID
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := ports.ResetPlan{IdempotencyKey: "reset-rollback", LeaseID: leaseID, Previous: ports.TargetRef{ID: targetID, Generation: 1}, NextGeneration: 2, Mode: ports.ResetRecreate}
	if _, err := driver.Reset(ctx, targetID, plan); !domain.IsCode(err, domain.CodeUnavailable) {
		t.Fatalf("retirement failure = %v", err)
	}
	if !backend.Reachable(previous.Allocation.Serial) {
		t.Fatal("rollback did not preserve the reachable previous generation")
	}
	if backend.HasGeneration(2) {
		t.Fatal("rollback left the replacement instance alive")
	}
	if _, found := driver.targets[deviceKey(targetID, 1)]; !found {
		t.Fatal("rollback removed previous generation bookkeeping")
	}
	if _, found := driver.targets[deviceKey(targetID, 2)]; found {
		t.Fatal("rollback committed Next bookkeeping")
	}
	if allocatorGenerationCount(allocator, targetID) != 1 {
		t.Fatal("rollback leaked the Next allocation")
	}
}

func TestResetSnapshotIsExplicitlyUnavailableBeforeAllocation(t *testing.T) {
	driver, backend, allocator, targetID, leaseID, _ := resetTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := ports.ResetPlan{IdempotencyKey: "reset-snapshot", LeaseID: leaseID, Previous: ports.TargetRef{ID: targetID, Generation: 1}, NextGeneration: 2, Mode: ports.ResetSnapshot, SnapshotName: "baseline"}
	if _, err := driver.Reset(ctx, targetID, plan); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
		t.Fatalf("snapshot reset error = %v", err)
	}
	if backend.HasGeneration(2) || allocatorGenerationCount(allocator, targetID) != 1 {
		t.Fatal("unavailable snapshot reset mutated backend or allocation state")
	}
}

func TestResetOutcomeSurvivesRestartAndRejectsChangedReplay(t *testing.T) {
	restarted, backend, plan, original := resetRestartFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	actions := backend.Actions()
	replay, err := restarted.Reset(ctx, plan.Previous.ID, plan)
	if err != nil || replay != original {
		t.Fatalf("durable reset replay = %#v, %v; want %#v", replay, err, original)
	}
	if len(backend.Actions()) != len(actions) {
		t.Fatalf("durable reset replay mutated the backend: %v -> %v", actions, backend.Actions())
	}
	changedPayload := plan
	if changedPayload.Mode == ports.ResetRecreate {
		changedPayload.Mode = ports.ResetBaseline
	} else {
		changedPayload.Mode = ports.ResetRecreate
	}
	if _, err := restarted.Reset(ctx, plan.Previous.ID, changedPayload); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed reset payload error = %v", err)
	}
	changedKey := plan
	changedKey.IdempotencyKey = "android-reset-restart-other-key"
	if _, err := restarted.Reset(ctx, plan.Previous.ID, changedKey); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed reset key error = %v", err)
	}
}

func TestResetReconciliationCompletesCrashBeforeOutcomeCommit(t *testing.T) {
	restarted, backend, plan, interruptedResult := resetRestartFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	actions := backend.Actions()
	replay, err := restarted.Reset(ctx, plan.Previous.ID, plan)
	if err != nil || replay.Status.TargetID != interruptedResult.Status.TargetID || replay.Status.Generation != interruptedResult.Status.Generation || replay.Status.RuntimeID != interruptedResult.Status.RuntimeID || replay.Status.DeviceSerial != interruptedResult.Status.DeviceSerial || !replay.Status.Ready {
		t.Fatalf("recovered reset replay = %#v, %v; interrupted result %#v", replay, err, interruptedResult)
	}
	if len(backend.Actions()) != len(actions) {
		t.Fatalf("recovered reset replay repeated physical reset work: %v -> %v", actions, backend.Actions())
	}
}

func TestResetReconciliationConvergesAtEveryDurableCheckpoint(t *testing.T) {
	for _, checkpoint := range []resetCheckpoint{
		resetCheckpointTransitionCommitted,
		resetCheckpointReplacementManifest,
		resetCheckpointPreviousRetired,
		resetCheckpointOutcomeCommitted,
	} {
		t.Run(string(checkpoint), func(t *testing.T) {
			restarted, backend, plan, _ := resetRestartFixture(t, false, checkpoint)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			actions := backend.Actions()
			first, err := restarted.Reset(ctx, plan.Previous.ID, plan)
			if err != nil || !first.Created || !first.Status.Ready || first.Status.Generation != plan.NextGeneration {
				t.Fatalf("recovered reset replay = %#v, %v", first, err)
			}
			second, err := restarted.Reset(ctx, plan.Previous.ID, plan)
			if err != nil || second != first {
				t.Fatalf("second recovered reset replay = %#v, %v; want %#v", second, err, first)
			}
			if len(backend.Actions()) != len(actions) {
				t.Fatalf("exact reset replay repeated physical work: %v -> %v", actions, backend.Actions())
			}
			if backend.HasGeneration(plan.Previous.Generation) || !backend.HasGeneration(plan.NextGeneration) {
				t.Fatalf("reset recovery did not converge to only generation %d", plan.NextGeneration)
			}
		})
	}
}

func TestProbeExposesOnlyImplementedResetModes(t *testing.T) {
	driver, _, _, _, _, _ := resetTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fingerprint, err := driver.Probe(ctx, ports.TargetTemplate{
		Name: "android", Kind: domain.TargetAndroidVirtualDevice, Driver: "android-emulator",
		ImageDigest: domain.NewDigest([]byte("system")), IsolationProfile: "android-vm", BaselineState: ports.AndroidBaselineCleanBoot,
		RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true,
		GuestMemoryBytes: 2 << 30, BootTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	reset, found := fingerprint.Capability("target.android-reset")
	if !found {
		t.Fatal("reset capability missing")
	}
	constraints := reset.Constraints()
	if constraints["modes"] != "baseline,recreate" || constraints["snapshot"] != "false" {
		t.Fatalf("reset capability = %#v", constraints)
	}
}

func TestQuarantineContainsActiveInstanceAndPreservesAllocationAndRunState(t *testing.T) {
	driver, backend, allocator, targetID, leaseID, previous := resetTestDriver(t)
	if err := os.MkdirAll(previous.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(previous.StateDirectory, "evidence.marker")
	if err := os.WriteFile(marker, []byte("retain"), 0o600); err != nil {
		t.Fatal(err)
	}
	runPlan := targetRunPlanForGeneration(t, leaseID, targetID, 1, nil, "quarantine-active-run")
	if err := persistRunGenerationUse(driver.targets[deviceKey(targetID, 1)].plan, runPlan, time.Unix(1_500, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	runID := runPlan.Run.ID()
	startContext, cancelStart := context.WithCancel(context.Background())
	deadlineContext, cancelDeadline := context.WithCancel(context.Background())
	transport := &androidTransport{}
	driver.runs[runID.String()] = &runRecord{
		scope:      deviceproxy.Scope{LeaseID: leaseID, TargetID: targetID, Generation: 1, RunID: runID, Serial: previous.Allocation.Serial},
		allocation: previous.Allocation, started: true, starting: true, startCancel: cancelStart,
		deadlineCancel: cancelDeadline, transports: map[*androidTransport]struct{}{transport: {}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := ports.TargetQuarantinePlan{IdempotencyKey: "quarantine-active", Target: ports.TargetRef{ID: targetID, Generation: 1}, Reason: "contain active Android workload"}
	evidence, err := driver.Quarantine(ctx, plan)
	if err != nil || evidence.Validate(plan.Target) != nil {
		t.Fatalf("Quarantine() = %#v, %v", evidence, err)
	}
	if backend.Reachable(previous.Allocation.Serial) {
		t.Fatal("quarantined device remained reachable")
	}
	if allocatorGenerationCount(allocator, targetID) != 1 {
		t.Fatal("quarantine released the reserved device allocation")
	}
	if content, err := os.ReadFile(marker); err != nil || string(content) != "retain" {
		t.Fatalf("quarantine removed retained device state: %q, %v", content, err)
	}
	storedRun := driver.runs[runID.String()]
	if storedRun == nil || !storedRun.quarantined || storedRun.stopped || storedRun.deadlineCancel != nil {
		t.Fatalf("quarantine erased or incorrectly finalized run history: %#v", storedRun)
	}
	if !transport.closed {
		t.Fatal("quarantine left scoped Android transport open")
	}
	blocked := targetRunPlanForGeneration(t, leaseID, targetID, 1, nil, "quarantined-generation-run")
	if _, err := driver.PrepareRun(ctx, blocked); !domain.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("quarantined generation accepted a run: %v", err)
	}
	quarantine, found, err := loadGenerationQuarantine(driver.targets[deviceKey(targetID, 1)].plan)
	if err != nil || !found || quarantine.RuntimeID != previous.RuntimeID || quarantine.Containment.RuntimeID != previous.RuntimeID {
		t.Fatalf("durable quarantine proof = %#v, %v, found=%t", quarantine, err, found)
	}
	select {
	case <-startContext.Done():
	default:
		t.Fatal("quarantine did not cancel collector readiness")
	}
	select {
	case <-deadlineContext.Done():
	default:
		t.Fatal("quarantine did not cancel the run deadline")
	}
	actions := backend.Actions()
	replay, err := driver.Quarantine(ctx, plan)
	if err != nil || replay != evidence || len(backend.Actions()) != len(actions) {
		t.Fatalf("quarantine replay = %#v, %v; actions %v -> %v", replay, err, actions, backend.Actions())
	}
	conflict := plan
	conflict.Reason = "different containment request"
	if _, err := driver.Quarantine(ctx, conflict); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("quarantine idempotency conflict = %v", err)
	}
}

func TestQuarantineRejectsUnsupportedAndUnconfirmedBackends(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		driver, backend, _, targetID, _, _ := resetTestDriver(t)
		driver.backend = backendWithoutQuarantine{Backend: backend}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		plan := ports.TargetQuarantinePlan{IdempotencyKey: "quarantine-unsupported", Target: ports.TargetRef{ID: targetID, Generation: 1}, Reason: "unsupported backend"}
		if _, err := driver.Quarantine(ctx, plan); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("unsupported quarantine error = %v", err)
		}
	})
	t.Run("unconfirmed", func(t *testing.T) {
		driver, backend, _, targetID, _, _ := resetTestDriver(t)
		backend.quarantineUnconfirmed = true
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		plan := ports.TargetQuarantinePlan{IdempotencyKey: "quarantine-unconfirmed", Target: ports.TargetRef{ID: targetID, Generation: 1}, Reason: "unconfirmed backend"}
		if _, err := driver.Quarantine(ctx, plan); !domain.IsCode(err, domain.CodeFailedPrecondition) {
			t.Fatalf("unconfirmed quarantine error = %v", err)
		}
		if driver.targets[deviceKey(targetID, 1)].status.State == domain.TargetGenerationQuarantined {
			t.Fatal("unconfirmed backend advanced driver quarantine state")
		}
	})
}

type backendWithoutQuarantine struct{ Backend }

func resetRestartFixture(t *testing.T, failOutcomeCommit bool, checkpoints ...resetCheckpoint) (*Driver, *statefulBackend, ports.ResetPlan, ports.TargetResult) {
	t.Helper()
	if len(checkpoints) > 1 {
		t.Fatal("reset restart fixture accepts at most one interruption checkpoint")
	}
	root := t.TempDir()
	firstPort := findFreeEvenPortPair(t)
	allocatorConfig := DurableEmulatorAllocatorConfig{
		StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: firstPort, LastConsolePort: firstPort + 2,
	}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	input, build := reconciliationTargetPlan(t, root)
	files := newRecordingFileGateway(emulatorAllocation(firstPort).Serial)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, files)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := driver.Create(ctx, input); err != nil {
		t.Fatal(err)
	}
	previousStateDirectory := driver.targets[deviceKey(input.Target.ID(), 1)].plan.StateDirectory
	plan := ports.ResetPlan{
		IdempotencyKey: "android-reset-restart", LeaseID: input.LeaseID,
		Previous: ports.TargetRef{ID: input.Target.ID(), Generation: 1}, NextGeneration: 2, Mode: ports.ResetRecreate,
	}
	if failOutcomeCommit {
		driver.commitResetOutcome = func(VirtualDevicePlan, resetTransitionManifest, ports.TargetResult, error, time.Time) (resetOutcome, error) {
			return resetOutcome{}, fmt.Errorf("simulated controller loss before reset outcome commit")
		}
	}
	if len(checkpoints) == 1 {
		driver.resetCheckpoint = func(observed resetCheckpoint) error {
			if observed == checkpoints[0] {
				return fmt.Errorf("simulated controller loss after %s", observed)
			}
			return nil
		}
	}
	resetResult, resetErr := driver.Reset(ctx, input.Target.ID(), plan)
	if failOutcomeCommit {
		if !domain.IsCode(resetErr, domain.CodeUnavailable) || !resetResult.Status.Ready {
			t.Fatalf("interrupted reset = %#v, %v", resetResult, resetErr)
		}
	} else if len(checkpoints) == 1 {
		if !domain.IsCode(resetErr, domain.CodeUnavailable) {
			t.Fatalf("checkpoint-interrupted reset = %#v, %v", resetResult, resetErr)
		}
	} else if resetErr != nil {
		t.Fatal(resetErr)
	}
	if !backend.HasGeneration(1) {
		// Production backends delete the retired generation directory as
		// part of physical destruction. Recovery must rely on the immutable
		// N+1 transition once N is authoritatively absent.
		if err := os.RemoveAll(previousStateDirectory); err != nil {
			t.Fatal(err)
		}
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	restarted := reconciliationDriver(t, build, backend, restartedAllocator, files)
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Errorf("close restarted reset driver: %v", err)
		}
	})
	expected := targetPlanAfterReset(t, input, plan)
	report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{expected}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("reset reconciliation = %#v, %v", report, err)
	}
	return restarted, backend, plan, resetResult
}

func targetPlanAfterReset(t *testing.T, previous ports.TargetPlan, reset ports.ResetPlan) ports.TargetPlan {
	t.Helper()
	at := previous.Target.UpdatedAt().Add(time.Second).UTC()
	target, err := previous.Target.AdvanceGeneration(previous.Target.Revision(), reset.NextGeneration, at)
	if err != nil {
		t.Fatal(err)
	}
	previousGeneration := previous.Generation.Spec()
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: target.ID(), Generation: reset.NextGeneration,
		PolicyDigest: previous.PolicyDigest, CapabilityFingerprintDigest: previous.CapabilityFingerprintDigest,
		PreviousGeneration: reset.Previous.Generation, RecoveryIncidentID: reset.IncidentID, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if previousGeneration.TargetID != target.ID() {
		t.Fatal("previous target plan changed identity")
	}
	next := previous
	next.IdempotencyKey = "android-reset-reconcile-next"
	next.Target = target
	next.Generation = generation
	return next
}

func resetTestDriver(t *testing.T) (*Driver, *statefulBackend, *MemoryAllocator, domain.TargetID, domain.LeaseID, Instance) {
	t.Helper()
	targetRoot := cuttlefishTempDir(t, "world-cuttlefish-reset-target-")
	imageRoot := cuttlefishTempDir(t, "world-cuttlefish-reset-image-")
	targetID, _ := domain.NewTargetID()
	leaseID, _ := domain.NewLeaseID()
	sessionID, _ := domain.NewResearchSessionID()
	targetModel, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, 1, time.Unix(1_000, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := NewMemoryAllocator(1, 7600)
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := allocator.Reserve(context.Background(), targetID, 1)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := ResetFingerprint{
		BackendVersion: "cvd-test", RuntimeVersion: "aosp-test",
		SystemImageDigest: domain.NewDigest([]byte("system")), DeviceConfigDigest: domain.NewDigest([]byte("device")),
		Features: []string{"root"},
	}
	plan := VirtualDevicePlan{
		Name: "world-android-" + targetID.UUID() + "-g1", LeaseID: leaseID, TargetID: targetID, Generation: 1,
		StateDirectory:       filepath.Join(targetRoot, targetID.String(), "generations", "1"),
		SystemImageDirectory: filepath.Join(imageRoot, "system"), Allocation: allocation, Fingerprint: fingerprint,
		ADBServer: ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5037},
		Resources: admission.Resources{CPUMilli: 1000, MemoryBytes: 6 << 30, StorageBytes: 1 << 30}, GuestMemoryBytes: 2 << 30,
		Labels:        map[string]string{"world.target-generation": "1"},
		BaselineState: ports.AndroidBaselineCleanBoot, RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true, BootTimeout: time.Minute,
	}
	previous := instanceFromPlan(plan)
	backend := newStatefulBackend(previous)
	driver := &Driver{
		build:   BuildConfig{TargetRoot: targetRoot, SystemImageRoot: imageRoot, ADBServerEndpoint: DefaultADBServerEndpoint, BackendVersion: "cvd-test", RuntimeVersion: "aosp-test", DeviceConfigDigest: fingerprint.DeviceConfigDigest},
		backend: backend, allocator: allocator, now: func() time.Time { return time.Unix(2_000, 0).UTC() },
		targets: map[string]deviceRecord{deviceKey(targetID, 1): {
			input: ports.TargetPlan{IdempotencyKey: "create-target", Target: targetModel}, plan: plan, instance: previous,
			status: ports.TargetStatus{TargetID: targetID, Generation: 1, Kind: domain.TargetAndroidVirtualDevice, Ready: true, RuntimeID: previous.RuntimeID, DeviceSerial: allocation.Serial},
		}},
		runs: make(map[string]*runRecord), idempotency: map[string]string{"create-target": deviceKey(targetID, 1)}, resetResults: make(map[string]resetOutcome),
	}
	return driver, backend, allocator, targetID, leaseID, previous
}

func allocatorGenerationCount(allocator *MemoryAllocator, targetID domain.TargetID) int {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	count := 0
	for key := range allocator.allocations {
		if len(key) >= len(targetID.String()) && key[:len(targetID.String())] == targetID.String() {
			count++
		}
	}
	return count
}

type statefulInstance struct {
	instance   Instance
	generation domain.TargetGeneration
	running    bool
}

type statefulBackend struct {
	mu                    sync.Mutex
	instances             map[string]*statefulInstance
	actions               []string
	containmentModes      []ports.StopMode
	failDestroyRuntime    string
	failRestoreRuntime    string
	quarantineUnconfirmed bool
}

func newStatefulBackend(previous Instance) *statefulBackend {
	return &statefulBackend{instances: map[string]*statefulInstance{previous.RuntimeID: {instance: previous, generation: 1, running: true}}}
}

func (b *statefulBackend) Probe(context.Context, ports.TargetTemplate) (BackendCapabilities, error) {
	return BackendCapabilities{BackendVersion: "cvd-test", RuntimeVersion: "aosp-test", KVM: true, ResetModes: []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline}}, nil
}

func (b *statefulBackend) Create(_ context.Context, plan VirtualDevicePlan) (Instance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "create:"+plan.Name)
	if _, exists := b.instances[plan.Allocation.InstanceName]; exists {
		return Instance{}, fmt.Errorf("instance already exists")
	}
	instance := instanceFromPlan(plan)
	b.instances[instance.RuntimeID] = &statefulInstance{instance: instance, generation: plan.Generation}
	return instance, nil
}

func (b *statefulBackend) ResumeUnstarted(ctx context.Context, instance Instance) (bool, error) {
	b.mu.Lock()
	state, found := b.instances[instance.RuntimeID]
	if !found || state.instance.Allocation != instance.Allocation {
		b.mu.Unlock()
		return false, fmt.Errorf("exact configured instance is absent")
	}
	if state.running {
		b.mu.Unlock()
		return false, nil
	}
	b.mu.Unlock()
	return true, b.Start(ctx, instance)
}

func (b *statefulBackend) Start(_ context.Context, instance Instance) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "start:"+instance.RuntimeID)
	if instance.RuntimeID == b.failRestoreRuntime {
		return fmt.Errorf("injected restore failure")
	}
	state, found := b.instances[instance.RuntimeID]
	if !found || state.instance.Allocation.Serial != instance.Allocation.Serial {
		return fmt.Errorf("instance was not created at the requested serial")
	}
	state.running = true
	return nil
}

func (b *statefulBackend) WaitReady(_ context.Context, instance Instance) (ReadinessState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "ready:"+instance.RuntimeID)
	state, found := b.instances[instance.RuntimeID]
	if !found || !state.running || state.instance.Allocation.Serial != instance.Allocation.Serial {
		return ReadinessState{}, fmt.Errorf("instance is not reachable at the requested serial")
	}
	return readyState(), nil
}

func (b *statefulBackend) Inspect(_ context.Context, instance Instance) (ReadinessState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "inspect:"+instance.RuntimeID)
	state, found := b.instances[instance.RuntimeID]
	if !found || !state.running || state.instance.Allocation.Serial != instance.Allocation.Serial {
		return ReadinessState{}, fmt.Errorf("instance is not reachable")
	}
	return readyState(), nil
}

func (b *statefulBackend) Stop(_ context.Context, instance Instance, _ ports.StopMode) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "stop:"+instance.RuntimeID)
	state, found := b.instances[instance.RuntimeID]
	if !found {
		return nil
	}
	state.running = false
	return nil
}

func (b *statefulBackend) Quarantine(_ context.Context, instance Instance, mode ports.StopMode) (BackendQuarantineState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "quarantine:"+instance.RuntimeID)
	b.containmentModes = append(b.containmentModes, mode)
	state, found := b.instances[instance.RuntimeID]
	if !found || state.instance.Allocation != instance.Allocation {
		return BackendQuarantineState{}, fmt.Errorf("exact instance is not owned by backend")
	}
	state.running = false
	return BackendQuarantineState{
		RuntimeID:        instance.RuntimeID,
		ExecutionStopped: true, NetworkUnreachable: !b.quarantineUnconfirmed,
		StatePreserved: true, ObservedAt: time.Unix(2_100, 0).UTC(),
	}, nil
}

func (b *statefulBackend) AdoptStopped(_ context.Context, instance Instance, proof BackendQuarantineState) (BackendQuarantineState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "adopt-stopped:"+instance.RuntimeID)
	if err := validateStoppedAdoption(instance, proof); err != nil {
		return BackendQuarantineState{}, err
	}
	state, found := b.instances[instance.RuntimeID]
	if !found || state.instance.Allocation != instance.Allocation {
		return BackendQuarantineState{}, fmt.Errorf("exact instance is not owned by backend")
	}
	if state.running {
		return BackendQuarantineState{}, fmt.Errorf("exact instance remains running")
	}
	proof.ObservedAt = proof.ObservedAt.Add(time.Nanosecond)
	return proof, nil
}

func (b *statefulBackend) Destroy(_ context.Context, instance Instance) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "destroy:"+instance.RuntimeID)
	if instance.RuntimeID == b.failDestroyRuntime {
		if instance.RuntimeID == b.failRestoreRuntime {
			if state := b.instances[instance.RuntimeID]; state != nil {
				state.running = false
			}
		}
		return fmt.Errorf("injected destroy failure")
	}
	delete(b.instances, instance.RuntimeID)
	return nil
}

func (b *statefulBackend) ListRuntimeIDs(context.Context) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]string, 0, len(b.instances))
	for id := range b.instances {
		result = append(result, id)
	}
	return result, nil
}

func (b *statefulBackend) Reachable(serial string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, state := range b.instances {
		if state.instance.Allocation.Serial == serial && state.running {
			return true
		}
	}
	return false
}

func (b *statefulBackend) HasGeneration(generation domain.TargetGeneration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, state := range b.instances {
		if state.generation == generation {
			return true
		}
	}
	return false
}

func (b *statefulBackend) Actions() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.actions...)
}

func (b *statefulBackend) ContainmentModes() []ports.StopMode {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]ports.StopMode(nil), b.containmentModes...)
}

func (b *statefulBackend) ActionBefore(first, second string) bool {
	firstIndex, secondIndex := -1, -1
	for index, action := range b.Actions() {
		if action == first && firstIndex < 0 {
			firstIndex = index
		}
		if action == second && secondIndex < 0 {
			secondIndex = index
		}
	}
	return firstIndex >= 0 && secondIndex > firstIndex
}

func readyState() ReadinessState {
	return ReadinessState{
		ProcessRunning: true, ADBReady: true, DeviceState: "device", BootCompleted: true, FrameworkReady: true, PackageManagerReady: true,
		Identity:   AndroidIdentity{SerialNumber: "CVD-TEST", BuildFingerprint: "aosp/test:userdebug/test-keys", SDK: "35", ABI: "x86_64", QEMU: true, Rooted: true, Debuggable: true},
		ObservedAt: time.Unix(2_000, 0).UTC(),
	}
}

var _ Backend = (*statefulBackend)(nil)
var _ BackendInventory = (*statefulBackend)(nil)
