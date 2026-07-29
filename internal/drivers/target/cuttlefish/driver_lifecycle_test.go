package cuttlefish

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestMaximumRunDurationStopsRunRevokesTransportAndPreservesEvidence(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6560")
	driver, stateDirectory := materializationTestDriver(t, lease, target, files)
	allocation := driver.targets[deviceKey(target, 1)].instance.Allocation
	endpointGateway := &recordingEndpointGateway{expectedAllocation: allocation}
	driver.gateway = endpointGateway
	plan := targetRunPlanForMaterial(t, lease, target, []ports.TargetMaterialPlan{targetMaterial(t, "input.bin", 0o600, []byte("input"), nil)}, "android-duration")
	plan.MaximumDuration = 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	endpointGateway.expectedScope = driver.runs[plan.Run.ID().String()].scope
	driver.mu.Unlock()
	if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	transport, err := driver.OpenTransport(ctx, plan.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.OpenADB(ctx); err != nil {
		t.Fatal(err)
	}
	waitForAndroidRunStopped(t, driver, plan.Run.ID())
	result, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, plan, ports.RunFailed, ports.TargetRunFailureDurationExceeded)
	if !endpointGateway.Closed() {
		t.Fatal("duration stop did not revoke the scoped ADB endpoint")
	}
	files.mu.Lock()
	removed := files.removed
	files.mu.Unlock()
	if removed != 0 {
		t.Fatalf("duration stop mutated the preserved guest %d times", removed)
	}
	runDirectory := filepath.Join(stateDirectory, "runs", plan.Run.ID().String())
	if _, err := os.Stat(filepath.Join(runDirectory, runStopManifestFilename)); err != nil {
		t.Fatalf("duration stop did not retain its durable stopped-run authority: %v", err)
	}
	if modes := driver.backend.(*statefulBackend).ContainmentModes(); len(modes) != 1 || modes[0] != ports.StopForce {
		t.Fatalf("duration containment modes = %v, want [%s]", modes, ports.StopForce)
	}
}

func TestStopRunPropagatesRequestedBackendMode(t *testing.T) {
	for _, mode := range []ports.StopMode{ports.StopGraceful, ports.StopImmediate, ports.StopForce} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			lease, _ := domain.NewLeaseID()
			target, _ := domain.NewTargetID()
			driver, _ := materializationTestDriver(t, lease, target, newRecordingFileGateway("127.0.0.1:6567"))
			plan := targetRunPlanForMaterial(t, lease, target, nil, "android-stop-mode-"+string(mode))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := driver.PrepareRun(ctx, plan); err != nil {
				t.Fatal(err)
			}
			if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
				t.Fatal(err)
			}
			if _, err := driver.StopRun(ctx, plan.Run.ID(), mode); err != nil {
				t.Fatal(err)
			}
			if modes := driver.backend.(*statefulBackend).ContainmentModes(); len(modes) != 1 || modes[0] != mode {
				t.Fatalf("backend containment modes = %v, want [%s]", modes, mode)
			}
		})
	}
}

func TestStopWhileCollectorsWaitPreventsLateStart(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6561")
	driver, _ := materializationTestDriver(t, lease, target, files)
	collectorEntered := make(chan struct{})
	driver.collectors = CollectorReadinessFunc(func(ctx context.Context, _ domain.TargetRunID, _ []ports.ObservationRequirement) error {
		close(collectorEntered)
		<-ctx.Done()
		return ctx.Err()
	})
	plan := targetRunPlanForMaterial(t, lease, target, []ports.TargetMaterialPlan{targetMaterial(t, "input.bin", 0o600, []byte("input"), nil)}, "android-stop-start-race")
	plan.RequiredCoverage = []string{"android.process"}
	plan.Collectors = []ports.CollectorSpec{requiredCollectorSpec("android-process", "android.process")}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- driver.StartRun(ctx, plan.Run.ID()) }()
	select {
	case <-collectorEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopImmediate)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, plan, ports.RunFailed, ports.TargetRunFailureNeverStarted)
	select {
	case err := <-startResult:
		if err == nil {
			t.Fatal("run started after it was stopped during collector readiness")
		}
	case <-ctx.Done():
		t.Fatal("collector readiness was not cancelled by stop")
	}
	driver.mu.Lock()
	started := driver.runs[plan.Run.ID().String()].started
	driver.mu.Unlock()
	if started {
		t.Fatal("stopped run was committed as started")
	}
}

func TestConcurrentStopCleansRunOnceAndCancelsDuration(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6562")
	driver, _ := materializationTestDriver(t, lease, target, files)
	plan := targetRunPlanForMaterial(t, lease, target, []ports.TargetMaterialPlan{targetMaterial(t, "input.bin", 0o600, []byte("input"), nil)}, "android-concurrent-stop")
	plan.MaximumDuration = 80 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	const callers = 8
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopGraceful)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * plan.MaximumDuration)
	result, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, plan, ports.RunCompleted, ports.TargetRunFailureNone)
	files.mu.Lock()
	removed := files.removed
	files.mu.Unlock()
	if removed != 0 {
		t.Fatalf("concurrent/idempotent stop mutated the preserved guest %d times", removed)
	}
}

func TestAndroidStopRetriesFailedEndpointRevocationAfterContainment(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6563")
	driver, _ := materializationTestDriver(t, lease, target, files)
	allocation := driver.targets[deviceKey(target, 1)].instance.Allocation
	retryingEndpoint := &retryingScopedADBEndpoint{serial: allocation.Serial, address: "127.0.0.1:19002", remainingFailures: 1}
	gateway := &fixedEndpointGateway{expectedAllocation: allocation, endpoint: retryingEndpoint}
	driver.gateway = gateway
	plan := targetRunPlanForMaterial(t, lease, target, nil, "android-stop-close-retry")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	gateway.expectedScope = driver.runs[plan.Run.ID().String()].scope
	driver.mu.Unlock()
	if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	transport, err := driver.OpenTransport(ctx, plan.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.OpenADB(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce); !domain.IsCode(err, domain.CodeUnavailable) {
		t.Fatalf("first stop close failure = %v", err)
	}
	driver.mu.Lock()
	stoppedAfterFailure := driver.runs[plan.Run.ID().String()].stopped
	driver.mu.Unlock()
	if stoppedAfterFailure {
		t.Fatal("run receipt was sealed before endpoint revocation succeeded")
	}
	receipt, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatalf("retry stop: %v", err)
	}
	assertStopReceipt(t, receipt, plan, ports.RunCompleted, ports.TargetRunFailureNone)
	closeAttempts := retryingEndpoint.Attempts()
	if closeAttempts != 2 {
		t.Fatalf("endpoint close attempts = %d, want 2", closeAttempts)
	}
	files.mu.Lock()
	removed := files.removed
	files.mu.Unlock()
	if removed != 0 {
		t.Fatalf("endpoint-close retry mutated the preserved guest %d times", removed)
	}
	backend := driver.backend.(*statefulBackend)
	quarantines := 0
	for _, action := range backend.Actions() {
		if action == "quarantine:"+allocation.InstanceName {
			quarantines++
		}
	}
	if quarantines != 1 {
		t.Fatalf("retry repeated or skipped backend containment: %v", backend.Actions())
	}
}

func TestAndroidStopContainsGuestBeforeWaitingForBlockedTransfer(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6565")
	driver, _ := materializationTestDriver(t, lease, target, files)
	plan := targetRunPlanForMaterial(t, lease, target, nil, "android-stop-blocked-transfer")
	setupContext, cancelSetup := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelSetup()
	if _, err := driver.PrepareRun(setupContext, plan); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(setupContext, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	blockingFiles := &blockingPutFileGateway{ScopedFileGateway: files, entered: make(chan struct{}), release: make(chan struct{})}
	driver.files = blockingFiles
	transport, err := driver.OpenTransport(setupContext, plan.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	scope := driver.runs[plan.Run.ID().String()].scope
	allocation := driver.runs[plan.Run.ID().String()].allocation
	driver.mu.Unlock()
	content := []byte("blocked Android transfer")
	pushPlan := androidTransferPlan(t, scope, domain.TargetOperationPush, "blocked.bin", int64(len(content)), domain.NewDigest(content))
	pushDone := make(chan error, 1)
	go func() {
		_, err := transport.PushFile(setupContext, pushPlan, bytes.NewReader(content))
		pushDone <- err
	}()
	<-blockingFiles.entered
	stopContext, cancelStop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	started := time.Now()
	_, stopErr := driver.StopRun(stopContext, plan.Run.ID(), ports.StopForce)
	cancelStop()
	if !domain.IsCode(stopErr, domain.CodeUnavailable) {
		t.Fatalf("blocked transfer stop error = %v", stopErr)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("blocked transfer made StopRun ignore its deadline for %s", elapsed)
	}
	backend := driver.backend.(*statefulBackend)
	if backend.Reachable(allocation.Serial) {
		t.Fatal("StopRun returned with the exact Android guest still executing")
	}
	close(blockingFiles.release)
	if err := <-pushDone; err == nil {
		t.Fatal("revoked blocked transfer reported success")
	}
	retryContext, cancelRetry := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRetry()
	if _, err := driver.StopRun(retryContext, plan.Run.ID(), ports.StopForce); err != nil {
		t.Fatalf("retry stop after blocked transfer drained: %v", err)
	}
}

func TestAndroidStopNeverMutatesGuestAfterContainment(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6566")
	driver, _ := materializationTestDriver(t, lease, target, files)
	plan := targetRunPlanForMaterial(t, lease, target, nil, "android-stop-preserves-files")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	allocation := driver.targets[deviceKey(target, 1)].instance.Allocation
	backend := driver.backend.(*statefulBackend)
	if _, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce); err != nil {
		t.Fatal(err)
	}
	files.mu.Lock()
	removeAttempts := files.removed
	files.mu.Unlock()
	if backend.Reachable(allocation.Serial) || removeAttempts != 0 {
		t.Fatalf("stop did not preserve its containment boundary: remove_attempts=%d backend=%v", removeAttempts, backend.Actions())
	}
}

func TestAndroidStopReportsExactPushesOrRootOpaqueADBMutation(t *testing.T) {
	for _, test := range []struct {
		name    string
		openADB bool
	}{
		{name: "exact-scoped-push"},
		{name: "arbitrary-adb-authority", openADB: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lease, _ := domain.NewLeaseID()
			target, _ := domain.NewTargetID()
			files := newRecordingFileGateway("127.0.0.1:6564")
			driver, _ := materializationTestDriver(t, lease, target, files)
			allocation := driver.targets[deviceKey(target, 1)].instance.Allocation
			gateway := &recordingEndpointGateway{expectedAllocation: allocation}
			driver.gateway = gateway
			plan := targetRunPlanForMaterial(t, lease, target, nil, "android-target-changes-"+test.name)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := driver.PrepareRun(ctx, plan); err != nil {
				t.Fatal(err)
			}
			driver.mu.Lock()
			gateway.expectedScope = driver.runs[plan.Run.ID().String()].scope
			driver.mu.Unlock()
			if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
				t.Fatal(err)
			}
			transport, err := driver.OpenTransport(ctx, plan.Run.ID())
			if err != nil {
				t.Fatal(err)
			}
			content := []byte("observed Android write")
			push := androidTransferPlan(t, gateway.expectedScope, domain.TargetOperationPush, "results/output.bin", 64, domain.NewDigest(content))
			if _, err := transport.PushFile(ctx, push, bytes.NewReader(content)); err != nil {
				t.Fatal(err)
			}
			if test.openADB {
				if _, err := transport.OpenADB(ctx); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce)
			if err != nil {
				t.Fatal(err)
			}
			entries := receipt.TargetChanges.Entries()
			if len(entries) != 1 {
				t.Fatalf("target changes = %#v", entries)
			}
			spec := entries[0].Spec()
			if test.openADB {
				if spec.Kind != domain.ChangeOpaqueDirectory || spec.Path != "." || spec.Metadata["mutation_coverage"] != "opaque" {
					t.Fatalf("arbitrary ADB changes were narrowed: %#v", spec)
				}
			} else if spec.Kind != domain.ChangeAdded || spec.AfterDigest != domain.NewDigest(content) || spec.Path != "data/local/tmp/world/runs/"+target.String()+"/g1/"+plan.Run.ID().String()+"/writable/results/output.bin" {
				t.Fatalf("exact scoped push change = %#v", spec)
			}
			if !hasRunObservation(receipt.Observations, "target.transfer.succeeded", push.Operation.ID()) {
				t.Fatalf("push operation evidence missing: %#v", receipt.Observations)
			}
			if test.openADB && !hasRunObservation(receipt.Observations, "target.adb.authority-issued", domain.TargetOperationID{}) {
				t.Fatalf("ADB authority evidence missing: %#v", receipt.Observations)
			}
		})
	}
}

func hasRunObservation(observations []ports.TargetRunObservation, kind string, operationID domain.TargetOperationID) bool {
	for _, observation := range observations {
		if observation.Kind == kind && observation.TargetOperationID == operationID {
			return true
		}
	}
	return false
}

func waitForAndroidRunStopped(t *testing.T, driver *Driver, runID domain.TargetRunID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		driver.mu.Lock()
		run := driver.runs[runID.String()]
		stopped := run != nil && run.stopped
		driver.mu.Unlock()
		if stopped {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("run did not stop at its maximum duration")
}
