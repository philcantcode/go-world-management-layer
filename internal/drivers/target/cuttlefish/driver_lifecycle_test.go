package cuttlefish

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestMaximumRunDurationStopsRunRevokesTransportAndCleansFiles(t *testing.T) {
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
	if removed != 1 {
		t.Fatalf("device run cleanup calls = %d, want 1", removed)
	}
	runDirectory := filepath.Join(stateDirectory, "runs", plan.Run.ID().String())
	if _, err := os.Stat(runDirectory); !os.IsNotExist(err) {
		t.Fatalf("host run directory remains after duration stop: %v", err)
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
	if removed != 1 {
		t.Fatalf("concurrent/idempotent stop performed %d device cleanups", removed)
	}
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
