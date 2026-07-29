package linuxcontainer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestRestartResumesDurableResetAcrossEveryPhysicalFaultWindow(t *testing.T) {
	tests := map[string]struct {
		previous bool
		next     bool
	}{
		"after intent before successor create":         {previous: true},
		"after successor create before old retirement": {previous: true, next: true},
		"after old retirement before logical ready":    {next: true},
	}
	for name, window := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newResetDurabilityFixture(t, name)
			if window.previous {
				fixture.runtime.seed(testRuntimeID("runtime-previous"), fixture.previous)
			}
			if window.next {
				fixture.runtime.seed(testRuntimeID("runtime-next"), fixture.next)
			}
			restarted, err := New(fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{fixture.expected}})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
				t.Fatalf("reset restart reconciliation = %#v", report)
			}
			result, err := restarted.Reset(targetDeadline(t), fixture.reset.Previous.ID, fixture.reset)
			if err != nil || result.Status.Generation != fixture.reset.NextGeneration || !result.Created {
				t.Fatalf("durable reset replay = %#v, %v", result, err)
			}
			if _, err := fixture.runtime.Inspect(targetDeadline(t), testRuntimeID("runtime-previous")); err == nil {
				t.Fatal("reset recovery retained the retired runtime")
			}
			changedKey := fixture.reset
			changedKey.IdempotencyKey += "-other"
			if _, err := restarted.Reset(targetDeadline(t), fixture.reset.Previous.ID, changedKey); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed reset key error = %v, want conflict", err)
			}
			changedPayload := fixture.reset
			incidentID, err := domain.NewIncidentID()
			if err != nil {
				t.Fatal(err)
			}
			changedPayload.IncidentID = incidentID
			if _, err := restarted.Reset(targetDeadline(t), fixture.reset.Previous.ID, changedPayload); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed reset payload error = %v, want conflict", err)
			}
			if found, err := loadCanonicalLifecycleRecord(fixture.next.TargetDirectory, resetReceiptFile, &persistedResetReceipt{}); err != nil || !found {
				t.Fatalf("reconstructed reset receipt missing: found=%v err=%v", found, err)
			}
		})
	}
}

func TestRestartFailsClosedOnCorruptResetIntent(t *testing.T) {
	fixture := newResetDurabilityFixture(t, "corrupt")
	restarted, err := New(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.next.TargetDirectory, resetIntentFile)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(payload, ' '), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.seed(testRuntimeID("runtime-next"), fixture.next)
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{fixture.expected}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("corrupt reset state was adopted: %#v", report)
	}
}

func TestRestartAdoptsExactDurablyQuarantinedTargetAndRebuildsReplay(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("quarantine-restart-image")))
	config := Config{
		Build: BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target"}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	}
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	created, err := first.Create(targetDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	quarantine := ports.TargetQuarantinePlan{
		IdempotencyKey: "quarantine-restart", Target: ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation}, Reason: "preserve exact evidence",
	}
	evidence, err := first.Quarantine(targetDeadline(t), quarantine)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected[0].Classification != ports.PhysicalResourceAdopted || runtime.guestCallCount() != 1 {
		// The one guest call belongs to initial Create; quarantine adoption must
		// not execute inside the preserved target.
		t.Fatalf("quarantine restart reconciliation = %#v, guest calls=%d", report, runtime.guestCallCount())
	}
	record, err := restarted.requireTarget(quarantine.Target.ID, quarantine.Target.Generation)
	if err != nil || record.status.State != domain.TargetGenerationQuarantined || record.status.Ready {
		t.Fatalf("adopted quarantine record = %#v, %v", record, err)
	}
	replayed, err := restarted.Quarantine(targetDeadline(t), quarantine)
	if err != nil || replayed != evidence {
		t.Fatalf("quarantine replay = %#v, %v; want %#v", replayed, err, evidence)
	}
	changed := quarantine
	changed.IdempotencyKey = "quarantine-restart-other"
	if _, err := restarted.Quarantine(targetDeadline(t), changed); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed quarantine replay error = %v, want conflict", err)
	}
	if _, err := os.Stat(filepath.Join(record.plan.TargetDirectory, quarantineReceiptFile)); err != nil {
		t.Fatalf("quarantine evidence was not preserved: %v", err)
	}
	if err := restarted.Destroy(targetDeadline(t), quarantine.Target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(record.plan.TargetDirectory); !os.IsNotExist(err) {
		t.Fatalf("explicit destroy did not clean quarantined generation state: %v", err)
	}
}

func TestRestartQuarantineIntentRecoveryIsCompleteAndFailClosed(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid"
		if corrupt {
			name = "corrupt"
		}
		t.Run(name, func(t *testing.T) {
			runtime := newInventoryRuntime()
			input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("quarantine-intent-image/"+name)))
			first, restarted, plan := restartTargetDrivers(t, runtime, input)
			if err := prepareTargetDirectories(first.build.TargetRoot, plan); err != nil {
				t.Fatal(err)
			}
			runtime.seed(testRuntimeID("runtime-intent"), plan)
			quarantine := ports.TargetQuarantinePlan{
				IdempotencyKey: "quarantine-intent", Target: ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}, Reason: "crash before containment",
			}
			intent, err := newQuarantineIntent(quarantine, targetRecord{input: input, plan: plan, runtimeID: testRuntimeID("runtime-intent")})
			if err != nil {
				t.Fatal(err)
			}
			if err := persistQuarantineIntent(plan.TargetDirectory, intent); err != nil {
				t.Fatal(err)
			}
			if corrupt {
				path := filepath.Join(plan.TargetDirectory, quarantineIntentFile)
				payload, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(payload, ' '), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
			if err != nil {
				t.Fatal(err)
			}
			want := ports.PhysicalResourceAdopted
			if corrupt {
				want = ports.PhysicalResourceUncertain
			}
			if report.Expected[0].Classification != want {
				t.Fatalf("intent-only quarantine reconciliation = %#v", report)
			}
			state, err := runtime.Inspect(targetDeadline(t), testRuntimeID("runtime-intent"))
			if err != nil {
				t.Fatal(err)
			}
			if corrupt {
				if !state.Running {
					t.Fatal("corrupt quarantine authority was used to mutate the runtime")
				}
				return
			}
			if state.Running {
				t.Fatalf("intent-only quarantine left execution active: %#v", state)
			}
			if found, err := loadCanonicalLifecycleRecord(plan.TargetDirectory, quarantineReceiptFile, &persistedQuarantineReceipt{}); err != nil || !found {
				t.Fatalf("quarantine receipt was not reconstructed: found=%v err=%v", found, err)
			}
		})
	}
}

func TestStopRunReturnsWithinDeadlineForPermanentlyBlockingPushReader(t *testing.T) {
	runtime := &recordingRuntime{plans: make(map[string]ContainerPlan)}
	driver, authority := lifecycleTestDriverWithRuntime(t, runtime)
	if err := driver.StartRun(targetDeadline(t), authority.RunID); err != nil {
		t.Fatal(err)
	}
	record := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	scoped := &targetTransport{driver: driver, runtime: runtime, runtimeID: record.runtimeID, root: record.plan.writableRoot(), authority: authority}
	driver.runs[authority.RunID.String()].transports[scoped] = struct{}{}
	reader := &permanentlyBlockingReader{entered: make(chan struct{}), release: make(chan struct{})}
	pushDone := make(chan error, 1)
	pushContext, cancelPush := context.WithTimeout(context.Background(), time.Second)
	defer cancelPush()
	pushPlan := transferPlan(t, authority, domain.TargetOperationPush, "blocked.bin", 64)
	go func() {
		_, err := scoped.PushFile(pushContext, pushPlan, reader)
		pushDone <- err
	}()
	<-reader.entered
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	started := time.Now()
	_, stopErr := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	cancel()
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("StopRun wedged for %v on an uncooperative reader", elapsed)
	}
	if stopErr == nil {
		t.Fatal("deadline-limited transport drain unexpectedly completed")
	}
	if runtime.IsRunning(record.runtimeID) {
		t.Fatal("runtime was not contained before the transport drain deadline")
	}
	close(reader.release)
	if err := <-pushDone; err == nil {
		t.Fatal("revoked blocking push succeeded")
	}
}

type permanentlyBlockingReader struct {
	entered chan struct{}
	release chan struct{}
}

type resetDurabilityFixture struct {
	runtime  *inventoryRuntime
	config   Config
	initial  ports.TargetPlan
	expected ports.TargetPlan
	reset    ports.ResetPlan
	previous ContainerPlan
	next     ContainerPlan
}

func newResetDurabilityFixture(t *testing.T, label string) resetDurabilityFixture {
	t.Helper()
	runtime := newInventoryRuntime()
	initial, _ := dockerTargetFixture(t, domain.NewDigest([]byte("durable-reset-image/"+label)))
	config := Config{
		Build: BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target"}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
		Now:        func() time.Time { return time.Unix(90, 0).UTC() },
	}
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := BuildContainerPlan(initial, driver.build)
	if err != nil {
		t.Fatal(err)
	}
	reset := ports.ResetPlan{
		IdempotencyKey: "durable-reset", LeaseID: initial.LeaseID,
		Previous:       ports.TargetRef{ID: previous.TargetID, Generation: previous.Generation},
		NextGeneration: previous.Generation + 1, Mode: ports.ResetRecreate,
	}
	next, err := replacementContainerPlan(previous, reset.NextGeneration, driver.build.TargetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareTargetDirectories(driver.build.TargetRoot, previous); err != nil {
		t.Fatal(err)
	}
	if err := prepareTargetDirectories(driver.build.TargetRoot, next); err != nil {
		t.Fatal(err)
	}
	intent, err := newResetIntent(reset, targetRecord{input: initial, plan: previous, runtimeID: testRuntimeID("runtime-previous")}, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := persistResetIntent(next.TargetDirectory, intent); err != nil {
		t.Fatal(err)
	}
	return resetDurabilityFixture{
		runtime: runtime, config: config, initial: initial, expected: successorTargetPlan(t, initial, reset),
		reset: reset, previous: previous, next: next,
	}
}

func (r *permanentlyBlockingReader) Read([]byte) (int, error) {
	select {
	case <-r.entered:
	default:
		close(r.entered)
	}
	<-r.release
	return 0, io.EOF
}

func successorTargetPlan(t *testing.T, previous ports.TargetPlan, reset ports.ResetPlan) ports.TargetPlan {
	t.Helper()
	at := previous.Generation.UpdatedAt().Add(time.Second)
	target, err := previous.Target.AdvanceGeneration(previous.Target.Revision(), reset.NextGeneration, at)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: reset.Previous.ID, Generation: reset.NextGeneration,
		PolicyDigest: previous.PolicyDigest, CapabilityFingerprintDigest: previous.CapabilityFingerprintDigest,
		PreviousGeneration: reset.Previous.Generation, RecoveryIncidentID: reset.IncidentID, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	next := previous
	next.IdempotencyKey = "successor-target-plan"
	next.Target = target
	next.Generation = generation
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	return next
}
