package cuttlefish

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAndroidReconciliationDistinguishesAbsentResetSuccessorFromPartialState(t *testing.T) {
	for _, test := range []struct {
		name       string
		prepare    func(*testing.T, string)
		hideLookup bool
		want       ports.PhysicalResourceClassification
	}{
		{name: "authoritatively absent", want: ports.PhysicalResourceMissing},
		{
			name: "empty state directory", want: ports.PhysicalResourceForeign,
			prepare: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "partial runtime manifest", want: ports.PhysicalResourceForeign,
			prepare: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, runtimePlanManifestFilename), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "allocator absence is not authoritative", hideLookup: true, want: ports.PhysicalResourceUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			port := findFreeEvenPortPair(t)
			allocator, err := NewDurableEmulatorAllocator(DurableEmulatorAllocatorConfig{
				StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = allocator.Close() })
			previous, build := reconciliationTargetPlan(t, root)
			reset := ports.ResetPlan{
				IdempotencyKey: "android-pending-reset", LeaseID: previous.LeaseID,
				Previous: ports.TargetRef{ID: previous.Target.ID(), Generation: 1}, NextGeneration: 2, Mode: ports.ResetRecreate,
			}
			successor := targetPlanAfterReset(t, previous, reset)
			directory := filepath.Join(build.TargetRoot, successor.Target.ID().String(), "generations", "2")
			if test.prepare != nil {
				test.prepare(t, directory)
			}
			backend := newStatefulBackend(Instance{})
			delete(backend.instances, "")
			var driverAllocator Allocator = allocator
			if test.hideLookup {
				driverAllocator = struct{ Allocator }{Allocator: allocator}
			}
			driver := reconciliationDriver(t, build, backend, driverAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
			reconcileContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			report, err := driver.ReconcileTargets(reconcileContext, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{successor}})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Expected) != 1 || report.Expected[0].Classification != test.want || report.Expected[0].RuntimeID != "" || report.Expected[0].PlanMatched {
				t.Fatalf("pending reset successor reconciliation = %#v, want %s with no runtime", report, test.want)
			}
		})
	}
}

func TestAndroidRestartReplaysDurableStopBeforeCoreFinalization(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = allocator.Close() })
	input, build := reconciliationTargetPlan(t, root)
	serial := emulatorAllocation(port).Serial
	files := newRecordingFileGateway(serial)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, files)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	created, err := driver.Create(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	material := []ports.TargetMaterialPlan{targetMaterial(t, "fixture/payload.txt", 0o600, []byte("durable stop bytes"), nil)}
	runPlan := targetRunPlanForMaterial(t, input.LeaseID, input.Target.ID(), material, "android-durable-stop-run")
	prepared, err := driver.PrepareRun(ctx, runPlan)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, runPlan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	original, err := driver.StopRun(ctx, runPlan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Join(build.TargetRoot, input.Target.ID().String(), "generations", "1", "runs", runPlan.Run.ID().String())
	stopPath := filepath.Join(runDirectory, runStopManifestFilename)
	stopBytes, err := os.ReadFile(stopPath)
	if err != nil {
		t.Fatalf("durable stopped-run boundary was not retained: %v", err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}

	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedAllocator.Close() })
	restarted := reconciliationDriver(t, build, backend, restartedAllocator, files)
	actionsBeforeReconciliation := len(backend.Actions())
	report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].RuntimeID != created.Status.RuntimeID {
		t.Fatalf("stopped target reconciliation report = %#v", report)
	}
	if actions := backend.Actions()[actionsBeforeReconciliation:]; containsArgument(actions, "inspect:") || containsArgument(actions, "start:") || containsArgument(actions, "quarantine:") {
		t.Fatalf("stopped-boundary reconciliation touched guest execution: %v", actions)
	}
	recovered, err := restarted.RecoverInterruptedRun(ctx, runPlan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, prepared) {
		t.Fatalf("recovered preparation = %#v, want %#v", recovered, prepared)
	}
	replayed, err := restarted.StopRun(ctx, runPlan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, original) {
		t.Fatalf("replayed durable stop receipt differs:\n got %#v\nwant %#v", replayed, original)
	}
	retainedBytes, err := os.ReadFile(stopPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(retainedBytes) != string(stopBytes) {
		t.Fatal("idempotent restart replay rewrote the immutable stopped-run boundary")
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	var tamperedStop runStopManifest
	if err := json.Unmarshal(stopBytes, &tamperedStop); err != nil {
		t.Fatal(err)
	}
	tamperedStop.Containment.RuntimeID = "foreign-runtime"
	tamperedBytes, err := json.Marshal(tamperedStop)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stopPath, append(tamperedBytes, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	tamperedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tamperedAllocator.Close() })
	tamperedDriver := reconciliationDriver(t, build, backend, tamperedAllocator, files)
	tamperedReport, err := tamperedDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tamperedReport.Conflicts) == 0 || len(tamperedReport.Unclaimed) != 0 {
		t.Fatalf("tampered stopped authority was not rejected: %#v", tamperedReport)
	}
	if err := tamperedDriver.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err == nil {
		t.Fatal("tampered stopped authority authorized orphan destruction")
	}
	if !backend.HasGeneration(1) {
		t.Fatal("tampered stopped authority reached backend destruction")
	}
	if err := tamperedDriver.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stopPath, stopBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	orphanAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orphanAllocator.Close() })
	orphanDriver := reconciliationDriver(t, build, backend, orphanAllocator, files)
	orphanReport, err := orphanDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(orphanReport.Unclaimed) != 1 || orphanReport.Unclaimed[0].Classification != ports.PhysicalResourceOrphan || orphanReport.Unclaimed[0].Ref.ID != input.Target.ID() || orphanReport.Unclaimed[0].Ref.Generation != 1 {
		t.Fatalf("stopped orphan reconciliation report = %#v", orphanReport)
	}
	if err := orphanDriver.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err == nil {
		t.Fatal("present orphan was destroyed without an exact cleanup-only plan")
	}
	changedCleanup := input
	changedCleanup.Template.GuestMemoryBytes = 3 << 30
	mismatch, err := orphanDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{changedCleanup}})
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatch.Expected) != 1 || mismatch.Expected[0].Classification != ports.PhysicalResourceForeign || mismatch.Expected[0].PlanMatched {
		t.Fatalf("changed cleanup plan acquired deletion authority: %#v", mismatch)
	}
	requireExactAndroidCleanupOnly(t, ctx, orphanDriver, input)
	if err := orphanDriver.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
		t.Fatal(err)
	}
	verifiedEmpty, err := orphanDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(verifiedEmpty.Unclaimed) != 0 || len(verifiedEmpty.Conflicts) != 0 {
		t.Fatalf("destroyed stopped orphan remains in authoritative inventory: %#v", verifiedEmpty)
	}
	if _, err := os.Stat(runDirectory); !os.IsNotExist(err) {
		t.Fatalf("destroy did not retire retained run authority: %v", err)
	}
	if _, err := orphanDriver.Create(ctx, input); err != nil {
		t.Fatal(err)
	}
	quarantinePlan := ports.TargetQuarantinePlan{
		IdempotencyKey: "android-restart-quarantine", Target: ports.TargetRef{ID: input.Target.ID(), Generation: 1},
		Reason: "exercise durable stopped quarantine adoption",
	}
	originalQuarantine, err := orphanDriver.Quarantine(ctx, quarantinePlan)
	if err != nil {
		t.Fatal(err)
	}
	if err := orphanDriver.Close(); err != nil {
		t.Fatal(err)
	}
	quarantineAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = quarantineAllocator.Close() })
	quarantineDriver := reconciliationDriver(t, build, backend, quarantineAllocator, files)
	quarantineReport, err := quarantineDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantineReport.Expected) != 1 || quarantineReport.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("durably quarantined expected reconciliation report = %#v", quarantineReport)
	}
	actionsBeforeReplay := len(backend.Actions())
	replayedQuarantine, err := quarantineDriver.Quarantine(ctx, quarantinePlan)
	if err != nil || replayedQuarantine != originalQuarantine {
		t.Fatalf("durable quarantine replay = %#v, %v; want %#v", replayedQuarantine, err, originalQuarantine)
	}
	changedReason := quarantinePlan
	changedReason.Reason = "a changed quarantine reason"
	if _, err := quarantineDriver.Quarantine(ctx, changedReason); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed quarantine reason error = %v", err)
	}
	changedKey := quarantinePlan
	changedKey.IdempotencyKey = "android-restart-quarantine-other-key"
	if _, err := quarantineDriver.Quarantine(ctx, changedKey); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed quarantine key error = %v", err)
	}
	if len(backend.Actions()) != actionsBeforeReplay {
		t.Fatalf("quarantine replay or conflict repeated backend work: %v", backend.Actions()[actionsBeforeReplay:])
	}
	if err := quarantineDriver.Close(); err != nil {
		t.Fatal(err)
	}
	orphanQuarantineAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orphanQuarantineAllocator.Close() })
	orphanQuarantineDriver := reconciliationDriver(t, build, backend, orphanQuarantineAllocator, files)
	orphanQuarantineReport, err := orphanQuarantineDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(orphanQuarantineReport.Unclaimed) != 1 || orphanQuarantineReport.Unclaimed[0].Classification != ports.PhysicalResourceOrphan {
		t.Fatalf("durably quarantined orphan reconciliation report = %#v", orphanQuarantineReport)
	}
	requireExactAndroidCleanupOnly(t, ctx, orphanQuarantineDriver, input)
	if err := orphanQuarantineDriver.Destroy(ctx, quarantinePlan.Target); err != nil {
		t.Fatal(err)
	}
	quarantineEmpty, err := orphanQuarantineDriver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantineEmpty.Unclaimed) != 0 || len(quarantineEmpty.Conflicts) != 0 {
		t.Fatalf("destroyed quarantined orphan remains in authoritative inventory: %#v", quarantineEmpty)
	}
	if err := orphanQuarantineDriver.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireExactAndroidCleanupOnly(t *testing.T, ctx context.Context, driver *Driver, plan ports.TargetPlan) {
	t.Helper()
	report, err := driver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{plan}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted ||
		!report.Expected[0].PlanMatched || report.Expected[0].RuntimeID == "" {
		t.Fatalf("exact cleanup-only Android plan was not matched: %#v", report)
	}
	key := deviceKey(plan.Target.ID(), plan.Generation.Spec().Generation)
	driver.mu.Lock()
	_, executable := driver.targets[key]
	_, cleanupAuthorized := driver.cleanupOnly[key]
	driver.mu.Unlock()
	if executable || !cleanupAuthorized {
		t.Fatalf("cleanup-only Android plan executable=%t cleanup_authorized=%t", executable, cleanupAuthorized)
	}
}

func TestAndroidActiveReconciliationRequiresWaitReadyButCleanupOnlyUsesInspect(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	input, build := reconciliationTargetPlan(t, root)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Create(ctx, input); err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	failing := &readinessErrorBackend{statefulBackend: backend, err: fmt.Errorf("injected full resource-readiness proof failure")}
	restarted := reconciliationDriver(t, build, failing, restartedAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	t.Cleanup(func() { _ = restarted.Close() })
	actionsBefore := len(backend.Actions())
	report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceUncertain || report.Expected[0].PlanMatched || len(report.Conflicts) != 0 || len(report.Unclaimed) != 0 {
		t.Fatalf("active reconciliation adopted an Inspect-only runtime: %#v", report)
	}
	if actions := backend.Actions()[actionsBefore:]; !reflect.DeepEqual(actions, []string{"ready-failed:"}) {
		t.Fatalf("active reconciliation did not require full WaitReady proof: %v", actions)
	}
	actionsBefore = len(backend.Actions())
	report, err = restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || !report.Expected[0].PlanMatched {
		t.Fatalf("cleanup-only reconciliation did not use non-mutating Inspect proof: %#v", report)
	}
	if actions := backend.Actions()[actionsBefore:]; !reflect.DeepEqual(actions, []string{"inspect:" + emulatorAllocation(port).InstanceName}) {
		t.Fatalf("cleanup-only reconciliation invoked the wrong readiness path: %v", actions)
	}
	if err := restarted.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestAndroidReconciliationProvesUnexpectedStoppedRuntimeWithoutRestart(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	input, build := reconciliationTargetPlan(t, root)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	created, err := driver.Create(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}

	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	stoppedBackend := &unexpectedStoppedBackend{
		readinessErrorBackend: &readinessErrorBackend{statefulBackend: backend, err: fmt.Errorf("exact process is absent")},
		proof: BackendQuarantineState{
			RuntimeID: created.Status.RuntimeID, ExecutionStopped: true, NetworkUnreachable: true,
			StatePreserved: true, ObservedAt: time.Now().UTC(),
		},
	}
	restarted := reconciliationDriver(t, build, stoppedBackend, restartedAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	t.Cleanup(func() { _ = restarted.Close() })

	active, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(active.Expected) != 1 || active.Expected[0].Classification != ports.PhysicalResourceUncertain ||
		!active.Expected[0].PlanMatched || active.Expected[0].RuntimeID != created.Status.RuntimeID ||
		len(active.Conflicts) != 0 || len(active.Unclaimed) != 0 {
		t.Fatalf("unexpected stopped active reconciliation = %#v", active)
	}
	if stoppedBackend.inspectStoppedCalls != 1 {
		t.Fatalf("stopped inspections = %d, want 1", stoppedBackend.inspectStoppedCalls)
	}
	key := deviceKey(input.Target.ID(), input.Generation.Spec().Generation)
	restarted.mu.Lock()
	record, executable := restarted.targets[key]
	restarted.mu.Unlock()
	if !executable || record.status.Ready || record.status.State != domain.TargetGenerationResettable {
		t.Fatalf("stopped recovery record = %#v, executable=%t", record.status, executable)
	}

	cleanup, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Expected) != 1 || cleanup.Expected[0].Classification != ports.PhysicalResourceAdopted ||
		!cleanup.Expected[0].PlanMatched || cleanup.Expected[0].RuntimeID != created.Status.RuntimeID {
		t.Fatalf("unexpected stopped cleanup reconciliation = %#v", cleanup)
	}
	if stoppedBackend.inspectStoppedCalls != 2 {
		t.Fatalf("stopped inspections = %d, want 2", stoppedBackend.inspectStoppedCalls)
	}
	if err := restarted.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
		t.Fatal(err)
	}
}

type unexpectedStoppedBackend struct {
	*readinessErrorBackend
	proof               BackendQuarantineState
	err                 error
	inspectStoppedCalls int
}

func (b *unexpectedStoppedBackend) InspectStopped(context.Context, Instance) (BackendQuarantineState, error) {
	b.inspectStoppedCalls++
	return b.proof, b.err
}

func (b *unexpectedStoppedBackend) Inspect(context.Context, Instance) (ReadinessState, error) {
	return ReadinessState{}, b.readinessErrorBackend.err
}

func TestAndroidMissingExactResidueRequiresCleanupForActiveAndCleanupOnlyPlans(t *testing.T) {
	for _, active := range []bool{true, false} {
		name := "cleanup-only"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			port := findFreeEvenPortPair(t)
			allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
			allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			input, build := reconciliationTargetPlan(t, root)
			backend := newStatefulBackend(Instance{})
			delete(backend.instances, "")
			driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			created, err := driver.Create(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			delete(backend.instances, created.Status.RuntimeID)
			backend.mu.Unlock()
			if err := driver.Close(); err != nil {
				t.Fatal(err)
			}
			restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			restarted := reconciliationDriver(t, build, backend, restartedAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
			request := ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}}
			if active {
				request = ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}}
			}
			report, err := restarted.ReconcileTargets(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceMissing ||
				report.Expected[0].PlanMatched || report.Expected[0].RuntimeID != "" || !report.Expected[0].CleanupRequired {
				t.Fatalf("missing exact residue reconciliation = %#v", report)
			}
			key := deviceKey(input.Target.ID(), input.Generation.Spec().Generation)
			restarted.mu.Lock()
			_, executable := restarted.targets[key]
			cleanup, cleanupAuthorized := restarted.cleanupOnly[key]
			restarted.mu.Unlock()
			if executable || !cleanupAuthorized || cleanup.runtimePresent {
				t.Fatalf("missing residue executable=%t cleanup=%t runtime_present=%t", executable, cleanupAuthorized, cleanup.runtimePresent)
			}
			actionsBefore := len(backend.Actions())
			if err := restarted.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
				t.Fatal(err)
			}
			if actions := backend.Actions()[actionsBefore:]; containsArgument(actions, "destroy:") {
				t.Fatalf("fresh runtime absence still invoked backend destruction: %v", actions)
			}
			if _, err := os.Stat(filepath.Dir(filepath.Join(build.TargetRoot, input.Target.ID().String(), "generations", "1", targetPlanManifestFilename))); !os.IsNotExist(err) {
				t.Fatalf("exact local residue remains after cleanup-only destruction: %v", err)
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAndroidRestartReconcilesExactManifestAndRecoversInterruptedRunPreparedOnly(t *testing.T) {
	for _, test := range []struct {
		name    string
		started bool
	}{
		{name: "started-before-control-plane-loss", started: true},
		{name: "prepared-but-never-started", started: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			port := findFreeEvenPortPair(t)
			allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
			allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			input, build := reconciliationTargetPlan(t, root)
			serial := emulatorAllocation(port).Serial
			files := newRecordingFileGateway(serial)
			backend := newStatefulBackend(Instance{})
			delete(backend.instances, "")
			driver := reconciliationDriver(t, build, backend, allocator, files)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			created, err := driver.Create(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			if !created.Status.Ready || created.Status.DeviceSerial != serial {
				t.Fatalf("created target = %#v", created)
			}
			material := []ports.TargetMaterialPlan{targetMaterial(t, "fixture/payload.txt", 0o600, []byte("real interrupted bytes"), nil)}
			runPlan := targetRunPlanForMaterial(t, input.LeaseID, input.Target.ID(), material, "android-recovery-run")
			prepared, err := driver.PrepareRun(ctx, runPlan)
			if err != nil {
				t.Fatal(err)
			}
			var originalStartedAt time.Time
			if test.started {
				if err := driver.StartRun(ctx, runPlan.Run.ID()); err != nil {
					t.Fatal(err)
				}
				driver.mu.Lock()
				originalStartedAt = driver.runs[runPlan.Run.ID().String()].startedAt
				driver.mu.Unlock()
			}
			if err := allocator.Close(); err != nil {
				t.Fatal(err)
			}
			restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			restarted := reconciliationDriver(t, build, backend, restartedAllocator, files)
			report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].RuntimeID != created.Status.RuntimeID {
				t.Fatalf("reconciliation report = %#v", report)
			}
			actionsBeforeRecovery := len(backend.Actions())
			recovered, err := restarted.RecoverInterruptedRun(ctx, runPlan)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.RunID != prepared.RunID || recovered.PreparedAt.IsZero() {
				t.Fatalf("recovered prepared run = %#v", recovered)
			}
			restarted.mu.Lock()
			run := restarted.runs[runPlan.Run.ID().String()]
			started, starting, timer := run.started, run.starting, run.deadlineCancel
			controlPlaneLost, interruptedExecution, recoveredStartedAt := run.controlPlaneLost, run.interruptedExecution, run.startedAt
			restarted.mu.Unlock()
			if started || starting || timer != nil || !controlPlaneLost || interruptedExecution != test.started {
				t.Fatal("crash recovery resumed specimen execution or armed a duration timer")
			}
			if test.started && !recoveredStartedAt.Equal(originalStartedAt) {
				t.Fatalf("recovered start time = %s, want durable original %s", recoveredStartedAt, originalStartedAt)
			}
			if err := restarted.StartRun(ctx, runPlan.Run.ID()); !domain.IsCode(err, domain.CodeInvalidState) {
				t.Fatalf("control-plane-loss recovery was startable: %v", err)
			}
			recoveryActions := backend.Actions()[actionsBeforeRecovery:]
			if !containsArgument(recoveryActions, "quarantine:"+created.Status.RuntimeID) || containsArgument(recoveryActions, "start:"+created.Status.RuntimeID) {
				t.Fatalf("recovery did not leave the tainted guest stopped: %v", recoveryActions)
			}
			receipt, err := restarted.StopRun(ctx, runPlan.Run.ID(), ports.StopForce)
			if err != nil {
				t.Fatal(err)
			}
			if test.started {
				if receipt.Outcome != ports.RunFailed || receipt.FailureKind != ports.TargetRunFailureTarget || !receipt.StartedAt.Equal(originalStartedAt) || receipt.Observations[len(receipt.Observations)-1].Kind != "target.run.control-plane-failure" {
					t.Fatalf("started interrupted receipt = %#v", receipt)
				}
			} else if receipt.Outcome != ports.RunFailed || receipt.FailureKind != ports.TargetRunFailureNeverStarted || !receipt.StartedAt.IsZero() || receipt.Observations[len(receipt.Observations)-1].Kind != "target.run.never_started" {
				t.Fatalf("never-started interrupted receipt = %#v", receipt)
			}
			secondRun := targetRunPlanForMaterial(t, input.LeaseID, input.Target.ID(), nil, "android-recovery-second-run")
			if _, err := restarted.PrepareRun(ctx, secondRun); !domain.IsCode(err, domain.CodeFailedPrecondition) {
				t.Fatalf("restarted driver reused spent generation: %v", err)
			}
			if err := restarted.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAndroidRunPreparationConvergesAcrossEveryDurableCheckpoint(t *testing.T) {
	for _, checkpoint := range []prepareCheckpoint{
		prepareCheckpointIntentCommitted,
		prepareCheckpointMaterialized,
		prepareCheckpointGenerationClaimed,
	} {
		t.Run(string(checkpoint), func(t *testing.T) {
			root := t.TempDir()
			port := findFreeEvenPortPair(t)
			allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
			allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			input, build := reconciliationTargetPlan(t, root)
			files := newRecordingFileGateway(emulatorAllocation(port).Serial)
			backend := newStatefulBackend(Instance{})
			delete(backend.instances, "")
			driver := reconciliationDriver(t, build, backend, allocator, files)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := driver.Create(ctx, input); err != nil {
				t.Fatal(err)
			}
			material := []ports.TargetMaterialPlan{targetMaterial(t, "fixture/restart.txt", 0o600, []byte("durable preparation bytes"), nil)}
			runPlan := targetRunPlanForMaterial(t, input.LeaseID, input.Target.ID(), material, "android-prepare-checkpoint-"+string(checkpoint))
			interrupted := false
			driver.prepareCheckpoint = func(observed prepareCheckpoint) error {
				if !interrupted && observed == checkpoint {
					interrupted = true
					return fmt.Errorf("simulated controller loss after %s", observed)
				}
				return nil
			}
			if _, err := driver.PrepareRun(ctx, runPlan); !domain.IsCode(err, domain.CodeUnavailable) {
				t.Fatalf("checkpoint-interrupted PrepareRun error = %v", err)
			}
			changed := runPlan
			changed.MaximumDuration += time.Second
			if _, err := driver.PrepareRun(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed durable preparation replay error = %v", err)
			}
			if err := driver.Close(); err != nil {
				t.Fatal(err)
			}

			restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			restarted := reconciliationDriver(t, build, backend, restartedAllocator, files)
			report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
			if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
				t.Fatalf("preparation checkpoint reconciliation = %#v, %v", report, err)
			}
			prepared, err := restarted.RecoverInterruptedRun(ctx, runPlan)
			if err != nil {
				t.Fatalf("recover incomplete preparation: %v", err)
			}
			preparedCount := files.prepared
			replayed, err := restarted.PrepareRun(ctx, runPlan)
			if err != nil || !reflect.DeepEqual(replayed, prepared) {
				t.Fatalf("exact preparation replay = %#v, %v; want %#v", replayed, err, prepared)
			}
			if files.prepared != preparedCount {
				t.Fatal("exact preparation replay repeated guest materialization")
			}
			if _, err := restarted.StopRun(ctx, runPlan.Run.ID(), ports.StopForce); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAndroidCreateConvergesAcrossEveryDurableCheckpoint(t *testing.T) {
	for _, checkpoint := range []createCheckpoint{
		createCheckpointAllocationReserved,
		createCheckpointIntentCommitted,
		createCheckpointRuntimeCreated,
		createCheckpointRuntimeReady,
		createCheckpointManifestsCommitted,
	} {
		t.Run(string(checkpoint), func(t *testing.T) {
			root := t.TempDir()
			port := findFreeEvenPortPair(t)
			allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
			allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			input, build := reconciliationTargetPlan(t, root)
			backend := newStatefulBackend(Instance{})
			delete(backend.instances, "")
			driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
			interrupted := false
			driver.createCheckpoint = func(observed createCheckpoint) error {
				if !interrupted && observed == checkpoint {
					interrupted = true
					return fmt.Errorf("simulated controller loss after %s", observed)
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := driver.Create(ctx, input); !domain.IsCode(err, domain.CodeUnavailable) {
				t.Fatalf("checkpoint-interrupted Create error = %v", err)
			}
			if err := driver.Close(); err != nil {
				t.Fatal(err)
			}

			restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
			if err != nil {
				t.Fatal(err)
			}
			restarted := reconciliationDriver(t, build, backend, restartedAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
			report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
			if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
				t.Fatalf("create checkpoint reconciliation = %#v, %v", report, err)
			}
			actions := backend.Actions()
			replayed, err := restarted.Create(ctx, input)
			if err != nil || replayed.Created || !replayed.Status.Ready || replayed.Status.RuntimeID != report.Expected[0].RuntimeID {
				t.Fatalf("durable Create replay = %#v, %v", replayed, err)
			}
			changed := input
			changed.Resources.CPUMilli++
			if _, err := restarted.Create(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed durable Create replay error = %v", err)
			}
			if len(backend.Actions()) != len(actions) {
				t.Fatalf("Create replay repeated backend work: %v", backend.Actions()[len(actions):])
			}
			if err := restarted.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 1}); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAndroidRuntimeCreatedRecoveryDoesNotCommitManifestsWithoutFullReadiness(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocatorConfig := DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	input, build := reconciliationTargetPlan(t, root)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	driver.createCheckpoint = func(observed createCheckpoint) error {
		if observed == createCheckpointRuntimeCreated {
			return fmt.Errorf("simulated controller loss after runtime creation")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Create(ctx, input); !domain.IsCode(err, domain.CodeUnavailable) {
		t.Fatalf("runtime-created interruption = %v", err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	failing := &readinessErrorBackend{statefulBackend: backend, err: fmt.Errorf("injected recovered runtime resource-proof failure")}
	restarted := reconciliationDriver(t, build, failing, restartedAllocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	t.Cleanup(func() { _ = restarted.Close() })
	if _, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}}); err == nil {
		t.Fatal("runtime-created recovery committed without full readiness/resource proof")
	}
	directory := filepath.Join(build.TargetRoot, input.Target.ID().String(), "generations", "1")
	for _, filename := range []string{targetPlanManifestFilename, runtimePlanManifestFilename} {
		if _, err := os.Lstat(filepath.Join(directory, filename)); !os.IsNotExist(err) {
			t.Fatalf("failed recovered readiness committed %s: %v", filename, err)
		}
	}
}

func TestAndroidReconciliationFailsClosedForRuntimeWithoutManifest(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocator, err := NewDurableEmulatorAllocator(DurableEmulatorAllocatorConfig{StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port})
	if err != nil {
		t.Fatal(err)
	}
	defer allocator.Close()
	input, build := reconciliationTargetPlan(t, root)
	allocation, err := allocator.Reserve(context.Background(), input.Target.ID(), 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildVirtualDevicePlan(input, build, allocation)
	if err != nil {
		t.Fatal(err)
	}
	instance := instanceFromPlan(plan)
	backend := newStatefulBackend(instance)
	driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(allocation.Serial))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceForeign {
		t.Fatalf("unmanifested live runtime was not rejected: %#v", report)
	}
}

func reconciliationDriver(t *testing.T, build BuildConfig, backend Backend, allocator Allocator, files ScopedFileGateway) *Driver {
	t.Helper()
	driver, err := New(Config{
		Build: build, Backend: backend, Allocator: allocator, Gateway: &recordingEndpointGateway{}, Files: files,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func reconciliationTargetPlan(t *testing.T, root string) (ports.TargetPlan, BuildConfig) {
	t.Helper()
	targetID, _ := domain.NewTargetID()
	leaseID, _ := domain.NewLeaseID()
	sessionID, _ := domain.NewResearchSessionID()
	createdAt := time.Now().UTC()
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := domain.NewDigest([]byte("reconcile-policy"))
	capabilityDigest := domain.NewDigest([]byte("reconcile-capability"))
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: 1, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	imageDigest := domain.NewDigest([]byte("reconcile-image"))
	input := ports.TargetPlan{
		IdempotencyKey: "android-reconcile-create", LeaseID: leaseID, Target: target, Generation: generation,
		Template:     completeAndroidTemplate("android-reconcile", "android-emulator", imageDigest),
		PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
		Resources: admission.Resources{CPUMilli: 1000, MemoryBytes: 1 << 30, StorageBytes: 512 << 20},
	}
	build := BuildConfig{
		TargetRoot: filepath.Join(root, "targets"), SystemImageRoot: filepath.Join(root, "images"),
		ADBServerEndpoint: DefaultADBServerEndpoint,
		BackendVersion:    "cvd-test", RuntimeVersion: "aosp-test", DeviceConfigDigest: domain.NewDigest([]byte("reconcile-config")), Features: []string{"root"},
	}
	return input, build
}
