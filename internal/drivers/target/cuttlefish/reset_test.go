package cuttlefish

import (
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

func TestProbeExposesOnlyImplementedResetModes(t *testing.T) {
	driver, _, _, _, _, _ := resetTestDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fingerprint, err := driver.Probe(ctx, ports.TargetTemplate{
		Name: "android", Kind: domain.TargetAndroidVirtualDevice, Driver: "cuttlefish",
		ImageDigest: domain.NewDigest([]byte("system")), IsolationProfile: "android-vm",
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
	runID, _ := domain.NewTargetRunID()
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
		Resources: admission.Resources{}, Labels: map[string]string{"world.target-generation": "1"},
	}
	previous := Instance{RuntimeID: allocation.InstanceName, StateDirectory: plan.StateDirectory, SystemImageDirectory: plan.SystemImageDirectory, Allocation: allocation, Fingerprint: fingerprint}
	backend := newStatefulBackend(previous)
	driver := &Driver{
		build:   BuildConfig{TargetRoot: targetRoot, SystemImageRoot: imageRoot, BackendVersion: "cvd-test", RuntimeVersion: "aosp-test", DeviceConfigDigest: fingerprint.DeviceConfigDigest},
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
	instance := Instance{RuntimeID: plan.Allocation.InstanceName, StateDirectory: plan.StateDirectory, SystemImageDirectory: plan.SystemImageDirectory, Allocation: plan.Allocation, Fingerprint: plan.Fingerprint}
	b.instances[instance.RuntimeID] = &statefulInstance{instance: instance, generation: plan.Generation}
	return instance, nil
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

func (b *statefulBackend) Quarantine(_ context.Context, instance Instance) (BackendQuarantineState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actions = append(b.actions, "quarantine:"+instance.RuntimeID)
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
	return ReadinessState{ProcessRunning: true, ADBReady: true, BootCompleted: true, FrameworkReady: true, ObservedAt: time.Unix(2_000, 0).UTC()}
}

var _ Backend = (*statefulBackend)(nil)
