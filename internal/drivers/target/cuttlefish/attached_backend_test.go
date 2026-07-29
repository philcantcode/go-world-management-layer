package cuttlefish

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAttachedEmulatorBackendProbesOnlyExactSerial(t *testing.T) {
	runner := newAttachedADBRunner("emulator-5554")
	runner.properties["ro.boot.qemu.avd_name"] = "World_Test"
	runner.properties["ro.product.model"] = "sdk_gphone64_x86_64"
	backend, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{
		Runner: runner, Serial: "emulator-5554",
		ExpectedProperties: map[string]string{"ro.product.model": "sdk_gphone64_x86_64"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	capabilities, err := backend.Probe(ctx, ports.TargetTemplate{})
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.BackendKind != "android-sdk-emulator" || capabilities.BackendVersion != defaultAttachedEmulatorBackendVersion || capabilities.RuntimeVersion != runner.properties["ro.build.fingerprint"] {
		t.Fatalf("attached capabilities = %#v", capabilities)
	}
	if capabilities.KVMKnown || capabilities.KVM || len(capabilities.ResetModes) != 0 {
		t.Fatalf("attached backend invented host acceleration or reset support: %#v", capabilities)
	}
	if capabilities.Evidence["adb_serial"] != "emulator-5554" || capabilities.Evidence["avd_name"] != "World_Test" {
		t.Fatalf("attached evidence = %#v", capabilities.Evidence)
	}
	runner.requireExactSerial(t)
}

func TestAttachedEmulatorBackendLifecycleOnlyAttachesAndDetaches(t *testing.T) {
	runner := newAttachedADBRunner("emulator-5554")
	backend, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{Runner: runner, Serial: "emulator-5554", PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	allocation := Allocation{InstanceNumber: 5554, InstanceName: "attached-emulator-5554", Serial: "emulator-5554", ADBAddress: "emulator-5554"}
	plan := VirtualDevicePlan{StateDirectory: "state", SystemImageDirectory: "image", Allocation: allocation, ADBServer: ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5037}, Fingerprint: ResetFingerprint{}}
	instance, err := backend.Create(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := backend.Start(ctx, instance); err != nil {
		t.Fatal(err)
	}
	state, err := backend.WaitReady(ctx, instance)
	if err != nil || !state.Ready() {
		t.Fatalf("readiness = %#v, %v", state, err)
	}
	beforeDetach := runner.callCount()
	if err := backend.Stop(ctx, instance, ports.StopForce); err != nil {
		t.Fatal(err)
	}
	if err := backend.Destroy(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if runner.callCount() != beforeDetach {
		t.Fatal("detach invoked ADB and could have mutated the external emulator")
	}
	wrong := instance
	wrong.Allocation.Serial = "emulator-5556"
	if err := backend.Destroy(ctx, wrong); err == nil {
		t.Fatal("backend accepted an instance for another serial")
	}
	runner.requireExactSerial(t)
}

func TestAttachedEmulatorBackendRejectsPropertyMismatchAndUnsafeNames(t *testing.T) {
	runner := newAttachedADBRunner("emulator-5554")
	backend, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{
		Runner: runner, Serial: "emulator-5554", ExpectedProperties: map[string]string{"ro.product.model": "different"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := backend.Probe(ctx, ports.TargetTemplate{}); err == nil {
		t.Fatal("probe accepted a mismatched emulator identity property")
	}
	if _, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{
		Runner: runner, Serial: "emulator-5554", ExpectedProperties: map[string]string{"ro.product.model;id": "x"},
	}); err == nil {
		t.Fatal("unsafe remote shell property name was accepted")
	}
}

func TestAttachedEmulatorAllocatorHasOneExactOwner(t *testing.T) {
	allocator, err := NewAttachedEmulatorAllocator("emulator-5554")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := domain.NewTargetID()
	second, _ := domain.NewTargetID()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	allocation, err := allocator.Reserve(ctx, first, 1)
	if err != nil {
		t.Fatal(err)
	}
	if allocation.Serial != "emulator-5554" || allocation.ADBAddress != "emulator-5554" {
		t.Fatalf("allocation = %#v", allocation)
	}
	if replay, err := allocator.Reserve(ctx, first, 1); err != nil || replay != allocation {
		t.Fatalf("same-owner replay = %#v, %v", replay, err)
	}
	if _, err := allocator.Reserve(ctx, second, 1); err == nil {
		t.Fatal("one attached emulator was assigned to two target generations")
	}
	if err := allocator.Release(ctx, allocation); err != nil {
		t.Fatal(err)
	}
	if _, err := allocator.Reserve(ctx, second, 1); err != nil {
		t.Fatalf("released attached emulator was not reusable: %v", err)
	}
}

func TestAttachedEmulatorIntegration(t *testing.T) {
	if os.Getenv("WORLD_ANDROID_EMULATOR_INTEGRATION") != "1" {
		t.Skip("set WORLD_ANDROID_EMULATOR_INTEGRATION=1 to probe a running SDK emulator")
	}
	serial := os.Getenv("WORLD_ANDROID_EMULATOR_SERIAL")
	if serial == "" {
		serial = "emulator-5554"
	}
	backend, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{Serial: serial})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	capabilities, err := backend.Probe(ctx, ports.TargetTemplate{})
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Evidence["adb_serial"] != serial {
		t.Fatalf("probe selected %q, want %q", capabilities.Evidence["adb_serial"], serial)
	}
}

type attachedADBRunner struct {
	mu         sync.Mutex
	serial     string
	properties map[string]string
	calls      []command.Invocation
}

func newAttachedADBRunner(serial string) *attachedADBRunner {
	return &attachedADBRunner{serial: serial, properties: map[string]string{
		"ro.kernel.qemu":        "1",
		"sys.boot_completed":    "1",
		"init.svc.bootanim":     "stopped",
		"ro.build.fingerprint":  "google/sdk_gphone64_x86_64/test:userdebug/test-keys",
		"ro.build.version.sdk":  "35",
		"ro.product.cpu.abi":    "x86_64",
		"ro.serialno":           "EMULATOR5554",
		"ro.debuggable":         "1",
		"ro.secure":             "0",
		"ro.boot.qemu.avd_name": "",
		"ro.product.model":      "sdk_gphone64_x86_64",
	}}
}

func (r *attachedADBRunner) Run(_ context.Context, invocation command.Invocation) (command.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, invocation)
	action, exact := exactADBTestAction(invocation.Args, DefaultADBServerEndpoint, r.serial)
	if !exact {
		return command.Result{}, os.ErrInvalid
	}
	switch action[0] {
	case "get-state":
		return command.Result{Stdout: []byte("device\n")}, nil
	case "emu":
		if !reflect.DeepEqual(action[1:], []string{"avd", "name"}) {
			return command.Result{}, os.ErrInvalid
		}
		return command.Result{Stdout: []byte("World_Test\nOK\n")}, nil
	case "shell":
		if len(action) == 3 && action[1] == "getprop" {
			return command.Result{Stdout: []byte(r.properties[action[2]] + "\n")}, nil
		}
		if reflect.DeepEqual(action[1:], []string{"id", "-u"}) {
			return command.Result{Stdout: []byte("0\n")}, nil
		}
		if reflect.DeepEqual(action[1:], []string{"cmd", "package", "path", "android"}) {
			return command.Result{Stdout: []byte("package:/system/framework/framework-res.apk\n")}, nil
		}
		return command.Result{}, os.ErrInvalid
	default:
		return command.Result{}, os.ErrInvalid
	}
}

func (r *attachedADBRunner) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *attachedADBRunner) requireExactSerial(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		t.Fatal("ADB was not invoked")
	}
	for _, invocation := range r.calls {
		if _, exact := exactADBTestAction(invocation.Args, DefaultADBServerEndpoint, r.serial); invocation.Program != "adb" || !exact {
			t.Fatalf("non-exact ADB invocation: %#v", invocation)
		}
	}
}

var _ command.Runner = (*attachedADBRunner)(nil)
