package cuttlefish

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const defaultAttachedEmulatorBackendVersion = "attached-sdk-emulator"

// AttachedEmulatorBackendConfig identifies one Android SDK emulator which was
// started outside this process. The backend only observes and assigns that
// exact serial; it never starts, stops, or destroys the external process.
type AttachedEmulatorBackendConfig struct {
	Runner             command.Runner
	ADBBinary          string
	Serial             string
	PollInterval       time.Duration
	BackendVersion     string
	ExpectedProperties map[string]string
	Now                func() time.Time
}

type AttachedEmulatorBackend struct {
	runner             command.Runner
	adbBinary          string
	serial             string
	pollInterval       time.Duration
	backendVersion     string
	expectedProperties map[string]string
	now                func() time.Time
}

func NewAttachedEmulatorBackend(config AttachedEmulatorBackendConfig) (*AttachedEmulatorBackend, error) {
	if !safeExactADBSerial(config.Serial) {
		return nil, fmt.Errorf("attached emulator requires a safe exact ADB serial")
	}
	if config.Runner == nil {
		config.Runner = command.OS{}
	}
	if config.ADBBinary == "" {
		config.ADBBinary = "adb"
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 500 * time.Millisecond
	}
	if config.BackendVersion == "" {
		config.BackendVersion = defaultAttachedEmulatorBackendVersion
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	expected := make(map[string]string, len(config.ExpectedProperties))
	for name, value := range config.ExpectedProperties {
		if !safeAndroidPropertyName(name) {
			return nil, fmt.Errorf("expected Android property %q is unsafe", name)
		}
		expected[name] = value
	}
	return &AttachedEmulatorBackend{
		runner: config.Runner, adbBinary: config.ADBBinary, serial: config.Serial,
		pollInterval: config.PollInterval, backendVersion: config.BackendVersion,
		expectedProperties: expected, now: config.Now,
	}, nil
}

func (b *AttachedEmulatorBackend) Probe(ctx context.Context, _ ports.TargetTemplate) (BackendCapabilities, error) {
	instance := Instance{Allocation: Allocation{InstanceNumber: 1, InstanceName: "attached-probe", Serial: b.serial, ADBAddress: b.serial}}
	state, properties, err := b.inspectProperties(ctx, instance, true)
	if err != nil {
		return BackendCapabilities{}, err
	}
	if !state.Ready() {
		return BackendCapabilities{}, fmt.Errorf("attached emulator %q is not fully ready", b.serial)
	}
	evidence := map[string]string{
		"adb_serial":        b.serial,
		"build_fingerprint": properties["ro.build.fingerprint"],
		"sdk":               properties["ro.build.version.sdk"],
		"abi":               properties["ro.product.cpu.abi"],
	}
	if avdName := properties["ro.boot.qemu.avd_name"]; avdName != "" {
		evidence["avd_name"] = avdName
	}
	return BackendCapabilities{
		BackendKind: "android-sdk-emulator", BackendVersion: b.backendVersion,
		RuntimeVersion: properties["ro.build.fingerprint"], KVMKnown: false,
		ResetModes: nil, Evidence: evidence,
	}, nil
}

func (b *AttachedEmulatorBackend) Create(_ context.Context, plan VirtualDevicePlan) (Instance, error) {
	if err := b.requirePlanAllocation(plan.Allocation); err != nil {
		return Instance{}, err
	}
	return Instance{
		RuntimeID: plan.Allocation.InstanceName, StateDirectory: plan.StateDirectory,
		SystemImageDirectory: plan.SystemImageDirectory, Allocation: plan.Allocation,
		Fingerprint: plan.Fingerprint,
	}, nil
}

func (b *AttachedEmulatorBackend) Start(ctx context.Context, instance Instance) error {
	state, _, err := b.inspectProperties(ctx, instance, false)
	if err != nil {
		return err
	}
	if !state.ADBReady {
		return fmt.Errorf("attached emulator %q is not reachable", b.serial)
	}
	return nil
}

func (b *AttachedEmulatorBackend) WaitReady(ctx context.Context, instance Instance) (ReadinessState, error) {
	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	for {
		state, err := b.Inspect(ctx, instance)
		if err == nil && state.Ready() {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return ReadinessState{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *AttachedEmulatorBackend) Inspect(ctx context.Context, instance Instance) (ReadinessState, error) {
	state, _, err := b.inspectProperties(ctx, instance, false)
	return state, err
}

// Stop deliberately detaches without stopping the pre-existing emulator.
func (b *AttachedEmulatorBackend) Stop(ctx context.Context, instance Instance, mode ports.StopMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !mode.IsValid() {
		return fmt.Errorf("invalid stop mode %q", mode)
	}
	return b.requireInstance(instance)
}

// Destroy deliberately forgets ownership without destroying external state.
func (b *AttachedEmulatorBackend) Destroy(ctx context.Context, instance Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.requireInstance(instance)
}

func (b *AttachedEmulatorBackend) inspectProperties(ctx context.Context, instance Instance, includeIdentity bool) (ReadinessState, map[string]string, error) {
	if err := b.requireInstance(instance); err != nil {
		return ReadinessState{}, nil, err
	}
	state := ReadinessState{ObservedAt: b.now().UTC()}
	result, err := b.runADB(ctx, "get-state")
	if err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(string(result.Stdout)) != "device" {
		return state, nil, fmt.Errorf("attached emulator %q did not report device state", b.serial)
	}
	state.ProcessRunning, state.ADBReady = true, true

	names := map[string]struct{}{
		"ro.kernel.qemu": {}, "sys.boot_completed": {}, "init.svc.bootanim": {},
	}
	if includeIdentity {
		names["ro.build.fingerprint"] = struct{}{}
		names["ro.build.version.sdk"] = struct{}{}
		names["ro.product.cpu.abi"] = struct{}{}
		names["ro.boot.qemu.avd_name"] = struct{}{}
		for name := range b.expectedProperties {
			names[name] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	properties := make(map[string]string, len(ordered))
	for _, name := range ordered {
		value, propertyErr := b.getProperty(ctx, name)
		if propertyErr != nil {
			return state, nil, propertyErr
		}
		properties[name] = value
	}
	if properties["ro.kernel.qemu"] != "1" {
		return state, nil, fmt.Errorf("ADB serial %q is not an Android emulator", b.serial)
	}
	state.BootCompleted = properties["sys.boot_completed"] == "1"
	state.FrameworkReady = properties["init.svc.bootanim"] == "stopped"
	if includeIdentity {
		for _, name := range []string{"ro.build.fingerprint", "ro.build.version.sdk", "ro.product.cpu.abi"} {
			if properties[name] == "" {
				return state, nil, fmt.Errorf("attached emulator property %q is empty", name)
			}
		}
		for name, expected := range b.expectedProperties {
			if properties[name] != expected {
				return state, nil, fmt.Errorf("attached emulator property %q does not match the configured value", name)
			}
		}
	}
	return state, properties, nil
}

func (b *AttachedEmulatorBackend) getProperty(ctx context.Context, name string) (string, error) {
	result, err := b.runADB(ctx, "shell", "getprop", name)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(result.Stdout)), nil
}

func (b *AttachedEmulatorBackend) runADB(ctx context.Context, args ...string) (command.Result, error) {
	return runExactSerialADB(ctx, b.runner, b.adbBinary, b.serial, command.DefaultOutputLimit, args...)
}

func (b *AttachedEmulatorBackend) requirePlanAllocation(allocation Allocation) error {
	if err := allocation.Validate(); err != nil {
		return err
	}
	if allocation.Serial != b.serial || allocation.ADBAddress != b.serial {
		return fmt.Errorf("virtual-device plan does not identify attached emulator %q exactly", b.serial)
	}
	return nil
}

func (b *AttachedEmulatorBackend) requireInstance(instance Instance) error {
	return b.requirePlanAllocation(instance.Allocation)
}

func safeAndroidPropertyName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '.', '_', '-':
			continue
		default:
			return false
		}
	}
	return true
}

var _ Backend = (*AttachedEmulatorBackend)(nil)
