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

// AndroidIdentity contains only facts returned by the exact selected device.
// It is persisted with the runtime manifest and compared during adoption.
type AndroidIdentity struct {
	SerialNumber     string `json:"serial_number"`
	BuildFingerprint string `json:"build_fingerprint"`
	SDK              string `json:"sdk"`
	ABI              string `json:"abi"`
	AVDName          string `json:"avd_name,omitempty"`
	QEMU             bool   `json:"qemu"`
	Rooted           bool   `json:"rooted"`
	Debuggable       bool   `json:"debuggable"`
}

func (i AndroidIdentity) Validate() error {
	if strings.TrimSpace(i.SerialNumber) == "" || strings.TrimSpace(i.BuildFingerprint) == "" || strings.TrimSpace(i.SDK) == "" || strings.TrimSpace(i.ABI) == "" {
		return fmt.Errorf("Android serial number, build fingerprint, SDK, and ABI are required")
	}
	if !i.QEMU {
		return fmt.Errorf("exact device did not identify itself as an emulator")
	}
	return nil
}

type exactAndroidObservationConfig struct {
	Runner       command.Runner
	ADBBinary    string
	ADBServer    adbServerEndpoint
	Serial       string
	Now          func() time.Time
	ProcessProbe func(context.Context) (bool, error)
	Properties   []string
}

// observeExactAndroid is shared by every Android backend so exact device
// state, boot, framework, package-manager and identity checks cannot drift.
func observeExactAndroid(ctx context.Context, config exactAndroidObservationConfig) (ReadinessState, map[string]string, error) {
	if config.Runner == nil || config.ProcessProbe == nil || config.ADBServer.host == "" || config.ADBServer.port == "" || ports.ValidateExactADBSerial(config.Serial) != nil {
		return ReadinessState{}, nil, fmt.Errorf("runner, exact ADB server/serial, and process observation are required")
	}
	if config.ADBBinary == "" {
		config.ADBBinary = "adb"
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	state := ReadinessState{ObservedAt: config.Now().UTC()}
	running, err := config.ProcessProbe(ctx)
	if err != nil {
		return state, nil, fmt.Errorf("observe emulator process: %w", err)
	}
	state.ProcessRunning = running
	if !running {
		return state, nil, fmt.Errorf("exact emulator process is not running")
	}
	adb := func(args ...string) (command.Result, error) {
		return runExactSerialADBAt(ctx, config.Runner, config.ADBBinary, config.ADBServer, config.Serial, adbMetadataOutputLimit, args...)
	}
	device, err := adb("get-state")
	if err != nil {
		return state, nil, err
	}
	state.DeviceState = strings.TrimSpace(string(device.Stdout))
	if state.DeviceState != "device" {
		return state, nil, fmt.Errorf("exact ADB serial %q reported state %q, want device", config.Serial, state.DeviceState)
	}
	state.ADBReady = true

	names := []string{
		"init.svc.bootanim", "ro.boot.qemu.avd_name", "ro.build.fingerprint", "ro.build.version.sdk",
		"ro.debuggable", "ro.kernel.qemu", "ro.product.cpu.abi", "ro.secure", "ro.serialno", "sys.boot_completed",
	}
	names = append(names, config.Properties...)
	sort.Strings(names)
	properties := make(map[string]string, len(names))
	for _, name := range names {
		if !safeAndroidPropertyName(name) {
			return state, nil, fmt.Errorf("Android property %q is unsafe", name)
		}
		if _, duplicate := properties[name]; duplicate {
			continue
		}
		result, propertyErr := adb("shell", "getprop", name)
		if propertyErr != nil {
			return state, nil, fmt.Errorf("observe Android property %q: %w", name, propertyErr)
		}
		properties[name] = strings.TrimSpace(string(result.Stdout))
	}
	identity := AndroidIdentity{
		SerialNumber: properties["ro.serialno"], BuildFingerprint: properties["ro.build.fingerprint"],
		SDK: properties["ro.build.version.sdk"], ABI: properties["ro.product.cpu.abi"],
		AVDName: properties["ro.boot.qemu.avd_name"], QEMU: properties["ro.kernel.qemu"] == "1",
		Debuggable: properties["ro.debuggable"] == "1",
	}
	root, rootErr := adb("shell", "id", "-u")
	if rootErr == nil {
		identity.Rooted = strings.TrimSpace(string(root.Stdout)) == "0"
	}
	if err := identity.Validate(); err != nil {
		return state, properties, err
	}
	state.Identity = identity
	state.BootCompleted = properties["sys.boot_completed"] == "1"
	state.FrameworkReady = properties["init.svc.bootanim"] == "stopped"
	packages, packageErr := adb("shell", "cmd", "package", "path", "android")
	if packageErr == nil {
		state.PackageManagerReady = strings.Contains(strings.TrimSpace(string(packages.Stdout)), "package:")
	}
	if packageErr != nil {
		return state, properties, fmt.Errorf("observe Android package manager: %w", packageErr)
	}
	return state, properties, nil
}

func observeSDKEmulatorProcess(ctx context.Context, runner command.Runner, adbBinary string, server adbServerEndpoint, serial, expectedAVD string) (bool, error) {
	result, err := runExactSerialADBAt(ctx, runner, adbBinary, server, serial, adbMetadataOutputLimit, "emu", "avd", "name")
	if err != nil {
		return false, err
	}
	lines := strings.Fields(strings.ReplaceAll(string(result.Stdout), "OK", ""))
	if len(lines) == 0 {
		return false, fmt.Errorf("emulator console returned no AVD identity")
	}
	if expectedAVD != "" && lines[0] != expectedAVD {
		return false, fmt.Errorf("emulator console reported AVD %q, want %q", lines[0], expectedAVD)
	}
	return true, nil
}
