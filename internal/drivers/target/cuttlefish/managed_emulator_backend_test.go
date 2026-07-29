package cuttlefish

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/androidcontract"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestManagedEmulatorRunsWindowsSDKBatchWithSanitizedEnvironment(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("exercises the real Windows cmd.exe batch boundary")
	}
	root := t.TempDir()
	sdkRoot := root + string(os.PathSeparator) + "sdk root"
	avdHome := root + string(os.PathSeparator) + "managed avds"
	toolsDirectory := root + string(os.PathSeparator) + "command line tools"
	if err := os.MkdirAll(toolsDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	batchPath := toolsDirectory + string(os.PathSeparator) + "sdkmanager.bat"
	batch := "@echo off\r\n" +
		"if defined DEBUG echo leaked-debug\r\n" +
		"echo argument=%~1\r\n" +
		"echo sdk-root=%ANDROID_SDK_ROOT%\r\n" +
		"echo avd-home=%ANDROID_AVD_HOME%\r\n" +
		"echo 12.0\r\n"
	if err := os.WriteFile(batchPath, []byte(batch), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEBUG", "release")
	t.Setenv("android_sdk_root", "ambient-wrong-sdk")
	t.Setenv("ANDROID_HOME", "ambient-wrong-home")
	t.Setenv("android_avd_home", "ambient-wrong-avds")
	backend := &ManagedEmulatorBackend{
		runner: command.OS{}, sdkRoot: sdkRoot, avdHome: avdHome,
	}
	environment := backend.sdkEnvironment()
	for _, key := range []string{"ANDROID_SDK_ROOT", "ANDROID_HOME", "ANDROID_AVD_HOME"} {
		if countEnvironmentKey(environment, key) != 1 {
			t.Fatalf("sanitized environment has non-unique %s: %#v", key, environment)
		}
	}
	if countEnvironmentKey(environment, "DEBUG") != 0 {
		t.Fatalf("sanitized environment retained ambient DEBUG: %#v", environment)
	}
	output, err := backend.runTool(context.Background(), batchPath, []string{"--version"}, environment)
	if err != nil {
		t.Fatalf("execute real batch through cmd.exe: %v", err)
	}
	for _, expected := range []string{"argument=--version", "sdk-root=" + sdkRoot, "avd-home=" + avdHome, "12.0"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("batch output %q does not contain %q", output, expected)
		}
	}
	if strings.Contains(output, "leaked-debug") {
		t.Fatalf("batch observed ambient DEBUG: %q", output)
	}
	if version, err := exactSDKManagerVersion("prompt noise\n" + output); err != nil || version != "12.0" {
		t.Fatalf("exact sdkmanager version = %q, %v", version, err)
	}
}

func countEnvironmentKey(environment []string, key string) int {
	count := 0
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if found && strings.EqualFold(name, key) {
			count++
		}
	}
	return count
}

func prepareManagedFakeFormatter(t *testing.T, sdkRoot string) {
	t.Helper()
	binary, config := managedMKE2FSPaths(sdkRoot)
	if err := os.MkdirAll(filepath.Dir(binary), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("fake mke2fs executable identity\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte("[defaults]\nbase_features = sparse_super\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func materializeManagedTestStorage(t *testing.T, backend *ManagedEmulatorBackend, instance Instance, overlay bool) managedDataStorageBinding {
	t.Helper()
	if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := backend.createExactManagedDataImage(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	binding, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayAbsent)
	if err != nil {
		t.Fatal(err)
	}
	if !overlay {
		return binding
	}
	if err := writeManagedTestQCOW2(binding.OverlayPath, filepath.Base(binding.BackingPath), binding.BackingBytes); err != nil {
		t.Fatal(err)
	}
	binding, err = backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayPresent)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func managedTestStorageIdentity(instance Instance) managedAVDStorageIdentity {
	return managedAVDStorageIdentity{
		Version:            managedEmulatorDataIdentityVersion,
		RuntimeID:          instance.RuntimeID,
		StateDirectory:     filepath.Clean(instance.StateDirectory),
		DeviceConfigDigest: instance.Fingerprint.DeviceConfigDigest.String(),
		BackingFile:        managedEmulatorDataFilename,
		BackingBytes:       instance.Resources.StorageBytes,
		BackingDigest:      domain.NewDigest([]byte("test-data-backing")).String(),
		BackingReadOnly:    true,
		OverlayFile:        managedEmulatorDataOverlayFilename,
		Formatter: managedDataFormatterIdentity{
			Binary:       filepath.Join(instance.StateDirectory, "sdk", executableName("mke2fs", ".exe")),
			BinaryDigest: domain.NewDigest([]byte("test-mke2fs-binary")).String(),
			Config:       filepath.Join(instance.StateDirectory, "sdk", "mke2fs.conf"),
			ConfigDigest: domain.NewDigest([]byte("test-mke2fs-config")).String(),
			Version:      "mke2fs test",
		},
	}
}

func managedTestStorageBinding(instance Instance) managedDataStorageBinding {
	identity := managedTestStorageIdentity(instance)
	identityDigest, err := identity.digest()
	if err != nil {
		panic(fmt.Sprintf("digest fixed managed test storage identity: %v", err))
	}
	return managedDataStorageBinding{
		IdentityDigest: identityDigest.String(),
		BackingPath:    filepath.Join(instance.StateDirectory, managedEmulatorDataFilename),
		BackingBytes:   identity.BackingBytes,
		BackingDigest:  identity.BackingDigest,
		OverlayPath:    filepath.Join(instance.StateDirectory, managedEmulatorDataOverlayFilename),
		Formatter:      identity.Formatter,
	}
}

func persistManagedTestStorageIdentity(t *testing.T, instance Instance) managedDataStorageBinding {
	t.Helper()
	if err := writeExclusiveManagedManifest(
		filepath.Join(instance.StateDirectory, managedEmulatorDataIdentityFilename),
		managedTestStorageIdentity(instance),
	); err != nil {
		t.Fatal(err)
	}
	return managedTestStorageBinding(instance)
}

func managedTestStorageAuthority(instance Instance) managedDataStorageAuthority {
	return managedTestStorageBinding(instance).authority(instance)
}

func TestManagedEmulatorBackendObservesAndEnforcesExactRuntime(t *testing.T) {
	root := t.TempDir()
	sdkRoot := root + string(os.PathSeparator) + "sdk"
	prepareManagedFakeFormatter(t, sdkRoot)
	imageDirectory := sdkRoot + string(os.PathSeparator) + "system-images" + string(os.PathSeparator) + "android-35" + string(os.PathSeparator) + "google_apis" + string(os.PathSeparator) + "x86_64"
	if err := os.MkdirAll(imageDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	buildProperties := "ro.debuggable=1\nro.system.build.fingerprint=google/sdk_gphone64_x86_64/emu35:userdebug/test-keys\n"
	if err := os.WriteFile(imageDirectory+string(os.PathSeparator)+"build.prop", []byte(buildProperties), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(imageDirectory+string(os.PathSeparator)+"system.img", []byte("real-system-image-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := DigestManagedSystemImage(imageDirectory)
	if err != nil {
		t.Fatal(err)
	}
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	host.launcherExits = true
	backendConfig := ManagedEmulatorBackendConfig{
		Runner: host, Starter: host, EmulatorBinary: "emulator", ADBBinary: "adb", SDKManagerBinary: "sdkmanager",
		AVDManagerBinary: "avdmanager", SDKRoot: sdkRoot, StateRoot: root + string(os.PathSeparator) + "targets", ADBServerEndpoint: "127.0.0.1:5040",
		SystemImages: map[string]ManagedSystemImage{digest.String(): {Package: "system-images;android-35;google_apis;x86_64", Directory: imageDirectory}},
		PollInterval: time.Millisecond, ShutdownTimeout: defaultManagedEmulatorStopTimeout, MaximumLogBytes: 64, processAuthority: host,
	}
	backend, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	template := completeAndroidTemplate("managed", "android-emulator", digest)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	capabilities, err := backend.Probe(ctx, template)
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.BackendVersion != "Android emulator version 35.2.10" || capabilities.RuntimeVersion != "google/sdk_gphone64_x86_64/emu35:userdebug/test-keys" || capabilities.Evidence["os"] != "android" || capabilities.Evidence["hardware_acceleration"] != "WHPX(10.0) is installed and usable." {
		t.Fatalf("observed managed capabilities = %#v", capabilities)
	}
	if !capabilities.Managed || !capabilities.HardwareAccelerationKnown || !capabilities.HardwareAcceleration {
		t.Fatalf("managed capability facts = %#v", capabilities)
	}
	if !capabilities.CPUEnforced || !capabilities.MemoryEnforced || !capabilities.WritableStateEnforced {
		t.Fatalf("managed host and guest-data resource enforcement was not reported: %#v", capabilities)
	}
	plan := managedBackendTestPlan(t, root, digest)
	for key, want := range map[string]string{
		"host_cpu_containment": "process-tree-limit", "host_memory_containment": "process-tree-limit", "writable_state_scope": "guest-data-partition",
	} {
		if capabilities.Evidence[key] != want {
			t.Fatalf("managed capability evidence %q = %q, want %q", key, capabilities.Evidence[key], want)
		}
	}
	physical := androidPhysicalPolicyReport(template, capabilities, plan.Resources)
	for name, fact := range map[string]ports.PhysicalLimitFact{
		"cpu": physical.Resources.CPUMilli, "memory": physical.Resources.MemoryBytes, "writable-state": physical.Resources.WritableStateBytes,
	} {
		if fact.Support != ports.PhysicalSupportEnforced {
			t.Fatalf("managed SDK %s support = %q, want enforced", name, fact.Support)
		}
	}
	if capabilities.KVMKnown || capabilities.KVM {
		t.Fatalf("WHPX acceleration was misreported as KVM: %#v", capabilities)
	}
	instance, err := backend.Create(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if host.hasCall("sdkmanager", "--install") {
		t.Fatal("Create invoked sdkmanager for an already installed exact image")
	}
	if err := backend.Start(ctx, instance); err != nil {
		t.Fatal(err)
	}
	record := backend.processes[instance.RuntimeID]
	select {
	case <-record.done:
	case <-time.After(time.Second):
		t.Fatal("fake emulator launcher did not exit after handing off to its QEMU successor")
	}
	startsBeforeDuplicate := host.startCount()
	if err := backend.Start(ctx, instance); err == nil {
		t.Fatal("duplicate start ignored the live exact QEMU successor after launcher exit")
	}
	if host.startCount() != startsBeforeDuplicate {
		t.Fatal("duplicate start launched a second emulator process")
	}
	if err := os.Remove(filepath.Join(instance.StateDirectory, managedEmulatorOwnershipFilename)); err != nil {
		t.Fatal(err)
	}
	bootCrashBackend, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	recoveredRecord, err := bootCrashBackend.requireManagedProcessRecord(ctx, instance)
	if err != nil {
		t.Fatalf("reconstruct pre-readiness ownership from launch intent and fresh PID file: %v", err)
	}
	if running, stateErr := managedProcessState(recoveredRecord); stateErr != nil || !running {
		t.Fatalf("reconstructed pre-readiness process state = %t, %v", running, stateErr)
	}
	if _, found, err := loadManagedProcessOwnership(instance, backend.processAuthority); err != nil || !found {
		t.Fatalf("reconstructed process ownership was not durably committed: found=%t err=%v", found, err)
	}
	if err := closeManagedHostProcess(recoveredRecord); err != nil {
		t.Fatal(err)
	}
	state, err := backend.WaitReady(ctx, instance)
	if err != nil || !state.Ready() || !state.Identity.Rooted || !state.Identity.Debuggable || state.Identity.AVDName != instance.Allocation.InstanceName {
		t.Fatalf("managed readiness = %#v, %v", state, err)
	}
	proof := BackendQuarantineState{
		RuntimeID: instance.RuntimeID, ExecutionStopped: true, NetworkUnreachable: true,
		StatePreserved: true, ObservedAt: time.Now().UTC().Add(-time.Second),
	}
	reachableAdopter, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reachableAdopter.AdoptStopped(ctx, instance, proof); err == nil {
		t.Fatal("reachable managed emulator was adopted as stopped")
	}
	if _, err := reachableAdopter.InspectStopped(ctx, instance); err == nil {
		t.Fatal("reachable managed emulator was inspected as stopped")
	}
	unknownOwner, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := unknownOwner.Stop(ctx, instance, ports.StopGraceful); err != nil {
		t.Fatalf("acknowledged exact ADB kill with unknown local handle: %v", err)
	}
	startsBeforeRecovery := host.startCount()
	offlineUnknownOwner, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := offlineUnknownOwner.Stop(ctx, instance, ports.StopForce); err != nil {
		t.Fatalf("persisted ownership could not prove its exact exited process stopped: %v", err)
	}
	proof, err = offlineUnknownOwner.Quarantine(ctx, instance, ports.StopForce)
	if err != nil || !proof.ExecutionStopped || !proof.NetworkUnreachable || !proof.StatePreserved {
		t.Fatalf("persisted exact stopped ownership was not containable: %#v, %v", proof, err)
	}
	if host.startCount() != startsBeforeRecovery {
		t.Fatal("unproven crash recovery launched a second emulator")
	}
	inspectingOwner, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	inspected, err := inspectingOwner.InspectStopped(ctx, instance)
	if err != nil {
		t.Fatalf("inspect exact unexpectedly stopped runtime: %v", err)
	}
	if inspected.RuntimeID != instance.RuntimeID || !inspected.ExecutionStopped || !inspected.NetworkUnreachable || !inspected.StatePreserved || inspected.ObservedAt.IsZero() {
		t.Fatalf("inspected stopped evidence = %#v", inspected)
	}
	if host.startCount() != startsBeforeRecovery {
		t.Fatal("stopped inspection restarted the emulator")
	}
	foreignProof := proof
	foreignProof.RuntimeID = "world-emulator-5562"
	if _, err := offlineUnknownOwner.AdoptStopped(ctx, instance, foreignProof); err == nil {
		t.Fatal("foreign containment proof was adopted")
	}
	closedBeforeAdoption := host.closeCount()
	if _, err := backend.AdoptStopped(ctx, instance, proof); err != nil {
		t.Fatalf("adopt stopped runtime over retained exited process authority: %v", err)
	}
	if host.closeCount() != closedBeforeAdoption+1 {
		t.Fatal("stopped adoption leaked the prior retained host-process authority")
	}
	adoptingOwner, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := adoptingOwner.AdoptStopped(ctx, instance, proof)
	if err != nil {
		t.Fatal(err)
	}
	if adopted.RuntimeID != instance.RuntimeID || !adopted.ExecutionStopped || !adopted.NetworkUnreachable || !adopted.StatePreserved || adopted.ObservedAt.Before(proof.ObservedAt) {
		t.Fatalf("adopted stopped evidence = %#v", adopted)
	}
	if err := adoptingOwner.Stop(ctx, instance, ports.StopForce); err != nil {
		t.Fatalf("adopted stopped emulator was not idempotently stoppable: %v", err)
	}
	if host.startCount() != startsBeforeRecovery {
		t.Fatal("stopped adoption restarted the emulator")
	}
	invocation := host.startInvocation()
	for _, required := range [][]string{{"-port", "5560"}, {"-no-window"}, {"-accel", "on"}, {"-cores", "2"}, {"-memory", "2048"}, {"-data", filepath.Join(plan.StateDirectory, managedEmulatorDataFilename)}, {"-no-cache"}} {
		if !containsContiguous(invocation.Args, required) {
			t.Fatalf("managed emulator args %#v omit %v", invocation.Args, required)
		}
	}
	for _, forbidden := range []string{"-writable-system", "-snapshot", "-sdcard", "-wipe-data", "-partition-size"} {
		if slices.Contains(invocation.Args, forbidden) {
			t.Fatalf("managed emulator args %#v include forbidden state surface %q", invocation.Args, forbidden)
		}
	}
	if err := adoptingOwner.Destroy(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if host.running() {
		t.Fatal("destroy left managed emulator running")
	}
	if _, err := os.Stat(instance.StateDirectory); !os.IsNotExist(err) {
		t.Fatalf("managed state remains after destroy: %v", err)
	}
}

func TestManagedEmulatorForceStopRequiresProvenAbsenceAfterTerminationError(t *testing.T) {
	for _, test := range []struct {
		name            string
		exitDelay       time.Duration
		leaveRunning    bool
		wantErr         bool
		shutdownTimeout time.Duration
	}{
		{name: "process exits despite request error", exitDelay: 10 * time.Millisecond, shutdownTimeout: time.Second},
		{name: "process remains live", leaveRunning: true, wantErr: true, shutdownTimeout: 20 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newManagedEmulatorFakeHost("world-emulator-5560")
			host.hostProcessKillErr = fmt.Errorf("access denied")
			host.hostProcessExitDelay = test.exitDelay
			host.hostProcessKillLeavesRunning = test.leaveRunning
			backend, instance := managedRollbackTestBackend(t, host)
			backend.shutdownTimeout = test.shutdownTimeout
			if err := backend.Start(context.Background(), instance); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := backend.Stop(ctx, instance, ports.StopForce)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "access denied") || !host.running() {
					t.Fatalf("uncontained process error = %v, running=%t", err, host.running())
				}
				return
			}
			if err != nil {
				t.Fatalf("verified process absence retained a stale termination error: %v", err)
			}
			if host.running() {
				t.Fatal("force-stop returned before the exact managed process exited")
			}
		})
	}
}

func TestManagedEmulatorStopModesUseDistinctTerminationPaths(t *testing.T) {
	for _, test := range []struct {
		mode                   ports.StopMode
		guestStopLeavesRunning bool
		wantGuestStop          bool
		wantHostForce          bool
	}{
		{mode: ports.StopGraceful, wantGuestStop: true},
		{mode: ports.StopImmediate, guestStopLeavesRunning: true, wantGuestStop: true, wantHostForce: true},
		{mode: ports.StopForce, wantHostForce: true},
	} {
		test := test
		t.Run(string(test.mode), func(t *testing.T) {
			host := newManagedEmulatorFakeHost("world-emulator-5560")
			host.adbKillLeavesRunning = test.guestStopLeavesRunning
			backend, instance := managedRollbackTestBackend(t, host)
			backend.shutdownTimeout = 100 * time.Millisecond
			if err := backend.Start(context.Background(), instance); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := backend.Destroy(cleanup, instance); err != nil {
					t.Errorf("destroy stop-mode fixture: %v", err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := backend.Stop(ctx, instance, test.mode); err != nil {
				t.Fatal(err)
			}
			if got := host.hasExactADBAction("emu", "kill"); got != test.wantGuestStop {
				t.Fatalf("guest shutdown request observed = %t, want %t", got, test.wantGuestStop)
			}
			if got := host.hostProcessKillCount() > 0; got != test.wantHostForce {
				t.Fatalf("host force termination observed = %t, want %t", got, test.wantHostForce)
			}
			if host.running() {
				t.Fatal("stop mode returned while the exact managed emulator remained running")
			}
		})
	}
}

func TestManagedEmulatorForceStopRecoversOwnerAfterCallerCancellation(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	backend, instance := managedRollbackTestBackend(t, host)
	backend.shutdownTimeout = 30 * time.Second
	if err := backend.Start(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := backend.Destroy(ctx, instance); err != nil {
			t.Errorf("destroy recovered-owner force-stop fixture: %v", err)
		}
	})
	if _, found, err := loadManagedProcessOwnership(instance, host); err != nil || !found {
		t.Fatalf("persisted exact process ownership: found=%t err=%v", found, err)
	}

	restarted := &ManagedEmulatorBackend{
		runner: host, adbBinary: backend.adbBinary, adbServer: backend.adbServer,
		emulatorBinary: backend.emulatorBinary, sdkRoot: backend.sdkRoot, stateRoot: backend.stateRoot,
		pollInterval: time.Millisecond, shutdownTimeout: 30 * time.Second, now: time.Now,
		processAuthority: host, processes: make(map[string]*managedProcess),
	}
	if len(restarted.processes) != 0 {
		t.Fatal("fresh backend unexpectedly retained cached process ownership")
	}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	stopErr := restarted.Stop(requestContext, instance, ports.StopForce)
	if !errors.Is(stopErr, context.Canceled) {
		t.Fatalf("force-stop error = %v, want context.Canceled", stopErr)
	}
	if host.running() {
		t.Fatal("canceled force-stop returned without containing the recovered exact process")
	}
	recovered := restarted.processes[instance.RuntimeID]
	if recovered == nil {
		t.Fatal("force-stop did not reconstruct persisted process ownership in the fresh backend")
	}
	if running, err := managedProcessState(recovered); err != nil || running {
		t.Fatalf("recovered exact process state after force-stop = running=%t err=%v", running, err)
	}
	t.Cleanup(func() {
		if err := closeManagedHostProcess(recovered); err != nil {
			t.Errorf("close recovered exact process authority: %v", err)
		}
	})
}

func TestManagedSystemImageDigestRejectsMutation(t *testing.T) {
	directory := t.TempDir()
	path := directory + string(os.PathSeparator) + "build.prop"
	if err := os.WriteFile(path, []byte("ro.debuggable=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := DigestManagedSystemImage(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("ro.debuggable=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := DigestManagedSystemImage(directory)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("system-image byte mutation did not change digest")
	}
}

func TestValidateManagedEmulatorResourcesUsesExactRuntimeLimits(t *testing.T) {
	const mebibyte = int64(1 << 20)
	for _, valid := range []struct {
		resources   admission.Resources
		guestMemory int64
	}{
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte), 1536 * mebibyte},
		{managedEmulatorTestResources(64000, 12<<30, managedEmulatorMaximumPartitionMiB*mebibyte), 8192 * mebibyte},
	} {
		if err := ValidateManagedEmulatorResources(valid.resources, valid.guestMemory); err != nil {
			t.Fatalf("valid managed resources %#v: %v", valid, err)
		}
	}
	for _, invalid := range []struct {
		resources   admission.Resources
		guestMemory int64
	}{
		{managedEmulatorTestResources(999, 6<<30, 64*mebibyte), 2 << 30},
		{managedEmulatorTestResources(1500, 6<<30, 64*mebibyte), 2 << 30},
		{managedEmulatorTestResources(65000, 6<<30, 64*mebibyte), 2 << 30},
		{managedEmulatorTestResources(1000, 0, 64*mebibyte), 2 << 30},
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte), 1535 * mebibyte},
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte), 8193 * mebibyte},
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte), 1536*mebibyte + 1},
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte-1), 2 << 30},
		{managedEmulatorTestResources(1000, 6<<30, 64*mebibyte+1), 2 << 30},
		{managedEmulatorTestResources(1000, 6<<30, 63*mebibyte), 2 << 30},
		{managedEmulatorTestResources(1000, 6<<30, (managedEmulatorMaximumPartitionMiB+1)*mebibyte), 2 << 30},
	} {
		if err := ValidateManagedEmulatorResources(invalid.resources, invalid.guestMemory); err == nil {
			t.Fatalf("invalid managed resources were accepted: %#v", invalid)
		}
	}
}

func managedEmulatorTestResources(cpuMilli, memoryBytes, storageBytes int64) admission.Resources {
	return admission.Resources{CPUMilli: cpuMilli, MemoryBytes: memoryBytes, StorageBytes: storageBytes}
}

func TestManagedEmulatorWaitReadyReportsEarlyProcessExitAndLauncherLog(t *testing.T) {
	stateDirectory := t.TempDir()
	launcherFailure := "ERROR | partition-size (2048) must be between 10MB and 2047MB"
	if err := os.WriteFile(
		stateDirectory+string(os.PathSeparator)+managedEmulatorStderrFilename,
		[]byte(strings.Repeat("discarded-prefix-", 512)+launcherFailure+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	logsDone := make(chan struct{})
	close(done)
	close(logsDone)
	instance := Instance{
		RuntimeID:      "world-emulator-5560",
		StateDirectory: stateDirectory,
		Allocation:     emulatorAllocation(5560),
	}
	backend := &ManagedEmulatorBackend{
		pollInterval: time.Second,
		processes: map[string]*managedProcess{
			instance.RuntimeID: {done: done, logsDone: logsDone, waitErr: fmt.Errorf("exit status 1")},
		},
	}

	_, err := backend.WaitReady(context.Background(), instance)
	if err == nil {
		t.Fatal("early emulator process exit was accepted as readiness")
	}
	for _, expected := range []string{"process exited before Android readiness", "exit status 1", launcherFailure} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("early-exit diagnostic %q does not contain %q", err, expected)
		}
	}
}

func TestManagedEmulatorWaitReadyRequiresExactBootCompletedProperty(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	backend, instance := managedRollbackTestBackend(t, host)
	if err := backend.Start(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := backend.Destroy(ctx, instance); err != nil {
			t.Errorf("destroy boot-completion fixture: %v", err)
		}
	})

	// Make every signal except the exact Android boot property ready, then
	// wait on a long poll so the caller deadline deterministically ends the
	// first readiness attempt in WaitReady's ticker select.
	backend.pollInterval = time.Hour
	host.mu.Lock()
	host.rooted = true
	host.bootCompleted = "0"
	host.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	state, err := backend.WaitReady(ctx, instance)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("boot-incomplete readiness error = %v, want deadline exceeded", err)
	}
	if state.BootCompleted || !state.ProcessRunning || !state.ADBReady || state.DeviceState != "device" ||
		!state.FrameworkReady || !state.PackageManagerReady || !state.Identity.Rooted || !state.Identity.Debuggable ||
		state.Identity.AVDName != instance.Allocation.InstanceName {
		t.Fatalf("boot property was not the sole incomplete readiness signal: %#v", state)
	}
	if !strings.Contains(err.Error(), "boot_completed=false") {
		t.Fatalf("boot-incomplete readiness diagnostic was lost: %v", err)
	}
	if !host.hasExactADBAction("shell", "getprop", "sys.boot_completed") {
		t.Fatal("readiness did not query sys.boot_completed through the configured ADB server and exact serial")
	}

	// Change only the hard-gate property and prove the same runtime is ready.
	host.mu.Lock()
	host.bootCompleted = "1"
	host.mu.Unlock()
	state, err = backend.WaitReady(context.Background(), instance)
	if err != nil || !state.Ready() {
		t.Fatalf("readiness after sys.boot_completed=1 = %#v, %v", state, err)
	}
}

func TestDriverCreateSurfacesBackendReadinessDiagnosticAfterCleanup(t *testing.T) {
	root := t.TempDir()
	launcherFailure := "partition-size (2048) must be between 10MB and 2047MB"
	backend := &readinessErrorBackend{
		statefulBackend: &statefulBackend{instances: make(map[string]*statefulInstance)},
		err:             fmt.Errorf("managed emulator process exited: %s", launcherFailure),
	}
	driver := &Driver{backend: backend}
	plan := managedBackendTestPlan(t, root, domain.NewDigest([]byte("readiness-failure-image")))

	_, _, err := driver.createInstance(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), launcherFailure) {
		t.Fatalf("driver readiness failure did not surface backend diagnostic: %v", err)
	}
	if len(backend.instances) != 0 {
		t.Fatalf("driver left failed managed runtime registered after cleanup: %#v", backend.instances)
	}
}

func TestIncompleteAndroidReadinessDiagnosticNamesEverySignal(t *testing.T) {
	state := ReadinessState{ProcessRunning: true, ADBReady: true, DeviceState: "offline"}
	err := incompleteAndroidReadinessError(state, "world-emulator-5560")
	for _, expected := range []string{
		"process_running=true", "adb_ready=true", `device_state="offline"`, "boot_completed=false",
		"framework_ready=false", "package_manager_ready=false", "rooted=false", "debuggable=false",
		"identity_valid=false", `expected_avd_name="world-emulator-5560"`, "avd_matches=false",
	} {
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("readiness diagnostic %q does not contain %q", err, expected)
		}
	}
}

func TestManagedEmulatorIntentOnlyNeverBecomesTimedAbsence(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	backend, instance := intentRecoveryTestBackend(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	err := backend.Stop(ctx, instance, ports.StopForce)
	if !errors.Is(err, errManagedLaunchUnresolved) {
		t.Fatalf("intent-only launch was not classified as permanently unresolved: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("intent-only stop waited for a timer before failing closed: %v", time.Since(started))
	}
	if _, err := backend.Inspect(ctx, instance); !errors.Is(err, errManagedLaunchUnresolved) {
		t.Fatalf("startup inspection inferred a process from ADB without durable host authority: %v", err)
	}
	if host.startCount() != 0 || host.running() {
		t.Fatal("intent-only recovery inferred or launched an ambient emulator")
	}
}

func TestManagedEmulatorRecoversOnlyPIDBoundToExactLaunchArgument(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutate      func([]string) []string
		wantRecover bool
	}{
		{name: "exact binding", mutate: func(arguments []string) []string { return arguments }, wantRecover: true},
		{name: "foreign binding", mutate: func(arguments []string) []string {
			arguments[len(arguments)-1] = filepath.Join(t.TempDir(), "foreign.pid")
			return arguments
		}},
		{name: "missing headless binding", mutate: func(arguments []string) []string {
			return removeManagedTestArguments(arguments, "-no-window", 1)
		}},
		{name: "extra writable system binding", mutate: func(arguments []string) []string {
			return append(arguments, "-writable-system")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newManagedEmulatorFakeHost("world-emulator-5560")
			backend, instance := intentRecoveryTestBackend(t, host)
			pidFile := managedEmulatorPIDPath(instance)
			port, err := instance.Allocation.EmulatorConsolePort()
			if err != nil {
				t.Fatal(err)
			}
			arguments := managedEmulatorLaunchArguments(instance, port, backend.managedDataImagePath(instance), pidFile)
			host.mu.Lock()
			host.starts = 1
			host.reachable = true
			host.process = newManagedFakeProcess(nil)
			host.start = command.Invocation{Program: "emulator", Args: test.mutate(arguments)}
			host.mu.Unlock()
			if err := os.WriteFile(pidFile, []byte("4242\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			record, err := backend.requireManagedProcessRecord(context.Background(), instance)
			if test.wantRecover {
				if err != nil {
					t.Fatalf("recover exact PID-file binding: %v", err)
				}
				if running, stateErr := managedProcessState(record); stateErr != nil || !running {
					t.Fatalf("recovered exact bound process state = %t, %v", running, stateErr)
				}
				return
			}
			if !errors.Is(err, errManagedLaunchUnresolved) || !errors.Is(err, errManagedHostProcessIdentityMismatch) {
				t.Fatalf("foreign allowed-QEMU PID was not rejected as an unresolved identity mismatch: %v", err)
			}
			if !host.running() {
				t.Fatal("foreign PID rejection killed an unowned process")
			}
		})
	}
}

func TestManagedEmulatorRuntimeArgumentsBindExactGuestAndPhysicalIdentity(t *testing.T) {
	root := t.TempDir()
	instance := Instance{
		RuntimeID:        "world-emulator-5560",
		StateDirectory:   root,
		Allocation:       emulatorAllocation(5560),
		Resources:        admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 1 << 30},
		GuestMemoryBytes: 2 << 30,
	}
	pidFile := filepath.Join(root, managedEmulatorPIDFilename)
	dataImage := filepath.Join(root, managedEmulatorDataFilename)
	valid := append([]string{"qemu-system-x86_64-headless.exe"}, managedEmulatorLaunchArguments(instance, 5560, dataImage, pidFile)...)
	if err := requireExactManagedRuntimeArguments(valid, pidFile, dataImage, instance, runtime.GOOS == "windows"); err != nil {
		t.Fatalf("exact managed runtime arguments: %v", err)
	}
	change := func(name, value string) []string {
		arguments := append([]string(nil), valid...)
		for index := range arguments {
			if arguments[index] == name {
				arguments[index+1] = value
				return arguments
			}
		}
		t.Fatalf("test argument %s is absent", name)
		return nil
	}
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "foreign avd", args: change("-avd", "foreign-avd")},
		{name: "foreign port", args: change("-port", "5580")},
		{name: "foreign guest memory", args: change("-memory", "3072")},
		{name: "foreign cores", args: change("-cores", "4")},
		{name: "disabled acceleration", args: change("-accel", "off")},
		{name: "foreign gpu", args: change("-gpu", "host")},
		{name: "foreign data image", args: change("-data", filepath.Join(root, "foreign.avd", managedEmulatorDataFilename))},
		{name: "foreign pid file", args: change("-pidfile", filepath.Join(root, "foreign.pid"))},
		{name: "missing memory", args: removeManagedTestArguments(valid, "-memory", 2)},
		{name: "missing data image", args: removeManagedTestArguments(valid, "-data", 2)},
		{name: "missing headless", args: removeManagedTestArguments(valid, "-no-window", 1)},
		{name: "missing qemu boundary", args: removeManagedTestArguments(valid, "-qemu", 1)},
		{name: "duplicate headless", args: append(append([]string(nil), valid...), "-no-window")},
		{name: "duplicate qemu boundary", args: append(append([]string(nil), valid...), "-qemu")},
		{name: "unknown high-risk option", args: append(append([]string(nil), valid...), "-writable-system")},
		{name: "duplicate data", args: append(append([]string(nil), valid...), "-data", dataImage)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := requireExactManagedRuntimeArguments(test.args, pidFile, dataImage, instance, runtime.GOOS == "windows"); err == nil {
				t.Fatal("foreign or ambiguous managed runtime arguments were accepted")
			}
		})
	}
}

func TestManagedEmulatorRejectsImmutableRuntimeMismatchImmediately(t *testing.T) {
	for _, test := range []struct {
		name   string
		class  error
		mutate func(*managedEmulatorFakeHost, Instance)
	}{
		{
			name:  "runtime fingerprint",
			class: errManagedRuntimeFingerprintMismatch,
			mutate: func(host *managedEmulatorFakeHost, _ Instance) {
				host.runtimeFingerprint = "foreign/system/image:fingerprint"
			},
		},
		{
			name:  "guest data partition",
			class: errManagedGuestDataPartitionMismatch,
			mutate: func(host *managedEmulatorFakeHost, instance Instance) {
				host.dataPartitionBytes = instance.Resources.StorageBytes + androidcontract.Mebibyte
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newManagedEmulatorFakeHost("world-emulator-5560")
			backend, instance := managedRollbackTestBackend(t, host)
			if err := backend.Start(context.Background(), instance); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := backend.Destroy(ctx, instance); err != nil {
					t.Errorf("destroy immutable-runtime-mismatch test emulator: %v", err)
				}
			})
			host.mu.Lock()
			test.mutate(host, instance)
			host.mu.Unlock()
			started := time.Now()
			_, err := backend.WaitReady(context.Background(), instance)
			if !errors.Is(err, test.class) {
				t.Fatalf("immutable runtime mismatch was accepted or lost class %v: %v", test.class, err)
			}
			if elapsed := time.Since(started); elapsed >= instance.BootTimeout/2 {
				t.Fatalf("immutable runtime mismatch retried for %v", elapsed)
			}
		})
	}
}

func TestManagedEmulatorEndpointAbsenceRejectsRawADBTransportListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := requireManagedLoopbackPortAbsent(ctx, "console", 5560); err != nil {
		t.Skipf("console port 5560 is occupied by the host: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:5561")
	if err != nil {
		t.Skipf("ADB transport port 5561 is occupied by the host: %v", err)
	}
	defer listener.Close()
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	server, err := parseADBServerEndpoint("127.0.0.1:5040")
	if err != nil {
		t.Fatal(err)
	}
	backend := &ManagedEmulatorBackend{runner: host, adbBinary: "adb", adbServer: server}
	if err := backend.requireManagedEndpointAbsent(ctx, Instance{Allocation: emulatorAllocation(5560)}); err == nil || !strings.Contains(err.Error(), "ADB transport") {
		t.Fatalf("live raw ADB transport endpoint was not rejected exactly: %v", err)
	}
}

func TestManagedProcessOwnershipLoadRejectsAuthorityAndAnchorMutation(t *testing.T) {
	instance := instanceFromPlan(managedBackendTestPlan(t, t.TempDir(), domain.NewDigest([]byte("ownership-binding"))))
	if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	storage := persistManagedTestStorageIdentity(t, instance)
	host := newManagedEmulatorFakeHost(instance.Allocation.InstanceName)
	port, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		t.Fatal(err)
	}
	base := managedProcessOwnership{
		RuntimeID: instance.RuntimeID, AVDName: instance.Allocation.InstanceName, Serial: instance.Allocation.Serial,
		ConsolePort: port, PID: 4242, PIDFile: managedEmulatorPIDFilename, ExecutablePath: "qemu", StartToken: "start",
		ResourceAuthority: host.Kind(), ResourceIdentity: host.ResourceIdentity(instance), ResourceAnchored: true,
		CPUMilli: instance.Resources.CPUMilli, MemoryBytes: instance.Resources.MemoryBytes,
		StorageBytes: instance.Resources.StorageBytes, GuestMemoryBytes: instance.GuestMemoryBytes,
		Storage: storage.authority(instance),
	}
	for _, test := range []struct {
		name   string
		mutate func(*managedProcessOwnership)
	}{
		{name: "authority", mutate: func(value *managedProcessOwnership) { value.ResourceAuthority = "foreign_authority" }},
		{name: "identity", mutate: func(value *managedProcessOwnership) { value.ResourceIdentity = "foreign_identity" }},
		{name: "anchor", mutate: func(value *managedProcessOwnership) { value.ResourceAnchored = false }},
		{name: "storage", mutate: func(value *managedProcessOwnership) { value.StorageBytes += androidcontract.Mebibyte }},
		{name: "data backing path", mutate: func(value *managedProcessOwnership) { value.Storage.BackingPath += ".foreign" }},
		{name: "data overlay path", mutate: func(value *managedProcessOwnership) { value.Storage.OverlayPath += ".foreign" }},
		{name: "data backing digest", mutate: func(value *managedProcessOwnership) {
			value.Storage.BackingDigest = domain.NewDigest([]byte("foreign")).String()
		}},
		{name: "device config", mutate: func(value *managedProcessOwnership) {
			value.Storage.DeviceConfigDigest = domain.NewDigest([]byte("foreign-config")).String()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			encoded, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(instance.StateDirectory, managedEmulatorOwnershipFilename)
			if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := loadManagedProcessOwnership(instance, host); err == nil {
				t.Fatal("tampered persisted process ownership was accepted")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedAVDCreateRejectsFilesystemOnlyAndIndirectResidue(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *ManagedEmulatorBackend, string)
	}{
		{name: "partial directory", prepare: func(t *testing.T, backend *ManagedEmulatorBackend, name string) {
			t.Helper()
			if err := os.MkdirAll(backend.avdPath(name), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "orphan ini", prepare: func(t *testing.T, backend *ManagedEmulatorBackend, name string) {
			t.Helper()
			if err := writeManagedAVDINI(filepath.Join(backend.avdHome, name+".ini"), backend.avdPath(name)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "directory symlink", prepare: func(t *testing.T, backend *ManagedEmulatorBackend, name string) {
			t.Helper()
			target := filepath.Join(t.TempDir(), "foreign-avd")
			if err := os.MkdirAll(target, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, backend.avdPath(name)); err != nil {
				t.Skipf("directory symlink creation is unavailable: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, host := managedAVDPathTestBackend(t)
			name := "world-emulator-5560"
			test.prepare(t, backend, name)
			instance := Instance{RuntimeID: name, Allocation: emulatorAllocation(5560)}
			if err := backend.createExactManagedAVD(context.Background(), instance, "system-images;android-35;google_apis;x86_64"); err == nil {
				t.Fatal("pre-existing managed AVD filesystem residue was overwritten")
			}
			if host.hasCall("avdmanager", "create") {
				t.Fatal("avdmanager create was invoked over unproven residue")
			}
			ids, err := backend.ListRuntimeIDs(context.Background())
			if err != nil || !slices.Contains(ids, name) {
				t.Fatalf("filesystem-only managed AVD residue was not inventoried: ids=%v err=%v", ids, err)
			}
		})
	}
}

func TestManagedAVDDeletionRemovesUnregisteredExactPairAndRefusesRedirectedINI(t *testing.T) {
	t.Run("unregistered exact pair", func(t *testing.T) {
		backend, _ := managedAVDPathTestBackend(t)
		name := "world-emulator-5560"
		if err := os.MkdirAll(backend.avdPath(name), 0o700); err != nil {
			t.Fatal(err)
		}
		ini := filepath.Join(backend.avdHome, name+".ini")
		if err := writeManagedAVDINI(ini, backend.avdPath(name)); err != nil {
			t.Fatal(err)
		}
		if err := backend.deleteExactManagedAVD(context.Background(), name); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{backend.avdPath(name), ini} {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("exact unregistered residue %q remains: %v", path, err)
			}
		}
	})
	t.Run("listed redirected ini", func(t *testing.T) {
		backend, host := managedAVDPathTestBackend(t)
		name := "world-emulator-5560"
		external := filepath.Join(t.TempDir(), "foreign-avd")
		if err := os.MkdirAll(external, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(backend.avdPath(name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := writeManagedAVDINI(filepath.Join(backend.avdHome, name+".ini"), external); err != nil {
			t.Fatal(err)
		}
		host.mu.Lock()
		host.avds[name] = backend.avdPath(name)
		host.mu.Unlock()
		if err := backend.deleteExactManagedAVD(context.Background(), name); err == nil {
			t.Fatal("redirected managed AVD ini authorized avdmanager deletion")
		}
		if host.hasCall("avdmanager", "delete") {
			t.Fatal("redirected ini reached avdmanager delete")
		}
		if info, err := os.Stat(external); err != nil || !info.IsDir() {
			t.Fatalf("foreign redirected directory was altered: %v", err)
		}
	})
}

func managedAVDPathTestBackend(t *testing.T) (*ManagedEmulatorBackend, *managedEmulatorFakeHost) {
	t.Helper()
	avdHome := filepath.Join(t.TempDir(), managedAVDDirectory)
	if err := os.MkdirAll(avdHome, 0o700); err != nil {
		t.Fatal(err)
	}
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	return &ManagedEmulatorBackend{
		runner: host, emulatorBinary: "emulator", avdManagerBinary: "avdmanager", avdHome: avdHome,
	}, host
}

func TestManagedEmulatorResumeUnstartedRecreatesAndStartsExactlyOnce(t *testing.T) {
	backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
	resumed, err := backend.ResumeUnstarted(context.Background(), instance)
	if err != nil || !resumed {
		t.Fatalf("resume exact unstarted managed AVD = %t, %v", resumed, err)
	}
	if host.startCount() != 1 || !host.hasCall("avdmanager", "delete") || !host.hasCall("avdmanager", "create") {
		t.Fatalf("unstarted recovery did not delete, recreate, and start exactly: calls=%#v starts=%d", host.calls, host.startCount())
	}
	resumed, err = backend.ResumeUnstarted(context.Background(), instance)
	if err != nil || resumed || host.startCount() != 1 {
		t.Fatalf("retry after durable launch = %t, %v, starts=%d", resumed, err, host.startCount())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := backend.Destroy(ctx, instance); err != nil {
		t.Fatal(err)
	}
}

func TestManagedEmulatorResumeUnstartedRecoversExactPartialAVDPaths(t *testing.T) {
	for _, test := range []struct {
		name   string
		remove func(*ManagedEmulatorBackend, Instance) error
	}{
		{name: "crash after directory creation", remove: func(backend *ManagedEmulatorBackend, instance Instance) error {
			return os.Remove(filepath.Join(backend.avdHome, instance.RuntimeID+".ini"))
		}},
		{name: "crash after ini creation", remove: func(backend *ManagedEmulatorBackend, instance Instance) error {
			return os.RemoveAll(backend.avdPath(instance.RuntimeID))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
			host.mu.Lock()
			delete(host.avds, instance.RuntimeID)
			host.mu.Unlock()
			if err := test.remove(backend, instance); err != nil {
				t.Fatal(err)
			}
			resumed, err := backend.ResumeUnstarted(context.Background(), instance)
			if err != nil || !resumed {
				t.Fatalf("partial exact AVD recovery = %t, %v", resumed, err)
			}
			if host.startCount() != 1 || !host.hasCall("avdmanager", "create") {
				t.Fatal("partial exact AVD residue was not recreated and started")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := backend.Destroy(ctx, instance); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedEmulatorResumeUnstartedRefusesLiveEndpointAndLaunchResidue(t *testing.T) {
	t.Run("live ADB endpoint", func(t *testing.T) {
		backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
		host.mu.Lock()
		host.reachable = true
		host.mu.Unlock()
		if resumed, err := backend.ResumeUnstarted(context.Background(), instance); err == nil || resumed {
			t.Fatalf("live endpoint recovery = %t, %v", resumed, err)
		}
		if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
			t.Fatal("live endpoint refusal mutated the AVD")
		}
	})
	for _, artifact := range []string{managedEmulatorPIDFilename, managedEmulatorOwnershipFilename} {
		t.Run("orphan "+artifact, func(t *testing.T) {
			backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
			if err := os.WriteFile(filepath.Join(instance.StateDirectory, artifact), []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if resumed, err := backend.ResumeUnstarted(context.Background(), instance); err == nil || resumed {
				t.Fatalf("orphan launch artifact recovery = %t, %v", resumed, err)
			}
			if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
				t.Fatal("orphan launch artifact refusal mutated the AVD")
			}
		})
	}
	t.Run("overlay without launch intent", func(t *testing.T) {
		backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
		storage, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayAbsent)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeManagedTestQCOW2(storage.OverlayPath, filepath.Base(storage.BackingPath), storage.BackingBytes); err != nil {
			t.Fatal(err)
		}
		resumed, err := backend.ResumeUnstarted(context.Background(), instance)
		if err == nil || resumed || !strings.Contains(err.Error(), "prior guest execution cannot be excluded") {
			t.Fatalf("overlay-without-intent recovery = %t, %v", resumed, err)
		}
		if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
			t.Fatal("unowned data overlay reached AVD deletion or emulator start")
		}
		if _, err := os.Lstat(storage.OverlayPath); err != nil {
			t.Fatalf("unowned data overlay was not retained for investigation: %v", err)
		}
	})
	t.Run("durable launch intent", func(t *testing.T) {
		backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
		storage, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayAbsent)
		if err != nil {
			t.Fatal(err)
		}
		if err := commitManagedLaunchIntent(instance, backend.emulatorBinary, storage); err != nil {
			t.Fatal(err)
		}
		if resumed, err := backend.ResumeUnstarted(context.Background(), instance); err != nil || resumed {
			t.Fatalf("launch-intent recovery = %t, %v", resumed, err)
		}
		if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
			t.Fatal("unresolved launch intent mutated the AVD")
		}
	})
	t.Run("redirected ini", func(t *testing.T) {
		backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
		external := filepath.Join(t.TempDir(), "foreign-avd")
		if err := os.MkdirAll(external, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := writeManagedAVDINI(filepath.Join(backend.avdHome, instance.RuntimeID+".ini"), external); err != nil {
			t.Fatal(err)
		}
		if resumed, err := backend.ResumeUnstarted(context.Background(), instance); err == nil || resumed {
			t.Fatalf("redirected ini recovery = %t, %v", resumed, err)
		}
		if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
			t.Fatal("redirected ini reached AVD mutation")
		}
	})
}

func TestManagedEmulatorResumeUnstartedRecreatesPartialDataBackingStage(t *testing.T) {
	backend, host, instance, _ := managedUnstartedRecoveryFixture(t)
	backingPath := backend.managedDataImagePath(instance)
	if err := makeManagedDataBackingWritable(backingPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(backingPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(instance.StateDirectory, managedEmulatorDataIdentityFilename)); err != nil {
		t.Fatal(err)
	}
	creatingPath := filepath.Join(instance.StateDirectory, managedEmulatorDataCreatingFilename)
	if err := os.WriteFile(creatingPath, []byte("incomplete ext4 creation"), 0o600); err != nil {
		t.Fatal(err)
	}

	resumed, err := backend.ResumeUnstarted(context.Background(), instance)
	if err != nil || !resumed {
		t.Fatalf("partial managed data-backing recovery = %t, %v", resumed, err)
	}
	if host.startCount() != 1 || !host.hasCall("avdmanager", "delete") || !host.hasCall("avdmanager", "create") {
		t.Fatalf("partial managed data backing was not retired, recreated, and started: calls=%#v starts=%d", host.calls, host.startCount())
	}
	if _, err := os.Lstat(creatingPath); !os.IsNotExist(err) {
		t.Fatalf("partial managed data creation stage remains after recovery: %v", err)
	}
	if _, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayPresent); err != nil {
		t.Fatalf("recovered managed data storage is not exact: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := backend.Destroy(ctx, instance); err != nil {
		t.Fatal(err)
	}
}

func TestManagedEmulatorResumeUnstartedRehashesImageBeforeAVDMutation(t *testing.T) {
	backend, host, instance, imageFile := managedUnstartedRecoveryFixture(t)
	file, err := os.OpenFile(imageFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("mutated")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if resumed, err := backend.ResumeUnstarted(context.Background(), instance); err == nil || resumed {
		t.Fatalf("mutated mapped image recovery = %t, %v", resumed, err)
	}
	if host.hasCall("avdmanager", "delete") || host.startCount() != 0 {
		t.Fatal("mutated image bytes reached AVD deletion or emulator start")
	}
}

func managedUnstartedRecoveryFixture(t *testing.T) (*ManagedEmulatorBackend, *managedEmulatorFakeHost, Instance, string) {
	t.Helper()
	root := t.TempDir()
	imageDirectory := filepath.Join(root, "installed-image")
	if err := os.MkdirAll(imageDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	imageFile := filepath.Join(imageDirectory, "system.img")
	if err := os.WriteFile(imageFile, []byte("exact managed image bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := DigestManagedSystemImage(imageDirectory)
	if err != nil {
		t.Fatal(err)
	}
	plan := managedBackendTestPlan(t, root, digest)
	instance := instanceFromPlan(plan)
	for _, directory := range []string{instance.StateDirectory, instance.SystemImageDirectory} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	image := ManagedSystemImage{Package: "system-images;android-35;google_apis;x86_64", Directory: imageDirectory}
	binding, err := json.MarshalIndent(managedImageBinding{Digest: digest.String(), Package: image.Package, Directory: image.Directory}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(instance.SystemImageDirectory, managedImageBindingFilename), append(binding, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	avdHome := filepath.Join(root, managedAVDDirectory)
	sdkRoot := filepath.Join(root, "sdk")
	prepareManagedFakeFormatter(t, sdkRoot)
	avdPath := filepath.Join(avdHome, instance.RuntimeID+".avd")
	if err := os.MkdirAll(avdPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(avdPath, "config.ini"), []byte("disk.dataPartition.size = 6442450944\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedAVDINI(filepath.Join(avdHome, instance.RuntimeID+".ini"), avdPath); err != nil {
		t.Fatal(err)
	}
	host := newManagedEmulatorFakeHost(instance.RuntimeID)
	host.avds[instance.RuntimeID] = avdPath
	server, err := parseADBServerEndpoint("127.0.0.1:5040")
	if err != nil {
		t.Fatal(err)
	}
	backend := &ManagedEmulatorBackend{
		runner: host, starter: host, emulatorBinary: "emulator", adbBinary: "adb", adbServer: server,
		avdManagerBinary: "avdmanager", sdkRoot: sdkRoot, stateRoot: root, avdHome: avdHome,
		systemImages: map[string]ManagedSystemImage{digest.String(): image}, pollInterval: time.Millisecond,
		shutdownTimeout: time.Second, maximumLogBytes: 64, now: time.Now, processAuthority: host,
		commitLaunchIntent: commitManagedLaunchIntent, processes: make(map[string]*managedProcess),
	}
	if err := configureManagedAVDDataPartition(avdPath, backend.managedDataImagePath(instance), instance.Resources.StorageBytes); err != nil {
		t.Fatal(err)
	}
	materializeManagedTestStorage(t, backend, instance, false)
	return backend, host, instance, imageFile
}

func removeManagedTestArguments(arguments []string, name string, width int) []string {
	result := append([]string(nil), arguments...)
	for index := range result {
		if result[index] == name {
			return append(result[:index], result[index+width:]...)
		}
	}
	return result
}

func TestManagedEmulatorStartRetriesWrappedMissingOverlayUntilOwnershipCommit(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	host.managedOverlayMissingProofs = 2
	backend, instance := managedRollbackTestBackend(t, host, managedEmulatorMinimumPartitionMiB*androidcontract.Mebibyte)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := backend.Destroy(ctx, instance); err != nil {
			t.Errorf("destroy delayed-overlay fixture: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := backend.Start(ctx, instance); err != nil {
		t.Fatalf("start with PID preceding managed overlay: %v", err)
	}
	if proofs := host.managedOverlayProofCount(); proofs != 3 {
		t.Fatalf("managed overlay proofs after PID = %d, want exactly 3", proofs)
	}
	if _, found, err := loadManagedProcessOwnership(instance, host); err != nil || !found {
		t.Fatalf("delayed overlay did not commit exact process ownership: found=%t err=%v", found, err)
	}
	record := backend.processes[instance.RuntimeID]
	if record == nil {
		t.Fatal("delayed overlay start retained no in-memory process ownership")
	}
	if running, err := managedProcessState(record); err != nil || !running {
		t.Fatalf("delayed overlay process state = running=%t err=%v", running, err)
	}
}

func TestManagedEmulatorStartMissingOverlayRemainsBoundedAndFailClosed(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	host.managedOverlayNeverReady = true
	host.managedOverlayProofTarget = 3
	host.managedOverlayProofTargetReached = make(chan struct{})
	host.launcherContainmentStopsHost = true
	backend, instance := managedRollbackTestBackend(t, host, managedEmulatorMinimumPartitionMiB*androidcontract.Mebibyte)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() {
		startDone <- backend.Start(ctx, instance)
	}()
	select {
	case <-host.managedOverlayProofTargetReached:
		cancel()
	case err := <-startDone:
		cancel()
		t.Fatalf("start returned before retrying the missing overlay: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("start did not make bounded progress while the overlay remained absent")
	}

	var startErr error
	select {
	case startErr = <-startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("start did not finish bounded fail-closed cleanup after cancellation")
	}
	if !errors.Is(startErr, context.Canceled) {
		t.Fatalf("missing-overlay start error = %v, want context.Canceled", startErr)
	}
	if proofs := host.managedOverlayProofCount(); proofs < 3 {
		t.Fatalf("managed overlay proofs after PID = %d, want at least 3", proofs)
	}
	if host.running() {
		t.Fatal("missing-overlay cleanup left the fake QEMU successor running")
	}
	record := backend.processes[instance.RuntimeID]
	if record == nil || !channelClosed(record.done) || !channelClosed(record.logsDone) {
		t.Fatal("missing-overlay cleanup did not drain the retained launcher and logs")
	}
	if running, err := managedProcessState(record); err != nil || running {
		t.Fatalf("missing-overlay process state after cleanup = running=%t err=%v", running, err)
	}
	if _, found, err := loadManagedProcessOwnership(instance, host); err != nil || found {
		t.Fatalf("missing overlay committed process ownership: found=%t err=%v", found, err)
	}
	if _, found, err := loadManagedLaunchIntent(instance, backend.emulatorBinary); err != nil || !found {
		t.Fatalf("fail-closed cleanup lost unresolved launch intent: found=%t err=%v", found, err)
	}
	if _, err := os.Lstat(filepath.Join(instance.StateDirectory, managedEmulatorDataOverlayFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing overlay unexpectedly appeared: %v", err)
	}
}

func TestManagedEmulatorPreProcessStartFailuresPermitExactRollback(t *testing.T) {
	for _, test := range []struct {
		name            string
		preflightErr    error
		commitIntent    func(Instance, string, managedDataStorageBinding) error
		starterErr      error
		wantIntentClean bool
	}{
		{name: "authority preflight", preflightErr: fmt.Errorf("unsupported authority")},
		{name: "intent commit", commitIntent: func(Instance, string, managedDataStorageBinding) error { return fmt.Errorf("durable commit failed") }},
		{name: "starter", starterErr: fmt.Errorf("CreateProcess failed"), wantIntentClean: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newManagedEmulatorFakeHost("world-emulator-5560")
			host.preflightErr = test.preflightErr
			host.startErr = test.starterErr
			backend, instance := managedRollbackTestBackend(t, host)
			if test.commitIntent != nil {
				backend.commitLaunchIntent = test.commitIntent
			}
			if err := backend.Start(context.Background(), instance); err == nil {
				t.Fatal("pre-process start failure was accepted")
			}
			if host.startCount() != 0 || host.running() {
				t.Fatal("pre-process failure launched an emulator")
			}
			record := backend.processes[instance.RuntimeID]
			if record == nil {
				t.Fatal("pre-process failure did not record provably-never-started state for rollback")
			}
			if running, stateErr := managedProcessState(record); stateErr != nil || running {
				t.Fatalf("pre-process rollback state = running %t, error %v", running, stateErr)
			}
			if test.wantIntentClean {
				if _, err := os.Lstat(filepath.Join(instance.StateDirectory, managedEmulatorLaunchFilename)); !os.IsNotExist(err) {
					t.Fatalf("starter failure retained launch intent: %v", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := backend.Destroy(ctx, instance); err != nil {
				t.Fatalf("rollback provably-never-started AVD: %v", err)
			}
			if _, err := os.Stat(instance.StateDirectory); !os.IsNotExist(err) {
				t.Fatalf("rollback retained state directory: %v", err)
			}
		})
	}
}

func TestManagedEmulatorStaleIntentRemainsUnresolved(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	backend, instance := managedRollbackTestBackend(t, host)
	storage, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayAbsent)
	if err != nil {
		t.Fatal(err)
	}
	if err := commitManagedLaunchIntent(instance, backend.emulatorBinary, storage); err != nil {
		t.Fatal(err)
	}
	if err := backend.Start(context.Background(), instance); err == nil {
		t.Fatal("stale launch intent permitted a second launch")
	}
	if backend.processes[instance.RuntimeID] != nil {
		t.Fatal("stale launch intent was weakened to never-started state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := backend.Destroy(ctx, instance); !errors.Is(err, errManagedLaunchUnresolved) {
		t.Fatalf("stale launch intent authorized destruction: %v", err)
	}
	if _, err := os.Stat(backend.avdPath(instance.RuntimeID)); err != nil {
		t.Fatalf("unresolved AVD was not retained: %v", err)
	}
}

func TestManagedHostProcessAuthorityFailureClassesRemainFailClosed(t *testing.T) {
	root := t.TempDir()
	instance := instanceFromPlan(managedBackendTestPlan(t, root, domain.NewDigest([]byte("process-error-image"))))
	if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(instance.StateDirectory, managedEmulatorPIDFilename), []byte("4242\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sdkRoot := filepath.Join(root, "sdk")
	prepareManagedFakeFormatter(t, sdkRoot)
	runner := newManagedEmulatorFakeHost(instance.RuntimeID)
	storageBackend := &ManagedEmulatorBackend{runner: runner, sdkRoot: sdkRoot, stateRoot: root}
	storage := materializeManagedTestStorage(t, storageBackend, instance, false)
	if err := commitManagedLaunchIntent(instance, "emulator", storage); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedTestQCOW2(storage.OverlayPath, filepath.Base(storage.BackingPath), storage.BackingBytes); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		authorityErr error
		wantClass    error
	}{
		{name: "absent", authorityErr: fmt.Errorf("PID exited: %w", errManagedHostProcessNotFound), wantClass: errManagedHostProcessNotFound},
		{name: "reused executable", authorityErr: fmt.Errorf("PID image changed: %w", errManagedHostProcessIdentityMismatch), wantClass: errManagedHostProcessIdentityMismatch},
		{name: "access ambiguity", authorityErr: fmt.Errorf("access denied")},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &ManagedEmulatorBackend{
				runner: runner, sdkRoot: sdkRoot, stateRoot: root, emulatorBinary: "emulator",
				processAuthority: managedErrorProcessAuthority{err: test.authorityErr},
			}
			done := make(chan struct{})
			logsDone := make(chan struct{})
			close(done)
			close(logsDone)
			_, err := backend.adoptManagedHostProcess(context.Background(), instance, &managedProcess{done: done, logsDone: logsDone})
			if err == nil {
				t.Fatal("ambiguous host-process authority was accepted")
			}
			if test.wantClass != nil && !errors.Is(err, test.wantClass) {
				t.Fatalf("host-process error %v lost class %v", err, test.wantClass)
			}
			if test.wantClass == nil && (errors.Is(err, errManagedHostProcessNotFound) || errors.Is(err, errManagedHostProcessIdentityMismatch)) {
				t.Fatalf("access ambiguity was weakened to safe absence: %v", err)
			}
		})
	}
}

func TestManagedEmulatorOwnershipFailureAlwaysContainsRetainedLauncher(t *testing.T) {
	for _, test := range []struct {
		name         string
		pidContent   string
		authorityErr error
		wantErr      bool
	}{
		{name: "malformed PID", pidContent: "not-a-pid\n", wantErr: true},
		{name: "access ambiguity", pidContent: "4242\n", authorityErr: fmt.Errorf("access denied"), wantErr: true},
		{name: "identity mismatch", pidContent: "4242\n", authorityErr: fmt.Errorf("different process: %w", errManagedHostProcessIdentityMismatch), wantErr: true},
		{name: "successor absent", pidContent: "4242\n", authorityErr: fmt.Errorf("exited: %w", errManagedHostProcessNotFound)},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			instance := instanceFromPlan(managedBackendTestPlan(t, root, domain.NewDigest([]byte(test.name))))
			if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(managedEmulatorPIDPath(instance), []byte(test.pidContent), 0o600); err != nil {
				t.Fatal(err)
			}
			sdkRoot := filepath.Join(root, "sdk")
			prepareManagedFakeFormatter(t, sdkRoot)
			runner := newManagedEmulatorFakeHost(instance.RuntimeID)
			storageBackend := &ManagedEmulatorBackend{runner: runner, sdkRoot: sdkRoot, stateRoot: root}
			storage := materializeManagedTestStorage(t, storageBackend, instance, false)
			if err := commitManagedLaunchIntent(instance, "emulator", storage); err != nil {
				t.Fatal(err)
			}
			if err := writeManagedTestQCOW2(storage.OverlayPath, filepath.Base(storage.BackingPath), storage.BackingBytes); err != nil {
				t.Fatal(err)
			}
			launcher := newManagedFakeProcess(nil)
			record := &managedProcess{launcher: launcher, done: make(chan struct{}), logsDone: make(chan struct{})}
			go func() {
				record.waitErr = launcher.Wait()
				close(record.done)
				close(record.logsDone)
			}()
			backend := &ManagedEmulatorBackend{
				runner: runner, sdkRoot: sdkRoot, stateRoot: root, emulatorBinary: "emulator",
				processAuthority: managedErrorProcessAuthority{err: test.authorityErr},
			}
			err := backend.forceStopManagedProcess(instance, record)
			if (err != nil) != test.wantErr {
				t.Fatalf("force-stop error = %v, want error=%t", err, test.wantErr)
			}
			if !channelClosed(record.done) || !channelClosed(record.logsDone) {
				t.Fatal("ownership failure returned before retained launcher and logs were drained")
			}
		})
	}
}

func TestManagedEmulatorPIDFileRejectsAmbiguousValues(t *testing.T) {
	for _, content := range []string{"4242\n4243\n", "+4242\n", "04242\n", "0\n", ""} {
		t.Run(fmt.Sprintf("%q", content), func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, managedEmulatorPIDFilename), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readManagedEmulatorPID(directory); err == nil {
				t.Fatalf("ambiguous managed emulator PID %q was accepted", content)
			}
		})
	}
}

func TestManagedEmulatorRestartRejectsReusedPIDWithoutKillingIt(t *testing.T) {
	host := newManagedEmulatorFakeHost("world-emulator-5560")
	backend, instance := intentRecoveryTestBackend(t, host)
	host.mu.Lock()
	host.starts = 2
	host.reachable = true
	host.process = newManagedFakeProcess(nil)
	host.mu.Unlock()
	if err := os.WriteFile(filepath.Join(instance.StateDirectory, managedEmulatorPIDFilename), []byte("4242\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	port, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		t.Fatal(err)
	}
	storage, err := backend.requireManagedDataStorage(context.Background(), instance, managedDataOverlayPresent)
	if err != nil {
		t.Fatal(err)
	}
	ownership := managedProcessOwnership{
		RuntimeID: instance.RuntimeID, AVDName: instance.Allocation.InstanceName, Serial: instance.Allocation.Serial,
		ConsolePort: port, PID: 4242, PIDFile: managedEmulatorPIDFilename,
		ExecutablePath: `C:\fake-sdk\emulator\qemu-system-x86_64-headless.exe`, StartToken: "fake-start-1",
		ResourceAuthority: host.Kind(), ResourceIdentity: host.ResourceIdentity(instance),
		CPUMilli: instance.Resources.CPUMilli, MemoryBytes: instance.Resources.MemoryBytes, StorageBytes: instance.Resources.StorageBytes,
		GuestMemoryBytes: instance.GuestMemoryBytes, ResourceAnchored: true, Storage: storage.authority(instance),
	}
	if err := commitManagedProcessOwnership(instance, ownership, host); err != nil {
		t.Fatal(err)
	}
	record, err := backend.requireManagedProcessRecord(context.Background(), instance)
	if err != nil {
		t.Fatalf("reconstruct stopped authority after exact PID reuse: %v", err)
	}
	if running, stateErr := managedProcessState(record); stateErr != nil || running {
		t.Fatalf("reused PID was not classified as the original process being stopped: running=%t err=%v", running, stateErr)
	}
	if !host.running() {
		t.Fatal("PID-reuse rejection killed the unrelated successor")
	}
}

type managedErrorProcessAuthority struct{ err error }

func (a managedErrorProcessAuthority) ResolveExecutable(value string) (string, error) {
	return value, nil
}
func (a managedErrorProcessAuthority) Preflight(string) error  { return nil }
func (a managedErrorProcessAuthority) Kind() string            { return "test_error" }
func (a managedErrorProcessAuthority) ResourcesEnforced() bool { return true }
func (a managedErrorProcessAuthority) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}
func (a managedErrorProcessAuthority) PreflightResources(context.Context, admission.Resources) error {
	return a.err
}
func (a managedErrorProcessAuthority) StartContained(context.Context, command.Starter, command.Invocation, Instance) (command.Process, error) {
	return nil, a.err
}
func (a managedErrorProcessAuthority) Open(int, string, string, managedDataStorageBinding, Instance) (managedHostProcess, error) {
	return nil, a.err
}

func intentRecoveryTestBackend(t *testing.T, host *managedEmulatorFakeHost) (*ManagedEmulatorBackend, Instance) {
	t.Helper()
	root := t.TempDir()
	plan := managedBackendTestPlan(t, root, domain.NewDigest([]byte("incomplete-launch-image")))
	instance := instanceFromPlan(plan)
	if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	sdkRoot := filepath.Join(root, "sdk")
	prepareManagedFakeFormatter(t, sdkRoot)
	adbServer, err := parseADBServerEndpoint("127.0.0.1:5040")
	if err != nil {
		t.Fatal(err)
	}
	backend := &ManagedEmulatorBackend{
		runner: host, adbBinary: "adb", adbServer: adbServer, emulatorBinary: "emulator",
		sdkRoot: sdkRoot, stateRoot: root, avdHome: filepath.Join(root, managedAVDDirectory), processAuthority: host, pollInterval: time.Millisecond, shutdownTimeout: 3 * time.Second,
		now: time.Now, processes: make(map[string]*managedProcess),
	}
	binding := materializeManagedTestStorage(t, backend, instance, false)
	if err := commitManagedLaunchIntent(instance, "emulator", binding); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedTestQCOW2(binding.OverlayPath, filepath.Base(binding.BackingPath), binding.BackingBytes); err != nil {
		t.Fatal(err)
	}
	return backend, instance
}

func managedRollbackTestBackend(t *testing.T, host *managedEmulatorFakeHost, storageBytesOverride ...int64) (*ManagedEmulatorBackend, Instance) {
	t.Helper()
	root := t.TempDir()
	instance := instanceFromPlan(managedBackendTestPlan(t, root, domain.NewDigest([]byte("rollback-image"))))
	if len(storageBytesOverride) > 1 {
		t.Fatal("managed rollback fixture accepts at most one storage-size override")
	}
	if len(storageBytesOverride) == 1 {
		instance.Resources.StorageBytes = storageBytesOverride[0]
	}
	if err := os.MkdirAll(instance.StateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	avdHome := filepath.Join(root, managedAVDDirectory)
	sdkRoot := filepath.Join(root, "sdk")
	prepareManagedFakeFormatter(t, sdkRoot)
	avdPath := filepath.Join(avdHome, instance.RuntimeID+".avd")
	if err := os.MkdirAll(avdPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(avdPath, "config.ini"), []byte("disk.dataPartition.size = 6442450944\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedAVDINI(filepath.Join(avdHome, instance.RuntimeID+".ini"), avdPath); err != nil {
		t.Fatal(err)
	}
	host.mu.Lock()
	host.avds[instance.RuntimeID] = avdPath
	host.mu.Unlock()
	adbServer, err := parseADBServerEndpoint("127.0.0.1:5040")
	if err != nil {
		t.Fatal(err)
	}
	backend := &ManagedEmulatorBackend{
		runner: host, starter: host, emulatorBinary: "emulator", adbBinary: "adb", adbServer: adbServer,
		avdManagerBinary: "avdmanager", sdkRoot: sdkRoot, stateRoot: root, avdHome: avdHome,
		pollInterval: time.Millisecond, shutdownTimeout: time.Second, maximumLogBytes: 64, now: time.Now,
		processAuthority: host, commitLaunchIntent: commitManagedLaunchIntent, processes: make(map[string]*managedProcess),
	}
	if err := configureManagedAVDDataPartition(avdPath, backend.managedDataImagePath(instance), instance.Resources.StorageBytes); err != nil {
		t.Fatal(err)
	}
	materializeManagedTestStorage(t, backend, instance, false)
	return backend, instance
}

func managedBackendTestPlan(t *testing.T, root string, imageDigest domain.Digest) VirtualDevicePlan {
	t.Helper()
	targetID, _ := domain.NewTargetID()
	leaseID, _ := domain.NewLeaseID()
	targetRoot := root + string(os.PathSeparator) + "targets"
	imageRoot := root + string(os.PathSeparator) + "images"
	return VirtualDevicePlan{
		Name: "world-android-" + targetID.UUID() + "-g1", LeaseID: leaseID, TargetID: targetID, Generation: 1,
		StateDirectory:       targetRoot + string(os.PathSeparator) + targetID.String() + string(os.PathSeparator) + "generations" + string(os.PathSeparator) + "1",
		SystemImageDirectory: imageRoot + string(os.PathSeparator) + strings.ReplaceAll(imageDigest.String(), ":", "-"),
		Allocation:           emulatorAllocation(5560),
		ADBServer:            ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5037},
		Fingerprint: ResetFingerprint{
			BackendVersion: "Android emulator version 35.2.10", RuntimeVersion: "google/sdk_gphone64_x86_64/emu35:userdebug/test-keys",
			SystemImageDigest: imageDigest, DeviceConfigDigest: domain.NewDigest([]byte("managed-config")), Features: []string{"root", "headless"},
		},
		Resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 1 << 30}, GuestMemoryBytes: 2 << 30,
		Labels: map[string]string{"world.role": "android-virtual-target"}, BaselineState: ports.AndroidBaselineCleanBoot,
		RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true, BootTimeout: time.Second,
	}
}

func completeAndroidTemplate(name, driver string, image domain.Digest) ports.TargetTemplate {
	return ports.TargetTemplate{
		Name: name, Kind: domain.TargetAndroidVirtualDevice, Driver: driver, ImageDigest: image,
		IsolationProfile: "instrumented-android", BaselineState: ports.AndroidBaselineCleanBoot, RequireHardwareAcceleration: true,
		Headless: true, Rooted: true, Debuggable: true, GuestMemoryBytes: 2 << 30, BootTimeout: time.Minute,
	}
}

type managedEmulatorFakeHost struct {
	mu                               sync.Mutex
	avdName                          string
	avds                             map[string]string
	calls                            []command.Invocation
	start                            command.Invocation
	process                          *managedFakeProcess
	starts                           int
	reachable                        bool
	rooted                           bool
	launcherExits                    bool
	pidFileContent                   string
	closedAuthorities                int
	resourceAnchors                  int
	preflightErr                     error
	startErr                         error
	hostProcessKillErr               error
	hostProcessKills                 int
	hostProcessExitDelay             time.Duration
	hostProcessKillLeavesRunning     bool
	adbKillLeavesRunning             bool
	hostProcessExitScheduled         bool
	launcherContainmentStopsHost     bool
	dataPartitionBytes               int64
	runtimeFingerprint               string
	bootCompleted                    string
	managedOverlayMissingProofs      int
	managedOverlayNeverReady         bool
	managedOverlayProofs             int
	managedOverlayProofTarget        int
	managedOverlayProofTargetReached chan struct{}
	pendingManagedOverlayPath        string
	pendingManagedOverlayBacking     string
	pendingManagedOverlayBytes       int64
}

func newManagedEmulatorFakeHost(avdName string) *managedEmulatorFakeHost {
	return &managedEmulatorFakeHost{
		avdName: avdName, avds: make(map[string]string),
		runtimeFingerprint: "google/sdk_gphone64_x86_64/emu35:userdebug/test-keys", bootCompleted: "1",
	}
}

func (h *managedEmulatorFakeHost) Run(_ context.Context, invocation command.Invocation) (command.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, invocation)
	if strings.EqualFold(filepath.Base(invocation.Program), executableName("mke2fs", ".exe")) {
		if reflect.DeepEqual(invocation.Args, []string{"-V"}) {
			if err := h.advanceManagedOverlayProofLocked(); err != nil {
				return command.Result{}, err
			}
			return command.Result{Stderr: []byte("mke2fs 1.46.6 (1-Feb-2023)\n")}, nil
		}
		if len(invocation.Args) == 8 && reflect.DeepEqual(invocation.Args[:7], []string{"-t", "ext4", "-F", "-m", "0", "-L", "data"}) {
			config := filepath.Join(filepath.Dir(invocation.Program), "mke2fs.conf")
			if !environmentHasExactValue(invocation.Environment, "MKE2FS_CONFIG", config) {
				return command.Result{}, fmt.Errorf("mke2fs invocation omitted exact MKE2FS_CONFIG")
			}
			file, err := os.OpenFile(invocation.Args[7], os.O_WRONLY, 0)
			if err != nil {
				return command.Result{}, err
			}
			_, writeErr := file.WriteAt([]byte("fake-ext4"), 0x438)
			return command.Result{}, errors.Join(writeErr, file.Close())
		}
		return command.Result{}, fmt.Errorf("unexpected mke2fs invocation")
	}
	switch invocation.Program {
	case "emulator":
		if reflect.DeepEqual(invocation.Args, []string{"-version"}) {
			return command.Result{Stdout: []byte("Android emulator version 35.2.10\n")}, nil
		}
		if reflect.DeepEqual(invocation.Args, []string{"-accel-check"}) {
			return command.Result{Stdout: []byte("accel:\n0\nWHPX(10.0) is installed and usable.\n")}, nil
		}
		if reflect.DeepEqual(invocation.Args, []string{"-list-avds"}) {
			names := make([]string, 0, len(h.avds))
			for name := range h.avds {
				names = append(names, name)
			}
			return command.Result{Stdout: []byte(strings.Join(names, "\n"))}, nil
		}
	case "adb":
		if reflect.DeepEqual(invocation.Args, []string{"-H", "127.0.0.1", "-P", "5040", "version"}) {
			return command.Result{Stdout: []byte("Android Debug Bridge version 1.0.41\n")}, nil
		}
		return h.runADBLocked(invocation)
	case "sdkmanager":
		if reflect.DeepEqual(invocation.Args, []string{"--version"}) {
			return command.Result{Stdout: []byte("19.0\n")}, nil
		}
		return command.Result{}, fmt.Errorf("unexpected sdkmanager invocation")
	case "avdmanager":
		if len(invocation.Args) >= 2 && reflect.DeepEqual(invocation.Args[:2], []string{"create", "avd"}) {
			name, path := argumentAfter(invocation.Args, "--name"), argumentAfter(invocation.Args, "--path")
			if name == "" || path == "" {
				return command.Result{}, fmt.Errorf("incomplete avd create")
			}
			if err := os.MkdirAll(path, 0o700); err != nil {
				return command.Result{}, err
			}
			if err := os.WriteFile(filepath.Join(path, "config.ini"), []byte("disk.dataPartition.size = 6442450944\ndisk.cachePartition = yes\nhw.sdCard = yes\n"), 0o600); err != nil {
				return command.Result{}, err
			}
			if err := writeManagedAVDINI(filepath.Join(filepath.Dir(path), name+".ini"), path); err != nil {
				return command.Result{}, err
			}
			h.avds[name] = path
			return command.Result{Stdout: []byte("created")}, nil
		}
		if len(invocation.Args) >= 4 && reflect.DeepEqual(invocation.Args[:3], []string{"delete", "avd", "--name"}) {
			name := invocation.Args[3]
			path := h.avds[name]
			delete(h.avds, name)
			_ = os.RemoveAll(path)
			_ = os.Remove(filepath.Join(filepath.Dir(path), name+".ini"))
			return command.Result{}, nil
		}
	}
	return command.Result{}, fmt.Errorf("unexpected invocation: %#v", invocation)
}

func writeManagedAVDINI(path, avdPath string) error {
	return os.WriteFile(path, []byte("path="+avdPath+"\n"), 0o600)
}

func (h *managedEmulatorFakeHost) runADBLocked(invocation command.Invocation) (command.Result, error) {
	action, exact := exactADBTestAction(invocation.Args, "127.0.0.1:5040", "emulator-5560")
	if !exact {
		return command.Result{}, fmt.Errorf("ADB did not select exact serial")
	}
	if !h.reachable {
		return command.Result{ExitCode: 1, Stderr: []byte("error: device not found")}, fmt.Errorf("device not found")
	}
	switch {
	case reflect.DeepEqual(action, []string{"get-state"}):
		return command.Result{Stdout: []byte("device\n")}, nil
	case reflect.DeepEqual(action, []string{"emu", "avd", "name"}):
		return command.Result{Stdout: []byte(h.avdName + "\nOK\n")}, nil
	case reflect.DeepEqual(action, []string{"root"}):
		h.rooted = true
		return command.Result{Stdout: []byte("restarting adbd as root\n")}, nil
	case reflect.DeepEqual(action, []string{"emu", "kill"}):
		if !h.adbKillLeavesRunning {
			h.reachable = false
			if h.process != nil {
				// Finishing the process invokes an on-finish callback that takes h.mu.
				// This fake is already holding h.mu while it services the ADB command,
				// so let the callback run after Run releases the lock.
				go h.process.finish()
			}
		}
		return command.Result{Stdout: []byte("OK\n")}, nil
	case reflect.DeepEqual(action, []string{"shell", "id", "-u"}):
		if h.rooted {
			return command.Result{Stdout: []byte("0\n")}, nil
		}
		return command.Result{Stdout: []byte("2000\n")}, nil
	case reflect.DeepEqual(action, []string{"shell", "cmd", "package", "path", "android"}):
		return command.Result{Stdout: []byte("package:/system/framework/framework-res.apk\n")}, nil
	case reflect.DeepEqual(action, []string{"shell", "cat", "/proc/mounts"}):
		return command.Result{Stdout: []byte("/dev/block/dm-7 /data ext4 rw 0 0\n")}, nil
	case reflect.DeepEqual(action, []string{"shell", "blockdev", "--getsize64", "/dev/block/dm-7"}):
		return command.Result{Stdout: []byte(strconv.FormatInt(h.dataPartitionBytes, 10) + "\n")}, nil
	case len(action) == 4 && reflect.DeepEqual(action[:2], []string{"shell", "getprop"}):
		return command.Result{}, fmt.Errorf("invalid getprop")
	case len(action) == 3 && reflect.DeepEqual(action[:2], []string{"shell", "getprop"}):
		properties := map[string]string{
			"init.svc.bootanim": "stopped", "ro.boot.qemu.avd_name": h.avdName,
			"ro.build.fingerprint":        "google/sdk_gphone64_x86_64/emu35:userdebug/test-keys",
			"ro.system.build.fingerprint": h.runtimeFingerprint,
			"ro.build.version.sdk":        "35", "ro.debuggable": "1", "ro.kernel.qemu": "1",
			"ro.product.cpu.abi": "x86_64", "ro.secure": "0", "ro.serialno": "EMULATOR5560", "sys.boot_completed": h.bootCompleted,
		}
		return command.Result{Stdout: []byte(properties[action[2]] + "\n")}, nil
	}
	return command.Result{}, fmt.Errorf("unexpected exact-serial ADB action %#v", action)
}

func (h *managedEmulatorFakeHost) Start(_ context.Context, invocation command.Invocation) (command.Process, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if invocation.Program != "emulator" {
		return nil, fmt.Errorf("unexpected start program")
	}
	if h.startErr != nil {
		return nil, h.startErr
	}
	h.start = invocation
	h.starts++
	h.reachable, h.rooted = true, false
	avdPath := h.avds[argumentAfter(invocation.Args, "-avd")]
	config, err := os.ReadFile(filepath.Join(avdPath, "config.ini"))
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(config), "\n") {
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == "disk.dataPartition.size" {
			h.dataPartitionBytes, err = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	pidFile := argumentAfter(invocation.Args, "-pidfile")
	if pidFile == "" {
		return nil, fmt.Errorf("managed emulator start omitted exact QEMU PID file")
	}
	dataImage := argumentAfter(invocation.Args, "-data")
	if dataImage == "" {
		return nil, fmt.Errorf("managed emulator start omitted exact data backing")
	}
	if err := h.prepareManagedOverlayLocked(dataImage); err != nil {
		return nil, err
	}
	pidContent := h.pidFileContent
	if pidContent == "" {
		pidContent = "4242\n"
	}
	if err := os.WriteFile(pidFile, []byte(pidContent), 0o600); err != nil {
		return nil, err
	}
	process := newManagedFakeProcess(func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.launcherContainmentStopsHost {
			h.reachable = false
		}
	})
	h.process = process
	go func() {
		_, _ = process.stdoutW.Write(bytes.Repeat([]byte("o"), 256))
		_, _ = process.stderrW.Write(bytes.Repeat([]byte("e"), 256))
	}()
	if h.launcherExits {
		go process.finish()
	}
	return process, nil
}

func (h *managedEmulatorFakeHost) prepareManagedOverlayLocked(dataImage string) error {
	path := dataImage + ".qcow2"
	backing := filepath.Base(dataImage)
	if h.managedOverlayMissingProofs <= 0 && !h.managedOverlayNeverReady {
		return writeManagedTestQCOW2(path, backing, h.dataPartitionBytes)
	}
	h.pendingManagedOverlayPath = path
	h.pendingManagedOverlayBacking = backing
	h.pendingManagedOverlayBytes = h.dataPartitionBytes
	return nil
}

func (h *managedEmulatorFakeHost) advanceManagedOverlayProofLocked() error {
	if h.pendingManagedOverlayPath == "" {
		return nil
	}
	h.managedOverlayProofs++
	if h.managedOverlayProofTargetReached != nil && h.managedOverlayProofs == h.managedOverlayProofTarget {
		close(h.managedOverlayProofTargetReached)
	}
	if h.managedOverlayNeverReady || h.managedOverlayProofs <= h.managedOverlayMissingProofs {
		return nil
	}
	if err := writeManagedTestQCOW2(h.pendingManagedOverlayPath, h.pendingManagedOverlayBacking, h.pendingManagedOverlayBytes); err != nil {
		return err
	}
	h.pendingManagedOverlayPath = ""
	h.pendingManagedOverlayBacking = ""
	h.pendingManagedOverlayBytes = 0
	return nil
}

func (h *managedEmulatorFakeHost) managedOverlayProofCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.managedOverlayProofs
}

func (h *managedEmulatorFakeHost) hasCall(program, argument string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, invocation := range h.calls {
		if invocation.Program == program && len(invocation.Args) > 0 && invocation.Args[0] == argument {
			return true
		}
	}
	return false
}

func (h *managedEmulatorFakeHost) hasExactADBAction(expected ...string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, invocation := range h.calls {
		action, exact := exactADBTestAction(invocation.Args, "127.0.0.1:5040", "emulator-5560")
		if invocation.Program == "adb" && exact && reflect.DeepEqual(action, expected) {
			return true
		}
	}
	return false
}

func (h *managedEmulatorFakeHost) startInvocation() command.Invocation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.start
}

func (h *managedEmulatorFakeHost) running() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reachable
}

func (h *managedEmulatorFakeHost) startCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.starts
}

func (h *managedEmulatorFakeHost) closeCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closedAuthorities
}

func (h *managedEmulatorFakeHost) hostProcessKillCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hostProcessKills
}

func (h *managedEmulatorFakeHost) ResolveExecutable(value string) (string, error) {
	return value, nil
}
func (h *managedEmulatorFakeHost) Preflight(string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.preflightErr
}
func (h *managedEmulatorFakeHost) Kind() string            { return "test_exact_handle" }
func (h *managedEmulatorFakeHost) ResourcesEnforced() bool { return true }
func (h *managedEmulatorFakeHost) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}
func (h *managedEmulatorFakeHost) PreflightResources(context.Context, admission.Resources) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.preflightErr
}
func (h *managedEmulatorFakeHost) StartContained(ctx context.Context, starter command.Starter, invocation command.Invocation, _ Instance) (command.Process, error) {
	return starter.Start(ctx, invocation)
}
func (h *managedEmulatorFakeHost) Open(pid int, _ string, pidFile string, storage managedDataStorageBinding, instance Instance) (managedHostProcess, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if pid != 4242 {
		return nil, fmt.Errorf("unexpected fake managed emulator PID %d", pid)
	}
	if !h.reachable {
		return nil, fmt.Errorf("fake managed emulator PID %d is absent: %w", pid, errManagedHostProcessNotFound)
	}
	if startedPIDFile := argumentAfter(h.start.Args, "-pidfile"); filepath.Clean(startedPIDFile) != filepath.Clean(pidFile) {
		return nil, fmt.Errorf("fake managed emulator PID %d has a different PID-file launch binding: %w", pid, errManagedHostProcessIdentityMismatch)
	}
	if err := storage.validate(instance); err != nil {
		return nil, fmt.Errorf("fake managed emulator PID %d has different storage binding: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	if err := requireExactManagedRuntimeArguments(h.start.Args, pidFile, storage.BackingPath, instance, runtime.GOOS == "windows"); err != nil {
		return nil, fmt.Errorf("fake managed emulator PID %d has different runtime arguments: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	return &managedFakeHostProcess{host: h, pid: pid, startToken: fmt.Sprintf("fake-start-%d", h.starts)}, nil
}

func environmentHasExactValue(environment []string, key, value string) bool {
	for _, entry := range environment {
		name, candidate, found := strings.Cut(entry, "=")
		if found && strings.EqualFold(name, key) && candidate == value {
			return true
		}
	}
	return false
}

func writeManagedTestQCOW2(path, backingName string, virtualBytes int64) error {
	const backingOffset = 72
	header := make([]byte, backingOffset+len(backingName))
	copy(header[:4], []byte{'Q', 'F', 'I', 0xfb})
	binary.BigEndian.PutUint32(header[4:8], 3)
	binary.BigEndian.PutUint64(header[8:16], backingOffset)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(backingName)))
	binary.BigEndian.PutUint64(header[24:32], uint64(virtualBytes))
	copy(header[backingOffset:], backingName)
	return os.WriteFile(path, header, 0o600)
}

type managedFakeHostProcess struct {
	host       *managedEmulatorFakeHost
	pid        int
	startToken string
	closed     bool
}

func (p *managedFakeHostProcess) AnchorResourceAuthority() error {
	p.host.mu.Lock()
	defer p.host.mu.Unlock()
	p.host.resourceAnchors++
	return nil
}

func (p *managedFakeHostProcess) PID() int { return p.pid }
func (p *managedFakeHostProcess) ExecutablePath() string {
	return `C:\fake-sdk\emulator\qemu-system-x86_64-headless.exe`
}
func (p *managedFakeHostProcess) StartToken() string { return p.startToken }
func (p *managedFakeHostProcess) Running() (bool, error) {
	p.host.mu.Lock()
	defer p.host.mu.Unlock()
	if p.startToken != fmt.Sprintf("fake-start-%d", p.host.starts) {
		return false, fmt.Errorf("fake managed emulator PID was reused")
	}
	return p.host.reachable, nil
}
func (p *managedFakeHostProcess) Kill() error {
	p.host.mu.Lock()
	if p.startToken != fmt.Sprintf("fake-start-%d", p.host.starts) {
		p.host.mu.Unlock()
		return fmt.Errorf("fake managed emulator PID was reused")
	}
	p.host.hostProcessKills++
	killErr := p.host.hostProcessKillErr
	if p.host.hostProcessKillLeavesRunning {
		p.host.mu.Unlock()
		return killErr
	}
	exitDelay := p.host.hostProcessExitDelay
	if exitDelay > 0 {
		if !p.host.hostProcessExitScheduled {
			p.host.hostProcessExitScheduled = true
			time.AfterFunc(exitDelay, p.host.finishHostProcess)
		}
		p.host.mu.Unlock()
		return killErr
	}
	p.host.reachable = false
	launcher := p.host.process
	p.host.mu.Unlock()
	if launcher != nil {
		launcher.finish()
	}
	return killErr
}

func (h *managedEmulatorFakeHost) finishHostProcess() {
	h.mu.Lock()
	h.reachable = false
	launcher := h.process
	h.mu.Unlock()
	if launcher != nil {
		launcher.finish()
	}
}
func (p *managedFakeHostProcess) Close() error {
	p.host.mu.Lock()
	defer p.host.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.host.closedAuthorities++
	}
	return nil
}

type managedFakeProcess struct {
	stdin    io.WriteCloser
	stdoutR  *io.PipeReader
	stdoutW  *io.PipeWriter
	stderrR  *io.PipeReader
	stderrW  *io.PipeWriter
	done     chan struct{}
	onFinish func()
	once     sync.Once
}

func newManagedFakeProcess(onFinish func()) *managedFakeProcess {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &managedFakeProcess{stdin: managedDiscardWriteCloser{}, stdoutR: stdoutR, stdoutW: stdoutW, stderrR: stderrR, stderrW: stderrW, done: make(chan struct{}), onFinish: onFinish}
}

func (p *managedFakeProcess) Stdin() io.WriteCloser  { return p.stdin }
func (p *managedFakeProcess) Stdout() io.ReadCloser  { return p.stdoutR }
func (p *managedFakeProcess) Stderr() io.ReadCloser  { return p.stderrR }
func (p *managedFakeProcess) Wait() error            { <-p.done; return nil }
func (p *managedFakeProcess) Signal(os.Signal) error { p.finish(); return nil }
func (p *managedFakeProcess) Kill() error            { p.finish(); return nil }
func (p *managedFakeProcess) finish() {
	p.once.Do(func() {
		if p.onFinish != nil {
			p.onFinish()
		}
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		close(p.done)
	})
}

type managedDiscardWriteCloser struct{}

func (managedDiscardWriteCloser) Write(value []byte) (int, error) { return len(value), nil }
func (managedDiscardWriteCloser) Close() error                    { return nil }

type readinessErrorBackend struct {
	*statefulBackend
	err error
}

func (b *readinessErrorBackend) WaitReady(context.Context, Instance) (ReadinessState, error) {
	b.mu.Lock()
	b.actions = append(b.actions, "ready-failed:")
	b.mu.Unlock()
	return ReadinessState{}, b.err
}

func argumentAfter(args []string, name string) string {
	for index := range args {
		if args[index] == name && index+1 < len(args) {
			return args[index+1]
		}
	}
	return ""
}

func containsContiguous(values, expected []string) bool {
	for index := 0; index+len(expected) <= len(values); index++ {
		if reflect.DeepEqual(values[index:index+len(expected)], expected) {
			return true
		}
	}
	return false
}

var _ command.Runner = (*managedEmulatorFakeHost)(nil)
var _ command.Starter = (*managedEmulatorFakeHost)(nil)
