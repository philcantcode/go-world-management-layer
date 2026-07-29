package cuttlefish

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type BackendCapabilities struct {
	BackendKind               string
	BackendVersion            string
	RuntimeVersion            string
	KVM                       bool
	KVMKnown                  bool
	ResetModes                []ports.ResetMode
	Evidence                  map[string]string
	Managed                   bool
	Headless                  bool
	HeadlessKnown             bool
	Rooted                    bool
	RootedKnown               bool
	Debuggable                bool
	DebuggableKnown           bool
	CPUEnforced               bool
	MemoryEnforced            bool
	WritableStateEnforced     bool
	HardwareAcceleration      bool
	HardwareAccelerationKnown bool
}

type Instance struct {
	RuntimeID                   string
	StateDirectory              string
	SystemImageDirectory        string
	Allocation                  Allocation
	Fingerprint                 ResetFingerprint
	Resources                   admission.Resources
	BaselineState               string
	RequireHardwareAcceleration bool
	Headless                    bool
	Rooted                      bool
	Debuggable                  bool
	GuestMemoryBytes            int64
	BootTimeout                 time.Duration
}

type ReadinessState struct {
	ProcessRunning      bool
	ADBReady            bool
	BootCompleted       bool
	FrameworkReady      bool
	PackageManagerReady bool
	DeviceState         string
	Identity            AndroidIdentity
	ObservedAt          time.Time
}

func (r ReadinessState) Ready() bool {
	return r.ProcessRunning && r.ADBReady && r.DeviceState == "device" && r.BootCompleted && r.FrameworkReady && r.PackageManagerReady && r.Identity.Validate() == nil
}

func incompleteAndroidReadinessError(state ReadinessState, expectedAVDName string) error {
	identityErr := state.Identity.Validate()
	identityValid := identityErr == nil
	avdMatches := state.Identity.AVDName == expectedAVDName
	diagnostic := fmt.Sprintf(
		"Android readiness incomplete: process_running=%t adb_ready=%t device_state=%q boot_completed=%t framework_ready=%t package_manager_ready=%t rooted=%t debuggable=%t identity_valid=%t avd_name=%q expected_avd_name=%q avd_matches=%t",
		state.ProcessRunning,
		state.ADBReady,
		state.DeviceState,
		state.BootCompleted,
		state.FrameworkReady,
		state.PackageManagerReady,
		state.Identity.Rooted,
		state.Identity.Debuggable,
		identityValid,
		state.Identity.AVDName,
		expectedAVDName,
		avdMatches,
	)
	if identityErr != nil {
		diagnostic += fmt.Sprintf(" identity_error=%q", identityErr.Error())
	}
	return fmt.Errorf("%s", diagnostic)
}

type Backend interface {
	Probe(context.Context, ports.TargetTemplate) (BackendCapabilities, error)
	Create(context.Context, VirtualDevicePlan) (Instance, error)
	Start(context.Context, Instance) error
	WaitReady(context.Context, Instance) (ReadinessState, error)
	Inspect(context.Context, Instance) (ReadinessState, error)
	Stop(context.Context, Instance, ports.StopMode) error
	Destroy(context.Context, Instance) error
}

// BackendInventory is optional and authoritative only for runtime identities
// returned by the selected backend. A successful result must be complete.
type BackendInventory interface {
	ListRuntimeIDs(context.Context) ([]string, error)
}

// BackendUnstartedRecovery is implemented by backends whose authoritative
// inventory includes a fully configured physical resource before its runtime
// process starts. The method may start only an exact, durably planned resource
// after proving its endpoint is unused; it returns true only when it started it.
type BackendUnstartedRecovery interface {
	ResumeUnstarted(context.Context, Instance) (bool, error)
}

func instanceFromPlan(plan VirtualDevicePlan) Instance {
	return Instance{
		RuntimeID: plan.Allocation.InstanceName, StateDirectory: plan.StateDirectory,
		SystemImageDirectory: plan.SystemImageDirectory, Allocation: plan.Allocation,
		Fingerprint: plan.Fingerprint, Resources: plan.Resources.Clone(), BaselineState: plan.BaselineState,
		RequireHardwareAcceleration: plan.RequireHardwareAcceleration, Headless: plan.Headless,
		Rooted: plan.Rooted, Debuggable: plan.Debuggable, GuestMemoryBytes: plan.GuestMemoryBytes, BootTimeout: plan.BootTimeout,
	}
}

func instanceMatchesPlan(instance Instance, plan VirtualDevicePlan) bool {
	return instancesEqual(instance, instanceFromPlan(plan))
}

func instancesEqual(left, right Instance) bool {
	return left.RuntimeID != "" && left.RuntimeID == right.RuntimeID && left.Allocation == right.Allocation && left.Fingerprint.Compatible(right.Fingerprint) &&
		filepath.Clean(left.StateDirectory) == filepath.Clean(right.StateDirectory) && filepath.Clean(left.SystemImageDirectory) == filepath.Clean(right.SystemImageDirectory) &&
		resourcesEqual(left.Resources, right.Resources) && left.BaselineState == right.BaselineState &&
		left.RequireHardwareAcceleration == right.RequireHardwareAcceleration && left.Headless == right.Headless &&
		left.Rooted == right.Rooted && left.Debuggable == right.Debuggable && left.GuestMemoryBytes == right.GuestMemoryBytes && left.BootTimeout == right.BootTimeout
}

func resourcesEqual(left, right admission.Resources) bool {
	if left.CPUMilli != right.CPUMilli || left.MemoryBytes != right.MemoryBytes || left.SwapBytes != right.SwapBytes || left.StorageBytes != right.StorageBytes || left.CaptureBytes != right.CaptureBytes || left.Inodes != right.Inodes || left.PIDs != right.PIDs || len(left.Devices) != len(right.Devices) {
		return false
	}
	for name, value := range left.Devices {
		if right.Devices[name] != value {
			return false
		}
	}
	return true
}

// BackendQuarantiner is optional because an attached external emulator cannot
// be stopped or isolated without violating its ownership contract. The stop
// mode must be preserved so callers can select graceful, immediate, or forced
// containment while still receiving the same complete containment proof.
type BackendQuarantiner interface {
	Quarantine(context.Context, Instance, ports.StopMode) (BackendQuarantineState, error)
}

type BackendQuarantineState struct {
	RuntimeID          string
	ExecutionStopped   bool
	NetworkUnreachable bool
	StatePreserved     bool
	ObservedAt         time.Time
}

// BackendStoppedAdopter live-verifies that a previously sealed containment
// boundary still describes the exact preserved runtime, then registers that
// stopped ownership in a fresh backend process. This is deliberately distinct
// from Quarantine: reconciliation must never restart a tainted guest merely to
// recover backend-local process bookkeeping.
type BackendStoppedAdopter interface {
	AdoptStopped(context.Context, Instance, BackendQuarantineState) (BackendQuarantineState, error)
}

// BackendStoppedInspector live-proves that an unexpectedly stopped runtime is
// still the exact plan-owned physical resource and registers its stopped
// ownership in a fresh backend process. Unlike BackendStoppedAdopter, this
// carries no prior quarantine authority: callers may use it only for
// cleanup-only work or an already-durable interrupted-run recovery.
type BackendStoppedInspector interface {
	InspectStopped(context.Context, Instance) (BackendQuarantineState, error)
}

func validateStoppedAdoption(instance Instance, proof BackendQuarantineState) error {
	if strings.TrimSpace(instance.RuntimeID) == "" || proof.RuntimeID != instance.RuntimeID || !proof.ExecutionStopped || !proof.NetworkUnreachable || !proof.StatePreserved || proof.ObservedAt.IsZero() {
		return fmt.Errorf("stopped-runtime adoption requires exact complete containment evidence")
	}
	if err := validatePlannedEndpoint(instance.Allocation); err != nil {
		return err
	}
	return nil
}

func validatePlannedEndpoint(allocation Allocation) error {
	if err := allocation.Validate(); err != nil {
		return err
	}
	if strings.HasPrefix(allocation.Serial, "emulator-") {
		_, err := allocation.EmulatorConsolePort()
		return err
	}
	return nil
}

func confirmedADBUnreachable(result command.Result, err error) bool {
	if err == nil || result.ExitCode <= 0 {
		return false
	}
	output := strings.ToLower(string(result.Stderr) + "\n" + string(result.Stdout) + "\n" + err.Error())
	for _, marker := range []string{"not found", "offline", "no devices/emulators", "cannot connect", "failed to connect", "connection refused", "connection closed"} {
		if strings.Contains(output, marker) {
			return true
		}
	}
	return false
}
