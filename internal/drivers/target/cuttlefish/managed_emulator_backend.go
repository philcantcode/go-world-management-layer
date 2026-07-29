package cuttlefish

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/androidcontract"
	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	defaultManagedEmulatorPollInterval = 500 * time.Millisecond
	defaultManagedEmulatorStopTimeout  = 20 * time.Second
	managedEmulatorOwnershipTimeout    = 10 * time.Second
	defaultManagedEmulatorLogBytes     = int64(16 << 20)
	managedEmulatorDiagnosticLogBytes  = int64(4 << 10)
	managedEmulatorMinimumPartitionMiB = androidcontract.MinimumDataPartitionMiB
	managedEmulatorMaximumPartitionMiB = androidcontract.MaximumDataPartitionMiB
	managedImageBindingFilename        = "world-system-image.json"
	managedAVDDirectory                = "avds"
	managedEmulatorStdoutFilename      = "emulator.stdout.log"
	managedEmulatorStderrFilename      = "emulator.stderr.log"
	managedEmulatorPIDFilename         = "emulator.pid"
	managedEmulatorLaunchFilename      = "world-emulator-launch.json"
	managedEmulatorOwnershipFilename   = "world-emulator-process.json"
	managedWindowsResourceAuthority    = "windows_handle+job_object"
	maximumManagedEmulatorPIDBytes     = int64(64)
)

var (
	errManagedHostProcessNotFound         = errors.New("managed emulator host process not found")
	errManagedHostProcessIdentityMismatch = errors.New("managed emulator host process identity mismatch")
	errManagedLaunchUnresolved            = errors.New("managed emulator launch handoff is unresolved")
	errManagedRuntimeFingerprintMismatch  = errors.New("managed emulator runtime fingerprint mismatch")
	errManagedGuestDataPartitionMismatch  = errors.New("managed emulator guest data partition mismatch")
	errManagedDataOverlayNotReady         = errors.New("managed emulator data overlay not ready")
)

// ManagedSystemImage binds one authority-selected TargetTemplate image digest
// to one Android SDK package. Map keys in ManagedEmulatorBackendConfig must be
// exactly TargetTemplate.ImageDigest.String().
type ManagedSystemImage struct {
	Package   string `json:"package"`
	Directory string `json:"directory,omitempty"`
}

type ManagedEmulatorBackendConfig struct {
	Runner             command.Runner
	Starter            command.Starter
	EmulatorBinary     string
	ADBBinary          string
	ADBServerEndpoint  string
	SDKManagerBinary   string
	AVDManagerBinary   string
	SDKRoot            string
	StateRoot          string
	SystemImages       map[string]ManagedSystemImage
	PollInterval       time.Duration
	ShutdownTimeout    time.Duration
	MaximumLogBytes    int64
	Now                func() time.Time
	processAuthority   managedHostProcessAuthority
	commitLaunchIntent func(Instance, string, managedDataStorageBinding) error
}

type ManagedEmulatorBackend struct {
	runner             command.Runner
	starter            command.Starter
	emulatorBinary     string
	adbBinary          string
	adbServer          adbServerEndpoint
	sdkManagerBinary   string
	avdManagerBinary   string
	sdkRoot            string
	stateRoot          string
	avdHome            string
	systemImages       map[string]ManagedSystemImage
	pollInterval       time.Duration
	shutdownTimeout    time.Duration
	maximumLogBytes    int64
	now                func() time.Time
	processAuthority   managedHostProcessAuthority
	commitLaunchIntent func(Instance, string, managedDataStorageBinding) error

	mu        sync.Mutex
	processes map[string]*managedProcess
}

type managedProcess struct {
	launcher  command.Process
	done      chan struct{}
	logsDone  chan struct{}
	adoptMu   sync.Mutex
	mu        sync.Mutex
	waitErr   error
	owned     managedHostProcess
	authority managedProcessOwnership
	stopped   bool
}

type managedHostProcessAuthority interface {
	ResolveExecutable(emulatorBinary string) (string, error)
	Preflight(emulatorBinary string) error
	ResourcesEnforced() bool
	PreflightResources(context.Context, admission.Resources) error
	StartContained(context.Context, command.Starter, command.Invocation, Instance) (command.Process, error)
	ResourceIdentity(Instance) string
	Kind() string
	Open(pid int, emulatorBinary, pidFile string, storage managedDataStorageBinding, instance Instance) (managedHostProcess, error)
}

type managedHostProcess interface {
	PID() int
	ExecutablePath() string
	StartToken() string
	Running() (bool, error)
	Kill() error
	Close() error
}

// managedResourceLifetimeAnchor installs the minimal authority needed for a
// named resource-control object to remain reopenable after daemon and launcher
// handles close. It is invoked once, before the first ownership commit; a
// restart reopens and verifies the persisted authority without duplicating it.
type managedResourceLifetimeAnchor interface {
	AnchorResourceAuthority() error
}

type managedProcessOwnership struct {
	RuntimeID         string                      `json:"runtime_id"`
	AVDName           string                      `json:"avd_name"`
	Serial            string                      `json:"serial"`
	ConsolePort       int                         `json:"console_port"`
	PID               int                         `json:"pid"`
	PIDFile           string                      `json:"pid_file"`
	ExecutablePath    string                      `json:"executable_path"`
	StartToken        string                      `json:"start_token"`
	ResourceAuthority string                      `json:"resource_authority"`
	ResourceIdentity  string                      `json:"resource_identity"`
	CPUMilli          int64                       `json:"cpu_milli"`
	MemoryBytes       int64                       `json:"memory_bytes"`
	StorageBytes      int64                       `json:"storage_bytes"`
	GuestMemoryBytes  int64                       `json:"guest_memory_bytes"`
	ResourceAnchored  bool                        `json:"resource_anchored"`
	Storage           managedDataStorageAuthority `json:"storage"`
}

type managedLaunchIntent struct {
	Instance       Instance                    `json:"instance"`
	EmulatorBinary string                      `json:"emulator_binary"`
	PIDFile        string                      `json:"pid_file"`
	Storage        managedDataStorageAuthority `json:"storage"`
}

type managedImageBinding struct {
	Digest    string `json:"digest"`
	Package   string `json:"package"`
	Directory string `json:"directory"`
}

func NewManagedEmulatorBackend(config ManagedEmulatorBackendConfig) (*ManagedEmulatorBackend, error) {
	if strings.TrimSpace(config.SDKRoot) == "" || strings.TrimSpace(config.StateRoot) == "" {
		return nil, fmt.Errorf("Android SDK root and managed emulator state root are required")
	}
	if len(config.SystemImages) == 0 {
		return nil, fmt.Errorf("at least one exact system-image digest mapping is required")
	}
	if config.Runner == nil {
		config.Runner = command.OS{}
	}
	if config.Starter == nil {
		config.Starter = command.OS{}
	}
	if config.EmulatorBinary == "" {
		config.EmulatorBinary = "emulator"
	}
	if config.ADBBinary == "" {
		config.ADBBinary = "adb"
	}
	if config.SDKManagerBinary == "" {
		config.SDKManagerBinary = "sdkmanager"
	}
	if config.AVDManagerBinary == "" {
		config.AVDManagerBinary = "avdmanager"
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaultManagedEmulatorPollInterval
	}
	config.ShutdownTimeout = effectiveManagedEmulatorStopTimeout(config.ShutdownTimeout)
	if config.MaximumLogBytes <= 0 {
		config.MaximumLogBytes = defaultManagedEmulatorLogBytes
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.processAuthority == nil {
		config.processAuthority = newManagedHostProcessAuthority()
	}
	emulatorIdentity, err := config.processAuthority.ResolveExecutable(config.EmulatorBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve managed emulator executable identity: %w", err)
	}
	config.EmulatorBinary = emulatorIdentity
	if config.commitLaunchIntent == nil {
		config.commitLaunchIntent = commitManagedLaunchIntent
	}
	if err := config.processAuthority.Preflight(config.EmulatorBinary); err != nil {
		return nil, fmt.Errorf("managed emulator host-process authority preflight: %w", err)
	}
	sdkRoot, err := filepath.Abs(config.SDKRoot)
	if err != nil {
		return nil, err
	}
	stateRoot, err := filepath.Abs(config.StateRoot)
	if err != nil {
		return nil, err
	}
	adbServer, err := parseADBServerEndpoint(config.ADBServerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("managed emulator ADB server endpoint: %w", err)
	}
	images := make(map[string]ManagedSystemImage, len(config.SystemImages))
	for digestText, image := range config.SystemImages {
		digest, parseErr := domain.ParseDigest(digestText)
		if parseErr != nil || digest.String() != digestText {
			return nil, fmt.Errorf("managed system-image key %q is not a canonical target image digest", digestText)
		}
		if err := ValidateManagedSystemImagePackage(image.Package); err != nil {
			return nil, fmt.Errorf("managed system image %s: %w", digestText, err)
		}
		if image.Directory == "" {
			image.Directory = filepath.Join(append([]string{sdkRoot}, strings.Split(image.Package, ";")...)...)
		} else if !filepath.IsAbs(image.Directory) {
			image.Directory = filepath.Join(sdkRoot, image.Directory)
		}
		image.Directory = filepath.Clean(image.Directory)
		if err := requirePathWithin(sdkRoot, image.Directory, false); err != nil {
			return nil, fmt.Errorf("managed system image %s directory: %w", digestText, err)
		}
		images[digestText] = image
	}
	avdHome := filepath.Join(stateRoot, managedAVDDirectory)
	if err := os.MkdirAll(avdHome, 0o700); err != nil {
		return nil, fmt.Errorf("create managed AVD root: %w", err)
	}
	return &ManagedEmulatorBackend{
		runner: config.Runner, starter: config.Starter, emulatorBinary: config.EmulatorBinary,
		adbBinary: config.ADBBinary, adbServer: adbServer, sdkManagerBinary: config.SDKManagerBinary, avdManagerBinary: config.AVDManagerBinary,
		sdkRoot: sdkRoot, stateRoot: stateRoot, avdHome: avdHome, systemImages: images,
		pollInterval: config.PollInterval, shutdownTimeout: config.ShutdownTimeout,
		maximumLogBytes: config.MaximumLogBytes, now: config.Now, processAuthority: config.processAuthority,
		commitLaunchIntent: config.commitLaunchIntent,
		processes:          make(map[string]*managedProcess),
	}, nil
}

func effectiveManagedEmulatorStopTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return defaultManagedEmulatorStopTimeout
	}
	return configured
}

func (b *ManagedEmulatorBackend) newManagedEmulatorStopContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), effectiveManagedEmulatorStopTimeout(b.shutdownTimeout))
}

// EmulatorExecutableIdentity is the canonical immutable host executable path
// used by launch intents, recovery, and the physical device fingerprint.
func (b *ManagedEmulatorBackend) EmulatorExecutableIdentity() string {
	return b.emulatorBinary
}

// ValidateResourceEnforcement proves that the selected host can instantiate
// the exact process-tree limits before target creation mutates AVD state.
func (b *ManagedEmulatorBackend) ValidateResourceEnforcement(ctx context.Context, resources admission.Resources) error {
	return b.processAuthority.PreflightResources(ctx, resources)
}

func (b *ManagedEmulatorBackend) Probe(ctx context.Context, template ports.TargetTemplate) (BackendCapabilities, error) {
	if err := template.Validate(); err != nil {
		return BackendCapabilities{}, err
	}
	if template.Kind != domain.TargetAndroidVirtualDevice {
		return BackendCapabilities{}, fmt.Errorf("managed SDK emulator requires an Android virtual-device template")
	}
	image, err := b.requireSystemImage(template.ImageDigest, false)
	if err != nil {
		return BackendCapabilities{}, err
	}
	emulatorVersion, err := b.runTool(ctx, b.emulatorBinary, []string{"-version"}, nil)
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe Android emulator version: %w", err)
	}
	adbVersion, err := b.runTool(ctx, b.adbBinary, b.adbServer.globalArgs("version"), nil)
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe ADB version: %w", err)
	}
	sdkManagerVersion, err := b.runTool(ctx, b.sdkManagerBinary, []string{"--version"}, b.sdkEnvironment())
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe sdkmanager version: %w", err)
	}
	sdkManagerVersion, err = exactSDKManagerVersion(sdkManagerVersion)
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe sdkmanager version: %w", err)
	}
	formatter, err := b.observeManagedDataFormatter(ctx)
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe exact Android data formatter: %w", err)
	}
	acceleration, err := b.runTool(ctx, b.emulatorBinary, []string{"-accel-check"}, b.sdkEnvironment())
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("hardware acceleration is unavailable: %w", err)
	}
	accelerationEvidence := substantiveAccelerationEvidence(acceleration)
	if accelerationEvidence == "" {
		return BackendCapabilities{}, fmt.Errorf("hardware acceleration probe returned no substantive evidence")
	}
	kvmObserved := runtime.GOOS == "linux" && strings.Contains(strings.ToLower(accelerationEvidence), "kvm")
	properties, err := readSystemBuildProperties(image.Directory)
	if err != nil {
		return BackendCapabilities{}, fmt.Errorf("observe system-image build properties: %w", err)
	}
	debuggable := properties["ro.debuggable"] == "1"
	if !debuggable {
		return BackendCapabilities{}, fmt.Errorf("system image %s is not debuggable and cannot enforce rooted research execution", template.ImageDigest.String())
	}
	version := firstNonBlankLine(emulatorVersion)
	if version == "" {
		return BackendCapabilities{}, fmt.Errorf("Android emulator returned no observed version")
	}
	runtimeVersion := strings.TrimSpace(properties["ro.system.build.fingerprint"])
	if runtimeVersion == "" {
		return BackendCapabilities{}, fmt.Errorf("system image does not expose ro.system.build.fingerprint")
	}
	evidence := map[string]string{
		"os": "android", "managed": "true", "emulator_version": version,
		"host_os":     runtime.GOOS,
		"adb_version": firstNonBlankLine(adbVersion), "sdkmanager_version": sdkManagerVersion,
		"hardware_acceleration": accelerationEvidence, "system_image_package": image.Package,
		"system_image_directory": image.Directory, "system_image_digest": template.ImageDigest.String(),
		"runtime_fingerprint": runtimeVersion,
		"mke2fs_binary":       formatter.Binary, "mke2fs_binary_digest": formatter.BinaryDigest,
		"mke2fs_config": formatter.Config, "mke2fs_config_digest": formatter.ConfigDigest, "mke2fs_version": formatter.Version,
		"headless": "true", "rooted": "true", "debuggable": "true",
		"host_process_authority":  b.processAuthority.Kind(),
		"launch_handoff_recovery": "fail_closed_unresolved",
		"host_cpu_containment":    b.processAuthority.ResourceIdentity(Instance{}),
		"host_memory_containment": b.processAuthority.ResourceIdentity(Instance{}),
		"writable_state_scope":    "guest-data-partition",
	}
	return BackendCapabilities{
		BackendKind: "android-sdk-emulator", BackendVersion: version, RuntimeVersion: runtimeVersion,
		KVM: kvmObserved, KVMKnown: kvmObserved, HardwareAcceleration: true, HardwareAccelerationKnown: true,
		Managed: true, Headless: true, HeadlessKnown: true, Rooted: true, RootedKnown: true,
		Debuggable: true, DebuggableKnown: true,
		// Host CPU and memory are enforced independently of the guest topology
		// flags. WritableStateEnforced is scoped to the exact guest /data block
		// device, not to host-owned AVD metadata and diagnostic logs.
		CPUEnforced: b.processAuthority.ResourcesEnforced(), MemoryEnforced: b.processAuthority.ResourcesEnforced(), WritableStateEnforced: true,
		ResetModes: []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline}, Evidence: evidence,
	}, nil
}

func (b *ManagedEmulatorBackend) Create(ctx context.Context, plan VirtualDevicePlan) (Instance, error) {
	if err := plan.Validate(b.stateRoot, filepath.Dir(plan.SystemImageDirectory)); err != nil {
		return Instance{}, err
	}
	if _, err := plan.Allocation.EmulatorConsolePort(); err != nil {
		return Instance{}, err
	}
	if err := ValidateManagedEmulatorResources(plan.Resources, plan.GuestMemoryBytes); err != nil {
		return Instance{}, err
	}
	image, err := b.installAndRequireSystemImage(ctx, plan.Fingerprint.SystemImageDigest)
	if err != nil {
		return Instance{}, err
	}
	if err := os.MkdirAll(plan.StateDirectory, 0o700); err != nil {
		return Instance{}, err
	}
	if err := os.MkdirAll(plan.SystemImageDirectory, 0o700); err != nil {
		return Instance{}, err
	}
	if err := atomicfile.WriteJSON(filepath.Join(plan.SystemImageDirectory, managedImageBindingFilename), managedImageBinding{
		Digest: plan.Fingerprint.SystemImageDigest.String(), Package: image.Package, Directory: image.Directory,
	}, 0o600); err != nil {
		return Instance{}, err
	}
	instance := instanceFromPlan(plan)
	if err := b.createExactManagedAVD(ctx, instance, image.Package); err != nil {
		return Instance{}, err
	}
	return instance, nil
}

func (b *ManagedEmulatorBackend) createExactManagedAVD(ctx context.Context, instance Instance, imagePackage string) error {
	name := instance.Allocation.InstanceName
	before, err := b.inspectExactManagedAVD(ctx, name)
	if err != nil {
		return err
	}
	if before.any() {
		return fmt.Errorf(
			"managed AVD %q has pre-existing list or filesystem state (listed=%t directory=%t ini=%t) and was not re-adopted",
			name, before.listed, before.directoryPresent, before.iniPresent,
		)
	}
	avdPath := b.avdPath(name)
	args := []string{"create", "avd", "--name", name, "--package", imagePackage, "--path", avdPath}
	if _, err := b.runToolWithInput(ctx, b.avdManagerBinary, args, b.sdkEnvironment(), strings.NewReader("no\n")); err != nil {
		return fmt.Errorf("create exact managed AVD: %w", err)
	}
	created, err := b.inspectExactManagedAVD(ctx, name)
	if err != nil {
		return err
	}
	if err := created.requireComplete(); err != nil {
		return fmt.Errorf("avdmanager did not create one exact bound AVD: %w", err)
	}
	if err := configureManagedAVDDataPartition(avdPath, b.managedDataImagePath(instance), instance.Resources.StorageBytes); err != nil {
		return errors.Join(err, b.deleteExactManagedAVD(ctx, name))
	}
	if err := b.createExactManagedDataImage(ctx, instance); err != nil {
		return errors.Join(err, b.deleteExactManagedAVD(ctx, name))
	}
	return nil
}

func (b *ManagedEmulatorBackend) Start(ctx context.Context, instance Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	port, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		return err
	}
	if err := requirePathWithin(b.stateRoot, instance.StateDirectory, true); err != nil {
		return err
	}
	if err := ValidateManagedEmulatorResources(instance.Resources, instance.GuestMemoryBytes); err != nil {
		return err
	}
	if err := requireManagedAVDDataPartitionConfig(b.avdPath(instance.Allocation.InstanceName), b.managedDataImagePath(instance), instance.Resources.StorageBytes); err != nil {
		return fmt.Errorf("verify exact managed AVD data-partition configuration before launch: %w", err)
	}
	b.mu.Lock()
	if prior := b.processes[instance.RuntimeID]; prior != nil {
		running, stateErr := managedProcessState(prior)
		b.mu.Unlock()
		if stateErr != nil {
			return fmt.Errorf("managed emulator %q has ambiguous existing process ownership: %w", instance.RuntimeID, stateErr)
		}
		return fmt.Errorf("managed emulator %q already has exact process ownership (running=%t)", instance.RuntimeID, running)
	}
	b.mu.Unlock()
	artifacts, err := inspectManagedLaunchArtifacts(instance)
	if err != nil {
		return err
	}
	if artifacts.any() {
		return fmt.Errorf("managed emulator launch artifacts already exist before launch")
	}
	storage, err := b.requireManagedDataStorage(ctx, instance, managedDataOverlayAbsent)
	if err != nil {
		return fmt.Errorf("verify exact immutable managed data backing before launch: %w", err)
	}
	pidFile := managedEmulatorPIDPath(instance)
	launchPath := filepath.Join(instance.StateDirectory, managedEmulatorLaunchFilename)
	if err := b.processAuthority.Preflight(b.emulatorBinary); err != nil {
		b.rememberManagedEmulatorNeverStarted(instance)
		return fmt.Errorf("managed emulator host-process authority preflight: %w", err)
	}
	if err := b.processAuthority.PreflightResources(ctx, instance.Resources); err != nil {
		b.rememberManagedEmulatorNeverStarted(instance)
		return fmt.Errorf("managed emulator host-resource containment preflight: %w", err)
	}
	if err := b.commitLaunchIntent(instance, b.emulatorBinary, storage); err != nil {
		if !errors.Is(err, os.ErrExist) {
			b.rememberManagedEmulatorNeverStarted(instance)
		}
		return err
	}
	dataPath := b.managedDataImagePath(instance)
	args := managedEmulatorLaunchArguments(instance, port, dataPath, pidFile)
	stdoutLog, stderrLog, err := openManagedEmulatorLogs(instance)
	if err != nil {
		_ = os.Remove(launchPath)
		b.rememberManagedEmulatorNeverStarted(instance)
		return err
	}
	process, err := b.processAuthority.StartContained(context.Background(), b.starter, command.Invocation{
		Program: b.emulatorBinary, Args: args, Directory: instance.StateDirectory, Environment: b.sdkEnvironment(),
	}, instance)
	if err != nil {
		_ = stdoutLog.Close()
		_ = stderrLog.Close()
		_ = os.Remove(launchPath)
		b.rememberManagedEmulatorNeverStarted(instance)
		return fmt.Errorf("start managed Android emulator: %w", err)
	}
	record := b.captureProcessLogs(process, stdoutLog, stderrLog)
	b.mu.Lock()
	b.processes[instance.RuntimeID] = record
	b.mu.Unlock()
	ownershipContext, ownershipCancel := context.WithTimeout(ctx, managedEmulatorOwnershipTimeout)
	defer ownershipCancel()
	if err := b.waitForManagedHostProcessOwnership(ownershipContext, instance, record); err != nil {
		diagnosticErr := b.processExitBeforeReadiness(ownershipContext, instance)
		cleanupErr := b.forceStopAndProve(instance, record)
		return errors.Join(fmt.Errorf("prove managed emulator launcher-to-QEMU ownership handoff: %w", err), diagnosticErr, cleanupErr)
	}
	return nil
}

func (b *ManagedEmulatorBackend) managedDataImagePath(instance Instance) string {
	return filepath.Join(instance.StateDirectory, managedEmulatorDataFilename)
}

func managedEmulatorLaunchArguments(instance Instance, port int, dataImage, pidFile string) []string {
	return []string{
		"-avd", instance.Allocation.InstanceName, "-port", strconv.Itoa(port), "-no-window", "-no-audio", "-no-boot-anim",
		"-no-snapshot", "-no-snapshot-load", "-no-snapshot-save", "-no-cache", "-accel", "on",
		"-cores", strconv.FormatInt(instance.Resources.CPUMilli/1000, 10),
		"-memory", strconv.FormatInt(instance.GuestMemoryBytes/androidcontract.Mebibyte, 10),
		"-data", dataImage, "-gpu", "swiftshader_indirect",
		"-qemu", "-pidfile", pidFile,
	}
}

type managedLaunchArtifacts struct {
	pidFile   bool
	intent    bool
	ownership bool
}

func (a managedLaunchArtifacts) any() bool {
	return a.pidFile || a.intent || a.ownership
}

func inspectManagedLaunchArtifacts(instance Instance) (managedLaunchArtifacts, error) {
	var result managedLaunchArtifacts
	for path, destination := range map[string]*bool{
		managedEmulatorPIDPath(instance):                                         &result.pidFile,
		filepath.Join(instance.StateDirectory, managedEmulatorLaunchFilename):    &result.intent,
		filepath.Join(instance.StateDirectory, managedEmulatorOwnershipFilename): &result.ownership,
	} {
		found, err := strictManifestExists(path)
		if err != nil {
			return managedLaunchArtifacts{}, fmt.Errorf("inspect managed emulator launch artifact %q: %w", filepath.Base(path), err)
		}
		*destination = found
	}
	return result, nil
}

func (b *ManagedEmulatorBackend) ResumeUnstarted(ctx context.Context, instance Instance) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := ValidateManagedEmulatorResources(instance.Resources, instance.GuestMemoryBytes); err != nil {
		return false, err
	}
	if instance.RuntimeID != instance.Allocation.InstanceName {
		return false, fmt.Errorf("configured managed AVD identity differs from the exact runtime")
	}
	artifacts, err := inspectManagedLaunchArtifacts(instance)
	if err != nil {
		return false, err
	}
	if artifacts.intent {
		return false, nil
	}
	if artifacts.pidFile || artifacts.ownership {
		return false, fmt.Errorf("managed AVD has PID or ownership state without its immutable launch intent")
	}
	b.mu.Lock()
	record := b.processes[instance.RuntimeID]
	b.mu.Unlock()
	if record != nil {
		return false, fmt.Errorf("configured managed AVD has in-memory process state without durable launch authority")
	}
	avd, err := b.inspectExactManagedAVD(ctx, instance.RuntimeID)
	if err != nil {
		return false, err
	}
	if !avd.any() {
		return false, fmt.Errorf("configured managed AVD disappeared before unstarted recovery")
	}
	if avd.iniPresent {
		if err := avd.requireINIPathBinding(); err != nil {
			return false, fmt.Errorf("configured managed AVD metadata is redirected or ambiguous: %w", err)
		}
	}
	if avd.listed && !avd.iniPresent {
		return false, fmt.Errorf("listed managed AVD lacks the exact ini authority required for safe deletion")
	}
	avdPath := b.avdPath(instance.RuntimeID)
	if err := b.validateManagedImageBinding(instance); err != nil {
		return false, err
	}
	image, err := b.requireSystemImage(instance.Fingerprint.SystemImageDigest, true)
	if err != nil {
		return false, fmt.Errorf("re-prove exact managed system image before unstarted recovery: %w", err)
	}
	if err := b.requireManagedEndpointAbsent(ctx, instance); err != nil {
		return false, fmt.Errorf("prove configured managed AVD has no live endpoint before recreation: %w", err)
	}
	if err := b.requireManagedDataOverlayAbsentForRecovery(instance); err != nil {
		return false, err
	}
	if err := b.deleteExactManagedAVD(ctx, instance.RuntimeID); err != nil {
		return false, fmt.Errorf("retire unlaunched managed AVD before exact recreation: %w", err)
	}
	if err := b.removeExactManagedDataStorage(instance); err != nil {
		return false, fmt.Errorf("retire incomplete unlaunched managed data artifacts: %w", err)
	}
	if err := b.createExactManagedAVD(ctx, instance, image.Package); err != nil {
		return false, fmt.Errorf("recreate exact unlaunched managed AVD: %w", err)
	}
	if err := requireManagedAVDDataPartitionConfig(avdPath, b.managedDataImagePath(instance), instance.Resources.StorageBytes); err != nil {
		return false, err
	}
	if err := b.Start(ctx, instance); err != nil {
		return false, err
	}
	return true, nil
}

func (b *ManagedEmulatorBackend) WaitReady(ctx context.Context, instance Instance) (ReadinessState, error) {
	if instance.BootTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, instance.BootTimeout)
		defer cancel()
	}
	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	var lastErr error
	var lastState ReadinessState
	for {
		if err := ctx.Err(); err != nil {
			return lastState, errors.Join(err, lastErr)
		}
		record, recordErr := b.requireManagedProcessRecord(ctx, instance)
		if recordErr != nil {
			return ReadinessState{}, recordErr
		}
		if _, adoptErr := b.adoptManagedHostProcess(ctx, instance, record); adoptErr != nil {
			return ReadinessState{}, adoptErr
		}
		if exitErr := b.processExitBeforeReadiness(ctx, instance); exitErr != nil {
			return ReadinessState{}, exitErr
		}
		state, err := b.Inspect(ctx, instance)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return lastState, errors.Join(ctxErr, lastErr)
		}
		if err == nil {
			lastState = state
		}
		if errors.Is(err, errManagedRuntimeFingerprintMismatch) {
			return state, err
		}
		var rootErr error
		var ownershipErr error
		if err == nil && state.ADBReady && state.Identity.Debuggable && !state.Identity.Rooted {
			_, rootErr = runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "root")
			if rootErr != nil {
				rootErr = fmt.Errorf("restart exact managed emulator ADB daemon as root: %w", rootErr)
			}
			state, err = b.Inspect(ctx, instance)
			if err == nil {
				lastState = state
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			diagnostic := lastErr
			if diagnostic == nil && !lastState.ObservedAt.IsZero() {
				diagnostic = incompleteAndroidReadinessError(lastState, instance.Allocation.InstanceName)
			}
			return lastState, errors.Join(ctxErr, diagnostic, rootErr)
		}
		if err == nil && state.Ready() && state.Identity.Rooted && state.Identity.Debuggable && state.Identity.AVDName == instance.Allocation.InstanceName {
			ownershipErr = b.requireExactGuestDataPartition(ctx, instance)
			if errors.Is(ownershipErr, errManagedGuestDataPartitionMismatch) {
				return state, ownershipErr
			}
			if ownershipErr == nil {
				ownershipErr = b.requireReadyManagedHostProcess(ctx, instance, record, state)
			}
			if ownershipErr == nil {
				return state, nil
			}
		}
		if err == nil {
			if ownershipErr == nil {
				ownershipErr = incompleteAndroidReadinessError(state, instance.Allocation.InstanceName)
			}
			lastErr = errors.Join(rootErr, ownershipErr)
			lastState = state
		} else {
			lastErr = errors.Join(rootErr, err)
		}
		if exitErr := b.processExitBeforeReadiness(ctx, instance); exitErr != nil {
			return ReadinessState{}, errors.Join(exitErr, lastErr)
		}
		select {
		case <-ctx.Done():
			return lastState, errors.Join(ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func (b *ManagedEmulatorBackend) Inspect(ctx context.Context, instance Instance) (ReadinessState, error) {
	if _, err := instance.Allocation.EmulatorConsolePort(); err != nil {
		return ReadinessState{}, err
	}
	record, err := b.requireManagedProcessRecord(ctx, instance)
	if err != nil {
		return ReadinessState{}, fmt.Errorf("load exact managed emulator host-process authority: %w", err)
	}
	state, properties, err := observeExactAndroid(ctx, exactAndroidObservationConfig{
		Runner: b.runner, ADBBinary: b.adbBinary, ADBServer: b.adbServer, Serial: instance.Allocation.Serial, Now: b.now,
		Properties: []string{"ro.system.build.fingerprint"},
		ProcessProbe: func(context.Context) (bool, error) {
			return managedProcessState(record)
		},
	})
	if err != nil {
		return state, err
	}
	actualFingerprint := strings.TrimSpace(properties["ro.system.build.fingerprint"])
	if actualFingerprint == "" {
		return state, fmt.Errorf("exact managed emulator returned no ro.system.build.fingerprint")
	}
	if actualFingerprint != instance.Fingerprint.RuntimeVersion {
		return state, fmt.Errorf("%w: observed %q, want exact %q", errManagedRuntimeFingerprintMismatch, actualFingerprint, instance.Fingerprint.RuntimeVersion)
	}
	return state, nil
}

func (b *ManagedEmulatorBackend) Stop(ctx context.Context, instance Instance, mode ports.StopMode) error {
	if !mode.IsValid() {
		return fmt.Errorf("invalid stop mode %q", mode)
	}
	if _, err := instance.Allocation.EmulatorConsolePort(); err != nil {
		return err
	}
	if mode == ports.StopForce {
		return b.forceStopManagedEmulator(ctx, instance)
	}
	return b.stopManagedEmulatorViaGuest(ctx, instance, mode)
}

func (b *ManagedEmulatorBackend) forceStopManagedEmulator(requestContext context.Context, instance Instance) error {
	stopContext, cancel := b.newManagedEmulatorStopContext()
	defer cancel()
	record, err := b.requireManagedProcessRecord(stopContext, instance)
	if err != nil {
		return errors.Join(requestContext.Err(), fmt.Errorf("load exact managed emulator process ownership for forced containment: %w", err))
	}
	if err := b.forceStopAndProveWithin(stopContext, instance, record); err != nil {
		return errors.Join(requestContext.Err(), fmt.Errorf("force-stop exact managed emulator: %w", err))
	}
	return requestContext.Err()
}

func (b *ManagedEmulatorBackend) stopManagedEmulatorViaGuest(ctx context.Context, instance Instance, mode ports.StopMode) error {
	record, err := b.requireManagedProcessRecord(ctx, instance)
	if err != nil {
		return fmt.Errorf("load exact managed emulator process ownership: %w", err)
	}
	guestStopErr := b.requestGracefulManagedStop(ctx, instance)
	if guestStopErr == nil && mode == ports.StopGraceful {
		stopContext, cancel := context.WithTimeout(ctx, b.shutdownTimeout)
		defer cancel()
		if err := b.waitStopped(stopContext, instance, false); err == nil {
			return nil
		} else {
			guestStopErr = err
		}
	}
	if forceErr := b.forceStopAndProve(instance, record); forceErr != nil {
		return errors.Join(guestStopErr, forceErr)
	}
	return nil
}

// forceStopAndProve separates a termination request from its authoritative
// result. Host APIs can report a request error while an already-exiting process
// completes shutdown; only exact process, ADB, and console absence proves that
// containment actually succeeded.
func (b *ManagedEmulatorBackend) forceStopAndProve(instance Instance, record *managedProcess) error {
	// Forced containment must outlive a canceled operation request: storage
	// re-proof, exact process adoption, termination, launcher drain, and absence
	// proof are one bounded safety operation.
	stopContext, cancel := b.newManagedEmulatorStopContext()
	defer cancel()
	return b.forceStopAndProveWithin(stopContext, instance, record)
}

func (b *ManagedEmulatorBackend) forceStopAndProveWithin(ctx context.Context, instance Instance, record *managedProcess) error {
	requestErr := b.forceStopManagedProcessWithin(ctx, instance, record)
	if proofErr := b.waitStopped(ctx, instance, true); proofErr != nil {
		return errors.Join(requestErr, proofErr)
	}
	return nil
}

func (b *ManagedEmulatorBackend) Destroy(ctx context.Context, instance Instance) error {
	if err := b.Stop(ctx, instance, ports.StopForce); err != nil {
		return err
	}
	if err := b.deleteExactManagedAVD(ctx, instance.Allocation.InstanceName); err != nil {
		return err
	}
	if err := requirePathWithin(b.stateRoot, instance.StateDirectory, true); err != nil {
		return err
	}
	b.mu.Lock()
	record := b.processes[instance.RuntimeID]
	b.mu.Unlock()
	if record != nil {
		if drainErr := waitManagedProcessDrain(ctx, record); drainErr != nil {
			return fmt.Errorf("drain exact managed emulator launcher logs before state deletion: %w", drainErr)
		}
		if closeErr := closeManagedHostProcess(record); closeErr != nil {
			return fmt.Errorf("close exact managed emulator process ownership: %w", closeErr)
		}
	}
	if err := b.removeExactManagedDataStorage(instance); err != nil {
		return fmt.Errorf("remove exact generation-scoped managed data storage: %w", err)
	}
	b.mu.Lock()
	if b.processes[instance.RuntimeID] == record {
		delete(b.processes, instance.RuntimeID)
	}
	b.mu.Unlock()
	if err := os.RemoveAll(instance.StateDirectory); err != nil {
		return err
	}
	if _, err := os.Lstat(instance.StateDirectory); err == nil {
		return fmt.Errorf("managed emulator state directory remains after deletion")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("verify managed emulator state directory deletion: %w", err)
	}
	return nil
}

func (b *ManagedEmulatorBackend) deleteExactManagedAVD(ctx context.Context, name string) error {
	state, err := b.inspectExactManagedAVD(ctx, name)
	if err != nil {
		return err
	}
	if state.listed {
		if err := state.requireINIPathBinding(); err != nil {
			return fmt.Errorf("refuse avdmanager deletion without exact ini path authority: %w", err)
		}
		if _, err := b.runTool(ctx, b.avdManagerBinary, []string{"delete", "avd", "--name", name}, b.sdkEnvironment()); err != nil {
			return fmt.Errorf("delete exact managed AVD: %w", err)
		}
	}
	state, err = b.inspectExactManagedAVD(ctx, name)
	if err != nil {
		return fmt.Errorf("verify exact managed AVD deletion: %w", err)
	}
	if state.listed {
		return fmt.Errorf("exact managed AVD %q remains after deletion", name)
	}
	if err := removeExactManagedAVDPaths(state); err != nil {
		return fmt.Errorf("remove exact unregistered managed AVD filesystem residue: %w", err)
	}
	state, err = b.inspectExactManagedAVD(ctx, name)
	if err != nil {
		return fmt.Errorf("prove exact managed AVD filesystem deletion: %w", err)
	}
	if state.any() {
		return fmt.Errorf("managed AVD %q retains list or filesystem state after exact deletion", name)
	}
	return nil
}

func (b *ManagedEmulatorBackend) Quarantine(ctx context.Context, instance Instance, mode ports.StopMode) (BackendQuarantineState, error) {
	state := BackendQuarantineState{RuntimeID: instance.RuntimeID, ObservedAt: b.now().UTC()}
	if err := b.Stop(ctx, instance, mode); err != nil {
		return state, err
	}
	state.ExecutionStopped, state.NetworkUnreachable = true, true
	info, err := os.Stat(instance.StateDirectory)
	if err != nil {
		return state, err
	}
	if !info.IsDir() {
		return state, fmt.Errorf("managed emulator state path is not a directory")
	}
	state.StatePreserved = true
	return state, nil
}

func (b *ManagedEmulatorBackend) AdoptStopped(ctx context.Context, instance Instance, proof BackendQuarantineState) (BackendQuarantineState, error) {
	if err := validateStoppedAdoption(instance, proof); err != nil {
		return BackendQuarantineState{}, err
	}
	return b.inspectAndRememberStopped(ctx, instance, proof)
}

func (b *ManagedEmulatorBackend) InspectStopped(ctx context.Context, instance Instance) (BackendQuarantineState, error) {
	proof := BackendQuarantineState{
		RuntimeID:          instance.RuntimeID,
		ExecutionStopped:   true,
		NetworkUnreachable: true,
		StatePreserved:     true,
		ObservedAt:         b.now().UTC(),
	}
	return b.inspectAndRememberStopped(ctx, instance, proof)
}

func (b *ManagedEmulatorBackend) inspectAndRememberStopped(ctx context.Context, instance Instance, proof BackendQuarantineState) (BackendQuarantineState, error) {
	if err := requirePathWithin(b.stateRoot, instance.StateDirectory, true); err != nil {
		return BackendQuarantineState{}, err
	}
	avd, err := b.inspectExactManagedAVD(ctx, instance.RuntimeID)
	if err != nil {
		return BackendQuarantineState{}, err
	}
	if instance.RuntimeID != instance.Allocation.InstanceName {
		return BackendQuarantineState{}, fmt.Errorf("exact stopped managed AVD is absent from the configured AVD home")
	}
	if err := avd.requireComplete(); err != nil {
		return BackendQuarantineState{}, fmt.Errorf("exact stopped managed AVD is incomplete or redirected: %w", err)
	}
	info, statErr := os.Lstat(instance.StateDirectory)
	if statErr != nil {
		return BackendQuarantineState{}, fmt.Errorf("inspect preserved managed emulator directory %q: %w", instance.StateDirectory, statErr)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return BackendQuarantineState{}, fmt.Errorf("preserved managed emulator path %q is not an exact regular directory", instance.StateDirectory)
	}
	if err := b.validateManagedImageBinding(instance); err != nil {
		return BackendQuarantineState{}, err
	}
	if _, err := b.requireSystemImage(instance.Fingerprint.SystemImageDigest, true); err != nil {
		return BackendQuarantineState{}, fmt.Errorf("re-prove exact managed system image before stopped adoption: %w", err)
	}
	if err := b.requireManagedEndpointAbsent(ctx, instance); err != nil {
		return BackendQuarantineState{}, err
	}
	if err := b.requirePersistedManagedProcessStopped(ctx, instance); err != nil {
		return BackendQuarantineState{}, err
	}
	stopped := newStoppedManagedProcess()
	b.mu.Lock()
	prior := b.processes[instance.RuntimeID]
	if prior != nil {
		running, processErr := managedProcessState(prior)
		if processErr != nil {
			b.mu.Unlock()
			return BackendQuarantineState{}, fmt.Errorf("managed emulator %q has ambiguous process ownership during stopped adoption: %w", instance.RuntimeID, processErr)
		}
		if running {
			b.mu.Unlock()
			return BackendQuarantineState{}, fmt.Errorf("managed emulator %q has live process ownership during stopped adoption", instance.RuntimeID)
		}
	}
	b.mu.Unlock()
	if prior != nil {
		if err := closeManagedHostProcess(prior); err != nil {
			return BackendQuarantineState{}, fmt.Errorf("close prior stopped managed emulator process authority: %w", err)
		}
	}
	b.mu.Lock()
	if b.processes[instance.RuntimeID] != prior {
		b.mu.Unlock()
		return BackendQuarantineState{}, fmt.Errorf("managed emulator process ownership changed during stopped adoption")
	}
	b.processes[instance.RuntimeID] = stopped
	b.mu.Unlock()
	proof.ObservedAt = b.now().UTC()
	return proof, nil
}

func (b *ManagedEmulatorBackend) validateManagedImageBinding(instance Instance) error {
	image, found := b.systemImages[instance.Fingerprint.SystemImageDigest.String()]
	if !found {
		return fmt.Errorf("stopped managed emulator references an unconfigured system image")
	}
	var binding managedImageBinding
	if err := readStrictManifest(filepath.Join(instance.SystemImageDirectory, managedImageBindingFilename), &binding); err != nil {
		return fmt.Errorf("read stopped managed emulator system-image binding: %w", err)
	}
	if binding.Digest != instance.Fingerprint.SystemImageDigest.String() || binding.Package != image.Package || filepath.Clean(binding.Directory) != filepath.Clean(image.Directory) {
		return fmt.Errorf("stopped managed emulator system-image binding differs from configured authority")
	}
	return nil
}

func (b *ManagedEmulatorBackend) requireManagedEndpointAbsent(ctx context.Context, instance Instance) error {
	result, adbErr := runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "get-state")
	if !confirmedADBUnreachable(result, adbErr) {
		return fmt.Errorf("exact managed emulator serial is reachable or its absence is unproven: %w", adbErr)
	}
	port, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		return err
	}
	for _, endpoint := range []struct {
		name string
		port int
	}{
		{name: "console", port: port},
		{name: "ADB transport", port: port + 1},
	} {
		if err := requireManagedLoopbackPortAbsent(ctx, endpoint.name, endpoint.port); err != nil {
			return err
		}
	}
	return nil
}

func requireManagedLoopbackPortAbsent(ctx context.Context, name string, port int) error {
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err == nil {
		_ = connection.Close()
		return fmt.Errorf("exact managed emulator %s port %d remains reachable", name, port)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(strings.ToLower(err.Error()), "refused") {
		return nil
	}
	return fmt.Errorf("could not prove exact managed emulator %s port %d has no listener: %w", name, port, err)
}

func newStoppedManagedProcess() *managedProcess {
	done := make(chan struct{})
	logsDone := make(chan struct{})
	close(done)
	close(logsDone)
	return &managedProcess{done: done, logsDone: logsDone, stopped: true}
}

func (b *ManagedEmulatorBackend) rememberRecoveredStoppedManagedProcess(instance Instance, ownership managedProcessOwnership) *managedProcess {
	stopped := newStoppedManagedProcess()
	stopped.authority = ownership
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing := b.processes[instance.RuntimeID]; existing != nil {
		return existing
	}
	b.processes[instance.RuntimeID] = stopped
	return stopped
}

func (b *ManagedEmulatorBackend) rememberManagedEmulatorNeverStarted(instance Instance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.processes[instance.RuntimeID] == nil {
		b.processes[instance.RuntimeID] = newStoppedManagedProcess()
	}
}

func (b *ManagedEmulatorBackend) installAndRequireSystemImage(ctx context.Context, digest domain.Digest) (ManagedSystemImage, error) {
	image, found := b.systemImages[digest.String()]
	if !found {
		return ManagedSystemImage{}, fmt.Errorf("no managed system-image package maps exact digest %s", digest.String())
	}
	if info, err := os.Stat(image.Directory); err == nil && info.IsDir() {
		// An installed image is verified byte-for-byte before sdkmanager is
		// considered. A digest mismatch fails closed and is never "repaired" by
		// an online mutation.
		return b.requireSystemImage(digest, true)
	} else if err != nil && !os.IsNotExist(err) {
		return ManagedSystemImage{}, fmt.Errorf("inspect mapped system image before install: %w", err)
	}
	if _, err := b.runTool(ctx, b.sdkManagerBinary, []string{"--install", image.Package}, b.sdkEnvironment()); err != nil {
		return ManagedSystemImage{}, fmt.Errorf("install exact Android system image: %w", err)
	}
	return b.requireSystemImage(digest, true)
}

func (b *ManagedEmulatorBackend) requireSystemImage(digest domain.Digest, requireFiles bool) (ManagedSystemImage, error) {
	image, found := b.systemImages[digest.String()]
	if !found {
		return ManagedSystemImage{}, fmt.Errorf("no managed system-image package maps TargetTemplate.ImageDigest %s", digest.String())
	}
	info, err := os.Stat(image.Directory)
	if err != nil || !info.IsDir() {
		if !requireFiles && os.IsNotExist(err) {
			return ManagedSystemImage{}, fmt.Errorf("mapped system image %s is not installed at %q", digest.String(), image.Directory)
		}
		return ManagedSystemImage{}, fmt.Errorf("inspect mapped system image directory: %w", err)
	}
	observed, err := DigestManagedSystemImage(image.Directory)
	if err != nil {
		return ManagedSystemImage{}, err
	}
	if observed != digest {
		return ManagedSystemImage{}, fmt.Errorf("installed system image digest %s does not match planned digest %s", observed.String(), digest.String())
	}
	return image, nil
}

// DigestManagedSystemImage hashes the sorted relative paths, sizes, and bytes
// of a system-image tree. It gives profile configuration a reproducible value
// for TargetTemplate.ImageDigest and rejects symlinks/special files.
func DigestManagedSystemImage(root string) (domain.Digest, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return domain.Digest{}, err
	}
	paths := make([]string, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("system image contains unsupported path type %q", path)
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return domain.Digest{}, err
	}
	if len(paths) == 0 {
		return domain.Digest{}, fmt.Errorf("system image directory contains no regular files")
	}
	sort.Strings(paths)
	hash := sha256.New()
	_, _ = io.WriteString(hash, "world.android-system-image.v1\n")
	for _, relative := range paths {
		path := filepath.Join(root, filepath.FromSlash(relative))
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() {
			return domain.Digest{}, fmt.Errorf("inspect system-image file %q: %w", relative, statErr)
		}
		_ = binary.Write(hash, binary.BigEndian, uint32(len(relative)))
		_, _ = io.WriteString(hash, relative)
		_ = binary.Write(hash, binary.BigEndian, info.Size())
		file, openErr := os.Open(path)
		if openErr != nil {
			return domain.Digest{}, openErr
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return domain.Digest{}, errors.Join(copyErr, closeErr)
		}
	}
	digest, err := domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
	return digest, err
}

func openManagedEmulatorLogs(instance Instance) (*os.File, *os.File, error) {
	stdout, err := os.OpenFile(filepath.Join(instance.StateDirectory, managedEmulatorStdoutFilename), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	stderr, err := os.OpenFile(filepath.Join(instance.StateDirectory, managedEmulatorStderrFilename), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		_ = stdout.Close()
		return nil, nil, err
	}
	return stdout, stderr, nil
}

func (b *ManagedEmulatorBackend) captureProcessLogs(process command.Process, stdout, stderr *os.File) *managedProcess {
	record := &managedProcess{launcher: process, done: make(chan struct{}), logsDone: make(chan struct{})}
	var logs sync.WaitGroup
	logs.Add(2)
	copyLog := func(reader io.ReadCloser, file *os.File) {
		defer logs.Done()
		defer reader.Close()
		defer file.Close()
		_, _ = io.Copy(&boundedLogWriter{writer: file, remaining: b.maximumLogBytes}, reader)
	}
	go copyLog(process.Stdout(), stdout)
	go copyLog(process.Stderr(), stderr)
	go func() {
		err := process.Wait()
		record.mu.Lock()
		record.waitErr = err
		record.mu.Unlock()
		close(record.done)
		logs.Wait()
		close(record.logsDone)
	}()
	return record
}

func (b *ManagedEmulatorBackend) processExitBeforeReadiness(ctx context.Context, instance Instance) error {
	b.mu.Lock()
	record := b.processes[instance.RuntimeID]
	b.mu.Unlock()
	if record == nil || !channelClosed(record.done) {
		return nil
	}
	running, stateErr := managedProcessState(record)
	if stateErr == nil && running {
		return nil
	}

	select {
	case <-record.logsDone:
	case <-ctx.Done():
	}
	record.mu.Lock()
	waitErr := record.waitErr
	record.mu.Unlock()

	details := "managed emulator process exited before Android readiness"
	if stateErr != nil {
		details += ": successor verification failed: " + stateErr.Error()
	}
	if waitErr != nil {
		details += "; launcher_wait_error=" + waitErr.Error()
	}
	for _, log := range []struct {
		label    string
		filename string
	}{
		{label: "stdout", filename: managedEmulatorStdoutFilename},
		{label: "stderr", filename: managedEmulatorStderrFilename},
	} {
		tail, err := readManagedEmulatorLogTail(filepath.Join(instance.StateDirectory, log.filename), managedEmulatorDiagnosticLogBytes)
		if err != nil {
			if !os.IsNotExist(err) {
				details += fmt.Sprintf("; %s_log_error=%q", log.label, err.Error())
			}
			continue
		}
		if tail != "" {
			details += fmt.Sprintf("; %s_tail=%q", log.label, tail)
		}
	}
	return fmt.Errorf("%s", details)
}

func readManagedEmulatorLogTail(path string, limit int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	offset := info.Size() - limit
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	value, err := io.ReadAll(io.LimitReader(file, limit))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func (ownership managedProcessOwnership) validate(instance Instance, authority managedHostProcessAuthority) error {
	if authority == nil {
		return fmt.Errorf("managed emulator process ownership has no configured resource authority")
	}
	consolePort, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		return err
	}
	if ownership.RuntimeID != instance.RuntimeID || ownership.AVDName != instance.Allocation.InstanceName ||
		ownership.Serial != instance.Allocation.Serial || ownership.ConsolePort != consolePort {
		return fmt.Errorf("managed emulator process ownership identifies a different runtime, AVD, serial, or console port")
	}
	if ownership.PID <= 0 || ownership.PIDFile != managedEmulatorPIDFilename || strings.TrimSpace(ownership.ExecutablePath) == "" || strings.TrimSpace(ownership.StartToken) == "" ||
		ownership.ResourceAuthority != authority.Kind() || ownership.ResourceIdentity != authority.ResourceIdentity(instance) ||
		ownership.CPUMilli != instance.Resources.CPUMilli || ownership.MemoryBytes != instance.Resources.MemoryBytes ||
		ownership.StorageBytes != instance.Resources.StorageBytes || ownership.GuestMemoryBytes != instance.GuestMemoryBytes {
		return fmt.Errorf("managed emulator process ownership is incomplete")
	}
	if ownership.ResourceAnchored != authority.ResourcesEnforced() {
		return fmt.Errorf("managed emulator process ownership lifetime-anchor state differs from configured resource enforcement")
	}
	if err := ownership.Storage.validate(instance); err != nil {
		return fmt.Errorf("managed emulator process ownership storage authority: %w", err)
	}
	if err := ownership.Storage.validateStoredIdentity(instance); err != nil {
		return fmt.Errorf("managed emulator process ownership immutable storage identity: %w", err)
	}
	return nil
}

func managedEmulatorResourceIdentity(instance Instance) string {
	if instance.RuntimeID == "" || instance.StateDirectory == "" {
		return "process-tree-limit"
	}
	sum := sha256.Sum256([]byte(instance.RuntimeID + "\x00" + filepath.Clean(instance.StateDirectory)))
	return "world.android." + hex.EncodeToString(sum[:])
}

func (b *ManagedEmulatorBackend) adoptManagedHostProcess(ctx context.Context, instance Instance, record *managedProcess) (bool, error) {
	record.adoptMu.Lock()
	defer record.adoptMu.Unlock()
	record.mu.Lock()
	owned := record.owned
	stopped := record.stopped
	record.mu.Unlock()
	if stopped {
		return false, nil
	}
	if owned != nil {
		running, err := owned.Running()
		if err != nil {
			return false, fmt.Errorf("verify exact managed emulator host process: %w", err)
		}
		return running, nil
	}

	pid, found, err := readManagedEmulatorPID(instance.StateDirectory)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	storage, err := b.requireManagedDataStorage(ctx, instance, managedDataOverlayPresent)
	if errors.Is(err, errManagedDataOverlayNotReady) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("re-prove exact managed data storage before host-process adoption: %w", err)
	}
	intent, intentFound, err := loadManagedLaunchIntent(instance, b.emulatorBinary)
	if err != nil {
		return false, err
	}
	if !intentFound {
		return false, fmt.Errorf("managed emulator PID exists without immutable launch intent")
	}
	if err := intent.Storage.requireBinding(instance, storage); err != nil {
		return false, fmt.Errorf("managed emulator launch intent no longer binds exact data storage: %w", err)
	}
	hostProcess, err := b.processAuthority.Open(pid, b.emulatorBinary, managedEmulatorPIDPath(instance), storage, instance)
	if err != nil {
		return false, fmt.Errorf("open exact managed emulator PID %d from its fresh PID file: %w", pid, err)
	}
	ownership := managedProcessOwnership{
		RuntimeID: instance.RuntimeID, AVDName: instance.Allocation.InstanceName, Serial: instance.Allocation.Serial,
		PID: pid, PIDFile: managedEmulatorPIDFilename, ExecutablePath: hostProcess.ExecutablePath(), StartToken: hostProcess.StartToken(),
		ResourceAuthority: b.processAuthority.Kind(), ResourceIdentity: b.processAuthority.ResourceIdentity(instance),
		CPUMilli: instance.Resources.CPUMilli, MemoryBytes: instance.Resources.MemoryBytes, StorageBytes: instance.Resources.StorageBytes,
		GuestMemoryBytes: instance.GuestMemoryBytes,
		ResourceAnchored: false,
		Storage:          storage.authority(instance),
	}
	ownership.ConsolePort, err = instance.Allocation.EmulatorConsolePort()
	if err != nil {
		_ = hostProcess.Close()
		return false, err
	}
	running, err := hostProcess.Running()
	if err != nil || !running {
		_ = hostProcess.Close()
		if err != nil {
			return false, fmt.Errorf("verify fresh managed emulator PID %d: %w", pid, err)
		}
		return false, fmt.Errorf("fresh managed emulator PID %d is not running: %w", pid, errManagedHostProcessNotFound)
	}
	if b.processAuthority.ResourcesEnforced() {
		anchor, supported := hostProcess.(managedResourceLifetimeAnchor)
		if !supported {
			_ = hostProcess.Close()
			return false, fmt.Errorf("host resource authority does not provide an exact runtime lifetime anchor")
		}
		if err := anchor.AnchorResourceAuthority(); err != nil {
			_ = hostProcess.Close()
			return false, fmt.Errorf("anchor host resource authority in exact managed emulator PID %d: %w", pid, err)
		}
		ownership.ResourceAnchored = true
	}
	if err := ownership.validate(instance, b.processAuthority); err != nil {
		return false, cleanupFailedManagedHostAdoption(hostProcess, ownership.ResourceAnchored, err)
	}
	if err := commitManagedProcessOwnership(instance, ownership, b.processAuthority); err != nil {
		return false, cleanupFailedManagedHostAdoption(hostProcess, ownership.ResourceAnchored, err)
	}
	record.mu.Lock()
	if record.owned != nil {
		record.mu.Unlock()
		_ = hostProcess.Close()
		return true, nil
	}
	record.owned = hostProcess
	record.authority = ownership
	record.mu.Unlock()
	return true, nil
}

func cleanupFailedManagedHostAdoption(hostProcess managedHostProcess, anchored bool, cause error) error {
	if !anchored {
		return errors.Join(cause, hostProcess.Close())
	}
	return errors.Join(cause, hostProcess.Kill(), hostProcess.Close())
}

func (b *ManagedEmulatorBackend) waitForManagedHostProcessOwnership(ctx context.Context, instance Instance, record *managedProcess) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		running, err := b.adoptManagedHostProcess(ctx, instance, record)
		if err != nil {
			return err
		}
		if running {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *ManagedEmulatorBackend) requireReadyManagedHostProcess(ctx context.Context, instance Instance, record *managedProcess, state ReadinessState) error {
	if !state.Ready() || !state.Identity.Rooted || !state.Identity.Debuggable || state.Identity.AVDName != instance.Allocation.InstanceName {
		return incompleteAndroidReadinessError(state, instance.Allocation.InstanceName)
	}
	running, err := b.adoptManagedHostProcess(ctx, instance, record)
	if err != nil {
		return err
	}
	if !running {
		return fmt.Errorf("Android became ready before its exact emulator host-process successor was proven")
	}
	return nil
}

func readManagedEmulatorPID(stateDirectory string) (int, bool, error) {
	pidPath := filepath.Join(stateDirectory, managedEmulatorPIDFilename)
	if err := requirePathWithin(stateDirectory, pidPath, false); err != nil {
		return 0, false, err
	}
	info, err := os.Lstat(pidPath)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("inspect managed emulator PID file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumManagedEmulatorPIDBytes {
		return 0, false, fmt.Errorf("managed emulator PID file is not a bounded regular file")
	}
	content, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false, fmt.Errorf("read managed emulator PID file: %w", err)
	}
	canonical := strings.TrimSpace(string(content))
	parsed, err := strconv.ParseInt(canonical, 10, 32)
	if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != canonical {
		return 0, false, fmt.Errorf("managed emulator PID file must contain one canonical positive decimal PID")
	}
	return int(parsed), true, nil
}

func managedEmulatorPIDPath(instance Instance) string {
	return filepath.Join(instance.StateDirectory, managedEmulatorPIDFilename)
}

func commitManagedLaunchIntent(instance Instance, emulatorBinary string, storage managedDataStorageBinding) error {
	intent := managedLaunchIntent{
		Instance: instance, EmulatorBinary: emulatorBinary, PIDFile: managedEmulatorPIDFilename,
		Storage: storage.authority(instance),
	}
	if err := intent.validate(instance, emulatorBinary); err != nil {
		return err
	}
	if err := writeExclusiveManagedManifest(filepath.Join(instance.StateDirectory, managedEmulatorLaunchFilename), intent); err != nil {
		return fmt.Errorf("commit immutable managed emulator launch intent: %w", err)
	}
	return nil
}

func loadManagedLaunchIntent(instance Instance, emulatorBinary string) (managedLaunchIntent, bool, error) {
	var intent managedLaunchIntent
	err := readStrictManifest(filepath.Join(instance.StateDirectory, managedEmulatorLaunchFilename), &intent)
	if os.IsNotExist(err) {
		return managedLaunchIntent{}, false, nil
	}
	if err != nil {
		return managedLaunchIntent{}, false, fmt.Errorf("read managed emulator launch intent: %w", err)
	}
	if err := intent.validate(instance, emulatorBinary); err != nil {
		return managedLaunchIntent{}, false, err
	}
	return intent, true, nil
}

func (intent managedLaunchIntent) validate(instance Instance, emulatorBinary string) error {
	if !instancesEqual(intent.Instance, instance) || intent.EmulatorBinary != emulatorBinary || intent.PIDFile != managedEmulatorPIDFilename {
		return fmt.Errorf("managed emulator launch intent differs from the exact runtime plan or configured emulator")
	}
	if err := intent.Storage.validate(instance); err != nil {
		return fmt.Errorf("managed emulator launch intent storage authority: %w", err)
	}
	if err := intent.Storage.validateStoredIdentity(instance); err != nil {
		return fmt.Errorf("managed emulator launch intent immutable storage identity: %w", err)
	}
	return nil
}

func commitManagedProcessOwnership(instance Instance, ownership managedProcessOwnership, authority managedHostProcessAuthority) error {
	if err := ownership.validate(instance, authority); err != nil {
		return err
	}
	path := filepath.Join(instance.StateDirectory, managedEmulatorOwnershipFilename)
	if err := writeExclusiveManagedManifest(path, ownership); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("commit immutable managed emulator process ownership: %w", err)
	}
	existing, found, err := loadManagedProcessOwnership(instance, authority)
	if err != nil {
		return err
	}
	if !found || existing != ownership {
		return fmt.Errorf("existing managed emulator process ownership differs from the exact live successor")
	}
	return nil
}

func writeExclusiveManagedManifest(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.WriteExclusive(path, append(encoded, '\n'), 0o600)
}

func loadManagedProcessOwnership(instance Instance, authority managedHostProcessAuthority) (managedProcessOwnership, bool, error) {
	var ownership managedProcessOwnership
	err := readStrictManifest(filepath.Join(instance.StateDirectory, managedEmulatorOwnershipFilename), &ownership)
	if os.IsNotExist(err) {
		return managedProcessOwnership{}, false, nil
	}
	if err != nil {
		return managedProcessOwnership{}, false, fmt.Errorf("read managed emulator process ownership: %w", err)
	}
	if err := ownership.validate(instance, authority); err != nil {
		return managedProcessOwnership{}, false, err
	}
	return ownership, true, nil
}

func (b *ManagedEmulatorBackend) requireManagedProcessRecord(ctx context.Context, instance Instance) (*managedProcess, error) {
	b.mu.Lock()
	record := b.processes[instance.RuntimeID]
	b.mu.Unlock()
	if record != nil {
		return record, nil
	}
	ownership, found, err := loadManagedProcessOwnership(instance, b.processAuthority)
	if err != nil {
		return nil, err
	}
	if !found {
		if _, launchFound, launchErr := loadManagedLaunchIntent(instance, b.emulatorBinary); launchErr != nil {
			return nil, launchErr
		} else if !launchFound {
			return nil, fmt.Errorf("no exact persisted managed emulator launch or process ownership exists")
		}
		done := make(chan struct{})
		logsDone := make(chan struct{})
		close(done)
		close(logsDone)
		reconstructed := &managedProcess{done: done, logsDone: logsDone}
		running, adoptErr := b.adoptManagedHostProcess(ctx, instance, reconstructed)
		if adoptErr != nil {
			return nil, fmt.Errorf("%w: persisted launch intent has no durable successor ownership: %w", errManagedLaunchUnresolved, adoptErr)
		}
		if !running {
			return nil, fmt.Errorf("%w: persisted launch intent has no PID-bound live successor", errManagedLaunchUnresolved)
		}
		b.mu.Lock()
		if existing := b.processes[instance.RuntimeID]; existing != nil {
			b.mu.Unlock()
			_ = closeManagedHostProcess(reconstructed)
			return existing, nil
		}
		b.processes[instance.RuntimeID] = reconstructed
		b.mu.Unlock()
		return reconstructed, nil
	}
	hostProcess, err := b.openPersistedManagedHostProcess(ctx, instance, ownership)
	if errors.Is(err, errManagedHostProcessNotFound) || errors.Is(err, errManagedHostProcessIdentityMismatch) {
		return b.rememberRecoveredStoppedManagedProcess(instance, ownership), nil
	}
	if err != nil {
		return nil, err
	}
	if hostProcess.ExecutablePath() != ownership.ExecutablePath || hostProcess.StartToken() != ownership.StartToken {
		_ = hostProcess.Close()
		return b.rememberRecoveredStoppedManagedProcess(instance, ownership), nil
	}
	running, err := hostProcess.Running()
	if err != nil {
		_ = hostProcess.Close()
		return nil, err
	}
	if !running {
		_ = hostProcess.Close()
		return b.rememberRecoveredStoppedManagedProcess(instance, ownership), nil
	}
	done := make(chan struct{})
	logsDone := make(chan struct{})
	close(done)
	close(logsDone)
	recovered := &managedProcess{done: done, logsDone: logsDone, owned: hostProcess, authority: ownership}
	b.mu.Lock()
	if existing := b.processes[instance.RuntimeID]; existing != nil {
		b.mu.Unlock()
		_ = hostProcess.Close()
		return existing, nil
	}
	b.processes[instance.RuntimeID] = recovered
	b.mu.Unlock()
	return recovered, nil
}

func (b *ManagedEmulatorBackend) requirePersistedManagedProcessStopped(ctx context.Context, instance Instance) error {
	ownership, found, err := loadManagedProcessOwnership(instance, b.processAuthority)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("stopped managed emulator lacks exact persisted host-process ownership")
	}
	hostProcess, err := b.openPersistedManagedHostProcess(ctx, instance, ownership)
	if errors.Is(err, errManagedHostProcessNotFound) || errors.Is(err, errManagedHostProcessIdentityMismatch) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify stopped managed emulator host process: %w", err)
	}
	defer hostProcess.Close()
	if hostProcess.ExecutablePath() != ownership.ExecutablePath || hostProcess.StartToken() != ownership.StartToken {
		// The PID was reused after the exact recorded process exited. The retained
		// handle is never used against the unrelated successor.
		return nil
	}
	running, err := hostProcess.Running()
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("exact persisted managed emulator host process remains running")
	}
	return nil
}

func (b *ManagedEmulatorBackend) openPersistedManagedHostProcess(ctx context.Context, instance Instance, ownership managedProcessOwnership) (managedHostProcess, error) {
	storage, err := b.requireManagedDataStorage(ctx, instance, managedDataOverlayPresent)
	if err != nil {
		return nil, fmt.Errorf("re-prove persisted managed data storage before process adoption: %w", err)
	}
	if err := ownership.Storage.requireBinding(instance, storage); err != nil {
		return nil, fmt.Errorf("persisted managed process no longer binds exact data storage: %w", err)
	}
	return b.processAuthority.Open(ownership.PID, b.emulatorBinary, managedEmulatorPIDPath(instance), storage, instance)
}

func managedProcessState(record *managedProcess) (bool, error) {
	record.mu.Lock()
	owned := record.owned
	stopped := record.stopped
	record.mu.Unlock()
	if stopped {
		return false, nil
	}
	if owned != nil {
		running, err := owned.Running()
		if err != nil {
			return true, err
		}
		if running {
			return true, nil
		}
	}
	return !channelClosed(record.done), nil
}

func closeManagedHostProcess(record *managedProcess) error {
	record.mu.Lock()
	owned := record.owned
	launcher := record.launcher
	record.owned = nil
	record.mu.Unlock()
	var result error
	if owned != nil {
		result = owned.Close()
	}
	result = errors.Join(result, closeManagedLauncherContainment(launcher))
	return result
}

func closeManagedLauncherContainment(launcher command.Process) error {
	if contained, ok := launcher.(interface{ CloseContainment() error }); ok {
		return contained.CloseContainment()
	}
	return nil
}

func (b *ManagedEmulatorBackend) requestGracefulManagedStop(ctx context.Context, instance Instance) error {
	exact, err := observeSDKEmulatorProcess(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, instance.Allocation.InstanceName)
	if err != nil {
		result, stateErr := runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "get-state")
		if confirmedADBUnreachable(result, stateErr) {
			return nil
		}
		return errors.Join(err, stateErr)
	}
	if !exact {
		return fmt.Errorf("exact managed emulator AVD identity is absent at serial %q", instance.Allocation.Serial)
	}
	if _, err := runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "emu", "kill"); err != nil {
		return err
	}
	return nil
}

func (b *ManagedEmulatorBackend) forceStopManagedProcess(instance Instance, record *managedProcess) error {
	stopContext, cancel := b.newManagedEmulatorStopContext()
	defer cancel()
	return b.forceStopManagedProcessWithin(stopContext, instance, record)
}

func (b *ManagedEmulatorBackend) forceStopManagedProcessWithin(ctx context.Context, instance Instance, record *managedProcess) error {
	record.mu.Lock()
	stopped := record.stopped
	record.mu.Unlock()
	if stopped {
		return nil
	}
	var stopErrs []error
	_, adoptErr := b.adoptManagedHostProcess(ctx, instance, record)
	if adoptErr != nil && !errors.Is(adoptErr, errManagedHostProcessNotFound) {
		stopErrs = append(stopErrs, adoptErr)
	}
	record.mu.Lock()
	owned := record.owned
	launcher := record.launcher
	record.mu.Unlock()
	if owned != nil {
		running, err := owned.Running()
		if err != nil {
			stopErrs = append(stopErrs, err)
		} else if running {
			if err := b.requireExactManagedADBIdentityIfReachable(ctx, instance); err != nil {
				stopErrs = append(stopErrs, err)
			} else if err := owned.Kill(); err != nil {
				stopErrs = append(stopErrs, err)
			}
		}
	}
	if launcher != nil {
		if err := launcher.Kill(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("terminate retained managed emulator launcher containment: %w", err))
		}
	}
	if launcher != nil {
		if err := waitManagedProcessDrain(ctx, record); err != nil {
			stopErrs = append(stopErrs, err)
		}
		if err := closeManagedLauncherContainment(launcher); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("close retained managed emulator launcher containment: %w", err))
		}
	}
	return errors.Join(stopErrs...)
}

func waitManagedProcessDrain(ctx context.Context, record *managedProcess) error {
	for _, completion := range []struct {
		name string
		done <-chan struct{}
	}{
		{name: "launcher", done: record.done},
		{name: "launcher logs", done: record.logsDone},
	} {
		select {
		case <-completion.done:
		case <-ctx.Done():
			return fmt.Errorf("wait for managed emulator %s to drain: %w", completion.name, ctx.Err())
		}
	}
	return nil
}

func (b *ManagedEmulatorBackend) requireExactManagedADBIdentityIfReachable(ctx context.Context, instance Instance) error {
	result, err := runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "get-state")
	if err != nil {
		if confirmedADBUnreachable(result, err) {
			return nil
		}
		return err
	}
	exact, err := observeSDKEmulatorProcess(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, instance.Allocation.InstanceName)
	if err != nil {
		return err
	}
	if !exact {
		return fmt.Errorf("serial %q is reachable but does not identify exact managed AVD %q", instance.Allocation.Serial, instance.Allocation.InstanceName)
	}
	return nil
}

type boundedLogWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedLogWriter) Write(value []byte) (int, error) {
	original := len(value)
	if w.remaining <= 0 {
		return original, nil
	}
	if int64(len(value)) > w.remaining {
		value = value[:w.remaining]
	}
	written, err := w.writer.Write(value)
	w.remaining -= int64(written)
	if err != nil {
		return written, err
	}
	return original, nil
}

func (b *ManagedEmulatorBackend) ownedProcessRunning(runtimeID string) (bool, bool) {
	running, known, err := b.ownedProcessState(runtimeID)
	if err != nil {
		return true, known
	}
	return running, known
}

func (b *ManagedEmulatorBackend) ownedProcessState(runtimeID string) (bool, bool, error) {
	b.mu.Lock()
	record := b.processes[runtimeID]
	b.mu.Unlock()
	if record == nil {
		return false, false, nil
	}
	running, err := managedProcessState(record)
	return running, true, err
}

func (b *ManagedEmulatorBackend) waitStopped(ctx context.Context, instance Instance, force bool) error {
	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	var lastErr, lastForceErr error
	for {
		record, recordErr := b.requireManagedProcessRecord(ctx, instance)
		if recordErr != nil {
			return recordErr
		}
		_, adoptErr := b.adoptManagedHostProcess(ctx, instance, record)
		if adoptErr != nil {
			return fmt.Errorf("%w: exact successor ownership could not be established while stopping: %w", errManagedLaunchUnresolved, adoptErr)
		}
		if force {
			running, stateErr := managedProcessState(record)
			if stateErr != nil {
				return stateErr
			}
			if running {
				if stopErr := b.forceStopManagedProcessWithin(ctx, instance, record); stopErr != nil {
					lastForceErr = stopErr
				}
			}
		}
		record.mu.Lock()
		owned := record.owned
		stopped := record.stopped
		record.mu.Unlock()
		launcherDone := channelClosed(record.done)
		canSealProcessAbsence := stopped || owned != nil
		if !canSealProcessAbsence && launcherDone {
			return fmt.Errorf("%w: launcher exited without durable PID-bound successor ownership", errManagedLaunchUnresolved)
		}
		result, err := runExactSerialADBAt(ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial, adbMetadataOutputLimit, "get-state")
		if confirmedADBUnreachable(result, err) {
			running, known, processErr := b.ownedProcessState(instance.RuntimeID)
			if processErr != nil {
				return fmt.Errorf("verify exact managed emulator host process stopped: %w", processErr)
			}
			if !known {
				return fmt.Errorf("cannot prove exact managed emulator stopped: no owned process identity exists")
			}
			if !running && canSealProcessAbsence {
				if endpointErr := b.requireManagedEndpointAbsent(ctx, instance); endpointErr == nil {
					return nil
				} else {
					lastErr = endpointErr
				}
			}
		} else if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("could not prove exact managed emulator stopped: %w", errors.Join(ctx.Err(), lastErr, lastForceErr))
		case <-ticker.C:
		}
	}
}

func (b *ManagedEmulatorBackend) listAVDs(ctx context.Context) (map[string]struct{}, error) {
	output, err := b.runTool(ctx, b.emulatorBinary, []string{"-list-avds"}, b.sdkEnvironment())
	if err != nil {
		return nil, fmt.Errorf("inventory managed Android AVDs: %w", err)
	}
	result := make(map[string]struct{})
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if name == "" {
			continue
		}
		if !safeInstanceName(name) {
			return nil, fmt.Errorf("emulator inventory returned unsafe AVD name %q", name)
		}
		if _, duplicate := result[name]; duplicate {
			return nil, fmt.Errorf("emulator inventory returned duplicate AVD name %q", name)
		}
		result[name] = struct{}{}
	}
	return result, scanner.Err()
}

func (b *ManagedEmulatorBackend) ListRuntimeIDs(ctx context.Context) ([]string, error) {
	avds, err := b.listAVDs(ctx)
	if err != nil {
		return nil, err
	}
	filesystem, err := b.filesystemManagedAVDNames()
	if err != nil {
		return nil, err
	}
	for name := range filesystem {
		avds[name] = struct{}{}
	}
	result := make([]string, 0, len(avds))
	for name := range avds {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func (b *ManagedEmulatorBackend) runTool(ctx context.Context, program string, args, environment []string) (string, error) {
	return b.runToolWithInput(ctx, program, args, environment, nil)
}

func (b *ManagedEmulatorBackend) runToolWithInput(ctx context.Context, program string, args, environment []string, input io.Reader) (string, error) {
	invocation := command.Invocation{Program: program, Args: args, Environment: environment, Stdin: input, MaximumOutput: command.DefaultOutputLimit}
	if _, operatingSystemRunner := b.runner.(command.OS); operatingSystemRunner && runtime.GOOS == "windows" && isWindowsBatch(program) {
		batchArgs, commandErr := windowsBatchArguments(program, args)
		if commandErr != nil {
			return "", commandErr
		}
		invocation.Program = "cmd.exe"
		invocation.Args = batchArgs
	}
	result, err := b.runner.Run(ctx, invocation)
	output := strings.TrimSpace(string(result.Stdout) + "\n" + string(result.Stderr))
	return output, err
}

func (b *ManagedEmulatorBackend) sdkEnvironment() []string {
	return managedSDKEnvironment(os.Environ(), b.sdkRoot, b.avdHome)
}

func managedSDKEnvironment(ambient []string, sdkRoot, avdHome string) []string {
	overridden := []string{"ANDROID_SDK_ROOT", "ANDROID_HOME", "ANDROID_AVD_HOME"}
	result := make([]string, 0, len(ambient)+len(overridden))
	for _, entry := range ambient {
		name, _, found := strings.Cut(entry, "=")
		if found && (strings.EqualFold(name, "DEBUG") || containsFold(overridden, name)) {
			continue
		}
		result = append(result, entry)
	}
	return append(result,
		"ANDROID_SDK_ROOT="+sdkRoot,
		"ANDROID_HOME="+sdkRoot,
		"ANDROID_AVD_HOME="+avdHome,
	)
}

func containsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(value, candidate) {
			return true
		}
	}
	return false
}

func requireExactManagedRuntimeArguments(arguments []string, pidFile, dataImage string, instance Instance, caseInsensitive bool) error {
	port, err := instance.Allocation.EmulatorConsolePort()
	if err != nil {
		return err
	}
	expected := managedEmulatorLaunchArguments(instance, port, dataImage, pidFile)
	actual := arguments
	if len(actual) == len(expected)+1 {
		actual = actual[1:]
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("managed emulator process command line contains %d launch arguments; want exact %d", len(actual), len(expected))
	}
	for index := range expected {
		if index > 0 && (expected[index-1] == "-data" || expected[index-1] == "-pidfile") {
			if err := requireExactManagedProcessPathValue(actual[index], expected[index], caseInsensitive); err != nil {
				return fmt.Errorf("managed emulator process argument %d (%s): %w", index, expected[index-1], err)
			}
			continue
		}
		if actual[index] != expected[index] {
			return fmt.Errorf("managed emulator process argument %d is %q; want exact %q", index, actual[index], expected[index])
		}
	}
	return nil
}

func requireExactManagedProcessPathValue(actual, expected string, caseInsensitive bool) error {
	expectedPath, err := canonicalManagedProcessPathArgument(expected)
	if err != nil {
		return fmt.Errorf("canonicalize expected path: %w", err)
	}
	actualPath, err := canonicalManagedProcessPathArgument(actual)
	if err != nil {
		return fmt.Errorf("canonicalize observed path: %w", err)
	}
	matches := actualPath == expectedPath
	if caseInsensitive {
		matches = strings.EqualFold(actualPath, expectedPath)
	}
	if !matches {
		return fmt.Errorf("path %q differs from exact launch path %q", actualPath, expectedPath)
	}
	return nil
}

func canonicalManagedProcessPathArgument(path string) (string, error) {
	if strings.TrimSpace(path) == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("managed emulator process path must be absolute")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func (b *ManagedEmulatorBackend) avdPath(name string) string {
	return filepath.Join(b.avdHome, name+".avd")
}

// ValidateManagedEmulatorResources validates the host process-tree limits,
// guest -cores/-memory configuration, and exact guest /data block size.
// Daemon profile validation and runtime creation share this authority so their
// accepted configurations cannot drift.
func ValidateManagedEmulatorResources(resources admission.Resources, guestMemoryBytes int64) error {
	if err := androidcontract.ValidateHostCPUMilli(resources.CPUMilli); err != nil {
		return err
	}
	if resources.MemoryBytes <= 0 {
		return fmt.Errorf("managed emulator host process-tree memory limit must be positive")
	}
	if err := androidcontract.ValidateGuestMemoryBytes(guestMemoryBytes); err != nil {
		return err
	}
	if err := androidcontract.ValidateDataPartitionBytes(resources.StorageBytes); err != nil {
		return err
	}
	return nil
}

// ValidateManagedSystemImagePackage validates the exact sdkmanager package
// syntax accepted by the managed backend without duplicating profile rules.
func ValidateManagedSystemImagePackage(value string) error {
	parts := strings.Split(value, ";")
	if len(parts) < 2 {
		return fmt.Errorf("SDK package must be a semicolon-delimited system-image package")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, `/\\`) {
			return fmt.Errorf("SDK package contains an unsafe component")
		}
	}
	if parts[0] != "system-images" {
		return fmt.Errorf("SDK package is not a system image")
	}
	return nil
}

func requirePathWithin(root, candidate string, allowEqual bool) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) || (!allowEqual && relative == ".") {
		return fmt.Errorf("path is outside configured root")
	}
	return nil
}

func readSystemBuildProperties(root string) (map[string]string, error) {
	candidates := []string{filepath.Join(root, "build.prop"), filepath.Join(root, "system", "build.prop")}
	for _, candidate := range candidates {
		file, err := os.Open(candidate)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		properties := make(map[string]string)
		scanner := bufio.NewScanner(io.LimitReader(file, 4<<20))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			name, value, found := strings.Cut(line, "=")
			if found {
				properties[strings.TrimSpace(name)] = strings.TrimSpace(value)
			}
		}
		closeErr := file.Close()
		if err := errors.Join(scanner.Err(), closeErr); err != nil {
			return nil, err
		}
		return properties, nil
	}
	return nil, fmt.Errorf("system-image build.prop is unavailable")
}

func isWindowsBatch(program string) bool {
	extension := strings.ToLower(filepath.Ext(program))
	return extension == ".bat" || extension == ".cmd"
}

func windowsBatchArguments(program string, args []string) ([]string, error) {
	values := append([]string{program}, args...)
	for _, value := range values {
		if value == "" || strings.ContainsAny(value, "\r\n\"&|<>^%!()") {
			return nil, fmt.Errorf("Windows batch tool argument contains an unsafe cmd.exe metacharacter")
		}
	}
	// Keep each token separate so os/exec performs Windows argument quoting.
	// CALL is required because cmd.exe otherwise applies its special /C quote
	// stripping to a quoted batch path and can treat the quote as filename text.
	return append([]string{"/d", "/s", "/c", "call"}, values...), nil
}

func exactSDKManagerVersion(output string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		valid := true
		seenDigit := false
		for _, character := range line {
			if character >= '0' && character <= '9' {
				seenDigit = true
				continue
			}
			if character != '.' {
				valid = false
				break
			}
		}
		if valid && seenDigit && !strings.HasPrefix(line, ".") && !strings.HasSuffix(line, ".") && !strings.Contains(line, "..") {
			return line, nil
		}
	}
	return "", fmt.Errorf("sdkmanager returned no exact numeric version line")
}

func substantiveAccelerationEvidence(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.EqualFold(line, "accel") || strings.EqualFold(line, "accel:") {
			continue
		}
		if _, err := strconv.Atoi(line); err == nil {
			continue
		}
		return line
	}
	return ""
}

func firstNonBlankLine(value string) string {
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

func safeInstanceName(name string) bool {
	return Allocation{InstanceNumber: 1, InstanceName: name, Serial: "x", ADBAddress: "x"}.Validate() == nil
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

var _ Backend = (*ManagedEmulatorBackend)(nil)
var _ BackendQuarantiner = (*ManagedEmulatorBackend)(nil)
var _ BackendInventory = (*ManagedEmulatorBackend)(nil)
var _ BackendStoppedAdopter = (*ManagedEmulatorBackend)(nil)
