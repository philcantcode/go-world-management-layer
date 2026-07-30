// Command world-capabilities probes the same physical drivers used by
// world.Open and prints profile-ready capability fingerprints. It never
// provisions a target and never weakens Open's requirement for trusted image digests.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/androidcontract"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	agentdocker "github.com/philcantcode/go-world-management-layer/internal/drivers/agent/docker"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/linuxcontainer"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

const maximumPolicyBytes = int64(4 << 20)

type capabilityReport struct {
	Digest       string                      `json:"digest"`
	Evidence     map[string]string           `json:"evidence"`
	Capabilities map[string]capabilityDetail `json:"capabilities"`
}

type capabilityDetail struct {
	Status      string            `json:"status"`
	Constraints map[string]string `json:"constraints,omitempty"`
	Evidence    map[string]string `json:"evidence,omitempty"`
}

type report struct {
	Combined              capabilityReport                         `json:"combined"`
	Agent                 capabilityReport                         `json:"agent"`
	AgentPhysical         ports.AgentWorkspacePhysicalPolicyReport `json:"agent_physical_policy"`
	LinuxTarget           *capabilityReport                        `json:"linux_target,omitempty"`
	LinuxTargetPhysical   *ports.TargetPhysicalPolicyReport        `json:"linux_target_physical_policy,omitempty"`
	AndroidTarget         *capabilityReport                        `json:"android_target,omitempty"`
	AndroidTargetPhysical *ports.TargetPhysicalPolicyReport        `json:"android_target_physical_policy,omitempty"`
	ProcessObserver       *processObserverReport                   `json:"process_observer,omitempty"`
	EffectivePolicy       *effectivePolicyReport                   `json:"effective_policy,omitempty"`
}

type processObserverReport struct {
	Reference           string           `json:"reference"`
	Adapter             string           `json:"adapter"`
	Version             string           `json:"version"`
	RuntimeBinding      string           `json:"runtime_binding"`
	ConfigurationDigest string           `json:"configuration_digest"`
	Capability          capabilityReport `json:"capability"`
}

type processObserverOptions struct {
	Reference     string
	Configuration observerprocess.AdapterConfiguration
}

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string { return strings.Join(*values, ",") }

func (values *repeatedStringFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type androidProbeOptions struct {
	SDKRoot, ADBBinary, ADBServer, EmulatorBinary, SDKManagerBinary, AVDManagerBinary string
	BackendVersion, RuntimeVersion, SystemImageDigest, SystemImagePackage             string
	IsolationProfile, BaselineState                                                   string
	BaseConsolePort                                                                   int
	GuestMemoryBytes                                                                  int64
	BootTimeout                                                                       time.Duration
}

type effectivePolicyReport struct {
	Reference                   string `json:"reference"`
	Digest                      string `json:"digest"`
	CapabilityFingerprintDigest string `json:"capability_fingerprint_digest"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("world-capabilities", flag.ContinueOnError)
	dockerBinary := flags.String("docker-binary", "docker", "Docker CLI path")
	timeout := flags.Duration("timeout", 30*time.Second, "overall probe deadline")
	allowPtrace := flags.Bool("target-allow-ptrace", false, "match Open target ptrace setting")
	agentContainerUser := flags.String("agent-container-user", "65532:65532", "match Open unprivileged agent container user")
	linuxTargetDriver := flags.String("linux-target-driver", "docker", "Linux target driver: docker or none")
	androidTargetDriver := flags.String("android-target-driver", "none", "Android target driver: android-emulator or none")
	androidSDKRoot := flags.String("android-sdk-root", "", "absolute Android SDK root used by world.Open")
	androidADBBinary := flags.String("android-adb-binary", "adb", "ADB binary path used by world.Open")
	androidADBServer := flags.String("android-adb-server", "127.0.0.1:5037", "literal loopback ADB server endpoint used by world.Open")
	androidEmulatorBinary := flags.String("android-emulator-binary", "emulator", "Android emulator binary path used by world.Open")
	androidSDKManagerBinary := flags.String("android-sdkmanager-binary", "sdkmanager", "sdkmanager binary path used by world.Open")
	androidAVDManagerBinary := flags.String("android-avdmanager-binary", "avdmanager", "avdmanager binary path used by world.Open")
	androidBackendVersion := flags.String("android-backend-version", "", "exact observed emulator version expected by world.Open")
	androidRuntimeVersion := flags.String("android-runtime-version", "", "exact ro.system.build.fingerprint expected by world.Open")
	androidBaseConsolePort := flags.Int("android-adb-base-port", cuttlefish.ManagedEmulatorMinConsolePort, "first even managed-emulator console port used by world.Open")
	androidSystemImageDigest := flags.String("android-system-image-digest", "", "exact installed Android system-image tree digest")
	androidSystemImagePackage := flags.String("android-system-image-package", "", "exact SDK system-image package identifier")
	androidIsolationProfile := flags.String("android-isolation-profile", "instrumented-android", "Android target isolation profile")
	androidBaselineState := flags.String("android-baseline-state", "clean-boot", "Android target baseline state")
	androidGuestMemory := flags.Int64("android-guest-memory-bytes", 2<<30, "Android guest physical RAM in bytes (1536-8192 MiB, exact MiB alignment)")
	androidBootTimeout := flags.Duration("android-boot-timeout", 2*time.Minute, "Android target boot timeout")
	workspaceDriver := flags.String("workspace-driver", "directory", "workspace driver (only directory is currently probeable)")
	observerDriver := flags.String("observer-driver", "none", "observer driver: none or process")
	observerReference := flags.String("observer-reference", "", "deployment observer reference for an exact process observer")
	observerAdapter := flags.String("observer-adapter", "", "process observer adapter name")
	observerVersion := flags.String("observer-version", "", "process observer configuration version")
	observerSignalFamily := flags.String("observer-signal-family", "", "process observer signal family")
	observerPlacement := flags.String("observer-placement", "", "process observer placement")
	observerCoverageLevel := flags.String("observer-coverage-level", "", "process observer concrete coverage level")
	observerRuntimeBinding := flags.String("observer-runtime-binding", observerprocess.RuntimeBindingNone.String(), "typed process observer runtime binding")
	observerProgram := flags.String("observer-program", "", "absolute process observer executable path")
	observerReadinessProgram := flags.String("observer-readiness-program", "", "absolute process observer readiness executable path")
	observerReadinessInterval := flags.Duration("observer-readiness-interval", 0, "process observer readiness polling interval")
	var observerArgs, observerVersionArgs, observerReadinessArgs, observerEnvironment repeatedStringFlag
	flags.Var(&observerArgs, "observer-arg", "trusted process observer argument (repeat in exact order)")
	flags.Var(&observerVersionArgs, "observer-version-arg", "trusted process observer version-probe argument (repeat in exact order)")
	flags.Var(&observerReadinessArgs, "observer-readiness-arg", "trusted process observer readiness argument (repeat in exact order)")
	flags.Var(&observerEnvironment, "observer-environment", "trusted process observer environment entry NAME=VALUE (repeat; names must be unique)")
	captureDriver := flags.String("capture-driver", "none", "capture driver: none or ledger")
	policyPath := flags.String("policy", "", "strict policy YAML to compile against the probed complete capability fingerprint")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if *linuxTargetDriver != "docker" && *linuxTargetDriver != "none" {
		return fmt.Errorf("linux-target-driver must be docker or none")
	}
	if *androidTargetDriver != "android-emulator" && *androidTargetDriver != "none" {
		return fmt.Errorf("android-target-driver must be android-emulator or none")
	}
	androidOptions := androidProbeOptions{
		SDKRoot: *androidSDKRoot, ADBBinary: *androidADBBinary, ADBServer: *androidADBServer,
		EmulatorBinary: *androidEmulatorBinary, SDKManagerBinary: *androidSDKManagerBinary, AVDManagerBinary: *androidAVDManagerBinary,
		BackendVersion: *androidBackendVersion, RuntimeVersion: *androidRuntimeVersion,
		SystemImageDigest: *androidSystemImageDigest, SystemImagePackage: *androidSystemImagePackage,
		IsolationProfile: *androidIsolationProfile, BaselineState: *androidBaselineState,
		BaseConsolePort: *androidBaseConsolePort, GuestMemoryBytes: *androidGuestMemory, BootTimeout: *androidBootTimeout,
	}
	if *androidTargetDriver == "android-emulator" {
		if err := validateAndroidProbeOptions(androidOptions); err != nil {
			return err
		}
	}
	if *workspaceDriver != "directory" {
		return fmt.Errorf("workspace-driver=%q cannot be truthfully probed by this binary", *workspaceDriver)
	}
	if *observerDriver != "none" && *observerDriver != "process" {
		return fmt.Errorf("observer-driver must be none or process")
	}
	observerConfigurationProvided := false
	flags.Visit(func(value *flag.Flag) {
		if strings.HasPrefix(value.Name, "observer-") && value.Name != "observer-driver" {
			observerConfigurationProvided = true
		}
	})
	var configuredObserver *processObserverOptions
	if *observerDriver == "none" {
		if observerConfigurationProvided {
			return fmt.Errorf("observer configuration flags require observer-driver=process")
		}
	} else {
		environment, err := parseObserverEnvironment(observerEnvironment)
		if err != nil {
			return err
		}
		runtimeBinding, err := observerprocess.ParseRuntimeBinding(*observerRuntimeBinding)
		if err != nil {
			return fmt.Errorf("observer-runtime-binding: %w", err)
		}
		options := processObserverOptions{
			Reference: *observerReference,
			Configuration: observerprocess.AdapterConfiguration{
				Adapter: *observerAdapter, Version: *observerVersion, SignalFamily: *observerSignalFamily,
				Placement: domain.CollectorPlacement(*observerPlacement), CoverageLevel: domain.CoverageLevel(*observerCoverageLevel),
				RuntimeBinding: runtimeBinding,
				Program:        *observerProgram, Args: []string(observerArgs), Environment: environment,
				VersionArgs: []string(observerVersionArgs), ReadinessProgram: *observerReadinessProgram,
				ReadinessArgs: []string(observerReadinessArgs), ReadinessInterval: *observerReadinessInterval,
			},
		}
		if err := validateProcessObserverOptions(options); err != nil {
			return err
		}
		configuredObserver = &options
	}
	if *captureDriver != "none" && *captureDriver != "ledger" {
		return fmt.Errorf("capture-driver must be none or ledger")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	agent, err := agentdocker.New(agentdocker.Config{
		Build: agentdocker.BuildConfig{
			WorkspaceRoot: os.TempDir(), ImageRepository: "probe.invalid/world-agent", ContainerUser: *agentContainerUser,
		},
		Engine: agentdocker.NewCLIEngine(*dockerBinary, nil, nil),
	})
	if err != nil {
		return fmt.Errorf("configure agent probe: %w", err)
	}
	agentFingerprint, err := agent.Probe(ctx)
	if err != nil {
		return fmt.Errorf("probe Docker agent driver: %w", err)
	}

	agentReporter, ok := any(agent).(ports.AgentWorkspacePhysicalPolicyReporter)
	if !ok {
		return fmt.Errorf("Docker agent driver does not expose physical policy facts")
	}
	agentPhysical, err := agentReporter.AgentWorkspacePhysicalPolicy(ctx)
	if err != nil {
		return fmt.Errorf("probe Docker agent physical policy: %w", err)
	}
	if *captureDriver == "ledger" {
		agentPhysical = policyauthority.WithBoundedLedgerCaptureEnforcement(agentPhysical)
	}
	agentPhysicalFingerprint, err := policyauthority.AgentPhysicalPolicyFingerprint(agentPhysical)
	if err != nil {
		return fmt.Errorf("fingerprint Docker agent physical policy: %w", err)
	}

	components := []policyauthority.CapabilityComponent{
		{Name: "agent", Fingerprint: agentFingerprint},
		{Name: "agent-physical", Fingerprint: agentPhysicalFingerprint},
	}
	coverage := make(map[string][]string)
	var targetReport *capabilityReport
	var targetPhysicalReport *ports.TargetPhysicalPolicyReport
	if *linuxTargetDriver == "docker" {
		target, err := linuxcontainer.New(linuxcontainer.Config{
			Build:   linuxcontainer.BuildConfig{TargetRoot: os.TempDir(), ImageRepository: "probe.invalid/world-target", AllowPtrace: *allowPtrace},
			Runtime: linuxcontainer.NewDockerRuntime(*dockerBinary, nil, nil),
			Collectors: linuxcontainer.CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
				return fmt.Errorf("probe-only readiness gate cannot start runs")
			}),
		})
		if err != nil {
			return fmt.Errorf("configure Linux target probe: %w", err)
		}
		template := ports.TargetTemplate{
			Name: "capability-probe", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: "runc",
			ImageDigest: domain.NewDigest([]byte("probe-only-image")), IsolationProfile: "observable-container",
		}
		targetFingerprint, err := target.Probe(ctx, template)
		if err != nil {
			return fmt.Errorf("probe Docker Linux target driver: %w", err)
		}
		components = append(components, policyauthority.CapabilityComponent{Name: "linux-target", Fingerprint: targetFingerprint})
		targetReporter, ok := any(target).(ports.TargetPhysicalPolicyReporter)
		if !ok {
			return fmt.Errorf("Docker Linux target driver does not expose physical policy facts")
		}
		physical, err := targetReporter.TargetPhysicalPolicy(ctx, template)
		if err != nil {
			return fmt.Errorf("probe Docker Linux target physical policy: %w", err)
		}
		targetPhysicalFingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(physical)
		if err != nil {
			return fmt.Errorf("fingerprint Docker Linux target physical policy: %w", err)
		}
		components = append(components, policyauthority.CapabilityComponent{Name: "linux-target-physical", Fingerprint: targetPhysicalFingerprint})
		coverage["linux-container"] = []string{ports.TargetLifecycleSignal}
		mapped := mapCapabilityReport(targetFingerprint)
		targetReport = &mapped
		targetPhysicalReport = &physical
	}
	var androidReport *capabilityReport
	var androidPhysicalReport *ports.TargetPhysicalPolicyReport
	if *androidTargetDriver == "android-emulator" {
		fingerprint, physical, err := probeManagedAndroid(ctx, androidOptions)
		if err != nil {
			return err
		}
		physicalFingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(physical)
		if err != nil {
			return fmt.Errorf("fingerprint managed Android target physical policy: %w", err)
		}
		components = append(components,
			policyauthority.CapabilityComponent{Name: "android-target", Fingerprint: fingerprint},
			policyauthority.CapabilityComponent{Name: "android-target-physical", Fingerprint: physicalFingerprint},
		)
		coverage["android-virtual-device"] = []string{ports.TargetLifecycleSignal}
		mapped := mapCapabilityReport(fingerprint)
		androidReport = &mapped
		androidPhysicalReport = &physical
	}
	var configuredObserverReport *processObserverReport
	if configuredObserver != nil {
		fingerprint, configurationDigest, err := probeProcessObserver(ctx, *configuredObserver, nil)
		if err != nil {
			return err
		}
		components = append(components, policyauthority.CapabilityComponent{
			Name: "observer." + configuredObserver.Reference, Fingerprint: fingerprint,
			Adapter: configuredObserver.Configuration.Adapter,
		})
		configuredObserverReport = &processObserverReport{
			Reference: configuredObserver.Reference, Adapter: configuredObserver.Configuration.Adapter,
			Version: configuredObserver.Configuration.Version, RuntimeBinding: configuredObserver.Configuration.RuntimeBinding.String(), ConfigurationDigest: configurationDigest.String(),
			Capability: mapCapabilityReport(fingerprint),
		}
	}
	combined, err := policyauthority.BuildCapabilityFingerprint(policyauthority.CapabilityFacts{
		HostOS: runtime.GOOS, HostArchitecture: runtime.GOARCH, WorkspaceMode: "directory-copy-non-production",
		DirectoryCopy: true, Components: components, IntrinsicCoverage: coverage,
	})
	if err != nil {
		return fmt.Errorf("compose complete capability fingerprint: %w", err)
	}
	var effectivePolicy *effectivePolicyReport
	if *policyPath != "" {
		source, err := readPolicySource(*policyPath)
		if err != nil {
			return err
		}
		compiled, err := policy.Compile(source, policy.CompileOptions{Capabilities: combined})
		if err != nil {
			return fmt.Errorf("compile policy against probed capabilities: %w", err)
		}
		document := compiled.Policy()
		effectivePolicy = &effectivePolicyReport{
			Reference: fmt.Sprintf("%s@%d", document.Metadata.Name, document.Metadata.Revision),
			Digest:    compiled.Digest().String(), CapabilityFingerprintDigest: compiled.CapabilityFingerprintDigest().String(),
		}
	}

	encoded, err := json.MarshalIndent(report{
		Combined: mapCapabilityReport(combined), Agent: mapCapabilityReport(agentFingerprint), AgentPhysical: agentPhysical,
		LinuxTarget: targetReport, LinuxTargetPhysical: targetPhysicalReport,
		AndroidTarget: androidReport, AndroidTargetPhysical: androidPhysicalReport,
		ProcessObserver: configuredObserverReport, EffectivePolicy: effectivePolicy,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode capability report: %w", err)
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}

func parseObserverEnvironment(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	environment := make(map[string]string, len(entries))
	for index, entry := range entries {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return nil, fmt.Errorf("observer-environment[%d] must use NAME=VALUE", index)
		}
		if _, duplicate := environment[name]; duplicate {
			return nil, fmt.Errorf("observer-environment duplicates name %q", name)
		}
		environment[name] = value
	}
	return environment, nil
}

func validateProcessObserverOptions(options processObserverOptions) error {
	if strings.TrimSpace(options.Reference) == "" || options.Reference != strings.TrimSpace(options.Reference) {
		return fmt.Errorf("observer-reference must be non-blank and trimmed when observer-driver=process")
	}
	if err := ports.ValidateCollectorName(options.Reference); err != nil {
		return fmt.Errorf("observer-reference: %w", err)
	}
	if !filepath.IsAbs(options.Configuration.Program) || !filepath.IsAbs(options.Configuration.ReadinessProgram) {
		return fmt.Errorf("observer-program and observer-readiness-program must be absolute paths")
	}
	if err := options.Configuration.Validate(); err != nil {
		return fmt.Errorf("process observer configuration: %w", err)
	}
	return nil
}

func probeProcessObserver(ctx context.Context, options processObserverOptions, runner command.Runner) (fingerprint domain.CapabilityFingerprint, configurationDigest domain.Digest, resultErr error) {
	adapter, err := observerprocess.BuildAdapter(options.Configuration)
	if err != nil {
		return fingerprint, configurationDigest, fmt.Errorf("configure process observer probe: %w", err)
	}
	probeRoot, err := os.MkdirTemp("", "world-observer-capability-")
	if err != nil {
		return fingerprint, configurationDigest, fmt.Errorf("create process observer probe output authority: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(probeRoot)) }()
	outputs, err := observerprocess.NewLocalOutputFactory(observerprocess.LocalOutputConfig{Root: probeRoot})
	if err != nil {
		return fingerprint, configurationDigest, fmt.Errorf("configure process observer probe output authority: %w", err)
	}
	driver, err := observerprocess.New(observerprocess.Config{Runner: runner, Adapters: []observerprocess.Adapter{adapter}, Outputs: outputs})
	if err != nil {
		return fingerprint, configurationDigest, fmt.Errorf("configure process observer probe: %w", err)
	}
	requirement := ports.ObservationRequirement{
		SignalFamily: options.Configuration.SignalFamily, Placement: options.Configuration.Placement,
		MinimumLevel: options.Configuration.CoverageLevel, Required: true,
	}
	fingerprint, err = driver.Probe(ctx, requirement)
	if err != nil {
		return fingerprint, configurationDigest, fmt.Errorf("probe process observer adapter %q: %w", options.Reference, err)
	}
	return fingerprint, adapter.ConfigurationDigest, nil
}

func validateAndroidProbeOptions(options androidProbeOptions) error {
	for name, value := range map[string]string{
		"android-sdk-root": options.SDKRoot, "android-adb-binary": options.ADBBinary,
		"android-adb-server": options.ADBServer, "android-emulator-binary": options.EmulatorBinary,
		"android-sdkmanager-binary": options.SDKManagerBinary, "android-avdmanager-binary": options.AVDManagerBinary,
		"android-backend-version": options.BackendVersion, "android-runtime-version": options.RuntimeVersion,
		"android-system-image-digest": options.SystemImageDigest, "android-system-image-package": options.SystemImagePackage,
		"android-isolation-profile": options.IsolationProfile, "android-baseline-state": options.BaselineState,
	} {
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("%s must be non-blank and trimmed when android-target-driver=android-emulator", name)
		}
	}
	if !filepath.IsAbs(options.SDKRoot) {
		return fmt.Errorf("android-sdk-root must be absolute")
	}
	if _, err := domain.ParseDigest(options.SystemImageDigest); err != nil {
		return fmt.Errorf("android-system-image-digest: %w", err)
	}
	if err := cuttlefish.ValidateManagedSystemImagePackage(options.SystemImagePackage); err != nil {
		return fmt.Errorf("android-system-image-package: %w", err)
	}
	if options.BootTimeout <= 0 {
		return fmt.Errorf("android-boot-timeout must be positive")
	}
	if err := androidcontract.ValidateGuestMemoryBytes(options.GuestMemoryBytes); err != nil {
		return fmt.Errorf("android-guest-memory-bytes: %w", err)
	}
	if options.BaselineState != ports.AndroidBaselineCleanBoot {
		return fmt.Errorf("android-baseline-state must be %q", ports.AndroidBaselineCleanBoot)
	}
	// The actual emulator identity is resolved by the managed backend. Use an
	// absolute validation sentinel here so relative PATH configuration remains
	// valid without fingerprinting that unresolved spelling.
	validationEmulator := filepath.Join(options.SDKRoot, "emulator", "emulator")
	_, err := cuttlefish.ManagedEmulatorDeviceConfigDigest(managedAndroidDeviceIdentityWithEmulator(options, validationEmulator))
	return err
}

func managedAndroidDeviceIdentity(options androidProbeOptions) cuttlefish.ManagedEmulatorDeviceConfigIdentity {
	return managedAndroidDeviceIdentityWithEmulator(options, options.EmulatorBinary)
}

func managedAndroidDeviceIdentityWithEmulator(options androidProbeOptions, emulatorIdentity string) cuttlefish.ManagedEmulatorDeviceConfigIdentity {
	return cuttlefish.ManagedEmulatorDeviceConfigIdentity{
		EmulatorBinary: emulatorIdentity, ADBBinary: options.ADBBinary,
		SDKManagerBinary: options.SDKManagerBinary, AVDManagerBinary: options.AVDManagerBinary,
		SDKRoot: options.SDKRoot, ADBServerEndpoint: options.ADBServer,
		ExpectedBackendVersion: options.BackendVersion, ExpectedRuntimeVersion: options.RuntimeVersion,
		BaseConsolePort: options.BaseConsolePort, LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort,
		SystemImages: map[string]cuttlefish.ManagedSystemImage{
			options.SystemImageDigest: {Package: options.SystemImagePackage},
		},
	}
}

func probeManagedAndroid(ctx context.Context, options androidProbeOptions) (fingerprint domain.CapabilityFingerprint, physical ports.TargetPhysicalPolicyReport, resultErr error) {
	probeRoot, err := os.MkdirTemp("", "world-android-capability-")
	if err != nil {
		return fingerprint, physical, fmt.Errorf("create managed Android probe state: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(probeRoot)) }()
	digest, err := domain.ParseDigest(options.SystemImageDigest)
	if err != nil {
		return fingerprint, physical, fmt.Errorf("parse managed Android system-image digest: %w", err)
	}
	images := managedAndroidDeviceIdentity(options).SystemImages
	backend, err := cuttlefish.NewManagedEmulatorBackend(cuttlefish.ManagedEmulatorBackendConfig{
		EmulatorBinary: options.EmulatorBinary, ADBBinary: options.ADBBinary, ADBServerEndpoint: options.ADBServer,
		SDKManagerBinary: options.SDKManagerBinary, AVDManagerBinary: options.AVDManagerBinary,
		SDKRoot: options.SDKRoot, StateRoot: filepath.Join(probeRoot, "emulators"), SystemImages: images,
	})
	if err != nil {
		return fingerprint, physical, fmt.Errorf("configure managed Android backend probe: %w", err)
	}
	identity := managedAndroidDeviceIdentityWithEmulator(options, backend.EmulatorExecutableIdentity())
	deviceConfigDigest, err := cuttlefish.ManagedEmulatorDeviceConfigDigest(identity)
	if err != nil {
		return fingerprint, physical, fmt.Errorf("fingerprint managed Android probe configuration: %w", err)
	}
	allocator, err := cuttlefish.NewDurableEmulatorAllocator(cuttlefish.DurableEmulatorAllocatorConfig{
		StateRoot: filepath.Join(probeRoot, "allocations"), FirstConsolePort: options.BaseConsolePort,
		LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort, ListenHost: "127.0.0.1",
	})
	if err != nil {
		return fingerprint, physical, fmt.Errorf("configure managed Android allocator probe: %w", err)
	}
	closeAllocator := true
	defer func() {
		if closeAllocator {
			resultErr = errors.Join(resultErr, allocator.Close())
		}
	}()
	gateway, err := cuttlefish.NewDeviceProxyGateway(deviceproxy.GatewayConfig{UpstreamAddress: options.ADBServer})
	if err != nil {
		return fingerprint, physical, fmt.Errorf("configure managed Android scoped ADB probe: %w", err)
	}
	files, err := cuttlefish.NewCommandFileGateway(cuttlefish.CommandFileGatewayConfig{
		ADBBinary: options.ADBBinary, ADBServerEndpoint: options.ADBServer, StagingRoot: filepath.Join(probeRoot, "adb-staging"),
	})
	if err != nil {
		return fingerprint, physical, fmt.Errorf("configure managed Android file probe: %w", err)
	}
	driver, err := cuttlefish.New(cuttlefish.Config{
		Build: cuttlefish.BuildConfig{
			TargetRoot: filepath.Join(probeRoot, "targets"), SystemImageRoot: filepath.Join(options.SDKRoot, "system-images"),
			ADBServerEndpoint: options.ADBServer,
			BackendVersion:    options.BackendVersion, RuntimeVersion: options.RuntimeVersion,
			DeviceConfigDigest: deviceConfigDigest,
			Features:           []string{"adb-scoped-gateway", "guest-data-partition", "hardware-acceleration", "headless", "rooted"},
		},
		Backend: backend, Allocator: allocator, Gateway: gateway, Files: files,
		Collectors: cuttlefish.CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
			return fmt.Errorf("probe-only readiness gate cannot start runs")
		}),
	})
	if err != nil {
		return fingerprint, physical, fmt.Errorf("configure managed Android target probe: %w", err)
	}
	closeAllocator = false
	defer func() { resultErr = errors.Join(resultErr, driver.Close()) }()
	template := ports.TargetTemplate{
		Name: "android-capability-probe", Kind: domain.TargetAndroidVirtualDevice, Driver: "android-emulator",
		ImageDigest: digest, IsolationProfile: options.IsolationProfile, BaselineState: options.BaselineState,
		RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true,
		GuestMemoryBytes: options.GuestMemoryBytes, BootTimeout: options.BootTimeout,
	}
	fingerprint, err = driver.Probe(ctx, template)
	if err != nil {
		return fingerprint, physical, wrapProbeError("probe managed Android target driver", err)
	}
	physical, err = driver.TargetPhysicalPolicy(ctx, template)
	if err != nil {
		return fingerprint, physical, wrapProbeError("probe managed Android target physical policy", err)
	}
	return fingerprint, physical, nil
}

func wrapProbeError(operation string, err error) error {
	if cause := errors.Unwrap(err); cause != nil {
		return fmt.Errorf("%s: %w: %v", operation, err, cause)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func readPolicySource(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect policy: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("policy must be a regular file, not a symlink or special file")
	}
	opened, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open policy: %w", err)
	}
	defer opened.Close()
	openedInfo, err := opened.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened policy: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("policy changed while it was opened")
	}
	source, err := io.ReadAll(io.LimitReader(opened, maximumPolicyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read policy: %w", err)
	}
	if len(source) == 0 || int64(len(source)) > maximumPolicyBytes {
		return nil, fmt.Errorf("policy must be non-empty and no larger than %d bytes", maximumPolicyBytes)
	}
	return source, nil
}

func mapCapabilityReport(fingerprint domain.CapabilityFingerprint) capabilityReport {
	capabilities := fingerprint.Capabilities()
	names := make([]string, 0, len(capabilities))
	for name := range capabilities {
		names = append(names, name)
	}
	sort.Strings(names)
	mapped := make(map[string]capabilityDetail, len(names))
	for _, name := range names {
		capability := capabilities[name]
		mapped[name] = capabilityDetail{
			Status: string(capability.Status()), Constraints: capability.Constraints(), Evidence: capability.Evidence(),
		}
	}
	return capabilityReport{Digest: fingerprint.Digest().String(), Evidence: fingerprint.Evidence(), Capabilities: mapped}
}
