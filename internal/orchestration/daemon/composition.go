package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	agentdocker "github.com/philcantcode/go-world-management-layer/internal/drivers/agent/docker"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/linuxcontainer"
	workspacedirectory "github.com/philcantcode/go-world-management-layer/internal/drivers/workspace/directory"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type hostComposition struct {
	agent                 ports.AgentWorkspaceDriver
	targets               map[domain.TargetKind]ports.TargetDriver
	workspace             ports.WorkspaceDriver
	material              ports.MaterialAuthority
	captures              orchestration.CaptureController
	resolver              orchestration.ProvisioningResolver
	profileDigest         domain.Digest
	observers             *orchestration.RunObserverCoordinator
	policyAdmission       orchestration.PolicyAdmissionConfig
	capabilityFingerprint domain.CapabilityFingerprint
	close                 func() error
}

type agentDriverConfig struct {
	DockerBinary    string
	WorkspaceRoot   string
	ImageRepository string
	GuestBinary     string
	ContainerUser   string
}

type linuxTargetDriverConfig struct {
	DockerBinary    string
	TargetRoot      string
	ImageRepository string
	AllowPtrace     bool
}

type androidTargetDriverConfig struct {
	TargetRoot           string
	SystemImageRoot      string
	SDKRoot              string
	ADBBinary            string
	ADBServer            string
	EmulatorBinary       string
	SDKManagerBinary     string
	AVDManagerBinary     string
	BackendVersion       string
	RuntimeVersion       string
	FirstConsolePort     int
	MaximumTransferBytes int64
	MaximumADBBytes      int64
	ShutdownTimeout      time.Duration
	SystemImages         map[string]string
}

type observerDriverConfig struct {
	Adapters   []observerprocess.Adapter
	OutputRoot string
}

type compositionFactories struct {
	newWorkspace func(string) (ports.WorkspaceDriver, error)
	newAgent     func(agentDriverConfig) (ports.AgentWorkspaceDriver, error)
	newTarget    func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error)
	newAndroid   func(androidTargetDriverConfig, cuttlefish.CollectorReadiness) (ports.TargetDriver, error)
	newObserver  func(observerDriverConfig) (ports.ObserverDriver, error)
	verifyImage  func(context.Context, string, string) error
}

func productionCompositionFactories() compositionFactories {
	return compositionFactories{
		newWorkspace: func(root string) (ports.WorkspaceDriver, error) {
			return workspacedirectory.New(workspacedirectory.Config{Root: root})
		},
		newAgent: func(config agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
			engine := agentdocker.NewCLIEngine(config.DockerBinary, nil, nil)
			return agentdocker.New(agentdocker.Config{
				Build: agentdocker.BuildConfig{
					WorkspaceRoot: config.WorkspaceRoot, ImageRepository: config.ImageRepository,
					GuestBinary: config.GuestBinary, ContainerUser: config.ContainerUser,
				},
				Engine: engine,
			})
		},
		newTarget: func(config linuxTargetDriverConfig, readiness linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			containerRuntime := linuxcontainer.NewDockerRuntime(config.DockerBinary, nil, nil)
			return linuxcontainer.New(linuxcontainer.Config{
				Build: linuxcontainer.BuildConfig{
					TargetRoot: config.TargetRoot, ImageRepository: config.ImageRepository,
					AllowPtrace: config.AllowPtrace,
				},
				Runtime: containerRuntime, Collectors: readiness,
			})
		},
		newAndroid: newManagedAndroidTargetDriver,
		newObserver: func(config observerDriverConfig) (ports.ObserverDriver, error) {
			outputs, err := observerprocess.NewLocalOutputFactory(observerprocess.LocalOutputConfig{Root: config.OutputRoot})
			if err != nil {
				return nil, err
			}
			return observerprocess.New(observerprocess.Config{Adapters: config.Adapters, Outputs: outputs})
		},
		verifyImage: verifyDockerImage,
	}
}

func newManagedAndroidTargetDriver(config androidTargetDriverConfig, readiness cuttlefish.CollectorReadiness) (_ ports.TargetDriver, resultErr error) {
	images := managedAndroidSystemImages(config.SystemImages)
	backend, err := cuttlefish.NewManagedEmulatorBackend(cuttlefish.ManagedEmulatorBackendConfig{
		EmulatorBinary: config.EmulatorBinary, ADBBinary: config.ADBBinary, ADBServerEndpoint: config.ADBServer,
		SDKManagerBinary: config.SDKManagerBinary, AVDManagerBinary: config.AVDManagerBinary,
		SDKRoot: config.SDKRoot, StateRoot: config.TargetRoot, SystemImages: images,
		ShutdownTimeout: config.ShutdownTimeout,
	})
	if err != nil {
		return nil, err
	}
	config.EmulatorBinary = backend.EmulatorExecutableIdentity()
	allocator, err := cuttlefish.NewDurableEmulatorAllocator(cuttlefish.DurableEmulatorAllocatorConfig{
		StateRoot: filepath.Join(config.TargetRoot, "allocations"), FirstConsolePort: config.FirstConsolePort,
		LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort, ListenHost: "127.0.0.1",
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = closeResourceOnConstructionFailure(resultErr, allocator, "close durable Android emulator allocator")
	}()
	gateway, err := cuttlefish.NewDeviceProxyGateway(deviceproxy.GatewayConfig{
		UpstreamAddress: config.ADBServer, MaximumConnectionDuration: maximumConfiguredRunWindow,
		MaximumStreamBytes: config.MaximumADBBytes,
	})
	if err != nil {
		return nil, err
	}
	files, err := cuttlefish.NewCommandFileGateway(cuttlefish.CommandFileGatewayConfig{
		ADBBinary: config.ADBBinary, ADBServerEndpoint: config.ADBServer, StagingRoot: filepath.Join(config.TargetRoot, "adb-staging"),
		MaximumTransferBytes: config.MaximumTransferBytes,
	})
	if err != nil {
		return nil, err
	}
	deviceConfigDigest, err := androidDeviceConfigDigest(config)
	if err != nil {
		return nil, err
	}
	driver, err := cuttlefish.New(cuttlefish.Config{
		Build: cuttlefish.BuildConfig{
			TargetRoot: config.TargetRoot, SystemImageRoot: config.SystemImageRoot,
			ADBServerEndpoint: config.ADBServer,
			BackendVersion:    config.BackendVersion, RuntimeVersion: config.RuntimeVersion,
			DeviceConfigDigest: deviceConfigDigest,
			Features:           []string{"adb-scoped-gateway", "guest-data-partition", "hardware-acceleration", "headless", "rooted"},
		},
		Backend: backend, Allocator: allocator, Gateway: gateway, Files: files, Collectors: readiness,
	})
	if err != nil {
		return nil, err
	}
	return driver, nil
}

func closeResourceOnConstructionFailure(constructionErr error, resource io.Closer, description string) error {
	if constructionErr == nil || resource == nil {
		return constructionErr
	}
	if closeErr := resource.Close(); closeErr != nil {
		return errors.Join(constructionErr, fmt.Errorf("%s: %w", description, closeErr))
	}
	return constructionErr
}

func managedAndroidSystemImages(configured map[string]string) map[string]cuttlefish.ManagedSystemImage {
	images := make(map[string]cuttlefish.ManagedSystemImage, len(configured))
	for digest, packageID := range configured {
		images[digest] = cuttlefish.ManagedSystemImage{Package: packageID}
	}
	return images
}

func androidDeviceConfigDigest(config androidTargetDriverConfig) (domain.Digest, error) {
	digest, err := cuttlefish.ManagedEmulatorDeviceConfigDigest(cuttlefish.ManagedEmulatorDeviceConfigIdentity{
		EmulatorBinary: config.EmulatorBinary, ADBBinary: config.ADBBinary,
		SDKManagerBinary: config.SDKManagerBinary, AVDManagerBinary: config.AVDManagerBinary,
		SDKRoot: config.SDKRoot, ADBServerEndpoint: config.ADBServer,
		ExpectedBackendVersion: config.BackendVersion, ExpectedRuntimeVersion: config.RuntimeVersion,
		BaseConsolePort: config.FirstConsolePort, LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort, SystemImages: managedAndroidSystemImages(config.SystemImages),
	})
	if err != nil {
		return domain.Digest{}, fmt.Errorf("fingerprint managed Android device configuration: %w", err)
	}
	return digest, nil
}

func configureHostDrivers(ctx context.Context, configuration config, observations *ledger.Ledger, policies *policyauthority.Authority) (hostComposition, error) {
	return configureHostDriversWithFactories(ctx, configuration, observations, policies, productionCompositionFactories())
}

func configureHostDriversWithFactories(ctx context.Context, configuration config, observations *ledger.Ledger, policies *policyauthority.Authority, factories compositionFactories) (composition hostComposition, resultErr error) {
	ownedResources := make([]io.Closer, 0, 1)
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, closeCompositionResources(ownedResources))
		}
	}()
	if configuration.materialDriver != "local" {
		return hostComposition{}, unavailableDriver("material-driver", configuration.materialDriver, "this binary supports the local scope-bound material authority")
	}
	if configuration.physicalTargetDriver != "none" {
		return hostComposition{}, unavailableDriver("physical-target-driver", configuration.physicalTargetDriver, "physical devices require an injected device authority and containment controller")
	}
	if configuration.agentDriver == "none" {
		if configuration.captureDriver != "none" {
			return hostComposition{}, fmt.Errorf("capture-driver=ledger requires the physical local material composition")
		}
		if configuration.deploymentProfile != "" {
			return hostComposition{}, fmt.Errorf("deployment-profile is configured but all physical drivers are disabled")
		}
		return hostComposition{targets: make(map[domain.TargetKind]ports.TargetDriver)}, nil
	}
	if policies == nil {
		return hostComposition{}, fmt.Errorf("physical composition requires a durable effective-policy authority")
	}
	if factories.newWorkspace == nil || factories.newAgent == nil || factories.verifyImage == nil {
		return hostComposition{}, fmt.Errorf("physical composition factory is incomplete")
	}
	if configuration.linuxTargetDriver == "docker" && factories.newTarget == nil {
		return hostComposition{}, fmt.Errorf("Linux target composition factory is incomplete")
	}
	if configuration.androidTargetDriver == "android-emulator" && factories.newAndroid == nil {
		return hostComposition{}, fmt.Errorf("Android target composition factory is incomplete")
	}
	loadCtx, cancel := context.WithTimeout(ctx, configuration.probeTimeout)
	deployment, err := loadDeployment(loadCtx, configuration.deploymentProfile, configuration.materialRoot, configuration.maxBundleBytes)
	cancel()
	if err != nil {
		return hostComposition{}, fmt.Errorf("load deployment profile: %w", err)
	}
	hasExternalObservers := len(deployment.observerAdapters) > 0
	if hasExternalObservers && configuration.observerDriver != "process" {
		return hostComposition{}, fmt.Errorf("deployment profile configures external observers but observer-driver=process is not enabled")
	}
	if !hasExternalObservers && configuration.observerDriver == "process" {
		return hostComposition{}, fmt.Errorf("observer-driver=process requires at least one observer in the deployment profile")
	}
	if hasExternalObservers && factories.newObserver == nil {
		return hostComposition{}, fmt.Errorf("physical observer composition factory is incomplete")
	}
	if configuration.agentImageRepository != "" && configuration.agentImageRepository != deployment.agentRepository {
		return hostComposition{}, fmt.Errorf("agent-image-repository %q does not match deployment profile repository %q", configuration.agentImageRepository, deployment.agentRepository)
	}
	if configuration.linuxTargetDriver == "docker" {
		if len(deployment.linuxTargets) == 0 || deployment.targetRepository == "" {
			return hostComposition{}, fmt.Errorf("linux-target-driver=docker requires target templates in the deployment profile")
		}
		if configuration.targetImageRepository != "" && configuration.targetImageRepository != deployment.targetRepository {
			return hostComposition{}, fmt.Errorf("target-image-repository %q does not match deployment profile repository %q", configuration.targetImageRepository, deployment.targetRepository)
		}
	} else if len(deployment.linuxTargets) != 0 {
		return hostComposition{}, fmt.Errorf("deployment profile contains Linux target plans while linux-target-driver is disabled")
	}
	if configuration.androidTargetDriver != "none" {
		if len(deployment.androidTargets) == 0 {
			return hostComposition{}, fmt.Errorf("android-target-driver requires Android target templates in the deployment profile")
		}
		for _, template := range deployment.androidTargets {
			if template.Driver != configuration.androidTargetDriver {
				return hostComposition{}, fmt.Errorf("Android target template %q selects driver %q but android-target-driver=%s", template.Name, template.Driver, configuration.androidTargetDriver)
			}
		}
	} else if len(deployment.androidTargets) != 0 {
		return hostComposition{}, fmt.Errorf("deployment profile contains Android target plans while android-target-driver is disabled")
	}
	if len(deployment.targetTemplates) == 0 && deployment.runCount != 0 {
		return hostComposition{}, fmt.Errorf("deployment profile contains run plans without an enabled target driver")
	}
	if err := validatePhysicalRootSeparation(configuration, deployment.sourceRoot); err != nil {
		return hostComposition{}, err
	}
	workspace, err := factories.newWorkspace(configuration.agentWorkspaceRoot)
	if err != nil {
		return hostComposition{}, fmt.Errorf("open directory workspace driver: %w", err)
	}
	agent, err := factories.newAgent(agentDriverConfig{
		DockerBinary: configuration.dockerBinary, WorkspaceRoot: configuration.agentWorkspaceRoot,
		ImageRepository: deployment.agentRepository, GuestBinary: configuration.agentGuestBinary,
		ContainerUser: configuration.agentContainerUser,
	})
	if err != nil {
		return hostComposition{}, fmt.Errorf("open Docker agent driver: %w", err)
	}
	var agentFingerprint domain.CapabilityFingerprint
	if err := runProbe(ctx, configuration.probeTimeout, "agent Docker driver", func(probeCtx context.Context) error {
		fingerprint, err := agent.Probe(probeCtx)
		if err != nil {
			return err
		}
		if err := requireProbeCapabilities(fingerprint, "agent.docker", "agent.hardened-isolation"); err != nil {
			return err
		}
		agentFingerprint = fingerprint
		return nil
	}); err != nil {
		return hostComposition{}, err
	}
	rawAgentReporter, ok := agent.(ports.AgentWorkspacePhysicalPolicyReporter)
	if !ok {
		return hostComposition{}, fmt.Errorf("agent driver does not expose physical policy facts")
	}
	agentReporter, err := policyauthority.NewAgentPhysicalPolicyReporter(rawAgentReporter, policyauthority.AgentPhysicalEnforcement{
		BoundedLedgerCapture: configuration.captureDriver == "ledger",
	})
	if err != nil {
		return hostComposition{}, fmt.Errorf("compose agent physical policy reporter: %w", err)
	}
	var agentPhysical ports.AgentWorkspacePhysicalPolicyReport
	if err := runProbe(ctx, configuration.probeTimeout, "agent physical policy", func(probeCtx context.Context) error {
		var reportErr error
		agentPhysical, reportErr = agentReporter.AgentWorkspacePhysicalPolicy(probeCtx)
		return reportErr
	}); err != nil {
		return hostComposition{}, err
	}
	agentPhysicalFingerprint, err := policyauthority.AgentPhysicalPolicyFingerprint(agentPhysical)
	if err != nil {
		return hostComposition{}, fmt.Errorf("fingerprint agent physical policy: %w", err)
	}
	for _, reference := range deployment.imageReferences {
		imageReference := reference
		if err := runProbe(ctx, configuration.probeTimeout, "Docker image "+imageReference, func(probeCtx context.Context) error {
			return factories.verifyImage(probeCtx, configuration.dockerBinary, imageReference)
		}); err != nil {
			return hostComposition{}, err
		}
	}
	var observerDriver ports.ObserverDriver
	capabilityComponents := []policyauthority.CapabilityComponent{
		{Name: "agent", Fingerprint: agentFingerprint}, {Name: "agent-physical", Fingerprint: agentPhysicalFingerprint},
	}
	if hasExternalObservers {
		adapters := make([]observerprocess.Adapter, len(deployment.observerAdapters))
		for index, configured := range deployment.observerAdapters {
			adapters[index] = configured.Adapter
		}
		observerDriver, err = factories.newObserver(observerDriverConfig{Adapters: adapters, OutputRoot: configuration.observerOutputRoot})
		if err != nil {
			return hostComposition{}, fmt.Errorf("open process observer driver: %w", err)
		}
		for _, expected := range deployment.observerAdapters {
			configured := expected
			if err := runProbe(ctx, configuration.probeTimeout, "observer adapter "+configured.Reference, func(probeCtx context.Context) error {
				fingerprint, err := observerDriver.Probe(probeCtx, configured.Spec.Requirement)
				if err != nil {
					return err
				}
				if err := requireObserverProbe(fingerprint, configured.Adapter.Name); err != nil {
					return err
				}
				capabilityComponents = append(capabilityComponents, policyauthority.CapabilityComponent{
					Name: "observer." + configured.Reference, Fingerprint: fingerprint, Adapter: configured.Adapter.Name,
				})
				return nil
			}); err != nil {
				return hostComposition{}, err
			}
		}
	}
	coordinator, err := orchestration.NewRunObserverCoordinator(runObserverCoordinatorConfig(configuration, observerDriver, observations))
	if err != nil {
		return hostComposition{}, fmt.Errorf("open run observer coordinator: %w", err)
	}
	targets := make(map[domain.TargetKind]ports.TargetDriver)
	targetReporters := make(map[domain.TargetKind]ports.TargetPhysicalPolicyReporter)
	targetPhysicalReports := make(map[domain.TargetKind]map[string]ports.TargetPhysicalPolicyReport)
	if configuration.linuxTargetDriver == "docker" {
		readiness, err := orchestration.NewLedgerCollectorReadiness(observations)
		if err != nil {
			return hostComposition{}, fmt.Errorf("open collector readiness gate: %w", err)
		}
		target, err := factories.newTarget(linuxTargetDriverConfig{
			DockerBinary: configuration.dockerBinary, TargetRoot: configuration.targetRoot,
			ImageRepository: deployment.targetRepository, AllowPtrace: configuration.targetAllowPtrace,
		}, readiness)
		if err != nil {
			return hostComposition{}, fmt.Errorf("open Docker Linux target driver: %w", err)
		}
		targetReporter, ok := target.(ports.TargetPhysicalPolicyReporter)
		if !ok {
			return hostComposition{}, fmt.Errorf("Linux target driver does not expose physical policy facts")
		}
		targetFingerprint, targetPhysicalFingerprint, reports, err := probeTargetTemplates(ctx, configuration.probeTimeout, deployment, targetProbeSpec{
			Label: "Linux target", Kind: domain.TargetLinuxContainer, Templates: deployment.linuxTargets,
			Capabilities: []string{"target.linux-container", "target.visibility-first"}, OperatingSystem: "linux",
		}, target, targetReporter)
		if err != nil {
			return hostComposition{}, err
		}
		targetPhysicalReports[domain.TargetLinuxContainer] = reports
		targets[domain.TargetLinuxContainer] = target
		targetReporters[domain.TargetLinuxContainer] = targetReporter
		capabilityComponents = append(capabilityComponents, policyauthority.CapabilityComponent{Name: "linux-target", Fingerprint: targetFingerprint})
		capabilityComponents = append(capabilityComponents, policyauthority.CapabilityComponent{Name: "linux-target-physical", Fingerprint: targetPhysicalFingerprint})
	}
	if configuration.androidTargetDriver == "android-emulator" {
		readiness, err := orchestration.NewLedgerCollectorReadiness(observations)
		if err != nil {
			return hostComposition{}, fmt.Errorf("open Android collector readiness gate: %w", err)
		}
		target, err := factories.newAndroid(androidTargetDriverConfig{
			TargetRoot: configuration.androidTargetRoot, SystemImageRoot: configuration.androidSystemImageRoot,
			SDKRoot: configuration.androidSDKRoot, ADBBinary: configuration.androidADBBinary,
			ADBServer: configuration.androidADBServer, EmulatorBinary: configuration.androidEmulatorBinary,
			SDKManagerBinary: configuration.androidSDKManagerBinary, AVDManagerBinary: configuration.androidAVDManagerBinary,
			BackendVersion: configuration.androidBackendVersion, RuntimeVersion: configuration.androidRuntimeVersion,
			FirstConsolePort: configuration.androidADBBasePort, MaximumTransferBytes: configuration.maxTransferBytes,
			MaximumADBBytes: configuration.maxADBBytes, ShutdownTimeout: configuration.controlTimeout,
			SystemImages: deployment.androidImages,
		}, readiness)
		if err != nil {
			return hostComposition{}, fmt.Errorf("open managed Android target driver: %w", err)
		}
		closer, ok := target.(io.Closer)
		if !ok {
			return hostComposition{}, fmt.Errorf("managed Android target driver does not expose durable allocator shutdown")
		}
		ownedResources = append(ownedResources, closer)
		if _, ok := target.(ports.TargetReconciler); !ok {
			return hostComposition{}, fmt.Errorf("managed Android target driver does not expose authoritative reconciliation")
		}
		if _, ok := target.(ports.TargetRunCrashReconciler); !ok {
			return hostComposition{}, fmt.Errorf("managed Android target driver does not expose interrupted-run recovery")
		}
		targetReporter, ok := target.(ports.TargetPhysicalPolicyReporter)
		if !ok {
			return hostComposition{}, fmt.Errorf("managed Android target driver does not expose physical policy facts")
		}
		targetFingerprint, targetPhysicalFingerprint, reports, err := probeTargetTemplates(ctx, configuration.probeTimeout, deployment, targetProbeSpec{
			Label: "Android target", Kind: domain.TargetAndroidVirtualDevice, Templates: deployment.androidTargets,
			Capabilities: []string{"target.android-virtual", "target.android-reset", "target.scoped-adb"}, OperatingSystem: "android",
		}, target, targetReporter)
		if err != nil {
			return hostComposition{}, err
		}
		targetPhysicalReports[domain.TargetAndroidVirtualDevice] = reports
		targets[domain.TargetAndroidVirtualDevice] = target
		targetReporters[domain.TargetAndroidVirtualDevice] = targetReporter
		capabilityComponents = append(capabilityComponents,
			policyauthority.CapabilityComponent{Name: "android-target", Fingerprint: targetFingerprint},
			policyauthority.CapabilityComponent{Name: "android-target-physical", Fingerprint: targetPhysicalFingerprint},
		)
	}
	coverage := compositionIntrinsicCoverage(targets)
	combinedFingerprint, err := policyauthority.BuildCapabilityFingerprint(policyauthority.CapabilityFacts{
		HostOS: runtime.GOOS, HostArchitecture: runtime.GOARCH, WorkspaceMode: "directory-copy-non-production",
		DirectoryCopy: true, Components: capabilityComponents, IntrinsicCoverage: coverage,
	})
	if err != nil {
		return hostComposition{}, fmt.Errorf("compose complete capability fingerprint: %w", err)
	}
	compiledPolicies, err := deployment.compileAndBindPolicies(combinedFingerprint)
	if err != nil {
		return hostComposition{}, err
	}
	previewPolicies, err := newCompiledPolicyResolver(compiledPolicies)
	if err != nil {
		return hostComposition{}, err
	}
	preflightConfig := physicalPolicyAdmissionConfig(deployment, previewPolicies, agentPhysical, agentReporter, targetPhysicalReports, targetReporters)
	if err := preflightPhysicalPolicyPlans(ctx, configuration.probeTimeout, deployment, preflightConfig); err != nil {
		return hostComposition{}, fmt.Errorf("preflight published physical policy plans: %w", err)
	}
	if err := publishCompiledPolicies(ctx, policies, combinedFingerprint, compiledPolicies); err != nil {
		return hostComposition{}, err
	}
	admissionConfig := physicalPolicyAdmissionConfig(deployment, policies, agentPhysical, agentReporter, targetPhysicalReports, targetReporters)
	var captures orchestration.CaptureController
	if configuration.captureDriver == "ledger" {
		controller, err := orchestration.NewLedgerCaptureController(orchestration.LedgerCaptureConfig{
			Root: configuration.captureRoot, Ledger: observations, Material: deployment.authority,
			MaxBytes: configuration.maxBundleBytes, MaxRecords: configuration.maxCaptureRecords,
		})
		if err != nil {
			return hostComposition{}, fmt.Errorf("open ledger capture controller: %w", err)
		}
		captures = controller
	}
	return hostComposition{
		agent: agent, targets: targets, workspace: workspace, material: deployment.authority,
		captures: captures, resolver: deployment.resolver, profileDigest: deployment.profileDigest, observers: coordinator,
		policyAdmission: admissionConfig, capabilityFingerprint: combinedFingerprint,
		close: func() error { return closeCompositionResources(ownedResources) },
	}, nil
}

func closeCompositionResources(resources []io.Closer) error {
	errorsFound := make([]error, 0, len(resources))
	for index := len(resources) - 1; index >= 0; index-- {
		if resources[index] != nil {
			errorsFound = append(errorsFound, resources[index].Close())
		}
	}
	return errors.Join(errorsFound...)
}

func runObserverCoordinatorConfig(configuration config, driver ports.ObserverDriver, observations *ledger.Ledger) orchestration.RunObserverCoordinatorConfig {
	return orchestration.RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations,
		StateRoot:      filepath.Join(configuration.orchestrationStateRoot, "run-observers"),
		CleanupTimeout: configuration.controlTimeout,
	}
}

func runProbe(parent context.Context, timeout time.Duration, name string, probe func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := probe(ctx); err != nil {
		return fmt.Errorf("%s startup probe failed: %w", name, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s startup probe did not complete within %s: %w", name, timeout, err)
	}
	return nil
}

func requireProbeCapabilities(fingerprint domain.CapabilityFingerprint, names ...string) error {
	return requireProbeCapabilitiesForOS(fingerprint, "linux", names...)
}

func requireProbeCapabilitiesForOS(fingerprint domain.CapabilityFingerprint, expectedOS string, names ...string) error {
	if fingerprint.Digest().IsZero() {
		return fmt.Errorf("driver returned an empty capability fingerprint")
	}
	for _, name := range names {
		capability, found := fingerprint.Capability(name)
		if !found || capability.Status() != domain.CapabilitySupported {
			return fmt.Errorf("driver does not report required capability %q as supported", name)
		}
	}
	if operatingSystem := strings.ToLower(strings.TrimSpace(fingerprint.Evidence()["os"])); operatingSystem != expectedOS {
		return fmt.Errorf("driver reports operating system %q; %s is required", operatingSystem, expectedOS)
	}
	return nil
}

type targetProbeSpec struct {
	Label           string
	Kind            domain.TargetKind
	Templates       []ports.TargetTemplate
	Capabilities    []string
	OperatingSystem string
}

func probeTargetTemplates(ctx context.Context, timeout time.Duration, deployment builtDeployment, spec targetProbeSpec, target ports.TargetDriver, reporter ports.TargetPhysicalPolicyReporter) (domain.CapabilityFingerprint, domain.CapabilityFingerprint, map[string]ports.TargetPhysicalPolicyReport, error) {
	var capabilityFingerprint domain.CapabilityFingerprint
	var physicalFingerprint domain.CapabilityFingerprint
	reports := make(map[string]ports.TargetPhysicalPolicyReport, len(spec.Templates))
	for _, expected := range spec.Templates {
		configured := expected
		if configured.Kind != spec.Kind {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, fmt.Errorf("%s template %q has kind %q", spec.Label, configured.Name, configured.Kind)
		}
		configuredPlan, found := deployment.static.Targets[configured.Name]
		if !found {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, fmt.Errorf("%s template %q has no authoritative provisioning plan", spec.Label, configured.Name)
		}
		if err := runProbe(ctx, timeout, spec.Label+" template "+configured.Name, func(probeCtx context.Context) error {
			fingerprint, err := target.Probe(probeCtx, configured)
			if err != nil {
				return err
			}
			if err := requireProbeCapabilitiesForOS(fingerprint, spec.OperatingSystem, spec.Capabilities...); err != nil {
				return err
			}
			if capabilityFingerprint.Digest().IsZero() {
				capabilityFingerprint = fingerprint
			} else if capabilityFingerprint.Digest() != fingerprint.Digest() {
				return fmt.Errorf("%s templates produced different physical capability fingerprints", spec.Label)
			}
			return nil
		}); err != nil {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, err
		}
		var report ports.TargetPhysicalPolicyReport
		if err := runProbe(ctx, timeout, spec.Label+" physical policy "+configured.Name, func(probeCtx context.Context) error {
			var reportErr error
			report, reportErr = reporter.TargetPhysicalPolicy(probeCtx, configured)
			return reportErr
		}); err != nil {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, err
		}
		report = policyauthority.WithTargetConfiguredResources(report, configuredPlan.Resources)
		fingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(report)
		if err != nil {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, fmt.Errorf("fingerprint %s physical policy: %w", spec.Label, err)
		}
		if physicalFingerprint.Digest().IsZero() {
			physicalFingerprint = fingerprint
		} else if physicalFingerprint.Digest() != fingerprint.Digest() {
			return domain.CapabilityFingerprint{}, domain.CapabilityFingerprint{}, nil, fmt.Errorf("%s templates produced different physical policy facts", spec.Label)
		}
		reports[configured.Name] = report
	}
	return capabilityFingerprint, physicalFingerprint, reports, nil
}

func requireObserverProbe(fingerprint domain.CapabilityFingerprint, adapter string) error {
	if fingerprint.Digest().IsZero() {
		return fmt.Errorf("observer returned an empty capability fingerprint")
	}
	name := "observer." + adapter
	capability, found := fingerprint.Capability(name)
	if !found || capability.Status() != domain.CapabilitySupported {
		return fmt.Errorf("observer does not report required capability %q as supported", name)
	}
	return nil
}

func compositionIntrinsicCoverage(targets map[domain.TargetKind]ports.TargetDriver) map[string][]string {
	coverage := make(map[string][]string)
	if _, configured := targets[domain.TargetLinuxContainer]; configured {
		coverage["linux-container"] = []string{ports.TargetLifecycleSignal}
	}
	if _, configured := targets[domain.TargetAndroidVirtualDevice]; configured {
		coverage["android-virtual-device"] = []string{ports.TargetLifecycleSignal}
	}
	return coverage
}

func physicalPolicyAdmissionConfig(deployment builtDeployment, policies orchestration.EffectivePolicyResolver, agent ports.AgentWorkspacePhysicalPolicyReport, agentReporter ports.AgentWorkspacePhysicalPolicyReporter, targetPhysical map[domain.TargetKind]map[string]ports.TargetPhysicalPolicyReport, targets map[domain.TargetKind]ports.TargetPhysicalPolicyReporter) orchestration.PolicyAdmissionConfig {
	return orchestration.PolicyAdmissionConfig{
		Base: deployment.resolver, Policies: policies, WorkspaceMode: "directory-copy-non-production",
		AgentPhysical: agent, AgentReporter: agentReporter, TargetPhysical: targetPhysical, TargetReporters: targets,
	}
}

func verifyDockerImage(ctx context.Context, binary, reference string) error {
	result, err := (command.OS{}).Run(ctx, command.Invocation{Program: binary, Args: []string{"image", "inspect", reference}})
	if err != nil {
		return err
	}
	var images []struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
	}
	if err := json.Unmarshal(result.Stdout, &images); err != nil {
		return fmt.Errorf("decode Docker image inspection: %w", err)
	}
	if len(images) != 1 {
		return fmt.Errorf("decode Docker image inspection: expected one image, got %d", len(images))
	}
	_, digestText, _ := strings.Cut(reference, "@")
	if images[0].ID == digestText {
		return nil
	}
	for _, repositoryDigest := range images[0].RepoDigests {
		if repositoryDigest == reference {
			return nil
		}
	}
	return fmt.Errorf("Docker image inspection did not prove exact identity %s", reference)
}

func validatePhysicalRootSeparation(configuration config, sourceRoot string) error {
	roots := []struct {
		name string
		path string
	}{
		{"state", configuration.statePath},
		{"deployment-profile", configuration.deploymentProfile},
		{"agent-workspace-root", configuration.agentWorkspaceRoot},
		{"material-dir", configuration.materialRoot},
		{"bundle-dir", configuration.bundleRoot},
		{"orchestration-state-dir", configuration.orchestrationStateRoot},
		{"ledger-dir", configuration.ledgerDirectory},
		{"material.source_root", sourceRoot},
	}
	if configuration.observerDriver == "process" {
		roots = append(roots, struct {
			name string
			path string
		}{"observer-output-dir", configuration.observerOutputRoot})
	}
	if configuration.linuxTargetDriver == "docker" {
		roots = append(roots, struct {
			name string
			path string
		}{"target-root", configuration.targetRoot})
	}
	if configuration.androidTargetDriver != "none" {
		roots = append(roots,
			struct {
				name string
				path string
			}{"android-target-root", configuration.androidTargetRoot},
			struct {
				name string
				path string
			}{"android-system-image-root", configuration.androidSystemImageRoot},
			struct {
				name string
				path string
			}{"android-sdk-root", configuration.androidSDKRoot},
		)
	}
	if configuration.captureDriver == "ledger" {
		roots = append(roots, struct {
			name string
			path string
		}{"capture-dir", configuration.captureRoot})
	}
	if configuration.unixSocket != "" {
		roots = append(roots, struct {
			name string
			path string
		}{"unix-socket", configuration.unixSocket})
	}
	for _, root := range roots {
		if err := requireAbsoluteManagedRoot(root.name, root.path); err != nil {
			return err
		}
	}
	for left := 0; left < len(roots); left++ {
		for right := left + 1; right < len(roots); right++ {
			if pathsOverlap(roots[left].path, roots[right].path) {
				return fmt.Errorf("%s and %s must be separate non-overlapping roots", roots[left].name, roots[right].name)
			}
		}
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		left = strings.ToLower(left)
		right = strings.ToLower(right)
	}
	if left == right {
		return true
	}
	return pathIsWithin(left, right) || pathIsWithin(right, left)
}

func pathIsWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != "." && relative != ".." && !filepath.IsAbs(relative) &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
