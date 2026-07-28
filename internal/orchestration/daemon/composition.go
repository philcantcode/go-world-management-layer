package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	agentdocker "github.com/philcantcode/go-world-management-layer/internal/drivers/agent/docker"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
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

type observerDriverConfig struct {
	Adapters   []observerprocess.Adapter
	OutputRoot string
}

type compositionFactories struct {
	newWorkspace func(string) (ports.WorkspaceDriver, error)
	newAgent     func(agentDriverConfig) (ports.AgentWorkspaceDriver, error)
	newTarget    func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error)
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

func configureHostDrivers(ctx context.Context, configuration config, observations *ledger.Ledger, policies *policyauthority.Authority) (hostComposition, error) {
	return configureHostDriversWithFactories(ctx, configuration, observations, policies, productionCompositionFactories())
}

func configureHostDriversWithFactories(ctx context.Context, configuration config, observations *ledger.Ledger, policies *policyauthority.Authority, factories compositionFactories) (hostComposition, error) {
	if configuration.materialDriver != "local" {
		return hostComposition{}, unavailableDriver("material-driver", configuration.materialDriver, "this binary supports the local scope-bound material authority")
	}
	if configuration.androidTargetDriver != "none" {
		return hostComposition{}, unavailableDriver("android-target-driver", configuration.androidTargetDriver, "Android composition requires its device allocator and scoped ADB gateway")
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
	if factories.newWorkspace == nil || factories.newAgent == nil || factories.newTarget == nil || factories.verifyImage == nil {
		return hostComposition{}, fmt.Errorf("physical composition factory is incomplete")
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
		if len(deployment.targetTemplates) == 0 || deployment.targetRepository == "" {
			return hostComposition{}, fmt.Errorf("linux-target-driver=docker requires target templates in the deployment profile")
		}
		if configuration.targetImageRepository != "" && configuration.targetImageRepository != deployment.targetRepository {
			return hostComposition{}, fmt.Errorf("target-image-repository %q does not match deployment profile repository %q", configuration.targetImageRepository, deployment.targetRepository)
		}
	} else if len(deployment.targetTemplates) != 0 || deployment.runCount != 0 {
		return hostComposition{}, fmt.Errorf("deployment profile contains target or run plans while linux-target-driver is disabled")
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
		DirectoryWorkspace: true, BoundedLedgerCapture: configuration.captureDriver == "ledger",
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
	var targetFingerprint domain.CapabilityFingerprint
	var targetPhysicalFingerprint domain.CapabilityFingerprint
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
		for _, expected := range deployment.targetTemplates {
			configured := expected
			configuredPlan, found := deployment.static.Targets[configured.Name]
			if !found {
				return hostComposition{}, fmt.Errorf("Linux target template %q has no authoritative provisioning plan", configured.Name)
			}
			if err := runProbe(ctx, configuration.probeTimeout, "Linux target template "+configured.Name, func(probeCtx context.Context) error {
				fingerprint, err := target.Probe(probeCtx, configured)
				if err != nil {
					return err
				}
				if err := requireProbeCapabilities(fingerprint, "target.linux-container", "target.visibility-first"); err != nil {
					return err
				}
				if targetFingerprint.Digest().IsZero() {
					targetFingerprint = fingerprint
				} else if targetFingerprint.Digest() != fingerprint.Digest() {
					return fmt.Errorf("Linux target templates produced different physical capability fingerprints")
				}
				return nil
			}); err != nil {
				return hostComposition{}, err
			}
			var physicalReport ports.TargetPhysicalPolicyReport
			if err := runProbe(ctx, configuration.probeTimeout, "Linux target physical policy "+configured.Name, func(probeCtx context.Context) error {
				var reportErr error
				physicalReport, reportErr = targetReporter.TargetPhysicalPolicy(probeCtx, configured)
				return reportErr
			}); err != nil {
				return hostComposition{}, err
			}
			physicalReport = policyauthority.WithTargetConfiguredResources(physicalReport, configuredPlan.Resources)
			fingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(physicalReport)
			if err != nil {
				return hostComposition{}, fmt.Errorf("fingerprint Linux target physical policy: %w", err)
			}
			if targetPhysicalFingerprint.Digest().IsZero() {
				targetPhysicalFingerprint = fingerprint
			} else if targetPhysicalFingerprint.Digest() != fingerprint.Digest() {
				return hostComposition{}, fmt.Errorf("Linux target templates produced different physical policy facts")
			}
			if targetPhysicalReports[configured.Kind] == nil {
				targetPhysicalReports[configured.Kind] = make(map[string]ports.TargetPhysicalPolicyReport)
			}
			targetPhysicalReports[configured.Kind][configured.Name] = physicalReport
		}
		targets[domain.TargetLinuxContainer] = target
		targetReporters[domain.TargetLinuxContainer] = targetReporter
		capabilityComponents = append(capabilityComponents, policyauthority.CapabilityComponent{Name: "linux-target", Fingerprint: targetFingerprint})
		capabilityComponents = append(capabilityComponents, policyauthority.CapabilityComponent{Name: "linux-target-physical", Fingerprint: targetPhysicalFingerprint})
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
	}, nil
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
	if fingerprint.Digest().IsZero() {
		return fmt.Errorf("driver returned an empty capability fingerprint")
	}
	for _, name := range names {
		capability, found := fingerprint.Capability(name)
		if !found || capability.Status() != domain.CapabilitySupported {
			return fmt.Errorf("driver does not report required capability %q as supported", name)
		}
	}
	if operatingSystem := strings.ToLower(strings.TrimSpace(fingerprint.Evidence()["os"])); operatingSystem != "linux" {
		return fmt.Errorf("driver reports operating system %q; a Linux Docker engine is required", operatingSystem)
	}
	return nil
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
