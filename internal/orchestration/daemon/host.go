package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/platform"
	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/processlock"
	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

// HostConfig is the listen-free production composition input for OpenHost.
// Paths, drivers, and a fixed policy subject replace daemon flags that only
// existed for network authentication and WorldService listen.
type HostConfig struct {
	StatePath              string
	LedgerDirectory        string
	OrchestrationStateRoot string
	BundleRoot             string
	MaterialRoot           string
	DeploymentProfile      string

	// SubjectName is the fixed in-process policy subject installed on every
	// Service authorize path. Required when Subject is nil.
	SubjectName string
	// Subject optionally replaces the fixed SubjectName resolver (tests).
	Subject orchestration.SubjectResolver

	ControlTimeout         time.Duration
	ReconciliationInterval time.Duration
	ReconciliationTimeout  time.Duration
	ShutdownTimeout        time.Duration
	MaxTransferBytes       int64
	MaxExecBytes           int64
	MaxADBBytes            int64
	MaxBundleBytes         int64
	MaxCaptureRecords      int
	AllowRemoteADB         bool
	ProbeTimeout           time.Duration

	AgentDriver          string
	DockerBinary         string
	AgentWorkspaceRoot   string
	AgentImageRepository string
	AgentGuestBinary     string
	AgentContainerUser   string

	LinuxTargetDriver     string
	TargetRoot            string
	TargetImageRepository string
	TargetAllowPtrace     bool

	AndroidTargetDriver     string
	AndroidTargetRoot       string
	AndroidSystemImageRoot  string
	AndroidADBBinary        string
	AndroidADBServer        string
	AndroidEmulatorBinary   string
	AndroidSDKRoot          string
	AndroidSDKManagerBinary string
	AndroidAVDManagerBinary string
	AndroidADBBasePort      int
	AndroidBackendVersion   string
	AndroidRuntimeVersion   string

	PhysicalTargetDriver string
	ObserverDriver       string
	ObserverOutputRoot   string
	CaptureDriver        string
	CaptureRoot          string
	WorkspaceDriver      string
	MaterialDriver       string

	// Logger is optional; defaults to log.Printf.
	Logger func(string, ...any)
}

// Host is exclusive ownership of one control-state tree after successful
// startup reconciliation. It is not safe to OpenHost the same StatePath from
// two processes; the second call fails closed with processlock.ErrAlreadyHeld.
type Host struct {
	Production *orchestration.Production
	Store      *store.Store
	Ledger     *ledger.Ledger
	Subject    string
	// PlatformSupport is the structured host feature matrix logged at Open.
	PlatformSupport platform.SupportReport

	stateOwner  *processlock.Owner
	composition hostComposition
	core        *application.Core

	shutdownTimeout        time.Duration
	reconciliationInterval time.Duration
	reconciliationTimeout  time.Duration

	cancelBackground context.CancelFunc
	backgroundDone   <-chan struct{}
	closeOnce        sync.Once
	closeErr         error
}

// OpenHost acquires exclusive process ownership of StatePath, composes
// production Core/Service/drivers, runs startup reconciliation, and starts the
// background lease-termination ticker. It does not open a network listener.
func OpenHost(ctx context.Context, cfg HostConfig) (*Host, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	configuration, subjectName, err := hostConfigToDaemonConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := configuration.validateHost(); err != nil {
		return nil, err
	}
	subject := cfg.Subject
	if subject == nil {
		fixed := strings.TrimSpace(subjectName)
		subject = func(context.Context) (string, bool) {
			return fixed, true
		}
	}

	stateOwner, err := processlock.Acquire(configuration.statePath)
	if err != nil {
		return nil, fmt.Errorf("acquire exclusive ownership for control state %q: %w", configuration.statePath, err)
	}
	configuration.statePath = stateOwner.ControlPath()

	host, err := openHostOwned(ctx, configuration, subject, subjectName, stateOwner, cfg.Logger)
	if err != nil {
		_ = stateOwner.Release()
		return nil, err
	}
	return host, nil
}

func openHostOwned(
	ctx context.Context,
	configuration config,
	subject orchestration.SubjectResolver,
	subjectName string,
	stateOwner *processlock.Owner,
	logf func(string, ...any),
) (host *Host, resultErr error) {
	if logf == nil {
		logf = log.Printf
	}
	support := platform.Report()
	logPlatformSupport(logf, support, configuration)
	if err := platform.RequireSafePathNamespaces(); err != nil {
		return nil, fmt.Errorf("host platform preflight: %w", err)
	}
	if configuration.androidTargetDriver != "none" {
		if err := platform.RequireAndroidManagedHost(); err != nil {
			return nil, fmt.Errorf("host platform preflight: %w", err)
		}
	}
	controlStore, err := store.Open(ctx, store.Options{Path: configuration.statePath})
	if err != nil {
		return nil, fmt.Errorf("open control store: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, controlStore.Close())
		}
	}()
	core, err := application.NewCore(ctx, application.CoreOptions{Store: controlStore})
	if err != nil {
		return nil, fmt.Errorf("open application core: %w", err)
	}
	registry, err := policyregistry.New(controlStore)
	if err != nil {
		return nil, fmt.Errorf("open effective policy registry: %w", err)
	}
	policyAuthority, err := policyauthority.New(registry)
	if err != nil {
		return nil, fmt.Errorf("open effective policy authority: %w", err)
	}
	observations, recovery, err := ledger.Open(ledger.Options{Directory: configuration.ledgerDirectory})
	if err != nil {
		return nil, fmt.Errorf("open observation ledger: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, observations.Close())
		}
	}()
	for _, repair := range recovery.Repairs {
		logf("observation ledger recovered incomplete tail segment=%s offset=%d removed_bytes=%d", repair.Segment, repair.Offset, repair.RemovedBytes)
	}

	composition, err := configureHostDrivers(ctx, configuration, observations, policyAuthority)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil && composition.close != nil {
			resultErr = errors.Join(resultErr, composition.close())
		}
	}()
	var leasePolicyAdmission orchestration.LeaseOperationPolicyAdmission
	if composition.resolver != nil {
		admissionConfig := composition.policyAdmission
		admissionConfig.ResourceInventory = corePolicyResourceInventory(core)
		policyResolver, resolverErr := orchestration.NewPolicyAdmissionResolver(admissionConfig)
		if resolverErr != nil {
			return nil, fmt.Errorf("open effective-policy physical admission boundary: %w", resolverErr)
		}
		composition.resolver = policyResolver
		leasePolicyAdmission = policyResolver
	}
	var workspaceScope orchestration.WorkspaceResolver
	if composition.workspace != nil {
		workspaceScope, err = orchestration.NewCoreWorkspaceResolver(core)
		if err != nil {
			return nil, fmt.Errorf("create workspace scope resolver: %w", err)
		}
	}
	production, err := orchestration.NewProduction(orchestration.ProductionConfig{
		Service: orchestration.Config{
			Core: core, Ledger: observations, Agent: composition.agent, Targets: composition.targets,
			Workspace: composition.workspace, WorkspaceScope: workspaceScope, Material: composition.material,
			Captures: composition.captures, Observers: composition.observers,
			PolicyAdmission:  leasePolicyAdmission,
			Subject:          subject,
			StateRoot:        configuration.orchestrationStateRoot,
			MaxTransferBytes: configuration.maxTransferBytes,
			MaxExecBytes:     configuration.maxExecBytes,
			MaxADBBytes:      configuration.maxADBBytes,
			ControlTimeout:   configuration.controlTimeout,
			AllowRemoteADB:   configuration.allowRemoteADB,
		},
		Resolver:   composition.resolver,
		BundleRoot: configuration.bundleRoot, MaterialRoot: configuration.materialRoot,
		MaxBundleBytes: configuration.maxBundleBytes,
	})
	if err != nil {
		return nil, err
	}
	startupReports, err := reconcileStartupStateWithin(ctx, production.Core, configuration.reconciliationTimeout)
	if err != nil {
		return nil, fmt.Errorf("startup state reconciliation: %w", err)
	}
	logPhysicalReconciliation(startupReports.Physical)
	startupTermination := startupReports.LeaseTermination
	if startupTermination.Examined != 0 {
		logf("startup lease termination reconciliation examined=%d begun=%d completed=%d", startupTermination.Examined, startupTermination.Begun, startupTermination.Completed)
	}

	backgroundCtx, cancelBackground := context.WithCancel(context.Background())
	backgroundDone := make(chan struct{})
	go func() {
		defer close(backgroundDone)
		runLeaseTerminationTicker(backgroundCtx, production.Core, configuration.reconciliationInterval, configuration.reconciliationTimeout, logf)
	}()

	return &Host{
		Production:             production,
		Store:                  controlStore,
		Ledger:                 observations,
		Subject:                subjectName,
		PlatformSupport:        support,
		stateOwner:             stateOwner,
		composition:            composition,
		core:                   core,
		shutdownTimeout:        configuration.shutdownTimeout,
		reconciliationInterval: configuration.reconciliationInterval,
		reconciliationTimeout:  configuration.reconciliationTimeout,
		cancelBackground:       cancelBackground,
		backgroundDone:         backgroundDone,
	}, nil
}

func logPlatformSupport(logf func(string, ...any), support platform.SupportReport, configuration config) {
	logf("host platform support %s", support.CompactSummary())
	for _, line := range support.FormatWarningLines() {
		logf("%s", line)
	}
	for _, note := range support.EnabledDriverNotes(configuration.androidTargetDriver, configuration.linuxTargetDriver, configuration.agentDriver) {
		logf("platform support warning: selected composition: %s", note)
	}
	if encoded, err := support.JSON(); err == nil {
		logf("host platform support report json=%s", encoded)
	}
}

// Close stops background reconciliation, closes drivers/observers, releases the
// process lock, and closes store/ledger. Close is idempotent.
func (h *Host) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		var errs error
		if h.cancelBackground != nil {
			h.cancelBackground()
		}
		if h.backgroundDone != nil {
			<-h.backgroundDone
		}
		if err := closeRunObservers(h.composition.observers, h.shutdownTimeout); err != nil {
			errs = errors.Join(errs, err)
		}
		if h.composition.close != nil {
			errs = errors.Join(errs, h.composition.close())
		}
		if h.Ledger != nil {
			errs = errors.Join(errs, h.Ledger.Close())
		}
		if h.Store != nil {
			errs = errors.Join(errs, h.Store.Close())
		}
		if h.stateOwner != nil {
			errs = errors.Join(errs, h.stateOwner.Release())
		}
		h.closeErr = errs
	})
	return h.closeErr
}

// Controller returns the production controller (logical Core with physical lifecycle).
func (h *Host) Controller() *orchestration.Controller {
	if h == nil || h.Production == nil {
		return nil
	}
	return h.Production.Core
}

// Service returns the production capability service.
func (h *Host) Service() *orchestration.Service {
	if h == nil || h.Production == nil {
		return nil
	}
	return h.Production.Capabilities
}

// Core returns the application core embedded by the controller.
func (h *Host) Core() *application.Core {
	if h == nil {
		return nil
	}
	if h.Production != nil && h.Production.Core != nil {
		return h.Production.Core.Core
	}
	return h.core
}

// AgentDriver returns the composed agent workspace driver, if any.
func (h *Host) AgentDriver() ports.AgentWorkspaceDriver {
	if h == nil {
		return nil
	}
	return h.composition.agent
}

// Material returns the composed material authority, if any.
func (h *Host) Material() ports.MaterialAuthority {
	if h == nil || h.Production == nil || h.Production.Capabilities == nil {
		return h.composition.material
	}
	return h.Production.Capabilities.Material()
}

// ActionEvidence returns the research action evidence store when composed.
func (h *Host) ActionEvidence() *research.Store {
	if h == nil || h.Production == nil || h.Production.Capabilities == nil {
		return nil
	}
	return h.Production.Capabilities.ActionEvidence()
}

// Reconcile runs one physical + lease-termination reconciliation cycle.
func (h *Host) Reconcile(ctx context.Context) error {
	if h == nil || h.Production == nil {
		return fmt.Errorf("host is closed or not open")
	}
	_, err := reconcileStartupStateWithin(ctx, h.Production.Core, h.reconciliationTimeout)
	return err
}

func hostConfigToDaemonConfig(cfg HostConfig) (config, string, error) {
	subjectName := strings.TrimSpace(cfg.SubjectName)
	if cfg.Subject == nil && subjectName == "" {
		return config{}, "", fmt.Errorf("subject name is required")
	}
	if subjectName == "" {
		subjectName = "local-operator"
	}

	defaults, err := defaultHostConfig()
	if err != nil {
		return config{}, "", err
	}

	apply := func(value, fallback string) string {
		if strings.TrimSpace(value) != "" {
			return value
		}
		return fallback
	}
	applyDuration := func(value, fallback time.Duration) time.Duration {
		if value > 0 {
			return value
		}
		return fallback
	}
	applyInt64 := func(value, fallback int64) int64 {
		if value > 0 {
			return value
		}
		return fallback
	}
	applyInt := func(value, fallback int) int {
		if value > 0 {
			return value
		}
		return fallback
	}

	value := config{
		statePath:              apply(cfg.StatePath, defaults.statePath),
		ledgerDirectory:        apply(cfg.LedgerDirectory, defaults.ledgerDirectory),
		orchestrationStateRoot: apply(cfg.OrchestrationStateRoot, defaults.orchestrationStateRoot),
		bundleRoot:             apply(cfg.BundleRoot, defaults.bundleRoot),
		materialRoot:           apply(cfg.MaterialRoot, defaults.materialRoot),
		deploymentProfile:      strings.TrimSpace(cfg.DeploymentProfile),

		maxTransferBytes:       applyInt64(cfg.MaxTransferBytes, defaults.maxTransferBytes),
		maxExecBytes:           applyInt64(cfg.MaxExecBytes, defaults.maxExecBytes),
		maxADBBytes:            applyInt64(cfg.MaxADBBytes, defaults.maxADBBytes),
		maxBundleBytes:         applyInt64(cfg.MaxBundleBytes, defaults.maxBundleBytes),
		maxCaptureRecords:      applyInt(cfg.MaxCaptureRecords, defaults.maxCaptureRecords),
		allowRemoteADB:         cfg.AllowRemoteADB,
		probeTimeout:           applyDuration(cfg.ProbeTimeout, defaults.probeTimeout),
		controlTimeout:         applyDuration(cfg.ControlTimeout, defaults.controlTimeout),
		reconciliationInterval: applyDuration(cfg.ReconciliationInterval, defaults.reconciliationInterval),
		reconciliationTimeout:  applyDuration(cfg.ReconciliationTimeout, defaults.reconciliationTimeout),
		shutdownTimeout:        applyDuration(cfg.ShutdownTimeout, defaults.shutdownTimeout),

		agentDriver:          apply(cfg.AgentDriver, defaults.agentDriver),
		dockerBinary:         apply(cfg.DockerBinary, defaults.dockerBinary),
		agentWorkspaceRoot:   cfg.AgentWorkspaceRoot,
		agentImageRepository: cfg.AgentImageRepository,
		agentGuestBinary:     apply(cfg.AgentGuestBinary, defaults.agentGuestBinary),
		agentContainerUser:   apply(cfg.AgentContainerUser, defaults.agentContainerUser),

		linuxTargetDriver:     apply(cfg.LinuxTargetDriver, defaults.linuxTargetDriver),
		targetRoot:            cfg.TargetRoot,
		targetImageRepository: cfg.TargetImageRepository,
		targetAllowPtrace:     cfg.TargetAllowPtrace,

		androidTargetDriver:     apply(cfg.AndroidTargetDriver, defaults.androidTargetDriver),
		androidTargetRoot:       cfg.AndroidTargetRoot,
		androidSystemImageRoot:  cfg.AndroidSystemImageRoot,
		androidADBBinary:        apply(cfg.AndroidADBBinary, defaults.androidADBBinary),
		androidADBServer:        apply(cfg.AndroidADBServer, defaults.androidADBServer),
		androidEmulatorBinary:   apply(cfg.AndroidEmulatorBinary, defaults.androidEmulatorBinary),
		androidSDKRoot:          cfg.AndroidSDKRoot,
		androidSDKManagerBinary: apply(cfg.AndroidSDKManagerBinary, defaults.androidSDKManagerBinary),
		androidAVDManagerBinary: apply(cfg.AndroidAVDManagerBinary, defaults.androidAVDManagerBinary),
		androidADBBasePort:      applyInt(cfg.AndroidADBBasePort, defaults.androidADBBasePort),
		androidBackendVersion:   cfg.AndroidBackendVersion,
		androidRuntimeVersion:   cfg.AndroidRuntimeVersion,

		physicalTargetDriver: apply(cfg.PhysicalTargetDriver, defaults.physicalTargetDriver),
		observerDriver:       apply(cfg.ObserverDriver, defaults.observerDriver),
		observerOutputRoot:   cfg.ObserverOutputRoot,
		captureDriver:        apply(cfg.CaptureDriver, defaults.captureDriver),
		captureRoot:          cfg.CaptureRoot,
		workspaceDriver:      apply(cfg.WorkspaceDriver, defaults.workspaceDriver),
		materialDriver:       apply(cfg.MaterialDriver, defaults.materialDriver),
	}
	return value, subjectName, nil
}

func defaultHostConfig() (config, error) {
	return defaultConfig()
}

// validateHost enforces path/driver/timeout rules for OpenHost.
func (c config) validateHost() error {
	return c.validatePathsAndDrivers()
}

// validatePathsAndDrivers validates composition paths, drivers, and physical
// constraints without any listen or network authentication surface.
func (c config) validatePathsAndDrivers() error {
	paths := map[string]string{"state": c.statePath, "ledger-dir": c.ledgerDirectory, "orchestration-state-dir": c.orchestrationStateRoot, "bundle-dir": c.bundleRoot, "material-dir": c.materialRoot}
	for name, value := range paths {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be blank", name)
		}
	}
	if c.maxTransferBytes <= 0 || c.maxExecBytes <= 0 || c.maxADBBytes <= 0 || c.maxBundleBytes <= 0 {
		return fmt.Errorf("all byte limits must be positive")
	}
	if c.probeTimeout <= 0 || c.controlTimeout <= 0 || c.reconciliationInterval <= 0 || c.reconciliationTimeout <= 0 || c.shutdownTimeout <= 0 {
		return fmt.Errorf("driver-probe-timeout, control-timeout, reconciliation-interval, reconciliation-timeout, and shutdown-timeout must be positive")
	}
	for name, choice := range map[string]struct {
		value   string
		allowed []string
	}{
		"agent-driver":           {c.agentDriver, []string{"none", "docker"}},
		"linux-target-driver":    {c.linuxTargetDriver, []string{"none", "docker"}},
		"workspace-driver":       {c.workspaceDriver, []string{"none", "directory"}},
		"material-driver":        {c.materialDriver, []string{"local"}},
		"android-target-driver":  {c.androidTargetDriver, []string{"none", "android-emulator"}},
		"physical-target-driver": {c.physicalTargetDriver, []string{"none"}},
		"observer-driver":        {c.observerDriver, []string{"none", "process"}},
		"capture-driver":         {c.captureDriver, []string{"none", "ledger"}},
	} {
		if err := requireChoice(name, choice.value, choice.allowed...); err != nil {
			return err
		}
	}
	physical := c.agentDriver != "none" || c.workspaceDriver != "none" || c.linuxTargetDriver != "none" || c.androidTargetDriver != "none"
	if (c.agentDriver == "docker") != (c.workspaceDriver == "directory") {
		return fmt.Errorf("agent-driver=docker and workspace-driver=directory must be enabled together")
	}
	if c.linuxTargetDriver == "docker" && c.agentDriver != "docker" {
		return fmt.Errorf("linux-target-driver=docker requires the Docker agent and directory workspace drivers")
	}
	if c.androidTargetDriver != "none" && c.agentDriver != "docker" {
		return fmt.Errorf("android-target-driver requires the Docker agent and directory workspace drivers")
	}
	if !physical {
		if c.captureDriver != "none" {
			return fmt.Errorf("capture-driver=ledger requires the physical local material composition")
		}
		if c.deploymentProfile != "" {
			return fmt.Errorf("deployment-profile may be set only when physical drivers are enabled")
		}
		for name, configured := range map[string]bool{
			"agent-workspace-root": c.agentWorkspaceRoot != "", "agent-image-repository": c.agentImageRepository != "",
			"target-root": c.targetRoot != "", "target-image-repository": c.targetImageRepository != "",
			"target-allow-ptrace": c.targetAllowPtrace,
			"android-target-root": c.androidTargetRoot != "", "android-system-image-root": c.androidSystemImageRoot != "",
			"android-sdk-root":        c.androidSDKRoot != "",
			"android-backend-version": c.androidBackendVersion != "", "android-runtime-version": c.androidRuntimeVersion != "",
			"observer-output-dir": c.observerOutputRoot != "", "observer-driver": c.observerDriver != "none",
		} {
			if configured {
				return fmt.Errorf("%s may be set only when its physical driver is enabled", name)
			}
		}
		return nil
	}
	return c.validatePhysicalComposition()
}

func (c config) validatePhysicalComposition() error {
	if strings.TrimSpace(c.deploymentProfile) == "" || !filepath.IsAbs(c.deploymentProfile) {
		return fmt.Errorf("deployment-profile must be an absolute path when physical drivers are enabled")
	}
	if strings.TrimSpace(c.dockerBinary) == "" {
		return fmt.Errorf("docker-binary must not be blank")
	}
	if err := requireAbsoluteManagedRoot("agent-workspace-root", c.agentWorkspaceRoot); err != nil {
		return err
	}
	for name, root := range map[string]string{
		"state": c.statePath, "ledger-dir": c.ledgerDirectory, "orchestration-state-dir": c.orchestrationStateRoot,
		"bundle-dir": c.bundleRoot, "material-dir": c.materialRoot,
	} {
		if err := requireAbsoluteManagedRoot(name, root); err != nil {
			return err
		}
	}
	if c.captureDriver == "ledger" {
		if err := requireAbsoluteManagedRoot("capture-dir", c.captureRoot); err != nil {
			return err
		}
		if c.maxCaptureRecords <= 0 {
			return fmt.Errorf("max-capture-records must be positive")
		}
	} else if c.captureRoot != "" {
		return fmt.Errorf("capture-dir requires capture-driver=ledger")
	}
	if c.observerDriver == "process" {
		if err := requireAbsoluteManagedRoot("observer-output-dir", c.observerOutputRoot); err != nil {
			return err
		}
	} else if c.observerOutputRoot != "" {
		return fmt.Errorf("observer-output-dir requires observer-driver=process")
	}
	if c.linuxTargetDriver == "docker" {
		if err := requireAbsoluteManagedRoot("target-root", c.targetRoot); err != nil {
			return err
		}
	} else {
		if c.targetRoot != "" || c.targetImageRepository != "" || c.targetAllowPtrace {
			return fmt.Errorf("target-root, target-image-repository, and target-allow-ptrace require linux-target-driver=docker")
		}
	}
	if c.androidTargetDriver != "none" {
		if err := platform.RequireAndroidManagedHost(); err != nil {
			return err
		}
		if err := requireAbsoluteManagedRoot("android-target-root", c.androidTargetRoot); err != nil {
			return err
		}
		if err := requireAbsoluteManagedRoot("android-system-image-root", c.androidSystemImageRoot); err != nil {
			return err
		}
		for name, value := range map[string]string{
			"android-adb-binary": c.androidADBBinary, "android-adb-server": c.androidADBServer,
			"android-backend-version": c.androidBackendVersion, "android-runtime-version": c.androidRuntimeVersion,
		} {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s must not be blank when the Android target driver is enabled", name)
			}
		}
		if c.androidADBBasePort < cuttlefish.ManagedEmulatorMinConsolePort || c.androidADBBasePort > cuttlefish.ManagedEmulatorMaxConsolePort || c.androidADBBasePort%2 != 0 {
			return fmt.Errorf("android-adb-base-port must be an even console port from %d through %d", cuttlefish.ManagedEmulatorMinConsolePort, cuttlefish.ManagedEmulatorMaxConsolePort)
		}
		if err := cuttlefish.ValidateManagedADBServerEndpoint(c.androidADBServer); err != nil {
			return fmt.Errorf("android-adb-server: %w", err)
		}
		if strings.TrimSpace(c.androidEmulatorBinary) == "" || strings.TrimSpace(c.androidSDKManagerBinary) == "" || strings.TrimSpace(c.androidAVDManagerBinary) == "" {
			return fmt.Errorf("android-emulator requires non-blank emulator, sdkmanager, and avdmanager binaries")
		}
		if err := requireAbsoluteManagedRoot("android-sdk-root", c.androidSDKRoot); err != nil {
			return err
		}
	} else if c.androidTargetRoot != "" || c.androidSystemImageRoot != "" || c.androidSDKRoot != "" || c.androidBackendVersion != "" || c.androidRuntimeVersion != "" {
		return fmt.Errorf("Android roots and version expectations require android-target-driver")
	}
	if !validContainerPath(c.agentGuestBinary) {
		return fmt.Errorf("agent-guest-binary must be a normalized absolute container path")
	}
	if !validNumericContainerUser(c.agentContainerUser) {
		return fmt.Errorf("agent-container-user must be a numeric uid:gid pair")
	}
	for name, repository := range map[string]string{
		"agent-image-repository":  c.agentImageRepository,
		"target-image-repository": c.targetImageRepository,
	} {
		if repository != "" && (!repositoryPattern.MatchString(repository) || strings.Contains(repository, "..")) {
			return fmt.Errorf("%s is not a normalized Docker repository", name)
		}
	}
	return nil
}
