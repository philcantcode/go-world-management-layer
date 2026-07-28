package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/internal/processlock"
	"github.com/philcantcode/go-world-management-layer/internal/rpc"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Mode string

const (
	ModeController Mode = "worldd"
	ModeNode       Mode = "world-node"
)

type config struct {
	statePath              string
	ledgerDirectory        string
	orchestrationStateRoot string
	bundleRoot             string
	materialRoot           string
	unixSocket             string
	tcpAddress             string
	bearerToken            string
	bearerSubject          string
	trustedNodeSubjects    string
	serverCert             string
	serverKey              string
	clientCA               string
	maxMessageBytes        int
	maxTransferBytes       int64
	maxExecBytes           int64
	maxADBBytes            int64
	maxBundleBytes         int64
	maxCaptureRecords      int
	allowRemoteADB         bool
	probeTimeout           time.Duration
	controlTimeout         time.Duration
	reconciliationInterval time.Duration
	reconciliationTimeout  time.Duration
	shutdownTimeout        time.Duration
	deploymentProfile      string

	agentDriver          string
	dockerBinary         string
	agentWorkspaceRoot   string
	agentImageRepository string
	agentGuestBinary     string
	agentContainerUser   string

	linuxTargetDriver     string
	targetRoot            string
	targetImageRepository string
	targetAllowPtrace     bool
	androidTargetDriver   string
	physicalTargetDriver  string
	observerDriver        string
	observerOutputRoot    string
	captureDriver         string
	captureRoot           string
	workspaceDriver       string
	materialDriver        string
}

// Main is the shared executable entrypoint. Both binaries intentionally use
// the same authenticated transport and production service constructor.
func Main(mode Mode) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, os.Args[1:], mode); err != nil {
		log.Fatal(err)
	}
}

func Run(ctx context.Context, args []string, mode Mode) (runErr error) {
	configuration, err := parseConfig(args, mode)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	stateOwner, err := processlock.Acquire(configuration.statePath)
	if err != nil {
		return fmt.Errorf("acquire exclusive daemon ownership for control state %q: %w", configuration.statePath, err)
	}
	defer func() {
		if err := stateOwner.Release(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	configuration.statePath = stateOwner.ControlPath()
	transportCredentials, err := loadServerCredentials(configuration)
	if err != nil {
		return err
	}
	bearerSubjects := configuredBearerSubjects(configuration)
	if len(bearerSubjects) == 0 && transportCredentials == nil {
		return fmt.Errorf("configure WORLD_BEARER_TOKEN/-bearer-token or tls-cert, tls-key, and client-ca")
	}
	if transportCredentials == nil && configuration.tcpAddress != "" && !loopbackAddress(configuration.tcpAddress) {
		return fmt.Errorf("a bearer credential without TLS may listen only on loopback TCP")
	}

	controlStore, err := store.Open(ctx, store.Options{Path: configuration.statePath})
	if err != nil {
		return fmt.Errorf("open control store: %w", err)
	}
	defer controlStore.Close()
	core, err := application.NewCore(ctx, application.CoreOptions{Store: controlStore})
	if err != nil {
		return fmt.Errorf("open application core: %w", err)
	}
	registry, err := policyregistry.New(controlStore)
	if err != nil {
		return fmt.Errorf("open effective policy registry: %w", err)
	}
	policyAuthority, err := policyauthority.New(registry)
	if err != nil {
		return fmt.Errorf("open effective policy authority: %w", err)
	}
	observations, recovery, err := ledger.Open(ledger.Options{Directory: configuration.ledgerDirectory})
	if err != nil {
		return fmt.Errorf("open observation ledger: %w", err)
	}
	defer observations.Close()
	for _, repair := range recovery.Repairs {
		log.Printf("observation ledger recovered incomplete tail segment=%s offset=%d removed_bytes=%d", repair.Segment, repair.Offset, repair.RemovedBytes)
	}

	composition, err := configureHostDrivers(ctx, configuration, observations, policyAuthority)
	if err != nil {
		return err
	}
	var leasePolicyAdmission orchestration.LeaseOperationPolicyAdmission
	if composition.resolver != nil {
		admissionConfig := composition.policyAdmission
		admissionConfig.ResourceInventory = corePolicyResourceInventory(core)
		policyResolver, resolverErr := orchestration.NewPolicyAdmissionResolver(admissionConfig)
		err = resolverErr
		if err != nil {
			return fmt.Errorf("open effective-policy physical admission boundary: %w", err)
		}
		composition.resolver = policyResolver
		leasePolicyAdmission = policyResolver
	}
	defer func() {
		if err := closeRunObservers(composition.observers, configuration.shutdownTimeout); err != nil {
			log.Printf("run observer shutdown failed: %v", err)
		}
	}()
	var workspaceScope orchestration.WorkspaceResolver
	if composition.workspace != nil {
		workspaceScope, err = orchestration.NewCoreWorkspaceResolver(core)
		if err != nil {
			return fmt.Errorf("create workspace scope resolver: %w", err)
		}
	}
	production, err := orchestration.NewProduction(orchestration.ProductionConfig{
		Service: orchestration.Config{
			Core: core, Ledger: observations, Agent: composition.agent, Targets: composition.targets,
			Workspace: composition.workspace, WorkspaceScope: workspaceScope, Material: composition.material,
			Captures: composition.captures, Observers: composition.observers,
			PolicyAdmission: leasePolicyAdmission,
			Subject: func(ctx context.Context) (string, bool) {
				identity, ok := rpc.IdentityFromContext(ctx)
				return identity.Subject, ok
			},
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
		return err
	}
	startupReports, err := reconcileStartupStateWithin(ctx, production.Core, configuration.reconciliationTimeout)
	if err != nil {
		return fmt.Errorf("startup state reconciliation: %w", err)
	}
	logPhysicalReconciliation(startupReports.Physical)
	startupTermination := startupReports.LeaseTermination
	if startupTermination.Examined != 0 {
		log.Printf("startup lease termination reconciliation examined=%d begun=%d completed=%d", startupTermination.Examined, startupTermination.Begun, startupTermination.Completed)
	}
	server, err := rpc.NewServer(production.Core, rpc.ServerOptions{
		Authenticator: rpc.BearerOrMTLSAuthenticator{BearerSubjects: bearerSubjects, AllowMTLS: transportCredentials != nil},
		Capabilities:  production.Capabilities, TrustedNodeSubjects: parseSubjects(configuration.trustedNodeSubjects),
		MaxMessageBytes: configuration.maxMessageBytes, TransportCredentials: transportCredentials,
	})
	if err != nil {
		return fmt.Errorf("create RPC server: %w", err)
	}
	listener, err := rpc.Listen(rpc.ListenOptions{UnixSocket: configuration.unixSocket, TCPAddress: configuration.tcpAddress})
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	reconciliationCtx, stopReconciliation := context.WithCancel(ctx)
	defer stopReconciliation()
	go runLeaseTerminationTicker(reconciliationCtx, production.Core, configuration.reconciliationInterval, configuration.reconciliationTimeout, log.Printf)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	profileDigest := "none"
	if !composition.profileDigest.IsZero() {
		profileDigest = composition.profileDigest.String()
	}
	log.Printf("%s listening on %s (agent=%s linux_target=%s observer=%s capture=%s workspace=%s material=%s deployment_profile=%s)",
		mode, listener.Addr(), configuration.agentDriver, configuration.linuxTargetDriver,
		configuration.observerDriver, configuration.captureDriver, configuration.workspaceDriver, configuration.materialDriver, profileDigest)
	select {
	case <-ctx.Done():
		stopServer(server, configuration.shutdownTimeout)
		return closeRunObservers(composition.observers, configuration.shutdownTimeout)
	case serveErr := <-serveErrors:
		observerErr := closeRunObservers(composition.observers, configuration.shutdownTimeout)
		if serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped) || errors.Is(serveErr, context.Canceled) {
			return observerErr
		}
		return errors.Join(serveErr, observerErr)
	}
}

func corePolicyResourceInventory(core *application.Core) orchestration.PolicyResourceInventory {
	return func(ctx context.Context) ([]application.ResearchSessionView, error) {
		return core.ListResearchSessions(ctx)
	}
}

func closeRunObservers(observers *orchestration.RunObserverCoordinator, timeout time.Duration) error {
	if observers == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return observers.Close(ctx)
}

func unavailableDriver(flagName, value, detail string) error {
	return fmt.Errorf("%s=%q is unavailable: %s", flagName, value, detail)
}

func stopServer(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		server.Stop()
		<-done
	}
}

func parseConfig(args []string, mode Mode) (config, error) {
	if mode != ModeController && mode != ModeNode {
		return config{}, fmt.Errorf("unknown daemon mode %q", mode)
	}
	value, err := defaultConfig(mode)
	if err != nil {
		return config{}, err
	}
	flags := flag.NewFlagSet(string(mode), flag.ContinueOnError)
	flags.StringVar(&value.statePath, "state", value.statePath, "SQLite control state path (WORLD_STATE)")
	flags.StringVar(&value.ledgerDirectory, "ledger-dir", value.ledgerDirectory, "durable observation ledger directory (WORLD_LEDGER_DIR)")
	flags.StringVar(&value.orchestrationStateRoot, "orchestration-state-dir", value.orchestrationStateRoot, "orchestration state directory (WORLD_ORCHESTRATION_STATE_DIR)")
	flags.StringVar(&value.bundleRoot, "bundle-dir", value.bundleRoot, "sealed observation bundle staging directory (WORLD_BUNDLE_DIR)")
	flags.StringVar(&value.materialRoot, "material-dir", value.materialRoot, "content-addressed bundle material directory (WORLD_MATERIAL_DIR)")
	flags.StringVar(&value.unixSocket, "unix-socket", value.unixSocket, "Unix-domain socket path (WORLD_UNIX_SOCKET)")
	flags.StringVar(&value.tcpAddress, "listen", value.tcpAddress, "TCP listen address (WORLD_LISTEN)")
	flags.StringVar(&value.bearerToken, "bearer-token", value.bearerToken, "local bearer token (prefer WORLD_BEARER_TOKEN)")
	flags.StringVar(&value.bearerSubject, "bearer-subject", value.bearerSubject, "policy identity for the local bearer (WORLD_BEARER_SUBJECT)")
	flags.StringVar(&value.trustedNodeSubjects, "trusted-node-subjects", value.trustedNodeSubjects, "comma-separated subjects allowed to report node state (WORLD_TRUSTED_NODE_SUBJECTS)")
	flags.StringVar(&value.serverCert, "tls-cert", value.serverCert, "server certificate PEM (WORLD_TLS_CERT)")
	flags.StringVar(&value.serverKey, "tls-key", value.serverKey, "server private key PEM (WORLD_TLS_KEY)")
	flags.StringVar(&value.clientCA, "client-ca", value.clientCA, "client CA PEM; enables required mTLS (WORLD_CLIENT_CA)")
	flags.IntVar(&value.maxMessageBytes, "max-message-bytes", value.maxMessageBytes, "maximum request or response bytes")
	flags.Int64Var(&value.maxTransferBytes, "max-transfer-bytes", value.maxTransferBytes, "maximum bytes per target transfer")
	flags.Int64Var(&value.maxExecBytes, "max-exec-bytes", value.maxExecBytes, "maximum bytes per exec stream direction")
	flags.Int64Var(&value.maxADBBytes, "max-adb-bytes", value.maxADBBytes, "maximum bytes per ADB tunnel direction")
	flags.Int64Var(&value.maxBundleBytes, "max-bundle-bytes", value.maxBundleBytes, "maximum sealed observation bundle bytes")
	flags.IntVar(&value.maxCaptureRecords, "max-capture-records", value.maxCaptureRecords, "maximum observation records per ledger capture (WORLD_MAX_CAPTURE_RECORDS)")
	flags.BoolVar(&value.allowRemoteADB, "allow-remote-adb", value.allowRemoteADB, "allow driver-issued non-loopback ADB endpoints (WORLD_ALLOW_REMOTE_ADB)")
	flags.DurationVar(&value.probeTimeout, "driver-probe-timeout", value.probeTimeout, "startup timeout for each requested host driver")
	flags.DurationVar(&value.controlTimeout, "control-timeout", value.controlTimeout, "detached physical cleanup and exec cleanup timeout (WORLD_CONTROL_TIMEOUT)")
	flags.DurationVar(&value.reconciliationInterval, "reconciliation-interval", value.reconciliationInterval, "lease termination reconciliation interval (WORLD_RECONCILIATION_INTERVAL)")
	flags.DurationVar(&value.reconciliationTimeout, "reconciliation-timeout", value.reconciliationTimeout, "timeout for each startup or periodic reconciliation attempt (WORLD_RECONCILIATION_TIMEOUT)")
	flags.DurationVar(&value.shutdownTimeout, "shutdown-timeout", value.shutdownTimeout, "graceful RPC shutdown timeout")
	flags.StringVar(&value.deploymentProfile, "deployment-profile", value.deploymentProfile, "trusted immutable physical provisioning profile JSON (WORLD_DEPLOYMENT_PROFILE)")
	flags.StringVar(&value.agentDriver, "agent-driver", value.agentDriver, "agent driver: none or docker (WORLD_AGENT_DRIVER)")
	flags.StringVar(&value.dockerBinary, "docker-binary", value.dockerBinary, "Docker CLI path (WORLD_DOCKER_BINARY)")
	flags.StringVar(&value.agentWorkspaceRoot, "agent-workspace-root", value.agentWorkspaceRoot, "authorized agent workspace root (WORLD_AGENT_WORKSPACE_ROOT)")
	flags.StringVar(&value.agentImageRepository, "agent-image-repository", value.agentImageRepository, "digest-pinned agent image repository prefix (WORLD_AGENT_IMAGE_REPOSITORY)")
	flags.StringVar(&value.agentGuestBinary, "agent-guest-binary", value.agentGuestBinary, "world-guest path inside agent images (WORLD_AGENT_GUEST_BINARY)")
	flags.StringVar(&value.agentContainerUser, "agent-container-user", value.agentContainerUser, "unprivileged user inside agent images (WORLD_AGENT_CONTAINER_USER)")
	flags.StringVar(&value.linuxTargetDriver, "linux-target-driver", value.linuxTargetDriver, "Linux target driver: none or docker (WORLD_LINUX_TARGET_DRIVER)")
	flags.StringVar(&value.targetRoot, "target-root", value.targetRoot, "authorized Linux target root (WORLD_TARGET_ROOT)")
	flags.StringVar(&value.targetImageRepository, "target-image-repository", value.targetImageRepository, "digest-pinned target image repository prefix (WORLD_TARGET_IMAGE_REPOSITORY)")
	flags.BoolVar(&value.targetAllowPtrace, "target-allow-ptrace", value.targetAllowPtrace, "allow SYS_PTRACE in Linux targets (WORLD_TARGET_ALLOW_PTRACE)")
	flags.StringVar(&value.androidTargetDriver, "android-target-driver", value.androidTargetDriver, "Android target driver selection (WORLD_ANDROID_TARGET_DRIVER)")
	flags.StringVar(&value.physicalTargetDriver, "physical-target-driver", value.physicalTargetDriver, "physical target driver selection (WORLD_PHYSICAL_TARGET_DRIVER)")
	flags.StringVar(&value.observerDriver, "observer-driver", value.observerDriver, "external observer driver: none or process (WORLD_OBSERVER_DRIVER)")
	flags.StringVar(&value.observerOutputRoot, "observer-output-dir", value.observerOutputRoot, "durable process observer output directory (WORLD_OBSERVER_OUTPUT_DIR)")
	flags.StringVar(&value.captureDriver, "capture-driver", value.captureDriver, "capture driver selection (WORLD_CAPTURE_DRIVER)")
	flags.StringVar(&value.captureRoot, "capture-dir", value.captureRoot, "durable ledger capture state directory (WORLD_CAPTURE_DIR)")
	flags.StringVar(&value.workspaceDriver, "workspace-driver", value.workspaceDriver, "workspace driver selection (WORLD_WORKSPACE_DRIVER)")
	flags.StringVar(&value.materialDriver, "material-driver", value.materialDriver, "material driver: local (WORLD_MATERIAL_DRIVER)")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if len(flags.Args()) != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if err := value.validate(); err != nil {
		return config{}, err
	}
	return value, nil
}

func defaultConfig(mode Mode) (config, error) {
	prefix := "world"
	defaultSocket, defaultTCP := "/tmp/worldd.sock", ""
	if mode == ModeNode {
		prefix, defaultSocket = "world-node", "/tmp/world-node.sock"
	}
	if runtime.GOOS == "windows" {
		defaultSocket = ""
		defaultTCP = "127.0.0.1:7777"
		if mode == ModeNode {
			defaultTCP = "127.0.0.1:7778"
		}
	}
	allowRemoteADB, err := environmentBool("WORLD_ALLOW_REMOTE_ADB", false)
	if err != nil {
		return config{}, err
	}
	allowPtrace, err := environmentBool("WORLD_TARGET_ALLOW_PTRACE", false)
	if err != nil {
		return config{}, err
	}
	maxCaptureRecords, err := environmentInt("WORLD_MAX_CAPTURE_RECORDS", 10000)
	if err != nil {
		return config{}, err
	}
	controlTimeout, err := environmentDuration("WORLD_CONTROL_TIMEOUT", 30*time.Second)
	if err != nil {
		return config{}, err
	}
	reconciliationInterval, err := environmentDuration("WORLD_RECONCILIATION_INTERVAL", 30*time.Second)
	if err != nil {
		return config{}, err
	}
	reconciliationTimeout, err := environmentDuration("WORLD_RECONCILIATION_TIMEOUT", 10*time.Second)
	if err != nil {
		return config{}, err
	}
	return config{
		statePath: envOr("WORLD_STATE", prefix+"-control.db"), ledgerDirectory: envOr("WORLD_LEDGER_DIR", prefix+"-ledger"),
		orchestrationStateRoot: envOr("WORLD_ORCHESTRATION_STATE_DIR", prefix+"-orchestration"), bundleRoot: envOr("WORLD_BUNDLE_DIR", prefix+"-bundles"),
		materialRoot: envOr("WORLD_MATERIAL_DIR", prefix+"-material"), unixSocket: envOr("WORLD_UNIX_SOCKET", defaultSocket), tcpAddress: envOr("WORLD_LISTEN", defaultTCP),
		bearerToken: os.Getenv("WORLD_BEARER_TOKEN"), bearerSubject: envOr("WORLD_BEARER_SUBJECT", "local-operator"), trustedNodeSubjects: os.Getenv("WORLD_TRUSTED_NODE_SUBJECTS"),
		serverCert: os.Getenv("WORLD_TLS_CERT"), serverKey: os.Getenv("WORLD_TLS_KEY"), clientCA: os.Getenv("WORLD_CLIENT_CA"),
		maxMessageBytes: 4 << 20, maxTransferBytes: 64 << 20, maxExecBytes: 64 << 20, maxADBBytes: 64 << 20, maxBundleBytes: 64 << 20,
		maxCaptureRecords: maxCaptureRecords,
		allowRemoteADB:    allowRemoteADB, probeTimeout: 10 * time.Second, controlTimeout: controlTimeout,
		reconciliationInterval: reconciliationInterval, reconciliationTimeout: reconciliationTimeout, shutdownTimeout: 10 * time.Second,
		deploymentProfile: os.Getenv("WORLD_DEPLOYMENT_PROFILE"),
		agentDriver:       envOr("WORLD_AGENT_DRIVER", "none"), dockerBinary: envOr("WORLD_DOCKER_BINARY", "docker"), agentWorkspaceRoot: os.Getenv("WORLD_AGENT_WORKSPACE_ROOT"),
		agentImageRepository: os.Getenv("WORLD_AGENT_IMAGE_REPOSITORY"), agentGuestBinary: envOr("WORLD_AGENT_GUEST_BINARY", "/usr/local/bin/world-guest"), agentContainerUser: envOr("WORLD_AGENT_CONTAINER_USER", "65532:65532"),
		linuxTargetDriver: envOr("WORLD_LINUX_TARGET_DRIVER", "none"), targetRoot: os.Getenv("WORLD_TARGET_ROOT"), targetImageRepository: os.Getenv("WORLD_TARGET_IMAGE_REPOSITORY"), targetAllowPtrace: allowPtrace,
		androidTargetDriver: envOr("WORLD_ANDROID_TARGET_DRIVER", "none"), physicalTargetDriver: envOr("WORLD_PHYSICAL_TARGET_DRIVER", "none"),
		observerDriver: envOr("WORLD_OBSERVER_DRIVER", "none"), observerOutputRoot: os.Getenv("WORLD_OBSERVER_OUTPUT_DIR"),
		captureDriver: envOr("WORLD_CAPTURE_DRIVER", "none"), captureRoot: os.Getenv("WORLD_CAPTURE_DIR"),
		workspaceDriver: envOr("WORLD_WORKSPACE_DRIVER", "none"), materialDriver: envOr("WORLD_MATERIAL_DRIVER", "local"),
	}, nil
}

func (c config) validate() error {
	paths := map[string]string{"state": c.statePath, "ledger-dir": c.ledgerDirectory, "orchestration-state-dir": c.orchestrationStateRoot, "bundle-dir": c.bundleRoot, "material-dir": c.materialRoot}
	for name, value := range paths {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be blank", name)
		}
	}
	if c.unixSocket == "" && c.tcpAddress == "" {
		return fmt.Errorf("unix-socket or listen is required")
	}
	if c.bearerToken != "" && strings.TrimSpace(c.bearerSubject) == "" {
		return fmt.Errorf("bearer-subject is required when bearer-token is configured")
	}
	if c.maxMessageBytes <= 0 || c.maxTransferBytes <= 0 || c.maxExecBytes <= 0 || c.maxADBBytes <= 0 || c.maxBundleBytes <= 0 {
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
		"android-target-driver":  {c.androidTargetDriver, []string{"none"}},
		"physical-target-driver": {c.physicalTargetDriver, []string{"none"}},
		"observer-driver":        {c.observerDriver, []string{"none", "process"}},
		"capture-driver":         {c.captureDriver, []string{"none", "ledger"}},
	} {
		if err := requireChoice(name, choice.value, choice.allowed...); err != nil {
			return err
		}
	}
	physical := c.agentDriver != "none" || c.workspaceDriver != "none" || c.linuxTargetDriver != "none"
	if (c.agentDriver == "docker") != (c.workspaceDriver == "directory") {
		return fmt.Errorf("agent-driver=docker and workspace-driver=directory must be enabled together")
	}
	if c.linuxTargetDriver == "docker" && c.agentDriver != "docker" {
		return fmt.Errorf("linux-target-driver=docker requires the Docker agent and directory workspace drivers")
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
			"observer-output-dir": c.observerOutputRoot != "", "observer-driver": c.observerDriver != "none",
		} {
			if configured {
				return fmt.Errorf("%s may be set only when its physical driver is enabled", name)
			}
		}
		return nil
	}
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

func requireChoice(name, value string, allowed ...string) error {
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("%s=%q is invalid; allowed values: %s", name, value, strings.Join(allowed, ", "))
}

func validContainerPath(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value && value != "/"
}

func validNumericContainerUser(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 32)
		if err != nil || parsed == 0 {
			return false
		}
	}
	return true
}

func configuredBearerSubjects(configuration config) map[string]string {
	result := make(map[string]string)
	if configuration.bearerToken != "" {
		result[configuration.bearerToken] = strings.TrimSpace(configuration.bearerSubject)
	}
	return result
}

func parseSubjects(value string) map[string]bool {
	result := make(map[string]bool)
	for _, subject := range strings.Split(value, ",") {
		if subject = strings.TrimSpace(subject); subject != "" {
			result[subject] = true
		}
	}
	return result
}

func envOr(name, fallback string) string {
	if value, found := os.LookupEnv(name); found {
		return value
	}
	return fallback
}

func environmentBool(name string, fallback bool) (bool, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}

func environmentInt(name string, fallback int) (int, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func environmentDuration(name string, fallback time.Duration) (time.Duration, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	parsed := net.ParseIP(host)
	return parsed != nil && parsed.IsLoopback()
}

func loadServerCredentials(configuration config) (credentials.TransportCredentials, error) {
	if configuration.serverCert == "" && configuration.serverKey == "" && configuration.clientCA == "" {
		return nil, nil
	}
	if configuration.serverCert == "" || configuration.serverKey == "" || configuration.clientCA == "" {
		return nil, fmt.Errorf("tls-cert, tls-key, and client-ca must be configured together")
	}
	certificate, err := tls.LoadX509KeyPair(configuration.serverCert, configuration.serverKey)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	clientPEM, err := os.ReadFile(configuration.clientCA)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(clientPEM) {
		return nil, fmt.Errorf("client CA contains no certificates")
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: clientRoots, MinVersion: tls.VersionTLS13,
	}), nil
}
