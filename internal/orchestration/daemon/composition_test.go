package daemon

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/linuxcontainer"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

// directory-copy-non-production policies bind host.profile.directory-copy-non-production
// (windows and darwin). Physical composition tests that load the e2e fixture
// only run on those host classes.
func requireDirectoryCopyCompositionHost(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
		t.Skip("directory-copy-non-production composition requires windows or darwin")
	}
}

func requireManagedAndroidHostProcess(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("managed Android host-process authority is not implemented on " + runtime.GOOS)
	}
}

func TestManagedAndroidFactoryReleasesAllocatorAfterConstructionFailure(t *testing.T) {
	requireManagedAndroidHostProcess(t)
	config := managedAndroidFactoryTestConfig(t, "127.0.0.1:5037")
	if _, err := newManagedAndroidTargetDriver(config, nil); err == nil || !strings.Contains(err.Error(), "collector gate") {
		t.Fatalf("construction error = %v", err)
	}
	allocator, err := cuttlefish.NewDurableEmulatorAllocator(cuttlefish.DurableEmulatorAllocatorConfig{
		StateRoot: filepath.Join(config.TargetRoot, "allocations"), FirstConsolePort: config.FirstConsolePort,
		LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort, ListenHost: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("construction failure leaked durable allocator ownership: %v", err)
	}
	if err := allocator.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProductionCompositionFactoryBuildsCompleteManagedAndroidDriver(t *testing.T) {
	requireManagedAndroidHostProcess(t)
	readiness := cuttlefish.CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
		return nil
	})
	driver, err := productionCompositionFactories().newAndroid(managedAndroidFactoryTestConfig(t, "127.0.0.1:5037"), readiness)
	if err != nil {
		t.Fatal(err)
	}
	closer, ok := driver.(io.Closer)
	if !ok {
		t.Fatal("production managed Android driver does not implement io.Closer")
	}
	if _, ok := driver.(ports.TargetReconciler); !ok {
		t.Fatal("production managed Android driver does not implement ports.TargetReconciler")
	}
	if _, ok := driver.(ports.TargetRunCrashReconciler); !ok {
		t.Fatal("production managed Android driver does not implement ports.TargetRunCrashReconciler")
	}
	if _, ok := driver.(ports.TargetPhysicalPolicyReporter); !ok {
		t.Fatal("production managed Android driver does not implement ports.TargetPhysicalPolicyReporter")
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close production managed Android driver: %v", err)
	}
}

func managedAndroidFactoryTestConfig(t *testing.T, adbServer string) androidTargetDriverConfig {
	t.Helper()
	digest := domain.NewDigest([]byte("managed-android-image"))
	toolRoot := t.TempDir()
	emulatorBinary := filepath.Join(toolRoot, "emulator")
	if runtime.GOOS == "windows" {
		emulatorBinary += ".exe"
	}
	if err := os.WriteFile(emulatorBinary, []byte("managed Android emulator test executable\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return androidTargetDriverConfig{
		TargetRoot: t.TempDir(), SystemImageRoot: t.TempDir(), SDKRoot: t.TempDir(),
		ADBBinary: filepath.Join(toolRoot, "adb"), ADBServer: adbServer,
		EmulatorBinary: emulatorBinary, SDKManagerBinary: filepath.Join(toolRoot, "sdkmanager"),
		AVDManagerBinary: filepath.Join(toolRoot, "avdmanager"), BackendVersion: "emulator-1", RuntimeVersion: "android-35",
		FirstConsolePort: cuttlefish.ManagedEmulatorMinConsolePort, MaximumTransferBytes: 1 << 20, MaximumADBBytes: 1 << 20, ShutdownTimeout: time.Second,
		SystemImages: map[string]string{digest.String(): "system-images;android-35;google_apis;x86_64"},
	}
}

func TestCloseResourceOnConstructionFailureJoinsCloseError(t *testing.T) {
	constructionErr := errors.New("construct driver")
	closeErr := errors.New("release allocator lock")
	closeCalls := 0
	resource := closeFunc(func() error {
		closeCalls++
		return closeErr
	})
	resultErr := closeResourceOnConstructionFailure(constructionErr, resource, "close test resource")
	if !errors.Is(resultErr, constructionErr) || !errors.Is(resultErr, closeErr) || closeCalls != 1 {
		t.Fatalf("joined construction error = %v; close calls = %d", resultErr, closeCalls)
	}
	if resultErr := closeResourceOnConstructionFailure(nil, resource, "close test resource"); resultErr != nil || closeCalls != 1 {
		t.Fatalf("successful construction closed transferred resource: error=%v calls=%d", resultErr, closeCalls)
	}
}

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func TestConfigureHostDriversUsesProfileAndProbesWithoutDocker(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	fixture := newProfileFixture(t, true)
	fixture.profile.Runs[0].RequiredCoverage = []string{linuxcontainer.IntrinsicSignalFamily}
	observations := testObservationLedger(t)
	configuration := physicalTestConfig(t, fixture, true)
	configuration.captureDriver = "ledger"
	configuration.captureRoot = t.TempDir()
	configuration.maxCaptureRecords = 256
	clock := testkit.NewClock(time.Now().UTC())
	agentFingerprint := supportedFingerprint(t, "agent.docker", "agent.hardened-isolation")
	targetFingerprint := supportedFingerprint(t, "target.linux-container", "target.visibility-first")
	writeProfile(t, fixture.profilePath, fixture.profile)
	agent := testkit.NewFakeAgentWorkspaceDriver(agentFingerprint, clock, nil, nil)
	target := testkit.NewFakeTargetDriver(targetFingerprint, clock, nil, nil)
	workspace := testkit.NewFakeWorkspaceDriver(clock, nil, nil)
	var agentConfig agentDriverConfig
	var targetConfig linuxTargetDriverConfig
	var verified []string
	factories := compositionFactories{
		newWorkspace: func(root string) (ports.WorkspaceDriver, error) {
			if root != configuration.agentWorkspaceRoot {
				t.Fatalf("workspace root = %q", root)
			}
			return workspace, nil
		},
		newAgent: func(config agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
			agentConfig = config
			return reportingAgent(agent), nil
		},
		newTarget: func(config linuxTargetDriverConfig, readiness linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			if readiness == nil {
				t.Fatal("collector readiness was not wired")
			}
			targetConfig = config
			return reportingTarget(target), nil
		},
		verifyImage: func(_ context.Context, _, reference string) error {
			verified = append(verified, reference)
			return nil
		},
	}
	composition, err := configureHostDriversWithFactories(context.Background(), configuration, observations, testPolicyAuthority(t), factories)
	if err != nil {
		t.Fatal(err)
	}
	if composition.agent == nil || composition.workspace != workspace ||
		composition.targets[domain.TargetLinuxContainer] == nil ||
		composition.material == nil || composition.captures == nil ||
		composition.resolver == nil || composition.profileDigest.IsZero() {
		t.Fatalf("incomplete composition: %#v", composition)
	}
	if agentConfig.ImageRepository != "world-e2e:local" || targetConfig.ImageRepository != "world-e2e:local" {
		t.Fatalf("driver repositories = %q, %q", agentConfig.ImageRepository, targetConfig.ImageRepository)
	}
	if len(verified) != 1 || verified[0] != fixture.profile.Acquisitions[0].AgentImage {
		t.Fatalf("verified images = %#v", verified)
	}
}

func TestRunObserverCoordinatorUsesControlTimeoutForDetachedCleanup(t *testing.T) {
	configuration := config{
		orchestrationStateRoot: t.TempDir(),
		controlTimeout:         47 * time.Second,
		shutdownTimeout:        3 * time.Second,
	}
	coordinator := runObserverCoordinatorConfig(configuration, nil, nil)
	if coordinator.CleanupTimeout != configuration.controlTimeout {
		t.Fatalf("observer cleanup timeout = %s, want control timeout %s", coordinator.CleanupTimeout, configuration.controlTimeout)
	}
	if coordinator.CleanupTimeout == configuration.shutdownTimeout {
		t.Fatalf("observer cleanup incorrectly uses RPC shutdown timeout %s", configuration.shutdownTimeout)
	}
}

func TestConfigureHostDriversPreflightsTargetPhysicalPlan(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	tests := map[string]struct {
		wrap func(ports.TargetDriver) ports.TargetDriver
		want string
	}{
		"unsupported enforcement":         {wrap: unsupportedReportingTarget, want: "memory_bytes policy limit is not enforced"},
		"facts drifted after publication": {wrap: driftingReportingTarget, want: "differ from the published config-level facts"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newProfileFixture(t, true)
			configuration := physicalTestConfig(t, fixture, true)
			clock := testkit.NewClock(time.Now().UTC())
			agentFingerprint := supportedFingerprint(t, "agent.docker", "agent.hardened-isolation")
			targetFingerprint := supportedFingerprint(t, "target.linux-container", "target.visibility-first")
			writeProfile(t, fixture.profilePath, fixture.profile)
			factories := compositionFactories{
				newWorkspace: func(string) (ports.WorkspaceDriver, error) {
					return testkit.NewFakeWorkspaceDriver(clock, nil, nil), nil
				},
				newAgent: func(agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
					return reportingAgent(testkit.NewFakeAgentWorkspaceDriver(agentFingerprint, clock, nil, nil)), nil
				},
				newTarget: func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
					return test.wrap(testkit.NewFakeTargetDriver(targetFingerprint, clock, nil, nil)), nil
				},
				verifyImage: func(context.Context, string, string) error { return nil },
			}
			if _, err := configureHostDriversWithFactories(context.Background(), configuration, testObservationLedger(t), testPolicyAuthority(t), factories); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("target preflight error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestConfigureHostDriversFailsClosedOnProbeMismatch(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	fixture := newProfileFixture(t, false)
	configuration := physicalTestConfig(t, fixture, false)
	clock := testkit.NewClock(time.Now().UTC())
	agentFingerprint := supportedFingerprint(t, "agent.docker")
	writeProfile(t, fixture.profilePath, fixture.profile)
	agent := testkit.NewFakeAgentWorkspaceDriver(agentFingerprint, clock, nil, nil)
	factories := compositionFactories{
		newWorkspace: func(string) (ports.WorkspaceDriver, error) {
			return testkit.NewFakeWorkspaceDriver(clock, nil, nil), nil
		},
		newAgent: func(agentDriverConfig) (ports.AgentWorkspaceDriver, error) { return reportingAgent(agent), nil },
		newTarget: func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			t.Fatal("target factory was unexpectedly called")
			return nil, nil
		},
		verifyImage: func(context.Context, string, string) error { return nil },
	}
	if _, err := configureHostDriversWithFactories(context.Background(), configuration, nil, testPolicyAuthority(t), factories); err == nil ||
		!strings.Contains(err.Error(), "agent.hardened-isolation") {
		t.Fatalf("probe mismatch error = %v", err)
	}
}

func TestConfigureHostDriversRejectsMissingPolicyCapabilities(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	fixture := newProfileFixture(t, false)
	configuration := physicalTestConfig(t, fixture, false)
	clock := testkit.NewClock(time.Now().UTC())
	fingerprint := supportedFingerprint(t, "agent.docker", "agent.hardened-isolation")
	// The published policy requires a Linux target composition; an agent-only
	// composition must fail during effective-policy publication.
	factories := compositionFactories{
		newWorkspace: func(string) (ports.WorkspaceDriver, error) {
			return testkit.NewFakeWorkspaceDriver(clock, nil, nil), nil
		},
		newAgent: func(agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
			return reportingAgent(testkit.NewFakeAgentWorkspaceDriver(fingerprint, clock, nil, nil)), nil
		},
		newTarget: func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			t.Fatal("target factory was unexpectedly called")
			return nil, nil
		},
		verifyImage: func(context.Context, string, string) error { return nil },
	}
	if _, err := configureHostDriversWithFactories(context.Background(), configuration, testObservationLedger(t), testPolicyAuthority(t), factories); err == nil ||
		!strings.Contains(err.Error(), "required capability") {
		t.Fatalf("missing policy capability error = %v", err)
	}
}

func TestConfigureHostDriversComposesProbedExternalObserver(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	fixture := newProfileFixture(t, true)
	configuration := physicalTestConfig(t, fixture, true)
	configuration.observerDriver = "process"
	configuration.observerOutputRoot = t.TempDir()
	observer := observerProfile{
		Reference: "process-trace", Adapter: "trace", Version: "v1", SignalFamily: "process",
		Placement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelComplete, Required: true,
		Program: filepath.Join(t.TempDir(), "trusted-observer"), VersionArgs: []string{"--version"},
		Readiness:    observerReadinessProfile{Program: filepath.Join(t.TempDir(), "trusted-ready"), Args: []string{"--ready"}, Interval: "10ms"},
		MaximumBytes: 64 << 10,
	}
	configurationDigest, err := observerprocess.ConfigurationDigest(observerAdapterConfiguration(observer, 10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	observer.ConfigurationDigest = configurationDigest.String()
	observerFingerprint := supportedFingerprint(t, "observer.trace")
	fixture.profile.Observers = []observerProfile{observer}
	fixture.profile.Runs[0].CollectorReferences = []string{observer.Reference}
	fixture.profile.Runs[0].RequiredCoverage = []string{linuxcontainer.IntrinsicSignalFamily, observer.SignalFamily}
	agentFingerprint := supportedFingerprint(t, "agent.docker", "agent.hardened-isolation")
	targetFingerprint := supportedFingerprint(t, "target.linux-container", "target.visibility-first")
	writeProfile(t, fixture.profilePath, fixture.profile)
	observations := testObservationLedger(t)
	clock := testkit.NewClock(time.Now().UTC())
	fakeObserver := testkit.NewFakeObserverDriver(observerFingerprint, clock, nil, nil)
	var observerConfig observerDriverConfig
	factories := compositionFactories{
		newWorkspace: func(string) (ports.WorkspaceDriver, error) {
			return testkit.NewFakeWorkspaceDriver(clock, nil, nil), nil
		},
		newAgent: func(agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
			return reportingAgent(testkit.NewFakeAgentWorkspaceDriver(agentFingerprint, clock, nil, nil)), nil
		},
		newTarget: func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			return reportingTarget(testkit.NewFakeTargetDriver(targetFingerprint, clock, nil, nil)), nil
		},
		newObserver: func(config observerDriverConfig) (ports.ObserverDriver, error) {
			observerConfig = config
			return fakeObserver, nil
		},
		verifyImage: func(context.Context, string, string) error { return nil },
	}
	composition, err := configureHostDriversWithFactories(context.Background(), configuration, observations, testPolicyAuthority(t), factories)
	if err != nil {
		t.Fatal(err)
	}
	if composition.observers == nil || len(observerConfig.Adapters) != 1 || observerConfig.OutputRoot != configuration.observerOutputRoot {
		t.Fatalf("external observer was not composed exactly: %#v %#v", composition, observerConfig)
	}
}

func TestConfigureHostDriversRejectsOverlappingRoots(t *testing.T) {
	requireDirectoryCopyCompositionHost(t)
	fixture := newProfileFixture(t, false)
	configuration := physicalTestConfig(t, fixture, false)
	configuration.agentWorkspaceRoot = fixture.sourceRoot
	clock := testkit.NewClock(time.Now().UTC())
	agentFingerprint := supportedFingerprint(t, "agent.docker", "agent.hardened-isolation")
	writeProfile(t, fixture.profilePath, fixture.profile)
	factories := compositionFactories{
		newWorkspace: func(string) (ports.WorkspaceDriver, error) {
			return testkit.NewFakeWorkspaceDriver(clock, nil, nil), nil
		},
		newAgent: func(agentDriverConfig) (ports.AgentWorkspaceDriver, error) {
			return reportingAgent(testkit.NewFakeAgentWorkspaceDriver(agentFingerprint, clock, nil, nil)), nil
		},
		newTarget: func(linuxTargetDriverConfig, linuxcontainer.CollectorReadiness) (ports.TargetDriver, error) {
			return nil, nil
		},
		verifyImage: func(context.Context, string, string) error { return nil },
	}
	if _, err := configureHostDriversWithFactories(context.Background(), configuration, nil, testPolicyAuthority(t), factories); err == nil ||
		!strings.Contains(err.Error(), "non-overlapping") {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestConfigureHostDriversPreservesLogicalOnlyMode(t *testing.T) {
	configuration := baseTestConfig()
	configuration.agentDriver = "none"
	configuration.workspaceDriver = "none"
	configuration.linuxTargetDriver = "none"
	composition, err := configureHostDriversWithFactories(context.Background(), configuration, nil, nil, compositionFactories{})
	if err != nil {
		t.Fatal(err)
	}
	if composition.agent != nil || composition.workspace != nil || composition.material != nil ||
		composition.captures != nil || composition.resolver != nil || len(composition.targets) != 0 {
		t.Fatalf("logical composition allocated physical dependencies: %#v", composition)
	}
}

func TestConfigValidatesPhysicalDriverMatrix(t *testing.T) {
	logical := baseTestConfig()
	logical.agentDriver, logical.workspaceDriver, logical.linuxTargetDriver = "none", "none", "none"
	if err := logical.validate(); err != nil {
		t.Fatalf("logical config: %v", err)
	}
	invalidReconciliation := logical
	invalidReconciliation.reconciliationInterval = 0
	if err := invalidReconciliation.validate(); err == nil || !strings.Contains(err.Error(), "reconciliation-interval") {
		t.Fatalf("reconciliation interval error = %v", err)
	}
	logicalCapture := logical
	logicalCapture.captureDriver, logicalCapture.captureRoot = "ledger", t.TempDir()
	if err := logicalCapture.validate(); err == nil || !strings.Contains(err.Error(), "physical local material") {
		t.Fatalf("logical capture error = %v", err)
	}
	partial := baseTestConfig()
	partial.agentDriver, partial.workspaceDriver = "docker", "none"
	partial.deploymentProfile = filepath.Join(t.TempDir(), "profile.json")
	partial.agentWorkspaceRoot = t.TempDir()
	if err := partial.validate(); err == nil || !strings.Contains(err.Error(), "enabled together") {
		t.Fatalf("partial driver error = %v", err)
	}
	physical := baseTestConfig()
	physical.agentDriver, physical.workspaceDriver, physical.linuxTargetDriver = "docker", "directory", "docker"
	physical.statePath = filepath.Join(t.TempDir(), "state.db")
	physical.deploymentProfile = filepath.Join(t.TempDir(), "profile.json")
	physical.agentWorkspaceRoot, physical.targetRoot = t.TempDir(), t.TempDir()
	physical.ledgerDirectory, physical.orchestrationStateRoot = t.TempDir(), t.TempDir()
	physical.bundleRoot, physical.materialRoot = t.TempDir(), t.TempDir()
	physical.captureDriver, physical.captureRoot, physical.maxCaptureRecords = "ledger", t.TempDir(), 100
	if err := physical.validate(); err != nil {
		t.Fatalf("physical config: %v", err)
	}
	physical.agentContainerUser = "root"
	if err := physical.validate(); err == nil || !strings.Contains(err.Error(), "uid:gid") {
		t.Fatalf("container user error = %v", err)
	}
	unused := baseTestConfig()
	unused.targetRoot = t.TempDir()
	if err := unused.validate(); err == nil || !strings.Contains(err.Error(), "only when its physical driver is enabled") {
		t.Fatalf("unused physical setting error = %v", err)
	}
}

func TestPhysicalConfigRetainsDriverSelections(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "deployment.json")
	configuration := baseTestConfig()
	configuration.statePath = filepath.Join(t.TempDir(), "state.db")
	configuration.ledgerDirectory = t.TempDir()
	configuration.orchestrationStateRoot = t.TempDir()
	configuration.bundleRoot = t.TempDir()
	configuration.materialRoot = t.TempDir()
	configuration.agentDriver = "docker"
	configuration.workspaceDriver = "directory"
	configuration.linuxTargetDriver = "docker"
	configuration.deploymentProfile = profilePath
	configuration.agentWorkspaceRoot = t.TempDir()
	configuration.targetRoot = t.TempDir()
	configuration.controlTimeout = 7 * time.Second
	configuration.reconciliationInterval = 17 * time.Second
	configuration.reconciliationTimeout = 3 * time.Second
	if err := configuration.validate(); err != nil {
		t.Fatal(err)
	}
	if configuration.agentDriver != "docker" || configuration.workspaceDriver != "directory" ||
		configuration.linuxTargetDriver != "docker" || configuration.deploymentProfile != profilePath ||
		configuration.controlTimeout != 7*time.Second || configuration.reconciliationInterval != 17*time.Second || configuration.reconciliationTimeout != 3*time.Second {
		t.Fatalf("physical selections were not retained: %#v", configuration)
	}
}

func TestManagedAndroidConfigValidation(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "deployment.json")
	androidRoot, imageRoot, sdkRoot := t.TempDir(), t.TempDir(), t.TempDir()
	configuration := baseTestConfig()
	configuration.statePath = filepath.Join(t.TempDir(), "state.db")
	configuration.ledgerDirectory = t.TempDir()
	configuration.orchestrationStateRoot = t.TempDir()
	configuration.bundleRoot = t.TempDir()
	configuration.materialRoot = t.TempDir()
	configuration.agentDriver = "docker"
	configuration.workspaceDriver = "directory"
	configuration.deploymentProfile = profilePath
	configuration.agentWorkspaceRoot = t.TempDir()
	configuration.androidTargetDriver = "android-emulator"
	configuration.androidTargetRoot = androidRoot
	configuration.androidSystemImageRoot = imageRoot
	configuration.androidADBBinary = `C:\Android\adb.exe`
	configuration.androidSDKRoot = sdkRoot
	configuration.androidSDKManagerBinary = `C:\Android\sdkmanager.bat`
	configuration.androidAVDManagerBinary = `C:\Android\avdmanager.bat`
	configuration.androidEmulatorBinary = `C:\Android\emulator.exe`
	configuration.androidBackendVersion = "36.3.10"
	configuration.androidRuntimeVersion = "aosp-35"
	configuration.androidADBBasePort = 5554
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		if err := configuration.validate(); err == nil || !strings.Contains(err.Error(), "android-target-driver=android-emulator") {
			t.Fatalf("unsupported Android host error = %v", err)
		}
		return
	}
	if err := configuration.validate(); err != nil {
		t.Fatal(err)
	}
	if configuration.androidTargetDriver != "android-emulator" || configuration.androidTargetRoot != androidRoot ||
		configuration.androidSystemImageRoot != imageRoot || configuration.androidSDKRoot != sdkRoot || configuration.androidADBBasePort != 5554 {
		t.Fatalf("Android selections were not retained: %#v", configuration)
	}
	for _, unsupported := range []int{
		cuttlefish.ManagedEmulatorMinConsolePort - 2,
		cuttlefish.ManagedEmulatorMinConsolePort + 1,
		cuttlefish.ManagedEmulatorMaxConsolePort + 2,
	} {
		invalid := configuration
		invalid.androidADBBasePort = unsupported
		if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "5554") || !strings.Contains(err.Error(), "5584") {
			t.Fatalf("unsupported Android console port %d error = %v", unsupported, err)
		}
	}
	for _, endpoint := range []string{"localhost:5037", "192.0.2.1:5037", "127.0.0.1:0", " 127.0.0.1:5037"} {
		invalid := configuration
		invalid.androidADBServer = endpoint
		if err := invalid.validate(); err == nil || !strings.Contains(err.Error(), "android-adb-server") {
			t.Fatalf("unsupported Android ADB endpoint %q error = %v", endpoint, err)
		}
	}
}

func physicalTestConfig(t *testing.T, fixture profileFixture, withTarget bool) config {
	t.Helper()
	value := baseTestConfig()
	value.agentDriver = "docker"
	value.workspaceDriver = "directory"
	value.linuxTargetDriver = "none"
	if withTarget {
		value.linuxTargetDriver = "docker"
	}
	value.deploymentProfile = fixture.profilePath
	value.statePath = filepath.Join(t.TempDir(), "state.db")
	value.agentWorkspaceRoot = t.TempDir()
	value.targetRoot = t.TempDir()
	value.materialRoot = fixture.publicationRoot
	value.bundleRoot = t.TempDir()
	value.orchestrationStateRoot = t.TempDir()
	value.ledgerDirectory = t.TempDir()
	value.maxBundleBytes = 1 << 20
	return value
}

func baseTestConfig() config {
	return config{
		statePath: "state.db", ledgerDirectory: "ledger", orchestrationStateRoot: "orchestration",
		bundleRoot: "bundles", materialRoot: "material",
		maxTransferBytes: 1 << 20, maxExecBytes: 1 << 20,
		maxADBBytes: 1 << 20, maxBundleBytes: 1 << 20,
		maxCaptureRecords: 10000,
		probeTimeout:      time.Second, controlTimeout: time.Second, reconciliationInterval: time.Minute,
		reconciliationTimeout: time.Second, shutdownTimeout: time.Second,
		agentDriver: "none", linuxTargetDriver: "none", workspaceDriver: "none",
		materialDriver: "local", androidTargetDriver: "none", physicalTargetDriver: "none",
		observerDriver: "none", captureDriver: "none", dockerBinary: "docker", agentGuestBinary: "/usr/local/bin/world-guest",
		agentContainerUser: "65532:65532",
		androidADBBinary:   "adb", androidADBServer: "127.0.0.1:5037", androidEmulatorBinary: "emulator",
		androidSDKManagerBinary: "sdkmanager", androidAVDManagerBinary: "avdmanager",
		androidADBBasePort: 5554,
	}
}

func supportedFingerprint(t *testing.T, names ...string) domain.CapabilityFingerprint {
	t.Helper()
	capabilities := make(map[string]domain.Capability, len(names))
	for _, name := range names {
		constraints := map[string]string(nil)
		if name == "target.visibility-first" {
			constraints = map[string]string{"runtime": "runc"}
		}
		capability, err := domain.NewCapability(domain.CapabilitySupported, constraints, nil)
		if err != nil {
			t.Fatal(err)
		}
		capabilities[name] = capability
	}
	fingerprint, err := domain.NewCapabilityFingerprint(capabilities, map[string]string{"os": "linux"})
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func testObservationLedger(t *testing.T) *ledger.Ledger {
	t.Helper()
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observations.Close() })
	return observations
}

func testPolicyAuthority(t *testing.T) *policyauthority.Authority {
	t.Helper()
	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "control.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	registry, err := policyregistry.New(controlStore)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := policyauthority.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

type reportingAgentDriver struct{ ports.AgentWorkspaceDriver }

func reportingAgent(driver ports.AgentWorkspaceDriver) ports.AgentWorkspaceDriver {
	return &reportingAgentDriver{AgentWorkspaceDriver: driver}
}

func (d *reportingAgentDriver) AgentWorkspacePhysicalPolicy(context.Context) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	return testAgentPhysicalReport(), nil
}

func (d *reportingAgentDriver) AgentWorkspacePlanPhysicalPolicy(_ context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	report := testAgentPhysicalReport()
	report.Runtime.ImageDigest = plan.ImageDigest.String()
	report.Resources.CPUMilli.Value = plan.Resources.CPUMilli
	report.Resources.MemoryBytes.Value = plan.Resources.MemoryBytes
	report.Resources.SwapBytes.Value = plan.Resources.SwapBytes
	report.Resources.WorkspaceBytes.Value = plan.Resources.StorageBytes
	report.Resources.CaptureBytes.Value = plan.Resources.CaptureBytes
	report.Resources.Inodes.Value = plan.Resources.Inodes
	report.Resources.PIDs.Value = plan.Resources.PIDs
	return report, nil
}

type reportingTargetDriver struct{ ports.TargetDriver }

func reportingTarget(driver ports.TargetDriver) ports.TargetDriver {
	return &reportingTargetDriver{TargetDriver: driver}
}

func (d *reportingTargetDriver) TargetPhysicalPolicy(_ context.Context, template ports.TargetTemplate) (ports.TargetPhysicalPolicyReport, error) {
	return testTargetPhysicalReport(template), nil
}

func (d *reportingTargetDriver) TargetPlanPhysicalPolicy(_ context.Context, plan ports.TargetPlan) (ports.TargetPhysicalPolicyReport, error) {
	report := testTargetPhysicalReport(plan.Template)
	bindTargetPhysicalResourceValues(&report, plan)
	return report, nil
}

type unsupportedReportingTargetDriver struct{ ports.TargetDriver }

func unsupportedReportingTarget(driver ports.TargetDriver) ports.TargetDriver {
	return &unsupportedReportingTargetDriver{TargetDriver: driver}
}

func (d *unsupportedReportingTargetDriver) TargetPhysicalPolicy(_ context.Context, template ports.TargetTemplate) (ports.TargetPhysicalPolicyReport, error) {
	report := testTargetPhysicalReport(template)
	makeTargetMemoryUnsupported(&report)
	return report, nil
}

func (d *unsupportedReportingTargetDriver) TargetPlanPhysicalPolicy(_ context.Context, plan ports.TargetPlan) (ports.TargetPhysicalPolicyReport, error) {
	report := testTargetPhysicalReport(plan.Template)
	bindTargetPhysicalResourceValues(&report, plan)
	makeTargetMemoryUnsupported(&report)
	return report, nil
}

func bindTargetPhysicalResourceValues(report *ports.TargetPhysicalPolicyReport, plan ports.TargetPlan) {
	report.Resources.CPUMilli.Value = plan.Resources.CPUMilli
	report.Resources.MemoryBytes.Value = plan.Resources.MemoryBytes
	report.Resources.SwapBytes.Value = plan.Resources.SwapBytes
	report.Resources.WritableStateBytes.Value = plan.Resources.StorageBytes
	report.Resources.CaptureBytes.Value = plan.Resources.CaptureBytes
	report.Resources.Inodes.Value = plan.Resources.Inodes
	report.Resources.PIDs.Value = plan.Resources.PIDs
}

func makeTargetMemoryUnsupported(report *ports.TargetPhysicalPolicyReport) {
	report.Resources.MemoryBytes.Support = ports.PhysicalSupportUnsupported
	report.Resources.MemoryBytes.Detail = "memory controller is unavailable"
}

type driftingReportingTargetDriver struct{ ports.TargetDriver }

func driftingReportingTarget(driver ports.TargetDriver) ports.TargetDriver {
	return &driftingReportingTargetDriver{TargetDriver: driver}
}

func (d *driftingReportingTargetDriver) TargetPhysicalPolicy(_ context.Context, template ports.TargetTemplate) (ports.TargetPhysicalPolicyReport, error) {
	return testTargetPhysicalReport(template), nil
}

func (d *driftingReportingTargetDriver) TargetPlanPhysicalPolicy(_ context.Context, plan ports.TargetPlan) (ports.TargetPhysicalPolicyReport, error) {
	report := testTargetPhysicalReport(plan.Template)
	bindTargetPhysicalResourceValues(&report, plan)
	report.Runtime.User = "1000:1000"
	return report, nil
}

func testAgentPhysicalReport() ports.AgentWorkspacePhysicalPolicyReport {
	return ports.AgentWorkspacePhysicalPolicyReport{
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: "docker", IsolationProfile: "agent-standard", RootFilesystem: "readOnly", User: "65532:65532",
			CapabilityDrop: []string{"ALL"}, NoNewPrivileges: true, SeccompProfile: "runtime-default",
			UserEnforced: true, SeccompEnforced: true, CapabilitySupport: ports.PhysicalSupportEnforced,
			NoNewPrivilegesSupport: ports.PhysicalSupportEnforced, UserSupport: ports.PhysicalSupportEnforced,
			SeccompSupport: ports.PhysicalSupportEnforced,
		},
		Network: ports.ContainerNetworkPhysicalFacts{
			Mode: "none", DenyPrivateRanges: true, TargetAccess: "none", Support: ports.PhysicalSupportEnforced,
		},
		Resources: enforcedResourceFacts(),
	}
}

func testTargetPhysicalReport(template ports.TargetTemplate) ports.TargetPhysicalPolicyReport {
	return ports.TargetPhysicalPolicyReport{
		Template: template.Name, Kind: string(template.Kind),
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: template.Driver, Runtime: template.Runtime, ImageDigest: template.ImageDigest.String(), IsolationProfile: template.IsolationProfile,
			RootFilesystem: "readOnly", BaseImage: "readOnly", User: "65532:65532", CapabilityDrop: []string{"ALL"},
			NoNewPrivileges: true, SeccompProfile: "runtime-default", UserEnforced: true, SeccompEnforced: true,
			CapabilitySupport: ports.PhysicalSupportEnforced, NoNewPrivilegesSupport: ports.PhysicalSupportEnforced,
			UserSupport: ports.PhysicalSupportEnforced, SeccompSupport: ports.PhysicalSupportEnforced,
		},
		MaterialMountPoint: "/target/input", WritableStateMode: "private-directory-non-production", WritableStateEnforced: false,
		CommandAuthority: "arbitrary-inside-assigned-target", ExecTransport: "direct-argv-and-explicit-shell",
		FileTransfer: "push-pull-target-relative", NetworkEndpoints: "none",
		DeniedInfrastructureAuthority: []string{"host-exec", "docker-api", "host-mounts", "other-targets"},
		ResetAfterEveryRun:            true, ResetMode: "recreate-new-target-generation",
		InteractionSupport: ports.PhysicalSupportEnforced, ResetSupport: ports.PhysicalSupportEnforced,
		Resources: nonProductionTargetResourceFacts(),
	}
}

func nonProductionTargetResourceFacts() ports.ContainerResourcePhysicalFacts {
	resources := enforcedResourceFacts()
	resources.WritableStateBytes.Support = ports.PhysicalSupportUnsupported
	resources.WritableStateBytes.Detail = "bind-backed target state is not byte-quota enforced"
	return resources
}

func enforcedResourceFacts() ports.ContainerResourcePhysicalFacts {
	fact := func() ports.PhysicalLimitFact { return ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced} }
	return ports.ContainerResourcePhysicalFacts{
		CPUMilli: fact(), MemoryBytes: fact(), SwapBytes: fact(), WorkspaceBytes: fact(), WritableStateBytes: fact(),
		CaptureBytes: fact(), Inodes: fact(), PIDs: fact(),
	}
}
