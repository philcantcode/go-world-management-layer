package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
)

func TestMapCapabilityReportPreservesFingerprintEvidence(t *testing.T) {
	capability, err := domain.NewCapability(domain.CapabilitySupported, map[string]string{"bounded": "true"}, map[string]string{"version": "1"})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := domain.NewCapabilityFingerprint(map[string]domain.Capability{"example": capability}, map[string]string{"os": "linux"})
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapCapabilityReport(fingerprint)
	if mapped.Digest != fingerprint.Digest().String() || mapped.Evidence["os"] != "linux" || mapped.Capabilities["example"].Status != "supported" || mapped.Capabilities["example"].Constraints["bounded"] != "true" {
		t.Fatalf("mapped report = %#v", mapped)
	}
	// Public accessors and the report must not alias the immutable domain model.
	mapped.Evidence["os"] = "mutated"
	if fingerprint.Evidence()["os"] != "linux" {
		t.Fatal("mapped report aliases capability fingerprint evidence")
	}
}

func TestReadPolicySourceRequiresNonEmptyBoundedRegularFile(t *testing.T) {
	root := t.TempDir()
	validPath := filepath.Join(root, "policy.yaml")
	if err := os.WriteFile(validPath, []byte("policy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if source, err := readPolicySource(validPath); err != nil || string(source) != "policy" {
		t.Fatalf("read valid policy = %q, %v", source, err)
	}
	emptyPath := filepath.Join(root, "empty.yaml")
	if err := os.WriteFile(emptyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPolicySource(emptyPath); err == nil {
		t.Fatal("empty policy was accepted")
	}
	largePath := filepath.Join(root, "large.yaml")
	if err := os.WriteFile(largePath, bytes.Repeat([]byte{'x'}, int(maximumPolicyBytes)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPolicySource(largePath); err == nil {
		t.Fatal("oversized policy was accepted")
	}
	if _, err := readPolicySource(root); err == nil {
		t.Fatal("directory policy source was accepted")
	}
}

func TestRunRejectsUnprobeableCompositionBeforeContactingDocker(t *testing.T) {
	if err := run([]string{"--workspace-driver", "overlayfs"}); err == nil || !strings.Contains(err.Error(), "cannot be truthfully") {
		t.Fatalf("unprobeable workspace error = %v", err)
	}
	nonexistentDocker := filepath.Join(t.TempDir(), "docker-must-not-run")
	if err := run([]string{"--docker-binary", nonexistentDocker, "--observer-driver", "process"}); err == nil || !strings.Contains(err.Error(), "observer") {
		t.Fatalf("incomplete process observer error = %v", err)
	}
	if err := run([]string{"--observer-reference", "unexpected"}); err == nil || !strings.Contains(err.Error(), "require observer-driver=process") {
		t.Fatalf("ambiguous observer flags error = %v", err)
	}
}

func TestProcessObserverOptionsProduceRealExactProbe(t *testing.T) {
	options := exactProcessObserverOptions(t)
	if err := validateProcessObserverOptions(options); err != nil {
		t.Fatal(err)
	}
	var invocation command.Invocation
	runner := capabilityRunnerFunc(func(_ context.Context, value command.Invocation) (command.Result, error) {
		invocation = value
		return command.Result{Stdout: []byte("Android Debug Bridge version 1.0.41\r\n")}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fingerprint, digest, err := probeProcessObserver(ctx, options, runner)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := observerprocess.ConfigurationDigest(options.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	if digest != wantDigest {
		t.Fatalf("reported configuration digest = %s, want %s", digest.String(), wantDigest.String())
	}
	capability, found := fingerprint.Capability("observer.logcat")
	if !found || capability.Status() != domain.CapabilitySupported || capability.Constraints()["configuration_digest"] != wantDigest.String() || capability.Constraints()["runtime_binding"] != observerprocess.RuntimeBindingAndroidExactADB.String() {
		t.Fatalf("observer capability = %#v, found=%t", capability, found)
	}
	if invocation.Program != options.Configuration.Program || !reflect.DeepEqual(invocation.Args, options.Configuration.VersionArgs) {
		t.Fatalf("real version probe invocation = %#v", invocation)
	}
}

func TestGenericProcessObserverBindingRoundTripsThroughCapabilityAndCLI(t *testing.T) {
	options := genericProcessObserverOptions(t)
	runner := capabilityRunnerFunc(func(_ context.Context, _ command.Invocation) (command.Result, error) {
		return command.Result{Stdout: []byte("observer 1\n")}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fingerprint, _, err := probeProcessObserver(ctx, options, runner)
	if err != nil {
		t.Fatal(err)
	}
	capability, found := fingerprint.Capability("observer.process")
	if !found || capability.Status() != domain.CapabilitySupported {
		t.Fatalf("generic observer capability = %#v, found=%t", capability, found)
	}
	reportedBinding := capability.Constraints()["runtime_binding"]
	if reportedBinding != "none" {
		t.Fatalf("generic observer runtime-binding constraint = %q, want %q", reportedBinding, "none")
	}

	arguments := []string{
		"--observer-driver", "process",
		"--observer-reference", options.Reference,
		"--observer-adapter", options.Configuration.Adapter,
		"--observer-version", options.Configuration.Version,
		"--observer-signal-family", options.Configuration.SignalFamily,
		"--observer-placement", string(options.Configuration.Placement),
		"--observer-coverage-level", string(options.Configuration.CoverageLevel),
		"--observer-runtime-binding", reportedBinding,
		"--observer-program", options.Configuration.Program,
		"--observer-version-arg=" + options.Configuration.VersionArgs[0],
		"--observer-readiness-program", options.Configuration.ReadinessProgram,
		"--observer-readiness-arg", options.Configuration.ReadinessArgs[0],
		"--observer-readiness-interval", options.Configuration.ReadinessInterval.String(),
		"--capture-driver", "invalid-after-observer-validation",
	}
	if err := run(arguments); err == nil || !strings.Contains(err.Error(), "capture-driver must be none or ledger") {
		t.Fatalf("capability runtime binding did not pass CLI observer validation: %v", err)
	}
}

func TestParseObserverEnvironmentRejectsDuplicateOrAmbiguousEntries(t *testing.T) {
	values, err := parseObserverEnvironment([]string{"LANG=C", "VALUE=contains=equals"})
	if err != nil || values["LANG"] != "C" || values["VALUE"] != "contains=equals" {
		t.Fatalf("parsed environment = %#v, %v", values, err)
	}
	for _, entries := range [][]string{{"missing-separator"}, {"NAME=first", "NAME=second"}} {
		if _, err := parseObserverEnvironment(entries); err == nil {
			t.Fatalf("ambiguous environment %v was accepted", entries)
		}
	}
}

func exactProcessObserverOptions(t *testing.T) processObserverOptions {
	t.Helper()
	adb := filepath.Join(t.TempDir(), "adb.exe")
	return processObserverOptions{
		Reference: "android-logcat",
		Configuration: observerprocess.AdapterConfiguration{
			Adapter: "logcat", Version: "1", SignalFamily: "android.logcat",
			Placement: domain.CollectorPlacementGuest, CoverageLevel: domain.CoverageLevelPartial,
			RuntimeBinding: observerprocess.RuntimeBindingAndroidExactADB,
			Program:        adb,
			Args:           []string{"logcat", "-v", "threadtime"},
			VersionArgs:    []string{"version"}, ReadinessProgram: adb,
			ReadinessArgs:     []string{"get-state"},
			ReadinessInterval: 250 * time.Millisecond,
		},
	}
}

func genericProcessObserverOptions(t *testing.T) processObserverOptions {
	t.Helper()
	program := filepath.Join(t.TempDir(), "observer.exe")
	return processObserverOptions{
		Reference: "process-stdout",
		Configuration: observerprocess.AdapterConfiguration{
			Adapter: "process", Version: "1", SignalFamily: "process.stdout",
			Placement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelPartial,
			RuntimeBinding: observerprocess.RuntimeBindingNone,
			Program:        program, VersionArgs: []string{"--version"}, ReadinessProgram: program,
			ReadinessArgs: []string{"ready"}, ReadinessInterval: 250 * time.Millisecond,
		},
	}
}

type capabilityRunnerFunc func(context.Context, command.Invocation) (command.Result, error)

func (function capabilityRunnerFunc) Run(ctx context.Context, invocation command.Invocation) (command.Result, error) {
	return function(ctx, invocation)
}

func TestRunRejectsUnsupportedAndroidDriverBeforeContactingDocker(t *testing.T) {
	if err := run([]string{"--android-target-driver", "attached"}); err == nil || !strings.Contains(err.Error(), "must be android-emulator or none") {
		t.Fatalf("unsupported Android driver error = %v", err)
	}
}

func TestValidateAndroidProbeOptionsBindsExactManagedRuntime(t *testing.T) {
	options := androidProbeOptions{
		SDKRoot: t.TempDir(), ADBBinary: "adb", ADBServer: "127.0.0.1:5037",
		EmulatorBinary: "emulator", SDKManagerBinary: "sdkmanager", AVDManagerBinary: "avdmanager",
		BackendVersion: "emulator-36.3.10", RuntimeVersion: "android-build-fingerprint",
		SystemImageDigest:  domain.NewDigest([]byte("system-image")).String(),
		SystemImagePackage: "system-images;android-35;google_apis;x86_64",
		IsolationProfile:   "instrumented-android", BaselineState: "clean-boot",
		BaseConsolePort: cuttlefish.ManagedEmulatorMinConsolePort, GuestMemoryBytes: 2 << 30, BootTimeout: 2 * time.Minute,
	}
	if err := validateAndroidProbeOptions(options); err != nil {
		t.Fatal(err)
	}
	options.ADBServer = "192.0.2.1:5037"
	if err := validateAndroidProbeOptions(options); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("remote ADB endpoint error = %v", err)
	}
	options.ADBServer = "127.0.0.1:5037"
	options.BaselineState = "named-snapshot"
	if err := validateAndroidProbeOptions(options); err == nil || !strings.Contains(err.Error(), "clean-boot") {
		t.Fatalf("unsupported Android baseline error = %v", err)
	}
}

func TestManagedAndroidDeviceIdentityBindsEveryProbeOption(t *testing.T) {
	options := androidProbeOptions{
		SDKRoot: `C:\Android\Sdk`, ADBBinary: `C:\Android\adb.exe`, ADBServer: "127.0.0.1:5037",
		EmulatorBinary: `C:\Android\emulator.exe`, SDKManagerBinary: `C:\Android\sdkmanager.bat`,
		AVDManagerBinary: `C:\Android\avdmanager.bat`, BackendVersion: "emulator-36.3.10",
		RuntimeVersion: "android-build-fingerprint", SystemImageDigest: domain.NewDigest([]byte("system-image")).String(),
		SystemImagePackage: "system-images;android-35;google_apis;x86_64", BaseConsolePort: 5580,
	}
	want := cuttlefish.ManagedEmulatorDeviceConfigIdentity{
		EmulatorBinary: options.EmulatorBinary, ADBBinary: options.ADBBinary,
		SDKManagerBinary: options.SDKManagerBinary, AVDManagerBinary: options.AVDManagerBinary,
		SDKRoot: options.SDKRoot, ADBServerEndpoint: options.ADBServer,
		ExpectedBackendVersion: options.BackendVersion, ExpectedRuntimeVersion: options.RuntimeVersion,
		BaseConsolePort: options.BaseConsolePort, LastConsolePort: cuttlefish.ManagedEmulatorMaxConsolePort,
		SystemImages: map[string]cuttlefish.ManagedSystemImage{
			options.SystemImageDigest: {Package: options.SystemImagePackage},
		},
	}
	if got := managedAndroidDeviceIdentity(options); !reflect.DeepEqual(got, want) {
		t.Fatalf("managed Android identity = %#v, want %#v", got, want)
	}
}

func TestManagedAndroidDeviceIdentityUsesCanonicalBackendExecutable(t *testing.T) {
	options := androidProbeOptions{
		SDKRoot: t.TempDir(), ADBBinary: "adb", ADBServer: "127.0.0.1:5037",
		EmulatorBinary: "emulator", SDKManagerBinary: "sdkmanager", AVDManagerBinary: "avdmanager",
		BackendVersion: "emulator-version", RuntimeVersion: "runtime-version",
		SystemImageDigest:  domain.NewDigest([]byte("system-image")).String(),
		SystemImagePackage: "system-images;android-35;google_apis;x86_64",
		BaseConsolePort:    cuttlefish.ManagedEmulatorMinConsolePort,
	}
	canonical := filepath.Join(options.SDKRoot, "emulator", "emulator.exe")
	identity := managedAndroidDeviceIdentityWithEmulator(options, canonical)
	if identity.EmulatorBinary != canonical {
		t.Fatalf("emulator identity = %q, want canonical backend identity %q", identity.EmulatorBinary, canonical)
	}
	if _, err := cuttlefish.ManagedEmulatorDeviceConfigDigest(managedAndroidDeviceIdentity(options)); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("unresolved PATH executable was fingerprinted: %v", err)
	}
	canonicalDigest, err := cuttlefish.ManagedEmulatorDeviceConfigDigest(identity)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalDigest.IsZero() {
		t.Fatal("canonical emulator executable produced a zero managed device fingerprint")
	}
}

func TestRunRejectsIncompleteManagedAndroidBeforeContactingDocker(t *testing.T) {
	if err := run([]string{"--android-target-driver", "android-emulator"}); err == nil || !strings.Contains(err.Error(), "when android-target-driver=android-emulator") {
		t.Fatalf("incomplete Android probe error = %v", err)
	}
}
