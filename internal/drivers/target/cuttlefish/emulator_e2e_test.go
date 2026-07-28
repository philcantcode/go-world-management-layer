package cuttlefish

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// TestAttachedEmulatorDriverEndToEnd is an opt-in real-system qualification.
// It assigns one already-running SDK emulator, projects run material through
// exact-serial ADB, exposes only the run-scoped ADB server, installs and
// launches the specimen APK through that capability, rejects a foreign serial,
// then proves teardown revokes the endpoint without stopping the external AVD.
func TestAttachedEmulatorDriverEndToEnd(t *testing.T) {
	if os.Getenv("WORLD_ANDROID_EMULATOR_E2E") != "1" {
		t.Skip("set WORLD_ANDROID_EMULATOR_E2E=1 to exercise a running SDK emulator")
	}
	serial := valueOrDefault(os.Getenv("WORLD_ANDROID_EMULATOR_SERIAL"), "emulator-5554")
	adb := valueOrDefault(os.Getenv("WORLD_ANDROID_ADB"), "adb")
	apk := os.Getenv("WORLD_ANDROID_SPECIMEN_APK")
	if apk == "" {
		apk = filepath.Join("..", "..", "..", "..", "testdata", "e2e", "android-specimen", "build", "world-specimen.apk")
	}
	apk, err := filepath.Abs(apk)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(apk); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("specimen APK %q is unavailable: %v", apk, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	root := t.TempDir()
	backend, err := NewAttachedEmulatorBackend(AttachedEmulatorBackendConfig{ADBBinary: adb, Serial: serial})
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := NewAttachedEmulatorAllocator(serial)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewDeviceProxyGateway(deviceproxy.GatewayConfig{
		UpstreamAddress: "127.0.0.1:5037", MaximumConnections: 8,
		MaximumConnectionDuration: time.Minute, MaximumStreamBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	files, err := NewCommandFileGateway(CommandFileGatewayConfig{
		ADBBinary: adb, StagingRoot: filepath.Join(root, "adb-staging"), MaximumTransferBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	driver, err := New(Config{
		Build: BuildConfig{
			TargetRoot: filepath.Join(root, "targets"), SystemImageRoot: filepath.Join(root, "images"),
			BackendVersion: defaultAttachedEmulatorBackendVersion, RuntimeVersion: "attached-runtime",
			DeviceConfigDigest: domain.NewDigest([]byte("world-e2e-attached-emulator")), Features: []string{"scoped-adb"},
		},
		Backend: backend, Allocator: allocator, Gateway: gateway, Files: files,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	template := ports.TargetTemplate{
		Name: "android-e2e", Kind: domain.TargetAndroidVirtualDevice, Driver: "attached-emulator",
		Runtime: "android-sdk", ImageDigest: domain.NewDigest([]byte("api-35-system-image")), IsolationProfile: "attached-e2e",
	}
	if _, err := driver.Probe(ctx, template); err != nil {
		t.Fatalf("probe attached emulator: %v", err)
	}

	leaseID, _ := domain.NewLeaseID()
	sessionID, _ := domain.NewResearchSessionID()
	targetID, _ := domain.NewTargetID()
	policyDigest := domain.NewDigest([]byte("android-e2e-policy"))
	capabilityDigest := domain.NewDigest([]byte("android-e2e-capabilities"))
	createdAt := time.Now().UTC()
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: 1, PolicyDigest: policyDigest,
		CapabilityFingerprintDigest: capabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.Create(ctx, ports.TargetPlan{
		IdempotencyKey: "android-e2e-create", LeaseID: leaseID, Target: target, Generation: generation,
		Template: template, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Status.Ready || result.Status.DeviceSerial != serial {
		t.Fatalf("attached target was not ready on exact serial: %#v", result.Status)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := driver.Destroy(cleanupContext, ports.TargetRef{ID: targetID, Generation: 1}); err != nil {
			t.Errorf("destroy attached target ownership: %v", err)
		}
	}()

	material := []ports.TargetMaterialPlan{targetMaterial(t, "fixture/payload.txt", 0o600, []byte("android-e2e-material\n"), nil)}
	runPlan := targetRunPlanForMaterial(t, leaseID, targetID, material, "android-e2e-run")
	prepared, err := driver.PrepareRun(ctx, runPlan)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = driver.StopRun(cleanupContext, runPlan.Run.ID(), ports.StopForce)
	}()
	if prepared.RunID != runPlan.Run.ID() || prepared.Attachment.RuntimeID == "" || prepared.Attachment.RuntimeID == serial {
		// The serial remains in the opaque scoped capability, never in the
		// host-level observation attachment.
		t.Fatalf("prepared run leaked or changed identity: %#v", prepared)
	}
	if err := driver.StartRun(ctx, runPlan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	transport, err := driver.OpenTransport(ctx, runPlan.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := transport.OpenADB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(endpoint.Address())
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"-H", host, "-P", port}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "install", "-r", apk)...); err != nil {
		t.Fatalf("install APK through scoped endpoint: %v\n%s", err, output)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "am", "start", "-W", "-n", "dev.philcantcode.worldspecimen/.MainActivity")...); err != nil || !strings.Contains(output, "Status: ok") {
		t.Fatalf("launch APK through scoped endpoint: %v\n%s", err, output)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "cmd", "package", "path", "dev.philcantcode.worldspecimen")...); err != nil || !strings.Contains(output, "package:") {
		t.Fatalf("query installed APK through scoped endpoint: %v\n%s", err, output)
	}
	reportOutput, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "run-as", "dev.philcantcode.worldspecimen", "cat", "files/world-report.json")...)
	if err != nil {
		t.Fatalf("read specimen report through scoped endpoint: %v\n%s", err, reportOutput)
	}
	var report struct {
		Package                 string `json:"package"`
		SDK                     int    `json:"sdk"`
		Mode                    string `json:"mode"`
		HostDockerSocketVisible bool   `json:"host_docker_socket_visible"`
		HostWorkspaceVisible    bool   `json:"host_workspace_visible"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(reportOutput)), &report); err != nil {
		t.Fatalf("decode specimen report %q: %v", reportOutput, err)
	}
	if report.Package != "dev.philcantcode.worldspecimen" || report.SDK < 23 || report.Mode != "normal" || report.HostDockerSocketVisible || report.HostWorkspaceVisible {
		t.Fatalf("specimen report violated the attached-device boundary: %#v", report)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "am", "force-stop", "dev.philcantcode.worldspecimen")...); err != nil {
		t.Fatalf("stop specimen before crash probe: %v\n%s", err, output)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "logcat", "-c")...); err != nil {
		t.Fatalf("clear device log before crash probe: %v\n%s", err, output)
	}
	crashLaunch, crashLaunchErr := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "am", "start", "-W", "-n", "dev.philcantcode.worldspecimen/.MainActivity", "--es", "mode", "crash")...)
	if crashLaunchErr != nil && !strings.Contains(crashLaunch, "requested world specimen crash") {
		t.Fatalf("request specimen crash through scoped endpoint: %v\n%s", crashLaunchErr, crashLaunch)
	}
	crashDeadline := time.Now().Add(10 * time.Second)
	var crashLog string
	for time.Now().Before(crashDeadline) {
		crashLog, err = runADBCommand(ctx, adb, append(base, "-s", serial, "logcat", "-d", "-t", "400")...)
		if err == nil && strings.Contains(crashLog, "requested world specimen crash") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(crashLog, "requested world specimen crash") {
		t.Fatalf("Android did not record the requested specimen crash: %v\n%s", err, crashLog)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "pidof", "dev.philcantcode.worldspecimen")...); err == nil && strings.TrimSpace(output) != "" {
		t.Fatalf("crashed specimen process remained alive: %s", output)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "shell", "am", "start", "-W", "-n", "dev.philcantcode.worldspecimen/.MainActivity")...); err != nil || !strings.Contains(output, "Status: ok") {
		t.Fatalf("relaunch specimen after crash through scoped endpoint: %v\n%s", err, output)
	}
	foreign := "emulator-5556"
	if serial == foreign {
		foreign = "emulator-5558"
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", foreign, "get-state")...); err == nil {
		t.Fatalf("foreign serial unexpectedly crossed scoped endpoint: %s", output)
	}

	receipt, err := driver.StopRun(ctx, runPlan.Run.ID(), ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != ports.RunCompleted || receipt.FailureKind != ports.TargetRunFailureNone {
		t.Fatalf("Android run did not stop cleanly: %#v", receipt)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if output, err := runADBCommand(ctx, adb, append(base, "-s", serial, "get-state")...); err == nil {
		t.Fatalf("revoked scoped endpoint remained usable: %s", output)
	}
	if output, err := runADBCommand(ctx, adb, "-s", serial, "get-state"); err != nil || strings.TrimSpace(output) != "device" {
		t.Fatalf("attached emulator was stopped instead of detached: %v\n%s", err, output)
	}
}

func runADBCommand(ctx context.Context, adb string, arguments ...string) (string, error) {
	command := exec.CommandContext(ctx, adb, arguments...)
	output, err := command.CombinedOutput()
	return string(output), err
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
