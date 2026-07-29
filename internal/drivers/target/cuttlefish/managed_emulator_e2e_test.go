package cuttlefish

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// TestManagedEmulatorDriverEndToEnd qualifies the production Android path
// against an installed SDK image. It creates a headless accelerated AVD,
// projects real bytes, exercises and crashes the specimen APK only through the
// scoped ADB capability, proves the mutable generation is single-use, resets
// to a clean separately allocated generation, re-adopts it after a simulated
// daemon restart, and destroys the exact AVD.
func TestManagedEmulatorDriverEndToEnd(t *testing.T) {
	if os.Getenv("WORLD_ANDROID_MANAGED_E2E") != "1" {
		t.Skip("set WORLD_ANDROID_MANAGED_E2E=1 to qualify managed SDK-emulator lifecycle")
	}
	sdkRoot := managedE2ESDKRoot(t)
	imagePackage := valueOrDefault(os.Getenv("WORLD_ANDROID_SYSTEM_IMAGE_PACKAGE"), "system-images;android-35;google_apis;x86_64")
	if err := ValidateManagedSystemImagePackage(imagePackage); err != nil {
		t.Fatal(err)
	}
	imageDirectory := filepath.Join(append([]string{sdkRoot}, strings.Split(imagePackage, ";")...)...)
	imageDigest, err := DigestManagedSystemImage(imageDirectory)
	if err != nil {
		t.Fatalf("digest installed Android system image %q: %v", imageDirectory, err)
	}
	emulator := managedE2ETool(t, "WORLD_ANDROID_EMULATOR", filepath.Join(sdkRoot, "emulator", executableName("emulator", ".exe")))
	adb := managedE2ETool(t, "WORLD_ANDROID_ADB", filepath.Join(sdkRoot, "platform-tools", executableName("adb", ".exe")))
	sdkManager := managedE2ETool(t, "WORLD_ANDROID_SDKMANAGER", filepath.Join(sdkRoot, "cmdline-tools", "latest", "bin", executableName("sdkmanager", ".bat")))
	avdManager := managedE2ETool(t, "WORLD_ANDROID_AVDMANAGER", filepath.Join(sdkRoot, "cmdline-tools", "latest", "bin", executableName("avdmanager", ".bat")))
	apk := valueOrDefault(os.Getenv("WORLD_ANDROID_SPECIMEN_APK"), filepath.Join("..", "..", "..", "..", "testdata", "e2e", "android-specimen", "build", "world-specimen.apk"))
	apk, err = filepath.Abs(apk)
	if err != nil {
		t.Fatal(err)
	}
	requireRegularManagedE2EFile(t, apk)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	root := t.TempDir()
	targetRoot := filepath.Join(root, "targets")
	imageBindingRoot := filepath.Join(root, "image-bindings")
	allocatorRoot := filepath.Join(root, "allocator")
	firstPort := findFreeEvenPortPairs(t, 2)
	lastPort := firstPort + 2
	images := map[string]ManagedSystemImage{imageDigest.String(): {Package: imagePackage, Directory: imageDirectory}}
	backendConfig := ManagedEmulatorBackendConfig{
		EmulatorBinary: emulator, ADBBinary: adb, SDKManagerBinary: sdkManager, AVDManagerBinary: avdManager,
		SDKRoot: sdkRoot, StateRoot: targetRoot, ADBServerEndpoint: DefaultADBServerEndpoint, SystemImages: images,
		PollInterval: 500 * time.Millisecond, ShutdownTimeout: 30 * time.Second,
	}
	backend, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	template := completeAndroidTemplate("android-managed-e2e", "android-emulator", imageDigest)
	template.BootTimeout = 8 * time.Minute
	capabilities, err := backend.Probe(ctx, template)
	if err != nil {
		t.Fatalf("probe installed managed Android runtime: %v", err)
	}
	deviceConfigDigest, err := ManagedEmulatorDeviceConfigDigest(ManagedEmulatorDeviceConfigIdentity{
		EmulatorBinary: backend.EmulatorExecutableIdentity(), ADBBinary: adb, SDKManagerBinary: sdkManager, AVDManagerBinary: avdManager,
		SDKRoot: sdkRoot, ADBServerEndpoint: "127.0.0.1:5037",
		ExpectedBackendVersion: capabilities.BackendVersion, ExpectedRuntimeVersion: capabilities.RuntimeVersion,
		BaseConsolePort: firstPort, LastConsolePort: lastPort, SystemImages: images,
	})
	if err != nil {
		t.Fatal(err)
	}
	build := BuildConfig{
		TargetRoot: targetRoot, SystemImageRoot: imageBindingRoot,
		ADBServerEndpoint: DefaultADBServerEndpoint,
		BackendVersion:    capabilities.BackendVersion, RuntimeVersion: capabilities.RuntimeVersion,
		DeviceConfigDigest: deviceConfigDigest, Features: []string{"headless", "root", "scoped-adb", "exact-data-storage"},
	}
	allocatorConfig := DurableEmulatorAllocatorConfig{
		StateRoot: allocatorRoot, FirstConsolePort: firstPort, LastConsolePort: lastPort, ListenHost: "127.0.0.1",
	}
	allocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer allocator.Close()
	gateway, err := NewDeviceProxyGateway(deviceproxy.GatewayConfig{
		UpstreamAddress: "127.0.0.1:5037", MaximumConnections: 8,
		MaximumConnectionDuration: 15 * time.Minute, MaximumStreamBytes: 128 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	files, err := NewCommandFileGateway(CommandFileGatewayConfig{
		ADBBinary: adb, ADBServerEndpoint: DefaultADBServerEndpoint, StagingRoot: filepath.Join(root, "adb-staging"), MaximumTransferBytes: 128 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	driver := newManagedE2EDriver(t, build, backend, allocator, gateway, files)
	if _, err := driver.Probe(ctx, template); err != nil {
		t.Fatalf("probe production Android driver: %v", err)
	}
	input := managedE2ETargetPlan(t, template)
	created, err := driver.Create(ctx, input)
	if err != nil {
		t.Fatalf("create managed Android target: %v\n%s", err, managedE2EErrorTree(err))
	}
	createdInstance := driver.targets[deviceKey(input.Target.ID(), 1)].instance
	cleanupInstances := []Instance{createdInstance}
	cleanupBackend := backend
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cleanupCancel()
		for index := len(cleanupInstances) - 1; index >= 0; index-- {
			if err := cleanupBackend.Destroy(cleanupContext, cleanupInstances[index]); err != nil {
				t.Errorf("destroy managed E2E cleanup instance %q: %v", cleanupInstances[index].RuntimeID, err)
			}
		}
	})
	if !created.Status.Ready || created.Status.DeviceSerial == "" {
		t.Fatalf("managed Android target is not ready: %#v", created)
	}
	oldSerial := created.Status.DeviceSerial
	requireManagedE2EOwnershipResources(t, ctx, backend, createdInstance)
	requireManagedE2EDataPartition(t, ctx, backend, createdInstance, "generation 1")

	material := []ports.TargetMaterialPlan{targetMaterial(t, "fixture/payload.txt", 0o600, []byte("managed-emulator-real-data\n"), nil)}
	runPlan := targetRunPlanForGeneration(t, input.LeaseID, input.Target.ID(), 1, material, "managed-emulator-e2e-run")
	runPlan.MaximumDuration = 3 * time.Minute
	if _, err := driver.PrepareRun(ctx, runPlan); err != nil {
		t.Fatalf("prepare managed Android run: %v", err)
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
	proxyServer, err := parseADBServerEndpoint(net.JoinHostPort(host, port))
	if err != nil {
		t.Fatal(err)
	}
	const mutationPath = "/data/local/tmp/world-managed-e2e-mutation"
	const heartbeatPath = "/data/local/tmp/world-managed-e2e-heartbeat"
	writeMutation := adbRemoteShellCommand("printf world-managed-change > " + mutationPath)
	if output, err := runExactADBCommand(ctx, adb, proxyServer, oldSerial, writeMutation...); err != nil {
		t.Fatalf("write scoped guest mutation: %v\n%s", err, output)
	}
	if output, err := runExactADBCommand(ctx, adb, proxyServer, oldSerial, "shell", "cat", mutationPath); err != nil || strings.TrimSpace(output) != "world-managed-change" {
		t.Fatalf("read scoped guest mutation: %v\n%s", err, output)
	}
	heartbeatCommand := "while true; do echo tick >> " + heartbeatPath + "; sleep 1; done"
	launchHeartbeat, err := exactSerialADBArguments(proxyServer, oldSerial, adbRemoteShellCommand(heartbeatCommand)...)
	if err != nil {
		t.Fatal(err)
	}
	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	heartbeat := exec.CommandContext(heartbeatContext, adb, launchHeartbeat...)
	var heartbeatOutput bytes.Buffer
	heartbeat.Stdout, heartbeat.Stderr = &heartbeatOutput, &heartbeatOutput
	if err := heartbeat.Start(); err != nil {
		t.Fatalf("start persistent scoped guest mutation stream: %v", err)
	}
	heartbeatDone := make(chan struct{})
	var heartbeatErr error
	go func() {
		heartbeatErr = heartbeat.Wait()
		close(heartbeatDone)
	}()
	if err := waitForManagedE2EHeartbeatGrowth(ctx, adb, proxyServer, oldSerial, heartbeatPath, heartbeatDone); err != nil {
		cancelHeartbeat()
		<-heartbeatDone
		t.Fatalf("observe persistent scoped guest mutation stream: %v (adb=%v)\n%s", err, heartbeatErr, heartbeatOutput.String())
	}
	exerciseAndroidSpecimen(t, ctx, adb, oldSerial, apk, proxyServer)
	receipt, err := driver.StopRun(ctx, runPlan.Run.ID(), ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-heartbeatDone:
		if heartbeatErr == nil {
			t.Fatal("persistent scoped ADB mutation stream exited successfully instead of being revoked by StopRun")
		}
	case <-time.After(20 * time.Second):
		cancelHeartbeat()
		t.Fatal("persistent scoped ADB mutation stream remained live after StopRun")
	}
	changes := receipt.TargetChanges.Entries()
	if len(changes) != 1 || changes[0].Kind() != domain.ChangeOpaqueDirectory || changes[0].Path() != "." {
		t.Fatalf("arbitrary ADB mutation was not reported as root-opaque: %#v", changes)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, oldSerial, "get-state"); err == nil {
		t.Fatalf("stopped run's exact serial remained reachable: %s", output)
	}
	if running, known := backend.ownedProcessRunning(created.Status.RuntimeID); !known || running {
		t.Fatalf("persistent guest mutation could still execute after StopRun: process known=%t running=%t", known, running)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	blocked := targetRunPlanForGeneration(t, input.LeaseID, input.Target.ID(), 1, material, "managed-emulator-e2e-blocked")
	if _, err := driver.PrepareRun(ctx, blocked); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("mutable managed generation was reusable without reset: %v", err)
	}

	reset, err := driver.Reset(ctx, input.Target.ID(), ports.ResetPlan{
		IdempotencyKey: "managed-emulator-e2e-reset", LeaseID: input.LeaseID,
		Previous: ports.TargetRef{ID: input.Target.ID(), Generation: 1}, NextGeneration: 2, Mode: ports.ResetBaseline,
	})
	if err != nil {
		t.Fatalf("reset managed Android target: %v\n%s", err, managedE2EErrorTree(err))
	}
	if !reset.Status.Ready || reset.Status.DeviceSerial == oldSerial {
		t.Fatalf("reset did not create an independent reachable generation: %#v", reset)
	}
	newSerial := reset.Status.DeviceSerial
	// Reset has already destroyed generation 1, including its exact process
	// authority. Cleanup therefore owns only the live replacement from here.
	cleanupInstances = []Instance{driver.targets[deviceKey(input.Target.ID(), 2)].instance}
	requireManagedE2EDataPartition(t, ctx, backend, cleanupInstances[0], "generation 2 after reset")
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, oldSerial, "get-state"); err == nil {
		t.Fatalf("retired managed emulator remained reachable: %s", output)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, newSerial, "shell", "ls", mutationPath); err == nil {
		t.Fatalf("guest mutation crossed reset generation: %s", output)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, newSerial, "shell", "ls", heartbeatPath); err == nil {
		t.Fatalf("persistent mutation loop state crossed reset generation: %s", output)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, newSerial, "shell", "cmd", "package", "path", "dev.philcantcode.worldspecimen"); err == nil || strings.Contains(output, "package:") {
		t.Fatalf("installed specimen crossed reset generation: %v\n%s", err, output)
	}

	nextInput := managedE2ENextTargetPlan(t, input)
	if err := driver.Close(); err != nil {
		t.Fatalf("close first driver after stopped run: %v", err)
	}
	restartedAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedAllocator.Close()
	restartedBackend, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	cleanupBackend = restartedBackend
	restarted := newManagedE2EDriver(t, build, restartedBackend, restartedAllocator, gateway, files)
	report, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{nextInput}})
	if err != nil {
		t.Fatalf("reconcile managed Android target after restart: %v", err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].RuntimeID != reset.Status.RuntimeID {
		t.Fatalf("managed Android restart reconciliation = %#v", report)
	}
	requireManagedE2EOwnershipResources(t, ctx, restartedBackend, restarted.targets[deviceKey(input.Target.ID(), 2)].instance)
	requireManagedE2EDataPartition(t, ctx, restartedBackend, restarted.targets[deviceKey(input.Target.ID(), 2)].instance, "generation 2 after process adoption")
	quarantinePlan := ports.TargetQuarantinePlan{
		IdempotencyKey: "managed-emulator-e2e-quarantine",
		Target:         ports.TargetRef{ID: input.Target.ID(), Generation: 2},
		Reason:         "prove durable managed-emulator containment and exact restart replay",
	}
	quarantineEvidence, err := restarted.Quarantine(ctx, quarantinePlan)
	if err != nil {
		t.Fatalf("quarantine reconciled managed Android target: %v", err)
	}
	if err := quarantineEvidence.Validate(quarantinePlan.Target); err != nil {
		t.Fatalf("managed Android quarantine evidence: %v", err)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, newSerial, "get-state"); err == nil {
		t.Fatalf("quarantined managed emulator remained reachable: %s", output)
	}
	if running, known := restartedBackend.ownedProcessRunning(reset.Status.RuntimeID); !known || running {
		t.Fatalf("quarantined managed emulator process state known=%t running=%t", known, running)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	quarantineAllocator, err := NewDurableEmulatorAllocator(allocatorConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer quarantineAllocator.Close()
	quarantineBackend, err := NewManagedEmulatorBackend(backendConfig)
	if err != nil {
		t.Fatal(err)
	}
	cleanupBackend = quarantineBackend
	quarantineRestart := newManagedE2EDriver(t, build, quarantineBackend, quarantineAllocator, gateway, files)
	quarantineReport, err := quarantineRestart.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{nextInput}})
	if err != nil {
		t.Fatalf("reconcile quarantined managed Android target after restart: %v", err)
	}
	if len(quarantineReport.Expected) != 1 || quarantineReport.Expected[0].Classification != ports.PhysicalResourceAdopted || quarantineReport.Expected[0].RuntimeID != reset.Status.RuntimeID {
		t.Fatalf("quarantined managed Android restart reconciliation = %#v", quarantineReport)
	}
	quarantinedRecord := quarantineRestart.targets[deviceKey(input.Target.ID(), 2)]
	if quarantinedRecord.status.State != domain.TargetGenerationQuarantined || quarantinedRecord.status.Ready {
		t.Fatalf("reconciled managed Android quarantine status = %#v", quarantinedRecord.status)
	}
	replayedQuarantine, err := quarantineRestart.Quarantine(ctx, quarantinePlan)
	if err != nil || replayedQuarantine != quarantineEvidence {
		t.Fatalf("managed Android durable quarantine replay = %#v, %v; want %#v", replayedQuarantine, err, quarantineEvidence)
	}
	changedQuarantine := quarantinePlan
	changedQuarantine.Reason = "changed quarantine reason must conflict"
	if _, err := quarantineRestart.Quarantine(ctx, changedQuarantine); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed managed Android quarantine replay error = %v", err)
	}
	changedQuarantine = quarantinePlan
	changedQuarantine.IdempotencyKey = "managed-emulator-e2e-quarantine-other-key"
	if _, err := quarantineRestart.Quarantine(ctx, changedQuarantine); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed managed Android quarantine key error = %v", err)
	}
	if running, known := quarantineBackend.ownedProcessRunning(reset.Status.RuntimeID); !known || running {
		t.Fatalf("quarantine replay restarted managed emulator: known=%t running=%t", known, running)
	}
	destroyedAVDPath := quarantineBackend.avdPath(quarantineReport.Expected[0].RuntimeID)
	if err := quarantineRestart.Destroy(ctx, ports.TargetRef{ID: input.Target.ID(), Generation: 2}); err != nil {
		t.Fatalf("destroy reconciled managed Android target: %v", err)
	}
	if output, err := runExactADBCommand(ctx, adb, defaultADBServer, newSerial, "get-state"); err == nil {
		t.Fatalf("destroyed managed emulator remained reachable: %s", output)
	}
	if _, err := os.Stat(destroyedAVDPath); !os.IsNotExist(err) {
		t.Fatalf("destroyed managed AVD directory %q still exists: %v", destroyedAVDPath, err)
	}
	if err := quarantineRestart.Close(); err != nil {
		t.Fatal(err)
	}
	cleanupInstances = nil
}

func managedE2EErrorTree(err error) string {
	var result strings.Builder
	var visit func(error, int)
	visit = func(current error, depth int) {
		if current == nil {
			return
		}
		_, _ = fmt.Fprintf(&result, "%s%T: %v\n", strings.Repeat("  ", depth), current, current)
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				visit(child, depth+1)
			}
			return
		}
		if wrapped, ok := current.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	return strings.TrimSpace(result.String())
}

func requireManagedE2EOwnershipResources(t *testing.T, ctx context.Context, backend *ManagedEmulatorBackend, instance Instance) {
	t.Helper()
	ownership, found, err := loadManagedProcessOwnership(instance, backend.processAuthority)
	if err != nil || !found {
		t.Fatalf("load exact managed process resource ownership: found=%t err=%v", found, err)
	}
	if ownership.CPUMilli != instance.Resources.CPUMilli || ownership.MemoryBytes != instance.Resources.MemoryBytes || ownership.StorageBytes != instance.Resources.StorageBytes ||
		ownership.GuestMemoryBytes != instance.GuestMemoryBytes || ownership.ResourceAnchored != backend.processAuthority.ResourcesEnforced() {
		t.Fatalf("managed process resource ownership = %#v, want CPU=%d host_memory=%d storage=%d guest_memory=%d anchored=%t",
			ownership, instance.Resources.CPUMilli, instance.Resources.MemoryBytes, instance.Resources.StorageBytes, instance.GuestMemoryBytes, backend.processAuthority.ResourcesEnforced())
	}
	storage, err := backend.requireManagedDataStorage(ctx, instance, managedDataOverlayPresent)
	if err != nil {
		t.Fatalf("re-prove exact generation-scoped managed data storage: %v", err)
	}
	if err := ownership.Storage.requireBinding(instance, storage); err != nil {
		t.Fatalf("managed process ownership does not bind the re-proven data storage: %v", err)
	}
	t.Logf("managed Android storage runtime=%s backing=%s overlay=%s bytes=%d digest=%s",
		instance.RuntimeID, storage.BackingPath, storage.OverlayPath, storage.BackingBytes, storage.BackingDigest)
}

func requireManagedE2EDataPartition(t *testing.T, ctx context.Context, backend *ManagedEmulatorBackend, instance Instance, phase string) {
	t.Helper()
	actual, err := backend.observeExactGuestDataPartitionBytes(ctx, instance)
	if err != nil {
		t.Fatalf("managed guest /data partition differs from the exact plan: %v", err)
	}
	if actual != instance.Resources.StorageBytes {
		t.Fatalf("managed guest /data partition is %d bytes, want exact %d", actual, instance.Resources.StorageBytes)
	}
	t.Logf("managed Android %s guest /data bytes=%d", phase, actual)
}

func waitForManagedE2EHeartbeatGrowth(ctx context.Context, adb string, server adbServerEndpoint, serial, path string, streamDone <-chan struct{}) error {
	var previous string
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-streamDone:
			return fmt.Errorf("mutation stream exited before StopRun")
		default:
		}
		output, err := runExactADBCommand(ctx, adb, server, serial, "shell", "cat", path)
		current := strings.TrimSpace(output)
		if err == nil && previous != "" && current != previous && strings.HasPrefix(current, previous) {
			return nil
		}
		if err == nil && current != "" {
			previous = current
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func managedE2ESDKRoot(t *testing.T) string {
	t.Helper()
	if configured := strings.TrimSpace(os.Getenv("WORLD_ANDROID_SDK_ROOT")); configured != "" {
		absolute, err := filepath.Abs(configured)
		if err != nil {
			t.Fatal(err)
		}
		return absolute
	}
	if configured := strings.TrimSpace(os.Getenv("ANDROID_SDK_ROOT")); configured != "" {
		absolute, err := filepath.Abs(configured)
		if err != nil {
			t.Fatal(err)
		}
		return absolute
	}
	if runtime.GOOS == "windows" && os.Getenv("LOCALAPPDATA") != "" {
		return filepath.Join(os.Getenv("LOCALAPPDATA"), "Android", "Sdk")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(home, "Android", "Sdk")
}

func executableName(base, windowsSuffix string) string {
	if runtime.GOOS == "windows" {
		return base + windowsSuffix
	}
	return base
}

func managedE2ETool(t *testing.T, environmentName, fallback string) string {
	t.Helper()
	tool := valueOrDefault(os.Getenv(environmentName), fallback)
	absolute, err := filepath.Abs(tool)
	if err != nil {
		t.Fatal(err)
	}
	requireRegularManagedE2EFile(t, absolute)
	return absolute
}

func requireRegularManagedE2EFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("managed Android E2E file %q is unavailable: %v", path, err)
	}
}

func newManagedE2EDriver(t *testing.T, build BuildConfig, backend Backend, allocator Allocator, gateway Gateway, files ScopedFileGateway) *Driver {
	t.Helper()
	driver, err := New(Config{
		Build: build, Backend: backend, Allocator: allocator, Gateway: gateway, Files: files,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func managedE2ETargetPlan(t *testing.T, template ports.TargetTemplate) ports.TargetPlan {
	t.Helper()
	targetID, _ := domain.NewTargetID()
	leaseID, _ := domain.NewLeaseID()
	sessionID, _ := domain.NewResearchSessionID()
	createdAt := time.Now().UTC()
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := domain.NewDigest([]byte("managed-emulator-e2e-policy"))
	capabilityDigest := domain.NewDigest([]byte("managed-emulator-e2e-capabilities"))
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: 1, PolicyDigest: policyDigest,
		CapabilityFingerprintDigest: capabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetPlan{
		IdempotencyKey: "managed-emulator-e2e-create", LeaseID: leaseID, Target: target, Generation: generation,
		Template: template, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
		Resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 1 << 30},
	}
}

func managedE2ENextTargetPlan(t *testing.T, previous ports.TargetPlan) ports.TargetPlan {
	t.Helper()
	createdAt := time.Now().UTC()
	if !createdAt.After(previous.Target.UpdatedAt()) {
		createdAt = previous.Target.UpdatedAt().Add(time.Nanosecond)
	}
	target, err := previous.Target.AdvanceGeneration(previous.Target.Revision(), 2, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: target.ID(), Generation: 2, PreviousGeneration: 1,
		PolicyDigest: previous.PolicyDigest, CapabilityFingerprintDigest: previous.CapabilityFingerprintDigest,
		CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	next := previous
	next.IdempotencyKey = "managed-emulator-e2e-reconcile-next"
	next.Target = target
	next.Generation = generation
	return next
}
