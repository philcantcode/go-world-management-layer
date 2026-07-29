package linuxcontainer

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestDockerDriverLifecycleEndToEnd(t *testing.T) {
	image := os.Getenv("WORLD_LINUX_TARGET_E2E_IMAGE")
	repository, imageDigest, ok := splitPinnedImage(image)
	if !ok {
		t.Skip("WORLD_LINUX_TARGET_E2E_IMAGE must be a local repository@sha256:digest reference")
	}
	root, err := filepath.Abs(writableTempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	dockerRuntime := NewDockerRuntime("docker", nil, nil)
	driver, err := New(Config{
		Build:      BuildConfig{TargetRoot: root, ImageRepository: repository},
		Runtime:    dockerRuntime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	targetPlan, scope := dockerTargetFixture(t, imageDigest)
	created, err := driver.Create(ctx, targetPlan)
	if err != nil {
		t.Fatal(err)
	}
	knownRuntimeIDs := []string{created.Status.RuntimeID}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		for _, runtimeID := range knownRuntimeIDs {
			_ = dockerRuntime.Remove(cleanup, runtimeID)
		}
	})
	if !created.Status.Ready || created.Status.Generation != 1 {
		t.Fatalf("created target = %#v", created)
	}

	payload := []byte("hostile specimen payload with spaces ; $() and opaque bytes\x00")
	material := dockerMaterial(t, "payload.txt", payload)
	firstRun := dockerRunPlan(t, scope, 1, material, "docker-e2e-run-one", 30*time.Second)
	if _, err := driver.PrepareRun(ctx, firstRun); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, firstRun.Run.ID()); err != nil {
		t.Fatal(err)
	}
	targetTransport, err := driver.OpenTransport(ctx, firstRun.Run.ID())
	if err != nil {
		t.Fatal(err)
	}

	nativeBinaryPath, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "testdata", "e2e", "build", "linux-amd64", "native-specimen"))
	if err != nil {
		t.Fatal(err)
	}
	nativeBinary, err := os.ReadFile(nativeBinaryPath)
	if err != nil {
		t.Fatalf("read native specimen binary: %v", err)
	}
	pushPlan := dockerTransferPlan(t, scope, 1, firstRun.Run.ID(), domain.TargetOperationPush, "tools/native-specimen", domain.NewDigest(nativeBinary), 0o555, int64(len(nativeBinary)))
	pushResult, err := targetTransport.PushFile(ctx, pushPlan, bytes.NewReader(nativeBinary))
	if err != nil || pushResult.Digest != domain.NewDigest(nativeBinary) {
		t.Fatalf("push native specimen = %#v, %v", pushResult, err)
	}
	if mode := dockerArchiveMode(t, ctx, created.Status.RuntimeID, "/target/tools/native-specimen"); mode != 0o555 {
		t.Fatalf("explicit pushed file mode = %04o, want 0555", mode)
	}
	defaultPush := dockerTransferPlan(t, scope, 1, firstRun.Run.ID(), domain.TargetOperationPush, "default-mode.txt", domain.NewDigest([]byte("default mode")), 0, 64)
	if _, err := targetTransport.PushFile(ctx, defaultPush, strings.NewReader("default mode")); err != nil {
		t.Fatal(err)
	}
	defaultMode := dockerArchiveMode(t, ctx, created.Status.RuntimeID, "/target/default-mode.txt")
	if runtime.GOOS != "windows" && defaultMode != 0o600 {
		t.Fatalf("default pushed file mode = %04o, want 0600", defaultMode)
	}
	if runtime.GOOS == "windows" {
		t.Logf("EVIDENCE mode_boundary host=windows requested=0600 observed_in_linux_container=%04o", defaultMode)
	}

	execPlan := dockerExecPlan(t, scope, 1, firstRun.Run.ID(), "/target/tools/native-specimen", []string{"-input", "/target/input/payload.txt", "-output", "/target/result.json"}, time.Now().Add(20*time.Second))
	stdout, terminal, err := receiveDockerExec(ctx, targetTransport, execPlan)
	if err != nil || terminal.ExitCode != 0 || !terminal.CleanupConfirmed {
		t.Fatalf("native exec terminal = %#v, stdout=%q, err=%v", terminal, stdout, err)
	}
	wantPayloadDigest := domain.NewDigest(payload)
	if !strings.Contains(string(stdout), wantPayloadDigest.String()) {
		t.Fatalf("native exec stdout = %q, want digest %s", stdout, wantPayloadDigest)
	}
	pullPlan := dockerTransferPlan(t, scope, 1, firstRun.Run.ID(), domain.TargetOperationPull, "result.json", domain.Digest{}, 0, 1<<20)
	pulled, err := targetTransport.PullFile(ctx, pullPlan)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, readErr := io.ReadAll(pulled)
	closeErr := pulled.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal(errors.Join(readErr, closeErr))
	}
	var specimen struct {
		InputDigest string `json:"input_digest"`
		Probes      []struct {
			Path       string `json:"path"`
			Accessible bool   `json:"accessible"`
		} `json:"boundary_probes"`
	}
	if err := json.Unmarshal(resultBytes, &specimen); err != nil {
		t.Fatal(err)
	}
	if specimen.InputDigest != wantPayloadDigest.String() {
		t.Fatalf("specimen digest = %s, want %s", specimen.InputDigest, wantPayloadDigest)
	}
	for _, probe := range specimen.Probes {
		if probe.Accessible {
			t.Fatalf("forbidden host boundary was visible: %s", probe.Path)
		}
	}
	record, err := driver.requireTarget(scope.targetID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(record.plan.materialRoot(), "result.json")); !os.IsNotExist(err) {
		t.Fatalf("writable result entered immutable material projection: %v", err)
	}
	if _, err := os.Stat(filepath.Join(record.plan.writableRoot(), "result.json")); err != nil {
		t.Fatalf("writable result missing from target-private state: %v", err)
	}
	const detachedMutationDelay = 3 * time.Second
	detachedReadyPath := filepath.Join(record.plan.writableRoot(), "daemon", "ready.txt")
	detachedOutputPath := filepath.Join(record.plan.writableRoot(), "daemon", "escaped-after-stop.txt")
	detachedPlan := dockerExecPlan(t, scope, 1, firstRun.Run.ID(), "/target/tools/native-specimen", []string{
		"-detached-ready", "/target/daemon/ready.txt",
		"-detached-output", "/target/daemon/escaped-after-stop.txt",
		"-detached-delay", detachedMutationDelay.String(),
	}, time.Now().Add(20*time.Second))
	if _, terminal, err := receiveDockerExec(ctx, targetTransport, detachedPlan); err != nil || terminal.ExitCode != 0 || !terminal.CleanupConfirmed {
		t.Fatalf("detached setsid launch terminal = %#v, %v", terminal, err)
	}
	requireFileAppears(t, detachedReadyPath, time.Second)
	if _, err := os.Stat(detachedOutputPath); !os.IsNotExist(err) {
		t.Fatalf("detached mutation occurred before StopRun: %v", err)
	}
	stopped, err := driver.StopRun(ctx, firstRun.Run.ID(), ports.StopGraceful)
	if err != nil || stopped.Outcome != ports.RunCompleted || stopped.FailureKind != ports.TargetRunFailureNone {
		t.Fatalf("first run stop = %#v, %v", stopped, err)
	}
	requireAddedTargetChanges(t, stopped.TargetChanges, map[string]domain.Digest{
		"default-mode.txt":      domain.NewDigest([]byte("default mode")),
		"daemon/ready.txt":      domain.NewDigest([]byte("ready\n")),
		"result.json":           domain.NewDigest(resultBytes),
		"tools/native-specimen": domain.NewDigest(nativeBinary),
	})
	if state, err := dockerRuntime.Inspect(ctx, created.Status.RuntimeID); err != nil || state.Running {
		t.Fatalf("stopped run container state = %#v, %v", state, err)
	}
	withoutReset := dockerRunPlan(t, scope, 1, material, "docker-e2e-run-without-reset", 30*time.Second)
	if _, err := driver.PrepareRun(ctx, withoutReset); !domain.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("second run reused stopped generation without reset: %v", err)
	}
	time.Sleep(detachedMutationDelay + time.Second)
	if _, err := os.Stat(detachedOutputPath); !os.IsNotExist(err) {
		t.Fatalf("setsid daemon mutated target state after the sealed boundary: %v", err)
	}

	firstReset := ports.ResetPlan{IdempotencyKey: "docker-e2e-reset-one", LeaseID: scope.leaseID, Previous: ports.TargetRef{ID: scope.targetID, Generation: 1}, NextGeneration: 2, Mode: ports.ResetRecreate}
	firstResetResult, err := driver.Reset(ctx, scope.targetID, firstReset)
	if err != nil {
		t.Fatal(err)
	}
	knownRuntimeIDs = append(knownRuntimeIDs, firstResetResult.Status.RuntimeID)
	if _, err := dockerRuntime.Inspect(ctx, created.Status.RuntimeID); err == nil {
		t.Fatal("first reset left the consumed generation runtime reachable")
	}

	deadlineRun := dockerRunPlan(t, scope, 2, material, "docker-e2e-run-deadline", 8*time.Second)
	if _, err := driver.PrepareRun(ctx, deadlineRun); err != nil {
		t.Fatal(err)
	}
	deadlineStarted := time.Now()
	if err := driver.StartRun(ctx, deadlineRun.Run.ID()); err != nil {
		t.Fatal(err)
	}
	deadlineTransport, err := driver.OpenTransport(ctx, deadlineRun.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	deadlinePush := dockerTransferPlan(t, scope, 2, deadlineRun.Run.ID(), domain.TargetOperationPush, "tools/native-specimen", domain.NewDigest(nativeBinary), 0o555, int64(len(nativeBinary)))
	if _, err := deadlineTransport.PushFile(ctx, deadlinePush, bytes.NewReader(nativeBinary)); err != nil {
		t.Fatal(err)
	}
	longExec := dockerExecPlan(t, scope, 2, deadlineRun.Run.ID(), "/target/tools/native-specimen", []string{"-sleep", "30s", "-input", "/target/input/payload.txt"}, time.Now().Add(45*time.Second))
	longSession, err := deadlineTransport.OpenExec(ctx, longExec)
	if err != nil {
		t.Fatal(err)
	}
	requireProcessStarted(t, ctx, longSession)
	for {
		if _, err := longSession.Receive(ctx); err != nil {
			break
		}
	}
	if elapsed := time.Since(deadlineStarted); elapsed > 15*time.Second {
		t.Fatalf("run deadline took too long to revoke exec: %s", elapsed)
	}
	deadlineResult, err := driver.StopRun(ctx, deadlineRun.Run.ID(), ports.StopGraceful)
	if err != nil || deadlineResult.Outcome != ports.RunFailed || deadlineResult.FailureKind != ports.TargetRunFailureDurationExceeded {
		t.Fatalf("deadline result = %#v, %v", deadlineResult, err)
	}
	if _, err := driver.OpenTransport(ctx, deadlineRun.Run.ID()); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("expired run reopened transport: %v", err)
	}
	if state, err := dockerRuntime.Inspect(ctx, firstResetResult.Status.RuntimeID); err != nil || state.Running {
		t.Fatalf("expired run container state = %#v, %v", state, err)
	}

	reset := ports.ResetPlan{IdempotencyKey: "docker-e2e-reset-two", LeaseID: scope.leaseID, Previous: ports.TargetRef{ID: scope.targetID, Generation: 2}, NextGeneration: 3, Mode: ports.ResetRecreate}
	resetResult, err := driver.Reset(ctx, scope.targetID, reset)
	if err != nil {
		t.Fatal(err)
	}
	knownRuntimeIDs = append(knownRuntimeIDs, resetResult.Status.RuntimeID)
	if resetResult.Status.Generation != 3 || !resetResult.Status.Ready {
		t.Fatalf("reset result = %#v", resetResult)
	}
	if _, err := dockerRuntime.Inspect(ctx, firstResetResult.Status.RuntimeID); err == nil {
		t.Fatal("reset left the previous runtime reachable")
	}
	if state, err := dockerRuntime.Inspect(ctx, resetResult.Status.RuntimeID); err != nil || !state.Running {
		t.Fatalf("replacement runtime state = %#v, %v", state, err)
	}
	if replay, err := driver.Reset(ctx, scope.targetID, reset); err != nil || replay.Status.RuntimeID != resetResult.Status.RuntimeID {
		t.Fatalf("reset replay = %#v, %v", replay, err)
	}

	// Prove the reset receipt is sufficient after complete driver state loss,
	// then quarantine and independently adopt the exact stopped realization.
	expectedGeneration2 := successorTargetPlan(t, targetPlan, firstReset)
	expectedGeneration2.IdempotencyKey = "docker-e2e-generation-two"
	expectedGeneration3 := successorTargetPlan(t, expectedGeneration2, reset)
	expectedGeneration3.IdempotencyKey = "docker-e2e-generation-three"
	restarted, err := New(Config{
		Build:      BuildConfig{TargetRoot: root, ImageRepository: repository},
		Runtime:    dockerRuntime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	restartReport, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{expectedGeneration3}})
	if err != nil || len(restartReport.Expected) != 1 || restartReport.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("reset receipt restart reconciliation = %#v, %v", restartReport, err)
	}
	if replay, err := restarted.Reset(ctx, scope.targetID, reset); err != nil || replay.Status.RuntimeID != resetResult.Status.RuntimeID {
		t.Fatalf("restart reset replay = %#v, %v", replay, err)
	}
	restartedRecord, err := restarted.requireTarget(scope.targetID, 3)
	if err != nil {
		t.Fatal(err)
	}
	preservedPath := filepath.Join(restartedRecord.plan.writableRoot(), "quarantine-preserved.txt")
	preservedBytes := []byte("real quarantine evidence state\n")
	if err := os.WriteFile(preservedPath, preservedBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	quarantinePlan := ports.TargetQuarantinePlan{
		IdempotencyKey: "docker-e2e-quarantine", Target: ports.TargetRef{ID: scope.targetID, Generation: 3}, Reason: "restart custody qualification",
	}
	quarantineEvidence, err := restarted.Quarantine(ctx, quarantinePlan)
	if err != nil {
		t.Fatal(err)
	}
	if !quarantineEvidence.ExecutionStopped || !quarantineEvidence.NetworkUnreachable || !quarantineEvidence.StatePreserved {
		t.Fatalf("quarantine evidence = %#v", quarantineEvidence)
	}
	quarantineRestart, err := New(Config{
		Build:      BuildConfig{TargetRoot: root, ImageRepository: repository},
		Runtime:    dockerRuntime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	quarantineReport, err := quarantineRestart.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{expectedGeneration3}})
	if err != nil || len(quarantineReport.Expected) != 1 || quarantineReport.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("quarantine restart reconciliation = %#v, %v", quarantineReport, err)
	}
	quarantinedRecord, err := quarantineRestart.requireTarget(scope.targetID, 3)
	if err != nil || quarantinedRecord.status.State != domain.TargetGenerationQuarantined || quarantinedRecord.status.Ready {
		t.Fatalf("restart quarantined record = %#v, %v", quarantinedRecord, err)
	}
	if contents, err := os.ReadFile(preservedPath); err != nil || !bytes.Equal(contents, preservedBytes) {
		t.Fatalf("quarantined state was not preserved: bytes=%q err=%v", contents, err)
	}
	if replay, err := quarantineRestart.Quarantine(ctx, quarantinePlan); err != nil || replay != quarantineEvidence {
		t.Fatalf("restart quarantine replay = %#v, %v; want %#v", replay, err, quarantineEvidence)
	}

	if err := quarantineRestart.Destroy(ctx, ports.TargetRef{ID: scope.targetID, Generation: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := dockerRuntime.Inspect(ctx, resetResult.Status.RuntimeID); err == nil {
		t.Fatal("destroy left the replacement runtime reachable")
	}
	listing, err := (command.OS{}).Run(ctx, command.Invocation{Program: "docker", Args: []string{"ps", "-a", "--filter", "label=world.target=" + scope.targetID.String(), "--format", "{{.ID}}"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(listing.Stdout)) != "" {
		t.Fatalf("orphan target containers = %q", listing.Stdout)
	}
}

type dockerTargetScope struct {
	leaseID  domain.LeaseID
	targetID domain.TargetID
	agentID  domain.AgentWorkspaceID
	session  domain.ResearchSessionID
}

func dockerTargetFixture(t *testing.T, imageDigest domain.Digest) (ports.TargetPlan, dockerTargetScope) {
	t.Helper()
	createdAt := time.Now().UTC()
	leaseID, _ := domain.NewLeaseID()
	targetID, _ := domain.NewTargetID()
	agentID, _ := domain.NewAgentWorkspaceID()
	session, _ := domain.NewResearchSessionID()
	policy := domain.NewDigest([]byte("docker-driver-e2e-policy"))
	capability := domain.NewDigest([]byte("docker-driver-e2e-capability"))
	target, err := domain.NewTarget(targetID, session, domain.TargetLinuxContainer, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{TargetID: targetID, Generation: 1, PolicyDigest: policy, CapabilityFingerprintDigest: capability, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	plan := ports.TargetPlan{
		IdempotencyKey: "docker-e2e-create", LeaseID: leaseID, Target: target, Generation: generation,
		Template:     ports.TargetTemplate{Name: "docker-e2e", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime, ImageDigest: imageDigest, IsolationProfile: "observable-container"},
		PolicyDigest: policy, CapabilityFingerprintDigest: capability,
		Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, PIDs: 64},
	}
	return plan, dockerTargetScope{leaseID: leaseID, targetID: targetID, agentID: agentID, session: session}
}

func dockerMaterial(t *testing.T, logicalPath string, content []byte) []ports.TargetMaterialPlan {
	t.Helper()
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: "artifact://docker-driver-e2e/" + logicalPath, Digest: domain.NewDigest(content), Size: int64(len(content)), Role: "target-input", Sensitivity: domain.SensitivityRestricted})
	if err != nil {
		t.Fatal(err)
	}
	return []ports.TargetMaterialPlan{{Artifact: artifact, LogicalPath: logicalPath, Mode: 0o444, Content: memorySource{content: content, digest: domain.NewDigest(content)}}}
}

func dockerRunPlan(t *testing.T, scope dockerTargetScope, generation domain.TargetGeneration, material []ports.TargetMaterialPlan, key string, maximum time.Duration) ports.TargetRunPlan {
	t.Helper()
	runID, _ := domain.NewTargetRunID()
	digest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.NewTargetRun(domain.TargetRunSpec{ID: runID, LeaseID: scope.leaseID, TargetID: scope.targetID, TargetGeneration: generation, AgentWorkspaceID: scope.agentID, AgentGeneration: 1, MaterializationDigest: digest, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetRunPlan{
		IdempotencyKey: key, Run: run, RequiredCoverage: []string{"process"},
		Collectors: []ports.CollectorSpec{{
			Name: "process-e2e", Adapter: "test.external-process", Version: "1",
			ConfigurationDigest: domain.NewDigest([]byte("process-e2e-config")), MaximumBytes: 1 << 20,
			Requirement: ports.ObservationRequirement{
				SignalFamily: "process", Placement: domain.CollectorPlacementHost,
				MinimumLevel: domain.CoverageLevelComplete, Required: true,
			},
		}},
		Material: material, MaximumDuration: maximum,
	}
}

func dockerTransferPlan(t *testing.T, scope dockerTargetScope, generation domain.TargetGeneration, runID domain.TargetRunID, kind domain.TargetOperationKind, path string, digest domain.Digest, mode uint32, maximum int64) ports.TargetTransferPlan {
	t.Helper()
	operationID, _ := domain.NewTargetOperationID()
	operation, err := domain.NewTargetOperation(domain.TargetOperationSpec{ID: operationID, LeaseID: scope.leaseID, TargetID: scope.targetID, TargetGeneration: generation, TargetRunID: runID, Kind: kind, CommandDisplay: string(kind) + " " + path, ContentDigest: digest, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetTransferPlan{Operation: operation, RelativePath: path, Mode: mode, MaximumBytes: maximum}
}

func dockerExecPlan(t *testing.T, scope dockerTargetScope, generation domain.TargetGeneration, runID domain.TargetRunID, executable string, argv []string, deadline time.Time) ports.TargetExecPlan {
	t.Helper()
	operationID, _ := domain.NewTargetOperationID()
	operation, err := domain.NewTargetOperation(domain.TargetOperationSpec{ID: operationID, LeaseID: scope.leaseID, TargetID: scope.targetID, TargetGeneration: generation, TargetRunID: runID, Kind: domain.TargetOperationExec, CommandDisplay: strings.Join(argv, " "), CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetExecPlan{Operation: operation, Start: transport.ExecStart{ExecID: operationID.String(), IdempotencyKey: "docker-exec-" + operationID.String(), Executable: executable, Argv: argv, WorkingDirectory: "/target", Deadline: deadline.UTC(), MaxOutputBytes: 1 << 20, CleanupGrace: time.Second}}
}

func receiveDockerExec(ctx context.Context, target ports.TargetTransport, plan ports.TargetExecPlan) ([]byte, transport.Terminal, error) {
	session, err := target.OpenExec(ctx, plan)
	if err != nil {
		return nil, transport.Terminal{}, err
	}
	defer session.Close()
	var stdout bytes.Buffer
	for {
		frame, err := session.Receive(ctx)
		if err != nil {
			return stdout.Bytes(), transport.Terminal{}, err
		}
		switch frame.Kind {
		case transport.KindStdout:
			_, _ = stdout.Write(frame.Data)
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			return stdout.Bytes(), terminal, err
		}
	}
}

func requireProcessStarted(t *testing.T, ctx context.Context, session ports.ExecTransport) {
	t.Helper()
	for {
		frame, err := session.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Kind != transport.KindProcess {
			continue
		}
		event, err := transport.DecodeJSON[transport.ProcessEvent](frame)
		if err != nil {
			t.Fatal(err)
		}
		if event.Kind == "started" {
			return
		}
	}
}

func requireFileAppears(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("file %q did not appear within %s", path, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dockerArchiveMode(t *testing.T, ctx context.Context, runtimeID, source string) int64 {
	t.Helper()
	result, err := (command.OS{}).Run(ctx, command.Invocation{Program: "docker", Args: []string{"cp", runtimeID + ":" + source, "-"}})
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(result.Stdout))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			t.Fatalf("docker archive for %s contained no file", source)
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			return header.Mode & 0o777
		}
	}
}

func splitPinnedImage(image string) (string, domain.Digest, bool) {
	separator := strings.LastIndex(image, "@sha256:")
	if separator <= 0 {
		return "", domain.Digest{}, false
	}
	digest, err := domain.ParseDigest(image[separator+1:])
	if err != nil {
		return "", domain.Digest{}, false
	}
	return image[:separator], digest, true
}

func requireAddedTargetChanges(t *testing.T, changes domain.ChangeSet, expected map[string]domain.Digest) {
	t.Helper()
	if changes.Scope() != domain.ChangeScopeTarget {
		t.Fatalf("target change scope = %q, want %q", changes.Scope(), domain.ChangeScopeTarget)
	}
	entries := changes.Entries()
	if len(entries) != len(expected) {
		t.Fatalf("target changes = %#v, want %d exact additions", entries, len(expected))
	}
	for _, entry := range entries {
		spec := entry.Spec()
		want, found := expected[spec.Path]
		if !found {
			t.Fatalf("unexpected target change %q", spec.Path)
		}
		if spec.Kind != domain.ChangeAdded || !spec.BeforeDigest.IsZero() || spec.AfterDigest != want {
			t.Fatalf("target change %q = %#v, want added digest %s", spec.Path, spec, want)
		}
	}
}
