package docker

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestProvisionRequiresFramedGuestReadinessAndUsesConfiguredBinary(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	merged := filepath.Join(root, plan.Workspace.ID().String(), "merged")
	if err := os.MkdirAll(merged, 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &recordingEngine{readiness: successfulReadinessFrames(t)}
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", GuestBinary: "/opt/world/world-guest", ContainerUser: testGuestUser(t)},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := driver.Provision(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Status.Ready || result.Status.GuestProtocol != uint32(transport.ProtocolVersion) {
		t.Fatalf("provisioned status = %#v", result.Status)
	}
	if engine.openedGuest != "/opt/world/world-guest" || engine.openedPlan.Start.Executable != engine.openedGuest {
		t.Fatalf("readiness used guest %q with executable %q", engine.openedGuest, engine.openedPlan.Start.Executable)
	}
	if len(engine.openedPlan.Start.Argv) != 1 || engine.openedPlan.Start.Argv[0] != transport.GuestSelfTestArgument {
		t.Fatalf("readiness argv = %#v", engine.openedPlan.Start.Argv)
	}

	execPlan := testExecPlan(t, plan, "/workspace/tool")
	engine.readiness = successfulReadinessFrames(t)
	if _, err := driver.OpenExec(ctx, execPlan); err != nil {
		t.Fatal(err)
	}
	if engine.openedGuest != "/opt/world/world-guest" {
		t.Fatalf("OpenExec used guest %q", engine.openedGuest)
	}
}

func TestProbeReportsHostCgroupIdentityAuthority(t *testing.T) {
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: &recordingEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fingerprint, err := driver.Probe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, found := fingerprint.Capability("agent.docker")
	if !found {
		t.Fatal("agent.docker capability is missing")
	}
	if got := capability.Constraints()["cgroup_identity_authority"]; got != dockercli.ContainerCgroupIdentityAuthority() {
		t.Fatalf("cgroup identity authority = %q", got)
	}
}

func TestProbeReportsExactNamespaceFactsForDaemonUsernsMode(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  []string
		wantUser string
	}{
		{name: "empty userns mode", options: []string{}, wantUser: "host"},
		{name: "daemon userns remap", options: []string{"name=userns", "name=seccomp,profile=builtin"}, wantUser: "remapped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver, err := New(Config{
				Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
				Engine: namespaceProbeEngine{recordingEngine: &recordingEngine{}, securityOptions: test.options},
			})
			if err != nil {
				t.Fatal(err)
			}
			fingerprint, err := driver.Probe(testDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			capability, found := fingerprint.Capability("agent.hardened-isolation")
			if !found {
				t.Fatal("agent.hardened-isolation capability is missing")
			}
			facts := capability.Constraints()
			if _, found := facts["host_namespaces"]; found {
				t.Fatalf("ambiguous aggregate host namespace claim remains: %#v", facts)
			}
			if facts["user_namespace"] != test.wantUser || facts["pid_namespace"] != "private" || facts["ipc_namespace"] != "private" || facts["cgroup_namespace"] != "private" || facts["uts_namespace"] != "private" || facts["network_namespace"] != "none" {
				t.Fatalf("namespace facts = %#v", facts)
			}
		})
	}
}

func TestProbeAcceptsCgroupV1ResourceControllers(t *testing.T) {
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: &cgroupVersionEngine{recordingEngine: &recordingEngine{}, version: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fingerprint, err := driver.Probe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, found := fingerprint.Capability("agent.docker")
	if !found || capability.Constraints()["cgroup_version"] != "1" {
		t.Fatalf("cgroup v1 capability = %#v", capability.Constraints())
	}
}

func TestProbeRejectsUnreportedOrUnsupportedCgroupVersion(t *testing.T) {
	for _, version := range []string{"", "3"} {
		t.Run("version_"+version, func(t *testing.T) {
			driver, err := New(Config{
				Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
				Engine: &cgroupVersionEngine{recordingEngine: &recordingEngine{}, version: version},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := driver.Probe(ctx); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
				t.Fatalf("Probe() error = %v, want capability unavailable", err)
			}
		})
	}
}

func TestProvisionRejectsIdempotencyKeyReusedWithChangedPlan(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	if err := os.MkdirAll(filepath.Join(root, plan.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &recordingEngine{readiness: successfulReadinessFrames(t)}
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.Provision(ctx, plan); err != nil {
		t.Fatal(err)
	}
	exact, err := driver.Provision(ctx, plan)
	if err != nil || exact.Created || !exact.Status.Ready {
		t.Fatalf("exact idempotency replay = %#v, %v", exact, err)
	}
	changedResources := plan
	changedResources.Resources.CPUMilli++
	if _, err := driver.Provision(ctx, changedResources); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed-resource idempotency replay error = %v, want conflict", err)
	}
	changedProvenance := plan
	generationSpec := changedProvenance.Generation.Spec()
	generationSpec.CreatedAt = generationSpec.CreatedAt.Add(time.Second)
	changedProvenance.Generation, err = domain.NewAgentWorkspaceGeneration(generationSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Provision(ctx, changedProvenance); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("changed-provenance idempotency replay error = %v, want conflict", err)
	}
	mismatchedInput := plan
	generationSpec = mismatchedInput.Generation.Spec()
	generationSpec.InputViewID = domain.NewInputViewID([]byte("different-input-view"))
	mismatchedInput.Generation, err = domain.NewAgentWorkspaceGeneration(generationSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Provision(ctx, mismatchedInput); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("generation/workspace input mismatch error = %v, want conflict", err)
	}
	if engine.createCalls != 1 {
		t.Fatalf("Docker create calls = %d, want 1", engine.createCalls)
	}
}

func TestProvisionRemovesContainerWhenGuestReadinessIsNotAuthoritative(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	if err := os.MkdirAll(filepath.Join(root, plan.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	badTerminal, err := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: false})
	if err != nil {
		t.Fatal(err)
	}
	engine := &recordingEngine{readiness: []transport.Frame{{Kind: transport.KindTerminal, Data: badTerminal}}}
	driver, err := New(Config{Build: BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)}, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.Provision(ctx, plan); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("Provision() error = %v, want failed precondition", err)
	}
	if engine.removed != engine.containerID {
		t.Fatalf("failed container %q was not removed (removed %q)", engine.containerID, engine.removed)
	}
}

func TestInspectPreservesIntentionalSealedStop(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	if err := os.MkdirAll(filepath.Join(root, plan.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &recordingEngine{readiness: successfulReadinessFrames(t)}
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	created, err := driver.Provision(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	ref := ports.AgentWorkspaceRef{ID: created.Status.AgentWorkspaceID, Generation: created.Status.Generation}
	if err := driver.Stop(ctx, ref, ports.StopGraceful); err != nil {
		t.Fatal(err)
	}
	status, err := driver.Inspect(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if status.Ready || status.State != domain.AgentGenerationSealed {
		t.Fatalf("stopped inspection = %#v, want sealed and not ready", status)
	}
}

func TestStopFailsClosedWhenEngineReportsSuccessWithoutStoppingExactContainer(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	if err := os.MkdirAll(filepath.Join(root, plan.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &recordingEngine{readiness: successfulReadinessFrames(t), stopLeavesRunning: true}
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", GuestBinary: "/opt/world/world-guest", ContainerUser: testGuestUser(t)},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := driver.Provision(testDeadline(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	ref := ports.AgentWorkspaceRef{ID: created.Status.AgentWorkspaceID, Generation: created.Status.Generation}
	if err := driver.Stop(testDeadline(t), ref, ports.StopGraceful); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("no-op Stop() error = %v, want failed precondition", err)
	}
	status, err := driver.Inspect(testDeadline(t), ref)
	if err != nil || !status.Ready || status.State != domain.AgentGenerationReady {
		t.Fatalf("failed stop changed logical readiness: %#v, %v", status, err)
	}
}

func TestProvisionRejectsCreateInspectIdentitySubstitution(t *testing.T) {
	root := t.TempDir()
	plan := testAgentWorkspacePlan(t)
	if err := os.MkdirAll(filepath.Join(root, plan.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	createdID := testContainerID('a')
	engine := &recordingEngine{containerID: createdID, inspectID: testContainerID('b'), readiness: successfulReadinessFrames(t)}
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Provision(testDeadline(t), plan); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("substituted Provision() error = %v, want integrity violation", err)
	}
	if engine.removed != "" {
		t.Fatalf("identity-substituted provisioning mutated unproven resource %q", engine.removed)
	}
}

func successfulReadinessFrames(t *testing.T) []transport.Frame {
	t.Helper()
	started, err := json.Marshal(transport.ProcessEvent{Kind: "started", PID: 10, ProcessStartNS: 1})
	if err != nil {
		t.Fatal(err)
	}
	exited, err := json.Marshal(transport.ProcessEvent{Kind: "exited", PID: 10, ProcessStartNS: 1})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	return []transport.Frame{{Sequence: 1, Kind: transport.KindProcess, Data: started}, {Sequence: 2, Kind: transport.KindProcess, Data: exited}, {Sequence: 3, Kind: transport.KindTerminal, Data: terminal}}
}

func testAgentWorkspacePlan(t *testing.T) ports.AgentWorkspacePlan {
	t.Helper()
	return newAgentFixture(t, domain.NewDigest([]byte("image")), map[string]fixtureInput{"input.bin": {bytes: []byte("input"), mode: 0o400}}).agent
}

func testExecPlan(t *testing.T, agent ports.AgentWorkspacePlan, executable string) ports.ExecPlan {
	t.Helper()
	return newTestExecPlan(t, agent, executable, nil, nil)
}

func newTestExecPlan(t *testing.T, agent ports.AgentWorkspacePlan, executable string, argv []string, temporary []transport.TemporaryInput) ports.ExecPlan {
	t.Helper()
	execID, err := domain.NewExecID()
	if err != nil {
		t.Fatal(err)
	}
	scope := agent.Generation.Spec()
	now := time.Now().UTC()
	exec, err := domain.NewExec(domain.ExecSpec{
		ID: execID, LeaseID: agent.LeaseID, AgentWorkspaceID: scope.AgentWorkspaceID, AgentGeneration: scope.Generation,
		Kind: domain.ExecTool, Executable: executable, Argv: argv, WorkingDirectory: ".", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.ExecPlan{
		LeaseID: agent.LeaseID, AgentWorkspaceID: scope.AgentWorkspaceID, AgentGeneration: scope.Generation, Exec: exec,
		Start: transport.ExecStart{
			ExecID: execID.String(), IdempotencyKey: "exec-test", Executable: executable, Argv: append([]string(nil), argv...),
			WorkingDirectory: ".", TemporaryInputs: append([]transport.TemporaryInput(nil), temporary...),
			Deadline: now.Add(time.Minute), MaxOutputBytes: 1 << 20, CleanupGrace: time.Second,
		},
	}
}

type recordingEngine struct {
	containerID       string
	createCalls       int
	startCalls        int
	exists            bool
	createdPlan       ContainerPlan
	readiness         []transport.Frame
	openedGuest       string
	openedPlan        ports.ExecPlan
	removed           string
	stopped           bool
	status            string
	inspectID         string
	stopLeavesRunning bool
}

type cgroupVersionEngine struct {
	*recordingEngine
	version string
}

type namespaceProbeEngine struct {
	*recordingEngine
	securityOptions []string
}

func (e namespaceProbeEngine) Probe(ctx context.Context) (EngineCapabilities, error) {
	capabilities, err := e.recordingEngine.Probe(ctx)
	capabilities.SecurityOptions = append([]string(nil), e.securityOptions...)
	return capabilities, err
}

func (e *cgroupVersionEngine) Probe(ctx context.Context) (EngineCapabilities, error) {
	capabilities, err := e.recordingEngine.Probe(ctx)
	capabilities.CgroupVersion = e.version
	return capabilities, err
}

func (e *recordingEngine) Probe(context.Context) (EngineCapabilities, error) {
	return EngineCapabilities{
		EngineVersion: "29.0", APIVersion: "1.52", CgroupVersion: "2",
		OSType: "linux", Architecture: "amd64", Runtimes: []string{dockercli.RuncRuntime},
		SecurityOptions: []string{"name=seccomp,profile=builtin"}, CPUCFSQuota: true, MemoryLimit: true, SwapLimit: true, PIDsLimit: true,
	}, nil
}
func (e *recordingEngine) Create(_ context.Context, plan ContainerPlan) (string, error) {
	e.createCalls++
	if e.containerID == "" {
		e.containerID = testContainerID('a')
	}
	e.createdPlan = plan
	e.exists = true
	e.stopped = true
	e.status = dockercli.StoppedStatusCreated
	return e.containerID, nil
}
func (e *recordingEngine) Start(context.Context, string) error {
	if !e.exists {
		return os.ErrNotExist
	}
	e.startCalls++
	e.stopped = false
	e.status = "running"
	return nil
}
func (e *recordingEngine) Inspect(_ context.Context, id string) (ContainerState, error) {
	if !e.exists || id != e.containerID {
		return ContainerState{}, os.ErrNotExist
	}
	status := e.status
	if status == "" {
		status = "running"
		if e.stopped {
			status = dockercli.StoppedStatusExited
		}
	}
	observedID := id
	if e.inspectID != "" {
		observedID = e.inspectID
	}
	return ContainerState{ID: observedID, Name: e.createdPlan.Name, Running: !e.stopped, Status: status, Labels: e.openedContainerLabels(), Configuration: expectedAgentConfiguration(e.createdPlan)}, nil
}
func (e *recordingEngine) ListContainers(context.Context) ([]ContainerState, error) {
	if !e.exists {
		return nil, nil
	}
	state, err := e.Inspect(context.Background(), e.containerID)
	if err != nil {
		return nil, err
	}
	return []ContainerState{state}, nil
}
func (e *recordingEngine) openedContainerLabels() map[string]string {
	return e.createdPlan.Labels
}
func (e *recordingEngine) Stop(context.Context, string, ports.StopMode) error {
	if !e.stopLeavesRunning {
		e.stopped = true
		e.status = dockercli.StoppedStatusExited
	}
	return nil
}
func (e *recordingEngine) Remove(_ context.Context, id string) error {
	e.removed = id
	e.exists = false
	return nil
}
func (e *recordingEngine) OpenExec(_ context.Context, _ string, guest string, plan ports.ExecPlan) (ports.ExecTransport, error) {
	e.openedGuest, e.openedPlan = guest, plan
	return &scriptedExecTransport{frames: append([]transport.Frame(nil), e.readiness...)}, nil
}

type scriptedExecTransport struct {
	frames []transport.Frame
	index  int
}

func (t *scriptedExecTransport) Send(context.Context, transport.Kind, []byte) (transport.Frame, error) {
	return transport.Frame{}, nil
}
func (t *scriptedExecTransport) Receive(context.Context) (transport.Frame, error) {
	if t.index == len(t.frames) {
		return transport.Frame{}, io.EOF
	}
	frame := t.frames[t.index]
	t.index++
	return frame, nil
}
func (t *scriptedExecTransport) Close() error { return nil }

var _ Engine = (*recordingEngine)(nil)
var _ EngineInventory = (*recordingEngine)(nil)
var _ ports.ExecTransport = (*scriptedExecTransport)(nil)
