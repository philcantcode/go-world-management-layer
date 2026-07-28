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
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", GuestBinary: "/opt/world/world-guest"},
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
	driver, err := New(Config{Build: BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent"}, Engine: engine})
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
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent"},
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
	containerID string
	createdPlan ContainerPlan
	readiness   []transport.Frame
	openedGuest string
	openedPlan  ports.ExecPlan
	removed     string
	stopped     bool
}

func (e *recordingEngine) Probe(context.Context) (EngineCapabilities, error) {
	return EngineCapabilities{
		OSType: "linux", Architecture: "amd64", Runtimes: []string{dockercli.RuncRuntime},
		SecurityOptions: []string{"name=seccomp,profile=builtin"}, CPUCFSQuota: true, MemoryLimit: true, SwapLimit: true, PIDsLimit: true,
	}, nil
}
func (e *recordingEngine) Create(_ context.Context, plan ContainerPlan) (string, error) {
	if e.containerID == "" {
		e.containerID = "container-test"
	}
	e.createdPlan = plan
	e.stopped = false
	return e.containerID, nil
}
func (e *recordingEngine) Start(context.Context, string) error { return nil }
func (e *recordingEngine) Inspect(_ context.Context, id string) (ContainerState, error) {
	return ContainerState{ID: id, Name: e.createdPlan.Name, Running: !e.stopped, Labels: e.openedContainerLabels(), Configuration: expectedAgentConfiguration(e.createdPlan)}, nil
}
func (e *recordingEngine) openedContainerLabels() map[string]string {
	return e.createdPlan.Labels
}
func (e *recordingEngine) Stop(context.Context, string, ports.StopMode) error {
	e.stopped = true
	return nil
}
func (e *recordingEngine) Remove(_ context.Context, id string) error {
	e.removed = id
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
var _ ports.ExecTransport = (*scriptedExecTransport)(nil)
