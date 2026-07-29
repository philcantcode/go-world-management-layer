package docker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestRecoverInterruptedExecsStopsOldBoundaryBeforeFreshReadiness(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	base.readiness = successfulReadinessFrames(t)
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, plan := newExecCrashDriver(t, engine, input)
	containerID := testContainerID('a')
	base.seed(containerID, plan)

	proof, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.ValidateFor(input); err != nil {
		t.Fatalf("recovery proof = %#v: %v", proof, err)
	}
	if strings.Join(engine.events, ",") != "stop,start,readiness" {
		t.Fatalf("recovery events = %v, want stop/start/readiness", engine.events)
	}
	if len(engine.stopModes) != 1 || engine.stopModes[0] != ports.StopForce {
		t.Fatalf("stop modes = %v, want force", engine.stopModes)
	}
	status, err := restarted.Inspect(testDeadline(t), ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation})
	if err != nil || !status.Ready || status.ContainerID != containerID {
		t.Fatalf("restarted status = %#v, %v", status, err)
	}
}

func TestRecoverInterruptedExecsStopsOldBoundaryBeforeWorkspaceAccess(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, plan := newExecCrashDriver(t, engine, input)
	containerID := testContainerID('a')
	base.seed(containerID, plan)
	escape := filepath.Join(plan.ExpectedWorkspaceSource, "escape")
	if err := os.Symlink(t.TempDir(), escape); err != nil {
		t.Skipf("symlink unavailable for workspace-access ordering proof: %v", err)
	}

	_, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err == nil || !strings.Contains(err.Error(), "workspace") {
		t.Fatalf("workspace access failure = %v", err)
	}
	if strings.Join(engine.events, ",") != "stop" || base.opened != 0 {
		t.Fatalf("workspace validation ran before boundary stop: events=%v opened=%d", engine.events, base.opened)
	}
	state, inspectErr := base.Inspect(testDeadline(t), containerID)
	if inspectErr != nil || state.Running {
		t.Fatalf("old boundary was not proven stopped before workspace validation: state=%#v err=%v", state, inspectErr)
	}
}

func TestRecoverInterruptedExecsReplaysStopSuccessCrashBeforeStart(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	base.panicBeforeStart = true
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, plan := newExecCrashDriver(t, engine, input)
	containerID := testContainerID('a')
	base.seed(containerID, plan)

	crashed := false
	func() {
		defer func() { crashed = recover() != nil }()
		_, _ = restarted.RecoverInterruptedExecs(testDeadline(t), input)
	}()
	if !crashed {
		t.Fatal("recovery did not reach the injected crash before start")
	}
	state, err := base.Inspect(testDeadline(t), containerID)
	if err != nil || state.Running || base.opened != 0 {
		t.Fatalf("crash boundary state=%#v err=%v readiness=%d", state, err, base.opened)
	}

	base.panicBeforeStart = false
	base.readiness = successfulReadinessFrames(t)
	proof, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.ValidateFor(input); err != nil {
		t.Fatal(err)
	}
	if strings.Join(engine.events, ",") != "stop,start,start,readiness" || len(engine.stopModes) != 1 {
		t.Fatalf("replayed recovery events=%v stops=%v", engine.events, engine.stopModes)
	}
}

func TestRecoverInterruptedExecsReplaysStartSuccessBeforeLogicalFinalization(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	base.readiness = successfulReadinessFrames(t)
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, plan := newExecCrashDriver(t, engine, input)
	base.seed(testContainerID('a'), plan)

	first, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ValidateFor(input); err != nil {
		t.Fatal(err)
	}
	if err := second.ValidateFor(input); err != nil {
		t.Fatal(err)
	}
	if first.Status.ContainerID != second.Status.ContainerID || strings.Join(engine.events, ",") != "stop,start,readiness,stop,start,readiness" {
		t.Fatalf("idempotent recovery proofs=%#v/%#v events=%v", first, second, engine.events)
	}
}

func TestRecoverInterruptedExecsUsesAuthoritativeAbsenceAsOldBoundaryProof(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	base.readiness = successfulReadinessFrames(t)
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, _ := newExecCrashDriver(t, engine, input)

	proof, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.ValidateFor(input); err != nil {
		t.Fatal(err)
	}
	if strings.Join(engine.events, ",") != "start,readiness" || len(engine.stopModes) != 0 || base.created != 1 {
		t.Fatalf("missing-boundary recovery events=%v stops=%v creates=%d", engine.events, engine.stopModes, base.created)
	}
}

func TestRecoverInterruptedExecsFailsClosedWhenStopDoesNotTerminateBoundary(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	base.readiness = successfulReadinessFrames(t)
	engine := &execCrashEngine{inventoryEngine: base, leaveRunning: true}
	restarted, plan := newExecCrashDriver(t, engine, input)
	base.seed(testContainerID('b'), plan)

	_, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err == nil || !strings.Contains(err.Error(), "remained running") {
		t.Fatalf("non-terminating stop error = %v", err)
	}
	if strings.Join(engine.events, ",") != "stop" || base.opened != 0 {
		t.Fatalf("failed recovery crossed into readiness: events=%v opened=%d", engine.events, base.opened)
	}
}

func TestRecoverInterruptedExecsRejectsForeignExactNameWithoutStopping(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	base := newInventoryEngine()
	engine := &execCrashEngine{inventoryEngine: base}
	restarted, plan := newExecCrashDriver(t, engine, input)
	containerID := testContainerID('c')
	base.seed(containerID, plan)
	base.mu.Lock()
	state := base.states[containerID]
	state.Configuration.NetworkMode = "bridge"
	base.states[containerID] = state
	base.mu.Unlock()

	_, err := restarted.RecoverInterruptedExecs(testDeadline(t), input)
	if err == nil {
		t.Fatal("foreign container was accepted")
	}
	if len(engine.events) != 0 || len(engine.stopModes) != 0 {
		t.Fatalf("foreign container was mutated: events=%v stops=%v", engine.events, engine.stopModes)
	}
}

type execCrashEngine struct {
	*inventoryEngine
	events       []string
	stopModes    []ports.StopMode
	stopErr      error
	leaveRunning bool
}

func (e *execCrashEngine) Stop(ctx context.Context, id string, mode ports.StopMode) error {
	e.events = append(e.events, "stop")
	e.stopModes = append(e.stopModes, mode)
	if e.stopErr != nil {
		return e.stopErr
	}
	if e.leaveRunning {
		return nil
	}
	return e.inventoryEngine.Stop(ctx, id, mode)
}

func (e *execCrashEngine) Start(ctx context.Context, id string) error {
	e.events = append(e.events, "start")
	return e.inventoryEngine.Start(ctx, id)
}

func (e *execCrashEngine) OpenExec(ctx context.Context, id, guest string, plan ports.ExecPlan) (ports.ExecTransport, error) {
	e.events = append(e.events, "readiness")
	return e.inventoryEngine.OpenExec(ctx, id, guest, plan)
}

var _ Engine = (*execCrashEngine)(nil)
var _ EngineInventory = (*execCrashEngine)(nil)

func newExecCrashDriver(t *testing.T, engine Engine, input ports.AgentWorkspacePlan) (*Driver, ContainerPlan) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, input.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	driver, err := New(Config{Build: BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent"}, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildContainerPlan(input, driver.build)
	if err != nil {
		t.Fatal(err)
	}
	return driver, plan
}
