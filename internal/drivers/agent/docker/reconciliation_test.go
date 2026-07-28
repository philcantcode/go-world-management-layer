package docker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestRestartReconcileAdoptsExactReadyAgentContainer(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	_, restarted, plan := restartAgentDrivers(t, engine, input)
	engine.seed("container-ready", plan)

	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), []ports.AgentWorkspacePlan{input})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].ContainerID != "container-ready" {
		t.Fatalf("reconciliation = %#v", report)
	}
	status, err := restarted.Inspect(testDeadline(t), report.Expected[0].Ref)
	if err != nil || !status.Ready || status.ContainerID != "container-ready" {
		t.Fatalf("adopted Inspect() = %#v, %v", status, err)
	}
	replay, err := restarted.Provision(testDeadline(t), input)
	if err != nil || replay.Created {
		t.Fatalf("adopted idempotency replay = %#v, %v", replay, err)
	}
}

func TestRestartReconcileRejectsForeignAgentCollisions(t *testing.T) {
	tests := map[string]func(*ContainerState){
		"label mismatch": func(state *ContainerState) {
			state.Labels["world.policy-digest"] = domain.NewDigest([]byte("foreign-policy")).String()
		},
		"foreign name collision": func(state *ContainerState) { state.Labels = map[string]string{"owner": "someone-else"} },
		"image mismatch": func(state *ContainerState) {
			state.Configuration.Image = "example.invalid/foreign@" + domain.NewDigest([]byte("foreign-image")).String()
		},
		"runtime mismatch": func(state *ContainerState) { state.Configuration.Runtime = "foreign" },
		"user mismatch":    func(state *ContainerState) { state.Configuration.User = "65531:65531" },
		"seccomp mismatch": func(state *ContainerState) {
			state.Configuration.SecurityOptions = []string{dockercli.NoNewPrivilegesOption}
		},
		"swap mismatch":     func(state *ContainerState) { state.Configuration.MemorySwapBytes++ },
		"security mismatch": func(state *ContainerState) { state.Configuration.Privileged = true },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			engine := newInventoryEngine()
			input := testAgentWorkspacePlan(t)
			_, restarted, plan := restartAgentDrivers(t, engine, input)
			state := agentStateForPlan("container-foreign", plan)
			mutate(&state)
			engine.states[state.ID] = state
			report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), []ports.AgentWorkspacePlan{input})
			if err != nil {
				t.Fatal(err)
			}
			if got := report.Expected[0].Classification; got != ports.PhysicalResourceForeign {
				t.Fatalf("classification = %q, report %#v", got, report)
			}
			if _, err := restarted.Inspect(testDeadline(t), report.Expected[0].Ref); !domain.IsCode(err, domain.CodeNotFound) {
				t.Fatalf("foreign resource was adopted: %v", err)
			}
		})
	}
}

func TestRestartReconcileReportsAgentOrphansAndDuplicateIdentities(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	_, restarted, plan := restartAgentDrivers(t, engine, input)
	engine.seed("container-orphan", plan)

	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unclaimed) != 1 || report.Unclaimed[0].Classification != ports.PhysicalResourceOrphan {
		t.Fatalf("orphan report = %#v", report)
	}

	engine.seed("container-duplicate", plan)
	report, err = restarted.ReconcileAgentWorkspaces(testDeadline(t), []ports.AgentWorkspacePlan{input})
	if err != nil {
		t.Fatal(err)
	}
	if got := report.Expected[0].Classification; got != ports.PhysicalResourceUncertain {
		t.Fatalf("duplicate classification = %q, report %#v", got, report)
	}
}

func TestRestartReconcileMarksAgentMissingOnlyAfterAuthoritativeInventory(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	_, restarted, _ := restartAgentDrivers(t, engine, input)
	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), []ports.AgentWorkspacePlan{input})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceMissing {
		t.Fatalf("authoritative empty inventory = %#v, %v", report, err)
	}
	engine.inventoryErr = errors.New("inventory unavailable")
	report, err = restarted.ReconcileAgentWorkspaces(testDeadline(t), []ports.AgentWorkspacePlan{input})
	if err == nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("failed inventory = %#v, %v", report, err)
	}
}

func TestRestartDestroyAgentRequiresAndRemembersProvenAbsence(t *testing.T) {
	engine := newInventoryEngine()
	engine.stickyRemove = true
	input := testAgentWorkspacePlan(t)
	_, restarted, plan := restartAgentDrivers(t, engine, input)
	engine.seed("container-destroy", plan)
	ref := ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}

	if err := restarted.Destroy(testDeadline(t), ref); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("unproven Destroy() error = %v", err)
	}
	engine.mu.Lock()
	engine.stickyRemove = false
	engine.mu.Unlock()
	if err := restarted.Destroy(testDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Destroy(testDeadline(t), ref); err != nil {
		t.Fatalf("idempotent destroy after authoritative absence = %v", err)
	}
	if calls := engine.removeCalls(); calls != 2 {
		t.Fatalf("Remove calls = %d, want 2", calls)
	}
}

func restartAgentDrivers(t *testing.T, engine *inventoryEngine, input ports.AgentWorkspacePlan) (*Driver, *Driver, ContainerPlan) {
	t.Helper()
	config := Config{Build: BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent"}, Engine: engine}
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildContainerPlan(input, first.build)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return first, restarted, plan
}

func testDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

type inventoryEngine struct {
	mu           sync.Mutex
	states       map[string]ContainerState
	plans        map[string]ContainerPlan
	removed      int
	stickyRemove bool
	inventoryErr error
}

func newInventoryEngine() *inventoryEngine {
	return &inventoryEngine{states: make(map[string]ContainerState), plans: make(map[string]ContainerPlan)}
}

func (e *inventoryEngine) seed(id string, plan ContainerPlan) {
	e.mu.Lock()
	e.states[id], e.plans[id] = agentStateForPlan(id, plan), plan
	e.mu.Unlock()
}

func (e *inventoryEngine) Probe(context.Context) (EngineCapabilities, error) {
	return EngineCapabilities{}, nil
}

func (e *inventoryEngine) Create(_ context.Context, plan ContainerPlan) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := fmt.Sprintf("container-%d", len(e.states)+1)
	e.states[id], e.plans[id] = agentStateForPlan(id, plan), plan
	return id, nil
}

func (e *inventoryEngine) Start(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	state, found := e.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = true
	e.states[id] = state
	return nil
}

func (e *inventoryEngine) Inspect(_ context.Context, id string) (ContainerState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state, found := e.states[id]
	if !found {
		return ContainerState{}, errors.New("not found")
	}
	return cloneContainerState(state), nil
}

func (e *inventoryEngine) Stop(_ context.Context, id string, _ ports.StopMode) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	state, found := e.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = false
	e.states[id] = state
	return nil
}

func (e *inventoryEngine) Remove(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removed++
	if !e.stickyRemove {
		delete(e.states, id)
		delete(e.plans, id)
	}
	return nil
}

func (e *inventoryEngine) OpenExec(context.Context, string, string, ports.ExecPlan) (ports.ExecTransport, error) {
	return nil, errors.New("not used")
}

func (e *inventoryEngine) ListContainers(context.Context) ([]ContainerState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inventoryErr != nil {
		return nil, e.inventoryErr
	}
	result := make([]ContainerState, 0, len(e.states))
	for _, state := range e.states {
		result = append(result, cloneContainerState(state))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (e *inventoryEngine) removeCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.removed
}

func agentStateForPlan(id string, plan ContainerPlan) ContainerState {
	return ContainerState{ID: id, Name: plan.Name, Running: true, Status: "running", Labels: cloneTestLabels(plan.Labels), CgroupID: "cgroup/" + id, Configuration: expectedAgentConfiguration(plan)}
}

func cloneContainerState(state ContainerState) ContainerState {
	state.Labels = cloneTestLabels(state.Labels)
	state.Configuration.Entrypoint = append([]string(nil), state.Configuration.Entrypoint...)
	state.Configuration.Command = append([]string(nil), state.Configuration.Command...)
	state.Configuration.Mounts = append([]dockercli.Mount(nil), state.Configuration.Mounts...)
	return state
}

func cloneTestLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for name, value := range labels {
		result[name] = value
	}
	return result
}

var _ Engine = (*inventoryEngine)(nil)
var _ EngineInventory = (*inventoryEngine)(nil)
