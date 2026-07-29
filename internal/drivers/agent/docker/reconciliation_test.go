package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestRestartReconcileRequiresCurrentDaemonGuestReadinessBeforeAdoption(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	_, restarted, plan := restartAgentDrivers(t, engine, input)
	containerID := testContainerID('a')
	engine.seed(containerID, plan)

	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceUncertain || report.Expected[0].ContainerID != containerID {
		t.Fatalf("reconciliation = %#v", report)
	}
	if _, err := restarted.Inspect(testDeadline(t), report.Expected[0].Ref); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("unprobed container was committed as ready: %v", err)
	}

	engine.readiness = successfulReadinessFrames(t)
	provisioned, err := restarted.Provision(testDeadline(t), input)
	if err != nil || provisioned.Created || !provisioned.Status.Ready {
		t.Fatalf("recovered Provision() = %#v, %v", provisioned, err)
	}
	report, err = restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("post-readiness reconciliation = %#v, %v", report, err)
	}
	status, err := restarted.Inspect(testDeadline(t), report.Expected[0].Ref)
	if err != nil || !status.Ready || status.ContainerID != containerID {
		t.Fatalf("readiness-proven Inspect() = %#v, %v", status, err)
	}
	replay, err := restarted.Provision(testDeadline(t), input)
	if err != nil || replay.Created {
		t.Fatalf("readiness-proven idempotency replay = %#v, %v", replay, err)
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
		"restart policy mismatch": func(state *ContainerState) {
			state.Configuration.RestartPolicy = dockercli.RestartPolicy{Name: "always"}
		},
		"auto remove mismatch": func(state *ContainerState) { state.Configuration.AutoRemove = true },
		"supplementary group mismatch": func(state *ContainerState) {
			state.Configuration.GroupAdd = []string{"999"}
		},
		"device request mismatch": func(state *ContainerState) {
			state.Configuration.DeviceRequests = []dockercli.DeviceRequest{{Driver: "gpu", Count: 1}}
		},
		"network attachment mismatch": func(state *ContainerState) {
			state.Configuration.NetworkAttachments = []string{"bridge"}
		},
		"working directory mismatch": func(state *ContainerState) { state.Configuration.WorkingDir = "/tmp" },
		"stdin once mismatch":        func(state *ContainerState) { state.Configuration.StdinOnce = false },
		"cgroup mismatch":            func(state *ContainerState) { state.Configuration.Cgroup = "foreign" },
		"configured mount mismatch": func(state *ContainerState) {
			state.Configuration.ConfiguredMounts[0].BindOptionsKnown = true
			state.Configuration.ConfiguredMounts[0].BindOptions.NonRecursive = true
		},
		"tty mismatch": func(state *ContainerState) { state.Configuration.TTY = true },
		"environment mismatch": func(state *ContainerState) {
			state.Configuration.Environment = []string{"UNPLANNED=true"}
		},
		"healthcheck mismatch": func(state *ContainerState) {
			state.Configuration.HealthcheckKnown = true
			state.Configuration.Healthcheck = dockercli.Healthcheck{Test: []string{"CMD", "false"}}
		},
		"stop signal mismatch": func(state *ContainerState) { state.Configuration.StopSignal = "SIGKILL" },
		"stop timeout mismatch": func(state *ContainerState) {
			state.Configuration.StopTimeoutKnown = true
			state.Configuration.StopTimeout = 1
		},
		"memory reservation mismatch": func(state *ContainerState) { state.Configuration.MemoryReservation = 1 },
		"cpu shares mismatch":         func(state *ContainerState) { state.Configuration.CPUShares = 1 },
		"cpu quota mismatch":          func(state *ContainerState) { state.Configuration.CPUQuota = 1 },
		"cpuset mismatch":             func(state *ContainerState) { state.Configuration.CpusetCPUs = "0" },
		"ulimit mismatch": func(state *ContainerState) {
			state.Configuration.Ulimits = []dockercli.Ulimit{{Name: "nofile", Soft: 1, Hard: 1}}
		},
		"sysctl mismatch": func(state *ContainerState) {
			state.Configuration.Sysctls = map[string]string{"kernel.domainname": "foreign"}
		},
		"masked paths mismatch": func(state *ContainerState) {
			state.Configuration.MaskedPaths = append(state.Configuration.MaskedPaths, "/foreign")
		},
		"readonly paths mismatch": func(state *ContainerState) {
			state.Configuration.ReadonlyPaths = append(state.Configuration.ReadonlyPaths, "/foreign")
		},
		"shared memory mismatch": func(state *ContainerState) { state.Configuration.ShmSize++ },
		"log driver mismatch": func(state *ContainerState) {
			state.Configuration.LogConfig = dockercli.LogConfiguration{Type: "json-file"}
		},
		"volume driver mismatch": func(state *ContainerState) { state.Configuration.VolumeDriver = "local" },
		"storage option mismatch": func(state *ContainerState) {
			state.Configuration.StorageOptions = map[string]string{"size": "1G"}
		},
		"paused running state":     func(state *ContainerState) { state.Paused = true },
		"restarting running state": func(state *ContainerState) { state.Restarting = true },
		"dead running state":       func(state *ContainerState) { state.Dead = true },
		"noncanonical live status": func(state *ContainerState) { state.Status = "paused" },
		"paused stopped state": func(state *ContainerState) {
			state.Running, state.Paused, state.Status = false, true, "paused"
		},
		"restarting stopped state": func(state *ContainerState) {
			state.Running, state.Restarting, state.Status = false, true, "restarting"
		},
		"dead stopped state": func(state *ContainerState) {
			state.Running, state.Dead, state.Status = false, true, "dead"
		},
		"unknown stopped state": func(state *ContainerState) { state.Running, state.Status = false, "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			engine := newInventoryEngine()
			input := testAgentWorkspacePlan(t)
			_, restarted, plan := restartAgentDrivers(t, engine, input)
			state := agentStateForPlan(testContainerID('b'), plan)
			mutate(&state)
			engine.states[state.ID] = state
			report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
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
	engine.seed(testContainerID('c'), plan)

	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unclaimed) != 1 || report.Unclaimed[0].Classification != ports.PhysicalResourceOrphan {
		t.Fatalf("orphan report = %#v", report)
	}

	engine.seed(testContainerID('d'), plan)
	report, err = restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
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
	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceMissing {
		t.Fatalf("authoritative empty inventory = %#v, %v", report, err)
	}
	engine.inventoryErr = errors.New("inventory unavailable")
	report, err = restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err == nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("failed inventory = %#v, %v", report, err)
	}
}

func TestRestartDestroyAgentRequiresAndRemembersProvenAbsence(t *testing.T) {
	engine := newInventoryEngine()
	engine.stickyRemove = true
	input := testAgentWorkspacePlan(t)
	_, restarted, plan := restartAgentDrivers(t, engine, input)
	engine.seed(testContainerID('e'), plan)
	ref := ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}

	if err := restarted.Destroy(testDeadline(t), ref); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("unproven Destroy() error = %v", err)
	}
	if calls := engine.removeCalls(); calls != 0 {
		t.Fatalf("unproven Destroy called Remove %d times", calls)
	}
	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{CleanupOnly: []ports.AgentWorkspacePlan{input}})
	if err != nil || len(report.Expected) != 1 || !report.Expected[0].PlanMatched {
		t.Fatalf("cleanup-only reconciliation = %#v, %v", report, err)
	}
	if _, err := restarted.Inspect(testDeadline(t), ref); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("cleanup-only generation became executable: %v", err)
	}
	if err := restarted.Destroy(testDeadline(t), ref); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("sticky reconciled Destroy() error = %v", err)
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

func TestRestartCleanupOnlyAgentRejectsEveryPlanMismatchWithoutRemove(t *testing.T) {
	tests := map[string]func(*ContainerState){
		"lease": func(state *ContainerState) { state.Labels["world.lease"] = testAgentWorkspacePlan(t).LeaseID.String() },
		"workspace": func(state *ContainerState) {
			state.Labels["world.workspace"] = testAgentWorkspacePlan(t).Workspace.ID().String()
		},
		"policy": func(state *ContainerState) {
			state.Labels["world.policy-digest"] = domain.NewDigest([]byte("wrong-policy")).String()
		},
		"plan": func(state *ContainerState) {
			state.Labels[planDigestLabel] = domain.NewDigest([]byte("wrong-plan")).String()
		},
		"configuration": func(state *ContainerState) { state.Configuration.MemoryBytes++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			engine := newInventoryEngine()
			input := testAgentWorkspacePlan(t)
			_, restarted, plan := restartAgentDrivers(t, engine, input)
			state := agentStateForPlan(testContainerID('f'), plan)
			mutate(&state)
			engine.states[state.ID] = state
			report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{CleanupOnly: []ports.AgentWorkspacePlan{input}})
			if err != nil {
				t.Fatal(err)
			}
			if report.Expected[0].Classification != ports.PhysicalResourceForeign || report.Expected[0].PlanMatched {
				t.Fatalf("mismatch report = %#v", report)
			}
			ref := ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}
			if err := restarted.Destroy(testDeadline(t), ref); !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("Destroy mismatch error = %v", err)
			}
			if calls := engine.removeCalls(); calls != 0 {
				t.Fatalf("mismatch called Remove %d times", calls)
			}
		})
	}
}

func restartAgentDrivers(t *testing.T, engine *inventoryEngine, input ports.AgentWorkspacePlan) (*Driver, *Driver, ContainerPlan) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, input.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engine,
	}
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
	mu                   sync.Mutex
	states               map[string]ContainerState
	plans                map[string]ContainerPlan
	readiness            []transport.Frame
	openExecErr          error
	panicBeforeStart     bool
	panicBeforeReadiness bool
	created              int
	started              int
	opened               int
	removed              int
	stickyRemove         bool
	inventoryErr         error
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
	id := fmt.Sprintf("%064x", len(e.states)+1)
	state := agentStateForPlan(id, plan)
	state.Running = false
	state.Status = "created"
	e.states[id], e.plans[id] = state, plan
	e.created++
	return id, nil
}

func (e *inventoryEngine) Start(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.panicBeforeStart {
		panic("injected crash before Docker start")
	}
	state, found := e.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = true
	state.Status = "running"
	e.states[id] = state
	e.started++
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
	state.Status = "exited"
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
	e.mu.Lock()
	panicBeforeReadiness := e.panicBeforeReadiness
	err := e.openExecErr
	frames := append([]transport.Frame(nil), e.readiness...)
	e.opened++
	e.mu.Unlock()
	if panicBeforeReadiness {
		panic("injected crash before framed guest readiness")
	}
	if err != nil {
		return nil, err
	}
	return &scriptedExecTransport{frames: frames}, nil
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
	state.Configuration.ConfiguredMounts = append([]dockercli.ConfiguredMount(nil), state.Configuration.ConfiguredMounts...)
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

func testContainerID(character byte) string {
	return strings.Repeat(string(character), 64)
}
