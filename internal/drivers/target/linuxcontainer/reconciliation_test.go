package linuxcontainer

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

func TestRestartReconcileAdoptsExactReadyTarget(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed("runtime-ready", plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), []ports.TargetPlan{input})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].RuntimeID != "runtime-ready" {
		t.Fatalf("reconciliation = %#v", report)
	}
	result, err := restarted.Create(targetDeadline(t), input)
	if err != nil || result.Created || !result.Status.Ready || result.Status.RuntimeID != "runtime-ready" {
		t.Fatalf("adopted create replay = %#v, %v", result, err)
	}
}

func TestRestartReconcileRejectsForeignTargetCollisions(t *testing.T) {
	tests := map[string]func(*RuntimeState){
		"label mismatch": func(state *RuntimeState) {
			state.Labels["world.capability-digest"] = domain.NewDigest([]byte("foreign-capability")).String()
		},
		"foreign name collision": func(state *RuntimeState) { state.Labels = map[string]string{"owner": "someone-else"} },
		"mount mismatch":         func(state *RuntimeState) { state.Configuration.Mounts[0].Source += "-foreign" },
		"runtime mismatch":       func(state *RuntimeState) { state.Configuration.Runtime = "foreign" },
		"user mismatch":          func(state *RuntimeState) { state.Configuration.User = "65531:65531" },
		"seccomp mismatch": func(state *RuntimeState) {
			state.Configuration.SecurityOptions = []string{dockercli.NoNewPrivilegesOption}
		},
		"swap mismatch":     func(state *RuntimeState) { state.Configuration.MemorySwapBytes++ },
		"security mismatch": func(state *RuntimeState) { state.Configuration.NetworkMode = "host" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			runtime := newInventoryRuntime()
			input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
			_, restarted, plan := restartTargetDrivers(t, runtime, input)
			state := targetStateForPlan("runtime-foreign", plan)
			mutate(&state)
			runtime.states[state.ID] = state
			report, err := restarted.ReconcileTargets(targetDeadline(t), []ports.TargetPlan{input})
			if err != nil {
				t.Fatal(err)
			}
			if got := report.Expected[0].Classification; got != ports.PhysicalResourceForeign {
				t.Fatalf("classification = %q, report %#v", got, report)
			}
			if _, err := restarted.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
				t.Fatalf("foreign target was adopted: %v", err)
			}
		})
	}
}

func TestRestartReconcileReportsTargetOrphansAndDuplicateIdentities(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed("runtime-orphan", plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unclaimed) != 1 || report.Unclaimed[0].Classification != ports.PhysicalResourceOrphan {
		t.Fatalf("orphan report = %#v", report)
	}

	runtime.seed("runtime-duplicate", plan)
	report, err = restarted.ReconcileTargets(targetDeadline(t), []ports.TargetPlan{input})
	if err != nil {
		t.Fatal(err)
	}
	if got := report.Expected[0].Classification; got != ports.PhysicalResourceUncertain {
		t.Fatalf("duplicate classification = %q, report %#v", got, report)
	}
}

func TestRestartReconcileMarksTargetMissingOnlyAfterAuthoritativeInventory(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, _ := restartTargetDrivers(t, runtime, input)
	report, err := restarted.ReconcileTargets(targetDeadline(t), []ports.TargetPlan{input})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceMissing {
		t.Fatalf("authoritative empty inventory = %#v, %v", report, err)
	}
	runtime.inventoryErr = errors.New("inventory unavailable")
	report, err = restarted.ReconcileTargets(targetDeadline(t), []ports.TargetPlan{input})
	if err == nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("failed inventory = %#v, %v", report, err)
	}
}

func TestRestartDestroyTargetRequiresAndRemembersProvenAbsence(t *testing.T) {
	runtime := newInventoryRuntime()
	runtime.stickyRemove = true
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed("runtime-destroy", plan)
	ref := ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}

	if err := restarted.Destroy(targetDeadline(t), ref); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("unproven Destroy() error = %v", err)
	}
	runtime.mu.Lock()
	runtime.stickyRemove = false
	runtime.mu.Unlock()
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatalf("idempotent destroy after authoritative absence = %v", err)
	}
	if calls := runtime.removeCalls(); calls != 2 {
		t.Fatalf("Remove calls = %d, want 2", calls)
	}
}

func restartTargetDrivers(t *testing.T, runtime *inventoryRuntime, input ports.TargetPlan) (*Driver, *Driver, ContainerPlan) {
	t.Helper()
	config := Config{
		Build: BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target"}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
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

func targetDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

type inventoryRuntime struct {
	mu           sync.Mutex
	states       map[string]RuntimeState
	plans        map[string]ContainerPlan
	removed      int
	stickyRemove bool
	inventoryErr error
}

func newInventoryRuntime() *inventoryRuntime {
	return &inventoryRuntime{states: make(map[string]RuntimeState), plans: make(map[string]ContainerPlan)}
}

func (r *inventoryRuntime) seed(id string, plan ContainerPlan) {
	r.mu.Lock()
	r.states[id], r.plans[id] = targetStateForPlan(id, plan), plan
	r.mu.Unlock()
}

func (*inventoryRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{}, nil
}

func (r *inventoryRuntime) Create(_ context.Context, plan ContainerPlan) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.states {
		if state.Name == plan.Name {
			return "", errors.New("name already exists")
		}
	}
	id := fmt.Sprintf("runtime-%d", len(r.states)+1)
	r.states[id], r.plans[id] = targetStateForPlan(id, plan), plan
	return id, nil
}

func (r *inventoryRuntime) Start(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = true
	r.states[id] = state
	return nil
}

func (r *inventoryRuntime) Inspect(_ context.Context, id string) (RuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return RuntimeState{}, errors.New("not found")
	}
	return cloneRuntimeState(state), nil
}

func (r *inventoryRuntime) Stop(_ context.Context, id string, _ ports.StopMode) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = false
	r.states[id] = state
	return nil
}

func (r *inventoryRuntime) Remove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed++
	if !r.stickyRemove {
		delete(r.states, id)
		delete(r.plans, id)
	}
	return nil
}

func (*inventoryRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	return nil, errors.New("not used")
}

func (r *inventoryRuntime) ListContainers(context.Context) ([]RuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inventoryErr != nil {
		return nil, r.inventoryErr
	}
	result := make([]RuntimeState, 0, len(r.states))
	for _, state := range r.states {
		result = append(result, cloneRuntimeState(state))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (r *inventoryRuntime) removeCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removed
}

func targetStateForPlan(id string, plan ContainerPlan) RuntimeState {
	return RuntimeState{ID: id, Name: plan.Name, Running: true, Status: "running", Labels: cloneStrings(plan.Labels), CgroupID: "cgroup/" + id, Configuration: expectedTargetConfiguration(plan)}
}

func cloneRuntimeState(state RuntimeState) RuntimeState {
	state.Labels = cloneStrings(state.Labels)
	state.Configuration.Entrypoint = append([]string(nil), state.Configuration.Entrypoint...)
	state.Configuration.Command = append([]string(nil), state.Configuration.Command...)
	state.Configuration.Mounts = append([]dockercli.Mount(nil), state.Configuration.Mounts...)
	return state
}

var _ Runtime = (*inventoryRuntime)(nil)
var _ RuntimeInventory = (*inventoryRuntime)(nil)
