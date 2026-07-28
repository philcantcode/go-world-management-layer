package linuxcontainer

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestRecoverInterruptedRunResetsRuntimeAndNeverStartsOrArmsDuration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed target directories require the dedicated Linux host filesystem")
	}
	physical := &crashRecoveryRuntime{}
	driver, authority := lifecycleTestDriverWithRuntime(t, physical)
	target := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	physical.state = RuntimeState{
		ID: "runtime-1", Name: target.plan.Name, Running: true, Labels: cloneStrings(target.plan.Labels),
		CgroupID: "old-cgroup", Configuration: expectedTargetConfiguration(target.plan),
	}
	run := driver.runs[authority.RunID.String()]
	plan := run.plan
	plan.RequiredCoverage = []string{ports.TargetLifecycleSignal}
	plan.Collectors = nil
	driver.runs = make(map[string]*runRecord)
	driver.idempotency = make(map[string]string)
	driver.materialized = make(map[string]*materializationState)
	driver.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, 64))
	timers := 0
	driver.afterFunc = func(time.Duration, func()) RunTimer {
		timers++
		return &manualRunTimer{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prepared, err := driver.RecoverInterruptedRun(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	wantActions := []string{"inspect:runtime-1", "stop:runtime-1:force", "inspect:runtime-1", "start:runtime-1", "inspect:runtime-1"}
	if !reflect.DeepEqual(physical.actions, wantActions) {
		t.Fatalf("runtime crash recovery actions = %v, want %v", physical.actions, wantActions)
	}
	if timers != 0 {
		t.Fatalf("crash recovery armed %d maximum-duration timers", timers)
	}
	recovered := driver.runs[authority.RunID.String()]
	if recovered == nil || recovered.started || recovered.timer != nil || prepared.RunID != authority.RunID {
		t.Fatalf("recovered run is not prepared-only: prepared=%#v record=%#v", prepared, recovered)
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != ports.RunFailed || receipt.FailureKind != ports.TargetRunFailureNeverStarted || len(receipt.Observations) != 2 || receipt.Observations[0].Kind != "target.run.control-plane-loss" || receipt.Observations[1].Kind != "target.run.never-started" {
		t.Fatalf("recovered stop receipt = %#v", receipt)
	}
}

type crashRecoveryRuntime struct {
	state   RuntimeState
	actions []string
}

func (r *crashRecoveryRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{}, nil
}

func (r *crashRecoveryRuntime) Create(context.Context, ContainerPlan) (string, error) {
	return "", fmt.Errorf("unexpected create")
}

func (r *crashRecoveryRuntime) Start(_ context.Context, id string) error {
	r.actions = append(r.actions, "start:"+id)
	r.state.Running = true
	r.state.CgroupID = "new-cgroup"
	return nil
}

func (r *crashRecoveryRuntime) Inspect(_ context.Context, id string) (RuntimeState, error) {
	r.actions = append(r.actions, "inspect:"+id)
	state := r.state
	state.Labels = cloneStrings(r.state.Labels)
	return state, nil
}

func (r *crashRecoveryRuntime) Stop(_ context.Context, id string, mode ports.StopMode) error {
	r.actions = append(r.actions, "stop:"+id+":"+string(mode))
	r.state.Running = false
	return nil
}

func (r *crashRecoveryRuntime) Remove(context.Context, string) error {
	return fmt.Errorf("unexpected remove")
}

func (r *crashRecoveryRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	return nil, fmt.Errorf("unexpected exec")
}

var _ Runtime = (*crashRecoveryRuntime)(nil)
var _ ports.TargetRunCrashReconciler = (*Driver)(nil)
