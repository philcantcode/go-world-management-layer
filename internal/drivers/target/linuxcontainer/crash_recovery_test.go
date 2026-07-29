package linuxcontainer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
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
		ID: testRuntimeID("runtime-1"), Name: target.plan.Name, Running: true, Labels: cloneStrings(target.plan.Labels),
		CgroupID: "old-cgroup", Configuration: expectedTargetConfiguration(target.plan),
	}
	setLifecycleCoverage(t, driver, authority.RunID, []string{ports.TargetLifecycleSignal})
	run := driver.runs[authority.RunID.String()]
	plan := run.plan
	crashOutput := []byte("written before controller loss")
	if err := os.WriteFile(filepath.Join(target.plan.writableRoot(), "pre-crash-result.bin"), crashOutput, 0o600); err != nil {
		t.Fatal(err)
	}
	priorStartedAt := time.Unix(20, 0).UTC()
	if err := persistRunStart(run.directory, run.authority, priorStartedAt, testRuntimeID("runtime-1"), "old-cgroup", plan.Run.Spec().MaterializationDigest); err != nil {
		t.Fatal(err)
	}
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
	wantActions := []string{
		"inspect:" + testRuntimeID("runtime-1"),
		"inspect:" + testRuntimeID("runtime-1"),
		"stop:" + testRuntimeID("runtime-1") + ":force",
		"inspect:" + testRuntimeID("runtime-1"),
	}
	if !reflect.DeepEqual(physical.actions, wantActions) {
		t.Fatalf("runtime crash recovery actions = %v, want %v", physical.actions, wantActions)
	}
	if timers != 0 {
		t.Fatalf("crash recovery armed %d maximum-duration timers", timers)
	}
	recovered := driver.runs[authority.RunID.String()]
	if recovered == nil || recovered.started || !recovered.controlPlaneLost || recovered.timer != nil || prepared.RunID != authority.RunID {
		t.Fatalf("recovered run is not prepared-only: prepared=%#v record=%#v", prepared, recovered)
	}
	receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != ports.RunFailed || receipt.FailureKind != ports.TargetRunFailureTarget || receipt.StartedAt != priorStartedAt || len(receipt.Observations) != 2 || receipt.Observations[0].Kind != "target.run.control-plane-loss" || receipt.Observations[1].Kind != "target.run.control-plane-failure" {
		t.Fatalf("recovered stop receipt = %#v", receipt)
	}
	if bytes.Contains(receipt.Observations[0].Payload, []byte("prior_execution_terminated")) {
		t.Fatalf("recovery fabricated prior-execution proof: %s", receipt.Observations[0].Payload)
	}
	if !bytes.Contains(receipt.Observations[0].Payload, []byte(`"run_start_committed":true`)) {
		t.Fatalf("recovery omitted the durable prior-start proof: %s", receipt.Observations[0].Payload)
	}
	changes := receipt.TargetChanges.Entries()
	if len(changes) != 1 || changes[0].Path() != "pre-crash-result.bin" || changes[0].Spec().AfterDigest != domain.NewDigest(crashOutput) {
		t.Fatalf("recovered durable target changes = %#v", changes)
	}
}

func TestRecoverInterruptedRunAdoptsDurableStoppedBoundaryWithoutRestart(t *testing.T) {
	physical := &crashRecoveryRuntime{}
	driver, authority := lifecycleTestDriverWithRuntime(t, physical)
	target := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	physical.state = RuntimeState{
		ID: testRuntimeID("runtime-1"), Name: target.plan.Name, Running: true, Labels: cloneStrings(target.plan.Labels),
		CgroupID: "run-cgroup", Configuration: expectedTargetConfiguration(target.plan),
	}
	setLifecycleCoverage(t, driver, authority.RunID, []string{ports.TargetLifecycleSignal})
	runPlan := driver.runs[authority.RunID.String()].plan
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := driver.StartRun(ctx, authority.RunID); err != nil {
		t.Fatal(err)
	}
	crashWindowOutput := []byte("sealed before observer finalization")
	if err := os.WriteFile(filepath.Join(target.plan.writableRoot(), "stop-crash-window.bin"), crashWindowOutput, 0o600); err != nil {
		t.Fatal(err)
	}
	if receipt, err := driver.StopRun(ctx, authority.RunID, ports.StopImmediate); err != nil || receipt.Outcome != ports.RunCompleted {
		t.Fatalf("pre-crash driver stop = %#v, %v", receipt, err)
	}
	claim, err := requireTargetGenerationRunClaim(target.plan.TargetDirectory, runPlan)
	if err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Join(target.plan.TargetDirectory, "runs", authority.RunID.String())
	boundaryBefore, found, err := loadStoppedRunBoundary(runDirectory, claim, target.runtimeID)
	if err != nil || !found {
		t.Fatalf("pre-crash stopped boundary = %#v, found=%t, err=%v", boundaryBefore, found, err)
	}

	restarted, err := New(Config{
		Build: driver.build, Runtime: physical,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	timers := 0
	restarted.afterFunc = func(time.Duration, func()) RunTimer {
		timers++
		return &manualRunTimer{}
	}
	physical.actions = nil
	reconciliation, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{target.input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reconciliation.Expected) != 1 || reconciliation.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("stopped-boundary reconciliation = %#v", reconciliation)
	}
	adopted, err := restarted.requireTarget(authority.TargetID, authority.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if adopted.status.State != domain.TargetGenerationResettable || adopted.status.Ready {
		t.Fatalf("adopted stopped generation = %#v", adopted.status)
	}
	if _, err := restarted.RecoverInterruptedRun(ctx, runPlan); err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Outcome != ports.RunFailed || recovered.FailureKind != ports.TargetRunFailureTarget || len(recovered.Observations) != 2 || recovered.Observations[0].Kind != "target.run.control-plane-loss" || recovered.Observations[1].Kind != "target.run.control-plane-failure" {
		t.Fatalf("post-crash recovered receipt = %#v", recovered)
	}
	boundaryAfter, found, err := loadStoppedRunBoundary(runDirectory, claim, target.runtimeID)
	if err != nil || !found || !reflect.DeepEqual(boundaryAfter, boundaryBefore) {
		t.Fatalf("recovery replaced the durable boundary: before=%#v after=%#v found=%t err=%v", boundaryBefore, boundaryAfter, found, err)
	}
	if changes := recovered.TargetChanges.Entries(); len(changes) != 1 || changes[0].Path() != "stop-crash-window.bin" || changes[0].Spec().AfterDigest != domain.NewDigest(crashWindowOutput) {
		t.Fatalf("post-crash recovered target changes = %#v", changes)
	}
	if timers != 0 {
		t.Fatalf("already-stopped recovery armed %d duration timers", timers)
	}
	for _, action := range physical.actions {
		if strings.HasPrefix(action, "start:") || strings.HasPrefix(action, "stop:") || strings.HasPrefix(action, "exec:") {
			t.Fatalf("already-stopped recovery reopened execution authority: actions=%v", physical.actions)
		}
	}
}

func TestRecoverInterruptedRunCompletesClaimBeforeRunDirectoryWindow(t *testing.T) {
	physical := &crashRecoveryRuntime{}
	driver, authority := lifecycleTestDriverWithRuntime(t, physical)
	setLifecycleCoverage(t, driver, authority.RunID, []string{ports.TargetLifecycleSignal})
	target := driver.targets[targetKey(authority.TargetID, authority.Generation)]
	runPlan := driver.runs[authority.RunID.String()].plan
	physical.state = RuntimeState{
		ID: testRuntimeID("runtime-1"), Name: target.plan.Name, Running: true, Labels: cloneStrings(target.plan.Labels),
		CgroupID: "claimed-cgroup", Configuration: expectedTargetConfiguration(target.plan),
	}
	runDirectory := filepath.Join(target.plan.TargetDirectory, "runs", authority.RunID.String())
	if err := removeManagedDirectory(driver.build.TargetRoot, runDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := requireTargetGenerationRunClaim(target.plan.TargetDirectory, runPlan); err != nil {
		t.Fatalf("claim did not survive removed run directory: %v", err)
	}

	restarted, err := New(Config{
		Build: driver.build, Runtime: physical,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	timers := 0
	restarted.afterFunc = func(time.Duration, func()) RunTimer {
		timers++
		return &manualRunTimer{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reconciliation, err := restarted.ReconcileTargets(ctx, ports.TargetReconciliationRequest{Active: []ports.TargetPlan{target.input}})
	if err != nil {
		t.Fatal(err)
	}
	if reconciliation.Expected[0].Classification != ports.PhysicalResourceAdopted || physical.state.Running {
		t.Fatalf("claimed preparation reconciliation = %#v, runtime=%#v", reconciliation, physical.state)
	}
	for _, action := range physical.actions {
		if strings.HasPrefix(action, "exec:") {
			t.Fatalf("claimed running recovery used guest readiness before containment: %v", physical.actions)
		}
	}
	prepared, err := restarted.RecoverInterruptedRun(ctx, runPlan)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.RunID != authority.RunID {
		t.Fatalf("recovered preparation = %#v", prepared)
	}
	if _, err := loadRunBaseline(runDirectory); err != nil {
		t.Fatalf("recovery did not restore the durable pre-execution baseline: %v", err)
	}
	receipt, err := restarted.StopRun(ctx, authority.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != ports.RunFailed || receipt.FailureKind != ports.TargetRunFailureNeverStarted || !receipt.StartedAt.IsZero() || len(receipt.TargetChanges.Entries()) != 0 {
		t.Fatalf("claim-window recovery receipt = %#v", receipt)
	}
	if timers != 0 {
		t.Fatalf("claim-window recovery armed %d duration timers", timers)
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
	r.state.Status = "running"
	r.state.CgroupID = "new-cgroup"
	return nil
}

func (r *crashRecoveryRuntime) Inspect(_ context.Context, id string) (RuntimeState, error) {
	r.actions = append(r.actions, "inspect:"+id)
	state := r.state
	if state.Status == "" {
		if state.Running {
			state.Status = "running"
		} else {
			state.Status = "exited"
		}
	}
	state.Labels = cloneStrings(r.state.Labels)
	return state, nil
}

func (r *crashRecoveryRuntime) Stop(_ context.Context, id string, mode ports.StopMode) error {
	r.actions = append(r.actions, "stop:"+id+":"+string(mode))
	r.state.Running = false
	r.state.Status = "exited"
	return nil
}

func (r *crashRecoveryRuntime) Remove(context.Context, string) error {
	return fmt.Errorf("unexpected remove")
}

func (r *crashRecoveryRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	r.actions = append(r.actions, "exec:"+r.state.ID)
	return successfulTargetReadinessTransport(), nil
}

func (r *crashRecoveryRuntime) ListContainers(context.Context) ([]RuntimeState, error) {
	state := r.state
	if state.Status == "" {
		if state.Running {
			state.Status = "running"
		} else {
			state.Status = "exited"
		}
	}
	state.Labels = cloneStrings(r.state.Labels)
	return []RuntimeState{state}, nil
}

var _ Runtime = (*crashRecoveryRuntime)(nil)
var _ RuntimeInventory = (*crashRecoveryRuntime)(nil)
var _ ports.TargetRunCrashReconciler = (*Driver)(nil)
