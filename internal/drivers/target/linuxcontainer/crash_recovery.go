package linuxcontainer

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/runevidence"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// RecoverInterruptedRun stops the adopted container at its process boundary,
// which authoritatively kills every process left by the old daemon, and then
// reconstructs only the prepared run for evidence sealing. It deliberately
// never restarts the container, calls StartRun, waits for collectors, or
// creates a maximum-duration timer.
func (d *Driver) RecoverInterruptedRun(ctx context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.recover_interrupted_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	if prepared, found, err := d.existingPreparedRecovery(plan); found || err != nil {
		return prepared, err
	}
	spec := plan.Run.Spec()
	target, err := d.requireTarget(spec.TargetID, spec.TargetGeneration)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if _, err := requireTargetGenerationRunClaim(target.plan.TargetDirectory, plan); err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "run_claim", "durable target generation run claim is missing or invalid", err)
	}
	recoveryAuthority := RunAuthority{LeaseID: spec.LeaseID, TargetID: spec.TargetID, Generation: spec.TargetGeneration, RunID: spec.ID}
	runDirectory := filepath.Join(target.plan.TargetDirectory, "runs", spec.ID.String())
	if err := prepareManagedDirectory(d.build.TargetRoot, runDirectory); err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "run_directory", "could not restore the interrupted run directory", err)
	}
	startRecord, startCommitted, err := loadRunStart(runDirectory, recoveryAuthority)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "run_start", "durable target run start record is invalid", err)
	}
	if startCommitted && (startRecord.RuntimeID != target.runtimeID || startRecord.Materialization != spec.MaterializationDigest) {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "run_start", "durable target run start record identifies different physical execution", nil)
	}
	initialState, err := d.inspectOwnedRuntime(ctx, target, "linux_target.recover_interrupted_run")
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	runtimeWasRunning := initialState.Running
	minimumBoundary := spec.CreatedAt
	if startCommitted {
		minimumBoundary = startRecord.StartedAt
	}
	recoveryRun := &runRecord{plan: plan, authority: recoveryAuthority, directory: runDirectory}
	if _, err := d.requireStoppedRunBoundary(ctx, target, recoveryRun, ports.StopForce, minimumBoundary); err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime_stop", "could not establish the durable stopped-container boundary", err)
	}
	prepared, err := d.prepareRun(ctx, plan, true)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	payload, err := json.Marshal(struct {
		RuntimeID                   string `json:"runtime_id"`
		RuntimeWasRunning           bool   `json:"runtime_was_running"`
		ProcessBoundaryReset        bool   `json:"process_boundary_reset"`
		RunStartCommitted           bool   `json:"run_start_committed"`
		PriorCgroupID               string `json:"prior_cgroup_id,omitempty"`
		ControlPlaneContinuityLost  bool   `json:"control_plane_continuity_lost"`
		SpecimenExecutionWasResumed bool   `json:"specimen_execution_was_resumed"`
	}{
		RuntimeID: target.runtimeID, RuntimeWasRunning: runtimeWasRunning, ProcessBoundaryReset: true,
		RunStartCommitted: startCommitted, PriorCgroupID: startRecord.CgroupID,
		ControlPlaneContinuityLost: true, SpecimenExecutionWasResumed: false,
	})
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.mu.Lock()
	if run := d.runs[spec.ID.String()]; run != nil {
		run.controlPlaneLost = true
		run.interruptedExecution = startCommitted
		if startCommitted {
			run.startedAt = startRecord.StartedAt
		}
		run.observations = append(run.observations, ports.TargetRunObservation{
			Kind: "target.run.control-plane-loss", ObservedAt: prepared.PreparedAt, Payload: payload,
		})
	}
	d.mu.Unlock()
	return runevidence.ClonePrepared(prepared), nil
}

func (d *Driver) existingPreparedRecovery(plan ports.TargetRunPlan) (ports.PreparedTargetRun, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	run := d.runs[plan.Run.ID().String()]
	if run == nil {
		return ports.PreparedTargetRun{}, false, nil
	}
	if !reflect.DeepEqual(run.plan, plan) {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeConflict, "linux_target.recover_interrupted_run", "run_plan", "an existing prepared recovery has a different plan", nil)
	}
	if run.started || run.timer != nil {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "run", "recovered run unexpectedly owns execution or a duration timer", nil)
	}
	if !run.controlPlaneLost {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "run", "existing run was not reconstructed as a control-plane-loss recovery", nil)
	}
	return runevidence.ClonePrepared(run.prepared), true, nil
}

var _ ports.TargetRunCrashReconciler = (*Driver)(nil)
