package linuxcontainer

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/runevidence"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// RecoverInterruptedRun resets the adopted container at its process boundary,
// which authoritatively kills every docker-exec child left by the old daemon,
// and then reconstructs only the prepared run. It deliberately never calls
// StartRun, waits for collectors, or creates a maximum-duration timer.
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
	state, err := d.runtime.Inspect(ctx, target.runtimeID)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime", "could not inspect the adopted target", err)
	}
	if state.ID != target.runtimeID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "linux_target.recover_interrupted_run", "runtime_id", "runtime inspect returned a different target", nil)
	}
	if err := validateRuntimeIdentity(state, target.plan); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if state.Running {
		if err := d.runtime.Stop(ctx, target.runtimeID, ports.StopForce); err != nil {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime_stop", "could not terminate execution left by the old controller", err)
		}
		state, err = d.runtime.Inspect(ctx, target.runtimeID)
		if err != nil {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime_stop", "could not prove the adopted target stopped", err)
		}
		if state.ID != target.runtimeID || state.Running {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "linux_target.recover_interrupted_run", "runtime_stop", "the adopted target still executes after forced stop", nil)
		}
		if err := validateRuntimeIdentity(state, target.plan); err != nil {
			return ports.PreparedTargetRun{}, err
		}
	}
	if err := d.runtime.Start(ctx, target.runtimeID); err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime_start", "could not restart the clean adopted target", err)
	}
	state, err = d.runtime.Inspect(ctx, target.runtimeID)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.recover_interrupted_run", "runtime_start", "could not inspect the restarted adopted target", err)
	}
	if state.ID != target.runtimeID || !state.Running {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "linux_target.recover_interrupted_run", "runtime_start", "the clean adopted target did not become running", nil)
	}
	if err := validateRuntimeIdentity(state, target.plan); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.mu.Lock()
	current, exists := d.targets[targetKey(spec.TargetID, spec.TargetGeneration)]
	if !exists || current.runtimeID != target.runtimeID {
		d.mu.Unlock()
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "linux_target.recover_interrupted_run", "target", "adopted target ownership changed during recovery", nil)
	}
	current.status = adoptedTargetStatus(current.plan, state, d.now().UTC())
	d.targets[targetKey(spec.TargetID, spec.TargetGeneration)] = current
	d.mu.Unlock()

	prepared, err := d.PrepareRun(ctx, plan)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	payload, err := json.Marshal(struct {
		RuntimeID                   string `json:"runtime_id"`
		PriorExecutionTerminated    bool   `json:"prior_execution_terminated"`
		ControlPlaneContinuityLost  bool   `json:"control_plane_continuity_lost"`
		SpecimenExecutionWasResumed bool   `json:"specimen_execution_was_resumed"`
	}{target.runtimeID, true, true, false})
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.mu.Lock()
	if run := d.runs[spec.ID.String()]; run != nil {
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
	return runevidence.ClonePrepared(run.prepared), true, nil
}

var _ ports.TargetRunCrashReconciler = (*Driver)(nil)
