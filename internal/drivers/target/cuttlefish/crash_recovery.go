package cuttlefish

import (
	"context"
	"encoding/json"
	"path/filepath"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/runevidence"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func (d *Driver) RecoverInterruptedRun(ctx context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.recover_interrupted_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if prepared, found, err := d.existingPreparedAndroidRecovery(plan); found || err != nil {
		return prepared, err
	}
	spec := plan.Run.Spec()
	device, err := d.requireDevice(spec.TargetID, spec.TargetGeneration)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	_, _, err = loadTargetRuntimeManifests(device.plan.StateDirectory)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "runtime_manifest", "exact adopted runtime manifest is unavailable", err)
	}
	_, intentFound, generationClaimed, err := loadRunPreparation(device.plan, plan)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run_intent", "durable run preparation intent differs from the exact recovery plan", err)
	}
	if !intentFound || !generationClaimed {
		if !intentFound && !generationClaimed {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeNotFound, "cuttlefish.recover_interrupted_run", "run_intent", "no durable run preparation intent identifies this run", nil)
		}
		return d.prepareRun(ctx, plan)
	}
	directory := filepath.Join(device.plan.StateDirectory, "runs", spec.ID.String())
	persisted, err := loadExpectedRunManifest(directory, plan)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run_manifest", "exact interrupted run manifest is unavailable or differs", err)
	}
	if persisted.Allocation != device.instance.Allocation || persisted.RuntimeID != device.instance.RuntimeID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "runtime", "interrupted run manifest identifies another physical runtime", nil)
	}
	planSignature, err := runPlanSignature(plan)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInternal, "cuttlefish.recover_interrupted_run", "plan_signature", "could not bind the exact recovered run request", err)
	}
	_, stoppedReceipt, stoppedContainment, stopped, err := loadBoundRunStop(directory, device.plan, persisted)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run_stop", "durable Android stopped-run evidence is invalid", err)
	}
	if stopped {
		return d.recoverStoppedAndroidRun(plan, device, persisted, directory, stoppedReceipt, stoppedContainment)
	}
	startRecord, startCommitted, err := loadExpectedRunStart(directory, plan, persisted.Allocation, persisted.RuntimeID, spec.CreatedAt)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run_start", "durable Android run-start record is invalid", err)
	}
	contained, err := d.verifyBackendContainment(ctx, device.instance, ports.StopForce)
	if err != nil {
		return ports.PreparedTargetRun{}, classifiedDriverFailure("cuttlefish.recover_interrupted_run", "execution_containment", "could not prove termination of execution left by the lost controller", err)
	}
	prepared := runevidence.ClonePrepared(persisted.Prepared)
	startPayload := json.RawMessage(nil)
	if startCommitted {
		startPayload, err = json.Marshal(struct {
			RuntimeID string `json:"runtime_id"`
			Serial    string `json:"serial"`
		}{RuntimeID: persisted.RuntimeID, Serial: persisted.Allocation.Serial})
		if err != nil {
			return ports.PreparedTargetRun{}, err
		}
	}
	payload, err := json.Marshal(struct {
		RuntimeID                   string `json:"runtime_id"`
		PriorExecutionTerminated    bool   `json:"prior_execution_terminated"`
		RunStartCommitted           bool   `json:"run_start_committed"`
		ControlPlaneContinuityLost  bool   `json:"control_plane_continuity_lost"`
		SpecimenExecutionWasResumed bool   `json:"specimen_execution_was_resumed"`
	}{device.instance.RuntimeID, true, startCommitted, true, false})
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	d.mu.Lock()
	current, found := d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)]
	if !found || current.instance.RuntimeID != device.instance.RuntimeID {
		d.mu.Unlock()
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.recover_interrupted_run", "target", "adopted target ownership changed during recovery", nil)
	}
	if prior, used := d.idempotency[plan.IdempotencyKey]; used && prior != spec.ID.String() {
		d.mu.Unlock()
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.recover_interrupted_run", "idempotency_key", "was used for another run", nil)
	}
	observations := make([]ports.TargetRunObservation, 0, 2)
	if startCommitted {
		observations = append(observations, ports.TargetRunObservation{Kind: "target.run.started", ObservedAt: startRecord.StartedAt, Payload: startPayload})
	}
	observations = append(observations, ports.TargetRunObservation{Kind: "target.run.control-plane-loss", ObservedAt: contained.ObservedAt, Payload: payload})
	containedCopy := contained
	run := &runRecord{
		plan: runevidence.ClonePlan(plan), planSignature: planSignature, scope: persisted.Scope, allocation: persisted.Allocation,
		sourceInstance: persisted.RuntimeID, directory: directory, prepared: prepared,
		controlPlaneLost: true, interruptedExecution: startCommitted, observations: observations,
		transports: make(map[*androidTransport]struct{}), scopedWrites: make(map[string]scopedWriteEvidence), containment: &containedCopy,
	}
	if startCommitted {
		run.startedAt = startRecord.StartedAt
		run.opaqueMutationReason = "control-plane-loss-unbounded-guest-state"
	}
	d.runs[spec.ID.String()] = run
	d.idempotency[plan.IdempotencyKey] = spec.ID.String()
	current.status.Ready = false
	current.status.State = domain.TargetGenerationResettable
	current.status.ObservedAt = contained.ObservedAt
	d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)] = current
	d.mu.Unlock()
	return runevidence.ClonePrepared(prepared), nil
}

func (d *Driver) recoverStoppedAndroidRun(plan ports.TargetRunPlan, device deviceRecord, persisted runPlanManifest, directory string, receipt ports.TargetRunStopReceipt, containment BackendQuarantineState) (ports.PreparedTargetRun, error) {
	spec := plan.Run.Spec()
	planSignature, err := runPlanSignature(plan)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInternal, "cuttlefish.recover_interrupted_run", "plan_signature", "could not bind the exact recovered run request", err)
	}
	prepared := runevidence.ClonePrepared(persisted.Prepared)
	storedReceipt := runevidence.CloneStopReceipt(receipt)
	containedCopy := containment
	observations := append([]ports.TargetRunObservation(nil), receipt.Observations[:len(receipt.Observations)-1]...)
	for index := range observations {
		observations[index].Payload = append(json.RawMessage(nil), observations[index].Payload...)
	}
	run := &runRecord{
		plan: runevidence.ClonePlan(plan), planSignature: planSignature, scope: persisted.Scope, allocation: persisted.Allocation,
		sourceInstance: persisted.RuntimeID, directory: directory, prepared: prepared,
		started: !receipt.StartedAt.IsZero(), startedAt: receipt.StartedAt, stopped: true, receipt: &storedReceipt,
		interruptedExecution: receipt.FailureKind == ports.TargetRunFailureTarget,
		durationExceeded:     receipt.FailureKind == ports.TargetRunFailureDurationExceeded,
		observations:         observations, transports: nil, scopedWrites: make(map[string]scopedWriteEvidence), containment: &containedCopy,
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current, found := d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)]
	if !found || current.instance.RuntimeID != device.instance.RuntimeID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.recover_interrupted_run", "target", "adopted target ownership changed during stopped-run recovery", nil)
	}
	if prior, used := d.idempotency[plan.IdempotencyKey]; used && prior != spec.ID.String() {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.recover_interrupted_run", "idempotency_key", "was used for another run", nil)
	}
	d.runs[spec.ID.String()] = run
	d.idempotency[plan.IdempotencyKey] = spec.ID.String()
	current.status.Ready = false
	current.status.State = domain.TargetGenerationResettable
	current.status.ObservedAt = containment.ObservedAt
	d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)] = current
	return runevidence.ClonePrepared(prepared), nil
}

func (d *Driver) existingPreparedAndroidRecovery(plan ports.TargetRunPlan) (ports.PreparedTargetRun, bool, error) {
	expectedSignature, err := runPlanSignature(plan)
	if err != nil {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeInternal, "cuttlefish.recover_interrupted_run", "plan_signature", "could not bind the exact recovered run request", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	run := d.runs[plan.Run.ID().String()]
	if run == nil {
		return ports.PreparedTargetRun{}, false, nil
	}
	if run.planSignature.IsZero() || run.planSignature != expectedSignature {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeConflict, "cuttlefish.recover_interrupted_run", "run_plan", "existing prepared recovery has another exact plan", nil)
	}
	if run.stopped {
		if run.receipt == nil || run.receipt.Validate() != nil || run.containment == nil {
			return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run", "existing stopped recovery lacks its exact receipt or containment proof", nil)
		}
		return runevidence.ClonePrepared(run.prepared), true, nil
	}
	if run.started || run.starting || run.deadlineCancel != nil {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run", "recovered run unexpectedly owns execution or a duration timer", nil)
	}
	if !run.controlPlaneLost {
		return ports.PreparedTargetRun{}, true, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.recover_interrupted_run", "run", "existing run was not reconstructed as a control-plane-loss recovery", nil)
	}
	return runevidence.ClonePrepared(run.prepared), true, nil
}

var _ ports.TargetRunCrashReconciler = (*Driver)(nil)
