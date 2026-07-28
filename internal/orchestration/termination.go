package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type LeaseTerminationScanReport struct {
	Examined  int
	Begun     int
	Completed int
}

// ReconcileLeaseTerminations is the startup and periodic trusted reaper. It
// first persists an expiry gate, then drains evidence-bearing work, destroys
// physical ownership, records terminal work state, and finally completes the
// lease transition. Every child identity is derived from the lease so a
// restart resumes rather than duplicating side effects.
func (c *Controller) ReconcileLeaseTerminations(ctx context.Context) (LeaseTerminationScanReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	work, err := c.Core.ListLeaseTerminationWork(ctx)
	if err != nil {
		return LeaseTerminationScanReport{}, err
	}
	report := LeaseTerminationScanReport{Examined: len(work)}
	var reconcileErrors []error
	for _, item := range work {
		itemCtx, cancel := context.WithTimeout(ctx, c.cleanupTimeout)
		preparation, prepareErr := c.prepareLeaseTermination(itemCtx, item)
		if prepareErr == nil && item.NeedsBegin {
			report.Begun++
		}
		if prepareErr == nil {
			prepareErr = c.drainLeaseTermination(itemCtx, preparation)
		}
		if prepareErr == nil {
			_, prepareErr = c.Core.CompleteLeaseTermination(itemCtx, application.CompleteLeaseTerminationRequest{
				LeaseID: item.LeaseID, ExpectedRevision: preparation.TerminatingLeaseRevision,
			})
		}
		cancel()
		if prepareErr != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("lease %s termination: %w", item.LeaseID, prepareErr))
			continue
		}
		report.Completed++
	}
	return report, errors.Join(reconcileErrors...)
}

func (c *Controller) prepareLeaseTermination(ctx context.Context, work application.LeaseTerminationWork) (application.LeaseTerminationPreparation, error) {
	if work.NeedsBegin {
		return c.Core.BeginDueLeaseExpiry(ctx, application.BeginLeaseExpiryRequest{
			LeaseID: work.LeaseID, ExpectedRevision: work.LeaseRevision,
		})
	}
	return c.Core.ResumeLeaseTermination(ctx, work.LeaseID)
}

func (c *Controller) drainLeaseTermination(ctx context.Context, preparation application.LeaseTerminationPreparation) error {
	view := preparation.View
	meta, err := leaseTerminationMeta(view.Lease, deadline(ctx))
	if err != nil {
		return err
	}
	if c.capabilities != nil {
		if err := c.capabilities.stopLeaseCaptures(ctx, meta, view.Lease.ID); err != nil {
			return err
		}
		if err := c.capabilities.resumeLeaseExports(ctx, meta, view); err != nil {
			return err
		}
		if err := c.capabilities.requireNoCommittingExports(view.Lease.ID); err != nil {
			return err
		}
	}
	if err := c.finalizeTerminationRuns(ctx, meta, view); err != nil {
		return err
	}
	if !c.logicalOnly() {
		if err := c.requireAgentLifecycle("lease_termination"); err != nil {
			return err
		}
		if err := c.releasePhysical(ctx, view); err != nil {
			return err
		}
	}
	return c.finalizeTerminationOperations(ctx, meta, view)
}

func leaseTerminationMeta(lease application.LeaseRecord, attemptDeadline time.Time) (application.MutationMeta, error) {
	leaseID, err := domain.ParseLeaseID(lease.ID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if lease.PolicyDigest == "" {
		return application.MutationMeta{}, fmt.Errorf("lease %s has no policy digest", lease.ID)
	}
	return application.MutationMeta{
		IdempotencyKey:            "lease-termination/" + lease.ID,
		CorrelationID:             "corr_" + leaseID.UUID(),
		AuthorizedPolicyReference: lease.PolicyDigest,
		Deadline:                  attemptDeadline,
	}, nil
}

func (c *Controller) finalizeTerminationRuns(ctx context.Context, meta application.MutationMeta, view application.ResearchSessionView) error {
	for _, initialTarget := range view.Targets {
		for _, initialRun := range initialTarget.Runs {
			if initialRun.State.Terminal() {
				continue
			}
			target, err := c.Core.GetTarget(ctx, initialTarget.ID)
			if err != nil {
				return err
			}
			run, err := targetRun(target, initialRun.ID)
			if err != nil || run.State.Terminal() {
				continue
			}
			driver := c.targets[target.Kind]
			if driver == nil || c.capabilities == nil {
				return missingCapability("controller.lease_termination", "target_run_finalization", "target driver and finalization service are required")
			}
			runMeta := childMeta(meta, "run/"+run.ID, meta.Deadline)
			signature, err := terminationRunSignature(view.Lease.ID, target.ID, run.ID)
			if err != nil {
				return err
			}
			_, err = c.capabilities.stopAndFinalizeRun(ctx, target, run, driver, ports.StopForce, runMeta,
				"lease_termination_run", runMeta.IdempotencyKey, signature, fmt.Errorf("lease termination stopped the run"))
			if err == nil {
				continue
			}
			latest, loadErr := c.Core.GetTarget(ctx, target.ID)
			if loadErr == nil {
				if current, findErr := targetRun(latest, run.ID); findErr == nil && current.State.Terminal() {
					continue
				}
			}
			if !domain.IsCode(err, domain.CodeNotFound) {
				return errors.Join(err, loadErr)
			}
			target, run, err = c.prepareLostTerminationRun(ctx, runMeta, latest, run, driver)
			if err != nil {
				return errors.Join(err, loadErr)
			}
			if _, err := c.capabilities.stopAndFinalizeRun(ctx, target, run, driver, ports.StopForce, runMeta,
				"lease_termination_run", runMeta.IdempotencyKey, signature, fmt.Errorf("target run state was lost during lease termination")); err != nil {
				return err
			}
		}
	}
	return nil
}

func terminationRunSignature(leaseID, targetID, runID string) (string, error) {
	return requestSignature(struct {
		LeaseID  string `json:"lease_id"`
		TargetID string `json:"target_id"`
		RunID    string `json:"run_id"`
		Reason   string `json:"reason"`
	}{leaseID, targetID, runID, "lease_termination"})
}

// prepareLostTerminationRun re-establishes only enough physical and observer
// state to obtain an authoritative NeverStarted/Failed stop receipt. It never
// starts specimen execution and is used only when the driver proves the
// durable nonterminal run is absent after restart.
func (c *Controller) prepareLostTerminationRun(ctx context.Context, meta application.MutationMeta, target application.TargetRecord, run application.TargetRunRecord, driver ports.TargetDriver) (application.TargetRecord, application.TargetRunRecord, error) {
	plan, err := c.resolvePersistedTargetRunPlan(ctx, meta, target, run)
	if err != nil {
		return target, run, err
	}
	prepared, err := driver.PrepareRun(ctx, plan)
	if err != nil {
		return target, run, err
	}
	observerStart, err := bindRunObserverStart(plan, prepared, target)
	if err != nil {
		return target, run, err
	}
	if err := c.observers.Start(ctx, observerStart); err != nil {
		return target, run, err
	}
	if err := validatePreparedRun(plan, prepared); err != nil {
		return target, run, err
	}
	return target, run, nil
}

// resolvePersistedTargetRunPlan is the single reconstruction path for both
// lease termination compensation and daemon crash recovery. The durable
// provisioning identity is re-applied by bindTargetRunPlan, so a resolver that
// returns a different plan fails before any physical run ownership is created.
func (c *Controller) resolvePersistedTargetRunPlan(ctx context.Context, meta application.MutationMeta, target application.TargetRecord, run application.TargetRunRecord) (ports.TargetRunPlan, error) {
	request := application.StartTargetRunRequest{Meta: meta, TargetID: target.ID, MaterializationDigest: run.MaterializationDigest}
	resolved, err := c.resolver.ResolveTargetMaterial(ctx, request, target)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	return bindTargetRunPlan(request, resolved, target, run)
}

func (c *Controller) finalizeTerminationOperations(ctx context.Context, meta application.MutationMeta, view application.ResearchSessionView) error {
	for _, initial := range view.Execs {
		current, err := c.Core.GetExec(ctx, initial.ID)
		if err != nil {
			return err
		}
		if current.State.Terminal() {
			continue
		}
		_, err = c.Core.FinalizeExec(ctx, application.FinalizeExecRequest{
			Meta: childMeta(meta, "exec/"+current.ID, meta.Deadline), ExecID: current.ID,
			ExpectedRevision: current.Revision, State: domain.ExecCancelled, CleanupConfirmed: true,
			Error: "execution was stopped by lease termination",
		})
		if err != nil {
			latest, loadErr := c.Core.GetExec(ctx, current.ID)
			if loadErr != nil || !latest.State.Terminal() {
				return errors.Join(err, loadErr)
			}
		}
	}
	for _, initialTarget := range view.Targets {
		target, err := c.Core.GetTarget(ctx, initialTarget.ID)
		if err != nil {
			return err
		}
		for _, initialOperation := range target.Operations {
			if initialOperation.State.Terminal() {
				continue
			}
			_, err := c.Core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{
				Meta:     childMeta(meta, "target-operation/"+initialOperation.ID, meta.Deadline),
				TargetID: target.ID, OperationID: initialOperation.ID, ExpectedRevision: initialOperation.Revision,
				State: domain.TargetOperationCancelled,
			})
			if err != nil {
				latest, loadErr := c.Core.GetTarget(ctx, target.ID)
				if loadErr != nil {
					return errors.Join(err, loadErr)
				}
				current, findErr := targetOperation(latest, initialOperation.ID)
				if findErr != nil || !current.State.Terminal() {
					return errors.Join(err, findErr)
				}
			}
		}
	}
	return nil
}

func targetOperation(target application.TargetRecord, operationID string) (application.TargetOperationRecord, error) {
	for _, operation := range target.Operations {
		if operation.ID == operationID {
			return operation, nil
		}
	}
	return application.TargetOperationRecord{}, fmt.Errorf("target operation %s is missing", operationID)
}
