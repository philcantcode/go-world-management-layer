package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/internal/wiremap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) StopTargetRun(ctx context.Context, request *worldv1.StopTargetRunRequest) (*worldv1.ObservationBundle, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	s.targetRunLifecycleMu.Lock()
	defer s.targetRunLifecycleMu.Unlock()
	if err := s.requireRunFinalization(); err != nil {
		return nil, err
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	target, run, driver, err := s.scopedTarget(operationCtx, request.TargetId, request.TargetRunId, meta.AuthorizedPolicyReference)
	if err != nil {
		return nil, err
	}
	signature, err := requestSignature(struct {
		TargetID string `json:"target_id"`
		RunID    string `json:"run_id"`
		Revision uint64 `json:"revision"`
		Reason   string `json:"reason"`
		Policy   string `json:"policy"`
	}{target.ID, run.ID, request.ExpectedRevision, request.Reason, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	if run.State.Terminal() {
		if err := s.requireBundleReservation(target, run, "stop_target_run", meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		if err := s.resumeTerminalBundle(operationCtx, run.ID); err != nil {
			return nil, err
		}
		if err := s.completeStoppedBundle(operationCtx, run.ID); err != nil {
			return nil, err
		}
		return s.loadBundle(operationCtx, run.ID)
	}
	if run.Revision != request.ExpectedRevision {
		return nil, status.Errorf(codes.Aborted, "target run revision conflict: got %d, current %d", request.ExpectedRevision, run.Revision)
	}
	if run.State != domain.TargetRunRunning && run.State != domain.TargetRunFinalizing {
		return nil, status.Errorf(codes.FailedPrecondition, "target run in %s cannot be stopped", run.State)
	}
	return s.stopAndFinalizeRun(operationCtx, target, run, driver, ports.StopGraceful, meta, "stop_target_run", meta.IdempotencyKey, signature, nil)
}

// stopAndFinalizeRun is the single evidence-bearing stop path used by both an
// explicit StopTargetRun and controller compensation after a failed start.
func (s *Service) stopAndFinalizeRun(ctx context.Context, target application.TargetRecord, run application.TargetRunRecord, driver ports.TargetDriver, mode ports.StopMode, meta application.MutationMeta, namespace, key, signature string, failureCause error) (*worldv1.ObservationBundle, error) {
	if err := s.requireRunFinalization(); err != nil {
		return nil, err
	}
	reservation, err := s.reserveBundle(ctx, target, run, namespace, key, signature)
	if err != nil {
		return nil, err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterBundleReserved != nil {
		if err := s.finalizationFaults.afterBundleReserved(); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	existingPreparation, prepared := s.stopPreparations[run.ID]
	s.mu.RUnlock()
	if prepared {
		bundle, err := s.resumeBundleStopPreparation(ctx, cloneBundleStopPreparation(existingPreparation))
		if err != nil {
			return nil, err
		}
		return s.completePreparedStoppedBundle(ctx, run.ID, bundle)
	}
	cleanupCtx, cleanupCancel, cleanupDeadline := cleanupContext(s.controlTimeout)
	defer cleanupCancel()
	cleanupMeta := childMeta(meta, "physical-stop", cleanupDeadline)
	runID, err := requireStoredID("orchestration.stop_target_run", "target_run_id", run.ID, domain.ParseTargetRunID)
	if err != nil {
		return nil, err
	}
	receipt, evidence, restored, err := s.observers.RestoreFinalized(runID)
	if err != nil {
		return nil, err
	}
	if !restored {
		if err := s.observers.PrepareStop(cleanupCtx, runID); err != nil {
			return nil, err
		}
		var targetErr error
		receipt, targetErr = driver.StopRun(cleanupCtx, runID, mode)
		if targetErr != nil {
			// The target did not cross its authoritative terminal boundary. Use
			// an independent cleanup budget so a target timeout cannot prevent
			// the collectors from returning to their active state.
			rollbackCtx, rollbackCancel, _ := cleanupContext(s.controlTimeout)
			cancelErr := s.observers.CancelStopPreparation(rollbackCtx, runID)
			rollbackCancel()
			return nil, errors.Join(targetErr, cancelErr)
		}
		evidence, err = s.observers.Finalize(cleanupCtx, receipt)
		if err != nil {
			return nil, err
		}
	}
	result, err := assembleTargetRunResult(receipt, evidence)
	if err != nil {
		return nil, err
	}
	preparation, err := s.prepareStoppedBundle(cleanupCtx, reservation, target, run, cleanupMeta, result, evidence.Required, evidence.Failures, failureCause)
	if err != nil {
		return nil, err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterStopPrepared != nil {
		if err := s.finalizationFaults.afterStopPrepared(); err != nil {
			return nil, err
		}
	}
	bundle, err := s.resumeBundleStopPreparation(cleanupCtx, preparation)
	if err != nil {
		return nil, err
	}
	return s.completePreparedStoppedBundle(cleanupCtx, run.ID, bundle)
}

func (s *Service) requireRunFinalization() error {
	if s.finalization == nil {
		return status.Error(codes.FailedPrecondition, "run finalization is unavailable because no finalizer/material authority is configured")
	}
	if s.observers == nil {
		return status.Error(codes.FailedPrecondition, "run observer coordinator is unavailable")
	}
	return nil
}

func (s *Service) completePreparedStoppedBundle(ctx context.Context, runID string, bundle *worldv1.ObservationBundle) (*worldv1.ObservationBundle, error) {
	parsedRunID, err := domain.ParseTargetRunID(runID)
	if err != nil {
		return nil, err
	}
	if err := s.observers.Commit(ctx, parsedRunID); err != nil {
		return nil, err
	}
	if err := s.commitBundleCompletion(ctx, runID); err != nil {
		return nil, err
	}
	return bundle, nil
}

func (s *Service) resumeTerminalBundle(ctx context.Context, runID string) error {
	s.mu.RLock()
	publication, found := s.publications[runID]
	s.mu.RUnlock()
	if !found {
		return status.Error(codes.DataLoss, "terminal target run has no recoverable public bundle publication")
	}
	return s.resumeBundlePublication(ctx, cloneBundlePublication(publication))
}

func (s *Service) completeStoppedBundle(ctx context.Context, runID string) error {
	s.mu.RLock()
	_, complete := s.completions[runID]
	s.mu.RUnlock()
	if complete {
		return nil
	}
	if s.observers == nil {
		return status.Error(codes.FailedPrecondition, "run finalization is missing observer-commit proof")
	}
	parsed, err := domain.ParseTargetRunID(runID)
	if err != nil {
		return status.Error(codes.DataLoss, "persisted target run id is invalid")
	}
	if err := s.observers.RequireCommitted(parsed); err != nil {
		if commitErr := s.observers.Commit(ctx, parsed); commitErr != nil {
			return errors.Join(err, commitErr)
		}
	}
	return s.commitBundleCompletion(ctx, runID)
}

func (s *Service) DestroyTarget(ctx context.Context, request *worldv1.DestroyTargetRequest) (*worldv1.Outcome, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{TargetID: request.TargetId}); err != nil {
		return nil, err
	}
	target, err := s.core.GetTarget(operationCtx, request.TargetId)
	if err != nil {
		return nil, err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return nil, err
	}
	const namespace = "destroy_target"
	signature, err := requestSignature(struct {
		TargetID string `json:"target_id"`
		Revision uint64 `json:"revision"`
		Reason   string `json:"reason"`
		Policy   string `json:"policy"`
	}{target.ID, request.ExpectedRevision, request.Reason, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	if generation.State == domain.TargetGenerationDestroyed {
		if err := s.requireReservedTargetOperation(namespace, target.ID, generation.Generation, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return &worldv1.Outcome{ResourceId: target.ID, State: string(generation.State), Revision: target.Revision}, nil
	}
	_, reserved, err := s.exactReservedTargetOperation(namespace, target.ID, generation.Generation, meta.IdempotencyKey, signature)
	if err != nil {
		return nil, err
	}
	if !reserved {
		if target.Revision != request.ExpectedRevision && !(generation.State == domain.TargetGenerationResettable && target.Revision == request.ExpectedRevision+1) {
			return nil, status.Errorf(codes.Aborted, "target revision conflict: got %d, current %d", request.ExpectedRevision, target.Revision)
		}
		if err := requireNoNonterminalTargetRuns(target, target.CurrentGeneration); err != nil {
			return nil, err
		}
	}
	driver := s.targets[target.Kind]
	if driver == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", target.Kind)
	}
	if generation.State != domain.TargetGenerationReady && generation.State != domain.TargetGenerationResettable {
		return nil, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot transition to destroyed through the current domain state machine", generation.State)
	}
	if !reserved {
		if err := s.reserveOperation(operationCtx, namespace, target.ID, meta.IdempotencyKey, signature, ledger.Identity{
			ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
			TargetGeneration: target.CurrentGeneration,
		}); err != nil {
			return nil, err
		}
		if s.lifecycleFaults != nil && s.lifecycleFaults.afterDestroyReserved != nil {
			if err := s.lifecycleFaults.afterDestroyReserved(); err != nil {
				return nil, err
			}
		}
	}
	if generation.State == domain.TargetGenerationReady {
		target, err = s.core.TransitionTargetGeneration(operationCtx, application.TransitionTargetGenerationRequest{Meta: childMeta(meta, "resettable", deadline(operationCtx)), TargetID: target.ID, Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable})
		if err != nil {
			return nil, err
		}
		generation, err = targetGeneration(target)
		if err != nil {
			return nil, err
		}
	}
	if generation.State != domain.TargetGenerationResettable {
		return nil, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot transition to destroyed through the current domain state machine", generation.State)
	}
	// The Ready -> Resettable transition is the logical admission barrier. A
	// run may have linearized between the pre-reservation check and that
	// barrier, so physical deletion always reloads and reasserts quiescence.
	target, err = s.core.GetTarget(operationCtx, target.ID)
	if err != nil {
		return nil, err
	}
	if target.CurrentGeneration != generation.Generation {
		return nil, status.Error(codes.DataLoss, "destroy reservation no longer identifies the current target generation")
	}
	if err := requireNoNonterminalTargetRuns(target, target.CurrentGeneration); err != nil {
		return nil, err
	}
	generation, err = targetGeneration(target)
	if err != nil {
		return nil, err
	}
	targetID, err := requireStoredID("orchestration.destroy_target", "target_id", target.ID, domain.ParseTargetID)
	if err != nil {
		return nil, err
	}
	if err := driver.Destroy(operationCtx, ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)}); err != nil {
		return nil, err
	}
	target, err = s.core.TransitionTargetGeneration(operationCtx, application.TransitionTargetGenerationRequest{Meta: childMeta(meta, "destroyed", deadline(operationCtx)), TargetID: target.ID, Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: domain.TargetGenerationDestroyed})
	if err != nil {
		return nil, err
	}
	return &worldv1.Outcome{ResourceId: target.ID, State: string(domain.TargetGenerationDestroyed), Revision: target.Revision}, nil
}

func (s *Service) QuarantineTarget(ctx context.Context, request *worldv1.QuarantineTargetRequest) (*worldv1.Target, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	s.targetRunLifecycleMu.Lock()
	defer s.targetRunLifecycleMu.Unlock()
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{TargetID: request.TargetId}); err != nil {
		return nil, err
	}
	if _, err := domain.ParseTargetID(request.TargetId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid target_id is required")
	}
	target, err := s.core.GetTarget(operationCtx, request.TargetId)
	if err != nil {
		return nil, err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return nil, err
	}
	const namespace = "quarantine_target"
	signature, err := requestSignature(struct {
		TargetID string `json:"target_id"`
		Revision uint64 `json:"revision"`
		Reason   string `json:"reason"`
		Policy   string `json:"policy"`
	}{target.ID, request.ExpectedRevision, request.Reason, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	if generation.State == domain.TargetGenerationQuarantined {
		if err := s.requireReservedTargetOperation(namespace, target.ID, generation.Generation, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return wiremap.Target(target), nil
	}
	if generation.State.Terminal() {
		return nil, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot be quarantined", generation.State)
	}
	if _, found := nonterminalTargetRun(target, target.CurrentGeneration); found {
		if err := s.requireRunFinalization(); err != nil {
			return nil, err
		}
	}
	driver := s.targets[target.Kind]
	if driver == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", target.Kind)
	}
	targetID, err := requireStoredID("orchestration.quarantine_target", "target_id", target.ID, domain.ParseTargetID)
	if err != nil {
		return nil, err
	}
	plan := ports.TargetQuarantinePlan{
		IdempotencyKey: meta.IdempotencyKey,
		Target:         ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)},
		Reason:         request.Reason,
	}
	if err := plan.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	reservation, reserved, err := s.exactReservedTargetOperation(namespace, target.ID, generation.Generation, meta.IdempotencyKey, signature)
	if err != nil {
		return nil, err
	}
	if !reserved {
		if target.Revision != request.ExpectedRevision {
			return nil, status.Errorf(codes.Aborted, "target revision conflict: got %d, current %d", request.ExpectedRevision, target.Revision)
		}
		commitMeta := childMeta(meta, "commit", time.Time{})
		reservation, err = s.reserveTargetQuarantine(operationCtx, target.ID, meta.IdempotencyKey, signature, ledger.Identity{
			ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
			TargetGeneration: target.CurrentGeneration,
		}, targetQuarantineIntent{Plan: plan, CommitMeta: commitMeta})
		if err != nil {
			return nil, err
		}
		if s.lifecycleFaults != nil && s.lifecycleFaults.afterQuarantineReserved != nil {
			if err := s.lifecycleFaults.afterQuarantineReserved(); err != nil {
				return nil, err
			}
		}
	}
	target, err = s.resumeTargetQuarantine(operationCtx, target.Kind, reservation)
	if err != nil {
		if domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, err
	}
	return wiremap.Target(target), nil
}

func (s *Service) resumeTargetQuarantine(ctx context.Context, kind domain.TargetKind, reservation operationReservation) (application.TargetRecord, error) {
	if _, err := s.closeTargetQuarantineAdmission(ctx, kind, reservation); err != nil {
		return application.TargetRecord{}, err
	}
	if _, err := s.finalizeTargetRunsForQuarantine(ctx, kind, reservation); err != nil {
		return application.TargetRecord{}, err
	}
	_, containment, err := s.ensureTargetQuarantineContained(ctx, kind, reservation)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if s.lifecycleFaults != nil && s.lifecycleFaults.afterQuarantineContained != nil {
		if err := s.lifecycleFaults.afterQuarantineContained(); err != nil {
			return application.TargetRecord{}, err
		}
	}
	return s.commitTargetQuarantine(ctx, reservation, containment)
}

// closeTargetQuarantineAdmission installs the durable logical barrier before
// any run is stopped or the target-wide physical quarantine begins. A replay
// observes Resettable and does not repeat the transition.
func (s *Service) closeTargetQuarantineAdmission(ctx context.Context, kind domain.TargetKind, reservation operationReservation) (application.TargetRecord, error) {
	target, generation, err := s.requireTargetQuarantineScope(ctx, reservation)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if target.Kind != kind {
		return application.TargetRecord{}, status.Error(codes.DataLoss, "quarantine intent identifies a different target kind")
	}
	if generation.State == domain.TargetGenerationQuarantined {
		return target, nil
	}
	if generation.State.Terminal() {
		return application.TargetRecord{}, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot complete quarantine", generation.State)
	}
	if generation.State != domain.TargetGenerationReady {
		return target, nil
	}
	meta := childMeta(reservation.Quarantine.CommitMeta, "admission", deadline(ctx))
	return s.core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{
		Meta: meta, TargetID: target.ID, Generation: target.CurrentGeneration,
		ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable,
	})
}

// finalizeTargetRunsForQuarantine establishes the ordinary evidence-bearing
// stop boundary before target-wide quarantine. Real target drivers deliberately
// reject StopRun after Quarantine, so this order is part of the containment
// contract rather than an implementation detail.
func (s *Service) finalizeTargetRunsForQuarantine(ctx context.Context, kind domain.TargetKind, reservation operationReservation) (application.TargetRecord, error) {
	target, _, err := s.requireTargetQuarantineScope(ctx, reservation)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if target.Kind != kind {
		return application.TargetRecord{}, status.Error(codes.DataLoss, "quarantine intent identifies a different target kind")
	}
	driver := s.targets[kind]
	if driver == nil {
		return application.TargetRecord{}, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", kind)
	}
	_, contained := s.targetQuarantineContainment(reservation)
	runs := currentGenerationRuns(target)
	for _, run := range runs {
		if run.State.Terminal() {
			if err := s.completeTerminalRunFinalization(ctx, run); err != nil {
				return application.TargetRecord{}, err
			}
			continue
		}
		if contained {
			return application.TargetRecord{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.finalize_target_runs_for_quarantine", "target_run", "durable target containment predates evidence-bearing run finalization", nil)
		}
		if err := s.requireRunFinalization(); err != nil {
			return application.TargetRecord{}, err
		}
		meta := childMeta(reservation.Quarantine.CommitMeta, "finalize-run/"+run.ID, deadline(ctx))
		signature, err := targetQuarantineRunFinalizationSignature(reservation, run)
		if err != nil {
			return application.TargetRecord{}, err
		}
		if _, err := s.stopAndFinalizeRun(
			ctx, target, run, driver, ports.StopForce, meta,
			"quarantine_target_run", meta.IdempotencyKey, signature,
			fmt.Errorf("target quarantine requested: %s", reservation.Quarantine.Plan.Reason),
		); err != nil {
			return application.TargetRecord{}, fmt.Errorf("finalize run %s before target quarantine: %w", run.ID, err)
		}
		target, _, err = s.requireTargetQuarantineScope(ctx, reservation)
		if err != nil {
			return application.TargetRecord{}, err
		}
	}
	return s.requireTargetQuarantineRunsFinalized(ctx, reservation)
}

// requireTargetQuarantineRunsFinalized is the last gate before physical
// containment. It catches incomplete/unbound startup records that could not be
// reconstructed and also proves every terminal run has a public bundle and a
// committed observer marker.
func (s *Service) requireTargetQuarantineRunsFinalized(ctx context.Context, reservation operationReservation) (application.TargetRecord, error) {
	target, _, err := s.requireTargetQuarantineScope(ctx, reservation)
	if err != nil {
		return application.TargetRecord{}, err
	}
	for _, run := range currentGenerationRuns(target) {
		if !run.State.Terminal() {
			return application.TargetRecord{}, domain.NewError(
				domain.CodeIntegrityViolation,
				"orchestration.require_target_quarantine_runs_finalized",
				"target_run",
				"target quarantine cannot contain a nonterminal run without evidence-bearing finalization: "+run.ID,
				nil,
			)
		}
		if err := s.completeTerminalRunFinalization(ctx, run); err != nil {
			return application.TargetRecord{}, err
		}
	}
	return target, nil
}

func (s *Service) completeTerminalRunFinalization(ctx context.Context, run application.TargetRunRecord) error {
	if err := s.resumeTerminalBundle(ctx, run.ID); err != nil {
		return fmt.Errorf("verify terminal run %s before target quarantine: %w", run.ID, err)
	}
	if err := s.completeStoppedBundle(ctx, run.ID); err != nil {
		return fmt.Errorf("complete terminal run %s before target quarantine: %w", run.ID, err)
	}
	return nil
}

func currentGenerationRuns(target application.TargetRecord) []application.TargetRunRecord {
	runs := make([]application.TargetRunRecord, 0, len(target.Runs))
	for _, run := range target.Runs {
		if run.Generation == target.CurrentGeneration {
			runs = append(runs, run)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].ID < runs[j].ID })
	return runs
}

func nonterminalTargetRun(target application.TargetRecord, generation uint64) (application.TargetRunRecord, bool) {
	for _, run := range target.Runs {
		if run.Generation == generation && !run.State.Terminal() {
			return run, true
		}
	}
	return application.TargetRunRecord{}, false
}

func targetQuarantineRunFinalizationSignature(reservation operationReservation, run application.TargetRunRecord) (string, error) {
	return requestSignature(struct {
		TargetID            string `json:"target_id"`
		TargetGeneration    uint64 `json:"target_generation"`
		RunID               string `json:"run_id"`
		ProvisioningDigest  string `json:"provisioning_digest"`
		QuarantineSignature string `json:"quarantine_signature"`
	}{reservation.ResourceID, reservation.TargetGeneration, run.ID, run.ProvisioningPlanDigest, reservation.Signature})
}

func (s *Service) requireTargetQuarantineScope(ctx context.Context, reservation operationReservation) (application.TargetRecord, application.TargetGenerationRecord, error) {
	if err := validateOperationReservation(reservation); err != nil {
		return application.TargetRecord{}, application.TargetGenerationRecord{}, status.Errorf(codes.DataLoss, "invalid durable target quarantine intent: %v", err)
	}
	if reservation.Namespace != "quarantine_target" || reservation.Quarantine == nil {
		return application.TargetRecord{}, application.TargetGenerationRecord{}, status.Error(codes.DataLoss, "durable operation is not a target quarantine intent")
	}
	target, err := s.core.GetTarget(ctx, reservation.ResourceID)
	if err != nil {
		return application.TargetRecord{}, application.TargetGenerationRecord{}, err
	}
	if target.CurrentGeneration != reservation.TargetGeneration {
		return application.TargetRecord{}, application.TargetGenerationRecord{}, status.Error(codes.DataLoss, "quarantine intent no longer identifies the current target generation")
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return application.TargetRecord{}, application.TargetGenerationRecord{}, err
	}
	return target, generation, nil
}

func (s *Service) ensureTargetQuarantineContained(ctx context.Context, kind domain.TargetKind, reservation operationReservation) (application.TargetRecord, targetQuarantineContainment, error) {
	driver := s.targets[kind]
	if driver == nil {
		return application.TargetRecord{}, targetQuarantineContainment{}, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", kind)
	}
	target, generation, err := s.requireTargetQuarantineScope(ctx, reservation)
	if err != nil {
		return application.TargetRecord{}, targetQuarantineContainment{}, err
	}
	if target.Kind != kind {
		return application.TargetRecord{}, targetQuarantineContainment{}, status.Error(codes.DataLoss, "quarantine intent identifies a different target kind")
	}
	if generation.State == domain.TargetGenerationQuarantined {
		containment, found := s.targetQuarantineContainment(reservation)
		if !found {
			return application.TargetRecord{}, targetQuarantineContainment{}, status.Error(codes.DataLoss, "quarantined generation lacks durable containment evidence")
		}
		return target, containment, nil
	}
	if generation.State.Terminal() {
		return application.TargetRecord{}, targetQuarantineContainment{}, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot complete quarantine", generation.State)
	}
	containment, contained := s.targetQuarantineContainment(reservation)
	if !contained {
		evidence, quarantineErr := driver.Quarantine(ctx, reservation.Quarantine.Plan)
		if quarantineErr != nil {
			return application.TargetRecord{}, targetQuarantineContainment{}, quarantineErr
		}
		if err := evidence.Validate(reservation.Quarantine.Plan.Target); err != nil {
			return application.TargetRecord{}, targetQuarantineContainment{}, status.Errorf(codes.DataLoss, "target driver returned invalid quarantine evidence: %v", err)
		}
		containment, err = s.persistTargetQuarantineContainment(ctx, reservation, evidence, ledger.Identity{
			ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
			TargetGeneration: target.CurrentGeneration,
		})
		if err != nil {
			return application.TargetRecord{}, targetQuarantineContainment{}, err
		}
	}
	return target, containment, nil
}

func (s *Service) commitTargetQuarantine(ctx context.Context, reservation operationReservation, containment targetQuarantineContainment) (application.TargetRecord, error) {
	if err := validateTargetQuarantineContainment(reservation, containment); err != nil {
		return application.TargetRecord{}, status.Errorf(codes.DataLoss, "invalid durable target quarantine containment: %v", err)
	}
	// Reload after physical containment. Admission is already closed and every
	// current-generation run has an evidence-bearing terminal boundary, so this
	// commit cannot manufacture a terminal run without a bundle.
	target, err := s.core.GetTarget(ctx, reservation.ResourceID)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if target.CurrentGeneration != reservation.TargetGeneration {
		return application.TargetRecord{}, status.Error(codes.DataLoss, "quarantine intent no longer identifies the current target generation")
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if generation.State == domain.TargetGenerationQuarantined {
		return target, nil
	}
	if generation.State.Terminal() {
		return application.TargetRecord{}, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot complete quarantine", generation.State)
	}
	commitMeta := reservation.Quarantine.CommitMeta
	commitMeta.Deadline = deadline(ctx)
	return s.core.QuarantineTarget(ctx, application.QuarantineTargetRequest{
		Meta: commitMeta, TargetID: target.ID, ExpectedRevision: target.Revision,
		Reason: reservation.Quarantine.Plan.Reason, Evidence: containment.Evidence,
	})
}

func requireNoNonterminalTargetRuns(target application.TargetRecord, generation uint64) error {
	if run, found := nonterminalTargetRun(target, generation); found {
		return status.Errorf(codes.FailedPrecondition, "target run %s must be stopped and authoritatively finalized before destruction", run.ID)
	}
	return nil
}

func (s *Service) reserveBundle(ctx context.Context, target application.TargetRecord, run application.TargetRunRecord, namespace, key, signature string) (bundleReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, found := s.reservations[run.ID]; found {
		if existing.Namespace != namespace || existing.IdempotencyKey != key || existing.Signature != signature || existing.TargetID != target.ID || existing.LeaseID != target.LeaseID || existing.RunID != run.ID {
			return bundleReservation{}, status.Error(codes.AlreadyExists, "target run already has a different finalization reservation")
		}
		return existing, nil
	}
	if _, found, err := s.existingIdempotencyLocked(namespace, key, signature); found {
		if err != nil {
			return bundleReservation{}, err
		}
		return bundleReservation{}, status.Error(codes.DataLoss, "idempotency index references a missing bundle reservation")
	}
	bundleID, err := s.ids.ObservationBundleID()
	if err != nil {
		return bundleReservation{}, err
	}
	reservation := bundleReservation{Namespace: namespace, LeaseID: target.LeaseID, TargetID: target.ID, RunID: run.ID, BundleID: bundleID.String(), IdempotencyKey: key, Signature: signature}
	if err := validateBundleReservation(reservation); err != nil {
		return bundleReservation{}, err
	}
	event := stateEvent{Kind: "bundle.reserved", Namespace: namespace, IdempotencyKey: key, Signature: signature, Reservation: &reservation}
	identity := ledger.Identity{ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID, TargetGeneration: run.Generation, TargetRunID: run.ID}
	if err := s.persistStateLocked(ctx, event, identity); err != nil {
		return bundleReservation{}, err
	}
	return reservation, nil
}

func (s *Service) requireBundleReservation(target application.TargetRecord, run application.TargetRunRecord, namespace, key, signature string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	existing, found := s.reservations[run.ID]
	if !found {
		return status.Error(codes.DataLoss, "terminal target run has no durable finalization reservation")
	}
	if existing.Namespace != namespace || existing.IdempotencyKey != key || existing.Signature != signature || existing.TargetID != target.ID || existing.LeaseID != target.LeaseID {
		return status.Error(codes.AlreadyExists, "target run was finalized by a different request")
	}
	return nil
}

func (s *Service) persistBundle(ctx context.Context, leaseID, targetID, runID string, value *worldv1.ObservationBundle) error {
	if value == nil {
		return status.Error(codes.DataLoss, "authoritative finalization returned no observation bundle")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if int64(len(encoded)) > s.maxTransferBytes {
		return status.Errorf(codes.ResourceExhausted, "serialized observation bundle exceeds %d bytes", s.maxTransferBytes)
	}
	namespace, err := openDurableNamespace(s.stateRoot, "bundles")
	if err != nil {
		return err
	}
	defer namespace.Close()
	fileName := runID + ".json"
	s.mu.Lock()
	defer s.mu.Unlock()
	publication, staged := s.publications[runID]
	if !staged || publication.Reservation.LeaseID != leaseID || publication.Reservation.TargetID != targetID || publication.Reservation.BundleID != value.BundleId {
		return status.Error(codes.DataLoss, "observation bundle has no exact recoverable publication stage")
	}
	stagedWire, marshalErr := json.Marshal(publication.Bundle)
	if marshalErr != nil || !bytes.Equal(stagedWire, encoded) {
		return status.Error(codes.DataLoss, "observation bundle differs from its recoverable publication stage")
	}
	if err := namespace.EnsureRegularAtomically(fileName, encoded, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return status.Error(codes.DataLoss, "persisted bundle conflicts with authoritative finalization")
		}
		return err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterBundleFile != nil {
		if err := s.finalizationFaults.afterBundleFile(); err != nil {
			return err
		}
	}
	record := bundleRecord{
		LeaseID: leaseID, TargetID: targetID, RunID: runID, BundleID: value.BundleId, File: fileName,
		WireDigest: domain.NewDigest(encoded).String(), Size: int64(len(encoded)),
	}
	if err := validateBundleRecord(record, s.maxTransferBytes); err != nil {
		return status.Errorf(codes.DataLoss, "authoritative bundle index is invalid: %v", err)
	}
	if existing, found := s.bundles[runID]; found {
		if existing != record {
			return status.Error(codes.DataLoss, "bundle index conflicts with finalized bundle")
		}
		return nil
	}
	event := stateEvent{Kind: "bundle.indexed", Bundle: &record}
	if err := s.persistStateLocked(ctx, event, ledger.Identity{LeaseID: leaseID, TargetID: targetID, TargetRunID: runID}); err != nil {
		return err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterBundleIndexed != nil {
		return s.finalizationFaults.afterBundleIndexed()
	}
	return nil
}

func (s *Service) GetObservationBundle(ctx context.Context, request *worldv1.GetObservationBundleRequest) (*worldv1.ObservationBundle, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return s.loadBundle(ctx, request.TargetRunId)
}

func (s *Service) loadBundle(ctx context.Context, runID string) (*worldv1.ObservationBundle, error) {
	if _, err := domain.ParseTargetRunID(runID); err != nil {
		return nil, status.Error(codes.InvalidArgument, "valid target_run_id is required")
	}
	s.mu.RLock()
	record, found := s.bundles[runID]
	completion, complete := s.completions[runID]
	s.mu.RUnlock()
	if !found {
		return nil, status.Error(codes.NotFound, "observation bundle not found")
	}
	if !complete || completion.BundleID != record.BundleID || completion.WireDigest != record.WireDigest {
		return nil, status.Error(codes.FailedPrecondition, "observation bundle finalization is not complete")
	}
	if err := s.authorize(ctx, "", application.AuthorizationRequest{TargetID: record.TargetID}); err != nil {
		return nil, err
	}
	return s.verifyStoredBundle(ctx, record)
}

func (s *Service) verifyBundleIndexes(ctx context.Context) error {
	runIDs := make([]string, 0, len(s.bundles))
	for runID := range s.bundles {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := s.verifyStoredBundle(ctx, s.bundles[runID]); err != nil {
			return fmt.Errorf("run %s: %w", runID, err)
		}
		publication, staged := s.publications[runID]
		if !staged {
			return fmt.Errorf("run %s: indexed bundle has no recoverable publication", runID)
		}
		wire, err := json.Marshal(publication.Bundle)
		if err != nil || int64(len(wire)) != s.bundles[runID].Size || domain.NewDigest(wire).String() != s.bundles[runID].WireDigest {
			return fmt.Errorf("run %s: staged publication disagrees with bundle index", runID)
		}
	}
	for runID, completion := range s.completions {
		record, found := s.bundles[runID]
		if !found || completion.RunID != runID || completion.BundleID != record.BundleID || completion.WireDigest != record.WireDigest {
			return fmt.Errorf("run %s: bundle completion has no exact indexed publication", runID)
		}
	}
	return nil
}

func (s *Service) verifyStoredBundle(ctx context.Context, record bundleRecord) (*worldv1.ObservationBundle, error) {
	if err := validateBundleRecord(record, s.maxTransferBytes); err != nil {
		return nil, status.Errorf(codes.DataLoss, "persisted observation bundle index is invalid: %v", err)
	}
	s.mu.RLock()
	reservation, reserved := s.reservations[record.RunID]
	indexed, indexedOK := s.idempotency[idempotencyIndex(reservation.Namespace, reservation.IdempotencyKey)]
	s.mu.RUnlock()
	if !reserved || reservation.LeaseID != record.LeaseID || reservation.TargetID != record.TargetID || reservation.RunID != record.RunID || reservation.BundleID != record.BundleID || reservation.Namespace == "" || !domain.IsCanonicalIdempotencyKey(reservation.IdempotencyKey) || reservation.Signature == "" {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle has no matching durable finalization reservation")
	}
	if !indexedOK || indexed.Signature != reservation.Signature || indexed.ResourceID != reservation.BundleID {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle reservation has no matching idempotency index")
	}
	namespace, err := openDurableNamespace(s.stateRoot, "bundles")
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "open persisted observation bundle namespace: %v", err)
	}
	defer namespace.Close()
	encoded, err := namespace.ReadRegularBounded(record.File, s.maxTransferBytes)
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "read persisted observation bundle: %v", err)
	}
	if int64(len(encoded)) != record.Size || domain.NewDigest(encoded).String() != record.WireDigest {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle wire identity does not match its durable index")
	}
	var value worldv1.ObservationBundle
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return nil, status.Errorf(codes.DataLoss, "decode persisted observation bundle: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle contains trailing JSON")
	}
	canonical, err := json.Marshal(&value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle is not in canonical wire form")
	}
	if value.BundleId != record.BundleID || value.TargetRunId != record.RunID || value.TargetId != record.TargetID || value.State != string(domain.ObservationBundleSealed) {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle identity or state is invalid")
	}
	target, err := s.core.GetTarget(ctx, record.TargetID)
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "load authoritative target for persisted observation bundle: %v", err)
	}
	if target.LeaseID != record.LeaseID {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle lease identity is invalid")
	}
	run, err := targetRun(target, record.RunID)
	if err != nil || !run.State.Terminal() || run.BundleID != record.BundleID || run.Generation != value.TargetGeneration || run.AgentWorkspaceID != value.AgentWorkspaceId || run.AgentGeneration != value.AgentGeneration {
		return nil, status.Error(codes.DataLoss, "persisted observation bundle disagrees with authoritative run state")
	}
	return &value, nil
}

func validateBundleRecord(record bundleRecord, maximumBytes int64) error {
	if _, err := domain.ParseLeaseID(record.LeaseID); err != nil {
		return fmt.Errorf("lease id: %w", err)
	}
	if _, err := domain.ParseTargetID(record.TargetID); err != nil {
		return fmt.Errorf("target id: %w", err)
	}
	if _, err := domain.ParseTargetRunID(record.RunID); err != nil {
		return fmt.Errorf("target run id: %w", err)
	}
	if _, err := domain.ParseObservationBundleID(record.BundleID); err != nil {
		return fmt.Errorf("bundle id: %w", err)
	}
	if _, err := domain.ParseDigest(record.WireDigest); err != nil {
		return fmt.Errorf("wire digest: %w", err)
	}
	if record.Size <= 0 || record.Size > maximumBytes {
		return fmt.Errorf("wire size is outside configured bounds")
	}
	if record.File != record.RunID+".json" {
		return fmt.Errorf("bundle file is not derived from its target run id")
	}
	return nil
}
