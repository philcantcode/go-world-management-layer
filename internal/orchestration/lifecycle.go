package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

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
	if s.finalization == nil {
		return nil, status.Error(codes.FailedPrecondition, "run finalization is unavailable because no finalizer/material authority is configured")
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
	if s.observers == nil {
		return nil, status.Error(codes.FailedPrecondition, "run observer coordinator is unavailable")
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
		var targetErr error
		receipt, targetErr = driver.StopRun(cleanupCtx, runID, mode)
		if targetErr != nil {
			// Without a target stop receipt there is no authoritative terminal
			// boundary. Leave observers running so a retry cannot silently lose
			// the tail of the target run.
			return nil, targetErr
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
		if err := s.requireReservedOperation(namespace, target.ID, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return &worldv1.Outcome{ResourceId: target.ID, State: string(generation.State), Revision: target.Revision}, nil
	}
	if target.Revision != request.ExpectedRevision && !(generation.State == domain.TargetGenerationResettable && target.Revision == request.ExpectedRevision+1) {
		return nil, status.Errorf(codes.Aborted, "target revision conflict: got %d, current %d", request.ExpectedRevision, target.Revision)
	}
	for _, run := range target.Runs {
		if run.Generation == target.CurrentGeneration && !run.State.Terminal() {
			return nil, status.Errorf(codes.FailedPrecondition, "target run %s must be stopped and authoritatively finalized before destruction", run.ID)
		}
	}
	driver := s.targets[target.Kind]
	if driver == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", target.Kind)
	}
	if generation.State != domain.TargetGenerationReady && generation.State != domain.TargetGenerationResettable {
		return nil, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot transition to destroyed through the current domain state machine", generation.State)
	}
	if err := s.reserveOperation(operationCtx, namespace, target.ID, meta.IdempotencyKey, signature, ledger.Identity{
		ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
		TargetGeneration: target.CurrentGeneration,
	}); err != nil {
		return nil, err
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
		if err := s.requireReservedOperation(namespace, target.ID, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return wiremap.Target(target), nil
	}
	if generation.State.Terminal() {
		return nil, status.Errorf(codes.FailedPrecondition, "target generation in %s cannot be quarantined", generation.State)
	}
	if target.Revision != request.ExpectedRevision {
		return nil, status.Errorf(codes.Aborted, "target revision conflict: got %d, current %d", request.ExpectedRevision, target.Revision)
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
	if err := s.reserveOperation(operationCtx, namespace, target.ID, meta.IdempotencyKey, signature, ledger.Identity{
		ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID,
		TargetGeneration: target.CurrentGeneration,
	}); err != nil {
		return nil, err
	}
	evidence, err := driver.Quarantine(operationCtx, plan)
	if err != nil {
		if domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, err
	}
	if err := evidence.Validate(plan.Target); err != nil {
		return nil, status.Errorf(codes.DataLoss, "target driver returned invalid quarantine evidence: %v", err)
	}
	target, err = s.core.QuarantineTarget(operationCtx, application.QuarantineTargetRequest{
		Meta: childMeta(meta, "commit", deadline(operationCtx)), TargetID: target.ID,
		ExpectedRevision: target.Revision, Reason: request.Reason, Evidence: evidence,
	})
	if err != nil {
		return nil, err
	}
	return wiremap.Target(target), nil
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
