package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const bundleStopPreparationDirectory = "bundle-stop-preparations"

func (s *Service) prepareStoppedBundle(ctx context.Context, reservation bundleReservation, target application.TargetRecord, run application.TargetRunRecord, meta application.MutationMeta, result ports.TargetRunResult, required []string, failures []ObserverFailure, cause error) (stagedBundleStopPreparation, error) {
	persisted, err := persistTargetRunResult(s.ledger, result)
	if err != nil {
		return stagedBundleStopPreparation{}, err
	}
	observerDigest, err := stoppedResultDigest(persisted)
	if err != nil {
		return stagedBundleStopPreparation{}, err
	}
	meta.Deadline = deadline(ctx)
	preparation := stagedBundleStopPreparation{
		Version: bundleStopPreparationVersion, Reservation: reservation, Meta: meta,
		InitialRunState: run.State, InitialRevision: run.Revision, TargetGeneration: run.Generation,
		AgentWorkspaceID: run.AgentWorkspaceID, AgentGeneration: run.AgentGeneration, RunCreatedAt: run.CreatedAt.UTC(),
		RequiredCoverage: append([]string(nil), required...), Result: persisted, ObserverDigest: observerDigest,
		Incident: buildRunFailureIncidentIntent(result, failures, cause),
	}
	return s.stageBundleStopPreparation(ctx, preparation)
}

func (s *Service) stageBundleStopPreparation(ctx context.Context, preparation stagedBundleStopPreparation) (stagedBundleStopPreparation, error) {
	encoded, err := s.validateAndEncodeBundleStopPreparation(preparation)
	if err != nil {
		return stagedBundleStopPreparation{}, err
	}
	namespace, err := openDurableNamespace(s.stateRoot, bundleStopPreparationDirectory)
	if err != nil {
		return stagedBundleStopPreparation{}, err
	}
	defer namespace.Close()
	fileName := preparation.Reservation.RunID + ".json"
	record := bundleStopPreparationRecord{LeaseID: preparation.Reservation.LeaseID, TargetID: preparation.Reservation.TargetID, RunID: preparation.Reservation.RunID, BundleID: preparation.Reservation.BundleID, File: fileName, WireDigest: domain.NewDigest(encoded).String(), ObserverDigest: preparation.ObserverDigest, Size: int64(len(encoded))}
	s.mu.Lock()
	if err := namespace.EnsureRegularAtomically(fileName, encoded, 0o600); err != nil {
		s.mu.Unlock()
		if errors.Is(err, safepath.ErrConflict) {
			return stagedBundleStopPreparation{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.stage_stopped_bundle", "preparation", "existing preparation differs from the exact stopped evidence", err)
		}
		return stagedBundleStopPreparation{}, err
	}
	s.mu.Unlock()
	// Ordering is deliberate: the full exact result exists first, then the
	// observer marker binds its stopped journal to both the result and complete
	// preparation digests, and only then
	// does the hash-chained control ledger make the preparation authoritative.
	// Startup can safely delete a file that never reached the marker, while a
	// marker-bound file must be anchored rather than discarded.
	if err := s.observers.BindStoppedPreparation(preparation.Reservation.RunID, preparation.ObserverDigest, record.WireDigest); err != nil {
		if durableErr := s.observers.RequireStoppedPreparation(preparation.Reservation.RunID, preparation.ObserverDigest, record.WireDigest); durableErr != nil {
			return stagedBundleStopPreparation{}, errors.Join(err, durableErr)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, found := s.stopPreparationRecords[record.RunID]; found {
		if existing != record {
			return stagedBundleStopPreparation{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.stage_stopped_bundle", "anchor", "preparation differs from its hash-chained identity", nil)
		}
	} else if err := s.persistStateLocked(ctx, stateEvent{Kind: "bundle.stop_prepared", StopPreparation: &record}, ledger.Identity{LeaseID: record.LeaseID, TargetID: record.TargetID, TargetRunID: record.RunID}); err != nil {
		return stagedBundleStopPreparation{}, err
	}
	s.stopPreparations[record.RunID] = cloneBundleStopPreparation(preparation)
	return cloneBundleStopPreparation(preparation), nil
}

func (s *Service) validateAndEncodeBundleStopPreparation(preparation stagedBundleStopPreparation) ([]byte, error) {
	if preparation.Version != bundleStopPreparationVersion {
		return nil, fmt.Errorf("unsupported bundle stop preparation version %d", preparation.Version)
	}
	if err := validateBundleReservation(preparation.Reservation); err != nil {
		return nil, err
	}
	if preparation.Result.RunID != preparation.Reservation.RunID || preparation.InitialRevision == 0 || !preparation.InitialRunState.IsValid() || preparation.InitialRunState.Terminal() || preparation.TargetGeneration == 0 || preparation.AgentWorkspaceID == "" || preparation.AgentGeneration == 0 || preparation.RunCreatedAt.IsZero() || len(preparation.RequiredCoverage) == 0 {
		return nil, fmt.Errorf("bundle stop preparation scope is incomplete")
	}
	if !domain.IsCanonicalIdempotencyKey(preparation.Meta.IdempotencyKey) || preparation.Meta.CorrelationID == "" || preparation.Meta.AuthorizedPolicyReference == "" {
		return nil, fmt.Errorf("bundle stop preparation metadata is incomplete")
	}
	if preparation.ObserverDigest == "" {
		return nil, fmt.Errorf("bundle stop preparation observer digest is required")
	}
	if _, err := domain.ParseDigest(preparation.ObserverDigest); err != nil {
		return nil, err
	}
	if digest, err := stoppedResultDigest(preparation.Result); err != nil || digest != preparation.ObserverDigest {
		return nil, fmt.Errorf("bundle stop preparation observer digest differs from its result: %w", err)
	}
	wantsIncident := preparation.Result.Outcome == ports.RunFailed && len(preparation.Result.IncidentIDs) == 0
	if wantsIncident != (preparation.Incident != nil) {
		return nil, fmt.Errorf("bundle stop preparation failure incident intent is missing or unexpected")
	}
	if preparation.Incident != nil {
		if err := validateRunFailureIncidentIntent(*preparation.Incident); err != nil {
			return nil, err
		}
	}
	preparation.Meta.Deadline = application.MutationMeta{}.Deadline
	encoded, err := json.Marshal(preparation)
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) > s.maxBundlePublicationBytes() {
		return nil, fmt.Errorf("bundle stop preparation exceeds %d bytes", s.maxBundlePublicationBytes())
	}
	return encoded, nil
}

func stoppedResultDigest(result persistedTargetRunResult) (string, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return domain.NewDigest(encoded).String(), nil
}

func (s *Service) loadBundleStopPreparations() error {
	namespace, err := openDurableNamespace(s.stateRoot, bundleStopPreparationDirectory)
	if err != nil {
		return err
	}
	defer namespace.Close()
	if err := cleanupDurableNamespaceStages(namespace); err != nil {
		return err
	}
	names, err := namespace.ListNames()
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("bundle stop preparation directory contains unsupported entry %q", name)
		}
		encoded, err := namespace.ReadRegularBounded(name, s.maxBundlePublicationBytes())
		if err != nil {
			return err
		}
		var preparation stagedBundleStopPreparation
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&preparation)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		if decodeErr != nil || !errors.Is(trailingErr, io.EOF) {
			return fmt.Errorf("decode %s: %w", name, errors.Join(decodeErr, trailingErr))
		}
		canonical, err := s.validateAndEncodeBundleStopPreparation(preparation)
		if err != nil || !bytes.Equal(canonical, encoded) {
			return fmt.Errorf("validate %s: %w", name, err)
		}
		if name != preparation.Reservation.RunID+".json" {
			return fmt.Errorf("bundle stop preparation filename does not match its run")
		}
		reservation, found := s.reservations[preparation.Reservation.RunID]
		if !found || reservation != preparation.Reservation {
			return fmt.Errorf("bundle stop preparation has no exact reservation")
		}
		record, anchored := s.stopPreparationRecords[preparation.Reservation.RunID]
		if !anchored {
			target, targetErr := s.core.GetTarget(context.Background(), preparation.Reservation.TargetID)
			if targetErr != nil {
				return targetErr
			}
			run, runErr := targetRun(target, preparation.Reservation.RunID)
			if runErr != nil {
				return runErr
			}
			if run.State.Terminal() {
				return fmt.Errorf("terminal run %s has an unanchored stop preparation", run.ID)
			}
			wireDigest := domain.NewDigest(encoded).String()
			if s.observers != nil && s.observers.RequireStoppedPreparation(preparation.Reservation.RunID, preparation.ObserverDigest, wireDigest) == nil {
				// The file was fully flushed and the observer marker bound it, but
				// the control-ledger anchor was interrupted. Retain the exact file;
				// ReconcileRunFinalizations will append the missing anchor before
				// consuming any of its semantics.
				s.stopPreparations[reservation.RunID] = cloneBundleStopPreparation(preparation)
				continue
			}
			if err := namespace.RemoveRegular(name); err != nil {
				return err
			}
			continue
		}
		if record.File != name || record.Size != int64(len(encoded)) || record.WireDigest != domain.NewDigest(encoded).String() || record.ObserverDigest != preparation.ObserverDigest || record.LeaseID != reservation.LeaseID || record.TargetID != reservation.TargetID || record.BundleID != reservation.BundleID {
			return fmt.Errorf("bundle stop preparation %s differs from its hash-chained identity", name)
		}
		s.stopPreparations[reservation.RunID] = cloneBundleStopPreparation(preparation)
		seen[reservation.RunID] = struct{}{}
	}
	for runID := range s.stopPreparationRecords {
		if _, found := seen[runID]; !found {
			return fmt.Errorf("hash-chained bundle stop preparation for run %s has no exact file", runID)
		}
	}
	return nil
}

func validateBundleStopPreparationRecord(record bundleStopPreparationRecord, maximum int64) error {
	if _, err := domain.ParseLeaseID(record.LeaseID); err != nil {
		return err
	}
	if _, err := domain.ParseTargetID(record.TargetID); err != nil {
		return err
	}
	if _, err := domain.ParseTargetRunID(record.RunID); err != nil {
		return err
	}
	if _, err := domain.ParseObservationBundleID(record.BundleID); err != nil {
		return err
	}
	if _, err := domain.ParseDigest(record.WireDigest); err != nil {
		return err
	}
	if _, err := domain.ParseDigest(record.ObserverDigest); err != nil {
		return err
	}
	if record.File != record.RunID+".json" || record.Size <= 0 || record.Size > maximum {
		return fmt.Errorf("bundle stop preparation file identity or size is invalid")
	}
	return nil
}

func cloneBundleStopPreparation(value stagedBundleStopPreparation) stagedBundleStopPreparation {
	value.RequiredCoverage = append([]string(nil), value.RequiredCoverage...)
	if value.Incident != nil {
		clone := *value.Incident
		value.Incident = &clone
	}
	return value
}

func (s *Service) resumeBundleStopPreparation(ctx context.Context, preparation stagedBundleStopPreparation) (*worldv1.ObservationBundle, error) {
	encoded, err := s.validateAndEncodeBundleStopPreparation(preparation)
	if err != nil {
		return nil, err
	}
	if err := s.observers.RequireStoppedPreparation(preparation.Reservation.RunID, preparation.ObserverDigest, domain.NewDigest(encoded).String()); err != nil {
		return nil, err
	}
	target, err := s.core.GetTarget(ctx, preparation.Reservation.TargetID)
	if err != nil {
		return nil, err
	}
	if target.LeaseID != preparation.Reservation.LeaseID {
		return nil, fmt.Errorf("stop preparation lease no longer matches target")
	}
	run, err := targetRun(target, preparation.Reservation.RunID)
	if err != nil {
		return nil, err
	}
	if run.State.Terminal() {
		if err := s.resumeTerminalBundle(ctx, run.ID); err != nil {
			return nil, err
		}
		s.mu.RLock()
		publication := cloneBundlePublication(s.publications[run.ID])
		s.mu.RUnlock()
		return cloneObservationBundle(publication.Bundle), nil
	}
	result, err := preparation.Result.restore(s.ledger)
	if err != nil {
		return nil, err
	}
	meta := preparation.Meta
	meta.Deadline = deadline(ctx)
	if preparation.Incident != nil {
		request := buildRunFailureIncidentRequest(meta, target, preparation, result)
		request.Meta.Deadline = deadline(ctx)
		result, err = s.ensureRunFailureIncidentRequest(ctx, result, request)
		if err != nil {
			return nil, err
		}
		if s.finalizationFaults != nil && s.finalizationFaults.afterFailureIncident != nil {
			if err := s.finalizationFaults.afterFailureIncident(); err != nil {
				return nil, err
			}
		}
	}
	if result.Outcome == ports.RunCompleted && preparation.InitialRunState == domain.TargetRunObserving {
		run, err = s.core.TransitionTargetRun(ctx, application.TransitionTargetRunRequest{Meta: childMeta(meta, "reconcile-running", deadline(ctx)), TargetID: target.ID, RunID: run.ID, ExpectedRevision: preparation.InitialRevision, State: domain.TargetRunRunning})
		if err != nil {
			return nil, err
		}
	}
	if result.Outcome == ports.RunCompleted && run.State != domain.TargetRunRunning && run.State != domain.TargetRunFinalizing {
		return nil, fmt.Errorf("completed stopped result cannot be reconciled with run state %s", run.State)
	}
	bundleID, err := domain.ParseObservationBundleID(preparation.Reservation.BundleID)
	if err != nil {
		return nil, err
	}
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		return nil, err
	}
	agentID, err := domain.ParseAgentWorkspaceID(preparation.AgentWorkspaceID)
	if err != nil {
		return nil, err
	}
	prepared, err := s.finalization.Prepare(ctx, application.FinalizeRunEvidenceRequest{Meta: childMeta(meta, "finalize", deadline(ctx)), TargetID: target.ID, ExpectedRunRevision: run.Revision, Evidence: observationbundle.FinalizeRequest{BundleID: bundleID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(preparation.TargetGeneration), AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(preparation.AgentGeneration), RequiredCoverage: append([]string(nil), preparation.RequiredCoverage...), Result: result, CreatedAt: preparation.RunCreatedAt, FinalizedAt: result.StoppedAt.UTC()}})
	if err != nil {
		return nil, err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterEvidencePrepared != nil {
		if err := s.finalizationFaults.afterEvidencePrepared(); err != nil {
			return nil, err
		}
	}
	mapped := mapObservationBundle(prepared.Bundle)
	publication, err := s.stageBundlePublication(ctx, preparation.Reservation, prepared.Commit, incidentArtifact(prepared.Artifact), mapped)
	if err != nil {
		return nil, err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterPublicationStage != nil {
		if err := s.finalizationFaults.afterPublicationStage(); err != nil {
			return nil, err
		}
	}
	if _, err := s.finalization.Commit(ctx, prepared); err != nil {
		return nil, err
	}
	if s.finalizationFaults != nil && s.finalizationFaults.afterCoreCommit != nil {
		if err := s.finalizationFaults.afterCoreCommit(); err != nil {
			return nil, err
		}
	}
	if err := s.sealPublicationIncidents(ctx, publication); err != nil {
		return nil, err
	}
	if err := s.persistBundle(ctx, target.LeaseID, target.ID, run.ID, mapped); err != nil {
		return nil, err
	}
	return mapped, nil
}

func (s *Service) sortedStopPreparations() []stagedBundleStopPreparation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	runIDs := make([]string, 0, len(s.stopPreparations))
	for runID := range s.stopPreparations {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	result := make([]stagedBundleStopPreparation, 0, len(runIDs))
	for _, runID := range runIDs {
		result = append(result, cloneBundleStopPreparation(s.stopPreparations[runID]))
	}
	return result
}
