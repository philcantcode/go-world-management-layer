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
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const bundlePublicationDirectory = "bundle-publications"

func (s *Service) stageBundlePublication(ctx context.Context, reservation bundleReservation, commit application.FinalizeTargetRunRequest, artifact application.IncidentArtifactRecord, bundle *worldv1.ObservationBundle) (stagedBundlePublication, error) {
	publication := stagedBundlePublication{
		Version: bundlePublicationVersion, Reservation: reservation,
		Commit: commit, Artifact: artifact, Bundle: cloneObservationBundle(bundle),
	}
	encoded, err := s.validateAndEncodeBundlePublication(publication)
	if err != nil {
		return stagedBundlePublication{}, err
	}
	namespace, err := openDurableNamespace(s.stateRoot, bundlePublicationDirectory)
	if err != nil {
		return stagedBundlePublication{}, err
	}
	defer namespace.Close()
	fileName := reservation.RunID + ".json"

	s.mu.Lock()
	defer s.mu.Unlock()
	if preparation, found := s.stopPreparations[reservation.RunID]; !found || preparation.Reservation != reservation {
		return stagedBundlePublication{}, status.Error(codes.DataLoss, "staged observation bundle has no exact stopped-run preparation")
	}
	if err := namespace.EnsureRegularAtomically(fileName, encoded, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return stagedBundlePublication{}, status.Error(codes.DataLoss, "staged observation bundle conflicts with the exact finalization reservation")
		}
		return stagedBundlePublication{}, err
	}
	record := bundlePublicationRecord{
		LeaseID: reservation.LeaseID, TargetID: reservation.TargetID, RunID: reservation.RunID,
		BundleID: reservation.BundleID, File: fileName,
		WireDigest: domain.NewDigest(encoded).String(), Size: int64(len(encoded)),
	}
	if existing, found := s.publicationRecords[reservation.RunID]; found {
		if existing != record {
			return stagedBundlePublication{}, status.Error(codes.DataLoss, "staged observation bundle conflicts with its durable wire identity")
		}
	} else {
		event := stateEvent{Kind: "bundle.publication_staged", Publication: &record}
		identity := ledger.Identity{LeaseID: reservation.LeaseID, TargetID: reservation.TargetID, TargetRunID: reservation.RunID}
		if err := s.persistStateLocked(ctx, event, identity); err != nil {
			return stagedBundlePublication{}, err
		}
	}
	if existing, found := s.publications[reservation.RunID]; found {
		existingEncoded, encodeErr := json.Marshal(existing)
		if encodeErr != nil || !bytes.Equal(existingEncoded, encoded) {
			return stagedBundlePublication{}, status.Error(codes.DataLoss, "in-memory observation bundle stage conflicts with durable publication")
		}
	}
	s.publications[reservation.RunID] = cloneBundlePublication(publication)
	return cloneBundlePublication(publication), nil
}

func (s *Service) loadBundlePublications() error {
	namespace, err := openDurableNamespace(s.stateRoot, bundlePublicationDirectory)
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
			return fmt.Errorf("bundle publication directory contains unsupported entry %q", name)
		}
		encoded, err := namespace.ReadRegularBounded(name, s.maxBundlePublicationBytes())
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		var publication stagedBundlePublication
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&publication)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		if decodeErr != nil || !errors.Is(trailingErr, io.EOF) {
			return fmt.Errorf("decode %s: %w", name, errors.Join(decodeErr, trailingErr))
		}
		canonical, err := s.validateAndEncodeBundlePublication(publication)
		if err != nil {
			return fmt.Errorf("validate %s: %w", name, err)
		}
		if !bytes.Equal(canonical, encoded) {
			return fmt.Errorf("bundle publication %s is not canonical JSON", name)
		}
		if name != publication.Reservation.RunID+".json" {
			return fmt.Errorf("bundle publication filename does not match its run")
		}
		reservation, found := s.reservations[publication.Reservation.RunID]
		if !found || reservation != publication.Reservation {
			return fmt.Errorf("bundle publication %s has no exact durable reservation", name)
		}
		record, anchored := s.publicationRecords[publication.Reservation.RunID]
		if !anchored {
			target, targetErr := s.core.GetTarget(context.Background(), publication.Reservation.TargetID)
			if targetErr != nil {
				return fmt.Errorf("load unanchored bundle publication target: %w", targetErr)
			}
			run, runErr := targetRun(target, publication.Reservation.RunID)
			if runErr != nil {
				return runErr
			}
			if run.State.Terminal() {
				return fmt.Errorf("terminal run %s has an unanchored public bundle publication", run.ID)
			}
			// The stage event is the commit point for public wire identity. A
			// final file left before that event is rolled back and recomputed by
			// the exact reservation retry; it can never be served or committed.
			if err := namespace.RemoveRegular(name); err != nil {
				return fmt.Errorf("roll back unanchored bundle publication %s: %w", name, err)
			}
			continue
		}
		if record.File != name || record.Size != int64(len(encoded)) || record.WireDigest != domain.NewDigest(encoded).String() || record.LeaseID != publication.Reservation.LeaseID || record.TargetID != publication.Reservation.TargetID || record.BundleID != publication.Reservation.BundleID {
			return fmt.Errorf("bundle publication %s differs from its hash-chained stage identity", name)
		}
		if _, duplicate := s.publications[publication.Reservation.RunID]; duplicate {
			return fmt.Errorf("duplicate bundle publication for run %s", publication.Reservation.RunID)
		}
		s.publications[publication.Reservation.RunID] = cloneBundlePublication(publication)
		seen[publication.Reservation.RunID] = struct{}{}
	}
	for runID := range s.publicationRecords {
		if _, found := seen[runID]; !found {
			return fmt.Errorf("hash-chained bundle publication for run %s has no exact file", runID)
		}
	}
	return nil
}

func validateBundlePublicationRecord(record bundlePublicationRecord, maximumBytes int64) error {
	if _, err := domain.ParseLeaseID(record.LeaseID); err != nil {
		return fmt.Errorf("bundle publication lease id: %w", err)
	}
	if _, err := domain.ParseTargetID(record.TargetID); err != nil {
		return fmt.Errorf("bundle publication target id: %w", err)
	}
	if _, err := domain.ParseTargetRunID(record.RunID); err != nil {
		return fmt.Errorf("bundle publication run id: %w", err)
	}
	if _, err := domain.ParseObservationBundleID(record.BundleID); err != nil {
		return fmt.Errorf("bundle publication bundle id: %w", err)
	}
	if _, err := domain.ParseDigest(record.WireDigest); err != nil {
		return fmt.Errorf("bundle publication wire digest: %w", err)
	}
	if record.File != record.RunID+".json" || record.Size <= 0 || record.Size > maximumBytes {
		return fmt.Errorf("bundle publication file identity or size is invalid")
	}
	return nil
}

// reconcileBundleFiles removes only unreachable staging files, then proves
// every published JSON file belongs to an exact durable stage.
// A final bundle file without its index is valid interrupted progress and will
// be indexed by ReconcileRunFinalizations.
func (s *Service) reconcileBundleFiles() error {
	namespace, err := openDurableNamespace(s.stateRoot, "bundles")
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
	for _, name := range names {
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("bundle directory contains unsupported entry %q", name)
		}
		runID := strings.TrimSuffix(name, ".json")
		publication, staged := s.publications[runID]
		if !staged || name != publication.Reservation.RunID+".json" {
			return fmt.Errorf("bundle file %q has no exact recoverable publication", name)
		}
		encoded, err := namespace.ReadRegularBounded(name, s.maxTransferBytes)
		if err != nil {
			return err
		}
		expected, err := json.Marshal(publication.Bundle)
		if err != nil || !bytes.Equal(encoded, expected) {
			return fmt.Errorf("bundle file %q differs from its recoverable publication", name)
		}
		if record, indexed := s.bundles[runID]; indexed && record.File != name {
			return fmt.Errorf("bundle file %q disagrees with its durable index", name)
		}
	}
	return nil
}

func (s *Service) validateAndEncodeBundlePublication(publication stagedBundlePublication) ([]byte, error) {
	if publication.Version != bundlePublicationVersion {
		return nil, fmt.Errorf("unsupported bundle publication version %d", publication.Version)
	}
	if err := validateBundleReservation(publication.Reservation); err != nil {
		return nil, err
	}
	commit := publication.Commit
	if commit.TargetID != publication.Reservation.TargetID || commit.RunID != publication.Reservation.RunID || commit.BundleID != publication.Reservation.BundleID || commit.ExpectedRevision == 0 {
		return nil, fmt.Errorf("bundle publication commit identity does not match its reservation")
	}
	if !domain.IsCanonicalIdempotencyKey(commit.Meta.IdempotencyKey) || commit.Meta.CorrelationID == "" || commit.Meta.AuthorizedPolicyReference == "" {
		return nil, fmt.Errorf("bundle publication commit metadata is incomplete")
	}
	if _, err := domain.ParseCorrelationID(commit.Meta.CorrelationID); err != nil {
		return nil, fmt.Errorf("bundle publication correlation id: %w", err)
	}
	if commit.Meta.CausationID != "" {
		if _, err := domain.ParseEventID(commit.Meta.CausationID); err != nil {
			return nil, fmt.Errorf("bundle publication causation id: %w", err)
		}
	}
	if _, err := domain.ParseDigest(commit.Meta.AuthorizedPolicyReference); err != nil {
		return nil, fmt.Errorf("bundle publication policy reference: %w", err)
	}
	if strings.TrimSpace(commit.BundleArtifact) == "" {
		return nil, fmt.Errorf("bundle publication artifact reference is required")
	}
	if _, err := domain.ParseDigest(commit.BundleDigest); err != nil {
		return nil, fmt.Errorf("bundle publication artifact digest: %w", err)
	}
	artifactDigest, err := domain.ParseDigest(publication.Artifact.Digest)
	if err != nil {
		return nil, fmt.Errorf("bundle publication incident artifact digest: %w", err)
	}
	if _, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: publication.Artifact.Reference, Digest: artifactDigest, Size: publication.Artifact.Size,
		Role: publication.Artifact.Role, Sensitivity: publication.Artifact.Sensitivity,
	}); err != nil {
		return nil, fmt.Errorf("bundle publication incident artifact: %w", err)
	}
	if publication.Artifact.Reference != commit.BundleArtifact || publication.Artifact.Digest != commit.BundleDigest || publication.Artifact.Role != "observation-bundle" {
		return nil, fmt.Errorf("bundle publication incident artifact differs from the terminal commit")
	}
	if publication.Bundle == nil {
		return nil, fmt.Errorf("bundle publication has no public wire bundle")
	}
	bundle := publication.Bundle
	if bundle.BundleId != publication.Reservation.BundleID || bundle.TargetRunId != publication.Reservation.RunID || bundle.TargetId != publication.Reservation.TargetID || bundle.State != string(domain.ObservationBundleSealed) {
		return nil, fmt.Errorf("public bundle identity does not match its reservation")
	}
	if len(bundle.IncidentIds) != len(commit.IncidentIDs) {
		return nil, fmt.Errorf("public bundle incidents do not match the terminal commit")
	}
	for index := range bundle.IncidentIds {
		if bundle.IncidentIds[index] != commit.IncidentIDs[index] {
			return nil, fmt.Errorf("public bundle incidents do not match the terminal commit")
		}
	}
	encoded, err := json.Marshal(publication)
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) > s.maxBundlePublicationBytes() {
		return nil, status.Errorf(codes.ResourceExhausted, "staged observation bundle exceeds %d bytes", s.maxBundlePublicationBytes())
	}
	return encoded, nil
}

func validateBundleReservation(reservation bundleReservation) error {
	if !validBundleFinalizationNamespace(reservation.Namespace) {
		return fmt.Errorf("bundle reservation namespace is invalid")
	}
	if _, err := domain.ParseLeaseID(reservation.LeaseID); err != nil {
		return fmt.Errorf("bundle reservation lease id: %w", err)
	}
	if _, err := domain.ParseTargetID(reservation.TargetID); err != nil {
		return fmt.Errorf("bundle reservation target id: %w", err)
	}
	if _, err := domain.ParseTargetRunID(reservation.RunID); err != nil {
		return fmt.Errorf("bundle reservation run id: %w", err)
	}
	if _, err := domain.ParseObservationBundleID(reservation.BundleID); err != nil {
		return fmt.Errorf("bundle reservation bundle id: %w", err)
	}
	if !domain.IsCanonicalIdempotencyKey(reservation.IdempotencyKey) || strings.TrimSpace(reservation.Signature) == "" {
		return fmt.Errorf("bundle reservation idempotency identity is invalid")
	}
	return nil
}

func validBundleFinalizationNamespace(namespace string) bool {
	switch namespace {
	case "stop_target_run", "start_target_run_rollback", "lease_termination_run", "startup_run_recovery":
		return true
	default:
		return false
	}
}

// recoveryFinalizationIdentity preserves the original irreversible-operation
// owner. Startup may mint its own reservation only when no earlier stop path
// crossed the durable reservation boundary.
func (s *Service) recoveryFinalizationIdentity(target application.TargetRecord, run application.TargetRunRecord, fallbackMeta application.MutationMeta, fallbackNamespace, fallbackSignature string) (application.MutationMeta, string, string, string, error) {
	s.mu.RLock()
	reservation, found := s.reservations[run.ID]
	s.mu.RUnlock()
	if !found {
		return fallbackMeta, fallbackNamespace, fallbackMeta.IdempotencyKey, fallbackSignature, nil
	}
	if err := validateBundleReservation(reservation); err != nil {
		return application.MutationMeta{}, "", "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.recovery_finalization_identity", "reservation", "persisted bundle reservation is invalid", err)
	}
	if reservation.TargetID != target.ID || reservation.LeaseID != target.LeaseID || reservation.RunID != run.ID {
		return application.MutationMeta{}, "", "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.recovery_finalization_identity", "reservation", "persisted bundle reservation is outside the recovered run scope", nil)
	}
	fallbackMeta.IdempotencyKey = reservation.IdempotencyKey
	return fallbackMeta, reservation.Namespace, reservation.IdempotencyKey, reservation.Signature, nil
}

func (s *Service) maxBundlePublicationBytes() int64 {
	extra := int64(s.maxStateBytes)
	if s.maxTransferBytes > int64(^uint64(0)>>1)-extra {
		return int64(^uint64(0) >> 1)
	}
	return s.maxTransferBytes + extra
}

func cloneObservationBundle(bundle *worldv1.ObservationBundle) *worldv1.ObservationBundle {
	if bundle == nil {
		return nil
	}
	return proto.Clone(bundle).(*worldv1.ObservationBundle)
}

func cloneBundlePublication(publication stagedBundlePublication) stagedBundlePublication {
	publication.Commit.IncidentIDs = append([]string(nil), publication.Commit.IncidentIDs...)
	publication.Bundle = cloneObservationBundle(publication.Bundle)
	return publication
}

// ReconcileRunFinalizations repairs every staged publication before observer
// markers are promoted. No evidence is rebuilt: the exact canonical stage is
// either committed and indexed or startup fails closed.
func (s *Service) ReconcileRunFinalizations(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, preparation := range s.sortedStopPreparations() {
		anchored, err := s.stageBundleStopPreparation(ctx, preparation)
		if err != nil {
			return fmt.Errorf("anchor stopped-run preparation for run %s: %w", preparation.Reservation.RunID, err)
		}
		if _, err := s.resumeBundleStopPreparation(ctx, anchored); err != nil {
			return fmt.Errorf("resume stopped-run preparation for run %s: %w", preparation.Reservation.RunID, err)
		}
	}
	s.mu.RLock()
	runIDs := make([]string, 0, len(s.publications))
	publications := make(map[string]stagedBundlePublication, len(s.publications))
	for runID, publication := range s.publications {
		runIDs = append(runIDs, runID)
		publications[runID] = cloneBundlePublication(publication)
	}
	reservations := make(map[string]bundleReservation, len(s.reservations))
	for runID, reservation := range s.reservations {
		reservations[runID] = reservation
	}
	s.mu.RUnlock()
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if err := s.resumeBundlePublication(ctx, publications[runID]); err != nil {
			return fmt.Errorf("resume observation bundle publication for run %s: %w", runID, err)
		}
	}
	for runID, reservation := range reservations {
		target, err := s.core.GetTarget(ctx, reservation.TargetID)
		if err != nil {
			return fmt.Errorf("load reserved target run %s: %w", runID, err)
		}
		run, err := targetRun(target, runID)
		if err != nil {
			return err
		}
		if run.State.Terminal() {
			if _, staged := publications[runID]; !staged {
				return domain.NewError(domain.CodeIntegrityViolation, "orchestration.reconcile_run_finalizations", "publication", "terminal run has no recoverable public bundle publication", nil)
			}
		}
	}
	return nil
}

func (s *Service) resumeBundlePublication(ctx context.Context, publication stagedBundlePublication) error {
	target, err := s.core.GetTarget(ctx, publication.Reservation.TargetID)
	if err != nil {
		return err
	}
	if target.LeaseID != publication.Reservation.LeaseID {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.resume_bundle_publication", "lease_id", "publication lease does not match the target", nil)
	}
	run, err := targetRun(target, publication.Reservation.RunID)
	if err != nil {
		return err
	}
	if run.State.Terminal() {
		if err := requireTerminalRunPublication(run, publication); err != nil {
			return err
		}
	} else {
		if run.Revision != publication.Commit.ExpectedRevision {
			return domain.NewError(domain.CodeIntegrityViolation, "orchestration.resume_bundle_publication", "run_revision", "nonterminal run revision differs from its staged terminal commit", nil)
		}
		commit := publication.Commit
		commit.Meta.Deadline = deadline(ctx)
		if run, err = s.core.FinalizeTargetRun(ctx, commit); err != nil {
			return err
		}
	}
	if err := s.sealPublicationIncidents(ctx, publication); err != nil {
		return err
	}
	return s.persistBundle(ctx, publication.Reservation.LeaseID, publication.Reservation.TargetID, publication.Reservation.RunID, publication.Bundle)
}

func (s *Service) sealPublicationIncidents(ctx context.Context, publication stagedBundlePublication) error {
	if len(publication.Commit.IncidentIDs) == 0 {
		return nil
	}
	meta := publication.Commit.Meta
	meta.IdempotencyKey = domain.DeriveIdempotencyKey(meta.IdempotencyKey, "seal-incidents")
	meta.Deadline = deadline(ctx)
	_, err := s.core.SealRunIncidents(ctx, application.SealRunIncidentsRequest{
		Meta: meta, TargetID: publication.Reservation.TargetID, RunID: publication.Reservation.RunID,
		BundleID: publication.Reservation.BundleID, BundleArtifact: publication.Artifact,
		IncidentIDs: append([]string(nil), publication.Commit.IncidentIDs...),
	})
	return err
}

func requireTerminalRunPublication(run application.TargetRunRecord, publication stagedBundlePublication) error {
	commit := publication.Commit
	wantState := domain.TargetRunCompleted
	if commit.Failed {
		wantState = domain.TargetRunFailed
	}
	if run.State != wantState || run.BundleID != commit.BundleID || run.BundleArtifact != commit.BundleArtifact || run.BundleDigest != commit.BundleDigest {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.resume_bundle_publication", "terminal_run", "terminal run disagrees with its staged public bundle", nil)
	}
	if len(run.IncidentIDs) != len(commit.IncidentIDs) {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.resume_bundle_publication", "incident_ids", "terminal run incidents disagree with its staged public bundle", nil)
	}
	for index := range run.IncidentIDs {
		if run.IncidentIDs[index] != commit.IncidentIDs[index] {
			return domain.NewError(domain.CodeIntegrityViolation, "orchestration.resume_bundle_publication", "incident_ids", "terminal run incidents disagree with its staged public bundle", nil)
		}
	}
	return nil
}

func (s *Service) bundlePublicationComplete(ctx context.Context, run application.TargetRunRecord) (bool, error) {
	s.mu.RLock()
	publication, staged := s.publications[run.ID]
	record, indexed := s.bundles[run.ID]
	s.mu.RUnlock()
	if !staged && !indexed {
		return false, nil
	}
	if !staged || !indexed {
		return false, domain.NewError(domain.CodeIntegrityViolation, "orchestration.bundle_publication_complete", "publication", "terminal bundle publication is only partially durable", nil)
	}
	if err := requireTerminalRunPublication(run, publication); err != nil {
		return false, err
	}
	if _, err := s.verifyStoredBundle(ctx, record); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) commitBundleCompletion(ctx context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, indexed := s.bundles[runID]
	publication, staged := s.publications[runID]
	if !indexed || !staged || record.BundleID != publication.Reservation.BundleID {
		return status.Error(codes.DataLoss, "cannot complete an unverified observation bundle publication")
	}
	completion := bundleCompletion{RunID: runID, BundleID: record.BundleID, WireDigest: record.WireDigest}
	if existing, found := s.completions[runID]; found {
		if existing != completion {
			return status.Error(codes.DataLoss, "observation bundle completion conflicts with its publication")
		}
		return nil
	}
	event := stateEvent{Kind: "bundle.completed", Completion: &completion}
	return s.persistStateLocked(ctx, event, ledger.Identity{LeaseID: record.LeaseID, TargetID: record.TargetID, TargetRunID: record.RunID})
}

// ReconcileRunFinalizationCompletions is called after persisted observer
// markers have been matched/promoted. It creates the final public-read gate.
func (s *Service) ReconcileRunFinalizationCompletions(ctx context.Context) error {
	s.mu.RLock()
	runIDs := make([]string, 0, len(s.bundles))
	for runID := range s.bundles {
		if _, complete := s.completions[runID]; !complete {
			runIDs = append(runIDs, runID)
		}
	}
	s.mu.RUnlock()
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if s.observers == nil {
			return domain.NewError(domain.CodeFailedPrecondition, "orchestration.reconcile_run_finalizations", "observers", "an indexed bundle lacks observer-commit proof", nil)
		}
		parsed, err := domain.ParseTargetRunID(runID)
		if err != nil {
			return err
		}
		if err := s.observers.RequireCommitted(parsed); err != nil {
			return err
		}
		if err := s.commitBundleCompletion(ctx, runID); err != nil {
			return err
		}
	}
	return nil
}
