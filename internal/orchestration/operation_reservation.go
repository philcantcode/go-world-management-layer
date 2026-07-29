package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requireOperationReservationRecordLocked returns the exact durable reservation
// already owned by this request. The caller holds s.mu.
func (s *Service) requireOperationReservationRecordLocked(requested operationReservation) (operationReservation, bool, error) {
	if err := validateOperationReservation(requested); err != nil {
		return operationReservation{}, false, status.Error(codes.InvalidArgument, err.Error())
	}
	index := operationReservationIndex(requested.Namespace, requested.ResourceID, requested.TargetGeneration)
	reservation, found := s.operations[index]
	if found {
		if !sameOperationReservation(reservation, requested) {
			return operationReservation{}, true, status.Error(codes.AlreadyExists, "resource already has a different operation reservation")
		}
		return cloneOperationReservation(reservation), true, nil
	}
	if indexedResource, indexed, err := s.existingIdempotencyLocked(requested.Namespace, requested.IdempotencyKey, requested.Signature); indexed {
		if err != nil {
			return operationReservation{}, false, err
		}
		if indexedResource != requested.ResourceID {
			return operationReservation{}, false, status.Error(codes.AlreadyExists, "idempotency key already belongs to another resource")
		}
		return operationReservation{}, false, status.Error(codes.DataLoss, "idempotency index references a different operation generation")
	}
	return operationReservation{}, false, nil
}

func (s *Service) requireOperationReservationLocked(namespace, resourceID, key, signature string, targetGeneration uint64) (bool, error) {
	_, found, err := s.requireOperationReservationRecordLocked(operationReservation{
		Namespace: namespace, ResourceID: resourceID, TargetGeneration: targetGeneration,
		IdempotencyKey: key, Signature: signature,
	})
	return found, err
}

// reserveOperation persists ownership before the first logical or physical
// side effect. It is safe to call again with the exact request after restart.
func (s *Service) reserveOperation(ctx context.Context, namespace, resourceID, key, signature string, identity ledger.Identity) error {
	reservation := operationReservation{Namespace: namespace, ResourceID: resourceID, IdempotencyKey: key, Signature: signature}
	if targetOperationNamespace(namespace) {
		reservation.TargetGeneration = identity.TargetGeneration
	}
	_, err := s.reserveOperationRecord(ctx, reservation, identity)
	return err
}

func (s *Service) reserveTargetQuarantine(ctx context.Context, resourceID, key, signature string, identity ledger.Identity, intent targetQuarantineIntent) (operationReservation, error) {
	return s.reserveOperationRecord(ctx, operationReservation{
		Namespace: "quarantine_target", ResourceID: resourceID, TargetGeneration: identity.TargetGeneration,
		IdempotencyKey: key, Signature: signature, Quarantine: &intent,
	}, identity)
}

func (s *Service) reserveOperationRecord(ctx context.Context, requested operationReservation, identity ledger.Identity) (operationReservation, error) {
	if targetOperationNamespace(requested.Namespace) {
		if identity.TargetID != requested.ResourceID || identity.TargetGeneration != requested.TargetGeneration || !domain.TargetGeneration(identity.TargetGeneration).IsValid() {
			return operationReservation{}, status.Error(codes.InvalidArgument, "target operation reservation requires its exact target generation identity")
		}
	} else if requested.TargetGeneration != 0 {
		return operationReservation{}, status.Error(codes.InvalidArgument, "non-target operation reservation cannot identify a target generation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reservation, found, err := s.requireOperationReservationRecordLocked(requested)
	if err != nil {
		return operationReservation{}, err
	}
	if found {
		return reservation, nil
	}
	if targetOperationNamespace(requested.Namespace) {
		for _, namespace := range []string{"destroy_target", "quarantine_target"} {
			if namespace == requested.Namespace {
				continue
			}
			if _, conflict := s.operations[operationReservationIndex(namespace, requested.ResourceID, requested.TargetGeneration)]; conflict {
				return operationReservation{}, status.Error(codes.AlreadyExists, "target generation already has a different lifecycle operation reservation")
			}
		}
	}
	persisted := cloneOperationReservation(requested)
	if err := s.persistStateLocked(ctx, stateEvent{
		Kind: "operation.reserved", Namespace: persisted.Namespace, IdempotencyKey: persisted.IdempotencyKey,
		Signature: persisted.Signature, Operation: &persisted,
	}, identity); err != nil {
		return operationReservation{}, err
	}
	return cloneOperationReservation(s.operations[operationReservationIndex(persisted.Namespace, persisted.ResourceID, persisted.TargetGeneration)]), nil
}

func (s *Service) lookupOperationReservationLocked(namespace, resourceID string, targetGeneration uint64) (operationReservation, bool) {
	reservation, found := s.operations[operationReservationIndex(namespace, resourceID, targetGeneration)]
	return cloneOperationReservation(reservation), found
}

func targetOperationNamespace(namespace string) bool {
	return namespace == "destroy_target" || namespace == "quarantine_target"
}

func (s *Service) requireReservedOperation(namespace, resourceID, key, signature string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, found := s.lookupOperationReservationLocked(namespace, resourceID, 0)
	if !found {
		return status.Error(codes.DataLoss, "terminal resource has no durable operation reservation")
	}
	if reservation.IdempotencyKey != key || reservation.Signature != signature {
		return status.Error(codes.AlreadyExists, "resource reached its terminal state through a different request")
	}
	return nil
}

func (s *Service) requireReservedTargetOperation(namespace, resourceID string, targetGeneration uint64, key, signature string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, found := s.lookupOperationReservationLocked(namespace, resourceID, targetGeneration)
	if !found {
		return status.Error(codes.DataLoss, "terminal target generation has no durable operation reservation")
	}
	if reservation.TargetGeneration != targetGeneration {
		return status.Error(codes.AlreadyExists, "terminal target state belongs to another generation reservation")
	}
	if reservation.IdempotencyKey != key || reservation.Signature != signature {
		return status.Error(codes.AlreadyExists, "target generation reached its terminal state through a different request")
	}
	return nil
}

func (s *Service) exactReservedTargetOperation(namespace, resourceID string, targetGeneration uint64, key, signature string) (operationReservation, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, found := s.lookupOperationReservationLocked(namespace, resourceID, targetGeneration)
	if !found {
		return operationReservation{}, false, nil
	}
	if reservation.TargetGeneration != targetGeneration || reservation.IdempotencyKey != key || reservation.Signature != signature {
		return operationReservation{}, true, status.Error(codes.AlreadyExists, "target generation already has a different operation reservation")
	}
	return reservation, true, nil
}

// operationReservation returns the durable ownership record for one resource
// operation. Startup reconciliation uses this read-only lookup to distinguish
// an ordinary resettable target from the exact crash-resumable destroy intent
// that is allowed to make that target physically absent.
func (s *Service) operationReservation(namespace, resourceID string, targetGeneration uint64) (operationReservation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, found := s.operations[operationReservationIndex(namespace, resourceID, targetGeneration)]
	return cloneOperationReservation(reservation), found && reservation.TargetGeneration == targetGeneration
}

func validateOperationReservation(reservation operationReservation) error {
	if strings.TrimSpace(reservation.Namespace) == "" || strings.TrimSpace(reservation.ResourceID) == "" || !domain.IsCanonicalIdempotencyKey(reservation.IdempotencyKey) || strings.TrimSpace(reservation.Signature) == "" {
		return fmt.Errorf("operation reservation is incomplete")
	}
	switch reservation.Namespace {
	case "stop_capture", "commit_export":
		if reservation.TargetGeneration != 0 || reservation.Quarantine != nil {
			return fmt.Errorf("non-target operation reservation contains target lifecycle state")
		}
	case "destroy_target":
		if reservation.Quarantine != nil {
			return fmt.Errorf("destroy target reservation contains quarantine state")
		}
		if !domain.TargetGeneration(reservation.TargetGeneration).IsValid() {
			return fmt.Errorf("destroy target reservation requires an exact target generation")
		}
	case "quarantine_target":
		if !domain.TargetGeneration(reservation.TargetGeneration).IsValid() || reservation.Quarantine == nil {
			return fmt.Errorf("quarantine target reservation requires exact generation-bound intent")
		}
		if err := validateTargetQuarantineIntent(reservation, *reservation.Quarantine); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported operation reservation namespace %q", reservation.Namespace)
	}
	return nil
}

func validateTargetQuarantineIntent(reservation operationReservation, intent targetQuarantineIntent) error {
	if err := intent.Plan.Validate(); err != nil {
		return fmt.Errorf("target quarantine intent plan: %w", err)
	}
	if intent.Plan.Target.ID.String() != reservation.ResourceID || uint64(intent.Plan.Target.Generation) != reservation.TargetGeneration || intent.Plan.IdempotencyKey != reservation.IdempotencyKey {
		return fmt.Errorf("target quarantine intent does not match its exact reservation")
	}
	meta := intent.CommitMeta
	if !meta.Deadline.IsZero() {
		return fmt.Errorf("target quarantine intent persisted an attempt-scoped deadline")
	}
	if meta.IdempotencyKey != domain.DeriveIdempotencyKey(reservation.IdempotencyKey, "commit") || strings.TrimSpace(meta.AuthorizedPolicyReference) == "" {
		return fmt.Errorf("target quarantine commit identity is incomplete")
	}
	if _, err := domain.ParseCorrelationID(meta.CorrelationID); err != nil {
		return fmt.Errorf("target quarantine commit correlation is invalid: %w", err)
	}
	if meta.CausationID != "" {
		if _, err := domain.ParseEventID(meta.CausationID); err != nil {
			return fmt.Errorf("target quarantine commit causation is invalid: %w", err)
		}
	}
	return nil
}

func validateTargetQuarantineContainment(reservation operationReservation, containment targetQuarantineContainment) error {
	if reservation.Quarantine == nil || containment.Namespace != reservation.Namespace || containment.ResourceID != reservation.ResourceID || containment.TargetGeneration != reservation.TargetGeneration || containment.IdempotencyKey != reservation.IdempotencyKey || containment.Signature != reservation.Signature {
		return fmt.Errorf("target quarantine containment does not match its exact durable intent")
	}
	if err := containment.Evidence.Validate(reservation.Quarantine.Plan.Target); err != nil {
		return fmt.Errorf("target quarantine containment evidence is invalid: %w", err)
	}
	return nil
}

func cloneOperationReservation(reservation operationReservation) operationReservation {
	if reservation.Quarantine != nil {
		intent := *reservation.Quarantine
		reservation.Quarantine = &intent
	}
	return reservation
}

func sameOperationReservation(left, right operationReservation) bool {
	if left.Namespace != right.Namespace || left.ResourceID != right.ResourceID || left.TargetGeneration != right.TargetGeneration || left.IdempotencyKey != right.IdempotencyKey || left.Signature != right.Signature {
		return false
	}
	if left.Quarantine == nil || right.Quarantine == nil {
		return left.Quarantine == nil && right.Quarantine == nil
	}
	return *left.Quarantine == *right.Quarantine
}

func (s *Service) targetQuarantineContainment(reservation operationReservation) (targetQuarantineContainment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	containment, found := s.quarantineContainments[operationReservationIndex(reservation.Namespace, reservation.ResourceID, reservation.TargetGeneration)]
	return containment, found
}

func (s *Service) persistTargetQuarantineContainment(ctx context.Context, reservation operationReservation, evidence ports.TargetQuarantineEvidence, identity ledger.Identity) (targetQuarantineContainment, error) {
	containment := targetQuarantineContainment{
		Namespace: reservation.Namespace, ResourceID: reservation.ResourceID, TargetGeneration: reservation.TargetGeneration,
		IdempotencyKey: reservation.IdempotencyKey, Signature: reservation.Signature, Evidence: evidence,
	}
	if err := validateTargetQuarantineContainment(reservation, containment); err != nil {
		return targetQuarantineContainment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := operationReservationIndex(reservation.Namespace, reservation.ResourceID, reservation.TargetGeneration)
	persistedReservation, found := s.operations[index]
	if !found || !sameOperationReservation(persistedReservation, reservation) {
		return targetQuarantineContainment{}, status.Error(codes.DataLoss, "target quarantine containment has no exact durable intent")
	}
	if existing, found := s.quarantineContainments[index]; found {
		if existing != containment {
			return targetQuarantineContainment{}, status.Error(codes.DataLoss, "target quarantine containment evidence changed")
		}
		return existing, nil
	}
	if err := s.persistStateLocked(ctx, stateEvent{Kind: "target_quarantine.contained", Quarantine: &containment}, identity); err != nil {
		return targetQuarantineContainment{}, err
	}
	return s.quarantineContainments[index], nil
}

func (s *Service) verifyOperationIndexes(ctx context.Context) error {
	indexes := make([]string, 0, len(s.operations))
	for index := range s.operations {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	for _, index := range indexes {
		if err := ctx.Err(); err != nil {
			return err
		}
		reservation := s.operations[index]
		if err := validateOperationReservation(reservation); err != nil {
			return fmt.Errorf("operation %s/%s is invalid: %w", reservation.Namespace, reservation.ResourceID, err)
		}
		idempotency, found := s.idempotency[idempotencyIndex(reservation.Namespace, reservation.IdempotencyKey)]
		if !found || idempotency.Signature != reservation.Signature || idempotency.ResourceID != reservation.ResourceID {
			return fmt.Errorf("operation %s/%s has no matching idempotency index", reservation.Namespace, reservation.ResourceID)
		}
		switch reservation.Namespace {
		case "stop_capture":
			record, found := s.captureState[reservation.ResourceID]
			if !found || record.Capture == nil || record.Capture.CaptureId != reservation.ResourceID {
				return fmt.Errorf("stop capture reservation references missing capture %s", reservation.ResourceID)
			}
		case "commit_export":
			record, found := s.exportState[reservation.ResourceID]
			if !found || record.Export == nil || record.Export.ExportId != reservation.ResourceID || (record.Export.State != exportStateCommitting && record.Export.State != exportStateCommitted) {
				return fmt.Errorf("commit export reservation references invalid export %s", reservation.ResourceID)
			}
		case "destroy_target", "quarantine_target":
			if _, err := domain.ParseTargetID(reservation.ResourceID); err != nil {
				return fmt.Errorf("%s reservation has invalid target identity: %w", reservation.Namespace, err)
			}
			target, err := s.core.GetTarget(ctx, reservation.ResourceID)
			if err != nil {
				return fmt.Errorf("%s reservation references missing target %s: %w", reservation.Namespace, reservation.ResourceID, err)
			}
			if !domain.TargetGeneration(reservation.TargetGeneration).IsValid() {
				return fmt.Errorf("%s reservation has invalid target generation", reservation.Namespace)
			}
			foundGeneration := false
			for _, generation := range target.Generations {
				foundGeneration = foundGeneration || generation.Generation == reservation.TargetGeneration
			}
			if !foundGeneration {
				return fmt.Errorf("%s reservation references missing target generation %d", reservation.Namespace, reservation.TargetGeneration)
			}
			otherNamespace := "quarantine_target"
			if reservation.Namespace == otherNamespace {
				otherNamespace = "destroy_target"
			}
			if _, conflict := s.operations[operationReservationIndex(otherNamespace, reservation.ResourceID, reservation.TargetGeneration)]; conflict {
				return fmt.Errorf("target %s generation %d has conflicting lifecycle reservations", reservation.ResourceID, reservation.TargetGeneration)
			}
		default:
			return fmt.Errorf("unsupported operation reservation namespace %q", reservation.Namespace)
		}
	}
	for index, containment := range s.quarantineContainments {
		reservation, found := s.operations[index]
		if !found {
			return fmt.Errorf("target quarantine containment has no reservation")
		}
		if err := validateTargetQuarantineContainment(reservation, containment); err != nil {
			return err
		}
	}
	for captureID, record := range s.captureState {
		if record.Capture != nil && record.Capture.State == captureStateCompleted {
			if _, found := s.operations[operationIndex("stop_capture", captureID)]; !found {
				return fmt.Errorf("completed capture %s has no stop reservation", captureID)
			}
		}
	}
	for exportID, record := range s.exportState {
		if record.Export != nil && (record.Export.State == exportStateCommitting || record.Export.State == exportStateCommitted) {
			if _, found := s.operations[operationIndex("commit_export", exportID)]; !found {
				return fmt.Errorf("%s export %s has no commit reservation", record.Export.State, exportID)
			}
		}
	}
	return nil
}
