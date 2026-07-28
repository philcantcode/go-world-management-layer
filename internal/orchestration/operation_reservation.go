package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// requireOperationReservationLocked returns true only for the exact durable
// reservation already owned by this request. The caller must hold s.mu.
func (s *Service) requireOperationReservationLocked(namespace, resourceID, key, signature string) (bool, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(resourceID) == "" || !domain.IsCanonicalIdempotencyKey(key) || strings.TrimSpace(signature) == "" {
		return false, status.Error(codes.InvalidArgument, "operation reservation requires namespace, resource, idempotency key, and signature")
	}
	reservation, found := s.operations[operationIndex(namespace, resourceID)]
	if found {
		if reservation.IdempotencyKey != key || reservation.Signature != signature {
			return true, status.Error(codes.AlreadyExists, "resource already has a different operation reservation")
		}
		return true, nil
	}
	if indexedResource, indexed, err := s.existingIdempotencyLocked(namespace, key, signature); indexed {
		if err != nil {
			return false, err
		}
		if indexedResource != resourceID {
			return false, status.Error(codes.AlreadyExists, "idempotency key already belongs to another resource")
		}
		return false, status.Error(codes.DataLoss, "idempotency index references a missing operation reservation")
	}
	return false, nil
}

// reserveOperation persists ownership before the first logical or physical
// side effect. It is safe to call again with the exact request after restart.
func (s *Service) reserveOperation(ctx context.Context, namespace, resourceID, key, signature string, identity ledger.Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, err := s.requireOperationReservationLocked(namespace, resourceID, key, signature)
	if err != nil || found {
		return err
	}
	reservation := operationReservation{Namespace: namespace, ResourceID: resourceID, IdempotencyKey: key, Signature: signature}
	return s.persistStateLocked(ctx, stateEvent{
		Kind: "operation.reserved", Namespace: namespace, IdempotencyKey: key,
		Signature: signature, Operation: &reservation,
	}, identity)
}

func (s *Service) requireReservedOperation(namespace, resourceID, key, signature string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, found := s.operations[operationIndex(namespace, resourceID)]
	if !found {
		return status.Error(codes.DataLoss, "terminal resource has no durable operation reservation")
	}
	if reservation.IdempotencyKey != key || reservation.Signature != signature {
		return status.Error(codes.AlreadyExists, "resource reached its terminal state through a different request")
	}
	return nil
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
			if _, err := s.core.GetTarget(ctx, reservation.ResourceID); err != nil {
				return fmt.Errorf("%s reservation references missing target %s: %w", reservation.Namespace, reservation.ResourceID, err)
			}
		default:
			return fmt.Errorf("unsupported operation reservation namespace %q", reservation.Namespace)
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
