package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	captureStateStarting  = "starting"
	captureStateActive    = "active"
	captureStateFailed    = "failed"
	captureStateCompleted = string(domain.CaptureCompleted)
	exportStateDeclared   = "declared"
	exportStateCommitting = "committing"
	exportStateCommitted  = "committed"
	changeManifestRole    = "workspace-change-manifest"
)

func (s *Service) StartCapture(ctx context.Context, request *worldv1.StartCaptureRequest) (*worldv1.Capture, error) {
	if request == nil || request.CaptureSpec == nil {
		return nil, status.Error(codes.InvalidArgument, "capture_spec is required")
	}
	return s.startCapture(ctx, request.Mutation, request.LeaseId, cloneCaptureSpec(request.CaptureSpec))
}

func (s *Service) RequestCapture(ctx context.Context, request *worldv1.RequestCaptureRequest) (*worldv1.Capture, error) {
	if request == nil || strings.TrimSpace(request.NamedProfile) == "" {
		return nil, status.Error(codes.InvalidArgument, "named_profile is required")
	}
	profile, found := s.profiles[request.NamedProfile]
	if !found {
		return nil, status.Errorf(codes.NotFound, "capture profile %q is not configured", request.NamedProfile)
	}
	profile = cloneCaptureSpec(profile)
	profile.Profile = request.NamedProfile
	return s.startCapture(ctx, request.Mutation, request.LeaseId, profile)
}

func (s *Service) startCapture(ctx context.Context, mutation *worldv1.MutationMetadata, leaseID string, spec *worldv1.CaptureSpec) (*worldv1.Capture, error) {
	if s.captures == nil {
		return nil, status.Error(codes.FailedPrecondition, "capture collection is unavailable because no production capture controller is configured")
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{LeaseID: leaseID}); err != nil {
		return nil, err
	}
	if err := s.validateCaptureSpec(spec); err != nil {
		return nil, err
	}
	signature, err := requestSignature(struct {
		LeaseID string               `json:"lease_id"`
		Spec    *worldv1.CaptureSpec `json:"spec"`
		Policy  string               `json:"policy"`
	}{leaseID, spec, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	existing, found, err := s.existingCaptureLocked("start_capture", meta.IdempotencyKey, signature)
	if found {
		s.mu.Unlock()
		return s.replayOrResumeCapture(operationCtx, meta, existing, signature, err)
	}
	s.mu.Unlock()
	operationScope, workspace, err := s.admitCaptureSpec(operationCtx, leaseID, spec)
	if err != nil {
		return nil, err
	}
	if err := requireWritableWorkspaceScope(workspace); err != nil {
		return nil, err
	}
	s.mu.Lock()
	existing, found, err = s.existingCaptureLocked("start_capture", meta.IdempotencyKey, signature)
	if found {
		s.mu.Unlock()
		return s.replayOrResumeCapture(operationCtx, meta, existing, signature, err)
	}
	if s.leaseHasSealingExportLocked(leaseID) {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "capture cannot start after export sealing has begun")
	}
	captureID, generateErr := s.ids.CaptureID()
	if generateErr != nil {
		s.mu.Unlock()
		return nil, generateErr
	}
	record := captureRecord{
		Capture: &worldv1.Capture{CaptureId: captureID.String(), LeaseId: leaseID, Profile: spec.Profile, State: captureStateStarting, Revision: 1, StartedAt: protobufTimestamp(s.clock().UTC())},
		Spec:    cloneCaptureSpec(spec), Scope: operationScope,
	}
	event := stateEvent{Kind: "capture.upserted", Namespace: "start_capture", IdempotencyKey: meta.IdempotencyKey, Signature: signature, Capture: &record}
	if err := s.persistStateLocked(operationCtx, event, ledger.Identity{LeaseID: leaseID}); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	return s.resumeCapture(operationCtx, meta, record, signature)
}

func (s *Service) existingCaptureLocked(namespace, key, signature string) (captureRecord, bool, error) {
	resourceID, found, err := s.existingIdempotencyLocked(namespace, key, signature)
	if !found || err != nil {
		return captureRecord{}, found, err
	}
	record, ok := s.captureState[resourceID]
	if !ok || record.Capture == nil {
		return captureRecord{}, true, status.Error(codes.DataLoss, "capture idempotency index references missing state")
	}
	return cloneCaptureRecord(record), true, nil
}

func (s *Service) replayOrResumeCapture(ctx context.Context, meta application.MutationMeta, record captureRecord, signature string, replayErr error) (*worldv1.Capture, error) {
	if replayErr != nil {
		return nil, replayErr
	}
	if record.Capture.State == captureStateActive || record.Capture.State == captureStateCompleted {
		return cloneCaptureRecord(record).Capture, nil
	}
	return s.resumeCapture(ctx, meta, record, signature)
}

func (s *Service) resumeCapture(ctx context.Context, meta application.MutationMeta, record captureRecord, signature string) (*worldv1.Capture, error) {
	workspace, currentScope, scopeErr := s.resolveLeaseOperationScope(ctx, record.Capture.LeaseId, true)
	if scopeErr == nil {
		scopeErr = requireWritableWorkspaceScope(workspace)
	}
	if scopeErr == nil {
		scopeErr = requireMatchingLeaseOperationScope(record.Scope, currentScope, workspace)
	}
	startedAt, timeErr := nativeTimestamp(record.Capture.StartedAt, "capture.started_at", true)
	plan := CapturePlan{IdempotencyKey: meta.IdempotencyKey, CaptureID: record.Capture.CaptureId, LeaseID: record.Capture.LeaseId, Workspace: workspace, Spec: cloneCaptureSpec(record.Spec), StartedAt: startedAt}
	startErr := scopeErr
	if startErr == nil {
		startErr = timeErr
	}
	if startErr == nil {
		startErr = s.policyAdmission.AdmitCapture(ctx, record.Scope.PolicyDigest, record.Scope.CapabilityDigest, captureAdmission(record.Spec))
	}
	if startErr == nil {
		startErr = s.captures.Start(ctx, plan)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.captureState[record.Capture.CaptureId]
	if !found {
		return nil, status.Error(codes.DataLoss, "capture state disappeared while starting")
	}
	current = cloneCaptureRecord(current)
	if current.Capture.State == captureStateActive || current.Capture.State == captureStateCompleted {
		return cloneCaptureRecord(current).Capture, nil
	}
	current.Capture.Revision++
	current.Capture.State = captureStateActive
	if startErr != nil {
		current.Capture.State = captureStateFailed
	}
	event := stateEvent{Kind: "capture.upserted", Namespace: "start_capture", IdempotencyKey: meta.IdempotencyKey, Signature: signature, Capture: &current}
	if err := s.persistStateLocked(ctx, event, ledger.Identity{LeaseID: current.Capture.LeaseId}); err != nil {
		return nil, errors.Join(startErr, err)
	}
	if startErr != nil {
		return nil, startErr
	}
	return cloneCaptureRecord(current).Capture, nil
}

func (s *Service) StopCapture(ctx context.Context, request *worldv1.StopCaptureRequest) (*worldv1.Capture, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if _, err := domain.ParseCaptureID(request.CaptureId); err != nil || request.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "valid lease_id and capture_id are required")
	}
	s.mu.RLock()
	record, found := s.captureState[request.CaptureId]
	record = cloneCaptureRecord(record)
	s.mu.RUnlock()
	if !found || record.Capture.LeaseId != request.LeaseId {
		return nil, status.Error(codes.PermissionDenied, "capture is outside the requested lease scope")
	}
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{LeaseID: request.LeaseId}); err != nil {
		return nil, err
	}
	return s.stopCaptureRecord(operationCtx, meta, record, request.ExpectedRevision, false)
}

// stopCaptureRecord is the single durable stop path for public requests and
// trusted lease termination. Termination may stop a Starting or Failed record
// because either state can hide an ambiguous physical Start boundary.
func (s *Service) stopCaptureRecord(ctx context.Context, meta application.MutationMeta, record captureRecord, expectedRevision uint64, termination bool) (*worldv1.Capture, error) {
	const namespace = "stop_capture"
	signature, err := requestSignature(struct {
		LeaseID   string `json:"lease_id"`
		CaptureID string `json:"capture_id"`
		Revision  uint64 `json:"revision"`
	}{record.Capture.LeaseId, record.Capture.CaptureId, expectedRevision})
	if err != nil {
		return nil, err
	}
	if record.Capture.State == captureStateCompleted {
		if err := s.requireReservedOperation(namespace, record.Capture.CaptureId, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return cloneCaptureRecord(record).Capture, nil
	}
	if record.Capture.State != captureStateActive && !termination {
		return nil, status.Errorf(codes.FailedPrecondition, "capture in %s cannot be stopped", record.Capture.State)
	}
	if record.Capture.State != captureStateActive && record.Capture.State != captureStateStarting && record.Capture.State != captureStateFailed {
		return nil, status.Errorf(codes.FailedPrecondition, "capture in %s cannot be stopped", record.Capture.State)
	}
	if record.Capture.Revision != expectedRevision {
		return nil, status.Errorf(codes.Aborted, "capture revision conflict: got %d, current %d", expectedRevision, record.Capture.Revision)
	}
	if err := s.reserveOperation(ctx, namespace, record.Capture.CaptureId, meta.IdempotencyKey, signature, ledger.Identity{LeaseID: record.Capture.LeaseId}); err != nil {
		return nil, err
	}
	artifacts, err := s.captures.Stop(ctx, CaptureStopPlan{IdempotencyKey: meta.IdempotencyKey, CaptureID: record.Capture.CaptureId})
	if err != nil {
		physicalAbsenceCompletesTermination := termination && domain.IsCode(err, domain.CodeNotFound) &&
			(record.Capture.State == captureStateStarting || record.Capture.State == captureStateFailed)
		if !physicalAbsenceCompletesTermination {
			return nil, err
		}
		artifacts = nil
	}
	record.Capture.State, record.Capture.Revision, record.Capture.StoppedAt = captureStateCompleted, record.Capture.Revision+1, protobufTimestamp(s.clock().UTC())
	for _, artifact := range artifacts {
		record.Capture.Artifacts = append(record.Capture.Artifacts, mapArtifact(artifact))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.captureState[record.Capture.CaptureId]; current.Capture.State == captureStateCompleted {
		return cloneCaptureRecord(current).Capture, nil
	}
	event := stateEvent{Kind: "capture.upserted", Namespace: namespace, IdempotencyKey: meta.IdempotencyKey, Signature: signature, Capture: &record}
	if err := s.persistStateLocked(ctx, event, ledger.Identity{LeaseID: record.Capture.LeaseId}); err != nil {
		return nil, err
	}
	return cloneCaptureRecord(record).Capture, nil
}

func (s *Service) stopLeaseCaptures(ctx context.Context, meta application.MutationMeta, leaseID string) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	records := make([]captureRecord, 0)
	for _, record := range s.captureState {
		if record.Capture != nil && record.Capture.LeaseId == leaseID && record.Capture.State != captureStateCompleted {
			records = append(records, cloneCaptureRecord(record))
		}
	}
	s.mu.RUnlock()
	if len(records) == 0 {
		return nil
	}
	if s.captures == nil {
		return status.Error(codes.FailedPrecondition, "capture state exists but no capture controller is configured")
	}
	var stopErrors []error
	for _, record := range records {
		captureMeta := s.captureTerminationMeta(meta, record.Capture.CaptureId)
		_, err := s.stopCaptureRecord(ctx, captureMeta, record, record.Capture.Revision, true)
		stopErrors = append(stopErrors, err)
	}
	return errors.Join(stopErrors...)
}

// captureTerminationMeta resumes a caller-owned stop reservation when the
// process crossed the physical Stop boundary but crashed before recording the
// terminal capture. Manufacturing a lease-reaper key in that case would
// conflict with the durable reservation and strand termination forever.
func (s *Service) captureTerminationMeta(parent application.MutationMeta, captureID string) application.MutationMeta {
	meta := childMeta(parent, "capture/"+captureID, parent.Deadline)
	s.mu.RLock()
	reservation, found := s.operations[operationIndex("stop_capture", captureID)]
	s.mu.RUnlock()
	if found {
		meta.IdempotencyKey = reservation.IdempotencyKey
	}
	return meta
}

func (s *Service) requireNoCommittingExports(leaseID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.exportState {
		if record.Export != nil && record.Export.LeaseId == leaseID && record.Export.State == exportStateCommitting {
			return status.Errorf(codes.FailedPrecondition, "export %s is still committing; lease termination will retry after it reaches a durable terminal state", record.Export.ExportId)
		}
	}
	return nil
}

func (s *Service) DeclareExport(ctx context.Context, request *worldv1.DeclareExportRequest) (*worldv1.Export, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{LeaseID: request.LeaseId}); err != nil {
		return nil, err
	}
	paths, err := normalizeExportPaths(request.Paths)
	if err != nil {
		return nil, err
	}
	signature, err := requestSignature(struct {
		LeaseID string                `json:"lease_id"`
		Paths   []*worldv1.ExportPath `json:"paths"`
		Policy  string                `json:"policy"`
	}{request.LeaseId, paths, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	existing, found, err := s.existingExportLocked("declare_export", meta.IdempotencyKey, signature)
	if found {
		s.mu.Unlock()
		return existing.Export, err
	}
	s.mu.Unlock()
	operationScope, scope, err := s.admitExportDeclaration(operationCtx, request.LeaseId, paths)
	if err != nil {
		return nil, err
	}
	if err := requireWritableWorkspaceScope(scope); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, found, err = s.existingExportLocked("declare_export", meta.IdempotencyKey, signature)
	if found {
		return existing.Export, err
	}
	if s.leaseHasSealingExportLocked(request.LeaseId) {
		return nil, status.Error(codes.FailedPrecondition, "exports cannot be declared after workspace sealing has begun")
	}
	exportID, err := s.ids.ExportID()
	if err != nil {
		return nil, err
	}
	record := exportRecord{
		Export: &worldv1.Export{ExportId: exportID.String(), LeaseId: request.LeaseId, Paths: paths, State: exportStateDeclared, Revision: 1},
		Scope:  operationScope,
	}
	event := stateEvent{Kind: "export.upserted", Namespace: "declare_export", IdempotencyKey: meta.IdempotencyKey, Signature: signature, Export: &record}
	if err := s.persistStateLocked(operationCtx, event, ledger.Identity{LeaseID: request.LeaseId}); err != nil {
		return nil, err
	}
	return cloneExportRecord(record).Export, nil
}

// existingExportLocked resolves a stable export replay without consulting the
// current writable state. The caller must hold s.mu.
func (s *Service) existingExportLocked(namespace, key, signature string) (exportRecord, bool, error) {
	resourceID, found, err := s.existingIdempotencyLocked(namespace, key, signature)
	if !found || err != nil {
		return exportRecord{}, found, err
	}
	record, ok := s.exportState[resourceID]
	if !ok || record.Export == nil {
		return exportRecord{}, true, status.Error(codes.DataLoss, "export idempotency index references missing state")
	}
	return cloneExportRecord(record), true, nil
}

func (s *Service) PreviewChangeSet(ctx context.Context, request *worldv1.PreviewChangeSetRequest) (*worldv1.ChangeSet, error) {
	if request == nil || request.LeaseId == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_id is required")
	}
	operationCtx, cancel := context.WithTimeout(ctx, s.controlTimeout)
	defer cancel()
	if err := s.authorize(operationCtx, "", application.AuthorizationRequest{LeaseID: request.LeaseId}); err != nil {
		return nil, err
	}
	scope, err := s.requireWorkspaceScope(operationCtx, request.LeaseId)
	if err != nil {
		return nil, err
	}
	preview, err := s.workspace.Preview(operationCtx, scope.WorkspaceID)
	if err != nil {
		return nil, err
	}
	if !preview.ChangeSet.WorkspaceRevision().IsValid() || preview.ObservedAt.IsZero() {
		return nil, status.Error(codes.DataLoss, "workspace driver returned an invalid preview")
	}
	return mapChangeSet(preview.ChangeSet), nil
}

func (s *Service) CommitExport(ctx context.Context, request *worldv1.CommitExportRequest) (*worldv1.Export, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if s.material == nil || s.agent == nil {
		return nil, status.Error(codes.FailedPrecondition, "export commit requires material and agent workspace authorities")
	}
	operationCtx, cancel, meta, err := mutationContext(ctx, request.Mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if _, err := domain.ParseExportID(request.ExportId); err != nil || request.ExpectedWorkspaceRevision == 0 {
		return nil, domain.NewError(domain.CodeInvalidArgument, "orchestration.commit_export", "request", "valid export_id and positive expected_workspace_revision are required", err)
	}
	if _, err := domain.ParseLeaseID(request.LeaseId); err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, "orchestration.commit_export", "lease_id", "is invalid", err)
	}
	s.mu.RLock()
	record, found := s.exportState[request.ExportId]
	record = cloneExportRecord(record)
	s.mu.RUnlock()
	if !found || record.Export.LeaseId != request.LeaseId {
		return nil, status.Error(codes.PermissionDenied, "export is outside the requested lease scope")
	}
	if err := s.authorize(operationCtx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{LeaseID: request.LeaseId}); err != nil {
		return nil, err
	}
	const namespace = "commit_export"
	signature, err := requestSignature(struct {
		LeaseID  string `json:"lease_id"`
		ExportID string `json:"export_id"`
		Revision uint64 `json:"revision"`
		Policy   string `json:"policy"`
	}{request.LeaseId, request.ExportId, request.ExpectedWorkspaceRevision, meta.AuthorizedPolicyReference})
	if err != nil {
		return nil, err
	}
	if record.Export.State == exportStateCommitted {
		if err := s.requireReservedOperation(namespace, request.ExportId, meta.IdempotencyKey, signature); err != nil {
			return nil, err
		}
		return cloneExportRecord(record).Export, nil
	}
	scope, currentScope, err := s.resolveLeaseOperationScope(operationCtx, request.LeaseId, true)
	if err != nil {
		return nil, err
	}
	if err := requireMatchingLeaseOperationScope(record.Scope, currentScope, scope); err != nil {
		return nil, err
	}
	if record.Export.State == exportStateDeclared {
		handle, inspectErr := s.workspace.Inspect(operationCtx, scope.WorkspaceID)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if handle.WorkspaceID != scope.WorkspaceID || strings.TrimSpace(handle.MergedPath) == "" || !filepath.IsAbs(handle.MergedPath) {
			return nil, status.Error(codes.DataLoss, "workspace inspection returned an invalid export path")
		}
		if err := s.admitWorkspaceExport(operationCtx, record.Scope, handle.MergedPath, record.Export.Paths); err != nil {
			return nil, err
		}
	}
	record, err = s.beginExportCommit(operationCtx, namespace, record, scope, meta.IdempotencyKey, signature, request.ExpectedWorkspaceRevision)
	if err != nil {
		return nil, err
	}
	return s.resumeExportCommit(operationCtx, meta, record, scope, meta.IdempotencyKey, signature)
}

func (s *Service) resumeExportCommit(ctx context.Context, meta application.MutationMeta, record exportRecord, scope WorkspaceScope, key, signature string) (*worldv1.Export, error) {
	if err := s.requirePersistedLeaseOperationScope(ctx, record.Export.LeaseId, scope, record.Scope); err != nil {
		return nil, err
	}
	leaseID, err := domain.ParseLeaseID(record.Export.LeaseId)
	if err != nil {
		return nil, status.Error(codes.DataLoss, "persisted export has an invalid lease identity")
	}
	workspaceRevision := record.Export.WorkspaceRevision
	if workspaceRevision == 0 {
		return nil, status.Error(codes.DataLoss, "committing export has no workspace revision")
	}
	sealed, err := s.sealExportWorkspace(ctx, meta, record.Export.LeaseId, scope, workspaceRevision)
	if err != nil {
		return nil, err
	}
	selections, content, err := s.exportContent(sealed.ImmutablePath, record.Export.Paths)
	if err != nil {
		return nil, err
	}
	selections, content, err = appendFullChangeManifest(selections, content, sealed.ChangeSet, s.maxTransferBytes)
	if err != nil {
		return nil, err
	}
	if err := s.admitImmutableExport(ctx, record.Scope, content); err != nil {
		return nil, err
	}
	verified, err := s.workspace.Inspect(ctx, scope.WorkspaceID)
	if err != nil {
		return nil, err
	}
	if verified.WorkspaceID != scope.WorkspaceID || verified.State != domain.WorkspaceSealed {
		return nil, status.Error(codes.DataLoss, "workspace did not remain sealed while immutable export bytes were staged")
	}
	artifacts, err := s.material.CaptureOutputs(ctx, ports.OutputPlan{
		IdempotencyKey: childMeta(meta, "publish", deadline(ctx)).IdempotencyKey,
		LeaseID:        leaseID, WorkspaceID: scope.WorkspaceID,
		AgentWorkspaceID: scope.AgentWorkspaceID, AgentGeneration: scope.AgentGeneration,
		Selections: selections, Content: content,
		Provenance: map[string]string{"world.export_id": record.Export.ExportId, "world.workspace_revision": fmt.Sprint(workspaceRevision)},
	})
	if err != nil {
		return nil, err
	}
	if err := validatePublishedOutputs(selections, content, artifacts); err != nil {
		return nil, err
	}
	return s.completeExportCommit(ctx, "commit_export", record, scope, key, signature, artifacts)
}

// resumeLeaseExports completes only commits whose irreversible reservation was
// durably established before the lease gate closed. Declared exports are not
// promoted during termination.
func (s *Service) resumeLeaseExports(ctx context.Context, meta application.MutationMeta, view application.ResearchSessionView) error {
	if s == nil {
		return nil
	}
	scope, err := workspaceScopeFromView(view)
	if err != nil {
		return err
	}
	type pendingExport struct {
		record      exportRecord
		reservation operationReservation
	}
	s.mu.RLock()
	pending := make([]pendingExport, 0)
	for _, record := range s.exportState {
		if record.Export == nil || record.Export.LeaseId != view.Lease.ID || record.Export.State != exportStateCommitting {
			continue
		}
		reservation, found := s.operations[operationIndex("commit_export", record.Export.ExportId)]
		if !found {
			s.mu.RUnlock()
			return status.Errorf(codes.DataLoss, "committing export %s has no durable reservation", record.Export.ExportId)
		}
		pending = append(pending, pendingExport{record: cloneExportRecord(record), reservation: reservation})
	}
	s.mu.RUnlock()
	if len(pending) == 0 {
		return nil
	}
	if s.material == nil || s.agent == nil || s.workspace == nil {
		return status.Error(codes.FailedPrecondition, "committing export cannot be resumed without material, agent, and workspace authorities")
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].record.Export.ExportId < pending[j].record.Export.ExportId })
	var resumeErrors []error
	for _, item := range pending {
		exportMeta := childMeta(meta, "export/"+item.record.Export.ExportId, meta.Deadline)
		exportMeta.IdempotencyKey = item.reservation.IdempotencyKey
		_, err := s.resumeExportCommit(ctx, exportMeta, item.record, scope, item.reservation.IdempotencyKey, item.reservation.Signature)
		resumeErrors = append(resumeErrors, err)
	}
	return errors.Join(resumeErrors...)
}

func (s *Service) beginExportCommit(ctx context.Context, namespace string, record exportRecord, scope WorkspaceScope, key, signature string, workspaceRevision uint64) (exportRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.exportState[record.Export.ExportId]
	if !found || current.Export == nil || current.Export.LeaseId != record.Export.LeaseId {
		return exportRecord{}, status.Error(codes.DataLoss, "export state disappeared while reserving commit")
	}
	current = cloneExportRecord(current)
	reserved, err := s.requireOperationReservationLocked(namespace, current.Export.ExportId, key, signature, 0)
	if err != nil {
		return exportRecord{}, err
	}
	switch current.Export.State {
	case exportStateDeclared:
		if reserved {
			return exportRecord{}, status.Error(codes.DataLoss, "export reservation exists without its committing state")
		}
		for _, capture := range s.captureState {
			if capture.Capture != nil && capture.Capture.LeaseId == current.Export.LeaseId && capture.Capture.State != captureStateCompleted && capture.Capture.State != captureStateFailed {
				return exportRecord{}, status.Errorf(codes.FailedPrecondition, "capture %s must be stopped before export commit", capture.Capture.CaptureId)
			}
		}
		current.Export.State = exportStateCommitting
		current.Export.Revision++
		current.Export.WorkspaceRevision = workspaceRevision
		reservation := operationReservation{Namespace: namespace, ResourceID: current.Export.ExportId, IdempotencyKey: key, Signature: signature}
		event := stateEvent{Kind: "export.upserted", Namespace: namespace, IdempotencyKey: key, Signature: signature, Export: &current, Operation: &reservation}
		identity := ledger.Identity{LeaseID: current.Export.LeaseId, AgentWorkspaceID: scope.AgentWorkspaceID.String(), AgentGeneration: uint64(scope.AgentGeneration)}
		if err := s.persistStateLocked(ctx, event, identity); err != nil {
			return exportRecord{}, err
		}
	case exportStateCommitting:
		if !reserved || current.Export.WorkspaceRevision != workspaceRevision {
			return exportRecord{}, status.Error(codes.DataLoss, "committing export does not match its durable reservation")
		}
	case exportStateCommitted:
		if !reserved {
			return exportRecord{}, status.Error(codes.DataLoss, "committed export has no durable reservation")
		}
	default:
		return exportRecord{}, status.Errorf(codes.FailedPrecondition, "export in %s cannot be committed", current.Export.State)
	}
	return cloneExportRecord(current), nil
}

func (s *Service) sealExportWorkspace(ctx context.Context, meta application.MutationMeta, leaseID string, scope WorkspaceScope, workspaceRevision uint64) (ports.WorkspaceSealResult, error) {
	view, err := s.core.GetResearchSessionByLease(ctx, leaseID)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil || view.Lease.ID != leaseID || view.Agent.ID != scope.AgentWorkspaceID.String() || view.Agent.CurrentGeneration != uint64(scope.AgentGeneration) || generation.WorkspaceID != scope.WorkspaceID.String() {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.seal_export_workspace", "agent_workspace", "lease, agent generation, and workspace projections disagree", err)
	}
	ref := ports.AgentWorkspaceRef{ID: scope.AgentWorkspaceID, Generation: scope.AgentGeneration}
	if generation.State == domain.AgentGenerationReady || generation.State == domain.AgentGenerationRunning {
		agent, transitionErr := s.core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{
			Meta: childMeta(meta, "export-quiesce", deadline(ctx)), AgentWorkspaceID: view.Agent.ID,
			Generation: generation.Generation, ExpectedRevision: generation.Revision, State: domain.AgentGenerationQuiescing,
		})
		if transitionErr != nil {
			return ports.WorkspaceSealResult{}, transitionErr
		}
		generation, err = currentAgentGeneration(agent)
		if err != nil {
			return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.seal_export_workspace", "agent_generation", "quiesce transition lost the current generation", err)
		}
	}
	if generation.State == domain.AgentGenerationQuiescing {
		if err := s.agent.Stop(ctx, ref, ports.StopGraceful); err != nil {
			return ports.WorkspaceSealResult{}, err
		}
		statusValue, inspectErr := s.agent.Inspect(ctx, ref)
		if inspectErr != nil {
			return ports.WorkspaceSealResult{}, inspectErr
		}
		if statusValue.AgentWorkspaceID != ref.ID || statusValue.Generation != ref.Generation || statusValue.Ready || (statusValue.State != domain.AgentGenerationQuiescing && statusValue.State != domain.AgentGenerationSealed) {
			return ports.WorkspaceSealResult{}, status.Error(codes.DataLoss, "agent driver did not prove the workspace generation stopped")
		}
	} else if generation.State != domain.AgentGenerationSealed {
		return ports.WorkspaceSealResult{}, status.Errorf(codes.FailedPrecondition, "agent workspace generation in %s cannot be sealed for export", generation.State)
	}
	sealed, err := s.workspace.Seal(ctx, scope.WorkspaceID, domain.Revision(workspaceRevision))
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if uint64(sealed.ChangeSet.WorkspaceRevision()) != workspaceRevision || sealed.SealedAt.IsZero() || strings.TrimSpace(sealed.ImmutablePath) == "" || !filepath.IsAbs(sealed.ImmutablePath) {
		return ports.WorkspaceSealResult{}, status.Error(codes.DataLoss, "workspace seal result has an invalid revision or immutable snapshot path")
	}
	if generation.State == domain.AgentGenerationQuiescing {
		agent, transitionErr := s.core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{
			Meta: childMeta(meta, "export-sealed", deadline(ctx)), AgentWorkspaceID: view.Agent.ID,
			Generation: generation.Generation, ExpectedRevision: generation.Revision, State: domain.AgentGenerationSealed,
		})
		if transitionErr != nil {
			return ports.WorkspaceSealResult{}, transitionErr
		}
		committedGeneration, generationErr := currentAgentGeneration(agent)
		if generationErr != nil || committedGeneration.State != domain.AgentGenerationSealed {
			return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.seal_export_workspace", "agent_generation", "sealed transition returned an invalid projection", generationErr)
		}
	}
	return sealed, nil
}

func (s *Service) completeExportCommit(ctx context.Context, namespace string, record exportRecord, scope WorkspaceScope, key, signature string, artifacts []domain.ArtifactReference) (*worldv1.Export, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.exportState[record.Export.ExportId]
	if !found || current.Export == nil {
		return nil, status.Error(codes.DataLoss, "export state disappeared while completing commit")
	}
	current = cloneExportRecord(current)
	reserved, err := s.requireOperationReservationLocked(namespace, current.Export.ExportId, key, signature, 0)
	if err != nil {
		return nil, err
	}
	if !reserved {
		return nil, status.Error(codes.DataLoss, "export completion has no durable reservation")
	}
	if current.Export.State == exportStateCommitted {
		return cloneExportRecord(current).Export, nil
	}
	if current.Export.State != exportStateCommitting || current.Export.WorkspaceRevision != record.Export.WorkspaceRevision {
		return nil, status.Error(codes.Aborted, "export state changed while publishing immutable outputs")
	}
	current.Export.State = exportStateCommitted
	current.Export.Revision++
	current.Export.Artifacts = nil
	current.Export.OccurrenceRefs = nil
	for _, artifact := range artifacts {
		mapped := mapArtifact(artifact)
		current.Export.Artifacts = append(current.Export.Artifacts, mapped)
		current.Export.OccurrenceRefs = append(current.Export.OccurrenceRefs, mapped.Reference)
	}
	sort.Strings(current.Export.OccurrenceRefs)
	event := stateEvent{Kind: "export.upserted", Namespace: namespace, IdempotencyKey: key, Signature: signature, Export: &current}
	identity := ledger.Identity{LeaseID: current.Export.LeaseId, AgentWorkspaceID: scope.AgentWorkspaceID.String(), AgentGeneration: uint64(scope.AgentGeneration)}
	if err := s.persistStateLocked(ctx, event, identity); err != nil {
		return nil, err
	}
	return cloneExportRecord(current).Export, nil
}

func (s *Service) leaseHasSealingExportLocked(leaseID string) bool {
	for _, record := range s.exportState {
		if record.Export != nil && record.Export.LeaseId == leaseID && (record.Export.State == exportStateCommitting || record.Export.State == exportStateCommitted) {
			return true
		}
	}
	return false
}

func validatePublishedOutputs(selections []domain.ExportSelection, content map[string]ports.ContentSource, artifacts []domain.ArtifactReference) error {
	expected := make(map[string]int)
	total := 0
	for _, selection := range selections {
		spec := selection.Spec()
		source := content[spec.RelativePath]
		if source == nil {
			return status.Error(codes.DataLoss, "export selection lost its immutable content source")
		}
		for _, role := range spec.Roles {
			expected[artifactIdentity(role, source.Digest(), source.Size())]++
			total++
		}
	}
	if len(artifacts) != total {
		return status.Errorf(codes.DataLoss, "material authority returned %d artifacts for %d selected output roles", len(artifacts), total)
	}
	seenReferences := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		spec := artifact.Spec()
		if _, duplicate := seenReferences[spec.Reference]; duplicate {
			return status.Error(codes.DataLoss, "material authority returned a duplicate artifact reference")
		}
		seenReferences[spec.Reference] = struct{}{}
		identity := artifactIdentity(spec.Role, spec.Digest, spec.Size)
		if expected[identity] == 0 {
			return status.Error(codes.DataLoss, "material authority returned an artifact outside the selected immutable outputs")
		}
		expected[identity]--
	}
	return nil
}

func artifactIdentity(role string, digest domain.Digest, size int64) string {
	return role + "\x00" + digest.String() + "\x00" + fmt.Sprint(size)
}

func (s *Service) validateCaptureSpec(spec *worldv1.CaptureSpec) error {
	if spec == nil {
		return status.Error(codes.InvalidArgument, "capture specification is required")
	}
	duration, err := nativeDuration(spec.Duration, "capture_spec.duration", true)
	if err != nil || strings.TrimSpace(spec.Profile) == "" || duration <= 0 || spec.ByteLimit == 0 || spec.ByteLimit > uint64(s.maxTransferBytes) {
		return status.Error(codes.InvalidArgument, "capture profile, positive duration, and byte_limit within the service bound are required")
	}
	if len(spec.SignalFamilies) == 0 || len(spec.SignalFamilies) > maxFilterValues {
		return status.Error(codes.InvalidArgument, "capture requires a bounded non-empty signal_families list")
	}
	seen := make(map[string]struct{}, len(spec.SignalFamilies))
	for _, family := range spec.SignalFamilies {
		if strings.TrimSpace(family) == "" || len(family) > 256 {
			return status.Error(codes.InvalidArgument, "capture signal families must be non-blank and at most 256 bytes")
		}
		if _, duplicate := seen[family]; duplicate {
			return status.Error(codes.InvalidArgument, "capture signal families must not contain duplicates")
		}
		seen[family] = struct{}{}
	}
	return nil
}

func normalizeExportPaths(values []*worldv1.ExportPath) ([]*worldv1.ExportPath, error) {
	if len(values) == 0 || len(values) > maxFilterValues {
		return nil, status.Errorf(codes.InvalidArgument, "export paths must contain between 1 and %d entries", maxFilterValues)
	}
	result := make([]*worldv1.ExportPath, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value == nil {
			return nil, status.Errorf(codes.InvalidArgument, "export path %d is required", index)
		}
		path, err := safepath.Normalize(value.WorkspaceRelativePath)
		if err != nil || strings.TrimSpace(value.Role) == "" || len(value.Role) > 256 {
			return nil, status.Errorf(codes.InvalidArgument, "export path %d is unsafe or has an invalid role", index)
		}
		key := path + "\x00" + value.Role
		if _, duplicate := seen[key]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "export paths contain a duplicate path/role pair")
		}
		seen[key] = struct{}{}
		result[index] = &worldv1.ExportPath{WorkspaceRelativePath: path, Role: value.Role}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].WorkspaceRelativePath < result[j].WorkspaceRelativePath || result[i].WorkspaceRelativePath == result[j].WorkspaceRelativePath && result[i].Role < result[j].Role
	})
	return result, nil
}

func (s *Service) exportContent(root string, paths []*worldv1.ExportPath) ([]domain.ExportSelection, map[string]ports.ContentSource, error) {
	rolesByPath := make(map[string][]string)
	for _, value := range paths {
		if value == nil {
			return nil, nil, status.Error(codes.DataLoss, "persisted export contains a nil path")
		}
		rolesByPath[value.WorkspaceRelativePath] = append(rolesByPath[value.WorkspaceRelativePath], value.Role)
	}
	pathNames := make([]string, 0, len(rolesByPath))
	for path := range rolesByPath {
		pathNames = append(pathNames, path)
	}
	sort.Strings(pathNames)
	selections := make([]domain.ExportSelection, 0, len(pathNames))
	content := make(map[string]ports.ContentSource, len(pathNames))
	var total int64
	for _, path := range pathNames {
		selection, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: path, Roles: rolesByPath[path]})
		if err != nil {
			return nil, nil, err
		}
		source, err := newWorkspaceContentSource(root, path, s.maxTransferBytes-total)
		if err != nil {
			return nil, nil, err
		}
		total += source.Size()
		selections = append(selections, selection)
		content[path] = source
	}
	return selections, content, nil
}

type immutableContentSource struct {
	content []byte
	digest  domain.Digest
}

func newWorkspaceContentSource(root, path string, maxBytes int64) (*immutableContentSource, error) {
	if maxBytes <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "export content exceeds aggregate byte limit")
	}
	file, err := safepath.OpenRegular(root, path)
	if err != nil {
		return nil, err
	}
	if file.Size() > maxBytes {
		_ = file.Close()
		return nil, status.Error(codes.ResourceExhausted, "export content exceeds aggregate byte limit")
	}
	before := file.Info()
	var content bytes.Buffer
	digestWriter := sha256.New()
	size, err := safepath.CopyBounded(io.MultiWriter(&content, digestWriter), file, maxBytes)
	after, statErr := file.Stat()
	closeErr := file.Close()
	if err != nil || statErr != nil || closeErr != nil {
		return nil, errors.Join(err, statErr, closeErr)
	}
	if before.Size() != after.Size() || before.ModTime() != after.ModTime() || before.Mode() != after.Mode() || size != before.Size() {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "workspace_content.stage", "file", "file changed while immutable bytes were staged", nil)
	}
	digest, err := domain.ParseDigest(fmt.Sprintf("sha256:%x", digestWriter.Sum(nil)))
	if err != nil {
		return nil, err
	}
	return &immutableContentSource{content: bytes.Clone(content.Bytes()), digest: digest}, nil
}

func newImmutableContentSource(content []byte) *immutableContentSource {
	owned := bytes.Clone(content)
	return &immutableContentSource{content: owned, digest: domain.NewDigest(owned)}
}

func (s *immutableContentSource) Digest() domain.Digest { return s.digest }
func (s *immutableContentSource) Size() int64           { return int64(len(s.content)) }
func (s *immutableContentSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func appendFullChangeManifest(selections []domain.ExportSelection, content map[string]ports.ContentSource, changes domain.ChangeSet, maxBytes int64) ([]domain.ExportSelection, map[string]ports.ContentSource, error) {
	manifest, err := marshalFullChangeManifest(changes)
	if err != nil {
		return nil, nil, err
	}
	var total int64
	for path, source := range content {
		if source == nil || source.Size() < 0 || total > math.MaxInt64-source.Size() {
			return nil, nil, status.Errorf(codes.DataLoss, "immutable export content %q has invalid size authority", path)
		}
		total += source.Size()
	}
	if maxBytes <= 0 || total > maxBytes-int64(len(manifest)) {
		return nil, nil, status.Error(codes.ResourceExhausted, "export content and required change manifest exceed aggregate byte limit")
	}
	path := ".world/change-manifest.json"
	for suffix := 2; content[path] != nil; suffix++ {
		path = fmt.Sprintf(".world/change-manifest-%d.json", suffix)
	}
	selection, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: path, Roles: []string{changeManifestRole}})
	if err != nil {
		return nil, nil, err
	}
	resultSelections := append(append([]domain.ExportSelection(nil), selections...), selection)
	resultContent := make(map[string]ports.ContentSource, len(content)+1)
	for key, source := range content {
		resultContent[key] = source
	}
	resultContent[path] = newImmutableContentSource(manifest)
	return resultSelections, resultContent, nil
}

func marshalFullChangeManifest(changes domain.ChangeSet) ([]byte, error) {
	type manifestEntry struct {
		Kind         string            `json:"kind"`
		Path         string            `json:"path"`
		PreviousPath string            `json:"previous_path,omitempty"`
		BeforeDigest string            `json:"before_digest,omitempty"`
		AfterDigest  string            `json:"after_digest,omitempty"`
		Metadata     map[string]string `json:"metadata"`
	}
	entries := make([]manifestEntry, 0, len(changes.Entries()))
	for _, entry := range changes.Entries() {
		spec := entry.Spec()
		before, after := "", ""
		if !spec.BeforeDigest.IsZero() {
			before = spec.BeforeDigest.String()
		}
		if !spec.AfterDigest.IsZero() {
			after = spec.AfterDigest.String()
		}
		entries = append(entries, manifestEntry{
			Kind: string(spec.Kind), Path: spec.Path, PreviousPath: spec.PreviousPath,
			BeforeDigest: before, AfterDigest: after, Metadata: spec.Metadata,
		})
	}
	return json.Marshal(struct {
		SchemaVersion     int             `json:"schema_version"`
		Scope             string          `json:"scope"`
		WorkspaceRevision uint64          `json:"workspace_revision"`
		SealedAt          string          `json:"sealed_at"`
		Entries           []manifestEntry `json:"entries"`
	}{1, string(changes.Scope()), uint64(changes.WorkspaceRevision()), changes.SealedAt().UTC().Format(time.RFC3339Nano), entries})
}

var _ ports.ContentSource = (*immutableContentSource)(nil)
