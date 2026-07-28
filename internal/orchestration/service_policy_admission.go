package orchestration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LeaseOperationPolicyAdmission is the narrow policy boundary used by the RPC
// service. The service supplies only an exact persisted policy/capability pair
// and normalized operation facts; publication lookup remains authority-owned.
type LeaseOperationPolicyAdmission interface {
	AdmitCapture(context.Context, string, string, policyauthority.CaptureAdmission) error
	AdmitExport(context.Context, string, string, policyauthority.ExportAdmission) error
}

func (s *Service) admitCaptureSpec(ctx context.Context, leaseID string, spec *worldv1.CaptureSpec) (leaseOperationScope, WorkspaceScope, error) {
	workspace, scope, err := s.resolveLeaseOperationScope(ctx, leaseID, true)
	if err != nil {
		return leaseOperationScope{}, WorkspaceScope{}, err
	}
	if err := s.policyAdmission.AdmitCapture(ctx, scope.PolicyDigest, scope.CapabilityDigest, captureAdmission(spec)); err != nil {
		return leaseOperationScope{}, WorkspaceScope{}, err
	}
	return scope, workspace, nil
}

func captureAdmission(spec *worldv1.CaptureSpec) policyauthority.CaptureAdmission {
	duration, err := nativeDuration(spec.Duration, "capture_spec.duration", true)
	if err != nil || spec.ByteLimit > math.MaxInt64 {
		return policyauthority.CaptureAdmission{}
	}
	return policyauthority.CaptureAdmission{
		Name: spec.Profile, SignalFamilies: append([]string(nil), spec.SignalFamilies...), Duration: duration, Bytes: int64(spec.ByteLimit),
		// The public CaptureSpec currently carries no verifiable process/path or
		// flow-filter fields. Filter-requiring policies therefore fail closed.
		HasProcessOrPathFilter: false,
		HasFlowFilter:          false,
	}
}

func (s *Service) admitExportDeclaration(ctx context.Context, leaseID string, paths []*worldv1.ExportPath) (leaseOperationScope, WorkspaceScope, error) {
	seen := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		seen[value.WorkspaceRelativePath] = struct{}{}
	}
	workspace, scope, err := s.resolveLeaseOperationScope(ctx, leaseID, true)
	if err != nil {
		return leaseOperationScope{}, WorkspaceScope{}, err
	}
	if err := s.policyAdmission.AdmitExport(ctx, scope.PolicyDigest, scope.CapabilityDigest, policyauthority.ExportAdmission{DeclarationAuthority: "host", FileCount: int64(len(seen))}); err != nil {
		return leaseOperationScope{}, WorkspaceScope{}, err
	}
	return scope, workspace, nil
}

func (s *Service) admitWorkspaceExport(ctx context.Context, scope leaseOperationScope, root string, paths []*worldv1.ExportPath) error {
	facts, err := inspectWorkspaceExport(root, paths)
	if err != nil {
		return err
	}
	return s.admitExportFacts(ctx, scope, facts)
}

func (s *Service) admitImmutableExport(ctx context.Context, scope leaseOperationScope, content map[string]ports.ContentSource) error {
	facts := policyauthority.ExportAdmission{
		DeclarationAuthority: "host", FileCount: int64(len(content)),
		FinalPublication: true, RetainsFullChangeManifest: true,
	}
	for path, source := range content {
		if source == nil || source.Size() < 0 {
			return status.Errorf(codes.DataLoss, "immutable export content %q has invalid size authority", path)
		}
		if facts.Bytes > math.MaxInt64-source.Size() {
			return status.Error(codes.ResourceExhausted, "immutable export byte total overflows")
		}
		facts.Bytes += source.Size()
	}
	return s.admitExportFacts(ctx, scope, facts)
}

func (s *Service) admitExportFacts(ctx context.Context, scope leaseOperationScope, facts policyauthority.ExportAdmission) error {
	if err := scope.validate(); err != nil {
		return err
	}
	return s.policyAdmission.AdmitExport(ctx, scope.PolicyDigest, scope.CapabilityDigest, facts)
}

func (s *Service) resolveLeaseOperationScope(ctx context.Context, leaseID string, requireActive bool) (WorkspaceScope, leaseOperationScope, error) {
	if s.policyAdmission == nil {
		return WorkspaceScope{}, leaseOperationScope{}, status.Error(codes.FailedPrecondition, "effective-policy capture/export admission is not configured")
	}
	view, err := s.core.GetResearchSessionByLease(ctx, leaseID)
	if err != nil {
		return WorkspaceScope{}, leaseOperationScope{}, err
	}
	if requireActive && (view.Session.State != domain.ResearchSessionLeased || view.Lease.State != domain.LeaseActive || !view.Lease.Termination.Empty() || !view.Lease.ExpiresAt.After(s.clock())) {
		return WorkspaceScope{}, leaseOperationScope{}, domain.NewError(domain.CodeFailedPrecondition, "orchestration.resolve_lease_operation_scope", "lease", "must be active, unexpired, and not terminating", nil)
	}
	policyDigest, capabilityDigest, err := persistedResearchSessionPolicyPair(view, leaseID)
	if err != nil {
		return WorkspaceScope{}, leaseOperationScope{}, err
	}
	workspace, err := workspaceScopeFromView(view)
	if err != nil {
		return WorkspaceScope{}, leaseOperationScope{}, err
	}
	return workspace, bindLeaseOperationScope(policyDigest, capabilityDigest, workspace), nil
}

func persistedResearchSessionPolicyPair(view application.ResearchSessionView, leaseID string) (string, string, error) {
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_policy_pair", "agent_generation", "current generation is missing", err)
	}
	if view.Lease.ID != leaseID || view.Session.LeaseID != view.Lease.ID || view.Session.AgentWorkspaceID != view.Agent.ID ||
		view.Lease.SessionID != view.Session.ID || view.Lease.AgentWorkspaceID != view.Agent.ID || view.Agent.SessionID != view.Session.ID ||
		view.Lease.AgentGeneration != generation.Generation || view.Agent.CurrentGeneration != generation.Generation {
		return "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_policy_pair", "scope", "session, lease, and agent generation do not identify one persisted scope", nil)
	}
	if view.Session.InputViewID != view.Lease.InputViewID || view.Session.InputViewID != generation.InputViewID ||
		view.Session.PolicyDigest != view.Lease.PolicyDigest || view.Session.PolicyDigest != generation.PolicyDigest ||
		view.Session.CapabilityDigest != view.Lease.CapabilityDigest || view.Session.CapabilityDigest != generation.CapabilityDigest {
		return "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_policy_pair", "provenance", "session, lease, and agent generation policy provenance differs", nil)
	}
	policyDigest, err := domain.ParseDigest(view.Lease.PolicyDigest)
	if err != nil || policyDigest.String() != strings.TrimSpace(view.Lease.PolicyDigest) {
		return "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_policy_pair", "policy_digest", "persisted policy digest is not canonical", err)
	}
	capabilityDigest, err := domain.ParseDigest(view.Lease.CapabilityDigest)
	if err != nil || capabilityDigest.String() != strings.TrimSpace(view.Lease.CapabilityDigest) {
		return "", "", domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_policy_pair", "capability_digest", "persisted capability digest is not canonical", err)
	}
	return policyDigest.String(), capabilityDigest.String(), nil
}

func bindLeaseOperationScope(policyDigest, capabilityDigest string, workspace WorkspaceScope) leaseOperationScope {
	return leaseOperationScope{
		PolicyDigest: policyDigest, CapabilityDigest: capabilityDigest,
		WorkspaceID: workspace.WorkspaceID.String(), AgentWorkspaceID: workspace.AgentWorkspaceID.String(),
		AgentGeneration: uint64(workspace.AgentGeneration),
	}
}

func (s leaseOperationScope) validate() error {
	if err := requireCanonicalNonzeroDigest(s.PolicyDigest); err != nil {
		return err
	}
	if err := requireCanonicalNonzeroDigest(s.CapabilityDigest); err != nil {
		return err
	}
	if _, err := domain.ParseWorkspaceID(s.WorkspaceID); err != nil {
		return err
	}
	if _, err := domain.ParseAgentWorkspaceID(s.AgentWorkspaceID); err != nil {
		return err
	}
	if !domain.AgentGeneration(s.AgentGeneration).IsValid() {
		return fmt.Errorf("agent generation is invalid")
	}
	return nil
}

func requireCanonicalNonzeroDigest(value string) error {
	digest, err := domain.ParseDigest(value)
	if err != nil {
		return err
	}
	if digest.IsZero() || digest.String() != value {
		return fmt.Errorf("digest is zero or non-canonical")
	}
	return nil
}

func (s leaseOperationScope) matches(workspace WorkspaceScope) bool {
	return s.WorkspaceID == workspace.WorkspaceID.String() && s.AgentWorkspaceID == workspace.AgentWorkspaceID.String() &&
		s.AgentGeneration == uint64(workspace.AgentGeneration)
}

func (s *Service) requirePersistedLeaseOperationScope(ctx context.Context, leaseID string, workspace WorkspaceScope, persisted leaseOperationScope) error {
	currentWorkspace, current, err := s.resolveLeaseOperationScope(ctx, leaseID, false)
	if err != nil {
		return err
	}
	return requireMatchingLeaseOperationScope(persisted, current, workspace, currentWorkspace)
}

func requireMatchingLeaseOperationScope(persisted, current leaseOperationScope, workspaces ...WorkspaceScope) error {
	if err := persisted.validate(); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_operation_scope", "persisted_scope", "is invalid", err)
	}
	if persisted != current {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_operation_scope", "policy", "persisted operation no longer matches the lease policy or agent generation", nil)
	}
	for _, workspace := range workspaces {
		if !persisted.matches(workspace) {
			return domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_lease_operation_scope", "workspace", "persisted operation no longer matches the agent workspace generation", nil)
		}
	}
	return nil
}

func inspectWorkspaceExport(root string, paths []*worldv1.ExportPath) (policyauthority.ExportAdmission, error) {
	if strings.TrimSpace(root) == "" {
		return policyauthority.ExportAdmission{}, status.Error(codes.DataLoss, "workspace driver returned no merged export path")
	}
	seen := make(map[string]struct{}, len(paths))
	facts := policyauthority.ExportAdmission{DeclarationAuthority: "host"}
	for _, value := range paths {
		path := value.WorkspaceRelativePath
		if _, found := seen[path]; found {
			continue
		}
		seen[path] = struct{}{}
		facts.FileCount++
		file, err := safepath.OpenRegular(root, path)
		if errors.Is(err, safepath.ErrNotRegular) || errors.Is(err, safepath.ErrUnsafe) {
			facts.ContainsNonRegular = true
			continue
		}
		if err != nil {
			return policyauthority.ExportAdmission{}, fmt.Errorf("inspect declared export %q: %w", path, err)
		}
		size := file.Size()
		closeErr := file.Close()
		if closeErr != nil {
			return policyauthority.ExportAdmission{}, fmt.Errorf("close declared export %q: %w", path, closeErr)
		}
		if size < 0 || facts.Bytes > math.MaxInt64-size {
			return policyauthority.ExportAdmission{}, status.Error(codes.ResourceExhausted, "declared export byte total overflows")
		}
		facts.Bytes += size
	}
	return facts, nil
}
