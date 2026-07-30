package rpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Core captures every application.Core operation exposed through the public
// service, making transport tests independent from concrete persistence.
type Core interface {
	Authorize(context.Context, application.AuthorizationRequest) error
	AcquireResearchSession(context.Context, application.AcquireRequest) (application.ResearchSessionView, error)
	GetResearchSession(context.Context, string) (application.ResearchSessionView, error)
	RenewLease(context.Context, application.RenewLeaseRequest) (application.LeaseRecord, error)
	ReleaseResearchSession(context.Context, application.ReleaseResearchSessionRequest) (application.ReleaseOutcome, error)
	CreateTarget(context.Context, application.CreateTargetRequest) (application.TargetRecord, error)
	GetTarget(context.Context, string) (application.TargetRecord, error)
	StartTargetRun(context.Context, application.StartTargetRunRequest) (application.TargetRunRecord, error)
	ResetTarget(context.Context, application.ResetTargetRequest) (application.TargetRecord, error)
	TransitionAgentGeneration(context.Context, application.TransitionAgentRequest) (application.AgentWorkspaceRecord, error)
	TransitionTargetGeneration(context.Context, application.TransitionTargetGenerationRequest) (application.TargetRecord, error)
	TransitionTargetRun(context.Context, application.TransitionTargetRunRequest) (application.TargetRunRecord, error)
	CreateTargetOperation(context.Context, application.CreateTargetOperationRequest) (application.TargetOperationRecord, error)
	TransitionTargetOperation(context.Context, application.TransitionTargetOperationRequest) (application.TargetOperationRecord, error)
	CreateIncident(context.Context, application.CreateIncidentRequest) (application.IncidentRecord, error)
	GetIncident(context.Context, string) (application.IncidentRecord, error)
	TransitionIncident(context.Context, application.TransitionIncidentRequest) (application.IncidentRecord, error)
	RecoverIncident(context.Context, application.RecoverIncidentRequest) (application.RecoveryOutcome, error)
	GetExec(context.Context, string) (application.ExecRecord, error)
	CreateExec(context.Context, application.CreateExecRequest) (application.ExecRecord, error)
	TransitionExec(context.Context, application.TransitionExecRequest) (application.ExecRecord, error)
	FinalizeExec(context.Context, application.FinalizeExecRequest) (application.ExecRecord, error)
}

func (s *WorldServer) authorize(ctx context.Context, scope application.AuthorizationRequest, metadata *worldv1.MutationMetadata) error {
	identity, ok := IdentityFromContext(ctx)
	if !ok || strings.TrimSpace(identity.Subject) == "" {
		return status.Error(codes.Unauthenticated, "authenticated identity is unavailable")
	}
	scope.Subject = identity.Subject
	if metadata != nil {
		scope.PolicyReference = metadata.AuthorizedPolicyReference
	}
	if err := s.core.Authorize(ctx, scope); err != nil {
		return StatusError(err)
	}
	return nil
}

// ServerOptions configures the in-process WorldServer facade used by Manager.
// There is no gRPC Serve / Listen product entrypoint.
type ServerOptions struct {
	Capabilities        worldv1.WorldServiceServer
	TrustedNodeSubjects map[string]bool
	PollInterval        time.Duration
}

// WorldServer maps world.v1 DTOs onto application Core and orchestration
// capabilities. It is invoked in-process by world.Manager; it is not a host
// network control plane.
type WorldServer struct {
	worldv1.UnimplementedWorldServiceServer
	core         Core
	capabilities worldv1.WorldServiceServer
	pollInterval time.Duration
	trustedNodes map[string]bool
}

func NewWorldServer(core Core, options ServerOptions) (*WorldServer, error) {
	if core == nil {
		return nil, fmt.Errorf("application core is required")
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 25 * time.Millisecond
	}
	capabilities := options.Capabilities
	if capabilities == nil {
		capabilities = worldv1.UnimplementedWorldServiceServer{}
	}
	trustedNodes := make(map[string]bool, len(options.TrustedNodeSubjects))
	for subject, trusted := range options.TrustedNodeSubjects {
		if trusted && strings.TrimSpace(subject) != "" {
			trustedNodes[subject] = true
		}
	}
	return &WorldServer{core: core, capabilities: capabilities, pollInterval: options.PollInterval, trustedNodes: trustedNodes}, nil
}

func (s *WorldServer) authorizeNode(ctx context.Context) error {
	identity, ok := IdentityFromContext(ctx)
	if !ok || !s.trustedNodes[identity.Subject] {
		return status.Error(codes.PermissionDenied, "operation requires a configured trusted node identity")
	}
	return nil
}

func mutation(value *worldv1.MutationMetadata) (application.MutationMeta, error) {
	if value == nil {
		return application.MutationMeta{}, status.Error(codes.InvalidArgument, "mutation metadata is required")
	}
	if strings.TrimSpace(value.AuthorizedPolicyReference) == "" {
		return application.MutationMeta{}, status.Error(codes.InvalidArgument, "authorized_policy_reference is required")
	}
	deadline, err := nativeTimestamp(value.Deadline, "mutation.deadline", true)
	if err != nil {
		return application.MutationMeta{}, invalidArgument(err)
	}
	return application.MutationMeta{
		IdempotencyKey:            value.IdempotencyKey,
		CorrelationID:             value.CorrelationId,
		CausationID:               value.CausationId,
		AuthorizedPolicyReference: value.AuthorizedPolicyReference,
		Deadline:                  deadline,
	}, nil
}

func (s *WorldServer) AcquireResearchSession(ctx context.Context, request *worldv1.AcquireResearchSessionRequest) (*worldv1.AcquireResearchSessionResponse, error) {
	if request == nil || request.InputView == nil {
		return nil, status.Error(codes.InvalidArgument, "input_view is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if request.Mutation.AuthorizedPolicyReference != request.PolicyDigest {
		return nil, status.Error(codes.PermissionDenied, "authorized policy reference does not match requested policy")
	}
	identity, ok := IdentityFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "authenticated identity is unavailable")
	}
	selection := application.InputSelectionRequest{
		FrozenSelectionRef: request.InputView.FrozenSelectionRef,
		OccurrenceRefs:     append([]string(nil), request.InputView.ImmutableOccurrenceRefs...),
		AllowedSidecars:    append([]string(nil), request.InputView.AllowedSidecars...),
		SecurityScope:      request.InputView.CacheSecurityScope, RequireZeroCopy: request.InputView.RequireZeroCopy,
	}
	selection.PathMappings = make([]application.InputPathMappingRequest, len(request.InputView.PathMappings))
	if err := requireMessages(request.InputView.PathMappings, "input_view.path_mappings"); err != nil {
		return nil, invalidArgument(err)
	}
	for index, mapping := range request.InputView.PathMappings {
		selection.PathMappings[index] = application.InputPathMappingRequest{OccurrenceRef: mapping.OccurrenceRef, LogicalPath: mapping.WorkspaceRelativePath}
	}
	if request.InputView.ResolvedInputViewId == "" && selection.Empty() {
		return nil, status.Error(codes.InvalidArgument, "resolved input view or immutable input selection is required")
	}
	if request.InputView.ResolvedInputViewId != "" && !selection.Empty() {
		return nil, status.Error(codes.InvalidArgument, "resolved input view and unresolved input selection are mutually exclusive")
	}
	ttl, err := nativeDuration(request.Ttl, "ttl", true)
	if err != nil {
		return nil, invalidArgument(err)
	}
	view, err := s.core.AcquireResearchSession(ctx, application.AcquireRequest{
		Meta: meta, OwnerSubject: identity.Subject, InputViewID: request.InputView.ResolvedInputViewId, InputSelection: selection,
		PolicyDigest: request.PolicyDigest, CapabilityDigest: request.CapabilityDigest, TTL: ttl,
	})
	if err != nil {
		return nil, StatusError(err)
	}
	mapped := researchSessionView(view)
	return &worldv1.AcquireResearchSessionResponse{Lease: mapped.Lease, View: mapped}, nil
}

func (s *WorldServer) GetResearchSession(ctx context.Context, request *worldv1.GetResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{SessionID: request.ResearchSessionId}, nil); err != nil {
		return nil, err
	}
	view, err := s.core.GetResearchSession(ctx, request.ResearchSessionId)
	if err != nil {
		return nil, StatusError(err)
	}
	return researchSessionView(view), nil
}

func (s *WorldServer) WaitResearchSession(ctx context.Context, request *worldv1.WaitResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	if request == nil || !domain.ResearchSessionState(request.DesiredState).IsValid() {
		return nil, status.Error(codes.InvalidArgument, "valid desired_state is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{SessionID: request.ResearchSessionId}, nil); err != nil {
		return nil, err
	}
	return waitFor(ctx, s.pollInterval, func() (*worldv1.ResearchSessionView, bool, error) {
		view, err := s.core.GetResearchSession(ctx, request.ResearchSessionId)
		if err != nil {
			return nil, false, StatusError(err)
		}
		mapped := researchSessionView(view)
		return mapped, mapped.Session.State == request.DesiredState, nil
	})
}

func waitFor[Value any](ctx context.Context, interval time.Duration, load func() (*Value, bool, error)) (*Value, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		value, ready, err := load()
		if err != nil || ready {
			return value, err
		}
		select {
		case <-ctx.Done():
			return nil, StatusError(ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *WorldServer) RenewLease(ctx context.Context, request *worldv1.RenewLeaseRequest) (*worldv1.Lease, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	ttl, err := nativeDuration(request.Ttl, "ttl", true)
	if err != nil {
		return nil, invalidArgument(err)
	}
	result, err := s.core.RenewLease(ctx, application.RenewLeaseRequest{Meta: meta, LeaseID: request.LeaseId, ExpectedRevision: request.ExpectedRevision, TTL: ttl})
	if err != nil {
		return nil, StatusError(err)
	}
	return lease(result), nil
}

func (s *WorldServer) ReleaseResearchSession(ctx context.Context, request *worldv1.ReleaseResearchSessionRequest) (*worldv1.ReleaseOutcome, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.ReleaseResearchSession(ctx, application.ReleaseResearchSessionRequest{Meta: meta, LeaseID: request.LeaseId, ExpectedRevision: request.ExpectedRevision, Reason: request.Reason})
	if err != nil {
		return nil, StatusError(err)
	}
	return &worldv1.ReleaseOutcome{ResearchSessionId: result.SessionID, LeaseId: result.LeaseID, ReleasedAt: protobufTimestamp(result.ReleasedAt)}, nil
}

func (s *WorldServer) CreateTarget(ctx context.Context, request *worldv1.CreateTargetRequest) (*worldv1.Target, error) {
	if request == nil || request.Template == nil {
		return nil, status.Error(codes.InvalidArgument, "target template is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if request.Mutation.AuthorizedPolicyReference != request.Template.PolicyDigest {
		return nil, status.Error(codes.PermissionDenied, "authorized policy reference does not match target policy")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.CreateTarget(ctx, application.CreateTargetRequest{Meta: meta, LeaseID: request.LeaseId, Template: request.Template.Reference, Kind: domain.TargetKind(request.Template.Kind), PolicyDigest: request.Template.PolicyDigest, CapabilityDigest: request.Template.CapabilityDigest})
	if err != nil {
		return nil, StatusError(err)
	}
	return target(result), nil
}

func (s *WorldServer) GetTarget(ctx context.Context, request *worldv1.GetTargetRequest) (*worldv1.Target, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, nil); err != nil {
		return nil, err
	}
	result, err := s.core.GetTarget(ctx, request.TargetId)
	if err != nil {
		return nil, StatusError(err)
	}
	return target(result), nil
}

func (s *WorldServer) StartTargetRun(ctx context.Context, request *worldv1.StartTargetRunRequest) (*worldv1.TargetRun, error) {
	if request == nil || request.RunSpec == nil {
		return nil, status.Error(codes.InvalidArgument, "run_spec is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	if request.RunSpec.MaterializationDigest == "" && len(request.RunSpec.SpecimenOccurrenceRefs) == 0 && len(request.RunSpec.FixtureRefs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "resolved materialization digest or material references are required")
	}
	if request.RunSpec.MaterializationDigest != "" && (len(request.RunSpec.SpecimenOccurrenceRefs) > 0 || len(request.RunSpec.FixtureRefs) > 0) {
		return nil, status.Error(codes.InvalidArgument, "resolved materialization digest and unresolved material references are mutually exclusive")
	}
	result, err := s.core.StartTargetRun(ctx, application.StartTargetRunRequest{
		Meta: meta, TargetID: request.TargetId, MaterializationDigest: request.RunSpec.MaterializationDigest,
		SpecimenOccurrenceRefs: append([]string(nil), request.RunSpec.SpecimenOccurrenceRefs...),
		FixtureRefs:            append([]string(nil), request.RunSpec.FixtureRefs...),
	})
	if err != nil {
		return nil, StatusError(err)
	}
	return targetRun(result), nil
}

func (s *WorldServer) WaitTargetRun(ctx context.Context, request *worldv1.WaitTargetRunRequest) (*worldv1.TargetRun, error) {
	if request == nil || !domain.TargetRunState(request.DesiredState).IsValid() {
		return nil, status.Error(codes.InvalidArgument, "valid desired_state is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, nil); err != nil {
		return nil, err
	}
	return waitFor(ctx, s.pollInterval, func() (*worldv1.TargetRun, bool, error) {
		value, err := s.core.GetTarget(ctx, request.TargetId)
		if err != nil {
			return nil, false, StatusError(err)
		}
		for _, run := range value.Runs {
			if run.ID == request.TargetRunId {
				mapped := targetRun(run)
				return mapped, mapped.State == request.DesiredState, nil
			}
		}
		return nil, false, status.Error(codes.NotFound, "target run not found")
	})
}

func (s *WorldServer) ResetTarget(ctx context.Context, request *worldv1.ResetTargetRequest) (*worldv1.Target, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	mode := ports.ResetMode(request.ResetMode)
	if err := ports.ValidateResetSelection(mode, request.SnapshotName); err != nil {
		return nil, StatusError(err)
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.ResetTarget(ctx, application.ResetTargetRequest{
		Meta: meta, TargetID: request.TargetId, ExpectedRevision: request.ExpectedRevision,
		Mode: mode, SnapshotName: request.SnapshotName, RecoveryIncidentID: request.RecoveryIncidentId,
	})
	if err != nil {
		return nil, StatusError(err)
	}
	return target(result), nil
}

func (s *WorldServer) RequestRecovery(ctx context.Context, request *worldv1.RequestRecoveryRequest) (*worldv1.RecoveredResource, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{IncidentID: request.IncidentId}, request.Mutation); err != nil {
		return nil, err
	}
	resource := application.RecoveryResource(request.Resource)
	if resource == "" {
		value, getErr := s.core.GetIncident(ctx, request.IncidentId)
		if getErr != nil {
			return nil, StatusError(getErr)
		}
		if value.TargetID != "" {
			resource = application.RecoveryResourceTarget
		} else {
			resource = application.RecoveryResourceAgent
		}
	}
	strategy := request.Strategy
	if request.Strategy != "" && request.Mode != "" && request.Strategy != request.Mode {
		return nil, status.Error(codes.InvalidArgument, "mode and strategy disagree")
	}
	if strategy == "" {
		strategy = request.Mode
	}
	acknowledgement := request.VisibilityAcknowledgement
	if acknowledgement == "" {
		if identity, ok := IdentityFromContext(ctx); ok {
			acknowledgement = "requested-by:" + identity.Subject
		}
	}
	result, err := s.core.RecoverIncident(ctx, application.RecoverIncidentRequest{Meta: meta, IncidentID: request.IncidentId, ExpectedIncidentRevision: request.ExpectedRevision, Resource: resource, Strategy: strategy, VisibilityAcknowledgement: acknowledgement})
	if err != nil {
		return nil, StatusError(err)
	}
	mapped := &worldv1.RecoveredResource{ResourceKind: string(resource), Incident: incident(result.Incident)}
	if result.Target != nil {
		mapped.Target = target(*result.Target)
		mapped.ResourceId, mapped.Generation, mapped.Revision = result.Target.ID, result.Target.CurrentGeneration, result.Target.Revision
	}
	if result.Agent != nil {
		mapped.AgentWorkspace = agentWorkspace(*result.Agent)
		mapped.ResourceId, mapped.Generation, mapped.Revision = result.Agent.ID, result.Agent.CurrentGeneration, result.Agent.Revision
	}
	if result.Lease != nil {
		mapped.Lease = lease(*result.Lease)
	}
	return mapped, nil
}

func (s *WorldServer) GetIncident(ctx context.Context, request *worldv1.GetIncidentRequest) (*worldv1.Incident, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{IncidentID: request.IncidentId}, nil); err != nil {
		return nil, err
	}
	result, err := s.core.GetIncident(ctx, request.IncidentId)
	if err != nil {
		return nil, StatusError(err)
	}
	return incident(result), nil
}

func (s *WorldServer) TransitionAgentGeneration(ctx context.Context, request *worldv1.TransitionAgentGenerationRequest) (*worldv1.AgentWorkspace, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{AgentWorkspaceID: request.AgentWorkspaceId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{Meta: meta, AgentWorkspaceID: request.AgentWorkspaceId, Generation: request.Generation, ExpectedRevision: request.ExpectedRevision, State: domain.AgentGenerationState(request.State)})
	if err != nil {
		return nil, StatusError(err)
	}
	return agentWorkspace(result), nil
}

func (s *WorldServer) TransitionTargetGeneration(ctx context.Context, request *worldv1.TransitionTargetGenerationRequest) (*worldv1.Target, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{Meta: meta, TargetID: request.TargetId, Generation: request.Generation, ExpectedRevision: request.ExpectedRevision, State: domain.TargetGenerationState(request.State)})
	if err != nil {
		return nil, StatusError(err)
	}
	return target(result), nil
}

func (s *WorldServer) TransitionTargetRun(ctx context.Context, request *worldv1.TransitionTargetRunRequest) (*worldv1.TargetRun, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionTargetRun(ctx, application.TransitionTargetRunRequest{Meta: meta, TargetID: request.TargetId, RunID: request.TargetRunId, ExpectedRevision: request.ExpectedRevision, State: domain.TargetRunState(request.State)})
	if err != nil {
		return nil, StatusError(err)
	}
	return targetRun(result), nil
}

func (s *WorldServer) CreateTargetOperation(ctx context.Context, request *worldv1.CreateTargetOperationRequest) (*worldv1.TargetOperation, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.CreateTargetOperation(ctx, application.CreateTargetOperationRequest{Meta: meta, TargetID: request.TargetId, RunID: request.TargetRunId, Kind: domain.TargetOperationKind(request.Kind), CommandDisplay: request.CommandDisplay, ContentDigest: request.ContentDigest})
	if err != nil {
		return nil, StatusError(err)
	}
	return targetOperation(result), nil
}

func (s *WorldServer) TransitionTargetOperation(ctx context.Context, request *worldv1.TransitionTargetOperationRequest) (*worldv1.TargetOperation, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{Meta: meta, TargetID: request.TargetId, OperationID: request.TargetOperationId, ExpectedRevision: request.ExpectedRevision, State: domain.TargetOperationState(request.State)})
	if err != nil {
		return nil, StatusError(err)
	}
	return targetOperation(result), nil
}

func (s *WorldServer) CreateIncident(ctx context.Context, request *worldv1.CreateIncidentRequest) (*worldv1.Incident, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{SessionID: request.ResearchSessionId}, request.Mutation); err != nil {
		return nil, err
	}
	mappedCause, err := applicationCause(request.Cause)
	if err != nil {
		return nil, invalidArgument(err)
	}
	mappedArtifacts, err := applicationIncidentArtifacts(request.Artifacts)
	if err != nil {
		return nil, invalidArgument(err)
	}
	mappedMetrics, err := applicationIncidentMetrics(request.HighWaterMetrics)
	if err != nil {
		return nil, invalidArgument(err)
	}
	mappedCoverage, err := applicationIncidentCoverage(request.Coverage)
	if err != nil {
		return nil, invalidArgument(err)
	}
	result, err := s.core.CreateIncident(ctx, application.CreateIncidentRequest{
		Meta: meta, Classification: domain.IncidentClassification(request.Classification), SessionID: request.ResearchSessionId,
		LeaseID: request.LeaseId, AgentWorkspaceID: request.AgentWorkspaceId, AgentGeneration: request.AgentGeneration,
		ExecID: request.ExecId, TargetID: request.TargetId, TargetGeneration: request.TargetGeneration, TargetRunID: request.TargetRunId,
		Trigger: request.Trigger, LastKnownState: request.LastKnownState, Cause: mappedCause,
		HighWaterMetrics:    mappedMetrics,
		FirstRelevantCursor: request.FirstRelevantCursor, LastRelevantCursor: request.LastRelevantCursor,
		Coverage: mappedCoverage, ObservationBundleID: request.ObservationBundleId,
		Artifacts: mappedArtifacts,
	})
	if err != nil {
		return nil, StatusError(err)
	}
	return incident(result), nil
}

func (s *WorldServer) TransitionIncident(ctx context.Context, request *worldv1.TransitionIncidentRequest) (*worldv1.Incident, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{IncidentID: request.IncidentId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionIncident(ctx, application.TransitionIncidentRequest{Meta: meta, IncidentID: request.IncidentId, ExpectedRevision: request.ExpectedRevision, State: domain.IncidentState(request.State), RecoveryActions: append([]string(nil), request.RecoveryActions...), VisibilityAcknowledgements: append([]string(nil), request.VisibilityAcknowledgements...)})
	if err != nil {
		return nil, StatusError(err)
	}
	return incident(result), nil
}

func (s *WorldServer) GetExec(ctx context.Context, request *worldv1.GetExecRequest) (*worldv1.Exec, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{ExecID: request.ExecId}, nil); err != nil {
		return nil, err
	}
	result, err := s.core.GetExec(ctx, request.ExecId)
	if err != nil {
		return nil, StatusError(err)
	}
	return execution(result), nil
}

func (s *WorldServer) CreateExec(ctx context.Context, request *worldv1.CreateExecRequest) (*worldv1.Exec, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.CreateExec(ctx, application.CreateExecRequest{
		Meta: meta, LeaseID: request.LeaseId, Kind: domain.ExecKind(request.Kind), Executable: request.ProviderExecutable,
		Argv: append([]string(nil), request.Argv...), WorkingDirectory: request.WorkspaceRelativeWorkingDirectory,
	})
	if err != nil {
		return nil, StatusError(err)
	}
	return execution(result), nil
}

func (s *WorldServer) TransitionExec(ctx context.Context, request *worldv1.TransitionExecRequest) (*worldv1.Exec, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{ExecID: request.ExecId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: meta, ExecID: request.ExecId, ExpectedRevision: request.ExpectedRevision, State: domain.ExecState(request.State)})
	if err != nil {
		return nil, StatusError(err)
	}
	return execution(result), nil
}

func (s *WorldServer) FinalizeExec(ctx context.Context, request *worldv1.FinalizeExecRequest) (*worldv1.Exec, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	meta, err := mutation(request.Mutation)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeNode(ctx); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{ExecID: request.ExecId}, request.Mutation); err != nil {
		return nil, err
	}
	var exitCode *int
	if request.ExitCode != nil {
		value := int(*request.ExitCode)
		exitCode = &value
	}
	result, err := s.core.FinalizeExec(ctx, application.FinalizeExecRequest{
		Meta: meta, ExecID: request.ExecId, ExpectedRevision: request.ExpectedRevision, State: domain.ExecState(request.State),
		ExitCode: exitCode, Signal: request.Signal, IncidentIDs: append([]string(nil), request.IncidentIds...), CleanupConfirmed: request.CleanupConfirmed, Error: request.Error,
	})
	if err != nil {
		return nil, StatusError(err)
	}
	return execution(result), nil
}

func (s *WorldServer) StopTargetRun(ctx context.Context, request *worldv1.StopTargetRunRequest) (*worldv1.ObservationBundle, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.StopTargetRun(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) DestroyTarget(ctx context.Context, request *worldv1.DestroyTargetRequest) (*worldv1.Outcome, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.DestroyTarget(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) QuarantineTarget(ctx context.Context, request *worldv1.QuarantineTargetRequest) (*worldv1.Target, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.QuarantineTarget(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) OpenExec(stream worldv1.WorldService_OpenExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return StatusError(err)
	}
	if err := requireExecStartFrame(first); err != nil {
		return err
	}
	if err := requireMessages(first.Start.TemporaryInputs, "start.temporary_inputs"); err != nil {
		return invalidArgument(err)
	}
	if _, err := mutation(first.Start.Mutation); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{LeaseID: first.Start.LeaseId}, first.Start.Mutation); err != nil {
		return err
	}
	return StatusError(s.capabilities.OpenExec(&bufferedExecServer{WorldService_OpenExecServer: stream, first: first}))
}

func (s *WorldServer) OpenTargetExec(stream worldv1.WorldService_OpenTargetExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return StatusError(err)
	}
	if err := requireTargetExecStartFrame(first); err != nil {
		return err
	}
	if _, err := mutation(first.Start.Mutation); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{TargetID: first.Start.TargetId}, first.Start.Mutation); err != nil {
		return err
	}
	return StatusError(s.capabilities.OpenTargetExec(&bufferedTargetExecServer{WorldService_OpenTargetExecServer: stream, first: first}))
}

func (s *WorldServer) PushTargetFile(stream worldv1.WorldService_PushTargetFileServer) error {
	first, err := stream.Recv()
	if err != nil {
		return StatusError(err)
	}
	if err := requireFileTransferStartFrame(first); err != nil {
		return err
	}
	if _, err := mutation(first.Start.Mutation); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{TargetID: first.Start.TargetId}, first.Start.Mutation); err != nil {
		return err
	}
	return StatusError(s.capabilities.PushTargetFile(&bufferedPushServer{WorldService_PushTargetFileServer: stream, first: first}))
}

func (s *WorldServer) PullTargetFile(request *worldv1.PullTargetFileRequest, stream worldv1.WorldService_PullTargetFileServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{TargetID: request.TargetId}, request.Mutation); err != nil {
		return err
	}
	return StatusError(s.capabilities.PullTargetFile(request, stream))
}

func (s *WorldServer) OpenTargetADB(stream worldv1.WorldService_OpenTargetADBServer) error {
	first, err := stream.Recv()
	if err != nil {
		return StatusError(err)
	}
	if err := requireADBStartFrame(first); err != nil {
		return err
	}
	if _, err := mutation(first.Start.Mutation); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{TargetID: first.Start.TargetId}, first.Start.Mutation); err != nil {
		return err
	}
	return StatusError(s.capabilities.OpenTargetADB(&bufferedADBServer{WorldService_OpenTargetADBServer: stream, first: first}))
}

type bufferedExecServer struct {
	worldv1.WorldService_OpenExecServer
	first *worldv1.ExecFrame
}

func requireStartOnly(kind string, hasStart, hasOtherFields bool) error {
	if !hasStart || hasOtherFields {
		return status.Errorf(codes.InvalidArgument, "first %s frame must contain only start", kind)
	}
	return nil
}

func requireExecStartFrame(frame *worldv1.ExecFrame) error {
	return requireStartOnly("exec", frame != nil && frame.Start != nil, frame != nil && (len(frame.Stdin) > 0 || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Signal != "" || frame.Resize != nil || frame.Heartbeat || frame.Outcome != nil))
}

func requireTargetExecStartFrame(frame *worldv1.TargetExecFrame) error {
	return requireStartOnly("target exec", frame != nil && frame.Start != nil, frame != nil && (len(frame.Stdin) > 0 || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Signal != "" || frame.Resize != nil || frame.Heartbeat || frame.Outcome != nil))
}

func requireFileTransferStartFrame(frame *worldv1.FileTransferFrame) error {
	return requireStartOnly("file transfer", frame != nil && frame.Start != nil, frame != nil && (len(frame.Data) > 0 || frame.Offset != 0 || frame.Digest != "" || frame.Complete || frame.Operation != nil))
}

func requireADBStartFrame(frame *worldv1.ADBFrame) error {
	return requireStartOnly("ADB", frame != nil && frame.Start != nil, frame != nil && (len(frame.ClientBytes) > 0 || len(frame.ServerBytes) > 0 || frame.AssignedSerial != "" || frame.Complete))
}

func (s *bufferedExecServer) Recv() (*worldv1.ExecFrame, error) {
	if s.first != nil {
		value := s.first
		s.first = nil
		return value, nil
	}
	return s.WorldService_OpenExecServer.Recv()
}

type bufferedTargetExecServer struct {
	worldv1.WorldService_OpenTargetExecServer
	first *worldv1.TargetExecFrame
}

func (s *bufferedTargetExecServer) Recv() (*worldv1.TargetExecFrame, error) {
	if s.first != nil {
		value := s.first
		s.first = nil
		return value, nil
	}
	return s.WorldService_OpenTargetExecServer.Recv()
}

type bufferedPushServer struct {
	worldv1.WorldService_PushTargetFileServer
	first *worldv1.FileTransferFrame
}

func (s *bufferedPushServer) Recv() (*worldv1.FileTransferFrame, error) {
	if s.first != nil {
		value := s.first
		s.first = nil
		return value, nil
	}
	return s.WorldService_PushTargetFileServer.Recv()
}

type bufferedADBServer struct {
	worldv1.WorldService_OpenTargetADBServer
	first *worldv1.ADBFrame
}

func (s *bufferedADBServer) Recv() (*worldv1.ADBFrame, error) {
	if s.first != nil {
		value := s.first
		s.first = nil
		return value, nil
	}
	return s.WorldService_OpenTargetADBServer.Recv()
}

func (s *WorldServer) GetLiveSnapshot(ctx context.Context, request *worldv1.GetLiveSnapshotRequest) (*worldv1.LiveSnapshot, error) {
	if request == nil || request.Filter == nil {
		return nil, status.Error(codes.InvalidArgument, "observation filter is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.Filter.LeaseId}, nil); err != nil {
		return nil, err
	}
	result, err := s.capabilities.GetLiveSnapshot(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) SubscribeObservations(request *worldv1.SubscribeObservationsRequest, stream worldv1.WorldService_SubscribeObservationsServer) error {
	if request == nil || request.Filter == nil {
		return status.Error(codes.InvalidArgument, "observation filter is required")
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{LeaseID: request.Filter.LeaseId}, nil); err != nil {
		return err
	}
	return StatusError(s.capabilities.SubscribeObservations(request, stream))
}

func (s *WorldServer) SubscribeMetrics(request *worldv1.SubscribeMetricsRequest, stream worldv1.WorldService_SubscribeMetricsServer) error {
	if request == nil || request.Filter == nil {
		return status.Error(codes.InvalidArgument, "observation filter is required")
	}
	if err := validateDuration(request.Resolution, "resolution", false); err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), application.AuthorizationRequest{LeaseID: request.Filter.LeaseId}, nil); err != nil {
		return err
	}
	return StatusError(s.capabilities.SubscribeMetrics(request, stream))
}

func (s *WorldServer) StartCapture(ctx context.Context, request *worldv1.StartCaptureRequest) (*worldv1.Capture, error) {
	if request == nil || request.CaptureSpec == nil {
		return nil, status.Error(codes.InvalidArgument, "capture_spec is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := validateDuration(request.CaptureSpec.Duration, "capture_spec.duration", true); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.StartCapture(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) RequestCapture(ctx context.Context, request *worldv1.RequestCaptureRequest) (*worldv1.Capture, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.RequestCapture(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) StopCapture(ctx context.Context, request *worldv1.StopCaptureRequest) (*worldv1.Capture, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.StopCapture(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) GetObservationBundle(ctx context.Context, request *worldv1.GetObservationBundleRequest) (*worldv1.ObservationBundle, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{TargetRunID: request.TargetRunId}, nil); err != nil {
		return nil, err
	}
	result, err := s.capabilities.GetObservationBundle(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) DeclareExport(ctx context.Context, request *worldv1.DeclareExportRequest) (*worldv1.Export, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := requireMessages(request.Paths, "paths"); err != nil {
		return nil, invalidArgument(err)
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.DeclareExport(ctx, request)
	return result, StatusError(err)
}

func invalidArgument(err error) error {
	return status.Error(codes.InvalidArgument, err.Error())
}

func validateDuration(value *durationpb.Duration, field string, required bool) error {
	if _, err := nativeDuration(value, field, required); err != nil {
		return invalidArgument(err)
	}
	return nil
}

func (s *WorldServer) PreviewChangeSet(ctx context.Context, request *worldv1.PreviewChangeSetRequest) (*worldv1.ChangeSet, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, nil); err != nil {
		return nil, err
	}
	result, err := s.capabilities.PreviewChangeSet(ctx, request)
	return result, StatusError(err)
}

func (s *WorldServer) CommitExport(ctx context.Context, request *worldv1.CommitExportRequest) (*worldv1.Export, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := mutation(request.Mutation); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, application.AuthorizationRequest{LeaseID: request.LeaseId}, request.Mutation); err != nil {
		return nil, err
	}
	result, err := s.capabilities.CommitExport(ctx, request)
	return result, StatusError(err)
}
