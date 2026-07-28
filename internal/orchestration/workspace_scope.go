package orchestration

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// CoreWorkspaceResolver derives the physical workspace binding from the
// durable control-plane projection. It never infers a path and never accepts a
// caller-provided workspace or generation identity.
type CoreWorkspaceResolver struct {
	core *application.Core
}

func NewCoreWorkspaceResolver(core *application.Core) (*CoreWorkspaceResolver, error) {
	if core == nil {
		return nil, fmt.Errorf("application core is required")
	}
	return &CoreWorkspaceResolver{core: core}, nil
}

func (r *CoreWorkspaceResolver) ResolveWorkspace(ctx context.Context, leaseValue string) (WorkspaceScope, error) {
	const operation = "orchestration.workspace_scope.resolve"
	leaseID, err := domain.ParseLeaseID(leaseValue)
	if err != nil {
		return WorkspaceScope{}, domain.NewError(domain.CodeInvalidArgument, operation, "lease_id", "is invalid", err)
	}
	view, err := r.core.GetResearchSessionByLease(ctx, leaseID.String())
	if err != nil {
		return WorkspaceScope{}, err
	}
	if view.Lease.ID != leaseID.String() || view.Lease.State != domain.LeaseActive {
		return WorkspaceScope{}, domain.NewError(domain.CodeFailedPrecondition, operation, "lease", "must be the active authoritative lease", nil)
	}
	return workspaceScopeFromView(view)
}

// workspaceScopeFromView validates the immutable lease-to-workspace binding
// without imposing an admission state. Public workspace mutations call it
// only after requiring an active lease; the trusted termination path uses it
// to finish a commit that was durably reserved before admission closed.
func workspaceScopeFromView(view application.ResearchSessionView) (WorkspaceScope, error) {
	const operation = "orchestration.workspace_scope.from_view"
	agentID, err := domain.ParseAgentWorkspaceID(view.Lease.AgentWorkspaceID)
	if err != nil || view.Agent.ID != view.Lease.AgentWorkspaceID || view.Agent.CurrentGeneration != view.Lease.AgentGeneration {
		return WorkspaceScope{}, domain.NewError(domain.CodeIntegrityViolation, operation, "agent_workspace", "lease and session projections disagree", err)
	}
	var workspaceValue string
	var agentState domain.AgentGenerationState
	for _, generation := range view.Agent.Generations {
		if generation.Generation == view.Lease.AgentGeneration {
			workspaceValue = generation.WorkspaceID
			agentState = generation.State
			break
		}
	}
	workspaceID, err := domain.ParseWorkspaceID(workspaceValue)
	if err != nil {
		return WorkspaceScope{}, domain.NewError(domain.CodeIntegrityViolation, operation, "workspace", "active generation has no valid workspace", err)
	}
	return WorkspaceScope{
		WorkspaceID: workspaceID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.AgentGeneration(view.Lease.AgentGeneration), AgentState: agentState,
	}, nil
}

var _ WorkspaceResolver = (*CoreWorkspaceResolver)(nil)
