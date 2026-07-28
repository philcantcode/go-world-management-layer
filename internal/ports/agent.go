package ports

import (
	"context"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type AgentWorkspacePlan struct {
	IdempotencyKey              string
	LeaseID                     domain.LeaseID
	Generation                  domain.AgentWorkspaceGenerationRecord
	Workspace                   domain.Workspace
	ImageDigest                 domain.Digest
	PolicyDigest                domain.Digest
	CapabilityFingerprintDigest domain.Digest
	Resources                   admission.Resources
}

func (p AgentWorkspacePlan) Validate() error {
	const operation = "ports.agent_workspace_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.LeaseID.IsZero() || p.Generation.Spec().AgentWorkspaceID.IsZero() || p.Workspace.ID().IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "scope", "lease, generation, and workspace are required", nil)
	}
	generation := p.Generation.Spec()
	workspace := p.Workspace.Spec()
	if workspace.LeaseID != p.LeaseID || workspace.AgentWorkspaceID != generation.AgentWorkspaceID || workspace.AgentGeneration != generation.Generation || workspace.ID != generation.WorkspaceID {
		return domain.NewError(domain.CodeConflict, operation, "workspace", "does not match the generation scope", nil)
	}
	if p.ImageDigest.IsZero() || p.PolicyDigest.IsZero() || p.CapabilityFingerprintDigest.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "digests", "image, policy, and capability digests are required", nil)
	}
	if generation.PolicyDigest != p.PolicyDigest || generation.CapabilityFingerprintDigest != p.CapabilityFingerprintDigest {
		return domain.NewError(domain.CodeConflict, operation, "generation", "policy or capability digest does not match", nil)
	}
	if err := p.Resources.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "resources", "is invalid", err)
	}
	return nil
}

type AgentWorkspaceStatus struct {
	AgentWorkspaceID domain.AgentWorkspaceID
	Generation       domain.AgentGeneration
	State            domain.AgentGenerationState
	Ready            bool
	ContainerID      string
	CgroupID         string
	GuestProtocol    uint32
	ObservedAt       time.Time
}

type AgentWorkspaceResult struct {
	Status  AgentWorkspaceStatus
	Created bool
}

type AgentWorkspaceRef struct {
	ID         domain.AgentWorkspaceID
	Generation domain.AgentGeneration
}

func (r AgentWorkspaceRef) Validate() error {
	if r.ID.IsZero() || !r.Generation.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "ports.agent_workspace_ref.validate", "scope", "workspace and generation are required", nil)
	}
	return nil
}

// AgentWorkspaceDriver owns one agent container generation. Every call requires
// an active context deadline and must continue honoring cancellation.
type AgentWorkspaceDriver interface {
	Probe(context.Context) (domain.CapabilityFingerprint, error)
	Provision(context.Context, AgentWorkspacePlan) (AgentWorkspaceResult, error)
	OpenExec(context.Context, ExecPlan) (ExecTransport, error)
	Inspect(context.Context, AgentWorkspaceRef) (AgentWorkspaceStatus, error)
	Stop(context.Context, AgentWorkspaceRef, StopMode) error
	Destroy(context.Context, AgentWorkspaceRef) error
}
