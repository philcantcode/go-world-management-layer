package ports

import (
	"context"
	"encoding/json"
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
	if workspace.LeaseID != p.LeaseID || workspace.AgentWorkspaceID != generation.AgentWorkspaceID || workspace.AgentGeneration != generation.Generation || workspace.ID != generation.WorkspaceID || workspace.InputViewID != generation.InputViewID {
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

// AgentWorkspacePlanIdentityDigest binds every immutable semantic input to an
// agent-driver Provision request. Mutable lifecycle state and revisions are
// intentionally excluded; the immutable generation/workspace specifications,
// including their provenance and creation times, are included.
func AgentWorkspacePlanIdentityDigest(p AgentWorkspacePlan) (domain.Digest, error) {
	if err := p.Validate(); err != nil {
		return domain.Digest{}, err
	}
	generation := p.Generation.Spec()
	workspace := p.Workspace.Spec()
	identity := struct {
		Version                   string              `json:"version"`
		IdempotencyKey            string              `json:"idempotency_key"`
		LeaseID                   string              `json:"lease_id"`
		AgentWorkspaceID          string              `json:"agent_workspace_id"`
		Generation                uint64              `json:"generation"`
		WorkspaceID               string              `json:"workspace_id"`
		InputViewID               string              `json:"input_view_id"`
		PolicyDigest              string              `json:"policy_digest"`
		CapabilityFingerprint     string              `json:"capability_fingerprint_digest"`
		PreviousGeneration        uint64              `json:"previous_generation,omitempty"`
		RecoveryIncidentID        string              `json:"recovery_incident_id,omitempty"`
		GenerationCreatedAt       time.Time           `json:"generation_created_at"`
		WorkspaceLeaseID          string              `json:"workspace_lease_id"`
		WorkspaceAgentWorkspaceID string              `json:"workspace_agent_workspace_id"`
		WorkspaceAgentGeneration  uint64              `json:"workspace_agent_generation"`
		WorkspaceInputViewID      string              `json:"workspace_input_view_id"`
		WorkspaceCreatedAt        time.Time           `json:"workspace_created_at"`
		ImageDigest               string              `json:"image_digest"`
		Resources                 admission.Resources `json:"resources"`
	}{
		Version: "world.agent-workspace-plan.v1", IdempotencyKey: p.IdempotencyKey, LeaseID: p.LeaseID.String(),
		AgentWorkspaceID: generation.AgentWorkspaceID.String(), Generation: uint64(generation.Generation),
		WorkspaceID: generation.WorkspaceID.String(), InputViewID: generation.InputViewID.String(),
		PolicyDigest: p.PolicyDigest.String(), CapabilityFingerprint: p.CapabilityFingerprintDigest.String(),
		PreviousGeneration: uint64(generation.PreviousGeneration), RecoveryIncidentID: generation.RecoveryIncidentID.String(),
		GenerationCreatedAt: generation.CreatedAt, WorkspaceLeaseID: workspace.LeaseID.String(),
		WorkspaceAgentWorkspaceID: workspace.AgentWorkspaceID.String(), WorkspaceAgentGeneration: uint64(workspace.AgentGeneration),
		WorkspaceInputViewID: workspace.InputViewID.String(), WorkspaceCreatedAt: workspace.CreatedAt,
		ImageDigest: p.ImageDigest.String(), Resources: p.Resources.Clone(),
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(encoded), nil
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
