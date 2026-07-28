package application

import (
	"context"
	"encoding/json"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

// BindAgentGenerationPlan atomically freezes the exact semantic plan and the
// two physical idempotency identities before any workspace or container
// mutation. A generation can be bound once; exact retries replay through the
// application idempotency journal, while every changed plan is rejected.
type BindAgentGenerationPlanRequest struct {
	Meta                     MutationMeta `json:"meta"`
	AgentWorkspaceID         string       `json:"agent_workspace_id"`
	Generation               uint64       `json:"generation"`
	ExpectedRevision         uint64       `json:"expected_revision"`
	ProvisioningPlanDigest   string       `json:"provisioning_plan_digest"`
	WorkspaceProvisioningKey string       `json:"workspace_provisioning_key"`
	AgentProvisioningKey     string       `json:"agent_provisioning_key"`
}

func (c *Core) BindAgentGenerationPlan(ctx context.Context, request BindAgentGenerationPlanRequest) (AgentWorkspaceRecord, error) {
	const operation = "agent_generation.bind_plan"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	if _, err := domain.ParseAgentWorkspaceID(request.AgentWorkspaceID); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	if !domain.AgentGeneration(request.Generation).IsValid() || request.ExpectedRevision == 0 {
		return AgentWorkspaceRecord{}, invalidArgument(operation, "generation", "valid generation and expected revision are required", nil)
	}
	if _, err := domain.ParseDigest(request.ProvisioningPlanDigest); err != nil {
		return AgentWorkspaceRecord{}, invalidArgument(operation, "provisioning_plan_digest", "must be a valid digest", err)
	}
	if !domain.IsCanonicalIdempotencyKey(request.WorkspaceProvisioningKey) || !domain.IsCanonicalIdempotencyKey(request.AgentProvisioningKey) {
		return AgentWorkspaceRecord{}, invalidArgument(operation, "provisioning_key", "workspace and agent keys must be bounded non-blank values without surrounding whitespace", nil)
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return AgentWorkspaceRecord{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "bind_agent_generation_plan", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		agent, ok := detachedRecord(c.agents, request.AgentWorkspaceID, cloneAgent)
		if !ok {
			return nil, ErrNotFound
		}
		if agent.CurrentGeneration != request.Generation {
			return nil, failedPrecondition(operation, "generation", "must be the current agent generation", nil)
		}
		generation, err := findAgentGeneration(&agent, request.Generation)
		if err != nil {
			return nil, err
		}
		if generation.Revision != request.ExpectedRevision {
			return nil, store.ErrRevisionConflict
		}
		if generation.State != domain.AgentGenerationProvisioning {
			return nil, failedPrecondition(operation, "state", "plan must be bound before physical provisioning advances", nil)
		}
		if !agentProvisioningBindingEmpty(*generation) {
			return nil, store.ErrIdempotencyConflict
		}
		session, ok := c.sessions[agent.SessionID]
		if !ok {
			return nil, ErrNotFound
		}
		lease, ok := c.leases[session.LeaseID]
		if !ok || lease.AgentWorkspaceID != agent.ID || lease.AgentGeneration != generation.Generation {
			return nil, ErrScope
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		generation.ProvisioningPlanDigest = request.ProvisioningPlanDigest
		generation.WorkspaceProvisioningKey = request.WorkspaceProvisioningKey
		generation.AgentProvisioningKey = request.AgentProvisioningKey
		generation.Revision++
		generation.UpdatedAt = now
		agent.Revision++
		agent.UpdatedAt = now
		if err := appendControl(ctx, tx, "agent_workspace", agent.ID, "agent_generation.plan_bound", agent.Revision, agent); err != nil {
			return nil, err
		}
		return json.Marshal(agent)
	})
	if err != nil {
		return AgentWorkspaceRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	var agent AgentWorkspaceRecord
	if err := json.Unmarshal(response, &agent); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	return cloneAgent(agent), nil
}
