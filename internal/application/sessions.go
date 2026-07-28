package application

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type AcquireRequest struct {
	Meta             MutationMeta          `json:"meta"`
	OwnerSubject     string                `json:"owner_subject"`
	InputViewID      string                `json:"input_view_id"`
	InputSelection   InputSelectionRequest `json:"input_selection,omitempty"`
	PolicyDigest     string                `json:"policy_digest"`
	CapabilityDigest string                `json:"capability_digest"`
	TTL              time.Duration         `json:"ttl"`
}

// InputSelectionRequest carries only opaque repository references and logical
// projection intent. A trusted provisioning resolver replaces it with an
// authoritative InputViewID before the logical core accepts the mutation.
type InputSelectionRequest struct {
	FrozenSelectionRef string                    `json:"frozen_selection_ref,omitempty"`
	OccurrenceRefs     []string                  `json:"occurrence_refs,omitempty"`
	PathMappings       []InputPathMappingRequest `json:"path_mappings,omitempty"`
	AllowedSidecars    []string                  `json:"allowed_sidecars,omitempty"`
	SecurityScope      string                    `json:"security_scope,omitempty"`
	RequireZeroCopy    bool                      `json:"require_zero_copy,omitempty"`
}

type InputPathMappingRequest struct {
	OccurrenceRef string `json:"occurrence_ref"`
	LogicalPath   string `json:"logical_path"`
}

func (r InputSelectionRequest) Empty() bool {
	return r.FrozenSelectionRef == "" && len(r.OccurrenceRefs) == 0 && len(r.PathMappings) == 0 &&
		len(r.AllowedSidecars) == 0 && r.SecurityScope == "" && !r.RequireZeroCopy
}

type acquireResult struct {
	SessionID string `json:"session_id"`
}

func (c *Core) AcquireResearchSession(ctx context.Context, request AcquireRequest) (ResearchSessionView, error) {
	const operation = "research_session.acquire"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return ResearchSessionView{}, err
	}
	inputViewID, err := domain.ParseInputViewID(request.InputViewID)
	if err != nil {
		return ResearchSessionView{}, err
	}
	policyDigest, err := domain.ParseDigest(request.PolicyDigest)
	if err != nil {
		return ResearchSessionView{}, err
	}
	capabilityDigest, err := domain.ParseDigest(request.CapabilityDigest)
	if err != nil {
		return ResearchSessionView{}, err
	}
	if request.TTL <= 0 {
		return ResearchSessionView{}, invalidArgument(operation, "ttl", "must be positive", nil)
	}
	request.OwnerSubject = strings.TrimSpace(request.OwnerSubject)
	if request.OwnerSubject == "" {
		return ResearchSessionView{}, invalidArgument(operation, "owner_subject", "is required", nil)
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return ResearchSessionView{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	response, _, err := c.store.RunIdempotent(ctx, "acquire_research_session", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		now := c.clock().UTC()
		sessionID, err := c.ids.ResearchSessionID()
		if err != nil {
			return nil, err
		}
		leaseID, err := c.ids.LeaseID()
		if err != nil {
			return nil, err
		}
		agentID, err := c.ids.AgentWorkspaceID()
		if err != nil {
			return nil, err
		}
		workspaceID, err := c.ids.WorkspaceID()
		if err != nil {
			return nil, err
		}
		sessionModel, err := domain.NewResearchSession(sessionID, now)
		if err != nil {
			return nil, err
		}
		session := SessionRecord{ID: sessionID.String(), OwnerSubject: request.OwnerSubject, AcquisitionIdempotencyKey: request.Meta.IdempotencyKey, State: sessionModel.State(), Revision: uint64(sessionModel.Revision()), LeaseID: leaseID.String(), AgentWorkspaceID: agentID.String(), InputViewID: request.InputViewID, PolicyDigest: request.PolicyDigest, CapabilityDigest: request.CapabilityDigest, CreatedAt: now, UpdatedAt: now}
		if err := appendControl(ctx, tx, "session", session.ID, "session.requested", session.Revision, session); err != nil {
			return nil, err
		}
		sessionModel, err = sessionModel.Transition(domain.ResearchSessionAdmitted, sessionModel.Revision(), now)
		if err != nil {
			return nil, err
		}
		session.State, session.Revision = sessionModel.State(), uint64(sessionModel.Revision())
		if err := appendControl(ctx, tx, "session", session.ID, "session.admitted", session.Revision, session); err != nil {
			return nil, err
		}
		sessionModel, err = sessionModel.Transition(domain.ResearchSessionLeased, sessionModel.Revision(), now)
		if err != nil {
			return nil, err
		}
		session.State, session.Revision = sessionModel.State(), uint64(sessionModel.Revision())
		if err := appendControl(ctx, tx, "session", session.ID, "session.leased", session.Revision, session); err != nil {
			return nil, err
		}

		leaseModel, err := domain.NewLease(domain.LeaseSpec{ID: leaseID, ResearchSessionID: sessionID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration, InputViewID: inputViewID, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, ExpiresAt: now.Add(request.TTL), CreatedAt: now})
		if err != nil {
			return nil, err
		}
		lease := LeaseRecord{ID: leaseID.String(), SessionID: session.ID, AgentWorkspaceID: agentID.String(), AgentGeneration: 1, InputViewID: request.InputViewID, PolicyDigest: request.PolicyDigest, CapabilityDigest: request.CapabilityDigest, State: leaseModel.State(), Revision: uint64(leaseModel.Revision()), ExpiresAt: leaseModel.ExpiresAt(), CreatedAt: now, UpdatedAt: now}
		if err := appendControl(ctx, tx, "lease", lease.ID, "lease.created", lease.Revision, lease); err != nil {
			return nil, err
		}

		agentModel, err := domain.NewAgentWorkspace(agentID, sessionID, domain.InitialAgentGeneration, now)
		if err != nil {
			return nil, err
		}
		generationModel, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{AgentWorkspaceID: agentID, Generation: domain.InitialAgentGeneration, WorkspaceID: workspaceID, InputViewID: inputViewID, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now})
		if err != nil {
			return nil, err
		}
		agent := AgentWorkspaceRecord{ID: agentID.String(), SessionID: session.ID, CurrentGeneration: 1, Revision: uint64(agentModel.Revision()), CreatedAt: now, UpdatedAt: now, Generations: []AgentGenerationRecord{{Generation: 1, WorkspaceID: workspaceID.String(), InputViewID: request.InputViewID, PolicyDigest: request.PolicyDigest, CapabilityDigest: request.CapabilityDigest, State: generationModel.State(), Revision: uint64(generationModel.Revision()), CreatedAt: now, UpdatedAt: now}}}
		if err := appendControl(ctx, tx, "agent_workspace", agent.ID, "agent_workspace.created", agent.Revision, agent); err != nil {
			return nil, err
		}
		return json.Marshal(acquireResult{SessionID: session.ID})
	})
	if err != nil {
		return ResearchSessionView{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return ResearchSessionView{}, err
	}
	var result acquireResult
	if err := json.Unmarshal(response, &result); err != nil {
		return ResearchSessionView{}, err
	}
	return c.sessionViewLocked(result.SessionID)
}

func (c *Core) sessionViewLocked(sessionID string) (ResearchSessionView, error) {
	session, ok := c.sessions[sessionID]
	if !ok {
		return ResearchSessionView{}, ErrNotFound
	}
	lease, ok := c.leases[session.LeaseID]
	if !ok {
		return ResearchSessionView{}, ErrNotFound
	}
	agent, ok := c.agents[session.AgentWorkspaceID]
	if !ok {
		return ResearchSessionView{}, ErrNotFound
	}
	view := ResearchSessionView{Session: session, Lease: lease, Agent: cloneAgent(agent)}
	for _, execution := range c.execs {
		if execution.SessionID == sessionID {
			view.Execs = append(view.Execs, cloneExec(execution))
		}
	}
	for _, target := range c.targets {
		if target.SessionID == sessionID {
			view.Targets = append(view.Targets, cloneTarget(target))
		}
	}
	for _, incident := range c.incidents {
		if incident.SessionID == sessionID {
			view.Incidents = append(view.Incidents, cloneIncident(incident))
		}
	}
	sortSessionTargets(view.Targets)
	sort.Slice(view.Execs, func(i, j int) bool { return view.Execs[i].ID < view.Execs[j].ID })
	sort.Slice(view.Incidents, func(i, j int) bool { return view.Incidents[i].ID < view.Incidents[j].ID })
	return view, nil
}

type RenewLeaseRequest struct {
	Meta             MutationMeta  `json:"meta"`
	LeaseID          string        `json:"lease_id"`
	ExpectedRevision uint64        `json:"expected_revision"`
	TTL              time.Duration `json:"ttl"`
}

func (c *Core) RenewLease(ctx context.Context, request RenewLeaseRequest) (LeaseRecord, error) {
	const operation = "lease.renew"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return LeaseRecord{}, err
	}
	if _, err := domain.ParseLeaseID(request.LeaseID); err != nil {
		return LeaseRecord{}, err
	}
	if request.TTL <= 0 {
		return LeaseRecord{}, invalidArgument(operation, "ttl", "must be positive", nil)
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return LeaseRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "renew_lease", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if lease.Revision != request.ExpectedRevision {
			return nil, fmt.Errorf("%w: lease revision", store.ErrRevisionConflict)
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		nextExpiry := now.Add(request.TTL)
		if !nextExpiry.After(lease.ExpiresAt) {
			return nil, invalidArgument(operation, "ttl", "must extend the current lease expiry", nil)
		}
		lease.Revision++
		lease.ExpiresAt = nextExpiry
		lease.UpdatedAt = now
		if err := appendControl(ctx, tx, "lease", lease.ID, "lease.renewed", lease.Revision, lease); err != nil {
			return nil, err
		}
		return json.Marshal(lease)
	})
	if err != nil {
		return LeaseRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return LeaseRecord{}, err
	}
	var lease LeaseRecord
	if err := json.Unmarshal(response, &lease); err != nil {
		return LeaseRecord{}, err
	}
	return lease, nil
}

type TransitionAgentRequest struct {
	Meta             MutationMeta                `json:"meta"`
	AgentWorkspaceID string                      `json:"agent_workspace_id"`
	Generation       uint64                      `json:"generation"`
	ExpectedRevision uint64                      `json:"expected_revision"`
	State            domain.AgentGenerationState `json:"state"`
}

func (c *Core) TransitionAgentGeneration(ctx context.Context, request TransitionAgentRequest) (AgentWorkspaceRecord, error) {
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return AgentWorkspaceRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "transition_agent_generation", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		agent, ok := detachedRecord(c.agents, request.AgentWorkspaceID, cloneAgent)
		if !ok {
			return nil, ErrNotFound
		}
		generation, err := findAgentGeneration(&agent, request.Generation)
		if err != nil {
			return nil, err
		}
		if generation.Revision != request.ExpectedRevision {
			return nil, store.ErrRevisionConflict
		}
		if err := domain.RequireAgentGenerationTransition(generation.State, request.State); err != nil {
			return nil, err
		}
		if request.State == domain.AgentGenerationQuiescing {
			if err := c.requireAgentGenerationIdle(agent.ID, generation.Generation); err != nil {
				return nil, err
			}
		}
		generation.State, generation.Revision, generation.UpdatedAt = request.State, generation.Revision+1, c.clock().UTC()
		agent.Revision++
		agent.UpdatedAt = generation.UpdatedAt
		if err := appendControl(ctx, tx, "agent_workspace", agent.ID, "agent_generation.transitioned", agent.Revision, agent); err != nil {
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
