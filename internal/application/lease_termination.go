package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

const leaseExpiryReason = "lease lifetime elapsed"

type ReleaseResearchSessionRequest struct {
	Meta             MutationMeta `json:"meta"`
	LeaseID          string       `json:"lease_id"`
	ExpectedRevision uint64       `json:"expected_revision"`
	Reason           string       `json:"reason"`
}

type ReleaseOutcome struct {
	SessionID  string    `json:"session_id"`
	LeaseID    string    `json:"lease_id"`
	ReleasedAt time.Time `json:"released_at"`
}

// ReleasePreparation is the durable gate between accepting a release and
// retiring physical resources. Once prepared, the lease is no longer active,
// so no new exec, target, or run can race with host cleanup.
type ReleasePreparation struct {
	View                   ResearchSessionView `json:"view"`
	ReleasingLeaseRevision uint64              `json:"releasing_lease_revision"`
}

// BeginLeaseExpiryRequest is accepted only by trusted lifecycle coordination.
// It intentionally has no client MutationMeta: the lease's immutable expiry is
// the authority and the supplied revision only prevents acting on a stale scan.
type BeginLeaseExpiryRequest struct {
	LeaseID          string `json:"lease_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

// CompleteLeaseTerminationRequest resumes an already durable termination
// intent. It is safe for startup recovery and periodic reapers because it does
// not depend on the expired initiating request deadline.
type CompleteLeaseTerminationRequest struct {
	LeaseID          string `json:"lease_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

type LeaseTerminationPreparation struct {
	View                     ResearchSessionView  `json:"view"`
	Kind                     LeaseTerminationKind `json:"kind"`
	TerminatingLeaseRevision uint64               `json:"terminating_lease_revision"`
}

type LeaseTerminationOutcome struct {
	SessionID   string               `json:"session_id"`
	LeaseID     string               `json:"lease_id"`
	Kind        LeaseTerminationKind `json:"kind"`
	LeaseState  domain.LeaseState    `json:"lease_state"`
	CompletedAt time.Time            `json:"completed_at"`
}

// LeaseTerminationWork is a stable, sorted unit for a startup or periodic
// controller scan. NeedsBegin distinguishes a newly overdue lease from cleanup
// that was durably begun before a crash.
type LeaseTerminationWork struct {
	LeaseID       string                `json:"lease_id"`
	SessionID     string                `json:"session_id"`
	Kind          LeaseTerminationKind  `json:"kind"`
	State         LeaseTerminationState `json:"state,omitempty"`
	LeaseRevision uint64                `json:"lease_revision"`
	ExpiresAt     time.Time             `json:"expires_at"`
	NeedsBegin    bool                  `json:"needs_begin"`
}

type releasePreparationResult struct {
	SessionID              string `json:"session_id"`
	ReleasingLeaseRevision uint64 `json:"releasing_lease_revision"`
}

type stableMutationMeta struct {
	IdempotencyKey            string `json:"idempotency_key"`
	CorrelationID             string `json:"correlation_id"`
	CausationID               string `json:"causation_id,omitempty"`
	AuthorizedPolicyReference string `json:"authorized_policy_reference"`
}

type stableReleaseRequest struct {
	Meta             stableMutationMeta `json:"meta"`
	LeaseID          string             `json:"lease_id"`
	ExpectedRevision uint64             `json:"expected_revision"`
	Reason           string             `json:"reason"`
}

func stableRelease(request ReleaseResearchSessionRequest) stableReleaseRequest {
	return stableReleaseRequest{
		Meta: stableMutationMeta{
			IdempotencyKey:            request.Meta.IdempotencyKey,
			CorrelationID:             request.Meta.CorrelationID,
			CausationID:               request.Meta.CausationID,
			AuthorizedPolicyReference: request.Meta.AuthorizedPolicyReference,
		},
		LeaseID: request.LeaseID, ExpectedRevision: request.ExpectedRevision, Reason: strings.TrimSpace(request.Reason),
	}
}

func canonicalRequest(value any) ([]byte, string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(payload)
	return payload, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (r ReleaseResearchSessionRequest) validate(ctx context.Context, now time.Time) error {
	const operation = "research_session.release"
	if err := r.Meta.Validate(ctx, now); err != nil {
		return err
	}
	if _, err := domain.ParseLeaseID(r.LeaseID); err != nil {
		return err
	}
	if strings.TrimSpace(r.Reason) == "" {
		return invalidArgument(operation, "reason", "is required", nil)
	}
	if r.ExpectedRevision == 0 {
		return invalidArgument(operation, "expected_revision", "must be positive", nil)
	}
	return nil
}

func validateTrustedTerminationRequest(operation, leaseID string, expectedRevision uint64) error {
	if _, err := domain.ParseLeaseID(leaseID); err != nil {
		return err
	}
	if expectedRevision == 0 {
		return invalidArgument(operation, "expected_revision", "must be positive", nil)
	}
	return nil
}

// BeginReleaseResearchSession atomically gates a caller-requested release. Its
// idempotency signature excludes only Meta.Deadline, allowing a controller to
// retry the same logical request under a fresh bounded cleanup context.
func (c *Core) BeginReleaseResearchSession(ctx context.Context, request ReleaseResearchSessionRequest) (ReleasePreparation, error) {
	const operation = "research_session.begin_release"
	if err := request.validate(ctx, c.clock()); err != nil {
		return ReleasePreparation{}, err
	}
	stable := stableRelease(request)
	requestBytes, requestDigest, err := canonicalRequest(stable)
	if err != nil {
		return ReleasePreparation{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ReleasePreparation{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "begin_release_research_session", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		return c.beginLeaseTermination(ctx, tx, lease, beginLeaseTerminationParams{
			Operation: operation, Kind: LeaseTerminationRelease, Reason: stable.Reason,
			ExpectedRevision: request.ExpectedRevision, IdempotencyKey: request.Meta.IdempotencyKey,
			RequestDigest: requestDigest,
		})
	})
	if err != nil {
		return ReleasePreparation{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return ReleasePreparation{}, err
	}
	var result releasePreparationResult
	if err := json.Unmarshal(response, &result); err != nil {
		return ReleasePreparation{}, err
	}
	view, err := c.sessionViewLocked(result.SessionID)
	if err != nil {
		return ReleasePreparation{}, err
	}
	return ReleasePreparation{View: view, ReleasingLeaseRevision: result.ReleasingLeaseRevision}, nil
}

// BeginDueLeaseExpiry converts an at-or-past-deadline lease into a durable
// expiring intent. It does not require active work to have stopped: the intent
// first closes admission, then the controller drains physical work and calls
// CompleteLeaseTermination.
func (c *Core) BeginDueLeaseExpiry(ctx context.Context, request BeginLeaseExpiryRequest) (LeaseTerminationPreparation, error) {
	const operation = "lease.begin_expiry"
	if err := validateTrustedTerminationRequest(operation, request.LeaseID, request.ExpectedRevision); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	requestBytes, requestDigest, err := canonicalRequest(request)
	if err != nil {
		return LeaseTerminationPreparation{}, err
	}
	idempotencyKey := domain.DeriveIdempotencyKey("expiry", request.LeaseID)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "begin_due_lease_expiry", idempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if c.clock().Before(lease.ExpiresAt) {
			return nil, failedPrecondition(operation, "expires_at", "lease is not due", nil)
		}
		return c.beginLeaseTermination(ctx, tx, lease, beginLeaseTerminationParams{
			Operation: operation, Kind: LeaseTerminationExpiry, Reason: leaseExpiryReason,
			ExpectedRevision: request.ExpectedRevision, IdempotencyKey: idempotencyKey,
			RequestDigest: requestDigest,
		})
	})
	if err != nil {
		return LeaseTerminationPreparation{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	return c.decodeTerminationPreparation(response)
}

type beginLeaseTerminationParams struct {
	Operation        string
	Kind             LeaseTerminationKind
	Reason           string
	ExpectedRevision uint64
	IdempotencyKey   string
	RequestDigest    string
}

func (c *Core) beginLeaseTermination(ctx context.Context, tx *store.Tx, lease LeaseRecord, params beginLeaseTerminationParams) ([]byte, error) {
	if !lease.Termination.Empty() {
		return nil, store.ErrIdempotencyConflict
	}
	if lease.Revision != params.ExpectedRevision {
		return nil, store.ErrRevisionConflict
	}
	if lease.State != domain.LeaseActive {
		return nil, failedPrecondition(params.Operation, "lease", "is not active", nil)
	}
	if params.Kind == LeaseTerminationRelease {
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
	}

	session, ok := c.sessions[lease.SessionID]
	if !ok {
		return nil, ErrNotFound
	}
	now := c.clock().UTC()
	nextLeaseState := lease.State
	nextTerminationState := LeaseTerminationExpiring
	leaseEvent := "lease.expiring"
	if params.Kind == LeaseTerminationRelease {
		if err := domain.RequireLeaseTransition(lease.State, domain.LeaseReleasing); err != nil {
			return nil, err
		}
		nextLeaseState = domain.LeaseReleasing
		nextTerminationState = LeaseTerminationReleasing
		leaseEvent = "lease.releasing"
	}
	lease.State = nextLeaseState
	lease.Revision++
	lease.UpdatedAt = now
	lease.Termination = LeaseTerminationRecord{
		Kind: params.Kind, State: nextTerminationState, Reason: params.Reason,
		BeginIdempotencyKey: params.IdempotencyKey, BeginRequestDigest: params.RequestDigest,
		InitiatedLeaseRevision: lease.Revision, InitiatedAt: now,
	}
	if err := appendControl(ctx, tx, "lease", lease.ID, leaseEvent, lease.Revision, lease); err != nil {
		return nil, err
	}
	if err := domain.RequireResearchSessionTransition(session.State, domain.ResearchSessionReleasing); err != nil {
		return nil, err
	}
	session.State, session.Revision, session.UpdatedAt = domain.ResearchSessionReleasing, session.Revision+1, now
	if err := appendControl(ctx, tx, "session", session.ID, "session.releasing", session.Revision, session); err != nil {
		return nil, err
	}
	return json.Marshal(releasePreparationResult{SessionID: session.ID, ReleasingLeaseRevision: lease.Revision})
}

func (c *Core) decodeTerminationPreparation(response []byte) (LeaseTerminationPreparation, error) {
	var result releasePreparationResult
	if err := json.Unmarshal(response, &result); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	view, err := c.sessionViewLocked(result.SessionID)
	if err != nil {
		return LeaseTerminationPreparation{}, err
	}
	return LeaseTerminationPreparation{
		View: view, Kind: view.Lease.Termination.Kind,
		TerminatingLeaseRevision: result.ReleasingLeaseRevision,
	}, nil
}

// ListLeaseTerminationWork returns overdue leases that need a durable begin and
// every unfinished release/expiry that must be resumed after restart.
func (c *Core) ListLeaseTerminationWork(ctx context.Context) ([]LeaseTerminationWork, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return nil, err
	}
	now := c.clock()
	work := make([]LeaseTerminationWork, 0)
	for _, lease := range c.leases {
		item := LeaseTerminationWork{
			LeaseID: lease.ID, SessionID: lease.SessionID, LeaseRevision: lease.Revision, ExpiresAt: lease.ExpiresAt,
		}
		switch {
		case lease.Termination.InProgress():
			item.Kind, item.State = lease.Termination.Kind, lease.Termination.State
			work = append(work, item)
		case lease.Termination.Empty() && lease.State == domain.LeaseActive && !lease.ExpiresAt.After(now):
			item.Kind, item.NeedsBegin = LeaseTerminationExpiry, true
			work = append(work, item)
		}
	}
	sort.Slice(work, func(i, j int) bool {
		if work[i].ExpiresAt.Equal(work[j].ExpiresAt) {
			return work[i].LeaseID < work[j].LeaseID
		}
		return work[i].ExpiresAt.Before(work[j].ExpiresAt)
	})
	return work, nil
}

// ResumeLeaseTermination reconstructs authoritative cleanup scope without
// replaying an expired caller request or manufacturing a new idempotency key.
func (c *Core) ResumeLeaseTermination(ctx context.Context, leaseID string) (LeaseTerminationPreparation, error) {
	if _, err := domain.ParseLeaseID(leaseID); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return LeaseTerminationPreparation{}, err
	}
	lease, ok := c.leases[leaseID]
	if !ok {
		return LeaseTerminationPreparation{}, ErrNotFound
	}
	if !lease.Termination.InProgress() {
		return LeaseTerminationPreparation{}, failedPrecondition("lease.resume_termination", "termination", "is not in progress", nil)
	}
	view, err := c.sessionViewLocked(lease.SessionID)
	if err != nil {
		return LeaseTerminationPreparation{}, err
	}
	return LeaseTerminationPreparation{View: view, Kind: lease.Termination.Kind, TerminatingLeaseRevision: lease.Revision}, nil
}

// CompleteReleaseResearchSession records terminal logical state only after the
// caller has authoritatively retired all physical resources. Deadline is not
// part of the stored request signature, so a detached retry remains stable.
func (c *Core) CompleteReleaseResearchSession(ctx context.Context, request ReleaseResearchSessionRequest) (ReleaseOutcome, error) {
	const operation = "research_session.complete_release"
	if err := request.validate(ctx, c.clock()); err != nil {
		return ReleaseOutcome{}, err
	}
	stable := stableRelease(request)
	requestBytes, requestDigest, err := canonicalRequest(stable)
	if err != nil {
		return ReleaseOutcome{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ReleaseOutcome{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "complete_release_research_session", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if lease.Termination.Kind != LeaseTerminationRelease || lease.Termination.Reason != stable.Reason {
			return nil, store.ErrIdempotencyConflict
		}
		outcome, err := c.completeLeaseTermination(ctx, tx, lease, completionIdentity{
			Operation: operation, ExpectedRevision: request.ExpectedRevision,
			IdempotencyKey: request.Meta.IdempotencyKey, RequestDigest: requestDigest,
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(ReleaseOutcome{SessionID: outcome.SessionID, LeaseID: outcome.LeaseID, ReleasedAt: outcome.CompletedAt})
	})
	if err != nil {
		return ReleaseOutcome{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return ReleaseOutcome{}, err
	}
	var outcome ReleaseOutcome
	if err := json.Unmarshal(response, &outcome); err != nil {
		return ReleaseOutcome{}, err
	}
	return outcome, nil
}

// CompleteLeaseTermination is the trusted restart path for either expiry or
// release. Its deterministic key is the lease identity; exact retries replay,
// while a changed expected revision conflicts.
func (c *Core) CompleteLeaseTermination(ctx context.Context, request CompleteLeaseTerminationRequest) (LeaseTerminationOutcome, error) {
	const operation = "lease.complete_termination"
	if err := validateTrustedTerminationRequest(operation, request.LeaseID, request.ExpectedRevision); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	requestBytes, requestDigest, err := canonicalRequest(request)
	if err != nil {
		return LeaseTerminationOutcome{}, err
	}
	idempotencyKey := domain.DeriveIdempotencyKey("termination", request.LeaseID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "complete_lease_termination", idempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		outcome, err := c.completeLeaseTermination(ctx, tx, lease, completionIdentity{
			Operation: operation, ExpectedRevision: request.ExpectedRevision,
			IdempotencyKey: idempotencyKey, RequestDigest: requestDigest,
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(outcome)
	})
	if err != nil {
		return LeaseTerminationOutcome{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	var outcome LeaseTerminationOutcome
	if err := json.Unmarshal(response, &outcome); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	return outcome, nil
}

type completionIdentity struct {
	Operation        string
	ExpectedRevision uint64
	IdempotencyKey   string
	RequestDigest    string
}

func (c *Core) completeLeaseTermination(ctx context.Context, tx *store.Tx, lease LeaseRecord, identity completionIdentity) (LeaseTerminationOutcome, error) {
	if !lease.Termination.InProgress() {
		if lease.Termination.Terminal() {
			return LeaseTerminationOutcome{}, store.ErrIdempotencyConflict
		}
		return LeaseTerminationOutcome{}, failedPrecondition(identity.Operation, "termination", "has not begun", nil)
	}
	if lease.Termination.CompleteIdempotencyKey != "" || lease.Termination.CompleteRequestDigest != "" {
		return LeaseTerminationOutcome{}, store.ErrIdempotencyConflict
	}
	if lease.Revision != identity.ExpectedRevision {
		return LeaseTerminationOutcome{}, store.ErrRevisionConflict
	}
	if err := c.requireLeaseWorkFinalized(identity.Operation, lease.ID); err != nil {
		return LeaseTerminationOutcome{}, err
	}

	session, ok := c.sessions[lease.SessionID]
	if !ok {
		return LeaseTerminationOutcome{}, ErrNotFound
	}
	agent, ok := detachedRecord(c.agents, lease.AgentWorkspaceID, cloneAgent)
	if !ok {
		return LeaseTerminationOutcome{}, ErrNotFound
	}
	now := c.clock().UTC()
	for _, targetID := range c.targetIDsForLease(lease.ID) {
		target, ok := detachedRecord(c.targets, targetID, cloneTarget)
		if !ok {
			return LeaseTerminationOutcome{}, ErrNotFound
		}
		generation, err := findTargetGeneration(&target, target.CurrentGeneration)
		if err != nil {
			return LeaseTerminationOutcome{}, err
		}
		states, err := targetGenerationRetirementPath(generation.State)
		if err != nil {
			return LeaseTerminationOutcome{}, err
		}
		if err := appendTargetGenerationTransitions(ctx, tx, &target, generation, states, "target.termination_transition", now); err != nil {
			return LeaseTerminationOutcome{}, err
		}
	}
	agentGeneration, err := findAgentGeneration(&agent, agent.CurrentGeneration)
	if err != nil {
		return LeaseTerminationOutcome{}, err
	}
	agentStates, err := agentGenerationRetirementPath(agentGeneration.State)
	if err != nil {
		return LeaseTerminationOutcome{}, err
	}
	if err := appendAgentGenerationTransitions(ctx, tx, &agent, agentGeneration, agentStates, "agent_generation.termination_transition", now); err != nil {
		return LeaseTerminationOutcome{}, err
	}

	nextLeaseState, nextTerminationState := domain.LeaseReleased, LeaseTerminationReleased
	if lease.Termination.Kind == LeaseTerminationExpiry {
		nextLeaseState, nextTerminationState = domain.LeaseExpired, LeaseTerminationExpired
	}
	if err := domain.RequireLeaseTransition(lease.State, nextLeaseState); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	lease.State, lease.Revision, lease.UpdatedAt = nextLeaseState, lease.Revision+1, now
	lease.Termination.State = nextTerminationState
	lease.Termination.CompleteIdempotencyKey = identity.IdempotencyKey
	lease.Termination.CompleteRequestDigest = identity.RequestDigest
	lease.Termination.CompletedAt = now
	if err := appendControl(ctx, tx, "lease", lease.ID, "lease."+nextLeaseState.String(), lease.Revision, lease); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	if err := domain.RequireResearchSessionTransition(session.State, domain.ResearchSessionReleased); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	session.State, session.Revision, session.UpdatedAt = domain.ResearchSessionReleased, session.Revision+1, now
	if err := appendControl(ctx, tx, "session", session.ID, "session.released", session.Revision, session); err != nil {
		return LeaseTerminationOutcome{}, err
	}
	return LeaseTerminationOutcome{
		SessionID: session.ID, LeaseID: lease.ID, Kind: lease.Termination.Kind,
		LeaseState: lease.State, CompletedAt: now,
	}, nil
}

func (r LeaseTerminationRecord) Terminal() bool {
	return r.State.Terminal()
}

func (c *Core) requireLeaseWorkFinalized(operation, leaseID string) error {
	for _, execID := range c.execIDsForLease(leaseID) {
		if execution := c.execs[execID]; !execution.State.Terminal() {
			return failedPrecondition(operation, "exec", fmt.Sprintf("%s must be finalized before termination", execID), nil)
		}
	}
	for _, targetID := range c.targetIDsForLease(leaseID) {
		target := c.targets[targetID]
		for _, run := range target.Runs {
			if !run.State.Terminal() {
				return failedPrecondition(operation, "target_run", fmt.Sprintf("%s must be finalized before termination", run.ID), nil)
			}
		}
		for _, targetOperation := range target.Operations {
			if !targetOperation.State.Terminal() {
				return failedPrecondition(operation, "target_operation", fmt.Sprintf("%s must be finalized before termination", targetOperation.ID), nil)
			}
		}
	}
	return nil
}

// ReleaseResearchSession preserves the purely logical application operation
// for callers without physical drivers. Production orchestration uses the two
// explicit phases around host cleanup.
func (c *Core) ReleaseResearchSession(ctx context.Context, request ReleaseResearchSessionRequest) (ReleaseOutcome, error) {
	// The logical-only convenience operation has no physical controller with
	// which to stop work. Keep it fail-closed, while the explicit Begin API is
	// allowed to establish the admission gate before a Controller drains work.
	c.mu.Lock()
	if err := c.syncLocked(ctx); err != nil {
		c.mu.Unlock()
		return ReleaseOutcome{}, err
	}
	if err := c.requireLeaseWorkFinalized("research_session.release", request.LeaseID); err != nil {
		c.mu.Unlock()
		return ReleaseOutcome{}, err
	}
	c.mu.Unlock()
	preparation, err := c.BeginReleaseResearchSession(ctx, request)
	if err != nil {
		return ReleaseOutcome{}, err
	}
	if preparation.View.Lease.State == domain.LeaseReleased {
		return ReleaseOutcome{SessionID: preparation.View.Session.ID, LeaseID: preparation.View.Lease.ID, ReleasedAt: preparation.View.Lease.UpdatedAt}, nil
	}
	request.ExpectedRevision = preparation.ReleasingLeaseRevision
	return c.CompleteReleaseResearchSession(ctx, request)
}
