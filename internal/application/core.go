// Package application implements revisioned, idempotent logical lifecycle
// commands. Drivers report facts to this core; they never mutate domain state.
package application

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

var (
	ErrNotFound = errors.New("application resource not found")
	ErrScope    = errors.New("resource is outside the active lease scope")
)

// AuthorizationRequest resolves exactly one resource to its immutable owning
// session. PolicyReference is optional for reads and required by RPC mutation
// authorization.
type AuthorizationRequest struct {
	Subject          string
	PolicyReference  string
	SessionID        string
	LeaseID          string
	AgentWorkspaceID string
	ExecID           string
	TargetID         string
	TargetRunID      string
	IncidentID       string
}

type CoreOptions struct {
	Store *store.Store
	IDs   *domain.IDGenerator
	Clock func() time.Time
}

type Core struct {
	mu           sync.Mutex
	store        *store.Store
	ids          *domain.IDGenerator
	clock        func() time.Time
	lastSequence int64
	sessions     map[string]SessionRecord
	leases       map[string]LeaseRecord
	agents       map[string]AgentWorkspaceRecord
	execs        map[string]ExecRecord
	targets      map[string]TargetRecord
	incidents    map[string]IncidentRecord
}

func NewCore(ctx context.Context, options CoreOptions) (*Core, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("control store is required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.IDs == nil {
		generator, err := domain.NewIDGenerator(options.Clock, rand.Reader)
		if err != nil {
			return nil, err
		}
		options.IDs = generator
	}
	core := &Core{store: options.Store, ids: options.IDs, clock: options.Clock, sessions: make(map[string]SessionRecord), leases: make(map[string]LeaseRecord), agents: make(map[string]AgentWorkspaceRecord), execs: make(map[string]ExecRecord), targets: make(map[string]TargetRecord), incidents: make(map[string]IncidentRecord)}
	if err := core.syncLocked(ctx); err != nil {
		return nil, err
	}
	return core, nil
}

func (c *Core) syncLocked(ctx context.Context) error {
	for {
		records, err := c.store.Records(ctx, c.lastSequence, 1000)
		if err != nil {
			return err
		}
		for _, record := range records {
			if err := c.apply(record); err != nil {
				return fmt.Errorf("replay control sequence %d: %w", record.Sequence, err)
			}
			c.lastSequence = record.Sequence
		}
		if len(records) < 1000 {
			return nil
		}
	}
}

func (c *Core) apply(record store.ControlRecord) error {
	switch record.AggregateKind {
	case "session":
		value, err := decodeReplayAggregate(record, func(value SessionRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplaySessionProjection)
		if err != nil {
			return err
		}
		c.sessions[value.ID] = value
	case "lease":
		value, err := decodeReplayAggregate(record, func(value LeaseRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplayLeaseProjection)
		if err != nil {
			return err
		}
		c.leases[value.ID] = value
	case "agent_workspace":
		value, err := decodeReplayAggregate(record, func(value AgentWorkspaceRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplayAgentWorkspaceProjection)
		if err != nil {
			return err
		}
		c.agents[value.ID] = value
	case "exec":
		value, err := decodeReplayAggregate(record, func(value ExecRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplayExecProjection)
		if err != nil {
			return err
		}
		c.execs[value.ID] = value
	case "target":
		value, err := decodeReplayAggregate(record, func(value TargetRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplayTargetProjection)
		if err != nil {
			return err
		}
		c.targets[value.ID] = value
	case "incident":
		value, err := decodeReplayAggregate(record, func(value IncidentRecord) (string, uint64) { return value.ID, value.Revision }, c.validateReplayIncidentProjection)
		if err != nil {
			return err
		}
		c.incidents[value.ID] = value
	default:
		return integrityViolation("application.replay", "aggregate_kind", fmt.Sprintf("unknown aggregate kind %q", record.AggregateKind), nil)
	}
	return nil
}

func appendControl(ctx context.Context, tx *store.Tx, aggregateKind, aggregateID, eventKind string, revision uint64, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := tx.AppendControl(ctx, store.ControlRecord{AggregateKind: aggregateKind, AggregateID: aggregateID, Revision: revision, Kind: eventKind, Payload: payload}); err != nil {
		return err
	}
	return tx.PutSnapshot(ctx, aggregateKind, aggregateID, revision, payload)
}

func requireStoredID[T any](field, value string, parse func(string) (T, error)) (T, error) {
	id, err := parse(value)
	if err != nil {
		var zero T
		return zero, integrityViolation("application.require_stored_id", field, "persisted resource identity is invalid", err)
	}
	return id, nil
}

func (c *Core) GetResearchSession(ctx context.Context, sessionID string) (ResearchSessionView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ResearchSessionView{}, err
	}
	session, ok := c.sessions[sessionID]
	if !ok {
		return ResearchSessionView{}, ErrNotFound
	}
	return c.researchSessionViewLocked(session)
}

// GetResearchSessionByLease resolves the authoritative session projection for
// an exact lease identity. It exists for trusted composition code that must
// bind physical resources without scanning or inferring identifiers.
func (c *Core) GetResearchSessionByLease(ctx context.Context, leaseID string) (ResearchSessionView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ResearchSessionView{}, err
	}
	lease, ok := c.leases[leaseID]
	if !ok {
		return ResearchSessionView{}, ErrNotFound
	}
	session, ok := c.sessions[lease.SessionID]
	if !ok || session.LeaseID != leaseID {
		return ResearchSessionView{}, fmt.Errorf("%w: lease session", ErrNotFound)
	}
	return c.researchSessionViewLocked(session)
}

// ListResearchSessions returns a stable snapshot of every authoritative
// session projection. It is intentionally a trusted application API: startup
// reconciliation must enumerate durable ownership without inferring resource
// identities from a physical runtime.
func (c *Core) ListResearchSessions(ctx context.Context) ([]ResearchSessionView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(c.sessions))
	for id := range c.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	views := make([]ResearchSessionView, 0, len(ids))
	for _, id := range ids {
		view, err := c.researchSessionViewLocked(c.sessions[id])
		if err != nil {
			return nil, fmt.Errorf("list research session %s: %w", id, err)
		}
		views = append(views, view)
	}
	return views, nil
}

func (c *Core) researchSessionViewLocked(session SessionRecord) (ResearchSessionView, error) {
	lease, ok := c.leases[session.LeaseID]
	if !ok {
		return ResearchSessionView{}, fmt.Errorf("%w: session lease", ErrNotFound)
	}
	agent, ok := c.agents[session.AgentWorkspaceID]
	if !ok {
		return ResearchSessionView{}, fmt.Errorf("%w: session agent workspace", ErrNotFound)
	}
	view := ResearchSessionView{Session: session, Lease: lease, Agent: cloneAgent(agent)}
	for _, execution := range c.execs {
		if execution.SessionID == session.ID {
			view.Execs = append(view.Execs, cloneExec(execution))
		}
	}
	for _, target := range c.targets {
		if target.SessionID == session.ID {
			view.Targets = append(view.Targets, cloneTarget(target))
		}
	}
	for _, incident := range c.incidents {
		if incident.SessionID == session.ID {
			view.Incidents = append(view.Incidents, cloneIncident(incident))
		}
	}
	sortSessionTargets(view.Targets)
	sort.Slice(view.Execs, func(i, j int) bool { return view.Execs[i].ID < view.Execs[j].ID })
	sort.Slice(view.Incidents, func(i, j int) bool { return view.Incidents[i].ID < view.Incidents[j].ID })
	return view, nil
}

// Authorize verifies transport identity ownership and policy scope without
// exposing cross-lease resource metadata to the caller.
func (c *Core) Authorize(ctx context.Context, request AuthorizationRequest) error {
	if strings.TrimSpace(request.Subject) == "" {
		return ErrScope
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return err
	}
	identifiers := 0
	sessionID := request.SessionID
	if sessionID != "" {
		identifiers++
	}
	if request.LeaseID != "" {
		identifiers++
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return ErrNotFound
		}
		sessionID = lease.SessionID
	}
	if request.AgentWorkspaceID != "" {
		identifiers++
		agent, ok := c.agents[request.AgentWorkspaceID]
		if !ok {
			return ErrNotFound
		}
		sessionID = agent.SessionID
	}
	if request.ExecID != "" {
		identifiers++
		execution, ok := c.execs[request.ExecID]
		if !ok {
			return ErrNotFound
		}
		sessionID = execution.SessionID
	}
	if request.TargetID != "" {
		identifiers++
		target, ok := c.targets[request.TargetID]
		if !ok {
			return ErrNotFound
		}
		sessionID = target.SessionID
	}
	if request.TargetRunID != "" {
		identifiers++
		found := false
		for _, target := range c.targets {
			for _, run := range target.Runs {
				if run.ID == request.TargetRunID {
					sessionID, found = target.SessionID, true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return ErrNotFound
		}
	}
	if request.IncidentID != "" {
		identifiers++
		incident, ok := c.incidents[request.IncidentID]
		if !ok {
			return ErrNotFound
		}
		sessionID = incident.SessionID
	}
	if identifiers != 1 {
		return invalidArgument("authorize", "resource_identity", "exactly one resource identity is required", nil)
	}
	session, ok := c.sessions[sessionID]
	if !ok {
		return ErrNotFound
	}
	if session.OwnerSubject == "" || session.OwnerSubject != request.Subject {
		return ErrScope
	}
	if request.PolicyReference != "" && request.PolicyReference != session.PolicyDigest {
		return ErrScope
	}
	if request.PolicyReference != "" {
		lease, ok := c.leases[session.LeaseID]
		if !ok || lease.SessionID != session.ID {
			return ErrNotFound
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return ErrScope
		}
	}
	return nil
}

func (c *Core) GetExec(ctx context.Context, execID string) (ExecRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ExecRecord{}, err
	}
	execution, ok := c.execs[execID]
	if !ok {
		return ExecRecord{}, ErrNotFound
	}
	return cloneExec(execution), nil
}

func (c *Core) GetTarget(ctx context.Context, targetID string) (TargetRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return TargetRecord{}, err
	}
	target, ok := c.targets[targetID]
	if !ok {
		return TargetRecord{}, ErrNotFound
	}
	return cloneTarget(target), nil
}

func requireLeaseActive(lease LeaseRecord, now time.Time) error {
	if lease.State != domain.LeaseActive || !lease.Termination.Empty() || !lease.ExpiresAt.After(now) {
		return fmt.Errorf("%w: lease is not active", ErrScope)
	}
	return nil
}

// requireAgentGenerationAcceptingWork resolves the exact generation under the
// Core mutex and closes every work-admission path once quiescing begins.
func (c *Core) requireAgentGenerationAcceptingWork(operation, workspaceID string, generation uint64) (AgentWorkspaceRecord, error) {
	agent, ok := c.agents[workspaceID]
	if !ok || agent.CurrentGeneration != generation {
		return AgentWorkspaceRecord{}, ErrScope
	}
	record, err := findAgentGeneration(&agent, generation)
	if err != nil {
		return AgentWorkspaceRecord{}, err
	}
	if record.State != domain.AgentGenerationReady && record.State != domain.AgentGenerationRunning {
		return AgentWorkspaceRecord{}, failedPrecondition(operation, "agent_generation", "is not accepting work", nil)
	}
	return agent, nil
}

// requireAgentGenerationIdle is evaluated in the same critical section as the
// transition to Quiescing. Exec creation, target-run start, and target-operation
// creation take the same lock, so either work is admitted first and quiescing
// fails, or quiescing commits first and admission fails.
func (c *Core) requireAgentGenerationIdle(workspaceID string, generation uint64) error {
	const operation = "agent_generation.quiesce"
	for _, execution := range c.execs {
		if execution.AgentWorkspaceID == workspaceID && execution.AgentGeneration == generation && !execution.State.Terminal() {
			return failedPrecondition(operation, "exec", fmt.Sprintf("%s is not terminal", execution.ID), nil)
		}
	}
	for _, target := range c.targets {
		for _, targetOperation := range target.Operations {
			if targetOperation.State.Terminal() {
				continue
			}
			run, err := findRun(&target, targetOperation.RunID)
			if err != nil {
				return integrityViolation(operation, "target_operation.run_id", "operation does not resolve to its run", err)
			}
			if run.AgentWorkspaceID == workspaceID && run.AgentGeneration == generation {
				return failedPrecondition(operation, "target_operation", fmt.Sprintf("%s is not terminal", targetOperation.ID), nil)
			}
		}
	}
	return nil
}

func findAgentGeneration(agent *AgentWorkspaceRecord, generation uint64) (*AgentGenerationRecord, error) {
	for index := range agent.Generations {
		if agent.Generations[index].Generation == generation {
			return &agent.Generations[index], nil
		}
	}
	return nil, ErrNotFound
}
func findTargetGeneration(target *TargetRecord, generation uint64) (*TargetGenerationRecord, error) {
	for index := range target.Generations {
		if target.Generations[index].Generation == generation {
			return &target.Generations[index], nil
		}
	}
	return nil, ErrNotFound
}
func findRun(target *TargetRecord, runID string) (*TargetRunRecord, error) {
	for index := range target.Runs {
		if target.Runs[index].ID == runID {
			return &target.Runs[index], nil
		}
	}
	return nil, ErrNotFound
}
func findOperation(target *TargetRecord, operationID string) (*TargetOperationRecord, error) {
	for index := range target.Operations {
		if target.Operations[index].ID == operationID {
			return &target.Operations[index], nil
		}
	}
	return nil, ErrNotFound
}

func (c *Core) targetIDsForLease(leaseID string) []string {
	ids := make([]string, 0)
	for id, target := range c.targets {
		if target.LeaseID == leaseID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (c *Core) execIDsForLease(leaseID string) []string {
	ids := make([]string, 0)
	for id, execution := range c.execs {
		if execution.LeaseID == leaseID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// targetGenerationRetirementPath returns the explicit state changes required
// before a target realization can be released or replaced.
func targetGenerationRetirementPath(state domain.TargetGenerationState) ([]domain.TargetGenerationState, error) {
	switch state {
	case domain.TargetGenerationReady:
		return []domain.TargetGenerationState{domain.TargetGenerationResettable, domain.TargetGenerationDestroyed}, nil
	case domain.TargetGenerationResettable:
		return []domain.TargetGenerationState{domain.TargetGenerationDestroyed}, nil
	case domain.TargetGenerationProvisioning, domain.TargetGenerationInstrumenting:
		return []domain.TargetGenerationState{domain.TargetGenerationFailed}, nil
	case domain.TargetGenerationDestroyed, domain.TargetGenerationFailed, domain.TargetGenerationQuarantined, domain.TargetGenerationLost:
		return nil, nil
	default:
		return nil, failedPrecondition("target_generation.retire", "state", fmt.Sprintf("cannot retire generation in %s", state), nil)
	}
}

// agentGenerationRetirementPath returns the explicit state changes required
// before an agent realization can be released or replaced.
func agentGenerationRetirementPath(state domain.AgentGenerationState) ([]domain.AgentGenerationState, error) {
	switch state {
	case domain.AgentGenerationReady, domain.AgentGenerationRunning:
		return []domain.AgentGenerationState{domain.AgentGenerationQuiescing, domain.AgentGenerationSealed}, nil
	case domain.AgentGenerationQuiescing:
		return []domain.AgentGenerationState{domain.AgentGenerationSealed}, nil
	case domain.AgentGenerationProvisioning, domain.AgentGenerationBooting:
		return []domain.AgentGenerationState{domain.AgentGenerationFailed}, nil
	case domain.AgentGenerationSealed, domain.AgentGenerationFailed, domain.AgentGenerationQuarantined, domain.AgentGenerationLost:
		return nil, nil
	default:
		return nil, failedPrecondition("agent_generation.retire", "state", fmt.Sprintf("cannot retire generation in %s", state), nil)
	}
}

func appendTargetGenerationTransitions(ctx context.Context, tx *store.Tx, target *TargetRecord, generation *TargetGenerationRecord, states []domain.TargetGenerationState, eventKind string, at time.Time) error {
	for _, next := range states {
		if err := domain.RequireTargetGenerationTransition(generation.State, next); err != nil {
			return err
		}
		generation.State, generation.Revision, generation.UpdatedAt = next, generation.Revision+1, at
		target.Revision++
		target.UpdatedAt = at
		if err := appendControl(ctx, tx, "target", target.ID, eventKind, target.Revision, *target); err != nil {
			return err
		}
	}
	return nil
}

func appendAgentGenerationTransitions(ctx context.Context, tx *store.Tx, agent *AgentWorkspaceRecord, generation *AgentGenerationRecord, states []domain.AgentGenerationState, eventKind string, at time.Time) error {
	for _, next := range states {
		if err := domain.RequireAgentGenerationTransition(generation.State, next); err != nil {
			return err
		}
		generation.State, generation.Revision, generation.UpdatedAt = next, generation.Revision+1, at
		agent.Revision++
		agent.UpdatedAt = at
		if err := appendControl(ctx, tx, "agent_workspace", agent.ID, eventKind, agent.Revision, *agent); err != nil {
			return err
		}
	}
	return nil
}
