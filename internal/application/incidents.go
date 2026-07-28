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

type CreateIncidentRequest struct {
	Meta                MutationMeta                  `json:"meta"`
	Classification      domain.IncidentClassification `json:"classification"`
	SessionID           string                        `json:"session_id"`
	LeaseID             string                        `json:"lease_id,omitempty"`
	AgentWorkspaceID    string                        `json:"agent_workspace_id,omitempty"`
	AgentGeneration     uint64                        `json:"agent_generation,omitempty"`
	ExecID              string                        `json:"exec_id,omitempty"`
	TargetID            string                        `json:"target_id,omitempty"`
	TargetGeneration    uint64                        `json:"target_generation,omitempty"`
	TargetRunID         string                        `json:"target_run_id,omitempty"`
	Trigger             string                        `json:"trigger"`
	LastKnownState      string                        `json:"last_known_state"`
	Cause               CauseRecord                   `json:"cause"`
	HighWaterMetrics    []IncidentMetricRecord        `json:"high_water_metrics,omitempty"`
	FirstRelevantCursor uint64                        `json:"first_relevant_cursor,omitempty"`
	LastRelevantCursor  uint64                        `json:"last_relevant_cursor,omitempty"`
	Coverage            []IncidentCoverageRecord      `json:"coverage,omitempty"`
	ObservationBundleID string                        `json:"observation_bundle_id,omitempty"`
	Artifacts           []IncidentArtifactRecord      `json:"artifacts,omitempty"`
}

type incidentResult struct {
	IncidentID string `json:"incident_id"`
}

func (c *Core) CreateIncident(ctx context.Context, request CreateIncidentRequest) (IncidentRecord, error) {
	const operation = "incident.create"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return IncidentRecord{}, err
	}
	if !request.Classification.IsValid() || strings.TrimSpace(request.Trigger) == "" || strings.TrimSpace(request.LastKnownState) == "" {
		return IncidentRecord{}, invalidArgument(operation, "classification_trigger_state", "recognized classification, trigger, and last known state are required", nil)
	}
	cause, err := domain.NewCauseAssessment(domain.CauseAssessmentSpec{Kind: request.Cause.Kind, Summary: request.Cause.Summary, Method: request.Cause.Method, Confidence: request.Cause.Confidence})
	if err != nil {
		return IncidentRecord{}, err
	}
	sessionID, err := domain.ParseResearchSessionID(request.SessionID)
	if err != nil {
		return IncidentRecord{}, err
	}
	leaseID, err := optionalLeaseID(request.LeaseID)
	if err != nil {
		return IncidentRecord{}, err
	}
	agentID, err := optionalAgentID(request.AgentWorkspaceID)
	if err != nil {
		return IncidentRecord{}, err
	}
	execID, err := optionalExecID(request.ExecID)
	if err != nil {
		return IncidentRecord{}, err
	}
	targetID, err := optionalTargetID(request.TargetID)
	if err != nil {
		return IncidentRecord{}, err
	}
	runID, err := optionalRunID(request.TargetRunID)
	if err != nil {
		return IncidentRecord{}, err
	}
	highWaterMetrics, err := incidentMetricModels(request.HighWaterMetrics)
	if err != nil {
		return IncidentRecord{}, err
	}
	coverage, err := incidentCoverageModels(request.Coverage)
	if err != nil {
		return IncidentRecord{}, err
	}
	bundleID, err := optionalBundleID(request.ObservationBundleID)
	if err != nil {
		return IncidentRecord{}, err
	}
	artifacts, err := incidentArtifactModels(request.Artifacts)
	if err != nil {
		return IncidentRecord{}, err
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return IncidentRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "create_incident", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		if _, ok := c.sessions[request.SessionID]; !ok {
			return nil, ErrNotFound
		}
		if request.LeaseID != "" {
			lease, ok := c.leases[request.LeaseID]
			if !ok || lease.SessionID != request.SessionID {
				return nil, ErrScope
			}
		}
		if request.AgentWorkspaceID != "" {
			agent, ok := c.agents[request.AgentWorkspaceID]
			if !ok || agent.SessionID != request.SessionID {
				return nil, ErrScope
			}
			if request.AgentGeneration > 0 {
				if _, err := findAgentGeneration(&agent, request.AgentGeneration); err != nil {
					return nil, ErrScope
				}
			}
		}
		var execution *ExecRecord
		if request.ExecID != "" {
			value, ok := detachedRecord(c.execs, request.ExecID, cloneExec)
			if !ok || value.SessionID != request.SessionID || value.LeaseID != request.LeaseID || value.AgentWorkspaceID != request.AgentWorkspaceID || value.AgentGeneration != request.AgentGeneration {
				return nil, ErrScope
			}
			execution = &value
		}
		var target *TargetRecord
		if request.TargetID != "" {
			value, ok := detachedRecord(c.targets, request.TargetID, cloneTarget)
			if !ok || value.SessionID != request.SessionID {
				return nil, ErrScope
			}
			target = &value
			if request.TargetGeneration > 0 {
				if _, err := findTargetGeneration(target, request.TargetGeneration); err != nil {
					return nil, ErrScope
				}
			}
			if request.TargetRunID != "" {
				run, err := findRun(target, request.TargetRunID)
				if err != nil || run.Generation != request.TargetGeneration {
					return nil, ErrScope
				}
				if request.AgentWorkspaceID != "" && (run.AgentWorkspaceID != request.AgentWorkspaceID || run.AgentGeneration != request.AgentGeneration) {
					return nil, ErrScope
				}
			}
		}
		now := c.clock().UTC()
		incidentID, err := c.ids.IncidentID()
		if err != nil {
			return nil, err
		}
		model, err := domain.NewIncident(domain.IncidentSpec{ID: incidentID, Classification: request.Classification, ResearchSessionID: sessionID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(request.AgentGeneration), ExecID: execID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(request.TargetGeneration), TargetRunID: runID, Trigger: request.Trigger, LastKnownState: request.LastKnownState, Cause: cause, HighWaterMetrics: highWaterMetrics, FirstRelevantCursor: domain.ObservationCursor(request.FirstRelevantCursor), LastRelevantCursor: domain.ObservationCursor(request.LastRelevantCursor), Coverage: coverage, ObservationBundleID: bundleID, Artifacts: artifacts, OccurredAt: now})
		if err != nil {
			return nil, err
		}
		incident := IncidentRecord{ID: incidentID.String(), Classification: request.Classification, SessionID: request.SessionID, LeaseID: request.LeaseID, AgentWorkspaceID: request.AgentWorkspaceID, AgentGeneration: request.AgentGeneration, ExecID: request.ExecID, TargetID: request.TargetID, TargetGeneration: request.TargetGeneration, TargetRunID: request.TargetRunID, Trigger: request.Trigger, LastKnownState: request.LastKnownState, Cause: request.Cause, HighWaterMetrics: append([]IncidentMetricRecord(nil), request.HighWaterMetrics...), FirstRelevantCursor: request.FirstRelevantCursor, LastRelevantCursor: request.LastRelevantCursor, Coverage: append([]IncidentCoverageRecord(nil), request.Coverage...), ObservationBundleID: request.ObservationBundleID, Artifacts: append([]IncidentArtifactRecord(nil), request.Artifacts...), State: model.State(), Revision: uint64(model.Revision()), OccurredAt: now, UpdatedAt: now}
		incident = cloneIncident(incident)
		if err := appendControl(ctx, tx, "incident", incident.ID, "incident.opened", incident.Revision, incident); err != nil {
			return nil, err
		}
		if target != nil && request.TargetRunID != "" {
			run, _ := findRun(target, request.TargetRunID)
			run.IncidentIDs, err = mergedNonBlank(run.IncidentIDs, incident.ID)
			if err != nil {
				return nil, err
			}
			target.Revision++
			target.UpdatedAt = now
			if err := appendControl(ctx, tx, "target", target.ID, "target_run.incident_linked", target.Revision, *target); err != nil {
				return nil, err
			}
		}
		if execution != nil {
			execution.IncidentIDs, err = mergedNonBlank(execution.IncidentIDs, incident.ID)
			if err != nil {
				return nil, err
			}
			execution.Revision++
			execution.UpdatedAt = now
			if err := appendControl(ctx, tx, "exec", execution.ID, "exec.incident_linked", execution.Revision, *execution); err != nil {
				return nil, err
			}
		}
		return json.Marshal(incidentResult{IncidentID: incident.ID})
	})
	if err != nil {
		return IncidentRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return IncidentRecord{}, err
	}
	var result incidentResult
	if err := json.Unmarshal(response, &result); err != nil {
		return IncidentRecord{}, err
	}
	return cloneIncident(c.incidents[result.IncidentID]), nil
}

func optionalLeaseID(value string) (domain.LeaseID, error) {
	if value == "" {
		return domain.LeaseID{}, nil
	}
	return domain.ParseLeaseID(value)
}
func optionalAgentID(value string) (domain.AgentWorkspaceID, error) {
	if value == "" {
		return domain.AgentWorkspaceID{}, nil
	}
	return domain.ParseAgentWorkspaceID(value)
}
func optionalExecID(value string) (domain.ExecID, error) {
	if value == "" {
		return domain.ExecID{}, nil
	}
	return domain.ParseExecID(value)
}
func optionalTargetID(value string) (domain.TargetID, error) {
	if value == "" {
		return domain.TargetID{}, nil
	}
	return domain.ParseTargetID(value)
}
func optionalRunID(value string) (domain.TargetRunID, error) {
	if value == "" {
		return domain.TargetRunID{}, nil
	}
	return domain.ParseTargetRunID(value)
}

func optionalBundleID(value string) (domain.ObservationBundleID, error) {
	if value == "" {
		return domain.ObservationBundleID{}, nil
	}
	return domain.ParseObservationBundleID(value)
}

func incidentMetricModels(records []IncidentMetricRecord) ([]domain.MetricSample, error) {
	result := make([]domain.MetricSample, len(records))
	for index, record := range records {
		subjectID, err := domain.ParseSubjectID(record.SubjectID)
		if err != nil {
			return nil, fmt.Errorf("high water metric %d subject: %w", index, err)
		}
		execID, err := optionalExecID(record.ExecID)
		if err != nil {
			return nil, fmt.Errorf("high water metric %d exec: %w", index, err)
		}
		runID, err := optionalRunID(record.TargetRunID)
		if err != nil {
			return nil, fmt.Errorf("high water metric %d target run: %w", index, err)
		}
		result[index], err = domain.NewMetricSample(domain.MetricSampleSpec{SubjectID: subjectID, SubjectKind: record.SubjectKind, Name: record.Name, Unit: record.Unit, Kind: record.Kind, Availability: record.Availability, CounterValue: record.CounterValue, NumericValue: record.NumericValue, CollectedAt: record.CollectedAt, PublishedAt: record.PublishedAt, Cursor: domain.ObservationCursor(record.Cursor), Labels: cloneStringMap(record.Labels), ExecID: execID, TargetRunID: runID})
		if err != nil {
			return nil, fmt.Errorf("high water metric %d: %w", index, err)
		}
	}
	return result, nil
}

func incidentCoverageModels(records []IncidentCoverageRecord) ([]domain.CollectorCoverage, error) {
	result := make([]domain.CollectorCoverage, len(records))
	for index, record := range records {
		collectorID, err := domain.ParseCollectorID(record.CollectorID)
		if err != nil {
			return nil, fmt.Errorf("coverage %d collector: %w", index, err)
		}
		gaps := make([]domain.Gap, len(record.Gaps))
		for gapIndex, gap := range record.Gaps {
			gaps[gapIndex], err = domain.NewGap(domain.GapSpec{Kind: gap.Kind, Source: gap.Source, SourceInstance: gap.SourceInstance, FirstSourceSequence: gap.FirstSourceSequence, LastSourceSequence: gap.LastSourceSequence, FirstCursor: domain.ObservationCursor(gap.FirstCursor), LastCursor: domain.ObservationCursor(gap.LastCursor), StartedAt: gap.StartedAt, EndedAt: gap.EndedAt, LostRecords: gap.LostRecords, Reason: gap.Reason})
			if err != nil {
				return nil, fmt.Errorf("coverage %d gap %d: %w", index, gapIndex, err)
			}
		}
		result[index], err = domain.NewCollectorCoverage(domain.CollectorCoverageSpec{CollectorID: collectorID, SignalFamily: record.SignalFamily, Placement: record.Placement, Level: record.Level, Status: record.Status, Required: record.Required, StartedAt: record.StartedAt, EndedAt: record.EndedAt, DroppedRecords: record.DroppedRecords, Gaps: gaps})
		if err != nil {
			return nil, fmt.Errorf("coverage %d: %w", index, err)
		}
	}
	return result, nil
}

func incidentArtifactModels(records []IncidentArtifactRecord) ([]domain.ArtifactReference, error) {
	result := make([]domain.ArtifactReference, len(records))
	for index, record := range records {
		digest, err := domain.ParseDigest(record.Digest)
		if err != nil {
			return nil, fmt.Errorf("artifact %d digest: %w", index, err)
		}
		result[index], err = domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: record.Reference, Digest: digest, Size: record.Size, Role: record.Role, Sensitivity: record.Sensitivity})
		if err != nil {
			return nil, fmt.Errorf("artifact %d: %w", index, err)
		}
	}
	return result, nil
}

func normalizedNonBlank(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, invalidArgument("normalize_non_blank", "values", "must not contain blank entries", nil)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func mergedNonBlank(current []string, additional ...string) ([]string, error) {
	values := append(append([]string(nil), current...), additional...)
	return normalizedNonBlank(values)
}

type TransitionIncidentRequest struct {
	Meta                       MutationMeta         `json:"meta"`
	IncidentID                 string               `json:"incident_id"`
	ExpectedRevision           uint64               `json:"expected_revision"`
	State                      domain.IncidentState `json:"state"`
	RecoveryActions            []string             `json:"recovery_actions,omitempty"`
	VisibilityAcknowledgements []string             `json:"visibility_acknowledgements,omitempty"`
}

func (c *Core) TransitionIncident(ctx context.Context, request TransitionIncidentRequest) (IncidentRecord, error) {
	const operation = "incident.transition"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return IncidentRecord{}, err
	}
	if _, err := domain.ParseIncidentID(request.IncidentID); err != nil {
		return IncidentRecord{}, err
	}
	if request.State == domain.IncidentRecovering {
		return IncidentRecord{}, failedPrecondition(operation, "state", "recovery must use RecoverIncident so generation rollover is atomic", nil)
	}
	var err error
	request.RecoveryActions, err = normalizedNonBlank(request.RecoveryActions)
	if err != nil {
		return IncidentRecord{}, err
	}
	request.VisibilityAcknowledgements, err = normalizedNonBlank(request.VisibilityAcknowledgements)
	if err != nil {
		return IncidentRecord{}, err
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return IncidentRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "transition_incident", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		incident, ok := detachedRecord(c.incidents, request.IncidentID, cloneIncident)
		if !ok {
			return nil, ErrNotFound
		}
		if incident.Revision != request.ExpectedRevision {
			return nil, store.ErrRevisionConflict
		}
		if request.State == domain.IncidentEvidenceSealed && len(incident.Artifacts) == 0 && incident.ObservationBundleID == "" {
			return nil, failedPrecondition(operation, "evidence", "cannot be sealed without a typed artifact or observation bundle", nil)
		}
		if err := domain.RequireIncidentTransition(incident.State, request.State); err != nil {
			return nil, err
		}
		incident.State, incident.Revision, incident.UpdatedAt = request.State, incident.Revision+1, c.clock().UTC()
		incident.RecoveryActions = append([]string(nil), request.RecoveryActions...)
		incident.VisibilityAcknowledgements = append([]string(nil), request.VisibilityAcknowledgements...)
		if err := appendControl(ctx, tx, "incident", incident.ID, "incident.transitioned", incident.Revision, incident); err != nil {
			return nil, err
		}
		return json.Marshal(incident)
	})
	if err != nil {
		return IncidentRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return IncidentRecord{}, err
	}
	var incident IncidentRecord
	if err := json.Unmarshal(response, &incident); err != nil {
		return IncidentRecord{}, err
	}
	return cloneIncident(incident), nil
}

type RecoveryResource string

const (
	RecoveryResourceTarget RecoveryResource = "target"
	RecoveryResourceAgent  RecoveryResource = "agent_workspace"
)

func (r RecoveryResource) valid() bool {
	return r == RecoveryResourceTarget || r == RecoveryResourceAgent
}

type RecoverIncidentRequest struct {
	Meta                      MutationMeta     `json:"meta"`
	IncidentID                string           `json:"incident_id"`
	ExpectedIncidentRevision  uint64           `json:"expected_incident_revision"`
	Resource                  RecoveryResource `json:"resource"`
	Strategy                  string           `json:"strategy"`
	VisibilityAcknowledgement string           `json:"visibility_acknowledgement"`
}

type RecoveryOutcome struct {
	Incident IncidentRecord        `json:"incident"`
	Target   *TargetRecord         `json:"target,omitempty"`
	Agent    *AgentWorkspaceRecord `json:"agent_workspace,omitempty"`
	Lease    *LeaseRecord          `json:"lease,omitempty"`
}

// RecoverIncident atomically makes the sealed incident visible as recovering
// and creates a new generation for only the affected resource.
func (c *Core) RecoverIncident(ctx context.Context, request RecoverIncidentRequest) (RecoveryOutcome, error) {
	const operation = "incident.recover"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return RecoveryOutcome{}, err
	}
	incidentID, err := domain.ParseIncidentID(request.IncidentID)
	if err != nil {
		return RecoveryOutcome{}, err
	}
	request.Strategy = strings.TrimSpace(request.Strategy)
	request.VisibilityAcknowledgement = strings.TrimSpace(request.VisibilityAcknowledgement)
	if !request.Resource.valid() || request.Strategy == "" || request.VisibilityAcknowledgement == "" {
		return RecoveryOutcome{}, invalidArgument(operation, "recovery", "recognized resource, strategy, and visibility acknowledgement are required", nil)
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return RecoveryOutcome{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return RecoveryOutcome{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "recover_incident", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		incident, ok := detachedRecord(c.incidents, request.IncidentID, cloneIncident)
		if !ok {
			return nil, ErrNotFound
		}
		if incident.Revision != request.ExpectedIncidentRevision {
			return nil, store.ErrRevisionConflict
		}
		if incident.State != domain.IncidentEvidenceSealed {
			return nil, failedPrecondition(operation, "incident", "evidence must be sealed before recovery", nil)
		}
		lease, ok := c.leases[incident.LeaseID]
		if !ok || lease.SessionID != incident.SessionID {
			return nil, ErrScope
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		if err := domain.RequireIncidentTransition(incident.State, domain.IncidentRecovering); err != nil {
			return nil, err
		}
		incident.State, incident.Revision, incident.UpdatedAt = domain.IncidentRecovering, incident.Revision+1, now
		incident.RecoveryActions, err = mergedNonBlank(incident.RecoveryActions, string(request.Resource)+":"+request.Strategy)
		if err != nil {
			return nil, err
		}
		incident.VisibilityAcknowledgements, err = mergedNonBlank(incident.VisibilityAcknowledgements, request.VisibilityAcknowledgement)
		if err != nil {
			return nil, err
		}
		if err := appendControl(ctx, tx, "incident", incident.ID, "incident.recovery_started", incident.Revision, incident); err != nil {
			return nil, err
		}

		outcome := RecoveryOutcome{Incident: incident}
		switch request.Resource {
		case RecoveryResourceTarget:
			target, err := c.recoverTargetGeneration(ctx, tx, incidentID, incident, lease, now)
			if err != nil {
				return nil, err
			}
			outcome.Target = &target
		case RecoveryResourceAgent:
			agent, updatedLease, err := c.recoverAgentGeneration(ctx, tx, incidentID, incident, lease, now)
			if err != nil {
				return nil, err
			}
			outcome.Agent = &agent
			outcome.Lease = &updatedLease
		}
		return json.Marshal(outcome)
	})
	if err != nil {
		return RecoveryOutcome{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return RecoveryOutcome{}, err
	}
	var outcome RecoveryOutcome
	if err := json.Unmarshal(response, &outcome); err != nil {
		return RecoveryOutcome{}, err
	}
	return outcome, nil
}

func (c *Core) recoverTargetGeneration(ctx context.Context, tx *store.Tx, incidentID domain.IncidentID, incident IncidentRecord, lease LeaseRecord, now time.Time) (TargetRecord, error) {
	const operation = "incident.recover_target"
	if incident.TargetID == "" || incident.TargetGeneration == 0 {
		return TargetRecord{}, failedPrecondition(operation, "incident", "does not identify a target generation", nil)
	}
	target, ok := detachedRecord(c.targets, incident.TargetID, cloneTarget)
	if !ok || target.SessionID != incident.SessionID || target.LeaseID != lease.ID {
		return TargetRecord{}, ErrScope
	}
	if target.CurrentGeneration != incident.TargetGeneration {
		return TargetRecord{}, failedPrecondition(operation, "target_generation", "incident generation is no longer current", nil)
	}
	for _, run := range target.Runs {
		if run.Generation != target.CurrentGeneration {
			continue
		}
		if !run.State.Terminal() || run.BundleID == "" {
			return TargetRecord{}, failedPrecondition(operation, "target_run", fmt.Sprintf("%s must be finalized with an observation bundle before recovery", run.ID), nil)
		}
	}
	old, err := findTargetGeneration(&target, target.CurrentGeneration)
	if err != nil {
		return TargetRecord{}, err
	}
	path, err := targetGenerationRetirementPath(old.State)
	if err != nil {
		return TargetRecord{}, err
	}
	if err := appendTargetGenerationTransitions(ctx, tx, &target, old, path, "target_generation.recovery_retirement", now); err != nil {
		return TargetRecord{}, err
	}
	policyDigest, err := domain.ParseDigest(old.PolicyDigest)
	if err != nil {
		return TargetRecord{}, err
	}
	capabilityDigest, err := domain.ParseDigest(old.CapabilityDigest)
	if err != nil {
		return TargetRecord{}, err
	}
	newGeneration := old.Generation + 1
	targetID, err := requireStoredID("target.id", target.ID, domain.ParseTargetID)
	if err != nil {
		return TargetRecord{}, err
	}
	model, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{TargetID: targetID, Generation: domain.TargetGeneration(newGeneration), PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, PreviousGeneration: domain.TargetGeneration(old.Generation), RecoveryIncidentID: incidentID, CreatedAt: now})
	if err != nil {
		return TargetRecord{}, err
	}
	target.CurrentGeneration = newGeneration
	target.Generations = append(target.Generations, TargetGenerationRecord{Generation: newGeneration, PolicyDigest: old.PolicyDigest, CapabilityDigest: old.CapabilityDigest, Previous: old.Generation, RecoveryIncident: incident.ID, State: model.State(), Revision: uint64(model.Revision()), CreatedAt: now, UpdatedAt: now})
	target.Revision++
	target.UpdatedAt = now
	if err := appendControl(ctx, tx, "target", target.ID, "target_generation.recovery_created", target.Revision, target); err != nil {
		return TargetRecord{}, err
	}
	return target, nil
}

func (c *Core) recoverAgentGeneration(ctx context.Context, tx *store.Tx, incidentID domain.IncidentID, incident IncidentRecord, lease LeaseRecord, now time.Time) (AgentWorkspaceRecord, LeaseRecord, error) {
	const operation = "incident.recover_agent"
	if incident.AgentWorkspaceID == "" || incident.AgentGeneration == 0 {
		return AgentWorkspaceRecord{}, LeaseRecord{}, failedPrecondition(operation, "incident", "does not identify an agent generation", nil)
	}
	agent, ok := detachedRecord(c.agents, incident.AgentWorkspaceID, cloneAgent)
	if !ok || agent.SessionID != incident.SessionID || lease.AgentWorkspaceID != agent.ID {
		return AgentWorkspaceRecord{}, LeaseRecord{}, ErrScope
	}
	if agent.CurrentGeneration != incident.AgentGeneration || lease.AgentGeneration != incident.AgentGeneration {
		return AgentWorkspaceRecord{}, LeaseRecord{}, failedPrecondition(operation, "agent_generation", "incident generation is no longer current", nil)
	}
	for _, execID := range c.execIDsForLease(lease.ID) {
		execution := c.execs[execID]
		if execution.AgentGeneration == agent.CurrentGeneration && !execution.State.Terminal() {
			return AgentWorkspaceRecord{}, LeaseRecord{}, failedPrecondition(operation, "exec", fmt.Sprintf("%s must be finalized before agent recovery", execID), nil)
		}
	}
	for _, targetID := range c.targetIDsForLease(lease.ID) {
		target := c.targets[targetID]
		for _, run := range target.Runs {
			if run.AgentWorkspaceID == agent.ID && run.AgentGeneration == agent.CurrentGeneration && (!run.State.Terminal() || run.BundleID == "") {
				return AgentWorkspaceRecord{}, LeaseRecord{}, failedPrecondition(operation, "target_run", fmt.Sprintf("%s must be finalized with an observation bundle before agent recovery", run.ID), nil)
			}
		}
	}
	old, err := findAgentGeneration(&agent, agent.CurrentGeneration)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	path, err := agentGenerationRetirementPath(old.State)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	if err := appendAgentGenerationTransitions(ctx, tx, &agent, old, path, "agent_generation.recovery_retirement", now); err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	workspaceID, err := c.ids.WorkspaceID()
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	inputViewID, err := domain.ParseInputViewID(old.InputViewID)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	policyDigest, err := domain.ParseDigest(old.PolicyDigest)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	capabilityDigest, err := domain.ParseDigest(old.CapabilityDigest)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	newGeneration := old.Generation + 1
	agentID, err := requireStoredID("agent_workspace.id", agent.ID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	model, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{AgentWorkspaceID: agentID, Generation: domain.AgentGeneration(newGeneration), WorkspaceID: workspaceID, InputViewID: inputViewID, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, PreviousGeneration: domain.AgentGeneration(old.Generation), RecoveryIncidentID: incidentID, CreatedAt: now})
	if err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	agent.CurrentGeneration = newGeneration
	agent.Generations = append(agent.Generations, AgentGenerationRecord{Generation: newGeneration, WorkspaceID: workspaceID.String(), InputViewID: old.InputViewID, PolicyDigest: old.PolicyDigest, CapabilityDigest: old.CapabilityDigest, Previous: old.Generation, RecoveryIncident: incident.ID, State: model.State(), Revision: uint64(model.Revision()), CreatedAt: now, UpdatedAt: now})
	agent.Revision++
	agent.UpdatedAt = now
	if err := appendControl(ctx, tx, "agent_workspace", agent.ID, "agent_generation.recovery_created", agent.Revision, agent); err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	lease.AgentGeneration = newGeneration
	lease.Revision++
	lease.UpdatedAt = now
	if err := appendControl(ctx, tx, "lease", lease.ID, "lease.agent_generation_changed", lease.Revision, lease); err != nil {
		return AgentWorkspaceRecord{}, LeaseRecord{}, err
	}
	return agent, lease, nil
}

func (c *Core) GetIncident(ctx context.Context, incidentID string) (IncidentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return IncidentRecord{}, err
	}
	incident, ok := c.incidents[incidentID]
	if !ok {
		return IncidentRecord{}, ErrNotFound
	}
	return cloneIncident(incident), nil
}
