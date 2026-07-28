package application

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type MutationMeta struct {
	IdempotencyKey            string `json:"idempotency_key"`
	CorrelationID             string `json:"correlation_id"`
	CausationID               string `json:"causation_id,omitempty"`
	AuthorizedPolicyReference string `json:"authorized_policy_reference"`
	// Deadline bounds this attempt; it is deliberately excluded from the
	// canonical request bytes used for idempotency. A crash-safe retry must be
	// able to supply a fresh deadline without changing the logical mutation.
	Deadline time.Time `json:"-"`
}

func (m MutationMeta) Validate(ctx context.Context, now time.Time) error {
	const operation = "mutation_meta.validate"
	if !domain.IsCanonicalIdempotencyKey(m.IdempotencyKey) {
		return invalidArgument(operation, "idempotency_key", "must be a canonical non-blank value of at most 1024 bytes", nil)
	}
	if strings.TrimSpace(m.AuthorizedPolicyReference) == "" {
		return invalidArgument(operation, "authorized_policy_reference", "is required", nil)
	}
	if _, err := domain.ParseCorrelationID(m.CorrelationID); err != nil {
		return invalidArgument(operation, "correlation_id", "is invalid", err)
	}
	if m.CausationID != "" {
		if _, err := domain.ParseEventID(m.CausationID); err != nil {
			return invalidArgument(operation, "causation_id", "is invalid", err)
		}
	}
	if m.Deadline.IsZero() || !m.Deadline.After(now) {
		return invalidArgument(operation, "deadline", "must be in the future", nil)
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.After(m.Deadline) {
		return invalidArgument(operation, "deadline", "context deadline exceeds the declared mutation deadline", nil)
	}
	return nil
}

type SessionRecord struct {
	ID                        string                      `json:"id"`
	OwnerSubject              string                      `json:"owner_subject"`
	AcquisitionIdempotencyKey string                      `json:"acquisition_idempotency_key"`
	State                     domain.ResearchSessionState `json:"state"`
	Revision                  uint64                      `json:"revision"`
	LeaseID                   string                      `json:"lease_id"`
	AgentWorkspaceID          string                      `json:"agent_workspace_id"`
	InputViewID               string                      `json:"input_view_id"`
	PolicyDigest              string                      `json:"policy_digest"`
	CapabilityDigest          string                      `json:"capability_digest"`
	CreatedAt                 time.Time                   `json:"created_at"`
	UpdatedAt                 time.Time                   `json:"updated_at"`
}

type LeaseRecord struct {
	ID               string                 `json:"id"`
	SessionID        string                 `json:"session_id"`
	AgentWorkspaceID string                 `json:"agent_workspace_id"`
	AgentGeneration  uint64                 `json:"agent_generation"`
	InputViewID      string                 `json:"input_view_id"`
	PolicyDigest     string                 `json:"policy_digest"`
	CapabilityDigest string                 `json:"capability_digest"`
	State            domain.LeaseState      `json:"state"`
	Revision         uint64                 `json:"revision"`
	ExpiresAt        time.Time              `json:"expires_at"`
	Termination      LeaseTerminationRecord `json:"termination"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
}

// LeaseTerminationKind records why a lease stopped accepting new work. It is
// deliberately separate from domain.LeaseState: the domain state machine uses
// LeaseReleasing for a requested release, while an overdue active lease keeps
// its domain state until physical cleanup has completed. The durable intent
// still exposes that intermediate lease as explicitly expiring.
type LeaseTerminationKind string

const (
	LeaseTerminationRelease LeaseTerminationKind = "release"
	LeaseTerminationExpiry  LeaseTerminationKind = "expiry"
)

func (k LeaseTerminationKind) IsValid() bool {
	return k == LeaseTerminationRelease || k == LeaseTerminationExpiry
}

type LeaseTerminationState string

const (
	LeaseTerminationReleasing LeaseTerminationState = "releasing"
	LeaseTerminationExpiring  LeaseTerminationState = "expiring"
	LeaseTerminationReleased  LeaseTerminationState = "released"
	LeaseTerminationExpired   LeaseTerminationState = "expired"
)

func (s LeaseTerminationState) IsValid() bool {
	switch s {
	case LeaseTerminationReleasing, LeaseTerminationExpiring, LeaseTerminationReleased, LeaseTerminationExpired:
		return true
	default:
		return false
	}
}

func (s LeaseTerminationState) Terminal() bool {
	return s == LeaseTerminationReleased || s == LeaseTerminationExpired
}

// LeaseTerminationRecord is the durable hand-off between logical admission
// and physical teardown. BeginRequestDigest intentionally excludes the caller
// deadline, because a controller may have to resume cleanup under a new
// controller-owned deadline after a crash. All other request identity remains
// immutable and is checked on replay.
type LeaseTerminationRecord struct {
	Kind                   LeaseTerminationKind  `json:"kind,omitempty"`
	State                  LeaseTerminationState `json:"state,omitempty"`
	Reason                 string                `json:"reason,omitempty"`
	BeginIdempotencyKey    string                `json:"begin_idempotency_key,omitempty"`
	BeginRequestDigest     string                `json:"begin_request_digest,omitempty"`
	InitiatedLeaseRevision uint64                `json:"initiated_lease_revision,omitempty"`
	InitiatedAt            time.Time             `json:"initiated_at,omitempty"`
	CompleteIdempotencyKey string                `json:"complete_idempotency_key,omitempty"`
	CompleteRequestDigest  string                `json:"complete_request_digest,omitempty"`
	CompletedAt            time.Time             `json:"completed_at,omitempty"`
}

func (r LeaseTerminationRecord) Empty() bool {
	return r == (LeaseTerminationRecord{})
}

func (r LeaseTerminationRecord) InProgress() bool {
	return r.State == LeaseTerminationReleasing || r.State == LeaseTerminationExpiring
}

type ExecRecord struct {
	ID               string          `json:"id"`
	SessionID        string          `json:"session_id"`
	LeaseID          string          `json:"lease_id"`
	AgentWorkspaceID string          `json:"agent_workspace_id"`
	AgentGeneration  uint64          `json:"agent_generation"`
	Kind             domain.ExecKind `json:"kind"`
	Executable       string          `json:"executable"`
	// Argv contains only the arguments after argv[0].
	Argv             []string         `json:"argv,omitempty"`
	WorkingDirectory string           `json:"working_directory,omitempty"`
	State            domain.ExecState `json:"state"`
	Revision         uint64           `json:"revision"`
	ExitCode         *int             `json:"exit_code,omitempty"`
	Signal           string           `json:"signal,omitempty"`
	IncidentIDs      []string         `json:"incident_ids,omitempty"`
	CleanupConfirmed bool             `json:"cleanup_confirmed"`
	Error            string           `json:"error,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
}

type AgentWorkspaceRecord struct {
	ID                string                  `json:"id"`
	SessionID         string                  `json:"session_id"`
	CurrentGeneration uint64                  `json:"current_generation"`
	Revision          uint64                  `json:"revision"`
	CreatedAt         time.Time               `json:"created_at"`
	UpdatedAt         time.Time               `json:"updated_at"`
	Generations       []AgentGenerationRecord `json:"generations"`
}

type AgentGenerationRecord struct {
	Generation               uint64                      `json:"generation"`
	WorkspaceID              string                      `json:"workspace_id"`
	InputViewID              string                      `json:"input_view_id"`
	PolicyDigest             string                      `json:"policy_digest"`
	CapabilityDigest         string                      `json:"capability_digest"`
	ProvisioningPlanDigest   string                      `json:"provisioning_plan_digest,omitempty"`
	WorkspaceProvisioningKey string                      `json:"workspace_provisioning_key,omitempty"`
	AgentProvisioningKey     string                      `json:"agent_provisioning_key,omitempty"`
	Previous                 uint64                      `json:"previous,omitempty"`
	RecoveryIncident         string                      `json:"recovery_incident,omitempty"`
	State                    domain.AgentGenerationState `json:"state"`
	Revision                 uint64                      `json:"revision"`
	CreatedAt                time.Time                   `json:"created_at"`
	UpdatedAt                time.Time                   `json:"updated_at"`
}

type TargetRecord struct {
	ID                     string                   `json:"id"`
	SessionID              string                   `json:"session_id"`
	LeaseID                string                   `json:"lease_id"`
	CreationIdempotencyKey string                   `json:"creation_idempotency_key"`
	Template               string                   `json:"template"`
	Kind                   domain.TargetKind        `json:"kind"`
	CurrentGeneration      uint64                   `json:"current_generation"`
	Revision               uint64                   `json:"revision"`
	CreatedAt              time.Time                `json:"created_at"`
	UpdatedAt              time.Time                `json:"updated_at"`
	Generations            []TargetGenerationRecord `json:"generations"`
	Runs                   []TargetRunRecord        `json:"runs"`
	Operations             []TargetOperationRecord  `json:"operations"`
}

type TargetGenerationRecord struct {
	Generation             uint64                       `json:"generation"`
	PolicyDigest           string                       `json:"policy_digest"`
	CapabilityDigest       string                       `json:"capability_digest"`
	ProvisioningPlanDigest string                       `json:"provisioning_plan_digest,omitempty"`
	ProvisioningKey        string                       `json:"provisioning_key,omitempty"`
	Previous               uint64                       `json:"previous,omitempty"`
	RecoveryIncident       string                       `json:"recovery_incident,omitempty"`
	State                  domain.TargetGenerationState `json:"state"`
	Revision               uint64                       `json:"revision"`
	CreatedAt              time.Time                    `json:"created_at"`
	UpdatedAt              time.Time                    `json:"updated_at"`
}

type TargetRunRecord struct {
	ID                     string                `json:"id"`
	Generation             uint64                `json:"generation"`
	AgentWorkspaceID       string                `json:"agent_workspace_id"`
	AgentGeneration        uint64                `json:"agent_generation"`
	MaterializationDigest  string                `json:"materialization_digest"`
	ProvisioningPlanDigest string                `json:"provisioning_plan_digest,omitempty"`
	ProvisioningKey        string                `json:"provisioning_key,omitempty"`
	State                  domain.TargetRunState `json:"state"`
	Revision               uint64                `json:"revision"`
	BundleID               string                `json:"bundle_id,omitempty"`
	BundleArtifact         string                `json:"bundle_artifact,omitempty"`
	BundleDigest           string                `json:"bundle_digest,omitempty"`
	IncidentIDs            []string              `json:"incident_ids,omitempty"`
	CreatedAt              time.Time             `json:"created_at"`
	UpdatedAt              time.Time             `json:"updated_at"`
}

type TargetOperationRecord struct {
	ID             string                      `json:"id"`
	RunID          string                      `json:"run_id"`
	Generation     uint64                      `json:"generation"`
	Kind           domain.TargetOperationKind  `json:"kind"`
	CommandDisplay string                      `json:"command_display,omitempty"`
	ContentDigest  string                      `json:"content_digest,omitempty"`
	State          domain.TargetOperationState `json:"state"`
	Revision       uint64                      `json:"revision"`
	CreatedAt      time.Time                   `json:"created_at"`
	UpdatedAt      time.Time                   `json:"updated_at"`
}

type ResearchSessionView struct {
	Session   SessionRecord        `json:"session"`
	Lease     LeaseRecord          `json:"lease"`
	Agent     AgentWorkspaceRecord `json:"agent_workspace"`
	Execs     []ExecRecord         `json:"execs,omitempty"`
	Targets   []TargetRecord       `json:"targets"`
	Incidents []IncidentRecord     `json:"incidents,omitempty"`
}

func cloneExec(value ExecRecord) ExecRecord {
	value.Argv = cloneSlice(value.Argv)
	value.IncidentIDs = cloneSlice(value.IncidentIDs)
	if value.ExitCode != nil {
		exitCode := *value.ExitCode
		value.ExitCode = &exitCode
	}
	return value
}

type CauseRecord struct {
	Kind       domain.CauseKind `json:"kind"`
	Summary    string           `json:"summary"`
	Method     string           `json:"method,omitempty"`
	Confidence float64          `json:"confidence"`
}

type IncidentMetricRecord struct {
	SubjectID    string                    `json:"subject_id"`
	SubjectKind  domain.SubjectKind        `json:"subject_kind"`
	Name         string                    `json:"name"`
	Unit         string                    `json:"unit"`
	Kind         domain.MetricKind         `json:"kind"`
	Availability domain.MetricAvailability `json:"availability"`
	CounterValue *uint64                   `json:"counter_value,omitempty"`
	NumericValue *float64                  `json:"numeric_value,omitempty"`
	CollectedAt  time.Time                 `json:"collected_at"`
	PublishedAt  time.Time                 `json:"published_at"`
	Cursor       uint64                    `json:"cursor"`
	Labels       map[string]string         `json:"labels,omitempty"`
	ExecID       string                    `json:"exec_id,omitempty"`
	TargetRunID  string                    `json:"target_run_id,omitempty"`
}

type IncidentGapRecord struct {
	Kind                domain.GapKind `json:"kind"`
	Source              string         `json:"source"`
	SourceInstance      string         `json:"source_instance,omitempty"`
	FirstSourceSequence uint64         `json:"first_source_sequence,omitempty"`
	LastSourceSequence  uint64         `json:"last_source_sequence,omitempty"`
	FirstCursor         uint64         `json:"first_cursor,omitempty"`
	LastCursor          uint64         `json:"last_cursor,omitempty"`
	StartedAt           time.Time      `json:"started_at,omitempty"`
	EndedAt             time.Time      `json:"ended_at,omitempty"`
	LostRecords         uint64         `json:"lost_records,omitempty"`
	Reason              string         `json:"reason"`
}

type IncidentCoverageRecord struct {
	CollectorID    string                    `json:"collector_id"`
	SignalFamily   string                    `json:"signal_family"`
	Placement      domain.CollectorPlacement `json:"placement"`
	Level          domain.CoverageLevel      `json:"level"`
	Status         domain.CoverageStatus     `json:"status"`
	Required       bool                      `json:"required"`
	StartedAt      time.Time                 `json:"started_at,omitempty"`
	EndedAt        time.Time                 `json:"ended_at,omitempty"`
	DroppedRecords uint64                    `json:"dropped_records,omitempty"`
	Gaps           []IncidentGapRecord       `json:"gaps,omitempty"`
}

type IncidentArtifactRecord struct {
	Reference   string             `json:"reference"`
	Digest      string             `json:"digest"`
	Size        int64              `json:"size"`
	Role        string             `json:"role"`
	Sensitivity domain.Sensitivity `json:"sensitivity"`
}

type IncidentRecord struct {
	ID                         string                        `json:"id"`
	Classification             domain.IncidentClassification `json:"classification"`
	SessionID                  string                        `json:"session_id"`
	LeaseID                    string                        `json:"lease_id,omitempty"`
	AgentWorkspaceID           string                        `json:"agent_workspace_id,omitempty"`
	AgentGeneration            uint64                        `json:"agent_generation,omitempty"`
	ExecID                     string                        `json:"exec_id,omitempty"`
	TargetID                   string                        `json:"target_id,omitempty"`
	TargetGeneration           uint64                        `json:"target_generation,omitempty"`
	TargetRunID                string                        `json:"target_run_id,omitempty"`
	Trigger                    string                        `json:"trigger"`
	LastKnownState             string                        `json:"last_known_state"`
	Cause                      CauseRecord                   `json:"cause"`
	HighWaterMetrics           []IncidentMetricRecord        `json:"high_water_metrics,omitempty"`
	FirstRelevantCursor        uint64                        `json:"first_relevant_cursor,omitempty"`
	LastRelevantCursor         uint64                        `json:"last_relevant_cursor,omitempty"`
	Coverage                   []IncidentCoverageRecord      `json:"coverage,omitempty"`
	ObservationBundleID        string                        `json:"observation_bundle_id,omitempty"`
	Artifacts                  []IncidentArtifactRecord      `json:"artifacts,omitempty"`
	RecoveryActions            []string                      `json:"recovery_actions,omitempty"`
	VisibilityAcknowledgements []string                      `json:"visibility_acknowledgements,omitempty"`
	State                      domain.IncidentState          `json:"state"`
	Revision                   uint64                        `json:"revision"`
	OccurredAt                 time.Time                     `json:"occurred_at"`
	UpdatedAt                  time.Time                     `json:"updated_at"`
}

func cloneIncident(value IncidentRecord) IncidentRecord {
	value.HighWaterMetrics = cloneSlice(value.HighWaterMetrics)
	for index := range value.HighWaterMetrics {
		value.HighWaterMetrics[index].Labels = cloneStringMap(value.HighWaterMetrics[index].Labels)
		if value.HighWaterMetrics[index].CounterValue != nil {
			counter := *value.HighWaterMetrics[index].CounterValue
			value.HighWaterMetrics[index].CounterValue = &counter
		}
		if value.HighWaterMetrics[index].NumericValue != nil {
			numeric := *value.HighWaterMetrics[index].NumericValue
			value.HighWaterMetrics[index].NumericValue = &numeric
		}
	}
	value.Coverage = cloneSlice(value.Coverage)
	for index := range value.Coverage {
		value.Coverage[index].Gaps = cloneSlice(value.Coverage[index].Gaps)
	}
	value.Artifacts = cloneSlice(value.Artifacts)
	value.RecoveryActions = cloneSlice(value.RecoveryActions)
	value.VisibilityAcknowledgements = cloneSlice(value.VisibilityAcknowledgements)
	return value
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneAgent(value AgentWorkspaceRecord) AgentWorkspaceRecord {
	value.Generations = cloneSlice(value.Generations)
	return value
}
func cloneTarget(value TargetRecord) TargetRecord {
	value.Generations = cloneSlice(value.Generations)
	value.Runs = cloneSlice(value.Runs)
	for index := range value.Runs {
		value.Runs[index].IncidentIDs = cloneSlice(value.Runs[index].IncidentIDs)
	}
	value.Operations = cloneSlice(value.Operations)
	return value
}

func cloneSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	result := make([]T, len(values))
	copy(result, values)
	return result
}

// detachedRecord returns a deep mutation copy so transaction callbacks cannot
// alias the committed in-memory projection before their journal writes commit.
func detachedRecord[T any](records map[string]T, id string, clone func(T) T) (T, bool) {
	value, ok := records[id]
	if !ok {
		var zero T
		return zero, false
	}
	return clone(value), true
}

func sortSessionTargets(values []TargetRecord) {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
}
