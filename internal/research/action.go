package research

import (
	"time"
)

// ActionScope identifies which world execution produced the action.
type ActionScope string

const (
	ActionScopeAgentExec       ActionScope = "agent_exec"
	ActionScopeTargetOperation ActionScope = "target_operation"
)

func (s ActionScope) IsValid() bool {
	return s == ActionScopeAgentExec || s == ActionScopeTargetOperation
}

// ActionStart is the immutable start-side identity for an instrumented action.
type ActionStart struct {
	ActionID          string      `json:"action_id"`
	Scope             ActionScope `json:"scope"`
	LeaseID           string      `json:"lease_id,omitempty"`
	ResearchSessionID string      `json:"research_session_id,omitempty"`
	AgentWorkspaceID  string      `json:"agent_workspace_id,omitempty"`
	AgentGeneration   uint64      `json:"agent_generation,omitempty"`
	ExecID            string      `json:"exec_id,omitempty"`
	TargetID          string      `json:"target_id,omitempty"`
	TargetGeneration  uint64      `json:"target_generation,omitempty"`
	TargetRunID       string      `json:"target_run_id,omitempty"`
	TargetOperationID string      `json:"target_operation_id,omitempty"`
	CorrelationID     string      `json:"correlation_id,omitempty"`
	IdempotencyKey    string      `json:"idempotency_key,omitempty"`
	Executable        string      `json:"executable"`
	// Argv is arguments after argv[0]; Executable is argv[0].
	Argv               []string          `json:"argv,omitempty"`
	WorkingDirectory   string            `json:"working_directory,omitempty"`
	EnvironmentKeys    []string          `json:"environment_keys,omitempty"`
	StimulusClass      StimulusClass     `json:"stimulus_class"`
	ObservationLevel   ObservationLevel  `json:"observation_level"`
	Policy             PolicyObservation `json:"policy"`
	IntendedCompanions []CompanionRole   `json:"intended_companions"`
	StartedAt          time.Time         `json:"started_at"`
}

// ActionOutcome is the finish-side record sealed into action.json.
type ActionOutcome struct {
	EndedAt          time.Time `json:"ended_at"`
	ExitCode         *int      `json:"exit_code,omitempty"`
	Signal           string    `json:"signal,omitempty"`
	Error            string    `json:"error,omitempty"`
	CleanupConfirmed bool      `json:"cleanup_confirmed"`
	ProcessID        int64     `json:"process_id,omitempty"`
	ProcessStartNS   int64     `json:"process_start_ns,omitempty"`
	ParentPID        int64     `json:"parent_pid,omitempty"`
	StdoutBytes      int64     `json:"stdout_bytes"`
	StderrBytes      int64     `json:"stderr_bytes"`
	StdoutTruncated  bool      `json:"stdout_truncated"`
	StderrTruncated  bool      `json:"stderr_truncated"`
	// CaptureBound is the per-stream evidence retention limit. Transport may
	// enforce a separate maxExecBytes ceiling on the live wire; truncation
	// flags below only describe this bound.
	CaptureBound int64 `json:"capture_bound,omitempty"`
	Sealed       bool  `json:"sealed"`
}

// ActionDocument is the sealed action.json content.
type ActionDocument struct {
	SchemaVersion uint32           `json:"schema_version"`
	Start         ActionStart      `json:"start"`
	Outcome       ActionOutcome    `json:"outcome"`
	Gaps          []GapRecord      `json:"gaps"`
	Coverage      []CoverageRecord `json:"coverage"`
}

// GapRecord is an explicit absence (never a silent skip).
type GapRecord struct {
	Kind   string `json:"kind"`
	Source string `json:"source"`
	Role   string `json:"role,omitempty"`
	Reason string `json:"reason"`
}

// CoverageRecord records collector/signal availability for the action.
type CoverageRecord struct {
	Source   string `json:"source"`
	Role     string `json:"role"`
	Status   string `json:"status"` // available | gap | unsupported
	Required bool   `json:"required"`
	Detail   string `json:"detail,omitempty"`
}

// ActionSummary is the agent-facing bounded summary.json.
type ActionSummary struct {
	SchemaVersion      uint32           `json:"schema_version"`
	ActionID           string           `json:"action_id"`
	LeaseID            string           `json:"lease_id,omitempty"`
	TargetRunID        string           `json:"target_run_id,omitempty"`
	StimulusClass      StimulusClass    `json:"stimulus_class"`
	ObservationLevel   ObservationLevel `json:"observation_level"`
	ExitCode           *int             `json:"exit_code,omitempty"`
	Signal             string           `json:"signal,omitempty"`
	Error              string           `json:"error,omitempty"`
	ConfidenceFloor    ConfidenceFloor  `json:"confidence_floor"`
	EvidenceRoles      RoleChecklist    `json:"evidence_roles"`
	IntendedCompanions []CompanionRole  `json:"intended_companions"`
	Gaps               []GapRecord      `json:"gaps"`
	Text               string           `json:"text"`
	DurationMS         int64            `json:"duration_ms"`
}

const (
	ActionSchemaVersion  uint32 = 1
	SummarySchemaVersion uint32 = 1
	// DefaultCaptureBound limits retained stdout/stderr per stream in the
	// evidence bundle. Live exec transport may apply a separate maxExecBytes.
	DefaultCaptureBound int64 = 256 << 10
	// DefaultListLimit caps Store.List results to bound scan cost.
	DefaultListLimit = 256
	// MaxListLimit is the hard ceiling for ListOptions.Limit.
	MaxListLimit = 1024
)

// Stable gap reason codes for agent-facing views (no host path leakage).
const (
	ReasonHostCaptureFailed        = "host_capture_failed"
	ReasonHostUnavailable          = "host_unavailable"
	ReasonHostNotAttributed        = "host_not_action_attributed"
	ReasonNetworkCaptureFailed     = "network_capture_failed"
	ReasonNetworkUnavailable       = "network_unavailable"
	ReasonNetworkNotAttributed     = "network_not_action_attributed"
	ReasonNetworkToolMissing       = "network_capture_tool_missing"
	ReasonNetworkDecodeFailed      = "network_decode_failed"
	ReasonNetworkDecodeUnavailable = "network_decode_unavailable"
	ReasonStateCaptureFailed       = "state_capture_failed"
	ReasonStateUnavailable         = "state_unavailable"
	ReasonStateLevelBaseline       = "state_requires_deep_or_payload"
	ReasonStateScopeChanged        = "state_scope_changed"
	ReasonStateScopeUnavailable    = "state_scope_unavailable"
	ReasonStateSnapshotTruncated   = "state_snapshot_truncated"
	ReasonStateNotAttributed       = "state_not_action_attributed"
	ReasonSyscallCaptureFailed     = "syscall_capture_failed"
	ReasonSyscallUnavailable       = "syscall_unavailable"
	ReasonSyscallToolMissing       = "syscall_tool_missing"
	ReasonSyscallNotAttributed     = "syscall_not_action_attributed"
	ReasonStaticCaptureFailed      = "static_context_capture_failed"
	ReasonStaticUnavailable        = "static_context_unavailable"
	ReasonOracleNotConfigured      = "target_oracle_not_configured"
	ReasonOracleCaptureFailed      = "target_oracle_capture_failed"
	ReasonOracleUnavailable        = "target_oracle_unavailable"
	ReasonReplayCaptureFailed      = "replay_capture_failed"
	ReasonReplayUnavailable        = "replay_unavailable"
	ReasonCompanionUnconfigured    = "companion_not_configured"
	ReasonProcessLifecycleMissing  = "process_lifecycle_missing"
	ReasonProcessStartNSMissing    = "process_start_ns_missing"
	ReasonProcessIdentityMismatch  = "process_identity_mismatch"
	ReasonNetworkWindowUnjoined    = "network_window_capture_unjoined"
	ReasonBeginConflict            = "action_begin_conflict"
	ReasonSealAbandoned            = "action_seal_abandoned"
)
