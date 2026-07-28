package admission

import (
	"fmt"
	"sort"
	"time"
)

type PressureStage uint8

const (
	StageNormal PressureStage = iota
	StageRaiseObservation
	StageStopAdmission
	StageExpireReservations
	StageShrinkWarmPools
	StageQuiesceIdleTargets
	StageRevokeTargetRuns
	StageRevokeAgentWorkspaces
	StageQuarantineNode
)

func (s PressureStage) String() string {
	names := [...]string{"normal", "raise_observation", "stop_admission", "expire_reservations", "shrink_warm_pools", "quiesce_idle_targets", "revoke_target_runs", "revoke_agent_workspaces", "quarantine_node"}
	if int(s) >= len(names) {
		return "unknown"
	}
	return names[s]
}

type CandidateKind string

const (
	CandidateTargetRun      CandidateKind = "target_run"
	CandidateAgentWorkspace CandidateKind = "agent_workspace"
)

type RevocationCandidate struct {
	LeaseKey    string        `json:"lease_key"`
	ResourceKey string        `json:"resource_key"`
	Kind        CandidateKind `json:"kind"`
	Priority    int           `json:"priority"`
	Preemptible bool          `json:"preemptible"`
	Idle        bool          `json:"idle"`
	StartedAt   time.Time     `json:"started_at"`
}

type PressureInput struct {
	Stage              PressureStage         `json:"stage"`
	Pressure           Pressure              `json:"pressure"`
	Thresholds         PressureThresholds    `json:"thresholds"`
	UnusedReservations int                   `json:"unused_reservations"`
	WarmPoolTargets    int                   `json:"warm_pool_targets"`
	Candidates         []RevocationCandidate `json:"candidates"`
	ObservedAt         time.Time             `json:"observed_at"`
}

type PressureDecision struct {
	Stage                           PressureStage `json:"stage"`
	Action                          string        `json:"action"`
	ResourceKey                     string        `json:"resource_key,omitempty"`
	Reason                          string        `json:"reason"`
	InputsDigest                    string        `json:"inputs_digest"`
	CreatesResourceEvictionIncident bool          `json:"creates_resource_eviction_incident"`
	StopsAdmission                  bool          `json:"stops_admission"`
}

// DecidePressure advances at most one fixed stage. Callers report whether the
// prior action restored safety before invoking it again; this makes decisions
// replayable and prevents destructive-stage skipping.
func DecidePressure(input PressureInput) (PressureDecision, error) {
	digest, err := decisionDigest(input)
	if err != nil {
		return PressureDecision{}, err
	}
	decision := PressureDecision{InputsDigest: digest}
	if err := input.Pressure.Validate(); err != nil {
		return decision, err
	}
	if err := input.Thresholds.Validate(); err != nil {
		return decision, err
	}
	reasons := input.Pressure.unsafe(input.Thresholds)
	if len(reasons) == 0 {
		decision.Stage = StageNormal
		decision.Action = "resume_admission"
		decision.Reason = "pressure is below configured thresholds"
		return decision, nil
	}
	if input.Stage > StageQuarantineNode {
		return decision, fmt.Errorf("invalid pressure stage %d", input.Stage)
	}
	next := input.Stage + 1
	if input.Stage == StageNormal {
		next = StageRaiseObservation
	}
	decision.Stage = next
	decision.Reason = "verified pressure: " + joinReasons(reasons)
	switch next {
	case StageRaiseObservation:
		decision.Action = "increase_metric_resolution"
	case StageStopAdmission:
		decision.Action = "stop_admission"
		decision.StopsAdmission = true
	case StageExpireReservations:
		decision.Action = "expire_unused_reservations"
		if input.UnusedReservations == 0 {
			decision.Reason += "; no unused reservation is currently eligible"
		}
	case StageShrinkWarmPools:
		decision.Action = "shrink_unleased_warm_pools"
		if input.WarmPoolTargets == 0 {
			decision.Reason += "; no warm target is currently eligible"
		}
	case StageQuiesceIdleTargets:
		decision.Action = "quiesce_idle_preemptible_target"
		decision.ResourceKey = selectCandidate(input.Candidates, CandidateTargetRun, true)
	case StageRevokeTargetRuns:
		decision.Action = "revoke_preemptible_target_run"
		decision.ResourceKey = selectCandidate(input.Candidates, CandidateTargetRun, false)
		decision.CreatesResourceEvictionIncident = decision.ResourceKey != ""
	case StageRevokeAgentWorkspaces:
		decision.Action = "revoke_preemptible_agent_workspace"
		decision.ResourceKey = selectCandidate(input.Candidates, CandidateAgentWorkspace, false)
		decision.CreatesResourceEvictionIncident = decision.ResourceKey != ""
	case StageQuarantineNode:
		decision.Action = "quarantine_node"
	}
	return decision, nil
}

func selectCandidate(candidates []RevocationCandidate, kind CandidateKind, idleOnly bool) string {
	eligible := make([]RevocationCandidate, 0)
	for _, candidate := range candidates {
		if candidate.Kind == kind && candidate.Preemptible && (!idleOnly || candidate.Idle) {
			eligible = append(eligible, candidate)
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority < eligible[j].Priority
		}
		if !eligible[i].StartedAt.Equal(eligible[j].StartedAt) {
			return eligible[i].StartedAt.After(eligible[j].StartedAt)
		}
		return eligible[i].ResourceKey < eligible[j].ResourceKey
	})
	if len(eligible) == 0 {
		return ""
	}
	return eligible[0].ResourceKey
}

type QueuedLease struct {
	LeaseKey   string
	Priority   int
	EnqueuedAt time.Time
}

// OrderQueue combines configured priority with bounded age promotion. One
// priority point is added per agingInterval, preventing indefinite starvation.
func OrderQueue(items []QueuedLease, now time.Time, agingInterval time.Duration) ([]QueuedLease, error) {
	if agingInterval <= 0 {
		return nil, fmt.Errorf("aging interval must be positive")
	}
	ordered := append([]QueuedLease(nil), items...)
	score := func(item QueuedLease) int64 {
		age := now.Sub(item.EnqueuedAt)
		if age < 0 {
			age = 0
		}
		return int64(item.Priority) + int64(age/agingInterval)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := score(ordered[i]), score(ordered[j])
		if left != right {
			return left > right
		}
		if !ordered[i].EnqueuedAt.Equal(ordered[j].EnqueuedAt) {
			return ordered[i].EnqueuedAt.Before(ordered[j].EnqueuedAt)
		}
		return ordered[i].LeaseKey < ordered[j].LeaseKey
	})
	return ordered, nil
}
