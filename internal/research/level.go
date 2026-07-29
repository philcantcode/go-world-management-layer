package research

import "strings"

// ObservationLevel aligns with ADR 0006 cumulative observation levels.
type ObservationLevel string

const (
	// ObservationLevelBaseline is required metadata: lifecycle, exit, bounded
	// stdout/stderr, process identity when available, coverage/gaps.
	ObservationLevelBaseline ObservationLevel = "baseline"
	// ObservationLevelDeep is policy-triggered broader observation: state
	// snapshots, packet rings, richer host events.
	ObservationLevelDeep ObservationLevel = "deep"
	// ObservationLevelPayload is explicit invasive capture: payloads, buffers,
	// memory, or screens within strict bounds.
	ObservationLevelPayload ObservationLevel = "payload"
)

func (l ObservationLevel) IsValid() bool {
	return l == ObservationLevelBaseline || l == ObservationLevelDeep || l == ObservationLevelPayload
}

func (l ObservationLevel) Rank() int {
	switch l {
	case ObservationLevelPayload:
		return 3
	case ObservationLevelDeep:
		return 2
	case ObservationLevelBaseline:
		return 1
	default:
		return 0
	}
}

func (l ObservationLevel) AtLeast(minimum ObservationLevel) bool {
	return l.Rank() >= minimum.Rank()
}

// PolicyObservation maps escalate / named-profile style requests onto a level.
// Unknown or empty values remain baseline (fail closed to low overhead).
type PolicyObservation struct {
	// Level is the effective level after policy evaluation.
	Level ObservationLevel `json:"level"`
	// Escalated records whether an explicit escalate request was honored.
	Escalated bool `json:"escalated"`
	// NamedProfile is the accepted capture profile name, if any.
	NamedProfile string `json:"named_profile,omitempty"`
	// Reason explains the chosen level for the action bundle.
	Reason string `json:"reason"`
}

// Known observation profile names that raise above baseline. Unknown non-empty
// profiles stay baseline (fail closed); they never silently escalate.
const (
	ProfileDeep     = "deep"
	ProfileEscalate = "escalate"
	ProfilePayload  = "payload"
	ProfileInvasive = "invasive"
)

// ResolveObservationLevel chooses the effective level from optional escalate
// intent and an allowlisted named profile. Payload requires an explicit
// invasive flag. Unknown profiles remain baseline.
func ResolveObservationLevel(escalate bool, namedProfile string, allowPayload bool) PolicyObservation {
	profile := strings.TrimSpace(namedProfile)
	if allowPayload && (profile == ProfilePayload || profile == ProfileInvasive) {
		return PolicyObservation{
			Level: ObservationLevelPayload, Escalated: true, NamedProfile: profile,
			Reason: "explicit payload/invasive profile",
		}
	}
	if profile == ProfilePayload || profile == ProfileInvasive {
		// Payload requested without allowance: stay baseline fail-closed.
		return PolicyObservation{
			Level: ObservationLevelBaseline, Escalated: false, NamedProfile: profile,
			Reason: "payload profile denied without explicit allowance; baseline retained",
		}
	}
	if escalate || profile == ProfileDeep || profile == ProfileEscalate {
		return PolicyObservation{
			Level: ObservationLevelDeep, Escalated: true, NamedProfile: profile,
			Reason: "policy escalate or deep profile",
		}
	}
	if profile != "" {
		return PolicyObservation{
			Level: ObservationLevelBaseline, Escalated: false, NamedProfile: profile,
			Reason: "unknown observation profile kept at baseline",
		}
	}
	return PolicyObservation{
		Level: ObservationLevelBaseline, Escalated: false,
		Reason: "default baseline metadata",
	}
}
