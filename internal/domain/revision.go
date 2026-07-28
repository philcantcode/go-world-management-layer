package domain

import "math"

type Revision uint64

const InitialRevision Revision = 1

func (r Revision) IsValid() bool { return r >= InitialRevision }

func (r Revision) Next() (Revision, error) {
	if !r.IsValid() {
		return 0, NewError(CodeInvalidArgument, "revision.next", "revision", "must be positive", nil)
	}
	if r == Revision(math.MaxUint64) {
		return 0, NewError(CodeResourceExhausted, "revision.next", "revision", "has reached its maximum", nil)
	}
	return r + 1, nil
}

func RequireRevision(actual, expected Revision) error {
	if !expected.IsValid() {
		return NewError(CodeInvalidArgument, "revision.require", "expected_revision", "must be positive", nil)
	}
	if actual != expected {
		return NewDetailedError(CodeStaleRevision, "revision.require", "expected_revision", "does not match current revision", map[string]string{
			"actual": uintString(uint64(actual)), "expected": uintString(uint64(expected)),
		}, nil)
	}
	return nil
}

type AgentGeneration uint64
type TargetGeneration uint64

const (
	InitialAgentGeneration  AgentGeneration  = 1
	InitialTargetGeneration TargetGeneration = 1
)

func (g AgentGeneration) IsValid() bool  { return g >= InitialAgentGeneration }
func (g TargetGeneration) IsValid() bool { return g >= InitialTargetGeneration }

func (g AgentGeneration) Next() (AgentGeneration, error) {
	if !g.IsValid() {
		return 0, NewError(CodeInvalidArgument, "agent_generation.next", "generation", "must be positive", nil)
	}
	if g == AgentGeneration(math.MaxUint64) {
		return 0, NewError(CodeResourceExhausted, "agent_generation.next", "generation", "cannot advance generation", nil)
	}
	return g + 1, nil
}

func (g TargetGeneration) Next() (TargetGeneration, error) {
	if !g.IsValid() {
		return 0, NewError(CodeInvalidArgument, "target_generation.next", "generation", "must be positive", nil)
	}
	if g == TargetGeneration(math.MaxUint64) {
		return 0, NewError(CodeResourceExhausted, "target_generation.next", "generation", "cannot advance generation", nil)
	}
	return g + 1, nil
}

func uintString(value uint64) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}
