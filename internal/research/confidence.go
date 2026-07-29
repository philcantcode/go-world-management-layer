package research

// ConfidenceFloor is a conservative analytical ladder derived from available
// evidence roles. It is not a full findings system.
type ConfidenceFloor string

const (
	ConfidenceReported     ConfidenceFloor = "reported"
	ConfidenceObserved     ConfidenceFloor = "observed"
	ConfidenceAttributed   ConfidenceFloor = "attributed"
	ConfidenceValidated    ConfidenceFloor = "validated"
	ConfidenceDemonstrated ConfidenceFloor = "demonstrated"
	ConfidenceReproduced   ConfidenceFloor = "reproduced"
	ConfidenceRootCaused   ConfidenceFloor = "root_caused"
)

func (c ConfidenceFloor) IsValid() bool {
	switch c {
	case ConfidenceReported, ConfidenceObserved, ConfidenceAttributed, ConfidenceValidated,
		ConfidenceDemonstrated, ConfidenceReproduced, ConfidenceRootCaused:
		return true
	}
	return false
}

func (c ConfidenceFloor) Rank() int {
	switch c {
	case ConfidenceRootCaused:
		return 7
	case ConfidenceReproduced:
		return 6
	case ConfidenceDemonstrated:
		return 5
	case ConfidenceValidated:
		return 4
	case ConfidenceAttributed:
		return 3
	case ConfidenceObserved:
		return 2
	case ConfidenceReported:
		return 1
	default:
		return 0
	}
}

// EvidenceRole checklist keys used in summary.json.
type EvidenceRole string

const (
	RoleStimulus     EvidenceRole = "stimulus"
	RoleRaw          EvidenceRole = "raw"
	RoleSemantic     EvidenceRole = "semantic"
	RoleCausal       EvidenceRole = "causal"
	RoleStatic       EvidenceRole = "static"
	RoleTargetOracle EvidenceRole = "target_oracle"
	RoleStateDiff    EvidenceRole = "state_diff"
	RoleReplay       EvidenceRole = "replay"
)

// RoleStatus is present, gap, or unsupported for a role.
type RoleStatus string

const (
	RolePresent     RoleStatus = "present"
	RoleGap         RoleStatus = "gap"
	RoleUnsupported RoleStatus = "unsupported"
)

// RoleChecklist is the agent-facing evidence role matrix for one action.
type RoleChecklist map[EvidenceRole]RoleStatus

// DeriveConfidenceFloor returns the highest floor that the checklist can
// conservatively support. Missing roles never inflate confidence.
//
// Ladder (conservative):
//   - reported: stimulus metadata only
//   - observed: stimulus + raw output (stdout/stderr/exit)
//   - attributed: observed + causal host/process identity
//   - validated: attributed + semantic or network decode
//   - demonstrated: validated + state_diff or target_oracle
//   - reproduced: demonstrated + replay
//   - root_caused: reproduced + static context
func DeriveConfidenceFloor(roles RoleChecklist) ConfidenceFloor {
	has := func(role EvidenceRole) bool {
		return roles[role] == RolePresent
	}
	if !has(RoleStimulus) {
		return ConfidenceReported
	}
	if !has(RoleRaw) {
		return ConfidenceReported
	}
	floor := ConfidenceObserved
	if has(RoleCausal) {
		floor = ConfidenceAttributed
	} else {
		return floor
	}
	if has(RoleSemantic) {
		floor = ConfidenceValidated
	} else {
		return floor
	}
	if has(RoleStateDiff) || has(RoleTargetOracle) {
		floor = ConfidenceDemonstrated
	} else {
		return floor
	}
	if has(RoleReplay) {
		floor = ConfidenceReproduced
	} else {
		return floor
	}
	if has(RoleStatic) {
		return ConfidenceRootCaused
	}
	return floor
}

// BuildRoleChecklist constructs the checklist from sealed coverage flags.
// Absent evidence is RoleGap (caller should prefer BuildRoleChecklistForAction
// so unintended roles become RoleUnsupported).
func BuildRoleChecklist(hasStimulus, hasRaw, hasSemantic, hasCausal, hasStatic, hasOracle, hasStateDiff, hasReplay bool) RoleChecklist {
	status := func(present bool) RoleStatus {
		if present {
			return RolePresent
		}
		return RoleGap
	}
	return RoleChecklist{
		RoleStimulus:     status(hasStimulus),
		RoleRaw:          status(hasRaw),
		RoleSemantic:     status(hasSemantic),
		RoleCausal:       status(hasCausal),
		RoleStatic:       status(hasStatic),
		RoleTargetOracle: status(hasOracle),
		RoleStateDiff:    status(hasStateDiff),
		RoleReplay:       status(hasReplay),
	}
}

// BuildRoleChecklistForAction sets RolePresent when evidence exists, RoleGap
// when the role was intended but missing, and RoleUnsupported otherwise.
func BuildRoleChecklistForAction(intended []CompanionRole, hasStimulus, hasRaw, hasSemantic, hasCausal, hasStatic, hasOracle, hasStateDiff, hasReplay bool) RoleChecklist {
	intendedSet := map[EvidenceRole]bool{
		RoleStimulus: true, // always intended for instrumented actions
		RoleRaw:      true,
	}
	for _, companion := range intended {
		switch companion {
		case CompanionHostProcess, CompanionHostSyscall:
			intendedSet[RoleCausal] = true
		case CompanionNetworkCapture, CompanionNetworkDecode:
			intendedSet[RoleSemantic] = true
		case CompanionStateDiff:
			intendedSet[RoleStateDiff] = true
		case CompanionStaticContext:
			intendedSet[RoleStatic] = true
		case CompanionTargetOracle:
			intendedSet[RoleTargetOracle] = true
		case CompanionReplay:
			intendedSet[RoleReplay] = true
		}
	}
	// Process lifecycle causal is always desired when available even without companion.
	intendedSet[RoleCausal] = true

	role := func(r EvidenceRole, present bool) RoleStatus {
		if present {
			return RolePresent
		}
		if intendedSet[r] {
			return RoleGap
		}
		return RoleUnsupported
	}
	return RoleChecklist{
		RoleStimulus:     role(RoleStimulus, hasStimulus),
		RoleRaw:          role(RoleRaw, hasRaw),
		RoleSemantic:     role(RoleSemantic, hasSemantic),
		RoleCausal:       role(RoleCausal, hasCausal),
		RoleStatic:       role(RoleStatic, hasStatic),
		RoleTargetOracle: role(RoleTargetOracle, hasOracle),
		RoleStateDiff:    role(RoleStateDiff, hasStateDiff),
		RoleReplay:       role(RoleReplay, hasReplay),
	}
}
