package research

import (
	"path"
	"strings"
	"time"
)

// StartFromCommand builds an ActionStart from an instrumented command.
// actionID should be the durable join key (exec_id or target_operation_id).
//
// Classification uses the executable path only on the production wiring path:
// WML launches with Executable as argv[0], so a separate alternate argv[0] is
// not available. Callers that wrap binaries (e.g. /usr/bin/env) should pass
// the real program path as executable when known; ClassifyStimulus still
// accepts an independent argv0 when tests or future wrappers supply one.
func StartFromCommand(actionID string, scope ActionScope, executable string, argv []string, workingDirectory string, startedAt time.Time, policy PolicyObservation) ActionStart {
	if policy.Level == "" {
		policy = ResolveObservationLevel(false, "", false)
	}
	argv0 := executable
	if base := path.Base(strings.ReplaceAll(executable, "\\", "/")); base != "" && base != "." {
		argv0 = base
	}
	class := ClassifyStimulus(executable, argv0)
	level := policy.Level
	if !level.IsValid() {
		level = ObservationLevelBaseline
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return ActionStart{
		ActionID:           actionID,
		Scope:              scope,
		Executable:         executable,
		Argv:               append([]string(nil), argv...),
		WorkingDirectory:   workingDirectory,
		StimulusClass:      class,
		ObservationLevel:   level,
		Policy:             policy,
		IntendedCompanions: IntendedCompanions(class, level),
		StartedAt:          startedAt.UTC(),
	}
}

// EnvironmentKeys returns sorted environment variable names without values
// (values may be secret; names alone support stimulus context).
func EnvironmentKeys(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	// insertion sort to avoid importing sort solely for tiny maps in hot path
	for i := 1; i < len(keys); i++ {
		j := i
		for j > 0 && keys[j-1] > keys[j] {
			keys[j-1], keys[j] = keys[j], keys[j-1]
			j--
		}
	}
	return keys
}
