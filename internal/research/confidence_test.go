package research

import "testing"

func TestDeriveConfidenceFloorLadder(t *testing.T) {
	tests := []struct {
		name  string
		roles RoleChecklist
		want  ConfidenceFloor
	}{
		{
			name:  "stimulus only",
			roles: BuildRoleChecklist(true, false, false, false, false, false, false, false),
			want:  ConfidenceReported,
		},
		{
			name:  "observed raw",
			roles: BuildRoleChecklist(true, true, false, false, false, false, false, false),
			want:  ConfidenceObserved,
		},
		{
			name:  "attributed",
			roles: BuildRoleChecklist(true, true, false, true, false, false, false, false),
			want:  ConfidenceAttributed,
		},
		{
			name:  "validated semantic",
			roles: BuildRoleChecklist(true, true, true, true, false, false, false, false),
			want:  ConfidenceValidated,
		},
		{
			name:  "demonstrated state",
			roles: BuildRoleChecklist(true, true, true, true, false, false, true, false),
			want:  ConfidenceDemonstrated,
		},
		{
			name:  "demonstrated oracle",
			roles: BuildRoleChecklist(true, true, true, true, false, true, false, false),
			want:  ConfidenceDemonstrated,
		},
		{
			name:  "reproduced",
			roles: BuildRoleChecklist(true, true, true, true, false, true, true, true),
			want:  ConfidenceReproduced,
		},
		{
			name:  "root caused",
			roles: BuildRoleChecklist(true, true, true, true, true, true, true, true),
			want:  ConfidenceRootCaused,
		},
		{
			name:  "cannot skip causal",
			roles: BuildRoleChecklist(true, true, true, false, true, true, true, true),
			want:  ConfidenceObserved,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveConfidenceFloor(tc.roles)
			if got != tc.want {
				t.Fatalf("floor = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveObservationLevel(t *testing.T) {
	if got := ResolveObservationLevel(false, "", false); got.Level != ObservationLevelBaseline || got.Escalated {
		t.Fatalf("default = %#v", got)
	}
	if got := ResolveObservationLevel(true, "", false); got.Level != ObservationLevelDeep || !got.Escalated {
		t.Fatalf("escalate = %#v", got)
	}
	if got := ResolveObservationLevel(false, "deep", false); got.Level != ObservationLevelDeep {
		t.Fatalf("named deep = %#v", got)
	}
	if got := ResolveObservationLevel(false, "payload", true); got.Level != ObservationLevelPayload {
		t.Fatalf("payload = %#v", got)
	}
	// payload without allow stays baseline (fail closed)
	if got := ResolveObservationLevel(false, "payload", false); got.Level != ObservationLevelBaseline || got.Escalated {
		t.Fatalf("payload denied = %#v", got)
	}
	// unknown profile stays baseline
	if got := ResolveObservationLevel(false, "mystery-profile", false); got.Level != ObservationLevelBaseline || got.Escalated {
		t.Fatalf("unknown profile = %#v", got)
	}
}

func TestBuildRoleChecklistForActionUnsupported(t *testing.T) {
	roles := BuildRoleChecklistForAction(
		[]CompanionRole{CompanionHostProcess},
		true, true, false, true, false, false, false, false,
	)
	if roles[RoleStatic] != RoleUnsupported || roles[RoleReplay] != RoleUnsupported {
		t.Fatalf("unintended roles should be unsupported: %#v", roles)
	}
	if roles[RoleCausal] != RolePresent {
		t.Fatalf("causal present: %#v", roles)
	}
	if roles[RoleSemantic] != RoleUnsupported {
		t.Fatalf("semantic without network companion is unsupported: %#v", roles)
	}
	rolesHTTP := BuildRoleChecklistForAction(
		IntendedCompanions(StimulusHTTPClient, ObservationLevelBaseline),
		true, true, false, true, false, false, false, false,
	)
	if rolesHTTP[RoleSemantic] != RoleGap {
		t.Fatalf("http_client intends semantic: %#v", rolesHTTP)
	}
}
