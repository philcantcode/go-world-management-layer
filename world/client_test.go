package world

import (
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
)

func TestDefensiveCopyDoesNotShareNestedState(t *testing.T) {
	original := &worldv1.ResearchSessionView{
		Session:   &worldv1.ResearchSession{ResearchSessionId: "session"},
		Targets:   []*worldv1.Target{{TargetId: "target", Runs: []*worldv1.TargetRun{{TargetRunId: "run", IncidentIds: []string{"incident"}}}}},
		Incidents: []*worldv1.Incident{{IncidentId: "incident", HighWaterMetrics: []*worldv1.IncidentMetric{{Labels: map[string]string{"scope": "target"}}}, Coverage: []*worldv1.IncidentCoverage{{Gaps: []*worldv1.IncidentGap{{Reason: "overflow"}}}}, Artifacts: []*worldv1.ArtifactReference{{Reference: "artifact://evidence"}}}},
	}
	copy, err := defensiveCopy(original)
	if err != nil {
		t.Fatal(err)
	}
	copy.Session.ResearchSessionId = "changed"
	copy.Targets[0].Runs[0].IncidentIds[0] = "changed"
	copy.Incidents[0].HighWaterMetrics[0].Labels["scope"] = "changed"
	copy.Incidents[0].Coverage[0].Gaps[0].Reason = "changed"
	copy.Incidents[0].Artifacts[0].Reference = "changed"
	if original.Session.ResearchSessionId != "session" || original.Targets[0].Runs[0].IncidentIds[0] != "incident" || original.Incidents[0].HighWaterMetrics[0].Labels["scope"] != "target" || original.Incidents[0].Coverage[0].Gaps[0].Reason != "overflow" || original.Incidents[0].Artifacts[0].Reference != "artifact://evidence" {
		t.Fatal("returned view shares mutable nested state")
	}
}
