package rpc

import (
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/protobuf/proto"
)

func TestApplicationProjectionCoversNestedLifecycleFields(t *testing.T) {
	now := time.Unix(123, 456).UTC()
	input := application.ResearchSessionView{
		Session: application.SessionRecord{ID: "session", OwnerSubject: "owner", State: domain.ResearchSessionLeased, Revision: 3, LeaseID: "lease", AgentWorkspaceID: "agent", InputViewID: "view", PolicyDigest: "policy", CapabilityDigest: "capability", CreatedAt: now, UpdatedAt: now},
		Lease:   application.LeaseRecord{ID: "lease", SessionID: "session", AgentWorkspaceID: "agent", AgentGeneration: 2, InputViewID: "view", PolicyDigest: "policy", CapabilityDigest: "capability", State: domain.LeaseActive, Revision: 4, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now},
		Agent:   application.AgentWorkspaceRecord{ID: "agent", SessionID: "session", CurrentGeneration: 2, Revision: 5, CreatedAt: now, UpdatedAt: now, Generations: []application.AgentGenerationRecord{{Generation: 2, WorkspaceID: "workspace", InputViewID: "view", PolicyDigest: "policy", CapabilityDigest: "capability", Previous: 1, RecoveryIncident: "incident", ProvisioningPlanDigest: "plan", State: domain.AgentGenerationRunning, Revision: 6, CreatedAt: now, UpdatedAt: now}}},
		Execs:   []application.ExecRecord{{ID: "exec", SessionID: "session", LeaseID: "lease", AgentWorkspaceID: "agent", AgentGeneration: 2, Kind: domain.ExecProvider, Executable: "provider", Argv: []string{"--flag"}, WorkingDirectory: ".", State: domain.ExecCompleted, Revision: 3, ExitCode: intPointer(0), IncidentIDs: []string{"incident"}, CleanupConfirmed: true, CreatedAt: now, UpdatedAt: now}},
		Targets: []application.TargetRecord{{
			ID: "target", SessionID: "session", LeaseID: "lease", Template: "template", Kind: domain.TargetLinuxContainer, CurrentGeneration: 2, Revision: 7, CreatedAt: now, UpdatedAt: now,
			Generations: []application.TargetGenerationRecord{{Generation: 2, PolicyDigest: "policy", CapabilityDigest: "capability", ProvisioningPlanDigest: "target-plan", Previous: 1, RecoveryIncident: "incident", State: domain.TargetGenerationReady, Revision: 8, CreatedAt: now, UpdatedAt: now}},
			Runs:        []application.TargetRunRecord{{ID: "run", Generation: 2, AgentWorkspaceID: "agent", AgentGeneration: 2, MaterializationDigest: "material", ProvisioningPlanDigest: "run-plan", State: domain.TargetRunRunning, Revision: 9, BundleID: "bundle", BundleArtifact: "artifact", BundleDigest: "digest", IncidentIDs: []string{"incident"}, CreatedAt: now, UpdatedAt: now}},
			Operations:  []application.TargetOperationRecord{{ID: "operation", RunID: "run", Generation: 2, Kind: domain.TargetOperationExec, CommandDisplay: "program --flag", ContentDigest: "content", State: domain.TargetOperationRunning, Revision: 10, CreatedAt: now, UpdatedAt: now}},
		}},
		Incidents: []application.IncidentRecord{{
			ID: "incident", Classification: domain.IncidentLinuxTargetFailure, SessionID: "session", LeaseID: "lease", AgentWorkspaceID: "agent", AgentGeneration: 2,
			ExecID: "exec", TargetID: "target", TargetGeneration: 2, TargetRunID: "run", Trigger: "trigger", LastKnownState: "running",
			Cause:               application.CauseRecord{Kind: domain.CauseCorrelated, Summary: "summary", Method: "method", Confidence: .75},
			HighWaterMetrics:    []application.IncidentMetricRecord{{SubjectID: "subject", SubjectKind: domain.SubjectLinuxTarget, Name: "memory.events.oom", Unit: "count", Kind: domain.MetricCounter, Availability: domain.MetricAvailable, CounterValue: uint64Pointer(2), CollectedAt: now, PublishedAt: now, Cursor: 11, Labels: map[string]string{"scope": "target"}, ExecID: "exec", TargetRunID: "run"}},
			FirstRelevantCursor: 11, LastRelevantCursor: 12,
			Coverage:            []application.IncidentCoverageRecord{{CollectorID: "collector", SignalFamily: "kernel", Placement: domain.CollectorPlacementHost, Level: domain.CoverageLevelPartial, Status: domain.CoverageDegraded, Required: true, StartedAt: now, EndedAt: now, DroppedRecords: 1, Gaps: []application.IncidentGapRecord{{Kind: domain.GapDropped, Source: "ring", SourceInstance: "0", FirstSourceSequence: 3, LastSourceSequence: 4, FirstCursor: 11, LastCursor: 12, StartedAt: now, EndedAt: now, LostRecords: 1, Reason: "overflow"}}}},
			ObservationBundleID: "bundle", Artifacts: []application.IncidentArtifactRecord{{Reference: "artifact://incident", Digest: "sha256:abc", Size: 7, Role: "evidence", Sensitivity: domain.SensitivityInternal}},
			RecoveryActions: []string{"recover"}, VisibilityAcknowledgements: []string{"ack"}, State: domain.IncidentRecovering, Revision: 13, OccurredAt: now, UpdatedAt: now,
		}},
	}
	got := researchSessionView(input)
	want := &worldv1.ResearchSessionView{
		Session:        &worldv1.ResearchSession{ResearchSessionId: "session", OwnerSubject: "owner", State: "leased", Revision: 3, LeaseId: "lease", AgentWorkspaceId: "agent", InputViewId: "view", PolicyDigest: "policy", CapabilityDigest: "capability", CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)},
		Lease:          &worldv1.Lease{LeaseId: "lease", ResearchSessionId: "session", AgentWorkspaceId: "agent", AgentGeneration: 2, InputViewId: "view", PolicyDigest: "policy", CapabilityDigest: "capability", State: "active", Revision: 4, ExpiresAt: protobufTimestamp(now.Add(time.Hour)), CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)},
		AgentWorkspace: &worldv1.AgentWorkspace{AgentWorkspaceId: "agent", ResearchSessionId: "session", CurrentGeneration: 2, Revision: 5, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now), Generations: []*worldv1.AgentGeneration{{Generation: 2, WorkspaceId: "workspace", InputViewId: "view", PolicyDigest: "policy", CapabilityDigest: "capability", PreviousGeneration: 1, RecoveryIncidentId: "incident", ProvisioningPlanDigest: "plan", State: "running", Revision: 6, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)}}},
		Targets:        []*worldv1.Target{{TargetId: "target", ResearchSessionId: "session", LeaseId: "lease", TemplateReference: "template", Kind: "linux_container", CurrentGeneration: 2, Revision: 7, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now), Generations: []*worldv1.TargetGeneration{{Generation: 2, PolicyDigest: "policy", CapabilityDigest: "capability", ProvisioningPlanDigest: "target-plan", PreviousGeneration: 1, RecoveryIncidentId: "incident", State: "ready", Revision: 8, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)}}, Runs: []*worldv1.TargetRun{{TargetRunId: "run", Generation: 2, AgentWorkspaceId: "agent", AgentGeneration: 2, MaterializationDigest: "material", ProvisioningPlanDigest: "run-plan", State: "running", Revision: 9, BundleId: "bundle", BundleArtifact: "artifact", BundleDigest: "digest", IncidentIds: []string{"incident"}, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)}}, Operations: []*worldv1.TargetOperation{{TargetOperationId: "operation", TargetRunId: "run", Generation: 2, Kind: "exec", CommandDisplay: "program --flag", ContentDigest: "content", State: "running", Revision: 10, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)}}}},
		Incidents: []*worldv1.Incident{{
			IncidentId: "incident", Classification: "linux_target_failure", ResearchSessionId: "session", LeaseId: "lease", AgentWorkspaceId: "agent", AgentGeneration: 2,
			ExecId: "exec", TargetId: "target", TargetGeneration: 2, TargetRunId: "run", Trigger: "trigger", LastKnownState: "running",
			Cause:               &worldv1.Cause{Kind: "correlated", Summary: "summary", Method: "method", Confidence: .75},
			HighWaterMetrics:    []*worldv1.IncidentMetric{{SubjectId: "subject", SubjectKind: "linux_target", Name: "memory.events.oom", Unit: "count", Kind: "counter", Availability: "available", CounterValue: uint64Pointer(2), CollectedAt: protobufTimestamp(now), PublishedAt: protobufTimestamp(now), Cursor: 11, Labels: map[string]string{"scope": "target"}, ExecId: "exec", TargetRunId: "run"}},
			FirstRelevantCursor: 11, LastRelevantCursor: 12,
			Coverage:            []*worldv1.IncidentCoverage{{CollectorId: "collector", SignalFamily: "kernel", Placement: "host", Level: "partial", Status: "degraded", Required: true, StartedAt: protobufTimestamp(now), EndedAt: protobufTimestamp(now), DroppedRecords: 1, Gaps: []*worldv1.IncidentGap{{Kind: "dropped", Source: "ring", SourceInstance: "0", FirstSourceSequence: 3, LastSourceSequence: 4, FirstCursor: 11, LastCursor: 12, StartedAt: protobufTimestamp(now), EndedAt: protobufTimestamp(now), LostRecords: 1, Reason: "overflow"}}}},
			ObservationBundleId: "bundle", Artifacts: []*worldv1.ArtifactReference{{Reference: "artifact://incident", Digest: "sha256:abc", Size: 7, Role: "evidence", Sensitivity: "internal"}},
			RecoveryActions: []string{"recover"}, VisibilityAcknowledgements: []string{"ack"}, State: "recovering", Revision: 13, OccurredAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now),
		}},
		Execs: []*worldv1.Exec{{ExecId: "exec", ResearchSessionId: "session", LeaseId: "lease", AgentWorkspaceId: "agent", AgentGeneration: 2, Kind: "provider", Executable: "provider", Argv: []string{"--flag"}, WorkspaceRelativeWorkingDirectory: ".", State: "completed", Revision: 3, ExitCode: int32Pointer(0), IncidentIds: []string{"incident"}, CleanupConfirmed: true, CreatedAt: protobufTimestamp(now), UpdatedAt: protobufTimestamp(now)}},
	}
	if !proto.Equal(got, want) {
		t.Fatalf("projection omitted or changed lifecycle data\ngot:  %#v\nwant: %#v", got, want)
	}
}

func intPointer(value int) *int          { return &value }
func int32Pointer(value int32) *int32    { return &value }
func uint64Pointer(value uint64) *uint64 { return &value }
