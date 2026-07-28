package ports

import (
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestInterruptedCollectorReconciliationReportValidatesExactArtifacts(t *testing.T) {
	plan := reconciliationCollectorPlan(t)
	request := InterruptedCollectorReconciliation{TargetRunID: plan.TargetRunID, Collectors: []InterruptedCollectorBinding{{Plan: plan, StartCommitted: true}}}
	artifacts := reconciliationArtifacts(t, plan, 6, 4)
	valid := InterruptedCollectorReconciliationReport{
		TargetRunID: plan.TargetRunID,
		Outputs: []InterruptedCollectorOutput{{
			CollectorID: plan.CollectorID, State: InterruptedCollectorOutputFinalized,
			Artifacts: artifacts, CaptureLimitExceeded: true,
		}},
	}
	if err := valid.ValidateFor(request); err != nil {
		t.Fatalf("valid exact report: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*InterruptedCollectorReconciliationReport)
	}{
		{name: "missing stream", mutate: func(report *InterruptedCollectorReconciliationReport) {
			report.Outputs[0].Artifacts = report.Outputs[0].Artifacts[:1]
		}},
		{name: "duplicate role and reference", mutate: func(report *InterruptedCollectorReconciliationReport) {
			report.Outputs[0].Artifacts[1] = report.Outputs[0].Artifacts[0]
		}},
		{name: "foreign collector reference", mutate: func(report *InterruptedCollectorReconciliationReport) {
			spec := report.Outputs[0].Artifacts[0].Spec()
			spec.Reference = strings.Replace(spec.Reference, plan.CollectorID.String(), "collector_01890f3a-2d00-7000-8000-000000000001", 1)
			report.Outputs[0].Artifacts[0], _ = domain.NewArtifactReference(spec)
		}},
		{name: "shared size exceeds limit", mutate: func(report *InterruptedCollectorReconciliationReport) {
			spec := report.Outputs[0].Artifacts[0].Spec()
			spec.Size++
			report.Outputs[0].Artifacts[0], _ = domain.NewArtifactReference(spec)
		}},
		{name: "false limit boundary", mutate: func(report *InterruptedCollectorReconciliationReport) {
			spec := report.Outputs[0].Artifacts[0].Spec()
			spec.Size--
			report.Outputs[0].Artifacts[0], _ = domain.NewArtifactReference(spec)
		}},
		{name: "aborted with artifacts", mutate: func(report *InterruptedCollectorReconciliationReport) {
			report.Outputs[0].State = InterruptedCollectorOutputAborted
			report.Outputs[0].CaptureLimitExceeded = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := valid
			report.Outputs = append([]InterruptedCollectorOutput(nil), valid.Outputs...)
			report.Outputs[0].Artifacts = append([]domain.ArtifactReference(nil), valid.Outputs[0].Artifacts...)
			test.mutate(&report)
			if err := report.ValidateFor(request); !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("invalid report error = %v", err)
			}
		})
	}
}

func reconciliationCollectorPlan(t *testing.T) CollectorPlan {
	t.Helper()
	collectorID, _ := domain.NewCollectorID()
	sessionID, _ := domain.NewResearchSessionID()
	leaseID, _ := domain.NewLeaseID()
	agentID, _ := domain.NewAgentWorkspaceID()
	targetID, _ := domain.NewTargetID()
	runID, _ := domain.NewTargetRunID()
	return CollectorPlan{
		IdempotencyKey: "collector-reconciliation", CollectorID: collectorID, ResearchSessionID: sessionID,
		LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration, TargetRunID: runID,
		Attachment:  ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: "runtime"},
		Requirement: ObservationRequirement{SignalFamily: "process", Placement: domain.CollectorPlacementHost, MinimumLevel: domain.CoverageLevelComplete, Required: true},
		Adapter:     "trace", Version: "v1", ConfigurationDigest: domain.NewDigest([]byte("config")),
		MaximumBytes: 10, StartedAt: time.Now().UTC(),
	}
}

func reconciliationArtifacts(t *testing.T, plan CollectorPlan, stdoutSize, stderrSize int64) []domain.ArtifactReference {
	t.Helper()
	result := make([]domain.ArtifactReference, 0, 2)
	for _, value := range []struct {
		role    string
		content []byte
		size    int64
	}{{CollectorStdoutArtifactRole, []byte("stdout"), stdoutSize}, {CollectorStderrArtifactRole, []byte("stderr"), stderrSize}} {
		digest := domain.NewDigest(value.content)
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
			Reference: "observer://collectors/" + plan.CollectorID.String() + "/" + strings.TrimPrefix(value.role, "collector.") + "/" + digest.String(),
			Digest:    digest, Size: value.size, Role: value.role, Sensitivity: domain.SensitivityInternal,
		})
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, artifact)
	}
	return result
}
