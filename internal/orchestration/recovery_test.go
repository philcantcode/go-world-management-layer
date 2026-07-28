package orchestration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestParseTargetRecoveryStrategyIsExplicit(t *testing.T) {
	tests := []struct {
		strategy string
		mode     ports.ResetMode
		snapshot string
		valid    bool
	}{
		{strategy: "recreate", mode: ports.ResetRecreate, valid: true},
		{strategy: "baseline", mode: ports.ResetBaseline, valid: true},
		{strategy: "snapshot:known-good", mode: ports.ResetSnapshot, snapshot: "known-good", valid: true},
		{strategy: "snapshot", valid: false},
		{strategy: "snapshot:", valid: false},
		{strategy: "cold recreate", valid: false},
		{strategy: " recreate", valid: false},
	}
	for _, test := range tests {
		t.Run(test.strategy, func(t *testing.T) {
			selection, err := parseTargetRecoveryStrategy(test.strategy)
			if (err == nil) != test.valid {
				t.Fatalf("parseTargetRecoveryStrategy(%q) error = %v, valid=%t", test.strategy, err, test.valid)
			}
			if err == nil && (selection.mode != test.mode || selection.snapshotName != test.snapshot) {
				t.Fatalf("selection = %#v, want mode=%s snapshot=%q", selection, test.mode, test.snapshot)
			}
		})
	}
}

func TestControllerTargetRecoveryResumesAfterAmbiguousPhysicalReset(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentLinuxTargetFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		TargetID: target.ID, TargetGeneration: target.CurrentGeneration, Trigger: "target runtime stopped responding",
		LastKnownState: "ready",
	})
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("controller-target-recovery"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceTarget,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence visible",
	}
	targetFaults.FailNext("target.reset.after", errors.New("ambiguous reset response"))
	if _, err := harness.controller.RecoverIncident(context.Background(), request); err == nil {
		t.Fatal("ambiguous physical reset unexpectedly completed recovery")
	}
	interim, err := fixture.core.GetIncident(context.Background(), incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if interim.State != domain.IncidentRecovering {
		t.Fatalf("incident state after ambiguous reset = %s, want recovering", interim.State)
	}

	request.Meta.Deadline = request.Meta.Deadline.Add(time.Minute)
	outcome, err := harness.controller.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertCompletedTargetRecovery(t, outcome, target.CurrentGeneration+1)
	request.Meta.Deadline = request.Meta.Deadline.Add(time.Minute)
	replayed, err := harness.controller.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertCompletedTargetRecovery(t, replayed, target.CurrentGeneration+1)
}

func TestControllerAgentRecoveryRetiresOldPhysicalGenerationAndResumes(t *testing.T) {
	fixture := newIntegrationFixture(t)
	agentFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, agentFaults, nil)
	view := harness.acquire(t, fixture)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentAgentWorkspaceFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		AgentWorkspaceID: view.Agent.ID, AgentGeneration: view.Agent.CurrentGeneration,
		Trigger: "agent supervisor stopped responding", LastKnownState: "ready",
	})
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("controller-agent-recovery"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceAgent,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence visible",
	}
	agentFaults.FailNext("agent.provision.after", errors.New("ambiguous agent provision response"))
	if _, err := harness.controller.RecoverIncident(context.Background(), request); err == nil {
		t.Fatal("ambiguous physical provision unexpectedly completed recovery")
	}
	interim, err := fixture.core.GetIncident(context.Background(), incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if interim.State != domain.IncidentRecovering {
		t.Fatalf("incident state after ambiguous provision = %s, want recovering", interim.State)
	}

	request.Meta.Deadline = request.Meta.Deadline.Add(time.Minute)
	outcome, err := harness.controller.RecoverIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Incident.State != domain.IncidentResolved || outcome.Agent == nil || outcome.Lease == nil {
		t.Fatalf("agent recovery outcome = %#v", outcome)
	}
	generation, err := currentAgentGeneration(*outcome.Agent)
	if err != nil || generation.Generation != view.Agent.CurrentGeneration+1 || generation.State != domain.AgentGenerationReady {
		t.Fatalf("recovered agent generation = %#v, %v", generation, err)
	}
	if outcome.Lease.AgentGeneration != generation.Generation {
		t.Fatalf("lease agent generation = %d, want %d", outcome.Lease.AgentGeneration, generation.Generation)
	}
	if err := requireOnlyCurrentAgentOwnership(harness.tracker.Snapshot(), outcome.Agent.ID, generation.Generation); err != nil {
		t.Fatal(err)
	}
	request.Meta.Deadline = request.Meta.Deadline.Add(time.Minute)
	if _, err := harness.controller.RecoverIncident(context.Background(), request); err != nil {
		t.Fatalf("exact terminal recovery replay: %v", err)
	}
}

func TestControllerAgentRecoveryAdmissionDenialPreservesPreviousPhysicalGeneration(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentAgentWorkspaceFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		AgentWorkspaceID: view.Agent.ID, AgentGeneration: view.Agent.CurrentGeneration,
		Trigger: "agent policy drift", LastKnownState: "ready",
	})
	denied := errors.New("physical policy denied recovery plan")
	resolver := &denyingAgentPlanResolver{ProvisioningResolver: harness.resolver, err: denied}
	harness.controller.resolver = resolver
	before := harness.tracker.Snapshot()
	request := application.RecoverIncidentRequest{
		Meta: fixture.meta("controller-agent-recovery-denied"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceAgent,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence visible",
	}
	if _, err := harness.controller.RecoverIncident(context.Background(), request); !errors.Is(err, denied) {
		t.Fatalf("recovery admission error = %v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("agent plan admission calls = %d, want 1", resolver.calls)
	}
	after := harness.tracker.Snapshot()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("denied recovery changed physical ownership:\nbefore=%#v\nafter=%#v", before, after)
	}
	current, err := fixture.core.GetIncident(context.Background(), incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.IncidentRecovering {
		t.Fatalf("incident state = %s, want recovering for retry", current.State)
	}
}

type denyingAgentPlanResolver struct {
	ProvisioningResolver
	err   error
	calls int
}

func (r *denyingAgentPlanResolver) AdmitAgentWorkspacePlan(context.Context, ports.AgentWorkspacePlan) error {
	r.calls++
	return r.err
}

func sealControllerIncident(t *testing.T, fixture *integrationFixture, request application.CreateIncidentRequest) application.IncidentRecord {
	t.Helper()
	content := []byte(request.Trigger)
	request.Meta = fixture.meta("controller-incident")
	request.Cause = application.CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not yet proven", Confidence: 0}
	request.Artifacts = []application.IncidentArtifactRecord{{
		Reference: "artifact://controller-recovery/evidence", Digest: domain.NewDigest(content).String(), Size: int64(len(content)),
		Role: "incident-evidence", Sensitivity: domain.SensitivityInternal,
	}}
	incident, err := fixture.core.CreateIncident(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	incident, err = fixture.core.TransitionIncident(context.Background(), application.TransitionIncidentRequest{
		Meta: fixture.meta("controller-incident-seal"), IncidentID: incident.ID, ExpectedRevision: incident.Revision,
		State: domain.IncidentEvidenceSealed, VisibilityAcknowledgements: []string{"evidence indexed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return incident
}

func assertCompletedTargetRecovery(t *testing.T, outcome application.RecoveryOutcome, generation uint64) {
	t.Helper()
	if outcome.Incident.State != domain.IncidentResolved || outcome.Target == nil {
		t.Fatalf("target recovery outcome = %#v", outcome)
	}
	current, err := targetGeneration(*outcome.Target)
	if err != nil || current.Generation != generation || current.State != domain.TargetGenerationReady {
		t.Fatalf("recovered target generation = %#v, %v", current, err)
	}
}

func requireOnlyCurrentAgentOwnership(entries []testkit.Ownership, agentID string, generation uint64) error {
	want := agentID + "/" + fmt.Sprint(generation)
	count := 0
	for _, entry := range entries {
		if entry.Kind != "agent_workspace" {
			continue
		}
		count++
		if entry.ID != want {
			return fmt.Errorf("stale agent ownership remains: %#v", entries)
		}
	}
	if count != 1 {
		return fmt.Errorf("current agent ownership count = %d, want 1: %#v", count, entries)
	}
	return nil
}
