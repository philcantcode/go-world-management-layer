package orchestration

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestControllerCreateTargetAdmissionDenialPrecedesLogicalAndPhysicalAllocation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	before, err := fixture.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	physicalCalls := targetFaults.Hits("target.create.before")
	physicalOwnership := harness.tracker.Snapshot()

	denied := errors.New("target request denied by policy")
	resolver := &orderingAdmissionResolver{
		ProvisioningResolver: harness.resolver,
		targetRequestErr:     denied,
	}
	harness.controller.resolver = resolver
	_, err = harness.controller.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: fixture.meta("target-request-admission-denied"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	})
	if !errors.Is(err, denied) {
		t.Fatalf("CreateTarget error = %v, want admission denial", err)
	}
	if resolver.targetRequestCalls != 1 {
		t.Fatalf("target request admission calls = %d, want 1", resolver.targetRequestCalls)
	}
	after, err := fixture.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	requireUnchangedLogicalView(t, before, after)
	requireNoNewPhysicalCall(t, targetFaults, "target.create.before", physicalCalls)
	if got := harness.tracker.Snapshot(); !reflect.DeepEqual(got, physicalOwnership) {
		t.Fatalf("denied target request changed physical ownership:\nbefore=%#v\nafter=%#v", physicalOwnership, got)
	}
}

func TestControllerResetTargetAdmissionDenialPrecedesLogicalRolloverAndPhysicalReset(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	before, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	physicalCalls := targetFaults.Hits("target.reset.before")
	physicalOwnership := harness.tracker.Snapshot()

	denied := errors.New("target reset denied by policy")
	resolver := &orderingAdmissionResolver{
		ProvisioningResolver: harness.resolver,
		targetResetErr:       denied,
	}
	harness.controller.resolver = resolver
	_, err = harness.controller.ResetTarget(context.Background(), application.ResetTargetRequest{
		Meta: fixture.meta("target-reset-admission-denied"), TargetID: target.ID,
		ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
	})
	if !errors.Is(err, denied) {
		t.Fatalf("ResetTarget error = %v, want admission denial", err)
	}
	if resolver.targetResetCalls != 1 {
		t.Fatalf("target reset admission calls = %d, want 1", resolver.targetResetCalls)
	}
	after, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("denied target reset changed logical target:\nbefore=%#v\nafter=%#v", before, after)
	}
	requireNoNewPhysicalCall(t, targetFaults, "target.reset.before", physicalCalls)
	if got := harness.tracker.Snapshot(); !reflect.DeepEqual(got, physicalOwnership) {
		t.Fatalf("denied target reset changed physical ownership:\nbefore=%#v\nafter=%#v", physicalOwnership, got)
	}
}

func TestControllerTargetRecoveryAdmissionDenialPrecedesIncidentAndTargetMutation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentLinuxTargetFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		TargetID: target.ID, TargetGeneration: target.CurrentGeneration, Trigger: "target recovery policy denial",
		LastKnownState: "ready",
	})
	beforeTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	physicalCalls := targetFaults.Hits("target.reset.before")
	physicalOwnership := harness.tracker.Snapshot()

	denied := errors.New("target incident recovery denied by policy")
	resolver := &orderingAdmissionResolver{
		ProvisioningResolver: harness.resolver,
		targetResetErr:       denied,
	}
	harness.controller.resolver = resolver
	_, err = harness.controller.RecoverIncident(context.Background(), application.RecoverIncidentRequest{
		Meta: fixture.meta("target-recovery-admission-denied"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceTarget,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence remains visible",
	})
	if !errors.Is(err, denied) {
		t.Fatalf("target RecoverIncident error = %v, want admission denial", err)
	}
	if resolver.targetResetCalls != 1 {
		t.Fatalf("target recovery admission calls = %d, want 1", resolver.targetResetCalls)
	}
	requireEvidenceSealedIncident(t, fixture, incident)
	afterTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterTarget, beforeTarget) {
		t.Fatalf("denied target recovery changed logical target:\nbefore=%#v\nafter=%#v", beforeTarget, afterTarget)
	}
	requireNoNewPhysicalCall(t, targetFaults, "target.reset.before", physicalCalls)
	if got := harness.tracker.Snapshot(); !reflect.DeepEqual(got, physicalOwnership) {
		t.Fatalf("denied target recovery changed physical ownership:\nbefore=%#v\nafter=%#v", physicalOwnership, got)
	}
}

func TestControllerAgentRecoveryAdmissionDenialPrecedesIncidentAndAgentMutation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	agentFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, agentFaults, nil)
	view := harness.acquire(t, fixture)
	incident := sealControllerIncident(t, fixture, application.CreateIncidentRequest{
		Classification: domain.IncidentAgentWorkspaceFailure, SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		AgentWorkspaceID: view.Agent.ID, AgentGeneration: view.Agent.CurrentGeneration,
		Trigger: "agent recovery policy denial", LastKnownState: "ready",
	})
	before, err := fixture.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	provisionCalls := agentFaults.Hits("agent.provision.before")
	destroyCalls := agentFaults.Hits("agent.destroy")
	physicalOwnership := harness.tracker.Snapshot()

	denied := errors.New("agent incident recovery denied by policy")
	resolver := &orderingAdmissionResolver{
		ProvisioningResolver: harness.resolver,
		agentRecoveryErr:     denied,
	}
	harness.controller.resolver = resolver
	_, err = harness.controller.RecoverIncident(context.Background(), application.RecoverIncidentRequest{
		Meta: fixture.meta("agent-recovery-request-admission-denied"), IncidentID: incident.ID,
		ExpectedIncidentRevision: incident.Revision, Resource: application.RecoveryResourceAgent,
		Strategy: string(ports.ResetRecreate), VisibilityAcknowledgement: "sealed evidence remains visible",
	})
	if !errors.Is(err, denied) {
		t.Fatalf("agent RecoverIncident error = %v, want admission denial", err)
	}
	if resolver.agentRecoveryCalls != 1 {
		t.Fatalf("agent recovery request admission calls = %d, want 1", resolver.agentRecoveryCalls)
	}
	requireEvidenceSealedIncident(t, fixture, incident)
	after, err := fixture.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	requireUnchangedLogicalView(t, before, after)
	requireNoNewPhysicalCall(t, agentFaults, "agent.provision.before", provisionCalls)
	requireNoNewPhysicalCall(t, agentFaults, "agent.destroy", destroyCalls)
	if got := harness.tracker.Snapshot(); !reflect.DeepEqual(got, physicalOwnership) {
		t.Fatalf("denied agent recovery changed physical ownership:\nbefore=%#v\nafter=%#v", physicalOwnership, got)
	}
}

type orderingAdmissionResolver struct {
	ProvisioningResolver
	targetRequestErr   error
	targetResetErr     error
	agentRecoveryErr   error
	targetRequestCalls int
	targetResetCalls   int
	agentRecoveryCalls int
}

func (r *orderingAdmissionResolver) AdmitTargetRequest(context.Context, application.CreateTargetRequest, application.ResearchSessionView) error {
	r.targetRequestCalls++
	return r.targetRequestErr
}

func (r *orderingAdmissionResolver) AdmitTargetReset(context.Context, application.ResetTargetRequest, application.TargetRecord, application.ResearchSessionView, *application.IncidentRecord) error {
	r.targetResetCalls++
	return r.targetResetErr
}

func (r *orderingAdmissionResolver) AdmitAgentRecoveryRequest(context.Context, application.RecoverIncidentRequest, application.ResearchSessionView, application.IncidentRecord) error {
	r.agentRecoveryCalls++
	return r.agentRecoveryErr
}

func requireUnchangedLogicalView(t *testing.T, before, after application.ResearchSessionView) {
	t.Helper()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("admission denial changed logical session view:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func requireNoNewPhysicalCall(t *testing.T, faults *testkit.FaultInjector, point string, before int) {
	t.Helper()
	if after := faults.Hits(point); after != before {
		t.Fatalf("physical call %q hits = %d, want unchanged %d", point, after, before)
	}
}

func requireEvidenceSealedIncident(t *testing.T, fixture *integrationFixture, before application.IncidentRecord) {
	t.Helper()
	after, err := fixture.core.GetIncident(context.Background(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.IncidentEvidenceSealed {
		t.Fatalf("incident state = %s, want evidence_sealed", after.State)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("admission denial changed incident:\nbefore=%#v\nafter=%#v", before, after)
	}
}
