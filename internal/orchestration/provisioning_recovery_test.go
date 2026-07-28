package orchestration

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestStaticProvisioningResolveAgentRecoveryUsesPersistedGenerationIdentity(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := recoverySessionView(t, fixture, harness.acquire(t, fixture))
	harness.resolver.now = fixture.clock.Now
	request := application.RecoverIncidentRequest{IncidentID: view.Incidents[0].ID, Resource: application.RecoveryResourceAgent}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resolved, err := harness.resolver.ResolveAgentRecovery(ctx, request, view)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.InputView.ID().String() != generation.InputViewID || resolved.PolicyDigest.String() != generation.PolicyDigest || resolved.CapabilityDigest.String() != generation.CapabilityDigest {
		t.Fatalf("resolved recovery provenance does not match generation: %#v", resolved)
	}
}

func TestStaticProvisioningResolveAgentRecoveryFailsClosedOnScopeDrift(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	valid := recoverySessionView(t, fixture, harness.acquire(t, fixture))
	harness.resolver.now = fixture.clock.Now

	tests := map[string]func(*application.ResearchSessionView){
		"expired lease": func(view *application.ResearchSessionView) {
			view.Lease.ExpiresAt = fixture.clock.Now()
		},
		"terminating lease": func(view *application.ResearchSessionView) {
			view.Lease.Termination = application.LeaseTerminationRecord{Kind: application.LeaseTerminationRelease, State: application.LeaseTerminationReleasing}
		},
		"mismatched capability": func(view *application.ResearchSessionView) {
			view.Lease.CapabilityDigest = domain.NewDigest([]byte("different")).String()
		},
		"unlinked incident": func(view *application.ResearchSessionView) {
			view.Agent.Generations[len(view.Agent.Generations)-1].RecoveryIncident = ""
		},
		"wrong incident generation": func(view *application.ResearchSessionView) {
			view.Incidents[0].AgentGeneration++
		},
		"nonadjacent predecessor": func(view *application.ResearchSessionView) {
			view.Agent.Generations[len(view.Agent.Generations)-1].Previous = 42
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			view := cloneRecoverySessionView(valid)
			mutate(&view)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := harness.resolver.ResolveAgentRecovery(ctx, application.RecoverIncidentRequest{
				IncidentID: valid.Incidents[0].ID, Resource: application.RecoveryResourceAgent,
			}, view)
			if err == nil {
				t.Fatal("scope drift was accepted")
			}
		})
	}
}

func TestStaticProvisioningRejectsPersistedProvisioningProvenanceDrift(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	acquire := application.AcquireRequest{
		Meta: fixture.meta("provenance-acquire"), InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resolved, err := harness.resolver.ResolveAcquisition(ctx, acquire)
	if err != nil {
		t.Fatal(err)
	}
	driftedView := view
	driftedView.Agent.Generations = append([]application.AgentGenerationRecord(nil), view.Agent.Generations...)
	driftedView.Agent.Generations[len(driftedView.Agent.Generations)-1].CapabilityDigest = domain.NewDigest([]byte("drifted-capability")).String()
	if _, err := bindAgentProvisioning(acquire, resolved, driftedView); err == nil {
		t.Fatal("persisted agent-generation provenance drift was overwritten")
	}

	target := harness.createTarget(t, fixture, view)
	request := application.CreateTargetRequest{
		Meta: fixture.meta("provenance-target"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	}
	target.Generations = append([]application.TargetGenerationRecord(nil), target.Generations...)
	target.Generations[len(target.Generations)-1].PolicyDigest = domain.NewDigest([]byte("drifted-policy")).String()
	if _, err := harness.resolver.ResolveTarget(ctx, request, target); err == nil {
		t.Fatal("persisted target-generation provenance drift was overwritten")
	}
}

func TestBoundAgentPlanRejectsProfileDriftBeforePhysicalRetry(t *testing.T) {
	fixture := newIntegrationFixture(t)
	faults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, faults, nil)
	request := application.AcquireRequest{
		Meta: fixture.meta("bound-plan-acquire"), OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	}
	view, err := harness.controller.AcquireResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil || generation.ProvisioningPlanDigest == "" || generation.WorkspaceProvisioningKey == "" || generation.AgentProvisioningKey == "" {
		t.Fatalf("agent generation has no immutable physical binding: %#v, %v", generation, err)
	}
	ownershipBefore := harness.tracker.Snapshot()
	before := faults.Hits("agent.provision.before")
	configured := harness.resolver.agents[harness.inputViewID]
	configured.ImageDigest = domain.NewDigest([]byte("silently changed image"))
	harness.resolver.agents[harness.inputViewID] = configured
	if _, err := harness.controller.AcquireResearchSession(context.Background(), request); err == nil {
		t.Fatal("exact acquisition retry accepted a changed physical profile")
	}
	if got := faults.Hits("agent.provision.before"); got != before {
		t.Fatalf("profile drift reached the physical driver: before=%d after=%d", before, got)
	}
	if after := harness.tracker.Snapshot(); !reflect.DeepEqual(after, ownershipBefore) {
		t.Fatalf("profile drift changed existing physical ownership:\nbefore=%#v\nafter=%#v", ownershipBefore, after)
	}
	persisted, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedGeneration, err := currentAgentGeneration(persisted.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Lease.State != domain.LeaseActive || persistedGeneration.State != domain.AgentGenerationReady {
		t.Fatalf("profile drift retired the durable generation: lease=%s generation=%s", persisted.Lease.State, persistedGeneration.State)
	}
}

func TestBoundTargetRunPlanRejectsObserverDriftBeforePhysicalRetry(t *testing.T) {
	fixture := newIntegrationFixture(t)
	faults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, faults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	request := application.StartTargetRunRequest{
		Meta: fixture.meta("bound-target-run-plan"), TargetID: target.ID,
		SpecimenOccurrenceRefs: append([]string(nil), harness.specimenRefs...),
	}
	run, err := harness.controller.StartTargetRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if run.ProvisioningPlanDigest == "" || run.ProvisioningKey == "" || run.State != domain.TargetRunRunning {
		t.Fatalf("target run has no immutable physical binding: %#v", run)
	}
	beforeHits := faults.Hits("target.prepare_run.before")
	beforeOwnership := harness.tracker.Snapshot()
	configured := harness.resolver.runs[harness.materialDigest]
	configured.MaximumDuration += time.Second
	harness.resolver.runs[harness.materialDigest] = configured
	if _, err := harness.controller.StartTargetRun(context.Background(), request); err == nil {
		t.Fatal("exact target run retry accepted changed observer/duration authority")
	}
	if got := faults.Hits("target.prepare_run.before"); got != beforeHits {
		t.Fatalf("run-plan drift reached the physical driver: before=%d after=%d", beforeHits, got)
	}
	if after := harness.tracker.Snapshot(); !reflect.DeepEqual(after, beforeOwnership) {
		t.Fatalf("run-plan drift changed physical ownership:\nbefore=%#v\nafter=%#v", beforeOwnership, after)
	}
	persisted, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedRun, err := targetRun(persisted, run.ID)
	if err != nil || persistedRun.State != domain.TargetRunRunning {
		t.Fatalf("run-plan drift retired the durable run: %#v, %v", persistedRun, err)
	}
}

func recoverySessionView(t *testing.T, fixture *integrationFixture, view application.ResearchSessionView) application.ResearchSessionView {
	t.Helper()
	incidentID, err := fixture.ids.IncidentID()
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, err := fixture.ids.WorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	previous, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	current := previous
	current.Generation++
	current.WorkspaceID = workspaceID.String()
	current.Previous = previous.Generation
	current.RecoveryIncident = incidentID.String()
	current.State = domain.AgentGenerationProvisioning
	current.Revision = 1
	current.CreatedAt = fixture.clock.Now()
	current.UpdatedAt = fixture.clock.Now()
	view.Agent.Generations = append(append([]application.AgentGenerationRecord(nil), view.Agent.Generations...), current)
	view.Agent.CurrentGeneration = current.Generation
	view.Lease.AgentGeneration = current.Generation
	view.Incidents = []application.IncidentRecord{{
		ID: incidentID.String(), SessionID: view.Session.ID, LeaseID: view.Lease.ID,
		AgentWorkspaceID: view.Agent.ID, AgentGeneration: previous.Generation,
		State: domain.IncidentRecovering,
	}}
	return view
}

func cloneRecoverySessionView(value application.ResearchSessionView) application.ResearchSessionView {
	value.Agent.Generations = append([]application.AgentGenerationRecord(nil), value.Agent.Generations...)
	value.Incidents = append([]application.IncidentRecord(nil), value.Incidents...)
	return value
}
