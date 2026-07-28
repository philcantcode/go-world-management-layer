package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

type onceFailWorkspaceDriver struct {
	ports.WorkspaceDriver
	mountErr   error
	releaseErr error
}

func (d *onceFailWorkspaceDriver) Mount(ctx context.Context, id domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	if d.mountErr != nil {
		err := d.mountErr
		d.mountErr = nil
		return ports.WorkspaceHandle{}, err
	}
	return d.WorkspaceDriver.Mount(ctx, id)
}

func (d *onceFailWorkspaceDriver) Release(ctx context.Context, id domain.WorkspaceID) error {
	if d.releaseErr != nil {
		err := d.releaseErr
		d.releaseErr = nil
		return err
	}
	return d.WorkspaceDriver.Release(ctx, id)
}

type cancellingProvisionDriver struct {
	ports.AgentWorkspaceDriver
	cancel context.CancelFunc
}

func (d cancellingProvisionDriver) Provision(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	result, err := d.AgentWorkspaceDriver.Provision(ctx, plan)
	if err != nil {
		return result, err
	}
	d.cancel()
	<-ctx.Done()
	return result, ctx.Err()
}

type coverageMismatchTargetDriver struct{ ports.TargetDriver }

func (d coverageMismatchTargetDriver) PrepareRun(ctx context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	prepared, err := d.TargetDriver.PrepareRun(ctx, plan)
	if err == nil {
		prepared.RequiredCoverage = []string{"different-family"}
	}
	return prepared, err
}

type cancelOnDestroyTargetDriver struct {
	ports.TargetDriver
	cancel       context.CancelFunc
	destroyCalls int
}

func (d *cancelOnDestroyTargetDriver) Destroy(ctx context.Context, _ ports.TargetRef) error {
	d.destroyCalls++
	d.cancel()
	return ctx.Err()
}

func TestControllerAllowsOnlyFullyLogicalOrFullyPhysicalComposition(t *testing.T) {
	fixture := newIntegrationFixture(t)
	logical, err := NewController(ControllerConfig{Core: fixture.core})
	if err != nil {
		t.Fatal(err)
	}
	view, err := logical.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("logical-only"), OwnerSubject: integrationOwner,
		InputViewID: domain.NewInputViewID([]byte("logical-only")).String(), PolicyDigest: fixture.view.Session.PolicyDigest,
		CapabilityDigest: fixture.view.Session.CapabilityDigest, TTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("deliberate logical-only acquisition failed: %v", err)
	}
	if view.Lease.State != domain.LeaseActive {
		t.Fatalf("logical-only lease state = %s, want active", view.Lease.State)
	}

	resolver, err := NewStaticProvisioningResolver(StaticProvisioningConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewController(ControllerConfig{Core: fixture.core, Resolver: resolver}); err == nil {
		t.Fatal("partial physical composition unexpectedly succeeded")
	}
}

func TestReleasePhysicalStopsCallingDriversAfterContextEnds(t *testing.T) {
	targetID, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	driver := &cancelOnDestroyTargetDriver{cancel: cancel}
	controller := &Controller{targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver}}
	view := application.ResearchSessionView{Targets: []application.TargetRecord{{
		ID: targetID.String(), Kind: domain.TargetLinuxContainer,
		Generations: []application.TargetGenerationRecord{{Generation: 1}, {Generation: 2}},
	}}}
	if err := controller.releasePhysical(ctx, view); !errors.Is(err, context.Canceled) {
		t.Fatalf("releasePhysical() error = %v, want cancellation", err)
	}
	if driver.destroyCalls != 1 {
		t.Fatalf("Destroy() calls = %d, want no calls after context cancellation", driver.destroyCalls)
	}
}

func TestControllerRejectsMissingTargetDriverBeforeMutation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	before, err := fixture.store.Records(context.Background(), 0, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	_, err = harness.controller.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: fixture.meta("missing-target-driver"), LeaseID: view.Lease.ID, Template: "android-visible",
		Kind: domain.TargetAndroidVirtualDevice, PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	})
	if !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
		t.Fatalf("CreateTarget() error = %v, want capability unavailable", err)
	}
	after, err := fixture.store.Records(context.Background(), 0, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("missing target driver persisted %d unexpected control records", len(after)-len(before))
	}
	releaseControllerSession(t, fixture, harness, view)
}

func TestControllerResolvesAcquisitionSelectionBeforeCore(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view, err := harness.controller.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("selection-acquire"), OwnerSubject: integrationOwner,
		InputSelection: cloneInputSelection(harness.inputSelection), PolicyDigest: harness.policyDigest,
		CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.Session.InputViewID != harness.inputViewID || view.Lease.InputViewID != harness.inputViewID {
		t.Fatalf("resolved input view was not propagated: %#v", view)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	inspectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handle, err := harness.workspace.Inspect(inspectCtx, workspaceID)
	if err != nil || handle.State != domain.WorkspaceMounted {
		t.Fatalf("workspace Inspect() = %#v, %v", handle, err)
	}
	releaseControllerSession(t, fixture, harness, view)
}

func TestControllerResolvesTargetReferencesBeforeCore(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("reference-run"), TargetID: target.ID,
		SpecimenOccurrenceRefs: append([]string(nil), harness.specimenRefs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.MaterializationDigest != harness.materialDigest || run.State != domain.TargetRunRunning {
		t.Fatalf("resolved run = %#v", run)
	}
	if _, err := harness.capabilities.StopTargetRun(context.Background(), &worldv1.StopTargetRunRequest{
		Mutation: fixture.wireMeta("stop-reference-run"), TargetId: target.ID, TargetRunId: run.ID,
		ExpectedRevision: run.Revision, Reason: "test complete",
	}); err != nil {
		t.Fatal(err)
	}
	releaseControllerSession(t, fixture, harness, view)
}

func TestControllerCompensatesWorkspaceMountFailure(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	harness.controller.workspace = &onceFailWorkspaceDriver{WorkspaceDriver: harness.workspace, mountErr: errors.New("mount failed")}
	view, err := harness.controller.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("mount-failure"), OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	})
	if err == nil {
		t.Fatal("workspace mount failure unexpectedly succeeded")
	}
	assertFailedAcquisitionReleased(t, fixture, harness, view)
}

func TestControllerRetriesInterruptedAcquisitionCompensation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	harness.controller.workspace = &onceFailWorkspaceDriver{
		WorkspaceDriver: harness.workspace,
		mountErr:        errors.New("mount failed"),
		releaseErr:      errors.New("first compensation release failed"),
	}
	request := application.AcquireRequest{
		Meta: fixture.meta("retry-compensation"), OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	}
	view, err := harness.controller.AcquireResearchSession(context.Background(), request)
	if err == nil {
		t.Fatal("acquisition unexpectedly succeeded")
	}
	persisted, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Lease.State != domain.LeaseReleasing {
		t.Fatalf("interrupted compensation lease state = %s, want releasing", persisted.Lease.State)
	}
	view, err = harness.controller.AcquireResearchSession(context.Background(), request)
	if err == nil {
		t.Fatal("failed acquisition replay unexpectedly reported success")
	}
	assertFailedAcquisitionReleased(t, fixture, harness, view)
}

func TestControllerCleanupSurvivesCallerCancellation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness.controller.agent = cancellingProvisionDriver{AgentWorkspaceDriver: harness.agent, cancel: cancel}
	meta := fixture.meta("deadline-provision")
	view, err := harness.controller.AcquireResearchSession(requestContext, application.AcquireRequest{
		Meta: meta, OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	})
	if err == nil {
		t.Fatal("deadline-crossing provision unexpectedly succeeded")
	}
	assertFailedAcquisitionReleased(t, fixture, harness, view)
}

func TestControllerReleaseRetryStaysReleasingUntilWorkspaceCleanupSucceeds(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	harness.controller.workspace = &onceFailWorkspaceDriver{WorkspaceDriver: harness.workspace, releaseErr: errors.New("release failed")}
	request := application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("retry-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "done",
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), request); err == nil {
		t.Fatal("release unexpectedly succeeded despite workspace failure")
	}
	intermediate, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if intermediate.Lease.State != domain.LeaseReleasing || intermediate.Session.State != domain.ResearchSessionReleasing {
		t.Fatalf("failed cleanup manufactured terminal release: %#v", intermediate)
	}
	agentGeneration, _ := currentAgentGeneration(intermediate.Agent)
	if agentGeneration.State != domain.AgentGenerationReady {
		t.Fatalf("failed cleanup retired logical agent before completion: %s", agentGeneration.State)
	}
	outcome, err := harness.controller.ReleaseResearchSession(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.SessionID != view.Session.ID {
		t.Fatalf("release outcome = %#v", outcome)
	}
	assertReleasedWithoutLeaks(t, fixture, harness, view.Session.ID)
}

func TestControllerReleaseRetryStaysReleasingUntilTargetCleanupSucceeds(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	harness.createTarget(t, fixture, view)
	targetFaults.FailNext("target.destroy", errors.New("target destroy failed"))
	request := application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("retry-target-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "done",
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), request); err == nil {
		t.Fatal("release unexpectedly succeeded despite target cleanup failure")
	}
	intermediate, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if intermediate.Lease.State != domain.LeaseReleasing {
		t.Fatalf("lease state = %s, want releasing", intermediate.Lease.State)
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	assertReleasedWithoutLeaks(t, fixture, harness, view.Session.ID)
}

func TestControllerRejectsPreparedCoverageDriftWithoutManufacturingSuccess(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	wrapped := coverageMismatchTargetDriver{TargetDriver: harness.target}
	harness.controller.targets[domain.TargetLinuxContainer] = wrapped
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("coverage-drift"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err == nil {
		t.Fatal("coverage drift unexpectedly succeeded")
	}
	persistedTarget, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	persistedRun, loadErr := targetRun(persistedTarget, run.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if persistedRun.State != domain.TargetRunFailed || persistedRun.BundleID == "" || len(persistedRun.IncidentIDs) != 1 {
		t.Fatalf("coverage drift was not finalized as a typed failure: %#v; error=%v", persistedRun, err)
	}
	incident, loadErr := fixture.core.GetIncident(context.Background(), persistedRun.IncidentIDs[0])
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if incident.Classification != domain.IncidentControlPlaneFailure || incident.TargetRunID != persistedRun.ID || incident.TargetID != target.ID {
		t.Fatalf("rollback incident does not identify the failed run: %#v", incident)
	}
	if _, err := harness.capabilities.GetObservationBundle(context.Background(), &worldv1.GetObservationBundleRequest{TargetRunId: persistedRun.ID}); err != nil {
		t.Fatal(err)
	}
	releaseControllerSession(t, fixture, harness, view)
}

func releaseControllerSession(t *testing.T, fixture *integrationFixture, harness controllerHarness, view application.ResearchSessionView) {
	t.Helper()
	current, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("test-release"), LeaseID: current.Lease.ID,
		ExpectedRevision: current.Lease.Revision, Reason: "test cleanup",
	}); err != nil {
		t.Fatal(err)
	}
	assertReleasedWithoutLeaks(t, fixture, harness, view.Session.ID)
}

func assertFailedAcquisitionReleased(t *testing.T, fixture *integrationFixture, harness controllerHarness, view application.ResearchSessionView) {
	t.Helper()
	if view.Session.ID == "" {
		t.Fatal("failed post-core acquisition did not return its audit identity")
	}
	persisted, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(persisted.Agent)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Lease.State != domain.LeaseReleased || persisted.Session.State != domain.ResearchSessionReleased || generation.State != domain.AgentGenerationFailed {
		t.Fatalf("failed acquisition was not safely retired: %#v", persisted)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func assertReleasedWithoutLeaks(t *testing.T, fixture *integrationFixture, harness controllerHarness, sessionID string) {
	t.Helper()
	persisted, err := fixture.core.GetResearchSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Lease.State != domain.LeaseReleased || persisted.Session.State != domain.ResearchSessionReleased {
		t.Fatalf("session did not reach released: %#v", persisted)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}
