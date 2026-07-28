package orchestration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type controllerHarness struct {
	controller       *Controller
	capabilities     *Service
	agent            *testkit.FakeAgentWorkspaceDriver
	target           *testkit.FakeTargetDriver
	workspace        *testkit.FakeWorkspaceDriver
	capture          *captureControllerStub
	resolver         *StaticProvisioningResolver
	tracker          *testkit.OwnershipTracker
	inputViewID      string
	inputPath        string
	inputContent     []byte
	policyDigest     string
	capabilityDigest string
	materialDigest   string
	specimenRefs     []string
	inputSelection   application.InputSelectionRequest
}

func newControllerHarness(t *testing.T, fixture *integrationFixture, agentFaults, targetFaults *testkit.FaultInjector) controllerHarness {
	t.Helper()
	policy, err := domain.ParseDigest(fixture.view.Session.PolicyDigest)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := domain.ParseDigest(fixture.view.Session.CapabilityDigest)
	if err != nil {
		t.Fatal(err)
	}
	inputContent := []byte(t.Name() + "/input")
	inputSource := testkit.NewMemoryContentSource(inputContent)
	inputEntry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
		LogicalPath: "input/specimen.bin", OccurrenceRef: "memory://input/specimen",
		Digest: inputSource.Digest(), Size: inputSource.Size(), Mode: 0o444,
	})
	if err != nil {
		t.Fatal(err)
	}
	inputView, err := domain.NewInputViewManifest([]domain.InputViewEntry{inputEntry})
	if err != nil {
		t.Fatal(err)
	}
	material, materialDigest := targetMaterial(t)
	inputSelection := application.InputSelectionRequest{
		OccurrenceRefs: []string{"memory://input/specimen"},
		PathMappings:   []application.InputPathMappingRequest{{OccurrenceRef: "memory://input/specimen", LogicalPath: "input/specimen.bin"}},
		SecurityScope:  "test-scope",
	}
	resolver, err := NewStaticProvisioningResolver(StaticProvisioningConfig{
		Now: fixture.clock.Now,
		Agents: map[string]StaticAgentPlan{
			inputView.ID().String(): {
				Selection: inputSelection,
				InputView: inputView, SecurityScope: "test-scope", Construction: domain.InputViewAllowCopy,
				Content:        map[string]ports.ContentSource{"input/specimen.bin": inputSource},
				UpperByteLimit: 1 << 20, UpperInodeLimit: 128,
				PolicyDigest: policy, CapabilityDigest: capability,
				ImageDigest: domain.NewDigest([]byte("agent-image")),
			},
		},
		Targets: map[string]StaticTargetPlan{
			"linux-visible": {
				PolicyDigest: policy, CapabilityDigest: capability,
				Template: ports.TargetTemplate{
					Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "fake", Runtime: "fake",
					ImageDigest: domain.NewDigest([]byte("target-image")), IsolationProfile: "test",
				},
			},
		},
		Runs: map[string]StaticRunPlan{
			materialDigest.String(): {
				SpecimenOccurrenceRefs: []string{"memory://material/specimen"},
				RequiredCoverage:       []string{ports.TargetLifecycleSignal}, Material: material, MaximumDuration: time.Minute,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tracker := testkit.NewOwnershipTracker()
	agent := testkit.NewFakeAgentWorkspaceDriver(domain.CapabilityFingerprint{}, fixture.clock, agentFaults, tracker)
	target := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, targetFaults, tracker)
	workspace := testkit.NewFakeWorkspaceDriver(fixture.clock, nil, tracker)
	workspaceScope, err := NewCoreWorkspaceResolver(fixture.core)
	if err != nil {
		t.Fatal(err)
	}
	capture := &captureControllerStub{}
	finalizer, err := observationbundle.New(filepath.Join(t.TempDir(), "sealed"))
	if err != nil {
		t.Fatal(err)
	}
	finalization, err := application.NewRunFinalizationService(fixture.core, finalizer, testkit.NewFakeMaterialAuthority(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	observers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: fixture.ledger, IDs: fixture.ids, Clock: fixture.clock.Now,
		StateRoot: filepath.Join(t.TempDir(), "run-observers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := fixture.service(Config{
		Finalization: finalization, Agent: agent,
		Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: target},
		Observers: observers, Workspace: workspace, WorkspaceScope: workspaceScope, Captures: capture,
	})
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agent, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: target},
		Workspace: workspace, Resolver: resolver, Capabilities: capabilities, Observers: observers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controllerHarness{
		controller: controller, capabilities: capabilities, agent: agent, target: target, workspace: workspace, capture: capture, resolver: resolver, tracker: tracker,
		inputViewID: inputView.ID().String(), inputPath: "input/specimen.bin", inputContent: append([]byte(nil), inputContent...),
		policyDigest: policy.String(), capabilityDigest: capability.String(),
		materialDigest: materialDigest.String(), specimenRefs: []string{"memory://material/specimen"},
		inputSelection: cloneInputSelection(inputSelection),
	}
}

func (h controllerHarness) acquire(t *testing.T, fixture *integrationFixture) application.ResearchSessionView {
	t.Helper()
	view, err := h.controller.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("controller-acquire"), OwnerSubject: integrationOwner, InputViewID: h.inputViewID,
		PolicyDigest: h.policyDigest, CapabilityDigest: h.capabilityDigest, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil || generation.State != domain.AgentGenerationReady {
		t.Fatalf("agent generation was not physically readied: %#v, %v", generation, err)
	}
	return view
}

func (h controllerHarness) requireBoundInputContent(t *testing.T, fixture *integrationFixture, view application.ResearchSessionView) {
	t.Helper()
	request := application.AcquireRequest{
		Meta: fixture.meta("inspect-bound-input"), OwnerSubject: integrationOwner, InputViewID: h.inputViewID,
		PolicyDigest: h.policyDigest, CapabilityDigest: h.capabilityDigest, TTL: time.Hour,
	}
	ctx, cancel := context.WithDeadline(context.Background(), request.Meta.Deadline)
	defer cancel()
	resolved, err := h.resolver.ResolveAcquisition(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := bindAgentProvisioning(request, resolved, view)
	if err != nil {
		t.Fatal(err)
	}
	source := plan.Workspace.Content[h.inputPath]
	if source == nil || source.Digest() != domain.NewDigest(h.inputContent) || source.Size() != int64(len(h.inputContent)) {
		t.Fatalf("bound workspace content metadata does not match the manifest")
	}
	reader, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, h.inputContent) {
		t.Fatalf("bound workspace content = %q, want %q", content, h.inputContent)
	}
}

func (h controllerHarness) createTarget(t *testing.T, fixture *integrationFixture, view application.ResearchSessionView) application.TargetRecord {
	t.Helper()
	target, err := h.controller.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: fixture.meta("controller-target"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: h.policyDigest, CapabilityDigest: h.capabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(target)
	if err != nil || generation.State != domain.TargetGenerationReady {
		t.Fatalf("target generation was not physically readied: %#v, %v", generation, err)
	}
	return target
}

func TestControllerDrivesProvisionRunResetAndRelease(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	harness.requireBoundInputContent(t, fixture, view)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("controller-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.State != domain.TargetRunRunning {
		t.Fatalf("run state = %s, want running", run.State)
	}
	if _, err := harness.capabilities.StopTargetRun(context.Background(), &worldv1.StopTargetRunRequest{
		Mutation: fixture.wireMeta("controller-stop"), TargetId: target.ID, TargetRunId: run.ID,
		ExpectedRevision: run.Revision, Reason: "integration complete",
	}); err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = harness.controller.ResetTarget(context.Background(), application.ResetTargetRequest{
		Meta: fixture.meta("controller-reset"), TargetID: target.ID, ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := targetGeneration(target)
	if target.CurrentGeneration != 2 || generation.State != domain.TargetGenerationReady {
		t.Fatalf("reset target = %#v", target)
	}
	destroyRequest := &worldv1.DestroyTargetRequest{
		Mutation: fixture.wireMeta("controller-destroy"), TargetId: target.ID, ExpectedRevision: target.Revision,
		Reason: "release",
	}
	destroyed, err := harness.capabilities.DestroyTarget(context.Background(), destroyRequest)
	if err != nil {
		t.Fatal(err)
	}
	replayedDestroy, err := harness.capabilities.DestroyTarget(context.Background(), destroyRequest)
	if err != nil || replayedDestroy.Revision != destroyed.Revision {
		t.Fatalf("exact destroy replay = %#v, %v", replayedDestroy, err)
	}
	newDestroyKey := proto.Clone(destroyRequest).(*worldv1.DestroyTargetRequest)
	newDestroyKey.Mutation = fixture.wireMeta("controller-destroy-new-key")
	if _, err := harness.capabilities.DestroyTarget(context.Background(), newDestroyKey); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("terminal destroy with new key error = %v, want AlreadyExists", err)
	}
	changedDestroy := proto.Clone(destroyRequest).(*worldv1.DestroyTargetRequest)
	changedDestroy.Reason = "changed release reason"
	if _, err := harness.capabilities.DestroyTarget(context.Background(), changedDestroy); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("terminal destroy with changed request error = %v, want AlreadyExists", err)
	}
	view, err = fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("controller-release"), LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision, Reason: "done",
	}); err != nil {
		t.Fatal(err)
	}
	if err := harness.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestBindTargetResetPlanPreservesModeAndSnapshotSelection(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	target.CurrentGeneration = 2

	selections := []struct {
		mode         ports.ResetMode
		snapshotName string
	}{
		{mode: ports.ResetBaseline},
		{mode: ports.ResetRecreate},
		{mode: ports.ResetSnapshot, snapshotName: "known-good"},
	}
	for _, selection := range selections {
		request := application.ResetTargetRequest{
			Meta: fixture.meta("bind-reset"), TargetID: target.ID, ExpectedRevision: target.Revision,
			Mode: selection.mode, SnapshotName: selection.snapshotName,
		}
		plan, err := bindTargetResetPlan(request, target)
		if err != nil {
			t.Fatalf("%s: %v", selection.mode, err)
		}
		if plan.Mode != selection.mode || plan.SnapshotName != selection.snapshotName {
			t.Errorf("%s plan changed selection to %s/%q", selection.mode, plan.Mode, plan.SnapshotName)
		}
	}
}

func TestControllerRollsBackAmbiguousAgentAndTargetCreates(t *testing.T) {
	fixture := newIntegrationFixture(t)
	agentFaults, targetFaults := testkit.NewFaultInjector(), testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, agentFaults, targetFaults)
	agentFaults.FailNext("agent.provision.after", errors.New("ambiguous agent create"))
	view, err := harness.controller.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("rollback-agent"), OwnerSubject: integrationOwner, InputViewID: harness.inputViewID,
		PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest, TTL: time.Hour,
	})
	if err == nil {
		t.Fatal("ambiguous agent create unexpectedly succeeded")
	}
	persisted, loadErr := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	agentGeneration, _ := currentAgentGeneration(persisted.Agent)
	if agentGeneration.State != domain.AgentGenerationFailed || len(harness.tracker.Snapshot()) != 0 {
		t.Fatalf("agent rollback left state=%s ownership=%#v", agentGeneration.State, harness.tracker.Snapshot())
	}

	// Use a fresh harness/input plan because the failed logical acquisition is
	// intentionally terminal and cannot be retried under another physical plan.
	harness = newControllerHarness(t, fixture, nil, targetFaults)
	view = harness.acquire(t, fixture)
	targetFaults.FailNext("target.create.after", errors.New("ambiguous target create"))
	target, err := harness.controller.CreateTarget(context.Background(), application.CreateTargetRequest{
		Meta: fixture.meta("rollback-target"), LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: harness.policyDigest, CapabilityDigest: harness.capabilityDigest,
	})
	if err == nil {
		t.Fatal("ambiguous target create unexpectedly succeeded")
	}
	persistedTarget, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	targetGeneration, _ := targetGeneration(persistedTarget)
	for _, ownership := range harness.tracker.Snapshot() {
		if ownership.Kind == "target" {
			t.Fatalf("target rollback leaked ownership: %#v", harness.tracker.Snapshot())
		}
	}
	if targetGeneration.State != domain.TargetGenerationFailed {
		t.Fatalf("target rollback state = %s", targetGeneration.State)
	}
}

func TestControllerStartFailureStopsAndFinalizesEvidence(t *testing.T) {
	fixture := newIntegrationFixture(t)
	targetFaults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, targetFaults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	targetFaults.FailNext("target.start_run.after", errors.New("ambiguous target start"))
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("rollback-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err == nil {
		t.Fatal("ambiguous target start unexpectedly succeeded")
	}
	persistedTarget, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	persistedRun, loadErr := targetRun(persistedTarget, run.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	// The injected error occurs after the fake crosses its start boundary, so
	// its authoritative StopRun result is Completed despite the ambiguous RPC
	// outcome. The controller must preserve that evidence rather than force a
	// caller-selected Failed state.
	if persistedRun.State != domain.TargetRunCompleted || persistedRun.BundleID == "" {
		t.Fatalf("run rollback was not authoritatively finalized: %#v; controller error: %v", persistedRun, err)
	}
	if _, err := harness.capabilities.GetObservationBundle(context.Background(), &worldv1.GetObservationBundleRequest{TargetRunId: run.ID}); err != nil {
		t.Fatal(err)
	}
	for _, ownership := range harness.tracker.Snapshot() {
		if ownership.Kind == "target_run" {
			t.Fatalf("run rollback leaked ownership: %#v", harness.tracker.Snapshot())
		}
	}
}
