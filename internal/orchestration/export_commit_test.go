package orchestration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	workspacedirectory "github.com/philcantcode/go-world-management-layer/internal/drivers/workspace/directory"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type exportCommitScenario struct {
	fixture           *integrationFixture
	base              controllerHarness
	workspaceRoot     string
	workspace         *workspacedirectory.Driver
	workspaceResolver *CoreWorkspaceResolver
	materialFaults    *testkit.FaultInjector
	material          *testkit.FakeMaterialAuthority
	captureController *captureControllerStub
	service           *Service
	controller        *Controller
	view              application.ResearchSessionView
	workspaceID       domain.WorkspaceID
	handle            ports.WorkspaceHandle
	outputs           map[string][]byte
	preview           *worldv1.ChangeSet
	declareMutation   *worldv1.MutationMetadata
	declareRequest    *worldv1.DeclareExportRequest
	declared          *worldv1.Export
	commitMutation    *worldv1.MutationMetadata
	commitRequest     *worldv1.CommitExportRequest
}

func newExportCommitScenario(t *testing.T) exportCommitScenario {
	t.Helper()
	fixture := newIntegrationFixture(t)
	base := newControllerHarness(t, fixture, nil, nil)
	workspaceRoot := filepath.Join(t.TempDir(), "workspaces")
	workspace, err := workspacedirectory.New(workspacedirectory.Config{Root: workspaceRoot, Now: fixture.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	workspaceResolver, err := NewCoreWorkspaceResolver(fixture.core)
	if err != nil {
		t.Fatal(err)
	}
	materialFaults := testkit.NewFaultInjector()
	material := testkit.NewFakeMaterialAuthority(materialFaults, nil)
	captureController := &captureControllerStub{}
	service := fixture.service(Config{
		Agent: base.agent, Workspace: workspace, WorkspaceScope: workspaceResolver,
		Material: material, Captures: captureController,
	})
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: base.agent, Workspace: workspace, Resolver: base.resolver, Capabilities: service,
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := controller.AcquireResearchSession(context.Background(), application.AcquireRequest{
		Meta: fixture.meta("export-acquire"), OwnerSubject: integrationOwner, InputViewID: base.inputViewID,
		PolicyDigest: base.policyDigest, CapabilityDigest: base.capabilityDigest, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil || generation.State != domain.AgentGenerationReady {
		t.Fatalf("acquired agent generation = %#v, %v", generation, err)
	}
	workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), time.Minute)
	defer inspectCancel()
	handle, err := workspace.Inspect(inspectCtx, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[string][]byte{
		"output/result-a.txt": []byte("first immutable result"),
		"output/result-b.txt": []byte("second immutable result"),
	}
	for relativePath, content := range outputs {
		absolutePath := filepath.Join(handle.MergedPath, filepath.FromSlash(relativePath))
		if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolutePath, content, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := material.RegisterOutput(workspaceID, relativePath, content, domain.SensitivityInternal); err != nil {
			t.Fatal(err)
		}
	}
	preview, err := service.PreviewChangeSet(context.Background(), &worldv1.PreviewChangeSetRequest{LeaseId: view.Lease.ID})
	if err != nil || preview.WorkspaceRevision == 0 {
		t.Fatalf("preview = %#v, %v", preview, err)
	}
	declareMutation := fixture.wireMeta("export-declare")
	declareRequest := &worldv1.DeclareExportRequest{
		Mutation: declareMutation, LeaseId: view.Lease.ID,
		Paths: []*worldv1.ExportPath{
			{WorkspaceRelativePath: "output/result-a.txt", Role: "primary"},
			{WorkspaceRelativePath: "output/result-b.txt", Role: "secondary"},
		},
	}
	declared, err := service.DeclareExport(context.Background(), declareRequest)
	if err != nil {
		t.Fatal(err)
	}
	commitMutation := fixture.wireMeta("export-commit")
	commitRequest := &worldv1.CommitExportRequest{
		Mutation: commitMutation, ExportId: declared.ExportId, LeaseId: view.Lease.ID,
		ExpectedWorkspaceRevision: preview.WorkspaceRevision,
	}
	return exportCommitScenario{
		fixture: fixture, base: base, workspaceRoot: workspaceRoot, workspace: workspace,
		workspaceResolver: workspaceResolver, materialFaults: materialFaults, material: material,
		captureController: captureController, service: service, controller: controller, view: view,
		workspaceID: workspaceID, handle: handle, outputs: outputs, preview: preview,
		declareMutation: declareMutation, declareRequest: declareRequest, declared: declared,
		commitMutation: commitMutation, commitRequest: commitRequest,
	}
}

func (h exportCommitScenario) restart(t *testing.T) (*workspacedirectory.Driver, *Service, *Controller) {
	t.Helper()
	workspace, err := workspacedirectory.New(workspacedirectory.Config{Root: h.workspaceRoot, Now: h.fixture.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	service := h.fixture.service(Config{
		Agent: h.base.agent, Workspace: workspace, WorkspaceScope: h.workspaceResolver,
		Material: h.material, Captures: h.captureController,
	})
	controller, err := NewController(ControllerConfig{
		Core: h.fixture.core, Agent: h.base.agent, Workspace: workspace, Resolver: h.base.resolver, Capabilities: service,
	})
	if err != nil {
		t.Fatal(err)
	}
	return workspace, service, controller
}

func TestCommitExportReservesQuiescesSnapshotsAndResumesAfterPublicationFailure(t *testing.T) {
	h := newExportCommitScenario(t)
	fixture, materialFaults, service, view := h.fixture, h.materialFaults, h.service, h.view
	workspaceID, handle, outputs, preview := h.workspaceID, h.handle, h.outputs, h.preview
	declareRequest, declared, commitRequest := h.declareRequest, h.declared, h.commitRequest

	captureMutation := fixture.wireMeta("export-capture")
	captureRequest := &worldv1.StartCaptureRequest{
		Mutation: captureMutation, LeaseId: view.Lease.ID,
		CaptureSpec: &worldv1.CaptureSpec{Profile: "export-boundary", SignalFamilies: []string{"process"}, Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20},
	}
	activeCapture, err := service.StartCapture(context.Background(), captureRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CommitExport(context.Background(), commitRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit with active capture error = %v, want FailedPrecondition", err)
	}
	if _, err := service.StopCapture(context.Background(), &worldv1.StopCaptureRequest{
		Mutation: fixture.wireMeta("export-capture-stop"), LeaseId: view.Lease.ID,
		CaptureId: activeCapture.CaptureId, ExpectedRevision: activeCapture.Revision,
	}); err != nil {
		t.Fatal(err)
	}

	publicationFailure := errors.New("publication response lost after durable write")
	materialFaults.FailNext("material.capture_outputs.after", publicationFailure)
	if _, err := service.CommitExport(context.Background(), commitRequest); !errors.Is(err, publicationFailure) {
		t.Fatalf("first commit error = %v, want injected post-publication failure", err)
	}
	service.mu.RLock()
	committing := cloneExportRecord(service.exportState[declared.ExportId])
	service.mu.RUnlock()
	if committing.Export.State != exportStateCommitting || committing.Export.WorkspaceRevision != preview.WorkspaceRevision {
		t.Fatalf("durable in-flight export = %#v", committing.Export)
	}
	conflicting := proto.Clone(commitRequest).(*worldv1.CommitExportRequest)
	conflicting.Mutation = fixture.wireMeta("export-commit-conflict")
	if _, err := service.CommitExport(context.Background(), conflicting); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("concurrent commit key error = %v, want AlreadyExists", err)
	}

	restartedWorkspace, restarted, _ := h.restart(t)
	committed, err := restarted.CommitExport(context.Background(), commitRequest)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != exportStateCommitted || committed.WorkspaceRevision != preview.WorkspaceRevision || len(committed.Artifacts) != len(outputs)+1 || len(committed.OccurrenceRefs) != len(outputs)+1 {
		t.Fatalf("committed export = %#v", committed)
	}
	manifestArtifacts := 0
	for _, artifact := range committed.Artifacts {
		if artifact.Role == changeManifestRole {
			manifestArtifacts++
		}
	}
	if manifestArtifacts != 1 {
		t.Fatalf("change manifest artifacts = %d, want exactly one", manifestArtifacts)
	}
	if materialFaults.Hits("material.capture_outputs.before") != 2 || materialFaults.Hits("material.capture_outputs.after") != 1 {
		t.Fatalf("material publication hits before=%d after=%d", materialFaults.Hits("material.capture_outputs.before"), materialFaults.Hits("material.capture_outputs.after"))
	}
	exactReplay, err := restarted.CommitExport(context.Background(), commitRequest)
	if err != nil || exactReplay.Revision != committed.Revision {
		t.Fatalf("exact terminal replay = %#v, %v", exactReplay, err)
	}
	if materialFaults.Hits("material.capture_outputs.before") != 2 {
		t.Fatal("terminal replay repeated physical publication")
	}
	declaredReplay, err := restarted.DeclareExport(context.Background(), declareRequest)
	if err != nil || declaredReplay.State != exportStateCommitted {
		t.Fatalf("declaration replay after seal = %#v, %v", declaredReplay, err)
	}
	captureReplay, err := restarted.StartCapture(context.Background(), captureRequest)
	if err != nil || captureReplay.State != captureStateCompleted {
		t.Fatalf("capture start replay after seal = %#v, %v", captureReplay, err)
	}
	newKey := proto.Clone(commitRequest).(*worldv1.CommitExportRequest)
	newKey.Mutation = fixture.wireMeta("export-commit-after-terminal")
	if _, err := restarted.CommitExport(context.Background(), newKey); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("new terminal key error = %v, want AlreadyExists", err)
	}
	changed := proto.Clone(commitRequest).(*worldv1.CommitExportRequest)
	changed.ExpectedWorkspaceRevision++
	if _, err := restarted.CommitExport(context.Background(), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed terminal request error = %v, want AlreadyExists", err)
	}

	sealCtx, sealCancel := context.WithTimeout(context.Background(), time.Minute)
	defer sealCancel()
	sealed, err := restartedWorkspace.Seal(sealCtx, workspaceID, domain.Revision(preview.WorkspaceRevision))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handle.MergedPath, "output", "result-a.txt"), []byte("mutated merged workspace"), 0o600); err != nil {
		t.Fatal(err)
	}
	sealedBytes, err := os.ReadFile(filepath.Join(sealed.ImmutablePath, "output", "result-a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(sealedBytes) != string(outputs["output/result-a.txt"]) {
		t.Fatalf("immutable snapshot changed to %q", sealedBytes)
	}
	finalView, err := fixture.core.GetResearchSessionByLease(context.Background(), view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	finalGeneration, err := currentAgentGeneration(finalView.Agent)
	if err != nil || finalGeneration.State != domain.AgentGenerationSealed {
		t.Fatalf("agent generation after export = %#v, %v", finalGeneration, err)
	}
}

func TestLeaseTerminationCompletesDurablyReservedExportBeforeRelease(t *testing.T) {
	h := newExportCommitScenario(t)
	publicationFailure := errors.New("publication response lost after durable write")
	h.materialFaults.FailNext("material.capture_outputs.after", publicationFailure)
	if _, err := h.service.CommitExport(context.Background(), h.commitRequest); !errors.Is(err, publicationFailure) {
		t.Fatalf("first commit error = %v, want injected post-publication failure", err)
	}

	// Reconstruct orchestration state as a restarted daemon would. Public
	// authorization cannot replay a mutation once release establishes its gate,
	// so the trusted termination path must resume the exact durable reservation.
	_, restarted, controller := h.restart(t)
	release := application.ReleaseResearchSessionRequest{
		Meta: h.fixture.meta("release-with-committing-export"), LeaseID: h.view.Lease.ID,
		ExpectedRevision: h.view.Lease.Revision, Reason: "finish committed outputs before cleanup",
	}
	if _, err := controller.ReleaseResearchSession(context.Background(), release); err != nil {
		t.Fatalf("release did not finish the reserved export: %v", err)
	}
	restarted.mu.RLock()
	export := cloneExportRecord(restarted.exportState[h.declared.ExportId])
	restarted.mu.RUnlock()
	if export.Export == nil || export.Export.State != exportStateCommitted || len(export.Export.Artifacts) != len(h.outputs)+1 {
		t.Fatalf("export was not committed before release: %#v", export.Export)
	}
	assertTerminationState(t, h.fixture, h.view.Session.ID, domain.LeaseReleased, application.LeaseTerminationReleased)
	if h.materialFaults.Hits("material.capture_outputs.before") != 2 {
		t.Fatalf("publication attempts = %d, want original plus exact recovery", h.materialFaults.Hits("material.capture_outputs.before"))
	}
	if err := h.base.tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}
