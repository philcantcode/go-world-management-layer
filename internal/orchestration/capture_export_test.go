package orchestration

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkspaceContentSourceStagesOneImmutableFileIdentity(t *testing.T) {
	root := t.TempDir()
	original := []byte("sealed export bytes")
	if err := os.WriteFile(root+string(os.PathSeparator)+"result.bin", original, 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := newWorkspaceContentSource(root, "result.bin", int64(len(original)))
	if err != nil {
		t.Fatal(err)
	}
	if source.Size() != int64(len(original)) || source.Digest() != domain.NewDigest(original) {
		t.Fatalf("staged identity = (%s, %d), want (%s, %d)", source.Digest(), source.Size(), domain.NewDigest(original), len(original))
	}
	if err := os.WriteFile(root+string(os.PathSeparator)+"result.bin", []byte("mutated after seal!"), 0o600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		reader, err := source.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read staged content: read=%v close=%v", readErr, closeErr)
		}
		if string(got) != string(original) {
			t.Fatalf("attempt %d read %q, want immutable %q", attempt, got, original)
		}
	}
}

func TestAppendFullChangeManifestRetainsEveryFieldAndAvoidsSelectedPathCollision(t *testing.T) {
	before := domain.NewDigest([]byte("before"))
	after := domain.NewDigest([]byte("after"))
	entry, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
		Kind: domain.ChangeRenamed, Path: "new.txt", PreviousPath: "old.txt",
		BeforeDigest: before, AfterDigest: after, Metadata: map[string]string{"mode": "0600"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sealedAt := time.Date(2026, time.July, 27, 12, 30, 45, 123, time.UTC)
	changes, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, []domain.ChangeEntry{entry}, 7, sealedAt)
	if err != nil {
		t.Fatal(err)
	}
	existingPath := ".world/change-manifest.json"
	existing, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: existingPath, Roles: []string{"user-output"}})
	if err != nil {
		t.Fatal(err)
	}
	existingSource := testkit.NewMemoryContentSource([]byte("user-selected collision"))
	selections, content, err := appendFullChangeManifest(
		[]domain.ExportSelection{existing}, map[string]ports.ContentSource{existingPath: existingSource}, changes, 1<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	retainedSource := content[existingPath]
	if len(selections) != 2 || len(content) != 2 || retainedSource == nil || retainedSource.Digest() != existingSource.Digest() || retainedSource.Size() != existingSource.Size() {
		t.Fatalf("manifest append changed existing selection/content: selections=%d content=%d", len(selections), len(content))
	}
	manifestSpec := selections[1].Spec()
	if manifestSpec.RelativePath != ".world/change-manifest-2.json" || len(manifestSpec.Roles) != 1 || manifestSpec.Roles[0] != changeManifestRole {
		t.Fatalf("manifest selection = %#v", manifestSpec)
	}
	reader, err := content[manifestSpec.RelativePath].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifest, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read manifest: read=%v close=%v", readErr, closeErr)
	}
	expected := `{"schema_version":1,"scope":"agent_workspace","workspace_revision":7,"sealed_at":"2026-07-27T12:30:45.000000123Z","entries":[{"kind":"renamed","path":"new.txt","previous_path":"old.txt","before_digest":"` + before.String() + `","after_digest":"` + after.String() + `","metadata":{"mode":"0600"}}]}`
	if string(manifest) != expected {
		t.Fatalf("manifest = %s, want %s", manifest, expected)
	}
}

func TestStopCapturePersistsDomainCompletedStateAcrossReplay(t *testing.T) {
	fixture := newIntegrationFixture(t)
	fixture.readyAgent()
	workspaceID, _ := domain.NewWorkspaceID()
	agentID, _ := domain.NewAgentWorkspaceID()
	scope := WorkspaceScope{WorkspaceID: workspaceID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration, AgentState: domain.AgentGenerationReady}
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "artifact://capture/completed", Digest: domain.NewDigest([]byte("capture")), Size: 7,
		Role: "capture", Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := &captureControllerStub{artifacts: []domain.ArtifactReference{artifact}}
	config := Config{
		Workspace:      testkit.NewFakeWorkspaceDriver(fixture.clock, nil, nil),
		WorkspaceScope: fixedWorkspaceResolver{scope: scope}, Captures: controller,
	}
	service := fixture.service(config)
	started, err := service.StartCapture(context.Background(), &worldv1.StartCaptureRequest{
		Mutation: fixture.wireMeta("capture-start"), LeaseId: fixture.view.Lease.ID,
		CaptureSpec: &worldv1.CaptureSpec{
			Profile: "process-window", SignalFamilies: []string{"process"},
			Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.State != captureStateActive {
		t.Fatalf("started capture state = %q", started.State)
	}
	stopMutation := fixture.wireMeta("capture-stop")
	completed, err := service.StopCapture(context.Background(), &worldv1.StopCaptureRequest{
		Mutation: stopMutation, LeaseId: fixture.view.Lease.ID,
		CaptureId: started.CaptureId, ExpectedRevision: started.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != string(domain.CaptureCompleted) || len(completed.Artifacts) != 1 || completed.StoppedAt == nil {
		t.Fatalf("completed capture = %#v", completed)
	}

	restarted := fixture.service(config)
	replayed, err := restarted.StopCapture(context.Background(), &worldv1.StopCaptureRequest{
		Mutation: stopMutation, LeaseId: fixture.view.Lease.ID,
		CaptureId: started.CaptureId, ExpectedRevision: started.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != string(domain.CaptureCompleted) || replayed.Revision != completed.Revision || controller.stopCalls != 1 {
		t.Fatalf("capture replay = %#v; physical stops=%d", replayed, controller.stopCalls)
	}
	if _, err := restarted.StopCapture(context.Background(), &worldv1.StopCaptureRequest{
		Mutation: fixture.wireMeta("capture-stop-new-key"), LeaseId: fixture.view.Lease.ID,
		CaptureId: started.CaptureId, ExpectedRevision: 1,
	}); err == nil {
		t.Fatal("terminal capture accepted a different idempotency key")
	}
	changed := proto.Clone(stopMutation).(*worldv1.MutationMetadata)
	changed.IdempotencyKey = stopMutation.IdempotencyKey
	if _, err := restarted.StopCapture(context.Background(), &worldv1.StopCaptureRequest{
		Mutation: changed, LeaseId: fixture.view.Lease.ID,
		CaptureId: started.CaptureId, ExpectedRevision: started.Revision + 1,
	}); err == nil {
		t.Fatal("terminal capture accepted changed request arguments under the original key")
	}
}

func TestLeaseTerminationPersistsAmbiguousCaptureCompletedWhenPhysicalRecordIsAbsent(t *testing.T) {
	for _, state := range []string{captureStateStarting, captureStateFailed} {
		t.Run(state, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			fixture.readyAgent()
			workspaceID, _ := domain.NewWorkspaceID()
			agentID, _ := domain.NewAgentWorkspaceID()
			controller := &captureControllerStub{}
			service := fixture.service(Config{
				Workspace: testkit.NewFakeWorkspaceDriver(fixture.clock, nil, nil),
				WorkspaceScope: fixedWorkspaceResolver{scope: WorkspaceScope{
					WorkspaceID: workspaceID, AgentWorkspaceID: agentID,
					AgentGeneration: domain.InitialAgentGeneration, AgentState: domain.AgentGenerationReady,
				}},
				Captures: controller,
			})
			started, err := service.StartCapture(context.Background(), &worldv1.StartCaptureRequest{
				Mutation: fixture.wireMeta("capture-ambiguous-start-" + state), LeaseId: fixture.view.Lease.ID,
				CaptureSpec: &worldv1.CaptureSpec{
					Profile: "process-window", SignalFamilies: []string{"process"},
					Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20,
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			service.mu.Lock()
			record := service.captureState[started.CaptureId]
			record.Capture.State = state
			service.captureState[started.CaptureId] = record
			service.mu.Unlock()
			controller.stopError = domain.NewError(domain.CodeNotFound, "test.capture_stop", "capture_id", "physical capture is absent", nil)

			if err := service.stopLeaseCaptures(context.Background(), fixture.meta("capture-ambiguous-terminate-"+state), fixture.view.Lease.ID); err != nil {
				t.Fatalf("terminate %s capture: %v", state, err)
			}
			service.mu.RLock()
			completed := cloneCaptureRecord(service.captureState[started.CaptureId])
			service.mu.RUnlock()
			if completed.Capture.State != captureStateCompleted || completed.Capture.StoppedAt == nil {
				t.Fatalf("capture after termination = %#v, want durably completed", completed.Capture)
			}
			if controller.stopCalls != 1 {
				t.Fatalf("physical stop calls = %d, want 1", controller.stopCalls)
			}
		})
	}
}

type fixedWorkspaceResolver struct{ scope WorkspaceScope }

func (r fixedWorkspaceResolver) ResolveWorkspace(context.Context, string) (WorkspaceScope, error) {
	return r.scope, nil
}

type captureControllerStub struct {
	artifacts  []domain.ArtifactReference
	startCalls int
	stopCalls  int
	stopError  error
}

func (c *captureControllerStub) Start(context.Context, CapturePlan) error {
	c.startCalls++
	return nil
}
func (c *captureControllerStub) Stop(context.Context, CaptureStopPlan) ([]domain.ArtifactReference, error) {
	c.stopCalls++
	if c.stopError != nil {
		err := c.stopError
		c.stopError = nil
		return nil, err
	}
	return append([]domain.ArtifactReference(nil), c.artifacts...), nil
}

var _ WorkspaceResolver = fixedWorkspaceResolver{}
var _ CaptureController = (*captureControllerStub)(nil)
var _ ports.WorkspaceDriver = (*testkit.FakeWorkspaceDriver)(nil)
