package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestLedgerCaptureFiltersBoundsPersistsAndReplays(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_770_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	leaseID, _ := ids.LeaseID()
	otherLeaseID, _ := ids.LeaseID()
	workspaceID, _ := ids.WorkspaceID()
	agentID, _ := ids.AgentWorkspaceID()
	captureID, _ := ids.CaptureID()
	otherCaptureID, _ := ids.CaptureID()

	observationLedger, _, err := ledger.Open(ledger.Options{Directory: t.TempDir(), SubscriberBuffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observationLedger.Close() })
	appendCaptureRecord(t, observationLedger, leaseID.String(), "process", clock.Now().Add(-time.Second), "before")

	authority := newRecordingCaptureAuthority()
	root := t.TempDir()
	controller, err := NewLedgerCaptureController(LedgerCaptureConfig{
		Root: root, Ledger: observationLedger, Material: authority, MaxBytes: 64 << 10, MaxRecords: 2, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := CapturePlan{
		IdempotencyKey: "capture-start", CaptureID: captureID.String(), LeaseID: leaseID.String(),
		Workspace: WorkspaceScope{WorkspaceID: workspaceID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration},
		Spec:      &worldv1.CaptureSpec{Profile: "process-window", SignalFamilies: []string{"resource", "process"}, Duration: durationpb.New(time.Minute), ByteLimit: 64 << 10},
		StartedAt: clock.Now(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := controller.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	appendCaptureRecord(t, observationLedger, leaseID.String(), "process", clock.Now().Add(time.Second), "kept-1")
	appendCaptureRecord(t, observationLedger, leaseID.String(), "network", clock.Now().Add(2*time.Second), "wrong-family")
	appendCaptureRecord(t, observationLedger, otherLeaseID.String(), "process", clock.Now().Add(3*time.Second), "wrong-lease")
	appendCaptureRecord(t, observationLedger, leaseID.String(), "process", clock.Now().Add(4*time.Second), "kept-2")
	appendCaptureRecord(t, observationLedger, leaseID.String(), "process", clock.Now().Add(5*time.Second), "truncated")
	appendCaptureRecord(t, observationLedger, leaseID.String(), "process", clock.Now().Add(2*time.Minute), "after-duration")

	// A repeated start stays bound to the original cursor even after the ledger advances.
	if err := controller.Start(ctx, plan); err != nil {
		t.Fatalf("idempotent Start() error = %v", err)
	}
	conflict := plan
	conflict.CaptureID = otherCaptureID.String()
	if err := controller.Start(ctx, conflict); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("reused start idempotency error = %v", err)
	}

	clock.Advance(10 * time.Second)
	artifacts, err := controller.Stop(ctx, CaptureStopPlan{IdempotencyKey: "capture-stop", CaptureID: captureID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 || artifacts[0].Spec().Role != "observation-capture" {
		t.Fatalf("artifacts = %#v", artifacts)
	}
	content, calls := authority.snapshot()
	if calls != 1 {
		t.Fatalf("capture publication calls = %d, want 1", calls)
	}
	var document captureDocument
	if err := json.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	if !document.Truncated || len(document.Records) != 2 || string(document.Records[0].Payload) != "kept-1" || string(document.Records[1].Payload) != "kept-2" {
		t.Fatalf("captured document = %#v", document)
	}
	if len(document.SignalFamilies) != 2 || document.SignalFamilies[0] != "process" || document.SignalFamilies[1] != "resource" {
		t.Fatalf("capture document signal families = %#v", document.SignalFamilies)
	}
	for _, record := range document.Records {
		if record.Identity.LeaseID != leaseID.String() || record.SignalFamily != "process" {
			t.Fatalf("out-of-scope record captured: %#v", record)
		}
	}

	restarted, err := NewLedgerCaptureController(LedgerCaptureConfig{
		Root: root, Ledger: observationLedger, Material: authority, MaxBytes: 64 << 10, MaxRecords: 2, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Stop(ctx, CaptureStopPlan{IdempotencyKey: "capture-stop", CaptureID: captureID.String()})
	if err != nil || len(replayed) != 1 || replayed[0].Spec().Digest != artifacts[0].Spec().Digest {
		t.Fatalf("replayed Stop() = %#v, %v", replayed, err)
	}
	if _, calls = authority.snapshot(); calls != 1 {
		t.Fatalf("restart replay republished output; calls = %d", calls)
	}
}

func TestLedgerCaptureRejectsMalformedProtobufSpec(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_770_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	leaseID, _ := ids.LeaseID()
	workspaceID, _ := ids.WorkspaceID()
	agentID, _ := ids.AgentWorkspaceID()
	captureID, _ := ids.CaptureID()
	observationLedger, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observationLedger.Close()
	controller, err := NewLedgerCaptureController(LedgerCaptureConfig{Root: t.TempDir(), Ledger: observationLedger, Material: newRecordingCaptureAuthority(), MaxBytes: 4096, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	base := CapturePlan{
		IdempotencyKey: "start", CaptureID: captureID.String(), LeaseID: leaseID.String(),
		Workspace: WorkspaceScope{WorkspaceID: workspaceID, AgentWorkspaceID: agentID, AgentGeneration: 1},
		StartedAt: clock.Now(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for name, spec := range map[string]*worldv1.CaptureSpec{
		"nil":                nil,
		"malformed duration": {Profile: "p", SignalFamilies: []string{"process"}, Duration: &durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000}, ByteLimit: 1024},
		"duplicate families": {Profile: "p", SignalFamilies: []string{"process", "process"}, Duration: durationpb.New(time.Second), ByteLimit: 1024},
	} {
		t.Run(name, func(t *testing.T) {
			plan := base
			plan.Spec = spec
			if err := controller.Start(ctx, plan); err == nil {
				t.Fatal("malformed specification was accepted")
			}
		})
	}
}

func appendCaptureRecord(t *testing.T, observationLedger *ledger.Ledger, leaseID, family string, observed time.Time, payload string) {
	t.Helper()
	if _, err := observationLedger.Append(context.Background(), ledger.Record{
		Kind: ledger.RecordObservation, Identity: ledger.Identity{LeaseID: leaseID}, SignalFamily: family,
		Source: "capture-test", SourceInstance: "one", ObservedWallUnixNano: observed.UnixNano(), Origin: ledger.OriginSystem,
		Payload: []byte(payload),
	}); err != nil {
		t.Fatal(err)
	}
}

type recordingCaptureAuthority struct {
	*testkit.FakeMaterialAuthority
	mu      sync.Mutex
	content []byte
	calls   int
}

func newRecordingCaptureAuthority() *recordingCaptureAuthority {
	return &recordingCaptureAuthority{FakeMaterialAuthority: testkit.NewFakeMaterialAuthority(nil, nil)}
}

func (a *recordingCaptureAuthority) CaptureOutputs(ctx context.Context, plan ports.OutputPlan) ([]domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "test.capture_outputs"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if len(plan.Selections) != 1 {
		return nil, fmt.Errorf("expected one capture selection")
	}
	path := plan.Selections[0].Spec().RelativePath
	source := plan.Content[path]
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read capture output: %v / %v", readErr, closeErr)
	}
	if int64(len(content)) != source.Size() || domain.NewDigest(content) != source.Digest() {
		return nil, fmt.Errorf("capture source identity mismatch")
	}
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "memory://" + path, Digest: source.Digest(), Size: source.Size(), Role: "observation-capture", Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.calls++
	a.content = append([]byte(nil), content...)
	a.mu.Unlock()
	return []domain.ArtifactReference{artifact}, nil
}

func (a *recordingCaptureAuthority) snapshot() ([]byte, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]byte(nil), a.content...), a.calls
}

var _ ports.MaterialAuthority = (*recordingCaptureAuthority)(nil)
