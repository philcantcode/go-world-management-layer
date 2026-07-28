package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const integrationOwner = "integration-owner"

type integrationFixture struct {
	t          *testing.T
	clock      *testkit.Clock
	ids        *domain.IDGenerator
	core       *application.Core
	store      *store.Store
	ledger     *ledger.Ledger
	ledgerPath string
	stateRoot  string
	view       application.ResearchSessionView
	sequence   int
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	root := t.TempDir()
	clock := testkit.NewClock(time.Unix(1_750_000_000, 0).UTC())
	ids := testkit.NewIDGenerator(clock)
	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(root, "control.db"), Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	core, err := application.NewCore(context.Background(), application.CoreOptions{Store: controlStore, IDs: ids, Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	ledgerPath := filepath.Join(root, "ledger")
	observationLedger, _, err := ledger.Open(ledger.Options{Directory: ledgerPath, SubscriberBuffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &integrationFixture{t: t, clock: clock, ids: ids, core: core, store: controlStore, ledger: observationLedger, ledgerPath: ledgerPath, stateRoot: filepath.Join(root, "orchestration")}
	fixture.view = fixture.acquire()
	t.Cleanup(func() {
		_ = observationLedger.Close()
		_ = controlStore.Close()
	})
	return fixture
}

func (f *integrationFixture) meta(prefix string) application.MutationMeta {
	f.t.Helper()
	f.sequence++
	correlation, err := f.ids.CorrelationID()
	if err != nil {
		f.t.Fatal(err)
	}
	return application.MutationMeta{IdempotencyKey: fmt.Sprintf("%s-%d", prefix, f.sequence), CorrelationID: correlation.String(), AuthorizedPolicyReference: f.viewPolicy(), Deadline: time.Now().Add(time.Minute)}
}

func (f *integrationFixture) wireMeta(prefix string) *worldv1.MutationMetadata {
	meta := f.meta(prefix)
	return &worldv1.MutationMetadata{IdempotencyKey: meta.IdempotencyKey, CorrelationId: meta.CorrelationID, AuthorizedPolicyReference: meta.AuthorizedPolicyReference, Deadline: protobufTimestamp(meta.Deadline)}
}

func (f *integrationFixture) viewPolicy() string {
	if f.view.Session.PolicyDigest != "" {
		return f.view.Session.PolicyDigest
	}
	return domain.NewDigest([]byte("policy")).String()
}

func (f *integrationFixture) acquire() application.ResearchSessionView {
	request := application.AcquireRequest{
		Meta:         application.MutationMeta{IdempotencyKey: "acquire", CorrelationID: mustCorrelation(f.t, f.ids), AuthorizedPolicyReference: domain.NewDigest([]byte("policy")).String(), Deadline: time.Now().Add(time.Minute)},
		OwnerSubject: integrationOwner, InputViewID: domain.NewInputViewID([]byte("input")).String(),
		PolicyDigest: domain.NewDigest([]byte("policy")).String(), CapabilityDigest: domain.NewDigest([]byte("capability")).String(), TTL: time.Hour,
	}
	view, err := f.core.AcquireResearchSession(context.Background(), request)
	if err != nil {
		f.t.Fatal(err)
	}
	return view
}

func (f *integrationFixture) service(config Config) *Service {
	f.t.Helper()
	config = f.serviceConfig(config)
	service, err := New(config)
	if err != nil {
		f.t.Fatal(err)
	}
	return service
}

func (f *integrationFixture) serviceConfig(config Config) Config {
	config.Core, config.Ledger, config.IDs, config.Clock, config.StateRoot = f.core, f.ledger, f.ids, f.clock.Now, f.stateRoot
	config.Subject = func(context.Context) (string, bool) { return integrationOwner, true }
	if config.PolicyAdmission == nil {
		config.PolicyAdmission = allowLeaseOperationPolicyAdmission{}
	}
	return config
}

type allowLeaseOperationPolicyAdmission struct{}

func (allowLeaseOperationPolicyAdmission) AdmitCapture(context.Context, string, string, policyauthority.CaptureAdmission) error {
	return nil
}

func (allowLeaseOperationPolicyAdmission) AdmitExport(context.Context, string, string, policyauthority.ExportAdmission) error {
	return nil
}

func (f *integrationFixture) readyTargetAndRun() (application.TargetRecord, application.TargetRunRecord) {
	f.readyAgent()
	target, err := f.core.CreateTarget(context.Background(), application.CreateTargetRequest{Meta: f.meta("target"), LeaseID: f.view.Lease.ID, Template: "linux-visible", Kind: domain.TargetLinuxContainer, PolicyDigest: f.view.Session.PolicyDigest, CapabilityDigest: f.view.Session.CapabilityDigest})
	if err != nil {
		f.t.Fatal(err)
	}
	for _, state := range []domain.TargetGenerationState{domain.TargetGenerationInstrumenting, domain.TargetGenerationReady} {
		generation, _ := targetGeneration(target)
		target, err = f.core.TransitionTargetGeneration(context.Background(), application.TransitionTargetGenerationRequest{Meta: f.meta("target-state"), TargetID: target.ID, Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			f.t.Fatal(err)
		}
	}
	_, materializationDigest := targetMaterial(f.t)
	run, err := f.core.StartTargetRun(context.Background(), application.StartTargetRunRequest{Meta: f.meta("run"), TargetID: target.ID, MaterializationDigest: materializationDigest.String()})
	if err != nil {
		f.t.Fatal(err)
	}
	for _, state := range []domain.TargetRunState{domain.TargetRunPreparing, domain.TargetRunObserving, domain.TargetRunRunning} {
		run, err = f.core.TransitionTargetRun(context.Background(), application.TransitionTargetRunRequest{Meta: f.meta("run-state"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision, State: state})
		if err != nil {
			f.t.Fatal(err)
		}
	}
	target, err = f.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	return target, run
}

func (f *integrationFixture) readyAgent() {
	agent := f.view.Agent
	for _, state := range []domain.AgentGenerationState{domain.AgentGenerationBooting, domain.AgentGenerationReady} {
		generation, generationErr := currentAgentGeneration(agent)
		if generationErr != nil {
			f.t.Fatal(generationErr)
		}
		agent, generationErr = f.core.TransitionAgentGeneration(context.Background(), application.TransitionAgentRequest{
			Meta: f.meta("agent-state"), AgentWorkspaceID: agent.ID, Generation: agent.CurrentGeneration,
			ExpectedRevision: generation.Revision, State: state,
		})
		if generationErr != nil {
			f.t.Fatal(generationErr)
		}
	}
	f.view.Agent = agent
}

func TestLedgerSnapshotAndResumableSubscription(t *testing.T) {
	fixture := newIntegrationFixture(t)
	subjectID, err := fixture.ids.SubjectID()
	if err != nil {
		t.Fatal(err)
	}
	topology, _ := jsonMarshal(&worldv1.Subject{SubjectId: subjectID.String(), Kind: "lease", Labels: map[string]string{"name": "test"}})
	metric, _ := jsonMarshal(&worldv1.MetricSample{SubjectId: subjectID.String(), SignalFamily: "cpu", Name: "usage", State: "present", Value: floatPointer(0), CollectedAt: protobufTimestamp(fixture.clock.Now())})
	identity := ledger.Identity{ResearchSessionID: fixture.view.Session.ID, LeaseID: fixture.view.Lease.ID}
	if _, err := fixture.ledger.Append(context.Background(), ledger.Record{Kind: ledger.RecordTopology, Identity: identity, SignalFamily: "topology", SubjectID: subjectID.String(), Source: "test", SourceInstance: "one", ObservedWallUnixNano: fixture.clock.Now().UnixNano(), Origin: ledger.OriginSystem, Payload: topology}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.ledger.Append(context.Background(), ledger.Record{Kind: ledger.RecordMetric, Identity: identity, SignalFamily: "cpu", SubjectID: subjectID.String(), Source: "test", SourceInstance: "one", ObservedWallUnixNano: fixture.clock.Now().UnixNano(), Origin: ledger.OriginSystem, Payload: metric}); err != nil {
		t.Fatal(err)
	}
	service := fixture.service(Config{})
	snapshot, err := service.GetLiveSnapshot(context.Background(), &worldv1.GetLiveSnapshotRequest{Filter: &worldv1.ObservationFilter{LeaseId: fixture.view.Lease.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Cursor != 2 || len(snapshot.Subjects) != 1 || len(snapshot.Metrics) != 1 || snapshot.Metrics[0].Value == nil || *snapshot.Metrics[0].Value != 0 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	stop := errors.New("received durable record")
	stream := &recordingObservationStream{ctx: context.Background(), stop: stop}
	err = service.SubscribeObservations(&worldv1.SubscribeObservationsRequest{Filter: &worldv1.ObservationFilter{LeaseId: fixture.view.Lease.ID}, AfterCursor: 1}, stream)
	if !errors.Is(err, stop) || len(stream.records) != 1 || stream.records[0].Cursor != 2 {
		t.Fatalf("resumable subscription = records %#v, err %v", stream.records, err)
	}
}

func TestStopRunFinalizesDriverEvidenceAndSurvivesRestart(t *testing.T) {
	fixture := newIntegrationFixture(t)
	target, run := fixture.readyTargetAndRun()
	fakeTarget := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, nil)
	plan, prepared := preparePhysicalTarget(t, fixture, fakeTarget, target, run)
	observers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: fixture.ledger, IDs: fixture.ids, Clock: fixture.clock.Now,
		StateRoot: filepath.Join(t.TempDir(), "run-observers"),
	})
	if err != nil {
		t.Fatal(err)
	}
	observerStart, err := bindRunObserverStart(plan, prepared, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := observers.Start(context.Background(), observerStart); err != nil {
		t.Fatal(err)
	}
	finalizer, err := observationbundle.New(filepath.Join(t.TempDir(), "sealed"))
	if err != nil {
		t.Fatal(err)
	}
	authority := testkit.NewFakeMaterialAuthority(nil, nil)
	finalization, err := application.NewRunFinalizationService(fixture.core, finalizer, authority)
	if err != nil {
		t.Fatal(err)
	}
	service := fixture.service(Config{
		Finalization: finalization, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: fakeTarget},
		Observers: observers,
	})
	request := &worldv1.StopTargetRunRequest{Mutation: fixture.wireMeta("stop"), TargetId: target.ID, TargetRunId: run.ID, ExpectedRevision: run.Revision, Reason: "integration complete"}
	bundle, err := service.StopTargetRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.State != string(domain.ObservationBundleSealed) || bundle.TargetRunId != run.ID || len(bundle.NormalizedEvents) == 0 || len(bundle.Coverage) == 0 {
		t.Fatalf("unexpected finalized bundle: %#v", bundle)
	}
	updatedTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	updatedRun, err := targetRun(updatedTarget, run.ID)
	if err != nil || updatedRun.State != domain.TargetRunCompleted || updatedRun.BundleID != bundle.BundleId || updatedRun.BundleArtifact == "" {
		t.Fatalf("run was not authoritatively finalized: %#v, %v", updatedRun, err)
	}
	replayedBundle, err := service.StopTargetRun(context.Background(), request)
	if err != nil || replayedBundle.BundleId != bundle.BundleId {
		t.Fatalf("exact stop replay = %#v, %v", replayedBundle, err)
	}
	newKey := proto.Clone(request).(*worldv1.StopTargetRunRequest)
	newKey.Mutation = fixture.wireMeta("stop-new-key")
	if _, err := service.StopTargetRun(context.Background(), newKey); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("terminal stop with new key error = %v, want AlreadyExists", err)
	}
	changed := proto.Clone(request).(*worldv1.StopTargetRunRequest)
	changed.Reason = "changed reason"
	if _, err := service.StopTargetRun(context.Background(), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("terminal stop with changed request error = %v, want AlreadyExists", err)
	}
	if err := fixture.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := ledger.Open(ledger.Options{Directory: fixture.ledgerPath})
	if err != nil {
		t.Fatal(err)
	}
	fixture.ledger = reopened
	restarted := fixture.service(Config{})
	loaded, err := restarted.GetObservationBundle(context.Background(), &worldv1.GetObservationBundleRequest{TargetRunId: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.BundleId != bundle.BundleId || loaded.TargetId != target.ID {
		t.Fatalf("bundle lookup did not survive restart: %#v", loaded)
	}

	bundlePath := filepath.Join(fixture.stateRoot, "bundles", run.ID+".json")
	original, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	publicationPath := filepath.Join(fixture.stateRoot, bundlePublicationDirectory, run.ID+".json")
	originalPublication, err := os.ReadFile(publicationPath)
	if err != nil {
		t.Fatal(err)
	}
	var tamperedPublication stagedBundlePublication
	if err := json.Unmarshal(originalPublication, &tamperedPublication); err != nil {
		t.Fatal(err)
	}
	if tamperedPublication.Bundle == nil || tamperedPublication.Bundle.Summary == nil {
		t.Fatal("staged publication has no summary to tamper")
	}
	tamperedPublication.Bundle.Summary.Text += " (canonical stage tamper)"
	modifiedPublication, err := json.Marshal(tamperedPublication)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publicationPath, modifiedPublication, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(fixture.serviceConfig(Config{})); err == nil {
		t.Fatal("restart accepted canonical staged-bundle bytes that differ from the hash-chained wire digest")
	}
	if err := os.WriteFile(publicationPath, originalPublication, 0o600); err != nil {
		t.Fatal(err)
	}
	var tampered worldv1.ObservationBundle
	if err := json.Unmarshal(original, &tampered); err != nil {
		t.Fatal(err)
	}
	if tampered.Summary == nil {
		t.Fatal("finalized bundle has no summary to tamper")
	}
	tampered.Summary.Text += " (tampered)"
	modified, err := json.Marshal(&tampered)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bundlePath, modified, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(fixture.serviceConfig(Config{})); err == nil {
		t.Fatal("restart accepted a valid-JSON bundle whose wire digest was tampered")
	}
	if err := os.WriteFile(bundlePath, append(append([]byte(nil), original...), []byte("{}")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(fixture.serviceConfig(Config{})); err == nil {
		t.Fatal("restart accepted trailing JSON in a persisted bundle")
	}
	if err := os.WriteFile(bundlePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(fixture.serviceConfig(Config{MaxTransferBytes: int64(len(original) - 1)})); err == nil {
		t.Fatal("restart accepted a persisted bundle outside the configured wire bound")
	}
}

func preparePhysicalTarget(t *testing.T, fixture *integrationFixture, driver *testkit.FakeTargetDriver, target application.TargetRecord, run application.TargetRunRecord) (ports.TargetRunPlan, ports.PreparedTargetRun) {
	t.Helper()
	targetID, _ := domain.ParseTargetID(target.ID)
	sessionID, _ := domain.ParseResearchSessionID(target.SessionID)
	policy, _ := domain.ParseDigest(fixture.view.Session.PolicyDigest)
	capability, _ := domain.ParseDigest(fixture.view.Session.CapabilityDigest)
	targetModel, err := domain.NewTarget(targetID, sessionID, target.Kind, domain.TargetGeneration(target.CurrentGeneration), target.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	generationModel, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{TargetID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration), PolicyDigest: policy, CapabilityFingerprintDigest: capability, CreatedAt: target.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	image := domain.NewDigest([]byte("target-image"))
	leaseID, err := domain.ParseLeaseID(target.LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = driver.Create(ctx, ports.TargetPlan{IdempotencyKey: "physical-target", LeaseID: leaseID, Target: targetModel, Generation: generationModel, Template: ports.TargetTemplate{Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "fake", Runtime: "fake", ImageDigest: image, IsolationProfile: "test"}, PolicyDigest: policy, CapabilityFingerprintDigest: capability, Resources: admission.Resources{}})
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := domain.ParseTargetRunID(run.ID)
	material, materialDigest := targetMaterial(t)
	if materialDigest.String() != run.MaterializationDigest {
		t.Fatalf("target material digest = %s, run records %s", materialDigest, run.MaterializationDigest)
	}
	agentID, err := domain.ParseAgentWorkspaceID(run.AgentWorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	runModel, err := domain.NewTargetRun(domain.TargetRunSpec{ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(run.Generation), AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(run.AgentGeneration), MaterializationDigest: materialDigest, CreatedAt: run.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	plan := ports.TargetRunPlan{IdempotencyKey: "physical-run", Run: runModel, RequiredCoverage: []string{ports.TargetLifecycleSignal}, Material: material, MaximumDuration: time.Minute}
	prepared, err := driver.PrepareRun(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	return plan, prepared
}

func targetMaterial(t *testing.T) ([]ports.TargetMaterialPlan, domain.Digest) {
	t.Helper()
	content := []byte("specimen")
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "memory://material/specimen", Digest: domain.NewDigest(content), Size: int64(len(content)),
		Role: "specimen", Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	material := []ports.TargetMaterialPlan{{
		Artifact: artifact, LogicalPath: "specimen.bin", Mode: 0o444,
		Content: testkit.NewMemoryContentSource(content),
	}}
	digest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	return material, digest
}

type recordingObservationStream struct {
	grpc.ServerStream
	ctx     context.Context
	stop    error
	records []*worldv1.ObservationRecord
}

func (s *recordingObservationStream) Context() context.Context { return s.ctx }
func (s *recordingObservationStream) Send(record *worldv1.ObservationRecord) error {
	s.records = append(s.records, record)
	return s.stop
}

func mustCorrelation(t *testing.T, ids *domain.IDGenerator) string {
	t.Helper()
	id, err := ids.CorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func floatPointer(value float64) *float64 { return &value }

func jsonMarshal(value proto.Message) ([]byte, error) {
	return protojson.Marshal(value)
}
