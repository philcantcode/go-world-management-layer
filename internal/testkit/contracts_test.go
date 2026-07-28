package testkit

import (
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type contractFixture struct {
	clock        *Clock
	capabilities domain.CapabilityFingerprint
	leaseID      domain.LeaseID
	agentID      domain.AgentWorkspaceID
	workspaceID  domain.WorkspaceID
	targetID     domain.TargetID
	runID        domain.TargetRunID
	collectorID  domain.CollectorID
	manifest     domain.InputViewManifest
	workspace    domain.Workspace
	agentPlan    ports.AgentWorkspacePlan
	execPlan     ports.ExecPlan
	targetPlan   ports.TargetPlan
	runPlan      ports.TargetRunPlan
	requirement  ports.ObservationRequirement
	collector    ports.CollectorPlan
	occurrence   ports.ArtifactOccurrence
	content      []byte
}

func TestFakeAgentWorkspaceDriverContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeAgentWorkspaceDriver(fixture.capabilities, fixture.clock, nil, tracker)
	RunAgentWorkspaceDriverContract(t, AgentWorkspaceDriverContract{Driver: driver, Plan: fixture.agentPlan, Exec: fixture.execPlan, Tracker: tracker})
}

func TestFakeTargetDriverContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeTargetDriver(fixture.capabilities, fixture.clock, nil, tracker)
	RunTargetDriverContract(t, TargetDriverContract{Driver: driver, Create: fixture.targetPlan, Run: fixture.runPlan, Tracker: tracker})
}

func TestFakeTargetDriverFinalizesPreparedRunFailure(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeTargetDriver(fixture.capabilities, fixture.clock, nil, tracker)
	ctx, cancel := contractContext(t)
	defer cancel()
	created, err := driver.Create(ctx, fixture.targetPlan)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := driver.PrepareRun(ctx, fixture.runPlan)
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.StopRun(ctx, prepared.RunID, ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != ports.RunFailed || result.FailureKind != ports.TargetRunFailureNeverStarted ||
		result.RunID != prepared.RunID || len(result.Observations) == 0 ||
		result.TargetChanges.Scope() != domain.ChangeScopeTarget {
		t.Fatalf("StopRun(prepared) did not return an intrinsic failure receipt: %#v", result)
	}
	if replay, replayErr := driver.StopRun(ctx, prepared.RunID, ports.StopForce); replayErr != nil || replay.StoppedAt != result.StoppedAt {
		t.Fatalf("StopRun(prepared replay) = %#v, %v", replay, replayErr)
	}
	if err := driver.Destroy(ctx, ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestFakeObserverDriverContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeObserverDriver(fixture.capabilities, fixture.clock, nil, tracker)
	RunObserverDriverContract(t, ObserverDriverContract{Driver: driver, Requirement: fixture.requirement, Plan: fixture.collector, Tracker: tracker})
}

func TestFakeWorkspaceDriverContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeWorkspaceDriver(fixture.clock, nil, tracker)
	plan := ports.WorkspacePlan{
		IdempotencyKey: "workspace-prepare", Workspace: fixture.workspace, InputView: fixture.manifest,
		Content:      map[string]ports.ContentSource{"input/specimen.bin": NewMemoryContentSource(fixture.content)},
		Construction: domain.InputViewAllowCopy, UpperByteLimit: 1 << 20, UpperInodeLimit: 100,
	}
	RunWorkspaceDriverContract(t, WorkspaceDriverContract{Driver: driver, Plan: plan, Tracker: tracker})
}

func TestFakeInputCacheContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	cache := NewFakeInputCache(fixture.clock, nil, tracker)
	contentPlan := func() ports.CacheContentPlan {
		return ports.CacheContentPlan{
			SecurityScope: "contract", Occurrence: fixture.occurrence,
			Reader: newMemoryContentReader(fixture.content, fixture.occurrence.Digest, nil),
		}
	}
	build := ports.InputViewBuildPlan{SecurityScope: "contract", Manifest: fixture.manifest, Construction: domain.InputViewAllowCopy}
	pin := ports.CachePin{SecurityScope: "contract", InputViewID: fixture.manifest.ID(), Owner: fixture.leaseID.String()}
	RunInputCacheContract(t, InputCacheContract{Cache: cache, ContentPlan: contentPlan, BuildPlan: build, Pin: pin, Tracker: tracker})
}

func TestFakeMaterialAuthorityContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	authority := NewFakeMaterialAuthority(nil, tracker)
	occurrence, err := authority.RegisterContent(fixture.occurrence.Reference, fixture.content)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.RegisterOutput(fixture.workspaceID, "result/report.json", []byte(`{"ok":true}`), domain.SensitivityInternal); err != nil {
		t.Fatal(err)
	}
	selection, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: "result/report.json", Roles: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	input := ports.InputPlan{SecurityScope: "contract", Entries: []ports.InputEntryPlan{{Occurrence: occurrence, LogicalPath: "input/specimen.bin", Mode: 0o400}}}
	output := ports.OutputPlan{
		IdempotencyKey: "capture-output", LeaseID: fixture.leaseID, WorkspaceID: fixture.workspaceID,
		AgentWorkspaceID: fixture.agentID, AgentGeneration: domain.InitialAgentGeneration,
		Selections: []domain.ExportSelection{selection}, Content: map[string]ports.ContentSource{"result/report.json": NewMemoryContentSource([]byte(`{"ok":true}`))}, Provenance: map[string]string{"contract": "true"},
	}
	RunMaterialAuthorityContract(t, MaterialAuthorityContract{Authority: authority, Input: input, Occurrence: occurrence, Output: output})
	if err := tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func TestFakeResourceControllerContract(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	controller := NewFakeResourceController(fixture.capabilities, fixture.clock, nil, tracker)
	requests := admission.Resources{CPUMilli: 100, MemoryBytes: 1 << 20, PIDs: 4}
	limits := admission.Resources{CPUMilli: 200, MemoryBytes: 2 << 20, PIDs: 8}
	reserve := ports.ResourcePlan{IdempotencyKey: "resource-reserve", LeaseID: fixture.leaseID, OwnerKind: ports.ResourceLease, OwnerID: fixture.leaseID.String(), Requests: requests, Limits: limits}
	apply := reserve
	apply.IdempotencyKey = "resource-apply"
	RunResourceControllerContract(t, ResourceControllerContract{Controller: controller, Reserve: reserve, Apply: apply, Tracker: tracker})
}

func TestFakeDriversPreserveCommittedStateAcrossAfterFault(t *testing.T) {
	fixture := newContractFixture(t)
	faults := NewFaultInjector()
	faults.FailNext("target.create.after", nil)
	driver := NewFakeTargetDriver(fixture.capabilities, fixture.clock, faults, nil)
	ctx, cancel := contractContext(t)
	defer cancel()
	if _, err := driver.Create(ctx, fixture.targetPlan); err == nil {
		t.Fatal("Create() unexpectedly hid injected ambiguous-outcome fault")
	}
	result, err := driver.Create(ctx, fixture.targetPlan)
	if err != nil || result.Status.TargetID != fixture.targetID {
		t.Fatalf("Create(retry) = %#v, %v", result, err)
	}
}

func newContractFixture(t *testing.T) contractFixture {
	t.Helper()
	clock := NewClock(time.Unix(1_800_000_000, 0).UTC())
	ids := NewIDGenerator(clock)
	sessionID := mustID(t, ids.ResearchSessionID)
	leaseID := mustID(t, ids.LeaseID)
	agentID := mustID(t, ids.AgentWorkspaceID)
	workspaceID := mustID(t, ids.WorkspaceID)
	targetID := mustID(t, ids.TargetID)
	runID := mustID(t, ids.TargetRunID)
	collectorID := mustID(t, ids.CollectorID)
	execID := mustID(t, ids.ExecID)

	capability, err := domain.NewCapability(domain.CapabilitySupported, map[string]string{"driver": "fake"}, map[string]string{"source": "contract"})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := domain.NewCapabilityFingerprint(map[string]domain.Capability{"execution": capability}, map[string]string{"probe": "deterministic"})
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := domain.NewDigest([]byte("policy"))
	content := []byte("contract specimen")
	occurrence := ports.ArtifactOccurrence{Reference: "artifact://contract/specimen", Digest: domain.NewDigest(content), Size: int64(len(content))}
	entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
		LogicalPath: "input/specimen.bin", OccurrenceRef: occurrence.Reference,
		Digest: occurrence.Digest, Size: occurrence.Size, Mode: 0o400,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := domain.NewInputViewManifest([]domain.InputViewEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.InitialAgentGeneration, InputViewID: manifest.ID(), CreatedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	agentGeneration, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
		AgentWorkspaceID: agentID, Generation: domain.InitialAgentGeneration, WorkspaceID: workspaceID,
		InputViewID: manifest.ID(), PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilities.Digest(), CreatedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	agentPlan := ports.AgentWorkspacePlan{
		IdempotencyKey: "agent-provision", LeaseID: leaseID, Generation: agentGeneration, Workspace: workspace,
		ImageDigest: domain.NewDigest([]byte("agent-image")), PolicyDigest: policyDigest,
		CapabilityFingerprintDigest: capabilities.Digest(), Resources: admission.Resources{CPUMilli: 100},
	}
	exec, err := domain.NewExec(domain.ExecSpec{
		ID: execID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		Kind: domain.ExecTool, Executable: "/bin/true", WorkingDirectory: ".", CreatedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	execPlan := ports.ExecPlan{
		LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration, Exec: exec,
		Start: transport.ExecStart{
			ExecID: execID.String(), IdempotencyKey: "exec-start", Executable: "/bin/true",
			WorkingDirectory: ".",
			Deadline:         clock.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: time.Second,
		},
	}
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetLinuxContainer, domain.InitialTargetGeneration, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	targetGeneration, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: domain.InitialTargetGeneration,
		PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilities.Digest(), CreatedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	template := ports.TargetTemplate{
		Name: "contract-linux", Kind: domain.TargetLinuxContainer, Driver: "fake", Runtime: "fake",
		ImageDigest: domain.NewDigest([]byte("target-image")), IsolationProfile: "contract",
	}
	targetPlan := ports.TargetPlan{
		IdempotencyKey: "target-create", LeaseID: leaseID, Target: target, Generation: targetGeneration,
		Template: template, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilities.Digest(),
		Resources: admission.Resources{CPUMilli: 100},
	}
	material, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: occurrence.Reference, Digest: occurrence.Digest, Size: occurrence.Size,
		Role: "input-material", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		t.Fatal(err)
	}
	materialPlan := []ports.TargetMaterialPlan{{Artifact: material, LogicalPath: "specimen.bin", Mode: 0o444, Content: NewMemoryContentSource(content)}}
	materializationDigest, err := ports.TargetMaterializationDigest(materialPlan)
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.NewTargetRun(domain.TargetRunSpec{
		ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration,
		AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		MaterializationDigest: materializationDigest, CreatedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runPlan := ports.TargetRunPlan{
		IdempotencyKey: "target-run-prepare", Run: run, RequiredCoverage: []string{ports.TargetLifecycleSignal},
		Material: materialPlan, MaximumDuration: time.Minute,
	}
	requirement := ports.ObservationRequirement{
		SignalFamily: "process", Placement: domain.CollectorPlacementHost,
		MinimumLevel: domain.CoverageLevelComplete, Required: true,
	}
	collector := ports.CollectorPlan{
		IdempotencyKey: "collector-start", CollectorID: collectorID,
		ResearchSessionID: sessionID, LeaseID: leaseID,
		AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		TargetID: targetID, TargetGeneration: domain.InitialTargetGeneration, TargetRunID: runID,
		Attachment:  ports.ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: "contract-runtime"},
		Requirement: requirement, Adapter: "fake", Version: "1", ConfigurationDigest: domain.NewDigest([]byte("collector-config")),
		Resources: admission.Resources{CaptureBytes: 1024}, MaximumBytes: 1024, StartedAt: clock.Now(),
	}
	return contractFixture{
		clock: clock, capabilities: capabilities, leaseID: leaseID, agentID: agentID, workspaceID: workspaceID,
		targetID: targetID, runID: runID, collectorID: collectorID, manifest: manifest, workspace: workspace,
		agentPlan: agentPlan, execPlan: execPlan, targetPlan: targetPlan, runPlan: runPlan,
		requirement: requirement, collector: collector, occurrence: occurrence, content: content,
	}
}

func mustID[T any](t *testing.T, generate func() (T, error)) T {
	t.Helper()
	value, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
