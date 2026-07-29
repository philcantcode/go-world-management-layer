package domain

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

type fixtureIDs struct {
	session        ResearchSessionID
	lease          LeaseID
	agentWorkspace AgentWorkspaceID
	exec           ExecID
	target         TargetID
	run            TargetRunID
	operation      TargetOperationID
	workspace      WorkspaceID
	incident       IncidentID
	capture        CaptureID
	bundle         ObservationBundleID
	export         ExportID
	event          EventID
	collector      CollectorID
	subject        SubjectID
}

func newFixtureIDs(t *testing.T) fixtureIDs {
	t.Helper()
	seed := make([]byte, 4096)
	for i := range seed {
		seed[i] = byte(i*37 + 11)
	}
	g, err := NewIDGenerator(func() time.Time { return time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC) }, bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	var ids fixtureIDs
	if ids.session, err = g.ResearchSessionID(); err != nil {
		t.Fatal(err)
	}
	if ids.lease, err = g.LeaseID(); err != nil {
		t.Fatal(err)
	}
	if ids.agentWorkspace, err = g.AgentWorkspaceID(); err != nil {
		t.Fatal(err)
	}
	if ids.exec, err = g.ExecID(); err != nil {
		t.Fatal(err)
	}
	if ids.target, err = g.TargetID(); err != nil {
		t.Fatal(err)
	}
	if ids.run, err = g.TargetRunID(); err != nil {
		t.Fatal(err)
	}
	if ids.operation, err = g.TargetOperationID(); err != nil {
		t.Fatal(err)
	}
	if ids.workspace, err = g.WorkspaceID(); err != nil {
		t.Fatal(err)
	}
	if ids.incident, err = g.IncidentID(); err != nil {
		t.Fatal(err)
	}
	if ids.capture, err = g.CaptureID(); err != nil {
		t.Fatal(err)
	}
	if ids.bundle, err = g.ObservationBundleID(); err != nil {
		t.Fatal(err)
	}
	if ids.export, err = g.ExportID(); err != nil {
		t.Fatal(err)
	}
	if ids.event, err = g.EventID(); err != nil {
		t.Fatal(err)
	}
	if ids.collector, err = g.CollectorID(); err != nil {
		t.Fatal(err)
	}
	if ids.subject, err = g.SubjectID(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestIndependentGenerationInvariants(t *testing.T) {
	ids := newFixtureIDs(t)
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	agent, err := NewAgentWorkspace(ids.agentWorkspace, ids.session, InitialAgentGeneration, now)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewTarget(ids.target, ids.session, TargetLinuxContainer, InitialTargetGeneration, now)
	if err != nil {
		t.Fatal(err)
	}
	run, err := NewTargetRun(TargetRunSpec{ID: ids.run, LeaseID: ids.lease, TargetID: ids.target, TargetGeneration: target.CurrentGeneration(), AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: agent.CurrentGeneration(), MaterializationDigest: NewDigest([]byte("material")), CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}

	resetAt := now.Add(time.Minute)
	target, err = target.AdvanceGeneration(target.Revision(), 2, resetAt)
	if err != nil {
		t.Fatal(err)
	}
	if target.CurrentGeneration() != 2 {
		t.Fatal("target generation did not advance")
	}
	if agent.CurrentGeneration() != 1 {
		t.Fatal("target reset changed healthy agent generation")
	}
	if run.Spec().TargetGeneration != 1 || run.Spec().AgentGeneration != 1 {
		t.Fatal("target reset rewrote run history")
	}

	agent, err = agent.AdvanceGeneration(agent.Revision(), 2, resetAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if agent.CurrentGeneration() != 2 {
		t.Fatal("agent generation did not advance")
	}
	if target.CurrentGeneration() != 2 {
		t.Fatal("agent recovery changed target generation")
	}
	if _, err := target.AdvanceGeneration(target.Revision(), 4, resetAt.Add(2*time.Minute)); !IsCode(err, CodeConflict) {
		t.Fatalf("skipped target generation: %v", err)
	}
	if _, err := agent.AdvanceGeneration(agent.Revision(), 2, resetAt.Add(2*time.Minute)); !IsCode(err, CodeConflict) {
		t.Fatalf("reused agent generation: %v", err)
	}

	policyDigest, capabilityDigest := NewDigest([]byte("policy")), NewDigest([]byte("capabilities"))
	if _, err := NewTargetGeneration(TargetGenerationSpec{TargetID: ids.target, Generation: 2, PreviousGeneration: 1, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now}); err != nil {
		t.Fatalf("clean reset generation rejected: %v", err)
	}
	if _, err := NewAgentWorkspaceGeneration(AgentWorkspaceGenerationSpec{AgentWorkspaceID: ids.agentWorkspace, Generation: 2, WorkspaceID: ids.workspace, InputViewID: NewInputViewID([]byte("view")), PreviousGeneration: 1, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now}); err != nil {
		t.Fatalf("non-incident agent generation rejected: %v", err)
	}
	if _, err := NewTargetGeneration(TargetGenerationSpec{TargetID: ids.target, Generation: 2, PreviousGeneration: 1, RecoveryIncidentID: ids.incident, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now}); err != nil {
		t.Fatalf("valid target recovery: %v", err)
	}
}

func TestLifecycleModelsValidateRevisionsAndCopyInputs(t *testing.T) {
	ids := newFixtureIDs(t)
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	session, err := NewResearchSession(ids.session, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Transition(ResearchSessionAdmitted, 2, now.Add(time.Second)); !IsCode(err, CodeStaleRevision) {
		t.Fatalf("stale session transition: %v", err)
	}
	session, err = session.Transition(ResearchSessionAdmitted, session.Revision(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if session.Revision() != 2 {
		t.Fatal("transition did not advance revision")
	}

	viewID := NewInputViewID([]byte("manifest"))
	policyDigest := NewDigest([]byte("policy"))
	capabilityDigest := NewDigest([]byte("capability"))
	lease, err := NewLease(LeaseSpec{ID: ids.lease, ResearchSessionID: ids.session, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, InputViewID: viewID, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now, ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Renew(lease.Revision(), now.Add(30*time.Minute), now.Add(time.Minute)); err == nil {
		t.Fatal("lease shortened")
	}
	lease, err = lease.Renew(lease.Revision(), now.Add(2*time.Hour), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if lease.Revision() != 2 {
		t.Fatal("renew did not advance revision")
	}

	argv := []string{"--flag", "value"}
	exec, err := NewExec(ExecSpec{ID: ids.exec, LeaseID: ids.lease, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, Kind: ExecProvider, Executable: "bin/provider", Argv: argv, WorkingDirectory: ".", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	argv[0] = "mutated"
	if exec.Spec().Argv[0] != "--flag" {
		t.Fatal("exec retained argv input")
	}
	copySpec := exec.Spec()
	copySpec.Argv[0] = "again"
	if exec.Spec().Argv[0] != "--flag" {
		t.Fatal("exec exposed argv")
	}
	if _, err := exec.Transition(ExecCompleted, exec.Revision(), now.Add(time.Second)); !IsCode(err, CodeInvalidTransition) {
		t.Fatalf("illegal exec transition: %v", err)
	}
}

func TestInputManifestCanonicalizationConflictsAndCopySafety(t *testing.T) {
	firstSidecars := []string{"metadata", "provenance"}
	first, err := NewInputViewEntry(InputViewEntrySpec{LogicalPath: "a/input.bin", OccurrenceRef: "occ:1", Digest: NewDigest([]byte("one")), Size: 3, Mode: 0o444, PermittedSidecars: firstSidecars})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewInputViewEntry(InputViewEntrySpec{LogicalPath: "b/input.bin", OccurrenceRef: "occ:2", Digest: NewDigest([]byte("two")), Size: 3, Mode: 0o400})
	if err != nil {
		t.Fatal(err)
	}
	manifestA, err := NewInputViewManifest([]InputViewEntry{second, first})
	if err != nil {
		t.Fatal(err)
	}
	manifestB, err := NewInputViewManifest([]InputViewEntry{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if manifestA.ID() != manifestB.ID() {
		t.Fatal("manifest identity depended on input ordering")
	}
	firstSidecars[0] = "mutated"
	if manifestA.Entries()[0].Spec().PermittedSidecars[0] != "metadata" {
		t.Fatal("entry retained sidecar slice")
	}
	exposed := manifestA.Entries()
	exposed[0] = second
	if manifestA.Entries()[0].LogicalPath() != "a/input.bin" {
		t.Fatal("manifest exposed entries")
	}
	ancestor, err := NewInputViewEntry(InputViewEntrySpec{LogicalPath: "a", OccurrenceRef: "occ:3", Digest: NewDigest([]byte("x")), Size: 1, Mode: 0o400})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInputViewManifest([]InputViewEntry{ancestor, first}); !IsCode(err, CodeConflict) {
		t.Fatalf("ancestor conflict: %v", err)
	}
	defaultMode, err := NewInputViewEntry(InputViewEntrySpec{LogicalPath: "default-mode", OccurrenceRef: "occ:4", Digest: NewDigest([]byte("x")), Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	if defaultMode.Spec().Mode != 0o444 {
		t.Fatalf("default input mode = %#o, want 0444", defaultMode.Spec().Mode)
	}
	if _, err := NewInputViewEntry(InputViewEntrySpec{LogicalPath: "special-mode", OccurrenceRef: "occ:5", Digest: NewDigest([]byte("x")), Size: 1, Mode: 0o1000}); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("special mode error = %v, want invalid argument", err)
	}
	if _, err := NewExportSelection(ExportSelectionSpec{RelativePath: "../escape", Roles: []string{"output"}}); err == nil {
		t.Fatal("unsafe export path accepted")
	}
}

func TestObservationTypesDistinguishUnavailableZeroAndProtectPayloads(t *testing.T) {
	ids := newFixtureIDs(t)
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	zero := uint64(0)
	metric, err := NewMetricSample(MetricSampleSpec{SubjectID: ids.subject, SubjectKind: SubjectLinuxTarget, Name: "memory.events.oom", Unit: "count", Kind: MetricCounter, Availability: MetricAvailable, CounterValue: &zero, CollectedAt: now, PublishedAt: now.Add(time.Millisecond), Cursor: 1, Labels: map[string]string{"scope": "target"}})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := metric.Counter(); !ok || value != 0 {
		t.Fatal("available numeric zero was lost")
	}
	unsupported, err := NewMetricSample(MetricSampleSpec{SubjectID: ids.subject, SubjectKind: SubjectLinuxTarget, Name: "gpu", Unit: "percent", Kind: MetricGauge, Availability: MetricUnsupported, CollectedAt: now, PublishedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unsupported.Numeric(); ok {
		t.Fatal("unsupported metric exposed a numeric zero")
	}
	numeric := float64(0)
	if _, err := NewMetricSample(MetricSampleSpec{SubjectID: ids.subject, SubjectKind: SubjectLinuxTarget, Name: "gpu", Unit: "percent", Kind: MetricGauge, Availability: MetricUnsupported, NumericValue: &numeric, CollectedAt: now, PublishedAt: now}); err == nil {
		t.Fatal("unavailable metric accepted numeric value")
	}

	payload := json.RawMessage(`{"status":"ok"}`)
	event, err := NewEventEnvelope(EventEnvelopeParams{SchemaVersion: 1, EventID: ids.event, Kind: "target.started", ResearchSessionID: ids.session, LeaseID: ids.lease, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, TargetID: ids.target, TargetGeneration: 1, TargetRunID: ids.run, Source: "world-node", SourceInstance: "node-1", ObservedWallTime: now, Sensitivity: SensitivityInternal, Completeness: CompletenessComplete, Confidence: 1, Origin: OriginSystem, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	payload[2] = 'X'
	if string(event.Payload()) != `{"status":"ok"}` {
		t.Fatal("event retained payload input")
	}
	exposedPayload := event.Payload()
	exposedPayload[2] = 'Y'
	if string(event.Payload()) != `{"status":"ok"}` {
		t.Fatal("event exposed payload")
	}
	bad := event.Params()
	bad.TargetID = TargetID{}
	bad.TargetGeneration = 1
	if _, err := NewEventEnvelope(bad); err == nil {
		t.Fatal("event accepted target generation without target")
	}
}

func TestChangeEntryAllowsOnlyOpaqueDirectoryAtTargetRoot(t *testing.T) {
	if _, err := NewChangeEntry(ChangeEntrySpec{Kind: ChangeOpaqueDirectory, Path: "."}); err != nil {
		t.Fatalf("root opaque-directory entry: %v", err)
	}
	if _, err := NewChangeEntry(ChangeEntrySpec{Kind: ChangeOpaqueDirectory, Path: "data"}); err != nil {
		t.Fatalf("nested opaque-directory entry: %v", err)
	}
	if _, err := NewChangeEntry(ChangeEntrySpec{Kind: ChangeModified, Path: "."}); err == nil {
		t.Fatal("root path must remain invalid for non-opaque change entries")
	}
}

func TestIncidentCaptureExportAndBundleModels(t *testing.T) {
	ids := newFixtureIDs(t)
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	artifact, err := NewArtifactReference(ArtifactReferenceSpec{Reference: "artifact:raw", Digest: NewDigest([]byte("raw")), Size: 3, Role: "raw_trace", Sensitivity: SensitivityRestricted})
	if err != nil {
		t.Fatal(err)
	}
	collectorCoverage, err := NewCollectorCoverage(CollectorCoverageSpec{CollectorID: ids.collector, SignalFamily: "process-tree", Placement: CollectorPlacementHost, Level: CoverageLevelComplete, Status: CoverageAvailable, Required: true, StartedAt: now, EndedAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	cause, err := NewCauseAssessment(CauseAssessmentSpec{Kind: CauseUnknown, Summary: "cause not established", Confidence: 0})
	if err != nil {
		t.Fatal(err)
	}
	incident, err := NewIncident(IncidentSpec{ID: ids.incident, Classification: IncidentLinuxTargetFailure, ResearchSessionID: ids.session, LeaseID: ids.lease, TargetID: ids.target, TargetGeneration: 1, TargetRunID: ids.run, Trigger: "container exited", LastKnownState: "running", Cause: cause, FirstRelevantCursor: 1, LastRelevantCursor: 2, Coverage: []CollectorCoverage{collectorCoverage}, Artifacts: []ArtifactReference{artifact}, RecoveryActions: []string{"recreate target"}, VisibilityAcknowledgements: []string{"collector complete"}, OccurredAt: now})
	if err != nil {
		t.Fatal(err)
	}
	incident, err = incident.Transition(IncidentEvidenceSealed, incident.Revision(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if incident.Revision() != 2 {
		t.Fatal("incident revision did not advance")
	}

	capture, err := NewCapture(CaptureSpec{ID: ids.capture, LeaseID: ids.lease, TargetRunID: ids.run, NamedProfile: "packet-ring", Kind: CapturePacket, Sensitivity: SensitivityRestricted, MaximumDuration: time.Minute, MaximumBytes: 1024, PolicyDigest: NewDigest([]byte("policy")), RequestedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	capture, err = capture.Transition(CaptureRunning, capture.Revision(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	capture, err = capture.Transition(CaptureFinalizing, capture.Revision(), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	capture, err = capture.Complete([]ArtifactReference{artifact}, capture.Revision(), now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if capture.State() != CaptureCompleted || len(capture.Artifacts()) != 1 {
		t.Fatal("capture did not complete")
	}

	selection, err := NewExportSelection(ExportSelectionSpec{RelativePath: "reports/result.json", Roles: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	export, err := NewExport(ExportSpec{ID: ids.export, LeaseID: ids.lease, WorkspaceID: ids.workspace, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, Selections: []ExportSelection{selection}, WorkspaceRevision: 1, DeclaredAt: now})
	if err != nil {
		t.Fatal(err)
	}
	export, err = export.Transition(ExportCommitting, export.Revision(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	export, err = export.Commit([]ArtifactReference{artifact}, export.Revision(), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if export.State() != ExportCommitted {
		t.Fatal("export did not commit")
	}

	change, err := NewChangeEntry(ChangeEntrySpec{Kind: ChangeModified, Path: "data/state", BeforeDigest: NewDigest([]byte("before")), AfterDigest: NewDigest([]byte("after"))})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := NewChangeSet(ChangeScopeTarget, []ChangeEntry{change}, 1, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	summary, err := NewDerivedSummary(DerivedSummarySpec{Text: "target exited", Citations: []EvidenceCitation{{FirstCursor: 1, LastCursor: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEventEnvelope(EventEnvelopeParams{SchemaVersion: 1, EventID: ids.event, Kind: "target.exit", ResearchSessionID: ids.session, LeaseID: ids.lease, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, TargetID: ids.target, TargetGeneration: 1, TargetRunID: ids.run, Source: "collector", SourceInstance: "collector-1", CollectorID: ids.collector, CollectorPlacement: CollectorPlacementHost, CoverageLevel: CoverageLevelComplete, ObservedWallTime: now, Sensitivity: SensitivityInternal, Completeness: CompletenessComplete, Confidence: 1, Origin: OriginSpecimen, Payload: json.RawMessage(`{"exit_code":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := NewObservationBundle(ObservationBundleSpec{ID: ids.bundle, TargetRunID: ids.run, TargetID: ids.target, TargetGeneration: 1, AgentWorkspaceID: ids.agentWorkspace, AgentGeneration: 1, FirstCursor: 1, LastCursor: 2, RawArtifacts: []ArtifactReference{artifact}, NormalizedEvents: []EventEnvelope{event}, Coverage: []CollectorCoverage{collectorCoverage}, TargetChanges: changes, IncidentIDs: []IncidentID{ids.incident}, Summary: summary, CreatedAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err = bundle.Seal(bundle.Revision(), now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if bundle.State() != ObservationBundleSealed || bundle.SealedAt().IsZero() {
		t.Fatal("bundle did not seal")
	}
	copySpec := bundle.Spec()
	copySpec.RawArtifacts[0] = ArtifactReference{}
	if bundle.Spec().RawArtifacts[0].Spec().Reference == "" {
		t.Fatal("bundle exposed artifact slice")
	}
}
