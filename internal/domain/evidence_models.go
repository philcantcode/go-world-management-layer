package domain

import (
	"fmt"
	"time"
)

type IncidentClassification string

const (
	IncidentAgentExecFailure      IncidentClassification = "agent_exec_failure"
	IncidentAgentWorkspaceFailure IncidentClassification = "agent_workspace_failure"
	IncidentTargetWorkloadExit    IncidentClassification = "target_workload_exit"
	IncidentLinuxTargetFailure    IncidentClassification = "linux_target_failure"
	IncidentEmulatorFailure       IncidentClassification = "emulator_failure"
	IncidentAndroidFailure        IncidentClassification = "android_failure"
	IncidentDeviceDisconnect      IncidentClassification = "device_disconnect"
	IncidentHostPressure          IncidentClassification = "host_pressure"
	IncidentResourceEviction      IncidentClassification = "resource_eviction"
	IncidentObserverFailure       IncidentClassification = "observer_failure"
	IncidentWorkspaceIntegrity    IncidentClassification = "workspace_integrity"
	IncidentControlPlaneFailure   IncidentClassification = "control_plane_failure"
)

func (c IncidentClassification) IsValid() bool {
	switch c {
	case IncidentAgentExecFailure, IncidentAgentWorkspaceFailure, IncidentTargetWorkloadExit,
		IncidentLinuxTargetFailure, IncidentEmulatorFailure, IncidentAndroidFailure,
		IncidentDeviceDisconnect, IncidentHostPressure, IncidentResourceEviction,
		IncidentObserverFailure, IncidentWorkspaceIntegrity, IncidentControlPlaneFailure:
		return true
	}
	return false
}

type CauseKind string

const (
	CauseProven     CauseKind = "proven"
	CauseCorrelated CauseKind = "correlated"
	CauseUnknown    CauseKind = "unknown"
)

func (k CauseKind) IsValid() bool {
	return k == CauseProven || k == CauseCorrelated || k == CauseUnknown
}

type CauseAssessmentSpec struct {
	Kind       CauseKind
	Summary    string
	Method     string
	Confidence float64
}
type CauseAssessment struct{ spec CauseAssessmentSpec }

func NewCauseAssessment(spec CauseAssessmentSpec) (CauseAssessment, error) {
	if !spec.Kind.IsValid() {
		return CauseAssessment{}, NewError(CodeInvalidArgument, "cause.new", "kind", "is not recognized", nil)
	}
	if err := requireNonBlank("summary", spec.Summary); err != nil {
		return CauseAssessment{}, err
	}
	if spec.Confidence < 0 || spec.Confidence > 1 {
		return CauseAssessment{}, NewError(CodeInvalidArgument, "cause.new", "confidence", "must be between 0 and 1", nil)
	}
	if spec.Kind == CauseCorrelated {
		if err := requireNonBlank("method", spec.Method); err != nil {
			return CauseAssessment{}, err
		}
	} else if spec.Method != "" {
		return CauseAssessment{}, NewError(CodeInvalidArgument, "cause.new", "method", "is valid only for correlated causes", nil)
	}
	if spec.Kind == CauseProven && spec.Confidence != 1 {
		return CauseAssessment{}, NewError(CodeInvalidArgument, "cause.new", "confidence", "proven causes must have confidence 1", nil)
	}
	return CauseAssessment{spec: spec}, nil
}
func (c CauseAssessment) Spec() CauseAssessmentSpec { return c.spec }

type IncidentSpec struct {
	ID                         IncidentID
	Classification             IncidentClassification
	ResearchSessionID          ResearchSessionID
	LeaseID                    LeaseID
	AgentWorkspaceID           AgentWorkspaceID
	AgentGeneration            AgentGeneration
	ExecID                     ExecID
	TargetID                   TargetID
	TargetGeneration           TargetGeneration
	TargetRunID                TargetRunID
	Trigger                    string
	LastKnownState             string
	Cause                      CauseAssessment
	HighWaterMetrics           []MetricSample
	FirstRelevantCursor        ObservationCursor
	LastRelevantCursor         ObservationCursor
	Coverage                   []CollectorCoverage
	ObservationBundleID        ObservationBundleID
	Artifacts                  []ArtifactReference
	RecoveryActions            []string
	VisibilityAcknowledgements []string
	OccurredAt                 time.Time
}

type Incident struct {
	spec      IncidentSpec
	state     IncidentState
	revision  Revision
	updatedAt time.Time
}

func NewIncident(spec IncidentSpec) (Incident, error) {
	if err := requireID("incident_id", spec.ID); err != nil {
		return Incident{}, err
	}
	if !spec.Classification.IsValid() {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "classification", "is not recognized", nil)
	}
	if err := requireID("research_session_id", spec.ResearchSessionID); err != nil {
		return Incident{}, err
	}
	if err := requireNonBlank("trigger", spec.Trigger); err != nil {
		return Incident{}, err
	}
	if err := requireNonBlank("last_known_state", spec.LastKnownState); err != nil {
		return Incident{}, err
	}
	if !spec.Cause.spec.Kind.IsValid() {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "cause", "must be initialized", nil)
	}
	if err := requireTime("occurred_at", spec.OccurredAt); err != nil {
		return Incident{}, err
	}
	if !spec.ExecID.IsZero() && (spec.AgentWorkspaceID.IsZero() || !spec.AgentGeneration.IsValid()) {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "exec_id", "requires agent workspace and generation", nil)
	}
	if spec.AgentGeneration > 0 && spec.AgentWorkspaceID.IsZero() {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "agent_generation", "requires agent_workspace_id", nil)
	}
	if spec.TargetGeneration > 0 && spec.TargetID.IsZero() {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "target_generation", "requires target_id", nil)
	}
	if !spec.TargetRunID.IsZero() && (spec.TargetID.IsZero() || !spec.TargetGeneration.IsValid()) {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "target_run_id", "requires target and target generation", nil)
	}
	if spec.LastRelevantCursor > 0 && spec.LastRelevantCursor < spec.FirstRelevantCursor {
		return Incident{}, NewError(CodeInvalidArgument, "incident.new", "last_relevant_cursor", "must not precede first_relevant_cursor", nil)
	}
	if err := validateConstructedEvidence(spec.HighWaterMetrics, spec.Coverage, spec.Artifacts); err != nil {
		return Incident{}, err
	}
	recovery, err := uniqueNonBlank(spec.RecoveryActions, "recovery_actions")
	if err != nil {
		return Incident{}, err
	}
	acknowledgements, err := uniqueNonBlank(spec.VisibilityAcknowledgements, "visibility_acknowledgements")
	if err != nil {
		return Incident{}, err
	}
	spec.HighWaterMetrics = cloneSlice(spec.HighWaterMetrics)
	spec.Coverage = cloneSlice(spec.Coverage)
	spec.Artifacts = cloneSlice(spec.Artifacts)
	spec.RecoveryActions = recovery
	spec.VisibilityAcknowledgements = acknowledgements
	return Incident{spec: spec, state: IncidentOpen, revision: InitialRevision, updatedAt: spec.OccurredAt}, nil
}

func validateConstructedEvidence(metrics []MetricSample, coverage []CollectorCoverage, artifacts []ArtifactReference) error {
	for i, metric := range metrics {
		if metric.spec.Name == "" {
			return NewError(CodeInvalidArgument, "evidence.validate", fmt.Sprintf("metrics[%d]", i), "must be constructed and valid", nil)
		}
	}
	for i, item := range coverage {
		if item.spec.CollectorID.IsZero() {
			return NewError(CodeInvalidArgument, "evidence.validate", fmt.Sprintf("coverage[%d]", i), "must be constructed and valid", nil)
		}
	}
	for i, artifact := range artifacts {
		if artifact.spec.Reference == "" || artifact.spec.Digest.IsZero() {
			return NewError(CodeInvalidArgument, "evidence.validate", fmt.Sprintf("artifacts[%d]", i), "must be constructed and valid", nil)
		}
	}
	return nil
}

func (i Incident) Spec() IncidentSpec {
	result := i.spec
	result.HighWaterMetrics = cloneSlice(i.spec.HighWaterMetrics)
	result.Coverage = cloneSlice(i.spec.Coverage)
	result.Artifacts = cloneSlice(i.spec.Artifacts)
	result.RecoveryActions = cloneSlice(i.spec.RecoveryActions)
	result.VisibilityAcknowledgements = cloneSlice(i.spec.VisibilityAcknowledgements)
	return result
}
func (i Incident) ID() IncidentID       { return i.spec.ID }
func (i Incident) State() IncidentState { return i.state }
func (i Incident) Revision() Revision   { return i.revision }
func (i Incident) Transition(next IncidentState, expected Revision, at time.Time) (Incident, error) {
	if err := RequireIncidentTransition(i.state, next); err != nil {
		return Incident{}, err
	}
	revision, err := nextModelRevision(i.revision, expected, i.updatedAt, at)
	if err != nil {
		return Incident{}, err
	}
	i.state, i.revision, i.updatedAt = next, revision, at
	return i, nil
}

type CaptureKind string

const (
	CapturePacket          CaptureKind = "packet"
	CaptureTrace           CaptureKind = "trace"
	CaptureLog             CaptureKind = "log"
	CaptureProfile         CaptureKind = "profile"
	CaptureFilesystem      CaptureKind = "filesystem"
	CaptureScreenshot      CaptureKind = "screenshot"
	CaptureScreenRecording CaptureKind = "screen_recording"
)

func (k CaptureKind) IsValid() bool {
	switch k {
	case CapturePacket, CaptureTrace, CaptureLog, CaptureProfile, CaptureFilesystem, CaptureScreenshot, CaptureScreenRecording:
		return true
	}
	return false
}

type CaptureSpec struct {
	ID              CaptureID
	LeaseID         LeaseID
	TargetRunID     TargetRunID
	NamedProfile    string
	Kind            CaptureKind
	Sensitivity     Sensitivity
	MaximumDuration time.Duration
	MaximumBytes    int64
	PolicyDigest    Digest
	RequestedAt     time.Time
}

type Capture struct {
	spec      CaptureSpec
	state     CaptureState
	revision  Revision
	updatedAt time.Time
	artifacts []ArtifactReference
}

func NewCapture(spec CaptureSpec) (Capture, error) {
	if err := requireID("capture_id", spec.ID); err != nil {
		return Capture{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return Capture{}, err
	}
	if err := requireNonBlank("named_profile", spec.NamedProfile); err != nil {
		return Capture{}, err
	}
	if !spec.Kind.IsValid() {
		return Capture{}, NewError(CodeInvalidArgument, "capture.new", "kind", "is not recognized", nil)
	}
	if !spec.Sensitivity.IsValid() {
		return Capture{}, NewError(CodeInvalidArgument, "capture.new", "sensitivity", "is not recognized", nil)
	}
	if spec.MaximumDuration <= 0 {
		return Capture{}, NewError(CodeInvalidArgument, "capture.new", "maximum_duration", "must be positive", nil)
	}
	if spec.MaximumBytes <= 0 {
		return Capture{}, NewError(CodeInvalidArgument, "capture.new", "maximum_bytes", "must be positive", nil)
	}
	if spec.PolicyDigest.IsZero() {
		return Capture{}, NewError(CodeInvalidArgument, "capture.new", "policy_digest", "must be set", nil)
	}
	if err := requireTime("requested_at", spec.RequestedAt); err != nil {
		return Capture{}, err
	}
	return Capture{spec: spec, state: CaptureRequested, revision: InitialRevision, updatedAt: spec.RequestedAt}, nil
}

func (c Capture) Spec() CaptureSpec              { return c.spec }
func (c Capture) ID() CaptureID                  { return c.spec.ID }
func (c Capture) State() CaptureState            { return c.state }
func (c Capture) Revision() Revision             { return c.revision }
func (c Capture) Artifacts() []ArtifactReference { return cloneSlice(c.artifacts) }
func (c Capture) Transition(next CaptureState, expected Revision, at time.Time) (Capture, error) {
	if err := RequireCaptureTransition(c.state, next); err != nil {
		return Capture{}, err
	}
	revision, err := nextModelRevision(c.revision, expected, c.updatedAt, at)
	if err != nil {
		return Capture{}, err
	}
	c.state, c.revision, c.updatedAt = next, revision, at
	return c, nil
}
func (c Capture) Complete(artifacts []ArtifactReference, expected Revision, at time.Time) (Capture, error) {
	if c.state != CaptureFinalizing {
		return Capture{}, NewError(CodeFailedPrecondition, "capture.complete", "state", "must be finalizing", nil)
	}
	if len(artifacts) == 0 {
		return Capture{}, NewError(CodeInvalidArgument, "capture.complete", "artifacts", "must not be empty", nil)
	}
	if err := validateConstructedEvidence(nil, nil, artifacts); err != nil {
		return Capture{}, err
	}
	revision, err := nextModelRevision(c.revision, expected, c.updatedAt, at)
	if err != nil {
		return Capture{}, err
	}
	c.artifacts = cloneSlice(artifacts)
	c.state, c.revision, c.updatedAt = CaptureCompleted, revision, at
	return c, nil
}

type EvidenceCitation struct {
	FirstCursor ObservationCursor
	LastCursor  ObservationCursor
	Artifact    ArtifactReference
}

type DerivedSummarySpec struct {
	Text       string
	Citations  []EvidenceCitation
	Inferences []string
}
type DerivedSummary struct{ spec DerivedSummarySpec }

func NewDerivedSummary(spec DerivedSummarySpec) (DerivedSummary, error) {
	if err := requireNonBlank("text", spec.Text); err != nil {
		return DerivedSummary{}, err
	}
	if len(spec.Citations) == 0 {
		return DerivedSummary{}, NewError(CodeInvalidArgument, "summary.new", "citations", "must not be empty", nil)
	}
	for i, citation := range spec.Citations {
		if citation.LastCursor > 0 && citation.LastCursor < citation.FirstCursor {
			return DerivedSummary{}, NewError(CodeInvalidArgument, "summary.new", fmt.Sprintf("citations[%d]", i), "cursor range is reversed", nil)
		}
		if citation.FirstCursor == 0 && citation.Artifact.spec.Reference == "" {
			return DerivedSummary{}, NewError(CodeInvalidArgument, "summary.new", fmt.Sprintf("citations[%d]", i), "must cite a cursor range or artifact", nil)
		}
	}
	inferences, err := uniqueNonBlank(spec.Inferences, "inferences")
	if err != nil {
		return DerivedSummary{}, err
	}
	spec.Citations = cloneSlice(spec.Citations)
	spec.Inferences = inferences
	return DerivedSummary{spec: spec}, nil
}
func (s DerivedSummary) Spec() DerivedSummarySpec {
	result := s.spec
	result.Citations = cloneSlice(s.spec.Citations)
	result.Inferences = cloneSlice(s.spec.Inferences)
	return result
}

type ObservationBundleSpec struct {
	ID               ObservationBundleID
	TargetRunID      TargetRunID
	TargetID         TargetID
	TargetGeneration TargetGeneration
	AgentWorkspaceID AgentWorkspaceID
	AgentGeneration  AgentGeneration
	FirstCursor      ObservationCursor
	LastCursor       ObservationCursor
	RawArtifacts     []ArtifactReference
	NormalizedEvents []EventEnvelope
	Metrics          []MetricSample
	Coverage         []CollectorCoverage
	Gaps             []Gap
	TargetChanges    ChangeSet
	IncidentIDs      []IncidentID
	Summary          DerivedSummary
	CreatedAt        time.Time
}

type ObservationBundle struct {
	spec      ObservationBundleSpec
	state     ObservationBundleState
	revision  Revision
	updatedAt time.Time
	sealedAt  time.Time
}

func NewObservationBundle(spec ObservationBundleSpec) (ObservationBundle, error) {
	if err := requireID("observation_bundle_id", spec.ID); err != nil {
		return ObservationBundle{}, err
	}
	if err := requireID("target_run_id", spec.TargetRunID); err != nil {
		return ObservationBundle{}, err
	}
	if err := requireID("target_id", spec.TargetID); err != nil {
		return ObservationBundle{}, err
	}
	if !spec.TargetGeneration.IsValid() {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "target_generation", "must be positive", nil)
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return ObservationBundle{}, err
	}
	if !spec.AgentGeneration.IsValid() {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "agent_generation", "must be positive", nil)
	}
	if spec.FirstCursor == 0 || spec.LastCursor < spec.FirstCursor {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "cursor_range", "must be a non-empty ordered range", nil)
	}
	if len(spec.RawArtifacts) == 0 || len(spec.NormalizedEvents) == 0 || len(spec.Coverage) == 0 {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "contents", "raw artifacts, normalized events, and coverage must not be empty", nil)
	}
	if err := validateConstructedEvidence(spec.Metrics, spec.Coverage, spec.RawArtifacts); err != nil {
		return ObservationBundle{}, err
	}
	for i, event := range spec.NormalizedEvents {
		if event.params.EventID.IsZero() {
			return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", fmt.Sprintf("normalized_events[%d]", i), "must be constructed and valid", nil)
		}
		if event.params.TargetRunID != spec.TargetRunID {
			return ObservationBundle{}, NewError(CodeConflict, "bundle.new", fmt.Sprintf("normalized_events[%d].target_run_id", i), "does not match the bundle run", nil)
		}
	}
	if spec.TargetChanges.scope != ChangeScopeTarget {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "target_changes", "must be an initialized target change set", nil)
	}
	if !spec.Summary.spec.TextIsValid() {
		return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "summary", "must be initialized", nil)
	}
	seenIncidents := make(map[IncidentID]struct{}, len(spec.IncidentIDs))
	for i, id := range spec.IncidentIDs {
		if id.IsZero() {
			return ObservationBundle{}, NewError(CodeInvalidID, "bundle.new", fmt.Sprintf("incident_ids[%d]", i), "must be set", nil)
		}
		if _, duplicate := seenIncidents[id]; duplicate {
			return ObservationBundle{}, NewError(CodeInvalidArgument, "bundle.new", "incident_ids", "must not contain duplicates", nil)
		}
		seenIncidents[id] = struct{}{}
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return ObservationBundle{}, err
	}
	spec.RawArtifacts = cloneSlice(spec.RawArtifacts)
	spec.NormalizedEvents = cloneSlice(spec.NormalizedEvents)
	spec.Metrics = cloneSlice(spec.Metrics)
	spec.Coverage = cloneSlice(spec.Coverage)
	spec.Gaps = cloneSlice(spec.Gaps)
	spec.TargetChanges.entries = cloneSlice(spec.TargetChanges.entries)
	spec.IncidentIDs = cloneSlice(spec.IncidentIDs)
	spec.Summary.spec.Citations = cloneSlice(spec.Summary.spec.Citations)
	spec.Summary.spec.Inferences = cloneSlice(spec.Summary.spec.Inferences)
	return ObservationBundle{spec: spec, state: ObservationBundleBuilding, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}

func (s DerivedSummarySpec) TextIsValid() bool { return s.Text != "" && len(s.Citations) > 0 }

func (b ObservationBundle) Spec() ObservationBundleSpec {
	result := b.spec
	result.RawArtifacts = cloneSlice(b.spec.RawArtifacts)
	result.NormalizedEvents = cloneSlice(b.spec.NormalizedEvents)
	result.Metrics = cloneSlice(b.spec.Metrics)
	result.Coverage = cloneSlice(b.spec.Coverage)
	result.Gaps = cloneSlice(b.spec.Gaps)
	result.TargetChanges.entries = cloneSlice(b.spec.TargetChanges.entries)
	result.IncidentIDs = cloneSlice(b.spec.IncidentIDs)
	result.Summary.spec.Citations = cloneSlice(b.spec.Summary.spec.Citations)
	result.Summary.spec.Inferences = cloneSlice(b.spec.Summary.spec.Inferences)
	return result
}
func (b ObservationBundle) ID() ObservationBundleID       { return b.spec.ID }
func (b ObservationBundle) TargetRunID() TargetRunID      { return b.spec.TargetRunID }
func (b ObservationBundle) State() ObservationBundleState { return b.state }
func (b ObservationBundle) Revision() Revision            { return b.revision }
func (b ObservationBundle) SealedAt() time.Time           { return b.sealedAt }
func (b ObservationBundle) Seal(expected Revision, at time.Time) (ObservationBundle, error) {
	return b.Transition(ObservationBundleSealed, expected, at)
}
func (b ObservationBundle) Transition(next ObservationBundleState, expected Revision, at time.Time) (ObservationBundle, error) {
	if err := RequireObservationBundleTransition(b.state, next); err != nil {
		return ObservationBundle{}, err
	}
	revision, err := nextModelRevision(b.revision, expected, b.updatedAt, at)
	if err != nil {
		return ObservationBundle{}, err
	}
	b.state, b.revision, b.updatedAt = next, revision, at
	if next == ObservationBundleSealed {
		b.sealedAt = at
	}
	return b, nil
}
