package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

type Sensitivity string

const (
	SensitivityPublic     Sensitivity = "public"
	SensitivityInternal   Sensitivity = "internal"
	SensitivityRestricted Sensitivity = "restricted"
	SensitivitySecret     Sensitivity = "secret"
)

func (s Sensitivity) IsValid() bool {
	return s == SensitivityPublic || s == SensitivityInternal || s == SensitivityRestricted || s == SensitivitySecret
}

type Completeness string

const (
	CompletenessUnknown  Completeness = "unknown"
	CompletenessPartial  Completeness = "partial"
	CompletenessComplete Completeness = "complete"
)

func (c Completeness) IsValid() bool {
	return c == CompletenessUnknown || c == CompletenessPartial || c == CompletenessComplete
}

type Origin string

const (
	OriginAgentControl         Origin = "agent-control"
	OriginAgentInstrumentation Origin = "agent-instrumentation"
	OriginSpecimen             Origin = "specimen"
	OriginSystem               Origin = "system"
	OriginMixedOrUnknown       Origin = "mixed-or-unknown"
)

func (o Origin) IsValid() bool {
	return o == OriginAgentControl || o == OriginAgentInstrumentation || o == OriginSpecimen || o == OriginSystem || o == OriginMixedOrUnknown
}

type CollectorPlacement string

const (
	CollectorPlacementHost              CollectorPlacement = "host"
	CollectorPlacementObserverNamespace CollectorPlacement = "observer-namespace"
	CollectorPlacementGuest             CollectorPlacement = "guest"
	CollectorPlacementInjectedProcess   CollectorPlacement = "injected-process"
	CollectorPlacementExternal          CollectorPlacement = "external"
)

func (p CollectorPlacement) IsValid() bool {
	return p == CollectorPlacementHost || p == CollectorPlacementObserverNamespace || p == CollectorPlacementGuest || p == CollectorPlacementInjectedProcess || p == CollectorPlacementExternal
}

type CoverageLevel string

const (
	CoverageLevelUnknown  CoverageLevel = "unknown"
	CoverageLevelNone     CoverageLevel = "none"
	CoverageLevelPartial  CoverageLevel = "partial"
	CoverageLevelComplete CoverageLevel = "complete"
)

func (l CoverageLevel) IsValid() bool {
	return l == CoverageLevelUnknown || l == CoverageLevelNone || l == CoverageLevelPartial || l == CoverageLevelComplete
}

type CoverageStatus string

const (
	CoverageUnknown     CoverageStatus = "unknown"
	CoverageAvailable   CoverageStatus = "available"
	CoverageDegraded    CoverageStatus = "degraded"
	CoverageLost        CoverageStatus = "lost"
	CoverageUnsupported CoverageStatus = "unsupported"
)

func (s CoverageStatus) IsValid() bool {
	return s == CoverageUnknown || s == CoverageAvailable || s == CoverageDegraded || s == CoverageLost || s == CoverageUnsupported
}

type GapKind string

const (
	GapDropped          GapKind = "dropped"
	GapOverflow         GapKind = "overflow"
	GapUnavailable      GapKind = "unavailable"
	GapStale            GapKind = "stale"
	GapCompacted        GapKind = "compacted"
	GapCollectorRestart GapKind = "collector_restart"
	GapClockUncertainty GapKind = "clock_uncertainty"
	GapLedgerRepair     GapKind = "ledger_repair"
)

func (k GapKind) IsValid() bool {
	return k == GapDropped || k == GapOverflow || k == GapUnavailable || k == GapStale || k == GapCompacted || k == GapCollectorRestart || k == GapClockUncertainty || k == GapLedgerRepair
}

type GapSpec struct {
	Kind                GapKind
	Source              string
	SourceInstance      string
	FirstSourceSequence uint64
	LastSourceSequence  uint64
	FirstCursor         ObservationCursor
	LastCursor          ObservationCursor
	StartedAt           time.Time
	EndedAt             time.Time
	LostRecords         uint64
	Reason              string
}
type Gap struct{ spec GapSpec }

func NewGap(spec GapSpec) (Gap, error) {
	if !spec.Kind.IsValid() {
		return Gap{}, NewError(CodeInvalidArgument, "gap.new", "kind", "is not recognized", nil)
	}
	if err := requireNonBlank("source", spec.Source); err != nil {
		return Gap{}, err
	}
	if err := requireNonBlank("reason", spec.Reason); err != nil {
		return Gap{}, err
	}
	if spec.LastSourceSequence > 0 && spec.LastSourceSequence < spec.FirstSourceSequence {
		return Gap{}, NewError(CodeInvalidArgument, "gap.new", "last_source_sequence", "must not precede first_source_sequence", nil)
	}
	if spec.LastCursor > 0 && spec.LastCursor < spec.FirstCursor {
		return Gap{}, NewError(CodeInvalidArgument, "gap.new", "last_cursor", "must not precede first_cursor", nil)
	}
	if spec.StartedAt.IsZero() != spec.EndedAt.IsZero() {
		return Gap{}, NewError(CodeInvalidArgument, "gap.new", "time_range", "must provide both start and end or neither", nil)
	}
	if !spec.StartedAt.IsZero() && spec.EndedAt.Before(spec.StartedAt) {
		return Gap{}, NewError(CodeInvalidArgument, "gap.new", "ended_at", "must not precede started_at", nil)
	}
	return Gap{spec: spec}, nil
}
func (g Gap) Spec() GapSpec  { return g.spec }
func (g Gap) Kind() GapKind  { return g.spec.Kind }
func (g Gap) Source() string { return g.spec.Source }

type CollectorCoverageSpec struct {
	CollectorID    CollectorID
	SignalFamily   string
	Placement      CollectorPlacement
	Level          CoverageLevel
	Status         CoverageStatus
	Required       bool
	StartedAt      time.Time
	EndedAt        time.Time
	DroppedRecords uint64
	Gaps           []Gap
}
type CollectorCoverage struct{ spec CollectorCoverageSpec }

func NewCollectorCoverage(spec CollectorCoverageSpec) (CollectorCoverage, error) {
	if spec.CollectorID.IsZero() {
		return CollectorCoverage{}, NewError(CodeInvalidID, "coverage.new", "collector_id", "must be set", nil)
	}
	if err := requireNonBlank("signal_family", spec.SignalFamily); err != nil {
		return CollectorCoverage{}, err
	}
	if !spec.Placement.IsValid() {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "placement", "is not recognized", nil)
	}
	if !spec.Level.IsValid() {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "level", "is not recognized", nil)
	}
	if !spec.Status.IsValid() {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "status", "is not recognized", nil)
	}
	if spec.StartedAt.IsZero() != spec.EndedAt.IsZero() {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "time_range", "must provide both start and end or neither", nil)
	}
	if !spec.StartedAt.IsZero() && spec.EndedAt.Before(spec.StartedAt) {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "ended_at", "must not precede started_at", nil)
	}
	if spec.Status == CoverageUnsupported && spec.Level != CoverageLevelNone {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "level", "unsupported coverage must have level none", nil)
	}
	if spec.Level == CoverageLevelComplete && (spec.DroppedRecords > 0 || len(spec.Gaps) > 0 || spec.Status != CoverageAvailable) {
		return CollectorCoverage{}, NewError(CodeInvalidArgument, "coverage.new", "level", "complete coverage cannot contain drops, gaps, or non-available status", nil)
	}
	spec.Gaps = cloneSlice(spec.Gaps)
	return CollectorCoverage{spec: spec}, nil
}
func (c CollectorCoverage) Spec() CollectorCoverageSpec {
	result := c.spec
	result.Gaps = cloneSlice(c.spec.Gaps)
	return result
}
func (c CollectorCoverage) CollectorID() CollectorID { return c.spec.CollectorID }
func (c CollectorCoverage) Level() CoverageLevel     { return c.spec.Level }
func (c CollectorCoverage) Gaps() []Gap              { return cloneSlice(c.spec.Gaps) }

type ChangeScope string

const (
	ChangeScopeAgentWorkspace ChangeScope = "agent_workspace"
	ChangeScopeTarget         ChangeScope = "target"
)

func (s ChangeScope) IsValid() bool { return s == ChangeScopeAgentWorkspace || s == ChangeScopeTarget }

type ChangeKind string

const (
	ChangeAdded           ChangeKind = "added"
	ChangeModified        ChangeKind = "modified"
	ChangeDeleted         ChangeKind = "deleted"
	ChangeRenamed         ChangeKind = "renamed"
	ChangeMetadataOnly    ChangeKind = "metadata_only"
	ChangeOpaqueDirectory ChangeKind = "opaque_directory"
)

func (k ChangeKind) IsValid() bool {
	return k == ChangeAdded || k == ChangeModified || k == ChangeDeleted || k == ChangeRenamed || k == ChangeMetadataOnly || k == ChangeOpaqueDirectory
}

type ChangeEntrySpec struct {
	Kind         ChangeKind
	Path         string
	PreviousPath string
	BeforeDigest Digest
	AfterDigest  Digest
	Metadata     map[string]string
}
type ChangeEntry struct{ spec ChangeEntrySpec }

func NewChangeEntry(spec ChangeEntrySpec) (ChangeEntry, error) {
	if !spec.Kind.IsValid() {
		return ChangeEntry{}, NewError(CodeInvalidArgument, "change.new", "kind", "is not recognized", nil)
	}
	if err := requireRelativePath("path", spec.Path, false); err != nil {
		return ChangeEntry{}, err
	}
	if spec.Kind == ChangeRenamed {
		if err := requireRelativePath("previous_path", spec.PreviousPath, false); err != nil {
			return ChangeEntry{}, err
		}
		if spec.PreviousPath == spec.Path {
			return ChangeEntry{}, NewError(CodeInvalidArgument, "change.new", "previous_path", "must differ from path", nil)
		}
	} else if spec.PreviousPath != "" {
		return ChangeEntry{}, NewError(CodeInvalidArgument, "change.new", "previous_path", "is valid only for renamed entries", nil)
	}
	if err := validateStringMap("change.new", "metadata", spec.Metadata); err != nil {
		return ChangeEntry{}, err
	}
	spec.Metadata = cloneMap(spec.Metadata)
	return ChangeEntry{spec: spec}, nil
}
func (c ChangeEntry) Spec() ChangeEntrySpec {
	result := c.spec
	result.Metadata = cloneMap(c.spec.Metadata)
	return result
}
func (c ChangeEntry) Kind() ChangeKind { return c.spec.Kind }
func (c ChangeEntry) Path() string     { return c.spec.Path }

type ChangeSet struct {
	scope             ChangeScope
	entries           []ChangeEntry
	workspaceRevision Revision
	sealedAt          time.Time
}

func NewChangeSet(scope ChangeScope, entries []ChangeEntry, workspaceRevision Revision, sealedAt time.Time) (ChangeSet, error) {
	if !scope.IsValid() {
		return ChangeSet{}, NewError(CodeInvalidArgument, "change_set.new", "scope", "is not recognized", nil)
	}
	if !workspaceRevision.IsValid() {
		return ChangeSet{}, NewError(CodeInvalidArgument, "change_set.new", "workspace_revision", "must be positive", nil)
	}
	if err := requireTime("sealed_at", sealedAt); err != nil {
		return ChangeSet{}, err
	}
	owned := cloneSlice(entries)
	sort.Slice(owned, func(i, j int) bool { return owned[i].spec.Path < owned[j].spec.Path })
	for i := range owned {
		if i > 0 && owned[i-1].spec.Path == owned[i].spec.Path {
			return ChangeSet{}, NewError(CodeInvalidArgument, "change_set.new", "entries", "contains duplicate path "+owned[i].spec.Path, nil)
		}
	}
	return ChangeSet{scope: scope, entries: owned, workspaceRevision: workspaceRevision, sealedAt: sealedAt}, nil
}
func (c ChangeSet) Scope() ChangeScope          { return c.scope }
func (c ChangeSet) Entries() []ChangeEntry      { return cloneSlice(c.entries) }
func (c ChangeSet) WorkspaceRevision() Revision { return c.workspaceRevision }
func (c ChangeSet) SealedAt() time.Time         { return c.sealedAt }

type MetricKind string

const (
	MetricCounter MetricKind = "counter"
	MetricGauge   MetricKind = "gauge"
	MetricRate    MetricKind = "rate"
)

func (k MetricKind) IsValid() bool { return k == MetricCounter || k == MetricGauge || k == MetricRate }

type MetricAvailability string

const (
	MetricAvailable   MetricAvailability = "available"
	MetricUnsupported MetricAvailability = "unsupported"
	MetricStale       MetricAvailability = "stale"
	MetricLost        MetricAvailability = "lost"
	MetricUnknown     MetricAvailability = "unknown"
)

func (a MetricAvailability) IsValid() bool {
	return a == MetricAvailable || a == MetricUnsupported || a == MetricStale || a == MetricLost || a == MetricUnknown
}

type SubjectKind string

const (
	SubjectHost           SubjectKind = "host"
	SubjectLease          SubjectKind = "lease"
	SubjectAgentWorkspace SubjectKind = "agent_workspace"
	SubjectExec           SubjectKind = "exec"
	SubjectLinuxTarget    SubjectKind = "linux_target"
	SubjectAndroidRuntime SubjectKind = "android_runtime"
	SubjectAndroidGuest   SubjectKind = "android_guest"
	SubjectAndroidApp     SubjectKind = "android_app"
	SubjectProcess        SubjectKind = "process"
	SubjectCollector      SubjectKind = "collector"
	SubjectInputView      SubjectKind = "input_view"
	SubjectNetwork        SubjectKind = "network"
)

func (k SubjectKind) IsValid() bool {
	switch k {
	case SubjectHost, SubjectLease, SubjectAgentWorkspace, SubjectExec, SubjectLinuxTarget, SubjectAndroidRuntime, SubjectAndroidGuest, SubjectAndroidApp, SubjectProcess, SubjectCollector, SubjectInputView, SubjectNetwork:
		return true
	}
	return false
}

type MetricSampleSpec struct {
	SubjectID    SubjectID
	SubjectKind  SubjectKind
	Name         string
	Unit         string
	Kind         MetricKind
	Availability MetricAvailability
	CounterValue *uint64
	NumericValue *float64
	CollectedAt  time.Time
	PublishedAt  time.Time
	Cursor       ObservationCursor
	Labels       map[string]string
	ExecID       ExecID
	TargetRunID  TargetRunID
}
type MetricSample struct{ spec MetricSampleSpec }

func NewMetricSample(spec MetricSampleSpec) (MetricSample, error) {
	if spec.SubjectID.IsZero() {
		return MetricSample{}, NewError(CodeInvalidID, "metric.new", "subject_id", "must be set", nil)
	}
	if !spec.SubjectKind.IsValid() {
		return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "subject_kind", "is not recognized", nil)
	}
	if err := requireNonBlank("name", spec.Name); err != nil {
		return MetricSample{}, err
	}
	if err := requireNonBlank("unit", spec.Unit); err != nil {
		return MetricSample{}, err
	}
	if !spec.Kind.IsValid() {
		return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "kind", "is not recognized", nil)
	}
	if !spec.Availability.IsValid() {
		return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "availability", "is not recognized", nil)
	}
	if err := requireOrderedTimes("collected_at", spec.CollectedAt, "published_at", spec.PublishedAt); err != nil {
		return MetricSample{}, err
	}
	if err := validateStringMap("metric.new", "labels", spec.Labels); err != nil {
		return MetricSample{}, err
	}
	if spec.Availability == MetricAvailable {
		if spec.Kind == MetricCounter {
			if spec.CounterValue == nil || spec.NumericValue != nil {
				return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "value", "available counters require only counter_value", nil)
			}
		} else {
			if spec.NumericValue == nil || spec.CounterValue != nil {
				return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "value", "available gauges and rates require only numeric_value", nil)
			}
			if math.IsNaN(*spec.NumericValue) || math.IsInf(*spec.NumericValue, 0) {
				return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "numeric_value", "must be finite", nil)
			}
		}
	} else if spec.CounterValue != nil || spec.NumericValue != nil {
		return MetricSample{}, NewError(CodeInvalidArgument, "metric.new", "value", "unavailable metrics must not carry a numeric value", nil)
	}
	owned := spec
	if spec.CounterValue != nil {
		value := *spec.CounterValue
		owned.CounterValue = &value
	}
	if spec.NumericValue != nil {
		value := *spec.NumericValue
		owned.NumericValue = &value
	}
	owned.Labels = cloneMap(spec.Labels)
	return MetricSample{spec: owned}, nil
}
func (m MetricSample) Spec() MetricSampleSpec {
	result := m.spec
	result.Labels = cloneMap(m.spec.Labels)
	if m.spec.CounterValue != nil {
		v := *m.spec.CounterValue
		result.CounterValue = &v
	}
	if m.spec.NumericValue != nil {
		v := *m.spec.NumericValue
		result.NumericValue = &v
	}
	return result
}
func (m MetricSample) Counter() (uint64, bool) {
	if m.spec.CounterValue == nil {
		return 0, false
	}
	return *m.spec.CounterValue, true
}
func (m MetricSample) Numeric() (float64, bool) {
	if m.spec.NumericValue == nil {
		return 0, false
	}
	return *m.spec.NumericValue, true
}
func (m MetricSample) Availability() MetricAvailability { return m.spec.Availability }

type CorrelationLink struct {
	EventID    EventID
	Method     string
	Confidence float64
}

type EventEnvelopeParams struct {
	SchemaVersion               uint32
	EventID                     EventID
	Kind                        string
	ResearchSessionID           ResearchSessionID
	LeaseID                     LeaseID
	AgentWorkspaceID            AgentWorkspaceID
	AgentGeneration             AgentGeneration
	ExecID                      ExecID
	TargetID                    TargetID
	TargetGeneration            TargetGeneration
	TargetRunID                 TargetRunID
	TargetOperationID           TargetOperationID
	CorrelationID               CorrelationID
	CausationID                 EventID
	CorrelatedWith              []CorrelationLink
	TraceID                     string
	SpanID                      string
	Source                      string
	SourceInstance              string
	SourceSequence              uint64
	SourceCursor                ObservationCursor
	CollectorID                 CollectorID
	CollectorPlacement          CollectorPlacement
	CoverageLevel               CoverageLevel
	ObservedWallTime            time.Time
	ObservedMonotonicTime       time.Duration
	SubjectTime                 time.Time
	SubjectClockDomain          string
	ClockSyncEpoch              uint64
	HostBootID                  string
	ContainerID                 string
	CgroupID                    string
	ProcessID                   int64
	ProcessStartTime            time.Time
	AndroidPID                  int64
	AndroidUID                  int64
	AndroidPackage              string
	PolicyDigest                Digest
	CapabilityFingerprintDigest Digest
	Payload                     json.RawMessage
	Sensitivity                 Sensitivity
	Completeness                Completeness
	Confidence                  float64
	Origin                      Origin
}
type EventEnvelope struct{ params EventEnvelopeParams }

func NewEventEnvelope(params EventEnvelopeParams) (EventEnvelope, error) {
	if params.SchemaVersion == 0 {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "schema_version", "must be positive", nil)
	}
	if params.EventID.IsZero() {
		return EventEnvelope{}, NewError(CodeInvalidID, "event.new", "event_id", "must be set", nil)
	}
	for field, value := range map[string]string{"kind": params.Kind, "source": params.Source, "source_instance": params.SourceInstance} {
		if err := requireNonBlank(field, value); err != nil {
			return EventEnvelope{}, err
		}
	}
	if err := requireTime("observed_wall_time", params.ObservedWallTime); err != nil {
		return EventEnvelope{}, err
	}
	if params.ObservedMonotonicTime < 0 {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "observed_monotonic_time", "must not be negative", nil)
	}
	if !params.Sensitivity.IsValid() {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "sensitivity", "is not recognized", nil)
	}
	if !params.Completeness.IsValid() {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "completeness", "is not recognized", nil)
	}
	if !params.Origin.IsValid() {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "origin", "is not recognized", nil)
	}
	if params.Confidence < 0 || params.Confidence > 1 || math.IsNaN(params.Confidence) {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "confidence", "must be between 0 and 1", nil)
	}
	if len(params.Payload) > 0 && !json.Valid(params.Payload) {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "payload", "must contain valid JSON", nil)
	}
	if !params.LeaseID.IsZero() && params.ResearchSessionID.IsZero() {
		return EventEnvelope{}, missingEventParent("lease_id", "research_session_id")
	}
	if !params.AgentWorkspaceID.IsZero() && params.ResearchSessionID.IsZero() {
		return EventEnvelope{}, missingEventParent("agent_workspace_id", "research_session_id")
	}
	if params.AgentGeneration > 0 && params.AgentWorkspaceID.IsZero() {
		return EventEnvelope{}, missingEventParent("agent_generation", "agent_workspace_id")
	}
	if !params.ExecID.IsZero() && (params.LeaseID.IsZero() || params.AgentWorkspaceID.IsZero() || !params.AgentGeneration.IsValid()) {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "exec_id", "requires lease, agent workspace, and agent generation", nil)
	}
	if !params.TargetID.IsZero() && params.ResearchSessionID.IsZero() {
		return EventEnvelope{}, missingEventParent("target_id", "research_session_id")
	}
	if params.TargetGeneration > 0 && params.TargetID.IsZero() {
		return EventEnvelope{}, missingEventParent("target_generation", "target_id")
	}
	if !params.TargetRunID.IsZero() && (params.TargetID.IsZero() || !params.TargetGeneration.IsValid() || params.LeaseID.IsZero()) {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "target_run_id", "requires lease, target, and target generation", nil)
	}
	if !params.TargetOperationID.IsZero() && params.TargetRunID.IsZero() {
		return EventEnvelope{}, missingEventParent("target_operation_id", "target_run_id")
	}
	if params.ProcessID < 0 || params.AndroidPID < 0 || params.AndroidUID < 0 {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "process_identity", "numeric identities must not be negative", nil)
	}
	if params.ProcessID > 0 && params.ProcessStartTime.IsZero() {
		return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "process_start_time", "is required with process_id", nil)
	}
	if !params.CollectorID.IsZero() {
		if !params.CollectorPlacement.IsValid() {
			return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "collector_placement", "is required with collector_id", nil)
		}
		if !params.CoverageLevel.IsValid() {
			return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", "coverage_level", "is required with collector_id", nil)
		}
	}
	for i, link := range params.CorrelatedWith {
		if link.EventID.IsZero() {
			return EventEnvelope{}, NewError(CodeInvalidID, "event.new", fmt.Sprintf("correlated_with[%d].event_id", i), "must be set", nil)
		}
		if err := requireNonBlank(fmt.Sprintf("correlated_with[%d].method", i), link.Method); err != nil {
			return EventEnvelope{}, err
		}
		if link.Confidence < 0 || link.Confidence > 1 || math.IsNaN(link.Confidence) {
			return EventEnvelope{}, NewError(CodeInvalidArgument, "event.new", fmt.Sprintf("correlated_with[%d].confidence", i), "must be between 0 and 1", nil)
		}
	}
	params.Payload = cloneSlice(params.Payload)
	params.CorrelatedWith = cloneSlice(params.CorrelatedWith)
	return EventEnvelope{params: params}, nil
}
func missingEventParent(child, parent string) error {
	return NewError(CodeInvalidArgument, "event.new", child, "requires "+parent, nil)
}
func (e EventEnvelope) Params() EventEnvelopeParams {
	result := e.params
	result.Payload = cloneSlice(e.params.Payload)
	result.CorrelatedWith = cloneSlice(e.params.CorrelatedWith)
	return result
}
func (e EventEnvelope) ID() EventID              { return e.params.EventID }
func (e EventEnvelope) Kind() string             { return e.params.Kind }
func (e EventEnvelope) Payload() json.RawMessage { return cloneSlice(e.params.Payload) }

type ArtifactReferenceSpec struct {
	Reference   string
	Digest      Digest
	Size        int64
	Role        string
	Sensitivity Sensitivity
}
type ArtifactReference struct{ spec ArtifactReferenceSpec }

func NewArtifactReference(spec ArtifactReferenceSpec) (ArtifactReference, error) {
	if err := requireNonBlank("reference", spec.Reference); err != nil {
		return ArtifactReference{}, err
	}
	if spec.Digest.IsZero() {
		return ArtifactReference{}, NewError(CodeInvalidArgument, "artifact_reference.new", "digest", "must be set", nil)
	}
	if err := requireNonNegative("size", spec.Size); err != nil {
		return ArtifactReference{}, err
	}
	if err := requireNonBlank("role", spec.Role); err != nil {
		return ArtifactReference{}, err
	}
	if !spec.Sensitivity.IsValid() {
		return ArtifactReference{}, NewError(CodeInvalidArgument, "artifact_reference.new", "sensitivity", "is not recognized", nil)
	}
	return ArtifactReference{spec: spec}, nil
}
func (r ArtifactReference) Spec() ArtifactReferenceSpec { return r.spec }

var _ = sort.Strings
