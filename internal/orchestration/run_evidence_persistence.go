package orchestration

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// persistedRunEvidence is the lossless, restart-safe representation shared by
// observer append journals and the Service-owned pre-publication checkpoint.
// Event payload bytes remain in the hash-chained ledger; the checkpoint binds
// each exact record by cursor and chain hash and retains every semantic field
// that is not represented by ledger.Record.
type persistedRunEvidence struct {
	Required    []string                   `json:"required"`
	FirstCursor uint64                     `json:"first_cursor"`
	LastCursor  uint64                     `json:"last_cursor"`
	Artifacts   []persistedArtifact        `json:"artifacts"`
	Events      []persistedEvent           `json:"events"`
	Metrics     []persistedMetric          `json:"metrics"`
	Coverage    []persistedCoverage        `json:"coverage"`
	Gaps        []persistedGap             `json:"gaps"`
	StoppedAt   time.Time                  `json:"stopped_at"`
	Failures    []persistedObserverFailure `json:"failures"`
}

type persistedTargetRunResult struct {
	RunID         string              `json:"run_id"`
	Outcome       ports.RunOutcome    `json:"outcome"`
	FirstCursor   uint64              `json:"first_cursor"`
	LastCursor    uint64              `json:"last_cursor"`
	Artifacts     []persistedArtifact `json:"artifacts"`
	Events        []persistedEvent    `json:"events"`
	Metrics       []persistedMetric   `json:"metrics"`
	Coverage      []persistedCoverage `json:"coverage"`
	Gaps          []persistedGap      `json:"gaps"`
	TargetChanges persistedChangeSet  `json:"target_changes"`
	IncidentIDs   []string            `json:"incident_ids"`
	Summary       persistedSummary    `json:"summary"`
	StoppedAt     time.Time           `json:"stopped_at"`
}

type persistedObserverFailure struct {
	CollectorID string `json:"collector_id"`
	Family      string `json:"family"`
	Required    bool   `json:"required"`
	Reason      string `json:"reason"`
}

type persistedArtifact struct {
	Reference   string             `json:"reference"`
	Digest      string             `json:"digest"`
	Size        int64              `json:"size"`
	Role        string             `json:"role"`
	Sensitivity domain.Sensitivity `json:"sensitivity"`
}

type persistedGap struct {
	Kind                domain.GapKind `json:"kind"`
	Source              string         `json:"source"`
	SourceInstance      string         `json:"source_instance,omitempty"`
	FirstSourceSequence uint64         `json:"first_source_sequence,omitempty"`
	LastSourceSequence  uint64         `json:"last_source_sequence,omitempty"`
	FirstCursor         uint64         `json:"first_cursor,omitempty"`
	LastCursor          uint64         `json:"last_cursor,omitempty"`
	StartedAt           time.Time      `json:"started_at,omitempty"`
	EndedAt             time.Time      `json:"ended_at,omitempty"`
	LostRecords         uint64         `json:"lost_records,omitempty"`
	Reason              string         `json:"reason"`
}

type persistedCoverage struct {
	CollectorID    string                    `json:"collector_id"`
	SignalFamily   string                    `json:"signal_family"`
	Placement      domain.CollectorPlacement `json:"placement"`
	Level          domain.CoverageLevel      `json:"level"`
	Status         domain.CoverageStatus     `json:"status"`
	Required       bool                      `json:"required"`
	StartedAt      time.Time                 `json:"started_at"`
	EndedAt        time.Time                 `json:"ended_at"`
	DroppedRecords uint64                    `json:"dropped_records,omitempty"`
	Gaps           []persistedGap            `json:"gaps"`
	GapReferences  []string                  `json:"gap_references,omitempty"`
}

type persistedMetric struct {
	SubjectID    string                    `json:"subject_id"`
	SubjectKind  domain.SubjectKind        `json:"subject_kind"`
	Name         string                    `json:"name"`
	Unit         string                    `json:"unit"`
	Kind         domain.MetricKind         `json:"kind"`
	Availability domain.MetricAvailability `json:"availability"`
	CounterValue *uint64                   `json:"counter_value,omitempty"`
	NumericValue *float64                  `json:"numeric_value,omitempty"`
	CollectedAt  time.Time                 `json:"collected_at"`
	PublishedAt  time.Time                 `json:"published_at"`
	Cursor       uint64                    `json:"cursor"`
	Labels       map[string]string         `json:"labels,omitempty"`
	ExecID       string                    `json:"exec_id,omitempty"`
	TargetRunID  string                    `json:"target_run_id,omitempty"`
}

type persistedEvent struct {
	LedgerCursor                 uint64                     `json:"ledger_cursor"`
	LedgerChainHash              string                     `json:"ledger_chain_hash"`
	SchemaVersion                uint32                     `json:"schema_version"`
	EventID                      string                     `json:"event_id"`
	Kind                         string                     `json:"kind"`
	ResearchSessionID            string                     `json:"research_session_id,omitempty"`
	LeaseID                      string                     `json:"lease_id,omitempty"`
	AgentWorkspaceID             string                     `json:"agent_workspace_id,omitempty"`
	AgentGeneration              uint64                     `json:"agent_generation,omitempty"`
	ExecID                       string                     `json:"exec_id,omitempty"`
	TargetID                     string                     `json:"target_id,omitempty"`
	TargetGeneration             uint64                     `json:"target_generation,omitempty"`
	TargetRunID                  string                     `json:"target_run_id,omitempty"`
	TargetOperationID            string                     `json:"target_operation_id,omitempty"`
	CorrelationID                string                     `json:"correlation_id,omitempty"`
	CausationID                  string                     `json:"causation_id,omitempty"`
	CorrelatedWith               []persistedCorrelationLink `json:"correlated_with"`
	TraceID                      string                     `json:"trace_id,omitempty"`
	SpanID                       string                     `json:"span_id,omitempty"`
	Source                       string                     `json:"source"`
	SourceInstance               string                     `json:"source_instance"`
	SourceSequence               uint64                     `json:"source_sequence,omitempty"`
	CollectorID                  string                     `json:"collector_id,omitempty"`
	CollectorPlacement           domain.CollectorPlacement  `json:"collector_placement,omitempty"`
	CoverageLevel                domain.CoverageLevel       `json:"coverage_level,omitempty"`
	ObservedWallTime             time.Time                  `json:"observed_wall_time"`
	ObservedMonotonicNanoseconds int64                      `json:"observed_monotonic_nanoseconds,omitempty"`
	SubjectTime                  time.Time                  `json:"subject_time,omitempty"`
	SubjectClockDomain           string                     `json:"subject_clock_domain,omitempty"`
	ClockSyncEpoch               uint64                     `json:"clock_sync_epoch,omitempty"`
	HostBootID                   string                     `json:"host_boot_id,omitempty"`
	ContainerID                  string                     `json:"container_id,omitempty"`
	CgroupID                     string                     `json:"cgroup_id,omitempty"`
	ProcessID                    int64                      `json:"process_id,omitempty"`
	ProcessStartTime             time.Time                  `json:"process_start_time,omitempty"`
	AndroidPID                   int64                      `json:"android_pid,omitempty"`
	AndroidUID                   int64                      `json:"android_uid,omitempty"`
	AndroidPackage               string                     `json:"android_package,omitempty"`
	PolicyDigest                 string                     `json:"policy_digest,omitempty"`
	CapabilityFingerprintDigest  string                     `json:"capability_fingerprint_digest,omitempty"`
	Sensitivity                  domain.Sensitivity         `json:"sensitivity"`
	Completeness                 domain.Completeness        `json:"completeness"`
	Confidence                   float64                    `json:"confidence"`
	Origin                       domain.Origin              `json:"origin"`
}

type persistedCorrelationLink struct {
	EventID    string  `json:"event_id"`
	Method     string  `json:"method"`
	Confidence float64 `json:"confidence"`
}

type persistedChangeSet struct {
	Scope             domain.ChangeScope     `json:"scope"`
	Entries           []persistedChangeEntry `json:"entries"`
	WorkspaceRevision uint64                 `json:"workspace_revision"`
	SealedAt          time.Time              `json:"sealed_at"`
}

type persistedChangeEntry struct {
	Kind         domain.ChangeKind `json:"kind"`
	Path         string            `json:"path"`
	PreviousPath string            `json:"previous_path,omitempty"`
	BeforeDigest string            `json:"before_digest,omitempty"`
	AfterDigest  string            `json:"after_digest,omitempty"`
	Metadata     map[string]string `json:"metadata"`
}

type persistedSummary struct {
	Text       string                      `json:"text"`
	Citations  []persistedEvidenceCitation `json:"citations"`
	Inferences []string                    `json:"inferences"`
}

type persistedEvidenceCitation struct {
	FirstCursor uint64             `json:"first_cursor,omitempty"`
	LastCursor  uint64             `json:"last_cursor,omitempty"`
	Artifact    *persistedArtifact `json:"artifact,omitempty"`
}

func persistRunObservationEvidence(store *ledger.Ledger, value RunObservationEvidence) (persistedRunEvidence, error) {
	events, err := persistEvents(store, value.Events)
	if err != nil {
		return persistedRunEvidence{}, err
	}
	gaps := persistGaps(value.Gaps)
	coverage, err := persistCoverageReferencing(value.Coverage, gaps)
	if err != nil {
		return persistedRunEvidence{}, err
	}
	return persistedRunEvidence{
		Required: append([]string(nil), value.Required...), FirstCursor: uint64(value.FirstCursor), LastCursor: uint64(value.LastCursor),
		Artifacts: persistArtifacts(value.Artifacts), Events: events, Metrics: persistMetrics(value.Metrics), Coverage: coverage,
		Gaps: gaps, StoppedAt: value.StoppedAt.UTC(), Failures: persistObserverFailures(value.Failures),
	}, nil
}

func (value persistedRunEvidence) restore(store *ledger.Ledger) (RunObservationEvidence, error) {
	events, err := restoreEvents(store, value.Events)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	artifacts, err := restoreArtifacts(value.Artifacts)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	metrics, err := restoreMetrics(value.Metrics)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	gaps, err := restoreGaps(value.Gaps)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	coverage, err := restoreCoverageReferencing(value.Coverage, value.Gaps)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	failures, err := restoreObserverFailures(value.Failures)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	return RunObservationEvidence{Required: append([]string(nil), value.Required...), FirstCursor: domain.ObservationCursor(value.FirstCursor), LastCursor: domain.ObservationCursor(value.LastCursor), Artifacts: artifacts, Events: events, Metrics: metrics, Coverage: coverage, Gaps: gaps, StoppedAt: value.StoppedAt.UTC(), Failures: failures}, nil
}

func persistTargetRunResult(store *ledger.Ledger, value ports.TargetRunResult) (persistedTargetRunResult, error) {
	events, err := persistEvents(store, value.NormalizedEvents)
	if err != nil {
		return persistedTargetRunResult{}, err
	}
	ids := make([]string, len(value.IncidentIDs))
	for i, id := range value.IncidentIDs {
		ids[i] = id.String()
	}
	gaps := persistGaps(value.Gaps)
	coverage, err := persistCoverageReferencing(value.Coverage, gaps)
	if err != nil {
		return persistedTargetRunResult{}, err
	}
	return persistedTargetRunResult{RunID: value.RunID.String(), Outcome: value.Outcome, FirstCursor: uint64(value.FirstCursor), LastCursor: uint64(value.LastCursor), Artifacts: persistArtifacts(value.RawArtifacts), Events: events, Metrics: persistMetrics(value.Metrics), Coverage: coverage, Gaps: gaps, TargetChanges: persistChangeSet(value.TargetChanges), IncidentIDs: ids, Summary: persistSummary(value.Summary), StoppedAt: value.StoppedAt.UTC()}, nil
}

func (value persistedTargetRunResult) restore(store *ledger.Ledger) (ports.TargetRunResult, error) {
	runID, err := domain.ParseTargetRunID(value.RunID)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	events, err := restoreEvents(store, value.Events)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	artifacts, err := restoreArtifacts(value.Artifacts)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	metrics, err := restoreMetrics(value.Metrics)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	gaps, err := restoreGaps(value.Gaps)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	coverage, err := restoreCoverageReferencing(value.Coverage, value.Gaps)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	changes, err := value.TargetChanges.restore()
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	incidents := make([]domain.IncidentID, len(value.IncidentIDs))
	for i, raw := range value.IncidentIDs {
		incidents[i], err = domain.ParseIncidentID(raw)
		if err != nil {
			return ports.TargetRunResult{}, err
		}
	}
	summary, err := value.Summary.restore()
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	return ports.TargetRunResult{RunID: runID, Outcome: value.Outcome, FirstCursor: domain.ObservationCursor(value.FirstCursor), LastCursor: domain.ObservationCursor(value.LastCursor), RawArtifacts: artifacts, NormalizedEvents: events, Metrics: metrics, Coverage: coverage, Gaps: gaps, TargetChanges: changes, IncidentIDs: incidents, Summary: summary, StoppedAt: value.StoppedAt.UTC()}, nil
}

func persistEvents(store *ledger.Ledger, values []domain.EventEnvelope) ([]persistedEvent, error) {
	result := make([]persistedEvent, len(values))
	for i, event := range values {
		params := event.Params()
		if params.SourceCursor == 0 {
			return nil, fmt.Errorf("event %d has no ledger cursor", i)
		}
		record, err := readExactLedgerRecord(store, ledger.Cursor(params.SourceCursor))
		if err != nil {
			return nil, err
		}
		result[i] = persistEvent(params, record)
		restored, err := result[i].restore(record)
		if err != nil || !reflect.DeepEqual(restored.Params(), params) {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.persist", fmt.Sprintf("events[%d]", i), "event does not match its exact ledger record", err)
		}
	}
	return result, nil
}

func restoreEvents(store *ledger.Ledger, values []persistedEvent) ([]domain.EventEnvelope, error) {
	result := make([]domain.EventEnvelope, len(values))
	for i, item := range values {
		record, err := readExactLedgerRecord(store, ledger.Cursor(item.LedgerCursor))
		if err != nil {
			return nil, err
		}
		result[i], err = item.restore(record)
		if err != nil {
			return nil, fmt.Errorf("restore event %d: %w", i, err)
		}
	}
	return result, nil
}

func readExactLedgerRecord(store *ledger.Ledger, cursor ledger.Cursor) (ledger.Record, error) {
	if store == nil || cursor == 0 {
		return ledger.Record{}, fmt.Errorf("ledger and positive cursor are required")
	}
	records, err := store.ReadAfter(cursor-1, 1)
	if err != nil {
		return ledger.Record{}, err
	}
	if len(records) != 1 || records[0].Cursor != cursor {
		return ledger.Record{}, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.restore", "ledger_cursor", "bound evidence record is missing", nil)
	}
	return records[0], nil
}

func persistEvent(p domain.EventEnvelopeParams, record ledger.Record) persistedEvent {
	links := make([]persistedCorrelationLink, len(p.CorrelatedWith))
	for i, link := range p.CorrelatedWith {
		links[i] = persistedCorrelationLink{EventID: link.EventID.String(), Method: link.Method, Confidence: link.Confidence}
	}
	return persistedEvent{LedgerCursor: uint64(record.Cursor), LedgerChainHash: hex.EncodeToString(record.ChainHash[:]), SchemaVersion: p.SchemaVersion, EventID: p.EventID.String(), Kind: p.Kind, ResearchSessionID: p.ResearchSessionID.String(), LeaseID: p.LeaseID.String(), AgentWorkspaceID: p.AgentWorkspaceID.String(), AgentGeneration: uint64(p.AgentGeneration), ExecID: p.ExecID.String(), TargetID: p.TargetID.String(), TargetGeneration: uint64(p.TargetGeneration), TargetRunID: p.TargetRunID.String(), TargetOperationID: p.TargetOperationID.String(), CorrelationID: p.CorrelationID.String(), CausationID: p.CausationID.String(), CorrelatedWith: links, TraceID: p.TraceID, SpanID: p.SpanID, Source: p.Source, SourceInstance: p.SourceInstance, SourceSequence: p.SourceSequence, CollectorID: p.CollectorID.String(), CollectorPlacement: p.CollectorPlacement, CoverageLevel: p.CoverageLevel, ObservedWallTime: p.ObservedWallTime.UTC(), ObservedMonotonicNanoseconds: int64(p.ObservedMonotonicTime), SubjectTime: p.SubjectTime.UTC(), SubjectClockDomain: p.SubjectClockDomain, ClockSyncEpoch: p.ClockSyncEpoch, HostBootID: p.HostBootID, ContainerID: p.ContainerID, CgroupID: p.CgroupID, ProcessID: p.ProcessID, ProcessStartTime: p.ProcessStartTime.UTC(), AndroidPID: p.AndroidPID, AndroidUID: p.AndroidUID, AndroidPackage: p.AndroidPackage, PolicyDigest: p.PolicyDigest.String(), CapabilityFingerprintDigest: p.CapabilityFingerprintDigest.String(), Sensitivity: p.Sensitivity, Completeness: p.Completeness, Confidence: p.Confidence, Origin: p.Origin}
}

func (value persistedEvent) restore(record ledger.Record) (domain.EventEnvelope, error) {
	hash, err := hex.DecodeString(value.LedgerChainHash)
	if err != nil || len(hash) != len(record.ChainHash) || !bytes.Equal(hash, record.ChainHash[:]) || uint64(record.Cursor) != value.LedgerCursor || record.EventID != value.EventID {
		return domain.EventEnvelope{}, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.restore", "event_binding", "ledger event identity or chain hash changed", err)
	}
	eventID, err := domain.ParseEventID(value.EventID)
	if err != nil {
		return domain.EventEnvelope{}, err
	}
	params := domain.EventEnvelopeParams{SchemaVersion: value.SchemaVersion, EventID: eventID, Kind: value.Kind, AgentGeneration: domain.AgentGeneration(value.AgentGeneration), TargetGeneration: domain.TargetGeneration(value.TargetGeneration), Source: value.Source, SourceInstance: value.SourceInstance, SourceSequence: value.SourceSequence, SourceCursor: domain.ObservationCursor(value.LedgerCursor), CollectorPlacement: value.CollectorPlacement, CoverageLevel: value.CoverageLevel, ObservedWallTime: value.ObservedWallTime.UTC(), ObservedMonotonicTime: time.Duration(value.ObservedMonotonicNanoseconds), SubjectTime: value.SubjectTime.UTC(), SubjectClockDomain: value.SubjectClockDomain, ClockSyncEpoch: value.ClockSyncEpoch, HostBootID: value.HostBootID, ContainerID: value.ContainerID, CgroupID: value.CgroupID, ProcessID: value.ProcessID, ProcessStartTime: value.ProcessStartTime.UTC(), AndroidPID: value.AndroidPID, AndroidUID: value.AndroidUID, AndroidPackage: value.AndroidPackage, Payload: append(json.RawMessage(nil), record.Payload...), Sensitivity: value.Sensitivity, Completeness: value.Completeness, Confidence: value.Confidence, Origin: value.Origin}
	if value.ResearchSessionID != "" {
		params.ResearchSessionID, err = domain.ParseResearchSessionID(value.ResearchSessionID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.LeaseID != "" {
		params.LeaseID, err = domain.ParseLeaseID(value.LeaseID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.AgentWorkspaceID != "" {
		params.AgentWorkspaceID, err = domain.ParseAgentWorkspaceID(value.AgentWorkspaceID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.ExecID != "" {
		params.ExecID, err = domain.ParseExecID(value.ExecID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.TargetID != "" {
		params.TargetID, err = domain.ParseTargetID(value.TargetID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.TargetRunID != "" {
		params.TargetRunID, err = domain.ParseTargetRunID(value.TargetRunID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.TargetOperationID != "" {
		params.TargetOperationID, err = domain.ParseTargetOperationID(value.TargetOperationID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.CorrelationID != "" {
		params.CorrelationID, err = domain.ParseCorrelationID(value.CorrelationID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.CausationID != "" {
		params.CausationID, err = domain.ParseEventID(value.CausationID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.CollectorID != "" {
		params.CollectorID, err = domain.ParseCollectorID(value.CollectorID)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.PolicyDigest != "" {
		params.PolicyDigest, err = domain.ParseDigest(value.PolicyDigest)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	if value.CapabilityFingerprintDigest != "" {
		params.CapabilityFingerprintDigest, err = domain.ParseDigest(value.CapabilityFingerprintDigest)
		if err != nil {
			return domain.EventEnvelope{}, err
		}
	}
	for _, link := range value.CorrelatedWith {
		id, parseErr := domain.ParseEventID(link.EventID)
		if parseErr != nil {
			return domain.EventEnvelope{}, parseErr
		}
		params.CorrelatedWith = append(params.CorrelatedWith, domain.CorrelationLink{EventID: id, Method: link.Method, Confidence: link.Confidence})
	}
	if err := requireLedgerEventBinding(record, params); err != nil {
		return domain.EventEnvelope{}, err
	}
	return domain.NewEventEnvelope(params)
}

func requireLedgerEventBinding(record ledger.Record, p domain.EventEnvelopeParams) error {
	if record.Kind != ledger.RecordObservation || record.EventID != p.EventID.String() || record.Identity.ResearchSessionID != p.ResearchSessionID.String() || record.Identity.LeaseID != p.LeaseID.String() || record.Identity.AgentWorkspaceID != p.AgentWorkspaceID.String() || record.Identity.AgentGeneration != uint64(p.AgentGeneration) || record.Identity.ExecID != p.ExecID.String() || record.Identity.TargetID != p.TargetID.String() || record.Identity.TargetGeneration != uint64(p.TargetGeneration) || record.Identity.TargetRunID != p.TargetRunID.String() || record.Identity.TargetOperationID != p.TargetOperationID.String() || record.Source != p.Source || record.SourceInstance != p.SourceInstance || record.SourceSequence != p.SourceSequence || record.ObservedWallUnixNano != p.ObservedWallTime.UnixNano() || record.PolicyDigest != p.PolicyDigest.String() || record.CapabilityDigest != p.CapabilityFingerprintDigest.String() {
		return domain.NewError(domain.CodeIntegrityViolation, "run_evidence.restore", "ledger_event", "ledger record differs from persisted event semantics", nil)
	}
	return nil
}

func persistArtifacts(values []domain.ArtifactReference) []persistedArtifact {
	result := make([]persistedArtifact, len(values))
	for i, value := range values {
		s := value.Spec()
		result[i] = persistedArtifact{Reference: s.Reference, Digest: s.Digest.String(), Size: s.Size, Role: s.Role, Sensitivity: s.Sensitivity}
	}
	return result
}
func restoreArtifacts(values []persistedArtifact) ([]domain.ArtifactReference, error) {
	result := make([]domain.ArtifactReference, len(values))
	for i, v := range values {
		d, e := domain.ParseDigest(v.Digest)
		if e != nil {
			return nil, e
		}
		result[i], e = domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: v.Reference, Digest: d, Size: v.Size, Role: v.Role, Sensitivity: v.Sensitivity})
		if e != nil {
			return nil, e
		}
	}
	return result, nil
}
func persistGaps(values []domain.Gap) []persistedGap {
	result := make([]persistedGap, len(values))
	for i, v := range values {
		s := v.Spec()
		result[i] = persistedGap{Kind: s.Kind, Source: s.Source, SourceInstance: s.SourceInstance, FirstSourceSequence: s.FirstSourceSequence, LastSourceSequence: s.LastSourceSequence, FirstCursor: uint64(s.FirstCursor), LastCursor: uint64(s.LastCursor), StartedAt: s.StartedAt.UTC(), EndedAt: s.EndedAt.UTC(), LostRecords: s.LostRecords, Reason: s.Reason}
	}
	return result
}
func restoreGaps(values []persistedGap) ([]domain.Gap, error) {
	result := make([]domain.Gap, len(values))
	for i, v := range values {
		var e error
		result[i], e = domain.NewGap(domain.GapSpec{Kind: v.Kind, Source: v.Source, SourceInstance: v.SourceInstance, FirstSourceSequence: v.FirstSourceSequence, LastSourceSequence: v.LastSourceSequence, FirstCursor: domain.ObservationCursor(v.FirstCursor), LastCursor: domain.ObservationCursor(v.LastCursor), StartedAt: v.StartedAt.UTC(), EndedAt: v.EndedAt.UTC(), LostRecords: v.LostRecords, Reason: v.Reason})
		if e != nil {
			return nil, e
		}
	}
	return result, nil
}
func persistCoverage(values []domain.CollectorCoverage) []persistedCoverage {
	result := make([]persistedCoverage, len(values))
	for i, v := range values {
		s := v.Spec()
		result[i] = persistedCoverage{CollectorID: s.CollectorID.String(), SignalFamily: s.SignalFamily, Placement: s.Placement, Level: s.Level, Status: s.Status, Required: s.Required, StartedAt: s.StartedAt.UTC(), EndedAt: s.EndedAt.UTC(), DroppedRecords: s.DroppedRecords, Gaps: persistGaps(s.Gaps)}
	}
	return result
}

// persistCoverageReferencing stores coverage gaps once in the enclosing gap
// collection and retains only content-addressed references from each coverage
// record. This prevents a valid maximum-sized failure result from being
// multiplied across the result, coverage, and incident checkpoint sections.
func persistCoverageReferencing(values []domain.CollectorCoverage, gaps []persistedGap) ([]persistedCoverage, error) {
	available := make(map[string]struct{}, len(gaps))
	for _, gap := range gaps {
		reference, err := persistedGapReference(gap)
		if err != nil {
			return nil, err
		}
		available[reference] = struct{}{}
	}
	result := persistCoverage(values)
	for index := range result {
		inline := result[index].Gaps
		result[index].Gaps = nil
		result[index].GapReferences = make([]string, len(inline))
		for gapIndex, gap := range inline {
			reference, err := persistedGapReference(gap)
			if err != nil {
				return nil, err
			}
			if _, found := available[reference]; !found {
				return nil, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.persist", fmt.Sprintf("coverage[%d].gaps[%d]", index, gapIndex), "coverage gap is absent from the enclosing evidence gaps", nil)
			}
			result[index].GapReferences[gapIndex] = reference
		}
	}
	return result, nil
}

func restoreCoverageReferencing(values []persistedCoverage, gaps []persistedGap) ([]domain.CollectorCoverage, error) {
	available := make(map[string]persistedGap, len(gaps))
	for _, gap := range gaps {
		reference, err := persistedGapReference(gap)
		if err != nil {
			return nil, err
		}
		available[reference] = gap
	}
	resolved := make([]persistedCoverage, len(values))
	for index, coverage := range values {
		if len(coverage.Gaps) != 0 {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.restore", fmt.Sprintf("coverage[%d].gaps", index), "referenced coverage contains duplicated inline gaps", nil)
		}
		coverage.Gaps = make([]persistedGap, len(coverage.GapReferences))
		for gapIndex, reference := range coverage.GapReferences {
			if _, err := domain.ParseDigest(reference); err != nil {
				return nil, fmt.Errorf("coverage %d gap reference %d: %w", index, gapIndex, err)
			}
			gap, found := available[reference]
			if !found {
				return nil, domain.NewError(domain.CodeIntegrityViolation, "run_evidence.restore", fmt.Sprintf("coverage[%d].gap_references[%d]", index, gapIndex), "coverage gap reference is absent from the enclosing evidence gaps", nil)
			}
			coverage.Gaps[gapIndex] = gap
		}
		coverage.GapReferences = nil
		resolved[index] = coverage
	}
	return restoreCoverage(resolved)
}

func persistedGapReference(value persistedGap) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode persisted gap reference: %w", err)
	}
	return domain.NewDigest(encoded).String(), nil
}

func restoreCoverage(values []persistedCoverage) ([]domain.CollectorCoverage, error) {
	result := make([]domain.CollectorCoverage, len(values))
	for i, v := range values {
		id, e := domain.ParseCollectorID(v.CollectorID)
		if e != nil {
			return nil, e
		}
		gaps, e := restoreGaps(v.Gaps)
		if e != nil {
			return nil, e
		}
		result[i], e = domain.NewCollectorCoverage(domain.CollectorCoverageSpec{CollectorID: id, SignalFamily: v.SignalFamily, Placement: v.Placement, Level: v.Level, Status: v.Status, Required: v.Required, StartedAt: v.StartedAt.UTC(), EndedAt: v.EndedAt.UTC(), DroppedRecords: v.DroppedRecords, Gaps: gaps})
		if e != nil {
			return nil, e
		}
	}
	return result, nil
}
func persistMetrics(values []domain.MetricSample) []persistedMetric {
	result := make([]persistedMetric, len(values))
	for i, v := range values {
		s := v.Spec()
		result[i] = persistedMetric{SubjectID: s.SubjectID.String(), SubjectKind: s.SubjectKind, Name: s.Name, Unit: s.Unit, Kind: s.Kind, Availability: s.Availability, CounterValue: s.CounterValue, NumericValue: s.NumericValue, CollectedAt: s.CollectedAt.UTC(), PublishedAt: s.PublishedAt.UTC(), Cursor: uint64(s.Cursor), Labels: cloneStringMap(s.Labels), ExecID: s.ExecID.String(), TargetRunID: s.TargetRunID.String()}
	}
	return result
}
func restoreMetrics(values []persistedMetric) ([]domain.MetricSample, error) {
	result := make([]domain.MetricSample, len(values))
	for i, v := range values {
		subject, e := domain.ParseSubjectID(v.SubjectID)
		if e != nil {
			return nil, e
		}
		var exec domain.ExecID
		if v.ExecID != "" {
			exec, e = domain.ParseExecID(v.ExecID)
			if e != nil {
				return nil, e
			}
		}
		var run domain.TargetRunID
		if v.TargetRunID != "" {
			run, e = domain.ParseTargetRunID(v.TargetRunID)
			if e != nil {
				return nil, e
			}
		}
		result[i], e = domain.NewMetricSample(domain.MetricSampleSpec{SubjectID: subject, SubjectKind: v.SubjectKind, Name: v.Name, Unit: v.Unit, Kind: v.Kind, Availability: v.Availability, CounterValue: v.CounterValue, NumericValue: v.NumericValue, CollectedAt: v.CollectedAt.UTC(), PublishedAt: v.PublishedAt.UTC(), Cursor: domain.ObservationCursor(v.Cursor), Labels: cloneStringMap(v.Labels), ExecID: exec, TargetRunID: run})
		if e != nil {
			return nil, e
		}
	}
	return result, nil
}
func persistObserverFailures(values []ObserverFailure) []persistedObserverFailure {
	result := make([]persistedObserverFailure, len(values))
	for i, v := range values {
		result[i] = persistedObserverFailure{CollectorID: v.CollectorID.String(), Family: v.Family, Required: v.Required, Reason: v.Reason}
	}
	return result
}
func restoreObserverFailures(values []persistedObserverFailure) ([]ObserverFailure, error) {
	result := make([]ObserverFailure, len(values))
	for i, v := range values {
		id, e := domain.ParseCollectorID(v.CollectorID)
		if e != nil {
			return nil, e
		}
		result[i] = ObserverFailure{CollectorID: id, Family: v.Family, Required: v.Required, Reason: v.Reason}
	}
	return result, nil
}

func persistChangeSet(value domain.ChangeSet) persistedChangeSet {
	result := persistedChangeSet{Scope: value.Scope(), WorkspaceRevision: uint64(value.WorkspaceRevision()), SealedAt: value.SealedAt().UTC()}
	for _, entry := range value.Entries() {
		s := entry.Spec()
		result.Entries = append(result.Entries, persistedChangeEntry{Kind: s.Kind, Path: s.Path, PreviousPath: s.PreviousPath, BeforeDigest: s.BeforeDigest.String(), AfterDigest: s.AfterDigest.String(), Metadata: cloneStringMap(s.Metadata)})
	}
	return result
}
func (value persistedChangeSet) restore() (domain.ChangeSet, error) {
	entries := make([]domain.ChangeEntry, len(value.Entries))
	for i, v := range value.Entries {
		var before, after domain.Digest
		var e error
		if v.BeforeDigest != "" {
			before, e = domain.ParseDigest(v.BeforeDigest)
			if e != nil {
				return domain.ChangeSet{}, e
			}
		}
		if v.AfterDigest != "" {
			after, e = domain.ParseDigest(v.AfterDigest)
			if e != nil {
				return domain.ChangeSet{}, e
			}
		}
		entries[i], e = domain.NewChangeEntry(domain.ChangeEntrySpec{Kind: v.Kind, Path: v.Path, PreviousPath: v.PreviousPath, BeforeDigest: before, AfterDigest: after, Metadata: cloneStringMap(v.Metadata)})
		if e != nil {
			return domain.ChangeSet{}, e
		}
	}
	return domain.NewChangeSet(value.Scope, entries, domain.Revision(value.WorkspaceRevision), value.SealedAt.UTC())
}
func persistSummary(value domain.DerivedSummary) persistedSummary {
	s := value.Spec()
	result := persistedSummary{Text: s.Text, Inferences: append([]string(nil), s.Inferences...)}
	for _, c := range s.Citations {
		item := persistedEvidenceCitation{FirstCursor: uint64(c.FirstCursor), LastCursor: uint64(c.LastCursor)}
		if c.Artifact.Spec().Reference != "" {
			a := persistArtifacts([]domain.ArtifactReference{c.Artifact})[0]
			item.Artifact = &a
		}
		result.Citations = append(result.Citations, item)
	}
	return result
}
func (value persistedSummary) restore() (domain.DerivedSummary, error) {
	citations := make([]domain.EvidenceCitation, len(value.Citations))
	for i, v := range value.Citations {
		citations[i] = domain.EvidenceCitation{FirstCursor: domain.ObservationCursor(v.FirstCursor), LastCursor: domain.ObservationCursor(v.LastCursor)}
		if v.Artifact != nil {
			a, e := restoreArtifacts([]persistedArtifact{*v.Artifact})
			if e != nil {
				return domain.DerivedSummary{}, e
			}
			citations[i].Artifact = a[0]
		}
	}
	return domain.NewDerivedSummary(domain.DerivedSummarySpec{Text: value.Text, Citations: citations, Inferences: append([]string(nil), value.Inferences...)})
}
