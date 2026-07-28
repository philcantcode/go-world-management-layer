package orchestration

import (
	"fmt"
	"sort"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mapTargetOperation(value application.TargetOperationRecord) *worldv1.TargetOperation {
	return &worldv1.TargetOperation{
		TargetOperationId: value.ID, TargetRunId: value.RunID, Generation: value.Generation,
		Kind: string(value.Kind), CommandDisplay: value.CommandDisplay, ContentDigest: value.ContentDigest,
		State: string(value.State), Revision: value.Revision, CreatedAt: protobufTimestamp(value.CreatedAt), UpdatedAt: protobufTimestamp(value.UpdatedAt),
	}
}

func mapObservationBundle(value domain.ObservationBundle) *worldv1.ObservationBundle {
	spec := value.Spec()
	result := &worldv1.ObservationBundle{
		BundleId: value.ID().String(), TargetRunId: spec.TargetRunID.String(), TargetId: spec.TargetID.String(),
		TargetGeneration: uint64(spec.TargetGeneration), AgentWorkspaceId: spec.AgentWorkspaceID.String(),
		AgentGeneration: uint64(spec.AgentGeneration), FirstCursor: uint64(spec.FirstCursor), LastCursor: uint64(spec.LastCursor),
		State: string(value.State()), Revision: uint64(value.Revision()), CreatedAt: protobufTimestamp(spec.CreatedAt), SealedAt: protobufTimestamp(value.SealedAt()),
		TargetChanges: mapChangeSet(spec.TargetChanges), Summary: mapDerivedSummary(spec.Summary),
	}
	for _, artifact := range spec.RawArtifacts {
		result.RawArtifacts = append(result.RawArtifacts, mapArtifact(artifact))
	}
	for _, event := range spec.NormalizedEvents {
		result.NormalizedEvents = append(result.NormalizedEvents, mapEvent(event))
	}
	for _, metric := range spec.Metrics {
		result.Metrics = append(result.Metrics, mapDomainMetric(metric))
	}
	for _, coverage := range spec.Coverage {
		result.Coverage = append(result.Coverage, mapDomainCoverage(coverage))
	}
	for _, gap := range spec.Gaps {
		result.Gaps = append(result.Gaps, mapDomainGap(gap))
	}
	for _, incidentID := range spec.IncidentIDs {
		result.IncidentIds = append(result.IncidentIds, incidentID.String())
	}
	sort.Strings(result.IncidentIds)
	return result
}

func mapArtifact(value domain.ArtifactReference) *worldv1.ArtifactReference {
	spec := value.Spec()
	return &worldv1.ArtifactReference{Reference: spec.Reference, Digest: spec.Digest.String(), Size: uint64(spec.Size), Role: spec.Role, Sensitivity: string(spec.Sensitivity)}
}

func mapEvent(value domain.EventEnvelope) *worldv1.ObservationRecord {
	params := value.Params()
	result := &worldv1.ObservationRecord{
		Cursor: uint64(params.SourceCursor), Kind: params.Kind, EventId: params.EventID.String(),
		Identity: &worldv1.ObservationIdentity{
			ResearchSessionId: params.ResearchSessionID.String(), LeaseId: params.LeaseID.String(),
			AgentWorkspaceId: params.AgentWorkspaceID.String(), AgentGeneration: uint64(params.AgentGeneration),
			ExecId: params.ExecID.String(), TargetId: params.TargetID.String(), TargetGeneration: uint64(params.TargetGeneration),
			TargetRunId: params.TargetRunID.String(), TargetOperationId: params.TargetOperationID.String(),
		},
		Source: params.Source, SourceInstance: params.SourceInstance, SourceSequence: params.SourceSequence,
		HasSourceSequence: params.SourceSequence > 0, ObservedAt: protobufTimestamp(params.ObservedWallTime),
		PolicyDigest: params.PolicyDigest.String(), CapabilityDigest: params.CapabilityFingerprintDigest.String(),
		Payload: append([]byte(nil), params.Payload...),
		Causal:  &worldv1.CausalContext{CausationId: params.CausationID.String(), CorrelationId: params.CorrelationID.String()},
	}
	return result
}

func mapDomainMetric(value domain.MetricSample) *worldv1.MetricSample {
	spec := value.Spec()
	result := &worldv1.MetricSample{
		Cursor: uint64(spec.Cursor), SubjectId: spec.SubjectID.String(), Name: spec.Name,
		State: string(spec.Availability), CollectedAt: protobufTimestamp(spec.CollectedAt),
		Detail: fmt.Sprintf("kind=%s unit=%s", spec.Kind, spec.Unit),
	}
	if spec.NumericValue != nil {
		numeric := *spec.NumericValue
		result.Value = &numeric
	} else if spec.CounterValue != nil {
		numeric := float64(*spec.CounterValue)
		result.Value = &numeric
	}
	return result
}

func mapDomainCoverage(value domain.CollectorCoverage) *worldv1.CollectorCoverage {
	spec := value.Spec()
	result := &worldv1.CollectorCoverage{
		CollectorId: spec.CollectorID.String(), SignalFamily: spec.SignalFamily,
		Placement: string(spec.Placement), Level: string(spec.Level), Status: string(spec.Status),
		Required: spec.Required, DroppedRecords: spec.DroppedRecords,
	}
	if len(spec.Gaps) > 0 {
		result.Gap = mapDomainGap(spec.Gaps[0])
	}
	return result
}

func mapDomainGap(value domain.Gap) *worldv1.Gap {
	spec := value.Spec()
	return &worldv1.Gap{
		Cause: string(spec.Kind), Source: spec.Source, SourceInstance: spec.SourceInstance,
		FromSequence: spec.FirstSourceSequence, ThroughSequence: spec.LastSourceSequence,
		FromCursor: uint64(spec.FirstCursor), ThroughCursor: uint64(spec.LastCursor), Detail: spec.Reason,
	}
}

func mapChangeSet(value domain.ChangeSet) *worldv1.ChangeSet {
	result := &worldv1.ChangeSet{Scope: string(value.Scope()), WorkspaceRevision: uint64(value.WorkspaceRevision()), SealedAt: protobufTimestamp(value.SealedAt())}
	for _, entry := range value.Entries() {
		spec := entry.Spec()
		result.Changes = append(result.Changes, &worldv1.Change{
			Kind: string(spec.Kind), WorkspaceRelativePath: spec.Path, PreviousWorkspaceRelativePath: spec.PreviousPath,
			BeforeDigest: spec.BeforeDigest.String(), AfterDigest: spec.AfterDigest.String(), Metadata: cloneStringMap(spec.Metadata),
		})
	}
	return result
}

func mapDerivedSummary(value domain.DerivedSummary) *worldv1.DerivedSummary {
	spec := value.Spec()
	result := &worldv1.DerivedSummary{Text: spec.Text, Inferences: append([]string(nil), spec.Inferences...)}
	for _, citation := range spec.Citations {
		mapped := &worldv1.EvidenceCitation{FirstCursor: uint64(citation.FirstCursor), LastCursor: uint64(citation.LastCursor)}
		if citation.Artifact.Spec().Reference != "" {
			mapped.Artifact = mapArtifact(citation.Artifact)
		}
		result.Citations = append(result.Citations, mapped)
	}
	return result
}

func protobufTimestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	result := timestamppb.New(value.UTC())
	if result.CheckValid() != nil {
		return nil
	}
	return result
}

func nativeTimestamp(value *timestamppb.Timestamp, field string, required bool) (time.Time, error) {
	if value == nil {
		if required {
			return time.Time{}, fmt.Errorf("%s is required", field)
		}
		return time.Time{}, nil
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("%s is invalid: %w", field, err)
	}
	return value.AsTime().UTC(), nil
}

func nativeDuration(value *durationpb.Duration, field string, required bool) (time.Duration, error) {
	if value == nil {
		if required {
			return 0, fmt.Errorf("%s is required", field)
		}
		return 0, nil
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("%s is invalid: %w", field, err)
	}
	result := value.AsDuration()
	roundTrip := durationpb.New(result)
	if roundTrip.Seconds != value.Seconds || roundTrip.Nanos != value.Nanos {
		return 0, fmt.Errorf("%s exceeds native duration range", field)
	}
	return result, nil
}

func protobufDuration(value time.Duration) *durationpb.Duration {
	if value == 0 {
		return nil
	}
	result := durationpb.New(value)
	if result.CheckValid() != nil {
		return nil
	}
	return result
}

func mapChangeSetFromSeal(value domain.ChangeSet) *worldv1.ChangeSet { return mapChangeSet(value) }

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
