package rpc

import (
	"fmt"
	"math"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/wiremap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func researchSessionView(value application.ResearchSessionView) *worldv1.ResearchSessionView {
	return &worldv1.ResearchSessionView{
		Session:        researchSession(value.Session),
		Lease:          lease(value.Lease),
		AgentWorkspace: agentWorkspace(value.Agent),
		Targets:        mapValues(value.Targets, target),
		Incidents:      mapValues(value.Incidents, incident),
		Execs:          mapValues(value.Execs, execution),
	}
}

func execution(value application.ExecRecord) *worldv1.Exec {
	result := &worldv1.Exec{
		ExecId:                            value.ID,
		ResearchSessionId:                 value.SessionID,
		LeaseId:                           value.LeaseID,
		AgentWorkspaceId:                  value.AgentWorkspaceID,
		AgentGeneration:                   value.AgentGeneration,
		Kind:                              string(value.Kind),
		Executable:                        value.Executable,
		Argv:                              append([]string(nil), value.Argv...),
		WorkspaceRelativeWorkingDirectory: value.WorkingDirectory,
		State:                             string(value.State),
		Revision:                          value.Revision,
		Signal:                            value.Signal,
		IncidentIds:                       append([]string(nil), value.IncidentIDs...),
		CleanupConfirmed:                  value.CleanupConfirmed,
		Error:                             value.Error,
		CreatedAt:                         protobufTimestamp(value.CreatedAt),
		UpdatedAt:                         protobufTimestamp(value.UpdatedAt),
	}
	if value.ExitCode != nil {
		exitCode := int32(*value.ExitCode)
		result.ExitCode = &exitCode
	}
	return result
}

func researchSession(value application.SessionRecord) *worldv1.ResearchSession {
	return &worldv1.ResearchSession{
		ResearchSessionId: value.ID,
		OwnerSubject:      value.OwnerSubject,
		State:             string(value.State),
		Revision:          value.Revision,
		LeaseId:           value.LeaseID,
		AgentWorkspaceId:  value.AgentWorkspaceID,
		InputViewId:       value.InputViewID,
		PolicyDigest:      value.PolicyDigest,
		CapabilityDigest:  value.CapabilityDigest,
		CreatedAt:         protobufTimestamp(value.CreatedAt),
		UpdatedAt:         protobufTimestamp(value.UpdatedAt),
	}
}

func lease(value application.LeaseRecord) *worldv1.Lease {
	termination := leaseTermination(value.Termination)
	state := string(value.State)
	if termination != nil {
		state = termination.State
	}
	return &worldv1.Lease{
		LeaseId:           value.ID,
		ResearchSessionId: value.SessionID,
		AgentWorkspaceId:  value.AgentWorkspaceID,
		AgentGeneration:   value.AgentGeneration,
		InputViewId:       value.InputViewID,
		PolicyDigest:      value.PolicyDigest,
		CapabilityDigest:  value.CapabilityDigest,
		State:             state,
		Revision:          value.Revision,
		ExpiresAt:         protobufTimestamp(value.ExpiresAt),
		CreatedAt:         protobufTimestamp(value.CreatedAt),
		UpdatedAt:         protobufTimestamp(value.UpdatedAt),
		Termination:       termination,
	}
}

func leaseTermination(value application.LeaseTerminationRecord) *worldv1.LeaseTermination {
	if value.Empty() {
		return nil
	}
	return &worldv1.LeaseTermination{
		Kind:                   string(value.Kind),
		State:                  string(value.State),
		Reason:                 value.Reason,
		BeginIdempotencyKey:    value.BeginIdempotencyKey,
		BeginRequestDigest:     value.BeginRequestDigest,
		InitiatedLeaseRevision: value.InitiatedLeaseRevision,
		InitiatedAt:            protobufTimestamp(value.InitiatedAt),
		CompleteIdempotencyKey: value.CompleteIdempotencyKey,
		CompleteRequestDigest:  value.CompleteRequestDigest,
		CompletedAt:            protobufTimestamp(value.CompletedAt),
	}
}

func agentWorkspace(value application.AgentWorkspaceRecord) *worldv1.AgentWorkspace {
	return &worldv1.AgentWorkspace{
		AgentWorkspaceId:  value.ID,
		ResearchSessionId: value.SessionID,
		CurrentGeneration: value.CurrentGeneration,
		Revision:          value.Revision,
		CreatedAt:         protobufTimestamp(value.CreatedAt),
		UpdatedAt:         protobufTimestamp(value.UpdatedAt),
		Generations: mapValues(value.Generations, func(generation application.AgentGenerationRecord) *worldv1.AgentGeneration {
			return &worldv1.AgentGeneration{
				Generation:             generation.Generation,
				WorkspaceId:            generation.WorkspaceID,
				InputViewId:            generation.InputViewID,
				PolicyDigest:           generation.PolicyDigest,
				CapabilityDigest:       generation.CapabilityDigest,
				PreviousGeneration:     generation.Previous,
				RecoveryIncidentId:     generation.RecoveryIncident,
				State:                  string(generation.State),
				Revision:               generation.Revision,
				CreatedAt:              protobufTimestamp(generation.CreatedAt),
				UpdatedAt:              protobufTimestamp(generation.UpdatedAt),
				ProvisioningPlanDigest: generation.ProvisioningPlanDigest,
			}
		}),
	}
}

func target(value application.TargetRecord) *worldv1.Target {
	return wiremap.Target(value)
}

func targetRun(value application.TargetRunRecord) *worldv1.TargetRun {
	return wiremap.TargetRun(value)
}

func targetOperation(value application.TargetOperationRecord) *worldv1.TargetOperation {
	return wiremap.TargetOperation(value)
}

func incident(value application.IncidentRecord) *worldv1.Incident {
	return &worldv1.Incident{
		IncidentId:                 value.ID,
		Classification:             string(value.Classification),
		ResearchSessionId:          value.SessionID,
		LeaseId:                    value.LeaseID,
		AgentWorkspaceId:           value.AgentWorkspaceID,
		AgentGeneration:            value.AgentGeneration,
		ExecId:                     value.ExecID,
		TargetId:                   value.TargetID,
		TargetGeneration:           value.TargetGeneration,
		TargetRunId:                value.TargetRunID,
		Trigger:                    value.Trigger,
		LastKnownState:             value.LastKnownState,
		Cause:                      cause(value.Cause),
		HighWaterMetrics:           incidentMetrics(value.HighWaterMetrics),
		FirstRelevantCursor:        value.FirstRelevantCursor,
		LastRelevantCursor:         value.LastRelevantCursor,
		Coverage:                   incidentCoverage(value.Coverage),
		ObservationBundleId:        value.ObservationBundleID,
		Artifacts:                  incidentArtifacts(value.Artifacts),
		RecoveryActions:            append([]string(nil), value.RecoveryActions...),
		VisibilityAcknowledgements: append([]string(nil), value.VisibilityAcknowledgements...),
		State:                      string(value.State),
		Revision:                   value.Revision,
		OccurredAt:                 protobufTimestamp(value.OccurredAt),
		UpdatedAt:                  protobufTimestamp(value.UpdatedAt),
	}
}

func incidentMetrics(values []application.IncidentMetricRecord) []*worldv1.IncidentMetric {
	return mapValues(values, func(value application.IncidentMetricRecord) *worldv1.IncidentMetric {
		return &worldv1.IncidentMetric{
			SubjectId: value.SubjectID, SubjectKind: string(value.SubjectKind), Name: value.Name, Unit: value.Unit,
			Kind: string(value.Kind), Availability: string(value.Availability), CounterValue: cloneUint64(value.CounterValue),
			NumericValue: cloneFloat64(value.NumericValue), CollectedAt: protobufTimestamp(value.CollectedAt), PublishedAt: protobufTimestamp(value.PublishedAt),
			Cursor: value.Cursor, Labels: copyStringMap(value.Labels), ExecId: value.ExecID, TargetRunId: value.TargetRunID,
		}
	})
}

func incidentCoverage(values []application.IncidentCoverageRecord) []*worldv1.IncidentCoverage {
	return mapValues(values, func(value application.IncidentCoverageRecord) *worldv1.IncidentCoverage {
		return &worldv1.IncidentCoverage{
			CollectorId: value.CollectorID, SignalFamily: value.SignalFamily, Placement: string(value.Placement),
			Level: string(value.Level), Status: string(value.Status), Required: value.Required, StartedAt: protobufTimestamp(value.StartedAt),
			EndedAt: protobufTimestamp(value.EndedAt), DroppedRecords: value.DroppedRecords, Gaps: incidentGaps(value.Gaps),
		}
	})
}

func incidentGaps(values []application.IncidentGapRecord) []*worldv1.IncidentGap {
	return mapValues(values, func(value application.IncidentGapRecord) *worldv1.IncidentGap {
		return &worldv1.IncidentGap{
			Kind: string(value.Kind), Source: value.Source, SourceInstance: value.SourceInstance,
			FirstSourceSequence: value.FirstSourceSequence, LastSourceSequence: value.LastSourceSequence,
			FirstCursor: value.FirstCursor, LastCursor: value.LastCursor, StartedAt: protobufTimestamp(value.StartedAt),
			EndedAt: protobufTimestamp(value.EndedAt), LostRecords: value.LostRecords, Reason: value.Reason,
		}
	})
}

func incidentArtifacts(values []application.IncidentArtifactRecord) []*worldv1.ArtifactReference {
	return mapValues(values, func(value application.IncidentArtifactRecord) *worldv1.ArtifactReference {
		return &worldv1.ArtifactReference{
			Reference: value.Reference, Digest: value.Digest, Size: uint64(value.Size),
			Role: value.Role, Sensitivity: string(value.Sensitivity),
		}
	})
}

func mapValues[Input, Output any](values []Input, convert func(Input) Output) []Output {
	if values == nil {
		return nil
	}
	result := make([]Output, len(values))
	for index, value := range values {
		result[index] = convert(value)
	}
	return result
}

func applicationIncidentMetrics(values []*worldv1.IncidentMetric) ([]application.IncidentMetricRecord, error) {
	if err := requireMessages(values, "high_water_metrics"); err != nil {
		return nil, err
	}
	result := make([]application.IncidentMetricRecord, len(values))
	for index, value := range values {
		collectedAt, err := nativeTimestamp(value.CollectedAt, fmt.Sprintf("high_water_metrics[%d].collected_at", index), false)
		if err != nil {
			return nil, err
		}
		publishedAt, err := nativeTimestamp(value.PublishedAt, fmt.Sprintf("high_water_metrics[%d].published_at", index), false)
		if err != nil {
			return nil, err
		}
		result[index] = application.IncidentMetricRecord{
			SubjectID: value.SubjectId, SubjectKind: domain.SubjectKind(value.SubjectKind), Name: value.Name, Unit: value.Unit,
			Kind: domain.MetricKind(value.Kind), Availability: domain.MetricAvailability(value.Availability),
			CounterValue: cloneUint64(value.CounterValue), NumericValue: cloneFloat64(value.NumericValue),
			CollectedAt: collectedAt, PublishedAt: publishedAt, Cursor: value.Cursor,
			Labels: copyStringMap(value.Labels), ExecID: value.ExecId, TargetRunID: value.TargetRunId,
		}
	}
	return result, nil
}

func applicationIncidentCoverage(values []*worldv1.IncidentCoverage) ([]application.IncidentCoverageRecord, error) {
	if err := requireMessages(values, "coverage"); err != nil {
		return nil, err
	}
	result := make([]application.IncidentCoverageRecord, len(values))
	for index, value := range values {
		startedAt, err := nativeTimestamp(value.StartedAt, fmt.Sprintf("coverage[%d].started_at", index), false)
		if err != nil {
			return nil, err
		}
		endedAt, err := nativeTimestamp(value.EndedAt, fmt.Sprintf("coverage[%d].ended_at", index), false)
		if err != nil {
			return nil, err
		}
		gaps, err := applicationIncidentGaps(value.Gaps, index)
		if err != nil {
			return nil, err
		}
		result[index] = application.IncidentCoverageRecord{
			CollectorID: value.CollectorId, SignalFamily: value.SignalFamily, Placement: domain.CollectorPlacement(value.Placement),
			Level: domain.CoverageLevel(value.Level), Status: domain.CoverageStatus(value.Status), Required: value.Required,
			StartedAt: startedAt, EndedAt: endedAt, DroppedRecords: value.DroppedRecords, Gaps: gaps,
		}
	}
	return result, nil
}

func applicationIncidentGaps(values []*worldv1.IncidentGap, coverageIndex int) ([]application.IncidentGapRecord, error) {
	field := fmt.Sprintf("coverage[%d].gaps", coverageIndex)
	if err := requireMessages(values, field); err != nil {
		return nil, err
	}
	result := make([]application.IncidentGapRecord, len(values))
	for index, value := range values {
		startedAt, err := nativeTimestamp(value.StartedAt, fmt.Sprintf("%s[%d].started_at", field, index), false)
		if err != nil {
			return nil, err
		}
		endedAt, err := nativeTimestamp(value.EndedAt, fmt.Sprintf("%s[%d].ended_at", field, index), false)
		if err != nil {
			return nil, err
		}
		result[index] = application.IncidentGapRecord{
			Kind: domain.GapKind(value.Kind), Source: value.Source, SourceInstance: value.SourceInstance,
			FirstSourceSequence: value.FirstSourceSequence, LastSourceSequence: value.LastSourceSequence,
			FirstCursor: value.FirstCursor, LastCursor: value.LastCursor, StartedAt: startedAt,
			EndedAt: endedAt, LostRecords: value.LostRecords, Reason: value.Reason,
		}
	}
	return result, nil
}

func applicationIncidentArtifacts(values []*worldv1.ArtifactReference) ([]application.IncidentArtifactRecord, error) {
	if err := requireMessages(values, "artifacts"); err != nil {
		return nil, err
	}
	result := make([]application.IncidentArtifactRecord, len(values))
	for index, value := range values {
		if value.Size > math.MaxInt64 {
			field := fmt.Sprintf("artifacts[%d].size", index)
			return nil, domain.NewError(domain.CodeResourceExhausted, "rpc.decode", field, "exceeds the supported maximum", nil)
		}
		result[index] = application.IncidentArtifactRecord{
			Reference: value.Reference, Digest: value.Digest, Size: int64(value.Size), Role: value.Role,
			Sensitivity: domain.Sensitivity(value.Sensitivity),
		}
	}
	return result, nil
}

func requireMessages[Message any](values []*Message, field string) error {
	for index, value := range values {
		if value == nil {
			return domain.NewError(domain.CodeInvalidArgument, "rpc.decode", fmt.Sprintf("%s[%d]", field, index), "is required", nil)
		}
	}
	return nil
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cause(value application.CauseRecord) *worldv1.Cause {
	return &worldv1.Cause{Kind: string(value.Kind), Summary: value.Summary, Method: value.Method, Confidence: value.Confidence}
}

func applicationCause(value *worldv1.Cause) (application.CauseRecord, error) {
	if value == nil {
		return application.CauseRecord{}, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", "cause", "is required", nil)
	}
	return application.CauseRecord{Kind: domain.CauseKind(value.Kind), Summary: value.Summary, Method: value.Method, Confidence: value.Confidence}, nil
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
			return time.Time{}, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", field, "is required", nil)
		}
		return time.Time{}, nil
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", field, "is invalid", err)
	}
	return value.AsTime().UTC(), nil
}

func nativeDuration(value *durationpb.Duration, field string, required bool) (time.Duration, error) {
	if value == nil {
		if required {
			return 0, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", field, "is required", nil)
		}
		return 0, nil
	}
	if err := value.CheckValid(); err != nil {
		return 0, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", field, "is invalid", err)
	}
	result := value.AsDuration()
	roundTrip := durationpb.New(result)
	if roundTrip.Seconds != value.Seconds || roundTrip.Nanos != value.Nanos {
		return 0, domain.NewError(domain.CodeInvalidArgument, "rpc.decode", field, "exceeds native duration range", nil)
	}
	return result, nil
}
