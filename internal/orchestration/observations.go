package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/observation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type compiledFilter struct {
	leaseID        string
	targetIDs      map[string]struct{}
	targetRunIDs   map[string]struct{}
	subjectIDs     map[string]struct{}
	signalFamilies map[string]struct{}
	recordKinds    map[ledger.RecordKind]struct{}
}

func (s *Service) GetLiveSnapshot(ctx context.Context, request *worldv1.GetLiveSnapshotRequest) (*worldv1.LiveSnapshot, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	filter, err := s.compileFilter(ctx, request.Filter)
	if err != nil {
		return nil, err
	}
	records, err := s.ledger.ReadAfter(0, 0)
	if err != nil {
		return nil, err
	}
	events := make([]observation.Event, 0, len(records))
	for _, record := range records {
		event, mapErr := snapshotEvent(filter, record)
		if mapErr != nil {
			return nil, mapErr
		}
		events = append(events, event)
	}
	reduced, err := observation.Reduce(events)
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "reduce durable observation ledger: %v", err)
	}
	snapshot := projectSnapshot(filter, reduced)
	sortSnapshot(snapshot)
	return snapshot, nil
}

func (s *Service) SubscribeObservations(request *worldv1.SubscribeObservationsRequest, stream worldv1.WorldService_SubscribeObservationsServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	filter, err := s.compileFilter(stream.Context(), request.Filter)
	if err != nil {
		return err
	}
	subscription, err := s.ledger.Subscribe(ledger.Cursor(request.AfterCursor), s.streamBuffer)
	if err != nil {
		return subscriptionError(err)
	}
	defer subscription.Close()
	for {
		delivery, err := subscription.Next(stream.Context())
		if err != nil {
			return streamEnd(err)
		}
		if delivery.Gap != nil {
			if err := stream.Send(&worldv1.ObservationRecord{Kind: "gap", Cursor: uint64(delivery.Gap.ThroughCursor), Gap: mapGap(*delivery.Gap)}); err != nil {
				return err
			}
			continue
		}
		if delivery.Record == nil || !filter.matches(*delivery.Record) {
			continue
		}
		if err := stream.Send(mapObservationRecord(*delivery.Record)); err != nil {
			return err
		}
	}
}

func (s *Service) SubscribeMetrics(request *worldv1.SubscribeMetricsRequest, stream worldv1.WorldService_SubscribeMetricsServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	resolution, err := nativeDuration(request.Resolution, "resolution", false)
	if err != nil || resolution < 0 || resolution > time.Minute {
		return status.Error(codes.InvalidArgument, "resolution must be between zero and one minute")
	}
	filter, err := s.compileFilter(stream.Context(), request.Filter)
	if err != nil {
		return err
	}
	subscription, err := s.ledger.Subscribe(ledger.Cursor(request.AfterCursor), s.streamBuffer)
	if err != nil {
		return subscriptionError(err)
	}
	defer subscription.Close()
	var nextSend time.Time
	for {
		delivery, err := subscription.Next(stream.Context())
		if err != nil {
			return streamEnd(err)
		}
		var sample *worldv1.MetricSample
		if delivery.Gap != nil {
			sample = &worldv1.MetricSample{Cursor: uint64(delivery.Gap.ThroughCursor), State: "gap", Gap: mapGap(*delivery.Gap), Detail: delivery.Gap.Detail}
		} else if delivery.Record != nil && delivery.Record.Kind == ledger.RecordMetric && filter.matches(*delivery.Record) {
			sample, err = metricFromRecord(*delivery.Record)
			if err != nil {
				return err
			}
		} else {
			continue
		}
		if resolution > 0 && !nextSend.IsZero() {
			if err := waitUntil(stream.Context(), nextSend); err != nil {
				return streamEnd(err)
			}
		}
		if err := stream.Send(sample); err != nil {
			return err
		}
		nextSend = time.Now().Add(resolution)
	}
}

func (s *Service) compileFilter(ctx context.Context, value *worldv1.ObservationFilter) (compiledFilter, error) {
	if value == nil || value.LeaseId == "" {
		return compiledFilter{}, status.Error(codes.InvalidArgument, "filter.lease_id is required")
	}
	if _, err := domain.ParseLeaseID(value.LeaseId); err != nil {
		return compiledFilter{}, status.Error(codes.InvalidArgument, "filter.lease_id is invalid")
	}
	if err := s.authorize(ctx, "", application.AuthorizationRequest{LeaseID: value.LeaseId}); err != nil {
		return compiledFilter{}, err
	}
	if len(value.TargetIds) > maxFilterValues || len(value.TargetRunIds) > maxFilterValues || len(value.SubjectIds) > maxFilterValues || len(value.SignalFamilies) > maxFilterValues || len(value.RecordKinds) > maxFilterValues {
		return compiledFilter{}, status.Errorf(codes.ResourceExhausted, "each filter list is limited to %d values", maxFilterValues)
	}
	filter := compiledFilter{
		leaseID: value.LeaseId, targetIDs: stringSet(value.TargetIds), targetRunIDs: stringSet(value.TargetRunIds),
		subjectIDs: stringSet(value.SubjectIds), signalFamilies: stringSet(value.SignalFamilies),
		recordKinds: make(map[ledger.RecordKind]struct{}, len(value.RecordKinds)),
	}
	for targetID := range filter.targetIDs {
		if _, err := domain.ParseTargetID(targetID); err != nil {
			return compiledFilter{}, status.Errorf(codes.InvalidArgument, "invalid target_id %q", targetID)
		}
		target, err := s.core.GetTarget(ctx, targetID)
		if err != nil || target.LeaseID != value.LeaseId {
			return compiledFilter{}, status.Error(codes.PermissionDenied, "target filter is outside the lease scope")
		}
	}
	if len(filter.targetRunIDs) > 0 && len(filter.targetIDs) == 0 {
		return compiledFilter{}, status.Error(codes.InvalidArgument, "target_run_ids require target_ids so run scope can be verified")
	}
	for runID := range filter.targetRunIDs {
		if _, err := domain.ParseTargetRunID(runID); err != nil {
			return compiledFilter{}, status.Errorf(codes.InvalidArgument, "invalid target_run_id %q", runID)
		}
		found := false
		for targetID := range filter.targetIDs {
			target, err := s.core.GetTarget(ctx, targetID)
			if err != nil {
				return compiledFilter{}, err
			}
			if _, err := targetRun(target, runID); err == nil {
				found = true
				break
			}
		}
		if !found {
			return compiledFilter{}, status.Error(codes.PermissionDenied, "target run filter is outside the selected targets")
		}
	}
	for subjectID := range filter.subjectIDs {
		if _, err := domain.ParseSubjectID(subjectID); err != nil {
			return compiledFilter{}, status.Errorf(codes.InvalidArgument, "invalid subject_id %q", subjectID)
		}
	}
	for family := range filter.signalFamilies {
		if strings.TrimSpace(family) == "" || len(family) > 256 {
			return compiledFilter{}, status.Error(codes.InvalidArgument, "signal families must be non-blank and at most 256 bytes")
		}
	}
	for _, kind := range value.RecordKinds {
		mapped, ok := parseRecordKind(kind)
		if !ok || mapped == ledger.RecordControl {
			return compiledFilter{}, status.Errorf(codes.InvalidArgument, "unsupported observation record kind %q", kind)
		}
		filter.recordKinds[mapped] = struct{}{}
	}
	return filter, nil
}

func (f compiledFilter) matches(record ledger.Record) bool {
	if !f.matchesScope(record) {
		return false
	}
	if len(f.subjectIDs) > 0 {
		if _, ok := f.subjectIDs[record.SubjectID]; !ok {
			return false
		}
	}
	if len(f.signalFamilies) > 0 {
		if _, ok := f.signalFamilies[record.SignalFamily]; !ok {
			return false
		}
	}
	if len(f.recordKinds) > 0 {
		if _, ok := f.recordKinds[record.Kind]; !ok {
			return false
		}
	}
	return true
}

func (f compiledFilter) matchesScope(record ledger.Record) bool {
	if record.Source == stateSource || record.Kind == ledger.RecordControl || record.Identity.LeaseID != f.leaseID {
		return false
	}
	if len(f.targetIDs) > 0 {
		if _, ok := f.targetIDs[record.Identity.TargetID]; !ok {
			return false
		}
	}
	if len(f.targetRunIDs) > 0 {
		if _, ok := f.targetRunIDs[record.Identity.TargetRunID]; !ok {
			return false
		}
	}
	return true
}

func (f compiledFilter) allowsProjection(kind ledger.RecordKind, subjectID, signalFamily string) bool {
	if len(f.subjectIDs) > 0 {
		if _, ok := f.subjectIDs[subjectID]; !ok {
			return false
		}
	}
	if len(f.signalFamilies) > 0 {
		if _, ok := f.signalFamilies[signalFamily]; !ok {
			return false
		}
	}
	if len(f.recordKinds) > 0 {
		if _, ok := f.recordKinds[kind]; !ok {
			return false
		}
	}
	return true
}

func snapshotEvent(filter compiledFilter, record ledger.Record) (observation.Event, error) {
	event := observation.Event{Cursor: domain.ObservationCursor(record.Cursor), Kind: observation.EventCheckpoint}
	if !filter.matchesScope(record) {
		return event, nil
	}
	leaseID, err := domain.ParseLeaseID(record.Identity.LeaseID)
	if err != nil {
		return event, corruptProjection(record, "lease identity is invalid")
	}
	event.LeaseID, event.SignalFamily = leaseID, record.SignalFamily
	subjectID, err := optionalSubjectID(record.SubjectID)
	if err != nil {
		return event, corruptProjection(record, "subject identity is invalid")
	}
	event.SubjectID = subjectID
	switch record.Kind {
	case ledger.RecordTopology:
		var value worldv1.Subject
		if err := decodeProjection(record, &value); err != nil {
			return event, err
		}
		if value.SubjectId == "" || value.SubjectId != record.SubjectID {
			return event, corruptProjection(record, "topology subject_id is missing or disagrees with ledger identity")
		}
		if value.LeaseId != "" && value.LeaseId != record.Identity.LeaseID || value.SignalFamily != "" && value.SignalFamily != record.SignalFamily {
			return event, corruptProjection(record, "topology payload scope disagrees with ledger identity")
		}
		parentID, err := optionalSubjectID(value.ParentSubjectId)
		if err != nil {
			return event, corruptProjection(record, "topology parent_subject_id is invalid")
		}
		event.Kind = observation.EventSubjectUpsert
		event.Subject = &observation.Subject{ID: subjectID, ParentID: parentID, LeaseID: leaseID, Kind: domain.SubjectKind(value.Kind), SignalFamily: record.SignalFamily, Labels: cloneStringMap(value.Labels)}
	case ledger.RecordMetric:
		value, err := metricFromRecord(record)
		if err != nil {
			return event, err
		}
		state, err := observationMetricState(value)
		if err != nil {
			return event, corruptProjection(record, err.Error())
		}
		event.Kind, event.MetricName, event.Metric = observation.EventMetricSet, value.Name, &state
	case ledger.RecordCoverage:
		var value worldv1.CollectorCoverage
		if err := decodeProjection(record, &value); err != nil {
			return event, err
		}
		if value.SubjectId != "" && value.SubjectId != record.SubjectID || value.SignalFamily != "" && value.SignalFamily != record.SignalFamily {
			return event, corruptProjection(record, "coverage payload scope disagrees with ledger identity")
		}
		collectorID, err := domain.ParseCollectorID(value.CollectorId)
		if err != nil {
			return event, corruptProjection(record, "coverage collector_id is invalid")
		}
		gap, err := ledgerGap(value.Gap)
		if err != nil {
			return event, corruptProjection(record, err.Error())
		}
		event.Kind = observation.EventCoverageSet
		event.Coverage = &observation.CoverageView{
			LeaseID: leaseID, CollectorID: collectorID, SubjectID: subjectID, SignalFamily: record.SignalFamily,
			Placement: domain.CollectorPlacement(value.Placement), Level: domain.CoverageLevel(value.Level), Status: domain.CoverageStatus(value.Status),
			Required: value.Required, Dropped: value.DroppedRecords, Gap: gap,
		}
	case ledger.RecordIncident:
		var value worldv1.IncidentSummary
		if err := decodeProjection(record, &value); err != nil {
			return event, err
		}
		if value.LeaseId != "" && value.LeaseId != record.Identity.LeaseID || value.SubjectId != "" && value.SubjectId != record.SubjectID || value.SignalFamily != "" && value.SignalFamily != record.SignalFamily {
			return event, corruptProjection(record, "incident payload scope disagrees with ledger identity")
		}
		incidentID, err := domain.ParseIncidentID(value.IncidentId)
		if err != nil {
			return event, corruptProjection(record, "incident_id is invalid")
		}
		event.Kind = observation.EventIncidentSet
		event.Incident = &observation.IncidentView{ID: incidentID, LeaseID: leaseID, SubjectID: subjectID, SignalFamily: record.SignalFamily, State: domain.IncidentState(value.State), Summary: value.Summary}
	case ledger.RecordPressure:
		var value worldv1.PressureTransition
		if err := decodeProjection(record, &value); err != nil {
			return event, err
		}
		if value.LeaseId != "" && value.LeaseId != record.Identity.LeaseID || value.SubjectId != "" && value.SubjectId != record.SubjectID || value.SignalFamily != "" && value.SignalFamily != record.SignalFamily {
			return event, corruptProjection(record, "pressure payload scope disagrees with ledger identity")
		}
		level, ok := pressureLevel(value.Level)
		if !ok {
			return event, corruptProjection(record, "pressure level is invalid")
		}
		event.Kind = observation.EventPressureSet
		event.Pressure = &observation.PressureView{LeaseID: leaseID, SubjectID: subjectID, SignalFamily: record.SignalFamily, Resource: value.Resource, Level: level, Value: value.Value, Detail: value.Detail}
	}
	return event, nil
}

func projectSnapshot(filter compiledFilter, snapshot observation.LiveSnapshot) *worldv1.LiveSnapshot {
	result := &worldv1.LiveSnapshot{Cursor: uint64(snapshot.Cursor)}
	for _, value := range snapshot.Subjects {
		if !filter.allowsProjection(ledger.RecordTopology, value.ID.String(), value.SignalFamily) {
			continue
		}
		result.Subjects = append(result.Subjects, &worldv1.Subject{
			SubjectId: value.ID.String(), ParentSubjectId: optionalSubjectString(value.ParentID), LeaseId: value.LeaseID.String(),
			Kind: string(value.Kind), SignalFamily: value.SignalFamily, Labels: cloneStringMap(value.Labels),
		})
	}
	for _, value := range snapshot.Metrics {
		if !filter.allowsProjection(ledger.RecordMetric, value.SubjectID.String(), value.SignalFamily) {
			continue
		}
		result.Metrics = append(result.Metrics, mapReducedMetric(value))
	}
	for _, value := range snapshot.Coverage {
		if !filter.allowsProjection(ledger.RecordCoverage, optionalSubjectString(value.SubjectID), value.SignalFamily) {
			continue
		}
		result.Coverage = append(result.Coverage, &worldv1.CollectorCoverage{
			CollectorId: value.CollectorID.String(), SubjectId: optionalSubjectString(value.SubjectID), SignalFamily: value.SignalFamily,
			Placement: string(value.Placement), Level: string(value.Level), Status: string(value.Status), Required: value.Required,
			DroppedRecords: value.Dropped, Gap: mapOptionalGap(value.Gap), UpdatedCursor: uint64(value.UpdatedCursor),
		})
	}
	for _, value := range snapshot.Incidents {
		if !filter.allowsProjection(ledger.RecordIncident, optionalSubjectString(value.SubjectID), value.SignalFamily) {
			continue
		}
		result.Incidents = append(result.Incidents, &worldv1.IncidentSummary{
			IncidentId: value.ID.String(), LeaseId: value.LeaseID.String(), SubjectId: optionalSubjectString(value.SubjectID),
			SignalFamily: value.SignalFamily, State: string(value.State), Summary: value.Summary, UpdatedCursor: uint64(value.UpdatedCursor),
		})
	}
	for _, value := range snapshot.Pressure {
		if !filter.allowsProjection(ledger.RecordPressure, optionalSubjectString(value.SubjectID), value.SignalFamily) {
			continue
		}
		result.Pressure = append(result.Pressure, &worldv1.PressureTransition{
			LeaseId: value.LeaseID.String(), SubjectId: optionalSubjectString(value.SubjectID), SignalFamily: value.SignalFamily,
			Resource: value.Resource, Level: pressureLevelName(value.Level), Value: value.Value, Detail: value.Detail, Cursor: uint64(value.UpdatedCursor),
		})
	}
	return result
}

func observationMetricState(value *worldv1.MetricSample) (observation.MetricState, error) {
	if value == nil {
		return observation.MetricState{}, fmt.Errorf("metric is required")
	}
	collectedAt, err := nativeTimestamp(value.CollectedAt, "metric.collected_at", false)
	if err != nil {
		return observation.MetricState{}, err
	}
	sampleAge, err := nativeDuration(value.SampleAge, "metric.sample_age", false)
	if err != nil {
		return observation.MetricState{}, err
	}
	switch value.State {
	case "present":
		if value.Value == nil {
			return observation.MetricState{}, fmt.Errorf("present metric has no value")
		}
		return observation.Present(*value.Value, collectedAt), nil
	case "missing":
		return observation.Missing(value.Detail), nil
	case "unsupported":
		return observation.Unsupported(value.Detail), nil
	case "stale":
		return observation.Stale(value.LastValue, sampleAge, value.Detail), nil
	case "gap":
		gap, err := ledgerGap(value.Gap)
		if err != nil || gap == nil {
			return observation.MetricState{}, fmt.Errorf("gap metric has invalid gap metadata: %w", err)
		}
		return observation.GapState(*gap, value.Detail), nil
	default:
		return observation.MetricState{}, fmt.Errorf("metric state %q is invalid", value.State)
	}
}

func mapReducedMetric(value observation.MetricView) *worldv1.MetricSample {
	state := value.State
	return &worldv1.MetricSample{
		Cursor: uint64(state.UpdatedCursor), LeaseId: value.LeaseID.String(), SubjectId: value.SubjectID.String(),
		SignalFamily: value.SignalFamily, Name: value.Name, State: metricStateName(state.Kind),
		Value: cloneFloat64(state.Value), LastValue: cloneFloat64(state.LastValue), CollectedAt: protobufTimestamp(state.CollectedAt),
		SampleAge: protobufDuration(state.SampleAge), Gap: mapOptionalGap(state.Gap), Detail: state.Detail,
	}
}

func metricStateName(value observation.MetricStateKind) string {
	switch value {
	case observation.MetricPresent:
		return "present"
	case observation.MetricMissing:
		return "missing"
	case observation.MetricUnsupported:
		return "unsupported"
	case observation.MetricStale:
		return "stale"
	case observation.MetricGap:
		return "gap"
	default:
		return "unknown"
	}
}

func optionalSubjectID(value string) (domain.SubjectID, error) {
	if value == "" {
		return domain.SubjectID{}, nil
	}
	return domain.ParseSubjectID(value)
}

func optionalSubjectString(value domain.SubjectID) string {
	if value.IsZero() {
		return ""
	}
	return value.String()
}

func ledgerGap(value *worldv1.Gap) (*ledger.Gap, error) {
	if value == nil {
		return nil, nil
	}
	cause, ok := parseGapCause(value.Cause)
	if !ok {
		return nil, fmt.Errorf("gap cause %q is invalid", value.Cause)
	}
	return &ledger.Gap{
		Cause: cause, Source: value.Source, SourceInstance: value.SourceInstance,
		FromSequence: value.FromSequence, ThroughSequence: value.ThroughSequence,
		FromCursor: ledger.Cursor(value.FromCursor), ThroughCursor: ledger.Cursor(value.ThroughCursor), Detail: value.Detail,
	}, nil
}

func parseGapCause(value string) (ledger.GapCause, bool) {
	for cause := ledger.GapUnknown; cause <= ledger.GapSegmentRepair; cause++ {
		if gapCauseName(cause) == value {
			return cause, true
		}
	}
	return 0, false
}

func mapOptionalGap(value *ledger.Gap) *worldv1.Gap {
	if value == nil {
		return nil
	}
	return mapGap(*value)
}

func pressureLevel(value string) (observation.PressureLevel, bool) {
	switch value {
	case "normal":
		return observation.PressureNormal, true
	case "observed":
		return observation.PressureObserved, true
	case "admission_stopped":
		return observation.PressureAdmissionStopped, true
	case "shedding":
		return observation.PressureShedding, true
	case "critical":
		return observation.PressureCritical, true
	default:
		return 0, false
	}
}

func pressureLevelName(value observation.PressureLevel) string {
	switch value {
	case observation.PressureNormal:
		return "normal"
	case observation.PressureObserved:
		return "observed"
	case observation.PressureAdmissionStopped:
		return "admission_stopped"
	case observation.PressureShedding:
		return "shedding"
	case observation.PressureCritical:
		return "critical"
	default:
		return "unknown"
	}
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func mapObservationRecord(record ledger.Record) *worldv1.ObservationRecord {
	mapped := &worldv1.ObservationRecord{
		Cursor: uint64(record.Cursor), Kind: recordKindName(record.Kind), EventId: record.EventID,
		Identity: &worldv1.ObservationIdentity{
			ResearchSessionId: record.Identity.ResearchSessionID, LeaseId: record.Identity.LeaseID,
			AgentWorkspaceId: record.Identity.AgentWorkspaceID, AgentGeneration: record.Identity.AgentGeneration,
			ExecId: record.Identity.ExecID, TargetId: record.Identity.TargetID,
			TargetGeneration: record.Identity.TargetGeneration, TargetRunId: record.Identity.TargetRunID,
			TargetOperationId: record.Identity.TargetOperationID,
		},
		SignalFamily: record.SignalFamily, SubjectId: record.SubjectID, Source: record.Source,
		SourceInstance: record.SourceInstance, SourceSequence: record.SourceSequence,
		HasSourceSequence: record.HasSourceSequence, ObservedAt: protobufTimestamp(time.Unix(0, record.ObservedWallUnixNano).UTC()),
		PolicyDigest: record.PolicyDigest, CapabilityDigest: record.CapabilityDigest,
		Payload: append([]byte(nil), record.Payload...),
		Causal:  &worldv1.CausalContext{CausationId: record.Causal.CausationID, CorrelationId: record.Causal.CorrelationID, CorrelationMethod: record.Causal.CorrelationMethod, Confidence: record.Causal.Confidence},
	}
	if record.Gap != nil {
		mapped.Gap = mapGap(*record.Gap)
	}
	return mapped
}

func metricFromRecord(record ledger.Record) (*worldv1.MetricSample, error) {
	var value worldv1.MetricSample
	if err := decodeProjection(record, &value); err != nil {
		return nil, err
	}
	if _, err := nativeTimestamp(value.CollectedAt, "metric.collected_at", true); value.Name == "" || err != nil {
		return nil, corruptProjection(record, "metric name and collected_at are required")
	}
	if value.LeaseId != "" && value.LeaseId != record.Identity.LeaseID || value.SubjectId != "" && value.SubjectId != record.SubjectID || value.SignalFamily != "" && value.SignalFamily != record.SignalFamily {
		return nil, corruptProjection(record, "metric payload scope disagrees with ledger identity")
	}
	value.Cursor, value.LeaseId, value.SubjectId, value.SignalFamily = uint64(record.Cursor), record.Identity.LeaseID, record.SubjectID, record.SignalFamily
	return &value, nil
}

func decodeProjection(record ledger.Record, destination proto.Message) error {
	if len(record.Payload) == 0 {
		return corruptProjection(record, "projection payload is empty")
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Payload, destination); err != nil {
		return corruptProjection(record, fmt.Sprintf("projection payload is invalid: %v", err))
	}
	return nil
}

func corruptProjection(record ledger.Record, detail string) error {
	return status.Errorf(codes.DataLoss, "ledger cursor %d: %s", record.Cursor, detail)
}

func mapGap(value ledger.Gap) *worldv1.Gap {
	return &worldv1.Gap{Cause: gapCauseName(value.Cause), Source: value.Source, SourceInstance: value.SourceInstance, FromSequence: value.FromSequence, ThroughSequence: value.ThroughSequence, FromCursor: uint64(value.FromCursor), ThroughCursor: uint64(value.ThroughCursor), Detail: value.Detail}
}

func parseRecordKind(value string) (ledger.RecordKind, bool) {
	for kind := ledger.RecordObservation; kind <= ledger.RecordDuplicate; kind++ {
		if recordKindName(kind) == value {
			return kind, true
		}
	}
	return 0, false
}

func recordKindName(value ledger.RecordKind) string {
	switch value {
	case ledger.RecordObservation:
		return "observation"
	case ledger.RecordMetric:
		return "metric"
	case ledger.RecordControl:
		return "control"
	case ledger.RecordIncident:
		return "incident"
	case ledger.RecordCoverage:
		return "coverage"
	case ledger.RecordPressure:
		return "pressure"
	case ledger.RecordTopology:
		return "topology"
	case ledger.RecordGap:
		return "gap"
	case ledger.RecordDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

func gapCauseName(value ledger.GapCause) string {
	switch value {
	case ledger.GapCollectorOverflow:
		return "collector_overflow"
	case ledger.GapCollectorRestart:
		return "collector_restart"
	case ledger.GapCompaction:
		return "compaction"
	case ledger.GapCollectorLoss:
		return "collector_loss"
	case ledger.GapSourceSequence:
		return "source_sequence"
	case ledger.GapSubscriberOverflow:
		return "subscriber_overflow"
	case ledger.GapSegmentRepair:
		return "segment_repair"
	default:
		return "unknown"
	}
}

func sortSnapshot(value *worldv1.LiveSnapshot) {
	sort.Slice(value.Subjects, func(i, j int) bool { return value.Subjects[i].SubjectId < value.Subjects[j].SubjectId })
	sort.Slice(value.Metrics, func(i, j int) bool {
		left, right := value.Metrics[i], value.Metrics[j]
		return left.SubjectId < right.SubjectId || left.SubjectId == right.SubjectId && left.Name < right.Name
	})
	sort.Slice(value.Coverage, func(i, j int) bool { return value.Coverage[i].CollectorId < value.Coverage[j].CollectorId })
	sort.Slice(value.Incidents, func(i, j int) bool { return value.Incidents[i].IncidentId < value.Incidents[j].IncidentId })
	sort.Slice(value.Pressure, func(i, j int) bool {
		left, right := value.Pressure[i], value.Pressure[j]
		return left.SubjectId < right.SubjectId || left.SubjectId == right.SubjectId && left.Resource < right.Resource
	})
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func waitUntil(ctx context.Context, at time.Time) error {
	delay := time.Until(at)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func subscriptionError(err error) error {
	if err == ledger.ErrCursorOutOfRange {
		return status.Error(codes.OutOfRange, "after_cursor is beyond the durable ledger head")
	}
	return err
}

func streamEnd(err error) error {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return err
	}
	return err
}
