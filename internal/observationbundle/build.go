package observationbundle

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func build(request FinalizeRequest) (domain.ObservationBundle, MetadataPayload, error) {
	const operation = "observation_bundle.build"
	result := &request.Result
	if request.BundleID.IsZero() || request.TargetID.IsZero() || !request.TargetGeneration.IsValid() || request.AgentWorkspaceID.IsZero() || !request.AgentGeneration.IsValid() || result.RunID.IsZero() {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInvalidArgument, operation, "scope", "bundle, run, target, and agent identities are required", nil)
	}
	if !result.Outcome.IsValid() {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInvalidArgument, operation, "outcome", "must be completed or failed", nil)
	}
	if request.CreatedAt.IsZero() || request.FinalizedAt.IsZero() || request.FinalizedAt.Before(request.CreatedAt) || result.StoppedAt.IsZero() || result.StoppedAt.After(request.FinalizedAt) {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInvalidArgument, operation, "time_range", "created, stopped, and finalized times must be ordered", nil)
	}
	if result.FirstCursor == 0 || result.LastCursor < result.FirstCursor {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInvalidArgument, operation, "cursor_range", "must be non-empty and ordered", nil)
	}
	required, err := normalizedRequiredCoverage(request.RequiredCoverage)
	if err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateArtifacts(result.RawArtifacts); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateEvents(result.NormalizedEvents, result.RunID, result.FirstCursor, result.LastCursor); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateMetrics(result.Metrics, result.FirstCursor, result.LastCursor); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateGaps(result.Gaps, result.FirstCursor, result.LastCursor); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateCoverage(result.Coverage, required, result.Gaps, result.Outcome, len(result.IncidentIDs) > 0); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if result.TargetChanges.Scope() != domain.ChangeScopeTarget || !result.TargetChanges.WorkspaceRevision().IsValid() || result.TargetChanges.SealedAt().IsZero() {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInvalidArgument, operation, "target_changes", "must be an initialized target change set", nil)
	}
	if err := validateIncidents(result.IncidentIDs, result.Outcome); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	if err := validateSummary(result.Summary, result.RawArtifacts, result.FirstCursor, result.LastCursor); err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}

	bundle, err := domain.NewObservationBundle(domain.ObservationBundleSpec{
		ID: request.BundleID, TargetRunID: result.RunID, TargetID: request.TargetID,
		TargetGeneration: request.TargetGeneration, AgentWorkspaceID: request.AgentWorkspaceID,
		AgentGeneration: request.AgentGeneration, FirstCursor: result.FirstCursor, LastCursor: result.LastCursor,
		RawArtifacts: result.RawArtifacts, NormalizedEvents: result.NormalizedEvents, Metrics: result.Metrics,
		Coverage: result.Coverage, Gaps: result.Gaps, TargetChanges: result.TargetChanges,
		IncidentIDs: result.IncidentIDs, Summary: result.Summary, CreatedAt: request.CreatedAt,
	})
	if err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	bundle, err = bundle.Seal(domain.InitialRevision, request.FinalizedAt)
	if err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, err
	}
	payload, err := metadataPayload(request, required)
	if err != nil {
		return domain.ObservationBundle{}, MetadataPayload{}, domain.NewError(domain.CodeInternal, operation, "metadata", "cannot be projected", err)
	}
	return bundle, payload, nil
}

func normalizedRequiredCoverage(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "required_coverage", "must not be empty", nil)
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	for index, value := range result {
		if strings.TrimSpace(value) == "" {
			return nil, domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "required_coverage", "must not contain blanks", nil)
		}
		if index > 0 && value == result[index-1] {
			return nil, domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "required_coverage", "must not contain duplicates", nil)
		}
	}
	return result, nil
}

func validateArtifacts(values []domain.ArtifactReference) error {
	if len(values) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "raw_artifacts", "must not be empty", nil)
	}
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		spec := value.Spec()
		if spec.Reference == "" || spec.Digest.IsZero() {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("raw_artifacts[%d]", index), "must be initialized", nil)
		}
		key := spec.Reference + "\x00" + spec.Digest.String() + "\x00" + spec.Role
		if _, duplicate := seen[key]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "raw_artifacts", "must not contain duplicates", nil)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateEvents(values []domain.EventEnvelope, runID domain.TargetRunID, first, last domain.ObservationCursor) error {
	if len(values) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "normalized_events", "must not be empty", nil)
	}
	seen := make(map[domain.EventID]struct{}, len(values))
	for index, value := range values {
		params := value.Params()
		if params.EventID.IsZero() || params.TargetRunID != runID {
			return domain.NewError(domain.CodeConflict, "observation_bundle.build", fmt.Sprintf("normalized_events[%d]", index), "must belong to the target run", nil)
		}
		if params.SourceCursor < first || params.SourceCursor > last {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("normalized_events[%d].source_cursor", index), "falls outside the bundle cursor range", nil)
		}
		if _, duplicate := seen[params.EventID]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "normalized_events", "contains duplicate event identity", nil)
		}
		seen[params.EventID] = struct{}{}
	}
	return nil
}

func validateMetrics(values []domain.MetricSample, first, last domain.ObservationCursor) error {
	for index, value := range values {
		spec := value.Spec()
		if spec.Name == "" {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("metrics[%d]", index), "must be initialized", nil)
		}
		if spec.Cursor < first || spec.Cursor > last {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("metrics[%d].cursor", index), "falls outside the bundle cursor range", nil)
		}
	}
	return nil
}

func validateGaps(values []domain.Gap, first, last domain.ObservationCursor) error {
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		spec := value.Spec()
		if spec.Source == "" || !spec.Kind.IsValid() {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("gaps[%d]", index), "must be initialized", nil)
		}
		if spec.FirstCursor > 0 && (spec.FirstCursor < first || spec.FirstCursor > last) {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("gaps[%d].first_cursor", index), "falls outside the bundle cursor range", nil)
		}
		if spec.LastCursor > 0 && (spec.LastCursor < first || spec.LastCursor > last) {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("gaps[%d].last_cursor", index), "falls outside the bundle cursor range", nil)
		}
		key := gapKey(value)
		if _, duplicate := seen[key]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "gaps", "must not contain duplicates", nil)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateCoverage(values []domain.CollectorCoverage, required []string, gaps []domain.Gap, outcome ports.RunOutcome, hasIncident bool) error {
	if len(values) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "coverage", "must not be empty", nil)
	}
	topGaps := make(map[string]struct{}, len(gaps))
	for _, gap := range gaps {
		topGaps[gapKey(gap)] = struct{}{}
	}
	byFamily := make(map[string][]domain.CollectorCoverageSpec)
	seenCollectors := make(map[domain.CollectorID]struct{})
	for index, coverage := range values {
		spec := coverage.Spec()
		if spec.CollectorID.IsZero() || spec.SignalFamily == "" {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("coverage[%d]", index), "must be initialized", nil)
		}
		if _, duplicate := seenCollectors[spec.CollectorID]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "coverage", "contains duplicate collector identity", nil)
		}
		seenCollectors[spec.CollectorID] = struct{}{}
		for gapIndex, gap := range spec.Gaps {
			if _, included := topGaps[gapKey(gap)]; !included {
				return domain.NewError(domain.CodeConflict, "observation_bundle.build", fmt.Sprintf("coverage[%d].gaps[%d]", index, gapIndex), "is absent from the bundle gap list", nil)
			}
		}
		byFamily[spec.SignalFamily] = append(byFamily[spec.SignalFamily], spec)
	}
	for _, family := range required {
		items := byFamily[family]
		if len(items) == 0 {
			return domain.NewError(domain.CodeCapabilityUnavailable, "observation_bundle.build", "coverage", "required signal family has no coverage record: "+family, nil)
		}
		acceptable := false
		failedWithEvidence := false
		for _, item := range items {
			if !item.Required {
				continue
			}
			if item.Status == domain.CoverageAvailable && item.Level != domain.CoverageLevelNone && item.Level != domain.CoverageLevelUnknown {
				acceptable = true
			}
			if (item.Status != domain.CoverageAvailable || item.Level == domain.CoverageLevelNone || item.Level == domain.CoverageLevelUnknown) && (len(item.Gaps) > 0 || item.DroppedRecords > 0) {
				failedWithEvidence = true
			}
		}
		if outcome == ports.RunCompleted && !acceptable {
			return domain.NewError(domain.CodeCapabilityUnavailable, "observation_bundle.build", "coverage", "completed run lacks required coverage: "+family, nil)
		}
		if outcome == ports.RunFailed && !acceptable && !(failedWithEvidence && hasIncident) {
			return domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.build", "coverage", "failed run lacks a gap and incident for required coverage loss: "+family, nil)
		}
	}
	return nil
}

func validateIncidents(values []domain.IncidentID, outcome ports.RunOutcome) error {
	if outcome == ports.RunFailed && len(values) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "incident_ids", "failed run must cite at least one incident", nil)
	}
	seen := make(map[domain.IncidentID]struct{}, len(values))
	for _, value := range values {
		if value.IsZero() {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "incident_ids", "must not contain zero IDs", nil)
		}
		if _, duplicate := seen[value]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "incident_ids", "must not contain duplicates", nil)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateSummary(summary domain.DerivedSummary, artifacts []domain.ArtifactReference, first, last domain.ObservationCursor) error {
	spec := summary.Spec()
	if spec.Text == "" || len(spec.Citations) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", "summary", "must contain text and evidence citations", nil)
	}
	artifactSet := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		s := artifact.Spec()
		artifactSet[s.Reference+"\x00"+s.Digest.String()] = struct{}{}
	}
	for index, citation := range spec.Citations {
		if citation.FirstCursor > 0 {
			end := citation.LastCursor
			if end == 0 {
				end = citation.FirstCursor
			}
			if citation.FirstCursor < first || end > last || end < citation.FirstCursor {
				return domain.NewError(domain.CodeInvalidArgument, "observation_bundle.build", fmt.Sprintf("summary.citations[%d]", index), "cursor citation falls outside the bundle", nil)
			}
		}
		artifact := citation.Artifact.Spec()
		if artifact.Reference != "" {
			if _, found := artifactSet[artifact.Reference+"\x00"+artifact.Digest.String()]; !found {
				return domain.NewError(domain.CodeConflict, "observation_bundle.build", fmt.Sprintf("summary.citations[%d].artifact", index), "is not a raw bundle artifact", nil)
			}
		}
	}
	return nil
}

func metadataPayload(request FinalizeRequest, required []string) (MetadataPayload, error) {
	result := request.Result
	payload := MetadataPayload{
		BundleID: request.BundleID.String(), TargetRunID: result.RunID.String(), TargetID: request.TargetID.String(),
		TargetGeneration: uint64(request.TargetGeneration), AgentWorkspaceID: request.AgentWorkspaceID.String(), AgentGeneration: uint64(request.AgentGeneration),
		Outcome: result.Outcome, FirstCursor: uint64(result.FirstCursor), LastCursor: uint64(result.LastCursor),
		RequiredCoverage: append([]string(nil), required...), CreatedAt: request.CreatedAt.UTC(), FinalizedAt: request.FinalizedAt.UTC(),
	}
	for _, artifact := range result.RawArtifacts {
		payload.RawArtifacts = append(payload.RawArtifacts, artifactMetadata(artifact))
	}
	sort.Slice(payload.RawArtifacts, func(i, j int) bool {
		left, right := payload.RawArtifacts[i], payload.RawArtifacts[j]
		return left.Reference+left.Role+left.Digest < right.Reference+right.Role+right.Digest
	})
	for _, event := range result.NormalizedEvents {
		params := event.Params()
		digest, err := projectionDigest(params)
		if err != nil {
			return MetadataPayload{}, err
		}
		payload.NormalizedEvents = append(payload.NormalizedEvents, EventMetadata{EventID: params.EventID.String(), Kind: params.Kind, Source: params.Source, SourceCursor: uint64(params.SourceCursor), Digest: digest})
	}
	sort.Slice(payload.NormalizedEvents, func(i, j int) bool {
		if payload.NormalizedEvents[i].SourceCursor == payload.NormalizedEvents[j].SourceCursor {
			return payload.NormalizedEvents[i].EventID < payload.NormalizedEvents[j].EventID
		}
		return payload.NormalizedEvents[i].SourceCursor < payload.NormalizedEvents[j].SourceCursor
	})
	for _, metric := range result.Metrics {
		spec := metric.Spec()
		digest, err := projectionDigest(spec)
		if err != nil {
			return MetadataPayload{}, err
		}
		payload.Metrics = append(payload.Metrics, MetricMetadata{SubjectID: spec.SubjectID.String(), Name: spec.Name, Cursor: uint64(spec.Cursor), CollectedAt: spec.CollectedAt.UTC(), Digest: digest})
	}
	sort.Slice(payload.Metrics, func(i, j int) bool { return payload.Metrics[i].Cursor < payload.Metrics[j].Cursor })
	for _, coverage := range result.Coverage {
		payload.Coverage = append(payload.Coverage, coverageMetadata(coverage))
	}
	sort.Slice(payload.Coverage, func(i, j int) bool {
		return payload.Coverage[i].SignalFamily+payload.Coverage[i].CollectorID < payload.Coverage[j].SignalFamily+payload.Coverage[j].CollectorID
	})
	for _, gap := range result.Gaps {
		payload.Gaps = append(payload.Gaps, gapMetadata(gap))
	}
	sort.Slice(payload.Gaps, func(i, j int) bool { return gapMetadataKey(payload.Gaps[i]) < gapMetadataKey(payload.Gaps[j]) })
	payload.TargetChanges = changeSetMetadata(result.TargetChanges)
	for _, id := range result.IncidentIDs {
		payload.IncidentIDs = append(payload.IncidentIDs, id.String())
	}
	sort.Strings(payload.IncidentIDs)
	payload.Summary = summaryMetadata(result.Summary)
	return payload, nil
}

func artifactMetadata(value domain.ArtifactReference) ArtifactMetadata {
	spec := value.Spec()
	return ArtifactMetadata{Reference: spec.Reference, Digest: spec.Digest.String(), Size: spec.Size, Role: spec.Role, Sensitivity: string(spec.Sensitivity)}
}

func gapMetadata(value domain.Gap) GapMetadata {
	spec := value.Spec()
	return GapMetadata{Kind: string(spec.Kind), Source: spec.Source, SourceInstance: spec.SourceInstance, FirstSourceSequence: spec.FirstSourceSequence, LastSourceSequence: spec.LastSourceSequence, FirstCursor: uint64(spec.FirstCursor), LastCursor: uint64(spec.LastCursor), StartedAt: spec.StartedAt.UTC(), EndedAt: spec.EndedAt.UTC(), LostRecords: spec.LostRecords, Reason: spec.Reason}
}

func gapKey(value domain.Gap) string { return gapMetadataKey(gapMetadata(value)) }

func gapMetadataKey(value GapMetadata) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func coverageMetadata(value domain.CollectorCoverage) CoverageMetadata {
	spec := value.Spec()
	result := CoverageMetadata{CollectorID: spec.CollectorID.String(), SignalFamily: spec.SignalFamily, Placement: string(spec.Placement), Level: string(spec.Level), Status: string(spec.Status), Required: spec.Required, StartedAt: spec.StartedAt.UTC(), EndedAt: spec.EndedAt.UTC(), DroppedRecords: spec.DroppedRecords}
	for _, gap := range spec.Gaps {
		result.Gaps = append(result.Gaps, gapMetadata(gap))
	}
	sort.Slice(result.Gaps, func(i, j int) bool { return gapMetadataKey(result.Gaps[i]) < gapMetadataKey(result.Gaps[j]) })
	return result
}

func changeSetMetadata(value domain.ChangeSet) ChangeSetMetadata {
	result := ChangeSetMetadata{Scope: string(value.Scope()), WorkspaceRevision: uint64(value.WorkspaceRevision()), SealedAt: value.SealedAt().UTC()}
	for _, entry := range value.Entries() {
		spec := entry.Spec()
		metadata := make(map[string]string, len(spec.Metadata))
		for key, item := range spec.Metadata {
			metadata[key] = item
		}
		result.Entries = append(result.Entries, ChangeMetadata{Kind: string(spec.Kind), Path: spec.Path, PreviousPath: spec.PreviousPath, BeforeDigest: spec.BeforeDigest.String(), AfterDigest: spec.AfterDigest.String(), Metadata: metadata})
	}
	return result
}

func summaryMetadata(value domain.DerivedSummary) SummaryMetadata {
	spec := value.Spec()
	result := SummaryMetadata{Text: spec.Text, Inferences: append([]string(nil), spec.Inferences...)}
	for _, citation := range spec.Citations {
		item := CitationMetadata{FirstCursor: uint64(citation.FirstCursor), LastCursor: uint64(citation.LastCursor)}
		if artifact := citation.Artifact.Spec(); artifact.Reference != "" {
			metadata := artifactMetadata(citation.Artifact)
			item.Artifact = &metadata
		}
		result.Citations = append(result.Citations, item)
	}
	return result
}

func projectionDigest(value any) (string, error) {
	projected, err := projectReflect(reflect.ValueOf(value))
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return "", err
	}
	return domain.NewDigest(encoded).String(), nil
}

var timeType = reflect.TypeOf(time.Time{})

func projectReflect(value reflect.Value) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		return projectReflect(value.Elem())
	}
	if value.Type() == timeType {
		return value.Interface().(time.Time).UTC().Format(time.RFC3339Nano), nil
	}
	if value.CanInterface() {
		if stringer, ok := value.Interface().(fmt.Stringer); ok {
			return stringer.String(), nil
		}
	}
	switch value.Kind() {
	case reflect.Struct:
		result := make(map[string]any)
		typ := value.Type()
		for index := 0; index < value.NumField(); index++ {
			if !typ.Field(index).IsExported() {
				continue
			}
			projected, err := projectReflect(value.Field(index))
			if err != nil {
				return nil, err
			}
			result[typ.Field(index).Name] = projected
		}
		return result, nil
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			encoded := append([]byte(nil), value.Bytes()...)
			if json.Valid(encoded) {
				var normalized any
				if err := json.Unmarshal(encoded, &normalized); err == nil {
					return normalized, nil
				}
			}
			return string(encoded), nil
		}
		result := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			projected, err := projectReflect(value.Index(index))
			if err != nil {
				return nil, err
			}
			result[index] = projected
		}
		return result, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported non-string map key %s", value.Type().Key())
		}
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			projected, err := projectReflect(iterator.Value())
			if err != nil {
				return nil, err
			}
			result[iterator.Key().String()] = projected
		}
		return result, nil
	default:
		return value.Interface(), nil
	}
}
