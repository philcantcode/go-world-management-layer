package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// requireCoverage is the single coverage-policy comparison used at readiness,
// coordinator stop, and final evidence assembly.
func requireCoverage(requirement ports.ObservationRequirement, collectorID domain.CollectorID, coverage domain.CollectorCoverage, final bool) error {
	if err := requirement.Validate(); err != nil {
		return err
	}
	spec := coverage.Spec()
	if spec.CollectorID != collectorID || spec.SignalFamily != requirement.SignalFamily || spec.Placement != requirement.Placement || spec.Required != requirement.Required {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_coverage", "scope", "collector coverage does not match its authorized requirement", nil)
	}
	if spec.Status != domain.CoverageAvailable || coverageLevelRank(spec.Level) < coverageLevelRank(requirement.MinimumLevel) || len(spec.Gaps) != 0 || spec.DroppedRecords != 0 {
		return domain.NewError(domain.CodeCapabilityUnavailable, "orchestration.require_coverage", "coverage", "collector is not gap-free and available at the required level", nil)
	}
	if final && (spec.StartedAt.IsZero() || spec.EndedAt.IsZero() || spec.EndedAt.Before(spec.StartedAt)) {
		return domain.NewError(domain.CodeIntegrityViolation, "orchestration.require_coverage", "time_range", "final coverage lacks a valid collection interval", nil)
	}
	return nil
}

func coverageLevelRank(level domain.CoverageLevel) int {
	switch level {
	case domain.CoverageLevelComplete:
		return 3
	case domain.CoverageLevelPartial:
		return 2
	case domain.CoverageLevelNone:
		return 1
	default:
		return 0
	}
}

func assembleTargetRunResult(receipt ports.TargetRunStopReceipt, evidence RunObservationEvidence) (ports.TargetRunResult, error) {
	if err := receipt.Validate(); err != nil {
		return ports.TargetRunResult{}, err
	}
	if receipt.RunID.IsZero() || evidence.FirstCursor == 0 || evidence.LastCursor < evidence.FirstCursor || len(evidence.Required) == 0 || len(evidence.Artifacts) == 0 || len(evidence.Events) == 0 || len(evidence.Coverage) == 0 {
		return ports.TargetRunResult{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.assemble_run_evidence", "evidence", "requires an ordered cursor range, configured policy, artifacts, events, and coverage", nil)
	}
	required, err := exactCoverageSet(evidence.Required)
	if err != nil || len(required) == 0 {
		return ports.TargetRunResult{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.assemble_run_evidence", "required", "configured required coverage is invalid", err)
	}
	available := make(map[string]bool, len(required))
	for _, coverage := range evidence.Coverage {
		spec := coverage.Spec()
		if _, wanted := required[spec.SignalFamily]; wanted && spec.Required && spec.Status == domain.CoverageAvailable && spec.Level != domain.CoverageLevelNone && spec.Level != domain.CoverageLevelUnknown && len(spec.Gaps) == 0 && spec.DroppedRecords == 0 {
			available[spec.SignalFamily] = true
		}
	}
	failed := receipt.Outcome == ports.RunFailed
	for family := range required {
		if !available[family] {
			failed = true
		}
	}
	for _, failure := range evidence.Failures {
		failed = failed || failure.Required
	}
	outcome := ports.RunCompleted
	if failed {
		outcome = ports.RunFailed
	}
	stoppedAt := receipt.StoppedAt.UTC()
	if evidence.StoppedAt.After(stoppedAt) {
		stoppedAt = evidence.StoppedAt.UTC()
	}
	summaryText := "The target stopped cleanly and every configured required collector produced gap-free evidence at its authorized level."
	inferences := []string{"Readiness and final coverage were validated against the configured collector policy."}
	if failed {
		summaryText = "The run failed because the target or at least one configured required collector did not complete with authoritative gap-free evidence."
		inferences = append(inferences, "Missing or degraded coverage is retained as an explicit gap and is not inferred from readiness.")
	}
	summary, err := domain.NewDerivedSummary(domain.DerivedSummarySpec{
		Text:       summaryText,
		Citations:  []domain.EvidenceCitation{{FirstCursor: evidence.FirstCursor, LastCursor: evidence.LastCursor, Artifact: evidence.Artifacts[len(evidence.Artifacts)-1]}},
		Inferences: inferences,
	})
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	return ports.TargetRunResult{
		RunID: receipt.RunID, Outcome: outcome, FirstCursor: evidence.FirstCursor, LastCursor: evidence.LastCursor,
		RawArtifacts:     append([]domain.ArtifactReference(nil), evidence.Artifacts...),
		NormalizedEvents: append([]domain.EventEnvelope(nil), evidence.Events...), Metrics: append([]domain.MetricSample(nil), evidence.Metrics...),
		Coverage: append([]domain.CollectorCoverage(nil), evidence.Coverage...), Gaps: append([]domain.Gap(nil), evidence.Gaps...),
		TargetChanges: receipt.TargetChanges, Summary: summary, StoppedAt: stoppedAt,
	}, nil
}

func (s *Service) ensureRunFailureIncidentRequest(ctx context.Context, result ports.TargetRunResult, request application.CreateIncidentRequest) (ports.TargetRunResult, error) {
	request.Meta.Deadline = deadline(ctx)
	incident, err := s.core.CreateIncident(ctx, request)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	incidentID, err := domain.ParseIncidentID(incident.ID)
	if err != nil {
		return ports.TargetRunResult{}, err
	}
	result.IncidentIDs = []domain.IncidentID{incidentID}
	return result, nil
}

func buildRunFailureIncidentIntent(result ports.TargetRunResult, failures []ObserverFailure, cause error) *runFailureIncidentIntent {
	if result.Outcome != ports.RunFailed || len(result.IncidentIDs) != 0 {
		return nil
	}
	classification := domain.IncidentLinuxTargetFailure
	trigger := "target run failed"
	detail := "target driver reported a failed run"
	for _, failure := range failures {
		if failure.Required {
			classification = domain.IncidentObserverFailure
			trigger = "required collector failed"
			detail = failure.Reason
			break
		}
	}
	if cause != nil {
		classification = domain.IncidentControlPlaneFailure
		trigger = "target run controller rollback"
		detail = cause.Error()
	}
	detail = boundedIncidentDetail(detail)
	return &runFailureIncidentIntent{
		Classification: classification, Trigger: trigger,
		Cause: application.CauseRecord{Kind: domain.CauseProven, Summary: detail, Confidence: 1},
	}
}

func validateRunFailureIncidentIntent(intent runFailureIncidentIntent) error {
	if !intent.Classification.IsValid() {
		return fmt.Errorf("failure incident classification is invalid")
	}
	wantedTrigger := map[domain.IncidentClassification]string{
		domain.IncidentLinuxTargetFailure:  "target run failed",
		domain.IncidentObserverFailure:     "required collector failed",
		domain.IncidentControlPlaneFailure: "target run controller rollback",
	}[intent.Classification]
	if wantedTrigger == "" || intent.Trigger != wantedTrigger {
		return fmt.Errorf("failure incident trigger does not match its classification")
	}
	if intent.Cause.Kind != domain.CauseProven || intent.Cause.Method != "" || intent.Cause.Confidence != 1 ||
		intent.Cause.Summary != strings.TrimSpace(intent.Cause.Summary) || intent.Cause.Summary == "" ||
		len(intent.Cause.Summary) > 1024 || !utf8.ValidString(intent.Cause.Summary) {
		return fmt.Errorf("failure incident cause is invalid")
	}
	return nil
}

func boundedIncidentDetail(value string) string {
	value = strings.TrimSpace(strings.ToValidUTF8(value, "\uFFFD"))
	if len(value) <= 1024 {
		return value
	}
	value = value[:1024]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func buildRunFailureIncidentRequest(meta application.MutationMeta, target application.TargetRecord, preparation stagedBundleStopPreparation, result ports.TargetRunResult) application.CreateIncidentRequest {
	intent := *preparation.Incident
	request := application.CreateIncidentRequest{
		Meta: childMeta(meta, "failure-incident", meta.Deadline), Classification: intent.Classification,
		SessionID: target.SessionID, LeaseID: preparation.Reservation.LeaseID, AgentWorkspaceID: preparation.AgentWorkspaceID,
		AgentGeneration: preparation.AgentGeneration, TargetID: preparation.Reservation.TargetID, TargetGeneration: preparation.TargetGeneration, TargetRunID: preparation.Reservation.RunID,
		Trigger: intent.Trigger, LastKnownState: string(preparation.InitialRunState), Cause: intent.Cause,
		FirstRelevantCursor: uint64(result.FirstCursor), LastRelevantCursor: uint64(result.LastCursor),
		Coverage: incidentCoverage(result.Coverage), Artifacts: incidentArtifacts(result.RawArtifacts),
	}
	request.Meta.Deadline = time.Time{}
	return request
}

func incidentCoverage(values []domain.CollectorCoverage) []application.IncidentCoverageRecord {
	result := make([]application.IncidentCoverageRecord, 0, len(values))
	for _, value := range values {
		spec := value.Spec()
		item := application.IncidentCoverageRecord{
			CollectorID: spec.CollectorID.String(), SignalFamily: spec.SignalFamily, Placement: spec.Placement,
			Level: spec.Level, Status: spec.Status, Required: spec.Required, StartedAt: spec.StartedAt,
			EndedAt: spec.EndedAt, DroppedRecords: spec.DroppedRecords,
		}
		for _, value := range spec.Gaps {
			gap := value.Spec()
			item.Gaps = append(item.Gaps, application.IncidentGapRecord{
				Kind: gap.Kind, Source: gap.Source, SourceInstance: gap.SourceInstance,
				FirstSourceSequence: gap.FirstSourceSequence, LastSourceSequence: gap.LastSourceSequence,
				FirstCursor: uint64(gap.FirstCursor), LastCursor: uint64(gap.LastCursor),
				StartedAt: gap.StartedAt, EndedAt: gap.EndedAt, LostRecords: gap.LostRecords, Reason: gap.Reason,
			})
		}
		result = append(result, item)
	}
	return result
}

func incidentArtifacts(values []domain.ArtifactReference) []application.IncidentArtifactRecord {
	result := make([]application.IncidentArtifactRecord, 0, len(values))
	for _, value := range values {
		result = append(result, incidentArtifact(value))
	}
	return result
}

func incidentArtifact(value domain.ArtifactReference) application.IncidentArtifactRecord {
	spec := value.Spec()
	return application.IncidentArtifactRecord{
		Reference: spec.Reference, Digest: spec.Digest.String(), Size: spec.Size, Role: spec.Role, Sensitivity: spec.Sensitivity,
	}
}

func sortedCoverageFamilies(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
