package application

import (
	"context"
	"fmt"
	"sort"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type ObservationBundleFinalizer interface {
	Finalize(context.Context, observationbundle.FinalizeRequest) (observationbundle.Result, error)
}

type RunFinalizationService struct {
	core      *Core
	finalizer ObservationBundleFinalizer
	authority ports.MaterialAuthority
}

func NewRunFinalizationService(core *Core, finalizer ObservationBundleFinalizer, authority ports.MaterialAuthority) (*RunFinalizationService, error) {
	if core == nil || finalizer == nil || authority == nil {
		return nil, fmt.Errorf("core, observation bundle finalizer, and material authority are required")
	}
	return &RunFinalizationService{core: core, finalizer: finalizer, authority: authority}, nil
}

type FinalizeRunEvidenceRequest struct {
	Meta                MutationMeta                      `json:"meta"`
	TargetID            string                            `json:"target_id"`
	ExpectedRunRevision uint64                            `json:"expected_run_revision"`
	Evidence            observationbundle.FinalizeRequest `json:"evidence"`
}

type FinalizeRunEvidenceOutcome struct {
	Run      TargetRunRecord            `json:"run"`
	Bundle   domain.ObservationBundle   `json:"-"`
	Metadata observationbundle.Metadata `json:"metadata"`
	Artifact domain.ArtifactReference   `json:"-"`
}

// PreparedRunEvidence is the immutable half of target-run finalization. The
// bundle and its material-authority artifact already exist, while Commit is the
// exact control-plane mutation that is still to be made. Callers that need an
// independently recoverable publication boundary must durably record this
// value (and any public projection) before calling Commit.
type PreparedRunEvidence struct {
	Bundle   domain.ObservationBundle
	Metadata observationbundle.Metadata
	Artifact domain.ArtifactReference
	Commit   FinalizeTargetRunRequest
}

// Prepare seals local evidence and publishes it to the immutable material
// authority without changing the target-run control-plane state.
func (s *RunFinalizationService) Prepare(ctx context.Context, request FinalizeRunEvidenceRequest) (PreparedRunEvidence, error) {
	if err := request.Meta.Validate(ctx, s.core.clock()); err != nil {
		return PreparedRunEvidence{}, err
	}
	targetID, err := domain.ParseTargetID(request.TargetID)
	if err != nil {
		return PreparedRunEvidence{}, err
	}
	target, err := s.core.GetTarget(ctx, request.TargetID)
	if err != nil {
		return PreparedRunEvidence{}, err
	}
	run, err := findRun(&target, request.Evidence.Result.RunID.String())
	if err != nil {
		return PreparedRunEvidence{}, err
	}
	if run.Revision != request.ExpectedRunRevision && !(run.State.Terminal() && run.BundleID == request.Evidence.BundleID.String()) {
		return PreparedRunEvidence{}, store.ErrRevisionConflict
	}
	if targetID != request.Evidence.TargetID || run.Generation != uint64(request.Evidence.TargetGeneration) || run.AgentWorkspaceID != request.Evidence.AgentWorkspaceID.String() || run.AgentGeneration != uint64(request.Evidence.AgentGeneration) {
		return PreparedRunEvidence{}, ErrScope
	}
	local, err := s.finalizer.Finalize(ctx, request.Evidence)
	if err != nil {
		return PreparedRunEvidence{}, err
	}
	if local.Content == nil {
		return PreparedRunEvidence{}, domain.NewError(domain.CodeIntegrityViolation, "run_finalization.prepare", "bundle_content", "finalizer returned no sealed content", nil)
	}
	artifact, err := s.authority.CaptureObservationBundle(ctx, ports.ObservationBundlePlan{IdempotencyKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "artifact"), Bundle: local.Bundle, Content: local.Content})
	if err != nil {
		return PreparedRunEvidence{}, err
	}
	artifactSpec := artifact.Spec()
	if artifactSpec.Digest != local.Content.Digest() || artifactSpec.Size != local.Content.Size() {
		return PreparedRunEvidence{}, domain.NewError(domain.CodeIntegrityViolation, "run_finalization.prepare", "bundle_artifact", "published artifact digest does not match sealed metadata", nil)
	}
	incidentIDs := make([]string, len(request.Evidence.Result.IncidentIDs))
	for index, incidentID := range request.Evidence.Result.IncidentIDs {
		incidentIDs[index] = incidentID.String()
	}
	sort.Strings(incidentIDs)
	meta := request.Meta
	meta.IdempotencyKey = domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "control")
	return PreparedRunEvidence{
		Bundle: local.Bundle, Metadata: local.Metadata, Artifact: artifact,
		Commit: FinalizeTargetRunRequest{
			Meta: meta, TargetID: request.TargetID, RunID: run.ID,
			ExpectedRevision: request.ExpectedRunRevision,
			Failed:           request.Evidence.Result.Outcome == ports.RunFailed,
			BundleID:         local.Bundle.ID().String(), BundleArtifact: artifactSpec.Reference,
			BundleDigest: artifactSpec.Digest.String(), IncidentIDs: incidentIDs,
		},
	}, nil
}

// Commit records the terminal target-run state for an already prepared bundle.
func (s *RunFinalizationService) Commit(ctx context.Context, prepared PreparedRunEvidence) (FinalizeRunEvidenceOutcome, error) {
	finalized, err := s.core.FinalizeTargetRun(ctx, prepared.Commit)
	if err != nil {
		return FinalizeRunEvidenceOutcome{}, err
	}
	return FinalizeRunEvidenceOutcome{Run: finalized, Bundle: prepared.Bundle, Metadata: prepared.Metadata, Artifact: prepared.Artifact}, nil
}

// Finalize is the atomic-control-plane convenience path for callers that do
// not own an additional durable public projection. Orchestration uses
// Prepare/Commit so it can stage that projection before the terminal mutation.
func (s *RunFinalizationService) Finalize(ctx context.Context, request FinalizeRunEvidenceRequest) (FinalizeRunEvidenceOutcome, error) {
	prepared, err := s.Prepare(ctx, request)
	if err != nil {
		return FinalizeRunEvidenceOutcome{}, err
	}
	return s.Commit(ctx, prepared)
}

var _ ObservationBundleFinalizer = (*observationbundle.Finalizer)(nil)
