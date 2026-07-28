// Package wiremap contains shared, side-effect-free mappings from application
// read models to public wire values. Orchestration and the RPC facade use the
// same mapper so new fields cannot disappear silently from one response path.
package wiremap

import (
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Target(value application.TargetRecord) *worldv1.Target {
	result := &worldv1.Target{
		TargetId: value.ID, ResearchSessionId: value.SessionID, LeaseId: value.LeaseID,
		TemplateReference: value.Template, Kind: string(value.Kind), CurrentGeneration: value.CurrentGeneration,
		Revision: value.Revision, CreatedAt: protobufTimestamp(value.CreatedAt), UpdatedAt: protobufTimestamp(value.UpdatedAt),
		Generations: make([]*worldv1.TargetGeneration, len(value.Generations)),
		Runs:        make([]*worldv1.TargetRun, len(value.Runs)), Operations: make([]*worldv1.TargetOperation, len(value.Operations)),
	}
	for index, generation := range value.Generations {
		result.Generations[index] = &worldv1.TargetGeneration{
			Generation: generation.Generation, PolicyDigest: generation.PolicyDigest, CapabilityDigest: generation.CapabilityDigest,
			PreviousGeneration: generation.Previous, RecoveryIncidentId: generation.RecoveryIncident,
			State: string(generation.State), Revision: generation.Revision,
			CreatedAt: protobufTimestamp(generation.CreatedAt), UpdatedAt: protobufTimestamp(generation.UpdatedAt),
			ProvisioningPlanDigest: generation.ProvisioningPlanDigest,
		}
	}
	for index := range value.Runs {
		result.Runs[index] = TargetRun(value.Runs[index])
	}
	for index := range value.Operations {
		result.Operations[index] = TargetOperation(value.Operations[index])
	}
	return result
}

func TargetRun(value application.TargetRunRecord) *worldv1.TargetRun {
	return &worldv1.TargetRun{
		TargetRunId: value.ID, Generation: value.Generation, AgentWorkspaceId: value.AgentWorkspaceID,
		AgentGeneration: value.AgentGeneration, MaterializationDigest: value.MaterializationDigest,
		State: string(value.State), Revision: value.Revision, BundleId: value.BundleID,
		BundleArtifact: value.BundleArtifact, BundleDigest: value.BundleDigest,
		ProvisioningPlanDigest: value.ProvisioningPlanDigest,
		IncidentIds:            append([]string(nil), value.IncidentIDs...),
		CreatedAt:              protobufTimestamp(value.CreatedAt), UpdatedAt: protobufTimestamp(value.UpdatedAt),
	}
}

func TargetOperation(value application.TargetOperationRecord) *worldv1.TargetOperation {
	return &worldv1.TargetOperation{
		TargetOperationId: value.ID, TargetRunId: value.RunID, Generation: value.Generation,
		Kind: string(value.Kind), CommandDisplay: value.CommandDisplay, ContentDigest: value.ContentDigest,
		State: string(value.State), Revision: value.Revision,
		CreatedAt: protobufTimestamp(value.CreatedAt), UpdatedAt: protobufTimestamp(value.UpdatedAt),
	}
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
