package application

import (
	"context"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

// BindTargetGenerationPlan atomically freezes the exact semantic target plan
// and physical idempotency identity before a driver creates or resets the
// generation. A generation accepts one binding only; exact retries replay from
// the application idempotency journal.
type BindTargetGenerationPlanRequest struct {
	Meta                   MutationMeta `json:"meta"`
	TargetID               string       `json:"target_id"`
	Generation             uint64       `json:"generation"`
	ExpectedRevision       uint64       `json:"expected_revision"`
	ProvisioningPlanDigest string       `json:"provisioning_plan_digest"`
	ProvisioningKey        string       `json:"provisioning_key"`
}

func (c *Core) BindTargetGenerationPlan(ctx context.Context, request BindTargetGenerationPlanRequest) (TargetRecord, error) {
	const operation = "target_generation.bind_plan"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRecord{}, err
	}
	if _, err := domain.ParseTargetID(request.TargetID); err != nil {
		return TargetRecord{}, err
	}
	if !domain.TargetGeneration(request.Generation).IsValid() || request.ExpectedRevision == 0 {
		return TargetRecord{}, invalidArgument(operation, "generation", "valid generation and expected revision are required", nil)
	}
	if _, err := domain.ParseDigest(request.ProvisioningPlanDigest); err != nil {
		return TargetRecord{}, invalidArgument(operation, "provisioning_plan_digest", "must be a valid digest", err)
	}
	if !domain.IsCanonicalIdempotencyKey(request.ProvisioningKey) {
		return TargetRecord{}, invalidArgument(operation, "provisioning_key", "must be a bounded non-blank value without surrounding whitespace", nil)
	}
	return c.mutateTarget(ctx, "bind_target_generation_plan", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		if target.CurrentGeneration != request.Generation {
			return "", failedPrecondition(operation, "generation", "must be the current target generation", nil)
		}
		generation, err := findTargetGeneration(target, request.Generation)
		if err != nil {
			return "", err
		}
		if generation.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if generation.State != domain.TargetGenerationProvisioning {
			return "", failedPrecondition(operation, "state", "plan must be bound before physical provisioning advances", nil)
		}
		if !targetProvisioningBindingEmpty(*generation) {
			return "", store.ErrIdempotencyConflict
		}
		generation.ProvisioningPlanDigest = request.ProvisioningPlanDigest
		generation.ProvisioningKey = request.ProvisioningKey
		generation.Revision++
		generation.UpdatedAt = c.clock().UTC()
		return "target_generation.plan_bound", nil
	})
}
