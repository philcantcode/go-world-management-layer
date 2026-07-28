package application

import (
	"context"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type BindTargetRunPlanRequest struct {
	Meta                   MutationMeta `json:"meta"`
	TargetID               string       `json:"target_id"`
	RunID                  string       `json:"run_id"`
	ExpectedRevision       uint64       `json:"expected_revision"`
	ProvisioningPlanDigest string       `json:"provisioning_plan_digest"`
	ProvisioningKey        string       `json:"provisioning_key"`
}

// BindTargetRunPlan freezes material, observation, collector, and duration
// authority before any target preparation or collector process is started.
func (c *Core) BindTargetRunPlan(ctx context.Context, request BindTargetRunPlanRequest) (TargetRunRecord, error) {
	const operation = "target_run.bind_plan"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRunRecord{}, err
	}
	if _, err := domain.ParseTargetID(request.TargetID); err != nil {
		return TargetRunRecord{}, err
	}
	if _, err := domain.ParseTargetRunID(request.RunID); err != nil {
		return TargetRunRecord{}, err
	}
	if request.ExpectedRevision == 0 {
		return TargetRunRecord{}, invalidArgument(operation, "expected_revision", "must be positive", nil)
	}
	if _, err := domain.ParseDigest(request.ProvisioningPlanDigest); err != nil {
		return TargetRunRecord{}, invalidArgument(operation, "provisioning_plan_digest", "must be a valid digest", err)
	}
	if !domain.IsCanonicalIdempotencyKey(request.ProvisioningKey) {
		return TargetRunRecord{}, invalidArgument(operation, "provisioning_key", "must be a bounded non-blank value without surrounding whitespace", nil)
	}
	target, err := c.mutateTarget(ctx, "bind_target_run_plan", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		run, err := findRun(target, request.RunID)
		if err != nil {
			return "", err
		}
		if run.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if run.State != domain.TargetRunRequested {
			return "", failedPrecondition(operation, "state", "plan must be bound before physical preparation advances", nil)
		}
		if !targetRunProvisioningBindingEmpty(*run) {
			return "", store.ErrIdempotencyConflict
		}
		run.ProvisioningPlanDigest = request.ProvisioningPlanDigest
		run.ProvisioningKey = request.ProvisioningKey
		run.Revision++
		run.UpdatedAt = c.clock().UTC()
		return "target_run.plan_bound", nil
	})
	if err != nil {
		return TargetRunRecord{}, err
	}
	run, err := findRun(&target, request.RunID)
	if err != nil {
		return TargetRunRecord{}, err
	}
	return *run, nil
}
