package linuxcontainer

import (
	"context"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// resumeOrCreateRuntime closes the crash window between Docker Create and the
// driver's in-memory commit. A unique exact runtime is resumed through start
// and framed guest readiness; absence creates one; ambiguity and foreign
// configuration fail closed.
func (d *Driver) resumeOrCreateRuntime(ctx context.Context, plan ContainerPlan) (string, RuntimeState, bool, error) {
	inventory, supported := d.runtime.(RuntimeInventory)
	if !supported {
		runtimeID, state, err := d.createRuntime(ctx, plan)
		return runtimeID, state, true, err
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return "", RuntimeState{}, false, domain.NewError(domain.CodeUnavailable, "linux_target.create", "inventory", "target runtime inventory failed while resuming provisioning", err)
	}
	if err := validateRuntimeInventory(states); err != nil {
		return "", RuntimeState{}, false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.create", "inventory", "target runtime inventory is ambiguous while resuming provisioning", err)
	}
	item := expectedTargetContainer{
		plan: plan,
		ref:  ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation},
	}
	candidates := targetCandidates(states, item)
	switch len(candidates) {
	case 0:
		runtimeID, state, createErr := d.createRuntime(ctx, plan)
		return runtimeID, state, true, createErr
	case 1:
		state := states[candidates[0]]
		if err := validateRuntimeIdentity(state, plan); err != nil {
			// The durable plan does not authorize mutating or deleting a foreign
			// name/label collision, so do not return its runtime ID to cleanup.
			return "", RuntimeState{}, false, err
		}
		ready, err := d.resumeExactRuntime(ctx, plan, state, "linux_target.create")
		return state.ID, ready, false, err
	default:
		return "", RuntimeState{}, false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.create", "runtime.identity", "multiple runtime resources claim the target generation", nil)
	}
}

func (d *Driver) resumeExactRuntime(ctx context.Context, plan ContainerPlan, state RuntimeState, operation string) (RuntimeState, error) {
	if err := validateRuntimeIdentity(state, plan); err != nil {
		return RuntimeState{}, err
	}
	if !state.Running {
		if err := requireStoppedRuntimeState(state, operation, dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
			return RuntimeState{}, err
		}
		if err := d.runtime.Start(ctx, state.ID); err != nil {
			return RuntimeState{}, domain.NewError(domain.CodeUnavailable, operation, "runtime.start", "could not resume the exact target runtime", err)
		}
		observed, err := d.runtime.Inspect(ctx, state.ID)
		if err != nil {
			return RuntimeState{}, domain.NewError(domain.CodeFailedPrecondition, operation, "runtime.start", "resumed target runtime did not become running", err)
		}
		if err := requireExactRuntimeID(observed, state.ID, operation); err != nil {
			return RuntimeState{}, err
		}
		if !observed.Running {
			return RuntimeState{}, domain.NewError(domain.CodeFailedPrecondition, operation, "runtime.start", "resumed target runtime did not become running", nil)
		}
		if err := validateRuntimeIdentity(observed, plan); err != nil {
			return RuntimeState{}, err
		}
		state = observed
	}
	if err := requireLiveRuntimeState(state, operation); err != nil {
		return RuntimeState{}, err
	}
	if err := d.requireGuestReadiness(ctx, state.ID, plan); err != nil {
		return RuntimeState{}, domain.NewError(domain.CodeFailedPrecondition, operation, "guest_readiness", "resumed target guest readiness failed", err)
	}
	return state, nil
}
