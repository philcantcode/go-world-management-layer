package docker

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// RecoverInterruptedExecs crosses a container stop/start boundary for the
// exact persisted generation. Docker stops every process in the container,
// including processes created by docker exec, before Start creates a fresh
// container execution boundary. The fresh framed guest probe is the readiness
// proof for the restarted daemon.
func (d *Driver) RecoverInterruptedExecs(ctx context.Context, input ports.AgentWorkspacePlan) (ports.AgentExecCrashRecovery, error) {
	const operation = "docker.recover_interrupted_execs"
	if err := requireContext(ctx, operation); err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	containerPlan, err := BuildContainerPlan(input, d.build)
	if err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}

	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	containerID, found, err := d.findProvisionContainer(ctx, input, containerPlan)
	if err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	var state ContainerState
	if found {
		state, err = d.requireOwnedContainerStopped(ctx, containerID, containerPlan, ports.StopForce, operation)
		if err != nil {
			return ports.AgentExecCrashRecovery{}, err
		}
	}

	// Workspace ownership changes, container creation, start, and guest probes
	// all happen only after the old boundary is authoritatively absent or has
	// been inspected as stopped.
	if err := prepareWorkspaceAccess(containerPlan.ExpectedWorkspaceSource, containerPlan.User); err != nil {
		return ports.AgentExecCrashRecovery{}, domain.NewError(domain.CodeFailedPrecondition, operation, "workspace_access", "workspace could not be restricted to the configured container identity", err)
	}
	if !found {
		containerID, err = d.engine.Create(ctx, containerPlan)
		if err != nil {
			return ports.AgentExecCrashRecovery{}, domain.NewError(domain.CodeUnavailable, operation, "create", "Docker create failed", err)
		}
		state, err = d.inspectProvisionContainer(ctx, containerID, containerPlan)
		if err != nil {
			return ports.AgentExecCrashRecovery{}, d.failProvisionContainer(containerID, err)
		}
	}

	state, err = d.establishFreshAgentReadiness(ctx, containerID, containerPlan, state, operation)
	if err != nil {
		return ports.AgentExecCrashRecovery{}, d.failProvisionContainer(containerID, err)
	}
	status := readyAgentStatus(containerPlan, state, d.now().UTC())
	record := workspaceRecord{plan: input, containerPlan: containerPlan, containerID: containerID, status: status}
	key := workspaceKey(containerPlan.AgentWorkspaceID, containerPlan.Generation)
	d.mu.Lock()
	d.workspaces[key] = record
	delete(d.cleanupOnly, key)
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()

	result := ports.AgentExecCrashRecovery{Status: status, PreviousBoundaryTerminated: true}
	if err := result.ValidateFor(input); err != nil {
		return ports.AgentExecCrashRecovery{}, fmt.Errorf("validate Docker exec crash recovery proof: %w", err)
	}
	return result, nil
}

// establishFreshAgentReadiness is the shared stop/create successor path for
// ordinary provisioning and crash recovery. It guarantees a running exact
// container, a successful framed guest probe, and a final running inspection.
func (d *Driver) establishFreshAgentReadiness(ctx context.Context, containerID string, plan ContainerPlan, state ContainerState, operation string) (ContainerState, error) {
	if !state.Running {
		if err := requireStoppedContainerState(state, operation, dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
			return ContainerState{}, err
		}
		if err := d.engine.Start(ctx, containerID); err != nil {
			return ContainerState{}, domain.NewError(domain.CodeUnavailable, operation, "start", "Docker start failed", err)
		}
	}
	state, err := d.requireRunningProvisionContainer(ctx, containerID, plan)
	if err != nil {
		return ContainerState{}, err
	}
	if err := d.requireGuestReadiness(ctx, containerID, plan); err != nil {
		return ContainerState{}, domain.NewError(domain.CodeFailedPrecondition, operation, "guest_protocol", "world-guest did not complete its framed readiness probe", err)
	}
	return d.requireRunningProvisionContainer(ctx, containerID, plan)
}

var _ ports.AgentExecCrashReconciler = (*Driver)(nil)
