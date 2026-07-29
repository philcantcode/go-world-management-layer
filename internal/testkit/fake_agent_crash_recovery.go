package testkit

import (
	"context"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// RecoverInterruptedExecs gives orchestration tests an explicit crash
// boundary instead of treating ordinary fake provisioning as process cleanup.
func (d *FakeAgentWorkspaceDriver) RecoverInterruptedExecs(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentExecCrashRecovery, error) {
	if err := ports.RequireDeadline(ctx, "fake_agent.recover_interrupted_execs"); err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	if err := d.faults.Check("agent.recover_execs.before"); err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	spec := plan.Generation.Spec()
	ref := ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation}
	d.mu.Lock()
	status, found := d.workspaces[ref]
	if !found {
		d.mu.Unlock()
		return ports.AgentExecCrashRecovery{}, domain.NewError(domain.CodeNotFound, "fake_agent.recover_interrupted_execs", "generation", "workspace generation is not provisioned", nil)
	}
	status.State = domain.AgentGenerationReady
	status.Ready = true
	status.ObservedAt = d.clock.Now()
	d.workspaces[ref] = status
	d.mu.Unlock()
	if err := d.faults.Check("agent.recover_execs.after"); err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	return ports.AgentExecCrashRecovery{Status: status, PreviousBoundaryTerminated: true}, nil
}

var _ ports.AgentExecCrashReconciler = (*FakeAgentWorkspaceDriver)(nil)
