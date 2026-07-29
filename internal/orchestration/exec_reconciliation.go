package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const interruptedExecError = "execution lost control-plane continuity during daemon restart; its previous container execution boundary was terminated"

type persistedExecRecovery struct {
	plan  ports.AgentWorkspacePlan
	execs []application.ExecRecord
}

// recoverPersistedExecs establishes one physical crash boundary per affected
// agent generation before it terminalizes any logical exec. Ordinary
// Provision is deliberately insufficient proof: a running Docker container
// can survive the daemon and continue a docker-exec child process.
func (c *Controller) recoverPersistedExecs(
	ctx context.Context,
	recoveries []persistedExecRecovery,
	report *PhysicalReconciliationReport,
) error {
	if len(recoveries) == 0 {
		return nil
	}
	reconciler, ok := c.agent.(ports.AgentExecCrashReconciler)
	if !ok {
		return missingCapability("controller.reconcile_physical", "agent_exec_crash_recovery", "agent driver cannot prove cleanup of executions interrupted by daemon loss")
	}
	for _, recovery := range recoveries {
		proof, err := reconciler.RecoverInterruptedExecs(ctx, recovery.plan)
		if err != nil {
			return fmt.Errorf("recover interrupted agent executions for %s: %w", agentPlanRefKey(recovery.plan), err)
		}
		if err := proof.ValidateFor(recovery.plan); err != nil {
			return fmt.Errorf("validate interrupted agent execution recovery for %s: %w", agentPlanRefKey(recovery.plan), err)
		}
		spec := recovery.plan.Generation.Spec()
		if err := requireReadyAgentStatus(proof.Status, spec.AgentWorkspaceID, spec.Generation); err != nil {
			return fmt.Errorf("validate interrupted agent execution recovery for %s: %w", agentPlanRefKey(recovery.plan), err)
		}
		for _, execution := range recovery.execs {
			if err := c.finalizeInterruptedExec(ctx, execution, recovery.plan.PolicyDigest.String(), report); err != nil {
				return err
			}
		}
	}
	return nil
}

// persistedExecRecoveryAgents returns the exact generations whose ordinary
// startup provisioning must be suppressed. RecoverInterruptedExecs already
// establishes their physical container and fresh readiness, and it must be the
// first host/guest mutation after a daemon restart.
func persistedExecRecoveryAgents(recoveries []persistedExecRecovery) map[string]struct{} {
	result := make(map[string]struct{}, len(recoveries))
	for _, recovery := range recoveries {
		result[agentPlanRefKey(recovery.plan)] = struct{}{}
	}
	return result
}

func persistedExecRecoveries(views []application.ResearchSessionView, expected []ports.AgentWorkspacePlan) ([]persistedExecRecovery, error) {
	plans := make(map[string]ports.AgentWorkspacePlan, len(expected))
	for _, plan := range expected {
		if err := plan.Validate(); err != nil {
			return nil, fmt.Errorf("validate persisted agent plan for exec recovery: %w", err)
		}
		key := agentPlanRefKey(plan)
		if _, duplicate := plans[key]; duplicate {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_exec_plan", "persisted agent plans contain a duplicate generation", nil)
		}
		plans[key] = plan
	}

	grouped := make(map[string][]application.ExecRecord)
	for _, view := range views {
		// Lease termination owns a stronger cleanup boundary: physical agent
		// teardown is verified during this reconciliation and the startup
		// termination stage finalizes its execs before the listener opens.
		if leaseCleanupInProgress(view.Lease) {
			continue
		}
		for _, execution := range view.Execs {
			if execution.State != domain.ExecStarting && execution.State != domain.ExecRunning {
				continue
			}
			if execution.AgentWorkspaceID != view.Agent.ID || execution.AgentGeneration != view.Agent.CurrentGeneration {
				return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_exec_generation", "nonterminal exec does not belong to the current agent generation", nil)
			}
			ref, err := agentGenerationRef(execution.AgentWorkspaceID, execution.AgentGeneration)
			if err != nil {
				return nil, err
			}
			key := agentRefKey(ref)
			if _, found := plans[key]; !found {
				return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_exec_plan", "nonterminal exec has no exact persisted current agent plan", nil)
			}
			grouped[key] = append(grouped[key], execution)
		}
	}

	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]persistedExecRecovery, 0, len(keys))
	for _, key := range keys {
		executions := grouped[key]
		sort.Slice(executions, func(i, j int) bool { return executions[i].ID < executions[j].ID })
		result = append(result, persistedExecRecovery{plan: plans[key], execs: executions})
	}
	return result, nil
}

func (c *Controller) finalizeInterruptedExec(ctx context.Context, initial application.ExecRecord, policy string, report *PhysicalReconciliationReport) error {
	current, err := c.Core.GetExec(ctx, initial.ID)
	if err != nil {
		return fmt.Errorf("reload interrupted exec %s: %w", initial.ID, err)
	}
	if current.State.Terminal() {
		return nil
	}
	if current.State != domain.ExecStarting && current.State != domain.ExecRunning {
		return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_exec_state", "interrupted exec changed to an unexpected nonterminal state", nil)
	}
	if current.AgentWorkspaceID != initial.AgentWorkspaceID || current.AgentGeneration != initial.AgentGeneration {
		return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_exec_generation", "interrupted exec scope changed during recovery", nil)
	}
	meta, err := startupExecRecoveryMeta(ctx, current, policy)
	if err != nil {
		return err
	}
	_, err = c.Core.FinalizeExec(ctx, application.FinalizeExecRequest{
		Meta: meta, ExecID: current.ID, ExpectedRevision: current.Revision,
		State: domain.ExecLost, CleanupConfirmed: true, Error: interruptedExecError,
	})
	if err != nil {
		latest, loadErr := c.Core.GetExec(ctx, current.ID)
		if loadErr != nil || !latest.State.Terminal() {
			return fmt.Errorf("finalize interrupted exec %s: %w", current.ID, errors.Join(err, loadErr))
		}
	}
	report.RecoveredExecs = append(report.RecoveredExecs, current.ID)
	return nil
}

func startupExecRecoveryMeta(ctx context.Context, execution application.ExecRecord, policy string) (application.MutationMeta, error) {
	execID, err := domain.ParseExecID(execution.ID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(policy); err != nil {
		return application.MutationMeta{}, err
	}
	return application.MutationMeta{
		IdempotencyKey:            "startup-exec-loss/" + execution.ID,
		CorrelationID:             "corr_" + execID.UUID(),
		AuthorizedPolicyReference: policy,
		Deadline:                  deadline(ctx),
	}, nil
}

func agentPlanRefKey(plan ports.AgentWorkspacePlan) string {
	spec := plan.Generation.Spec()
	return agentRefKey(ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation})
}
