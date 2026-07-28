package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// PhysicalReconciliationReport records the authoritative inventories accepted
// before physical admission opens. Removed entries were proven to correspond
// to terminal durable generations and were removed through their owning
// driver, then verified absent by a second inventory.
type PhysicalReconciliationReport struct {
	Agent                ports.AgentWorkspaceReconciliationReport
	Targets              map[domain.TargetKind]ports.TargetReconciliationReport
	RemovedAgentOrphans  []ports.AgentWorkspaceRef
	RemovedTargetOrphans map[domain.TargetKind][]ports.TargetRef
	RecoveredRuns        []string
	LostTargetOperations []string
}

type persistedPhysicalPlans struct {
	agents           []ports.AgentWorkspacePlan
	targets          map[domain.TargetKind][]ports.TargetPlan
	agentTerminal    map[string]bool
	targetTerminal   map[domain.TargetKind]map[string]bool
	observerBindings []PersistedRunObserverBinding
	runRecoveries    []persistedRunRecovery
	lostOperations   []persistedLostTargetOperation
}

type persistedRunRecovery struct {
	target application.TargetRecord
	run    application.TargetRunRecord
	plan   ports.TargetRunPlan
}

type persistedLostTargetOperation struct {
	target    application.TargetRecord
	operation application.TargetOperationRecord
	policy    string
}

// ReconcilePhysicalResources reconstructs exact current plans from durable
// Core state and the trusted resolver, re-applies physical admission, and then
// asks every configured driver for an authoritative inventory. It adopts only
// exact matches. Missing, foreign, uncertain, conflicting, and unprovable
// orphan resources fail closed.
func (c *Controller) ReconcilePhysicalResources(ctx context.Context) (PhysicalReconciliationReport, error) {
	if err := ports.RequireDeadline(ctx, "controller.reconcile_physical"); err != nil {
		return PhysicalReconciliationReport{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	report := PhysicalReconciliationReport{
		Targets:              make(map[domain.TargetKind]ports.TargetReconciliationReport),
		RemovedTargetOrphans: make(map[domain.TargetKind][]ports.TargetRef),
	}
	if c.capabilities != nil {
		if err := c.capabilities.ReconcileRunFinalizations(ctx); err != nil {
			return report, fmt.Errorf("reconcile staged run finalizations: %w", err)
		}
	}
	views, err := c.Core.ListResearchSessions(ctx)
	if err != nil {
		return report, fmt.Errorf("enumerate persisted physical ownership: %w", err)
	}
	if c.logicalOnly() {
		if err := requireNoPersistedPhysicalBindings(views); err != nil {
			return report, err
		}
		return report, nil
	}
	if err := c.requireAgentLifecycle("reconcile_physical"); err != nil {
		return report, err
	}
	plans, err := c.resolvePersistedPhysicalPlans(ctx, views)
	if err != nil {
		return report, fmt.Errorf("resolve persisted physical plans: %w", err)
	}
	if c.observers == nil {
		if len(plans.observerBindings) != 0 || len(plans.runRecoveries) != 0 {
			return report, missingCapability("controller.reconcile_physical", "run_observers", "persisted target run history requires an observer coordinator")
		}
	} else if err := c.observers.ReconcilePersistedRuns(ctx, plans.observerBindings); err != nil {
		return report, fmt.Errorf("match persisted run observers: %w", err)
	}
	if c.capabilities != nil {
		if err := c.capabilities.ReconcileRunFinalizationCompletions(ctx); err != nil {
			return report, fmt.Errorf("complete reconciled run finalizations: %w", err)
		}
	}

	agentReconciler, ok := c.agent.(ports.AgentWorkspaceReconciler)
	if !ok {
		return report, missingCapability("controller.reconcile_physical", "agent_inventory", "agent driver does not provide authoritative reconciliation")
	}
	targetReconcilers := make(map[domain.TargetKind]ports.TargetReconciler, len(c.targets))
	for kind, driver := range c.targets {
		reconciler, supported := driver.(ports.TargetReconciler)
		if !supported {
			return report, missingCapability("controller.reconcile_physical", "target_inventory", "target driver does not provide authoritative reconciliation for "+string(kind))
		}
		targetReconcilers[kind] = reconciler
	}

	report.Agent, err = agentReconciler.ReconcileAgentWorkspaces(ctx, plans.agents)
	if err != nil {
		return report, fmt.Errorf("reconcile agent workspaces: %w", err)
	}
	for _, kind := range sortedTargetKinds(c.targets) {
		targetReport, reconcileErr := targetReconcilers[kind].ReconcileTargets(ctx, plans.targets[kind])
		report.Targets[kind] = targetReport
		if reconcileErr != nil {
			return report, fmt.Errorf("reconcile %s targets: %w", kind, reconcileErr)
		}
	}

	agentOrphans, agentErr := assessAgentReconciliation(plans.agents, plans.agentTerminal, report.Agent)
	targetOrphans := make(map[domain.TargetKind][]ports.TargetRef)
	var assessmentErrors []error
	if agentErr != nil {
		assessmentErrors = append(assessmentErrors, agentErr)
	}
	for _, kind := range sortedTargetKinds(c.targets) {
		orphans, assessErr := assessTargetReconciliation(kind, plans.targets[kind], plans.targetTerminal[kind], report.Targets[kind])
		targetOrphans[kind] = orphans
		if assessErr != nil {
			assessmentErrors = append(assessmentErrors, assessErr)
		}
	}
	if err := errors.Join(assessmentErrors...); err != nil {
		return report, err
	}
	if len(agentOrphans) != 0 || countTargetRefs(targetOrphans) != 0 {
		for _, ref := range agentOrphans {
			if err := c.agent.Destroy(ctx, ref); err != nil && !domain.IsCode(err, domain.CodeNotFound) {
				return report, fmt.Errorf("remove proven terminal agent orphan %s: %w", agentRefKey(ref), err)
			}
			report.RemovedAgentOrphans = append(report.RemovedAgentOrphans, ref)
		}
		for _, kind := range sortedTargetKinds(c.targets) {
			for _, ref := range targetOrphans[kind] {
				if err := c.targets[kind].Destroy(ctx, ref); err != nil && !domain.IsCode(err, domain.CodeNotFound) {
					return report, fmt.Errorf("remove proven terminal %s target orphan %s: %w", kind, targetRefKey(ref), err)
				}
				report.RemovedTargetOrphans[kind] = append(report.RemovedTargetOrphans[kind], ref)
			}
		}

		// Destruction is not treated as proof of absence. Repeat authoritative
		// inventory and accept startup only when the cleaned resources are gone.
		report.Agent, err = agentReconciler.ReconcileAgentWorkspaces(ctx, plans.agents)
		if err != nil {
			return report, fmt.Errorf("verify agent orphan cleanup: %w", err)
		}
		if remaining, verifyErr := assessAgentReconciliation(plans.agents, plans.agentTerminal, report.Agent); verifyErr != nil || len(remaining) != 0 {
			return report, errors.Join(verifyErr, fmt.Errorf("agent orphan cleanup did not produce a clean inventory"))
		}
		for _, kind := range sortedTargetKinds(c.targets) {
			targetReport, reconcileErr := targetReconcilers[kind].ReconcileTargets(ctx, plans.targets[kind])
			report.Targets[kind] = targetReport
			if reconcileErr != nil {
				return report, fmt.Errorf("verify %s target orphan cleanup: %w", kind, reconcileErr)
			}
			remaining, verifyErr := assessTargetReconciliation(kind, plans.targets[kind], plans.targetTerminal[kind], targetReport)
			if verifyErr != nil || len(remaining) != 0 {
				return report, errors.Join(verifyErr, fmt.Errorf("%s target orphan cleanup did not produce a clean inventory", kind))
			}
		}
	}
	if err := c.recoverPersistedRuns(ctx, plans.runRecoveries, &report); err != nil {
		return report, err
	}
	if err := c.finalizeLostTargetOperations(ctx, plans.lostOperations, &report); err != nil {
		return report, err
	}
	if c.observers != nil {
		if err := c.observers.Reconcile(ctx); err != nil {
			return report, fmt.Errorf("verify run observer recovery: %w", err)
		}
	}
	return report, nil
}

func (c *Controller) recoverPersistedRuns(ctx context.Context, recoveries []persistedRunRecovery, report *PhysicalReconciliationReport) error {
	for _, recovery := range recoveries {
		driver := c.targets[recovery.target.Kind]
		crashReconciler, ok := driver.(ports.TargetRunCrashReconciler)
		if !ok {
			return missingCapability("controller.reconcile_physical", "target_run_crash_recovery", "target driver cannot prove interrupted execution cleanup for "+string(recovery.target.Kind))
		}
		meta, err := startupRunRecoveryMeta(ctx, recovery.target, recovery.run)
		if err != nil {
			return err
		}
		prepared, err := crashReconciler.RecoverInterruptedRun(ctx, recovery.plan)
		if err != nil {
			return fmt.Errorf("recover interrupted target run %s: %w", recovery.run.ID, err)
		}
		if err := validatePreparedRun(recovery.plan, prepared); err != nil {
			return fmt.Errorf("validate recovered target run %s: %w", recovery.run.ID, err)
		}
		observerStart, err := bindRunObserverStart(recovery.plan, prepared, recovery.target)
		if err != nil {
			return err
		}
		if err := c.observers.RecoverInterrupted(ctx, observerStart, recovery.run.State); err != nil {
			return fmt.Errorf("recover interrupted run observers %s: %w", recovery.run.ID, err)
		}
		latest, err := c.Core.GetTarget(ctx, recovery.target.ID)
		if err != nil {
			return err
		}
		run, err := targetRun(latest, recovery.run.ID)
		if err != nil {
			return err
		}
		if run.State.Terminal() {
			return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_run", "run became terminal during serialized startup recovery", nil)
		}
		signature, err := startupRunRecoverySignature(latest.ID, run.ID, run.ProvisioningPlanDigest)
		if err != nil {
			return err
		}
		meta, namespace, key, signature, err := c.capabilities.recoveryFinalizationIdentity(latest, run, meta, "startup_run_recovery", signature)
		if err != nil {
			return err
		}
		if _, err := c.capabilities.stopAndFinalizeRun(
			ctx, latest, run, driver, ports.StopForce, meta, namespace, key, signature,
			fmt.Errorf("target run lost control-plane continuity during daemon restart"),
		); err != nil {
			return fmt.Errorf("finalize interrupted target run %s: %w", run.ID, err)
		}
		report.RecoveredRuns = append(report.RecoveredRuns, run.ID)
	}
	return nil
}

func (c *Controller) finalizeLostTargetOperations(ctx context.Context, operations []persistedLostTargetOperation, report *PhysicalReconciliationReport) error {
	for _, lost := range operations {
		meta, err := startupOperationRecoveryMeta(ctx, lost)
		if err != nil {
			return err
		}
		_, err = c.Core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{
			Meta: meta, TargetID: lost.target.ID, OperationID: lost.operation.ID,
			ExpectedRevision: lost.operation.Revision, State: domain.TargetOperationLost,
		})
		if err != nil {
			latest, loadErr := c.Core.GetTarget(ctx, lost.target.ID)
			if loadErr != nil {
				return errors.Join(err, loadErr)
			}
			operation, findErr := targetOperation(latest, lost.operation.ID)
			if findErr != nil || !operation.State.Terminal() {
				return errors.Join(err, findErr)
			}
		}
		report.LostTargetOperations = append(report.LostTargetOperations, lost.operation.ID)
	}
	return nil
}

func startupRunRecoveryMeta(ctx context.Context, target application.TargetRecord, run application.TargetRunRecord) (application.MutationMeta, error) {
	runID, err := domain.ParseTargetRunID(run.ID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	generation, err := persistedTargetGeneration(target, run.Generation)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(generation.PolicyDigest); err != nil {
		return application.MutationMeta{}, err
	}
	return application.MutationMeta{
		IdempotencyKey: "startup-run-recovery/" + run.ID, CorrelationID: "corr_" + runID.UUID(),
		AuthorizedPolicyReference: generation.PolicyDigest, Deadline: deadline(ctx),
	}, nil
}

func startupOperationRecoveryMeta(ctx context.Context, lost persistedLostTargetOperation) (application.MutationMeta, error) {
	runID, err := domain.ParseTargetRunID(lost.operation.RunID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(lost.policy); err != nil {
		return application.MutationMeta{}, err
	}
	return application.MutationMeta{
		IdempotencyKey: "startup-operation-loss/" + lost.operation.ID, CorrelationID: "corr_" + runID.UUID(),
		AuthorizedPolicyReference: lost.policy, Deadline: deadline(ctx),
	}, nil
}

func startupRunRecoverySignature(targetID, runID, planDigest string) (string, error) {
	return requestSignature(struct {
		TargetID   string `json:"target_id"`
		RunID      string `json:"run_id"`
		PlanDigest string `json:"plan_digest"`
		Reason     string `json:"reason"`
	}{targetID, runID, planDigest, "control_plane_loss"})
}

func persistedTargetGeneration(target application.TargetRecord, generation uint64) (application.TargetGenerationRecord, error) {
	for _, candidate := range target.Generations {
		if candidate.Generation == generation {
			return candidate, nil
		}
	}
	return application.TargetGenerationRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_generation", "persisted target generation is missing", nil)
}

func (c *Controller) resolvePersistedPhysicalPlans(ctx context.Context, views []application.ResearchSessionView) (persistedPhysicalPlans, error) {
	plans := persistedPhysicalPlans{
		targets: make(map[domain.TargetKind][]ports.TargetPlan), agentTerminal: make(map[string]bool),
		targetTerminal: make(map[domain.TargetKind]map[string]bool),
	}
	for _, view := range views {
		cleanupAuthorized := leaseCleanupInProgress(view.Lease)
		for _, generation := range view.Agent.Generations {
			ref, err := agentGenerationRef(view.Agent.ID, generation.Generation)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			key := agentRefKey(ref)
			if _, duplicate := plans.agentTerminal[key]; duplicate {
				return persistedPhysicalPlans{}, fmt.Errorf("duplicate persisted agent generation %s", key)
			}
			bound, err := completeProvisioningBinding("agent generation "+key, generation.ProvisioningPlanDigest, generation.WorkspaceProvisioningKey, generation.AgentProvisioningKey)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			plans.agentTerminal[key] = bound && (generation.State.Terminal() || cleanupAuthorized)
		}
		current, err := currentAgentGeneration(view.Agent)
		if err != nil {
			return persistedPhysicalPlans{}, err
		}
		if !current.State.Terminal() && !cleanupAuthorized {
			if err := requireRecoverablePhysicalLease(view.Lease); err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("agent workspace %s: %w", view.Agent.ID, err)
			}
			currentRef, err := agentGenerationRef(view.Agent.ID, current.Generation)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			bound, err := completeProvisioningBinding("agent generation "+agentRefKey(currentRef), current.ProvisioningPlanDigest, current.WorkspaceProvisioningKey, current.AgentProvisioningKey)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			if !bound {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_plan_binding", "nonterminal persisted agent generation has no complete physical plan binding", nil)
			}
			resolved, err := c.resolver.ResolvePersistedAgent(ctx, view)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("resolve agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			plan, err := bindAgentProvisioning(application.AcquireRequest{}, resolved, view)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("bind agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			if err := c.admitAgentWorkspacePlan(ctx, plan.Agent); err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("re-admit agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			plans.agents = append(plans.agents, plan.Agent)
		}

		for _, target := range view.Targets {
			if plans.targetTerminal[target.Kind] == nil {
				plans.targetTerminal[target.Kind] = make(map[string]bool)
			}
			current, err := targetGeneration(target)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			runsByID := make(map[string]application.TargetRunRecord, len(target.Runs))
			nonterminalByGeneration := make(map[uint64]int)
			for _, run := range target.Runs {
				if _, duplicate := runsByID[run.ID]; duplicate {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_runs", "persisted target contains duplicate run identities", nil)
				}
				runsByID[run.ID] = run
				bound, err := completeProvisioningBinding("target run "+run.ID, run.ProvisioningPlanDigest, run.ProvisioningKey)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				if bound {
					runID, parseErr := domain.ParseTargetRunID(run.ID)
					if parseErr != nil {
						return persistedPhysicalPlans{}, parseErr
					}
					digest, parseErr := domain.ParseDigest(run.ProvisioningPlanDigest)
					if parseErr != nil {
						return persistedPhysicalPlans{}, parseErr
					}
					published := false
					if run.State.Terminal() {
						if c.capabilities == nil {
							return persistedPhysicalPlans{}, missingCapability("controller.reconcile_physical", "run_finalization", "terminal physical run requires bundle publication verification")
						}
						published, parseErr = c.capabilities.bundlePublicationComplete(ctx, run)
						if parseErr != nil {
							return persistedPhysicalPlans{}, fmt.Errorf("verify target run %s bundle publication: %w", run.ID, parseErr)
						}
					}
					plans.observerBindings = append(plans.observerBindings, PersistedRunObserverBinding{RunID: runID, PlanDigest: digest, State: run.State, BundlePublished: published})
				}
				if run.State.Terminal() {
					continue
				}
				if !bound {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_run_plan_binding", "nonterminal persisted target run has no complete physical plan binding", nil)
				}
				nonterminalByGeneration[run.Generation]++
				if run.Generation != current.Generation || current.State.Terminal() {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_run_generation", "nonterminal target run does not belong to the current live generation", nil)
				}
			}
			if nonterminalByGeneration[current.Generation] > 1 {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_runs", "target generation has multiple nonterminal runs", nil)
			}
			for _, operation := range target.Operations {
				if operation.State.Terminal() {
					continue
				}
				run, found := runsByID[operation.RunID]
				if !found || run.Generation != operation.Generation {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_operation", "nonterminal target operation has no matching persisted run generation", nil)
				}
				generation, err := persistedTargetGeneration(target, operation.Generation)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				plans.lostOperations = append(plans.lostOperations, persistedLostTargetOperation{target: target, operation: operation, policy: generation.PolicyDigest})
			}
			hasBoundGeneration := false
			for _, generation := range target.Generations {
				ref, err := targetGenerationRef(target.ID, generation.Generation)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				key := targetRefKey(ref)
				if _, duplicate := plans.targetTerminal[target.Kind][key]; duplicate {
					return persistedPhysicalPlans{}, fmt.Errorf("duplicate persisted %s target generation %s", target.Kind, key)
				}
				bound, err := completeProvisioningBinding("target generation "+key, generation.ProvisioningPlanDigest, generation.ProvisioningKey)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				hasBoundGeneration = hasBoundGeneration || bound
				safeDuringCleanup := cleanupAuthorized && nonterminalByGeneration[generation.Generation] == 0
				plans.targetTerminal[target.Kind][key] = bound && (generation.State.Terminal() || safeDuringCleanup)
			}
			if hasBoundGeneration && c.targets[target.Kind] == nil {
				return persistedPhysicalPlans{}, missingCapability("controller.reconcile_physical", "target_driver", "persisted physical target history has no configured inventory driver for "+string(target.Kind))
			}
			if current.State.Terminal() {
				continue
			}
			bound, err := completeProvisioningBinding("target generation "+target.ID, current.ProvisioningPlanDigest, current.ProvisioningKey)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			if !bound {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_plan_binding", "nonterminal persisted target generation has no complete physical plan binding", nil)
			}
			if cleanupAuthorized && nonterminalByGeneration[current.Generation] == 0 {
				continue
			}
			if err := requireRecoverablePhysicalLease(view.Lease); err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("target %s: %w", target.ID, err)
			}
			if c.targets[target.Kind] == nil {
				return persistedPhysicalPlans{}, missingCapability("controller.reconcile_physical", "target_driver", "persisted active target has no configured inventory driver for "+string(target.Kind))
			}
			meta := application.MutationMeta{
				IdempotencyKey: current.ProvisioningKey, AuthorizedPolicyReference: current.PolicyDigest, Deadline: deadline(ctx),
			}
			plan, err := c.resolvePersistedTargetProvisioningPlan(ctx, meta, target, current.ProvisioningKey)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("resolve target %s generation %d: %w", target.ID, current.Generation, err)
			}
			plans.targets[target.Kind] = append(plans.targets[target.Kind], plan)
			for _, run := range target.Runs {
				if run.State.Terminal() {
					continue
				}
				meta, err := startupRunRecoveryMeta(ctx, target, run)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				runPlan, err := c.resolvePersistedTargetRunPlan(ctx, meta, target, run)
				if err != nil {
					return persistedPhysicalPlans{}, fmt.Errorf("resolve target run %s: %w", run.ID, err)
				}
				plans.runRecoveries = append(plans.runRecoveries, persistedRunRecovery{target: target, run: run, plan: runPlan})
			}
		}
	}
	sort.Slice(plans.runRecoveries, func(i, j int) bool { return plans.runRecoveries[i].run.ID < plans.runRecoveries[j].run.ID })
	sort.Slice(plans.lostOperations, func(i, j int) bool {
		return plans.lostOperations[i].operation.ID < plans.lostOperations[j].operation.ID
	})
	sort.Slice(plans.observerBindings, func(i, j int) bool {
		return plans.observerBindings[i].RunID.String() < plans.observerBindings[j].RunID.String()
	})
	return plans, nil
}

func completeProvisioningBinding(resource, digest string, keys ...string) (bool, error) {
	present := digest != ""
	allKeysPresent := true
	anyKeyPresent := false
	for _, key := range keys {
		allKeysPresent = allKeysPresent && key != ""
		anyKeyPresent = anyKeyPresent || key != ""
	}
	if !present && !anyKeyPresent {
		return false, nil
	}
	if !present || !allKeysPresent {
		return false, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "plan_binding", resource+" has a partial persisted physical plan binding", nil)
	}
	if _, err := domain.ParseDigest(digest); err != nil {
		return false, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "plan_binding", resource+" has an invalid persisted physical plan digest", err)
	}
	return true, nil
}

func requireNoPersistedPhysicalBindings(views []application.ResearchSessionView) error {
	for _, view := range views {
		for _, generation := range view.Agent.Generations {
			bound, err := completeProvisioningBinding("agent generation", generation.ProvisioningPlanDigest, generation.WorkspaceProvisioningKey, generation.AgentProvisioningKey)
			if err != nil {
				return err
			}
			if bound {
				return missingCapability("controller.reconcile_physical", "agent_inventory", "persisted physical agent history requires a configured inventory driver")
			}
		}
		for _, target := range view.Targets {
			for _, generation := range target.Generations {
				bound, err := completeProvisioningBinding("target generation", generation.ProvisioningPlanDigest, generation.ProvisioningKey)
				if err != nil {
					return err
				}
				if bound {
					return missingCapability("controller.reconcile_physical", "target_inventory", "persisted physical target history requires a configured inventory driver")
				}
			}
			for _, run := range target.Runs {
				bound, err := completeProvisioningBinding("target run", run.ProvisioningPlanDigest, run.ProvisioningKey)
				if err != nil {
					return err
				}
				if bound {
					return missingCapability("controller.reconcile_physical", "target_run_recovery", "persisted physical target run history requires configured target and observer drivers")
				}
			}
		}
	}
	return nil
}

func requireRecoverablePhysicalLease(lease application.LeaseRecord) error {
	valid := lease.State == domain.LeaseActive && lease.Termination.Empty() ||
		lease.State == domain.LeaseActive && lease.Termination.Kind == application.LeaseTerminationExpiry && lease.Termination.State == application.LeaseTerminationExpiring ||
		lease.State == domain.LeaseReleasing && lease.Termination.Kind == application.LeaseTerminationRelease && lease.Termination.State == application.LeaseTerminationReleasing
	if !valid {
		return fmt.Errorf("nonterminal physical generation belongs to lease %s in %s termination=%s", lease.ID, lease.State, lease.Termination.State)
	}
	if lease.ExpiresAt.Sub(lease.CreatedAt) <= 0 {
		return fmt.Errorf("lease %s has a non-positive persisted lifetime", lease.ID)
	}
	return nil
}

func leaseCleanupInProgress(lease application.LeaseRecord) bool {
	return lease.Termination.InProgress() && (lease.State == domain.LeaseActive || lease.State == domain.LeaseReleasing)
}

type reconciliationObservation struct {
	key            string
	runtimeID      string
	classification ports.PhysicalResourceClassification
	diagnostic     string
}

func assessAgentReconciliation(expected []ports.AgentWorkspacePlan, terminal map[string]bool, report ports.AgentWorkspaceReconciliationReport) ([]ports.AgentWorkspaceRef, error) {
	expectedKeys := make([]string, 0, len(expected))
	refs := make(map[string]ports.AgentWorkspaceRef, len(report.Unclaimed))
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		expectedKeys = append(expectedKeys, agentRefKey(ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation}))
	}
	expectedObservations := make([]reconciliationObservation, 0, len(report.Expected))
	for _, item := range report.Expected {
		expectedObservations = append(expectedObservations, reconciliationObservation{key: agentRefKey(item.Ref), runtimeID: item.ContainerID, classification: item.Classification, diagnostic: item.Diagnostic})
	}
	unclaimed := make([]reconciliationObservation, 0, len(report.Unclaimed))
	for _, item := range report.Unclaimed {
		key := agentRefKey(item.Ref)
		refs[key] = item.Ref
		unclaimed = append(unclaimed, reconciliationObservation{key: key, runtimeID: item.ContainerID, classification: item.Classification, diagnostic: item.Diagnostic})
	}
	safe, err := assessReconciliation("agent workspace", expectedKeys, terminal, report.ObservedAt, expectedObservations, unclaimed, report.Conflicts)
	result := make([]ports.AgentWorkspaceRef, 0, len(safe))
	for _, key := range safe {
		result = append(result, refs[key])
	}
	return result, err
}

func assessTargetReconciliation(kind domain.TargetKind, expected []ports.TargetPlan, terminal map[string]bool, report ports.TargetReconciliationReport) ([]ports.TargetRef, error) {
	expectedKeys := make([]string, 0, len(expected))
	refs := make(map[string]ports.TargetRef, len(report.Unclaimed))
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		expectedKeys = append(expectedKeys, targetRefKey(ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}))
	}
	expectedObservations := make([]reconciliationObservation, 0, len(report.Expected))
	for _, item := range report.Expected {
		expectedObservations = append(expectedObservations, reconciliationObservation{key: targetRefKey(item.Ref), runtimeID: item.RuntimeID, classification: item.Classification, diagnostic: item.Diagnostic})
	}
	unclaimed := make([]reconciliationObservation, 0, len(report.Unclaimed))
	for _, item := range report.Unclaimed {
		key := targetRefKey(item.Ref)
		refs[key] = item.Ref
		unclaimed = append(unclaimed, reconciliationObservation{key: key, runtimeID: item.RuntimeID, classification: item.Classification, diagnostic: item.Diagnostic})
	}
	safe, err := assessReconciliation(string(kind)+" target", expectedKeys, terminal, report.ObservedAt, expectedObservations, unclaimed, report.Conflicts)
	result := make([]ports.TargetRef, 0, len(safe))
	for _, key := range safe {
		result = append(result, refs[key])
	}
	return result, err
}

func assessReconciliation(resource string, expectedKeys []string, terminal map[string]bool, observedAt time.Time, expected, unclaimed []reconciliationObservation, conflicts []ports.PhysicalResourceConflict) ([]string, error) {
	expectedSet := make(map[string]bool, len(expectedKeys))
	var issues []error
	for _, key := range expectedKeys {
		if key == "" || expectedSet[key] {
			issues = append(issues, fmt.Errorf("%s expected plan set contains an empty or duplicate identity %q", resource, key))
		}
		expectedSet[key] = true
	}
	if observedAt.IsZero() {
		issues = append(issues, fmt.Errorf("%s inventory has no observation time", resource))
	}
	seen := make(map[string]bool, len(expected))
	for _, item := range expected {
		if !item.classification.IsValid() || item.key == "" || !expectedSet[item.key] || seen[item.key] {
			issues = append(issues, fmt.Errorf("%s inventory returned an invalid, unexpected, or duplicate expected identity %q", resource, item.key))
			continue
		}
		seen[item.key] = true
		if item.classification != ports.PhysicalResourceAdopted || item.runtimeID == "" {
			issues = append(issues, fmt.Errorf("%s %s is %s: %s", resource, item.key, item.classification, item.diagnostic))
		}
	}
	for key := range expectedSet {
		if !seen[key] {
			issues = append(issues, fmt.Errorf("%s %s is missing from the inventory report", resource, key))
		}
	}
	var safe []string
	seenUnclaimed := make(map[string]bool, len(unclaimed))
	for _, item := range unclaimed {
		if !item.classification.IsValid() || item.key == "" || seenUnclaimed[item.key] {
			issues = append(issues, fmt.Errorf("%s inventory returned an invalid or duplicate unclaimed identity %q", resource, item.key))
			continue
		}
		seenUnclaimed[item.key] = true
		if item.classification != ports.PhysicalResourceOrphan || item.runtimeID == "" || !terminal[item.key] {
			issues = append(issues, fmt.Errorf("unsafe %s orphan %s is %s: %s", resource, item.key, item.classification, item.diagnostic))
			continue
		}
		safe = append(safe, item.key)
	}
	for _, conflict := range conflicts {
		issues = append(issues, fmt.Errorf("%s inventory conflict resource=%q name=%q classification=%s: %s", resource, conflict.ResourceID, conflict.Name, conflict.Classification, conflict.Diagnostic))
	}
	sort.Strings(safe)
	return safe, errors.Join(issues...)
}

func sortedTargetKinds(targets map[domain.TargetKind]ports.TargetDriver) []domain.TargetKind {
	kinds := make([]domain.TargetKind, 0, len(targets))
	for kind := range targets {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

func agentGenerationRef(id string, generation uint64) (ports.AgentWorkspaceRef, error) {
	parsed, err := domain.ParseAgentWorkspaceID(id)
	if err != nil {
		return ports.AgentWorkspaceRef{}, err
	}
	value := domain.AgentGeneration(generation)
	if !value.IsValid() {
		return ports.AgentWorkspaceRef{}, fmt.Errorf("invalid agent generation %d", generation)
	}
	return ports.AgentWorkspaceRef{ID: parsed, Generation: value}, nil
}

func targetGenerationRef(id string, generation uint64) (ports.TargetRef, error) {
	parsed, err := domain.ParseTargetID(id)
	if err != nil {
		return ports.TargetRef{}, err
	}
	value := domain.TargetGeneration(generation)
	if !value.IsValid() {
		return ports.TargetRef{}, fmt.Errorf("invalid target generation %d", generation)
	}
	return ports.TargetRef{ID: parsed, Generation: value}, nil
}

func agentRefKey(ref ports.AgentWorkspaceRef) string {
	if ref.ID.IsZero() || !ref.Generation.IsValid() {
		return ""
	}
	return fmt.Sprintf("%s/%d", ref.ID, ref.Generation)
}

func targetRefKey(ref ports.TargetRef) string {
	if ref.ID.IsZero() || !ref.Generation.IsValid() {
		return ""
	}
	return fmt.Sprintf("%s/%d", ref.ID, ref.Generation)
}

func countTargetRefs(values map[domain.TargetKind][]ports.TargetRef) int {
	var count int
	for _, refs := range values {
		count += len(refs)
	}
	return count
}
