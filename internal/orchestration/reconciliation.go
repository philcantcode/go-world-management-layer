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
// driver, then verified absent by a second inventory. A recovered target
// destruction remains in Targets as an expected/missing observation and is
// named in RecoveredTargetDestructions; that exception requires its exact
// generation-bound durable operation reservation. A pending reset may also
// contain exactly one missing half of its predecessor/successor pair, provided
// the other half is authoritatively adopted.
type PhysicalReconciliationReport struct {
	Agent                        ports.AgentWorkspaceReconciliationReport
	Targets                      map[domain.TargetKind]ports.TargetReconciliationReport
	RemovedAgentOrphans          []ports.AgentWorkspaceRef
	RemovedTargetOrphans         map[domain.TargetKind][]ports.TargetRef
	RecoveredExecs               []string
	RecoveredRuns                []string
	RecoveredTargetQuarantines   []ports.TargetRef
	RecoveredTargetDestructions  []ports.TargetRef
	DeferredTargetDestructions   []ports.TargetRef
	RecoveredAgentProvisionings  []ports.AgentWorkspaceRef
	RecoveredAgentRecoveries     []ports.AgentWorkspaceRef
	RecoveredTargetProvisionings []ports.TargetRef
	CompletedTargetRecoveries    []ports.TargetRef
	PendingAgentProvisionings    []ports.AgentWorkspaceRef
	PendingTargetProvisionings   []ports.TargetRef
	PendingTargetRuns            []string
	LostTargetOperations         []string
}

type persistedPhysicalPlans struct {
	agents                    []ports.AgentWorkspacePlan
	agentCleanupPlans         map[string]ports.AgentWorkspacePlan
	targets                   map[domain.TargetKind][]ports.TargetPlan
	targetCleanupPlans        map[domain.TargetKind]map[string]ports.TargetPlan
	agentTerminal             map[string]bool
	agentWorkspaces           map[string]domain.WorkspaceID
	targetTerminal            map[domain.TargetKind]map[string]bool
	agentProvisionings        []persistedAgentProvisioning
	agentRecoveries           []persistedAgentRecovery
	resolvedAgentRecoveries   []persistedResolvedAgentRecovery
	targetProvisionings       []persistedTargetProvisioning
	completedTargetRecoveries []persistedTargetRecoveryCompletion
	resolvedTargetRecoveries  []persistedResolvedTargetRecovery
	pendingAgentProvisionings []ports.AgentWorkspaceRef
	pendingTargetResets       []persistedPendingTargetReset
	pendingTargetRuns         []string
	observerBindings          []PersistedRunObserverBinding
	runRecoveries             []persistedRunRecovery
	lostOperations            []persistedLostTargetOperation
	targetQuarantines         []persistedTargetQuarantine
	targetDestructions        []persistedTargetDestruction
}

type persistedAgentProvisioning struct {
	view         application.ResearchSessionView
	generation   application.AgentGenerationRecord
	plan         AgentProvisioningPlan
	meta         application.MutationMeta
	needsBinding bool
}

// persistedAgentRecovery is the durable remainder of the live replacement
// saga. The predecessor plan is always reconstructible from its frozen
// generation binding. The successor plan exists only after the original
// recovery request durably bound it.
type persistedAgentRecovery struct {
	view             application.ResearchSessionView
	generation       application.AgentGenerationRecord
	incident         application.IncidentRecord
	previousPlan     ports.AgentWorkspacePlan
	previousResource agentPhysicalResource
	currentPlan      AgentProvisioningPlan
	meta             application.MutationMeta
	bound            bool
}

type persistedResolvedAgentRecovery struct {
	incident     application.IncidentRecord
	current      ports.AgentWorkspaceRef
	previous     ports.AgentWorkspaceRef
	previousPlan ports.AgentWorkspacePlan
}

type persistedTargetProvisioning struct {
	target       application.TargetRecord
	generation   application.TargetGenerationRecord
	plan         ports.TargetPlan
	meta         application.MutationMeta
	needsBinding bool
}

// persistedTargetRecoveryCompletion closes the crash window after a recovery
// reset reached Ready but before its linked incident was resolved.
type persistedTargetRecoveryCompletion struct {
	target       application.TargetRecord
	generation   application.TargetGenerationRecord
	incident     application.IncidentRecord
	current      ports.TargetRef
	previous     ports.TargetRef
	previousPlan ports.TargetPlan
}

type persistedResolvedTargetRecovery struct {
	incident     application.IncidentRecord
	current      ports.TargetRef
	previous     ports.TargetRef
	previousPlan ports.TargetPlan
	kind         domain.TargetKind
}

type persistedPendingTargetReset struct {
	target          application.TargetRecord
	current         ports.TargetRef
	previous        ports.TargetRef
	currentExpected bool
}

type persistedRunRecovery struct {
	target application.TargetRecord
	run    application.TargetRunRecord
	plan   ports.TargetRunPlan
}

func interruptedRunRecoveryTargets(recoveries []persistedRunRecovery) map[domain.TargetKind]map[string]bool {
	result := make(map[domain.TargetKind]map[string]bool)
	for _, recovery := range recoveries {
		spec := recovery.plan.Run.Spec()
		kind := recovery.target.Kind
		if result[kind] == nil {
			result[kind] = make(map[string]bool)
		}
		result[kind][targetRefKey(ports.TargetRef{ID: spec.TargetID, Generation: spec.TargetGeneration})] = true
	}
	return result
}

type persistedLostTargetOperation struct {
	target    application.TargetRecord
	operation application.TargetOperationRecord
	policy    string
}

type persistedTargetDestruction struct {
	target      application.TargetRecord
	generation  application.TargetGenerationRecord
	ref         ports.TargetRef
	reservation operationReservation
}

type persistedTargetQuarantine struct {
	target      application.TargetRecord
	ref         ports.TargetRef
	reservation operationReservation
	runs        []persistedRunRecovery
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
	if err := c.closeAdmissionForDueLeaseExpiries(ctx); err != nil {
		return report, fmt.Errorf("close admission for due lease expiries: %w", err)
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
	report.PendingAgentProvisionings = append(report.PendingAgentProvisionings, plans.pendingAgentProvisionings...)
	report.PendingTargetRuns = append(report.PendingTargetRuns, plans.pendingTargetRuns...)
	for _, pending := range plans.pendingTargetResets {
		report.PendingTargetProvisionings = append(report.PendingTargetProvisionings, pending.current)
	}
	execRecoveries, err := persistedExecRecoveries(views, plans.agents)
	if err != nil {
		return report, err
	}
	// An interrupted docker-exec child may still be running in an otherwise
	// healthy container. Cross its destructive stop boundary before ordinary
	// provisioning can prepare the workspace or perform a readiness probe.
	if err := c.recoverPersistedExecs(ctx, execRecoveries, &report); err != nil {
		return report, err
	}
	if err := c.resumePersistedProvisioning(ctx, plans, persistedExecRecoveryAgents(execRecoveries), &report); err != nil {
		return report, err
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

	if _, ok := c.agent.(ports.AgentWorkspaceReconciler); !ok {
		return report, missingCapability("controller.reconcile_physical", "agent_inventory", "agent driver does not provide authoritative reconciliation")
	}
	for kind, driver := range c.targets {
		if _, ok := driver.(ports.TargetReconciler); !ok {
			return report, missingCapability("controller.reconcile_physical", "target_inventory", "target driver does not provide authoritative reconciliation for "+string(kind))
		}
	}

	initialAgentRequest := startupAgentInventoryRequest(plans, true, true)
	report.Agent, err = reconcileAgentInventory(ctx, c.agent, initialAgentRequest)
	if err != nil {
		return report, fmt.Errorf("reconcile agent workspaces: %w", err)
	}
	for _, kind := range sortedTargetKinds(c.targets) {
		targetReport, reconcileErr := reconcileTargetInventory(ctx, c.targets[kind], startupTargetInventoryRequest(plans, kind, true, true))
		report.Targets[kind] = targetReport
		if reconcileErr != nil {
			return report, fmt.Errorf("reconcile %s targets: %w", kind, reconcileErr)
		}
	}
	if err := verifyResolvedRecoveryPairs(plans, report.Agent, report.Targets); err != nil {
		return report, err
	}
	if err := c.resumePersistedAgentRecoveries(ctx, plans, report.Agent, &report); err != nil {
		return report, err
	}
	if err := c.completePersistedTargetRecoveries(ctx, plans, report.Targets, &report); err != nil {
		return report, err
	}

	finalAgentRequest := startupAgentInventoryRequest(plans, false, true)
	finalAgentPlans := allAgentInventoryPlans(finalAgentRequest)
	if len(plans.agentRecoveries) != 0 || len(plans.completedTargetRecoveries) != 0 || len(plans.resolvedAgentRecoveries) != 0 || len(plans.resolvedTargetRecoveries) != 0 {
		// Recovery inventory includes predecessor plans solely to authorize a
		// safe retirement or retention decision. Re-inventory the converged set
		// before generic assessment so no stale pre-mutation observation can
		// open startup admission.
		report.Agent, err = reconcileAgentInventory(ctx, c.agent, finalAgentRequest)
		if err != nil {
			return report, fmt.Errorf("verify recovered agent workspaces: %w", err)
		}
		for _, kind := range sortedTargetKinds(c.targets) {
			targetReport, reconcileErr := reconcileTargetInventory(ctx, c.targets[kind], startupTargetInventoryRequest(plans, kind, false, true))
			report.Targets[kind] = targetReport
			if reconcileErr != nil {
				return report, fmt.Errorf("verify recovered %s targets: %w", kind, reconcileErr)
			}
		}
	}
	if err := c.resumePersistedTargetQuarantines(ctx, plans, &report); err != nil {
		return report, err
	}
	destroyedMissing, err := c.resumePersistedTargetDestructions(ctx, plans, &report)
	if err != nil {
		return report, err
	}
	pendingResetSafety, err := requireSafePendingTargetResets(plans.pendingTargetResets, report.Targets)
	if err != nil {
		return report, err
	}
	if err := authorizePendingTargetCleanup(&plans, pendingResetSafety.cleanupRequired); err != nil {
		return report, err
	}
	allowedTargetMissing := mergeAllowedTargetMissing(destroyedMissing, pendingResetSafety.allowedMissing)
	allowedStoppedRecovery := interruptedRunRecoveryTargets(plans.runRecoveries)

	agentOrphans, agentErr := assessAgentReconciliationAllowRetained(finalAgentPlans, plans.agentTerminal, retainedPendingAgentPredecessors(plans), report.Agent)
	if agentErr == nil {
		workspaceOrphans, workspaceErr := c.terminalAgentWorkspaceCleanupCandidates(ctx, plans, report.Agent)
		agentOrphans = append(agentOrphans, workspaceOrphans...)
		agentErr = workspaceErr
	}
	targetOrphans := make(map[domain.TargetKind][]ports.TargetRef)
	var assessmentErrors []error
	if agentErr != nil {
		assessmentErrors = append(assessmentErrors, agentErr)
	}
	for _, kind := range sortedTargetKinds(c.targets) {
		expected := allTargetInventoryPlans(startupTargetInventoryRequest(plans, kind, false, true))
		orphans, assessErr := assessTargetReconciliationWithRecovery(kind, expected, plans.targetTerminal[kind], allowedTargetMissing[kind], allowedStoppedRecovery[kind], report.Targets[kind])
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
			workspaceID, ok := plans.agentWorkspaces[agentRefKey(ref)]
			if !ok {
				return report, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_orphan", "proven terminal agent orphan has no persisted workspace identity", nil)
			}
			if err := c.destroyAgentAndWorkspace(ctx, ref, workspaceID, ports.StopForce); err != nil {
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
		// inventory with the exact terminal plans still in scope so both runtime
		// resources and plan-owned local residue must be reported gone.
		verifyAgentRequest := startupAgentInventoryRequest(plans, false, true)
		verifyAgentPlans := allAgentInventoryPlans(verifyAgentRequest)
		report.Agent, err = reconcileAgentInventory(ctx, c.agent, verifyAgentRequest)
		if err != nil {
			return report, fmt.Errorf("verify agent orphan cleanup: %w", err)
		}
		if remaining, verifyErr := assessAgentReconciliationAllowRetained(verifyAgentPlans, plans.agentTerminal, retainedPendingAgentPredecessors(plans), report.Agent); verifyErr != nil || len(remaining) != 0 {
			return report, errors.Join(verifyErr, fmt.Errorf("agent orphan cleanup did not produce a clean inventory"))
		}
		verifiedTargetPlans := make(map[domain.TargetKind][]ports.TargetPlan, len(c.targets))
		for _, kind := range sortedTargetKinds(c.targets) {
			verifyTargetRequest := startupTargetInventoryRequest(plans, kind, false, true)
			verifiedTargetPlans[kind] = allTargetInventoryPlans(verifyTargetRequest)
			targetReport, reconcileErr := reconcileTargetInventory(ctx, c.targets[kind], verifyTargetRequest)
			report.Targets[kind] = targetReport
			if reconcileErr != nil {
				return report, fmt.Errorf("verify %s target orphan cleanup: %w", kind, reconcileErr)
			}
		}
		verifiedPendingSafety, verifyPendingErr := requireSafePendingTargetResets(plans.pendingTargetResets, report.Targets)
		if verifyPendingErr != nil {
			return report, verifyPendingErr
		}
		verifiedAllowedMissing := mergeAllowedTargetMissing(destroyedMissing, verifiedPendingSafety.allowedMissing)
		for _, kind := range sortedTargetKinds(c.targets) {
			remaining, verifyErr := assessTargetReconciliationWithRecovery(kind, verifiedTargetPlans[kind], plans.targetTerminal[kind], verifiedAllowedMissing[kind], allowedStoppedRecovery[kind], report.Targets[kind])
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

// resumePersistedTargetQuarantines closes every quarantine crash window in
// the same order as the live saga: close admission, finish evidence-bearing
// run finalization, establish physical containment, then commit quarantine.
// Durable containment may never predate a nonterminal run's finalization. The
// target remains an expected owned resource throughout; absence is never
// accepted as quarantine evidence.
func (c *Controller) resumePersistedTargetQuarantines(
	ctx context.Context,
	plans persistedPhysicalPlans,
	report *PhysicalReconciliationReport,
) error {
	if len(plans.targetQuarantines) == 0 {
		return nil
	}
	if c.capabilities == nil {
		return missingCapability("controller.reconcile_physical", "target_quarantine_recovery", "persisted quarantine intent requires lifecycle capabilities")
	}
	affected := make(map[domain.TargetKind]bool)
	for _, recovery := range plans.targetQuarantines {
		observation, err := exactTargetObservation(report.Targets[recovery.target.Kind], recovery.ref)
		if err != nil {
			return fmt.Errorf("resume target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		if observation.Classification != ports.PhysicalResourceAdopted || observation.RuntimeID == "" {
			return fmt.Errorf("resume target quarantine %s: physical resource is %s: %s", targetRefKey(recovery.ref), targetObservationSummary(observation), observation.Diagnostic)
		}
		if _, contained := c.capabilities.targetQuarantineContainment(recovery.reservation); contained {
			if _, err := c.capabilities.requireTargetQuarantineRunsFinalized(ctx, recovery.reservation); err != nil {
				return fmt.Errorf("verify pre-contained target quarantine %s: %w", targetRefKey(recovery.ref), err)
			}
		}
		if _, err := c.capabilities.closeTargetQuarantineAdmission(ctx, recovery.target.Kind, recovery.reservation); err != nil {
			return fmt.Errorf("close admission for target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		// Crash recovery is cleanup-only: TargetRunCrashReconciler is
		// contractually forbidden from restarting specimen execution. Finalize
		// any interrupted run before target-wide quarantine so strict real
		// drivers can establish their ordinary evidence-bearing stop boundary.
		if err := c.recoverPersistedRuns(ctx, recovery.runs, report); err != nil {
			return fmt.Errorf("finalize runs for target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		if _, err := c.capabilities.requireTargetQuarantineRunsFinalized(ctx, recovery.reservation); err != nil {
			return fmt.Errorf("verify finalized runs for target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		_, containment, err := c.capabilities.ensureTargetQuarantineContained(ctx, recovery.target.Kind, recovery.reservation)
		if err != nil {
			return fmt.Errorf("resume target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		if _, err := c.capabilities.commitTargetQuarantine(ctx, recovery.reservation, containment); err != nil {
			return fmt.Errorf("commit target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		affected[recovery.target.Kind] = true
		report.RecoveredTargetQuarantines = append(report.RecoveredTargetQuarantines, recovery.ref)
	}
	for _, kind := range sortedTargetKinds(c.targets) {
		if !affected[kind] {
			continue
		}
		verified, err := reconcileTargetInventory(ctx, c.targets[kind], startupTargetInventoryRequest(plans, kind, false, true))
		report.Targets[kind] = verified
		if err != nil {
			return fmt.Errorf("verify resumed %s target quarantine: %w", kind, err)
		}
	}
	for _, recovery := range plans.targetQuarantines {
		observation, err := exactTargetObservation(report.Targets[recovery.target.Kind], recovery.ref)
		if err != nil {
			return fmt.Errorf("verify resumed target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		if observation.Classification != ports.PhysicalResourceAdopted || observation.RuntimeID == "" {
			return fmt.Errorf("verify resumed target quarantine %s: preserved resource is %s: %s", targetRefKey(recovery.ref), targetObservationSummary(observation), observation.Diagnostic)
		}
		latest, err := c.Core.GetTarget(ctx, recovery.target.ID)
		if err != nil {
			return fmt.Errorf("reload target quarantine %s: %w", targetRefKey(recovery.ref), err)
		}
		generation, err := persistedTargetGeneration(latest, uint64(recovery.ref.Generation))
		if err != nil {
			return err
		}
		if latest.CurrentGeneration != uint64(recovery.ref.Generation) || generation.State != domain.TargetGenerationQuarantined {
			return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_quarantine", "recovered containment was not committed to the exact current generation", nil)
		}
	}
	return nil
}

// resumePersistedTargetDestructions closes the one intentional missing-target
// window in startup reconciliation. A Resettable state alone is never enough
// authority: only the exact durable destroy_target reservation permits an
// adopted or already-missing generation to be destroyed and committed.
func (c *Controller) resumePersistedTargetDestructions(
	ctx context.Context,
	plans persistedPhysicalPlans,
	report *PhysicalReconciliationReport,
) (map[domain.TargetKind]map[string]bool, error) {
	allowedMissing := make(map[domain.TargetKind]map[string]bool)
	if len(plans.targetDestructions) == 0 {
		return allowedMissing, nil
	}
	affected := make(map[domain.TargetKind]bool)
	eligible := make([]persistedTargetDestruction, 0, len(plans.targetDestructions))
	for _, recovery := range plans.targetDestructions {
		observation, err := exactTargetObservation(report.Targets[recovery.target.Kind], recovery.ref)
		if err != nil {
			return nil, fmt.Errorf("resume target destruction %s: %w", targetRefKey(recovery.ref), err)
		}
		switch observation.Classification {
		case ports.PhysicalResourceAdopted:
			if observation.RuntimeID == "" {
				return nil, fmt.Errorf("resume target destruction %s: adopted resource has no runtime identity", targetRefKey(recovery.ref))
			}
		case ports.PhysicalResourceMissing:
			if observation.RuntimeID != "" {
				return nil, fmt.Errorf("resume target destruction %s: missing resource unexpectedly has runtime identity", targetRefKey(recovery.ref))
			}
		default:
			return nil, fmt.Errorf("resume target destruction %s: inventory classification is %s: %s", targetRefKey(recovery.ref), observation.Classification, observation.Diagnostic)
		}
		latest, err := c.Core.GetTarget(ctx, recovery.target.ID)
		if err != nil {
			return nil, fmt.Errorf("reload target destruction %s: %w", targetRefKey(recovery.ref), err)
		}
		if latest.CurrentGeneration != uint64(recovery.ref.Generation) {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_destruction", "durable destroy intent no longer identifies the current generation", nil)
		}
		generation, err := persistedTargetGeneration(latest, uint64(recovery.ref.Generation))
		if err != nil {
			return nil, err
		}
		if generation.State == domain.TargetGenerationReady {
			meta, err := startupTargetDestructionMeta(ctx, latest, generation, recovery.reservation, "resettable")
			if err != nil {
				return nil, err
			}
			latest, err = c.Core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{
				Meta: meta, TargetID: latest.ID, Generation: generation.Generation,
				ExpectedRevision: generation.Revision, State: domain.TargetGenerationResettable,
			})
			if err != nil {
				return nil, fmt.Errorf("resume target destruction %s resettable boundary: %w", targetRefKey(recovery.ref), err)
			}
			generation, err = persistedTargetGeneration(latest, uint64(recovery.ref.Generation))
			if err != nil {
				return nil, err
			}
		}
		if generation.State != domain.TargetGenerationResettable {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_destruction", "durable destroy intent identifies a generation outside ready/resettable", nil)
		}
		// A run can linearize after the original RPC's reservation and before
		// its Ready -> Resettable admission barrier. Preserve the generation
		// until that run has been authoritatively finalized; never let durable
		// intent alone authorize deletion of live work.
		if err := requireNoNonterminalTargetRuns(latest, uint64(recovery.ref.Generation)); err != nil {
			report.DeferredTargetDestructions = append(report.DeferredTargetDestructions, recovery.ref)
			continue
		}
		if err := c.targets[recovery.target.Kind].Destroy(ctx, recovery.ref); err != nil && !domain.IsCode(err, domain.CodeNotFound) {
			return nil, fmt.Errorf("resume target destruction %s: %w", targetRefKey(recovery.ref), err)
		}
		eligible = append(eligible, recovery)
		affected[recovery.target.Kind] = true
	}

	for _, kind := range sortedTargetKinds(c.targets) {
		if !affected[kind] {
			continue
		}
		verified, err := reconcileTargetInventory(ctx, c.targets[kind], startupTargetInventoryRequest(plans, kind, false, true))
		report.Targets[kind] = verified
		if err != nil {
			return nil, fmt.Errorf("verify resumed %s target destruction: %w", kind, err)
		}
	}
	for _, recovery := range eligible {
		observation, err := exactTargetObservation(report.Targets[recovery.target.Kind], recovery.ref)
		if err != nil {
			return nil, fmt.Errorf("verify resumed target destruction %s: %w", targetRefKey(recovery.ref), err)
		}
		if observation.Classification != ports.PhysicalResourceMissing || observation.RuntimeID != "" || observation.CleanupRequired {
			return nil, fmt.Errorf("verify resumed target destruction %s: physical cleanup remains %s: %s", targetRefKey(recovery.ref), targetObservationSummary(observation), observation.Diagnostic)
		}
		latest, err := c.Core.GetTarget(ctx, recovery.target.ID)
		if err != nil {
			return nil, fmt.Errorf("reload target destruction %s: %w", targetRefKey(recovery.ref), err)
		}
		if latest.CurrentGeneration != uint64(recovery.ref.Generation) {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_destruction", "durable destroy intent no longer identifies the current generation", nil)
		}
		generation, err := persistedTargetGeneration(latest, uint64(recovery.ref.Generation))
		if err != nil {
			return nil, err
		}
		if generation.State != domain.TargetGenerationDestroyed {
			if generation.State != domain.TargetGenerationResettable {
				return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_destruction", "durable destroy intent identifies a generation outside resettable/destroyed", nil)
			}
			meta, err := startupTargetDestructionMeta(ctx, latest, generation, recovery.reservation, "destroyed")
			if err != nil {
				return nil, err
			}
			if _, err := c.Core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{
				Meta: meta, TargetID: latest.ID, Generation: generation.Generation,
				ExpectedRevision: generation.Revision, State: domain.TargetGenerationDestroyed,
			}); err != nil {
				return nil, fmt.Errorf("commit resumed target destruction %s: %w", targetRefKey(recovery.ref), err)
			}
		}
		if allowedMissing[recovery.target.Kind] == nil {
			allowedMissing[recovery.target.Kind] = make(map[string]bool)
		}
		allowedMissing[recovery.target.Kind][targetRefKey(recovery.ref)] = true
		report.RecoveredTargetDestructions = append(report.RecoveredTargetDestructions, recovery.ref)
	}
	return allowedMissing, nil
}

func exactTargetObservation(report ports.TargetReconciliationReport, ref ports.TargetRef) (ports.TargetReconciliation, error) {
	if report.ObservedAt.IsZero() {
		return ports.TargetReconciliation{}, fmt.Errorf("target inventory has no observation time")
	}
	var result ports.TargetReconciliation
	count := 0
	for _, observation := range report.Expected {
		if observation.Ref != ref {
			continue
		}
		result = observation
		count++
	}
	if count != 1 || !result.Classification.IsValid() {
		return ports.TargetReconciliation{}, fmt.Errorf("target inventory contains %d valid observations for the expected identity", count)
	}
	return result, nil
}

func startupTargetDestructionMeta(ctx context.Context, target application.TargetRecord, generation application.TargetGenerationRecord, reservation operationReservation, stage string) (application.MutationMeta, error) {
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(generation.PolicyDigest); err != nil {
		return application.MutationMeta{}, err
	}
	if stage != "resettable" && stage != "destroyed" {
		return application.MutationMeta{}, domain.NewError(domain.CodeInvalidArgument, "controller.reconcile_physical", "target_destruction", "startup destruction stage is invalid", nil)
	}
	identity := domain.NewDigest([]byte(reservation.Namespace + "\x00" + reservation.ResourceID + "\x00" + reservation.IdempotencyKey + "\x00" + reservation.Signature + "\x00" + stage))
	return application.MutationMeta{
		IdempotencyKey: "startup-target-destruction/" + identity.String(),
		CorrelationID:  "corr_" + targetID.UUID(), AuthorizedPolicyReference: generation.PolicyDigest,
		Deadline: deadline(ctx),
	}, nil
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
		agentCleanupPlans: make(map[string]ports.AgentWorkspacePlan), targetCleanupPlans: make(map[domain.TargetKind]map[string]ports.TargetPlan),
		agentWorkspaces: make(map[string]domain.WorkspaceID), targetTerminal: make(map[domain.TargetKind]map[string]bool),
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
			if bound {
				workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				plans.agentWorkspaces[key] = workspaceID
			}
			plans.agentTerminal[key] = bound && (generation.State.Terminal() || cleanupAuthorized)
			if plans.agentTerminal[key] {
				historical, err := c.resolvePersistedAgentGenerationPlan(ctx, view, generation)
				if err != nil {
					return persistedPhysicalPlans{}, fmt.Errorf("resolve terminal agent workspace %s: %w", key, err)
				}
				plans.agentCleanupPlans[key] = historical.Agent
			}
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
			initialUnbound := !bound && current.Generation == 1 && current.Previous == 0 && current.RecoveryIncident == "" && current.State == domain.AgentGenerationProvisioning
			recoveryLinked := current.Previous != 0 || current.RecoveryIncident != ""
			var resolvedRecovery *persistedResolvedAgentRecovery
			if recoveryLinked && (current.Previous == 0 || current.RecoveryIncident == "") {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "current generation has a partial recovery linkage", nil)
			}
			if recoveryLinked {
				incident, err := requirePersistedAgentRecoveryIncident(view, current)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				if incident.State == domain.IncidentRecovering {
					previousGeneration, err := persistedAgentGeneration(view.Agent, current.Previous)
					if err != nil {
						return persistedPhysicalPlans{}, err
					}
					previousPlan, err := c.resolvePersistedAgentGenerationPlan(ctx, view, previousGeneration)
					if err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("resolve agent workspace %s recovery predecessor generation %d: %w", view.Agent.ID, previousGeneration.Generation, err)
					}
					previousResource, err := requirePreviousAgentResource(view.Agent)
					if err != nil {
						return persistedPhysicalPlans{}, err
					}
					plans.agentTerminal[agentRefKey(previousResource.ref)] = false
					recovery := persistedAgentRecovery{
						view: view, generation: current, incident: incident,
						previousPlan: previousPlan.Agent, previousResource: previousResource, bound: bound,
					}
					if !bound {
						if current.State != domain.AgentGenerationProvisioning {
							return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_plan_binding", "unbound recovery generation is beyond the bindable provisioning state", nil)
						}
						plans.pendingAgentProvisionings = append(plans.pendingAgentProvisionings, currentRef)
						plans.agentRecoveries = append(plans.agentRecoveries, recovery)
						continue
					}
					resolved, err := c.resolver.ResolvePersistedAgent(ctx, view)
					if err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("resolve agent workspace %s recovery generation %d: %w", view.Agent.ID, current.Generation, err)
					}
					plan, err := bindAgentProvisioning(application.AcquireRequest{Meta: application.MutationMeta{IdempotencyKey: current.AgentProvisioningKey}}, resolved, view)
					if err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("bind agent workspace %s recovery generation %d: %w", view.Agent.ID, current.Generation, err)
					}
					if err := c.admitAgentWorkspacePlan(ctx, plan.Agent); err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("re-admit agent workspace %s recovery generation %d: %w", view.Agent.ID, current.Generation, err)
					}
					meta, err := startupProvisioningMeta(ctx, current.AgentProvisioningKey, view.Agent.ID, current.PolicyDigest, current.Generation)
					if err != nil {
						return persistedPhysicalPlans{}, err
					}
					recovery.currentPlan, recovery.meta = plan, meta
					plans.agents = append(plans.agents, plan.Agent)
					plans.agentRecoveries = append(plans.agentRecoveries, recovery)
					continue
				}
				if incident.State != domain.IncidentResolved {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "linked recovery incident is neither recovering nor resolved", nil)
				}
				if current.State != domain.AgentGenerationReady && current.State != domain.AgentGenerationRunning {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "resolved recovery incident has an incomplete successor generation", nil)
				}
				if !containsExactString(incident.RecoveryActions, "physical-agent:recreate") {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "resolved recovery incident lacks the exact physical completion action", nil)
				}
				previousGeneration, err := persistedAgentGeneration(view.Agent, current.Previous)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				previousPlan, err := c.resolvePersistedAgentGenerationPlan(ctx, view, previousGeneration)
				if err != nil {
					return persistedPhysicalPlans{}, fmt.Errorf("resolve resolved agent recovery predecessor generation %d: %w", previousGeneration.Generation, err)
				}
				previousResource, err := requirePreviousAgentResource(view.Agent)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				if !plans.agentTerminal[agentRefKey(previousResource.ref)] {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "resolved recovery predecessor is not a bound terminal generation", nil)
				}
				resolvedRecovery = &persistedResolvedAgentRecovery{incident: incident, previous: previousResource.ref, previousPlan: previousPlan.Agent}
			}
			if !bound && !initialUnbound {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_plan_binding", "unbound current generation is not an initial bindable provisioning state", nil)
			}
			resolved, err := c.resolver.ResolvePersistedAgent(ctx, view)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("resolve agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			planRoot := current.AgentProvisioningKey
			if initialUnbound {
				planRoot = view.Session.AcquisitionIdempotencyKey
			}
			plan, err := bindAgentProvisioning(application.AcquireRequest{Meta: application.MutationMeta{IdempotencyKey: planRoot}}, resolved, view)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("bind agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			if err := c.admitAgentWorkspacePlan(ctx, plan.Agent); err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("re-admit agent workspace %s generation %d: %w", view.Agent.ID, current.Generation, err)
			}
			meta, err := startupProvisioningMeta(ctx, planRoot, view.Agent.ID, current.PolicyDigest, current.Generation)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			plans.agents = append(plans.agents, plan.Agent)
			if resolvedRecovery != nil {
				resolvedRecovery.current = currentRef
				plans.resolvedAgentRecoveries = append(plans.resolvedAgentRecoveries, *resolvedRecovery)
			} else {
				switch current.State {
				case domain.AgentGenerationProvisioning, domain.AgentGenerationBooting, domain.AgentGenerationReady, domain.AgentGenerationRunning:
					plans.agentProvisionings = append(plans.agentProvisionings, persistedAgentProvisioning{
						view: view, generation: current, plan: plan, meta: meta, needsBinding: initialUnbound,
					})
				}
			}
		}

		for _, target := range view.Targets {
			if plans.targetTerminal[target.Kind] == nil {
				plans.targetTerminal[target.Kind] = make(map[string]bool)
			}
			if plans.targetCleanupPlans[target.Kind] == nil {
				plans.targetCleanupPlans[target.Kind] = make(map[string]ports.TargetPlan)
			}
			current, err := targetGeneration(target)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			var recoveringIncident *application.IncidentRecord
			var resolvedRecovery *persistedResolvedTargetRecovery
			if current.RecoveryIncident != "" {
				if current.Previous == 0 {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "current generation has an incident without a predecessor", nil)
				}
				incident, err := requirePersistedTargetRecoveryIncident(view, target, current)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				switch incident.State {
				case domain.IncidentRecovering:
					recoveringIncident = &incident
				case domain.IncidentResolved:
					if current.State != domain.TargetGenerationReady && current.State != domain.TargetGenerationResettable && !current.State.Terminal() {
						return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "resolved recovery incident has an incomplete successor generation", nil)
					}
					strategy, strategyErr := persistedTargetRecoveryStrategy(incident)
					if strategyErr != nil {
						return persistedPhysicalPlans{}, strategyErr
					}
					if !containsExactString(incident.RecoveryActions, "physical-target:"+strategy) {
						return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "resolved recovery incident lacks the exact physical completion action", nil)
					}
					if current.State == domain.TargetGenerationReady || current.State == domain.TargetGenerationResettable {
						previousGeneration, previousErr := persistedTargetGeneration(target, current.Previous)
						if previousErr != nil {
							return persistedPhysicalPlans{}, previousErr
						}
						previousPlan, previousErr := c.resolvePersistedTargetGenerationPlan(ctx, target, previousGeneration)
						if previousErr != nil {
							return persistedPhysicalPlans{}, fmt.Errorf("resolve resolved target recovery predecessor generation %d: %w", previousGeneration.Generation, previousErr)
						}
						resolvedRecovery = &persistedResolvedTargetRecovery{
							incident: incident, kind: target.Kind,
							previous: ports.TargetRef{ID: previousPlan.Target.ID(), Generation: previousPlan.Generation.Spec().Generation}, previousPlan: previousPlan,
						}
					}
				default:
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "linked recovery incident is neither recovering nor resolved", nil)
				}
				if recoveringIncident != nil && current.State != domain.TargetGenerationProvisioning && current.State != domain.TargetGenerationInstrumenting && current.State != domain.TargetGenerationReady {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "recovering incident has a successor outside the resumable lifecycle", nil)
				}
			}
			preserveQuarantine := current.State == domain.TargetGenerationQuarantined && !cleanupAuthorized
			var quarantineReservation operationReservation
			quarantining := false
			if c.capabilities != nil && !current.State.Terminal() {
				if reservation, found := c.capabilities.operationReservation("quarantine_target", target.ID, current.Generation); found {
					if reservation.Quarantine == nil || reservation.Quarantine.CommitMeta.AuthorizedPolicyReference != current.PolicyDigest {
						return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_quarantine", "durable quarantine intent does not match the current generation policy", nil)
					}
					quarantineReservation, quarantining = reservation, true
				}
			}
			var destructionReservation operationReservation
			destroying := false
			if c.capabilities != nil && (current.State == domain.TargetGenerationReady || current.State == domain.TargetGenerationResettable) {
				if reservation, found := c.capabilities.operationReservation("destroy_target", target.ID, current.Generation); found {
					destructionReservation, destroying = reservation, true
				}
			}
			if quarantining && destroying {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_lifecycle", "current target generation has conflicting quarantine and destruction intents", nil)
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
				nonterminalByGeneration[run.Generation]++
				if run.Generation != current.Generation || current.State.Terminal() {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_run_generation", "nonterminal target run does not belong to the current live generation", nil)
				}
				if !bound && run.State != domain.TargetRunRequested {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_run_plan_binding", "unbound target run is beyond the bindable requested state", nil)
				}
				if !bound {
					plans.pendingTargetRuns = append(plans.pendingTargetRuns, run.ID)
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
				if !quarantining {
					plans.lostOperations = append(plans.lostOperations, persistedLostTargetOperation{target: target, operation: operation, policy: generation.PolicyDigest})
				}
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
				preservedCurrent := preserveQuarantine && generation.Generation == current.Generation
				plans.targetTerminal[target.Kind][key] = bound && ((!preservedCurrent && generation.State.Terminal()) || safeDuringCleanup)
				if plans.targetTerminal[target.Kind][key] {
					historical, err := c.resolvePersistedTargetGenerationPlan(ctx, target, generation)
					if err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("resolve terminal %s target generation %s: %w", target.Kind, key, err)
					}
					plans.targetCleanupPlans[target.Kind][key] = historical
				}
			}
			if hasBoundGeneration && c.targets[target.Kind] == nil {
				return persistedPhysicalPlans{}, missingCapability("controller.reconcile_physical", "target_driver", "persisted physical target history has no configured inventory driver for "+string(target.Kind))
			}
			if resolvedRecovery != nil {
				if !plans.targetTerminal[target.Kind][targetRefKey(resolvedRecovery.previous)] {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "resolved recovery predecessor is not a bound terminal generation", nil)
				}
			}
			if current.State.Terminal() && !preserveQuarantine {
				continue
			}
			bound, err := completeProvisioningBinding("target generation "+target.ID, current.ProvisioningPlanDigest, current.ProvisioningKey)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			initialUnbound := !bound && current.Generation == 1 && current.Previous == 0 && current.RecoveryIncident == "" && current.State == domain.TargetGenerationProvisioning
			pendingReset := current.Previous > 0 && (current.State == domain.TargetGenerationProvisioning || current.State == domain.TargetGenerationInstrumenting)
			if !bound && !initialUnbound && !pendingReset {
				return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_plan_binding", "nonterminal persisted target generation has no recoverable physical plan binding", nil)
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
			if pendingReset {
				if !bound && current.State != domain.TargetGenerationProvisioning {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_plan_binding", "unbound reset generation is beyond the bindable provisioning state", nil)
				}
				previousGeneration, err := persistedTargetGeneration(target, current.Previous)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				previousBound, err := completeProvisioningBinding("target reset predecessor "+target.ID, previousGeneration.ProvisioningPlanDigest, previousGeneration.ProvisioningKey)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				if !previousBound {
					return persistedPhysicalPlans{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_reset_predecessor", "pending reset has no exact predecessor physical plan", nil)
				}
				previousPlan, err := c.resolvePersistedTargetGenerationPlan(ctx, target, previousGeneration)
				if err != nil {
					return persistedPhysicalPlans{}, fmt.Errorf("resolve target %s reset predecessor generation %d: %w", target.ID, previousGeneration.Generation, err)
				}
				plans.targets[target.Kind] = append(plans.targets[target.Kind], previousPlan)
				previousRef := ports.TargetRef{ID: previousPlan.Target.ID(), Generation: previousPlan.Generation.Spec().Generation}
				plans.targetTerminal[target.Kind][targetRefKey(previousRef)] = false
				currentRef := ports.TargetRef{ID: previousRef.ID, Generation: domain.TargetGeneration(current.Generation)}
				if bound {
					meta := application.MutationMeta{IdempotencyKey: current.ProvisioningKey, AuthorizedPolicyReference: current.PolicyDigest, Deadline: deadline(ctx)}
					currentPlan, err := c.resolvePersistedTargetProvisioningPlan(ctx, meta, target, current.ProvisioningKey)
					if err != nil {
						return persistedPhysicalPlans{}, fmt.Errorf("resolve target %s reset successor generation %d: %w", target.ID, current.Generation, err)
					}
					plans.targets[target.Kind] = append(plans.targets[target.Kind], currentPlan)
				}
				plans.pendingTargetResets = append(plans.pendingTargetResets, persistedPendingTargetReset{
					target: target, current: currentRef, previous: previousRef, currentExpected: bound,
				})
				continue
			}
			physicalKey := current.ProvisioningKey
			requestKey := current.ProvisioningKey
			if initialUnbound {
				requestKey = target.CreationIdempotencyKey
				physicalKey = domain.DeriveIdempotencyKey(requestKey, "physical/target")
			}
			requestMeta := application.MutationMeta{IdempotencyKey: requestKey, AuthorizedPolicyReference: current.PolicyDigest, Deadline: deadline(ctx)}
			plan, err := c.resolvePersistedTargetProvisioningPlan(ctx, requestMeta, target, physicalKey)
			if err != nil {
				return persistedPhysicalPlans{}, fmt.Errorf("resolve target %s generation %d: %w", target.ID, current.Generation, err)
			}
			meta, err := startupProvisioningMeta(ctx, physicalKey, target.ID, current.PolicyDigest, current.Generation)
			if err != nil {
				return persistedPhysicalPlans{}, err
			}
			plans.targets[target.Kind] = append(plans.targets[target.Kind], plan)
			if resolvedRecovery != nil {
				resolvedRecovery.current = ports.TargetRef{ID: plan.Target.ID(), Generation: plan.Generation.Spec().Generation}
				plans.resolvedTargetRecoveries = append(plans.resolvedTargetRecoveries, *resolvedRecovery)
			}
			if recoveringIncident != nil && current.State == domain.TargetGenerationReady {
				previousGeneration, err := persistedTargetGeneration(target, current.Previous)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				previousPlan, err := c.resolvePersistedTargetGenerationPlan(ctx, target, previousGeneration)
				if err != nil {
					return persistedPhysicalPlans{}, fmt.Errorf("resolve target %s recovery predecessor generation %d: %w", target.ID, previousGeneration.Generation, err)
				}
				plans.completedTargetRecoveries = append(plans.completedTargetRecoveries, persistedTargetRecoveryCompletion{
					target: target, generation: current, incident: *recoveringIncident,
					current:      ports.TargetRef{ID: plan.Target.ID(), Generation: plan.Generation.Spec().Generation},
					previous:     ports.TargetRef{ID: previousPlan.Target.ID(), Generation: previousPlan.Generation.Spec().Generation},
					previousPlan: previousPlan,
				})
			}
			if current.Previous == 0 && (current.State == domain.TargetGenerationProvisioning || current.State == domain.TargetGenerationInstrumenting) {
				plans.targetProvisionings = append(plans.targetProvisionings, persistedTargetProvisioning{
					target: target, generation: current, plan: plan, meta: meta, needsBinding: initialUnbound,
				})
			}
			var lifecycleRef ports.TargetRef
			if quarantining || destroying {
				targetID, err := domain.ParseTargetID(target.ID)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				lifecycleRef = ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(current.Generation)}
			}
			if destroying {
				plans.targetDestructions = append(plans.targetDestructions, persistedTargetDestruction{
					target: target, generation: current,
					ref:         lifecycleRef,
					reservation: destructionReservation,
				})
			}
			var targetRunRecoveries []persistedRunRecovery
			for _, run := range target.Runs {
				if run.State.Terminal() {
					continue
				}
				bound, err := completeProvisioningBinding("target run "+run.ID, run.ProvisioningPlanDigest, run.ProvisioningKey)
				if err != nil {
					return persistedPhysicalPlans{}, err
				}
				if !bound {
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
				targetRunRecoveries = append(targetRunRecoveries, persistedRunRecovery{target: target, run: run, plan: runPlan})
			}
			if quarantining {
				plans.targetQuarantines = append(plans.targetQuarantines, persistedTargetQuarantine{
					target: target, ref: lifecycleRef, reservation: quarantineReservation, runs: targetRunRecoveries,
				})
			} else {
				plans.runRecoveries = append(plans.runRecoveries, targetRunRecoveries...)
			}
		}
	}
	sort.Slice(plans.agentProvisionings, func(i, j int) bool {
		left := plans.agentProvisionings[i].plan.Agent.Generation.Spec()
		right := plans.agentProvisionings[j].plan.Agent.Generation.Spec()
		return agentRefKey(ports.AgentWorkspaceRef{ID: left.AgentWorkspaceID, Generation: left.Generation}) < agentRefKey(ports.AgentWorkspaceRef{ID: right.AgentWorkspaceID, Generation: right.Generation})
	})
	sort.Slice(plans.targetProvisionings, func(i, j int) bool {
		left := plans.targetProvisionings[i].plan.Generation.Spec()
		right := plans.targetProvisionings[j].plan.Generation.Spec()
		return targetRefKey(ports.TargetRef{ID: left.TargetID, Generation: left.Generation}) < targetRefKey(ports.TargetRef{ID: right.TargetID, Generation: right.Generation})
	})
	sort.Slice(plans.pendingAgentProvisionings, func(i, j int) bool {
		return agentRefKey(plans.pendingAgentProvisionings[i]) < agentRefKey(plans.pendingAgentProvisionings[j])
	})
	sort.Slice(plans.pendingTargetResets, func(i, j int) bool {
		return targetRefKey(plans.pendingTargetResets[i].current) < targetRefKey(plans.pendingTargetResets[j].current)
	})
	sort.Strings(plans.pendingTargetRuns)
	sort.Slice(plans.runRecoveries, func(i, j int) bool { return plans.runRecoveries[i].run.ID < plans.runRecoveries[j].run.ID })
	sort.Slice(plans.lostOperations, func(i, j int) bool {
		return plans.lostOperations[i].operation.ID < plans.lostOperations[j].operation.ID
	})
	sort.Slice(plans.targetQuarantines, func(i, j int) bool {
		return targetRefKey(plans.targetQuarantines[i].ref) < targetRefKey(plans.targetQuarantines[j].ref)
	})
	for index := range plans.targetQuarantines {
		sort.Slice(plans.targetQuarantines[index].runs, func(i, j int) bool {
			return plans.targetQuarantines[index].runs[i].run.ID < plans.targetQuarantines[index].runs[j].run.ID
		})
	}
	sort.Slice(plans.targetDestructions, func(i, j int) bool {
		return targetRefKey(plans.targetDestructions[i].ref) < targetRefKey(plans.targetDestructions[j].ref)
	})
	sort.Slice(plans.observerBindings, func(i, j int) bool {
		return plans.observerBindings[i].RunID.String() < plans.observerBindings[j].RunID.String()
	})
	return plans, nil
}

// closeAdmissionForDueLeaseExpiries persists every expiry gate detected by
// Core's authoritative clock before physical state is reconstructed. Without
// this boundary, an overdue Active lease can reject its own provisioning
// replay before the later termination reaper has a chance to begin expiry.
func (c *Controller) closeAdmissionForDueLeaseExpiries(ctx context.Context) error {
	work, err := c.Core.ListLeaseTerminationWork(ctx)
	if err != nil {
		return fmt.Errorf("enumerate lease termination work: %w", err)
	}
	for _, item := range work {
		if !item.NeedsBegin {
			continue
		}
		if _, err := c.Core.BeginDueLeaseExpiry(ctx, application.BeginLeaseExpiryRequest{
			LeaseID: item.LeaseID, ExpectedRevision: item.LeaseRevision,
		}); err != nil {
			return fmt.Errorf("begin expiry for lease %s: %w", item.LeaseID, err)
		}
	}
	return nil
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

func reconcileAgentInventory(ctx context.Context, driver ports.AgentWorkspaceDriver, request ports.AgentWorkspaceReconciliationRequest) (ports.AgentWorkspaceReconciliationReport, error) {
	reconciler, ok := driver.(ports.AgentWorkspaceReconciler)
	if !ok {
		return ports.AgentWorkspaceReconciliationReport{}, missingCapability("controller.reconcile_physical", "agent_inventory", "agent driver does not provide authoritative reconciliation")
	}
	return reconciler.ReconcileAgentWorkspaces(ctx, request)
}

func reconcileTargetInventory(ctx context.Context, driver ports.TargetDriver, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	reconciler, ok := driver.(ports.TargetReconciler)
	if !ok {
		return ports.TargetReconciliationReport{}, missingCapability("controller.reconcile_physical", "target_inventory", "target driver does not provide authoritative reconciliation")
	}
	return reconciler.ReconcileTargets(ctx, request)
}

// terminalAgentWorkspaceCleanupCandidates finds workspace state that remains
// after Docker has authoritatively proved the corresponding terminal
// container absent. Inspect is intentionally read-only; mutation still flows
// through destroyAgentAndWorkspace with the exact persisted generation and
// workspace identities.
func (c *Controller) terminalAgentWorkspaceCleanupCandidates(ctx context.Context, plans persistedPhysicalPlans, report ports.AgentWorkspaceReconciliationReport) ([]ports.AgentWorkspaceRef, error) {
	var candidates []ports.AgentWorkspaceRef
	for _, observation := range report.Expected {
		key := agentRefKey(observation.Ref)
		if !plans.agentTerminal[key] || observation.Classification != ports.PhysicalResourceMissing || observation.ContainerID != "" || observation.PlanMatched {
			continue
		}
		workspaceID, ok := plans.agentWorkspaces[key]
		if !ok {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_workspace_cleanup", "terminal agent generation has no persisted workspace identity", nil)
		}
		present, err := c.workspacePresent(ctx, workspaceID)
		if err != nil {
			return nil, fmt.Errorf("inspect terminal agent workspace %s: %w", workspaceID, err)
		}
		if present {
			candidates = append(candidates, observation.Ref)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return agentRefKey(candidates[i]) < agentRefKey(candidates[j]) })
	return candidates, nil
}

func (c *Controller) requireWorkspaceAbsent(ctx context.Context, workspaceID domain.WorkspaceID) error {
	present, err := c.workspacePresent(ctx, workspaceID)
	if err != nil {
		return err
	}
	if present {
		return domain.NewError(domain.CodeFailedPrecondition, "controller.reconcile_physical", "workspace_cleanup", "workspace remains present after release", nil)
	}
	return nil
}

func (c *Controller) workspacePresent(ctx context.Context, workspaceID domain.WorkspaceID) (bool, error) {
	_, err := c.workspace.Inspect(ctx, workspaceID)
	if domain.IsCode(err, domain.CodeNotFound) {
		return false, nil
	}
	return err == nil, err
}

type reconciliationObservation struct {
	key             string
	runtimeID       string
	classification  ports.PhysicalResourceClassification
	planMatched     bool
	cleanupRequired bool
	diagnostic      string
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
		key := agentRefKey(item.Ref)
		refs[key] = item.Ref
		expectedObservations = append(expectedObservations, reconciliationObservation{key: key, runtimeID: item.ContainerID, classification: item.Classification, planMatched: item.PlanMatched, diagnostic: item.Diagnostic})
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
	return assessTargetReconciliationAllowMissing(kind, expected, terminal, nil, report)
}

func assessTargetReconciliationAllowMissing(kind domain.TargetKind, expected []ports.TargetPlan, terminal, allowedMissing map[string]bool, report ports.TargetReconciliationReport) ([]ports.TargetRef, error) {
	return assessTargetReconciliationWithRecovery(kind, expected, terminal, allowedMissing, nil, report)
}

func assessTargetReconciliationWithRecovery(kind domain.TargetKind, expected []ports.TargetPlan, terminal, allowedMissing, allowedStoppedRecovery map[string]bool, report ports.TargetReconciliationReport) ([]ports.TargetRef, error) {
	expectedKeys := make([]string, 0, len(expected))
	refs := make(map[string]ports.TargetRef, len(report.Unclaimed))
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		expectedKeys = append(expectedKeys, targetRefKey(ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}))
	}
	expectedObservations := make([]reconciliationObservation, 0, len(report.Expected))
	for _, item := range report.Expected {
		key := targetRefKey(item.Ref)
		refs[key] = item.Ref
		expectedObservations = append(expectedObservations, reconciliationObservation{key: key, runtimeID: item.RuntimeID, classification: item.Classification, planMatched: item.PlanMatched, cleanupRequired: item.CleanupRequired, diagnostic: item.Diagnostic})
	}
	unclaimed := make([]reconciliationObservation, 0, len(report.Unclaimed))
	for _, item := range report.Unclaimed {
		key := targetRefKey(item.Ref)
		refs[key] = item.Ref
		unclaimed = append(unclaimed, reconciliationObservation{key: key, runtimeID: item.RuntimeID, classification: item.Classification, diagnostic: item.Diagnostic})
	}
	safe, err := assessReconciliationWithAllowedMissing(string(kind)+" target", expectedKeys, terminal, allowedMissing, allowedStoppedRecovery, report.ObservedAt, expectedObservations, unclaimed, report.Conflicts)
	result := make([]ports.TargetRef, 0, len(safe))
	for _, key := range safe {
		result = append(result, refs[key])
	}
	return result, err
}

func assessReconciliation(resource string, expectedKeys []string, terminal map[string]bool, observedAt time.Time, expected, unclaimed []reconciliationObservation, conflicts []ports.PhysicalResourceConflict) ([]string, error) {
	return assessReconciliationWithAllowedMissing(resource, expectedKeys, terminal, nil, nil, observedAt, expected, unclaimed, conflicts)
}

func assessReconciliationWithAllowedMissing(resource string, expectedKeys []string, terminal, allowedMissing, allowedStoppedRecovery map[string]bool, observedAt time.Time, expected, unclaimed []reconciliationObservation, conflicts []ports.PhysicalResourceConflict) ([]string, error) {
	expectedSet := make(map[string]bool, len(expectedKeys))
	var issues []error
	for _, key := range expectedKeys {
		if key == "" || expectedSet[key] {
			issues = append(issues, fmt.Errorf("%s expected plan set contains an empty or duplicate identity %q", resource, key))
		}
		expectedSet[key] = true
	}
	for key := range allowedMissing {
		if !expectedSet[key] {
			issues = append(issues, fmt.Errorf("%s missing authorization references unexpected identity %q", resource, key))
		}
	}
	for key := range allowedStoppedRecovery {
		if !expectedSet[key] {
			issues = append(issues, fmt.Errorf("%s stopped-run recovery authorization references unexpected identity %q", resource, key))
		}
	}
	if observedAt.IsZero() {
		issues = append(issues, fmt.Errorf("%s inventory has no observation time", resource))
	}
	seen := make(map[string]bool, len(expected))
	var safe []string
	for _, item := range expected {
		if !item.classification.IsValid() || item.key == "" || !expectedSet[item.key] || seen[item.key] {
			issues = append(issues, fmt.Errorf("%s inventory returned an invalid, unexpected, or duplicate expected identity %q", resource, item.key))
			continue
		}
		seen[item.key] = true
		if item.cleanupRequired && (item.classification != ports.PhysicalResourceMissing || item.runtimeID != "" || item.planMatched) {
			issues = append(issues, fmt.Errorf("%s %s returned invalid cleanup-required evidence: classification=%s plan_matched=%t runtime=%q", resource, item.key, item.classification, item.planMatched, item.runtimeID))
			continue
		}
		if terminal[item.key] {
			switch {
			case item.classification == ports.PhysicalResourceMissing && item.runtimeID == "" && !item.planMatched:
				if item.cleanupRequired {
					safe = append(safe, item.key)
				}
			case item.planMatched && item.runtimeID != "" && (item.classification == ports.PhysicalResourceAdopted || item.classification == ports.PhysicalResourceUncertain):
				safe = append(safe, item.key)
			default:
				issues = append(issues, fmt.Errorf("%s terminal generation %s lacks an exact cleanup-plan match: classification=%s plan_matched=%t cleanup_required=%t runtime=%q: %s", resource, item.key, item.classification, item.planMatched, item.cleanupRequired, item.runtimeID, item.diagnostic))
			}
			continue
		}
		if allowedMissing[item.key] {
			if item.classification != ports.PhysicalResourceMissing || item.runtimeID != "" {
				issues = append(issues, fmt.Errorf("%s %s has exact missing-resource recovery authority but inventory is %s runtime=%q: %s", resource, item.key, item.classification, item.runtimeID, item.diagnostic))
			}
			continue
		}
		if allowedStoppedRecovery[item.key] && item.classification == ports.PhysicalResourceUncertain && item.planMatched && item.runtimeID != "" {
			continue
		}
		if item.classification != ports.PhysicalResourceAdopted || item.runtimeID == "" {
			issues = append(issues, fmt.Errorf("%s %s is %s cleanup_required=%t: %s", resource, item.key, item.classification, item.cleanupRequired, item.diagnostic))
		}
	}
	for key := range expectedSet {
		if !seen[key] {
			issues = append(issues, fmt.Errorf("%s %s is missing from the inventory report", resource, key))
		}
	}
	seenUnclaimed := make(map[string]bool, len(unclaimed))
	for _, item := range unclaimed {
		if !item.classification.IsValid() || item.key == "" || seenUnclaimed[item.key] {
			issues = append(issues, fmt.Errorf("%s inventory returned an invalid or duplicate unclaimed identity %q", resource, item.key))
			continue
		}
		seenUnclaimed[item.key] = true
		issues = append(issues, fmt.Errorf("unsafe %s unclaimed resource %s is %s: no complete persisted plan was compared: %s", resource, item.key, item.classification, item.diagnostic))
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

func authorizePendingTargetCleanup(plans *persistedPhysicalPlans, required map[domain.TargetKind]map[string]bool) error {
	for kind, refs := range required {
		for key, cleanupRequired := range refs {
			if !cleanupRequired {
				continue
			}
			if _, found := plans.targetTerminal[kind][key]; !found {
				return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_reset_cleanup", "cleanup-required reset predecessor is absent from persisted generation ownership", nil)
			}
			if _, found := plans.targetCleanupPlans[kind][key]; !found {
				return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_reset_cleanup", "cleanup-required reset predecessor has no exact persisted plan", nil)
			}
			plans.targetTerminal[kind][key] = true
		}
	}
	return nil
}
