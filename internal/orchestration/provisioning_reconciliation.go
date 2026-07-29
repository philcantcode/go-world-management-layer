package orchestration

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// resumePersistedProvisioning closes the durable-binding/physical-mutation
// crash windows before authoritative inventory is assessed. Every candidate
// plan was already resolved, admitted, and validated without mutation.
func (c *Controller) resumePersistedProvisioning(
	ctx context.Context,
	plans persistedPhysicalPlans,
	execRecoveredAgents map[string]struct{},
	report *PhysicalReconciliationReport,
) error {
	for _, recovery := range plans.agentProvisionings {
		view := recovery.view
		if recovery.needsBinding {
			bound, err := c.bindAgentProvisioningPlan(ctx, recovery.meta, view, recovery.plan)
			if err != nil {
				return fmt.Errorf("recover agent workspace %s generation %d plan binding: %w", view.Agent.ID, recovery.generation.Generation, err)
			}
			view = bound
		}
		if _, recovered := execRecoveredAgents[agentPlanRefKey(recovery.plan.Agent)]; !recovered {
			if err := c.provisionAgentPhysical(ctx, recovery.plan); err != nil {
				return fmt.Errorf("recover agent workspace %s generation %d physical provisioning: %w", view.Agent.ID, recovery.generation.Generation, err)
			}
		}
		if recovery.generation.State == domain.AgentGenerationProvisioning || recovery.generation.State == domain.AgentGenerationBooting {
			if _, err := c.advanceAgentReady(ctx, recovery.meta, view.Agent); err != nil {
				return fmt.Errorf("recover agent workspace %s generation %d logical readiness: %w", view.Agent.ID, recovery.generation.Generation, err)
			}
			ref, err := agentGenerationRef(view.Agent.ID, recovery.generation.Generation)
			if err != nil {
				return err
			}
			report.RecoveredAgentProvisionings = append(report.RecoveredAgentProvisionings, ref)
		}
	}

	for _, recovery := range plans.targetProvisionings {
		target := recovery.target
		if recovery.needsBinding {
			bound, err := c.bindTargetProvisioningPlan(ctx, recovery.meta, target, recovery.plan)
			if err != nil {
				return fmt.Errorf("recover target %s generation %d plan binding: %w", target.ID, recovery.generation.Generation, err)
			}
			target = bound
		}
		driver := c.targets[target.Kind]
		if driver == nil {
			return missingCapability("controller.reconcile_physical", "target_driver", "persisted target provisioning has no configured driver for "+string(target.Kind))
		}
		if _, err := c.provisionTargetPhysical(ctx, driver, recovery.plan); err != nil {
			return fmt.Errorf("recover target %s generation %d physical provisioning: %w", target.ID, recovery.generation.Generation, err)
		}
		if _, err := c.advanceTargetReady(ctx, recovery.meta, target); err != nil {
			return fmt.Errorf("recover target %s generation %d logical readiness: %w", target.ID, recovery.generation.Generation, err)
		}
		ref, err := targetGenerationRef(target.ID, recovery.generation.Generation)
		if err != nil {
			return err
		}
		report.RecoveredTargetProvisionings = append(report.RecoveredTargetProvisionings, ref)
	}
	return nil
}

func startupAgentInventoryRequest(plans persistedPhysicalPlans, includeCompletedPredecessors, includeTerminalCleanup bool) ports.AgentWorkspaceReconciliationRequest {
	request := ports.AgentWorkspaceReconciliationRequest{Active: append([]ports.AgentWorkspacePlan(nil), plans.agents...)}
	cleanup := make(map[string]ports.AgentWorkspacePlan)
	if includeTerminalCleanup {
		for key, plan := range plans.agentCleanupPlans {
			if plans.agentTerminal[key] {
				cleanup[key] = plan
			}
		}
	}
	for _, recovery := range plans.agentRecoveries {
		if includeCompletedPredecessors || !recovery.bound {
			cleanup[agentPlanKey(recovery.previousPlan)] = recovery.previousPlan
		}
	}
	if includeCompletedPredecessors {
		for _, recovery := range plans.resolvedAgentRecoveries {
			cleanup[agentPlanKey(recovery.previousPlan)] = recovery.previousPlan
		}
	}
	request.CleanupOnly = sortedAgentPlanMap(cleanup)
	sortAgentPlans(request.Active)
	return request
}

func startupTargetInventoryRequest(plans persistedPhysicalPlans, kind domain.TargetKind, includeCompletedPredecessors, includeTerminalCleanup bool) ports.TargetReconciliationRequest {
	cleanup := make(map[string]ports.TargetPlan)
	if includeTerminalCleanup {
		for key, plan := range plans.targetCleanupPlans[kind] {
			if plans.targetTerminal[kind][key] {
				cleanup[key] = plan
			}
		}
	}
	for _, pending := range plans.pendingTargetResets {
		if pending.target.Kind == kind {
			for _, plan := range plans.targets[kind] {
				if targetPlanKey(plan) == targetRefKey(pending.previous) {
					cleanup[targetPlanKey(plan)] = plan
				}
			}
		}
	}
	if includeCompletedPredecessors {
		for _, recovery := range plans.completedTargetRecoveries {
			if recovery.target.Kind == kind {
				cleanup[targetPlanKey(recovery.previousPlan)] = recovery.previousPlan
			}
		}
		for _, recovery := range plans.resolvedTargetRecoveries {
			if recovery.kind == kind {
				cleanup[targetPlanKey(recovery.previousPlan)] = recovery.previousPlan
			}
		}
	}
	request := ports.TargetReconciliationRequest{CleanupOnly: sortedTargetPlanMap(cleanup)}
	for _, plan := range plans.targets[kind] {
		if _, cleanupOnly := cleanup[targetPlanKey(plan)]; !cleanupOnly {
			request.Active = append(request.Active, plan)
		}
	}
	sortTargetPlans(request.Active)
	return request
}

func agentPlanKey(plan ports.AgentWorkspacePlan) string {
	spec := plan.Generation.Spec()
	return agentRefKey(ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation})
}

func targetPlanKey(plan ports.TargetPlan) string {
	spec := plan.Generation.Spec()
	return targetRefKey(ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation})
}

func sortedAgentPlanMap(plans map[string]ports.AgentWorkspacePlan) []ports.AgentWorkspacePlan {
	result := make([]ports.AgentWorkspacePlan, 0, len(plans))
	for _, plan := range plans {
		result = append(result, plan)
	}
	sortAgentPlans(result)
	return result
}

func sortedTargetPlanMap(plans map[string]ports.TargetPlan) []ports.TargetPlan {
	result := make([]ports.TargetPlan, 0, len(plans))
	for _, plan := range plans {
		result = append(result, plan)
	}
	sortTargetPlans(result)
	return result
}

func sortAgentPlans(plans []ports.AgentWorkspacePlan) {
	sort.Slice(plans, func(i, j int) bool { return agentPlanKey(plans[i]) < agentPlanKey(plans[j]) })
}

func sortTargetPlans(plans []ports.TargetPlan) {
	sort.Slice(plans, func(i, j int) bool { return targetPlanKey(plans[i]) < targetPlanKey(plans[j]) })
}

func allAgentInventoryPlans(request ports.AgentWorkspaceReconciliationRequest) []ports.AgentWorkspacePlan {
	result := append([]ports.AgentWorkspacePlan(nil), request.Active...)
	return append(result, request.CleanupOnly...)
}

func allTargetInventoryPlans(request ports.TargetReconciliationRequest) []ports.TargetPlan {
	result := append([]ports.TargetPlan(nil), request.Active...)
	return append(result, request.CleanupOnly...)
}

func retainedPendingAgentPredecessors(plans persistedPhysicalPlans) map[string]bool {
	retained := make(map[string]bool)
	for _, recovery := range plans.agentRecoveries {
		if !recovery.bound {
			retained[agentRefKey(recovery.previousResource.ref)] = true
		}
	}
	return retained
}

// resumePersistedAgentRecoveries follows the same order as live recovery. A
// bound successor may be provisioned only after authoritative inventory has
// proved that the exact predecessor can be retired (or is already absent).
// An unbound successor has no reconstructible public request identity, so it
// retains the predecessor without mutation for the exact client retry.
func (c *Controller) resumePersistedAgentRecoveries(
	ctx context.Context,
	plans persistedPhysicalPlans,
	inventory ports.AgentWorkspaceReconciliationReport,
	report *PhysicalReconciliationReport,
) error {
	for _, recovery := range plans.agentRecoveries {
		previous, err := exactAgentObservation(inventory, recovery.previousResource.ref)
		if err != nil {
			return fmt.Errorf("verify agent recovery predecessor %s: %w", agentRefKey(recovery.previousResource.ref), err)
		}
		previousPresent := exactPlanMatchedAgentObservation(previous)
		previousMissing := exactMissingAgentObservation(previous)
		if !recovery.bound {
			if !previousPresent {
				return fmt.Errorf("pending unbound agent recovery %s cannot retain predecessor: classification=%s plan_matched=%t runtime=%q: %s", recovery.generation.RecoveryIncident, previous.Classification, previous.PlanMatched, previous.ContainerID, previous.Diagnostic)
			}
			continue
		}
		if !previousPresent && !previousMissing {
			return fmt.Errorf("bound agent recovery predecessor %s is not safely identifiable: classification=%s plan_matched=%t runtime=%q: %s", agentRefKey(recovery.previousResource.ref), previous.Classification, previous.PlanMatched, previous.ContainerID, previous.Diagnostic)
		}
		currentSpec := recovery.currentPlan.Agent.Generation.Spec()
		currentRef := ports.AgentWorkspaceRef{ID: currentSpec.AgentWorkspaceID, Generation: currentSpec.Generation}
		current, err := exactAgentObservation(inventory, currentRef)
		if err != nil {
			return fmt.Errorf("verify agent recovery successor %s: %w", agentRefKey(currentRef), err)
		}
		currentPresent := exactPlanMatchedAgentObservation(current)
		currentMissing := exactMissingAgentObservation(current)
		if !currentPresent && !currentMissing {
			return fmt.Errorf("bound agent recovery successor %s is not safely identifiable: classification=%s plan_matched=%t runtime=%q: %s", agentRefKey(currentRef), current.Classification, current.PlanMatched, current.ContainerID, current.Diagnostic)
		}
		if previousPresent && currentPresent {
			return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "predecessor and successor containers both exist outside the ordered recovery saga", nil)
		}
		// Even authoritative container absence may be the crash point between
		// physical removal and workspace sealing/release. Replay the complete
		// idempotent retirement so no predecessor workspace survives.
		if err := c.destroyAgentAndWorkspace(ctx, recovery.previousResource.ref, recovery.previousResource.workspaceID, ports.StopForce); err != nil {
			return fmt.Errorf("retire failed agent recovery predecessor %s: %w", agentRefKey(recovery.previousResource.ref), err)
		}
		if err := c.provisionAgentPhysical(ctx, recovery.currentPlan); err != nil {
			return fmt.Errorf("recover agent workspace %s generation %d physical provisioning: %w", recovery.view.Agent.ID, recovery.generation.Generation, err)
		}
		agent, err := c.advanceAgentReady(ctx, recovery.meta, recovery.view.Agent)
		if err != nil {
			return fmt.Errorf("recover agent workspace %s generation %d logical readiness: %w", recovery.view.Agent.ID, recovery.generation.Generation, err)
		}
		if err := c.completePersistedRecoveryIncident(ctx, recovery.incident, recovery.generation.PolicyDigest, recovery.generation.ProvisioningPlanDigest, "physical-agent:recreate"); err != nil {
			return fmt.Errorf("complete agent recovery incident %s: %w", recovery.incident.ID, err)
		}
		ref, err := agentGenerationRef(agent.ID, agent.CurrentGeneration)
		if err != nil {
			return err
		}
		report.RecoveredAgentRecoveries = append(report.RecoveredAgentRecoveries, ref)
	}
	return nil
}

func (c *Controller) completePersistedTargetRecoveries(
	ctx context.Context,
	plans persistedPhysicalPlans,
	inventory map[domain.TargetKind]ports.TargetReconciliationReport,
	report *PhysicalReconciliationReport,
) error {
	for _, recovery := range plans.completedTargetRecoveries {
		current, err := exactTargetObservation(inventory[recovery.target.Kind], recovery.current)
		if err != nil {
			return fmt.Errorf("verify completed target recovery successor %s: %w", targetRefKey(recovery.current), err)
		}
		previous, err := exactTargetObservation(inventory[recovery.target.Kind], recovery.previous)
		if err != nil {
			return fmt.Errorf("verify completed target recovery predecessor %s: %w", targetRefKey(recovery.previous), err)
		}
		if !exactAdoptedTargetObservation(current) || !exactMissingTargetObservation(previous) {
			return unsafeTargetRecoveryPairError(
				"target_recovery", "completed target recovery "+recovery.incident.ID,
				previous, current,
			)
		}
		strategy, err := persistedTargetRecoveryStrategy(recovery.incident)
		if err != nil {
			return err
		}
		if err := c.completePersistedRecoveryIncident(ctx, recovery.incident, recovery.generation.PolicyDigest, recovery.generation.ProvisioningPlanDigest, "physical-target:"+strategy); err != nil {
			return fmt.Errorf("complete target recovery incident %s: %w", recovery.incident.ID, err)
		}
		report.CompletedTargetRecoveries = append(report.CompletedTargetRecoveries, recovery.current)
	}
	return nil
}

func exactAgentObservation(report ports.AgentWorkspaceReconciliationReport, ref ports.AgentWorkspaceRef) (ports.AgentWorkspaceReconciliation, error) {
	if report.ObservedAt.IsZero() {
		return ports.AgentWorkspaceReconciliation{}, fmt.Errorf("agent inventory has no observation time")
	}
	var result ports.AgentWorkspaceReconciliation
	count := 0
	for _, observation := range report.Expected {
		if observation.Ref == ref {
			result, count = observation, count+1
		}
	}
	if count != 1 || !result.Classification.IsValid() {
		return ports.AgentWorkspaceReconciliation{}, fmt.Errorf("agent inventory contains %d valid observations for the expected identity", count)
	}
	return result, nil
}

func exactPlanMatchedAgentObservation(observation ports.AgentWorkspaceReconciliation) bool {
	return observation.PlanMatched && observation.ContainerID != "" &&
		(observation.Classification == ports.PhysicalResourceAdopted || observation.Classification == ports.PhysicalResourceUncertain)
}

func exactAdoptedAgentObservation(observation ports.AgentWorkspaceReconciliation) bool {
	return observation.PlanMatched && observation.Classification == ports.PhysicalResourceAdopted && observation.ContainerID != ""
}

func exactMissingAgentObservation(observation ports.AgentWorkspaceReconciliation) bool {
	return observation.Classification == ports.PhysicalResourceMissing && observation.ContainerID == "" && !observation.PlanMatched
}

func verifyResolvedRecoveryPairs(plans persistedPhysicalPlans, agents ports.AgentWorkspaceReconciliationReport, targets map[domain.TargetKind]ports.TargetReconciliationReport) error {
	for _, recovery := range plans.resolvedAgentRecoveries {
		current, err := exactAgentObservation(agents, recovery.current)
		if err != nil {
			return fmt.Errorf("verify resolved agent recovery successor %s: %w", agentRefKey(recovery.current), err)
		}
		previous, err := exactAgentObservation(agents, recovery.previous)
		if err != nil {
			return fmt.Errorf("verify resolved agent recovery predecessor %s: %w", agentRefKey(recovery.previous), err)
		}
		if !exactAdoptedAgentObservation(current) || !exactMissingAgentObservation(previous) {
			return domain.NewError(
				domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery",
				fmt.Sprintf("resolved agent recovery %s lacks its exact completed physical pair: predecessor=%s runtime=%q successor=%s runtime=%q", recovery.incident.ID, previous.Classification, previous.ContainerID, current.Classification, current.ContainerID), nil,
			)
		}
	}
	for _, recovery := range plans.resolvedTargetRecoveries {
		report := targets[recovery.kind]
		current, err := exactTargetObservation(report, recovery.current)
		if err != nil {
			return fmt.Errorf("verify resolved target recovery successor %s: %w", targetRefKey(recovery.current), err)
		}
		previous, err := exactTargetObservation(report, recovery.previous)
		if err != nil {
			return fmt.Errorf("verify resolved target recovery predecessor %s: %w", targetRefKey(recovery.previous), err)
		}
		if !exactAdoptedTargetObservation(current) || !exactMissingTargetObservation(previous) {
			return unsafeTargetRecoveryPairError("target_recovery", "resolved target recovery "+recovery.incident.ID, previous, current)
		}
	}
	return nil
}

func (c *Controller) completePersistedRecoveryIncident(ctx context.Context, expected application.IncidentRecord, policy, planDigest, action string) error {
	incident, err := c.Core.GetIncident(ctx, expected.ID)
	if err != nil {
		return err
	}
	if incident.State == domain.IncidentResolved {
		return nil
	}
	if incident.State != domain.IncidentRecovering || incident.Revision != expected.Revision {
		return domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "recovery_incident", "incident changed during serialized startup recovery", nil)
	}
	meta, err := startupRecoveryCompletionMeta(ctx, incident, policy, planDigest, action)
	if err != nil {
		return err
	}
	_, err = c.Core.CompleteIncidentRecovery(ctx, application.TransitionIncidentRequest{
		Meta: meta, IncidentID: incident.ID, ExpectedRevision: incident.Revision, State: domain.IncidentResolved,
		RecoveryActions:            appendUniqueString(incident.RecoveryActions, action),
		VisibilityAcknowledgements: append([]string(nil), incident.VisibilityAcknowledgements...),
	})
	return err
}

func startupRecoveryCompletionMeta(ctx context.Context, incident application.IncidentRecord, policy, planDigest, action string) (application.MutationMeta, error) {
	incidentID, err := domain.ParseIncidentID(incident.ID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(policy); err != nil {
		return application.MutationMeta{}, err
	}
	if _, err := domain.ParseDigest(planDigest); err != nil {
		return application.MutationMeta{}, err
	}
	identity := domain.NewDigest([]byte(incident.ID + "\x00" + planDigest + "\x00" + action))
	return application.MutationMeta{
		IdempotencyKey: "startup-recovery-completion/" + identity.String(),
		CorrelationID:  "corr_" + incidentID.UUID(), AuthorizedPolicyReference: policy, Deadline: deadline(ctx),
	}, nil
}

func appendUniqueString(values []string, value string) []string {
	result := append([]string(nil), values...)
	for _, existing := range result {
		if existing == value {
			return result
		}
	}
	return append(result, value)
}

func persistedAgentGeneration(agent application.AgentWorkspaceRecord, generation uint64) (application.AgentGenerationRecord, error) {
	for _, candidate := range agent.Generations {
		if candidate.Generation == generation {
			return candidate, nil
		}
	}
	return application.AgentGenerationRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_generation", "persisted agent generation is missing", nil)
}

func (c *Controller) resolvePersistedAgentGenerationPlan(ctx context.Context, view application.ResearchSessionView, generation application.AgentGenerationRecord) (AgentProvisioningPlan, error) {
	bound, err := completeProvisioningBinding("historical agent generation", generation.ProvisioningPlanDigest, generation.WorkspaceProvisioningKey, generation.AgentProvisioningKey)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	if !bound {
		return AgentProvisioningPlan{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_generation", "historical agent generation has no complete physical plan binding", nil)
	}
	historical := view
	historical.Agent.CurrentGeneration = generation.Generation
	historical.Lease.AgentGeneration = generation.Generation
	resolved, err := c.resolver.ResolvePersistedAgent(ctx, historical)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	return bindAgentProvisioning(application.AcquireRequest{Meta: application.MutationMeta{IdempotencyKey: generation.AgentProvisioningKey}}, resolved, historical)
}

func requirePersistedAgentRecoveryIncident(view application.ResearchSessionView, generation application.AgentGenerationRecord) (application.IncidentRecord, error) {
	incident, found := recoveryIncident(view.Incidents, generation.RecoveryIncident)
	if !found {
		return application.IncidentRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "linked incident is absent from the persisted session view", nil)
	}
	if generation.Generation <= 1 || generation.Previous != generation.Generation-1 ||
		view.Lease.AgentGeneration != generation.Generation || view.Agent.CurrentGeneration != generation.Generation ||
		incident.SessionID != view.Session.ID || incident.LeaseID != view.Lease.ID ||
		incident.AgentWorkspaceID != view.Agent.ID || incident.AgentGeneration != generation.Previous {
		return application.IncidentRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "incident does not authorize the exact predecessor/successor scope", nil)
	}
	if !containsExactString(incident.RecoveryActions, string(application.RecoveryResourceAgent)+":"+string(ports.ResetRecreate)) {
		return application.IncidentRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "agent_recovery", "incident lacks the durable recreate recovery action", nil)
	}
	return incident, nil
}

func requirePersistedTargetRecoveryIncident(view application.ResearchSessionView, target application.TargetRecord, generation application.TargetGenerationRecord) (application.IncidentRecord, error) {
	incident, found := recoveryIncident(view.Incidents, generation.RecoveryIncident)
	if !found {
		return application.IncidentRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "linked incident is absent from the persisted session view", nil)
	}
	if generation.Generation <= 1 || generation.Previous != generation.Generation-1 || target.CurrentGeneration != generation.Generation ||
		target.SessionID != view.Session.ID || target.LeaseID != view.Lease.ID ||
		incident.SessionID != view.Session.ID || incident.LeaseID != view.Lease.ID ||
		incident.TargetID != target.ID || incident.TargetGeneration != generation.Previous {
		return application.IncidentRecord{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "incident does not authorize the exact predecessor/successor scope", nil)
	}
	if _, err := persistedTargetRecoveryStrategy(incident); err != nil {
		return application.IncidentRecord{}, err
	}
	return incident, nil
}

func persistedTargetRecoveryStrategy(incident application.IncidentRecord) (string, error) {
	const prefix = "target:"
	strategy := ""
	for _, action := range incident.RecoveryActions {
		if !strings.HasPrefix(action, prefix) {
			continue
		}
		candidate := strings.TrimPrefix(action, prefix)
		selection, err := parseTargetRecoveryStrategy(candidate)
		if err != nil {
			return "", domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "incident contains an invalid target recovery action", err)
		}
		if strategy != "" && strategy != selection.canonical {
			return "", domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "incident contains conflicting target recovery actions", nil)
		}
		strategy = selection.canonical
	}
	if strategy == "" {
		return "", domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_recovery", "incident lacks a durable target recovery action", nil)
	}
	return strategy, nil
}

func containsExactString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// assessAgentReconciliationAllowRetained verifies each unbound recovery
// predecessor under its stronger full-plan-match rule, then removes those
// deliberately non-ready resources from the ordinary Ready-only assessment.
func assessAgentReconciliationAllowRetained(expected []ports.AgentWorkspacePlan, terminal, retained map[string]bool, report ports.AgentWorkspaceReconciliationReport) ([]ports.AgentWorkspaceRef, error) {
	if len(retained) == 0 {
		return assessAgentReconciliation(expected, terminal, report)
	}
	filteredPlans := make([]ports.AgentWorkspacePlan, 0, len(expected)-len(retained))
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		key := agentRefKey(ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation})
		if !retained[key] {
			filteredPlans = append(filteredPlans, plan)
		}
	}
	filteredReport := report
	filteredReport.Expected = make([]ports.AgentWorkspaceReconciliation, 0, len(report.Expected)-len(retained))
	seen := make(map[string]bool, len(retained))
	for _, observation := range report.Expected {
		key := agentRefKey(observation.Ref)
		if !retained[key] {
			filteredReport.Expected = append(filteredReport.Expected, observation)
			continue
		}
		if seen[key] || !exactPlanMatchedAgentObservation(observation) {
			return nil, fmt.Errorf("retained agent recovery predecessor %s is not one exact full-plan match: classification=%s plan_matched=%t runtime=%q: %s", key, observation.Classification, observation.PlanMatched, observation.ContainerID, observation.Diagnostic)
		}
		seen[key] = true
	}
	for key := range retained {
		if !seen[key] {
			return nil, fmt.Errorf("retained agent recovery predecessor %s is missing from inventory", key)
		}
	}
	return assessAgentReconciliation(filteredPlans, terminal, filteredReport)
}

func startupProvisioningMeta(ctx context.Context, baseKey, resourceID, policy string, generation uint64) (application.MutationMeta, error) {
	if !domain.IsCanonicalIdempotencyKey(baseKey) {
		return application.MutationMeta{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "provisioning_key", "persisted provisioning root is invalid", nil)
	}
	if _, err := domain.ParseDigest(policy); err != nil {
		return application.MutationMeta{}, err
	}
	correlationUUID, err := provisioningCorrelationUUID(resourceID)
	if err != nil {
		return application.MutationMeta{}, err
	}
	return application.MutationMeta{
		IdempotencyKey:            domain.DeriveIdempotencyKey(baseKey, fmt.Sprintf("startup/provisioning/%d", generation)),
		CorrelationID:             "corr_" + correlationUUID,
		AuthorizedPolicyReference: policy,
		Deadline:                  deadline(ctx),
	}, nil
}

func provisioningCorrelationUUID(resourceID string) (string, error) {
	if agentID, err := domain.ParseAgentWorkspaceID(resourceID); err == nil {
		return agentID.UUID(), nil
	}
	targetID, err := domain.ParseTargetID(resourceID)
	if err != nil {
		return "", domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "resource_id", "provisioning resource identity is invalid", err)
	}
	return targetID.UUID(), nil
}

func (c *Controller) resolvePersistedTargetGenerationPlan(ctx context.Context, target application.TargetRecord, generation application.TargetGenerationRecord) (ports.TargetPlan, error) {
	if generation.ProvisioningKey == "" {
		return ports.TargetPlan{}, domain.NewError(domain.CodeIntegrityViolation, "controller.reconcile_physical", "target_generation", "historical target generation has no physical provisioning key", nil)
	}
	historical := target
	historical.CurrentGeneration = generation.Generation
	meta := application.MutationMeta{
		IdempotencyKey:            generation.ProvisioningKey,
		AuthorizedPolicyReference: generation.PolicyDigest,
		Deadline:                  deadline(ctx),
	}
	return c.resolvePersistedTargetProvisioningPlan(ctx, meta, historical, generation.ProvisioningKey)
}

// requireSafePendingTargetResets accepts only the two exact physical states
// from which the original reset request can safely resume: the predecessor is
// still present and the successor is absent, or a durable driver reset already
// completed and the successor is present while the predecessor is absent.
type pendingTargetResetSafety struct {
	allowedMissing  map[domain.TargetKind]map[string]bool
	cleanupRequired map[domain.TargetKind]map[string]bool
}

func requireSafePendingTargetResets(pending []persistedPendingTargetReset, reports map[domain.TargetKind]ports.TargetReconciliationReport) (pendingTargetResetSafety, error) {
	safety := pendingTargetResetSafety{
		allowedMissing: make(map[domain.TargetKind]map[string]bool), cleanupRequired: make(map[domain.TargetKind]map[string]bool),
	}
	for _, recovery := range pending {
		report := reports[recovery.target.Kind]
		previous, err := exactTargetObservation(report, recovery.previous)
		if err != nil {
			return pendingTargetResetSafety{}, fmt.Errorf("verify pending target reset predecessor %s: %w", targetRefKey(recovery.previous), err)
		}
		previousAdopted := exactAdoptedTargetObservation(previous)
		previousMissing := exactMissingTargetObservation(previous)
		if !recovery.currentExpected {
			if !previousAdopted {
				return pendingTargetResetSafety{}, fmt.Errorf("pending unbound target reset %s cannot resume: predecessor is %s: %s", targetRefKey(recovery.current), targetObservationSummary(previous), previous.Diagnostic)
			}
			continue
		}
		current, err := exactTargetObservation(report, recovery.current)
		if err != nil {
			return pendingTargetResetSafety{}, fmt.Errorf("verify pending target reset successor %s: %w", targetRefKey(recovery.current), err)
		}
		currentAdopted := exactAdoptedTargetObservation(current)
		currentMissing := exactMissingTargetObservation(current)
		switch {
		case previousAdopted && currentMissing:
			addTargetRef(safety.allowedMissing, recovery.target.Kind, recovery.current)
		case previousMissing && currentAdopted:
			addTargetRef(safety.allowedMissing, recovery.target.Kind, recovery.previous)
			if previous.CleanupRequired {
				addTargetRef(safety.cleanupRequired, recovery.target.Kind, recovery.previous)
			}
		default:
			return pendingTargetResetSafety{}, unsafeTargetRecoveryPairError(
				"target_reset", "pending target reset "+targetRefKey(recovery.current),
				previous, current,
			)
		}
	}
	return safety, nil
}

func unsafeTargetRecoveryPairError(field, subject string, previous, current ports.TargetReconciliation) error {
	return domain.NewError(
		domain.CodeIntegrityViolation,
		"controller.reconcile_physical",
		field,
		fmt.Sprintf(
			"%s has unsafe physical pair: predecessor=(%s) successor=(%s)",
			subject, targetObservationSummary(previous), targetObservationSummary(current),
		),
		nil,
	)
}

func targetObservationSummary(observation ports.TargetReconciliation) string {
	return fmt.Sprintf(
		"classification=%s runtime=%q plan_matched=%t cleanup_required=%t",
		observation.Classification, observation.RuntimeID, observation.PlanMatched, observation.CleanupRequired,
	)
}

func exactAdoptedTargetObservation(observation ports.TargetReconciliation) bool {
	return observation.PlanMatched && observation.Classification == ports.PhysicalResourceAdopted && observation.RuntimeID != ""
}

func exactMissingTargetObservation(observation ports.TargetReconciliation) bool {
	return observation.Classification == ports.PhysicalResourceMissing && observation.RuntimeID == "" && !observation.PlanMatched
}

func addTargetRef(refs map[domain.TargetKind]map[string]bool, kind domain.TargetKind, ref ports.TargetRef) {
	if refs[kind] == nil {
		refs[kind] = make(map[string]bool)
	}
	refs[kind][targetRefKey(ref)] = true
}

func mergeAllowedTargetMissing(sets ...map[domain.TargetKind]map[string]bool) map[domain.TargetKind]map[string]bool {
	merged := make(map[domain.TargetKind]map[string]bool)
	for _, set := range sets {
		for kind, refs := range set {
			for key, allowed := range refs {
				if !allowed {
					continue
				}
				if merged[kind] == nil {
					merged[kind] = make(map[string]bool)
				}
				merged[kind][key] = true
			}
		}
	}
	return merged
}
