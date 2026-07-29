package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// RecoverIncident overrides the embedded logical operation in physical
// compositions. The logical rollover is the durable saga boundary: failures
// after it leave the incident in Recovering and an exact retry resumes the
// same driver operation rather than minting another generation.
func (c *Controller) RecoverIncident(ctx context.Context, request application.RecoverIncidentRequest) (application.RecoveryOutcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.RecoverIncident(ctx, request)
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()

	switch request.Resource {
	case application.RecoveryResourceTarget:
		selection, err := parseTargetRecoveryStrategy(request.Strategy)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		incident, err := c.Core.GetIncident(operationCtx, request.IncidentID)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		before, err := c.Core.GetTarget(operationCtx, incident.TargetID)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		driver, err := c.requireTargetLifecycle("recover_incident", before.Kind)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		reset := application.ResetTargetRequest{
			Meta: request.Meta, TargetID: before.ID, ExpectedRevision: before.Revision, Mode: selection.mode,
			SnapshotName: selection.snapshotName, RecoveryIncidentID: request.IncidentID,
		}
		if incident.State != domain.IncidentResolved {
			view, viewErr := c.Core.GetResearchSessionByLease(operationCtx, before.LeaseID)
			if viewErr != nil {
				return application.RecoveryOutcome{}, viewErr
			}
			if _, err := c.resolveAndAdmitTargetReset(operationCtx, reset, before, view, &incident, ""); err != nil {
				return application.RecoveryOutcome{}, fmt.Errorf("admit target recovery: %w", err)
			}
		}
		outcome, err := c.Core.RecoverIncident(operationCtx, request)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		if current, resolved, err := c.currentResolvedRecovery(operationCtx, outcome, request.Resource); err != nil || resolved {
			return current, err
		}
		if outcome.Target == nil {
			return outcome, fmt.Errorf("target recovery did not return a target generation")
		}
		reset.TargetID = outcome.Target.ID
		reset.ExpectedRevision = outcome.Target.Revision
		view, err := c.Core.GetResearchSessionByLease(operationCtx, outcome.Target.LeaseID)
		if err != nil {
			return outcome, err
		}
		physicalKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset")
		plan, err := c.resolveAndAdmitTargetReset(operationCtx, reset, *outcome.Target, view, &incident, physicalKey)
		if err != nil {
			return outcome, fmt.Errorf("admit bound target recovery: %w", err)
		}
		bound, err := c.bindTargetProvisioningPlan(operationCtx, request.Meta, *outcome.Target, plan)
		if err != nil {
			return outcome, fmt.Errorf("bind target recovery plan: %w", err)
		}
		outcome.Target = &bound
		target, _, err := c.resetTargetPhysical(operationCtx, reset, bound, driver)
		if err != nil {
			return outcome, fmt.Errorf("resume target recovery: %w", err)
		}
		outcome.Target = &target
		return c.completePhysicalRecovery(operationCtx, request, outcome, "physical-target:"+selection.canonical)

	case application.RecoveryResourceAgent:
		if err := requireAgentRecoveryStrategy(request.Strategy); err != nil {
			return application.RecoveryOutcome{}, err
		}
		if err := c.requireAgentLifecycle("recover_incident"); err != nil {
			return application.RecoveryOutcome{}, err
		}
		incident, err := c.Core.GetIncident(operationCtx, request.IncidentID)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		if incident.State != domain.IncidentResolved {
			view, viewErr := c.Core.GetResearchSessionByLease(operationCtx, incident.LeaseID)
			if viewErr != nil {
				return application.RecoveryOutcome{}, viewErr
			}
			if err := c.admitAgentRecoveryRequest(operationCtx, request, view, incident); err != nil {
				return application.RecoveryOutcome{}, fmt.Errorf("admit agent recovery: %w", err)
			}
		}
		outcome, err := c.Core.RecoverIncident(operationCtx, request)
		if err != nil {
			return application.RecoveryOutcome{}, err
		}
		if current, resolved, err := c.currentResolvedRecovery(operationCtx, outcome, request.Resource); err != nil || resolved {
			return current, err
		}
		if outcome.Agent == nil || outcome.Lease == nil {
			return outcome, fmt.Errorf("agent recovery did not return an agent generation and lease")
		}
		view, err := c.Core.GetResearchSession(operationCtx, outcome.Agent.SessionID)
		if err != nil {
			return outcome, err
		}
		resolved, err := c.resolver.ResolveAgentRecovery(operationCtx, request, view)
		if err != nil {
			return outcome, fmt.Errorf("resolve agent recovery: %w", err)
		}
		bindRequest := application.AcquireRequest{Meta: request.Meta}
		plan, err := bindAgentProvisioning(bindRequest, resolved, view)
		if err != nil {
			return outcome, err
		}
		// Admission must precede retirement of the old generation. A denied
		// replacement plan must leave the last physical evidence source intact.
		if err := c.admitAgentWorkspacePlan(operationCtx, plan.Agent); err != nil {
			return outcome, fmt.Errorf("admit agent recovery plan: %w", err)
		}
		view, err = c.bindAgentProvisioningPlan(operationCtx, request.Meta, view, plan)
		if err != nil {
			return outcome, fmt.Errorf("bind agent recovery plan: %w", err)
		}
		previous, err := requirePreviousAgentResource(view.Agent)
		if err != nil {
			return outcome, err
		}
		if err := c.destroyAgentAndWorkspace(operationCtx, previous.ref, previous.workspaceID, ports.StopForce); err != nil {
			return outcome, fmt.Errorf("retire failed agent generation: %w", err)
		}
		if err := c.provisionAgentPhysical(operationCtx, plan); err != nil {
			return outcome, fmt.Errorf("resume agent recovery: %w", err)
		}
		agent, err := c.advanceAgentReady(operationCtx, request.Meta, view.Agent)
		if err != nil {
			return outcome, err
		}
		outcome.Agent = &agent
		outcome.Lease = &view.Lease
		return c.completePhysicalRecovery(operationCtx, request, outcome, "physical-agent:recreate")

	default:
		return application.RecoveryOutcome{}, domain.NewError(domain.CodeInvalidArgument, "controller.recover_incident", "resource", "must be target or agent_workspace", nil)
	}
}

// currentResolvedRecovery turns Core's intentionally immutable idempotency
// response into the current public view after the physical saga completed.
// Calling Core first above is essential: it proves the retry carries the exact
// original key and payload before this terminal fast path is allowed.
func (c *Controller) currentResolvedRecovery(ctx context.Context, outcome application.RecoveryOutcome, resource application.RecoveryResource) (application.RecoveryOutcome, bool, error) {
	incident, err := c.Core.GetIncident(ctx, outcome.Incident.ID)
	if err != nil {
		return outcome, false, err
	}
	if incident.State != domain.IncidentResolved {
		return outcome, false, nil
	}
	outcome.Incident = incident
	switch resource {
	case application.RecoveryResourceTarget:
		target, err := c.Core.GetTarget(ctx, incident.TargetID)
		if err != nil {
			return outcome, true, err
		}
		outcome.Target = &target
	case application.RecoveryResourceAgent:
		view, err := c.Core.GetResearchSession(ctx, incident.SessionID)
		if err != nil {
			return outcome, true, err
		}
		outcome.Agent, outcome.Lease = &view.Agent, &view.Lease
	}
	return outcome, true, nil
}

type targetRecoverySelection struct {
	mode         ports.ResetMode
	snapshotName string
	canonical    string
}

func parseTargetRecoveryStrategy(strategy string) (targetRecoverySelection, error) {
	const operation = "controller.recover_incident"
	if strategy != strings.TrimSpace(strategy) {
		return targetRecoverySelection{}, domain.NewError(domain.CodeInvalidArgument, operation, "strategy", "must not have surrounding whitespace", nil)
	}
	selection := targetRecoverySelection{mode: ports.ResetMode(strategy), canonical: strategy}
	if strings.HasPrefix(strategy, string(ports.ResetSnapshot)+":") {
		selection.mode = ports.ResetSnapshot
		selection.snapshotName = strings.TrimPrefix(strategy, string(ports.ResetSnapshot)+":")
	}
	if err := ports.ValidateResetSelection(selection.mode, selection.snapshotName); err != nil {
		return targetRecoverySelection{}, err
	}
	return selection, nil
}

func requireAgentRecoveryStrategy(strategy string) error {
	if strategy != string(ports.ResetRecreate) {
		return domain.NewError(domain.CodeInvalidArgument, "controller.recover_incident", "strategy", "agent recovery strategy must be recreate", nil)
	}
	return nil
}

type agentPhysicalResource struct {
	ref         ports.AgentWorkspaceRef
	workspaceID domain.WorkspaceID
}

func requirePreviousAgentResource(agent application.AgentWorkspaceRecord) (agentPhysicalResource, error) {
	if agent.CurrentGeneration <= 1 {
		return agentPhysicalResource{}, fmt.Errorf("agent recovery did not advance the generation")
	}
	previousGeneration := agent.CurrentGeneration - 1
	for _, generation := range agent.Generations {
		if generation.Generation != previousGeneration {
			continue
		}
		agentID, err := domain.ParseAgentWorkspaceID(agent.ID)
		if err != nil {
			return agentPhysicalResource{}, err
		}
		workspaceID, err := domain.ParseWorkspaceID(generation.WorkspaceID)
		if err != nil {
			return agentPhysicalResource{}, err
		}
		return agentPhysicalResource{
			ref:         ports.AgentWorkspaceRef{ID: agentID, Generation: domain.AgentGeneration(previousGeneration)},
			workspaceID: workspaceID,
		}, nil
	}
	return agentPhysicalResource{}, fmt.Errorf("agent previous generation is missing")
}

func (c *Controller) completePhysicalRecovery(ctx context.Context, request application.RecoverIncidentRequest, outcome application.RecoveryOutcome, action string) (application.RecoveryOutcome, error) {
	incident, err := c.Core.CompleteIncidentRecovery(ctx, application.TransitionIncidentRequest{
		Meta:                       childMeta(request.Meta, "physical/recovery/resolved", request.Meta.Deadline),
		IncidentID:                 outcome.Incident.ID,
		ExpectedRevision:           outcome.Incident.Revision,
		State:                      domain.IncidentResolved,
		RecoveryActions:            append(append([]string(nil), outcome.Incident.RecoveryActions...), action),
		VisibilityAcknowledgements: append([]string(nil), outcome.Incident.VisibilityAcknowledgements...),
	})
	if err != nil {
		return outcome, err
	}
	outcome.Incident = incident
	return outcome, nil
}
