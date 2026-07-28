package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// preflightPhysicalPolicyPlans runs every immutable deployment plan through
// the same effective-policy and live reporter checks used immediately before
// mutation. This prevents publication from advertising a plan that the
// selected physical composition can never enforce.
func preflightPhysicalPolicyPlans(ctx context.Context, timeout time.Duration, deployment builtDeployment, config orchestration.PolicyAdmissionConfig) error {
	config.ResourceInventory = func(context.Context) ([]application.ResearchSessionView, error) {
		return nil, nil
	}
	resolver, err := orchestration.NewPolicyAdmissionResolver(config)
	if err != nil {
		return fmt.Errorf("open physical policy preflight: %w", err)
	}
	for _, inputViewID := range sortedStringKeys(deployment.static.Agents) {
		configured := deployment.static.Agents[inputViewID]
		if err := runProbe(ctx, timeout, "agent policy plan "+inputViewID, func(probeCtx context.Context) error {
			effective, err := config.Policies.Resolve(probeCtx, configured.PolicyDigest.String(), configured.CapabilityDigest.String())
			if err != nil {
				return err
			}
			resolved, err := resolver.ResolveAcquisition(probeCtx, application.AcquireRequest{
				InputViewID: inputViewID, PolicyDigest: configured.PolicyDigest.String(), CapabilityDigest: configured.CapabilityDigest.String(),
				TTL: effective.Policy().Spec.Lease.TTL.Duration(),
			})
			if err != nil {
				return err
			}
			plan, err := preflightAgentPlan(inputViewID, resolved)
			if err != nil {
				return err
			}
			return resolver.AdmitAgentWorkspacePlan(probeCtx, plan)
		}); err != nil {
			return err
		}
	}
	for _, reference := range sortedStringKeys(deployment.static.Targets) {
		configured := deployment.static.Targets[reference]
		if err := runProbe(ctx, timeout, "target policy plan "+reference, func(probeCtx context.Context) error {
			target, request, err := preflightTarget(reference, configured)
			if err != nil {
				return err
			}
			_, err = resolver.ResolveTarget(probeCtx, request, target)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func preflightAgentPlan(inputViewID string, resolved orchestration.ResolvedAcquisition) (ports.AgentWorkspacePlan, error) {
	leaseID, err := domain.NewLeaseID()
	if err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	agentID, err := domain.NewAgentWorkspaceID()
	if err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	workspaceID, err := domain.NewWorkspaceID()
	if err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	createdAt := time.Now().UTC()
	generation, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
		AgentWorkspaceID: agentID, Generation: 1, WorkspaceID: workspaceID, InputViewID: resolved.InputView.ID(),
		PolicyDigest: resolved.PolicyDigest, CapabilityFingerprintDigest: resolved.CapabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: 1,
		InputViewID: resolved.InputView.ID(), CreatedAt: createdAt,
	})
	if err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	return orchestration.BuildAgentWorkspacePlan("physical-policy-preflight/"+inputViewID, leaseID, generation, workspace, resolved)
}

func preflightTarget(reference string, configured orchestration.StaticTargetPlan) (application.TargetRecord, application.CreateTargetRequest, error) {
	targetID, err := domain.NewTargetID()
	if err != nil {
		return application.TargetRecord{}, application.CreateTargetRequest{}, err
	}
	sessionID, err := domain.NewResearchSessionID()
	if err != nil {
		return application.TargetRecord{}, application.CreateTargetRequest{}, err
	}
	leaseID, err := domain.NewLeaseID()
	if err != nil {
		return application.TargetRecord{}, application.CreateTargetRequest{}, err
	}
	now := time.Now().UTC()
	target := application.TargetRecord{
		ID: targetID.String(), SessionID: sessionID.String(), LeaseID: leaseID.String(), Template: reference,
		Kind: configured.Template.Kind, CurrentGeneration: 1, CreatedAt: now, UpdatedAt: now,
		Generations: []application.TargetGenerationRecord{{
			Generation: 1, PolicyDigest: configured.PolicyDigest.String(), CapabilityDigest: configured.CapabilityDigest.String(),
			State: domain.TargetGenerationProvisioning, Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
	}
	request := application.CreateTargetRequest{
		Meta:     application.MutationMeta{IdempotencyKey: "physical-policy-preflight/" + reference},
		LeaseID:  leaseID.String(),
		Template: reference, Kind: configured.Template.Kind,
		PolicyDigest: configured.PolicyDigest.String(), CapabilityDigest: configured.CapabilityDigest.String(),
	}
	return target, request, nil
}
