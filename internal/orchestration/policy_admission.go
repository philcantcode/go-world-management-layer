package orchestration

import (
	"context"
	"fmt"
	"math"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

// EffectivePolicyResolver is the narrow verified-publication contract used at
// the physical admission boundary.
type EffectivePolicyResolver interface {
	Resolve(context.Context, string, string) (*policy.EffectivePolicy, error)
}

// PolicyResourceInventory returns one authoritative, stable projection of all
// logical resources whose physical plans may still consume aggregate policy
// capacity. Admission re-resolves those durable plan identities rather than
// trusting caller-supplied resource totals.
type PolicyResourceInventory func(context.Context) ([]application.ResearchSessionView, error)

// PolicyAdmissionConfig contains only composition facts that are actually
// enforced by the selected drivers. The base resolver remains responsible for
// authenticating material and building exact immutable plans.
type PolicyAdmissionConfig struct {
	Base              ProvisioningResolver
	Policies          EffectivePolicyResolver
	WorkspaceMode     string
	AgentPhysical     ports.AgentWorkspacePhysicalPolicyReport
	AgentReporter     ports.AgentWorkspacePhysicalPolicyReporter
	TargetPhysical    map[domain.TargetKind]map[string]ports.TargetPhysicalPolicyReport
	TargetReporters   map[domain.TargetKind]ports.TargetPhysicalPolicyReporter
	ResourceInventory PolicyResourceInventory
}

// PolicyAdmissionResolver wraps a provisioning authority with verified
// EffectivePolicy resolution and fail-closed checks over every returned
// physical plan.
type PolicyAdmissionResolver struct {
	base              ProvisioningResolver
	policies          EffectivePolicyResolver
	workspaceMode     string
	agentPhysical     ports.AgentWorkspacePhysicalPolicyReport
	agentReporter     ports.AgentWorkspacePhysicalPolicyReporter
	targetPhysical    map[domain.TargetKind]map[string]ports.TargetPhysicalPolicyReport
	targetReporters   map[domain.TargetKind]ports.TargetPhysicalPolicyReporter
	resourceInventory PolicyResourceInventory
}

func NewPolicyAdmissionResolver(config PolicyAdmissionConfig) (*PolicyAdmissionResolver, error) {
	if config.Base == nil || config.Policies == nil {
		return nil, fmt.Errorf("base provisioning resolver and effective-policy resolver are required")
	}
	if config.WorkspaceMode == "" {
		return nil, fmt.Errorf("workspace mode is required")
	}
	if config.ResourceInventory == nil {
		return nil, fmt.Errorf("authoritative policy resource inventory is required")
	}
	if err := validateAgentPhysicalReport(config.AgentPhysical); err != nil {
		return nil, fmt.Errorf("agent physical policy report: %w", err)
	}
	targetReporters := make(map[domain.TargetKind]ports.TargetPhysicalPolicyReporter, len(config.TargetReporters))
	for kind, reporter := range config.TargetReporters {
		if !kind.IsValid() || reporter == nil {
			return nil, fmt.Errorf("target physical policy reporter for %q is invalid", kind)
		}
		targetReporters[kind] = reporter
	}
	targetPhysical := make(map[domain.TargetKind]map[string]ports.TargetPhysicalPolicyReport, len(config.TargetPhysical))
	for kind, reports := range config.TargetPhysical {
		if !kind.IsValid() || len(reports) == 0 {
			return nil, fmt.Errorf("target config-level physical reports for %q are invalid", kind)
		}
		targetPhysical[kind] = make(map[string]ports.TargetPhysicalPolicyReport, len(reports))
		for template, report := range reports {
			if template == "" || report.Template != template || report.Kind != string(kind) {
				return nil, fmt.Errorf("target config-level physical report for %q/%q has mismatched identity", kind, template)
			}
			targetPhysical[kind][template] = cloneTargetPhysicalReport(report)
		}
	}
	return &PolicyAdmissionResolver{
		base: config.Base, policies: config.Policies, workspaceMode: config.WorkspaceMode,
		agentPhysical: cloneAgentPhysicalReport(config.AgentPhysical), agentReporter: config.AgentReporter,
		targetPhysical: targetPhysical, targetReporters: targetReporters,
		resourceInventory: config.ResourceInventory,
	}, nil
}

// AgentWorkspacePlanAdmission is the exact post-bind check used before any
// workspace or container mutation for a concrete agent generation.
type AgentWorkspacePlanAdmission interface {
	AdmitAgentWorkspacePlan(context.Context, ports.AgentWorkspacePlan) error
}

// AgentRecoveryRequestAdmission authorizes the incident classification,
// persisted scope, and policy recovery action before Core rolls the logical
// agent generation.
type AgentRecoveryRequestAdmission interface {
	AdmitAgentRecoveryRequest(context.Context, application.RecoverIncidentRequest, application.ResearchSessionView, application.IncidentRecord) error
}

// TargetRequestAdmission is the pre-allocation policy check used before Core
// persists a target. Exact post-bind admission still runs after IDs exist.
type TargetRequestAdmission interface {
	AdmitTargetRequest(context.Context, application.CreateTargetRequest, application.ResearchSessionView) error
}

// TargetResetAdmission re-resolves the persisted generation pair and exact
// physical target facts before logical rollover and again before driver reset.
type TargetResetAdmission interface {
	AdmitTargetReset(context.Context, application.ResetTargetRequest, application.TargetRecord, application.ResearchSessionView, *application.IncidentRecord) error
}

func (r *PolicyAdmissionResolver) AdmitCapture(ctx context.Context, policyDigest, capabilityDigest string, request policyauthority.CaptureAdmission) error {
	effective, err := r.resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return err
	}
	return policyauthority.ValidateCapture(effective, request)
}

func (r *PolicyAdmissionResolver) AdmitExport(ctx context.Context, policyDigest, capabilityDigest string, request policyauthority.ExportAdmission) error {
	effective, err := r.resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return err
	}
	return policyauthority.ValidateExport(effective, request)
}

func (r *PolicyAdmissionResolver) AdmitAgentRecoveryRequest(ctx context.Context, request application.RecoverIncidentRequest, view application.ResearchSessionView, incident application.IncidentRecord) error {
	policyDigest, capabilityDigest, err := persistedResearchSessionPolicyPair(view, incident.LeaseID)
	if err != nil {
		return err
	}
	effective, err := r.resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return err
	}
	return validateIncidentAgentRecovery(effective.Policy(), request, view, incident)
}

func (r *PolicyAdmissionResolver) AdmitTargetRequest(ctx context.Context, request application.CreateTargetRequest, view application.ResearchSessionView) error {
	policyDigest, capabilityDigest, err := persistedResearchSessionPolicyPair(view, request.LeaseID)
	if err != nil {
		return err
	}
	if request.PolicyDigest != policyDigest || request.CapabilityDigest != capabilityDigest {
		return fmt.Errorf("%w: target request policy pair differs from its persisted lease", policyauthority.ErrPolicyDenied)
	}
	effective, err := r.resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return err
	}
	reports := r.targetPhysical[request.Kind]
	report, found := reports[request.Template]
	if !found {
		return fmt.Errorf("%w: target template %q has no published physical facts", policyauthority.ErrPolicyDenied, request.Template)
	}
	if _, found := r.targetReporters[request.Kind]; !found {
		return fmt.Errorf("%w: target kind %q has no exact-plan physical reporter", policyauthority.ErrPolicyDenied, request.Kind)
	}
	return r.validateTargetPhysicalAdmission(ctx, effective, request, application.TargetRecord{}, report)
}

func (r *PolicyAdmissionResolver) AdmitTargetReset(ctx context.Context, request application.ResetTargetRequest, target application.TargetRecord, view application.ResearchSessionView, incident *application.IncidentRecord) error {
	policyDigest, capabilityDigest, err := persistedResearchSessionPolicyPair(view, target.LeaseID)
	if err != nil {
		return err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return err
	}
	if request.TargetID != target.ID || target.SessionID != view.Session.ID || target.LeaseID != view.Lease.ID ||
		generation.PolicyDigest != policyDigest || generation.CapabilityDigest != capabilityDigest {
		return fmt.Errorf("%w: target reset scope differs from its persisted lease policy", policyauthority.ErrPolicyDenied)
	}
	effective, err := r.resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return err
	}
	template, found := effectiveTargetTemplate(effective.Policy(), target.Template)
	policyMode, supported := policyResetMode(template.Reset.Mode)
	if !found || !supported || request.Mode != policyMode {
		return fmt.Errorf("%w: target reset mode %q is not the policy mode for template %q", policyauthority.ErrPolicyDenied, request.Mode, target.Template)
	}
	if incident != nil {
		if err := validateIncidentTargetRecovery(effective.Policy(), target, *incident, request.Mode); err != nil {
			return err
		}
	}
	return nil
}

func (r *PolicyAdmissionResolver) AdmitAgentWorkspacePlan(ctx context.Context, plan ports.AgentWorkspacePlan) error {
	if r.agentReporter == nil {
		return fmt.Errorf("%w: agent plan physical policy reporter is not connected", policyauthority.ErrPolicyDenied)
	}
	report, err := r.agentReporter.AgentWorkspacePlanPhysicalPolicy(ctx, plan)
	if err != nil {
		return fmt.Errorf("report exact agent physical policy: %w", err)
	}
	if err := validateAgentPlanPhysicalReport(plan, r.agentPhysical, report); err != nil {
		return err
	}
	effective, err := r.resolve(ctx, plan.PolicyDigest.String(), plan.CapabilityFingerprintDigest.String())
	if err != nil {
		return err
	}
	if err := requireAgentResourceSupport(effective, report.Resources); err != nil {
		return err
	}
	if err := requireAgentPhysicalEnforcement(report); err != nil {
		return err
	}
	if err := policyauthority.ValidateAgentPlan(effective, policyauthority.AgentPlanAdmission{
		Runtime: agentRuntimeAdmission(report.Runtime), Resources: physicalAgentRuntimeResources(report.Resources),
	}); err != nil {
		return err
	}
	if err := policyauthority.ValidateNetwork(effective, agentNetworkAdmission(report.Network)); err != nil {
		return err
	}
	return r.validateAggregateCandidate(ctx, effective, aggregateCandidate{
		agentWorkspaceID:     plan.Generation.Spec().AgentWorkspaceID.String(),
		agentProvisioningKey: plan.IdempotencyKey,
		resources: policyauthority.RuntimeResources{
			CPUMilli: report.Resources.CPUMilli.Value, MemoryBytes: report.Resources.MemoryBytes.Value,
			CaptureBytes: report.Resources.CaptureBytes.Value,
		},
	})
}

func (r *PolicyAdmissionResolver) ResolveAcquisition(ctx context.Context, request application.AcquireRequest) (ResolvedAcquisition, error) {
	resolved, err := r.base.ResolveAcquisition(ctx, request)
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	effective, err := r.resolve(ctx, resolved.PolicyDigest.String(), resolved.CapabilityDigest.String())
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := policyauthority.ValidateSessionAcquisition(effective, policyauthority.SessionAdmission{
		PolicyDigest: resolved.PolicyDigest.String(), CapabilityDigest: resolved.CapabilityDigest.String(), TTL: request.TTL,
	}); err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := r.validateResolvedAgent(effective, resolved); err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := r.validateAggregateCandidate(ctx, effective, aggregateCandidate{
		agentAcquisitionKey:  request.Meta.IdempotencyKey,
		agentProvisioningKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/agent"),
		resources:            aggregateRuntimeResources(resolved.Resources),
	}); err != nil {
		return ResolvedAcquisition{}, err
	}
	return resolved, nil
}

// ResolveAgentRecovery revalidates the persisted effective-policy pair and
// every physical agent constraint without treating recovery as a new session
// acquisition or granting a fresh TTL.
func (r *PolicyAdmissionResolver) ResolveAgentRecovery(ctx context.Context, request application.RecoverIncidentRequest, view application.ResearchSessionView) (ResolvedAcquisition, error) {
	resolved, err := r.base.ResolveAgentRecovery(ctx, request, view)
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	effective, err := r.resolve(ctx, resolved.PolicyDigest.String(), resolved.CapabilityDigest.String())
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	incident, found := recoveryIncident(view.Incidents, request.IncidentID)
	if !found {
		return ResolvedAcquisition{}, fmt.Errorf("%w: recovery incident is missing from the persisted session", policyauthority.ErrPolicyDenied)
	}
	if err := validateIncidentAgentRecovery(effective.Policy(), request, view, incident); err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := r.validateResolvedAgent(effective, resolved); err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := r.validateAggregateCandidate(ctx, effective, aggregateCandidate{
		agentWorkspaceID: view.Agent.ID,
		resources:        aggregateRuntimeResources(resolved.Resources),
	}); err != nil {
		return ResolvedAcquisition{}, err
	}
	return resolved, nil
}

// ResolvePersistedAgent revalidates an already-persisted agent plan for
// reconciliation. It deliberately omits acquisition TTL checks: reconciliation
// restores the exact existing lease generation rather than minting a lease.
func (r *PolicyAdmissionResolver) ResolvePersistedAgent(ctx context.Context, view application.ResearchSessionView) (ResolvedAcquisition, error) {
	resolved, err := r.base.ResolvePersistedAgent(ctx, view)
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	effective, err := r.resolve(ctx, resolved.PolicyDigest.String(), resolved.CapabilityDigest.String())
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := r.validateResolvedAgent(effective, resolved); err != nil {
		return ResolvedAcquisition{}, err
	}
	usage, err := r.authoritativeResourceUsage(ctx, aggregateCandidate{})
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	if err := policyauthority.ValidateAggregateResources(effective, usage.resources); err != nil {
		return ResolvedAcquisition{}, err
	}
	return resolved, nil
}

func (r *PolicyAdmissionResolver) validateResolvedAgent(effective *policy.EffectivePolicy, resolved ResolvedAcquisition) error {
	if err := policyauthority.ValidateWorkspace(effective, policyauthority.WorkspaceAdmission{
		Mode: r.workspaceMode, Construction: string(resolved.Construction),
		UpperBytes: resolved.UpperByteLimit, UpperInodes: resolved.UpperInodeLimit,
	}); err != nil {
		return err
	}
	if err := requireAgentResourceSupport(effective, r.agentPhysical.Resources); err != nil {
		return err
	}
	if err := requireAgentPhysicalEnforcement(r.agentPhysical); err != nil {
		return err
	}
	runtime := agentRuntimeAdmission(r.agentPhysical.Runtime)
	runtime.ImageDigest = resolved.ImageDigest.String()
	resources := agentRuntimeResources(resolved.Resources, resolved.UpperByteLimit, resolved.UpperInodeLimit)
	if err := policyauthority.ValidateAgentPlan(effective, policyauthority.AgentPlanAdmission{Runtime: runtime, Resources: resources}); err != nil {
		return err
	}
	if err := policyauthority.ValidateNetwork(effective, agentNetworkAdmission(r.agentPhysical.Network)); err != nil {
		return err
	}
	return nil
}

func (r *PolicyAdmissionResolver) ResolveTarget(ctx context.Context, request application.CreateTargetRequest, target application.TargetRecord) (ports.TargetPlan, error) {
	plan, err := r.base.ResolveTarget(ctx, request, target)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	plan, err = ApplyPersistedTargetProvisioningIdentity(plan, generation)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	effective, err := r.resolve(ctx, plan.PolicyDigest.String(), plan.CapabilityFingerprintDigest.String())
	if err != nil {
		return ports.TargetPlan{}, err
	}
	reporter, found := r.targetReporters[plan.Template.Kind]
	if !found {
		return ports.TargetPlan{}, fmt.Errorf("%w: target kind %q has no physical policy reporter", policyauthority.ErrPolicyDenied, plan.Template.Kind)
	}
	report, err := reporter.TargetPlanPhysicalPolicy(ctx, plan)
	if err != nil {
		return ports.TargetPlan{}, fmt.Errorf("report exact target physical policy: %w", err)
	}
	if err := validateTargetPlanPhysicalReport(plan, report); err != nil {
		return ports.TargetPlan{}, err
	}
	configuredReports := r.targetPhysical[plan.Template.Kind]
	configured, found := configuredReports[plan.Template.Name]
	if !found {
		return ports.TargetPlan{}, fmt.Errorf("%w: target template %q has no published physical facts", policyauthority.ErrPolicyDenied, plan.Template.Name)
	}
	if err := requireMatchingTargetPhysicalFacts(configured, report); err != nil {
		return ports.TargetPlan{}, err
	}
	if err := r.validateTargetPhysicalAdmissionCandidate(ctx, effective, plan.Template.Name, report, aggregateCandidate{
		targetID:              plan.Target.ID().String(),
		targetProvisioningKey: plan.IdempotencyKey,
		resources:             aggregateRuntimeResources(plan.Resources),
	}); err != nil {
		return ports.TargetPlan{}, err
	}
	return plan, nil
}

func (r *PolicyAdmissionResolver) validateTargetPhysicalAdmission(ctx context.Context, effective *policy.EffectivePolicy, request application.CreateTargetRequest, target application.TargetRecord, report ports.TargetPhysicalPolicyReport) error {
	return r.validateTargetPhysicalAdmissionCandidate(ctx, effective, request.Template, report, aggregateCandidate{
		targetID:              target.ID,
		targetCreationKey:     request.Meta.IdempotencyKey,
		targetProvisioningKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/target"),
		resources: policyauthority.RuntimeResources{
			CPUMilli: report.Resources.CPUMilli.Value, MemoryBytes: report.Resources.MemoryBytes.Value,
			CaptureBytes: report.Resources.CaptureBytes.Value,
		},
	})
}

func (r *PolicyAdmissionResolver) validateTargetPhysicalAdmissionCandidate(ctx context.Context, effective *policy.EffectivePolicy, template string, report ports.TargetPhysicalPolicyReport, candidate aggregateCandidate) error {
	if err := requireTargetPhysicalEnforcement(report); err != nil {
		return err
	}
	if err := requireTargetResourceSupport(effective, template, report.Resources); err != nil {
		return err
	}
	facts := targetAdmission(report)
	concurrent, err := r.admitAggregateCandidate(ctx, effective, candidate)
	if err != nil {
		return err
	}
	facts.ConcurrentTargets = concurrent
	if err := policyauthority.ValidateTarget(effective, facts); err != nil {
		return err
	}
	return nil
}

type aggregateCandidate struct {
	agentAcquisitionKey   string
	agentWorkspaceID      string
	agentProvisioningKey  string
	targetCreationKey     string
	targetID              string
	targetProvisioningKey string
	resources             policyauthority.RuntimeResources
}

type authoritativeResourceUsage struct {
	resources         policyauthority.RuntimeResources
	concurrentTargets int64
}

func (r *PolicyAdmissionResolver) admitAggregateCandidate(ctx context.Context, effective *policy.EffectivePolicy, candidate aggregateCandidate) (int64, error) {
	usage, err := r.authoritativeResourceUsage(ctx, candidate)
	if err != nil {
		return 0, err
	}
	total, err := addAggregateResources(usage.resources, candidate.resources)
	if err != nil {
		return 0, err
	}
	if err := policyauthority.ValidateAggregateResources(effective, total); err != nil {
		return 0, err
	}
	return usage.concurrentTargets, nil
}

func (r *PolicyAdmissionResolver) validateAggregateCandidate(ctx context.Context, effective *policy.EffectivePolicy, candidate aggregateCandidate) error {
	_, err := r.admitAggregateCandidate(ctx, effective, candidate)
	return err
}

func (r *PolicyAdmissionResolver) authoritativeResourceUsage(ctx context.Context, candidate aggregateCandidate) (authoritativeResourceUsage, error) {
	views, err := r.resourceInventory(ctx)
	if err != nil {
		return authoritativeResourceUsage{}, fmt.Errorf("read authoritative policy resource inventory: %w", err)
	}
	var usage authoritativeResourceUsage
	var excludedAgent, excludedTarget bool
	for _, view := range views {
		policyDigest, capabilityDigest, err := persistedResearchSessionPolicyPair(view, view.Lease.ID)
		if err != nil {
			return authoritativeResourceUsage{}, fmt.Errorf("inventory session %q: %w", view.Session.ID, err)
		}
		generation, err := currentAgentGeneration(view.Agent)
		if err != nil {
			return authoritativeResourceUsage{}, fmt.Errorf("inventory agent %q: %w", view.Agent.ID, err)
		}
		if !generation.State.Terminal() {
			matchesCandidate := (candidate.agentAcquisitionKey != "" && candidate.agentAcquisitionKey == view.Session.AcquisitionIdempotencyKey) ||
				(candidate.agentWorkspaceID != "" && candidate.agentWorkspaceID == view.Agent.ID) ||
				(candidate.agentProvisioningKey != "" && generation.AgentProvisioningKey != "" && candidate.agentProvisioningKey == generation.AgentProvisioningKey)
			if matchesCandidate {
				if excludedAgent {
					return authoritativeResourceUsage{}, domain.NewError(domain.CodeIntegrityViolation, "policy.aggregate_inventory", "agent_candidate", "multiple live agents match one aggregate admission candidate", nil)
				}
				excludedAgent = true
			} else {
				resolved, err := r.base.ResolvePersistedAgent(ctx, view)
				if err != nil {
					return authoritativeResourceUsage{}, fmt.Errorf("resolve inventory agent %q: %w", view.Agent.ID, err)
				}
				if err := resolved.Validate(); err != nil {
					return authoritativeResourceUsage{}, fmt.Errorf("validate inventory agent %q: %w", view.Agent.ID, err)
				}
				if resolved.PolicyDigest.String() != policyDigest || resolved.CapabilityDigest.String() != capabilityDigest {
					return authoritativeResourceUsage{}, domain.NewError(domain.CodeIntegrityViolation, "policy.aggregate_inventory", "agent_policy", "resolved inventory agent differs from its persisted policy pair", nil)
				}
				usage.resources, err = addAggregateResources(usage.resources, aggregateRuntimeResources(resolved.Resources))
				if err != nil {
					return authoritativeResourceUsage{}, err
				}
			}
		}

		for _, target := range view.Targets {
			targetGeneration, err := targetGeneration(target)
			if err != nil {
				return authoritativeResourceUsage{}, fmt.Errorf("inventory target %q: %w", target.ID, err)
			}
			if target.SessionID != view.Session.ID || target.LeaseID != view.Lease.ID ||
				targetGeneration.PolicyDigest != policyDigest || targetGeneration.CapabilityDigest != capabilityDigest {
				return authoritativeResourceUsage{}, domain.NewError(domain.CodeIntegrityViolation, "policy.aggregate_inventory", "target_scope", "inventory target differs from its persisted session policy scope", nil)
			}
			if targetGeneration.State.Terminal() {
				continue
			}
			matchesCandidate := (candidate.targetCreationKey != "" && candidate.targetCreationKey == target.CreationIdempotencyKey) ||
				(candidate.targetID != "" && candidate.targetID == target.ID) ||
				(candidate.targetProvisioningKey != "" && targetGeneration.ProvisioningKey != "" && candidate.targetProvisioningKey == targetGeneration.ProvisioningKey)
			if matchesCandidate {
				if excludedTarget {
					return authoritativeResourceUsage{}, domain.NewError(domain.CodeIntegrityViolation, "policy.aggregate_inventory", "target_candidate", "multiple live targets match one aggregate admission candidate", nil)
				}
				excludedTarget = true
				continue
			}
			meta := application.MutationMeta{IdempotencyKey: "aggregate-inventory/" + target.ID, Deadline: deadline(ctx)}
			request, err := persistedTargetProvisioningRequest(meta, target)
			if err != nil {
				return authoritativeResourceUsage{}, err
			}
			plan, err := r.base.ResolveTarget(ctx, request, target)
			if err != nil {
				return authoritativeResourceUsage{}, fmt.Errorf("resolve inventory target %q: %w", target.ID, err)
			}
			plan, err = ApplyPersistedTargetProvisioningIdentity(plan, targetGeneration)
			if err != nil {
				return authoritativeResourceUsage{}, fmt.Errorf("bind inventory target %q: %w", target.ID, err)
			}
			if err := plan.Validate(); err != nil {
				return authoritativeResourceUsage{}, fmt.Errorf("validate inventory target %q: %w", target.ID, err)
			}
			usage.resources, err = addAggregateResources(usage.resources, aggregateRuntimeResources(plan.Resources))
			if err != nil {
				return authoritativeResourceUsage{}, err
			}
			if usage.concurrentTargets == math.MaxInt64 {
				return authoritativeResourceUsage{}, fmt.Errorf("%w: live target count overflows", policyauthority.ErrPolicyDenied)
			}
			usage.concurrentTargets++
		}
	}
	return usage, nil
}

func addAggregateResources(current, additional policyauthority.RuntimeResources) (policyauthority.RuntimeResources, error) {
	var result policyauthority.RuntimeResources
	var err error
	if result.CPUMilli, err = addAggregateResource("cpu_milli", current.CPUMilli, additional.CPUMilli); err != nil {
		return policyauthority.RuntimeResources{}, err
	}
	if result.MemoryBytes, err = addAggregateResource("memory_bytes", current.MemoryBytes, additional.MemoryBytes); err != nil {
		return policyauthority.RuntimeResources{}, err
	}
	if result.CaptureBytes, err = addAggregateResource("capture_bytes", current.CaptureBytes, additional.CaptureBytes); err != nil {
		return policyauthority.RuntimeResources{}, err
	}
	return result, nil
}

func addAggregateResource(field string, current, additional int64) (int64, error) {
	if current < 0 || additional < 0 || current > math.MaxInt64-additional {
		return 0, fmt.Errorf("%w: aggregate %s usage overflows", policyauthority.ErrPolicyDenied, field)
	}
	return current + additional, nil
}

func (r *PolicyAdmissionResolver) ResolveTargetMaterial(ctx context.Context, request application.StartTargetRunRequest, target application.TargetRecord) (ResolvedTargetRun, error) {
	resolved, err := r.base.ResolveTargetMaterial(ctx, request, target)
	if err != nil {
		return ResolvedTargetRun{}, err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return ResolvedTargetRun{}, err
	}
	effective, err := r.resolve(ctx, generation.PolicyDigest, generation.CapabilityDigest)
	if err != nil {
		return ResolvedTargetRun{}, err
	}
	materialBytes, err := totalMaterialBytes(resolved.Material)
	if err != nil {
		return ResolvedTargetRun{}, err
	}
	collectors := make([]policyauthority.CollectorAdmission, len(resolved.Collectors))
	for index, collector := range resolved.Collectors {
		collectors[index] = policyauthority.CollectorAdmission{
			Adapter: collector.Adapter, Placement: string(collector.Requirement.Placement), MaximumBytes: collector.MaximumBytes,
		}
	}
	if err := policyauthority.ValidateTargetRun(effective, policyauthority.TargetRunAdmission{
		Template: target.Template, MaterialBytes: materialBytes, MaximumDuration: resolved.MaximumDuration,
		RequiredCoverage: append([]string(nil), resolved.RequiredCoverage...), Collectors: collectors,
	}); err != nil {
		return ResolvedTargetRun{}, err
	}
	return resolved, nil
}

func (r *PolicyAdmissionResolver) resolve(ctx context.Context, policyDigest, capabilityDigest string) (*policy.EffectivePolicy, error) {
	effective, err := r.policies.Resolve(ctx, policyDigest, capabilityDigest)
	if err != nil {
		return nil, err
	}
	if err := policyauthority.ValidateIdentity(effective, policyDigest, capabilityDigest); err != nil {
		return nil, err
	}
	return effective, nil
}

func agentRuntimeResources(resources admission.Resources, workspaceBytes, workspaceInodes int64) policyauthority.RuntimeResources {
	return policyauthority.RuntimeResources{
		CPUMilli: resources.CPUMilli, MemoryBytes: resources.MemoryBytes, SwapBytes: resources.SwapBytes, WorkspaceBytes: workspaceBytes,
		CaptureBytes: resources.CaptureBytes, Inodes: workspaceInodes, PIDs: resources.PIDs,
	}
}

func targetRuntimeResources(resources admission.Resources) policyauthority.RuntimeResources {
	return policyauthority.RuntimeResources{
		CPUMilli: resources.CPUMilli, MemoryBytes: resources.MemoryBytes, SwapBytes: resources.SwapBytes, WritableStateBytes: resources.StorageBytes,
		CaptureBytes: resources.CaptureBytes, Inodes: resources.Inodes, PIDs: resources.PIDs,
	}
}

func aggregateRuntimeResources(resources admission.Resources) policyauthority.RuntimeResources {
	return policyauthority.RuntimeResources{CPUMilli: resources.CPUMilli, MemoryBytes: resources.MemoryBytes, CaptureBytes: resources.CaptureBytes}
}

func totalMaterialBytes(material []ports.TargetMaterialPlan) (int64, error) {
	var total int64
	for index := range material {
		size := material[index].Artifact.Spec().Size
		if size < 0 || total > math.MaxInt64-size {
			return 0, fmt.Errorf("target material byte total overflows")
		}
		total += size
	}
	return total, nil
}

func effectiveTargetTemplate(document policy.Policy, name string) (policy.TargetTemplate, bool) {
	for _, template := range document.Spec.Targets.Templates {
		if template.Name == name {
			return template, true
		}
	}
	return policy.TargetTemplate{}, false
}

func validateIncidentTargetRecovery(document policy.Policy, target application.TargetRecord, incident application.IncidentRecord, mode ports.ResetMode) error {
	if incident.TargetID != target.ID || incident.LeaseID != target.LeaseID || incident.SessionID != target.SessionID ||
		(incident.TargetGeneration != target.CurrentGeneration && incident.TargetGeneration+1 != target.CurrentGeneration) {
		return fmt.Errorf("%w: recovery incident does not identify the target generation", policyauthority.ErrPolicyDenied)
	}
	recovery := document.Spec.Incidents.Recovery
	var action string
	switch incident.Classification {
	case domain.IncidentLinuxTargetFailure:
		if target.Kind != domain.TargetLinuxContainer {
			return fmt.Errorf("%w: Linux target incident has a non-Linux target", policyauthority.ErrPolicyDenied)
		}
		action = recovery.LinuxTargetFailure
	case domain.IncidentEmulatorFailure, domain.IncidentAndroidFailure, domain.IncidentDeviceDisconnect:
		if target.Kind != domain.TargetAndroidVirtualDevice {
			return fmt.Errorf("%w: Android runtime incident has a non-Android target", policyauthority.ErrPolicyDenied)
		}
		action = recovery.AndroidRuntimeFailure
	case domain.IncidentTargetWorkloadExit:
		action = recovery.TargetWorkloadExit
	case domain.IncidentObserverFailure:
		action = recovery.ObserverFailure
	default:
		return fmt.Errorf("%w: incident classification %q has no target-generation recovery policy", policyauthority.ErrPolicyDenied, incident.Classification)
	}
	expected, supported := policyResetMode(action)
	if !supported {
		return fmt.Errorf("%w: incident recovery action %q does not authorize a target-generation reset", policyauthority.ErrPolicyDenied, action)
	}
	if mode != expected {
		return fmt.Errorf("%w: incident recovery mode %q differs from policy mode %q", policyauthority.ErrPolicyDenied, mode, expected)
	}
	return nil
}

func policyResetMode(action string) (ports.ResetMode, bool) {
	switch action {
	case "recreate-new-target-generation":
		return ports.ResetRecreate, true
	case "baseline-new-target-generation":
		return ports.ResetBaseline, true
	default:
		return "", false
	}
}

func validateIncidentAgentRecovery(document policy.Policy, request application.RecoverIncidentRequest, view application.ResearchSessionView, incident application.IncidentRecord) error {
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return err
	}
	if request.Resource != application.RecoveryResourceAgent || request.IncidentID != incident.ID ||
		incident.SessionID != view.Session.ID || incident.LeaseID != view.Lease.ID || incident.AgentWorkspaceID != view.Agent.ID ||
		(incident.AgentGeneration != generation.Generation && incident.AgentGeneration+1 != generation.Generation) {
		return fmt.Errorf("%w: recovery incident does not identify the agent generation", policyauthority.ErrPolicyDenied)
	}
	if incident.State != domain.IncidentEvidenceSealed && incident.State != domain.IncidentRecovering {
		return fmt.Errorf("%w: agent recovery incident is not sealed or recovering", policyauthority.ErrPolicyDenied)
	}
	if request.Strategy != string(ports.ResetRecreate) {
		return fmt.Errorf("%w: agent recovery strategy %q is not recreate", policyauthority.ErrPolicyDenied, request.Strategy)
	}
	if incident.Classification != domain.IncidentAgentWorkspaceFailure || document.Spec.Incidents.Recovery.AgentWorkspaceFailure != "recreate-new-agent-generation" {
		return fmt.Errorf("%w: incident classification is not authorized for agent generation recreation", policyauthority.ErrPolicyDenied)
	}
	return nil
}

var _ ProvisioningResolver = (*PolicyAdmissionResolver)(nil)
var _ AgentWorkspacePlanAdmission = (*PolicyAdmissionResolver)(nil)
var _ AgentRecoveryRequestAdmission = (*PolicyAdmissionResolver)(nil)
var _ LeaseOperationPolicyAdmission = (*PolicyAdmissionResolver)(nil)
var _ TargetRequestAdmission = (*PolicyAdmissionResolver)(nil)
var _ TargetResetAdmission = (*PolicyAdmissionResolver)(nil)
