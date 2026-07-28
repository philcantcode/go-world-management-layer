package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const defaultControllerCleanupTimeout = 10 * time.Second

type ControllerConfig struct {
	Core           *application.Core
	Agent          ports.AgentWorkspaceDriver
	Targets        map[domain.TargetKind]ports.TargetDriver
	Workspace      ports.WorkspaceDriver
	Resolver       ProvisioningResolver
	Capabilities   *Service
	Observers      *RunObserverCoordinator
	CleanupTimeout time.Duration
}

// Controller wraps the logical Core with physical lifecycle orchestration.
// Its embedded Core supplies the rest of rpc.Core unchanged; the overridden
// methods below are the only mutations that create or retire host resources.
type Controller struct {
	*application.Core
	agent          ports.AgentWorkspaceDriver
	targets        map[domain.TargetKind]ports.TargetDriver
	workspace      ports.WorkspaceDriver
	resolver       ProvisioningResolver
	capabilities   *Service
	observers      *RunObserverCoordinator
	cleanupTimeout time.Duration
	mu             sync.Mutex
}

func NewController(config ControllerConfig) (*Controller, error) {
	if config.Core == nil {
		return nil, fmt.Errorf("application core is required")
	}
	targets := make(map[domain.TargetKind]ports.TargetDriver, len(config.Targets))
	for kind, driver := range config.Targets {
		if !kind.IsValid() || driver == nil {
			return nil, fmt.Errorf("target driver map contains an invalid kind or nil driver")
		}
		targets[kind] = driver
	}
	physical := config.Agent != nil || config.Workspace != nil || config.Resolver != nil || len(targets) != 0
	if physical && (config.Agent == nil || config.Workspace == nil || config.Resolver == nil) {
		return nil, fmt.Errorf("a physical composition requires agent, workspace, and trusted provisioning resolver together")
	}
	if len(targets) != 0 && config.Capabilities == nil {
		return nil, fmt.Errorf("target drivers require evidence-finalization capabilities for rollback")
	}
	if len(targets) != 0 && config.Observers == nil {
		return nil, fmt.Errorf("target drivers require a run observer coordinator")
	}
	if config.CleanupTimeout <= 0 {
		config.CleanupTimeout = defaultControllerCleanupTimeout
	}
	return &Controller{
		Core: config.Core, agent: config.Agent, targets: targets, workspace: config.Workspace,
		resolver: config.Resolver, capabilities: config.Capabilities, observers: config.Observers, cleanupTimeout: config.CleanupTimeout,
	}, nil
}

func (c *Controller) AcquireResearchSession(ctx context.Context, request application.AcquireRequest) (application.ResearchSessionView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.AcquireResearchSession(ctx, request)
	}
	if err := c.requireAgentLifecycle("acquire_research_session"); err != nil {
		return application.ResearchSessionView{}, err
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()
	resolved, err := c.resolver.ResolveAcquisition(operationCtx, request)
	if err != nil {
		return application.ResearchSessionView{}, err
	}
	if err := resolved.Validate(); err != nil {
		return application.ResearchSessionView{}, err
	}
	request.InputViewID = resolved.InputView.ID().String()
	request.InputSelection = application.InputSelectionRequest{}
	request.PolicyDigest = resolved.PolicyDigest.String()
	request.CapabilityDigest = resolved.CapabilityDigest.String()
	view, err := c.Core.AcquireResearchSession(operationCtx, request)
	if err != nil {
		return application.ResearchSessionView{}, err
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return view, err
	}
	resumingDurableGeneration := generation.State != domain.AgentGenerationProvisioning || agentGenerationProvisioningIdentityPresent(generation)
	if generation.State == domain.AgentGenerationFailed && view.Lease.State != domain.LeaseReleased {
		plan, bindErr := bindAgentProvisioning(request, resolved, view)
		if bindErr != nil {
			return view, bindErr
		}
		return view, c.rollbackAcquisition(request.Meta, view, &plan, fmt.Errorf("retrying cleanup for a failed physical acquisition"))
	}
	if generation.State.Terminal() || view.Lease.State != domain.LeaseActive {
		return view, domain.NewError(domain.CodeFailedPrecondition, "controller.acquire", "generation", "the idempotent acquisition is already terminal", nil)
	}
	plan, err := bindAgentProvisioning(request, resolved, view)
	if err != nil {
		return view, c.failAcquisitionAttempt(request.Meta, view, nil, resumingDurableGeneration, err)
	}
	if err := c.admitAgentWorkspacePlan(operationCtx, plan.Agent); err != nil {
		return view, c.failAcquisitionAttempt(request.Meta, view, &plan, resumingDurableGeneration, err)
	}
	view, err = c.bindAgentProvisioningPlan(operationCtx, request.Meta, view, plan)
	if err != nil {
		return view, c.failAcquisitionAttempt(request.Meta, view, &plan, resumingDurableGeneration, err)
	}
	if err := c.provisionAgentPhysical(operationCtx, plan); err != nil {
		return view, c.failAcquisitionAttempt(request.Meta, view, &plan, resumingDurableGeneration, err)
	}
	if _, err := c.advanceAgentReady(operationCtx, request.Meta, view.Agent); err != nil {
		return view, c.failAcquisitionAttempt(request.Meta, view, &plan, resumingDurableGeneration, err)
	}
	return c.Core.GetResearchSession(operationCtx, view.Session.ID)
}

func agentGenerationProvisioningIdentityPresent(generation application.AgentGenerationRecord) bool {
	return generation.ProvisioningPlanDigest != "" || generation.WorkspaceProvisioningKey != "" || generation.AgentProvisioningKey != ""
}

// failAcquisitionAttempt compensates only resources whose durable generation
// was established by this invocation. A retry must never turn resolver drift,
// an admission outage, or an inspection failure into deletion of a generation
// that was already provisioned before the retry began.
func (c *Controller) failAcquisitionAttempt(meta application.MutationMeta, view application.ResearchSessionView, plan *AgentProvisioningPlan, resumingDurableGeneration bool, cause error) error {
	if resumingDurableGeneration {
		return cause
	}
	return c.rollbackAcquisition(meta, view, plan, cause)
}

func (c *Controller) bindAgentProvisioningPlan(ctx context.Context, meta application.MutationMeta, view application.ResearchSessionView, plan AgentProvisioningPlan) (application.ResearchSessionView, error) {
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return view, err
	}
	digest, err := AgentProvisioningPlanDigest(plan)
	if err != nil {
		return view, err
	}
	if generation.ProvisioningPlanDigest != "" {
		if generation.ProvisioningPlanDigest != digest.String() || generation.WorkspaceProvisioningKey != plan.Workspace.IdempotencyKey || generation.AgentProvisioningKey != plan.Agent.IdempotencyKey {
			return view, domain.NewError(domain.CodeIntegrityViolation, "controller.bind_agent_plan", "provisioning", "resolved plan differs from the durable generation binding", nil)
		}
		return view, nil
	}
	agent, err := c.Core.BindAgentGenerationPlan(ctx, application.BindAgentGenerationPlanRequest{
		Meta: childMeta(meta, "physical/plan-binding", meta.Deadline), AgentWorkspaceID: view.Agent.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision, ProvisioningPlanDigest: digest.String(),
		WorkspaceProvisioningKey: plan.Workspace.IdempotencyKey, AgentProvisioningKey: plan.Agent.IdempotencyKey,
	})
	if err != nil {
		return view, err
	}
	view.Agent = agent
	return view, nil
}

func targetGenerationProvisioningIdentityPresent(generation application.TargetGenerationRecord) bool {
	return generation.ProvisioningPlanDigest != "" || generation.ProvisioningKey != ""
}

func (c *Controller) bindTargetProvisioningPlan(ctx context.Context, meta application.MutationMeta, target application.TargetRecord, plan ports.TargetPlan) (application.TargetRecord, error) {
	generation, err := targetGeneration(target)
	if err != nil {
		return target, err
	}
	digest, err := TargetProvisioningPlanDigest(plan)
	if err != nil {
		return target, err
	}
	if targetGenerationProvisioningIdentityPresent(generation) {
		if generation.ProvisioningPlanDigest != digest.String() || generation.ProvisioningKey != plan.IdempotencyKey {
			return target, domain.NewError(domain.CodeIntegrityViolation, "controller.bind_target_plan", "provisioning", "resolved plan differs from the durable target generation binding", nil)
		}
		return target, nil
	}
	return c.Core.BindTargetGenerationPlan(ctx, application.BindTargetGenerationPlanRequest{
		Meta: childMeta(meta, "physical/target-plan-binding", meta.Deadline), TargetID: target.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision,
		ProvisioningPlanDigest: digest.String(), ProvisioningKey: plan.IdempotencyKey,
	})
}

func persistedTargetProvisioningRequest(meta application.MutationMeta, target application.TargetRecord) (application.CreateTargetRequest, error) {
	generation, err := targetGeneration(target)
	if err != nil {
		return application.CreateTargetRequest{}, err
	}
	return application.CreateTargetRequest{
		Meta: meta, LeaseID: target.LeaseID, Template: target.Template, Kind: target.Kind,
		PolicyDigest: generation.PolicyDigest, CapabilityDigest: generation.CapabilityDigest,
	}, nil
}

func (c *Controller) resolvePersistedTargetProvisioningPlan(ctx context.Context, meta application.MutationMeta, target application.TargetRecord, physicalKey string) (ports.TargetPlan, error) {
	request, err := persistedTargetProvisioningRequest(meta, target)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	plan, err := c.resolver.ResolveTarget(ctx, request, target)
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
	if !targetGenerationProvisioningIdentityPresent(generation) {
		plan.IdempotencyKey = physicalKey
	} else if plan.IdempotencyKey != physicalKey {
		return ports.TargetPlan{}, domain.NewError(domain.CodeIntegrityViolation, "controller.resolve_persisted_target_plan", "provisioning_key", "persisted target generation is bound to a different physical operation", nil)
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetPlan{}, err
	}
	return plan, nil
}

func (c *Controller) admitAgentWorkspacePlan(ctx context.Context, plan ports.AgentWorkspacePlan) error {
	admission, ok := c.resolver.(AgentWorkspacePlanAdmission)
	if !ok {
		return nil
	}
	return admission.AdmitAgentWorkspacePlan(ctx, plan)
}

func (c *Controller) admitAgentRecoveryRequest(ctx context.Context, request application.RecoverIncidentRequest, view application.ResearchSessionView, incident application.IncidentRecord) error {
	admission, ok := c.resolver.(AgentRecoveryRequestAdmission)
	if !ok {
		return nil
	}
	return admission.AdmitAgentRecoveryRequest(ctx, request, view, incident)
}

func (c *Controller) admitTargetRequest(ctx context.Context, request application.CreateTargetRequest, view application.ResearchSessionView) error {
	admission, ok := c.resolver.(TargetRequestAdmission)
	if !ok {
		return nil
	}
	return admission.AdmitTargetRequest(ctx, request, view)
}

func (c *Controller) admitTargetReset(ctx context.Context, request application.ResetTargetRequest, target application.TargetRecord, view application.ResearchSessionView, incident *application.IncidentRecord) error {
	admission, ok := c.resolver.(TargetResetAdmission)
	if !ok {
		return nil
	}
	return admission.AdmitTargetReset(ctx, request, target, view, incident)
}

func (c *Controller) resolveAndAdmitTargetReset(ctx context.Context, request application.ResetTargetRequest, target application.TargetRecord, view application.ResearchSessionView, incident *application.IncidentRecord, physicalKey string) (ports.TargetPlan, error) {
	var plan ports.TargetPlan
	var err error
	if physicalKey == "" {
		provisioningRequest, requestErr := persistedTargetProvisioningRequest(request.Meta, target)
		if requestErr != nil {
			return ports.TargetPlan{}, requestErr
		}
		plan, err = c.resolver.ResolveTarget(ctx, provisioningRequest, target)
	} else {
		plan, err = c.resolvePersistedTargetProvisioningPlan(ctx, request.Meta, target, physicalKey)
	}
	if err != nil {
		return ports.TargetPlan{}, err
	}
	if err := c.admitTargetReset(ctx, request, target, view, incident); err != nil {
		return ports.TargetPlan{}, err
	}
	return plan, nil
}

// provisionAgentPhysical establishes the exact workspace and agent plan. It is
// shared by initial acquisition and incident recovery so both paths enforce the
// same identity, mount, resource, and driver-result checks.
func (c *Controller) provisionAgentPhysical(ctx context.Context, plan AgentProvisioningPlan) error {
	prepared, err := c.workspace.Prepare(ctx, plan.Workspace)
	if err != nil {
		return err
	}
	if err := validateWorkspaceHandle(plan.Workspace, prepared, domain.WorkspaceReady, "prepare"); err != nil {
		return err
	}
	mounted, err := c.workspace.Mount(ctx, plan.Workspace.Workspace.ID())
	if err != nil {
		return err
	}
	if err := validateWorkspaceHandle(plan.Workspace, mounted, domain.WorkspaceMounted, "mount"); err != nil {
		return err
	}
	if mounted.MergedPath != prepared.MergedPath {
		return fmt.Errorf("workspace mount changed the authoritative merged path")
	}
	result, err := c.agent.Provision(ctx, plan.Agent)
	if err != nil {
		return err
	}
	return validateAgentProvisioningResult(plan.Agent, result)
}

func (c *Controller) CreateTarget(ctx context.Context, request application.CreateTargetRequest) (application.TargetRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.CreateTarget(ctx, request)
	}
	driver, err := c.requireTargetLifecycle("create_target", request.Kind)
	if err != nil {
		return application.TargetRecord{}, err
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()
	view, err := c.Core.GetResearchSessionByLease(operationCtx, request.LeaseID)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if err := c.admitTargetRequest(operationCtx, request, view); err != nil {
		return application.TargetRecord{}, fmt.Errorf("admit target request: %w", err)
	}
	target, err := c.Core.CreateTarget(operationCtx, request)
	if err != nil {
		return application.TargetRecord{}, err
	}
	generation, generationErr := targetGeneration(target)
	if generationErr != nil {
		return target, generationErr
	}
	if generation.State.Terminal() {
		return target, domain.NewError(domain.CodeFailedPrecondition, "controller.create_target", "generation", "the idempotent target creation is already terminal", nil)
	}
	prebound := targetGenerationProvisioningIdentityPresent(generation)
	plan, err := c.resolver.ResolveTarget(operationCtx, request, target)
	if err != nil {
		return target, c.failTargetProvisioningAttempt(request.Meta, target, driver, nil, prebound, err)
	}
	target, err = c.bindTargetProvisioningPlan(operationCtx, request.Meta, target, plan)
	if err != nil {
		return target, c.failTargetProvisioningAttempt(request.Meta, target, driver, nil, prebound, err)
	}
	result, err := driver.Create(operationCtx, plan)
	ref := ports.TargetRef{ID: plan.Target.ID(), Generation: plan.Generation.Spec().Generation}
	if err != nil {
		return target, c.failTargetProvisioningAttempt(request.Meta, target, driver, []ports.TargetRef{ref}, prebound, err)
	}
	if err := validateTargetProvisioningResult(plan, result); err != nil {
		return target, c.failTargetProvisioningAttempt(request.Meta, target, driver, []ports.TargetRef{ref}, prebound, err)
	}
	target, err = c.advanceTargetReady(operationCtx, request.Meta, target)
	if err != nil {
		return target, c.failTargetProvisioningAttempt(request.Meta, target, driver, []ports.TargetRef{ref}, prebound, err)
	}
	return target, nil
}

func (c *Controller) failTargetProvisioningAttempt(meta application.MutationMeta, target application.TargetRecord, driver ports.TargetDriver, refs []ports.TargetRef, prebound bool, cause error) error {
	if prebound {
		return cause
	}
	return c.rollbackTarget(meta, target, driver, refs, cause)
}

func (c *Controller) StartTargetRun(ctx context.Context, request application.StartTargetRunRequest) (application.TargetRunRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.StartTargetRun(ctx, request)
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()
	target, err := c.Core.GetTarget(operationCtx, request.TargetID)
	if err != nil {
		return application.TargetRunRecord{}, err
	}
	driver, err := c.requireTargetLifecycle("start_target_run", target.Kind)
	if err != nil {
		return application.TargetRunRecord{}, err
	}
	resolved, err := c.resolver.ResolveTargetMaterial(operationCtx, request, target)
	if err != nil {
		return application.TargetRunRecord{}, err
	}
	if err := resolved.Validate(); err != nil {
		return application.TargetRunRecord{}, err
	}
	request.MaterializationDigest = resolved.MaterializationDigest.String()
	request.SpecimenOccurrenceRefs = nil
	request.FixtureRefs = nil
	run, err := c.Core.StartTargetRun(operationCtx, request)
	if err != nil {
		return application.TargetRunRecord{}, err
	}
	plan, err := bindTargetRunPlan(request, resolved, target, run)
	if err != nil {
		return run, err
	}
	run, err = c.bindTargetRunProvisioningPlan(operationCtx, request.Meta, target.ID, run, plan)
	if err != nil {
		return run, err
	}
	if run.State.Terminal() || run.State == domain.TargetRunRunning {
		return run, nil
	}
	if run.State == domain.TargetRunRequested {
		run, err = c.transitionRun(operationCtx, request.Meta, target.ID, run, domain.TargetRunPreparing)
		if err != nil {
			return run, err
		}
	}
	if run.State != domain.TargetRunPreparing && run.State != domain.TargetRunObserving {
		return run, fmt.Errorf("target run in %s cannot start", run.State)
	}
	prepared, err := driver.PrepareRun(operationCtx, plan)
	if err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	observerStart, err := bindRunObserverStart(plan, prepared, target)
	if err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	if err := c.observers.Start(operationCtx, observerStart); err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	// Coordinator ownership begins before validating the driver's echoed
	// coverage policy. If that echo drifts, compensation can still stop every
	// collector and seal the target's intrinsic receipt as failed evidence.
	if err := validatePreparedRun(plan, prepared); err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	if run.State == domain.TargetRunPreparing {
		run, err = c.transitionRun(operationCtx, request.Meta, target.ID, run, domain.TargetRunObserving)
		if err != nil {
			return run, c.rollbackRunDetached(request, target, run, driver, err)
		}
	}
	if err := driver.StartRun(operationCtx, plan.Run.ID()); err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	run, err = c.transitionRun(operationCtx, request.Meta, target.ID, run, domain.TargetRunRunning)
	if err != nil {
		return run, c.rollbackRunDetached(request, target, run, driver, err)
	}
	return run, nil
}

func (c *Controller) bindTargetRunProvisioningPlan(ctx context.Context, meta application.MutationMeta, targetID string, run application.TargetRunRecord, plan ports.TargetRunPlan) (application.TargetRunRecord, error) {
	digest, err := TargetRunProvisioningPlanDigest(plan)
	if err != nil {
		return run, err
	}
	if run.ProvisioningPlanDigest != "" || run.ProvisioningKey != "" {
		if run.ProvisioningPlanDigest != digest.String() || run.ProvisioningKey != plan.IdempotencyKey {
			return run, domain.NewError(domain.CodeIntegrityViolation, "controller.bind_target_run_plan", "provisioning", "resolved plan differs from the durable target run binding", nil)
		}
		return run, nil
	}
	return c.Core.BindTargetRunPlan(ctx, application.BindTargetRunPlanRequest{
		Meta: childMeta(meta, "physical/run-plan-binding", meta.Deadline), TargetID: targetID, RunID: run.ID,
		ExpectedRevision: run.Revision, ProvisioningPlanDigest: digest.String(), ProvisioningKey: plan.IdempotencyKey,
	})
}

func bindRunObserverStart(plan ports.TargetRunPlan, prepared ports.PreparedTargetRun, target application.TargetRecord) (RunObserverStart, error) {
	sessionID, err := domain.ParseResearchSessionID(target.SessionID)
	if err != nil {
		return RunObserverStart{}, err
	}
	generation, err := targetGeneration(target)
	if err != nil {
		return RunObserverStart{}, err
	}
	policy, err := domain.ParseDigest(generation.PolicyDigest)
	if err != nil {
		return RunObserverStart{}, err
	}
	capability, err := domain.ParseDigest(generation.CapabilityDigest)
	if err != nil {
		return RunObserverStart{}, err
	}
	return RunObserverStart{
		Plan: plan, Prepared: prepared, TargetKind: target.Kind, ResearchSessionID: sessionID,
		PolicyDigest: policy, CapabilityFingerprintDigest: capability,
	}, nil
}

func (c *Controller) ResetTarget(ctx context.Context, request application.ResetTargetRequest) (application.TargetRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.ResetTarget(ctx, request)
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()
	before, err := c.Core.GetTarget(operationCtx, request.TargetID)
	if err != nil {
		return application.TargetRecord{}, err
	}
	driver, err := c.requireTargetLifecycle("reset_target", before.Kind)
	if err != nil {
		return application.TargetRecord{}, err
	}
	view, err := c.Core.GetResearchSessionByLease(operationCtx, before.LeaseID)
	if err != nil {
		return application.TargetRecord{}, err
	}
	if _, err := c.resolveAndAdmitTargetReset(operationCtx, request, before, view, nil, ""); err != nil {
		return application.TargetRecord{}, fmt.Errorf("admit target reset: %w", err)
	}
	target, err := c.Core.ResetTarget(operationCtx, request)
	if err != nil {
		return application.TargetRecord{}, err
	}
	generation, generationErr := targetGeneration(target)
	if generationErr != nil {
		return target, generationErr
	}
	if generation.State.Terminal() {
		return target, domain.NewError(domain.CodeFailedPrecondition, "controller.reset_target", "generation", "the idempotent target reset is already terminal", nil)
	}
	if target.CurrentGeneration <= 1 {
		return target, fmt.Errorf("reset did not advance the target generation")
	}
	view, err = c.Core.GetResearchSessionByLease(operationCtx, target.LeaseID)
	if err != nil {
		return target, err
	}
	physicalKey := domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset")
	plan, err := c.resolveAndAdmitTargetReset(operationCtx, request, target, view, nil, physicalKey)
	if err != nil {
		return target, fmt.Errorf("admit bound target reset: %w", err)
	}
	target, err = c.bindTargetProvisioningPlan(operationCtx, request.Meta, target, plan)
	if err != nil {
		return target, fmt.Errorf("bind target reset plan: %w", err)
	}
	target, refs, err := c.resetTargetPhysical(operationCtx, request, target, driver)
	if err != nil {
		return target, c.rollbackTarget(request.Meta, target, driver, refs, err)
	}
	return target, nil
}

// resetTargetPhysical performs only the retry-safe physical half of a logical
// generation rollover. Callers choose whether a failure should be compensated
// (an ordinary reset) or left recoverable (incident recovery).
func (c *Controller) resetTargetPhysical(ctx context.Context, request application.ResetTargetRequest, target application.TargetRecord, driver ports.TargetDriver) (application.TargetRecord, []ports.TargetRef, error) {
	reset, err := bindTargetResetPlan(request, target)
	if err != nil {
		return target, nil, err
	}
	targetID := reset.Previous.ID
	refs := []ports.TargetRef{reset.Previous, {ID: targetID, Generation: reset.NextGeneration}}
	result, err := driver.Reset(ctx, targetID, reset)
	if err != nil {
		return target, refs, err
	}
	if result.Status.TargetID != targetID || result.Status.Generation != reset.NextGeneration || !result.Status.Ready {
		return target, refs, fmt.Errorf("target reset returned a mismatched or unready generation")
	}
	target, err = c.advanceTargetReady(ctx, request.Meta, target)
	return target, refs, err
}

func bindTargetResetPlan(request application.ResetTargetRequest, target application.TargetRecord) (ports.ResetPlan, error) {
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		return ports.ResetPlan{}, err
	}
	leaseID, err := domain.ParseLeaseID(target.LeaseID)
	if err != nil {
		return ports.ResetPlan{}, err
	}
	var incidentID domain.IncidentID
	if request.RecoveryIncidentID != "" {
		incidentID, err = domain.ParseIncidentID(request.RecoveryIncidentID)
		if err != nil {
			return ports.ResetPlan{}, err
		}
	}
	plan := ports.ResetPlan{
		IdempotencyKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset"),
		LeaseID:        leaseID,
		Previous:       ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration - 1)},
		NextGeneration: domain.TargetGeneration(target.CurrentGeneration),
		Mode:           request.Mode,
		SnapshotName:   request.SnapshotName,
		IncidentID:     incidentID,
	}
	if err := plan.Validate(); err != nil {
		return ports.ResetPlan{}, err
	}
	return plan, nil
}

func (c *Controller) ReleaseResearchSession(ctx context.Context, request application.ReleaseResearchSessionRequest) (application.ReleaseOutcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logicalOnly() {
		return c.Core.ReleaseResearchSession(ctx, request)
	}
	if err := c.requireAgentLifecycle("release_research_session"); err != nil {
		return application.ReleaseOutcome{}, err
	}
	operationCtx, cancel := context.WithDeadline(ctx, request.Meta.Deadline)
	defer cancel()
	preparation, err := c.Core.BeginReleaseResearchSession(operationCtx, request)
	if err != nil {
		return application.ReleaseOutcome{}, err
	}
	view := preparation.View
	if view.Lease.State == domain.LeaseReleased {
		return application.ReleaseOutcome{SessionID: view.Session.ID, LeaseID: view.Lease.ID, ReleasedAt: view.Lease.UpdatedAt}, nil
	}
	cleanupCtx, cleanupMeta, cleanupCancel := c.detachedContext(request.Meta, "physical/release")
	defer cleanupCancel()
	if err := c.drainLeaseTermination(cleanupCtx, application.LeaseTerminationPreparation{
		View: view, Kind: application.LeaseTerminationRelease, TerminatingLeaseRevision: preparation.ReleasingLeaseRevision,
	}); err != nil {
		return application.ReleaseOutcome{}, err
	}
	request.Meta = cleanupMeta
	request.ExpectedRevision = preparation.ReleasingLeaseRevision
	return c.Core.CompleteReleaseResearchSession(cleanupCtx, request)
}

func (c *Controller) releasePhysical(ctx context.Context, view application.ResearchSessionView) error {
	var cleanup []error
	for _, target := range view.Targets {
		driver := c.targets[target.Kind]
		if driver == nil {
			cleanup = append(cleanup, missingCapability("controller.release", "target_driver", "no driver is configured for "+string(target.Kind)))
			continue
		}
		targetID, err := domain.ParseTargetID(target.ID)
		if err != nil {
			cleanup = append(cleanup, err)
			continue
		}
		for _, generation := range target.Generations {
			if err := endedCleanupContext(ctx, cleanup); err != nil {
				return err
			}
			cleanup = append(cleanup, driver.Destroy(ctx, ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(generation.Generation)}))
		}
	}
	if err := endedCleanupContext(ctx, cleanup); err != nil {
		return err
	}
	agentID, err := domain.ParseAgentWorkspaceID(view.Agent.ID)
	if err != nil {
		return errors.Join(append(cleanup, err)...)
	}
	type agentResource struct {
		ref         ports.AgentWorkspaceRef
		workspaceID domain.WorkspaceID
	}
	resources := make([]agentResource, 0, len(view.Agent.Generations))
	for _, generation := range view.Agent.Generations {
		workspaceID, parseErr := domain.ParseWorkspaceID(generation.WorkspaceID)
		if parseErr != nil {
			cleanup = append(cleanup, parseErr)
			continue
		}
		resource := agentResource{ref: ports.AgentWorkspaceRef{ID: agentID, Generation: domain.AgentGeneration(generation.Generation)}, workspaceID: workspaceID}
		resources = append(resources, resource)
	}
	for _, resource := range resources {
		if err := endedCleanupContext(ctx, cleanup); err != nil {
			return err
		}
		cleanup = append(cleanup, c.destroyAgentAndWorkspace(ctx, resource.ref, resource.workspaceID, ports.StopGraceful))
	}
	return errors.Join(cleanup...)
}

func (c *Controller) advanceAgentReady(ctx context.Context, meta application.MutationMeta, agent application.AgentWorkspaceRecord) (application.AgentWorkspaceRecord, error) {
	for {
		generation, err := currentAgentGeneration(agent)
		if err != nil {
			return agent, err
		}
		var next domain.AgentGenerationState
		switch generation.State {
		case domain.AgentGenerationProvisioning:
			next = domain.AgentGenerationBooting
		case domain.AgentGenerationBooting:
			next = domain.AgentGenerationReady
		case domain.AgentGenerationReady, domain.AgentGenerationRunning:
			return agent, nil
		default:
			return agent, fmt.Errorf("agent generation in %s cannot become ready", generation.State)
		}
		agent, err = c.Core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{
			Meta: childMeta(meta, "physical/agent/"+next.String(), meta.Deadline), AgentWorkspaceID: agent.ID,
			Generation: agent.CurrentGeneration, ExpectedRevision: generation.Revision, State: next,
		})
		if err != nil {
			return agent, err
		}
	}
}

func (c *Controller) advanceTargetReady(ctx context.Context, meta application.MutationMeta, target application.TargetRecord) (application.TargetRecord, error) {
	for {
		generation, err := targetGeneration(target)
		if err != nil {
			return target, err
		}
		var next domain.TargetGenerationState
		switch generation.State {
		case domain.TargetGenerationProvisioning:
			next = domain.TargetGenerationInstrumenting
		case domain.TargetGenerationInstrumenting:
			next = domain.TargetGenerationReady
		case domain.TargetGenerationReady, domain.TargetGenerationResettable:
			return target, nil
		default:
			return target, fmt.Errorf("target generation in %s cannot become ready", generation.State)
		}
		target, err = c.Core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{
			Meta: childMeta(meta, "physical/target/"+next.String(), meta.Deadline), TargetID: target.ID,
			Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: next,
		})
		if err != nil {
			return target, err
		}
	}
}

func (c *Controller) transitionRun(ctx context.Context, meta application.MutationMeta, targetID string, run application.TargetRunRecord, state domain.TargetRunState) (application.TargetRunRecord, error) {
	return c.Core.TransitionTargetRun(ctx, application.TransitionTargetRunRequest{
		Meta: childMeta(meta, "physical/run/"+state.String(), meta.Deadline), TargetID: targetID,
		RunID: run.ID, ExpectedRevision: run.Revision, State: state,
	})
}

func (c *Controller) failAgentGeneration(ctx context.Context, meta application.MutationMeta, agent application.AgentWorkspaceRecord) error {
	latest, loadErr := c.Core.GetResearchSession(ctx, agent.SessionID)
	if loadErr == nil {
		agent = latest.Agent
	}
	generation, err := currentAgentGeneration(agent)
	if err != nil || generation.State.Terminal() {
		return errors.Join(loadErr, err)
	}
	_, transitionErr := c.Core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{
		Meta: childMeta(meta, "physical/agent/failed", meta.Deadline), AgentWorkspaceID: agent.ID,
		Generation: agent.CurrentGeneration, ExpectedRevision: generation.Revision, State: domain.AgentGenerationFailed,
	})
	return errors.Join(loadErr, transitionErr)
}

func (c *Controller) failTargetGeneration(ctx context.Context, meta application.MutationMeta, target application.TargetRecord) error {
	latest, loadErr := c.Core.GetTarget(ctx, target.ID)
	if loadErr == nil {
		target = latest
	}
	generation, err := targetGeneration(target)
	if err != nil || generation.State.Terminal() {
		return errors.Join(loadErr, err)
	}
	_, transitionErr := c.Core.TransitionTargetGeneration(ctx, application.TransitionTargetGenerationRequest{
		Meta: childMeta(meta, "physical/target/failed", meta.Deadline), TargetID: target.ID,
		Generation: target.CurrentGeneration, ExpectedRevision: generation.Revision, State: domain.TargetGenerationFailed,
	})
	return errors.Join(loadErr, transitionErr)
}

func (c *Controller) rollbackAcquisition(meta application.MutationMeta, view application.ResearchSessionView, plan *AgentProvisioningPlan, cause error) error {
	ctx, cleanupMeta, cancel := c.detachedContext(meta, "physical/acquisition-rollback")
	defer cancel()
	failErr := c.failAgentGeneration(ctx, cleanupMeta, view.Agent)
	release := application.ReleaseResearchSessionRequest{
		Meta: childMeta(cleanupMeta, "release-failed-acquisition", cleanupMeta.Deadline), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "agent or workspace provisioning failed",
	}
	preparation := application.ReleasePreparation{View: view, ReleasingLeaseRevision: view.Lease.Revision}
	var beginErr error
	switch view.Lease.State {
	case domain.LeaseActive:
		preparation, beginErr = c.Core.BeginReleaseResearchSession(ctx, release)
	case domain.LeaseReleasing:
		// A previous compensation attempt already established the logical gate.
		// Resume physical cleanup without replaying Begin with a new detached deadline.
	default:
		beginErr = fmt.Errorf("failed acquisition lease is in unexpected state %s", view.Lease.State)
	}
	var cleanupErr error
	if plan != nil {
		ref := ports.AgentWorkspaceRef{ID: plan.Agent.Generation.Spec().AgentWorkspaceID, Generation: plan.Agent.Generation.Spec().Generation}
		cleanupErr = c.destroyAgentAndWorkspace(ctx, ref, plan.Workspace.Workspace.ID(), ports.StopForce)
	}
	if beginErr != nil || cleanupErr != nil {
		return errors.Join(cause, failErr, beginErr, cleanupErr)
	}
	release.ExpectedRevision = preparation.ReleasingLeaseRevision
	_, completeErr := c.Core.CompleteReleaseResearchSession(ctx, release)
	return errors.Join(cause, failErr, completeErr)
}

func (c *Controller) rollbackTarget(meta application.MutationMeta, target application.TargetRecord, driver ports.TargetDriver, refs []ports.TargetRef, cause error) error {
	ctx, cleanupMeta, cancel := c.detachedContext(meta, "physical/target-rollback")
	defer cancel()
	cleanup := make([]error, 0, len(refs)+1)
	for _, ref := range refs {
		cleanup = append(cleanup, driver.Destroy(ctx, ref))
	}
	cleanup = append(cleanup, c.failTargetGeneration(ctx, cleanupMeta, target))
	return errors.Join(append([]error{cause}, cleanup...)...)
}

func (c *Controller) rollbackRunDetached(request application.StartTargetRunRequest, target application.TargetRecord, run application.TargetRunRecord, driver ports.TargetDriver, cause error) error {
	ctx, cleanupMeta, cancel := c.detachedContext(request.Meta, "physical/run-rollback")
	defer cancel()
	request.Meta = cleanupMeta
	return c.rollbackRun(ctx, request, target, run, driver, cause)
}

func (c *Controller) rollbackRun(ctx context.Context, request application.StartTargetRunRequest, target application.TargetRecord, run application.TargetRunRecord, driver ports.TargetDriver, cause error) error {
	latest, loadErr := c.Core.GetTarget(ctx, target.ID)
	if loadErr == nil {
		target = latest
		if current, findErr := targetRun(target, run.ID); findErr == nil {
			run = current
		} else {
			loadErr = errors.Join(loadErr, findErr)
		}
	}
	signature, signatureErr := requestSignature(struct {
		TargetID string `json:"target_id"`
		RunID    string `json:"run_id"`
		Digest   string `json:"digest"`
	}{target.ID, run.ID, run.MaterializationDigest})
	if loadErr != nil || signatureErr != nil || c.capabilities == nil {
		return errors.Join(cause, loadErr, signatureErr, fmt.Errorf("run rollback finalization is unavailable"))
	}
	rollbackMeta := childMeta(request.Meta, "finalize", request.Meta.Deadline)
	if run.State.Terminal() {
		return cause
	}
	switch run.State {
	case domain.TargetRunRequested, domain.TargetRunPreparing, domain.TargetRunObserving, domain.TargetRunRunning, domain.TargetRunFinalizing:
	default:
		return errors.Join(cause, fmt.Errorf("run rollback encountered logical state %s", run.State))
	}
	_, rollbackErr := c.capabilities.stopAndFinalizeRun(ctx, target, run, driver, ports.StopForce, rollbackMeta, "start_target_run_rollback", rollbackMeta.IdempotencyKey, signature, cause)
	return errors.Join(cause, rollbackErr)
}

func (c *Controller) sealWorkspace(ctx context.Context, id domain.WorkspaceID) error {
	handle, err := c.workspace.Inspect(ctx, id)
	if domain.IsCode(err, domain.CodeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	switch handle.State {
	case domain.WorkspaceReady:
		handle, err = c.workspace.Mount(ctx, id)
		if err != nil {
			return err
		}
		fallthrough
	case domain.WorkspaceMounted:
		preview, previewErr := c.workspace.Preview(ctx, id)
		if previewErr != nil {
			return previewErr
		}
		_, err = c.workspace.Seal(ctx, id, preview.ChangeSet.WorkspaceRevision())
		return err
	case domain.WorkspaceSealed:
		return nil
	case domain.WorkspaceReleased:
		return nil
	default:
		return fmt.Errorf("workspace %s in %s cannot be sealed for release", id, handle.State)
	}
}

func (c *Controller) releaseWorkspace(ctx context.Context, id domain.WorkspaceID) error {
	err := c.workspace.Release(ctx, id)
	if domain.IsCode(err, domain.CodeNotFound) {
		return nil
	}
	return err
}

func (c *Controller) destroyAgentAndWorkspace(ctx context.Context, ref ports.AgentWorkspaceRef, workspaceID domain.WorkspaceID, mode ports.StopMode) error {
	stopErr := c.agent.Stop(ctx, ref, mode)
	if domain.IsCode(stopErr, domain.CodeNotFound) {
		stopErr = nil
	}
	cleanup := []error{stopErr}
	if err := endedCleanupContext(ctx, cleanup); err != nil {
		return err
	}
	destroyErr := c.agent.Destroy(ctx, ref)
	if domain.IsCode(destroyErr, domain.CodeNotFound) {
		destroyErr = nil
	}
	cleanup = append(cleanup, destroyErr)
	if err := endedCleanupContext(ctx, cleanup); err != nil {
		return err
	}
	if destroyErr != nil {
		return errors.Join(cleanup...)
	}
	sealErr := c.sealWorkspace(ctx, workspaceID)
	cleanup = append(cleanup, sealErr)
	if err := endedCleanupContext(ctx, cleanup); err != nil {
		return err
	}
	if sealErr != nil {
		return errors.Join(cleanup...)
	}
	return errors.Join(append(cleanup, c.releaseWorkspace(ctx, workspaceID))...)
}

// endedCleanupContext stops a best-effort cleanup walk once its shared budget
// is gone. Continuing to call drivers with an already-cancelled context cannot
// make progress and obscures the first actionable failure with cascaded noise.
func endedCleanupContext(ctx context.Context, accumulated []error) error {
	if err := ctx.Err(); err != nil {
		causes := append([]error(nil), accumulated...)
		return errors.Join(append(causes, err)...)
	}
	return nil
}

func (c *Controller) detachedContext(meta application.MutationMeta, suffix string) (context.Context, application.MutationMeta, context.CancelFunc) {
	ctx, cancel, deadline := cleanupContext(c.cleanupTimeout)
	return ctx, childMeta(meta, suffix, deadline), cancel
}

func (c *Controller) logicalOnly() bool {
	return c.agent == nil && c.workspace == nil && c.resolver == nil && len(c.targets) == 0
}

func (c *Controller) requireAgentLifecycle(operation string) error {
	if c.agent == nil || c.workspace == nil || c.resolver == nil {
		return missingCapability("controller."+operation, "agent_lifecycle", "agent, workspace, and provisioning resolver are required")
	}
	return nil
}

func (c *Controller) requireTargetLifecycle(operation string, kind domain.TargetKind) (ports.TargetDriver, error) {
	if c.resolver == nil {
		return nil, missingCapability("controller."+operation, "resolver", "trusted provisioning resolver is required")
	}
	driver := c.targets[kind]
	if driver == nil {
		return nil, missingCapability("controller."+operation, "target_driver", "no driver is configured for "+string(kind))
	}
	return driver, nil
}

func missingCapability(operation, field, message string) error {
	return domain.NewError(domain.CodeCapabilityUnavailable, operation, field, message, nil)
}

func validateWorkspaceHandle(plan ports.WorkspacePlan, handle ports.WorkspaceHandle, state domain.WorkspaceState, operation string) error {
	if handle.WorkspaceID != plan.Workspace.ID() || handle.State != state || strings.TrimSpace(handle.MergedPath) == "" {
		return fmt.Errorf("workspace %s returned a mismatched identity, state, or merged path", operation)
	}
	if handle.PhysicalBytes < 0 || handle.PhysicalBytes > plan.UpperByteLimit || handle.Inodes < 0 || handle.Inodes > plan.UpperInodeLimit {
		return fmt.Errorf("workspace %s exceeded its physical bounds", operation)
	}
	return nil
}

func validateAgentProvisioningResult(plan ports.AgentWorkspacePlan, result ports.AgentWorkspaceResult) error {
	spec := plan.Generation.Spec()
	if result.Status.AgentWorkspaceID != spec.AgentWorkspaceID || result.Status.Generation != spec.Generation ||
		result.Status.State != domain.AgentGenerationReady || !result.Status.Ready {
		return fmt.Errorf("agent driver returned a mismatched or unready generation")
	}
	return nil
}

func validateTargetProvisioningResult(plan ports.TargetPlan, result ports.TargetResult) error {
	if result.Status.TargetID != plan.Target.ID() || result.Status.Generation != plan.Generation.Spec().Generation ||
		result.Status.Kind != plan.Template.Kind || result.Status.State != domain.TargetGenerationReady || !result.Status.Ready {
		return fmt.Errorf("target driver returned a mismatched or unready generation")
	}
	return nil
}

func validatePreparedRun(plan ports.TargetRunPlan, result ports.PreparedTargetRun) error {
	spec := plan.Run.Spec()
	if result.RunID != spec.ID || result.TargetID != spec.TargetID || result.TargetGeneration != spec.TargetGeneration ||
		result.MaterializationDigest != spec.MaterializationDigest {
		return fmt.Errorf("target driver prepared a different run or materialization")
	}
	planned, planErr := exactCoverageSet(plan.RequiredCoverage)
	actual, resultErr := exactCoverageSet(result.RequiredCoverage)
	if planErr != nil || resultErr != nil || len(planned) != len(actual) {
		return fmt.Errorf("target driver prepared different required coverage: %v", errors.Join(planErr, resultErr))
	}
	for family := range planned {
		if _, found := actual[family]; !found {
			return fmt.Errorf("target driver omitted required coverage family %q", family)
		}
	}
	return nil
}

func exactCoverageSet(values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("required coverage family must not be blank")
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("required coverage family %q is duplicated", value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}
