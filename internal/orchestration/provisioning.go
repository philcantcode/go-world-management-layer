package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// ProvisioningResolver is the trusted authority boundary between public
// references and exact physical plans. Acquisition and target material are
// resolved before the logical core persists their content identities.
type ProvisioningResolver interface {
	ResolveAcquisition(context.Context, application.AcquireRequest) (ResolvedAcquisition, error)
	ResolvePersistedAgent(context.Context, application.ResearchSessionView) (ResolvedAcquisition, error)
	ResolveAgentRecovery(context.Context, application.RecoverIncidentRequest, application.ResearchSessionView) (ResolvedAcquisition, error)
	ResolveTarget(context.Context, application.CreateTargetRequest, application.TargetRecord) (ports.TargetPlan, error)
	ResolveTargetMaterial(context.Context, application.StartTargetRunRequest, application.TargetRecord) (ResolvedTargetRun, error)
}

// ResolvedAcquisition contains every authority-owned value needed to create a
// workspace and its agent. Content is keyed by the exact logical manifest path.
type ResolvedAcquisition struct {
	InputView        domain.InputViewManifest
	SecurityScope    string
	Construction     domain.InputViewConstruction
	Content          map[string]ports.ContentSource
	UpperByteLimit   int64
	UpperInodeLimit  int64
	PolicyDigest     domain.Digest
	CapabilityDigest domain.Digest
	ImageDigest      domain.Digest
	Resources        admission.Resources
}

func (r ResolvedAcquisition) Validate() error {
	const operation = "orchestration.resolved_acquisition.validate"
	if r.InputView.ID().IsZero() || strings.TrimSpace(r.SecurityScope) == "" || !r.Construction.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "input_view", "manifest, security scope, and construction are required", nil)
	}
	if r.UpperByteLimit <= 0 || r.UpperInodeLimit <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "limits", "positive byte and inode limits are required", nil)
	}
	if r.PolicyDigest.IsZero() || r.CapabilityDigest.IsZero() || r.ImageDigest.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "digests", "policy, capability, and image digests are required", nil)
	}
	if err := r.Resources.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "resources", "is invalid", err)
	}
	entries := r.InputView.Entries()
	if len(r.Content) != len(entries) {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "content", "must contain exactly one source per manifest entry", nil)
	}
	for _, entry := range entries {
		spec := entry.Spec()
		source, found := r.Content[spec.LogicalPath]
		if !found || source == nil || source.Digest() != spec.Digest || source.Size() != spec.Size {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "content", "does not match the manifest identity", nil)
		}
	}
	return nil
}

// AgentProvisioningPlan binds pre-resolved authority to IDs allocated by the
// logical core. Its validator prevents workspace and container scope drift.
type AgentProvisioningPlan struct {
	Workspace ports.WorkspacePlan
	Agent     ports.AgentWorkspacePlan
}

// BuildAgentWorkspacePlan assembles and validates the shared physical agent
// plan used by persisted provisioning and startup physical-policy preflight.
func BuildAgentWorkspacePlan(idempotencyKey string, leaseID domain.LeaseID, generation domain.AgentWorkspaceGenerationRecord, workspace domain.Workspace, resolved ResolvedAcquisition) (ports.AgentWorkspacePlan, error) {
	plan := ports.AgentWorkspacePlan{
		IdempotencyKey: idempotencyKey, LeaseID: leaseID, Generation: generation, Workspace: workspace,
		ImageDigest: resolved.ImageDigest, PolicyDigest: resolved.PolicyDigest,
		CapabilityFingerprintDigest: resolved.CapabilityDigest, Resources: resolved.Resources.Clone(),
	}
	if err := plan.Validate(); err != nil {
		return ports.AgentWorkspacePlan{}, err
	}
	return plan, nil
}

// AgentProvisioningPlanDigest is the control-plane identity of every semantic
// input that can change the workspace or agent generation. Driver-specific
// hardening remains bound by the capability fingerprint and the driver's own
// independently checked physical plan digest.
func AgentProvisioningPlanDigest(plan AgentProvisioningPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	workspace := plan.Workspace.Workspace.Spec()
	generation := plan.Agent.Generation.Spec()
	signature, err := requestSignature(struct {
		LeaseID             string              `json:"lease_id"`
		AgentWorkspaceID    string              `json:"agent_workspace_id"`
		Generation          uint64              `json:"generation"`
		WorkspaceID         string              `json:"workspace_id"`
		InputViewID         string              `json:"input_view_id"`
		PolicyDigest        string              `json:"policy_digest"`
		CapabilityDigest    string              `json:"capability_digest"`
		ImageDigest         string              `json:"image_digest"`
		Construction        string              `json:"construction"`
		UpperByteLimit      int64               `json:"upper_byte_limit"`
		UpperInodeLimit     int64               `json:"upper_inode_limit"`
		Resources           admission.Resources `json:"resources"`
		PreviousGeneration  uint64              `json:"previous_generation,omitempty"`
		RecoveryIncidentID  string              `json:"recovery_incident_id,omitempty"`
		WorkspaceCreatedAt  time.Time           `json:"workspace_created_at"`
		GenerationCreatedAt time.Time           `json:"generation_created_at"`
	}{
		LeaseID: plan.Agent.LeaseID.String(), AgentWorkspaceID: generation.AgentWorkspaceID.String(),
		Generation: uint64(generation.Generation), WorkspaceID: workspace.ID.String(), InputViewID: plan.Workspace.InputView.ID().String(),
		PolicyDigest: plan.Agent.PolicyDigest.String(), CapabilityDigest: plan.Agent.CapabilityFingerprintDigest.String(), ImageDigest: plan.Agent.ImageDigest.String(),
		Construction: string(plan.Workspace.Construction), UpperByteLimit: plan.Workspace.UpperByteLimit, UpperInodeLimit: plan.Workspace.UpperInodeLimit,
		Resources: plan.Agent.Resources.Clone(), PreviousGeneration: uint64(generation.PreviousGeneration), RecoveryIncidentID: generation.RecoveryIncidentID.String(),
		WorkspaceCreatedAt: workspace.CreatedAt, GenerationCreatedAt: generation.CreatedAt,
	})
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.ParseDigest("sha256:" + signature)
}

// ApplyPersistedAgentProvisioningIdentity restores the exact physical
// idempotency keys and verifies the semantic plan bound to a durable
// generation. It is used by retries and startup reconciliation.
func ApplyPersistedAgentProvisioningIdentity(plan AgentProvisioningPlan, generation application.AgentGenerationRecord) (AgentProvisioningPlan, error) {
	empty := generation.ProvisioningPlanDigest == "" && generation.WorkspaceProvisioningKey == "" && generation.AgentProvisioningKey == ""
	if empty {
		return plan, nil
	}
	if generation.ProvisioningPlanDigest == "" || generation.WorkspaceProvisioningKey == "" || generation.AgentProvisioningKey == "" {
		return AgentProvisioningPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_agent_provisioning_identity", "binding", "persisted provisioning identity is partial", nil)
	}
	plan.Workspace.IdempotencyKey = generation.WorkspaceProvisioningKey
	plan.Agent.IdempotencyKey = generation.AgentProvisioningKey
	digest, err := AgentProvisioningPlanDigest(plan)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	if digest.String() != generation.ProvisioningPlanDigest {
		return AgentProvisioningPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_agent_provisioning_identity", "plan_digest", "resolved agent plan differs from the plan bound to this generation", nil)
	}
	return plan, nil
}

// TargetProvisioningPlanDigest is the control-plane identity of every
// semantic input that determines one target generation. It intentionally
// excludes the retry key while including generation provenance, template,
// image/runtime selection, and resource limits.
func TargetProvisioningPlanDigest(plan ports.TargetPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	generation := plan.Generation.Spec()
	signature, err := requestSignature(struct {
		LeaseID            string              `json:"lease_id"`
		TargetID           string              `json:"target_id"`
		ResearchSessionID  string              `json:"research_session_id"`
		Generation         uint64              `json:"generation"`
		Kind               string              `json:"kind"`
		TemplateName       string              `json:"template_name"`
		Driver             string              `json:"driver"`
		Runtime            string              `json:"runtime"`
		ImageDigest        string              `json:"image_digest"`
		IsolationProfile   string              `json:"isolation_profile"`
		PolicyDigest       string              `json:"policy_digest"`
		CapabilityDigest   string              `json:"capability_digest"`
		Resources          admission.Resources `json:"resources"`
		PreviousGeneration uint64              `json:"previous_generation,omitempty"`
		RecoveryIncidentID string              `json:"recovery_incident_id,omitempty"`
		GenerationCreated  time.Time           `json:"generation_created_at"`
	}{
		LeaseID: plan.LeaseID.String(), TargetID: plan.Target.ID().String(), ResearchSessionID: plan.Target.ResearchSessionID().String(),
		Generation: uint64(generation.Generation), Kind: string(plan.Target.Kind()), TemplateName: plan.Template.Name,
		Driver: plan.Template.Driver, Runtime: plan.Template.Runtime, ImageDigest: plan.Template.ImageDigest.String(), IsolationProfile: plan.Template.IsolationProfile,
		PolicyDigest: plan.PolicyDigest.String(), CapabilityDigest: plan.CapabilityFingerprintDigest.String(), Resources: plan.Resources.Clone(),
		PreviousGeneration: uint64(generation.PreviousGeneration), RecoveryIncidentID: generation.RecoveryIncidentID.String(), GenerationCreated: generation.CreatedAt,
	})
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.ParseDigest("sha256:" + signature)
}

// ApplyPersistedTargetProvisioningIdentity restores the physical retry key
// and rejects a resolver profile that differs from the plan already frozen on
// the durable target generation.
func ApplyPersistedTargetProvisioningIdentity(plan ports.TargetPlan, generation application.TargetGenerationRecord) (ports.TargetPlan, error) {
	empty := generation.ProvisioningPlanDigest == "" && generation.ProvisioningKey == ""
	if empty {
		return plan, nil
	}
	if generation.ProvisioningPlanDigest == "" || generation.ProvisioningKey == "" {
		return ports.TargetPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_target_provisioning_identity", "binding", "persisted provisioning identity is partial", nil)
	}
	plan.IdempotencyKey = generation.ProvisioningKey
	digest, err := TargetProvisioningPlanDigest(plan)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	if digest.String() != generation.ProvisioningPlanDigest {
		return ports.TargetPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_target_provisioning_identity", "plan_digest", "resolved target plan differs from the plan bound to this generation", nil)
	}
	return plan, nil
}

// TargetRunProvisioningPlanDigest binds the complete physical preparation and
// observer policy for one run. Material bytes are represented by the already
// verified canonical materialization digest; collector and coverage ordering
// is normalized because neither is semantically ordered.
func TargetRunProvisioningPlanDigest(plan ports.TargetRunPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	type collectorIdentity struct {
		Name                string              `json:"name"`
		SignalFamily        string              `json:"signal_family"`
		Placement           string              `json:"placement"`
		MinimumLevel        string              `json:"minimum_level"`
		Required            bool                `json:"required"`
		Adapter             string              `json:"adapter"`
		Version             string              `json:"version"`
		ConfigurationDigest string              `json:"configuration_digest"`
		Resources           admission.Resources `json:"resources"`
		MaximumBytes        int64               `json:"maximum_bytes"`
	}
	collectors := make([]collectorIdentity, len(plan.Collectors))
	for index, collector := range plan.Collectors {
		collectors[index] = collectorIdentity{
			Name: collector.Name, SignalFamily: collector.Requirement.SignalFamily, Placement: string(collector.Requirement.Placement),
			MinimumLevel: string(collector.Requirement.MinimumLevel), Required: collector.Requirement.Required,
			Adapter: collector.Adapter, Version: collector.Version, ConfigurationDigest: collector.ConfigurationDigest.String(),
			Resources: collector.Resources.Clone(), MaximumBytes: collector.MaximumBytes,
		}
	}
	sort.Slice(collectors, func(i, j int) bool { return collectors[i].Name < collectors[j].Name })
	coverage := append([]string(nil), plan.RequiredCoverage...)
	sort.Strings(coverage)
	run := plan.Run.Spec()
	signature, err := requestSignature(struct {
		RunID                 string              `json:"run_id"`
		LeaseID               string              `json:"lease_id"`
		TargetID              string              `json:"target_id"`
		TargetGeneration      uint64              `json:"target_generation"`
		AgentWorkspaceID      string              `json:"agent_workspace_id"`
		AgentGeneration       uint64              `json:"agent_generation"`
		MaterializationDigest string              `json:"materialization_digest"`
		CreatedAt             time.Time           `json:"created_at"`
		RequiredCoverage      []string            `json:"required_coverage"`
		Collectors            []collectorIdentity `json:"collectors"`
		MaximumDuration       time.Duration       `json:"maximum_duration"`
	}{
		RunID: run.ID.String(), LeaseID: run.LeaseID.String(), TargetID: run.TargetID.String(), TargetGeneration: uint64(run.TargetGeneration),
		AgentWorkspaceID: run.AgentWorkspaceID.String(), AgentGeneration: uint64(run.AgentGeneration), MaterializationDigest: run.MaterializationDigest.String(),
		CreatedAt: run.CreatedAt, RequiredCoverage: coverage, Collectors: collectors, MaximumDuration: plan.MaximumDuration,
	})
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.ParseDigest("sha256:" + signature)
}

func ApplyPersistedTargetRunProvisioningIdentity(plan ports.TargetRunPlan, run application.TargetRunRecord) (ports.TargetRunPlan, error) {
	empty := run.ProvisioningPlanDigest == "" && run.ProvisioningKey == ""
	if empty {
		return plan, nil
	}
	if run.ProvisioningPlanDigest == "" || run.ProvisioningKey == "" {
		return ports.TargetRunPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_target_run_provisioning_identity", "binding", "persisted provisioning identity is partial", nil)
	}
	plan.IdempotencyKey = run.ProvisioningKey
	digest, err := TargetRunProvisioningPlanDigest(plan)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	if digest.String() != run.ProvisioningPlanDigest {
		return ports.TargetRunPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.apply_target_run_provisioning_identity", "plan_digest", "resolved target run plan differs from the plan bound to this run", nil)
	}
	return plan, nil
}

func (p AgentProvisioningPlan) Validate() error {
	const operation = "orchestration.agent_provisioning_plan.validate"
	if err := p.Workspace.Validate(); err != nil {
		return err
	}
	if err := p.Agent.Validate(); err != nil {
		return err
	}
	workspace := p.Workspace.Workspace.Spec()
	agentWorkspace := p.Agent.Workspace.Spec()
	generation := p.Agent.Generation.Spec()
	if workspace != agentWorkspace || workspace.LeaseID != p.Agent.LeaseID ||
		workspace.AgentWorkspaceID != generation.AgentWorkspaceID || workspace.AgentGeneration != generation.Generation ||
		workspace.InputViewID != p.Workspace.InputView.ID() {
		return domain.NewError(domain.CodeConflict, operation, "scope", "workspace and agent plans do not identify the same immutable generation", nil)
	}
	return nil
}

// ResolvedTargetRun is the authoritative material projection and observation
// policy selected before Core creates a TargetRunRecord.
type ResolvedTargetRun struct {
	MaterializationDigest domain.Digest
	RequiredCoverage      []string
	Collectors            []ports.CollectorSpec
	Material              []ports.TargetMaterialPlan
	MaximumDuration       time.Duration
}

func (r ResolvedTargetRun) Validate() error {
	const operation = "orchestration.resolved_target_run.validate"
	digest, err := ports.TargetMaterializationDigest(r.Material)
	if err != nil {
		return err
	}
	if digest != r.MaterializationDigest {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "materialization_digest", "does not identify the exact material projection", nil)
	}
	if r.MaximumDuration <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "maximum_duration", "must be positive", nil)
	}
	if families, err := exactCoverageSet(r.RequiredCoverage); err != nil || len(families) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "required_coverage", "must contain unique non-blank families", err)
	}
	required, err := requiredCoverageForCollectors(r.Collectors)
	if err != nil {
		return err
	}
	configured, _ := exactCoverageSet(r.RequiredCoverage)
	delete(configured, ports.TargetLifecycleSignal)
	if !sameStringSet(configured, required) {
		return domain.NewError(domain.CodeConflict, operation, "collectors", "required collectors do not exactly match non-intrinsic required coverage", nil)
	}
	return nil
}

type StaticAgentPlan struct {
	Selection        application.InputSelectionRequest
	InputView        domain.InputViewManifest
	SecurityScope    string
	Construction     domain.InputViewConstruction
	Content          map[string]ports.ContentSource
	UpperByteLimit   int64
	UpperInodeLimit  int64
	PolicyDigest     domain.Digest
	CapabilityDigest domain.Digest
	ImageDigest      domain.Digest
	Resources        admission.Resources
}

type StaticTargetPlan struct {
	PolicyDigest     domain.Digest
	CapabilityDigest domain.Digest
	Template         ports.TargetTemplate
	Resources        admission.Resources
}

type StaticRunPlan struct {
	SpecimenOccurrenceRefs []string
	FixtureRefs            []string
	RequiredCoverage       []string
	Collectors             []ports.CollectorSpec
	Material               []ports.TargetMaterialPlan
	MaximumDuration        time.Duration
}

type StaticProvisioningConfig struct {
	Agents  map[string]StaticAgentPlan  // resolved input-view ID -> physical plan
	Targets map[string]StaticTargetPlan // public template reference -> physical plan
	Runs    map[string]StaticRunPlan    // canonical materialization digest -> run plan
	Now     func() time.Time
}

// StaticProvisioningResolver is deterministic and immutable after creation.
// It is suitable for tests and configuration that has already authenticated
// and bounded all referenced bytes.
type StaticProvisioningResolver struct {
	agents            map[string]StaticAgentPlan
	agentsBySelection map[string]string
	targets           map[string]StaticTargetPlan
	runs              map[string]StaticRunPlan
	runsByReferences  map[string]string
	now               func() time.Time
}

func NewStaticProvisioningResolver(config StaticProvisioningConfig) (*StaticProvisioningResolver, error) {
	if config.Now == nil {
		config.Now = time.Now
	}
	resolver := &StaticProvisioningResolver{
		agents: make(map[string]StaticAgentPlan, len(config.Agents)), agentsBySelection: make(map[string]string),
		targets: make(map[string]StaticTargetPlan, len(config.Targets)), runs: make(map[string]StaticRunPlan, len(config.Runs)),
		runsByReferences: make(map[string]string), now: config.Now,
	}
	for inputViewID, plan := range config.Agents {
		parsedID, err := domain.ParseInputViewID(inputViewID)
		if err != nil {
			return nil, fmt.Errorf("agent plan input view %q: %w", inputViewID, err)
		}
		resolved := resolvedAcquisition(plan)
		if err := resolved.Validate(); err != nil {
			return nil, fmt.Errorf("agent plan %q: %w", inputViewID, err)
		}
		if resolved.InputView.ID() != parsedID {
			return nil, fmt.Errorf("agent plan %q manifest has ID %s", inputViewID, resolved.InputView.ID())
		}
		plan.Resources = plan.Resources.Clone()
		plan.Content = cloneContentSources(plan.Content)
		plan.Selection = cloneInputSelection(plan.Selection)
		resolver.agents[inputViewID] = plan
		if !plan.Selection.Empty() {
			signature, err := inputSelectionSignature(plan.Selection)
			if err != nil {
				return nil, fmt.Errorf("agent plan %q selection: %w", inputViewID, err)
			}
			if previous := resolver.agentsBySelection[signature]; previous != "" {
				return nil, fmt.Errorf("agent plans %q and %q have the same selection", previous, inputViewID)
			}
			resolver.agentsBySelection[signature] = inputViewID
		}
	}
	for reference, plan := range config.Targets {
		if strings.TrimSpace(reference) == "" {
			return nil, fmt.Errorf("target plan reference must not be blank")
		}
		if plan.PolicyDigest.IsZero() || plan.CapabilityDigest.IsZero() {
			return nil, fmt.Errorf("target plan %q requires policy and capability digests", reference)
		}
		if err := plan.Template.Validate(); err != nil {
			return nil, fmt.Errorf("target plan %q template: %w", reference, err)
		}
		if err := plan.Resources.Validate(); err != nil {
			return nil, fmt.Errorf("target plan %q resources: %w", reference, err)
		}
		plan.Resources = plan.Resources.Clone()
		resolver.targets[reference] = plan
	}
	for declaredDigest, plan := range config.Runs {
		digest, err := ports.TargetMaterializationDigest(plan.Material)
		if err != nil {
			return nil, fmt.Errorf("run plan %q material: %w", declaredDigest, err)
		}
		if declaredDigest != digest.String() {
			return nil, fmt.Errorf("run plan key %q does not match canonical digest %s", declaredDigest, digest)
		}
		resolved := resolvedTargetRun(plan, digest)
		if err := resolved.Validate(); err != nil {
			return nil, fmt.Errorf("run plan %q: %w", declaredDigest, err)
		}
		plan.RequiredCoverage = append([]string(nil), plan.RequiredCoverage...)
		plan.Collectors = cloneCollectorSpecs(plan.Collectors)
		plan.Material = append([]ports.TargetMaterialPlan(nil), plan.Material...)
		plan.SpecimenOccurrenceRefs = append([]string(nil), plan.SpecimenOccurrenceRefs...)
		plan.FixtureRefs = append([]string(nil), plan.FixtureRefs...)
		resolver.runs[declaredDigest] = plan
		if len(plan.SpecimenOccurrenceRefs)+len(plan.FixtureRefs) > 0 {
			signature, err := targetReferencesSignature(plan.SpecimenOccurrenceRefs, plan.FixtureRefs)
			if err != nil {
				return nil, fmt.Errorf("run plan %q references: %w", declaredDigest, err)
			}
			if previous := resolver.runsByReferences[signature]; previous != "" {
				return nil, fmt.Errorf("run plans %q and %q have the same public references", previous, declaredDigest)
			}
			resolver.runsByReferences[signature] = declaredDigest
		}
	}
	return resolver, nil
}

func (r *StaticProvisioningResolver) ResolveAcquisition(ctx context.Context, request application.AcquireRequest) (ResolvedAcquisition, error) {
	if err := ports.RequireDeadline(ctx, "static_provisioning.resolve_acquisition"); err != nil {
		return ResolvedAcquisition{}, err
	}
	inputViewID := request.InputViewID
	if !request.InputSelection.Empty() {
		signature, err := inputSelectionSignature(request.InputSelection)
		if err != nil {
			return ResolvedAcquisition{}, err
		}
		selectedID := r.agentsBySelection[signature]
		if selectedID == "" {
			return ResolvedAcquisition{}, domain.NewError(domain.CodeCapabilityUnavailable, "static_provisioning.resolve_acquisition", "input_selection", "has no authorized acquisition plan", nil)
		}
		if inputViewID != "" && inputViewID != selectedID {
			return ResolvedAcquisition{}, domain.NewError(domain.CodeConflict, "static_provisioning.resolve_acquisition", "input_view_id", "does not match the selected manifest", nil)
		}
		inputViewID = selectedID
	}
	return r.resolveAgent(inputViewID, request.PolicyDigest, request.CapabilityDigest, "static_provisioning.resolve_acquisition")
}

// ResolvePersistedAgent re-selects the immutable authority inputs for the
// current durable generation without manufacturing a fresh acquisition TTL or
// public selection request. Startup reconciliation subsequently binds and
// verifies the exact persisted physical plan identity.
func (r *StaticProvisioningResolver) ResolvePersistedAgent(ctx context.Context, view application.ResearchSessionView) (ResolvedAcquisition, error) {
	if err := ports.RequireDeadline(ctx, "static_provisioning.resolve_persisted_agent"); err != nil {
		return ResolvedAcquisition{}, err
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	return r.resolveAgent(generation.InputViewID, generation.PolicyDigest, generation.CapabilityDigest, "static_provisioning.resolve_persisted_agent")
}

// ResolveAgentRecovery re-selects the immutable agent plan identified by the
// newly created recovery generation. It deliberately does not reinterpret the
// original acquisition TTL as a new session request.
func (r *StaticProvisioningResolver) ResolveAgentRecovery(ctx context.Context, request application.RecoverIncidentRequest, view application.ResearchSessionView) (ResolvedAcquisition, error) {
	if err := ports.RequireDeadline(ctx, "static_provisioning.resolve_agent_recovery"); err != nil {
		return ResolvedAcquisition{}, err
	}
	generation, err := validateAgentRecoveryScope(request, view, r.now().UTC())
	if err != nil {
		return ResolvedAcquisition{}, err
	}
	return r.resolveAgent(generation.InputViewID, generation.PolicyDigest, generation.CapabilityDigest, "static_provisioning.resolve_agent_recovery")
}

func (r *StaticProvisioningResolver) resolveAgent(inputViewID, policyDigest, capabilityDigest, operation string) (ResolvedAcquisition, error) {
	configured, found := r.agents[inputViewID]
	if !found {
		return ResolvedAcquisition{}, domain.NewError(domain.CodeCapabilityUnavailable, operation, "input_view_id", "has no authorized agent plan", nil)
	}
	if _, _, err := requireConfiguredDigests(policyDigest, capabilityDigest, configured.PolicyDigest, configured.CapabilityDigest); err != nil {
		return ResolvedAcquisition{}, err
	}
	resolved := resolvedAcquisition(configured)
	if err := resolved.Validate(); err != nil {
		return ResolvedAcquisition{}, err
	}
	return resolved, nil
}

func resolvedAcquisition(plan StaticAgentPlan) ResolvedAcquisition {
	return ResolvedAcquisition{
		InputView: plan.InputView, SecurityScope: plan.SecurityScope, Construction: plan.Construction,
		Content: cloneContentSources(plan.Content), UpperByteLimit: plan.UpperByteLimit, UpperInodeLimit: plan.UpperInodeLimit,
		PolicyDigest: plan.PolicyDigest, CapabilityDigest: plan.CapabilityDigest, ImageDigest: plan.ImageDigest,
		Resources: plan.Resources.Clone(),
	}
}

func bindAgentProvisioning(request application.AcquireRequest, resolved ResolvedAcquisition, view application.ResearchSessionView) (AgentProvisioningPlan, error) {
	if err := resolved.Validate(); err != nil {
		return AgentProvisioningPlan{}, err
	}
	leaseID, err := domain.ParseLeaseID(view.Lease.ID)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	agentID, err := domain.ParseAgentWorkspaceID(view.Agent.ID)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	if _, err := domain.ParseResearchSessionID(view.Session.ID); err != nil {
		return AgentProvisioningPlan{}, err
	}
	generationRecord, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	workspaceID, err := domain.ParseWorkspaceID(generationRecord.WorkspaceID)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	inputViewID, err := domain.ParseInputViewID(generationRecord.InputViewID)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	if inputViewID != resolved.InputView.ID() {
		return AgentProvisioningPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.bind_agent_provisioning", "input_view", "logical identity differs from authoritative manifest", nil)
	}
	if generationRecord.PolicyDigest != resolved.PolicyDigest.String() || generationRecord.CapabilityDigest != resolved.CapabilityDigest.String() ||
		view.Session.InputViewID != generationRecord.InputViewID || view.Lease.InputViewID != generationRecord.InputViewID ||
		view.Session.PolicyDigest != generationRecord.PolicyDigest || view.Lease.PolicyDigest != generationRecord.PolicyDigest ||
		view.Session.CapabilityDigest != generationRecord.CapabilityDigest || view.Lease.CapabilityDigest != generationRecord.CapabilityDigest {
		return AgentProvisioningPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.bind_agent_provisioning", "provenance", "logical session, lease, or generation differs from the authoritative plan", nil)
	}
	generation := domain.AgentGeneration(generationRecord.Generation)
	var recoveryIncidentID domain.IncidentID
	if generationRecord.RecoveryIncident != "" {
		recoveryIncidentID, err = domain.ParseIncidentID(generationRecord.RecoveryIncident)
		if err != nil {
			return AgentProvisioningPlan{}, err
		}
	}
	domainGeneration, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
		AgentWorkspaceID: agentID, Generation: generation, WorkspaceID: workspaceID, InputViewID: inputViewID,
		PolicyDigest: resolved.PolicyDigest, CapabilityFingerprintDigest: resolved.CapabilityDigest,
		PreviousGeneration: domain.AgentGeneration(generationRecord.Previous), RecoveryIncidentID: recoveryIncidentID,
		CreatedAt: generationRecord.CreatedAt,
	})
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: generation,
		InputViewID: inputViewID, CreatedAt: generationRecord.CreatedAt,
	})
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	workspaceProvisioningKey := boundProvisioningKey(generationRecord.WorkspaceProvisioningKey, request.Meta.IdempotencyKey, "physical/workspace")
	agentProvisioningKey := boundProvisioningKey(generationRecord.AgentProvisioningKey, request.Meta.IdempotencyKey, "physical/agent")
	agentPlan, err := BuildAgentWorkspacePlan(
		agentProvisioningKey,
		leaseID, domainGeneration, workspace, resolved,
	)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	plan := AgentProvisioningPlan{
		Workspace: ports.WorkspacePlan{
			IdempotencyKey: workspaceProvisioningKey, Workspace: workspace,
			InputView: resolved.InputView, Content: cloneContentSources(resolved.Content), Construction: resolved.Construction,
			UpperByteLimit: resolved.UpperByteLimit, UpperInodeLimit: resolved.UpperInodeLimit,
		},
		Agent: agentPlan,
	}
	plan, err = ApplyPersistedAgentProvisioningIdentity(plan, generationRecord)
	if err != nil {
		return AgentProvisioningPlan{}, err
	}
	if err := plan.Validate(); err != nil {
		return AgentProvisioningPlan{}, err
	}
	return plan, nil
}

func boundProvisioningKey(persisted, requestKey, suffix string) string {
	if persisted != "" {
		return persisted
	}
	return domain.DeriveIdempotencyKey(requestKey, suffix)
}

func (r *StaticProvisioningResolver) ResolveTarget(ctx context.Context, request application.CreateTargetRequest, target application.TargetRecord) (ports.TargetPlan, error) {
	if err := ports.RequireDeadline(ctx, "static_provisioning.resolve_target"); err != nil {
		return ports.TargetPlan{}, err
	}
	configured, found := r.targets[request.Template]
	if !found {
		return ports.TargetPlan{}, domain.NewError(domain.CodeCapabilityUnavailable, "static_provisioning.resolve_target", "template", "has no authorized target plan", nil)
	}
	policy, capability, err := requireConfiguredDigests(request.PolicyDigest, request.CapabilityDigest, configured.PolicyDigest, configured.CapabilityDigest)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	if request.Kind != configured.Template.Kind || target.Kind != configured.Template.Kind {
		return ports.TargetPlan{}, domain.NewError(domain.CodeConflict, "static_provisioning.resolve_target", "kind", "does not match the authorized template", nil)
	}
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	sessionID, err := domain.ParseResearchSessionID(target.SessionID)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	leaseID, err := domain.ParseLeaseID(target.LeaseID)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	generationRecord, err := targetGeneration(target)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	if target.Template != request.Template || target.LeaseID != request.LeaseID {
		return ports.TargetPlan{}, domain.NewError(domain.CodeConflict, "static_provisioning.resolve_target", "scope", "request and persisted target do not identify the same template and lease", nil)
	}
	if generationRecord.PolicyDigest != policy.String() || generationRecord.CapabilityDigest != capability.String() {
		return ports.TargetPlan{}, domain.NewError(domain.CodeIntegrityViolation, "static_provisioning.resolve_target", "provenance", "persisted target generation differs from the authorized policy pair", nil)
	}
	generation := domain.TargetGeneration(generationRecord.Generation)
	targetModel, err := domain.NewTarget(targetID, sessionID, target.Kind, generation, target.CreatedAt)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	var recoveryIncidentID domain.IncidentID
	if generationRecord.RecoveryIncident != "" {
		recoveryIncidentID, err = domain.ParseIncidentID(generationRecord.RecoveryIncident)
		if err != nil {
			return ports.TargetPlan{}, err
		}
	}
	domainGeneration, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: generation, PolicyDigest: policy,
		CapabilityFingerprintDigest: capability, PreviousGeneration: domain.TargetGeneration(generationRecord.Previous),
		RecoveryIncidentID: recoveryIncidentID, CreatedAt: generationRecord.CreatedAt,
	})
	if err != nil {
		return ports.TargetPlan{}, err
	}
	plan := ports.TargetPlan{
		IdempotencyKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/target"), LeaseID: leaseID,
		Target: targetModel, Generation: domainGeneration, Template: configured.Template,
		PolicyDigest: policy, CapabilityFingerprintDigest: capability, Resources: configured.Resources.Clone(),
	}
	plan, err = ApplyPersistedTargetProvisioningIdentity(plan, generationRecord)
	if err != nil {
		return ports.TargetPlan{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetPlan{}, err
	}
	return plan, nil
}

func (r *StaticProvisioningResolver) ResolveTargetMaterial(ctx context.Context, request application.StartTargetRunRequest, _ application.TargetRecord) (ResolvedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "static_provisioning.resolve_target_material"); err != nil {
		return ResolvedTargetRun{}, err
	}
	digest := request.MaterializationDigest
	if len(request.SpecimenOccurrenceRefs)+len(request.FixtureRefs) > 0 {
		signature, err := targetReferencesSignature(request.SpecimenOccurrenceRefs, request.FixtureRefs)
		if err != nil {
			return ResolvedTargetRun{}, err
		}
		selectedDigest := r.runsByReferences[signature]
		if selectedDigest == "" {
			return ResolvedTargetRun{}, domain.NewError(domain.CodeCapabilityUnavailable, "static_provisioning.resolve_target_material", "references", "have no authorized material plan", nil)
		}
		if digest != "" && digest != selectedDigest {
			return ResolvedTargetRun{}, domain.NewError(domain.CodeIntegrityViolation, "static_provisioning.resolve_target_material", "materialization_digest", "does not match the authorized references", nil)
		}
		digest = selectedDigest
	}
	configured, found := r.runs[digest]
	if !found {
		return ResolvedTargetRun{}, domain.NewError(domain.CodeCapabilityUnavailable, "static_provisioning.resolve_target_material", "materialization_digest", "has no authorized material plan", nil)
	}
	parsed, err := domain.ParseDigest(digest)
	if err != nil {
		return ResolvedTargetRun{}, err
	}
	resolved := resolvedTargetRun(configured, parsed)
	if err := resolved.Validate(); err != nil {
		return ResolvedTargetRun{}, err
	}
	return resolved, nil
}

func resolvedTargetRun(plan StaticRunPlan, digest domain.Digest) ResolvedTargetRun {
	return ResolvedTargetRun{
		MaterializationDigest: digest, RequiredCoverage: append([]string(nil), plan.RequiredCoverage...),
		Collectors: cloneCollectorSpecs(plan.Collectors), Material: append([]ports.TargetMaterialPlan(nil), plan.Material...), MaximumDuration: plan.MaximumDuration,
	}
}

func bindTargetRunPlan(request application.StartTargetRunRequest, resolved ResolvedTargetRun, target application.TargetRecord, run application.TargetRunRecord) (ports.TargetRunPlan, error) {
	if err := resolved.Validate(); err != nil {
		return ports.TargetRunPlan{}, err
	}
	if run.MaterializationDigest != resolved.MaterializationDigest.String() {
		return ports.TargetRunPlan{}, domain.NewError(domain.CodeIntegrityViolation, "orchestration.bind_target_run", "materialization_digest", "logical run differs from authoritative material", nil)
	}
	runID, err := domain.ParseTargetRunID(run.ID)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	leaseID, err := domain.ParseLeaseID(target.LeaseID)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	agentID, err := domain.ParseAgentWorkspaceID(run.AgentWorkspaceID)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	runModel, err := domain.NewTargetRun(domain.TargetRunSpec{
		ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(run.Generation),
		AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(run.AgentGeneration),
		MaterializationDigest: resolved.MaterializationDigest, CreatedAt: run.CreatedAt,
	})
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	plan := ports.TargetRunPlan{
		IdempotencyKey: domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/run"), Run: runModel,
		RequiredCoverage: append([]string(nil), resolved.RequiredCoverage...),
		Collectors:       cloneCollectorSpecs(resolved.Collectors), Material: append([]ports.TargetMaterialPlan(nil), resolved.Material...), MaximumDuration: resolved.MaximumDuration,
	}
	plan, err = ApplyPersistedTargetRunProvisioningIdentity(plan, run)
	if err != nil {
		return ports.TargetRunPlan{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetRunPlan{}, err
	}
	return plan, nil
}

func cloneCollectorSpecs(values []ports.CollectorSpec) []ports.CollectorSpec {
	result := make([]ports.CollectorSpec, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Resources = value.Resources.Clone()
	}
	return result
}

func requiredCoverageForCollectors(values []ports.CollectorSpec) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	names := make(map[string]struct{}, len(values))
	families := make(map[string]struct{}, len(values))
	for index, value := range values {
		if err := value.Validate(); err != nil {
			return nil, fmt.Errorf("collector %d: %w", index, err)
		}
		if _, duplicate := names[value.Name]; duplicate {
			return nil, domain.NewError(domain.CodeConflict, "orchestration.collector_specs", "name", "must be unique", nil)
		}
		names[value.Name] = struct{}{}
		if value.Requirement.SignalFamily == ports.TargetLifecycleSignal {
			return nil, domain.NewError(domain.CodeConflict, "orchestration.collector_specs", "signal_family", "target.lifecycle is intrinsic and must not configure an observer adapter", nil)
		}
		if _, duplicate := families[value.Requirement.SignalFamily]; duplicate {
			return nil, domain.NewError(domain.CodeConflict, "orchestration.collector_specs", "signal_family", "must be unique", nil)
		}
		families[value.Requirement.SignalFamily] = struct{}{}
		if value.Requirement.Required {
			result[value.Requirement.SignalFamily] = struct{}{}
		}
	}
	return result, nil
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, found := right[value]; !found {
			return false
		}
	}
	return true
}

func requireConfiguredDigests(policyValue, capabilityValue string, policy, capability domain.Digest) (domain.Digest, domain.Digest, error) {
	requestedPolicy, err := domain.ParseDigest(policyValue)
	if err != nil {
		return domain.Digest{}, domain.Digest{}, err
	}
	requestedCapability, err := domain.ParseDigest(capabilityValue)
	if err != nil {
		return domain.Digest{}, domain.Digest{}, err
	}
	if requestedPolicy != policy || requestedCapability != capability {
		return domain.Digest{}, domain.Digest{}, domain.NewError(domain.CodeForbidden, "static_provisioning.require_digests", "provenance", "policy or capability digest is not authorized by the selected plan", nil)
	}
	return policy, capability, nil
}

func currentAgentGeneration(agent application.AgentWorkspaceRecord) (application.AgentGenerationRecord, error) {
	for _, generation := range agent.Generations {
		if generation.Generation == agent.CurrentGeneration {
			return generation, nil
		}
	}
	return application.AgentGenerationRecord{}, fmt.Errorf("agent current generation is missing")
}

func validateAgentRecoveryScope(request application.RecoverIncidentRequest, view application.ResearchSessionView, now time.Time) (application.AgentGenerationRecord, error) {
	const operation = "static_provisioning.resolve_agent_recovery"
	if request.Resource != application.RecoveryResourceAgent {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeInvalidArgument, operation, "resource", "must identify an agent-workspace recovery", nil)
	}
	if _, err := domain.ParseIncidentID(request.IncidentID); err != nil {
		return application.AgentGenerationRecord{}, err
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeIntegrityViolation, operation, "agent_generation", "current generation is missing", err)
	}
	if view.Session.State != domain.ResearchSessionLeased || view.Lease.State != domain.LeaseActive || !view.Lease.Termination.Empty() || !view.Lease.ExpiresAt.After(now) {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeFailedPrecondition, operation, "lease", "session must have an active unexpired lease", nil)
	}
	if view.Session.LeaseID != view.Lease.ID || view.Session.AgentWorkspaceID != view.Agent.ID ||
		view.Lease.SessionID != view.Session.ID || view.Lease.AgentWorkspaceID != view.Agent.ID || view.Agent.SessionID != view.Session.ID ||
		view.Lease.AgentGeneration != view.Agent.CurrentGeneration {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeForbidden, operation, "scope", "session, lease, and agent do not identify one scope", nil)
	}
	if generation.State != domain.AgentGenerationProvisioning || generation.Generation <= 1 || generation.Previous != generation.Generation-1 || generation.RecoveryIncident != request.IncidentID || !hasAgentGeneration(view.Agent.Generations, generation.Previous) {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeFailedPrecondition, operation, "agent_generation", "current generation is not the requested recovery generation", nil)
	}
	if view.Session.InputViewID != generation.InputViewID || view.Lease.InputViewID != generation.InputViewID ||
		view.Session.PolicyDigest != generation.PolicyDigest || view.Lease.PolicyDigest != generation.PolicyDigest ||
		view.Session.CapabilityDigest != generation.CapabilityDigest || view.Lease.CapabilityDigest != generation.CapabilityDigest {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeIntegrityViolation, operation, "provenance", "recovery generation provenance differs from its session or lease", nil)
	}
	incident, found := recoveryIncident(view.Incidents, request.IncidentID)
	if !found || incident.State != domain.IncidentRecovering || incident.SessionID != view.Session.ID || incident.LeaseID != view.Lease.ID ||
		incident.AgentWorkspaceID != view.Agent.ID || incident.AgentGeneration != generation.Previous {
		return application.AgentGenerationRecord{}, domain.NewError(domain.CodeFailedPrecondition, operation, "incident", "does not authorize the current recovery generation", nil)
	}
	return generation, nil
}

func hasAgentGeneration(generations []application.AgentGenerationRecord, generation uint64) bool {
	for _, candidate := range generations {
		if candidate.Generation == generation {
			return true
		}
	}
	return false
}

func recoveryIncident(incidents []application.IncidentRecord, incidentID string) (application.IncidentRecord, bool) {
	for _, incident := range incidents {
		if incident.ID == incidentID {
			return incident, true
		}
	}
	return application.IncidentRecord{}, false
}

func cloneContentSources(values map[string]ports.ContentSource) map[string]ports.ContentSource {
	if values == nil {
		return nil
	}
	result := make(map[string]ports.ContentSource, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneInputSelection(value application.InputSelectionRequest) application.InputSelectionRequest {
	value.OccurrenceRefs = append([]string(nil), value.OccurrenceRefs...)
	value.PathMappings = append([]application.InputPathMappingRequest(nil), value.PathMappings...)
	value.AllowedSidecars = append([]string(nil), value.AllowedSidecars...)
	return value
}

func inputSelectionSignature(value application.InputSelectionRequest) (string, error) {
	value = cloneInputSelection(value)
	sort.Strings(value.OccurrenceRefs)
	sort.Strings(value.AllowedSidecars)
	sort.Slice(value.PathMappings, func(i, j int) bool {
		if value.PathMappings[i].LogicalPath == value.PathMappings[j].LogicalPath {
			return value.PathMappings[i].OccurrenceRef < value.PathMappings[j].OccurrenceRef
		}
		return value.PathMappings[i].LogicalPath < value.PathMappings[j].LogicalPath
	})
	payload, err := json.Marshal(value)
	return string(payload), err
}

func targetReferencesSignature(specimens, fixtures []string) (string, error) {
	specimens = append([]string(nil), specimens...)
	fixtures = append([]string(nil), fixtures...)
	for _, values := range [][]string{specimens, fixtures} {
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return "", fmt.Errorf("material references must not contain blank values")
			}
		}
	}
	sort.Strings(specimens)
	sort.Strings(fixtures)
	payload, err := json.Marshal(struct {
		Specimens []string `json:"specimens"`
		Fixtures  []string `json:"fixtures"`
	}{specimens, fixtures})
	return string(payload), err
}
