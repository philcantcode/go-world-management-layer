package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/localmaterial"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

const (
	deploymentProfileVersion   = 3
	maxDeploymentProfileBytes  = int64(4 << 20)
	maximumConfiguredRunWindow = 24 * time.Hour
	maximumMaterialEntries     = 1024
	maximumDeploymentPlans     = 256
)

type deploymentProfile struct {
	Version       int                   `json:"version"`
	SecurityScope string                `json:"security_scope"`
	Policies      []policySourceProfile `json:"policies"`
	Material      materialProfile       `json:"material"`
	Acquisitions  []acquisitionProfile  `json:"acquisitions"`
	Targets       []targetProfile       `json:"targets,omitempty"`
	Observers     []observerProfile     `json:"observers,omitempty"`
	Runs          []runProfile          `json:"runs,omitempty"`
}

type policySourceProfile struct {
	Reference  string `json:"reference"`
	SourcePath string `json:"source_path"`
}

type materialProfile struct {
	SourceRoot     string                          `json:"source_root"`
	MaxObjectBytes int64                           `json:"max_object_bytes"`
	Entries        []materialEntryProfile          `json:"entries"`
	Selections     []localmaterial.SelectionConfig `json:"selections,omitempty"`
}

// materialEntryProfile makes the immutable byte identity explicit in the
// trusted profile. localmaterial.EntryConfig intentionally derives identity
// from bytes; the deployment loader additionally requires the operator's
// declared digest and size to match that derived identity exactly.
type materialEntryProfile struct {
	Reference     string             `json:"reference"`
	SecurityScope string             `json:"security_scope"`
	SourcePath    string             `json:"source_path"`
	Digest        string             `json:"digest"`
	Size          int64              `json:"size"`
	LogicalPath   string             `json:"logical_path"`
	Mode          uint32             `json:"mode"`
	Role          string             `json:"role"`
	Sensitivity   domain.Sensitivity `json:"sensitivity"`
	Sidecars      []string           `json:"sidecars,omitempty"`
}

type acquisitionProfile struct {
	Selection       application.InputSelectionRequest `json:"selection"`
	Construction    domain.InputViewConstruction      `json:"construction"`
	UpperByteLimit  int64                             `json:"upper_byte_limit"`
	UpperInodeLimit int64                             `json:"upper_inode_limit"`
	Policy          string                            `json:"policy"`
	AgentImage      string                            `json:"agent_image"`
	Resources       admission.Resources               `json:"resources"`
}

type targetProfile struct {
	Reference string                `json:"reference"`
	Policy    string                `json:"policy"`
	Template  targetTemplateProfile `json:"template"`
	Resources admission.Resources   `json:"resources"`
}

type targetTemplateProfile struct {
	Name                        string            `json:"name"`
	Kind                        domain.TargetKind `json:"kind"`
	Driver                      string            `json:"driver"`
	Runtime                     string            `json:"runtime,omitempty"`
	Image                       string            `json:"image,omitempty"`
	SystemImageDigest           string            `json:"system_image_digest,omitempty"`
	SystemImagePackage          string            `json:"system_image_package,omitempty"`
	IsolationProfile            string            `json:"isolation_profile"`
	BaselineState               string            `json:"baseline_state,omitempty"`
	RequireHardwareAcceleration bool              `json:"require_hardware_acceleration,omitempty"`
	Headless                    bool              `json:"headless,omitempty"`
	Rooted                      bool              `json:"rooted,omitempty"`
	Debuggable                  bool              `json:"debuggable,omitempty"`
	GuestMemoryBytes            int64             `json:"guest_memory_bytes,omitempty"`
	BootTimeout                 string            `json:"boot_timeout,omitempty"`
}

type observerProfile struct {
	Reference           string                         `json:"reference"`
	Adapter             string                         `json:"adapter"`
	Version             string                         `json:"version"`
	ConfigurationDigest string                         `json:"configuration_digest"`
	SignalFamily        string                         `json:"signal_family"`
	Placement           domain.CollectorPlacement      `json:"placement"`
	CoverageLevel       domain.CoverageLevel           `json:"coverage_level"`
	RuntimeBinding      observerprocess.RuntimeBinding `json:"runtime_binding,omitempty"`
	Required            bool                           `json:"required"`
	Program             string                         `json:"program"`
	Args                []string                       `json:"args,omitempty"`
	Environment         map[string]string              `json:"environment,omitempty"`
	VersionArgs         []string                       `json:"version_args,omitempty"`
	Readiness           observerReadinessProfile       `json:"readiness"`
	Resources           admission.Resources            `json:"resources"`
	MaximumBytes        int64                          `json:"maximum_bytes"`
}

type observerReadinessProfile struct {
	Program  string   `json:"program"`
	Args     []string `json:"args,omitempty"`
	Interval string   `json:"interval"`
}

type runProfile struct {
	TargetReferences       []string             `json:"target_references"`
	SpecimenOccurrenceRefs []string             `json:"specimen_occurrence_refs"`
	FixtureRefs            []string             `json:"fixture_refs,omitempty"`
	CollectorReferences    []string             `json:"collector_references,omitempty"`
	RequiredCoverage       []string             `json:"required_coverage"`
	Material               []runMaterialProfile `json:"material"`
	MaximumDuration        string               `json:"maximum_duration"`
}

type runMaterialProfile struct {
	Reference   string `json:"reference"`
	LogicalPath string `json:"logical_path"`
	Mode        uint32 `json:"mode"`
}

type builtDeployment struct {
	authority        *localmaterial.Authority
	resolver         orchestration.ProvisioningResolver
	sourceRoot       string
	agentRepository  string
	targetRepository string
	targetTemplates  []ports.TargetTemplate
	linuxTargets     []ports.TargetTemplate
	androidTargets   []ports.TargetTemplate
	androidImages    map[string]string
	runCount         int
	imageReferences  []string
	profileDigest    domain.Digest
	observerAdapters []observerAdapterPlan
	policySources    map[string][]byte
	static           orchestration.StaticProvisioningConfig
	agentPolicies    map[string]string
	targetPolicies   map[string]string
	runTargets       map[string]map[string]struct{}
}

type observerAdapterPlan struct {
	Reference string
	Spec      ports.CollectorSpec
	Adapter   observerprocess.Adapter
}

type targetBoundResolver struct {
	base       *orchestration.StaticProvisioningResolver
	runTargets map[string]map[string]struct{}
}

func (r *targetBoundResolver) ResolveAcquisition(ctx context.Context, request application.AcquireRequest) (orchestration.ResolvedAcquisition, error) {
	return r.base.ResolveAcquisition(ctx, request)
}

func (r *targetBoundResolver) ResolveAgentRecovery(ctx context.Context, request application.RecoverIncidentRequest, view application.ResearchSessionView) (orchestration.ResolvedAcquisition, error) {
	return r.base.ResolveAgentRecovery(ctx, request, view)
}

func (r *targetBoundResolver) ResolvePersistedAgent(ctx context.Context, view application.ResearchSessionView) (orchestration.ResolvedAcquisition, error) {
	return r.base.ResolvePersistedAgent(ctx, view)
}

func (r *targetBoundResolver) ResolveTarget(ctx context.Context, request application.CreateTargetRequest, target application.TargetRecord) (ports.TargetPlan, error) {
	return r.base.ResolveTarget(ctx, request, target)
}

func (r *targetBoundResolver) ResolveTargetMaterial(ctx context.Context, request application.StartTargetRunRequest, target application.TargetRecord) (orchestration.ResolvedTargetRun, error) {
	resolved, err := r.base.ResolveTargetMaterial(ctx, request, target)
	if err != nil {
		return orchestration.ResolvedTargetRun{}, err
	}
	allowed := r.runTargets[resolved.MaterializationDigest.String()]
	if _, found := allowed[target.Template]; !found {
		return orchestration.ResolvedTargetRun{}, domain.NewError(
			domain.CodeForbidden, "daemon.deployment.resolve_target_material", "target.template",
			"the selected material plan is not authorized for this target template", nil,
		)
	}
	return resolved, nil
}

type pinnedImage struct {
	repository string
	digest     domain.Digest
	reference  string
	packageID  string
}

type authorityContentSource struct {
	authority  *localmaterial.Authority
	occurrence ports.ArtifactOccurrence
}

func (s authorityContentSource) Digest() domain.Digest { return s.occurrence.Digest }
func (s authorityContentSource) Size() int64           { return s.occurrence.Size }
func (s authorityContentSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return s.authority.OpenContent(ctx, s.occurrence)
}

func loadDeployment(ctx context.Context, path, publicationRoot string, maximumObjectBytes int64) (builtDeployment, error) {
	raw, profile, err := readDeploymentProfile(path)
	if err != nil {
		return builtDeployment{}, err
	}
	if profile.Version != deploymentProfileVersion {
		return builtDeployment{}, fmt.Errorf("deployment profile version must be %d", deploymentProfileVersion)
	}
	scope := strings.TrimSpace(profile.SecurityScope)
	if scope == "" {
		return builtDeployment{}, fmt.Errorf("deployment profile security_scope must not be blank")
	}
	policySources, err := loadPolicySources(profile.Policies)
	if err != nil {
		return builtDeployment{}, err
	}
	if err := requireAbsoluteManagedRoot("material.source_root", profile.Material.SourceRoot); err != nil {
		return builtDeployment{}, err
	}
	if profile.Material.MaxObjectBytes <= 0 || profile.Material.MaxObjectBytes > maximumObjectBytes {
		return builtDeployment{}, fmt.Errorf("material.max_object_bytes must be positive and no greater than the daemon object limit")
	}
	if len(profile.Material.Entries) == 0 || len(profile.Acquisitions) == 0 {
		return builtDeployment{}, fmt.Errorf("deployment profile requires material entries and acquisitions")
	}
	if len(profile.Material.Entries) > maximumMaterialEntries || len(profile.Material.Selections) > maximumMaterialEntries {
		return builtDeployment{}, fmt.Errorf("deployment profile permits at most %d material entries and selections", maximumMaterialEntries)
	}
	for name, count := range map[string]int{
		"acquisitions": len(profile.Acquisitions), "targets": len(profile.Targets), "observers": len(profile.Observers), "runs": len(profile.Runs),
	} {
		if count > maximumDeploymentPlans {
			return builtDeployment{}, fmt.Errorf("deployment profile permits at most %d %s", maximumDeploymentPlans, name)
		}
	}
	entryConfigs := make([]localmaterial.EntryConfig, 0, len(profile.Material.Entries))
	var declaredBytes int64
	for index := range profile.Material.Entries {
		entry := profile.Material.Entries[index]
		if entry.SecurityScope != scope {
			return builtDeployment{}, fmt.Errorf("material entry %d security_scope does not match the deployment scope", index)
		}
		if err := requireRegularPermissionMode(entry.Mode); err != nil {
			return builtDeployment{}, fmt.Errorf("material entry %d mode: %w", index, err)
		}
		if _, err := domain.ParseDigest(entry.Digest); err != nil {
			return builtDeployment{}, fmt.Errorf("material entry %d digest: %w", index, err)
		}
		if entry.Size < 0 || entry.Size > profile.Material.MaxObjectBytes || declaredBytes > maximumObjectBytes-entry.Size {
			return builtDeployment{}, fmt.Errorf("material entry %d size exceeds the configured object or aggregate material bound", index)
		}
		declaredBytes += entry.Size
		entryConfigs = append(entryConfigs, localmaterial.EntryConfig{
			Reference: entry.Reference, SecurityScope: entry.SecurityScope, SourcePath: entry.SourcePath,
			LogicalPath: entry.LogicalPath, Mode: entry.Mode, Role: entry.Role,
			Sensitivity: entry.Sensitivity, Sidecars: append([]string(nil), entry.Sidecars...),
		})
	}
	for index := range profile.Material.Selections {
		if profile.Material.Selections[index].SecurityScope != scope {
			return builtDeployment{}, fmt.Errorf("material selection %d security_scope does not match the deployment scope", index)
		}
	}
	authority, err := localmaterial.New(localmaterial.Config{
		SourceRoot: profile.Material.SourceRoot, PublicationRoot: publicationRoot,
		MaxObjectBytes: profile.Material.MaxObjectBytes,
		Entries:        entryConfigs, Selections: profile.Material.Selections,
	})
	if err != nil {
		return builtDeployment{}, fmt.Errorf("open deployment material authority: %w", err)
	}
	for index, declared := range profile.Material.Entries {
		registered, err := authority.Entry(scope, declared.Reference)
		if err != nil {
			return builtDeployment{}, fmt.Errorf("verify material entry %d: %w", index, err)
		}
		digest, _ := domain.ParseDigest(declared.Digest)
		if registered.Occurrence.Digest != digest || registered.Occurrence.Size != declared.Size ||
			registered.Mode != declared.Mode || registered.LogicalPath != declared.LogicalPath {
			return builtDeployment{}, fmt.Errorf("material entry %d declared digest, size, mode, or logical path does not match the authorized source", index)
		}
	}
	static := orchestration.StaticProvisioningConfig{
		Agents:  make(map[string]orchestration.StaticAgentPlan, len(profile.Acquisitions)),
		Targets: make(map[string]orchestration.StaticTargetPlan, len(profile.Targets)),
		Runs:    make(map[string]orchestration.StaticRunPlan, len(profile.Runs)),
	}
	agentPolicies := make(map[string]string, len(profile.Acquisitions))
	targetPolicies := make(map[string]string, len(profile.Targets))
	var agentRepository string
	imageReferences := make(map[string]struct{})
	for index, acquisition := range profile.Acquisitions {
		plan, image, err := buildAcquisitionPlan(ctx, authority, scope, acquisition)
		if err != nil {
			return builtDeployment{}, fmt.Errorf("acquisition %d: %w", index, err)
		}
		if agentRepository == "" {
			agentRepository = image.repository
		} else if agentRepository != image.repository {
			return builtDeployment{}, fmt.Errorf("acquisition %d uses repository %q; all agent images must use %q", index, image.repository, agentRepository)
		}
		key := plan.InputView.ID().String()
		if _, duplicate := static.Agents[key]; duplicate {
			return builtDeployment{}, fmt.Errorf("acquisition %d duplicates resolved input view %s", index, key)
		}
		static.Agents[key] = plan
		if _, found := policySources[acquisition.Policy]; !found {
			return builtDeployment{}, fmt.Errorf("acquisition %d names unknown policy %q", index, acquisition.Policy)
		}
		agentPolicies[key] = acquisition.Policy
		imageReferences[image.reference] = struct{}{}
	}
	var targetRepository string
	targetTemplates := make([]ports.TargetTemplate, 0, len(profile.Targets))
	linuxTargets := make([]ports.TargetTemplate, 0, len(profile.Targets))
	androidTargets := make([]ports.TargetTemplate, 0, len(profile.Targets))
	androidImages := make(map[string]string)
	var requiredAndroidImage pinnedImage
	hasAndroidImage := false
	for index, configured := range profile.Targets {
		reference, plan, image, err := buildTargetPlan(configured)
		if err != nil {
			return builtDeployment{}, fmt.Errorf("target %d: %w", index, err)
		}
		if plan.Template.Kind == domain.TargetLinuxContainer {
			if targetRepository == "" {
				targetRepository = image.repository
			} else if targetRepository != image.repository {
				return builtDeployment{}, fmt.Errorf("target %d uses repository %q; all Linux targets must use %q", index, image.repository, targetRepository)
			}
			linuxTargets = append(linuxTargets, plan.Template)
		} else {
			androidTargets = append(androidTargets, plan.Template)
			if hasAndroidImage && (image.digest != requiredAndroidImage.digest || image.packageID != requiredAndroidImage.packageID) {
				return builtDeployment{}, fmt.Errorf(
					"target %d uses Android system-image identity (%s, %q); all Android targets must use one system-image digest/package identity (%s, %q)",
					index, image.digest, image.packageID, requiredAndroidImage.digest, requiredAndroidImage.packageID,
				)
			}
			if !hasAndroidImage {
				requiredAndroidImage = image
				hasAndroidImage = true
			}
			androidImages[image.digest.String()] = image.packageID
		}
		if _, duplicate := static.Targets[reference]; duplicate {
			return builtDeployment{}, fmt.Errorf("target %d duplicates reference %q", index, reference)
		}
		static.Targets[reference] = plan
		if _, found := policySources[configured.Policy]; !found {
			return builtDeployment{}, fmt.Errorf("target %d names unknown policy %q", index, configured.Policy)
		}
		targetPolicies[reference] = configured.Policy
		targetTemplates = append(targetTemplates, plan.Template)
		if image.reference != "" {
			imageReferences[image.reference] = struct{}{}
		}
	}
	observerAdapters, observerSpecs, err := buildObserverPlans(profile.Observers, maximumObjectBytes)
	if err != nil {
		return builtDeployment{}, err
	}
	runTargets := make(map[string]map[string]struct{}, len(profile.Runs))
	usedObservers := make(map[string]struct{}, len(observerSpecs))
	for index, configured := range profile.Runs {
		digest, plan, err := buildRunPlan(ctx, authority, scope, configured, observerSpecs, maximumObjectBytes)
		if err != nil {
			return builtDeployment{}, fmt.Errorf("run %d: %w", index, err)
		}
		if _, duplicate := static.Runs[digest.String()]; duplicate {
			return builtDeployment{}, fmt.Errorf("run %d duplicates materialization %s", index, digest)
		}
		static.Runs[digest.String()] = plan
		allowed, err := exactNonBlankSet("target_references", configured.TargetReferences)
		if err != nil {
			return builtDeployment{}, fmt.Errorf("run %d: %w", index, err)
		}
		for targetReference := range allowed {
			if _, found := static.Targets[targetReference]; !found {
				return builtDeployment{}, fmt.Errorf("run %d authorizes unknown target reference %q", index, targetReference)
			}
		}
		runTargets[digest.String()] = allowed
		for _, reference := range configured.CollectorReferences {
			usedObservers[reference] = struct{}{}
		}
	}
	for reference := range observerSpecs {
		if _, used := usedObservers[reference]; !used {
			return builtDeployment{}, fmt.Errorf("observer %q is not referenced by any run", reference)
		}
	}
	images := make([]string, 0, len(imageReferences))
	for reference := range imageReferences {
		images = append(images, reference)
	}
	sort.Strings(images)
	sortTargetTemplates(targetTemplates)
	sortTargetTemplates(linuxTargets)
	sortTargetTemplates(androidTargets)
	return builtDeployment{
		authority: authority, sourceRoot: filepath.Clean(profile.Material.SourceRoot),
		agentRepository:  agentRepository,
		targetRepository: targetRepository, targetTemplates: targetTemplates,
		linuxTargets: linuxTargets, androidTargets: androidTargets, androidImages: androidImages,
		runCount: len(profile.Runs), imageReferences: images, profileDigest: domain.NewDigest(raw),
		observerAdapters: observerAdapters, policySources: policySources, static: static,
		agentPolicies: agentPolicies, targetPolicies: targetPolicies, runTargets: runTargets,
	}, nil
}

func sortTargetTemplates(values []ports.TargetTemplate) {
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
}

func readDeploymentProfile(path string) ([]byte, deploymentProfile, error) {
	if strings.TrimSpace(path) == "" {
		return nil, deploymentProfile{}, fmt.Errorf("deployment-profile is required for physical drivers")
	}
	raw, err := readRegularBoundedFile(path, "deployment profile", maxDeploymentProfileBytes)
	if err != nil {
		return nil, deploymentProfile{}, err
	}
	if err := requireUniqueJSONKeys(raw); err != nil {
		return nil, deploymentProfile{}, fmt.Errorf("decode deployment profile: %w", err)
	}
	var profile deploymentProfile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return nil, deploymentProfile{}, fmt.Errorf("decode deployment profile: %w", err)
	}
	return raw, profile, nil
}

func loadPolicySources(configured []policySourceProfile) (map[string][]byte, error) {
	if len(configured) == 0 {
		return nil, fmt.Errorf("deployment profile requires at least one strict policy source")
	}
	if len(configured) > maximumDeploymentPlans {
		return nil, fmt.Errorf("deployment profile permits at most %d policy sources", maximumDeploymentPlans)
	}
	result := make(map[string][]byte, len(configured))
	paths := make(map[string]string, len(configured))
	for index, source := range configured {
		reference := strings.TrimSpace(source.Reference)
		if reference == "" || reference != source.Reference {
			return nil, fmt.Errorf("policy %d reference must be non-blank and trimmed", index)
		}
		if _, duplicate := result[reference]; duplicate {
			return nil, fmt.Errorf("policy %d duplicates reference %q", index, reference)
		}
		if err := requireAbsoluteManagedRoot(fmt.Sprintf("policies[%d].source_path", index), source.SourcePath); err != nil {
			return nil, err
		}
		cleaned := filepath.Clean(source.SourcePath)
		if previous := paths[cleaned]; previous != "" {
			return nil, fmt.Errorf("policies %q and %q use the same source path", previous, reference)
		}
		raw, err := readRegularBoundedFile(cleaned, fmt.Sprintf("policy %q source", reference), maxDeploymentProfileBytes)
		if err != nil {
			return nil, err
		}
		if _, err := policy.Requirements(raw); err != nil {
			return nil, fmt.Errorf("strictly validate policy %q source: %w", reference, err)
		}
		result[reference] = raw
		paths[cleaned] = reference
	}
	return result, nil
}

func readRegularBoundedFile(path, description string, maximumBytes int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", description, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file, not a symlink or special file", description)
	}
	opened, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", description, err)
	}
	defer opened.Close()
	openedInfo, err := opened.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened %s: %w", description, err)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("%s changed while it was opened", description)
	}
	raw, err := io.ReadAll(io.LimitReader(opened, maximumBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", description, err)
	}
	if len(raw) == 0 || int64(len(raw)) > maximumBytes {
		return nil, fmt.Errorf("%s must be non-empty and no larger than %d bytes", description, maximumBytes)
	}
	return raw, nil
}

func (d *builtDeployment) publishAndBindPolicies(ctx context.Context, authority *policyauthority.Authority, capabilities policy.CapabilityFingerprint) error {
	if d == nil || authority == nil || capabilities.Digest().IsZero() {
		return fmt.Errorf("deployment, policy authority, and complete capability fingerprint are required")
	}
	publications, err := d.compileAndBindPolicies(capabilities)
	if err != nil {
		return err
	}
	return publishCompiledPolicies(ctx, authority, capabilities, publications)
}

func (d *builtDeployment) compileAndBindPolicies(capabilities policy.CapabilityFingerprint) (map[string]*policy.EffectivePolicy, error) {
	if d == nil || capabilities.Digest().IsZero() {
		return nil, fmt.Errorf("deployment and complete capability fingerprint are required")
	}
	publications := make(map[string]*policy.EffectivePolicy, len(d.policySources))
	for _, reference := range sortedStringKeys(d.policySources) {
		effective, err := policy.Compile(d.policySources[reference], policy.CompileOptions{Capabilities: capabilities})
		if err != nil {
			return nil, fmt.Errorf("compile effective policy %q: %w", reference, err)
		}
		document := effective.Policy()
		actualReference := fmt.Sprintf("%s@%d", document.Metadata.Name, document.Metadata.Revision)
		if actualReference != reference {
			return nil, fmt.Errorf("policy source %q declares immutable reference %q", reference, actualReference)
		}
		publications[reference] = effective
	}
	bound := cloneStaticProvisioningConfig(d.static)
	for inputViewID, reference := range d.agentPolicies {
		effective := publications[reference]
		if effective == nil {
			return nil, fmt.Errorf("agent plan %s references uncompiled policy %q", inputViewID, reference)
		}
		plan := bound.Agents[inputViewID]
		plan.PolicyDigest = effective.Digest()
		plan.CapabilityDigest = effective.CapabilityFingerprintDigest()
		bound.Agents[inputViewID] = plan
	}
	for targetReference, reference := range d.targetPolicies {
		effective := publications[reference]
		if effective == nil {
			return nil, fmt.Errorf("target plan %s references uncompiled policy %q", targetReference, reference)
		}
		plan := bound.Targets[targetReference]
		plan.PolicyDigest = effective.Digest()
		plan.CapabilityDigest = effective.CapabilityFingerprintDigest()
		bound.Targets[targetReference] = plan
	}
	staticResolver, err := orchestration.NewStaticProvisioningResolver(bound)
	if err != nil {
		return nil, fmt.Errorf("compile effective-policy-bound provisioning resolver: %w", err)
	}
	d.resolver = &targetBoundResolver{base: staticResolver, runTargets: cloneRunTargets(d.runTargets)}
	d.static = bound
	return publications, nil
}

func publishCompiledPolicies(ctx context.Context, authority *policyauthority.Authority, capabilities policy.CapabilityFingerprint, publications map[string]*policy.EffectivePolicy) error {
	if authority == nil || capabilities.Digest().IsZero() || len(publications) == 0 {
		return fmt.Errorf("policy authority, complete capability fingerprint, and compiled policies are required")
	}
	for _, reference := range sortedStringKeys(publications) {
		effective := publications[reference]
		published, err := authority.PublishCompiled(ctx, effective, capabilities)
		if err != nil {
			return fmt.Errorf("publish effective policy %q: %w", reference, err)
		}
		if published.Digest() != effective.Digest() || published.CapabilityFingerprintDigest() != effective.CapabilityFingerprintDigest() {
			return fmt.Errorf("published effective policy %q changed immutable identity", reference)
		}
	}
	return nil
}

func cloneStaticProvisioningConfig(value orchestration.StaticProvisioningConfig) orchestration.StaticProvisioningConfig {
	result := orchestration.StaticProvisioningConfig{
		Agents:  make(map[string]orchestration.StaticAgentPlan, len(value.Agents)),
		Targets: make(map[string]orchestration.StaticTargetPlan, len(value.Targets)),
		Runs:    make(map[string]orchestration.StaticRunPlan, len(value.Runs)),
		Now:     value.Now,
	}
	for key, plan := range value.Agents {
		result.Agents[key] = plan
	}
	for key, plan := range value.Targets {
		result.Targets[key] = plan
	}
	for key, plan := range value.Runs {
		result.Runs[key] = plan
	}
	return result
}

func cloneRunTargets(value map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(value))
	for digest, targets := range value {
		cloned := make(map[string]struct{}, len(targets))
		for target := range targets {
			cloned[target] = struct{}{}
		}
		result[digest] = cloned
	}
	return result
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func buildAcquisitionPlan(ctx context.Context, authority *localmaterial.Authority, scope string, configured acquisitionProfile) (orchestration.StaticAgentPlan, pinnedImage, error) {
	if configured.Selection.Empty() {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("selection must not be empty")
	}
	if configured.Selection.SecurityScope != scope {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("selection security_scope does not match the deployment scope")
	}
	if configured.Construction != domain.InputViewAllowCopy || configured.Selection.RequireZeroCopy {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("the directory workspace driver requires construction=allow-copy and require_zero_copy=false")
	}
	if configured.UpperByteLimit <= 0 || configured.UpperInodeLimit <= 0 {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("workspace byte and inode limits must be positive")
	}
	image, err := parsePinnedImage(configured.AgentImage)
	if err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("agent_image: %w", err)
	}
	if err := requireAgentResources(configured.Resources); err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("resources: %w", err)
	}
	if configured.Resources.StorageBytes < configured.UpperByteLimit || configured.Resources.Inodes < configured.UpperInodeLimit {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("workspace limits exceed the authorized storage or inode resources")
	}
	selected, err := resolveSelectedEntries(ctx, authority, scope, configured.Selection)
	if err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, err
	}
	mappings, err := exactPathMappings(configured.Selection.PathMappings, selected)
	if err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, err
	}
	if err := validateRequestedSidecars(configured.Selection.AllowedSidecars, selected); err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, err
	}
	inputEntries := make([]ports.InputEntryPlan, 0, len(selected))
	content := make(map[string]ports.ContentSource, len(selected))
	for _, entry := range selected {
		if entry.Mode == 0 || entry.Mode&^uint32(0o777) != 0 {
			return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("selected input occurrence %q has a mode unsupported by the directory workspace driver", entry.Occurrence.Reference)
		}
		logicalPath := entry.LogicalPath
		if mapped := mappings[entry.Occurrence.Reference]; mapped != "" {
			logicalPath = mapped
		}
		sidecars := permittedSidecars(configured.Selection.AllowedSidecars, entry.Sidecars)
		inputEntries = append(inputEntries, ports.InputEntryPlan{
			Occurrence: entry.Occurrence, LogicalPath: logicalPath, Mode: entry.Mode,
			PermittedSidecars: sidecars,
		})
		content[logicalPath] = authorityContentSource{authority: authority, occurrence: entry.Occurrence}
	}
	manifest, err := authority.ResolveInputView(ctx, ports.InputPlan{SecurityScope: scope, Entries: inputEntries})
	if err != nil {
		return orchestration.StaticAgentPlan{}, pinnedImage{}, fmt.Errorf("resolve input view: %w", err)
	}
	return orchestration.StaticAgentPlan{
		Selection: configured.Selection, InputView: manifest, SecurityScope: scope,
		Construction: configured.Construction, Content: content,
		UpperByteLimit: configured.UpperByteLimit, UpperInodeLimit: configured.UpperInodeLimit,
		ImageDigest: image.digest,
		Resources:   configured.Resources,
	}, image, nil
}

func resolveSelectedEntries(ctx context.Context, authority *localmaterial.Authority, scope string, selection application.InputSelectionRequest) ([]localmaterial.CatalogEntry, error) {
	hasFrozen := strings.TrimSpace(selection.FrozenSelectionRef) != ""
	hasOccurrences := len(selection.OccurrenceRefs) != 0
	if hasFrozen == hasOccurrences {
		return nil, fmt.Errorf("selection must set exactly one of frozen_selection_ref or occurrence_refs")
	}
	if hasFrozen {
		entries, err := authority.ResolveSelection(ctx, scope, selection.FrozenSelectionRef)
		if err != nil {
			return nil, fmt.Errorf("resolve frozen selection: %w", err)
		}
		return entries, nil
	}
	result := make([]localmaterial.CatalogEntry, 0, len(selection.OccurrenceRefs))
	seen := make(map[string]struct{}, len(selection.OccurrenceRefs))
	for index, reference := range selection.OccurrenceRefs {
		if _, duplicate := seen[reference]; duplicate {
			return nil, fmt.Errorf("selection occurrence_refs contains duplicate %q", reference)
		}
		seen[reference] = struct{}{}
		if _, err := authority.ResolveOccurrence(ctx, scope, reference); err != nil {
			return nil, fmt.Errorf("resolve occurrence_refs[%d]: %w", index, err)
		}
		entry, err := authority.Entry(scope, reference)
		if err != nil {
			return nil, fmt.Errorf("load occurrence_refs[%d]: %w", index, err)
		}
		result = append(result, entry)
	}
	return result, nil
}

func exactPathMappings(configured []application.InputPathMappingRequest, selected []localmaterial.CatalogEntry) (map[string]string, error) {
	selectedReferences := make(map[string]struct{}, len(selected))
	for _, entry := range selected {
		selectedReferences[entry.Occurrence.Reference] = struct{}{}
	}
	result := make(map[string]string, len(configured))
	for index, mapping := range configured {
		if _, found := selectedReferences[mapping.OccurrenceRef]; !found {
			return nil, fmt.Errorf("path_mappings[%d] refers to an occurrence outside the selection", index)
		}
		if _, duplicate := result[mapping.OccurrenceRef]; duplicate {
			return nil, fmt.Errorf("path_mappings contains more than one mapping for %q", mapping.OccurrenceRef)
		}
		result[mapping.OccurrenceRef] = mapping.LogicalPath
	}
	return result, nil
}

func permittedSidecars(requested, registered []string) []string {
	allowed := make(map[string]struct{}, len(registered))
	for _, value := range registered {
		allowed[value] = struct{}{}
	}
	result := make([]string, 0, len(requested))
	for _, value := range requested {
		if _, found := allowed[value]; found {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func validateRequestedSidecars(requested []string, selected []localmaterial.CatalogEntry) error {
	available := make(map[string]struct{})
	for _, entry := range selected {
		for _, sidecar := range entry.Sidecars {
			available[sidecar] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(requested))
	for index, sidecar := range requested {
		if strings.TrimSpace(sidecar) == "" {
			return fmt.Errorf("allowed_sidecars[%d] must not be blank", index)
		}
		if _, duplicate := seen[sidecar]; duplicate {
			return fmt.Errorf("allowed_sidecars contains duplicate %q", sidecar)
		}
		seen[sidecar] = struct{}{}
		if _, found := available[sidecar]; !found {
			return fmt.Errorf("allowed sidecar %q is not registered for any selected occurrence", sidecar)
		}
	}
	return nil
}

func buildTargetPlan(configured targetProfile) (string, orchestration.StaticTargetPlan, pinnedImage, error) {
	reference := strings.TrimSpace(configured.Reference)
	if reference == "" {
		return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("reference must not be blank")
	}
	if reference != configured.Template.Name {
		return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("reference must exactly match template.name")
	}
	var image pinnedImage
	template := ports.TargetTemplate{
		Name: configured.Template.Name, Kind: configured.Template.Kind,
		Driver: configured.Template.Driver, Runtime: configured.Template.Runtime,
		IsolationProfile: configured.Template.IsolationProfile,
	}
	switch configured.Template.Kind {
	case domain.TargetLinuxContainer:
		var err error
		image, err = parsePinnedImage(configured.Template.Image)
		if err != nil {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("template.image: %w", err)
		}
		if configured.Template.Driver != "docker" || configured.Template.Runtime != "runc" {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("Linux target templates require driver=docker and runtime=runc")
		}
		if configured.Template.SystemImageDigest != "" || configured.Template.SystemImagePackage != "" || configured.Template.BaselineState != "" || configured.Template.RequireHardwareAcceleration || configured.Template.Headless || configured.Template.Rooted || configured.Template.Debuggable || configured.Template.GuestMemoryBytes != 0 || configured.Template.BootTimeout != "" {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("Linux target template contains Android-only fields")
		}
		if err := requireTargetResources(configured.Resources); err != nil {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("resources: %w", err)
		}
		template.ImageDigest = image.digest
	case domain.TargetAndroidVirtualDevice:
		if configured.Template.Driver != "android-emulator" {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("Android target templates require driver=android-emulator")
		}
		if configured.Template.Runtime != "" || configured.Template.Image != "" {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("Android target template contains Linux-only runtime/image fields")
		}
		if err := cuttlefish.ValidateManagedSystemImagePackage(configured.Template.SystemImagePackage); err != nil {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("template.system_image_package: %w", err)
		}
		digest, err := domain.ParseDigest(configured.Template.SystemImageDigest)
		if err != nil {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("template.system_image_digest: %w", err)
		}
		bootTimeout, err := time.ParseDuration(configured.Template.BootTimeout)
		if err != nil || bootTimeout <= 0 || bootTimeout > maximumConfiguredRunWindow {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("template.boot_timeout must be a positive Go duration no greater than %s", maximumConfiguredRunWindow)
		}
		if err := requireAndroidTargetResources(configured.Resources, configured.Template.GuestMemoryBytes); err != nil {
			return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("resources: %w", err)
		}
		template.ImageDigest = digest
		template.BaselineState = configured.Template.BaselineState
		template.RequireHardwareAcceleration = configured.Template.RequireHardwareAcceleration
		template.Headless = configured.Template.Headless
		template.Rooted = configured.Template.Rooted
		template.Debuggable = configured.Template.Debuggable
		template.GuestMemoryBytes = configured.Template.GuestMemoryBytes
		template.BootTimeout = bootTimeout
		image.digest = digest
		image.packageID = configured.Template.SystemImagePackage
	default:
		return "", orchestration.StaticTargetPlan{}, pinnedImage{}, fmt.Errorf("unsupported target kind %q", configured.Template.Kind)
	}
	if err := template.Validate(); err != nil {
		return "", orchestration.StaticTargetPlan{}, pinnedImage{}, err
	}
	return reference, orchestration.StaticTargetPlan{
		Template: template, Resources: configured.Resources,
	}, image, nil
}

func buildObserverPlans(configured []observerProfile, maximumBytes int64) ([]observerAdapterPlan, map[string]ports.CollectorSpec, error) {
	plans := make([]observerAdapterPlan, 0, len(configured))
	specs := make(map[string]ports.CollectorSpec, len(configured))
	adapters := make(map[string]struct{}, len(configured))
	for index, value := range configured {
		reference := strings.TrimSpace(value.Reference)
		adapterName := strings.TrimSpace(value.Adapter)
		version := strings.TrimSpace(value.Version)
		if reference == "" || reference != value.Reference || adapterName == "" || adapterName != value.Adapter || version == "" || version != value.Version {
			return nil, nil, fmt.Errorf("observer %d reference, adapter, and version must be non-blank and trimmed", index)
		}
		if err := ports.ValidateCollectorName(reference); err != nil {
			return nil, nil, fmt.Errorf("observer %d reference: %w", index, err)
		}
		if _, duplicate := specs[reference]; duplicate {
			return nil, nil, fmt.Errorf("observer %d duplicates reference %q", index, reference)
		}
		if _, duplicate := adapters[adapterName]; duplicate {
			return nil, nil, fmt.Errorf("observer %d duplicates adapter %q", index, adapterName)
		}
		adapters[adapterName] = struct{}{}
		if value.SignalFamily == ports.TargetLifecycleSignal {
			return nil, nil, fmt.Errorf("observer %d must not configure intrinsic signal family %q", index, ports.TargetLifecycleSignal)
		}
		requirement := ports.ObservationRequirement{
			SignalFamily: value.SignalFamily, Placement: value.Placement,
			MinimumLevel: value.CoverageLevel, Required: value.Required,
		}
		if err := requirement.Validate(); err != nil {
			return nil, nil, fmt.Errorf("observer %d requirement: %w", index, err)
		}
		if !filepath.IsAbs(value.Program) || !filepath.IsAbs(value.Readiness.Program) {
			return nil, nil, fmt.Errorf("observer %d program and readiness.program must be absolute paths", index)
		}
		interval, err := time.ParseDuration(value.Readiness.Interval)
		if err != nil || interval <= 0 || interval > time.Minute {
			return nil, nil, fmt.Errorf("observer %d readiness.interval must be a positive Go duration no greater than 1m", index)
		}
		if value.MaximumBytes <= 0 || value.MaximumBytes > maximumBytes {
			return nil, nil, fmt.Errorf("observer %d maximum_bytes must be positive and no greater than the daemon bundle limit", index)
		}
		if err := value.Resources.Validate(); err != nil {
			return nil, nil, fmt.Errorf("observer %d resources: %w", index, err)
		}
		if !value.Resources.IsZero() {
			return nil, nil, fmt.Errorf("observer %d resources must be zero because the local process supervisor does not enforce CPU, memory, storage, inode, PID, or device limits", index)
		}
		configuration := observerAdapterConfiguration(value, interval)
		adapter, err := observerprocess.BuildAdapter(configuration)
		if err != nil {
			return nil, nil, fmt.Errorf("observer %d configuration: %w", index, err)
		}
		declaredConfiguration, err := domain.ParseDigest(value.ConfigurationDigest)
		if err != nil {
			return nil, nil, fmt.Errorf("observer %d configuration_digest: %w", index, err)
		}
		computedConfiguration := adapter.ConfigurationDigest
		if declaredConfiguration != computedConfiguration {
			return nil, nil, fmt.Errorf("observer %d configuration_digest does not identify the exact adapter configuration", index)
		}
		spec := ports.CollectorSpec{
			Name: reference, Requirement: requirement, Adapter: adapterName, Version: version,
			ConfigurationDigest: declaredConfiguration, Resources: value.Resources.Clone(), MaximumBytes: value.MaximumBytes,
		}
		if err := spec.Validate(); err != nil {
			return nil, nil, fmt.Errorf("observer %d: %w", index, err)
		}
		plans = append(plans, observerAdapterPlan{Reference: reference, Spec: spec, Adapter: adapter})
		specs[reference] = spec
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Reference < plans[j].Reference })
	return plans, specs, nil
}

func observerAdapterConfiguration(value observerProfile, interval time.Duration) observerprocess.AdapterConfiguration {
	return observerprocess.AdapterConfiguration{
		Adapter: value.Adapter, Version: value.Version, SignalFamily: value.SignalFamily,
		Placement: value.Placement, CoverageLevel: value.CoverageLevel, RuntimeBinding: value.RuntimeBinding, Program: value.Program,
		Args: value.Args, Environment: value.Environment, VersionArgs: value.VersionArgs,
		ReadinessProgram: value.Readiness.Program, ReadinessArgs: value.Readiness.Args, ReadinessInterval: interval,
	}
}

func buildRunPlan(ctx context.Context, authority *localmaterial.Authority, scope string, configured runProfile, observerSpecs map[string]ports.CollectorSpec, maximumObserverBytes int64) (domain.Digest, orchestration.StaticRunPlan, error) {
	duration, err := time.ParseDuration(configured.MaximumDuration)
	if err != nil || duration <= 0 || duration > maximumConfiguredRunWindow {
		return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("maximum_duration must be a positive Go duration no greater than %s", maximumConfiguredRunWindow)
	}
	if len(configured.RequiredCoverage) == 0 || len(configured.Material) == 0 {
		return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("required_coverage and material must not be empty")
	}
	required, err := exactNonBlankSet("required_coverage", configured.RequiredCoverage)
	if err != nil {
		return domain.Digest{}, orchestration.StaticRunPlan{}, err
	}
	if _, found := required[ports.TargetLifecycleSignal]; !found {
		return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("required_coverage must include intrinsic family %q", ports.TargetLifecycleSignal)
	}
	collectors := make([]ports.CollectorSpec, 0, len(configured.CollectorReferences))
	collectorNames := make(map[string]struct{}, len(configured.CollectorReferences))
	collectorFamilies := make(map[string]struct{}, len(configured.CollectorReferences))
	var aggregateObserverBytes int64
	for index, reference := range configured.CollectorReferences {
		if strings.TrimSpace(reference) == "" || strings.TrimSpace(reference) != reference {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references[%d] must be non-blank and trimmed", index)
		}
		if err := ports.ValidateCollectorName(reference); err != nil {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references[%d]: %w", index, err)
		}
		if _, duplicate := collectorNames[reference]; duplicate {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references contains duplicate %q", reference)
		}
		collectorNames[reference] = struct{}{}
		spec, found := observerSpecs[reference]
		if !found {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references[%d] names unknown observer %q", index, reference)
		}
		family := spec.Requirement.SignalFamily
		if _, duplicate := collectorFamilies[family]; duplicate {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references contains more than one observer for signal family %q", family)
		}
		collectorFamilies[family] = struct{}{}
		_, familyRequired := required[family]
		if spec.Requirement.Required != familyRequired {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector %q required flag does not agree with required_coverage", reference)
		}
		if aggregateObserverBytes > maximumObserverBytes-spec.MaximumBytes {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("collector_references aggregate maximum_bytes exceeds the daemon bundle limit")
		}
		aggregateObserverBytes += spec.MaximumBytes
		collectors = append(collectors, spec)
	}
	for family := range required {
		if family == ports.TargetLifecycleSignal {
			continue
		}
		if _, found := collectorFamilies[family]; !found {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("required coverage family %q has no configured collector", family)
		}
	}
	referenceSet, err := exactRunReferences(configured.SpecimenOccurrenceRefs, configured.FixtureRefs)
	if err != nil {
		return domain.Digest{}, orchestration.StaticRunPlan{}, err
	}
	material := make([]ports.TargetMaterialPlan, 0, len(configured.Material))
	materialReferences := make(map[string]struct{}, len(configured.Material))
	for index, item := range configured.Material {
		if err := requireRegularPermissionMode(item.Mode); err != nil {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material[%d] mode: %w", index, err)
		}
		if _, duplicate := materialReferences[item.Reference]; duplicate {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material contains duplicate reference %q", item.Reference)
		}
		materialReferences[item.Reference] = struct{}{}
		if _, selected := referenceSet[item.Reference]; !selected {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material[%d] reference is not selected as a specimen or fixture", index)
		}
		occurrence, err := authority.ResolveOccurrence(ctx, scope, item.Reference)
		if err != nil {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material[%d]: %w", index, err)
		}
		entry, err := authority.Entry(scope, item.Reference)
		if err != nil {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material[%d]: %w", index, err)
		}
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
			Reference: occurrence.Reference, Digest: occurrence.Digest, Size: occurrence.Size,
			Role: entry.Role, Sensitivity: entry.Sensitivity,
		})
		if err != nil {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("material[%d] artifact: %w", index, err)
		}
		material = append(material, ports.TargetMaterialPlan{
			Artifact: artifact, LogicalPath: item.LogicalPath, Mode: item.Mode,
			Content: authorityContentSource{authority: authority, occurrence: occurrence},
		})
	}
	if len(materialReferences) != len(referenceSet) {
		return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("every selected specimen and fixture must have exactly one material projection")
	}
	for reference := range referenceSet {
		if _, found := materialReferences[reference]; !found {
			return domain.Digest{}, orchestration.StaticRunPlan{}, fmt.Errorf("selected reference %q has no material projection", reference)
		}
	}
	digest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		return domain.Digest{}, orchestration.StaticRunPlan{}, err
	}
	return digest, orchestration.StaticRunPlan{
		SpecimenOccurrenceRefs: append([]string(nil), configured.SpecimenOccurrenceRefs...),
		FixtureRefs:            append([]string(nil), configured.FixtureRefs...),
		RequiredCoverage:       append([]string(nil), configured.RequiredCoverage...),
		Collectors:             cloneCollectorSpecsForProfile(collectors),
		Material:               material, MaximumDuration: duration,
	}, nil
}

func cloneCollectorSpecsForProfile(values []ports.CollectorSpec) []ports.CollectorSpec {
	result := make([]ports.CollectorSpec, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Resources = value.Resources.Clone()
	}
	return result
}

func requireRegularPermissionMode(mode uint32) error {
	if mode == 0 || mode&^uint32(0o777) != 0 {
		return fmt.Errorf("must contain only non-zero user/group/other permission bits (0001 through 0777)")
	}
	return nil
}

func exactRunReferences(specimens, fixtures []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(specimens)+len(fixtures))
	for _, group := range []struct {
		name   string
		values []string
	}{{"specimen_occurrence_refs", specimens}, {"fixture_refs", fixtures}} {
		if err := addUniqueNonBlank(group.name, group.values, result); err != nil {
			return nil, err
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("at least one specimen or fixture reference is required")
	}
	return result, nil
}

func exactNonBlankSet(name string, values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	if err := addUniqueNonBlank(name, values, result); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%s must not be empty", name)
	}
	return result, nil
}

func addUniqueNonBlank(name string, values []string, result map[string]struct{}) error {
	for index, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s[%d] must not be blank", name, index)
		}
		if _, duplicate := result[value]; duplicate {
			return fmt.Errorf("%s contains duplicate %q", name, value)
		}
		result[value] = struct{}{}
	}
	return nil
}

var repositoryPattern = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?$`)

func parsePinnedImage(value string) (pinnedImage, error) {
	if strings.TrimSpace(value) != value || strings.Count(value, "@") != 1 {
		return pinnedImage{}, fmt.Errorf("must be repository@sha256:<64 lower-case hex characters>")
	}
	repository, digestText, found := strings.Cut(value, "@")
	if !found || !repositoryPattern.MatchString(repository) || strings.Contains(repository, "..") {
		return pinnedImage{}, fmt.Errorf("repository %q is not a normalized Docker repository", repository)
	}
	digest, err := domain.ParseDigest(digestText)
	if err != nil {
		return pinnedImage{}, err
	}
	return pinnedImage{repository: repository, digest: digest, reference: repository + "@" + digest.String()}, nil
}

func requireAgentResources(resources admission.Resources) error {
	if err := resources.Validate(); err != nil {
		return err
	}
	if resources.CPUMilli <= 0 || resources.MemoryBytes <= 0 || resources.StorageBytes <= 0 ||
		resources.CaptureBytes <= 0 || resources.Inodes <= 0 || resources.PIDs <= 0 {
		return fmt.Errorf("cpu_milli, memory_bytes, storage_bytes, capture_bytes, inodes, and pids must all be positive")
	}
	if len(resources.Devices) != 0 {
		return fmt.Errorf("local Docker profiles do not authorize devices")
	}
	return nil
}

func requireTargetResources(resources admission.Resources) error {
	if err := resources.Validate(); err != nil {
		return err
	}
	if resources.CPUMilli <= 0 || resources.MemoryBytes <= 0 || resources.StorageBytes <= 0 || resources.PIDs <= 0 {
		return fmt.Errorf("cpu_milli, memory_bytes, storage_bytes, and pids must all be positive")
	}
	if resources.CaptureBytes != 0 || resources.Inodes != 0 || len(resources.Devices) != 0 {
		return fmt.Errorf("target capture_bytes, inodes, and devices must be zero because the Linux target driver does not enforce them")
	}
	return nil
}

func requireAndroidTargetResources(resources admission.Resources, guestMemoryBytes int64) error {
	if err := resources.Validate(); err != nil {
		return err
	}
	if err := cuttlefish.ValidateManagedEmulatorResources(resources, guestMemoryBytes); err != nil {
		return err
	}
	if resources.SwapBytes != 0 || resources.CaptureBytes != 0 || resources.Inodes != 0 || resources.PIDs != 0 || len(resources.Devices) != 0 {
		return fmt.Errorf("swap_bytes, capture_bytes, inodes, pids, and devices must be zero for Android virtual devices")
	}
	return nil
}

func requireAbsoluteManagedRoot(name, value string) error {
	if strings.TrimSpace(value) == "" || !filepath.IsAbs(value) {
		return fmt.Errorf("%s must be an absolute path", name)
	}
	cleaned := filepath.Clean(value)
	volume := filepath.VolumeName(cleaned)
	relative := strings.TrimPrefix(cleaned, volume)
	relative = strings.Trim(relative, string(filepath.Separator))
	if relative == "" {
		return fmt.Errorf("%s must not be a filesystem root", name)
	}
	return nil
}

func requireUniqueJSONKeys(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := consumeUniqueJSONValue(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("contains more than one JSON value")
		}
		return err
	}
	return nil
}

func consumeUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	expected := json.Delim('}')
	if delimiter == '[' {
		expected = ']'
	}
	if closing != expected {
		return fmt.Errorf("mismatched JSON delimiter")
	}
	return nil
}

var _ orchestration.ProvisioningResolver = (*targetBoundResolver)(nil)
