package policy

import (
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// Public aliases keep policy callers on the policy package while capability
// evaluation uses the control core's immutable domain implementation.
type CapabilityStatus = domain.CapabilityStatus
type Capability = domain.Capability
type CapabilityFingerprint = domain.CapabilityFingerprint
type Digest = domain.Digest

const (
	CapabilityUnknown     = domain.CapabilityUnknown
	CapabilityUnsupported = domain.CapabilityUnsupported
	CapabilitySupported   = domain.CapabilitySupported
)

func NewCapability(status CapabilityStatus, constraints, evidence map[string]string) (Capability, error) {
	return domain.NewCapability(status, constraints, evidence)
}

func NewCapabilityFingerprint(capabilities map[string]Capability, evidence map[string]string) (CapabilityFingerprint, error) {
	return domain.NewCapabilityFingerprint(capabilities, evidence)
}

func newDigest(data []byte) Digest { return domain.NewDigest(data) }

type RequirementLevel string

const (
	RequirementRequired  RequirementLevel = "required"
	RequirementPreferred RequirementLevel = "preferred"
)

// CapabilityRequirement is the policy-derived requirement and its source path.
type CapabilityRequirement struct {
	Name        string
	Level       RequirementLevel
	Path        string
	Constraints map[string]string
}

// CapabilityResolution records how a policy requirement was resolved.
type CapabilityResolution struct {
	Requirement CapabilityRequirement
	Status      CapabilityStatus
	Satisfied   bool
	Downgraded  bool
	Reason      string
}

// Warning is an explicit, client-visible effective-policy downgrade.
type Warning struct {
	Code       string
	Path       string
	Capability string
	Message    string
}

type requirementAccumulator struct {
	ordered []CapabilityRequirement
	index   map[string]int
}

func newRequirementAccumulator() *requirementAccumulator {
	return &requirementAccumulator{index: make(map[string]int)}
}

func (a *requirementAccumulator) add(name string, level RequirementLevel, path string) {
	if name == "" {
		return
	}
	if index, exists := a.index[name]; exists {
		if level == RequirementRequired && a.ordered[index].Level == RequirementPreferred {
			a.ordered[index].Level = level
			a.ordered[index].Path = path
		}
		return
	}
	a.index[name] = len(a.ordered)
	a.ordered = append(a.ordered, CapabilityRequirement{Name: name, Level: level, Path: path, Constraints: map[string]string{}})
}

func deriveCapabilityRequirements(policy *Policy) []CapabilityRequirement {
	requirements := newRequirementAccumulator()
	if policy.Spec.Workspace.Mode == "directory-copy-non-production" {
		// Host profile covers windows and darwin; linux production uses overlayfs.
		requirements.add("host.profile.directory-copy-non-production", RequirementRequired, "spec.workspace.mode")
		requirements.add("filesystem.directory-copy.non-production", RequirementRequired, "spec.workspace.mode")
	} else {
		requirements.add("node.os.linux", RequirementRequired, "spec.workspace.mode")
		requirements.add("filesystem.overlayfs", RequirementRequired, "spec.workspace.mode")
	}
	if policy.Spec.Workspace.InputView.Construction == "require-reflink" {
		requirements.add("filesystem.reflink", RequirementRequired, "spec.workspace.inputView.construction")
	} else if policy.Spec.Workspace.Mode != "directory-copy-non-production" {
		requirements.add("filesystem.reflink", RequirementPreferred, "spec.workspace.inputView.construction")
	}
	requirements.add("runtime.driver."+policy.Spec.AgentWorkspace.Runtime.Driver, RequirementRequired, "spec.agentWorkspace.runtime.driver")
	requirements.add("runtime.isolation."+policy.Spec.AgentWorkspace.Runtime.IsolationProfile, RequirementRequired, "spec.agentWorkspace.runtime.isolationProfile")

	coverage := policy.Spec.Observation.RequiredCoverage
	for index := range policy.Spec.Targets.Templates {
		template := &policy.Spec.Targets.Templates[index]
		base := fmt.Sprintf("spec.targets.templates[%d]", index)
		requirements.add("target.kind."+template.Kind, RequirementRequired, base+".kind")
		requirements.add("runtime.driver."+template.Runtime.Driver, RequirementRequired, base+".runtime.driver")
		requirements.add("runtime.isolation."+template.Runtime.IsolationProfile, RequirementRequired, base+".runtime.isolationProfile")
		if template.Kind == "linux-container" {
			requirements.add("runtime.oci."+template.Runtime.Runtime, RequirementRequired, base+".runtime.runtime")
		}
		if template.Kind == "android-virtual-device" && template.Runtime.RequireHardwareAcceleration {
			requirements.add("android.hardware-acceleration", RequirementRequired, base+".runtime.requireHardwareAcceleration")
		}
		for coverageIndex, name := range coverage[template.Kind] {
			requirements.add("coverage."+template.Kind+"."+name, RequirementRequired,
				fmt.Sprintf("spec.observation.requiredCoverage.%s[%d]", template.Kind, coverageIndex))
		}
	}

	collectors := &policy.Spec.Observation.Collectors
	linuxLevel := RequirementPreferred
	if collectors.LinuxMetadata.FailRunOnCoverageLoss {
		linuxLevel = RequirementRequired
	}
	requirements.add("collector.adapter."+collectors.LinuxMetadata.Adapter, linuxLevel, "spec.observation.collectors.linuxMetadata.adapter")
	androidLevel := RequirementPreferred
	if collectors.AndroidSystem.FailRunOnCoverageLoss {
		androidLevel = RequirementRequired
	}
	for index, adapter := range collectors.AndroidSystem.Adapters {
		requirements.add("collector.adapter."+adapter, androidLevel, fmt.Sprintf("spec.observation.collectors.androidSystem.adapters[%d]", index))
	}
	hooksLevel := RequirementPreferred
	if containsString(coverage["android-virtual-device"], "app-api-intents") {
		hooksLevel = RequirementRequired
	}
	requirements.add("collector.adapter."+collectors.AndroidAppHooks.Adapter, hooksLevel, "spec.observation.collectors.androidAppHooks.adapter")
	packetLevel := RequirementPreferred
	for _, targetCoverage := range coverage {
		if containsString(targetCoverage, "packet-ring") {
			packetLevel = RequirementRequired
			break
		}
	}
	requirements.add("collector.adapter."+collectors.PacketCapture.Adapter, packetLevel, "spec.observation.collectors.packetCapture.adapter")
	requirements.add("collector.adapter."+collectors.ProtocolSummary.Adapter, RequirementPreferred, "spec.observation.collectors.protocolSummary.adapter")
	requirements.add("collector.adapter."+collectors.MobileAnalysis.Adapter, RequirementPreferred, "spec.observation.collectors.mobileAnalysis.adapter")
	return cloneRequirements(requirements.ordered)
}

func evaluateCapabilities(fingerprint CapabilityFingerprint, requirements []CapabilityRequirement, positions map[string]sourcePosition) ([]CapabilityResolution, []Warning, error) {
	if fingerprint.Digest().IsZero() {
		return nil, nil, &ValidationError{Problems: []FieldError{{Path: "$capabilities", Message: "capability fingerprint must be initialized"}}}
	}
	domainRequirements := make([]domain.CapabilityRequirement, len(requirements))
	for index, requirement := range requirements {
		level := domain.RequirementPreferred
		if requirement.Level == RequirementRequired {
			level = domain.RequirementRequired
		}
		domainRequirements[index] = domain.CapabilityRequirement{Name: requirement.Name, Level: level, Constraints: cloneStringMap(requirement.Constraints)}
	}
	evaluation, err := domain.EvaluateCapabilityRequirements(fingerprint, domainRequirements)
	if err != nil {
		return nil, nil, fmt.Errorf("evaluate policy capabilities: %w", err)
	}
	domainResolutions := evaluation.Resolutions()
	resolutions := make([]CapabilityResolution, len(domainResolutions))
	warnings := make([]Warning, 0)
	failures := newValidationCollector(positions)
	for index, resolved := range domainResolutions {
		resolution := CapabilityResolution{
			Requirement: cloneRequirement(requirements[index]),
			Status:      resolved.Status(),
			Satisfied:   resolved.Satisfied(),
			Downgraded:  resolved.Downgraded(),
			Reason:      resolved.Reason(),
		}
		resolutions[index] = resolution
		if resolution.Downgraded {
			warnings = append(warnings, Warning{
				Code: "capability_downgrade", Path: resolution.Requirement.Path,
				Capability: resolution.Requirement.Name,
				Message:    fmt.Sprintf("preferred capability %s is %s: %s", resolution.Requirement.Name, resolution.Status, resolution.Reason),
			})
		} else if !resolution.Satisfied {
			failures.add(resolution.Requirement.Path, "required capability %q is %s: %s", resolution.Requirement.Name, resolution.Status, resolution.Reason)
		}
	}
	if err := failures.err(); err != nil {
		return nil, nil, err
	}
	return resolutions, warnings, nil
}

func cloneRequirement(input CapabilityRequirement) CapabilityRequirement {
	return CapabilityRequirement{Name: input.Name, Level: input.Level, Path: input.Path, Constraints: cloneStringMap(input.Constraints)}
}

func cloneRequirements(input []CapabilityRequirement) []CapabilityRequirement {
	output := make([]CapabilityRequirement, len(input))
	for index, requirement := range input {
		output[index] = cloneRequirement(requirement)
	}
	return output
}

func cloneResolutions(input []CapabilityResolution) []CapabilityResolution {
	output := make([]CapabilityResolution, len(input))
	for index, resolution := range input {
		output[index] = resolution
		output[index].Requirement = cloneRequirement(resolution.Requirement)
	}
	return output
}

func cloneWarnings(input []Warning) []Warning {
	output := make([]Warning, len(input))
	copy(output, input)
	return output
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if strings.EqualFold(value, expected) {
			return true
		}
	}
	return false
}
