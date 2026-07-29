package policyauthority

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/policy"
)

// RuntimeResources is the common resource vector understood by policy. Fields
// absent from a particular runtime remain zero.
type RuntimeResources struct {
	CPUMilli           int64
	MemoryBytes        int64
	SwapBytes          int64
	WorkspaceBytes     int64
	WritableStateBytes int64
	CaptureBytes       int64
	Inodes             int64
	PIDs               int64
}

type SessionAdmission struct {
	PolicyDigest     string
	CapabilityDigest string
	TTL              time.Duration
}

func ValidateSessionAcquisition(effective *policy.EffectivePolicy, request SessionAdmission) error {
	if err := ValidateIdentity(effective, request.PolicyDigest, request.CapabilityDigest); err != nil {
		return err
	}
	return ValidateTTL(effective, request.TTL)
}

func ValidateTTL(effective *policy.EffectivePolicy, requested time.Duration) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	return validatePositiveLimit("ttl", int64(requested), int64(document.Spec.Lease.TTL.Duration()))
}

type WorkspaceAdmission struct {
	Mode         string
	Construction string
	UpperBytes   int64
	UpperInodes  int64
}

func ValidateWorkspace(effective *policy.EffectivePolicy, request WorkspaceAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	configured := document.Spec.Workspace
	if request.Mode != configured.Mode {
		return deny("workspace.mode", "got %q, policy requires %q", request.Mode, configured.Mode)
	}
	if request.Construction != configured.InputView.Construction {
		return deny("workspace.construction", "got %q, policy requires %q", request.Construction, configured.InputView.Construction)
	}
	if err := validatePositiveLimit("workspace.upper_bytes", request.UpperBytes, document.Spec.AgentWorkspace.Resources.Limits.Workspace.Bytes()); err != nil {
		return err
	}
	return validatePositiveLimit("workspace.upper_inodes", request.UpperInodes, document.Spec.AgentWorkspace.Resources.Limits.WorkspaceInodes)
}

type ExportAdmission struct {
	DeclarationAuthority      string
	FileCount                 int64
	Bytes                     int64
	ContainsNonRegular        bool
	FinalPublication          bool
	RetainsFullChangeManifest bool
}

func ValidateExport(effective *policy.EffectivePolicy, request ExportAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limit := document.Spec.Workspace.Export
	switch limit.Declaration {
	case "host-only":
		if request.DeclarationAuthority != "host" {
			return deny("export.declaration", "policy permits host declaration only")
		}
	case "agent-or-host":
		if request.DeclarationAuthority != "agent" && request.DeclarationAuthority != "host" {
			return deny("export.declaration", "must be proven as agent or host declaration")
		}
	default:
		return deny("export.declaration", "policy contains an unsupported declaration authority")
	}
	if limit.RegularFilesOnly && request.ContainsNonRegular {
		return deny("export.files", "policy permits regular files only")
	}
	if request.FinalPublication && limit.RetainFullChangeManifest && !request.RetainsFullChangeManifest {
		return deny("export.change_manifest", "full change manifest retention is required")
	}
	if err := validateNonNegativeLimit("export.files", request.FileCount, limit.MaxFiles); err != nil {
		return err
	}
	return validateNonNegativeLimit("export.bytes", request.Bytes, limit.MaxBytes.Bytes())
}

type NetworkAdmission struct {
	Mode              string
	AllowDNS          bool
	AllowedCIDRs      []string
	AllowedDomains    []string
	DenyPrivateRanges bool
	TargetAccess      string
}

type AgentPlanAdmission struct {
	Runtime   AgentRuntimeAdmission
	Resources RuntimeResources
}

func ValidateAgentPlan(effective *policy.EffectivePolicy, request AgentPlanAdmission) error {
	if err := ValidateAgentRuntime(effective, request.Runtime); err != nil {
		return err
	}
	return ValidateAgentResources(effective, request.Resources)
}

// AgentRuntimeAdmission is the security-sensitive Docker configuration that
// must be derived from the effective policy rather than accepted as daemon
// passthrough.
type AgentRuntimeAdmission struct {
	Driver           string
	ImageDigest      string
	IsolationProfile string
	RootFilesystem   string
	User             string
	CapabilityDrop   []string
	CapabilityAdd    []string
	NoNewPrivileges  bool
	SeccompProfile   string
	UserEnforced     bool
	SeccompEnforced  bool
}

func ValidateAgentRuntime(effective *policy.EffectivePolicy, request AgentRuntimeAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	configured := document.Spec.AgentWorkspace.Runtime
	if !request.UserEnforced {
		return deny("agent.runtime.user", "the physical runtime does not enforce the configured user")
	}
	if !request.SeccompEnforced {
		return deny("agent.runtime.seccomp_profile", "the physical runtime does not enforce the configured seccomp profile")
	}
	for _, value := range []struct{ field, actual, expected string }{
		{"agent.runtime.driver", request.Driver, configured.Driver},
		{"agent.runtime.image_digest", request.ImageDigest, pinnedDigest(configured.Image)},
		{"agent.runtime.isolation_profile", request.IsolationProfile, configured.IsolationProfile},
		{"agent.runtime.root_filesystem", request.RootFilesystem, configured.RootFilesystem},
		{"agent.runtime.user", request.User, configured.User},
		{"agent.runtime.seccomp_profile", request.SeccompProfile, configured.SeccompProfile},
	} {
		if value.actual != value.expected {
			return deny(value.field, "got %q, policy requires %q", value.actual, value.expected)
		}
	}
	if configured.NoNewPrivileges && !request.NoNewPrivileges {
		return deny("agent.runtime.no_new_privileges", "policy requires no-new-privileges")
	}
	if err := requireSetContains("agent.runtime.capability_drop", request.CapabilityDrop, configured.Capabilities.Drop); err != nil {
		return err
	}
	return requireSetSubset("agent.runtime.capability_add", request.CapabilityAdd, configured.Capabilities.Add)
}

func ValidateNetwork(effective *policy.EffectivePolicy, request NetworkAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limit := document.Spec.AgentWorkspace.Network
	if request.Mode != limit.Mode {
		return deny("network.mode", "got %q, policy requires %q", request.Mode, limit.Mode)
	}
	if request.AllowDNS && !limit.AllowDNS {
		return deny("network.allow_dns", "DNS is denied by policy")
	}
	if limit.DenyPrivateRanges && !request.DenyPrivateRanges {
		return deny("network.deny_private_ranges", "policy requires private ranges to be denied")
	}
	if request.TargetAccess != limit.TargetAccess {
		return deny("network.target_access", "got %q, policy requires %q", request.TargetAccess, limit.TargetAccess)
	}
	if !isSubset(request.AllowedCIDRs, limit.AllowedCIDRs) {
		return deny("network.allowed_cidrs", "contains a CIDR not allowed by policy")
	}
	if !isSubsetFold(request.AllowedDomains, limit.AllowedDomains) {
		return deny("network.allowed_domains", "contains a domain not allowed by policy")
	}
	return nil
}

func ValidateAgentResources(effective *policy.EffectivePolicy, actual RuntimeResources) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limits := document.Spec.AgentWorkspace.Resources.Limits
	maximum := RuntimeResources{
		CPUMilli: limits.CPU.MilliCPU(), MemoryBytes: limits.Memory.Bytes(), SwapBytes: limits.Swap.Bytes(),
		WorkspaceBytes: limits.Workspace.Bytes(), CaptureBytes: document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes(),
		Inodes: limits.WorkspaceInodes, PIDs: limits.PIDs,
	}
	if err := validateEnforcedResources("agent.resources", actual, maximum); err != nil {
		return err
	}
	requests := document.Spec.AgentWorkspace.Resources.Requests
	for _, value := range []struct {
		field       string
		actual, min int64
	}{
		{"agent.resources.cpu_milli", actual.CPUMilli, requests.CPU.MilliCPU()},
		{"agent.resources.memory_bytes", actual.MemoryBytes, requests.Memory.Bytes()},
		{"agent.resources.workspace_bytes", actual.WorkspaceBytes, requests.Workspace.Bytes()},
	} {
		if value.actual < value.min {
			return deny(value.field, "%d is below policy request %d", value.actual, value.min)
		}
	}
	return nil
}

type TargetAdmission struct {
	Template                      string
	Kind                          string
	Driver                        string
	Runtime                       string
	ImageDigest                   string
	IsolationProfile              string
	BaseImage                     string
	User                          string
	CapabilityDrop                []string
	CapabilityAdd                 []string
	NoNewPrivileges               bool
	SeccompProfile                string
	UserEnforced                  bool
	SeccompEnforced               bool
	MaterialMountPoint            string
	WritableStateMode             string
	WritableStateEnforced         bool
	CommandAuthority              string
	ExecTransport                 string
	FileTransfer                  string
	NetworkEndpoints              string
	ADB                           string
	DeviceScopedADBServices       string
	DeniedInfrastructureAuthority []string
	ResetAfterEveryRun            bool
	ResetMode                     string
	BaselineState                 string
	RequireHardwareAcceleration   bool
	HardwareAccelerationEnforced  bool
	Headless                      bool
	Rooted                        bool
	Debuggable                    bool
	GuestMemoryBytes              int64
	BootTimeout                   time.Duration
	Resources                     RuntimeResources
	ConcurrentTargets             int64
}

func ValidateTarget(effective *policy.EffectivePolicy, request TargetAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	if request.ConcurrentTargets < 0 || request.ConcurrentTargets >= document.Spec.Targets.MaxConcurrent {
		return deny("targets.max_concurrent", "current count %d does not leave capacity below %d", request.ConcurrentTargets, document.Spec.Targets.MaxConcurrent)
	}
	template, found := targetTemplate(document, request.Template)
	if !found {
		return deny("target.template", "%q is not declared", request.Template)
	}
	if template.Kind == "linux-container" {
		if !request.UserEnforced {
			return deny("target.runtime.user", "the physical runtime does not enforce the configured user")
		}
		if !request.SeccompEnforced {
			return deny("target.runtime.seccomp_profile", "the physical runtime does not enforce the configured seccomp profile")
		}
	} else if template.Kind == "android-virtual-device" {
		if template.Runtime.RequireHardwareAcceleration && (!request.RequireHardwareAcceleration || !request.HardwareAccelerationEnforced) {
			return deny("target.runtime.require_hardware_acceleration", "the physical runtime does not prove hardware acceleration")
		}
		for _, value := range []struct {
			field            string
			actual, expected bool
		}{
			{"target.runtime.headless", request.Headless, template.Runtime.Headless},
			{"target.runtime.rooted", request.Rooted, template.Runtime.Rooted},
			{"target.runtime.debuggable", request.Debuggable, template.Runtime.Debuggable},
		} {
			if value.actual != value.expected {
				return deny(value.field, "got %t, policy requires %t", value.actual, value.expected)
			}
		}
		if request.BaselineState != template.Runtime.BaselineState {
			return deny("target.runtime.baseline_state", "got %q, policy requires %q", request.BaselineState, template.Runtime.BaselineState)
		}
		if request.GuestMemoryBytes != template.Runtime.GuestMemory.Bytes() {
			return deny("target.runtime.guest_memory", "got %d bytes, policy requires %d bytes", request.GuestMemoryBytes, template.Runtime.GuestMemory.Bytes())
		}
		if request.BootTimeout <= 0 || request.BootTimeout > template.Runtime.BootTimeout.Duration() {
			return deny("target.runtime.boot_timeout", "got %s, policy limit is %s", request.BootTimeout, template.Runtime.BootTimeout.Duration())
		}
		if request.ADB != template.Interaction.ADB || request.DeviceScopedADBServices != template.Interaction.DeviceScopedADBServices {
			return deny("target.interaction.adb", "physical ADB facts do not match the Android policy")
		}
	}
	if (template.Material.WritableState == "private-overlay" || template.Material.WritableState == "guest-data-partition") && !request.WritableStateEnforced {
		return deny("target.resources.writable_state_bytes", "the physical runtime does not enforce the configured writable-state limit")
	}
	image := template.Runtime.Image
	if template.Kind == "android-virtual-device" {
		image = template.Runtime.SystemImageDigest
	}
	for _, value := range []struct{ field, actual, expected string }{
		{"target.kind", normalizeTargetKind(request.Kind), template.Kind}, {"target.driver", request.Driver, template.Runtime.Driver},
		{"target.runtime", request.Runtime, template.Runtime.Runtime}, {"target.image_digest", request.ImageDigest, pinnedDigest(image)},
		{"target.isolation_profile", request.IsolationProfile, template.Runtime.IsolationProfile},
		{"target.runtime.base_image", request.BaseImage, template.Runtime.BaseImage},
		{"target.runtime.user", request.User, template.Runtime.User},
		{"target.runtime.seccomp_profile", request.SeccompProfile, template.Runtime.SeccompProfile},
		{"target.material.mount_point", request.MaterialMountPoint, template.Material.MountPoint},
		{"target.material.writable_state", request.WritableStateMode, template.Material.WritableState},
		{"target.interaction.command_authority", request.CommandAuthority, template.Interaction.CommandAuthority},
		{"target.interaction.exec_transport", request.ExecTransport, template.Interaction.ExecTransport},
		{"target.interaction.file_transfer", request.FileTransfer, template.Interaction.FileTransfer},
		{"target.interaction.network_endpoints", request.NetworkEndpoints, template.Interaction.NetworkEndpoints},
		{"target.reset.mode", request.ResetMode, template.Reset.Mode},
	} {
		if value.actual != value.expected {
			return deny(value.field, "got %q, policy requires %q", value.actual, value.expected)
		}
	}
	if template.Runtime.NoNewPrivileges && !request.NoNewPrivileges {
		return deny("target.runtime.no_new_privileges", "policy requires no-new-privileges")
	}
	if request.ResetAfterEveryRun != template.Reset.AfterEveryRun {
		return deny("target.reset.after_every_run", "got %t, policy requires %t", request.ResetAfterEveryRun, template.Reset.AfterEveryRun)
	}
	if err := requireSetContains("target.runtime.capability_drop", request.CapabilityDrop, template.Runtime.Capabilities.Drop); err != nil {
		return err
	}
	if err := requireSetSubset("target.runtime.capability_add", request.CapabilityAdd, template.Runtime.Capabilities.Add); err != nil {
		return err
	}
	if err := requireSetContains("target.interaction.denied_infrastructure_authority", request.DeniedInfrastructureAuthority, template.Interaction.DeniedInfrastructureAuthority); err != nil {
		return err
	}
	limits := template.Resources.Limits
	maximum := RuntimeResources{CPUMilli: limits.CPU.MilliCPU(), MemoryBytes: limits.Memory.Bytes(), SwapBytes: limits.Swap.Bytes(), WritableStateBytes: limits.WritableState.Bytes(), PIDs: limits.PIDs}
	return validateEnforcedResources("target.resources", request.Resources, maximum)
}

type TargetRunAdmission struct {
	Template         string
	MaterialBytes    int64
	MaximumDuration  time.Duration
	RequiredCoverage []string
	Collectors       []CollectorAdmission
}

type CollectorAdmission struct {
	Adapter      string
	Placement    string
	MaximumBytes int64
}

func ValidateTargetRun(effective *policy.EffectivePolicy, request TargetRunAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	template, found := targetTemplate(document, request.Template)
	if !found {
		return deny("target_run.template", "%q is not declared", request.Template)
	}
	if err := validateNonNegativeLimit("target_run.material_bytes", request.MaterialBytes, document.Spec.Targets.MaterialTransfer.MaxTransferBytesPerRun.Bytes()); err != nil {
		return err
	}
	if err := validatePositiveLimit("target_run.maximum_duration", int64(request.MaximumDuration), int64(document.Spec.Lease.TTL.Duration())); err != nil {
		return err
	}
	if err := ValidateRequiredCoverage(effective, template.Kind, request.RequiredCoverage); err != nil {
		return err
	}
	return validateCollectors(document, request.Collectors)
}

type CaptureAdmission struct {
	Name                   string
	SignalFamilies         []string
	Duration               time.Duration
	Bytes                  int64
	HasProcessOrPathFilter bool
	HasFlowFilter          bool
}

func ValidateCapture(effective *policy.EffectivePolicy, request CaptureAdmission) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limits, found := captureLimits(document, request.Name)
	if !found {
		return deny("capture.name", "%q is not allowed for agent requests", request.Name)
	}
	if _, err := stringSet(request.SignalFamilies); err != nil || len(request.SignalFamilies) == 0 {
		return deny("capture.signal_families", "must be a non-empty unique list: %v", err)
	}
	if !isSubset(request.SignalFamilies, limits.signalFamilies) {
		return deny("capture.signal_families", "contains a family outside profile %q", request.Name)
	}
	if err := validatePositiveLimit("capture.duration", int64(request.Duration), int64(limits.duration)); err != nil {
		return err
	}
	if limits.bytes == 0 || limits.bytes > document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes() {
		limits.bytes = document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes()
	}
	if err := validatePositiveLimit("capture.bytes", request.Bytes, limits.bytes); err != nil {
		return err
	}
	if limits.processFilter && !request.HasProcessOrPathFilter {
		return deny("capture.process_or_path_filter", "policy requires a process or path filter")
	}
	if limits.flowFilter && !request.HasFlowFilter {
		return deny("capture.flow_filter", "policy requires a flow filter")
	}
	return nil
}

func ValidateRequiredCoverage(effective *policy.EffectivePolicy, targetKind string, provided []string) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	targetKind = normalizeTargetKind(targetKind)
	required, declared := document.Spec.Observation.RequiredCoverage[targetKind]
	if !declared {
		return deny("required_coverage", "target kind %q has no coverage policy", targetKind)
	}
	providedSet, err := stringSet(provided)
	if err != nil {
		return deny("required_coverage", "%v", err)
	}
	for _, family := range required {
		if _, found := providedSet[family]; !found {
			return deny("required_coverage", "missing required family %q", family)
		}
	}
	return nil
}

func ValidateAggregateResources(effective *policy.EffectivePolicy, actual RuntimeResources) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limits := document.Spec.Resources.AggregateLimits
	maximum := RuntimeResources{CPUMilli: limits.CPU.MilliCPU(), MemoryBytes: limits.Memory.Bytes(), CaptureBytes: limits.CaptureBytes.Bytes()}
	return validateResources("resources.aggregate", actual, maximum)
}

// ValidateAggregatePlan is the physical-plan variant: a configured positive
// aggregate limit must be represented by a positive enforced plan value.
func ValidateAggregatePlan(effective *policy.EffectivePolicy, actual RuntimeResources) error {
	document, err := requirePolicy(effective)
	if err != nil {
		return err
	}
	limits := document.Spec.Resources.AggregateLimits
	maximum := RuntimeResources{CPUMilli: limits.CPU.MilliCPU(), MemoryBytes: limits.Memory.Bytes(), CaptureBytes: limits.CaptureBytes.Bytes()}
	return validateEnforcedResources("resources.aggregate", actual, maximum)
}

func targetTemplate(document policy.Policy, name string) (policy.TargetTemplate, bool) {
	for _, template := range document.Spec.Targets.Templates {
		if template.Name == name {
			return template, true
		}
	}
	return policy.TargetTemplate{}, false
}

func pinnedDigest(reference string) string {
	if separator := strings.LastIndexByte(reference, '@'); separator >= 0 {
		return reference[separator+1:]
	}
	return reference
}

func normalizeTargetKind(kind string) string {
	switch kind {
	case "linux_container":
		return "linux-container"
	case "android_virtual_device":
		return "android-virtual-device"
	default:
		return kind
	}
}

type capturePolicyLimits struct {
	duration       time.Duration
	bytes          int64
	processFilter  bool
	flowFilter     bool
	signalFamilies []string
}

func captureLimits(document policy.Policy, name string) (capturePolicyLimits, bool) {
	if !contains(document.Spec.Observation.AgentRequests.Profiles, name) && !contains(document.Spec.Observation.AgentRequests.NamedCaptures, name) {
		return capturePolicyLimits{}, false
	}
	switch name {
	case "worldLifecycle":
		value := document.Spec.Observation.AllowedOnDemand.WorldLifecycle
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), signalFamilies: value.SignalFamilies}, true
	case "deep":
		value := document.Spec.Observation.Profiles.Deep
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), signalFamilies: value.SignalFamilies}, true
	case "payload":
		value := document.Spec.Observation.Profiles.Payload
		if value.Enabled != "authorized-on-demand" {
			return capturePolicyLimits{}, false
		}
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), processFilter: value.RequireProcessOrPathFilter, signalFamilies: value.SignalFamilies}, true
	case "strace":
		value := document.Spec.Observation.AllowedOnDemand.Strace
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), signalFamilies: value.SignalFamilies}, true
	case "perfetto":
		value := document.Spec.Observation.AllowedOnDemand.Perfetto
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), signalFamilies: value.SignalFamilies}, true
	case "packetPayload":
		value := document.Spec.Observation.AllowedOnDemand.PacketPayload
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), flowFilter: value.RequireFlowFilter, signalFamilies: value.SignalFamilies}, true
	case "mitmproxy":
		value := document.Spec.Observation.AllowedOnDemand.Mitmproxy
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), bytes: value.MaxBytes.Bytes(), signalFamilies: value.SignalFamilies}, true
	case "frida":
		value := document.Spec.Observation.AllowedOnDemand.Frida
		return capturePolicyLimits{duration: value.MaxDuration.Duration(), signalFamilies: value.SignalFamilies}, true
	default:
		return capturePolicyLimits{}, false
	}
}

func validateResources(field string, actual, maximum RuntimeResources) error {
	for _, value := range resourceValues(actual, maximum) {
		if err := validateNonNegativeLimit(field+"."+value.name, value.actual, value.max); err != nil {
			return err
		}
	}
	return nil
}

// validateEnforcedResources rejects zero for every configured positive hard
// limit. Runtime drivers conventionally omit a cgroup/runtime limit when a
// value is zero, so accepting zero here would silently turn a bounded policy
// into an unbounded physical plan.
func validateEnforcedResources(field string, actual, maximum RuntimeResources) error {
	if err := validateResources(field, actual, maximum); err != nil {
		return err
	}
	for _, value := range resourceValues(actual, maximum) {
		if value.max > 0 && value.actual == 0 {
			return deny(field+"."+value.name, "must be positive so the policy limit is enforced")
		}
	}
	return nil
}

type resourceValue struct {
	name        string
	actual, max int64
}

func resourceValues(actual, maximum RuntimeResources) []resourceValue {
	return []resourceValue{
		{"cpu_milli", actual.CPUMilli, maximum.CPUMilli}, {"memory_bytes", actual.MemoryBytes, maximum.MemoryBytes},
		{"swap_bytes", actual.SwapBytes, maximum.SwapBytes}, {"workspace_bytes", actual.WorkspaceBytes, maximum.WorkspaceBytes},
		{"writable_state_bytes", actual.WritableStateBytes, maximum.WritableStateBytes},
		{"capture_bytes", actual.CaptureBytes, maximum.CaptureBytes}, {"inodes", actual.Inodes, maximum.Inodes},
		{"pids", actual.PIDs, maximum.PIDs},
	}
}

func validateCollectors(document policy.Policy, collectors []CollectorAdmission) error {
	allowed := map[string]string{
		document.Spec.Observation.Collectors.LinuxMetadata.Adapter:   document.Spec.Observation.Collectors.LinuxMetadata.Placement,
		document.Spec.Observation.Collectors.AndroidAppHooks.Adapter: document.Spec.Observation.Collectors.AndroidAppHooks.Placement,
		document.Spec.Observation.Collectors.PacketCapture.Adapter:   document.Spec.Observation.Collectors.PacketCapture.Placement,
		document.Spec.Observation.Collectors.ProtocolSummary.Adapter: "",
		document.Spec.Observation.Collectors.MobileAnalysis.Adapter:  "",
	}
	for _, adapter := range document.Spec.Observation.Collectors.AndroidSystem.Adapters {
		allowed[adapter] = document.Spec.Observation.Collectors.AndroidSystem.Placement
	}
	seen := make(map[string]struct{}, len(collectors))
	var totalCaptureBytes int64
	for index, collector := range collectors {
		if strings.TrimSpace(collector.Adapter) == "" {
			return deny(fmt.Sprintf("target_run.collectors[%d].adapter", index), "must not be blank")
		}
		if _, duplicate := seen[collector.Adapter]; duplicate {
			return deny("target_run.collectors", "adapter %q is duplicated", collector.Adapter)
		}
		seen[collector.Adapter] = struct{}{}
		placement, found := allowed[collector.Adapter]
		if !found {
			return deny(fmt.Sprintf("target_run.collectors[%d].adapter", index), "%q is not declared by policy", collector.Adapter)
		}
		if placement != "" && collector.Placement != placement {
			return deny(fmt.Sprintf("target_run.collectors[%d].placement", index), "got %q, policy requires %q", collector.Placement, placement)
		}
		if err := validatePositiveLimit(fmt.Sprintf("target_run.collectors[%d].maximum_bytes", index), collector.MaximumBytes, document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes()); err != nil {
			return err
		}
		if totalCaptureBytes > math.MaxInt64-collector.MaximumBytes {
			return deny("target_run.collectors.maximum_bytes", "aggregate overflows")
		}
		totalCaptureBytes += collector.MaximumBytes
	}
	return validateNonNegativeLimit("target_run.collectors.maximum_bytes", totalCaptureBytes, document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes())
}

func requireSetContains(field string, actual, required []string) error {
	actualSet, err := stringSet(actual)
	if err != nil {
		return deny(field, "%v", err)
	}
	if _, err := stringSet(required); err != nil {
		return deny("policy", "compiled policy contains an invalid %s set: %v", field, err)
	}
	for _, value := range required {
		if _, found := actualSet[value]; !found {
			return deny(field, "is missing policy-required value %q", value)
		}
	}
	return nil
}

func requireSetSubset(field string, actual, allowed []string) error {
	actualSet, err := stringSet(actual)
	if err != nil {
		return deny(field, "%v", err)
	}
	allowedSet, err := stringSet(allowed)
	if err != nil {
		return deny("policy", "compiled policy contains an invalid %s set: %v", field, err)
	}
	for value := range actualSet {
		if _, found := allowedSet[value]; !found {
			return deny(field, "contains policy-unauthorized value %q", value)
		}
	}
	return nil
}

func validatePositiveLimit(field string, actual, maximum int64) error {
	if actual <= 0 {
		return deny(field, "must be positive")
	}
	return validateAtMost(field, actual, maximum)
}

func validateNonNegativeLimit(field string, actual, maximum int64) error {
	if actual < 0 {
		return deny(field, "must not be negative")
	}
	return validateAtMost(field, actual, maximum)
}

func validateAtMost(field string, actual, maximum int64) error {
	if maximum < 0 || actual > maximum {
		return deny(field, "%d exceeds policy limit %d", actual, maximum)
	}
	return nil
}

func stringSet(values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for index, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("family %d is blank", index)
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("family %q is duplicated", value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func isSubset(values, allowed []string) bool {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, value := range allowed {
		allowedSet[value] = struct{}{}
	}
	for _, value := range values {
		if _, found := allowedSet[value]; !found {
			return false
		}
	}
	return true
}

func isSubsetFold(values, allowed []string) bool {
	normalized := append([]string(nil), allowed...)
	for index := range normalized {
		normalized[index] = strings.ToLower(normalized[index])
	}
	sort.Strings(normalized)
	for _, value := range values {
		candidate := strings.ToLower(value)
		if index := sort.SearchStrings(normalized, candidate); index == len(normalized) || normalized[index] != candidate {
			return false
		}
	}
	return true
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
