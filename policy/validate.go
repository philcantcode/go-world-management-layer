package policy

import (
	"fmt"
	"math"
	"net"
	pathpkg "path"
	"regexp"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

var (
	policyNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,61}[a-z0-9])?$`)
	userPattern       = regexp.MustCompile(`^[0-9]+:[0-9]+$`)
)

func validatePolicy(policy *Policy, positions map[string]sourcePosition) error {
	v := newValidationCollector(positions)
	v.equal("apiVersion", policy.APIVersion, APIVersion)
	v.equal("kind", policy.Kind, Kind)
	v.required("metadata.name", policy.Metadata.Name)
	if policy.Metadata.Name != "" && !policyNamePattern.MatchString(policy.Metadata.Name) {
		v.add("metadata.name", "must be a lowercase DNS-like name of at most 63 characters")
	}
	v.positiveInt("metadata.revision", policy.Metadata.Revision)
	validateLease(v, &policy.Spec.Lease)
	validateAgentWorkspace(v, &policy.Spec.AgentWorkspace)
	validateWorkspace(v, &policy.Spec.Workspace)
	validateTargets(v, &policy.Spec.Targets, &policy.Spec.Resources)
	validateAggregateResources(v, &policy.Spec.Resources, &policy.Spec.AgentWorkspace, &policy.Spec.Targets)
	validateObservation(v, &policy.Spec.Observation, &policy.Spec.Targets, &policy.Spec.Resources)
	validateIncidents(v, &policy.Spec.Incidents)
	validatePressure(v, &policy.Spec.Pressure, &policy.Spec.Resources)
	return v.err()
}

func validateLease(v *validationCollector, policy *LeasePolicy) {
	v.positiveDuration("spec.lease.ttl", policy.TTL)
	v.rangeInt("spec.lease.priority", int64(policy.Priority), 0, 100)
	v.positiveDuration("spec.lease.quiesceDeadline", policy.QuiesceDeadline)
	if policy.TTL > 0 && policy.QuiesceDeadline > policy.TTL {
		v.add("spec.lease.quiesceDeadline", "must not exceed spec.lease.ttl")
	}
}

func validateAgentWorkspace(v *validationCollector, policy *AgentWorkspacePolicy) {
	runtime := &policy.Runtime
	v.equal("spec.agentWorkspace.runtime.driver", runtime.Driver, "docker")
	v.pinnedImage("spec.agentWorkspace.runtime.image", runtime.Image)
	v.equal("spec.agentWorkspace.runtime.isolationProfile", runtime.IsolationProfile, "agent-standard")
	v.equal("spec.agentWorkspace.runtime.rootFilesystem", runtime.RootFilesystem, "readOnly")
	v.user("spec.agentWorkspace.runtime.user", runtime.User)
	v.linuxCapabilities("spec.agentWorkspace.runtime.capabilities", runtime.Capabilities)
	v.mustTrue("spec.agentWorkspace.runtime.noNewPrivileges", runtime.NoNewPrivileges)
	v.required("spec.agentWorkspace.runtime.seccompProfile", runtime.SeccompProfile)

	network := &policy.Network
	v.oneOf("spec.agentWorkspace.network.mode", network.Mode, "restricted-egress", "none")
	v.uniqueStrings("spec.agentWorkspace.network.allowedCIDRs", network.AllowedCIDRs, false)
	for index, cidr := range network.AllowedCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			v.add(fmt.Sprintf("spec.agentWorkspace.network.allowedCIDRs[%d]", index), "invalid CIDR %q", cidr)
		}
	}
	v.uniqueStrings("spec.agentWorkspace.network.allowedDomains", network.AllowedDomains, false)
	if network.Mode == "none" {
		v.equal("spec.agentWorkspace.network.targetAccess", network.TargetAccess, "none")
	} else {
		v.equal("spec.agentWorkspace.network.targetAccess", network.TargetAccess, "scoped-gateway-and-declared-endpoints")
	}

	requests, limits := &policy.Resources.Requests, &policy.Resources.Limits
	v.positiveCPU("spec.agentWorkspace.resources.requests.cpu", requests.CPU)
	v.positiveBytes("spec.agentWorkspace.resources.requests.memory", requests.Memory)
	v.positiveBytes("spec.agentWorkspace.resources.requests.workspace", requests.Workspace)
	v.positiveCPU("spec.agentWorkspace.resources.limits.cpu", limits.CPU)
	v.positiveBytes("spec.agentWorkspace.resources.limits.memory", limits.Memory)
	v.nonNegativeBytes("spec.agentWorkspace.resources.limits.swap", limits.Swap)
	v.positiveInt("spec.agentWorkspace.resources.limits.pids", limits.PIDs)
	v.positiveBytes("spec.agentWorkspace.resources.limits.workspace", limits.Workspace)
	v.positiveInt("spec.agentWorkspace.resources.limits.workspaceInodes", limits.WorkspaceInodes)
	v.requestWithinLimit("spec.agentWorkspace.resources.requests.cpu", int64(requests.CPU), "spec.agentWorkspace.resources.limits.cpu", int64(limits.CPU))
	v.requestWithinLimit("spec.agentWorkspace.resources.requests.memory", int64(requests.Memory), "spec.agentWorkspace.resources.limits.memory", int64(limits.Memory))
	v.requestWithinLimit("spec.agentWorkspace.resources.requests.workspace", int64(requests.Workspace), "spec.agentWorkspace.resources.limits.workspace", int64(limits.Workspace))
}

func validateWorkspace(v *validationCollector, policy *WorkspacePolicy) {
	v.oneOf("spec.workspace.mode", policy.Mode, "overlayfs", "directory-copy-non-production")
	input := &policy.InputView
	v.equal("spec.workspace.inputView.source", input.Source, "frozen-artifact-selection")
	v.required("spec.workspace.inputView.layout", input.Layout)
	v.required("spec.workspace.inputView.cacheScope", input.CacheScope)
	v.oneOf("spec.workspace.inputView.construction", input.Construction, "require-reflink", "allow-copy")
	if policy.Mode == "directory-copy-non-production" && input.Construction != "allow-copy" {
		v.add("spec.workspace.inputView.construction", "directory-copy-non-production requires allow-copy")
	}
	v.mustTrue("spec.workspace.inputView.reuseExactView", input.ReuseExactView)
	v.mustTrue("spec.workspace.inputView.verifyBeforeMount", input.VerifyBeforeMount)
	v.mustTrue("spec.workspace.inputView.verifyAfterRun", input.VerifyAfterRun)
	v.positiveDuration("spec.workspace.inputView.viewRetention", input.ViewRetention)

	cache := &policy.Cache
	v.positiveBytes("spec.workspace.cache.maxBytes", cache.MaxBytes)
	v.percentStrict("spec.workspace.cache.highWaterPercent", cache.HighWaterPercent)
	v.percentStrict("spec.workspace.cache.lowWaterPercent", cache.LowWaterPercent)
	if cache.HighWaterPercent > 0 && cache.LowWaterPercent >= cache.HighWaterPercent {
		v.add("spec.workspace.cache.lowWaterPercent", "must be less than spec.workspace.cache.highWaterPercent")
	}
	v.mustTrue("spec.workspace.cache.verifyOnPopulate", cache.VerifyOnPopulate)
	v.oneOf("spec.workspace.cache.reverify", cache.Reverify, "on-uncertain-integrity", "always")

	export := &policy.Export
	v.oneOf("spec.workspace.export.declaration", export.Declaration, "agent-or-host", "host-only")
	v.mustTrue("spec.workspace.export.regularFilesOnly", export.RegularFilesOnly)
	v.positiveInt("spec.workspace.export.maxFiles", export.MaxFiles)
	v.positiveBytes("spec.workspace.export.maxBytes", export.MaxBytes)
	v.mustTrue("spec.workspace.export.retainFullChangeManifest", export.RetainFullChangeManifest)
}

func validateTargets(v *validationCollector, policy *TargetsPolicy, resources *AggregateResources) {
	v.positiveInt("spec.targets.maxConcurrent", policy.MaxConcurrent)
	transfer := &policy.MaterialTransfer
	v.equal("spec.targets.materialTransfer.source", transfer.Source, "declared-artifact-occurrences")
	v.equal("spec.targets.materialTransfer.agentWorkspacePush", transfer.AgentWorkspacePush, "allowed-beneath-workspace-recorded")
	v.positiveBytes("spec.targets.materialTransfer.maxTransferBytesPerRun", transfer.MaxTransferBytesPerRun)
	v.mustTrue("spec.targets.materialTransfer.verifyBeforeRun", transfer.VerifyBeforeRun)
	if len(policy.Templates) == 0 {
		v.add("spec.targets.templates", "must contain at least one target template")
	}
	seenNames := make(map[string]struct{}, len(policy.Templates))
	for index := range policy.Templates {
		template := &policy.Templates[index]
		base := fmt.Sprintf("spec.targets.templates[%d]", index)
		v.required(base+".name", template.Name)
		if _, duplicate := seenNames[template.Name]; duplicate && template.Name != "" {
			v.add(base+".name", "duplicate target template name %q", template.Name)
		}
		seenNames[template.Name] = struct{}{}
		v.oneOf(base+".kind", template.Kind, "linux-container", "android-virtual-device")
		switch template.Kind {
		case "linux-container":
			validateLinuxTemplate(v, base, template)
		case "android-virtual-device":
			validateAndroidTemplate(v, base, template)
		}
		limits := &template.Resources.Limits
		v.positiveCPU(base+".resources.limits.cpu", limits.CPU)
		v.positiveBytes(base+".resources.limits.memory", limits.Memory)
		v.nonNegativeBytes(base+".resources.limits.swap", limits.Swap)
		v.positiveBytes(base+".resources.limits.writableState", limits.WritableState)
		if limits.CPU > resources.AggregateLimits.CPU && resources.AggregateLimits.CPU > 0 {
			v.add(base+".resources.limits.cpu", "must not exceed spec.resources.aggregateLimits.cpu")
		}
		if limits.Memory > resources.AggregateLimits.Memory && resources.AggregateLimits.Memory > 0 {
			v.add(base+".resources.limits.memory", "must not exceed spec.resources.aggregateLimits.memory")
		}
	}
}

func validateLinuxTemplate(v *validationCollector, base string, template *TargetTemplate) {
	runtime := &template.Runtime
	v.equal(base+".runtime.driver", runtime.Driver, "docker")
	v.oneOf(base+".runtime.runtime", runtime.Runtime, "runc", "gvisor", "kata")
	v.pinnedImage(base+".runtime.image", runtime.Image)
	if runtime.Runtime == "runc" {
		v.equal(base+".runtime.isolationProfile", runtime.IsolationProfile, "observable-container")
	} else {
		v.equal(base+".runtime.isolationProfile", runtime.IsolationProfile, "sandboxed-kernel")
	}
	v.equal(base+".runtime.baseImage", runtime.BaseImage, "readOnly")
	v.user(base+".runtime.user", runtime.User)
	v.linuxCapabilities(base+".runtime.capabilities", runtime.Capabilities)
	v.mustTrue(base+".runtime.noNewPrivileges", runtime.NoNewPrivileges)
	v.required(base+".runtime.seccompProfile", runtime.SeccompProfile)
	v.mustEmpty(base+".runtime.systemImageDigest", runtime.SystemImageDigest, "is an Android-only field")
	v.mustEmpty(base+".runtime.baselineState", runtime.BaselineState, "is an Android-only field")
	if runtime.RequireHardwareAcceleration || runtime.Rooted || runtime.Debuggable || runtime.Headless || runtime.BootTimeout != 0 {
		v.add(base+".runtime", "contains Android-only runtime fields")
	}
	v.absoluteGuestPath(base+".material.mountPoint", template.Material.MountPoint)
	v.oneOf(base+".material.writableState", template.Material.WritableState, "private-overlay", "private-directory-non-production")

	interaction := &template.Interaction
	v.equal(base+".interaction.commandAuthority", interaction.CommandAuthority, "arbitrary-inside-assigned-target")
	v.equal(base+".interaction.execTransport", interaction.ExecTransport, "direct-argv-and-explicit-shell")
	v.equal(base+".interaction.fileTransfer", interaction.FileTransfer, "push-pull-target-relative")
	v.oneOf(base+".interaction.networkEndpoints", interaction.NetworkEndpoints, "policy-declared", "none")
	v.mustEmpty(base+".interaction.adb", interaction.ADB, "is an Android-only field")
	v.mustEmpty(base+".interaction.deviceScopedADBServices", interaction.DeviceScopedADBServices, "is an Android-only field")
	v.requiredDenials(base+".interaction.deniedInfrastructureAuthority", interaction.DeniedInfrastructureAuthority,
		"host-exec", "docker-api", "host-mounts", "other-targets")
	v.positiveInt(base+".resources.limits.pids", template.Resources.Limits.PIDs)
	v.oneOf(base+".reset.mode", template.Reset.Mode, "recreate-new-target-generation")
}

func validateAndroidTemplate(v *validationCollector, base string, template *TargetTemplate) {
	runtime := &template.Runtime
	v.oneOf(base+".runtime.driver", runtime.Driver, "cuttlefish", "android-emulator")
	v.digestReference(base+".runtime.systemImageDigest", runtime.SystemImageDigest)
	v.equal(base+".runtime.isolationProfile", runtime.IsolationProfile, "instrumented-android")
	v.required(base+".runtime.baselineState", runtime.BaselineState)
	v.mustTrue(base+".runtime.requireHardwareAcceleration", runtime.RequireHardwareAcceleration)
	v.mustTrue(base+".runtime.headless", runtime.Headless)
	v.mustTrue(base+".runtime.rooted", runtime.Rooted)
	v.mustTrue(base+".runtime.debuggable", runtime.Debuggable)
	v.positiveDuration(base+".runtime.bootTimeout", runtime.BootTimeout)
	if runtime.Runtime != "" || runtime.Image != "" || runtime.BaseImage != "" || runtime.User != "" || len(runtime.Capabilities.Drop) != 0 || len(runtime.Capabilities.Add) != 0 || runtime.NoNewPrivileges || runtime.SeccompProfile != "" {
		v.add(base+".runtime", "contains Linux-container-only runtime fields")
	}
	if template.Material.MountPoint != "" || template.Material.WritableState != "" {
		v.add(base+".material", "must be omitted for an Android virtual device")
	}
	interaction := &template.Interaction
	v.equal(base+".interaction.commandAuthority", interaction.CommandAuthority, "arbitrary-inside-assigned-device")
	v.equal(base+".interaction.adb", interaction.ADB, "scoped-gateway")
	v.equal(base+".interaction.deviceScopedADBServices", interaction.DeviceScopedADBServices, "arbitrary")
	v.equal(base+".interaction.fileTransfer", interaction.FileTransfer, "adb-sync-and-scoped-stream")
	v.mustEmpty(base+".interaction.execTransport", interaction.ExecTransport, "is a Linux-container-only field")
	v.mustEmpty(base+".interaction.networkEndpoints", interaction.NetworkEndpoints, "is a Linux-container-only field")
	v.requiredDenials(base+".interaction.deniedInfrastructureAuthority", interaction.DeniedInfrastructureAuthority,
		"host-adb-server-control", "other-serials", "raw-usb", "host-exec")
	if template.Resources.Limits.PIDs != 0 || template.Resources.Limits.Swap != 0 {
		v.add(base+".resources.limits", "pids and swap are Linux-container-only limits")
	}
	v.oneOf(base+".reset.mode", template.Reset.Mode, "baseline-new-target-generation")
}

func validateAggregateResources(v *validationCollector, policy *AggregateResources, agent *AgentWorkspacePolicy, targets *TargetsPolicy) {
	limits := &policy.AggregateLimits
	v.positiveCPU("spec.resources.aggregateLimits.cpu", limits.CPU)
	v.positiveBytes("spec.resources.aggregateLimits.memory", limits.Memory)
	v.positiveBytes("spec.resources.aggregateLimits.captureBytes", limits.CaptureBytes)
	if agent.Resources.Limits.CPU > limits.CPU && limits.CPU > 0 {
		v.add("spec.agentWorkspace.resources.limits.cpu", "must not exceed spec.resources.aggregateLimits.cpu")
	}
	if agent.Resources.Limits.Memory > limits.Memory && limits.Memory > 0 {
		v.add("spec.agentWorkspace.resources.limits.memory", "must not exceed spec.resources.aggregateLimits.memory")
	}
	if targets.MaterialTransfer.MaxTransferBytesPerRun > agent.Resources.Limits.Workspace && agent.Resources.Limits.Workspace > 0 {
		v.add("spec.targets.materialTransfer.maxTransferBytesPerRun", "must not exceed the agent workspace byte limit")
	}
	var largestTargetCPU CPUQuantity
	var largestTargetMemory ByteQuantity
	for _, template := range targets.Templates {
		if template.Resources.Limits.CPU > largestTargetCPU {
			largestTargetCPU = template.Resources.Limits.CPU
		}
		if template.Resources.Limits.Memory > largestTargetMemory {
			largestTargetMemory = template.Resources.Limits.Memory
		}
	}
	if aggregate, overflow := multiplyAdd(int64(agent.Resources.Limits.CPU), int64(largestTargetCPU), targets.MaxConcurrent); overflow || aggregate > int64(limits.CPU) {
		v.add("spec.resources.aggregateLimits.cpu", "must cover the agent limit plus maxConcurrent instances of the largest target CPU limit")
	}
	if aggregate, overflow := multiplyAdd(int64(agent.Resources.Limits.Memory), int64(largestTargetMemory), targets.MaxConcurrent); overflow || aggregate > int64(limits.Memory) {
		v.add("spec.resources.aggregateLimits.memory", "must cover the agent limit plus maxConcurrent instances of the largest target memory limit")
	}
}

func multiplyAdd(base, perItem, count int64) (int64, bool) {
	if base < 0 || perItem < 0 || count < 0 || (perItem != 0 && count > (math.MaxInt64-base)/perItem) {
		return 0, true
	}
	return base + perItem*count, false
}

func validateObservation(v *validationCollector, policy *ObservationPolicy, targets *TargetsPolicy, resources *AggregateResources) {
	v.equal("spec.observation.priority", policy.Priority, "visibility-first")
	validateCoverage(v, policy.RequiredCoverage, targets)
	profiles := &policy.Profiles
	v.equal("spec.observation.profiles.default", profiles.Default, "metadata")
	if profiles.Metadata.CapturePayloads {
		v.add("spec.observation.profiles.metadata.capturePayloads", "metadata profile must not capture payloads")
	}
	v.uniqueStrings("spec.observation.profiles.metadata.fileEvents", profiles.Metadata.FileEvents, true)
	v.required("spec.observation.profiles.metadata.syscallArguments", profiles.Metadata.SyscallArguments)
	v.boundedProfile("spec.observation.profiles.deep", profiles.Deep, resources.AggregateLimits.CaptureBytes)
	v.oneOf("spec.observation.profiles.payload.enabled", profiles.Payload.Enabled, "authorized-on-demand", "disabled")
	if profiles.Payload.Enabled != "disabled" {
		v.mustTrue("spec.observation.profiles.payload.requireProcessOrPathFilter", profiles.Payload.RequireProcessOrPathFilter)
		v.positiveDuration("spec.observation.profiles.payload.maxDuration", profiles.Payload.MaxDuration)
		v.positiveBytes("spec.observation.profiles.payload.maxBytes", profiles.Payload.MaxBytes)
		v.required("spec.observation.profiles.payload.sensitivity", profiles.Payload.Sensitivity)
		v.withinCaptureLimit("spec.observation.profiles.payload.maxBytes", profiles.Payload.MaxBytes, resources.AggregateLimits.CaptureBytes)
	}
	v.uniqueStrings("spec.observation.profiles.payload.signalFamilies", profiles.Payload.SignalFamilies, true)
	v.uniqueStrings("spec.observation.baseline", policy.Baseline, true)
	v.requireMembers("spec.observation.baseline", policy.Baseline, "world-lifecycle", "cgroup-metrics", "collector-health")
	validateCollectors(v, &policy.Collectors, targets, resources.AggregateLimits.CaptureBytes)
	validateMetrics(v, &policy.Metrics)
	validateLiveAccess(v, &policy.LiveAccess, &policy.Metrics)
	validateAgentRequests(v, &policy.AgentRequests)
	v.positiveInt("spec.observation.buffers.liveSubscriberEvents", policy.Buffers.LiveSubscriberEvents)
	v.positiveBytes("spec.observation.buffers.segmentBytes", policy.Buffers.SegmentBytes)
	v.positiveBytes("spec.observation.buffers.packetRingBytes", policy.Buffers.PacketRingBytes)
	v.withinCaptureLimit("spec.observation.buffers.packetRingBytes", policy.Buffers.PacketRingBytes, resources.AggregateLimits.CaptureBytes)
	validateOnDemand(v, &policy.AllowedOnDemand, resources.AggregateLimits.CaptureBytes)
	validateTriggers(v, policy.Triggers)
	validateBundles(v, &policy.Bundles)
	v.required("spec.observation.sensitivity.default", policy.Sensitivity.Default)
	v.required("spec.observation.sensitivity.decryptedTraffic", policy.Sensitivity.DecryptedTraffic)
	v.required("spec.observation.sensitivity.screenshots", policy.Sensitivity.Screenshots)
	v.required("spec.observation.sensitivity.payloads", policy.Sensitivity.Payloads)
	v.positiveDuration("spec.observation.retention.liveLocal", policy.Retention.LiveLocal)
	if policy.Retention.DeleteLocalAfterArtifactAck && !policy.Retention.FinalizeToArtifactStore {
		v.add("spec.observation.retention.deleteLocalAfterArtifactAck", "requires finalizeToArtifactStore")
	}
}

func validateCoverage(v *validationCollector, coverage map[string][]string, targets *TargetsPolicy) {
	targetKinds := make(map[string]struct{})
	for _, template := range targets.Templates {
		targetKinds[template.Kind] = struct{}{}
	}
	keys := sortedStringKeys(coverage)
	for _, kind := range keys {
		path := "spec.observation.requiredCoverage." + kind
		if _, configured := targetKinds[kind]; !configured {
			v.add(path, "coverage is declared for an unconfigured target kind")
		}
		v.uniqueStrings(path, coverage[kind], true)
	}
	for _, kind := range []string{"linux-container", "android-virtual-device"} {
		if _, configured := targetKinds[kind]; configured && len(coverage[kind]) == 0 {
			v.add("spec.observation.requiredCoverage."+kind, "must declare coverage for configured target kind %q", kind)
		}
	}
}

func validateCollectors(v *validationCollector, policy *CollectorPolicies, targets *TargetsPolicy, captureLimit ByteQuantity) {
	kinds := make(map[string]bool)
	for _, template := range targets.Templates {
		kinds[template.Kind] = true
	}
	if kinds["linux-container"] {
		v.required("spec.observation.collectors.linuxMetadata.adapter", policy.LinuxMetadata.Adapter)
		v.equal("spec.observation.collectors.linuxMetadata.placement", policy.LinuxMetadata.Placement, "host")
	}
	if kinds["android-virtual-device"] {
		v.uniqueStrings("spec.observation.collectors.androidSystem.adapters", policy.AndroidSystem.Adapters, true)
		v.equal("spec.observation.collectors.androidSystem.placement", policy.AndroidSystem.Placement, "guest")
		v.mustTrue("spec.observation.collectors.androidSystem.failRunOnCoverageLoss", policy.AndroidSystem.FailRunOnCoverageLoss)
		v.required("spec.observation.collectors.androidAppHooks.adapter", policy.AndroidAppHooks.Adapter)
		v.equal("spec.observation.collectors.androidAppHooks.placement", policy.AndroidAppHooks.Placement, "injected-app")
		v.required("spec.observation.collectors.androidAppHooks.coverage", policy.AndroidAppHooks.Coverage)
	}
	v.required("spec.observation.collectors.packetCapture.adapter", policy.PacketCapture.Adapter)
	v.equal("spec.observation.collectors.packetCapture.placement", policy.PacketCapture.Placement, "host-network-namespace")
	v.positiveBytes("spec.observation.collectors.packetCapture.ringBytes", policy.PacketCapture.RingBytes)
	v.withinCaptureLimit("spec.observation.collectors.packetCapture.ringBytes", policy.PacketCapture.RingBytes, captureLimit)
	v.required("spec.observation.collectors.protocolSummary.adapter", policy.ProtocolSummary.Adapter)
	v.required("spec.observation.collectors.protocolSummary.mode", policy.ProtocolSummary.Mode)
	v.required("spec.observation.collectors.mobileAnalysis.adapter", policy.MobileAnalysis.Adapter)
	v.required("spec.observation.collectors.mobileAnalysis.mode", policy.MobileAnalysis.Mode)
}

func validateMetrics(v *validationCollector, policy *MetricsPolicy) {
	v.positiveDuration("spec.observation.metrics.normalInterval", policy.NormalInterval)
	v.positiveDuration("spec.observation.metrics.incidentInterval", policy.IncidentInterval)
	v.positiveDuration("spec.observation.metrics.staleAfter", policy.StaleAfter)
	if policy.IncidentInterval > policy.NormalInterval && policy.NormalInterval > 0 {
		v.add("spec.observation.metrics.incidentInterval", "must not exceed normalInterval")
	}
	if policy.StaleAfter <= policy.NormalInterval && policy.NormalInterval > 0 {
		v.add("spec.observation.metrics.staleAfter", "must be greater than normalInterval")
	}
	for index, window := range policy.RateWindows {
		path := fmt.Sprintf("spec.observation.metrics.rateWindows[%d]", index)
		v.positiveDuration(path, window)
		if index > 0 && window <= policy.RateWindows[index-1] {
			v.add(path, "rate windows must be strictly increasing")
		}
	}
	if len(policy.RateWindows) == 0 {
		v.add("spec.observation.metrics.rateWindows", "must not be empty")
	}
	v.uniqueStrings("spec.observation.metrics.dimensions", policy.Dimensions, true)
}

func validateLiveAccess(v *validationCollector, policy *LiveAccessPolicy, metrics *MetricsPolicy) {
	if policy.Agent.Enabled {
		v.positiveDuration("spec.observation.liveAccess.agent.minimumMetricInterval", policy.Agent.MinimumMetricInterval)
		v.uniqueStrings("spec.observation.liveAccess.agent.signals", policy.Agent.Signals, true)
		if policy.Agent.MinimumMetricInterval > metrics.NormalInterval && metrics.NormalInterval > 0 {
			v.add("spec.observation.liveAccess.agent.minimumMetricInterval", "must not exceed normal metric interval")
		}
	}
	v.required("spec.observation.liveAccess.visual.screenshots", policy.Visual.Screenshots)
	if math.IsNaN(policy.Visual.LiveScreenMaxFPS) || math.IsInf(policy.Visual.LiveScreenMaxFPS, 0) || policy.Visual.LiveScreenMaxFPS < 0 || policy.Visual.LiveScreenMaxFPS > 30 {
		v.add("spec.observation.liveAccess.visual.liveScreenMaxFps", "must be a finite value between 0 and 30")
	}
}

func validateAgentRequests(v *validationCollector, policy *AgentRequestsPolicy) {
	v.uniqueStrings("spec.observation.agentRequests.profiles", policy.Profiles, false)
	for index, profile := range policy.Profiles {
		if profile != "deep" && profile != "payload" {
			v.add(fmt.Sprintf("spec.observation.agentRequests.profiles[%d]", index), "unknown requestable profile %q", profile)
		}
	}
	v.uniqueStrings("spec.observation.agentRequests.namedCaptures", policy.NamedCaptures, false)
	allowed := stringSet("worldLifecycle", "strace", "perfetto", "packetPayload", "mitmproxy", "frida")
	for index, capture := range policy.NamedCaptures {
		if _, ok := allowed[capture]; !ok {
			v.add(fmt.Sprintf("spec.observation.agentRequests.namedCaptures[%d]", index), "unknown named capture %q", capture)
		}
	}
}

func validateOnDemand(v *validationCollector, policy *AllowedOnDemandPolicy, limit ByteQuantity) {
	v.boundedProfile("spec.observation.allowedOnDemand.worldLifecycle", policy.WorldLifecycle, limit)
	v.boundedProfile("spec.observation.allowedOnDemand.strace", policy.Strace, limit)
	v.boundedProfile("spec.observation.allowedOnDemand.perfetto", policy.Perfetto, limit)
	v.positiveDuration("spec.observation.allowedOnDemand.packetPayload.maxDuration", policy.PacketPayload.MaxDuration)
	v.positiveBytes("spec.observation.allowedOnDemand.packetPayload.maxBytes", policy.PacketPayload.MaxBytes)
	v.withinCaptureLimit("spec.observation.allowedOnDemand.packetPayload.maxBytes", policy.PacketPayload.MaxBytes, limit)
	v.mustTrue("spec.observation.allowedOnDemand.packetPayload.requireFlowFilter", policy.PacketPayload.RequireFlowFilter)
	v.uniqueStrings("spec.observation.allowedOnDemand.packetPayload.signalFamilies", policy.PacketPayload.SignalFamilies, true)
	v.positiveDuration("spec.observation.allowedOnDemand.mitmproxy.maxDuration", policy.Mitmproxy.MaxDuration)
	v.positiveBytes("spec.observation.allowedOnDemand.mitmproxy.maxBytes", policy.Mitmproxy.MaxBytes)
	v.withinCaptureLimit("spec.observation.allowedOnDemand.mitmproxy.maxBytes", policy.Mitmproxy.MaxBytes, limit)
	v.required("spec.observation.allowedOnDemand.mitmproxy.decryptedPayloads", policy.Mitmproxy.DecryptedPayloads)
	v.uniqueStrings("spec.observation.allowedOnDemand.mitmproxy.signalFamilies", policy.Mitmproxy.SignalFamilies, true)
	v.positiveDuration("spec.observation.allowedOnDemand.frida.maxDuration", policy.Frida.MaxDuration)
	v.equal("spec.observation.allowedOnDemand.frida.hostManagedCollectorScripts", policy.Frida.HostManagedCollectorScripts, "pinned")
	v.equal("spec.observation.allowedOnDemand.frida.agentInstalledInstrumentation", policy.Frida.AgentInstalledInstrumentation, "allowed-recorded-not-coverage-authority")
	v.uniqueStrings("spec.observation.allowedOnDemand.frida.signalFamilies", policy.Frida.SignalFamilies, true)
}

func validateTriggers(v *validationCollector, triggers []ObservationTrigger) {
	seen := make(map[string]struct{}, len(triggers))
	for index := range triggers {
		trigger := &triggers[index]
		base := fmt.Sprintf("spec.observation.triggers[%d]", index)
		v.required(base+".name", trigger.Name)
		if _, duplicate := seen[trigger.Name]; duplicate && trigger.Name != "" {
			v.add(base+".name", "duplicate trigger name %q", trigger.Name)
		}
		seen[trigger.Name] = struct{}{}
		hasMetric, hasEvent := trigger.When.Metric != "", trigger.When.Event != ""
		if hasMetric == hasEvent {
			v.add(base+".when", "must set exactly one of metric or event")
		}
		if hasMetric {
			if trigger.When.Above == nil || math.IsNaN(valueOrNaN(trigger.When.Above)) || math.IsInf(valueOrNaN(trigger.When.Above), 0) {
				v.add(base+".when.above", "must be a finite threshold for a metric trigger")
			}
			v.positiveDuration(base+".when.for", trigger.When.For)
		} else if trigger.When.Above != nil || trigger.When.For != 0 {
			v.add(base+".when", "event trigger must not set above or for")
		}
		v.uniqueStrings(base+".actions", trigger.Actions, true)
	}
}

func validateBundles(v *validationCollector, policy *BundlePolicy) {
	fields := []struct {
		path  string
		value bool
	}{
		{"rawCollectorOutputs", policy.RawCollectorOutputs},
		{"normalizedEvents", policy.NormalizedEvents},
		{"targetChangeManifest", policy.TargetChangeManifest},
		{"collectorCoverageAndGaps", policy.CollectorCoverageAndGaps},
		{"derivedAgentSummary", policy.DerivedAgentSummary},
		{"summaryMustCiteEvidence", policy.SummaryMustCiteEvidence},
	}
	for _, field := range fields {
		v.mustTrue("spec.observation.bundles."+field.path, field.value)
	}
}

func validateIncidents(v *validationCollector, policy *IncidentsPolicy) {
	v.uniqueStrings("spec.incidents.minimumEvidence", policy.MinimumEvidence, true)
	v.requireMembers("spec.incidents.minimumEvidence", policy.MinimumEvidence,
		"lifecycle-window", "resource-window", "process-tree", "target-change-manifest", "collector-coverage-and-gaps")
	recovery := &policy.Recovery
	v.equal("spec.incidents.recovery.agentWorkspaceFailure", recovery.AgentWorkspaceFailure, "recreate-new-agent-generation")
	v.equal("spec.incidents.recovery.linuxTargetFailure", recovery.LinuxTargetFailure, "recreate-new-target-generation")
	v.equal("spec.incidents.recovery.androidRuntimeFailure", recovery.AndroidRuntimeFailure, "baseline-new-target-generation")
	v.equal("spec.incidents.recovery.targetWorkloadExit", recovery.TargetWorkloadExit, "finalize-run-and-report")
	v.equal("spec.incidents.recovery.androidAppCrash", recovery.AndroidAppCrash, "finalize-run-and-report")
	v.equal("spec.incidents.recovery.observerFailure", recovery.ObserverFailure, "fail-run-if-required")
}

func validatePressure(v *validationCollector, policy *PressurePolicy, resources *AggregateResources) {
	v.positiveBytes("spec.pressure.reserveHostMemory", policy.ReserveHostMemory)
	if policy.ReserveHostMemory >= resources.AggregateLimits.Memory && resources.AggregateLimits.Memory > 0 {
		v.add("spec.pressure.reserveHostMemory", "must be less than aggregate memory limit")
	}
	if policy.StopAdmissionOn.MemoryPSIFull <= 0 {
		v.add("spec.pressure.stopAdmissionOn.memoryPSIFull", "must be greater than zero")
	}
	if policy.StopAdmissionOn.IOPSIFull <= 0 {
		v.add("spec.pressure.stopAdmissionOn.ioPSIFull", "must be greater than zero")
	}
	v.percentStrict("spec.pressure.stopAdmissionOn.freeDiskPercent", policy.StopAdmissionOn.FreeDiskPercent)
	expected := []string{"unused-reservations", "unleased-warm-targets", "idle-preemptible-targets", "active-preemptible-target-runs", "preemptible-agent-workspaces"}
	if len(policy.ShedOrder) != len(expected) {
		v.add("spec.pressure.shedOrder", "must contain the mandated five-stage shed order")
		return
	}
	for index, value := range policy.ShedOrder {
		if value != expected[index] {
			v.add(fmt.Sprintf("spec.pressure.shedOrder[%d]", index), "must be %q", expected[index])
		}
	}
}

// Shared validation helpers.

func (v *validationCollector) required(path, value string) {
	if strings.TrimSpace(value) == "" {
		v.add(path, "must not be empty")
	}
}

func (v *validationCollector) equal(path, actual, expected string) {
	if actual != expected {
		v.add(path, "must be %q, got %q", expected, actual)
	}
}

func (v *validationCollector) oneOf(path, actual string, expected ...string) {
	for _, candidate := range expected {
		if actual == candidate {
			return
		}
	}
	v.add(path, "must be one of %s, got %q", strings.Join(expected, ", "), actual)
}

func (v *validationCollector) mustTrue(path string, value bool) {
	if !value {
		v.add(path, "must be true")
	}
}

func (v *validationCollector) mustEmpty(path, value, reason string) {
	if value != "" {
		v.add(path, reason)
	}
}

func (v *validationCollector) positiveInt(path string, value int64) {
	if value <= 0 {
		v.add(path, "must be greater than zero")
	}
}

func (v *validationCollector) rangeInt(path string, value, minimum, maximum int64) {
	if value < minimum || value > maximum {
		v.add(path, "must be between %d and %d", minimum, maximum)
	}
}

func (v *validationCollector) positiveCPU(path string, value CPUQuantity) {
	if value <= 0 {
		v.add(path, "must be greater than zero")
	}
}

func (v *validationCollector) positiveBytes(path string, value ByteQuantity) {
	if value <= 0 {
		v.add(path, "must be greater than zero")
	}
}

func (v *validationCollector) nonNegativeBytes(path string, value ByteQuantity) {
	if value < 0 {
		v.add(path, "must not be negative")
	}
}

func (v *validationCollector) positiveDuration(path string, value Duration) {
	if value <= 0 {
		v.add(path, "must be greater than zero")
	}
}

func (v *validationCollector) percentStrict(path string, value Percent) {
	if value <= 0 || value >= 100 {
		v.add(path, "must be greater than 0 and less than 100")
	}
}

func (v *validationCollector) requestWithinLimit(requestPath string, request int64, limitPath string, limit int64) {
	if request > limit && limit > 0 {
		v.add(requestPath, "must not exceed %s", limitPath)
	}
}

func (v *validationCollector) uniqueStrings(path string, values []string, requireNonEmpty bool) {
	if requireNonEmpty && len(values) == 0 {
		v.add(path, "must not be empty")
	}
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		if strings.TrimSpace(value) == "" {
			v.add(itemPath, "must not be empty")
		}
		if _, duplicate := seen[value]; duplicate {
			v.add(itemPath, "duplicate value %q", value)
		}
		seen[value] = struct{}{}
	}
}

func (v *validationCollector) requireMembers(path string, values []string, required ...string) {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	for _, value := range required {
		if _, found := set[value]; !found {
			v.add(path, "must contain %q", value)
		}
	}
}

func (v *validationCollector) requiredDenials(path string, values []string, required ...string) {
	v.uniqueStrings(path, values, true)
	v.requireMembers(path, values, required...)
}

func (v *validationCollector) linuxCapabilities(path string, capabilities LinuxCapabilities) {
	v.uniqueStrings(path+".drop", capabilities.Drop, true)
	v.uniqueStrings(path+".add", capabilities.Add, false)
	v.requireMembers(path+".drop", capabilities.Drop, "ALL")
}

func (v *validationCollector) user(path, value string) {
	if !userPattern.MatchString(value) {
		v.add(path, "must be a numeric uid:gid pair")
	}
}

func (v *validationCollector) pinnedImage(path, value string) {
	v.required(path, value)
	index := strings.LastIndex(value, "@sha256:")
	if index < 1 || strings.Count(value, "@") != 1 || strings.TrimSpace(value[:index]) != value[:index] || strings.ContainsAny(value[:index], " \t\r\n") {
		v.add(path, "must be pinned by an @sha256: digest")
		return
	}
	if _, err := domain.ParseDigest(value[index+1:]); err != nil {
		v.add(path, "must be pinned by an exact 64-hex-character sha256 digest")
	}
}

func (v *validationCollector) digestReference(path, value string) {
	if _, err := domain.ParseDigest(value); err != nil {
		v.add(path, "must be a sha256: digest reference")
	}
}

func (v *validationCollector) absoluteGuestPath(path, value string) {
	if value == "" || !strings.HasPrefix(value, "/") || pathpkg.Clean(value) != value || strings.Contains(value, "\\") {
		v.add(path, "must be a clean absolute guest path")
	}
}

func (v *validationCollector) boundedProfile(path string, profile BoundedProfile, captureLimit ByteQuantity) {
	v.positiveDuration(path+".maxDuration", profile.MaxDuration)
	v.positiveBytes(path+".maxBytes", profile.MaxBytes)
	v.withinCaptureLimit(path+".maxBytes", profile.MaxBytes, captureLimit)
	v.uniqueStrings(path+".signalFamilies", profile.SignalFamilies, true)
}

func (v *validationCollector) withinCaptureLimit(path string, value, limit ByteQuantity) {
	if limit > 0 && value > limit {
		v.add(path, "must not exceed spec.resources.aggregateLimits.captureBytes")
	}
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func stringSet(values ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func valueOrNaN(value *float64) float64 {
	if value == nil {
		return math.NaN()
	}
	return *value
}
