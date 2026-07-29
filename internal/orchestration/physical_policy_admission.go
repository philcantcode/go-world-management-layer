package orchestration

import (
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func validateAgentPhysicalReport(report ports.AgentWorkspacePhysicalPolicyReport) error {
	if report.Runtime.Driver == "" || report.Runtime.User == "" || report.Runtime.RootFilesystem == "" || report.Network.Mode == "" {
		return fmt.Errorf("runtime driver, root filesystem, user, and network mode are required")
	}
	for name, support := range map[string]ports.PhysicalSupport{
		"runtime.capabilities": report.Runtime.CapabilitySupport, "runtime.no_new_privileges": report.Runtime.NoNewPrivilegesSupport,
		"runtime.user": report.Runtime.UserSupport, "runtime.seccomp": report.Runtime.SeccompSupport, "network": report.Network.Support,
	} {
		if !support.IsValid() {
			return fmt.Errorf("%s support %q is invalid", name, support)
		}
	}
	return validatePhysicalResourceSupports(report.Resources)
}

func validateTargetPlanPhysicalReport(plan ports.TargetPlan, report ports.TargetPhysicalPolicyReport) error {
	if report.Template != plan.Template.Name || report.Kind != string(plan.Template.Kind) ||
		report.Runtime.Driver != plan.Template.Driver || report.Runtime.Runtime != plan.Template.Runtime ||
		report.Runtime.ImageDigest != plan.Template.ImageDigest.String() || report.Runtime.IsolationProfile != plan.Template.IsolationProfile {
		return fmt.Errorf("%w: target physical policy report does not identify the exact plan", policyauthority.ErrPolicyDenied)
	}
	if plan.Template.Kind == domain.TargetAndroidVirtualDevice {
		if report.Android.SystemImageDigest != plan.Template.ImageDigest.String() ||
			report.Android.BaselineState != plan.Template.BaselineState ||
			report.Android.HardwareAcceleration != plan.Template.RequireHardwareAcceleration ||
			report.Android.Headless != plan.Template.Headless || report.Android.Rooted != plan.Template.Rooted ||
			report.Android.Debuggable != plan.Template.Debuggable || report.Android.GuestMemoryBytes != plan.Template.GuestMemoryBytes ||
			report.Android.BootTimeout != plan.Template.BootTimeout {
			return fmt.Errorf("%w: Android physical policy report does not identify the exact virtual-device plan", policyauthority.ErrPolicyDenied)
		}
	}
	if err := requireTargetPhysicalEnforcement(report); err != nil {
		return err
	}
	for name, values := range map[string][2]int64{
		"cpu_milli":            {report.Resources.CPUMilli.Value, plan.Resources.CPUMilli},
		"memory_bytes":         {report.Resources.MemoryBytes.Value, plan.Resources.MemoryBytes},
		"swap_bytes":           {report.Resources.SwapBytes.Value, plan.Resources.SwapBytes},
		"writable_state_bytes": {report.Resources.WritableStateBytes.Value, plan.Resources.StorageBytes},
		"capture_bytes":        {report.Resources.CaptureBytes.Value, plan.Resources.CaptureBytes},
		"inodes":               {report.Resources.Inodes.Value, plan.Resources.Inodes},
		"pids":                 {report.Resources.PIDs.Value, plan.Resources.PIDs},
	} {
		if values[0] != values[1] {
			return fmt.Errorf("%w: target physical %s=%d does not match plan value %d", policyauthority.ErrPolicyDenied, name, values[0], values[1])
		}
	}
	return nil
}

func requireTargetPhysicalEnforcement(report ports.TargetPhysicalPolicyReport) error {
	if report.InteractionSupport != ports.PhysicalSupportEnforced || report.ResetSupport != ports.PhysicalSupportEnforced {
		return fmt.Errorf("%w: target interaction and reset facts must be enforced", policyauthority.ErrPolicyDenied)
	}
	switch report.Kind {
	case string(domain.TargetLinuxContainer):
		for name, support := range map[string]ports.PhysicalSupport{
			"runtime.capabilities": report.Runtime.CapabilitySupport, "runtime.no_new_privileges": report.Runtime.NoNewPrivilegesSupport,
			"runtime.user": report.Runtime.UserSupport, "runtime.seccomp": report.Runtime.SeccompSupport,
		} {
			if support != ports.PhysicalSupportEnforced {
				return fmt.Errorf("%w: target %s is not enforced (%s)", policyauthority.ErrPolicyDenied, name, support)
			}
		}
	case string(domain.TargetAndroidVirtualDevice):
		if report.Android.HardwareAccelerationSupport != ports.PhysicalSupportEnforced || !report.Android.HardwareAcceleration {
			return fmt.Errorf("%w: Android hardware acceleration is not enforced (%s)", policyauthority.ErrPolicyDenied, report.Android.HardwareAccelerationSupport)
		}
	default:
		return fmt.Errorf("%w: target kind %q has no physical enforcement contract", policyauthority.ErrPolicyDenied, report.Kind)
	}
	if err := validatePhysicalResourceSupports(report.Resources); err != nil {
		return err
	}
	return nil
}

func validateAgentPlanPhysicalReport(plan ports.AgentWorkspacePlan, configured, report ports.AgentWorkspacePhysicalPolicyReport) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if err := validateAgentPhysicalReport(report); err != nil {
		return fmt.Errorf("%w: invalid agent plan physical report: %v", policyauthority.ErrPolicyDenied, err)
	}
	if report.Runtime.ImageDigest != plan.ImageDigest.String() {
		return fmt.Errorf("%w: agent physical image %q does not match plan image %q", policyauthority.ErrPolicyDenied, report.Runtime.ImageDigest, plan.ImageDigest)
	}
	if len(plan.Resources.Devices) != 0 {
		return fmt.Errorf("%w: agent devices have no physical policy representation", policyauthority.ErrPolicyDenied)
	}
	for name, values := range map[string][2]int64{
		"cpu_milli":       {report.Resources.CPUMilli.Value, plan.Resources.CPUMilli},
		"memory_bytes":    {report.Resources.MemoryBytes.Value, plan.Resources.MemoryBytes},
		"swap_bytes":      {report.Resources.SwapBytes.Value, plan.Resources.SwapBytes},
		"workspace_bytes": {report.Resources.WorkspaceBytes.Value, plan.Resources.StorageBytes},
		"capture_bytes":   {report.Resources.CaptureBytes.Value, plan.Resources.CaptureBytes},
		"inodes":          {report.Resources.Inodes.Value, plan.Resources.Inodes},
		"pids":            {report.Resources.PIDs.Value, plan.Resources.PIDs},
	} {
		if values[0] != values[1] {
			return fmt.Errorf("%w: agent physical %s=%d does not match plan value %d", policyauthority.ErrPolicyDenied, name, values[0], values[1])
		}
	}
	configuredFingerprint, err := policyauthority.AgentPhysicalPolicyFingerprint(normalizeAgentConfigPhysicalReport(configured))
	if err != nil {
		return err
	}
	reportFingerprint, err := policyauthority.AgentPhysicalPolicyFingerprint(normalizeAgentConfigPhysicalReport(report))
	if err != nil {
		return err
	}
	if configuredFingerprint.Digest() != reportFingerprint.Digest() {
		return fmt.Errorf("%w: agent plan physical facts differ from the published config-level facts", policyauthority.ErrPolicyDenied)
	}
	return nil
}

func requireMatchingTargetPhysicalFacts(configured, report ports.TargetPhysicalPolicyReport) error {
	configuredFingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(configured)
	if err != nil {
		return err
	}
	reportFingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(report)
	if err != nil {
		return err
	}
	if configuredFingerprint.Digest() != reportFingerprint.Digest() {
		return fmt.Errorf("%w: target plan physical facts differ from the published config-level facts", policyauthority.ErrPolicyDenied)
	}
	return nil
}

func requireAgentPhysicalEnforcement(report ports.AgentWorkspacePhysicalPolicyReport) error {
	for name, support := range map[string]ports.PhysicalSupport{
		"runtime.capabilities": report.Runtime.CapabilitySupport, "runtime.no_new_privileges": report.Runtime.NoNewPrivilegesSupport,
		"runtime.user": report.Runtime.UserSupport, "runtime.seccomp": report.Runtime.SeccompSupport, "network": report.Network.Support,
	} {
		if support != ports.PhysicalSupportEnforced {
			return fmt.Errorf("%w: agent %s is not enforced (%s)", policyauthority.ErrPolicyDenied, name, support)
		}
	}
	return nil
}

func validatePhysicalResourceSupports(resources ports.ContainerResourcePhysicalFacts) error {
	for name, fact := range physicalLimitFacts(resources) {
		if fact.Value < 0 || !fact.Support.IsValid() {
			return fmt.Errorf("physical resource %s has invalid value or support", name)
		}
	}
	return nil
}

type physicalLimitRequirement struct {
	limit           int64
	fact            ports.PhysicalLimitFact
	requireWhenZero bool
}

func requireAgentResourceSupport(effective *policy.EffectivePolicy, resources ports.ContainerResourcePhysicalFacts) error {
	if err := requireAgentRuntimeSupport(resources); err != nil {
		return err
	}
	document := effective.Policy()
	limits := document.Spec.AgentWorkspace.Resources.Limits
	required := map[string]physicalLimitRequirement{
		"agent.resources.cpu_milli":     requirePositivePhysicalLimit(limits.CPU.MilliCPU(), resources.CPUMilli),
		"agent.resources.memory_bytes":  requirePositivePhysicalLimit(limits.Memory.Bytes(), resources.MemoryBytes),
		"agent.resources.swap_bytes":    requirePhysicalLimit(limits.Swap.Bytes(), resources.SwapBytes),
		"agent.resources.capture_bytes": requirePositivePhysicalLimit(document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes(), resources.CaptureBytes),
		"agent.resources.pids":          requirePositivePhysicalLimit(limits.PIDs, resources.PIDs),
	}
	if document.Spec.Workspace.Mode != "directory-copy-non-production" {
		required["agent.resources.workspace_bytes"] = requirePositivePhysicalLimit(limits.Workspace.Bytes(), resources.WorkspaceBytes)
		required["agent.resources.inodes"] = requirePositivePhysicalLimit(limits.WorkspaceInodes, resources.Inodes)
	}
	return requireEnforcedLimits(required)
}

func requireAgentRuntimeSupport(resources ports.ContainerResourcePhysicalFacts) error {
	return validatePhysicalResourceSupports(resources)
}

func requireTargetResourceSupport(effective *policy.EffectivePolicy, templateName string, resources ports.ContainerResourcePhysicalFacts) error {
	document := effective.Policy()
	var selected *policy.TargetTemplate
	for _, template := range document.Spec.Targets.Templates {
		if template.Name == templateName {
			copy := template
			selected = &copy
			break
		}
	}
	if selected == nil {
		return fmt.Errorf("%w: target template %q is not allowed", policyauthority.ErrPolicyDenied, templateName)
	}
	limits := selected.Resources.Limits
	required := map[string]physicalLimitRequirement{
		"target.resources.cpu_milli":    requirePositivePhysicalLimit(limits.CPU.MilliCPU(), resources.CPUMilli),
		"target.resources.memory_bytes": requirePositivePhysicalLimit(limits.Memory.Bytes(), resources.MemoryBytes),
		"target.resources.swap_bytes":   requirePositivePhysicalLimit(limits.Swap.Bytes(), resources.SwapBytes),
		"target.resources.pids":         requirePositivePhysicalLimit(limits.PIDs, resources.PIDs),
	}
	if selected.Kind == "linux-container" {
		required["target.resources.swap_bytes"] = requirePhysicalLimit(limits.Swap.Bytes(), resources.SwapBytes)
	}
	if selected.Material.WritableState != "private-directory-non-production" {
		required["target.resources.writable_state_bytes"] = requirePositivePhysicalLimit(limits.WritableState.Bytes(), resources.WritableStateBytes)
	}
	return requireEnforcedLimits(required)
}

func requirePositivePhysicalLimit(limit int64, fact ports.PhysicalLimitFact) physicalLimitRequirement {
	return physicalLimitRequirement{limit: limit, fact: fact}
}

func requirePhysicalLimit(limit int64, fact ports.PhysicalLimitFact) physicalLimitRequirement {
	return physicalLimitRequirement{limit: limit, fact: fact, requireWhenZero: true}
}

func requireEnforcedLimits(values map[string]physicalLimitRequirement) error {
	for name, value := range values {
		if (value.limit > 0 || value.requireWhenZero) && value.fact.Support != ports.PhysicalSupportEnforced {
			return fmt.Errorf("%w: %s policy limit is not enforced (%s: %s)", policyauthority.ErrPolicyDenied, name, value.fact.Support, value.fact.Detail)
		}
	}
	return nil
}

func agentRuntimeAdmission(value ports.ContainerRuntimePhysicalFacts) policyauthority.AgentRuntimeAdmission {
	return policyauthority.AgentRuntimeAdmission{
		Driver: value.Driver, ImageDigest: value.ImageDigest, IsolationProfile: value.IsolationProfile,
		RootFilesystem: value.RootFilesystem, User: value.User, CapabilityDrop: append([]string(nil), value.CapabilityDrop...),
		CapabilityAdd: append([]string(nil), value.CapabilityAdd...), NoNewPrivileges: value.NoNewPrivileges,
		SeccompProfile:  value.SeccompProfile,
		UserEnforced:    value.UserEnforced && value.UserSupport == ports.PhysicalSupportEnforced,
		SeccompEnforced: value.SeccompEnforced && value.SeccompSupport == ports.PhysicalSupportEnforced,
	}
}

func agentNetworkAdmission(value ports.ContainerNetworkPhysicalFacts) policyauthority.NetworkAdmission {
	return policyauthority.NetworkAdmission{
		Mode: value.Mode, AllowDNS: value.AllowDNS, AllowedCIDRs: append([]string(nil), value.AllowedCIDRs...),
		AllowedDomains: append([]string(nil), value.AllowedDomains...), DenyPrivateRanges: value.DenyPrivateRanges,
		TargetAccess: value.TargetAccess,
	}
}

func targetAdmission(report ports.TargetPhysicalPolicyReport) policyauthority.TargetAdmission {
	return policyauthority.TargetAdmission{
		Template: report.Template, Kind: report.Kind, Driver: report.Runtime.Driver, Runtime: report.Runtime.Runtime,
		ImageDigest: report.Runtime.ImageDigest, IsolationProfile: report.Runtime.IsolationProfile, BaseImage: report.Runtime.BaseImage,
		User: report.Runtime.User, CapabilityDrop: append([]string(nil), report.Runtime.CapabilityDrop...),
		CapabilityAdd: append([]string(nil), report.Runtime.CapabilityAdd...), NoNewPrivileges: report.Runtime.NoNewPrivileges,
		SeccompProfile:     report.Runtime.SeccompProfile,
		UserEnforced:       report.Runtime.UserEnforced && report.Runtime.UserSupport == ports.PhysicalSupportEnforced,
		SeccompEnforced:    report.Runtime.SeccompEnforced && report.Runtime.SeccompSupport == ports.PhysicalSupportEnforced,
		MaterialMountPoint: report.MaterialMountPoint, WritableStateMode: report.WritableStateMode,
		WritableStateEnforced: report.WritableStateEnforced && report.Resources.WritableStateBytes.Support == ports.PhysicalSupportEnforced,
		CommandAuthority:      report.CommandAuthority, ExecTransport: report.ExecTransport, FileTransfer: report.FileTransfer,
		NetworkEndpoints: report.NetworkEndpoints, DeniedInfrastructureAuthority: append([]string(nil), report.DeniedInfrastructureAuthority...),
		ADB: report.ADB, DeviceScopedADBServices: report.DeviceScopedADBServices,
		ResetAfterEveryRun: report.ResetAfterEveryRun, ResetMode: report.ResetMode,
		BaselineState:                report.Android.BaselineState,
		RequireHardwareAcceleration:  report.Android.HardwareAcceleration,
		HardwareAccelerationEnforced: report.Android.HardwareAccelerationSupport == ports.PhysicalSupportEnforced,
		Headless:                     report.Android.Headless, Rooted: report.Android.Rooted, Debuggable: report.Android.Debuggable,
		GuestMemoryBytes: report.Android.GuestMemoryBytes,
		BootTimeout:      report.Android.BootTimeout,
		Resources: policyauthority.RuntimeResources{
			CPUMilli: report.Resources.CPUMilli.Value, MemoryBytes: report.Resources.MemoryBytes.Value,
			SwapBytes: report.Resources.SwapBytes.Value, WritableStateBytes: report.Resources.WritableStateBytes.Value,
			CaptureBytes: report.Resources.CaptureBytes.Value, Inodes: report.Resources.Inodes.Value, PIDs: report.Resources.PIDs.Value,
		},
	}
}

func cloneAgentPhysicalReport(value ports.AgentWorkspacePhysicalPolicyReport) ports.AgentWorkspacePhysicalPolicyReport {
	value.Runtime.CapabilityDrop = append([]string(nil), value.Runtime.CapabilityDrop...)
	value.Runtime.CapabilityAdd = append([]string(nil), value.Runtime.CapabilityAdd...)
	value.Network.AllowedCIDRs = append([]string(nil), value.Network.AllowedCIDRs...)
	value.Network.AllowedDomains = append([]string(nil), value.Network.AllowedDomains...)
	return value
}

func cloneTargetPhysicalReport(value ports.TargetPhysicalPolicyReport) ports.TargetPhysicalPolicyReport {
	value.Runtime.CapabilityDrop = append([]string(nil), value.Runtime.CapabilityDrop...)
	value.Runtime.CapabilityAdd = append([]string(nil), value.Runtime.CapabilityAdd...)
	value.DeniedInfrastructureAuthority = append([]string(nil), value.DeniedInfrastructureAuthority...)
	return value
}

func normalizeAgentConfigPhysicalReport(value ports.AgentWorkspacePhysicalPolicyReport) ports.AgentWorkspacePhysicalPolicyReport {
	value = cloneAgentPhysicalReport(value)
	value.Runtime.ImageDigest = ""
	for name, fact := range physicalLimitFacts(value.Resources) {
		fact.Value = 0
		switch name {
		case "cpu_milli":
			value.Resources.CPUMilli = fact
		case "memory_bytes":
			value.Resources.MemoryBytes = fact
		case "swap_bytes":
			value.Resources.SwapBytes = fact
		case "workspace_bytes":
			value.Resources.WorkspaceBytes = fact
		case "writable_state_bytes":
			value.Resources.WritableStateBytes = fact
		case "capture_bytes":
			value.Resources.CaptureBytes = fact
		case "inodes":
			value.Resources.Inodes = fact
		case "pids":
			value.Resources.PIDs = fact
		}
	}
	return value
}

func physicalAgentRuntimeResources(resources ports.ContainerResourcePhysicalFacts) policyauthority.RuntimeResources {
	return policyauthority.RuntimeResources{
		CPUMilli: resources.CPUMilli.Value, MemoryBytes: resources.MemoryBytes.Value, SwapBytes: resources.SwapBytes.Value,
		WorkspaceBytes: resources.WorkspaceBytes.Value, CaptureBytes: resources.CaptureBytes.Value,
		Inodes: resources.Inodes.Value, PIDs: resources.PIDs.Value,
	}
}

func physicalLimitFacts(resources ports.ContainerResourcePhysicalFacts) map[string]ports.PhysicalLimitFact {
	return map[string]ports.PhysicalLimitFact{
		"cpu_milli": resources.CPUMilli, "memory_bytes": resources.MemoryBytes, "swap_bytes": resources.SwapBytes,
		"workspace_bytes": resources.WorkspaceBytes, "writable_state_bytes": resources.WritableStateBytes,
		"capture_bytes": resources.CaptureBytes, "inodes": resources.Inodes, "pids": resources.PIDs,
	}
}
