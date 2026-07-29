package policyauthority

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

// AgentPhysicalPolicyFingerprint converts an immutable config-level physical
// report into the exact component fingerprint included in policy publication.
// Set-like slices are normalized so equivalent driver reports cannot acquire
// different identities solely because of iteration order.
func AgentPhysicalPolicyFingerprint(report ports.AgentWorkspacePhysicalPolicyReport) (policy.CapabilityFingerprint, error) {
	report.Runtime.CapabilityDrop = sortedStrings(report.Runtime.CapabilityDrop)
	report.Runtime.CapabilityAdd = sortedStrings(report.Runtime.CapabilityAdd)
	report.Network.AllowedCIDRs = sortedStrings(report.Network.AllowedCIDRs)
	report.Network.AllowedDomains = sortedStrings(report.Network.AllowedDomains)
	return physicalPolicyFingerprint("agent", report)
}

// TargetPhysicalPolicyFingerprint removes plan identity fields from a
// config-level report before fingerprinting it. Template and image identity
// are authenticated independently by the effective policy and bound plan.
func TargetPhysicalPolicyFingerprint(report ports.TargetPhysicalPolicyReport) (policy.CapabilityFingerprint, error) {
	report.Template = ""
	report.Runtime.ImageDigest = ""
	report.Android.SystemImageDigest = ""
	// Guest RAM and the boot deadline are selected per trusted target template.
	// Neither is a property of the emulator backend, so templates with different
	// values must still produce one shared physical-capability identity.
	report.Android.GuestMemoryBytes = 0
	report.Android.BootTimeout = 0
	report.Runtime.CapabilityDrop = sortedStrings(report.Runtime.CapabilityDrop)
	report.Runtime.CapabilityAdd = sortedStrings(report.Runtime.CapabilityAdd)
	report.DeniedInfrastructureAuthority = sortedStrings(report.DeniedInfrastructureAuthority)
	report.Resources = zeroPhysicalResourceValues(report.Resources)
	return physicalPolicyFingerprint("target", report)
}

// WithBoundedLedgerCaptureEnforcement binds the configured ledger capture
// controller's hard byte ceiling into the agent plan's physical resource
// facts. Capture storage is owned by the host controller, not Docker's bind
// mount or cgroup resource controller.
func WithBoundedLedgerCaptureEnforcement(report ports.AgentWorkspacePhysicalPolicyReport) ports.AgentWorkspacePhysicalPolicyReport {
	report.Resources.CaptureBytes.Support = ports.PhysicalSupportEnforced
	report.Resources.CaptureBytes.Detail = "host ledger capture rejects output beyond the authorized byte limit"
	return report
}

// WithTargetConfiguredResources combines config-level backend support facts
// with the exact resource vector selected by the trusted deployment template.
// It is used before target IDs exist; the plan-level reporter independently
// re-derives and verifies the same values immediately before mutation.
func WithTargetConfiguredResources(report ports.TargetPhysicalPolicyReport, resources admission.Resources) ports.TargetPhysicalPolicyReport {
	report.Resources.CPUMilli.Value = resources.CPUMilli
	report.Resources.MemoryBytes.Value = resources.MemoryBytes
	report.Resources.SwapBytes.Value = resources.SwapBytes
	report.Resources.WritableStateBytes.Value = resources.StorageBytes
	report.Resources.CaptureBytes.Value = resources.CaptureBytes
	report.Resources.Inodes.Value = resources.Inodes
	report.Resources.PIDs.Value = resources.PIDs
	return report
}

func physicalPolicyFingerprint(kind string, report any) (policy.CapabilityFingerprint, error) {
	encoded, err := json.Marshal(report)
	if err != nil {
		return policy.CapabilityFingerprint{}, fmt.Errorf("encode %s physical policy report: %w", kind, err)
	}
	reportDigest := domain.NewDigest(encoded).String()
	capability, err := policy.NewCapability(policy.CapabilitySupported, nil, map[string]string{"report_digest": reportDigest})
	if err != nil {
		return policy.CapabilityFingerprint{}, err
	}
	return policy.NewCapabilityFingerprint(
		map[string]policy.Capability{"physical-policy." + kind: capability},
		map[string]string{"kind": kind, "report_digest": reportDigest},
	)
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func zeroPhysicalResourceValues(resources ports.ContainerResourcePhysicalFacts) ports.ContainerResourcePhysicalFacts {
	resources.CPUMilli.Value = 0
	resources.MemoryBytes.Value = 0
	resources.SwapBytes.Value = 0
	resources.WorkspaceBytes.Value = 0
	resources.WritableStateBytes.Value = 0
	resources.CaptureBytes.Value = 0
	resources.Inodes.Value = 0
	resources.PIDs.Value = 0
	return resources
}
