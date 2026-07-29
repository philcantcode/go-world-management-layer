package cuttlefish

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func (d *Driver) TargetPhysicalPolicy(ctx context.Context, template ports.TargetTemplate) (ports.TargetPhysicalPolicyReport, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.physical_policy"); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	if err := validateAndroidPhysicalTemplate(template); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	capabilities, err := d.backend.Probe(ctx, template)
	if err != nil {
		return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("probe Android target physical policy: %w", err)
	}
	if err := d.validateObservedBackendVersions(capabilities); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	return androidPhysicalPolicyReport(template, capabilities, admission.Resources{}), nil
}

func (d *Driver) TargetPlanPhysicalPolicy(ctx context.Context, input ports.TargetPlan) (ports.TargetPhysicalPolicyReport, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.plan_physical_policy"); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	if err := validateAndroidPhysicalTemplate(input.Template); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	capabilities, err := d.backend.Probe(ctx, input.Template)
	if err != nil {
		return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("probe Android target plan physical policy: %w", err)
	}
	if err := d.validateObservedBackendVersions(capabilities); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	if capabilities.Managed {
		if err := ValidateManagedEmulatorResources(input.Resources, input.Template.GuestMemoryBytes); err != nil {
			return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("managed emulator cannot enforce requested resources: %w", err)
		}
		validator, ok := d.backend.(interface {
			ValidateResourceEnforcement(context.Context, admission.Resources) error
		})
		if !ok {
			return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("managed emulator backend does not expose host resource-enforcement preflight")
		}
		if err := validator.ValidateResourceEnforcement(ctx, input.Resources); err != nil {
			return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("managed emulator host resource enforcement is unavailable: %w", err)
		}
	}
	return androidPhysicalPolicyReport(input.Template, capabilities, input.Resources), nil
}

func validateAndroidPhysicalTemplate(template ports.TargetTemplate) error {
	if err := template.Validate(); err != nil {
		return err
	}
	if template.Kind != domain.TargetAndroidVirtualDevice {
		return fmt.Errorf("Android physical reporter requires an Android virtual-device template")
	}
	return nil
}

func androidPhysicalPolicyReport(template ports.TargetTemplate, capabilities BackendCapabilities, resources admission.Resources) ports.TargetPhysicalPolicyReport {
	interaction := ports.PhysicalSupportEnforced
	reset := ports.PhysicalSupportUnsupported
	if capabilities.Managed && len(capabilities.ResetModes) > 0 {
		reset = ports.PhysicalSupportEnforced
	}
	hardwareSupport := ports.PhysicalSupportUnknown
	if capabilities.HardwareAccelerationKnown || capabilities.KVMKnown {
		hardware := capabilities.HardwareAcceleration || capabilities.KVM
		if hardware && capabilities.Managed {
			hardwareSupport = ports.PhysicalSupportEnforced
		} else {
			hardwareSupport = ports.PhysicalSupportUnsupported
		}
	}
	rooted := capabilities.RootedKnown && capabilities.Rooted
	debuggable := capabilities.DebuggableKnown && capabilities.Debuggable
	headless := capabilities.HeadlessKnown && capabilities.Headless
	return ports.TargetPhysicalPolicyReport{
		Template: template.Name, Kind: string(template.Kind),
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: template.Driver, Runtime: "", ImageDigest: template.ImageDigest.String(), IsolationProfile: template.IsolationProfile,
			RootFilesystem: "", BaseImage: "", User: "",
			CapabilityDrop: []string{}, CapabilityAdd: []string{}, NoNewPrivileges: false, SeccompProfile: "",
			CapabilitySupport: ports.PhysicalSupportUnsupported, NoNewPrivilegesSupport: ports.PhysicalSupportUnsupported,
			UserSupport: ports.PhysicalSupportUnsupported, SeccompSupport: ports.PhysicalSupportUnsupported,
			UserEnforced: false, SeccompEnforced: false,
		},
		MaterialMountPoint: "", WritableStateMode: "guest-data-partition",
		WritableStateEnforced: capabilities.WritableStateEnforced,
		CommandAuthority:      "arbitrary-inside-assigned-device", ExecTransport: "",
		FileTransfer: "adb-sync-and-scoped-stream", NetworkEndpoints: "",
		ADB: "scoped-gateway", DeviceScopedADBServices: "arbitrary",
		DeniedInfrastructureAuthority: []string{"host-adb-server-control", "other-serials", "raw-usb", "host-exec"},
		ResetAfterEveryRun:            true, ResetMode: "baseline-new-target-generation",
		InteractionSupport: interaction, ResetSupport: reset,
		Resources: androidPhysicalResources(capabilities, resources),
		Android: ports.AndroidRuntimePhysicalFacts{
			SystemImageDigest: template.ImageDigest.String(), BaselineState: template.BaselineState,
			HardwareAcceleration:        capabilities.HardwareAcceleration || capabilities.KVM,
			HardwareAccelerationSupport: hardwareSupport, Headless: headless, Rooted: rooted,
			Debuggable: debuggable, GuestMemoryBytes: template.GuestMemoryBytes, BootTimeout: template.BootTimeout,
		},
	}
}

func androidPhysicalResources(capabilities BackendCapabilities, resources admission.Resources) ports.ContainerResourcePhysicalFacts {
	return ports.ContainerResourcePhysicalFacts{
		CPUMilli:           physicalLimit(resources.CPUMilli, capabilities.CPUEnforced, "host process-tree CPU hard cap; emulator -cores independently configures guest vCPU topology"),
		MemoryBytes:        physicalLimit(resources.MemoryBytes, capabilities.MemoryEnforced, "host process-tree committed-memory limit; guest RAM is configured independently"),
		SwapBytes:          physicalUnsupported(resources.SwapBytes, "Android emulator does not expose an independently enforced swap limit"),
		WorkspaceBytes:     physicalUnsupported(0, "Android target has no agent workspace"),
		WritableStateBytes: physicalLimit(resources.StorageBytes, capabilities.WritableStateEnforced, "exact guest /data block-device capacity; host AVD metadata is outside this policy field"),
		CaptureBytes:       physicalUnsupported(resources.CaptureBytes, "capture limits are enforced by observer storage"),
		Inodes:             physicalUnsupported(resources.Inodes, "Android emulator does not expose an inode quota"),
		PIDs:               physicalUnsupported(resources.PIDs, "Android emulator does not expose a guest PID quota"),
	}
}

func physicalLimit(value int64, enforced bool, detail string) ports.PhysicalLimitFact {
	support := ports.PhysicalSupportUnsupported
	if enforced {
		support = ports.PhysicalSupportEnforced
	}
	return ports.PhysicalLimitFact{Value: value, Support: support, Detail: detail}
}

func physicalUnsupported(value int64, detail string) ports.PhysicalLimitFact {
	return ports.PhysicalLimitFact{Value: value, Support: ports.PhysicalSupportUnsupported, Detail: detail}
}

var _ ports.TargetPhysicalPolicyReporter = (*Driver)(nil)
