package linuxcontainer

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func (d *Driver) TargetPhysicalPolicy(ctx context.Context, template ports.TargetTemplate) (ports.TargetPhysicalPolicyReport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.physical_policy"); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	if err := validatePhysicalTemplate(template); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	user := configuredTargetUser(d.build.ContainerUser)
	if _, _, err := dockercli.ParseNumericUser(user); err != nil {
		return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("configured target container user: %w", err)
	}
	capabilities, err := d.runtime.Probe(ctx)
	if err != nil {
		return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("probe Docker target physical policy: %w", err)
	}
	return targetPhysicalPolicyReport(d.build, capabilities, template, user, nil), nil
}

func (d *Driver) TargetPlanPhysicalPolicy(ctx context.Context, input ports.TargetPlan) (ports.TargetPhysicalPolicyReport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.plan_physical_policy"); err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	plan, err := BuildContainerPlan(input, d.build)
	if err != nil {
		return ports.TargetPhysicalPolicyReport{}, err
	}
	capabilities, err := d.runtime.Probe(ctx)
	if err != nil {
		return ports.TargetPhysicalPolicyReport{}, fmt.Errorf("probe Docker target physical policy: %w", err)
	}
	return targetPhysicalPolicyReport(d.build, capabilities, input.Template, plan.User, &plan), nil
}

func validatePhysicalTemplate(template ports.TargetTemplate) error {
	if err := template.Validate(); err != nil {
		return err
	}
	if template.Kind != domain.TargetLinuxContainer || template.Driver != "docker" {
		return fmt.Errorf("physical reporter requires a Docker Linux-container template")
	}
	if isolation, ok := targetIsolationProfile(template.Runtime); !ok || isolation != template.IsolationProfile {
		return fmt.Errorf("target runtime and isolation profile are not a supported physical pair")
	}
	return nil
}

func targetPhysicalPolicyReport(build BuildConfig, capabilities RuntimeCapabilities, template ports.TargetTemplate, user string, plan *ContainerPlan) ports.TargetPhysicalPolicyReport {
	support := dockercli.AssessPhysicalSupport(dockercli.PhysicalCapabilities{
		OSType: capabilities.OSType, SecurityOptions: capabilities.SecurityOptions, Runtimes: capabilities.Runtimes,
		CPUCFSQuota: capabilities.CPUCFSQuota, MemoryLimit: capabilities.MemoryLimit, SwapLimit: capabilities.SwapLimit, PIDsLimit: capabilities.PIDsLimit,
	}, template.Runtime)
	values := dockercli.PhysicalResourceValues{}
	if plan != nil {
		values = dockercli.PhysicalResourceValues{
			CPUMilli: plan.Resources.CPUMilli, MemoryBytes: plan.Resources.MemoryBytes, SwapBytes: plan.Resources.SwapBytes,
			WritableStateBytes: plan.Resources.StorageBytes, CaptureBytes: plan.Resources.CaptureBytes,
			Inodes: plan.Resources.Inodes, PIDs: plan.Resources.PIDs,
		}
	}
	capabilityAdd := []string{}
	if build.AllowPtrace {
		capabilityAdd = []string{"SYS_PTRACE"}
	}
	return ports.TargetPhysicalPolicyReport{
		Template: template.Name, Kind: string(template.Kind),
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: template.Driver, Runtime: template.Runtime, ImageDigest: template.ImageDigest.String(), IsolationProfile: template.IsolationProfile,
			RootFilesystem: "readOnly", BaseImage: "readOnly", User: user,
			CapabilityDrop: []string{"ALL"}, CapabilityAdd: capabilityAdd, NoNewPrivileges: true,
			SeccompProfile: dockercli.RuntimeDefaultSeccompProfile,
			UserEnforced:   support.Container == ports.PhysicalSupportEnforced, SeccompEnforced: support.Seccomp == ports.PhysicalSupportEnforced,
			CapabilitySupport: support.Container, NoNewPrivilegesSupport: support.Container,
			UserSupport: support.Container, SeccompSupport: support.Seccomp,
		},
		MaterialMountPoint: TargetMaterialMount, WritableStateMode: "private-directory-non-production", WritableStateEnforced: false,
		CommandAuthority: "arbitrary-inside-assigned-target", ExecTransport: "direct-argv-and-explicit-shell",
		FileTransfer: "push-pull-target-relative", NetworkEndpoints: "none",
		DeniedInfrastructureAuthority: []string{"host-exec", "docker-api", "host-mounts", "other-targets"},
		ResetAfterEveryRun:            false, ResetMode: "recreate-new-target-generation",
		InteractionSupport: ports.PhysicalSupportEnforced, ResetSupport: ports.PhysicalSupportEnforced,
		Resources: dockercli.PhysicalResourceFacts(support, values),
	}
}

var _ ports.TargetPhysicalPolicyReporter = (*Driver)(nil)
