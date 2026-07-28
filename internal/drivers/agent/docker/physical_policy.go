package docker

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const agentIsolationProfile = "agent-standard"

func (d *Driver) AgentWorkspacePhysicalPolicy(ctx context.Context) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	if err := requireContext(ctx, "docker.physical_policy"); err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, err
	}
	user := configuredContainerUser(d.build.ContainerUser)
	if _, _, err := dockercli.ParseNumericUser(user); err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, fmt.Errorf("configured agent container user: %w", err)
	}
	capabilities, err := d.engine.Probe(ctx)
	if err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, fmt.Errorf("probe Docker physical policy: %w", err)
	}
	return agentPhysicalPolicyReport(capabilities, user, "", nil), nil
}

func (d *Driver) AgentWorkspacePlanPhysicalPolicy(ctx context.Context, input ports.AgentWorkspacePlan) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	if err := requireContext(ctx, "docker.plan_physical_policy"); err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, err
	}
	plan, err := BuildContainerPlan(input, d.build)
	if err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, err
	}
	capabilities, err := d.engine.Probe(ctx)
	if err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, fmt.Errorf("probe Docker physical policy: %w", err)
	}
	return agentPhysicalPolicyReport(capabilities, plan.User, input.ImageDigest.String(), &plan), nil
}

func agentPhysicalPolicyReport(capabilities EngineCapabilities, user, imageDigest string, plan *ContainerPlan) ports.AgentWorkspacePhysicalPolicyReport {
	support := dockercli.AssessPhysicalSupport(dockercli.PhysicalCapabilities{
		OSType: capabilities.OSType, SecurityOptions: capabilities.SecurityOptions, Runtimes: capabilities.Runtimes,
		CPUCFSQuota: capabilities.CPUCFSQuota, MemoryLimit: capabilities.MemoryLimit, SwapLimit: capabilities.SwapLimit, PIDsLimit: capabilities.PIDsLimit,
	}, dockercli.RuncRuntime)
	values := dockercli.PhysicalResourceValues{}
	if plan != nil {
		values = dockercli.PhysicalResourceValues{
			CPUMilli: plan.Resources.CPUMilli, MemoryBytes: plan.Resources.MemoryBytes, SwapBytes: plan.Resources.SwapBytes,
			WorkspaceBytes: plan.Resources.StorageBytes, CaptureBytes: plan.Resources.CaptureBytes,
			Inodes: plan.Resources.Inodes, PIDs: plan.Resources.PIDs,
		}
	}
	return ports.AgentWorkspacePhysicalPolicyReport{
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: "docker", Runtime: dockercli.RuncRuntime, ImageDigest: imageDigest, IsolationProfile: agentIsolationProfile,
			RootFilesystem: "readOnly", User: user, CapabilityDrop: []string{"ALL"}, CapabilityAdd: []string{},
			NoNewPrivileges: true, SeccompProfile: dockercli.RuntimeDefaultSeccompProfile,
			UserEnforced: support.Container == ports.PhysicalSupportEnforced, SeccompEnforced: support.Seccomp == ports.PhysicalSupportEnforced,
			CapabilitySupport: support.Container, NoNewPrivilegesSupport: support.Container,
			UserSupport: support.Container, SeccompSupport: support.Seccomp,
		},
		Network: ports.ContainerNetworkPhysicalFacts{
			Mode: "none", AllowDNS: false, AllowedCIDRs: []string{}, AllowedDomains: []string{},
			DenyPrivateRanges: true, TargetAccess: "none", Support: support.Container,
		},
		Resources: dockercli.PhysicalResourceFacts(support, values),
	}
}

var _ ports.AgentWorkspacePhysicalPolicyReporter = (*Driver)(nil)
