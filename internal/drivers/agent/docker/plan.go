package docker

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	WorkspaceMount     = "/workspace"
	defaultGuestBinary = "/usr/local/bin/world-guest"
	defaultGuestUser   = "65532:65532"
	agentRoleLabel     = "agent-workspace"
	planDigestLabel    = "world.plan-digest"
)

type Mount struct {
	Source   string
	Target   string
	ReadOnly bool
}

type ContainerPlan struct {
	Name                    string
	LeaseID                 domain.LeaseID
	AgentWorkspaceID        domain.AgentWorkspaceID
	Generation              domain.AgentGeneration
	WorkspaceID             domain.WorkspaceID
	Image                   string
	Runtime                 string
	PolicyDigest            domain.Digest
	CapabilityDigest        domain.Digest
	Resources               admission.Resources
	Mounts                  []Mount
	ExpectedWorkspaceSource string
	Labels                  map[string]string
	Entrypoint              []string
	ReadOnlyRoot            bool
	Privileged              bool
	HostPID                 bool
	HostIPC                 bool
	HostNetwork             bool
	HostCgroup              bool
	Devices                 []string
	Capabilities            []string
	NoNewPrivileges         bool
	SeccompProfile          string
	User                    string
}

// Validate enforces the authority boundary independently of the builder. This
// is intentionally fail-closed so a future caller cannot turn a typed plan
// into Docker passthrough.
func (p ContainerPlan) Validate(workspaceRoot string) error {
	if p.Name == "" || p.LeaseID.IsZero() || p.AgentWorkspaceID.IsZero() || !p.Generation.IsValid() || p.WorkspaceID.IsZero() {
		return fmt.Errorf("container identity and generation are required")
	}
	if p.Image == "" || !strings.Contains(p.Image, "@sha256:") {
		return fmt.Errorf("container image must be pinned by sha256 digest")
	}
	if p.Runtime != dockercli.RuncRuntime {
		return fmt.Errorf("agent container runtime must be runc")
	}
	if p.PolicyDigest.IsZero() || p.CapabilityDigest.IsZero() {
		return fmt.Errorf("policy and capability digests are required")
	}
	if err := p.Resources.Validate(); err != nil {
		return err
	}
	if _, err := dockercli.ResourceLimitArguments(p.Resources.CPUMilli, p.Resources.MemoryBytes, p.Resources.SwapBytes, p.Resources.PIDs); err != nil {
		return fmt.Errorf("container resource limits: %w", err)
	}
	if err := validatePlanLabels(p); err != nil {
		return err
	}
	if p.Privileged || p.HostPID || p.HostIPC || p.HostNetwork || p.HostCgroup || len(p.Devices) != 0 {
		return fmt.Errorf("privileged mode, host namespaces, and devices are forbidden")
	}
	if !p.ReadOnlyRoot || !p.NoNewPrivileges || p.SeccompProfile != dockercli.RuntimeDefaultSeccompProfile || p.User == "" {
		return fmt.Errorf("read-only root, no-new-privileges, runtime-default seccomp, and a non-empty user are required")
	}
	if _, _, err := dockercli.ParseNumericUser(p.User); err != nil {
		return fmt.Errorf("container user: %w", err)
	}
	if len(p.Capabilities) != 0 {
		return fmt.Errorf("agent container capabilities must be empty")
	}
	if len(p.Mounts) != 1 || p.Mounts[0].Target != WorkspaceMount || p.Mounts[0].ReadOnly {
		return fmt.Errorf("exactly one writable workspace mount is required")
	}
	if p.ExpectedWorkspaceSource == "" {
		return fmt.Errorf("resolved workspace source is required")
	}
	if err := requirePathBeneath(workspaceRoot, p.ExpectedWorkspaceSource); err != nil {
		return fmt.Errorf("expected workspace source: %w", err)
	}
	if err := requirePathBeneath(workspaceRoot, p.Mounts[0].Source); err != nil {
		return fmt.Errorf("workspace mount: %w", err)
	}
	if !dockercli.CanonicalHostBindSourceEqual(p.Mounts[0].Source, p.ExpectedWorkspaceSource) {
		return fmt.Errorf("workspace mount does not match the resolved workspace source")
	}
	for _, mount := range p.Mounts {
		lowerSource := strings.ToLower(filepath.ToSlash(mount.Source))
		lowerTarget := strings.ToLower(filepath.ToSlash(mount.Target))
		if strings.Contains(lowerSource, "docker.sock") || strings.Contains(lowerTarget, "docker.sock") || strings.Contains(lowerTarget, "containerd.sock") {
			return fmt.Errorf("runtime sockets may not be mounted")
		}
		if _, err := dockercli.RestrictedBindMountArgument(mount.Source, mount.Target, mount.ReadOnly); err != nil {
			return fmt.Errorf("workspace mount: %w", err)
		}
	}
	if len(p.Entrypoint) == 0 || !isCanonicalAbsoluteGuestPath(p.Entrypoint[0]) {
		return fmt.Errorf("guest entrypoint is required")
	}
	return nil
}

type BuildConfig struct {
	WorkspaceRoot   string
	WorkspacePath   func(domain.WorkspaceID) string
	ImageRepository string
	GuestBinary     string
	ContainerUser   string
}

func BuildContainerPlan(input ports.AgentWorkspacePlan, config BuildConfig) (ContainerPlan, error) {
	if err := input.Validate(); err != nil {
		return ContainerPlan{}, err
	}
	if config.WorkspaceRoot == "" || config.ImageRepository == "" {
		return ContainerPlan{}, fmt.Errorf("workspace root and image repository are required")
	}
	guest := configuredGuestBinary(config.GuestBinary)
	user := configuredContainerUser(config.ContainerUser)
	workspacePath := filepath.Join(config.WorkspaceRoot, input.Workspace.ID().String(), "merged")
	if config.WorkspacePath != nil {
		workspacePath = config.WorkspacePath(input.Workspace.ID())
	}
	generation := input.Generation.Spec()
	plan := ContainerPlan{
		Name:                    containerName(generation.AgentWorkspaceID, generation.Generation),
		LeaseID:                 input.LeaseID,
		AgentWorkspaceID:        generation.AgentWorkspaceID,
		Generation:              generation.Generation,
		WorkspaceID:             generation.WorkspaceID,
		Image:                   strings.TrimSuffix(config.ImageRepository, "@") + "@" + input.ImageDigest.String(),
		Runtime:                 dockercli.RuncRuntime,
		PolicyDigest:            input.PolicyDigest,
		CapabilityDigest:        input.CapabilityFingerprintDigest,
		Resources:               input.Resources.Clone(),
		Mounts:                  []Mount{{Source: workspacePath, Target: WorkspaceMount}},
		ExpectedWorkspaceSource: workspacePath,
		Labels: map[string]string{
			"world.role":              agentRoleLabel,
			"world.lease":             input.LeaseID.String(),
			"world.agent-workspace":   generation.AgentWorkspaceID.String(),
			"world.agent-generation":  strconv.FormatUint(uint64(generation.Generation), 10),
			"world.workspace":         generation.WorkspaceID.String(),
			"world.policy-digest":     input.PolicyDigest.String(),
			"world.capability-digest": input.CapabilityFingerprintDigest.String(),
		},
		Entrypoint:      []string{guest},
		ReadOnlyRoot:    true,
		NoNewPrivileges: true,
		SeccompProfile:  dockercli.RuntimeDefaultSeccompProfile,
		User:            user,
	}
	if err := setPlanDigest(&plan); err != nil {
		return ContainerPlan{}, err
	}
	if err := plan.Validate(config.WorkspaceRoot); err != nil {
		return ContainerPlan{}, err
	}
	return plan, nil
}

func setPlanDigest(plan *ContainerPlan) error {
	digest, err := containerPlanDigest(*plan)
	if err != nil {
		return fmt.Errorf("compute agent container plan identity: %w", err)
	}
	plan.Labels[planDigestLabel] = digest.String()
	return nil
}

func containerPlanDigest(plan ContainerPlan) (domain.Digest, error) {
	type identity struct {
		Name                    string
		LeaseID                 string
		AgentWorkspaceID        string
		Generation              uint64
		WorkspaceID             string
		Image                   string
		Runtime                 string
		PolicyDigest            string
		CapabilityDigest        string
		Resources               admission.Resources
		Mounts                  []Mount
		ExpectedWorkspaceSource string
		Entrypoint              []string
		ReadOnlyRoot            bool
		Privileged              bool
		HostPID                 bool
		HostIPC                 bool
		HostNetwork             bool
		HostCgroup              bool
		Devices                 []string
		Capabilities            []string
		NoNewPrivileges         bool
		SeccompProfile          string
		User                    string
	}
	return dockercli.PlanDigest("agent-workspace", identity{
		Name: plan.Name, LeaseID: plan.LeaseID.String(), AgentWorkspaceID: plan.AgentWorkspaceID.String(), Generation: uint64(plan.Generation),
		WorkspaceID: plan.WorkspaceID.String(), Image: plan.Image, Runtime: plan.Runtime, PolicyDigest: plan.PolicyDigest.String(), CapabilityDigest: plan.CapabilityDigest.String(),
		Resources: plan.Resources.Clone(), Mounts: append([]Mount(nil), plan.Mounts...), ExpectedWorkspaceSource: plan.ExpectedWorkspaceSource,
		Entrypoint: append([]string(nil), plan.Entrypoint...), ReadOnlyRoot: plan.ReadOnlyRoot, Privileged: plan.Privileged,
		HostPID: plan.HostPID, HostIPC: plan.HostIPC, HostNetwork: plan.HostNetwork, HostCgroup: plan.HostCgroup,
		Devices: append([]string(nil), plan.Devices...), Capabilities: append([]string(nil), plan.Capabilities...),
		NoNewPrivileges: plan.NoNewPrivileges, SeccompProfile: plan.SeccompProfile, User: plan.User,
	})
}

// sameAgentWorkspacePlanIdentity compares the complete semantic Provision
// payload. The physical container digest remains an independent runtime
// authority boundary and deliberately cannot erase higher-level provenance.
func sameAgentWorkspacePlanIdentity(existing, requested ports.AgentWorkspacePlan) (bool, error) {
	existingDigest, err := ports.AgentWorkspacePlanIdentityDigest(existing)
	if err != nil {
		return false, err
	}
	requestedDigest, err := ports.AgentWorkspacePlanIdentityDigest(requested)
	if err != nil {
		return false, err
	}
	return existingDigest == requestedDigest, nil
}

func validatePlanLabels(plan ContainerPlan) error {
	digest, err := containerPlanDigest(plan)
	if err != nil {
		return err
	}
	expected := map[string]string{
		"world.role": agentRoleLabel, "world.lease": plan.LeaseID.String(), "world.agent-workspace": plan.AgentWorkspaceID.String(),
		"world.agent-generation": strconv.FormatUint(uint64(plan.Generation), 10), "world.workspace": plan.WorkspaceID.String(),
		"world.policy-digest": plan.PolicyDigest.String(), "world.capability-digest": plan.CapabilityDigest.String(), planDigestLabel: digest.String(),
	}
	if !dockercli.ExactWorldLabels(plan.Labels, expected) {
		return fmt.Errorf("world labels do not exactly identify the agent container plan")
	}
	return nil
}

func configuredGuestBinary(value string) string {
	if value == "" {
		return defaultGuestBinary
	}
	return value
}

func configuredContainerUser(value string) string {
	if value == "" {
		return defaultGuestUser
	}
	return value
}

func isCanonicalAbsoluteGuestPath(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value && strings.IndexByte(value, 0) < 0
}

func (p ContainerPlan) DockerCreateArgs() ([]string, error) {
	resourceArguments, err := dockercli.ResourceLimitArguments(p.Resources.CPUMilli, p.Resources.MemoryBytes, p.Resources.SwapBytes, p.Resources.PIDs)
	if err != nil {
		return nil, err
	}
	args := []string{"create", "--name", p.Name, "--runtime", p.Runtime, "--init", "--interactive", "--read-only"}
	args = append(args, dockercli.RestrictedLifecycleArguments(p.Name)...)
	args = append(args, dockercli.PrivateNamespaceArguments()...)
	args = append(args, "--cap-drop", "ALL")
	args = append(args, dockercli.HardenedSecurityArguments()...)
	args = append(args, "--user", p.User, "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,mode=1777")
	args = append(args, resourceArguments...)
	for _, mount := range p.Mounts {
		value, err := dockercli.RestrictedBindMountArgument(mount.Source, mount.Target, mount.ReadOnly)
		if err != nil {
			return nil, err
		}
		args = append(args, "--mount", value)
	}
	names := make([]string, 0, len(p.Labels))
	for name := range p.Labels {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--label", name+"="+p.Labels[name])
	}
	args = append(args, "--entrypoint", p.Entrypoint[0], p.Image)
	args = append(args, p.Entrypoint[1:]...)
	return args, nil
}

func containerName(id domain.AgentWorkspaceID, generation domain.AgentGeneration) string {
	return "world-agent-" + id.UUID() + "-g" + strconv.FormatUint(uint64(generation), 10)
}

func requirePathBeneath(root, candidate string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("path is outside configured root")
	}
	return nil
}
