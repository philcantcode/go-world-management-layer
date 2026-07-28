package linuxcontainer

import (
	"fmt"
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
	TargetMount         = "/target"
	TargetMaterialMount = "/target/input"
	defaultTargetUser   = "65532:65532"
	materialDirectory   = "material"
	writableDirectory   = "writable"
	targetRoleLabel     = "linux-target"
	planDigestLabel     = "world.plan-digest"
)

type ContainerPlan struct {
	Name             string
	LeaseID          domain.LeaseID
	TargetID         domain.TargetID
	Generation       domain.TargetGeneration
	Image            string
	Runtime          string
	TargetDirectory  string
	PolicyDigest     domain.Digest
	CapabilityDigest domain.Digest
	Resources        admission.Resources
	Labels           map[string]string
	Privileged       bool
	HostPID          bool
	HostIPC          bool
	HostNetwork      bool
	HostCgroup       bool
	Devices          []string
	MountSources     []string
	Capabilities     []string
	User             string
	ReadOnlyRoot     bool
	NoNewPrivileges  bool
	SeccompProfile   string
}

func (p ContainerPlan) Validate(targetRoot string) error {
	if p.Name == "" || p.LeaseID.IsZero() || p.TargetID.IsZero() || !p.Generation.IsValid() {
		return fmt.Errorf("target identity and generation are required")
	}
	if p.Image == "" || !strings.Contains(p.Image, "@sha256:") {
		return fmt.Errorf("target image must be digest pinned")
	}
	if _, ok := targetIsolationProfile(p.Runtime); !ok {
		return fmt.Errorf("target runtime must be runc, gvisor, or kata")
	}
	if p.PolicyDigest.IsZero() || p.CapabilityDigest.IsZero() {
		return fmt.Errorf("policy and capability digests are required")
	}
	if err := p.Resources.Validate(); err != nil {
		return err
	}
	if _, err := dockercli.ResourceLimitArguments(p.Resources.CPUMilli, p.Resources.MemoryBytes, p.Resources.SwapBytes, p.Resources.PIDs); err != nil {
		return fmt.Errorf("target resource limits: %w", err)
	}
	if err := validatePlanLabels(p); err != nil {
		return err
	}
	if p.Privileged || p.HostPID || p.HostIPC || p.HostNetwork || p.HostCgroup || len(p.Devices) != 0 {
		return fmt.Errorf("privileged mode, host namespaces, and devices are forbidden")
	}
	if !p.ReadOnlyRoot || !p.NoNewPrivileges || p.SeccompProfile != dockercli.RuntimeDefaultSeccompProfile {
		return fmt.Errorf("read-only root, no-new-privileges, and runtime-default seccomp are required")
	}
	if _, _, err := dockercli.ParseNumericUser(p.User); err != nil {
		return fmt.Errorf("target container user: %w", err)
	}
	if err := requirePathBeneath(targetRoot, p.TargetDirectory); err != nil {
		return fmt.Errorf("target directory: %w", err)
	}
	expectedDirectory := filepath.Join(targetRoot, p.TargetID.String(), "generations", strconv.FormatUint(uint64(p.Generation), 10))
	if filepath.Clean(p.TargetDirectory) != filepath.Clean(expectedDirectory) {
		return fmt.Errorf("target directory does not match target and generation")
	}
	expectedMountSources := []string{p.writableRoot(), p.materialRoot()}
	if len(p.MountSources) != len(expectedMountSources) {
		return fmt.Errorf("only target-private writable state and material may be mounted")
	}
	for index := range expectedMountSources {
		if filepath.Clean(p.MountSources[index]) != filepath.Clean(expectedMountSources[index]) {
			return fmt.Errorf("mount source %d does not match the target layout", index)
		}
	}
	for _, source := range p.MountSources {
		if err := requirePathBeneath(targetRoot, source); err != nil {
			return fmt.Errorf("mount source: %w", err)
		}
		lower := strings.ToLower(filepath.ToSlash(source))
		if strings.Contains(lower, "docker.sock") || strings.Contains(lower, "containerd.sock") || strings.Contains(lower, "/workspace/") {
			return fmt.Errorf("runtime sockets and agent workspaces may not be mounted")
		}
	}
	for _, capability := range p.Capabilities {
		if capability != "SYS_PTRACE" {
			return fmt.Errorf("target capability %q is not in the visibility profile", capability)
		}
	}
	return nil
}

type BuildConfig struct {
	TargetRoot      string
	ImageRepository string
	AllowPtrace     bool
	ContainerUser   string
}

func BuildContainerPlan(input ports.TargetPlan, config BuildConfig) (ContainerPlan, error) {
	if err := input.Validate(); err != nil {
		return ContainerPlan{}, err
	}
	if input.Template.Kind != domain.TargetLinuxContainer {
		return ContainerPlan{}, fmt.Errorf("Linux container driver requires a linux_container template")
	}
	if input.Template.Driver != "docker" {
		return ContainerPlan{}, fmt.Errorf("Linux container driver requires the Docker physical driver")
	}
	if isolationProfile, ok := targetIsolationProfile(input.Template.Runtime); !ok || isolationProfile != input.Template.IsolationProfile {
		return ContainerPlan{}, fmt.Errorf("target runtime and isolation profile are not a supported physical pair")
	}
	if config.TargetRoot == "" || config.ImageRepository == "" {
		return ContainerPlan{}, fmt.Errorf("target root and image repository are required")
	}
	generation := input.Generation.Spec()
	directory := filepath.Join(config.TargetRoot, generation.TargetID.String(), "generations", strconv.FormatUint(uint64(generation.Generation), 10))
	capabilities := []string(nil)
	if config.AllowPtrace {
		capabilities = []string{"SYS_PTRACE"}
	}
	plan := ContainerPlan{
		Name:             targetContainerName(generation.TargetID, generation.Generation),
		LeaseID:          input.LeaseID,
		TargetID:         generation.TargetID,
		Generation:       generation.Generation,
		Image:            strings.TrimSuffix(config.ImageRepository, "@") + "@" + input.Template.ImageDigest.String(),
		Runtime:          input.Template.Runtime,
		TargetDirectory:  directory,
		PolicyDigest:     input.PolicyDigest,
		CapabilityDigest: input.CapabilityFingerprintDigest,
		Resources:        input.Resources.Clone(),
		Capabilities:     capabilities,
		User:             configuredTargetUser(config.ContainerUser),
		ReadOnlyRoot:     true,
		NoNewPrivileges:  true,
		SeccompProfile:   dockercli.RuntimeDefaultSeccompProfile,
		MountSources:     []string{filepath.Join(directory, writableDirectory), filepath.Join(directory, materialDirectory)},
		Labels: map[string]string{
			"world.role":              targetRoleLabel,
			"world.lease":             input.LeaseID.String(),
			"world.target":            generation.TargetID.String(),
			"world.target-generation": strconv.FormatUint(uint64(generation.Generation), 10),
			"world.policy-digest":     input.PolicyDigest.String(),
			"world.capability-digest": input.CapabilityFingerprintDigest.String(),
		},
	}
	if err := setPlanDigest(&plan); err != nil {
		return ContainerPlan{}, err
	}
	if err := plan.Validate(config.TargetRoot); err != nil {
		return ContainerPlan{}, err
	}
	return plan, nil
}

func setPlanDigest(plan *ContainerPlan) error {
	digest, err := containerPlanDigest(*plan)
	if err != nil {
		return fmt.Errorf("compute target container plan identity: %w", err)
	}
	plan.Labels[planDigestLabel] = digest.String()
	return nil
}

func containerPlanDigest(plan ContainerPlan) (domain.Digest, error) {
	type identity struct {
		Name             string
		LeaseID          string
		TargetID         string
		Generation       uint64
		Image            string
		Runtime          string
		TargetDirectory  string
		PolicyDigest     string
		CapabilityDigest string
		Resources        admission.Resources
		Privileged       bool
		HostPID          bool
		HostIPC          bool
		HostNetwork      bool
		HostCgroup       bool
		Devices          []string
		MountSources     []string
		Capabilities     []string
		User             string
		ReadOnlyRoot     bool
		NoNewPrivileges  bool
		SeccompProfile   string
	}
	return dockercli.PlanDigest("linux-target", identity{
		Name: plan.Name, LeaseID: plan.LeaseID.String(), TargetID: plan.TargetID.String(), Generation: uint64(plan.Generation), Image: plan.Image, Runtime: plan.Runtime,
		TargetDirectory: plan.TargetDirectory, PolicyDigest: plan.PolicyDigest.String(), CapabilityDigest: plan.CapabilityDigest.String(),
		Resources: plan.Resources.Clone(), Privileged: plan.Privileged, HostPID: plan.HostPID, HostIPC: plan.HostIPC,
		HostNetwork: plan.HostNetwork, HostCgroup: plan.HostCgroup, Devices: append([]string(nil), plan.Devices...),
		MountSources: append([]string(nil), plan.MountSources...), Capabilities: append([]string(nil), plan.Capabilities...),
		User: plan.User, ReadOnlyRoot: plan.ReadOnlyRoot, NoNewPrivileges: plan.NoNewPrivileges, SeccompProfile: plan.SeccompProfile,
	})
}

func validatePlanLabels(plan ContainerPlan) error {
	digest, err := containerPlanDigest(plan)
	if err != nil {
		return err
	}
	expected := map[string]string{
		"world.role": targetRoleLabel, "world.lease": plan.LeaseID.String(), "world.target": plan.TargetID.String(),
		"world.target-generation": strconv.FormatUint(uint64(plan.Generation), 10), "world.policy-digest": plan.PolicyDigest.String(),
		"world.capability-digest": plan.CapabilityDigest.String(), planDigestLabel: digest.String(),
	}
	if !dockercli.ExactWorldLabels(plan.Labels, expected) {
		return fmt.Errorf("world labels do not exactly identify the target container plan")
	}
	return nil
}

func (p ContainerPlan) DockerCreateArgs() ([]string, error) {
	resourceArguments, err := dockercli.ResourceLimitArguments(p.Resources.CPUMilli, p.Resources.MemoryBytes, p.Resources.SwapBytes, p.Resources.PIDs)
	if err != nil {
		return nil, err
	}
	args := []string{"create", "--name", p.Name, "--runtime", p.Runtime, "--init", "--read-only"}
	args = append(args, dockercli.PrivateNamespaceArguments()...)
	args = append(args, "--cap-drop", "ALL")
	args = append(args, dockercli.HardenedSecurityArguments()...)
	args = append(args,
		"--user", p.User, "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,mode=1777",
		"--mount", "type=bind,src="+p.writableRoot()+",dst="+TargetMount,
		"--mount", "type=bind,src="+p.materialRoot()+",dst="+TargetMaterialMount+",readonly",
	)
	for _, capability := range p.Capabilities {
		args = append(args, "--cap-add", capability)
	}
	args = append(args, resourceArguments...)
	names := make([]string, 0, len(p.Labels))
	for name := range p.Labels {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--label", name+"="+p.Labels[name])
	}
	// This controlled lifecycle binary only keeps the target namespace alive;
	// research commands are passed as opaque argv through world-guest. Requiring
	// no shell makes a digest-pinned scratch image sufficient.
	args = append(args, "--entrypoint", "/usr/local/bin/world-idle", p.Image)
	return args, nil
}

func configuredTargetUser(value string) string {
	if value == "" {
		return defaultTargetUser
	}
	return value
}

func targetIsolationProfile(runtime string) (string, bool) {
	switch runtime {
	case dockercli.RuncRuntime:
		return "observable-container", true
	case "gvisor", "kata":
		return "sandboxed-kernel", true
	default:
		return "", false
	}
}

func (p ContainerPlan) materialRoot() string {
	return filepath.Join(p.TargetDirectory, materialDirectory)
}
func (p ContainerPlan) writableRoot() string {
	return filepath.Join(p.TargetDirectory, writableDirectory)
}

func targetContainerName(id domain.TargetID, generation domain.TargetGeneration) string {
	return "world-target-" + id.UUID() + "-g" + strconv.FormatUint(uint64(generation), 10)
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
		return fmt.Errorf("path is outside configured target root")
	}
	return nil
}
