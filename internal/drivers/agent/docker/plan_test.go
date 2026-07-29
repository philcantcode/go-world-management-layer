package docker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
)

func TestContainerPlanRejectsInfrastructureAuthority(t *testing.T) {
	root := t.TempDir()
	plan := validContainerPlan(t, root)
	if err := os.MkdirAll(plan.ExpectedWorkspaceSource, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(root); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*ContainerPlan){
		"privileged":      func(p *ContainerPlan) { p.Privileged = true },
		"host pid":        func(p *ContainerPlan) { p.HostPID = true },
		"host network":    func(p *ContainerPlan) { p.HostNetwork = true },
		"device":          func(p *ContainerPlan) { p.Devices = []string{"/dev/kvm"} },
		"capability":      func(p *ContainerPlan) { p.Capabilities = []string{"SYS_ADMIN"} },
		"socket":          func(p *ContainerPlan) { p.Mounts[0].Source = filepath.Join(root, "docker.sock") },
		"arbitrary bind":  func(p *ContainerPlan) { p.Mounts = append(p.Mounts, Mount{Source: root, Target: "/host"}) },
		"other workspace": func(p *ContainerPlan) { p.Mounts[0].Source = filepath.Join(root, "other", "merged") },
		"outside root":    func(p *ContainerPlan) { p.Mounts[0].Source = filepath.Dir(root) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := plan
			candidate.Mounts = append([]Mount(nil), plan.Mounts...)
			mutate(&candidate)
			if candidate.Validate(root) == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
	arguments, err := plan.DockerCreateArgs()
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(arguments, " ")
	for _, forbidden := range []string{"--privileged", "--pid=host", "--network host", "--device", "docker.sock"} {
		if strings.Contains(args, forbidden) {
			t.Fatalf("Docker args contain %q: %s", forbidden, args)
		}
	}
}

func TestContainerPlanRequiresEveryDockerEnforcedLimit(t *testing.T) {
	root := t.TempDir()
	plan := validContainerPlan(t, root)
	tests := map[string]func(*ContainerPlan){
		"cpu":     func(p *ContainerPlan) { p.Resources.CPUMilli = 0 },
		"memory":  func(p *ContainerPlan) { p.Resources.MemoryBytes = 0 },
		"pids":    func(p *ContainerPlan) { p.Resources.PIDs = 0 },
		"seccomp": func(p *ContainerPlan) { p.SeccompProfile = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := plan
			mutate(&candidate)
			if err := candidate.Validate(root); err == nil {
				t.Fatal("unbounded or unhardened plan was accepted")
			}
		})
	}
	arguments, err := plan.DockerCreateArgs()
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(arguments, " ")
	for _, expected := range []string{"--memory 67108864", "--memory-swap 67108864", "--cpus 0.250", "--pids-limit 64", "seccomp=builtin", "--ipc private", "--cgroupns private", "--env " + dockercli.RestrictedPathEnvironment, "--workdir /"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("Docker args do not contain %q: %s", expected, joined)
		}
	}
}

func TestContainerPlanRequiresNumericUnprivilegedIdentityAndCanonicalGuest(t *testing.T) {
	root := t.TempDir()
	plan := validContainerPlan(t, root)
	for _, user := range []string{"root", "0:0", "65532", "-1:65532", "65532:0"} {
		candidate := plan
		candidate.User = user
		if err := candidate.Validate(root); err == nil {
			t.Fatalf("container user %q was accepted", user)
		}
	}
	for _, guest := range []string{"world-guest", "/usr/local/bin/../bin/world-guest", "/usr/local/bin/world-guest\x00other"} {
		candidate := plan
		candidate.Entrypoint = []string{guest}
		if err := candidate.Validate(root); err == nil {
			t.Fatalf("guest binary %q was accepted", guest)
		}
	}
}

func validContainerPlan(t *testing.T, root string) ContainerPlan {
	t.Helper()
	lease, _ := domain.NewLeaseID()
	workspace, _ := domain.NewWorkspaceID()
	agent, _ := domain.NewAgentWorkspaceID()
	digest := domain.NewDigest([]byte("digest"))
	source := filepath.Join(root, workspace.String(), "merged")
	plan := ContainerPlan{Name: "world-agent", LeaseID: lease, AgentWorkspaceID: agent, Generation: 1, WorkspaceID: workspace, Image: "example.invalid/agent@" + digest.String(), Runtime: dockercli.RuncRuntime, PolicyDigest: digest, CapabilityDigest: digest, Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, PIDs: 64}, Mounts: []Mount{{Source: source, Target: WorkspaceMount}}, ExpectedWorkspaceSource: source, Labels: map[string]string{
		"world.role": agentRoleLabel, "world.lease": lease.String(), "world.agent-workspace": agent.String(), "world.agent-generation": "1",
		"world.workspace": workspace.String(), "world.policy-digest": digest.String(), "world.capability-digest": digest.String(),
	}, Entrypoint: []string{"/world-guest"}, ReadOnlyRoot: true, NoNewPrivileges: true, SeccompProfile: dockercli.RuntimeDefaultSeccompProfile, User: "65532:65532"}
	if err := setPlanDigest(&plan); err != nil {
		t.Fatal(err)
	}
	return plan
}
