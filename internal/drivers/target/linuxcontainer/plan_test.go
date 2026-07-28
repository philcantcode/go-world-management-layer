package linuxcontainer

import (
	"slices"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
)

func TestDockerCreateArgsUseControlledScratchCompatibleIdleBinary(t *testing.T) {
	plan := ContainerPlan{
		Image: "world-target@" + domain.NewDigest([]byte("image")).String(), Runtime: dockercli.RuncRuntime, User: defaultTargetUser,
		ReadOnlyRoot: true, NoNewPrivileges: true, SeccompProfile: dockercli.RuntimeDefaultSeccompProfile,
		Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, PIDs: 64},
	}
	arguments, err := plan.DockerCreateArgs()
	if err != nil {
		t.Fatal(err)
	}
	entrypoint := slices.Index(arguments, "--entrypoint")
	if entrypoint < 0 || entrypoint+2 >= len(arguments) {
		t.Fatalf("entrypoint arguments = %v", arguments)
	}
	if arguments[entrypoint+1] != "/usr/local/bin/world-idle" || arguments[entrypoint+2] != plan.Image {
		t.Fatalf("entrypoint arguments = %v", arguments[entrypoint:])
	}
	joined := strings.Join(arguments, "\x00")
	if strings.Contains(joined, "/bin/sh") || strings.Contains(joined, "sleep 3600") || strings.Contains(joined, "-c\x00") {
		t.Fatalf("target lifecycle still depends on a shell: %v", arguments)
	}
	for _, expected := range []string{"--read-only", "--user\x0065532:65532", "--memory-swap\x0067108864", "--security-opt\x00seccomp=builtin", "--ipc\x00private", "--cgroupns\x00private"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("target arguments do not contain %q: %v", expected, arguments)
		}
	}
}

func TestContainerPlanRejectsUnboundedOrUnhardenedTarget(t *testing.T) {
	root := t.TempDir()
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	plan := validLifecycleContainerPlan(t, root, lease, target, 1)
	if err := plan.Validate(root); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*ContainerPlan){
		"runtime":              func(p *ContainerPlan) { p.Runtime = "" },
		"cpu":                  func(p *ContainerPlan) { p.Resources.CPUMilli = 0 },
		"memory":               func(p *ContainerPlan) { p.Resources.MemoryBytes = 0 },
		"pids":                 func(p *ContainerPlan) { p.Resources.PIDs = 0 },
		"root filesystem":      func(p *ContainerPlan) { p.ReadOnlyRoot = false },
		"user":                 func(p *ContainerPlan) { p.User = "0:0" },
		"no new privileges":    func(p *ContainerPlan) { p.NoNewPrivileges = false },
		"seccomp":              func(p *ContainerPlan) { p.SeccompProfile = "" },
		"forbidden capability": func(p *ContainerPlan) { p.Capabilities = []string{"SYS_ADMIN"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := plan
			candidate.Labels = cloneStrings(plan.Labels)
			mutate(&candidate)
			if err := setPlanDigest(&candidate); err != nil {
				t.Fatal(err)
			}
			if err := candidate.Validate(root); err == nil {
				t.Fatal("unsafe target plan was accepted")
			}
		})
	}
}
