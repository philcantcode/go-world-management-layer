package dockercli

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

// TestDockerCgroupIdentityEndToEnd qualifies the platform contract against a
// real engine: native Linux resolves an exact v2 path, while hosts without
// host-visible Linux PID authority report the identity as unavailable.
func TestDockerCgroupIdentityEndToEnd(t *testing.T) {
	image := strings.TrimSpace(os.Getenv("WORLD_LINUX_TARGET_E2E_IMAGE"))
	if image == "" {
		t.Skip("WORLD_LINUX_TARGET_E2E_IMAGE is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	runner := command.OS{}
	created, err := runner.Run(ctx, command.Invocation{
		Program: "docker",
		Args:    []string{"create", "--entrypoint", "/usr/local/bin/world-idle", "--label", "world.cgroup-e2e=true", image},
	})
	if err != nil {
		t.Fatalf("create cgroup qualification container: %v", err)
	}
	containerID := strings.TrimSpace(string(created.Stdout))
	if err := RequireCanonicalContainerID(containerID); err != nil {
		t.Fatalf("Docker create identity: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = runner.Run(cleanup, command.Invocation{Program: "docker", Args: []string{"rm", "--force", containerID}})
	})
	if _, err := runner.Run(ctx, command.Invocation{Program: "docker", Args: []string{"start", containerID}}); err != nil {
		t.Fatalf("start cgroup qualification container: %v", err)
	}

	container, err := Inspect(ctx, "docker", runner, containerID)
	if err != nil {
		t.Fatalf("inspect running Docker cgroup identity: %v", err)
	}
	requirePlatformDockerCgroupIdentity(t, ctx, runner, containerID, container)

	if _, err := runner.Run(ctx, command.Invocation{Program: "docker", Args: []string{"stop", "--time", "10", containerID}}); err != nil {
		t.Fatalf("stop cgroup qualification container: %v", err)
	}
	stopped, err := Inspect(ctx, "docker", runner, containerID)
	if err != nil {
		t.Fatalf("inspect stopped qualification container: %v", err)
	}
	if stopped.Running || stopped.CgroupID != "" {
		t.Fatalf("stopped container retained cgroup identity: %#v", stopped)
	}
}
