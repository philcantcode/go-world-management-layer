//go:build !linux

package dockercli

import (
	"context"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func requirePlatformDockerCgroupIdentity(t *testing.T, _ context.Context, _ command.Runner, _ string, container Container) {
	t.Helper()
	if !container.Running || container.CgroupID != "" {
		t.Fatalf("non-Linux or VM-backed engine claimed a host cgroup identity: %#v", container)
	}
}
