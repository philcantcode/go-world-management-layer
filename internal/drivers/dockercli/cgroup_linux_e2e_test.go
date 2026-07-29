//go:build linux

package dockercli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func requirePlatformDockerCgroupIdentity(t *testing.T, ctx context.Context, runner command.Runner, containerID string, container Container) {
	t.Helper()
	if !container.Running || container.CgroupID == "" {
		t.Fatalf("running native Linux container lacks exact cgroup identity: %#v", container)
	}
	pidResult, err := runner.Run(ctx, command.Invocation{Program: "docker", Args: []string{"inspect", "--format", "{{.State.Pid}}", containerID}})
	if err != nil {
		t.Fatalf("inspect qualification container PID independently: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidResult.Stdout)))
	if err != nil || pid <= 0 {
		t.Fatalf("qualification container PID = %q, %v", pidResult.Stdout, err)
	}
	membership, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		t.Fatalf("read qualification container cgroup independently: %v", err)
	}
	wantMembership := []byte(fmt.Sprintf("0::%s\n", container.CgroupID))
	if !bytes.Equal(membership, wantMembership) {
		t.Fatalf("host cgroup membership = %q, want %q", membership, wantMembership)
	}
}
