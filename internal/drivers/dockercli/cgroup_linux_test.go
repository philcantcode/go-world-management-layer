//go:build linux

package dockercli

import (
	"math"
	"os"
	"strings"
	"testing"
)

func TestResolveContainerCgroupIDTreatsMissingHostPIDAsUnavailable(t *testing.T) {
	got, err := resolveContainerCgroupID(math.MaxInt32, strings.Repeat("a", 64))
	if err != nil || got != "" {
		t.Fatalf("missing host PID cgroup = %q, %v", got, err)
	}
}

func TestResolveContainerCgroupIDNeverAttributesUnboundHostProcess(t *testing.T) {
	got, err := resolveContainerCgroupID(os.Getpid(), strings.Repeat("b", 64))
	if err == nil || got != "" {
		t.Fatalf("unbound host process cgroup = %q, %v", got, err)
	}
}

func TestContainerCgroupIdentityAuthorityIsNativeAndFailClosed(t *testing.T) {
	if got := ContainerCgroupIdentityAuthority(); got != "host-procfs-v2-exact-or-unavailable" {
		t.Fatalf("cgroup identity authority = %q", got)
	}
}
