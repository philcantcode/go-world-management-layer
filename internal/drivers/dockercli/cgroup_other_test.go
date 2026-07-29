//go:build !linux

package dockercli

import (
	"strings"
	"testing"
)

func TestResolveContainerCgroupIDIsUnavailableOutsideLinux(t *testing.T) {
	got, err := resolveContainerCgroupID(4242, strings.Repeat("a", 64))
	if err != nil || got != "" {
		t.Fatalf("non-Linux cgroup identity = %q, %v", got, err)
	}
}

func TestContainerCgroupIdentityAuthorityIsExplicitlyUnavailable(t *testing.T) {
	if got := ContainerCgroupIdentityAuthority(); got != "unavailable-on-non-linux-host" {
		t.Fatalf("cgroup identity authority = %q", got)
	}
}
