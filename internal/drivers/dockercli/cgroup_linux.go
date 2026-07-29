//go:build linux

package dockercli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ContainerCgroupIdentityAuthority describes the strongest cgroup identity
// claim this host implementation can make. An empty runtime CgroupID means
// either that the container is stopped or that the engine process is outside
// this host PID namespace.
func ContainerCgroupIdentityAuthority() string {
	return "host-procfs-v2-exact-or-unavailable"
}

// resolveContainerCgroupID resolves only host-visible native Linux authority.
// A missing /proc entry means the engine PID is not available in this host PID
// namespace (for example a remote or VM-backed engine), so no ID is claimed.
// Valid cgroup-v1 membership is accepted but has multiple controller paths, so
// no single synthetic CgroupID is reported.
func resolveContainerCgroupID(pid int, containerID string) (cgroupID string, resultErr error) {
	if pid <= 0 {
		return "", nil
	}
	cgroupFile := filepath.Join("/proc", strconv.Itoa(pid), "cgroup")
	file, err := os.Open(cgroupFile)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("open %s: %w", cgroupFile, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close %s: %w", cgroupFile, err))
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", cgroupFile, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular procfs membership file", cgroupFile)
	}
	membership, err := parseExactDockerCgroupMembership(file, containerID)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", cgroupFile, err)
	}
	if membership.Version == "1" {
		return "", nil
	}
	return membership.Path, nil
}
