//go:build !linux

package dockercli

// ContainerCgroupIdentityAuthority makes the absence of native host cgroup
// authority explicit to capability consumers on Docker Desktop and other
// non-Linux hosts.
func ContainerCgroupIdentityAuthority() string {
	return "unavailable-on-non-linux-host"
}

// Docker's Linux engine PID is not authoritative in a non-Linux host process
// namespace (including Docker Desktop), so the cgroup identity is unavailable.
func resolveContainerCgroupID(int, string) (string, error) {
	return "", nil
}
