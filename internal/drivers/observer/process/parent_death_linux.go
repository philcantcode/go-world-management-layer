//go:build linux

package process

import (
	"os/exec"
	"syscall"
)

// Go's Linux fork/exec path installs Pdeathsig and then verifies that the
// recorded parent is still the current parent. If the parent died during that
// race, the directly spawned child signals itself before exec returns to user
// code. Pdeathsig is not process-tree containment: a collector that daemonizes
// or creates independently surviving descendants needs an external authority
// such as a cgroup.
func configureCollectorParentDeathSignal(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}

func collectorParentDeathSignalGuaranteed() bool { return true }
