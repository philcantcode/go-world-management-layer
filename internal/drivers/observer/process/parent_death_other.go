//go:build !linux && !windows

package process

import "os/exec"

func configureCollectorParentDeathSignal(*exec.Cmd) {}

func collectorParentDeathSignalGuaranteed() bool { return false }
