//go:build !linux

package process

import "os/exec"

func configureCollectorParentDeathSignal(*exec.Cmd) {}

func collectorParentDeathSignalGuaranteed() bool { return false }
