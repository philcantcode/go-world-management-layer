//go:build linux

package main

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func startDetachedChild(readyPath, outputPath string, delay time.Duration) (int, error) {
	executable, err := os.Executable()
	if err != nil {
		return 0, err
	}
	command := exec.Command(executable,
		"-detached-child",
		"-detached-ready", readyPath,
		"-detached-output", outputPath,
		"-detached-delay", strconv.FormatInt(int64(delay), 10)+"ns",
	)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return 0, err
	}
	pid := command.Process.Pid
	if err := command.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}
