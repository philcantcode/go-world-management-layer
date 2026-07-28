//go:build linux

package guest

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func prepareCommand(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func ownStartedProcess(command *exec.Cmd) (processOwner, error) {
	return &linuxProcessGroup{pgid: command.Process.Pid}, nil
}

type linuxProcessGroup struct{ pgid int }

func (group *linuxProcessGroup) Signal(name string) error {
	signal, err := linuxSignal(name)
	if err != nil {
		return err
	}
	return ignoreGone(syscall.Kill(-group.pgid, signal))
}

func (group *linuxProcessGroup) Terminate() error {
	return ignoreGone(syscall.Kill(-group.pgid, syscall.SIGTERM))
}
func (group *linuxProcessGroup) Kill() error {
	return ignoreGone(syscall.Kill(-group.pgid, syscall.SIGKILL))
}

func (group *linuxProcessGroup) ConfirmCleanup(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := syscall.Kill(-group.pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}
	}
}

func (group *linuxProcessGroup) Close() error { return nil }

func linuxSignal(name string) (syscall.Signal, error) {
	name = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(name)), "SIG")
	switch name {
	case "TERM":
		return syscall.SIGTERM, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "INT":
		return syscall.SIGINT, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	default:
		return 0, fmt.Errorf("unsupported signal %q", name)
	}
}

func ignoreGone(err error) error {
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func processExitSignal(exitError *exec.ExitError) string {
	status, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() {
		return ""
	}
	return status.Signal().String()
}
