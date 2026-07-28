//go:build !linux && !windows

package guest

import (
	"context"
	"os"
	"os/exec"
)

func prepareCommand(*exec.Cmd) error { return nil }
func ownStartedProcess(command *exec.Cmd) (processOwner, error) {
	return &singleProcessOwner{process: command.Process}, nil
}

type singleProcessOwner struct{ process *os.Process }

func (owner *singleProcessOwner) Signal(name string) error {
	if name == "KILL" || name == "SIGKILL" {
		return owner.process.Kill()
	}
	return owner.process.Signal(os.Interrupt)
}
func (owner *singleProcessOwner) Terminate() error                             { return owner.process.Signal(os.Interrupt) }
func (owner *singleProcessOwner) Kill() error                                  { return owner.process.Kill() }
func (owner *singleProcessOwner) ConfirmCleanup(context.Context) (bool, error) { return true, nil }
func (owner *singleProcessOwner) Close() error                                 { return nil }
func processExitSignal(*exec.ExitError) string                                 { return "" }
