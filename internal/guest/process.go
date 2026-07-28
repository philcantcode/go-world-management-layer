package guest

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sort"
	"time"
)

// OSLauncher starts a direct executable with no shell or provider parsing.
type OSLauncher struct{}

func (OSLauncher) Launch(spec ProcessSpec) (Process, error) {
	command := exec.Command(spec.Executable, spec.Argv...)
	command.Dir = spec.WorkingDirectory
	command.Env = environmentList(spec.Environment)
	if err := prepareCommand(command); err != nil {
		return nil, err
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, stderr, childOutputs, err := attachOutputPipes(command)
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	startedAt := time.Now()
	if err = command.Start(); err != nil {
		_ = closeAll(append(childOutputs, stdin, stdout, stderr)...)
		return nil, err
	}
	// Cmd.Wait closes pipes created by StdoutPipe/StderrPipe as soon as it
	// observes process exit, racing the output pumps and occasionally losing
	// trailing bytes. These are caller-owned os.Pipes instead: after Start the
	// parent closes only its duplicate write handles, and readers reach EOF when
	// the launched process (and any descendants) actually close their handles.
	if err := closeAll(childOutputs...); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = closeAll(stdin, stdout, stderr)
		return nil, err
	}
	owner, err := ownStartedProcess(command)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = closeAll(stdin, stdout, stderr)
		return nil, err
	}
	return &osProcess{command: command, owner: owner, stdin: stdin, stdout: stdout, stderr: stderr, startedAt: startedAt}, nil
}

func attachOutputPipes(command *exec.Cmd) (io.ReadCloser, io.ReadCloser, []io.Closer, error) {
	stdout, stdoutChild, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stderr, stderrChild, err := os.Pipe()
	if err != nil {
		_ = closeAll(stdout, stdoutChild)
		return nil, nil, nil, err
	}
	command.Stdout = stdoutChild
	command.Stderr = stderrChild
	return stdout, stderr, []io.Closer{stdoutChild, stderrChild}, nil
}

func closeAll(closers ...io.Closer) error {
	errs := make([]error, 0, len(closers))
	for _, closer := range closers {
		if closer != nil {
			errs = append(errs, closer.Close())
		}
	}
	return errors.Join(errs...)
}

func environmentList(environment map[string]string) []string {
	names := make([]string, 0, len(environment))
	for name := range environment {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+environment[name])
	}
	return result
}

type processOwner interface {
	Signal(string) error
	Terminate() error
	Kill() error
	ConfirmCleanup(context.Context) (bool, error)
	Close() error
}

type osProcess struct {
	command   *exec.Cmd
	owner     processOwner
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	startedAt time.Time
}

func (process *osProcess) Identity() ProcessIdentity {
	return ProcessIdentity{PID: int64(process.command.Process.Pid), ParentPID: int64(os.Getpid()), ProcessStartNS: process.startedAt.UnixNano()}
}
func (process *osProcess) Stdin() io.WriteCloser { return process.stdin }
func (process *osProcess) Stdout() io.ReadCloser { return process.stdout }
func (process *osProcess) Stderr() io.ReadCloser { return process.stderr }

func (process *osProcess) Wait() ProcessResult {
	err := process.command.Wait()
	result := ProcessResult{ExitCode: 0}
	if err == nil {
		return result
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
		result.Signal = processExitSignal(exitError)
		return result
	}
	result.ExitCode = -1
	result.Err = err
	return result
}

func (process *osProcess) Signal(name string) error { return process.owner.Signal(name) }
func (process *osProcess) Terminate() error         { return process.owner.Terminate() }
func (process *osProcess) Kill() error              { return process.owner.Kill() }
func (process *osProcess) ConfirmCleanup(ctx context.Context) (bool, error) {
	return process.owner.ConfirmCleanup(ctx)
}
func (process *osProcess) Close() error { return process.owner.Close() }
