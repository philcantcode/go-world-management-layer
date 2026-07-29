//go:build !windows

package process

import (
	"context"
	"io"
	"os"
	"os/exec"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func startDetachedCollector(ctx context.Context, invocation command.Invocation) (command.Process, error) {
	cmd := exec.Command(invocation.Program, append([]string(nil), invocation.Args...)...)
	configureCollectorParentDeathSignal(cmd)
	cmd.Dir = invocation.Directory
	if invocation.Environment != nil {
		cmd.Env = append([]string(nil), invocation.Environment...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	process := &detachedProcess{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr}
	if err := ctx.Err(); err != nil {
		_ = process.Kill()
		_ = process.Wait()
		return nil, err
	}
	return process, nil
}

type detachedProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *detachedProcess) Stdin() io.WriteCloser         { return p.stdin }
func (p *detachedProcess) Stdout() io.ReadCloser         { return p.stdout }
func (p *detachedProcess) Stderr() io.ReadCloser         { return p.stderr }
func (p *detachedProcess) Wait() error                   { return p.cmd.Wait() }
func (p *detachedProcess) Signal(signal os.Signal) error { return p.cmd.Process.Signal(signal) }
func (p *detachedProcess) Kill() error                   { return p.cmd.Process.Kill() }
