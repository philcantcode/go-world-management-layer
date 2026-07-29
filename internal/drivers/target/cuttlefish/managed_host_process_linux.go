//go:build linux

package cuttlefish

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"golang.org/x/sys/unix"
)

type linuxManagedHostProcessAuthority struct{}

type linuxManagedHostProcess struct {
	pid            int
	executablePath string
	startToken     string

	mu     sync.Mutex
	pidfd  int
	closed bool
}

type linuxProcessIdentity struct {
	executablePath string
	startToken     string
	state          byte
}

func newManagedHostProcessAuthority() managedHostProcessAuthority {
	return linuxManagedHostProcessAuthority{}
}

func (linuxManagedHostProcessAuthority) ResolveExecutable(emulatorBinary string) (string, error) {
	return canonicalLinuxConfiguredExecutable(emulatorBinary)
}

func (linuxManagedHostProcessAuthority) Preflight(emulatorBinary string) error {
	if _, err := canonicalLinuxConfiguredExecutable(emulatorBinary); err != nil {
		return fmt.Errorf("resolve configured emulator executable: %w", err)
	}
	pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("Linux pidfd authority is unavailable: %w", err)
	}
	if err := unix.Close(pidfd); err != nil {
		return fmt.Errorf("close Linux pidfd authority preflight handle: %w", err)
	}
	return nil
}

func (linuxManagedHostProcessAuthority) Kind() string { return "linux_pidfd" }

func (linuxManagedHostProcessAuthority) ResourcesEnforced() bool { return false }

func (linuxManagedHostProcessAuthority) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}

func (linuxManagedHostProcessAuthority) PreflightResources(context.Context, admission.Resources) error {
	return fmt.Errorf("managed emulator host process-tree CPU and memory containment is unsupported on Linux")
}

func (linuxManagedHostProcessAuthority) StartContained(context.Context, command.Starter, command.Invocation, Instance) (command.Process, error) {
	return nil, fmt.Errorf("managed emulator host process-tree CPU and memory containment is unsupported on Linux")
}

func (linuxManagedHostProcessAuthority) Open(pid int, emulatorBinary, pidFile string, storage managedDataStorageBinding, instance Instance) (managedHostProcess, error) {
	if err := storage.validate(instance); err != nil {
		return nil, fmt.Errorf("validate exact managed data binding: %w", errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	if pid <= 0 {
		return nil, fmt.Errorf("managed emulator process PID %d is invalid", pid)
	}
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		if errors.Is(err, unix.ESRCH) {
			return nil, fmt.Errorf("managed emulator process PID %d does not exist: %w", pid, errManagedHostProcessNotFound)
		}
		return nil, fmt.Errorf("open exact pidfd for managed emulator process PID %d: %w", pid, err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = unix.Close(pidfd)
		}
	}()

	configuredExecutable, err := canonicalLinuxConfiguredExecutable(emulatorBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve configured emulator executable: %w", err)
	}
	identity, err := readLinuxProcessIdentity(pid)
	if err != nil {
		return nil, linuxManagedProcessOpenError(pid, pidfd, "inspect identity", err)
	}
	if err := validateLinuxManagedExecutable(configuredExecutable, identity.executablePath); err != nil {
		return nil, fmt.Errorf("validate managed emulator process PID %d: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	arguments, err := readLinuxProcessArguments(pid)
	if err != nil {
		return nil, linuxManagedProcessOpenError(pid, pidfd, "inspect command line", err)
	}
	if err := requireExactManagedRuntimeArguments(arguments, pidFile, storage.BackingPath, instance, false); err != nil {
		return nil, fmt.Errorf("validate managed emulator process PID %d launch binding: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	startAfterArguments, _, err := readLinuxProcessStat(pid)
	if err != nil {
		return nil, linuxManagedProcessOpenError(pid, pidfd, "recheck identity after command-line inspection", err)
	}
	if startAfterArguments != identity.startToken {
		return nil, fmt.Errorf("managed emulator process PID %d identity changed during command-line inspection: %w", pid, errManagedHostProcessIdentityMismatch)
	}
	if linuxProcessExited(identity.state) {
		return nil, fmt.Errorf("managed emulator process PID %d has already exited (state %q): %w", pid, identity.state, errManagedHostProcessNotFound)
	}
	running, err := linuxPidfdRunning(pidfd)
	if err != nil {
		return nil, fmt.Errorf("inspect exact pidfd for managed emulator process PID %d: %w", pid, err)
	}
	if !running {
		return nil, fmt.Errorf("managed emulator process PID %d exited while it was opened: %w", pid, errManagedHostProcessNotFound)
	}
	process := &linuxManagedHostProcess{
		pid:            pid,
		executablePath: identity.executablePath,
		startToken:     identity.startToken,
		pidfd:          pidfd,
	}
	closeOnError = false
	return process, nil
}

func readLinuxProcessArguments(pid int) ([]string, error) {
	content, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 || content[len(content)-1] != 0 {
		return nil, fmt.Errorf("malformed /proc/%d/cmdline", pid)
	}
	parts := strings.Split(string(content[:len(content)-1]), "\x00")
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("malformed empty argument in /proc/%d/cmdline", pid)
		}
	}
	return parts, nil
}

func linuxManagedProcessOpenError(pid, pidfd int, operation string, cause error) error {
	running, pollErr := linuxPidfdRunning(pidfd)
	if pollErr == nil && !running {
		return fmt.Errorf("%s for managed emulator process PID %d after it exited: %w", operation, pid, errors.Join(cause, errManagedHostProcessNotFound))
	}
	return fmt.Errorf("%s for managed emulator process PID %d: %w", operation, pid, cause)
}

func (p *linuxManagedHostProcess) PID() int {
	return p.pid
}

func (p *linuxManagedHostProcess) ExecutablePath() string {
	return p.executablePath
}

func (p *linuxManagedHostProcess) StartToken() string {
	return p.startToken
}

func (p *linuxManagedHostProcess) Running() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, fmt.Errorf("managed emulator process PID %d authority is closed", p.pid)
	}
	running, err := linuxPidfdRunning(p.pidfd)
	if err != nil {
		return false, fmt.Errorf("poll exact pidfd for managed emulator process PID %d: %w", p.pid, err)
	}
	if !running {
		return false, nil
	}
	identity, exists, err := p.currentIdentity()
	if err != nil || !exists {
		return false, err
	}
	if linuxProcessExited(identity.state) {
		return false, nil
	}
	return linuxPidfdRunning(p.pidfd)
}

func (p *linuxManagedHostProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("managed emulator process PID %d authority is closed", p.pid)
	}
	running, err := linuxPidfdRunning(p.pidfd)
	if err != nil {
		return fmt.Errorf("poll exact pidfd for managed emulator process PID %d before termination: %w", p.pid, err)
	}
	if !running {
		return nil
	}
	identity, exists, err := p.currentIdentity()
	if err != nil || !exists {
		return err
	}
	if linuxProcessExited(identity.state) {
		return nil
	}
	if err := unix.PidfdSendSignal(p.pidfd, unix.SIGKILL, nil, 0); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return nil
		}
		return fmt.Errorf("terminate exact managed emulator process PID %d through pidfd: %w", p.pid, err)
	}
	return nil
}

func (p *linuxManagedHostProcess) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if err := unix.Close(p.pidfd); err != nil {
		return fmt.Errorf("close exact pidfd for managed emulator process PID %d: %w", p.pid, err)
	}
	p.pidfd = -1
	p.closed = true
	return nil
}

func (p *linuxManagedHostProcess) currentIdentity() (linuxProcessIdentity, bool, error) {
	identity, err := readLinuxProcessIdentity(p.pid)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			return linuxProcessIdentity{}, false, nil
		}
		return linuxProcessIdentity{}, false, fmt.Errorf("inspect managed emulator process PID %d identity: %w", p.pid, err)
	}
	if identity.executablePath != p.executablePath || identity.startToken != p.startToken {
		return linuxProcessIdentity{}, false, fmt.Errorf(
			"managed emulator process PID %d identity changed: executable=%q start_token=%q, expected executable=%q start_token=%q",
			p.pid,
			identity.executablePath,
			identity.startToken,
			p.executablePath,
			p.startToken,
		)
	}
	return identity, true, nil
}

func readLinuxProcessIdentity(pid int) (linuxProcessIdentity, error) {
	startBefore, _, err := readLinuxProcessStat(pid)
	if err != nil {
		return linuxProcessIdentity{}, err
	}
	executableTarget, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe"))
	if err != nil {
		return linuxProcessIdentity{}, err
	}
	executablePath, err := canonicalLinuxProcessExecutablePath(executableTarget)
	if err != nil {
		return linuxProcessIdentity{}, err
	}
	startAfter, state, err := readLinuxProcessStat(pid)
	if err != nil {
		return linuxProcessIdentity{}, err
	}
	if startBefore != startAfter {
		return linuxProcessIdentity{}, fmt.Errorf("process identity changed while it was inspected")
	}
	return linuxProcessIdentity{executablePath: executablePath, startToken: startAfter, state: state}, nil
}

func canonicalLinuxProcessExecutablePath(path string) (string, error) {
	canonical, err := canonicalLinuxExecutablePath(path)
	if err == nil {
		return canonical, nil
	}
	const deletedSuffix = " (deleted)"
	if !strings.HasSuffix(path, deletedSuffix) {
		return "", err
	}
	// Linux annotates /proc/<pid>/exe after its inode is unlinked. Preserve
	// authority over that already-open process by comparing its original,
	// absolute image path together with the immutable start token.
	absolute, absoluteErr := filepath.Abs(strings.TrimSuffix(path, deletedSuffix))
	if absoluteErr != nil {
		return "", absoluteErr
	}
	return filepath.Clean(absolute), nil
}

func readLinuxProcessStat(pid int) (string, byte, error) {
	content, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", 0, err
	}
	text := string(content)
	open := strings.IndexByte(text, '(')
	close := strings.LastIndex(text, ") ")
	if open <= 0 || close <= open {
		return "", 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	statPID, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil || statPID != pid {
		return "", 0, fmt.Errorf("unexpected PID in /proc/%d/stat", pid)
	}
	fields := strings.Fields(text[close+2:])
	// fields begins at proc(5) field 3 (state); starttime is field 22.
	if len(fields) <= 19 || len(fields[0]) != 1 {
		return "", 0, fmt.Errorf("malformed /proc/%d/stat fields", pid)
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", 0, fmt.Errorf("invalid /proc/%d/stat start time: %w", pid, err)
	}
	return fields[19], fields[0][0], nil
}

func linuxPidfdRunning(pidfd int) (bool, error) {
	if pidfd < 0 || uint64(pidfd) > uint64(^uint32(0)>>1) {
		return false, fmt.Errorf("invalid pidfd %d", pidfd)
	}
	fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	for {
		count, err := unix.Poll(fds, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, err
		}
		if count == 0 {
			return true, nil
		}
		events := fds[0].Revents
		if events&unix.POLLNVAL != 0 {
			return false, fmt.Errorf("pidfd is invalid")
		}
		if events&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) != 0 {
			return false, nil
		}
		return false, fmt.Errorf("pidfd poll returned unexpected events %#x", events)
	}
}

func linuxProcessExited(state byte) bool {
	return state == 'Z' || state == 'X' || state == 'x'
}

func canonicalLinuxConfiguredExecutable(emulatorBinary string) (string, error) {
	if strings.TrimSpace(emulatorBinary) == "" {
		return "", fmt.Errorf("configured emulator executable is empty")
	}
	resolved, err := exec.LookPath(emulatorBinary)
	if err != nil {
		return "", err
	}
	return canonicalLinuxExecutablePath(resolved)
}

func canonicalLinuxExecutablePath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return filepath.Clean(canonical), nil
}

func validateLinuxManagedExecutable(configuredExecutable, processExecutable string) error {
	if configuredExecutable == processExecutable {
		return nil
	}
	if !strings.HasPrefix(filepath.Base(processExecutable), "qemu-system-") {
		return fmt.Errorf("executable %q is not configured emulator %q or qemu-system-*", processExecutable, configuredExecutable)
	}
	root := filepath.Dir(configuredExecutable)
	if err := requirePathWithin(root, processExecutable, false); err != nil {
		return fmt.Errorf("qemu executable %q is outside configured emulator directory tree %q", processExecutable, root)
	}
	return nil
}
