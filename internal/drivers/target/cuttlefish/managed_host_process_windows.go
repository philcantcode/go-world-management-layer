//go:build windows

package cuttlefish

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"golang.org/x/sys/windows"
)

const managedProcessImagePathCharacters = 32768

type windowsManagedHostProcessAuthority struct{}

type windowsManagedHostProcess struct {
	pid            int
	executablePath string
	startToken     string

	mu       sync.Mutex
	handle   windows.Handle
	job      windows.Handle
	anchored bool
	closed   bool
}

func newManagedHostProcessAuthority() managedHostProcessAuthority {
	return windowsManagedHostProcessAuthority{}
}

func (windowsManagedHostProcessAuthority) ResolveExecutable(emulatorBinary string) (string, error) {
	return canonicalWindowsConfiguredExecutable(emulatorBinary)
}

func (windowsManagedHostProcessAuthority) Preflight(emulatorBinary string) error {
	if _, err := canonicalWindowsConfiguredExecutable(emulatorBinary); err != nil {
		return fmt.Errorf("resolve configured emulator executable: %w", err)
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, windows.GetCurrentProcessId())
	if err != nil {
		return fmt.Errorf("open current process for command-line authority preflight: %w", err)
	}
	defer windows.CloseHandle(handle)
	if _, err := queryWindowsProcessArguments(handle); err != nil {
		return fmt.Errorf("query process command line during authority preflight: %w", err)
	}
	return nil
}

func (windowsManagedHostProcessAuthority) Kind() string { return managedWindowsResourceAuthority }

func (windowsManagedHostProcessAuthority) ResourcesEnforced() bool { return true }

func (windowsManagedHostProcessAuthority) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}

func (windowsManagedHostProcessAuthority) PreflightResources(ctx context.Context, resources admission.Resources) error {
	return preflightWindowsManagedJob(ctx, resources)
}

func (windowsManagedHostProcessAuthority) StartContained(ctx context.Context, _ command.Starter, invocation command.Invocation, instance Instance) (command.Process, error) {
	return startWindowsManagedJobProcess(ctx, invocation, instance)
}

func (windowsManagedHostProcessAuthority) Open(pid int, emulatorBinary, pidFile string, storage managedDataStorageBinding, instance Instance) (managedHostProcess, error) {
	if err := storage.validate(instance); err != nil {
		return nil, fmt.Errorf("validate exact managed data binding: %w", errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	if pid <= 0 || uint64(pid) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("managed emulator process PID %d is invalid", pid)
	}
	handle, err := windows.OpenProcess(
		windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE|windows.PROCESS_TERMINATE|windows.PROCESS_DUP_HANDLE,
		false,
		uint32(pid),
	)
	if err != nil {
		if err == windows.ERROR_INVALID_PARAMETER {
			return nil, fmt.Errorf("managed emulator process PID %d does not exist: %w", pid, errManagedHostProcessNotFound)
		}
		return nil, fmt.Errorf("open managed emulator process PID %d: %w", pid, err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = windows.CloseHandle(handle)
		}
	}()

	configuredExecutable, err := canonicalWindowsConfiguredExecutable(emulatorBinary)
	if err != nil {
		return nil, fmt.Errorf("resolve configured emulator executable: %w", err)
	}
	executablePath, err := queryCanonicalWindowsProcessImage(handle)
	if err != nil {
		return nil, windowsManagedProcessOpenError(pid, handle, "query executable", err)
	}
	if err := validateWindowsManagedExecutable(configuredExecutable, executablePath); err != nil {
		return nil, fmt.Errorf("validate managed emulator process PID %d: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	arguments, err := queryWindowsProcessArguments(handle)
	if err != nil {
		return nil, windowsManagedProcessOpenError(pid, handle, "query command line", err)
	}
	if err := requireExactManagedRuntimeArguments(arguments, pidFile, storage.BackingPath, instance, true); err != nil {
		return nil, fmt.Errorf("validate managed emulator process PID %d launch binding: %w", pid, errors.Join(err, errManagedHostProcessIdentityMismatch))
	}
	startToken, err := queryWindowsProcessStartToken(handle)
	if err != nil {
		return nil, windowsManagedProcessOpenError(pid, handle, "query start token", err)
	}
	running, err := windowsProcessHandleRunning(handle)
	if err != nil {
		return nil, fmt.Errorf("inspect managed emulator process PID %d after opening it: %w", pid, err)
	}
	if !running {
		return nil, fmt.Errorf("managed emulator process PID %d has already exited: %w", pid, errManagedHostProcessNotFound)
	}
	job, err := openWindowsManagedJob(pid, instance)
	if err != nil {
		return nil, fmt.Errorf("verify managed emulator process PID %d resource containment: %w", pid, err)
	}
	process := &windowsManagedHostProcess{
		pid:            pid,
		executablePath: executablePath,
		startToken:     startToken,
		handle:         handle,
		job:            job,
	}
	closeOnError = false
	return process, nil
}

func windowsManagedProcessOpenError(pid int, handle windows.Handle, operation string, cause error) error {
	running, waitErr := windowsProcessHandleRunning(handle)
	if waitErr == nil && !running {
		return fmt.Errorf("%s for managed emulator process PID %d after it exited: %w", operation, pid, errors.Join(cause, errManagedHostProcessNotFound))
	}
	return fmt.Errorf("%s for managed emulator process PID %d: %w", operation, pid, cause)
}

func (p *windowsManagedHostProcess) PID() int {
	return p.pid
}

func (p *windowsManagedHostProcess) ExecutablePath() string {
	return p.executablePath
}

func (p *windowsManagedHostProcess) StartToken() string {
	return p.startToken
}

func (p *windowsManagedHostProcess) AnchorResourceAuthority() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("managed emulator process PID %d authority is closed", p.pid)
	}
	if p.anchored {
		return nil
	}
	_, err := duplicateWindowsJobQueryHandle(p.job, p.handle, false)
	if err != nil {
		return fmt.Errorf("duplicate reduced-rights named Job handle into managed emulator PID %d: %w", p.pid, err)
	}
	// remote is meaningful only in the target process. It is intentionally
	// left there as the named Job lifetime anchor and closes with that exact
	// process; closing the numeric value in this process would be incorrect.
	p.anchored = true
	return nil
}

func (p *windowsManagedHostProcess) Running() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, fmt.Errorf("managed emulator process PID %d authority is closed", p.pid)
	}
	return windowsProcessHandleRunning(p.handle)
}

func (p *windowsManagedHostProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("managed emulator process PID %d authority is closed", p.pid)
	}
	running, err := windowsProcessHandleRunning(p.handle)
	if err != nil {
		return fmt.Errorf("inspect managed emulator process PID %d before termination: %w", p.pid, err)
	}
	if !running {
		return nil
	}
	if err := windows.TerminateJobObject(p.job, 1); err != nil {
		stillRunning, inspectErr := windowsProcessHandleRunning(p.handle)
		if inspectErr == nil && !stillRunning {
			return nil
		}
		return fmt.Errorf("terminate managed emulator process PID %d: %w", p.pid, err)
	}
	return nil
}

func (p *windowsManagedHostProcess) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	err := errors.Join(windows.CloseHandle(p.handle), windows.CloseHandle(p.job))
	if err != nil {
		return fmt.Errorf("close managed emulator process PID %d authority: %w", p.pid, err)
	}
	p.handle = 0
	p.job = 0
	p.closed = true
	return nil
}

func windowsProcessHandleRunning(handle windows.Handle) (bool, error) {
	const waitTimeout = uint32(258)
	result, err := windows.WaitForSingleObject(handle, 0)
	if err != nil {
		return false, err
	}
	switch result {
	case windows.WAIT_OBJECT_0:
		return false, nil
	case waitTimeout:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected process wait result %#x", result)
	}
}

func queryCanonicalWindowsProcessImage(handle windows.Handle) (string, error) {
	buffer := make([]uint16, managedProcessImagePathCharacters)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(handle, 0, &buffer[0], &size); err != nil {
		return "", err
	}
	if size == 0 || size > uint32(len(buffer)) {
		return "", fmt.Errorf("process image query returned invalid path length %d", size)
	}
	return canonicalWindowsExecutablePath(windows.UTF16ToString(buffer[:size]))
}

func queryWindowsProcessArguments(handle windows.Handle) ([]string, error) {
	var required uint32
	_ = windows.NtQueryInformationProcess(handle, windows.ProcessCommandLineInformation, nil, 0, &required)
	const maximumCommandLineBytes = uint32(1 << 20)
	if required < uint32(unsafe.Sizeof(windows.NTUnicodeString{})) || required > maximumCommandLineBytes {
		return nil, fmt.Errorf("process command-line query returned invalid required size %d", required)
	}
	buffer := make([]byte, required)
	if err := windows.NtQueryInformationProcess(
		handle,
		windows.ProcessCommandLineInformation,
		unsafe.Pointer(&buffer[0]),
		uint32(len(buffer)),
		&required,
	); err != nil {
		return nil, err
	}
	commandLine := (*windows.NTUnicodeString)(unsafe.Pointer(&buffer[0]))
	if commandLine.Length == 0 || commandLine.Length%2 != 0 || commandLine.Buffer == nil {
		return nil, fmt.Errorf("process command-line query returned a malformed UTF-16 string")
	}
	base := uintptr(unsafe.Pointer(&buffer[0]))
	end := base + uintptr(len(buffer))
	textStart := uintptr(unsafe.Pointer(commandLine.Buffer))
	textEnd := textStart + uintptr(commandLine.Length)
	if textStart < base || textEnd < textStart || textEnd > end {
		return nil, fmt.Errorf("process command-line query returned a string outside its result buffer")
	}
	units := unsafe.Slice(commandLine.Buffer, int(commandLine.Length/2))
	arguments, err := windows.DecomposeCommandLine(windows.UTF16ToString(units))
	if err != nil {
		return nil, err
	}
	if len(arguments) == 0 {
		return nil, fmt.Errorf("process command-line query returned no arguments")
	}
	return arguments, nil
}

func queryWindowsProcessStartToken(handle windows.Handle) (string, error) {
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return "", err
	}
	ticks := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	return strconv.FormatUint(ticks, 10), nil
}

func canonicalWindowsConfiguredExecutable(emulatorBinary string) (string, error) {
	if strings.TrimSpace(emulatorBinary) == "" {
		return "", fmt.Errorf("configured emulator executable is empty")
	}
	resolved, err := exec.LookPath(emulatorBinary)
	if err != nil {
		return "", err
	}
	return canonicalWindowsExecutablePath(resolved)
}

func canonicalWindowsExecutablePath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	pathPointer, err := windows.UTF16PtrFromString(absolute)
	if err != nil {
		return "", err
	}
	handle, err := windows.CreateFile(
		pathPointer,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	buffer := make([]uint16, managedProcessImagePathCharacters)
	length, err := windows.GetFinalPathNameByHandle(handle, &buffer[0], uint32(len(buffer)), 0)
	if err != nil {
		return "", err
	}
	if length == 0 || length >= uint32(len(buffer)) {
		return "", fmt.Errorf("final executable path query returned invalid length %d", length)
	}
	canonical := windows.UTF16ToString(buffer[:length])
	if strings.HasPrefix(canonical, `\\?\UNC\`) {
		canonical = `\\` + strings.TrimPrefix(canonical, `\\?\UNC\`)
	} else {
		canonical = strings.TrimPrefix(canonical, `\\?\`)
	}
	if !filepath.IsAbs(canonical) {
		return "", fmt.Errorf("final executable path %q is not absolute", canonical)
	}
	return filepath.Clean(canonical), nil
}

func validateWindowsManagedExecutable(configuredExecutable, processExecutable string) error {
	if strings.EqualFold(configuredExecutable, processExecutable) {
		return nil
	}
	if !strings.HasPrefix(strings.ToLower(filepath.Base(processExecutable)), "qemu-system-") {
		return fmt.Errorf("executable %q is not configured emulator %q or qemu-system-*", processExecutable, configuredExecutable)
	}
	root := filepath.Dir(configuredExecutable)
	if err := requirePathWithin(root, processExecutable, false); err != nil {
		return fmt.Errorf("qemu executable %q is outside configured emulator directory tree %q", processExecutable, root)
	}
	return nil
}
