//go:build windows

package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf16"
	"unsafe"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"golang.org/x/sys/windows"
)

const (
	windowsCollectorJobListAttribute = uintptr(0x0002000d)
	windowsCollectorStillActive      = uint32(259)
	windowsCollectorWaitTimeout      = uint32(258)
)

var windowsCollectorIsProcessInJob = windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob")

type windowsCollectorJobAccounting struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

// windowsCollectorJobProcess owns one anonymous, non-inherited Job handle.
// Wait observes only the directly launched collector so an early parent exit
// remains visible even while a descendant is still active in the Job.
type windowsCollectorJobProcess struct {
	process windows.Handle
	job     windows.Handle
	pid     uint32
	stdin   *os.File
	stdout  *os.File
	stderr  *os.File

	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error

	processMu sync.Mutex
	jobMu     sync.Mutex
	jobClosed bool
}

func startDetachedCollector(ctx context.Context, invocation command.Invocation) (command.Process, error) {
	process, err := startWindowsCollectorJobProcess(ctx, invocation)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		cleanupErr := abortWindowsCollectorProcess(process, defaultCleanupGrace)
		return nil, errors.Join(err, cleanupErr)
	}
	return process, nil
}

// startWindowsCollectorJobProcess creates the Job before the process and binds
// membership through PROC_THREAD_ATTRIBUTE_JOB_LIST. The inherited-handle
// allow-list contains only the three standard streams: the child never owns a
// Job handle that could keep the tree alive after daemon death.
func startWindowsCollectorJobProcess(ctx context.Context, invocation command.Invocation) (*windowsCollectorJobProcess, error) {
	if err := invocation.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	executable, err := exec.LookPath(invocation.Program)
	if err != nil {
		return nil, fmt.Errorf("resolve collector executable %q: %w", invocation.Program, err)
	}
	if executable, err = filepath.Abs(executable); err != nil {
		return nil, fmt.Errorf("resolve absolute collector executable: %w", err)
	}

	job, err := createWindowsCollectorJob()
	if err != nil {
		return nil, err
	}
	closeJob := true
	defer func() {
		if closeJob {
			_ = windows.CloseHandle(job)
		}
	}()

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		return nil, err
	}
	parentFiles := []*os.File{stdinWriter, stdoutReader, stderrReader}
	childFiles := []*os.File{stdinReader, stdoutWriter, stderrWriter}
	closeFiles := func(files []*os.File) error {
		var result error
		for _, file := range files {
			if file != nil {
				result = errors.Join(result, file.Close())
			}
		}
		return result
	}
	closePipes := true
	defer func() {
		if closePipes {
			_ = closeFiles(parentFiles)
			_ = closeFiles(childFiles)
		}
	}()

	childHandles := []windows.Handle{
		windows.Handle(stdinReader.Fd()),
		windows.Handle(stdoutWriter.Fd()),
		windows.Handle(stderrWriter.Fd()),
	}
	for _, handle := range childHandles {
		if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
			return nil, fmt.Errorf("make collector standard stream inheritable: %w", err)
		}
	}
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return nil, fmt.Errorf("allocate collector process attributes: %w", err)
	}
	defer attributes.Delete()
	if err := attributes.Update(
		windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
		unsafe.Pointer(&childHandles[0]),
		uintptr(len(childHandles))*unsafe.Sizeof(childHandles[0]),
	); err != nil {
		return nil, fmt.Errorf("bind collector inherited handles: %w", err)
	}
	jobs := []windows.Handle{job}
	if err := attributes.Update(
		windowsCollectorJobListAttribute,
		unsafe.Pointer(&jobs[0]),
		unsafe.Sizeof(jobs[0]),
	); err != nil {
		return nil, fmt.Errorf("bind collector atomic Job membership: %w", err)
	}

	applicationName, err := windows.UTF16PtrFromString(executable)
	if err != nil {
		return nil, err
	}
	commandLine, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(append([]string{invocation.Program}, invocation.Args...)))
	if err != nil {
		return nil, err
	}
	var directory *uint16
	if invocation.Directory != "" {
		directory, err = windows.UTF16PtrFromString(invocation.Directory)
		if err != nil {
			return nil, err
		}
	}
	environment, err := windowsCollectorEnvironmentBlock(invocation.Environment)
	if err != nil {
		return nil, err
	}
	var environmentPointer *uint16
	if len(environment) > 0 {
		environmentPointer = &environment[0]
	}
	startup := windows.StartupInfoEx{
		StartupInfo: windows.StartupInfo{
			Cb:       uint32(unsafe.Sizeof(windows.StartupInfoEx{})),
			Flags:    windows.STARTF_USESTDHANDLES,
			StdInput: childHandles[0], StdOutput: childHandles[1], StdErr: childHandles[2],
		},
		ProcThreadAttributeList: attributes.List(),
	}
	var information windows.ProcessInformation
	if err := windows.CreateProcess(
		applicationName,
		commandLine,
		nil,
		nil,
		true,
		windows.CREATE_UNICODE_ENVIRONMENT|windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_NO_WINDOW,
		environmentPointer,
		directory,
		&startup.StartupInfo,
		&information,
	); err != nil {
		return nil, fmt.Errorf("create collector in exact Windows Job: %w", err)
	}
	_ = windows.CloseHandle(information.Thread)
	process := &windowsCollectorJobProcess{
		process:  information.Process,
		job:      job,
		pid:      information.ProcessId,
		stdin:    stdinWriter,
		stdout:   stdoutReader,
		stderr:   stderrReader,
		waitDone: make(chan struct{}),
	}
	// From this point the wrapper owns every parent stream and the Job/process
	// handles. Error paths must tear it down through that single authority.
	closePipes = false
	closeJob = false
	if closeErr := closeFiles(childFiles); closeErr != nil {
		cleanupErr := abortWindowsCollectorProcess(process, defaultCleanupGrace)
		return nil, errors.Join(fmt.Errorf("close inherited collector stream handles: %w", closeErr), cleanupErr)
	}
	childFiles = nil

	inJob, membershipErr := windowsCollectorProcessInJob(information.Process, job)
	if membershipErr != nil || !inJob {
		if membershipErr == nil {
			membershipErr = errors.New("created collector is outside its exact Windows Job")
		}
		cleanupErr := abortWindowsCollectorProcess(process, defaultCleanupGrace)
		return nil, errors.Join(fmt.Errorf("verify collector atomic Job membership: %w", membershipErr), cleanupErr)
	}
	return process, nil
}

func createWindowsCollectorJob() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("create collector Windows Job: %w", err)
	}
	if err := windows.SetHandleInformation(job, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("make collector Windows Job non-inheritable: %w", err)
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("set collector Job kill-on-close limit: %w", err)
	}
	var observed windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if err := windows.QueryInformationJobObject(
		job,
		int32(windows.JobObjectExtendedLimitInformation),
		uintptr(unsafe.Pointer(&observed)),
		uint32(unsafe.Sizeof(observed)),
		nil,
	); err != nil {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("query collector Job limits: %w", err)
	}
	if observed.BasicLimitInformation.LimitFlags != windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("collector Job limits do not exactly enforce kill-on-close without breakaway")
	}
	return job, nil
}

func windowsCollectorProcessInJob(process, job windows.Handle) (bool, error) {
	var result int32
	call, _, callErr := windowsCollectorIsProcessInJob.Call(
		uintptr(process),
		uintptr(job),
		uintptr(unsafe.Pointer(&result)),
	)
	if call == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return false, callErr
	}
	return result != 0, nil
}

func windowsCollectorEnvironmentBlock(environment []string) ([]uint16, error) {
	if environment == nil {
		return nil, nil
	}
	values := append([]string(nil), environment...)
	for _, value := range values {
		if strings.ContainsRune(value, 0) || !strings.Contains(value, "=") {
			return nil, fmt.Errorf("collector environment contains a malformed entry")
		}
	}
	sort.SliceStable(values, func(left, right int) bool {
		return strings.ToUpper(values[left]) < strings.ToUpper(values[right])
	})
	return append(utf16.Encode([]rune(strings.Join(values, "\x00")+"\x00")), 0), nil
}

func (p *windowsCollectorJobProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *windowsCollectorJobProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *windowsCollectorJobProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *windowsCollectorJobProcess) Wait() error {
	p.waitOnce.Do(func() {
		defer close(p.waitDone)
		defer func() {
			_ = p.stdin.Close()
			p.processMu.Lock()
			if p.process != 0 {
				_ = windows.CloseHandle(p.process)
				p.process = 0
			}
			p.processMu.Unlock()
		}()
		p.processMu.Lock()
		process := p.process
		p.processMu.Unlock()
		if process == 0 {
			p.waitErr = os.ErrProcessDone
			return
		}
		result, err := windows.WaitForSingleObject(process, windows.INFINITE)
		if err != nil {
			p.waitErr = err
			return
		}
		if result != windows.WAIT_OBJECT_0 {
			p.waitErr = fmt.Errorf("collector process wait returned %#x", result)
			return
		}
		var exitCode uint32
		if err := windows.GetExitCodeProcess(process, &exitCode); err != nil {
			p.waitErr = err
			return
		}
		if exitCode == windowsCollectorStillActive {
			p.waitErr = fmt.Errorf("collector process remained active after signaled wait")
			return
		}
		if exitCode != 0 {
			p.waitErr = fmt.Errorf("collector process exited with status %d", exitCode)
		}
	})
	<-p.waitDone
	return p.waitErr
}

func (p *windowsCollectorJobProcess) Signal(signal os.Signal) error {
	if signal == os.Kill {
		return p.Kill()
	}
	return fmt.Errorf("Windows collector Job supports only KILL")
}

func (p *windowsCollectorJobProcess) Kill() error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()
	if p.jobClosed || p.job == 0 {
		return os.ErrProcessDone
	}
	if err := windows.TerminateJobObject(p.job, 1); err != nil {
		active, queryErr := queryWindowsCollectorJobActiveProcesses(p.job)
		if queryErr == nil && active == 0 {
			return nil
		}
		return fmt.Errorf("terminate collector Windows Job: %w", errors.Join(err, queryErr))
	}
	return nil
}

func (p *windowsCollectorJobProcess) ConfirmCleanup(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		p.jobMu.Lock()
		if p.jobClosed || p.job == 0 {
			p.jobMu.Unlock()
			return true, nil
		}
		active, err := queryWindowsCollectorJobActiveProcesses(p.job)
		p.jobMu.Unlock()
		if err != nil {
			return false, err
		}
		if active == 0 {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}
	}
}

func (p *windowsCollectorJobProcess) CloseContainment() error {
	p.jobMu.Lock()
	defer p.jobMu.Unlock()
	if p.jobClosed {
		return nil
	}
	if p.job == 0 {
		p.jobClosed = true
		return nil
	}
	if err := windows.CloseHandle(p.job); err != nil {
		return err
	}
	p.job = 0
	p.jobClosed = true
	return nil
}

func queryWindowsCollectorJobActiveProcesses(job windows.Handle) (uint32, error) {
	var information windowsCollectorJobAccounting
	if err := windows.QueryInformationJobObject(
		job,
		int32(windows.JobObjectBasicAccountingInformation),
		uintptr(unsafe.Pointer(&information)),
		uint32(unsafe.Sizeof(information)),
		nil,
	); err != nil {
		return 0, err
	}
	return information.ActiveProcesses, nil
}

func (p *windowsCollectorJobProcess) terminateDirectProcess() error {
	p.processMu.Lock()
	defer p.processMu.Unlock()
	if p.process == 0 {
		return nil
	}
	if err := windows.TerminateProcess(p.process, 1); err != nil && !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return err
	}
	return nil
}

func abortWindowsCollectorProcess(process *windowsCollectorJobProcess, grace time.Duration) error {
	var result error
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		result = errors.Join(result, err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	timer := time.NewTimer(grace)
	select {
	case <-waitDone:
		timer.Stop()
	case <-timer.C:
		result = errors.Join(result, errors.New("collector direct process did not exit during rollback"), process.terminateDirectProcess())
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	confirmed, cleanupErr := confirmAndCloseCollectorContainment(cleanupContext, process)
	result = errors.Join(result, cleanupErr)
	if !confirmed {
		// Returning no process transfers no authority to the caller. Closing the
		// sole handle is therefore mandatory even when the accounting query did
		// not complete; kill-on-close remains the final fail-safe.
		result = errors.Join(result, errors.New("collector Job cleanup was not confirmed during rollback"), process.CloseContainment())
	}
	_ = process.stdout.Close()
	_ = process.stderr.Close()
	return result
}

func preflightWindowsCollectorJob(ctx context.Context) error {
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		return fmt.Errorf("resolve Windows system directory: %w", err)
	}
	process, err := startWindowsCollectorJobProcess(ctx, command.Invocation{
		Program: filepath.Join(systemDirectory, "cmd.exe"),
		Args:    []string{"/d", "/q", "/c", "set /p OBSERVER_JOB_PREFLIGHT="},
	})
	if err != nil {
		return fmt.Errorf("prove atomic Windows collector Job creation: %w", err)
	}
	defer process.Stdout().Close()
	defer process.Stderr().Close()
	process.processMu.Lock()
	processHandle := process.process
	process.processMu.Unlock()
	state, err := windows.WaitForSingleObject(processHandle, 0)
	if err != nil || state != windowsCollectorWaitTimeout {
		cleanupErr := abortWindowsCollectorProcess(process, defaultCleanupGrace)
		return errors.Join(fmt.Errorf("collector Job preflight child was not running: state=%#x err=%v", state, err), cleanupErr)
	}
	// Closing the daemon's sole Job handle must itself terminate the member.
	// This live check proves both the configured flag and actual host behavior.
	if err := process.CloseContainment(); err != nil {
		cleanupErr := abortWindowsCollectorProcess(process, defaultCleanupGrace)
		return errors.Join(fmt.Errorf("close preflight collector Job: %w", err), cleanupErr)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	select {
	case <-ctx.Done():
		_ = process.terminateDirectProcess()
		return fmt.Errorf("collector Job kill-on-close preflight: %w", ctx.Err())
	case <-waitDone:
		return nil
	}
}

var _ command.Process = (*windowsCollectorJobProcess)(nil)
var _ collectorContainment = (*windowsCollectorJobProcess)(nil)
