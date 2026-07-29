//go:build windows

package cuttlefish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"golang.org/x/sys/windows"
)

const (
	procThreadAttributeJobList     = uintptr(0x0002000d)
	jobObjectQuery                 = uint32(0x0004)
	jobObjectTerminate             = uint32(0x0008)
	jobObjectCPURateControlEnable  = uint32(0x1)
	jobObjectCPURateControlHardCap = uint32(0x4)
	windowsStillActive             = uint32(259)
)

var (
	kernel32CreateJobObject = windows.NewLazySystemDLL("kernel32.dll").NewProc("CreateJobObjectW")
	kernel32OpenJobObject   = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	kernel32IsProcessInJob  = windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob")
)

type windowsJobCPURateControlInformation struct {
	ControlFlags uint32
	CPURate      uint32
}

type windowsManagedJobProcess struct {
	process windows.Handle
	job     windows.Handle
	pid     uint32
	stdin   *os.File
	stdout  *os.File
	stderr  *os.File

	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error

	containmentMu     sync.Mutex
	containmentClosed bool
}

func preflightWindowsManagedJob(ctx context.Context, resources admission.Resources) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		return fmt.Errorf("resolve Windows system directory: %w", err)
	}
	process, err := startWindowsJobProcess(
		ctx,
		command.Invocation{Program: filepath.Join(systemDirectory, "cmd.exe"), Args: []string{"/d", "/c", "exit", "0"}},
		resources,
		"",
	)
	if err != nil {
		return fmt.Errorf("prove atomic Windows Job process creation: %w", err)
	}
	_ = process.Stdin().Close()
	_, _ = io.Copy(io.Discard, process.Stdout())
	_, _ = io.Copy(io.Discard, process.Stderr())
	waitErr := process.Wait()
	closeErr := process.CloseContainment()
	if waitErr != nil || closeErr != nil {
		return errors.Join(fmt.Errorf("Windows Job preflight child: %w", waitErr), closeErr)
	}
	return nil
}

func startWindowsManagedJobProcess(ctx context.Context, invocation command.Invocation, instance Instance) (command.Process, error) {
	return startWindowsJobProcess(ctx, invocation, instance.Resources, managedEmulatorResourceIdentity(instance))
}

func startWindowsJobProcess(ctx context.Context, invocation command.Invocation, resources admission.Resources, identity string) (*windowsManagedJobProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := invocation.Validate(); err != nil {
		return nil, err
	}
	job, err := createConfiguredWindowsJob(identity, resources)
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
	closeAll := func(files []*os.File) {
		for _, file := range files {
			_ = file.Close()
		}
	}
	closePipes := true
	defer func() {
		if closePipes {
			closeAll(parentFiles)
			closeAll(childFiles)
		}
	}()

	launcherJobAnchor, err := duplicateWindowsJobQueryHandle(job, windows.CurrentProcess(), true)
	if err != nil {
		return nil, fmt.Errorf("create reduced-rights managed emulator launcher Job anchor: %w", err)
	}
	closeLauncherJobAnchor := true
	defer func() {
		if closeLauncherJobAnchor {
			_ = windows.CloseHandle(launcherJobAnchor)
		}
	}()

	// The full Job handle is never inherited. The child receives only a query-
	// rights lifetime anchor, while PROC_THREAD_ATTRIBUTE_JOB_LIST below assigns
	// atomic membership. The exact QEMU successor later receives its own query-
	// only anchor after PID/executable/argument/membership/limit verification.
	childHandles := []windows.Handle{
		windows.Handle(stdinReader.Fd()), windows.Handle(stdoutWriter.Fd()), windows.Handle(stderrWriter.Fd()), launcherJobAnchor,
	}
	for _, handle := range childHandles {
		if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
			return nil, fmt.Errorf("make managed emulator pipe inheritable: %w", err)
		}
	}
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return nil, fmt.Errorf("allocate managed emulator process attributes: %w", err)
	}
	defer attributes.Delete()
	if err := attributes.Update(
		windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
		unsafe.Pointer(&childHandles[0]),
		uintptr(len(childHandles))*unsafe.Sizeof(childHandles[0]),
	); err != nil {
		return nil, fmt.Errorf("bind managed emulator inherited handles: %w", err)
	}
	jobs := []windows.Handle{job}
	if err := attributes.Update(procThreadAttributeJobList, unsafe.Pointer(&jobs[0]), unsafe.Sizeof(jobs[0])); err != nil {
		return nil, fmt.Errorf("bind managed emulator atomic Job membership: %w", err)
	}

	executable, err := windows.UTF16PtrFromString(invocation.Program)
	if err != nil {
		return nil, err
	}
	arguments, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(append([]string{invocation.Program}, invocation.Args...)))
	if err != nil {
		return nil, err
	}
	directory := (*uint16)(nil)
	if invocation.Directory != "" {
		directory, err = windows.UTF16PtrFromString(invocation.Directory)
		if err != nil {
			return nil, err
		}
	}
	environment, err := windowsEnvironmentBlock(invocation.Environment)
	if err != nil {
		return nil, err
	}
	startup := windows.StartupInfoEx{
		StartupInfo: windows.StartupInfo{
			Cb: uint32(unsafe.Sizeof(windows.StartupInfoEx{})), Flags: windows.STARTF_USESTDHANDLES,
			StdInput: childHandles[0], StdOutput: childHandles[1], StdErr: childHandles[2],
		},
		ProcThreadAttributeList: attributes.List(),
	}
	var process windows.ProcessInformation
	if err := windows.CreateProcess(
		executable,
		arguments,
		nil,
		nil,
		true,
		windows.CREATE_UNICODE_ENVIRONMENT|windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_NO_WINDOW,
		&environment[0],
		directory,
		&startup.StartupInfo,
		&process,
	); err != nil {
		return nil, fmt.Errorf("create managed emulator in exact Windows Job: %w", err)
	}
	_ = windows.CloseHandle(process.Thread)
	_ = windows.CloseHandle(launcherJobAnchor)
	closeLauncherJobAnchor = false
	closeAll(childFiles)
	closePipes = false
	closeJob = false
	return &windowsManagedJobProcess{
		process: process.Process, job: job, stdin: stdinWriter, stdout: stdoutReader, stderr: stderrReader,
		pid: process.ProcessId, waitDone: make(chan struct{}),
	}, nil
}

func duplicateWindowsJobQueryHandle(job, targetProcess windows.Handle, inheritable bool) (windows.Handle, error) {
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(
		windows.CurrentProcess(),
		job,
		targetProcess,
		&duplicate,
		jobObjectQuery,
		inheritable,
		0,
	); err != nil {
		return 0, err
	}
	if duplicate == 0 {
		return 0, fmt.Errorf("DuplicateHandle returned an invalid target Job handle")
	}
	return duplicate, nil
}

func windowsEnvironmentBlock(environment []string) ([]uint16, error) {
	if environment == nil {
		environment = os.Environ()
	}
	values := append([]string(nil), environment...)
	for _, value := range values {
		if strings.ContainsRune(value, 0) || !strings.Contains(value, "=") {
			return nil, fmt.Errorf("managed emulator environment contains a malformed entry")
		}
	}
	sort.SliceStable(values, func(left, right int) bool {
		return strings.ToUpper(values[left]) < strings.ToUpper(values[right])
	})
	return append(utf16.Encode([]rune(strings.Join(values, "\x00")+"\x00")), 0), nil
}

func createConfiguredWindowsJob(identity string, resources admission.Resources) (windows.Handle, error) {
	name := (*uint16)(nil)
	var err error
	if identity != "" {
		name, err = windows.UTF16PtrFromString(identity)
		if err != nil {
			return 0, err
		}
	}
	job, existed, err := createWindowsJobObject(name)
	if err != nil {
		return 0, fmt.Errorf("create managed emulator Windows Job: %w", err)
	}
	if identity != "" && existed {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("managed emulator Windows Job %q already exists", identity)
	}
	if err := configureWindowsJob(job, resources); err != nil {
		_ = windows.CloseHandle(job)
		return 0, err
	}
	return job, nil
}

func createWindowsJobObject(name *uint16) (windows.Handle, bool, error) {
	result, _, callErr := kernel32CreateJobObject.Call(0, uintptr(unsafe.Pointer(name)))
	if result == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return 0, false, callErr
	}
	return windows.Handle(result), callErr == windows.ERROR_ALREADY_EXISTS, nil
}

func openNamedWindowsJob(name *uint16) (windows.Handle, error) {
	result, _, callErr := kernel32OpenJobObject.Call(
		uintptr(jobObjectQuery|jobObjectTerminate), 0, uintptr(unsafe.Pointer(name)),
	)
	if result == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return 0, callErr
	}
	return windows.Handle(result), nil
}

func configureWindowsJob(job windows.Handle, resources admission.Resources) error {
	if resources.MemoryBytes <= 0 || uint64(resources.MemoryBytes) > uint64(^uintptr(0)) {
		return fmt.Errorf("managed emulator Job memory limit %d is unsupported by this host", resources.MemoryBytes)
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{JobMemoryLimit: uintptr(resources.MemoryBytes)}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_JOB_MEMORY
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		return fmt.Errorf("set managed emulator Job memory limit: %w", err)
	}
	cpuRate, err := windowsJobCPURate(resources.CPUMilli, runtime.NumCPU())
	if err != nil {
		return err
	}
	cpu := windowsJobCPURateControlInformation{
		ControlFlags: jobObjectCPURateControlEnable | jobObjectCPURateControlHardCap,
		CPURate:      cpuRate,
	}
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectCpuRateControlInformation,
		uintptr(unsafe.Pointer(&cpu)),
		uint32(unsafe.Sizeof(cpu)),
	); err != nil {
		return fmt.Errorf("set managed emulator Job CPU hard cap: %w", err)
	}
	return verifyWindowsJobLimits(job, resources)
}

func windowsJobCPURate(cpuMilli int64, logicalCPUs int) (uint32, error) {
	if cpuMilli <= 0 || cpuMilli%1000 != 0 || logicalCPUs <= 0 || cpuMilli > int64(logicalCPUs)*1000 {
		return 0, fmt.Errorf("managed emulator CPU %d milli-CPU cannot be enforced on a %d-CPU Windows host", cpuMilli, logicalCPUs)
	}
	rate := cpuMilli * 10000 / (int64(logicalCPUs) * 1000)
	if rate < 1 || rate > 10000 {
		return 0, fmt.Errorf("managed emulator CPU hard-cap rate %d is outside Windows Job limits", rate)
	}
	return uint32(rate), nil
}

func verifyWindowsJobLimits(job windows.Handle, resources admission.Resources) error {
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if err := windows.QueryInformationJobObject(
		job,
		int32(windows.JobObjectExtendedLimitInformation),
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
		nil,
	); err != nil {
		return fmt.Errorf("query managed emulator Job memory limit: %w", err)
	}
	if limits.BasicLimitInformation.LimitFlags&windows.JOB_OBJECT_LIMIT_JOB_MEMORY == 0 || int64(limits.JobMemoryLimit) != resources.MemoryBytes {
		return fmt.Errorf("managed emulator Job memory limit differs from exact requested %d bytes", resources.MemoryBytes)
	}
	var cpu windowsJobCPURateControlInformation
	if err := windows.QueryInformationJobObject(
		job,
		int32(windows.JobObjectCpuRateControlInformation),
		uintptr(unsafe.Pointer(&cpu)),
		uint32(unsafe.Sizeof(cpu)),
		nil,
	); err != nil {
		return fmt.Errorf("query managed emulator Job CPU hard cap: %w", err)
	}
	wantRate, err := windowsJobCPURate(resources.CPUMilli, runtime.NumCPU())
	if err != nil {
		return err
	}
	wantFlags := jobObjectCPURateControlEnable | jobObjectCPURateControlHardCap
	if cpu.ControlFlags != wantFlags || cpu.CPURate != wantRate {
		return fmt.Errorf("managed emulator Job CPU control differs from exact requested hard cap")
	}
	return nil
}

func openWindowsManagedJob(pid int, instance Instance) (windows.Handle, error) {
	identity := managedEmulatorResourceIdentity(instance)
	name, err := windows.UTF16PtrFromString(identity)
	if err != nil {
		return 0, err
	}
	job, err := openNamedWindowsJob(name)
	if err != nil {
		return 0, fmt.Errorf("open managed emulator Windows Job %q: %w", identity, err)
	}
	if err := verifyWindowsJobLimits(job, instance.Resources); err != nil {
		_ = windows.CloseHandle(job)
		return 0, err
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return 0, fmt.Errorf("open process for Windows Job membership: %w", err)
	}
	defer windows.CloseHandle(process)
	inJob, err := windowsProcessInJob(process, job)
	if err != nil || !inJob {
		_ = windows.CloseHandle(job)
		if err == nil {
			err = fmt.Errorf("process is outside the exact Windows Job")
		}
		return 0, err
	}
	return job, nil
}

func windowsProcessInJob(process, job windows.Handle) (bool, error) {
	var result int32
	call, _, callErr := kernel32IsProcessInJob.Call(
		uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&result)),
	)
	if call == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return false, callErr
	}
	return result != 0, nil
}

func (p *windowsManagedJobProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *windowsManagedJobProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *windowsManagedJobProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *windowsManagedJobProcess) Wait() error {
	p.waitOnce.Do(func() {
		defer close(p.waitDone)
		defer func() {
			if p.process != 0 {
				_ = windows.CloseHandle(p.process)
				p.process = 0
			}
		}()
		result, err := windows.WaitForSingleObject(p.process, windows.INFINITE)
		if err != nil {
			p.waitErr = err
			return
		}
		if result != windows.WAIT_OBJECT_0 {
			p.waitErr = fmt.Errorf("managed emulator process wait returned %#x", result)
			return
		}
		var exitCode uint32
		if err := windows.GetExitCodeProcess(p.process, &exitCode); err != nil {
			p.waitErr = err
			return
		}
		if exitCode == windowsStillActive {
			p.waitErr = fmt.Errorf("managed emulator process remained active after signaled wait")
			return
		}
		if exitCode != 0 {
			p.waitErr = fmt.Errorf("managed emulator launcher exited with status %d", exitCode)
		}
	})
	<-p.waitDone
	return p.waitErr
}

func (p *windowsManagedJobProcess) Signal(signal os.Signal) error {
	if signal != os.Kill {
		return fmt.Errorf("Windows managed emulator supports only KILL")
	}
	return p.Kill()
}

func (p *windowsManagedJobProcess) Kill() error {
	p.containmentMu.Lock()
	defer p.containmentMu.Unlock()
	if p.containmentClosed || p.job == 0 {
		return os.ErrProcessDone
	}
	if err := windows.TerminateJobObject(p.job, 1); err != nil {
		return fmt.Errorf("terminate managed emulator Windows Job: %w", err)
	}
	return nil
}

func (p *windowsManagedJobProcess) CloseContainment() error {
	p.containmentMu.Lock()
	defer p.containmentMu.Unlock()
	if p.containmentClosed {
		return nil
	}
	p.containmentClosed = true
	if p.job == 0 {
		return nil
	}
	err := windows.CloseHandle(p.job)
	p.job = 0
	return err
}
