//go:build windows

// windows-runtime-verifier independently observes a managed Android emulator
// process and its named Windows Job. It intentionally lives outside the
// production driver package so qualification does not merely trust the
// driver's persisted ownership claims.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	jobObjectQuery                 = uint32(0x0004)
	jobObjectCPURateControlEnable  = uint32(0x1)
	jobObjectCPURateControlHardCap = uint32(0x4)
	maximumCommandLineBytes        = uint32(1 << 20)
)

var (
	kernel32OpenJobObject  = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	kernel32IsProcessInJob = windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob")
)

type jobCPURateControlInformation struct {
	ControlFlags uint32 `json:"control_flags"`
	CPURate      uint32 `json:"cpu_rate"`
}

type report struct {
	PID                       int      `json:"pid"`
	ExecutablePath            string   `json:"executable_path"`
	ActualArgv                []string `json:"actual_argv"`
	ExpectedArgv              []string `json:"expected_argv"`
	CommandLineExact          bool     `json:"command_line_exact"`
	JobName                   string   `json:"job_name"`
	ProcessInNamedJob         bool     `json:"process_in_named_job"`
	JobLimitFlags             uint32   `json:"job_limit_flags"`
	JobMemoryLimitBytes       uint64   `json:"job_memory_limit_bytes"`
	ExpectedMemoryLimitBytes  uint64   `json:"expected_memory_limit_bytes"`
	MemoryLimitExact          bool     `json:"memory_limit_exact"`
	CPUControlFlags           uint32   `json:"cpu_control_flags"`
	CPURate                   uint32   `json:"cpu_rate"`
	ExpectedCPURate           uint32   `json:"expected_cpu_rate"`
	LogicalProcessors         int      `json:"logical_processors"`
	CPUHardCapExact           bool     `json:"cpu_hard_cap_exact"`
	AllIndependentChecksExact bool     `json:"all_independent_checks_exact"`
}

func main() {
	pid := flag.Int("pid", 0, "exact live emulator PID")
	jobName := flag.String("job-name", "", "exact named Windows Job")
	expectedArgvPath := flag.String("expected-argv", "", "JSON file containing the exact expected full argv")
	cpuMilli := flag.Int64("cpu-milli", 0, "expected whole-core CPU contract in milli-CPU")
	memoryBytes := flag.Uint64("memory-bytes", 0, "expected Job memory limit")
	flag.Parse()
	if *pid <= 0 || *jobName == "" || *expectedArgvPath == "" || *cpuMilli <= 0 || *memoryBytes == 0 || flag.NArg() != 0 {
		fatalf("pid, job-name, expected-argv, cpu-milli, and memory-bytes are required")
	}

	expectedArgv, err := readExpectedArgv(*expectedArgvPath)
	if err != nil {
		fatalf("read expected argv: %v", err)
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(*pid))
	if err != nil {
		fatalf("open exact process %d: %v", *pid, err)
	}
	defer windows.CloseHandle(process)
	actualArgv, err := queryProcessArguments(process)
	if err != nil {
		fatalf("query exact process command line: %v", err)
	}
	executablePath, err := queryProcessImage(process)
	if err != nil {
		fatalf("query exact process image: %v", err)
	}

	job, err := openNamedJob(*jobName)
	if err != nil {
		fatalf("open named Job %q: %v", *jobName, err)
	}
	defer windows.CloseHandle(job)
	inJob, err := processInJob(process, job)
	if err != nil {
		fatalf("query exact process Job membership: %v", err)
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if err := windows.QueryInformationJobObject(job, int32(windows.JobObjectExtendedLimitInformation), uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)), nil); err != nil {
		fatalf("query named Job memory controls: %v", err)
	}
	var cpu jobCPURateControlInformation
	if err := windows.QueryInformationJobObject(job, int32(windows.JobObjectCpuRateControlInformation), uintptr(unsafe.Pointer(&cpu)), uint32(unsafe.Sizeof(cpu)), nil); err != nil {
		fatalf("query named Job CPU controls: %v", err)
	}
	expectedCPURate, err := expectedWindowsCPURate(*cpuMilli, runtime.NumCPU())
	if err != nil {
		fatalf("derive expected CPU hard cap: %v", err)
	}
	wantCPUFlags := jobObjectCPURateControlEnable | jobObjectCPURateControlHardCap
	memoryExact := limits.BasicLimitInformation.LimitFlags&windows.JOB_OBJECT_LIMIT_JOB_MEMORY != 0 && uint64(limits.JobMemoryLimit) == *memoryBytes
	cpuExact := cpu.ControlFlags == wantCPUFlags && cpu.CPURate == expectedCPURate
	commandExact := slices.Equal(actualArgv, expectedArgv)
	result := report{
		PID: *pid, ExecutablePath: executablePath, ActualArgv: actualArgv, ExpectedArgv: expectedArgv,
		CommandLineExact: commandExact, JobName: *jobName, ProcessInNamedJob: inJob,
		JobLimitFlags: limits.BasicLimitInformation.LimitFlags, JobMemoryLimitBytes: uint64(limits.JobMemoryLimit),
		ExpectedMemoryLimitBytes: *memoryBytes, MemoryLimitExact: memoryExact,
		CPUControlFlags: cpu.ControlFlags, CPURate: cpu.CPURate, ExpectedCPURate: expectedCPURate,
		LogicalProcessors: runtime.NumCPU(), CPUHardCapExact: cpuExact,
		AllIndependentChecksExact: commandExact && inJob && memoryExact && cpuExact,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		fatalf("encode report: %v", err)
	}
}

func readExpectedArgv(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var arguments []string
	if err := json.Unmarshal(data, &arguments); err != nil {
		return nil, err
	}
	if len(arguments) == 0 {
		return nil, fmt.Errorf("expected argv is empty")
	}
	return arguments, nil
}

func queryProcessArguments(handle windows.Handle) ([]string, error) {
	var required uint32
	_ = windows.NtQueryInformationProcess(handle, windows.ProcessCommandLineInformation, nil, 0, &required)
	if required < uint32(unsafe.Sizeof(windows.NTUnicodeString{})) || required > maximumCommandLineBytes {
		return nil, fmt.Errorf("invalid command-line result size %d", required)
	}
	buffer := make([]byte, required)
	if err := windows.NtQueryInformationProcess(handle, windows.ProcessCommandLineInformation, unsafe.Pointer(&buffer[0]), uint32(len(buffer)), &required); err != nil {
		return nil, err
	}
	commandLine := (*windows.NTUnicodeString)(unsafe.Pointer(&buffer[0]))
	if commandLine.Length == 0 || commandLine.Length%2 != 0 || commandLine.Buffer == nil {
		return nil, fmt.Errorf("malformed UTF-16 command line")
	}
	base := uintptr(unsafe.Pointer(&buffer[0]))
	end := base + uintptr(len(buffer))
	textStart := uintptr(unsafe.Pointer(commandLine.Buffer))
	textEnd := textStart + uintptr(commandLine.Length)
	if textStart < base || textEnd < textStart || textEnd > end {
		return nil, fmt.Errorf("command line points outside query buffer")
	}
	units := unsafe.Slice(commandLine.Buffer, int(commandLine.Length/2))
	return windows.DecomposeCommandLine(windows.UTF16ToString(units))
}

func queryProcessImage(handle windows.Handle) (string, error) {
	buffer := make([]uint16, 32768)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(handle, 0, &buffer[0], &size); err != nil {
		return "", err
	}
	if size == 0 || size > uint32(len(buffer)) {
		return "", fmt.Errorf("invalid process image length %d", size)
	}
	path := windows.UTF16ToString(buffer[:size])
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	file, err := windows.CreateFile(pointer, windows.FILE_READ_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(file)
	finalPath := make([]uint16, 32768)
	length, err := windows.GetFinalPathNameByHandle(file, &finalPath[0], uint32(len(finalPath)), 0)
	if err != nil {
		return "", err
	}
	if length == 0 || length >= uint32(len(finalPath)) {
		return "", fmt.Errorf("invalid final process image length %d", length)
	}
	canonical := windows.UTF16ToString(finalPath[:length])
	if strings.HasPrefix(canonical, `\\?\UNC\`) {
		canonical = `\\` + strings.TrimPrefix(canonical, `\\?\UNC\`)
	} else {
		canonical = strings.TrimPrefix(canonical, `\\?\`)
	}
	return filepath.Clean(canonical), nil
}

func openNamedJob(name string) (windows.Handle, error) {
	pointer, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	result, _, callErr := kernel32OpenJobObject.Call(uintptr(jobObjectQuery), 0, uintptr(unsafe.Pointer(pointer)))
	if result == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return 0, callErr
	}
	return windows.Handle(result), nil
}

func processInJob(process, job windows.Handle) (bool, error) {
	var result int32
	call, _, callErr := kernel32IsProcessInJob.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&result)))
	if call == 0 {
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		return false, callErr
	}
	return result != 0, nil
}

func expectedWindowsCPURate(cpuMilli int64, logicalProcessors int) (uint32, error) {
	if cpuMilli <= 0 || cpuMilli%1000 != 0 || logicalProcessors <= 0 || cpuMilli > int64(logicalProcessors)*1000 {
		return 0, fmt.Errorf("%d milli-CPU is not an enforceable whole-core limit on %d logical processors", cpuMilli, logicalProcessors)
	}
	rate := cpuMilli * 10000 / (int64(logicalProcessors) * 1000)
	if rate < 1 || rate > 10000 {
		return 0, fmt.Errorf("derived CPU rate %d is outside Windows Job limits", rate)
	}
	return uint32(rate), nil
}

func fatalf(format string, arguments ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", arguments...)
	os.Exit(2)
}
