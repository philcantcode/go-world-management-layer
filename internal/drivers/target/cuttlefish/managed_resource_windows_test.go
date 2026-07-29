//go:build windows

package cuttlefish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"golang.org/x/sys/windows"
)

func TestWindowsJobCPURateUsesWholeHostCapacity(t *testing.T) {
	if got, err := windowsJobCPURate(1000, 8); err != nil || got != 1250 {
		t.Fatalf("one CPU of eight = %d, %v; want 1250", got, err)
	}
	if got, err := windowsJobCPURate(8000, 8); err != nil || got != 10000 {
		t.Fatalf("eight CPUs of eight = %d, %v; want 10000", got, err)
	}
	for _, invalid := range []int64{0, 1500, 9000} {
		if _, err := windowsJobCPURate(invalid, 8); err == nil {
			t.Fatalf("invalid Windows CPU limit %d was accepted", invalid)
		}
	}
}

func TestNamedWindowsJobSurvivesParentHandleCloseAndReopensExactly(t *testing.T) {
	root := t.TempDir()
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcherExecutable := filepath.Join(root, "emulator.exe")
	successorExecutable := filepath.Join(root, "qemu-system-x86_64-headless.exe")
	for _, destination := range []string{launcherExecutable, successorExecutable} {
		if err := copyWindowsJobTestExecutable(testExecutable, destination); err != nil {
			t.Fatal(err)
		}
	}
	targetID, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	resources := admission.Resources{CPUMilli: 1000, MemoryBytes: 512 << 20, StorageBytes: 64 << 20}
	allocation := emulatorAllocation(5560)
	stateDirectory := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	instance := Instance{
		RuntimeID: "world-job-test-" + targetID.UUID(), StateDirectory: stateDirectory,
		Allocation: allocation, Resources: resources, GuestMemoryBytes: 2 << 30,
		Fingerprint: ResetFingerprint{DeviceConfigDigest: domain.NewDigest([]byte("windows-job-device-config"))},
	}
	pidFile := managedEmulatorPIDPath(instance)
	storage := managedTestStorageBinding(instance)
	dataImage := storage.BackingPath
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	process, err := startWindowsJobProcess(ctx, command.Invocation{
		Program: launcherExecutable,
		Environment: windowsJobTestEnvironment(map[string]string{
			windowsJobTestModeEnvironment:      "launcher",
			windowsJobTestSuccessorEnvironment: successorExecutable,
			windowsJobTestPIDFileEnvironment:   pidFile,
			windowsJobTestDataImageEnvironment: dataImage,
			windowsJobTestAVDEnvironment:       allocation.InstanceName,
			windowsJobTestStorageEnvironment:   strconv.FormatInt(resources.StorageBytes, 10),
		}),
	}, resources, managedEmulatorResourceIdentity(instance))
	if err != nil {
		t.Fatal(err)
	}
	_ = process.Stdin().Close()
	stdoutDone, stderrDone := make(chan struct{}), make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, process.Stdout()); close(stdoutDone) }()
	go func() { _, _ = io.Copy(io.Discard, process.Stderr()); close(stderrDone) }()
	t.Cleanup(func() {
		if pid, found, _ := readManagedEmulatorPID(stateDirectory); found {
			if handle, openErr := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid)); openErr == nil {
				_ = windows.TerminateProcess(handle, 1)
				_ = windows.CloseHandle(handle)
			}
		}
		_ = process.CloseContainment()
		if process.process != 0 {
			_ = process.Wait()
		}
	})

	if err := process.Wait(); err != nil {
		t.Fatalf("launcher helper: %v", err)
	}
	<-stdoutDone
	<-stderrDone
	pid := waitWindowsJobTestPID(t, stateDirectory)
	host, err := (windowsManagedHostProcessAuthority{}).Open(pid, launcherExecutable, pidFile, storage, instance)
	if err != nil {
		t.Fatalf("open exact non-inheriting successor: %v", err)
	}
	windowsHost := host.(*windowsManagedHostProcess)
	queryOnly, err := duplicateWindowsJobQueryHandle(windowsHost.job, windows.CurrentProcess(), false)
	if err != nil {
		_ = host.Close()
		t.Fatal(err)
	}
	if terminateErr := windows.TerminateJobObject(queryOnly, 1); !errors.Is(terminateErr, windows.ERROR_ACCESS_DENIED) {
		_ = windows.CloseHandle(queryOnly)
		_ = host.Close()
		t.Fatalf("query-only Job handle termination error = %v, want access denied", terminateErr)
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if _, setErr := windows.SetInformationJobObject(queryOnly, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); !errors.Is(setErr, windows.ERROR_ACCESS_DENIED) {
		_ = windows.CloseHandle(queryOnly)
		_ = host.Close()
		t.Fatalf("query-only Job handle limit mutation error = %v, want access denied", setErr)
	}
	if err := windows.CloseHandle(queryOnly); err != nil {
		_ = host.Close()
		t.Fatal(err)
	}
	if err := windowsHost.AnchorResourceAuthority(); err != nil {
		_ = host.Close()
		t.Fatalf("anchor exact successor: %v", err)
	}
	if err := process.CloseContainment(); err != nil {
		t.Fatalf("close daemon-side Job handle: %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("close exact successor process/Job authority: %v", err)
	}
	reopened, err := openWindowsManagedJob(pid, instance)
	if err != nil {
		t.Fatalf("reopen child-anchored named Job: %v", err)
	}
	if err := verifyWindowsJobLimits(reopened, resources); err != nil {
		_ = windows.CloseHandle(reopened)
		t.Fatalf("reopened Job limits: %v", err)
	}
	if err := windows.TerminateJobObject(reopened, 1); err != nil {
		_ = windows.CloseHandle(reopened)
		t.Fatalf("terminate reopened Job: %v", err)
	}
	if err := windows.CloseHandle(reopened); err != nil {
		t.Fatal(err)
	}
}

const (
	windowsJobTestModeEnvironment      = "WORLD_WINDOWS_JOB_TEST_MODE"
	windowsJobTestSuccessorEnvironment = "WORLD_WINDOWS_JOB_TEST_SUCCESSOR"
	windowsJobTestPIDFileEnvironment   = "WORLD_WINDOWS_JOB_TEST_PID_FILE"
	windowsJobTestDataImageEnvironment = "WORLD_WINDOWS_JOB_TEST_DATA_IMAGE"
	windowsJobTestAVDEnvironment       = "WORLD_WINDOWS_JOB_TEST_AVD"
	windowsJobTestStorageEnvironment   = "WORLD_WINDOWS_JOB_TEST_STORAGE_BYTES"
)

func TestMain(m *testing.M) {
	switch os.Getenv(windowsJobTestModeEnvironment) {
	case "":
		os.Exit(m.Run())
	case "successor":
		pidFile := os.Getenv(windowsJobTestPIDFileEnvironment)
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for {
			time.Sleep(time.Minute)
		}
	case "launcher":
		if err := runWindowsManagedJobLauncherHelper(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(0)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown Windows Job helper mode %q\n", os.Getenv(windowsJobTestModeEnvironment))
		os.Exit(2)
	}
}

func runWindowsManagedJobLauncherHelper() error {
	successor := os.Getenv(windowsJobTestSuccessorEnvironment)
	pidFile := os.Getenv(windowsJobTestPIDFileEnvironment)
	dataImage := os.Getenv(windowsJobTestDataImageEnvironment)
	storageBytes, err := strconv.ParseInt(os.Getenv(windowsJobTestStorageEnvironment), 10, 64)
	if err != nil {
		return fmt.Errorf("parse exact Windows Job test storage bytes: %w", err)
	}
	if storageBytes <= 0 {
		return fmt.Errorf("exact Windows Job test storage bytes must be positive")
	}
	instance := Instance{
		Allocation:       emulatorAllocation(5560),
		Resources:        admission.Resources{CPUMilli: 1000, StorageBytes: storageBytes},
		GuestMemoryBytes: 2 << 30,
	}
	instance.Allocation.InstanceName = os.Getenv(windowsJobTestAVDEnvironment)
	arguments := append([]string{successor}, managedEmulatorLaunchArguments(instance, 5560, dataImage, pidFile)...)
	executable, err := windows.UTF16PtrFromString(successor)
	if err != nil {
		return err
	}
	commandLine, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(arguments))
	if err != nil {
		return err
	}
	environment, err := windowsEnvironmentBlock(windowsJobTestEnvironment(map[string]string{windowsJobTestModeEnvironment: "successor"}))
	if err != nil {
		return err
	}
	startup := windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfo{}))}
	var process windows.ProcessInformation
	if err := windows.CreateProcess(executable, commandLine, nil, nil, false, windows.CREATE_UNICODE_ENVIRONMENT|windows.CREATE_NO_WINDOW, &environment[0], nil, &startup, &process); err != nil {
		return err
	}
	_ = windows.CloseHandle(process.Thread)
	return windows.CloseHandle(process.Process)
}

func windowsJobTestEnvironment(overrides map[string]string) []string {
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if _, replaced := overrides[name]; found && replaced {
			continue
		}
		result = append(result, entry)
	}
	for name, value := range overrides {
		result = append(result, name+"="+value)
	}
	return result
}

func copyWindowsJobTestExecutable(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Close())
}

func waitWindowsJobTestPID(t *testing.T, stateDirectory string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pid, found, err := readManagedEmulatorPID(stateDirectory); err != nil {
			t.Fatal(err)
		} else if found {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Errorf("successor did not publish its exact PID"))
	return 0
}
