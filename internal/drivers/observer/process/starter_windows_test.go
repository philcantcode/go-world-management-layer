//go:build windows

package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"golang.org/x/sys/windows"
)

const (
	windowsJobTestModeEnvironment          = "OBSERVER_JOB_TEST_MODE"
	windowsJobTestDirectPIDEnvironment     = "OBSERVER_JOB_TEST_DIRECT_PID"
	windowsJobTestLeafPIDEnvironment       = "OBSERVER_JOB_TEST_LEAF_PID"
	windowsJobTestTreeReadyEnvironment     = "OBSERVER_JOB_TEST_TREE_READY"
	windowsJobTestOwnerReadyEnvironment    = "OBSERVER_JOB_TEST_OWNER_READY"
	windowsJobTestDirectReleaseEnvironment = "OBSERVER_JOB_TEST_DIRECT_RELEASE"
	windowsJobTestLeafReleaseEnvironment   = "OBSERVER_JOB_TEST_LEAF_RELEASE"
	windowsJobTestOwnerReleaseEnvironment  = "OBSERVER_JOB_TEST_OWNER_RELEASE"
)

func TestWindowsCollectorJobPreflightAdvertisesCrashCleanup(t *testing.T) {
	if !collectorParentDeathSignalGuaranteed() {
		t.Fatalf("Windows collector Job preflight failed: %v", windowsCollectorPreflightErr)
	}
	driver, err := New(Config{Adapters: testAdapters(nil), Outputs: &memoryOutputFactory{capture: newMemoryCapture()}})
	if err != nil {
		t.Fatal(err)
	}
	if !driver.InterruptedCollectorCleanupGuaranteed() {
		t.Fatal("built-in Windows starter did not advertise its proven Job invariant")
	}
}

func TestWindowsCollectorStopObservesDirectExitAndClearsDescendant(t *testing.T) {
	paths := newWindowsJobTestPaths(t, "direct-exit")
	executable := windowsJobTestExecutable(t)
	capture := newMemoryCapture()
	adapter := windowsJobTestAdapter(executable, paths, ReadinessFunc(func(ctx context.Context, _ ports.CollectorPlan) error {
		return waitWindowsJobTestFile(ctx, paths.treeReady)
	}))
	driver, err := New(Config{
		Adapters: []Adapter{adapter}, Outputs: &memoryOutputFactory{capture: capture}, CleanupGrace: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := validCollectorPlan(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	direct := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.directPID))
	defer windows.CloseHandle(direct)
	leaf := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.leafPID))
	defer windows.CloseHandle(leaf)
	writeWindowsJobTestFile(t, paths.directRelease, "exit")
	waitForProcessExit(t, ctx, driver, plan.CollectorID)
	requireWindowsJobTestProcessExited(t, direct, 5*time.Second)
	requireWindowsJobTestProcessRunning(t, leaf)

	result, stopErr := driver.Stop(ctx, plan.CollectorID)
	if stopErr == nil || !strings.Contains(stopErr.Error(), "collector exited before stop was requested") {
		t.Fatalf("Stop error = %v", stopErr)
	}
	if !result.TeardownConfirmed || result.Coverage.Spec().Status != domain.CoverageLost {
		t.Fatalf("Stop result = %#v", result)
	}
	requireWindowsJobTestProcessExited(t, leaf, 5*time.Second)
}

func TestWindowsCollectorReadinessRollbackClearsProcessTree(t *testing.T) {
	paths := newWindowsJobTestPaths(t, "rollback")
	executable := windowsJobTestExecutable(t)
	var direct, leaf windows.Handle
	readinessErr := errors.New("injected readiness failure")
	adapter := windowsJobTestAdapter(executable, paths, ReadinessFunc(func(ctx context.Context, _ ports.CollectorPlan) error {
		if err := waitWindowsJobTestFile(ctx, paths.treeReady); err != nil {
			return err
		}
		direct = openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.directPID))
		leaf = openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.leafPID))
		return readinessErr
	}))
	capture := newMemoryCapture()
	driver, err := New(Config{
		Adapters: []Adapter{adapter}, Outputs: &memoryOutputFactory{capture: capture}, CleanupGrace: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx, validCollectorPlan(t)); err == nil || !errors.Is(err, readinessErr) {
		t.Fatalf("Start error = %v", err)
	}
	defer windows.CloseHandle(direct)
	defer windows.CloseHandle(leaf)
	requireWindowsJobTestProcessExited(t, direct, 5*time.Second)
	requireWindowsJobTestProcessExited(t, leaf, 5*time.Second)
	if capture.abortCount() != 1 || capture.finalizeCount() != 0 {
		t.Fatalf("capture abort=%d finalize=%d", capture.abortCount(), capture.finalizeCount())
	}
}

func TestWindowsCollectorInvalidStartClearsProcessTree(t *testing.T) {
	paths := newWindowsJobTestPaths(t, "invalid")
	process := startWindowsJobTestTree(t, paths)
	direct := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.directPID))
	defer windows.CloseHandle(direct)
	leaf := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.leafPID))
	defer windows.CloseHandle(leaf)
	invalid := &windowsInvalidCollectorProcess{windowsCollectorJobProcess: process}
	capture := newMemoryCapture()
	driver, err := New(Config{
		Starter:  starterFunc(func(context.Context, command.Invocation) (command.Process, error) { return invalid, nil }),
		Adapters: testAdapters(nil), Outputs: &memoryOutputFactory{capture: capture}, CleanupGrace: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx, validCollectorPlan(t)); err == nil {
		t.Fatal("invalid collector output streams were accepted")
	}
	requireWindowsJobTestProcessExited(t, direct, 5*time.Second)
	requireWindowsJobTestProcessExited(t, leaf, 5*time.Second)
	if !process.jobClosed || capture.abortCount() != 1 {
		t.Fatalf("job closed=%t capture abort=%d", process.jobClosed, capture.abortCount())
	}
	_ = process.stdout.Close()
}

func TestWindowsCollectorJobsStopIndependently(t *testing.T) {
	firstPaths := newWindowsJobTestPaths(t, "first")
	secondPaths := newWindowsJobTestPaths(t, "second")
	first := startWindowsJobTestTree(t, firstPaths)
	second := startWindowsJobTestTree(t, secondPaths)
	firstDirect := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, firstPaths.directPID))
	defer windows.CloseHandle(firstDirect)
	firstLeaf := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, firstPaths.leafPID))
	defer windows.CloseHandle(firstLeaf)
	secondDirect := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, secondPaths.directPID))
	defer windows.CloseHandle(secondDirect)
	secondLeaf := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, secondPaths.leafPID))
	defer windows.CloseHandle(secondLeaf)

	stopWindowsJobTestProcess(t, first)
	requireWindowsJobTestProcessExited(t, firstDirect, 5*time.Second)
	requireWindowsJobTestProcessExited(t, firstLeaf, 5*time.Second)
	requireWindowsJobTestProcessRunning(t, secondDirect)
	requireWindowsJobTestProcessRunning(t, secondLeaf)
	stopWindowsJobTestProcess(t, second)
}

func TestWindowsCollectorJobClosesOnDaemonLoss(t *testing.T) {
	paths := newWindowsJobTestPaths(t, "daemon-loss")
	executable := windowsJobTestExecutable(t)
	command := exec.Command(executable, "-test.run=^TestWindowsCollectorProcessHelper$")
	command.Env = windowsJobTestEnvironment(map[string]string{
		windowsJobTestModeEnvironment:          "owner",
		windowsJobTestDirectPIDEnvironment:     paths.directPID,
		windowsJobTestLeafPIDEnvironment:       paths.leafPID,
		windowsJobTestTreeReadyEnvironment:     paths.treeReady,
		windowsJobTestOwnerReadyEnvironment:    paths.ownerReady,
		windowsJobTestDirectReleaseEnvironment: paths.directRelease,
		windowsJobTestLeafReleaseEnvironment:   paths.leafRelease,
		windowsJobTestOwnerReleaseEnvironment:  paths.ownerRelease,
	})
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := waitWindowsJobTestFile(ctx, paths.ownerReady); err != nil {
		t.Fatalf("owner readiness: %v\n%s", err, output.String())
	}
	direct := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.directPID))
	defer windows.CloseHandle(direct)
	leaf := openWindowsJobTestProcess(t, readWindowsJobTestPID(t, paths.leafPID))
	defer windows.CloseHandle(leaf)
	writeWindowsJobTestFile(t, paths.ownerRelease, "crash")
	if err := command.Wait(); err != nil {
		t.Fatalf("daemon-owner helper: %v\n%s", err, output.String())
	}
	requireWindowsJobTestProcessExited(t, direct, 5*time.Second)
	requireWindowsJobTestProcessExited(t, leaf, 5*time.Second)
}

// TestWindowsCollectorProcessHelper is re-entered in child test binaries. The
// owner mode deliberately calls os.Exit without closing its Job handle, exactly
// modeling abrupt daemon loss rather than an orderly Stop path.
func TestWindowsCollectorProcessHelper(t *testing.T) {
	switch os.Getenv(windowsJobTestModeEnvironment) {
	case "":
		return
	case "leaf":
		windowsJobTestWritePID(os.Getenv(windowsJobTestLeafPIDEnvironment))
		windowsJobTestWaitForFile(os.Getenv(windowsJobTestLeafReleaseEnvironment))
	case "tree":
		windowsJobTestRunTreeParent()
	case "owner":
		windowsJobTestRunOwner()
	default:
		windowsJobTestFail("unknown helper mode %q", os.Getenv(windowsJobTestModeEnvironment))
	}
}

type windowsInvalidCollectorProcess struct {
	*windowsCollectorJobProcess
}

func (*windowsInvalidCollectorProcess) Stdout() io.ReadCloser { return nil }

type windowsJobTestPaths struct {
	directPID, leafPID, treeReady, ownerReady, directRelease, leafRelease, ownerRelease string
}

func newWindowsJobTestPaths(t *testing.T, name string) windowsJobTestPaths {
	t.Helper()
	root := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := windowsJobTestPaths{
		directPID: filepath.Join(root, "direct.pid"), leafPID: filepath.Join(root, "leaf.pid"),
		treeReady: filepath.Join(root, "tree.ready"), ownerReady: filepath.Join(root, "owner.ready"),
		directRelease: filepath.Join(root, "direct.release"), leafRelease: filepath.Join(root, "leaf.release"),
		ownerRelease: filepath.Join(root, "owner.release"),
	}
	t.Cleanup(func() {
		_ = os.WriteFile(paths.directRelease, []byte("cleanup"), 0o600)
		_ = os.WriteFile(paths.leafRelease, []byte("cleanup"), 0o600)
		_ = os.WriteFile(paths.ownerRelease, []byte("cleanup"), 0o600)
	})
	return paths
}

func windowsJobTestAdapter(executable string, paths windowsJobTestPaths, readiness Readiness) Adapter {
	adapter := testAdapters(readiness)[0]
	adapter.Program = executable
	adapter.Args = []string{"-test.run=^TestWindowsCollectorProcessHelper$"}
	adapter.Environment = map[string]string{
		windowsJobTestModeEnvironment:          "tree",
		windowsJobTestDirectPIDEnvironment:     paths.directPID,
		windowsJobTestLeafPIDEnvironment:       paths.leafPID,
		windowsJobTestTreeReadyEnvironment:     paths.treeReady,
		windowsJobTestDirectReleaseEnvironment: paths.directRelease,
		windowsJobTestLeafReleaseEnvironment:   paths.leafRelease,
	}
	return adapter
}

func startWindowsJobTestTree(t *testing.T, paths windowsJobTestPaths) *windowsCollectorJobProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	process, err := startWindowsCollectorJobProcess(ctx, command.Invocation{
		Program: windowsJobTestExecutable(t),
		Args:    []string{"-test.run=^TestWindowsCollectorProcessHelper$"},
		Environment: windowsJobTestEnvironment(map[string]string{
			windowsJobTestModeEnvironment:          "tree",
			windowsJobTestDirectPIDEnvironment:     paths.directPID,
			windowsJobTestLeafPIDEnvironment:       paths.leafPID,
			windowsJobTestTreeReadyEnvironment:     paths.treeReady,
			windowsJobTestDirectReleaseEnvironment: paths.directRelease,
			windowsJobTestLeafReleaseEnvironment:   paths.leafRelease,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = abortWindowsCollectorProcess(process, 5*time.Second) })
	if err := waitWindowsJobTestFile(ctx, paths.treeReady); err != nil {
		_ = abortWindowsCollectorProcess(process, 5*time.Second)
		t.Fatal(err)
	}
	return process
}

func stopWindowsJobTestProcess(t *testing.T, process *windowsCollectorJobProcess) {
	t.Helper()
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	_ = process.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirmed, err := confirmAndCloseCollectorContainment(ctx, process)
	if err != nil || !confirmed {
		t.Fatalf("collector Job cleanup confirmed=%t err=%v", confirmed, err)
	}
	_ = process.Stdout().Close()
	_ = process.Stderr().Close()
}

func windowsJobTestRunTreeParent() {
	windowsJobTestWritePID(os.Getenv(windowsJobTestDirectPIDEnvironment))
	executable, err := os.Executable()
	if err != nil {
		windowsJobTestFail("resolve helper executable: %v", err)
	}
	leaf := exec.Command(executable, "-test.run=^TestWindowsCollectorProcessHelper$")
	leaf.Env = windowsJobTestEnvironment(map[string]string{
		windowsJobTestModeEnvironment:        "leaf",
		windowsJobTestLeafPIDEnvironment:     os.Getenv(windowsJobTestLeafPIDEnvironment),
		windowsJobTestLeafReleaseEnvironment: os.Getenv(windowsJobTestLeafReleaseEnvironment),
	})
	leaf.Stdout, leaf.Stderr = io.Discard, io.Discard
	if err := leaf.Start(); err != nil {
		windowsJobTestFail("start leaf helper: %v", err)
	}
	_ = leaf.Process.Release()
	windowsJobTestWaitForFile(os.Getenv(windowsJobTestLeafPIDEnvironment))
	windowsJobTestWriteFile(os.Getenv(windowsJobTestTreeReadyEnvironment), "ready")
	windowsJobTestWaitForFile(os.Getenv(windowsJobTestDirectReleaseEnvironment))
}

func windowsJobTestRunOwner() {
	executable, err := os.Executable()
	if err != nil {
		windowsJobTestFail("resolve owner executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	process, err := startWindowsCollectorJobProcess(ctx, command.Invocation{
		Program: executable,
		Args:    []string{"-test.run=^TestWindowsCollectorProcessHelper$"},
		Environment: windowsJobTestEnvironment(map[string]string{
			windowsJobTestModeEnvironment:          "tree",
			windowsJobTestDirectPIDEnvironment:     os.Getenv(windowsJobTestDirectPIDEnvironment),
			windowsJobTestLeafPIDEnvironment:       os.Getenv(windowsJobTestLeafPIDEnvironment),
			windowsJobTestTreeReadyEnvironment:     os.Getenv(windowsJobTestTreeReadyEnvironment),
			windowsJobTestDirectReleaseEnvironment: os.Getenv(windowsJobTestDirectReleaseEnvironment),
			windowsJobTestLeafReleaseEnvironment:   os.Getenv(windowsJobTestLeafReleaseEnvironment),
		}),
	})
	if err != nil {
		windowsJobTestFail("start owned collector tree: %v", err)
	}
	_ = process.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, process.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, process.Stderr()) }()
	windowsJobTestWaitForFile(os.Getenv(windowsJobTestTreeReadyEnvironment))
	windowsJobTestWriteFile(os.Getenv(windowsJobTestOwnerReadyEnvironment), "ready")
	windowsJobTestWaitForFile(os.Getenv(windowsJobTestOwnerReleaseEnvironment))
	os.Exit(0)
}

func windowsJobTestEnvironment(overrides map[string]string) []string {
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		name, value, found := strings.Cut(entry, "=")
		if found && name != "" {
			values[strings.ToUpper(name)] = name + "=" + value
		}
	}
	for name, value := range overrides {
		values[strings.ToUpper(name)] = name + "=" + value
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func windowsJobTestExecutable(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return executable
}

func readWindowsJobTestPID(t *testing.T, path string) int {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil || pid <= 0 {
		t.Fatalf("PID file %q = %q, %v", path, contents, err)
	}
	return pid
}

func openWindowsJobTestProcess(t *testing.T, pid int) windows.Handle {
	t.Helper()
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		t.Fatalf("open exact helper PID %d: %v", pid, err)
	}
	return handle
}

func requireWindowsJobTestProcessRunning(t *testing.T, handle windows.Handle) {
	t.Helper()
	result, err := windows.WaitForSingleObject(handle, 0)
	if err != nil || result != windowsCollectorWaitTimeout {
		t.Fatalf("process is not running: wait=%#x err=%v", result, err)
	}
}

func requireWindowsJobTestProcessExited(t *testing.T, handle windows.Handle, timeout time.Duration) {
	t.Helper()
	milliseconds := timeout.Milliseconds()
	if milliseconds <= 0 || milliseconds > int64(^uint32(0)-1) {
		t.Fatalf("invalid process wait timeout %s", timeout)
	}
	result, err := windows.WaitForSingleObject(handle, uint32(milliseconds))
	if err != nil || result != windows.WAIT_OBJECT_0 {
		t.Fatalf("process did not exit: wait=%#x err=%v", result, err)
	}
}

func waitWindowsJobTestFile(ctx context.Context, path string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeWindowsJobTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func windowsJobTestWritePID(path string) {
	windowsJobTestWriteFile(path, strconv.Itoa(os.Getpid()))
}

func windowsJobTestWriteFile(path, value string) {
	if path == "" {
		windowsJobTestFail("helper path is empty")
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		windowsJobTestFail("write helper file %q: %v", path, err)
	}
}

func windowsJobTestWaitForFile(path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitWindowsJobTestFile(ctx, path); err != nil {
		windowsJobTestFail("wait for helper file %q: %v", path, err)
	}
}

func windowsJobTestFail(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(91)
}
