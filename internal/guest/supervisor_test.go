package guest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestSupervisorPreservesOpaqueStreamsAndCleansTemporaryInputs(t *testing.T) {
	stdout := []byte{0x00, 0xff, 'o', 'u', 't'}
	stderr := []byte{'e', 0x00, 0xfe, 'r'}
	process := newFakeProcess(stdout, stderr, true)
	process.complete(ProcessResult{ExitCode: 0})
	var launched ProcessSpec
	var temporaryPath string
	launcher := launchFunc(func(spec ProcessSpec) (Process, error) {
		launched = spec
		temporaryPath = spec.Argv[1]
		value, err := os.ReadFile(temporaryPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value, []byte("prompt bytes")) {
			t.Fatalf("temporary bytes = %q", value)
		}
		return process, nil
	})
	supervisor := newTestSupervisor(t, launcher, time.Second, 1024)
	start := validStart()
	start.Argv = []string{"first-argument", "replace-me", "untouched"}
	start.Environment = map[string]string{"WORLD_TEST": "opaque"}
	start.TemporaryInputs = []transport.TemporaryInput{{NameHint: "prompt.bin", ArgvIndex: 1, Mode: 0o600, Bytes: []byte("prompt bytes")}}
	wire, terminal, err := runFake(t, supervisor, start, make(chan transport.Frame), process)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal.CleanupConfirmed || terminal.ExitCode != 0 {
		t.Fatalf("terminal = %#v", terminal)
	}
	if launched.Executable != start.Executable || launched.WorkingDirectory != start.WorkingDirectory || launched.Argv[0] != "first-argument" || launched.Argv[2] != "untouched" || launched.Argv[1] == "replace-me" {
		t.Fatalf("launch spec = %#v", launched)
	}
	if launched.Environment["WORLD_TEST"] != "opaque" {
		t.Fatalf("environment = %#v", launched.Environment)
	}
	if _, statErr := os.Stat(temporaryPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary input remains: %v", statErr)
	}
	decoded := decodeGuestOutput(t, wire)
	if !bytes.Equal(decoded.stdout, stdout) || !bytes.Equal(decoded.stderr, stderr) {
		t.Fatalf("streams changed: stdout=%v stderr=%v", decoded.stdout, decoded.stderr)
	}
	if decoded.terminals != 1 || decoded.processEvents != 2 {
		t.Fatalf("terminal/process counts = %d/%d", decoded.terminals, decoded.processEvents)
	}
}

func TestSupervisorCancellationTerminatesGroupAndEmitsOneTerminal(t *testing.T) {
	process := newFakeProcess(nil, nil, true)
	launched := make(chan struct{})
	supervisor := newTestSupervisor(t, launchFunc(func(ProcessSpec) (Process, error) {
		close(launched)
		return process, nil
	}), time.Second, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-launched
		cancel()
	}()
	wire, terminal, err := runFakeContext(t, ctx, supervisor, validStart(), make(chan transport.Frame), process)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancellation", err)
	}
	if process.terminateCalls() != 1 || !terminal.CleanupConfirmed {
		t.Fatalf("terminate calls=%d terminal=%#v", process.terminateCalls(), terminal)
	}
	if decoded := decodeGuestOutput(t, wire); decoded.terminals != 1 {
		t.Fatalf("terminal count = %d", decoded.terminals)
	}
}

func TestSupervisorEnforcesOutputAndInputLimits(t *testing.T) {
	for _, test := range []struct {
		name     string
		stdout   []byte
		controls []transport.Frame
		wantErr  error
	}{
		{name: "output", stdout: []byte("12345"), wantErr: transport.ErrOutputLimit},
		{name: "input", controls: []transport.Frame{{Kind: transport.KindStdin, Data: []byte("12345")}}, wantErr: ErrInputLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			process := newFakeProcess(test.stdout, nil, true)
			supervisor := newTestSupervisor(t, launchFunc(func(ProcessSpec) (Process, error) { return process, nil }), time.Second, 4)
			controls := make(chan transport.Frame, len(test.controls))
			for _, frame := range test.controls {
				controls <- frame
			}
			start := validStart()
			if test.name == "output" {
				start.MaxOutputBytes = 4
			}
			wire, terminal, err := runFake(t, supervisor, start, controls, process)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if !terminal.CleanupConfirmed || process.terminateCalls() != 1 {
				t.Fatalf("terminal=%#v terminate=%d", terminal, process.terminateCalls())
			}
			if decoded := decodeGuestOutput(t, wire); decoded.terminals != 1 {
				t.Fatalf("terminal count = %d", decoded.terminals)
			}
		})
	}
}

func TestSupervisorReportsCleanupUnconfirmed(t *testing.T) {
	process := newFakeProcess(nil, nil, false)
	process.complete(ProcessResult{ExitCode: 0})
	supervisor := newTestSupervisor(t, launchFunc(func(ProcessSpec) (Process, error) { return process, nil }), time.Second, 1024)
	wire, terminal, err := runFake(t, supervisor, validStart(), make(chan transport.Frame), process)
	if !errors.Is(err, transport.ErrCleanupUnknown) || terminal.CleanupConfirmed || !strings.Contains(terminal.Error, "cleanup-unconfirmed") {
		t.Fatalf("terminal=%#v error=%v", terminal, err)
	}
	if decoded := decodeGuestOutput(t, wire); decoded.terminals != 1 {
		t.Fatalf("terminal count = %d", decoded.terminals)
	}
}

func TestHeartbeatExpiryShutsDownProcessGroup(t *testing.T) {
	process := newFakeProcess(nil, nil, true)
	supervisor := newTestSupervisor(t, launchFunc(func(ProcessSpec) (Process, error) { return process, nil }), 20*time.Millisecond, 1024)
	wire, terminal, err := runFake(t, supervisor, validStart(), make(chan transport.Frame), process)
	if !errors.Is(err, ErrHeartbeatExpired) || process.terminateCalls() != 1 || !terminal.CleanupConfirmed {
		t.Fatalf("terminal=%#v terminate=%d error=%v", terminal, process.terminateCalls(), err)
	}
	if decoded := decodeGuestOutput(t, wire); decoded.terminals != 1 {
		t.Fatalf("terminal count = %d", decoded.terminals)
	}
}

func TestSignalFrameIsForwardedWithoutProviderInterpretation(t *testing.T) {
	process := newFakeProcess(nil, nil, true)
	process.signalCompletes = true
	supervisor := newTestSupervisor(t, launchFunc(func(ProcessSpec) (Process, error) { return process, nil }), time.Second, 1024)
	encodedSignal, _ := json.Marshal(transport.Signal{Name: "TERM"})
	controls := make(chan transport.Frame, 1)
	controls <- transport.Frame{Kind: transport.KindSignal, Data: encodedSignal}
	_, _, err := runFake(t, supervisor, validStart(), controls, process)
	if err != nil || process.lastSignal() != "TERM" {
		t.Fatalf("signal=%q error=%v", process.lastSignal(), err)
	}
}

func validStart() transport.ExecStart {
	return transport.ExecStart{
		ExecID:           "exec-test",
		IdempotencyKey:   "idempotency-test",
		Executable:       filepath.Join(string(filepath.Separator), "bin", "provider"),
		Argv:             []string{"first-argument"},
		WorkingDirectory: filepath.Join(string(filepath.Separator), "workspace"),
		Deadline:         time.Now().Add(time.Minute),
		MaxOutputBytes:   1024,
		CleanupGrace:     100 * time.Millisecond,
	}
}

func newTestSupervisor(t *testing.T, launcher Launcher, heartbeat time.Duration, maxStdin int64) *Supervisor {
	t.Helper()
	supervisor, err := New(Config{TemporaryRoot: t.TempDir(), Launcher: launcher, HeartbeatTimeout: heartbeat, MaxStdinBytes: maxStdin})
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

func runFake(t *testing.T, supervisor *Supervisor, start transport.ExecStart, controls <-chan transport.Frame, process *fakeProcess) ([]byte, transport.Terminal, error) {
	return runFakeContext(t, context.Background(), supervisor, start, controls, process)
}

func runFakeContext(t *testing.T, ctx context.Context, supervisor *Supervisor, start transport.ExecStart, controls <-chan transport.Frame, _ *fakeProcess) ([]byte, transport.Terminal, error) {
	t.Helper()
	var wire bytes.Buffer
	terminal, err := supervisor.Run(ctx, start, controls, transport.NewEncoder(&wire, transport.DefaultMaxFrame))
	return append([]byte(nil), wire.Bytes()...), terminal, err
}

type decodedOutput struct {
	stdout        []byte
	stderr        []byte
	terminals     int
	processEvents int
}

func decodeGuestOutput(t *testing.T, wire []byte) decodedOutput {
	t.Helper()
	decoder := transport.NewDecoder(bytes.NewReader(wire), transport.DefaultMaxFrame)
	var result decodedOutput
	for {
		frame, err := decoder.Read()
		if errors.Is(err, io.EOF) {
			return result
		}
		if err != nil {
			t.Fatal(err)
		}
		switch frame.Kind {
		case transport.KindStdout:
			result.stdout = append(result.stdout, frame.Data...)
		case transport.KindStderr:
			result.stderr = append(result.stderr, frame.Data...)
		case transport.KindTerminal:
			result.terminals++
		case transport.KindProcess:
			result.processEvents++
		}
	}
}

type launchFunc func(ProcessSpec) (Process, error)

func (function launchFunc) Launch(spec ProcessSpec) (Process, error) { return function(spec) }

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	closed bool
}

func (buffer *lockedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.closed {
		return 0, io.ErrClosedPipe
	}
	return buffer.buffer.Write(value)
}
func (buffer *lockedBuffer) Close() error {
	buffer.mu.Lock()
	buffer.closed = true
	buffer.mu.Unlock()
	return nil
}

type fakeProcess struct {
	stdin           *lockedBuffer
	stdout          io.ReadCloser
	stderr          io.ReadCloser
	wait            chan ProcessResult
	confirm         bool
	mu              sync.Mutex
	terminated      int
	killed          int
	signal          string
	signalCompletes bool
	completeOnce    sync.Once
}

func newFakeProcess(stdout, stderr []byte, confirm bool) *fakeProcess {
	return &fakeProcess{
		stdin:   &lockedBuffer{},
		stdout:  io.NopCloser(bytes.NewReader(stdout)),
		stderr:  io.NopCloser(bytes.NewReader(stderr)),
		wait:    make(chan ProcessResult, 1),
		confirm: confirm,
	}
}
func (process *fakeProcess) Identity() ProcessIdentity {
	return ProcessIdentity{PID: 42, ParentPID: 1, ProcessStartNS: 1234}
}
func (process *fakeProcess) Stdin() io.WriteCloser { return process.stdin }
func (process *fakeProcess) Stdout() io.ReadCloser { return process.stdout }
func (process *fakeProcess) Stderr() io.ReadCloser { return process.stderr }
func (process *fakeProcess) Wait() ProcessResult   { return <-process.wait }
func (process *fakeProcess) Signal(name string) error {
	process.mu.Lock()
	process.signal = name
	completes := process.signalCompletes
	process.mu.Unlock()
	if completes {
		process.complete(ProcessResult{ExitCode: 0})
	}
	return nil
}
func (process *fakeProcess) Terminate() error {
	process.mu.Lock()
	process.terminated++
	process.mu.Unlock()
	process.complete(ProcessResult{ExitCode: -1, Signal: "terminated"})
	return nil
}
func (process *fakeProcess) Kill() error {
	process.mu.Lock()
	process.killed++
	process.mu.Unlock()
	process.complete(ProcessResult{ExitCode: -1, Signal: "killed"})
	return nil
}
func (process *fakeProcess) ConfirmCleanup(context.Context) (bool, error) {
	return process.confirm, nil
}
func (process *fakeProcess) Close() error { return nil }
func (process *fakeProcess) complete(result ProcessResult) {
	process.completeOnce.Do(func() { process.wait <- result })
}
func (process *fakeProcess) terminateCalls() int {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.terminated
}
func (process *fakeProcess) lastSignal() string {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.signal
}
