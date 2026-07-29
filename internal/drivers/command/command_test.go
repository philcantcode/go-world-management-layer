package command

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestGuestTransportPreservesArbitraryExecStart(t *testing.T) {
	process := newFakeProcess()
	process.stdin.onClose = finishGuestWithTerminal(process, transport.Terminal{CleanupConfirmed: true})
	starter := starterFunc(func(_ context.Context, invocation Invocation) (Process, error) {
		if invocation.Program != "docker" || !reflect.DeepEqual(invocation.Args, []string{"exec", "container", "world-guest"}) {
			t.Fatalf("host invocation changed: %#v", invocation)
		}
		return process, nil
	})
	start := transport.ExecStart{
		ExecID: "exec-arbitrary", IdempotencyKey: "key", Executable: "/bin/odd name",
		Argv:             []string{"--literal", "$(not-a-host-shell)", "a b", "'quotes'", "semi;colon"},
		WorkingDirectory: "/target", Environment: map[string]string{"OPAQUE": "a b;$()"},
		Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, err := StartGuestTransport(ctx, starter, Invocation{Program: "docker", Args: []string{"exec", "container", "world-guest"}}, start, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := transport.NewDecoder(bytes.NewReader(process.stdin.Bytes()), transport.DefaultMaxFrame).Read()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := transport.DecodeJSON[transport.ExecStart](frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Kind != transport.KindStart || !reflect.DeepEqual(decoded.Argv, start.Argv) || decoded.Executable != start.Executable || !reflect.DeepEqual(decoded.Environment, start.Environment) {
		t.Fatalf("start was changed: %#v", decoded)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGuestTransportStartFailureReapsStartedProcess(t *testing.T) {
	process := newFakeProcess()
	if err := process.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	start := transport.ExecStart{
		ExecID: "exec-start-failure", IdempotencyKey: "start-failure", Executable: "/bin/tool",
		WorkingDirectory: "/workspace", Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: 100 * time.Millisecond,
	}
	if _, err := StartGuestTransport(context.Background(), starterFunc(func(context.Context, Invocation) (Process, error) {
		return process, nil
	}), Invocation{Program: "docker"}, start, 100*time.Millisecond); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("StartGuestTransport() error = %v, want start-frame write failure", err)
	}
	select {
	case <-process.done:
	default:
		t.Fatal("start-frame failure left the started process running")
	}
}

func TestGuestTransportCloseUsesPortableEOFBeforeForcedCleanup(t *testing.T) {
	process := newFakeProcess()
	process.stdin.onClose = finishGuestWithTerminal(process, transport.Terminal{CleanupConfirmed: true})
	starter := starterFunc(func(context.Context, Invocation) (Process, error) {
		return &signalRejectingProcess{fakeProcess: process}, nil
	})
	start := transport.ExecStart{
		ExecID: "exec-close", IdempotencyKey: "close-key", Executable: "/bin/tool",
		Argv: []string{"--version"}, WorkingDirectory: "/workspace",
		Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: 100 * time.Millisecond,
	}
	session, err := StartGuestTransport(context.Background(), starter, Invocation{Program: "docker"}, start, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("portable EOF cleanup: %v", err)
	}
}

func TestGuestTransportCancelledReceiveDoesNotLoseCleanupTerminal(t *testing.T) {
	process := newFakeProcess()
	process.stdin.onClose = finishGuestWithTerminal(process, transport.Terminal{CleanupConfirmed: true})
	start := transport.ExecStart{
		ExecID: "exec-cancelled-receive", IdempotencyKey: "cancelled-receive", Executable: "/bin/tool",
		WorkingDirectory: "/workspace", Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: 100 * time.Millisecond,
	}
	session, err := StartGuestTransport(context.Background(), starterFunc(func(context.Context, Invocation) (Process, error) {
		return process, nil
	}), Invocation{Program: "docker"}, start, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	receiveContext, cancelReceive := context.WithCancel(context.Background())
	cancelReceive()
	if _, err := session.Receive(receiveContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive() error = %v, want context cancellation", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close() lost cleanup terminal after cancelled receive: %v", err)
	}
}

func TestGuestTransportCloseRequiresAuthoritativeCleanupTerminal(t *testing.T) {
	processFailure := errors.New("world-guest exited nonzero for specimen result")
	tests := []struct {
		name     string
		terminal *transport.Terminal
		waitErr  error
		wantErr  bool
	}{
		{name: "cleanup confirmed despite specimen exit", terminal: &transport.Terminal{ExitCode: 9, CleanupConfirmed: true, Error: "specimen failed"}, waitErr: processFailure},
		{name: "cleanup unconfirmed", terminal: &transport.Terminal{ExitCode: -1, CleanupConfirmed: false, Error: "cleanup-unconfirmed"}, wantErr: true},
		{name: "terminal missing", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			process := newFakeProcess()
			process.waitErr = test.waitErr
			if test.terminal == nil {
				process.stdin.onClose = process.finish
			} else {
				process.stdin.onClose = finishGuestWithTerminal(process, *test.terminal)
			}
			start := transport.ExecStart{
				ExecID: "exec-cleanup-proof", IdempotencyKey: "cleanup-proof", Executable: "/bin/specimen",
				WorkingDirectory: "/target", Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: 10 * time.Millisecond,
			}
			session, err := StartGuestTransport(context.Background(), starterFunc(func(context.Context, Invocation) (Process, error) {
				return process, nil
			}), Invocation{Program: "docker"}, start, 10*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			err = session.Close()
			if test.wantErr {
				if !errors.Is(err, transport.ErrCleanupUnknown) {
					t.Fatalf("Close() error = %v, want cleanup unknown", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Close() rejected authoritative cleanup receipt: %v", err)
			}
		})
	}
}

func TestProcessTransportForcedKillNeverClaimsCleanup(t *testing.T) {
	process := newFakeProcess()
	session, err := StartTransport(context.Background(), starterFunc(func(context.Context, Invocation) (Process, error) {
		return process, nil
	}), Invocation{Program: "specimen"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); !errors.Is(err, transport.ErrCleanupUnknown) {
		t.Fatalf("forced-kill Close() error = %v, want cleanup unknown", err)
	}
}

func TestTransportClosePersistsCleanupFailureAcrossRetriesAndConcurrency(t *testing.T) {
	cleanupErr := errors.New("process cleanup failed")
	tests := []struct {
		name string
		open func(*nonExitingProcess) ports.ExecTransport
	}{
		{
			name: "framed guest",
			open: func(process *nonExitingProcess) ports.ExecTransport {
				readDone := make(chan struct{})
				close(readDone)
				return &guestTransport{
					process: process, stdin: &synchronizedWriteCloser{},
					stdout: io.NopCloser(bytes.NewReader(nil)), stderr: io.NopCloser(bytes.NewReader(nil)),
					done: make(chan struct{}), readDone: readDone, closing: make(chan struct{}), cleanupGrace: time.Millisecond,
				}
			},
		},
		{
			name: "direct process",
			open: func(process *nonExitingProcess) ports.ExecTransport {
				return &processTransport{
					process: process, stdin: &synchronizedWriteCloser{}, done: make(chan struct{}),
					closing: make(chan struct{}), cleanupGrace: time.Millisecond,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			process := &nonExitingProcess{killErr: cleanupErr}
			session := test.open(process)
			const callers = 8
			errorsSeen := make(chan error, callers)
			var callersDone sync.WaitGroup
			callersDone.Add(callers)
			for range callers {
				go func() {
					defer callersDone.Done()
					errorsSeen <- session.Close()
				}()
			}
			callersDone.Wait()
			close(errorsSeen)
			for err := range errorsSeen {
				if !errors.Is(err, cleanupErr) || !errors.Is(err, transport.ErrCleanupUnknown) {
					t.Fatalf("concurrent Close() error = %v", err)
				}
			}
			if err := session.Close(); !errors.Is(err, cleanupErr) || !errors.Is(err, transport.ErrCleanupUnknown) {
				t.Fatalf("retried Close() error = %v", err)
			}
			if calls := process.killCount(); calls != 1 {
				t.Fatalf("process Kill() calls = %d, want 1", calls)
			}
		})
	}
}

type starterFunc func(context.Context, Invocation) (Process, error)

func (f starterFunc) Start(ctx context.Context, invocation Invocation) (Process, error) {
	return f(ctx, invocation)
}

type synchronizedWriteCloser struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	closed  bool
	onClose func()
}

func (w *synchronizedWriteCloser) Write(value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.buffer.Write(value)
}
func (w *synchronizedWriteCloser) Close() error {
	w.mu.Lock()
	w.closed = true
	onClose := w.onClose
	w.mu.Unlock()
	if onClose != nil {
		onClose()
	}
	return nil
}
func (w *synchronizedWriteCloser) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buffer.Bytes()...)
}

type fakeProcess struct {
	stdin   *synchronizedWriteCloser
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter
	done    chan struct{}
	once    sync.Once
	waitErr error
}

func newFakeProcess() *fakeProcess {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &fakeProcess{stdin: &synchronizedWriteCloser{}, stdoutR: stdoutR, stdoutW: stdoutW, stderrR: stderrR, stderrW: stderrW, done: make(chan struct{})}
}
func (p *fakeProcess) Stdin() io.WriteCloser  { return p.stdin }
func (p *fakeProcess) Stdout() io.ReadCloser  { return p.stdoutR }
func (p *fakeProcess) Stderr() io.ReadCloser  { return p.stderrR }
func (p *fakeProcess) Wait() error            { <-p.done; return p.waitErr }
func (p *fakeProcess) Signal(os.Signal) error { p.finish(); return nil }
func (p *fakeProcess) Kill() error            { p.finish(); return nil }
func (p *fakeProcess) finish() {
	p.once.Do(func() { _ = p.stdoutW.Close(); _ = p.stderrW.Close(); close(p.done) })
}

func finishGuestWithTerminal(process *fakeProcess, terminal transport.Terminal) func() {
	return func() {
		_, _ = transport.NewEncoder(process.stdoutW, transport.DefaultMaxFrame).WriteJSON(transport.KindTerminal, terminal)
		process.finish()
	}
}

var _ Starter = starterFunc(nil)

type signalRejectingProcess struct{ *fakeProcess }

func (*signalRejectingProcess) Signal(os.Signal) error {
	return errors.New("host interrupt is unsupported")
}

type nonExitingProcess struct {
	mu      sync.Mutex
	kills   int
	killErr error
}

func (*nonExitingProcess) Stdin() io.WriteCloser  { return &synchronizedWriteCloser{} }
func (*nonExitingProcess) Stdout() io.ReadCloser  { return io.NopCloser(bytes.NewReader(nil)) }
func (*nonExitingProcess) Stderr() io.ReadCloser  { return io.NopCloser(bytes.NewReader(nil)) }
func (*nonExitingProcess) Wait() error            { return nil }
func (*nonExitingProcess) Signal(os.Signal) error { return nil }
func (p *nonExitingProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.kills++
	return p.killErr
}
func (p *nonExitingProcess) killCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kills
}
