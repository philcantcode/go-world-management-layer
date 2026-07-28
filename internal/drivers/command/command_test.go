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

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestGuestTransportPreservesArbitraryExecStart(t *testing.T) {
	process := newFakeProcess()
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

func TestGuestTransportCloseUsesPortableEOFBeforeForcedCleanup(t *testing.T) {
	process := newFakeProcess()
	process.stdin.onClose = process.finish
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
}

func newFakeProcess() *fakeProcess {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &fakeProcess{stdin: &synchronizedWriteCloser{}, stdoutR: stdoutR, stdoutW: stdoutW, stderrR: stderrR, stderrW: stderrW, done: make(chan struct{})}
}
func (p *fakeProcess) Stdin() io.WriteCloser  { return p.stdin }
func (p *fakeProcess) Stdout() io.ReadCloser  { return p.stdoutR }
func (p *fakeProcess) Stderr() io.ReadCloser  { return p.stderrR }
func (p *fakeProcess) Wait() error            { <-p.done; return nil }
func (p *fakeProcess) Signal(os.Signal) error { p.finish(); return nil }
func (p *fakeProcess) Kill() error            { p.finish(); return nil }
func (p *fakeProcess) finish() {
	p.once.Do(func() { _ = p.stdoutW.Close(); _ = p.stderrW.Close(); close(p.done) })
}

var _ Starter = starterFunc(nil)

type signalRejectingProcess struct{ *fakeProcess }

func (*signalRejectingProcess) Signal(os.Signal) error {
	return errors.New("host interrupt is unsupported")
}
