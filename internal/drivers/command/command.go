// Package command provides the narrow process invocation seam used by host
// drivers. Driver plans remain vendor neutral and tests can replace Runner and
// Starter without requiring Docker, ADB, KVM, or a Linux host.
package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

const DefaultOutputLimit int64 = 8 << 20

// Invocation is passed directly to exec.CommandContext. Program and Args are
// intentionally separate: drivers never join guest-supplied arguments into a
// host shell command.
type Invocation struct {
	Program       string
	Args          []string
	Directory     string
	Environment   []string
	Stdin         io.Reader
	MaximumOutput int64
}

func (i Invocation) Validate() error {
	if i.Program == "" {
		return fmt.Errorf("command program is required")
	}
	for _, value := range append([]string{i.Program}, i.Args...) {
		for _, b := range []byte(value) {
			if b == 0 {
				return fmt.Errorf("command contains NUL")
			}
		}
	}
	if i.MaximumOutput < 0 {
		return fmt.Errorf("maximum output cannot be negative")
	}
	return nil
}

type Result struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

type Runner interface {
	Run(context.Context, Invocation) (Result, error)
}

type Starter interface {
	Start(context.Context, Invocation) (Process, error)
}

type Process interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait() error
	Signal(os.Signal) error
	Kill() error
}

// OS executes host tools directly without involving a shell.
type OS struct{}

func (OS) Run(ctx context.Context, invocation Invocation) (Result, error) {
	if err := invocation.Validate(); err != nil {
		return Result{}, err
	}
	limit := invocation.MaximumOutput
	if limit == 0 {
		limit = DefaultOutputLimit
	}
	cmd := buildCommand(ctx, invocation)
	var stdout, stderr limitedBuffer
	stdout.remaining = limit
	stderr.remaining = limit
	cmd.Stdout, cmd.Stderr, cmd.Stdin = &stdout, &stderr, invocation.Stdin
	err := cmd.Run()
	result := Result{Stdout: stdout.bytes(), Stderr: stderr.bytes(), ExitCode: exitCode(err)}
	if stdout.exceeded || stderr.exceeded {
		return result, fmt.Errorf("command output exceeded %d bytes", limit)
	}
	if err != nil {
		return result, fmt.Errorf("run %s: %w: %s", invocation.Program, err, string(result.Stderr))
	}
	return result, nil
}

func (OS) Start(ctx context.Context, invocation Invocation) (Process, error) {
	if err := invocation.Validate(); err != nil {
		return nil, err
	}
	cmd := buildCommand(ctx, invocation)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	return &osProcess{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr}, nil
}

func buildCommand(ctx context.Context, invocation Invocation) *exec.Cmd {
	cmd := exec.CommandContext(ctx, invocation.Program, append([]string(nil), invocation.Args...)...)
	cmd.Dir = invocation.Directory
	if invocation.Environment != nil {
		cmd.Env = append([]string(nil), invocation.Environment...)
	}
	return cmd
}

type osProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *osProcess) Stdin() io.WriteCloser         { return p.stdin }
func (p *osProcess) Stdout() io.ReadCloser         { return p.stdout }
func (p *osProcess) Stderr() io.ReadCloser         { return p.stderr }
func (p *osProcess) Wait() error                   { return p.cmd.Wait() }
func (p *osProcess) Kill() error                   { return p.cmd.Process.Kill() }
func (p *osProcess) Signal(signal os.Signal) error { return p.cmd.Process.Signal(signal) }

type limitedBuffer struct {
	buffer    bytes.Buffer
	remaining int64
	exceeded  bool
}

func (b *limitedBuffer) Write(value []byte) (int, error) {
	if int64(len(value)) > b.remaining {
		allowed := b.remaining
		if allowed > 0 {
			_, _ = b.buffer.Write(value[:allowed])
		}
		b.remaining = 0
		b.exceeded = true
		return len(value), nil
	}
	b.remaining -= int64(len(value))
	return b.buffer.Write(value)
}

func (b *limitedBuffer) bytes() []byte { return append([]byte(nil), b.buffer.Bytes()...) }

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}

// StartTransport converts a process into the framed, ordered port contract.
// The process invocation has already selected a specific container/device; the
// opaque guest command is never interpreted here.
func StartTransport(ctx context.Context, starter Starter, invocation Invocation, cleanupGrace time.Duration) (ports.ExecTransport, error) {
	if starter == nil {
		return nil, fmt.Errorf("command starter is required")
	}
	if cleanupGrace <= 0 {
		return nil, fmt.Errorf("positive cleanup grace is required")
	}
	process, err := starter.Start(ctx, invocation)
	if err != nil {
		return nil, err
	}
	t := &processTransport{
		process:      process,
		stdin:        process.Stdin(),
		frames:       make(chan transport.Frame, 32),
		done:         make(chan struct{}),
		closing:      make(chan struct{}),
		cleanupGrace: cleanupGrace,
	}
	t.start(process.Stdout(), process.Stderr())
	return t, nil
}

// StartGuestTransport starts a world-guest process, writes the mandatory start
// frame, and then exposes the remaining framed session. Unlike StartTransport,
// stdout is decoded as protocol frames rather than treated as raw guest output.
func StartGuestTransport(ctx context.Context, starter Starter, invocation Invocation, start transport.ExecStart, cleanupGrace time.Duration) (ports.ExecTransport, error) {
	if starter == nil {
		return nil, fmt.Errorf("command starter is required")
	}
	if err := start.Validate(64 << 20); err != nil {
		return nil, err
	}
	if cleanupGrace <= 0 {
		return nil, fmt.Errorf("positive cleanup grace is required")
	}
	process, err := starter.Start(ctx, invocation)
	if err != nil {
		return nil, err
	}
	stdin, stdout, stderr := process.Stdin(), process.Stdout(), process.Stderr()
	t := &guestTransport{
		process:      process,
		stdin:        stdin,
		stdout:       stdout,
		stderr:       stderr,
		encoder:      transport.NewEncoder(stdin, transport.DefaultMaxFrame),
		decoder:      transport.NewDecoder(stdout, transport.DefaultMaxFrame),
		cleanupGrace: cleanupGrace,
		done:         make(chan struct{}),
	}
	if _, err := t.encoder.WriteJSON(transport.KindStart, start); err != nil {
		_ = process.Kill()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	go func() {
		_, _ = io.Copy(io.Discard, stderr)
	}()
	go func() {
		t.waitErr = process.Wait()
		close(t.done)
	}()
	return t, nil
}

type guestTransport struct {
	process      Process
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	encoder      *transport.Encoder
	decoder      *transport.Decoder
	cleanupGrace time.Duration
	done         chan struct{}

	sendMu    sync.Mutex
	receiveMu sync.Mutex
	stateMu   sync.Mutex
	closed    bool
	waitErr   error
	closeOnce sync.Once
}

func (t *guestTransport) Send(ctx context.Context, kind transport.Kind, data []byte) (transport.Frame, error) {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return transport.Frame{}, err
	}
	t.stateMu.Lock()
	closed := t.closed
	t.stateMu.Unlock()
	if closed {
		return transport.Frame{}, io.ErrClosedPipe
	}
	return t.encoder.Write(kind, data)
}

func (t *guestTransport) Receive(ctx context.Context) (transport.Frame, error) {
	t.receiveMu.Lock()
	defer t.receiveMu.Unlock()
	t.stateMu.Lock()
	closed := t.closed
	t.stateMu.Unlock()
	if closed {
		return transport.Frame{}, io.ErrClosedPipe
	}
	type result struct {
		frame transport.Frame
		err   error
	}
	resultChannel := make(chan result, 1)
	go func() {
		frame, err := t.decoder.Read()
		resultChannel <- result{frame: frame, err: err}
	}()
	select {
	case <-ctx.Done():
		go func() { _ = t.Close() }()
		return transport.Frame{}, ctx.Err()
	case value := <-resultChannel:
		return value.frame, value.err
	}
}

func (t *guestTransport) Close() error {
	var result error
	t.closeOnce.Do(func() {
		t.stateMu.Lock()
		t.closed = true
		t.stateMu.Unlock()
		_ = t.stdin.Close()
		result = awaitProcessExit(t.process, t.done, t.cleanupGrace)
		_ = t.stdout.Close()
		_ = t.stderr.Close()
	})
	return result
}

type processTransport struct {
	process      Process
	stdin        io.WriteCloser
	frames       chan transport.Frame
	done         chan struct{}
	closing      chan struct{}
	cleanupGrace time.Duration

	sendMu          sync.Mutex
	receiveMu       sync.Mutex
	stateMu         sync.Mutex
	sendSequence    uint64
	receiveSequence uint64
	closed          bool
	closeOnce       sync.Once
	terminalOnce    sync.Once
	writers         sync.WaitGroup
}

func (t *processTransport) start(stdout, stderr io.ReadCloser) {
	t.writers.Add(2)
	go t.readStream(transport.KindStdout, stdout)
	go t.readStream(transport.KindStderr, stderr)
	go func() {
		err := t.process.Wait()
		t.writers.Wait()
		terminal := transport.Terminal{ExitCode: exitCode(err), CleanupConfirmed: true}
		if err != nil {
			terminal.Error = err.Error()
		}
		t.emitTerminal(terminal)
	}()
}

func (t *processTransport) readStream(kind transport.Kind, reader io.ReadCloser) {
	defer t.writers.Done()
	defer reader.Close()
	buffer := make([]byte, transport.DefaultChunkSize)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			t.emit(kind, append([]byte(nil), buffer[:n]...))
		}
		if err != nil {
			return
		}
	}
}

func (t *processTransport) Send(ctx context.Context, kind transport.Kind, data []byte) (transport.Frame, error) {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return transport.Frame{}, err
	}
	t.stateMu.Lock()
	closed := t.closed
	t.stateMu.Unlock()
	if closed {
		return transport.Frame{}, io.ErrClosedPipe
	}
	switch kind {
	case transport.KindStdin:
		if _, err := t.stdin.Write(data); err != nil {
			return transport.Frame{}, err
		}
	case transport.KindCloseInput:
		if err := t.stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			return transport.Frame{}, err
		}
	case transport.KindSignal:
		signal, err := transport.DecodeJSON[transport.Signal](transport.Frame{Kind: kind, Data: data})
		if err != nil {
			return transport.Frame{}, err
		}
		if signal.Name != "KILL" && signal.Name != "TERM" && signal.Name != "INT" {
			return transport.Frame{}, fmt.Errorf("unsupported process signal %q", signal.Name)
		}
		if err := t.process.Signal(namedSignal(signal.Name)); err != nil {
			return transport.Frame{}, err
		}
	case transport.KindResize, transport.KindHeartbeat:
		// Resize is transport metadata. CLI backends that support a PTY can
		// provide their own Starter; the generic process has no resize API.
	default:
		return transport.Frame{}, fmt.Errorf("frame kind %d cannot be sent to a process", kind)
	}
	t.sendSequence++
	return transport.Frame{Sequence: t.sendSequence, Kind: kind, Data: append([]byte(nil), data...)}, nil
}

func namedSignal(name string) os.Signal {
	switch name {
	case "KILL":
		return os.Kill
	default:
		return os.Interrupt
	}
}

func (t *processTransport) Receive(ctx context.Context) (transport.Frame, error) {
	t.receiveMu.Lock()
	defer t.receiveMu.Unlock()
	select {
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	case frame, ok := <-t.frames:
		if !ok {
			return transport.Frame{}, io.EOF
		}
		return frame, nil
	}
}

func (t *processTransport) Close() error {
	var result error
	t.closeOnce.Do(func() {
		t.stateMu.Lock()
		t.closed = true
		t.stateMu.Unlock()
		close(t.closing)
		_ = t.stdin.Close()
		result = awaitProcessExit(t.process, t.done, t.cleanupGrace)
	})
	return result
}

// awaitProcessExit gives stdin EOF one complete cleanup window to drive the
// framed guest or direct child to a normal exit. This is portable across host
// platforms where os.Interrupt is not a supported process signal. A forced
// kill gets a second bounded window so Close can never wait forever.
func awaitProcessExit(process Process, done <-chan struct{}, grace time.Duration) error {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
	}
	killErr := process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	timer.Reset(grace)
	select {
	case <-done:
		return killErr
	case <-timer.C:
		return errors.Join(killErr, transport.ErrCleanupUnknown)
	}
}

func (t *processTransport) emit(kind transport.Kind, data []byte) {
	t.stateMu.Lock()
	if t.closed {
		t.stateMu.Unlock()
		return
	}
	t.receiveSequence++
	frame := transport.Frame{Sequence: t.receiveSequence, Kind: kind, Data: data}
	t.stateMu.Unlock()
	select {
	case t.frames <- frame:
	case <-t.closing:
	case <-t.done:
	}
}

func (t *processTransport) emitTerminal(terminal transport.Terminal) {
	t.terminalOnce.Do(func() {
		encoded, _ := encodeTerminal(terminal)
		t.stateMu.Lock()
		t.receiveSequence++
		frame := transport.Frame{Sequence: t.receiveSequence, Kind: transport.KindTerminal, Data: encoded}
		t.closed = true
		t.stateMu.Unlock()
		select {
		case t.frames <- frame:
		case <-t.closing:
		}
		close(t.done)
		close(t.frames)
	})
}

func encodeTerminal(terminal transport.Terminal) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := transport.NewEncoder(&buffer, transport.DefaultMaxFrame)
	frame, err := encoder.WriteJSON(transport.KindTerminal, terminal)
	return frame.Data, err
}

var _ Runner = OS{}
var _ Starter = OS{}
var _ ports.ExecTransport = (*processTransport)(nil)
var _ ports.ExecTransport = (*guestTransport)(nil)
