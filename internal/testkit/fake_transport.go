package testkit

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type FakeExecTransport struct {
	mu         sync.Mutex
	sent       []transport.Frame
	receive    chan transport.Frame
	closed     chan struct{}
	closeOnce  sync.Once
	sendSeq    uint64
	receiveSeq uint64
	terminal   bool
	onClose    func()
}

func NewFakeExecTransport(onClose func()) *FakeExecTransport {
	return &FakeExecTransport{receive: make(chan transport.Frame, 64), closed: make(chan struct{}), onClose: onClose}
}

func (t *FakeExecTransport) Send(ctx context.Context, kind transport.Kind, data []byte) (transport.Frame, error) {
	if err := ports.RequireDeadline(ctx, "fake_exec.send"); err != nil {
		return transport.Frame{}, err
	}
	select {
	case <-t.closed:
		return transport.Frame{}, io.ErrClosedPipe
	default:
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sendSeq++
	frame := transport.Frame{Sequence: t.sendSeq, Kind: kind, Data: append([]byte(nil), data...)}
	t.sent = append(t.sent, frame)
	return frame, nil
}

func (t *FakeExecTransport) Receive(ctx context.Context) (transport.Frame, error) {
	if err := ports.RequireDeadline(ctx, "fake_exec.receive"); err != nil {
		return transport.Frame{}, err
	}
	select {
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	case <-t.closed:
		return transport.Frame{}, io.EOF
	case frame := <-t.receive:
		return transport.Frame{Sequence: frame.Sequence, Kind: frame.Kind, Data: append([]byte(nil), frame.Data...)}, nil
	}
}

func (t *FakeExecTransport) Queue(kind transport.Kind, data []byte) error {
	t.mu.Lock()
	if t.terminal {
		t.mu.Unlock()
		return transport.ErrTerminal
	}
	t.receiveSeq++
	frame := transport.Frame{Sequence: t.receiveSeq, Kind: kind, Data: append([]byte(nil), data...)}
	if kind == transport.KindTerminal {
		t.terminal = true
	}
	t.mu.Unlock()
	select {
	case <-t.closed:
		return io.ErrClosedPipe
	case t.receive <- frame:
		return nil
	}
}

func (t *FakeExecTransport) Sent() []transport.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]transport.Frame, len(t.sent))
	for index, frame := range t.sent {
		result[index] = transport.Frame{Sequence: frame.Sequence, Kind: frame.Kind, Data: append([]byte(nil), frame.Data...)}
	}
	return result
}

func (t *FakeExecTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		if t.onClose != nil {
			t.onClose()
		}
	})
	return nil
}

var _ ports.ExecTransport = (*FakeExecTransport)(nil)

type FakeTargetTransport struct {
	mu       sync.Mutex
	runID    domain.TargetRunID
	files    map[string][]byte
	tracker  *OwnershipTracker
	closed   bool
	execNo   uint64
	endpoint *FakeADBEndpoint
	closeFn  func()
}

func NewFakeTargetTransport(runID domain.TargetRunID, tracker *OwnershipTracker) *FakeTargetTransport {
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return newFakeTargetTransport(runID, tracker, nil)
}

func newFakeTargetTransport(runID domain.TargetRunID, tracker *OwnershipTracker, closeFn func()) *FakeTargetTransport {
	return &FakeTargetTransport{runID: runID, files: make(map[string][]byte), tracker: tracker, closeFn: closeFn}
}

func (t *FakeTargetTransport) OpenExec(ctx context.Context, plan ports.TargetExecPlan) (ports.ExecTransport, error) {
	if err := ports.RequireDeadline(ctx, "fake_target_transport.open_exec"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if plan.Operation.Spec().TargetRunID != t.runID {
		return nil, domain.NewError(domain.CodeForbidden, "fake_target_transport.open_exec", "target_run_id", "operation belongs to another run", nil)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, io.ErrClosedPipe
	}
	t.execNo++
	id := plan.Operation.ID().String()
	if err := t.tracker.Acquire("target_exec", id, t.runID.String()); err != nil {
		return nil, err
	}
	return NewFakeExecTransport(func() { _ = t.tracker.Release("target_exec", id, t.runID.String()) }), nil
}

func (t *FakeTargetTransport) PushFile(ctx context.Context, plan ports.TargetTransferPlan, reader io.Reader) (ports.TransferResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_target_transport.push"); err != nil {
		return ports.TransferResult{}, err
	}
	if err := plan.Validate(domain.TargetOperationPush); err != nil {
		return ports.TransferResult{}, err
	}
	if plan.Operation.Spec().TargetRunID != t.runID {
		return ports.TransferResult{}, domain.NewError(domain.CodeForbidden, "fake_target_transport.push", "target_run_id", "operation belongs to another run", nil)
	}
	limited := io.LimitReader(reader, plan.MaximumBytes+1)
	content, err := io.ReadAll(limited)
	if err != nil {
		return ports.TransferResult{}, err
	}
	if int64(len(content)) > plan.MaximumBytes {
		return ports.TransferResult{}, domain.NewError(domain.CodeResourceExhausted, "fake_target_transport.push", "maximum_bytes", "transfer limit exceeded", nil)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ports.TransferResult{}, io.ErrClosedPipe
	}
	t.files[plan.RelativePath] = append([]byte(nil), content...)
	return ports.TransferResult{OperationID: plan.Operation.ID(), Digest: domain.NewDigest(content), Bytes: int64(len(content))}, nil
}

func (t *FakeTargetTransport) PullFile(ctx context.Context, plan ports.TargetTransferPlan) (io.ReadCloser, error) {
	if err := ports.RequireDeadline(ctx, "fake_target_transport.pull"); err != nil {
		return nil, err
	}
	if err := plan.Validate(domain.TargetOperationPull); err != nil {
		return nil, err
	}
	if plan.Operation.Spec().TargetRunID != t.runID {
		return nil, domain.NewError(domain.CodeForbidden, "fake_target_transport.pull", "target_run_id", "operation belongs to another run", nil)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, io.ErrClosedPipe
	}
	content, found := t.files[plan.RelativePath]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "fake_target_transport.pull", "relative_path", "file not found", nil)
	}
	if int64(len(content)) > plan.MaximumBytes {
		return nil, domain.NewError(domain.CodeResourceExhausted, "fake_target_transport.pull", "maximum_bytes", "transfer limit exceeded", nil)
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), content...))), nil
}

func (t *FakeTargetTransport) OpenADB(ctx context.Context) (ports.ScopedADBEndpoint, error) {
	if err := ports.RequireDeadline(ctx, "fake_target_transport.open_adb"); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, io.ErrClosedPipe
	}
	if t.endpoint == nil {
		id := "adb-" + t.runID.String()
		if err := t.tracker.Acquire("adb_endpoint", id, t.runID.String()); err != nil {
			return nil, err
		}
		t.endpoint = &FakeADBEndpoint{serial: "serial-" + t.runID.String(), address: "127.0.0.1:5038", closeFn: func() { _ = t.tracker.Release("adb_endpoint", id, t.runID.String()) }}
	}
	return t.endpoint, nil
}

func (t *FakeTargetTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	if t.endpoint != nil {
		_ = t.endpoint.Close()
	}
	if t.closeFn != nil {
		t.closeFn()
	}
	return nil
}

var _ ports.TargetTransport = (*FakeTargetTransport)(nil)

type FakeADBEndpoint struct {
	mu      sync.Mutex
	serial  string
	address string
	closed  bool
	closeFn func()
}

func (e *FakeADBEndpoint) Serial() string  { return e.serial }
func (e *FakeADBEndpoint) Address() string { return e.address }
func (e *FakeADBEndpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed {
		e.closed = true
		if e.closeFn != nil {
			e.closeFn()
		}
	}
	return nil
}

var _ ports.ScopedADBEndpoint = (*FakeADBEndpoint)(nil)
