package world

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// ExecSession is an in-process agent exec session. It also implements the
// Send/Recv/CloseSend shape used by CLI stream pumps.
type ExecSession interface {
	Start(ctx context.Context, start *worldv1.ExecStart) error
	WriteStdin([]byte) error
	Signal(string) error
	Resize(*worldv1.TerminalSettings) error
	Heartbeat() error
	Send(*worldv1.ExecFrame) error
	Recv() (*worldv1.ExecFrame, error)
	CloseSend() error
	Close() error
}

// TargetExecSession is an in-process target exec session.
type TargetExecSession interface {
	Start(ctx context.Context, start *worldv1.TargetExecStart) error
	WriteStdin([]byte) error
	Signal(string) error
	Resize(*worldv1.TerminalSettings) error
	Heartbeat() error
	Send(*worldv1.TargetExecFrame) error
	Recv() (*worldv1.TargetExecFrame, error)
	CloseSend() error
	Close() error
}

// FileTransferSession is an in-process target file transfer session.
type FileTransferSession interface {
	Send(*worldv1.FileTransferFrame) error
	Recv() (*worldv1.FileTransferFrame, error)
	CloseSend() error
	Close() error
}

// ADBSession is an in-process scoped ADB session.
type ADBSession interface {
	Start(ctx context.Context, start *worldv1.ADBStart) error
	WriteClientBytes([]byte) error
	Complete() error
	Send(*worldv1.ADBFrame) error
	Recv() (*worldv1.ADBFrame, error)
	CloseSend() error
	Close() error
}

// ObservationSubscription streams durable observation records.
type ObservationSubscription interface {
	Recv() (*worldv1.ObservationRecord, error)
	Close() error
}

// MetricSubscription streams metric samples.
type MetricSubscription interface {
	Recv() (*worldv1.MetricSample, error)
	Close() error
}

// OpenExec opens an in-process agent exec session.
func (m *Manager) OpenExec(ctx context.Context) (ExecSession, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	bound := m.withSubject(ctx)
	pipe := newBidiPipe[worldv1.ExecFrame](bound)
	go func() {
		err := m.facade.OpenExec(pipe.server)
		pipe.finishServer(err)
	}()
	return &execSession{pipe: pipe}, nil
}

// OpenTargetExec opens an in-process target exec session.
func (m *Manager) OpenTargetExec(ctx context.Context) (TargetExecSession, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	bound := m.withSubject(ctx)
	pipe := newBidiPipe[worldv1.TargetExecFrame](bound)
	go func() {
		err := m.facade.OpenTargetExec(pipe.server)
		pipe.finishServer(err)
	}()
	return &targetExecSession{pipe: pipe}, nil
}

// PushTargetFile opens an in-process workspace→target file transfer session.
func (m *Manager) PushTargetFile(ctx context.Context) (FileTransferSession, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	bound := m.withSubject(ctx)
	pipe := newBidiPipe[worldv1.FileTransferFrame](bound)
	go func() {
		err := m.facade.PushTargetFile(pipe.server)
		pipe.finishServer(err)
	}()
	return &fileTransferSession{pipe: pipe}, nil
}

// PullTargetFile opens an in-process target→workspace file transfer session.
func (m *Manager) PullTargetFile(ctx context.Context, request *worldv1.PullTargetFileRequest) (FileTransferSession, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, fmt.Errorf("pull target file request is required")
	}
	bound := m.withSubject(ctx)
	pipe := newServerPipe[worldv1.FileTransferFrame](bound)
	go func() {
		err := m.facade.PullTargetFile(request, pipe.server)
		pipe.finishServer(err)
	}()
	return &serverFileTransferSession{pipe: pipe}, nil
}

// OpenTargetADB opens an in-process scoped ADB session.
func (m *Manager) OpenTargetADB(ctx context.Context) (ADBSession, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	bound := m.withSubject(ctx)
	pipe := newBidiPipe[worldv1.ADBFrame](bound)
	go func() {
		err := m.facade.OpenTargetADB(pipe.server)
		pipe.finishServer(err)
	}()
	return &adbSession{pipe: pipe}, nil
}

// SubscribeObservations opens an in-process observation subscription.
func (m *Manager) SubscribeObservations(ctx context.Context, request *worldv1.SubscribeObservationsRequest) (ObservationSubscription, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, fmt.Errorf("subscribe observations request is required")
	}
	bound := m.withSubject(ctx)
	pipe := newServerPipe[worldv1.ObservationRecord](bound)
	go func() {
		err := m.facade.SubscribeObservations(request, pipe.server)
		pipe.finishServer(err)
	}()
	return &observationSubscription{pipe: pipe}, nil
}

// SubscribeMetrics opens an in-process metric subscription.
func (m *Manager) SubscribeMetrics(ctx context.Context, request *worldv1.SubscribeMetricsRequest) (MetricSubscription, error) {
	if err := m.requireOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, fmt.Errorf("subscribe metrics request is required")
	}
	bound := m.withSubject(ctx)
	pipe := newServerPipe[worldv1.MetricSample](bound)
	go func() {
		err := m.facade.SubscribeMetrics(request, pipe.server)
		pipe.finishServer(err)
	}()
	return &metricSubscription{pipe: pipe}, nil
}

type execSession struct{ pipe *bidiPipe[worldv1.ExecFrame] }

func (s *execSession) Start(_ context.Context, start *worldv1.ExecStart) error {
	if start == nil {
		return fmt.Errorf("exec start is required")
	}
	return s.Send(&worldv1.ExecFrame{Start: start})
}
func (s *execSession) WriteStdin(data []byte) error {
	return s.Send(&worldv1.ExecFrame{Stdin: append([]byte(nil), data...)})
}
func (s *execSession) Signal(signal string) error {
	return s.Send(&worldv1.ExecFrame{Signal: signal})
}
func (s *execSession) Resize(settings *worldv1.TerminalSettings) error {
	return s.Send(&worldv1.ExecFrame{Resize: settings})
}
func (s *execSession) Heartbeat() error {
	return s.Send(&worldv1.ExecFrame{Heartbeat: true})
}
func (s *execSession) Send(frame *worldv1.ExecFrame) error { return s.pipe.clientSend(frame) }
func (s *execSession) Recv() (*worldv1.ExecFrame, error)   { return s.pipe.clientRecv() }
func (s *execSession) CloseSend() error                    { return s.pipe.clientCloseSend() }
func (s *execSession) Close() error                        { return s.pipe.close() }

type targetExecSession struct {
	pipe *bidiPipe[worldv1.TargetExecFrame]
}

func (s *targetExecSession) Start(_ context.Context, start *worldv1.TargetExecStart) error {
	if start == nil {
		return fmt.Errorf("target exec start is required")
	}
	return s.Send(&worldv1.TargetExecFrame{Start: start})
}
func (s *targetExecSession) WriteStdin(data []byte) error {
	return s.Send(&worldv1.TargetExecFrame{Stdin: append([]byte(nil), data...)})
}
func (s *targetExecSession) Signal(signal string) error {
	return s.Send(&worldv1.TargetExecFrame{Signal: signal})
}
func (s *targetExecSession) Resize(settings *worldv1.TerminalSettings) error {
	return s.Send(&worldv1.TargetExecFrame{Resize: settings})
}
func (s *targetExecSession) Heartbeat() error {
	return s.Send(&worldv1.TargetExecFrame{Heartbeat: true})
}
func (s *targetExecSession) Send(frame *worldv1.TargetExecFrame) error {
	return s.pipe.clientSend(frame)
}
func (s *targetExecSession) Recv() (*worldv1.TargetExecFrame, error) { return s.pipe.clientRecv() }
func (s *targetExecSession) CloseSend() error                        { return s.pipe.clientCloseSend() }
func (s *targetExecSession) Close() error                            { return s.pipe.close() }

type fileTransferSession struct {
	pipe *bidiPipe[worldv1.FileTransferFrame]
}

func (s *fileTransferSession) Send(frame *worldv1.FileTransferFrame) error {
	return s.pipe.clientSend(frame)
}
func (s *fileTransferSession) Recv() (*worldv1.FileTransferFrame, error) {
	return s.pipe.clientRecv()
}
func (s *fileTransferSession) CloseSend() error { return s.pipe.clientCloseSend() }
func (s *fileTransferSession) Close() error     { return s.pipe.close() }

type serverFileTransferSession struct {
	pipe *serverPipe[worldv1.FileTransferFrame]
}

func (s *serverFileTransferSession) Send(*worldv1.FileTransferFrame) error {
	return fmt.Errorf("pull target file sessions are receive-only")
}
func (s *serverFileTransferSession) Recv() (*worldv1.FileTransferFrame, error) {
	return s.pipe.clientRecv()
}
func (s *serverFileTransferSession) CloseSend() error { return nil }
func (s *serverFileTransferSession) Close() error     { return s.pipe.close() }

type adbSession struct{ pipe *bidiPipe[worldv1.ADBFrame] }

func (s *adbSession) Start(_ context.Context, start *worldv1.ADBStart) error {
	if start == nil {
		return fmt.Errorf("ADB start is required")
	}
	return s.Send(&worldv1.ADBFrame{Start: start})
}
func (s *adbSession) WriteClientBytes(data []byte) error {
	return s.Send(&worldv1.ADBFrame{ClientBytes: append([]byte(nil), data...)})
}
func (s *adbSession) Complete() error {
	return s.Send(&worldv1.ADBFrame{Complete: true})
}
func (s *adbSession) Send(frame *worldv1.ADBFrame) error { return s.pipe.clientSend(frame) }
func (s *adbSession) Recv() (*worldv1.ADBFrame, error)   { return s.pipe.clientRecv() }
func (s *adbSession) CloseSend() error                   { return s.pipe.clientCloseSend() }
func (s *adbSession) Close() error                       { return s.pipe.close() }

type observationSubscription struct {
	pipe *serverPipe[worldv1.ObservationRecord]
}

func (s *observationSubscription) Recv() (*worldv1.ObservationRecord, error) {
	return s.pipe.clientRecv()
}
func (s *observationSubscription) Close() error { return s.pipe.close() }

type metricSubscription struct {
	pipe *serverPipe[worldv1.MetricSample]
}

func (s *metricSubscription) Recv() (*worldv1.MetricSample, error) { return s.pipe.clientRecv() }
func (s *metricSubscription) Close() error                         { return s.pipe.close() }

// bidiPipe is a channel-backed client/server pair for one in-process stream.
type bidiPipe[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	toServer   chan *T
	toClient   chan *T
	serverDone chan struct{}

	sendClosed atomic.Bool
	closed     atomic.Bool

	serverErrMu sync.Mutex
	serverErr   error

	server *bidiServerStream[T]
}

func newBidiPipe[T any](parent context.Context) *bidiPipe[T] {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	pipe := &bidiPipe[T]{
		ctx:        ctx,
		cancel:     cancel,
		toServer:   make(chan *T, 16),
		toClient:   make(chan *T, 16),
		serverDone: make(chan struct{}),
	}
	pipe.server = &bidiServerStream[T]{pipe: pipe}
	return pipe
}

func (p *bidiPipe[T]) clientSend(frame *T) error {
	if frame == nil {
		return fmt.Errorf("stream frame is nil")
	}
	if p.sendClosed.Load() || p.closed.Load() {
		return io.EOF
	}
	cloned, err := cloneFrame(frame)
	if err != nil {
		return err
	}
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.serverDone:
		if err := p.serverError(); err != nil {
			return err
		}
		return io.EOF
	case p.toServer <- cloned:
		return nil
	}
}

func (p *bidiPipe[T]) clientRecv() (*T, error) {
	select {
	case <-p.ctx.Done():
		// Prefer draining a final server frame before surfacing cancel.
		select {
		case frame := <-p.toClient:
			return frame, nil
		default:
		}
		if err := p.serverError(); err != nil {
			return nil, err
		}
		return nil, p.ctx.Err()
	case frame, ok := <-p.toClient:
		if !ok {
			if err := p.serverError(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		return frame, nil
	}
}

func (p *bidiPipe[T]) clientCloseSend() error {
	if !p.sendClosed.CompareAndSwap(false, true) {
		return nil
	}
	close(p.toServer)
	return nil
}

func (p *bidiPipe[T]) finishServer(err error) {
	p.serverErrMu.Lock()
	p.serverErr = err
	p.serverErrMu.Unlock()
	close(p.toClient)
	close(p.serverDone)
	p.cancel()
}

func (p *bidiPipe[T]) serverError() error {
	p.serverErrMu.Lock()
	defer p.serverErrMu.Unlock()
	return p.serverErr
}

func (p *bidiPipe[T]) close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	_ = p.clientCloseSend()
	p.cancel()
	// Drain until the server handler exits so we do not leak goroutines.
	<-p.serverDone
	return p.serverError()
}

type bidiServerStream[T any] struct {
	pipe *bidiPipe[T]
}

func (s *bidiServerStream[T]) Send(frame *T) error {
	if frame == nil {
		return fmt.Errorf("stream frame is nil")
	}
	cloned, err := cloneFrame(frame)
	if err != nil {
		return err
	}
	select {
	case <-s.pipe.ctx.Done():
		return s.pipe.ctx.Err()
	case s.pipe.toClient <- cloned:
		return nil
	}
}

func (s *bidiServerStream[T]) Recv() (*T, error) {
	frame, ok := <-s.pipe.toServer
	if !ok {
		return nil, io.EOF
	}
	return frame, nil
}

func (s *bidiServerStream[T]) SetHeader(metadata.MD) error  { return nil }
func (s *bidiServerStream[T]) SendHeader(metadata.MD) error { return nil }
func (s *bidiServerStream[T]) SetTrailer(metadata.MD)       {}
func (s *bidiServerStream[T]) Context() context.Context     { return s.pipe.ctx }
func (s *bidiServerStream[T]) SendMsg(m any) error {
	frame, ok := m.(*T)
	if !ok {
		return fmt.Errorf("unexpected stream send type %T", m)
	}
	return s.Send(frame)
}
func (s *bidiServerStream[T]) RecvMsg(m any) error {
	frame, err := s.Recv()
	if err != nil {
		return err
	}
	dst, ok := m.(*T)
	if !ok {
		return fmt.Errorf("unexpected stream recv type %T", m)
	}
	*dst = *frame
	return nil
}

// serverPipe is a server-streaming client/server pair (no client frames).
type serverPipe[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	toClient   chan *T
	serverDone chan struct{}
	closed     atomic.Bool

	serverErrMu sync.Mutex
	serverErr   error

	server *serverOnlyStream[T]
}

func newServerPipe[T any](parent context.Context) *serverPipe[T] {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	pipe := &serverPipe[T]{
		ctx:        ctx,
		cancel:     cancel,
		toClient:   make(chan *T, 16),
		serverDone: make(chan struct{}),
	}
	pipe.server = &serverOnlyStream[T]{pipe: pipe}
	return pipe
}

func (p *serverPipe[T]) clientRecv() (*T, error) {
	select {
	case <-p.ctx.Done():
		select {
		case frame := <-p.toClient:
			return frame, nil
		default:
		}
		if err := p.serverError(); err != nil {
			return nil, err
		}
		return nil, p.ctx.Err()
	case frame, ok := <-p.toClient:
		if !ok {
			if err := p.serverError(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		return frame, nil
	}
}

func (p *serverPipe[T]) finishServer(err error) {
	p.serverErrMu.Lock()
	p.serverErr = err
	p.serverErrMu.Unlock()
	close(p.toClient)
	close(p.serverDone)
	p.cancel()
}

func (p *serverPipe[T]) serverError() error {
	p.serverErrMu.Lock()
	defer p.serverErrMu.Unlock()
	return p.serverErr
}

func (p *serverPipe[T]) close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	p.cancel()
	<-p.serverDone
	return p.serverError()
}

type serverOnlyStream[T any] struct {
	pipe *serverPipe[T]
}

func (s *serverOnlyStream[T]) Send(frame *T) error {
	if frame == nil {
		return fmt.Errorf("stream frame is nil")
	}
	cloned, err := cloneFrame(frame)
	if err != nil {
		return err
	}
	select {
	case <-s.pipe.ctx.Done():
		return s.pipe.ctx.Err()
	case s.pipe.toClient <- cloned:
		return nil
	}
}

func (s *serverOnlyStream[T]) SetHeader(metadata.MD) error  { return nil }
func (s *serverOnlyStream[T]) SendHeader(metadata.MD) error { return nil }
func (s *serverOnlyStream[T]) SetTrailer(metadata.MD)       {}
func (s *serverOnlyStream[T]) Context() context.Context     { return s.pipe.ctx }
func (s *serverOnlyStream[T]) SendMsg(m any) error {
	frame, ok := m.(*T)
	if !ok {
		return fmt.Errorf("unexpected stream send type %T", m)
	}
	return s.Send(frame)
}
func (s *serverOnlyStream[T]) RecvMsg(any) error {
	return fmt.Errorf("server-only stream does not receive client messages")
}

func cloneFrame[T any](frame *T) (*T, error) {
	message, ok := any(frame).(proto.Message)
	if !ok {
		cloned := *frame
		return &cloned, nil
	}
	copied, ok := proto.Clone(message).(proto.Message)
	if !ok {
		return nil, fmt.Errorf("clone stream frame")
	}
	typed, ok := any(copied).(*T)
	if !ok {
		return nil, fmt.Errorf("clone stream frame type mismatch")
	}
	return typed, nil
}
