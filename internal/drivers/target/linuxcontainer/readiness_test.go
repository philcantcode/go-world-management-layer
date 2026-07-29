package linuxcontainer

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type targetReadinessTransport struct {
	mu       sync.Mutex
	frames   []transport.Frame
	closeErr error
	closed   bool
}

func successfulTargetReadinessTransport() *targetReadinessTransport {
	started := transport.ProcessEvent{Kind: "started", PID: 7, ProcessStartNS: 11, ParentPID: 1}
	exited := transport.ProcessEvent{Kind: "exited", PID: 7, ProcessStartNS: 11, ParentPID: 1}
	terminal := transport.Terminal{ExitCode: 0, CleanupConfirmed: true}
	return &targetReadinessTransport{frames: []transport.Frame{
		targetJSONFrame(1, transport.KindProcess, started),
		targetJSONFrame(2, transport.KindProcess, exited),
		targetJSONFrame(3, transport.KindTerminal, terminal),
	}}
}

func targetJSONFrame(sequence uint64, kind transport.Kind, value any) transport.Frame {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return transport.Frame{Sequence: sequence, Kind: kind, Data: payload}
}

func (*targetReadinessTransport) Send(context.Context, transport.Kind, []byte) (transport.Frame, error) {
	return transport.Frame{}, io.ErrClosedPipe
}

func (t *targetReadinessTransport) Receive(ctx context.Context) (transport.Frame, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return transport.Frame{}, err
	}
	if t.closed || len(t.frames) == 0 {
		return transport.Frame{}, io.EOF
	}
	frame := t.frames[0]
	t.frames = t.frames[1:]
	return frame, nil
}

func (t *targetReadinessTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.closeErr
}
