package guest

import (
	"context"
	"fmt"
	"io"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

// Serve runs one framed exec session. Provider bytes are never decoded.
func (supervisor *Supervisor) Serve(ctx context.Context, reader io.Reader, writer io.Writer, maxFrame uint32) error {
	decoder := transport.NewDecoder(reader, maxFrame)
	emitter := transport.NewEncoder(writer, maxFrame)
	frame, err := decoder.Read()
	if err != nil {
		return err
	}
	if frame.Kind != transport.KindStart {
		terminal := transport.Terminal{ExitCode: -1, CleanupConfirmed: true, Error: fmt.Sprintf("%v: first frame must be start", transport.ErrProtocol)}
		_, writeErr := emitter.WriteJSON(transport.KindTerminal, terminal)
		if writeErr != nil {
			return writeErr
		}
		return transport.ErrProtocol
	}
	start, err := transport.DecodeJSON[transport.ExecStart](frame)
	if err != nil {
		terminal := transport.Terminal{ExitCode: -1, CleanupConfirmed: true, Error: err.Error()}
		_, _ = emitter.WriteJSON(transport.KindTerminal, terminal)
		return err
	}
	controls := make(chan transport.Frame, 16)
	go func() {
		defer close(controls)
		for {
			control, readErr := decoder.Read()
			if readErr != nil {
				return
			}
			select {
			case controls <- control:
			case <-ctx.Done():
				return
			}
		}
	}()
	_, err = supervisor.Run(ctx, start, controls, emitter)
	return err
}
