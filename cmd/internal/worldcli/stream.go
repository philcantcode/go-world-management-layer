package worldcli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/protobuf/proto"
)

// ExecOutput routes byte channels without contaminating stdout with control
// metadata and remembers the terminal outcome.
type ExecOutput struct {
	Outcome *worldv1.ExecOutcome
}

func (output *ExecOutput) Handle(stdoutWriter, stderrWriter io.Writer, stdout, stderr []byte, outcome *worldv1.ExecOutcome) error {
	if output.Outcome != nil {
		return fmt.Errorf("exec stream returned a frame after its terminal outcome")
	}
	if len(stdout) > 0 {
		if _, err := stdoutWriter.Write(stdout); err != nil {
			return err
		}
	}
	if len(stderr) > 0 {
		if _, err := stderrWriter.Write(stderr); err != nil {
			return err
		}
	}
	if outcome != nil {
		output.Outcome = proto.Clone(outcome).(*worldv1.ExecOutcome)
	}
	return nil
}

// Finish optionally emits the outcome on stderr and preserves non-zero remote
// exit status for the command process.
func (output *ExecOutput) Finish(stderr io.Writer, emitJSON bool) error {
	if output.Outcome == nil {
		return fmt.Errorf("exec stream closed without a terminal outcome")
	}
	if emitJSON && output.Outcome != nil {
		if err := Encoder(stderr).Encode(output.Outcome); err != nil {
			return err
		}
	}
	if outcome := output.Outcome; outcome != nil && (outcome.ExitCode != 0 || !successfulTermination(outcome.Termination) || outcome.Error != "") {
		return &ProcessExitError{Code: outcome.ExitCode, Termination: outcome.Termination, Detail: outcome.Error}
	}
	return nil
}

func successfulTermination(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "completed":
		return true
	default:
		return false
	}
}

// OpenInput resolves - to the supplied stdin and otherwise opens a named file.
func OpenInput(path string, stdin io.Reader) (io.Reader, func() error, error) {
	if path == "-" {
		return stdin, func() error { return nil }, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open input file: %w", err)
	}
	return file, file.Close, nil
}

// BidiStream is the common subset of generated bidirectional gRPC streams.
type BidiStream[Frame any] interface {
	Send(*Frame) error
	Recv() (*Frame, error)
	CloseSend() error
}

// PumpBidi sends a start frame, copies bounded chunks from input on the sole
// sending goroutine, and delivers server frames to handle. The stream context
// remains the caller's bound on a blocked or slow peer.
func PumpBidi[Frame any](stream BidiStream[Frame], start *Frame, input io.Reader, inputFrame func([]byte) *Frame, endFrame func() *Frame, handle func(*Frame) error) error {
	if start == nil {
		return fmt.Errorf("bidirectional stream start frame is nil")
	}
	if input == nil || inputFrame == nil || handle == nil {
		return fmt.Errorf("bidirectional stream input, frame builder, and handler are required")
	}
	if err := stream.Send(start); err != nil {
		return err
	}
	sendResult := make(chan error, 1)
	halfClosing := make(chan struct{}, 1)
	go func() {
		buffer := make([]byte, 32<<10)
		for {
			read, readErr := input.Read(buffer)
			if read > 0 {
				payload := append([]byte(nil), buffer[:read]...)
				frame := inputFrame(payload)
				if frame == nil {
					sendResult <- fmt.Errorf("bidirectional stream input frame builder returned nil")
					return
				}
				if err := stream.Send(frame); err != nil {
					sendResult <- err
					return
				}
			}
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					sendResult <- readErr
					return
				}
				if endFrame != nil {
					frame := endFrame()
					if frame == nil {
						sendResult <- fmt.Errorf("bidirectional stream end frame builder returned nil")
						return
					}
					if err := stream.Send(frame); err != nil {
						sendResult <- err
						return
					}
				}
				halfClosing <- struct{}{}
				sendResult <- stream.CloseSend()
				return
			}
		}
	}()

	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			select {
			case sendErr := <-sendResult:
				return sendErr
			default:
			}
			select {
			case <-halfClosing:
				return <-sendResult
			default:
				return fmt.Errorf("bidirectional stream closed before client input was half-closed")
			}
		}
		if err != nil {
			return err
		}
		if frame == nil {
			return fmt.Errorf("bidirectional stream returned a nil frame")
		}
		if err := handle(frame); err != nil {
			return err
		}
	}
}

// ProcessExitError preserves a remote process exit status for command mains.
type ProcessExitError struct {
	Code        int32
	Termination string
	Detail      string
}

func (err *ProcessExitError) Error() string {
	if err.Detail != "" {
		return err.Detail
	}
	if err.Termination != "" {
		return fmt.Sprintf("process terminated by %s", err.Termination)
	}
	return fmt.Sprintf("process exited with code %d", err.Code)
}

func (err *ProcessExitError) ExitCode() int {
	if err.Code > 0 && err.Code <= 255 {
		return int(err.Code)
	}
	return 1
}
