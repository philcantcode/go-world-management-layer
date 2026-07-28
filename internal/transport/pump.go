package transport

import (
	"context"
	"errors"
	"io"
	"sync"
)

// SendInput forwards opaque stdin in bounded chunks and emits exactly one
// close-input record. Encoder serializes it safely with simultaneous signals.
func SendInput(ctx context.Context, encoder *Encoder, input io.Reader, chunkSize int) error {
	if chunkSize <= 0 || chunkSize > int(DefaultMaxFrame)-8 {
		chunkSize = DefaultChunkSize
	}
	buffer := make([]byte, chunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := input.Read(buffer)
		if n > 0 {
			if _, writeErr := encoder.Write(KindStdin, buffer[:n]); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			_, closeErr := encoder.Write(KindCloseInput, nil)
			return closeErr
		}
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
	}
}

type BidirectionalResult struct {
	Terminal  Terminal
	InputErr  error
	OutputErr error
}

// Pump executes input and output concurrently so a full stderr pipe cannot
// deadlock stdin/stdout. Context cancellation is observed by both directions;
// the caller remains responsible for signal/kill escalation at the driver edge.
func Pump(ctx context.Context, encoder *Encoder, decoder *Decoder, input io.Reader, output Output) BidirectionalResult {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var result BidirectionalResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); result.InputErr = SendInput(child, encoder, input, DefaultChunkSize) }()
	go func() {
		defer wg.Done()
		result.Terminal, result.OutputErr = ReceiveOutput(child, decoder, output)
		cancel()
	}()
	wg.Wait()
	if errors.Is(result.InputErr, context.Canceled) && result.OutputErr == nil {
		result.InputErr = nil
	}
	return result
}
