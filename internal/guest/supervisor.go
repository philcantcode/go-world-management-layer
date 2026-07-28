package guest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

// Run executes one already-decoded start and emits exactly one terminal frame.
func (supervisor *Supervisor) Run(ctx context.Context, start transport.ExecStart, controls <-chan transport.Frame, emitter runEmitter) (terminal transport.Terminal, returnErr error) {
	terminal.ExitCode = -1
	defer func() {
		_, emitErr := emitter.WriteJSON(transport.KindTerminal, terminal)
		if returnErr == nil && emitErr != nil {
			returnErr = emitErr
		}
	}()

	if err := supervisor.validateStart(start); err != nil {
		terminal.CleanupConfirmed = true
		terminal.Error = err.Error()
		return terminal, err
	}
	if err := ctx.Err(); err != nil {
		terminal.CleanupConfirmed = true
		terminal.Error = err.Error()
		return terminal, err
	}
	inputs, err := materializeTemporaryInputs(supervisor.config.TemporaryRoot, start, supervisor.config.MaxTemporaryBytes)
	if err != nil {
		terminal.CleanupConfirmed = true
		terminal.Error = err.Error()
		return terminal, err
	}
	process, err := supervisor.config.Launcher.Launch(ProcessSpec{
		Executable:       start.Executable,
		Argv:             inputs.argv,
		WorkingDirectory: start.WorkingDirectory,
		Environment:      cloneEnvironment(start.Environment),
	})
	if err != nil {
		cleanupErr := inputs.cleanup()
		terminal.CleanupConfirmed = cleanupErr == nil
		terminal.Error = err.Error()
		if cleanupErr != nil {
			terminal.Error += "; " + cleanupErr.Error()
		}
		return terminal, err
	}
	defer process.Close()

	identity := process.Identity()
	if _, err = emitter.WriteJSON(transport.KindProcess, transport.ProcessEvent{Kind: "started", PID: identity.PID, ParentPID: identity.ParentPID, ProcessStartNS: identity.ProcessStartNS}); err != nil {
		return supervisor.finishAfterFailure(start, inputs, process, nil, err, terminal)
	}

	runCtx, cancel := context.WithDeadline(ctx, start.Deadline)
	defer cancel()
	waited := make(chan ProcessResult, 1)
	go func() { waited <- process.Wait() }()
	outputErrors := make(chan error, 2)
	outputDone := make(chan struct{})
	budget := &outputBudget{remaining: start.MaxOutputBytes}
	go pumpBothOutputs(process, emitter, supervisor.config.IOChunkSize, budget, outputErrors, outputDone)

	heartbeat := time.NewTimer(supervisor.config.HeartbeatTimeout)
	defer heartbeat.Stop()
	stdin := process.Stdin()
	stdinClosed := false
	var stdinBytes int64
	var result ProcessResult
	var stopCause error
	waitObserved := false

runLoop:
	for {
		select {
		case result = <-waited:
			waitObserved = true
			break runLoop
		case outputErr := <-outputErrors:
			if outputErr != nil {
				stopCause = outputErr
				break runLoop
			}
		case <-runCtx.Done():
			stopCause = runCtx.Err()
			break runLoop
		case <-heartbeat.C:
			stopCause = ErrHeartbeatExpired
			break runLoop
		case frame, ok := <-controls:
			if !ok {
				stopCause = io.ErrUnexpectedEOF
				break runLoop
			}
			controlErr := handleControl(frame, process, stdin, &stdinClosed, &stdinBytes, supervisor.config.MaxStdinBytes, heartbeat, supervisor.config.HeartbeatTimeout)
			if controlErr != nil {
				stopCause = controlErr
				break runLoop
			}
		}
	}

	if !stdinClosed {
		_ = stdin.Close()
		stdinClosed = true
	}
	if stopCause != nil && !waitObserved {
		result, waitObserved = stopProcess(process, waited, start.CleanupGrace)
	}
	outputDrained := waitForOutput(outputDone, start.CleanupGrace)
	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), start.CleanupGrace)
	processClean, confirmErr := process.ConfirmCleanup(cleanupContext)
	cleanupCancel()
	temporaryErr := inputs.cleanup()
	cleanupConfirmed := waitObserved && outputDrained && processClean && confirmErr == nil && temporaryErr == nil

	_, processEventErr := emitter.WriteJSON(transport.KindProcess, transport.ProcessEvent{Kind: "exited", PID: identity.PID, ParentPID: identity.ParentPID, ProcessStartNS: identity.ProcessStartNS})
	terminal.ExitCode = result.ExitCode
	terminal.Signal = result.Signal
	terminal.CleanupConfirmed = cleanupConfirmed
	terminal.Error = joinErrors(stopCause, result.Err, confirmErr, temporaryErr, processEventErr)
	if !cleanupConfirmed {
		terminal.Error = joinMessage("cleanup-unconfirmed", terminal.Error)
		return terminal, transport.ErrCleanupUnknown
	}
	if stopCause != nil {
		return terminal, stopCause
	}
	if result.Err != nil {
		return terminal, result.Err
	}
	if processEventErr != nil {
		return terminal, processEventErr
	}
	return terminal, nil
}

func (supervisor *Supervisor) validateStart(start transport.ExecStart) error {
	if err := start.Validate(supervisor.config.MaxTemporaryBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidStart, err)
	}
	if strings.TrimSpace(start.WorkingDirectory) == "" || strings.IndexByte(start.WorkingDirectory, 0) >= 0 {
		return fmt.Errorf("%w: explicit working directory is required", ErrInvalidStart)
	}
	if start.Terminal {
		return ErrUnsupportedTerminal
	}
	if !start.Deadline.After(supervisor.config.Now()) {
		return fmt.Errorf("%w: deadline has expired", ErrInvalidStart)
	}
	return nil
}

func (supervisor *Supervisor) finishAfterFailure(start transport.ExecStart, inputs materializedInputs, process Process, waited <-chan ProcessResult, cause error, terminal transport.Terminal) (transport.Terminal, error) {
	if waited == nil {
		waitChannel := make(chan ProcessResult, 1)
		go func() { waitChannel <- process.Wait() }()
		waited = waitChannel
	}
	result, waitObserved := stopProcess(process, waited, start.CleanupGrace)
	cleanupContext, cancel := context.WithTimeout(context.Background(), start.CleanupGrace)
	processClean, confirmErr := process.ConfirmCleanup(cleanupContext)
	cancel()
	temporaryErr := inputs.cleanup()
	terminal.ExitCode = result.ExitCode
	terminal.Signal = result.Signal
	terminal.CleanupConfirmed = waitObserved && processClean && confirmErr == nil && temporaryErr == nil
	terminal.Error = joinErrors(cause, result.Err, confirmErr, temporaryErr)
	if !terminal.CleanupConfirmed {
		terminal.Error = joinMessage("cleanup-unconfirmed", terminal.Error)
		return terminal, transport.ErrCleanupUnknown
	}
	return terminal, cause
}

func handleControl(frame transport.Frame, process Process, stdin io.WriteCloser, stdinClosed *bool, stdinBytes *int64, maxStdin int64, heartbeat *time.Timer, heartbeatTimeout time.Duration) error {
	switch frame.Kind {
	case transport.KindHeartbeat:
		resetTimer(heartbeat, heartbeatTimeout)
		return nil
	case transport.KindStdin:
		if *stdinClosed {
			return fmt.Errorf("%w: stdin after close", transport.ErrProtocol)
		}
		if int64(len(frame.Data)) > maxStdin-*stdinBytes {
			return ErrInputLimit
		}
		written, err := writeAll(stdin, frame.Data)
		*stdinBytes += written
		return err
	case transport.KindCloseInput:
		if *stdinClosed {
			return fmt.Errorf("%w: duplicate close-input", transport.ErrProtocol)
		}
		*stdinClosed = true
		return stdin.Close()
	case transport.KindSignal:
		signal, err := transport.DecodeJSON[transport.Signal](frame)
		if err != nil {
			return err
		}
		return process.Signal(signal.Name)
	case transport.KindResize:
		return ErrUnsupportedTerminal
	default:
		return fmt.Errorf("%w: unexpected guest control kind %d", transport.ErrProtocol, frame.Kind)
	}
}

type outputBudget struct {
	mu        sync.Mutex
	remaining int64
}

func (budget *outputBudget) reserve(size int) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if int64(size) > budget.remaining {
		return transport.ErrOutputLimit
	}
	budget.remaining -= int64(size)
	return nil
}

func pumpBothOutputs(process Process, emitter runEmitter, chunkSize int, budget *outputBudget, failures chan<- error, done chan<- struct{}) {
	var group sync.WaitGroup
	group.Add(2)
	pump := func(reader io.Reader, kind transport.Kind) {
		defer group.Done()
		buffer := make([]byte, chunkSize)
		for {
			count, err := reader.Read(buffer)
			if count > 0 {
				if budgetErr := budget.reserve(count); budgetErr != nil {
					select {
					case failures <- budgetErr:
					default:
					}
					return
				}
				if _, writeErr := emitter.Write(kind, buffer[:count]); writeErr != nil {
					select {
					case failures <- writeErr:
					default:
					}
					return
				}
			}
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				select {
				case failures <- err:
				default:
				}
				return
			}
		}
	}
	go pump(process.Stdout(), transport.KindStdout)
	go pump(process.Stderr(), transport.KindStderr)
	group.Wait()
	close(done)
}

func stopProcess(process Process, waited <-chan ProcessResult, grace time.Duration) (ProcessResult, bool) {
	_ = process.Terminate()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case result := <-waited:
		return result, true
	case <-timer.C:
	}
	_ = process.Kill()
	timer.Reset(grace)
	select {
	case result := <-waited:
		return result, true
	case <-timer.C:
		return ProcessResult{ExitCode: -1, Err: transport.ErrCleanupUnknown}, false
	}
}

func waitForOutput(done <-chan struct{}, grace time.Duration) bool {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func cloneEnvironment(environment map[string]string) map[string]string {
	cloned := make(map[string]string, len(environment))
	for name, value := range environment {
		cloned[name] = value
	}
	return cloned
}

func joinErrors(errs ...error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "; ")
}

func joinMessage(prefix, detail string) string {
	if detail == "" {
		return prefix
	}
	return prefix + ": " + detail
}
