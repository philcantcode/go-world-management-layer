// Package agentrunner adapts a lease-bound world agent workspace to a generic
// byte-transparent command execution environment.
package agentrunner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

const EnvironmentProtocolVersion uint16 = 1

var ErrExecutionInProgress = errors.New("world execution is already in progress")

type Options struct {
	Core                      *application.Core
	Driver                    ports.AgentWorkspaceDriver
	LeaseID                   domain.LeaseID
	AgentWorkspaceID          domain.AgentWorkspaceID
	AgentGeneration           domain.AgentGeneration
	CapabilityDigest          domain.Digest
	AuthorizedPolicyReference string
	ControlTimeout            time.Duration
	HeartbeatInterval         time.Duration
	ProtocolVersion           uint16
}

type Environment struct {
	core                      *application.Core
	driver                    ports.AgentWorkspaceDriver
	leaseID                   domain.LeaseID
	agentWorkspaceID          domain.AgentWorkspaceID
	agentGeneration           domain.AgentGeneration
	capabilityDigest          domain.Digest
	controlTimeout            time.Duration
	heartbeatInterval         time.Duration
	protocolVersion           uint16
	authorizedPolicyReference string
}

func New(options Options) (*Environment, error) {
	if options.Core == nil || options.Driver == nil || options.LeaseID.IsZero() || options.AgentWorkspaceID.IsZero() || !options.AgentGeneration.IsValid() || options.CapabilityDigest.IsZero() || options.AuthorizedPolicyReference == "" {
		return nil, fmt.Errorf("core, driver, lease, agent generation, and capability digest are required")
	}
	if options.ControlTimeout <= 0 {
		options.ControlTimeout = 10 * time.Second
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = 10 * time.Second
	}
	if options.ProtocolVersion == 0 {
		options.ProtocolVersion = EnvironmentProtocolVersion
	}
	return &Environment{core: options.Core, driver: options.Driver, leaseID: options.LeaseID, agentWorkspaceID: options.AgentWorkspaceID, agentGeneration: options.AgentGeneration, capabilityDigest: options.CapabilityDigest, authorizedPolicyReference: options.AuthorizedPolicyReference, controlTimeout: options.ControlTimeout, heartbeatInterval: options.HeartbeatInterval, protocolVersion: options.ProtocolVersion}, nil
}

func (e *Environment) ID() string {
	return fmt.Sprintf("world:%s:%s:%d:%s:exec-v%d", e.leaseID, e.agentWorkspaceID, e.agentGeneration, e.capabilityDigest, e.protocolVersion)
}

type Request struct {
	IdempotencyKey string
	CorrelationID  string
	Kind           domain.ExecKind
	Executable     string
	// Argv contains only the arguments after argv[0]. Executable supplies
	// both the program to launch and argv[0].
	Argv             []string
	WorkingDirectory string
	Environment      map[string]string
	TemporaryInputs  []transport.TemporaryInput
	Stdin            []byte
	Terminal         bool
	MaxOutputBytes   int64
	CleanupGrace     time.Duration
	OnStdout         func([]byte) error
	OnStderr         func([]byte) error
	OnControl        func(transport.Frame) error
}

type Result struct {
	ExecID           string
	ExitCode         int
	Signal           string
	IncidentID       string
	CleanupConfirmed bool
}

type ExecutionError struct {
	ExecID           string
	IncidentID       string
	CleanupConfirmed bool
	Cause            error
}

func (e *ExecutionError) Error() string {
	message := "world execution failed"
	if e.ExecID != "" {
		message += " (" + e.ExecID + ")"
	}
	if e.IncidentID != "" {
		message += " incident=" + e.IncidentID
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *ExecutionError) Unwrap() error { return e.Cause }

func (e *Environment) Execute(ctx context.Context, request Request) (Result, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return Result{}, fmt.Errorf("execution context deadline is required")
	}
	if !domain.IsCanonicalIdempotencyKey(request.IdempotencyKey) {
		return Result{}, fmt.Errorf("idempotency key must be canonical and at most 1024 bytes")
	}
	if _, err := domain.ParseCorrelationID(request.CorrelationID); err != nil {
		return Result{}, fmt.Errorf("correlation id: %w", err)
	}
	if request.MaxOutputBytes <= 0 {
		request.MaxOutputBytes = 64 << 20
	}
	if request.CleanupGrace <= 0 {
		request.CleanupGrace = 5 * time.Second
	}
	created, err := e.core.CreateExec(ctx, application.CreateExecRequest{Meta: e.meta(domain.DeriveIdempotencyKey(request.IdempotencyKey, "create"), request.CorrelationID, deadline), LeaseID: e.leaseID.String(), Kind: request.Kind, Executable: request.Executable, Argv: request.Argv, WorkingDirectory: request.WorkingDirectory})
	if err != nil {
		return Result{}, err
	}
	if created.State.Terminal() {
		return resultFromRecord(created)
	}
	if created.State != domain.ExecRequested {
		return Result{}, &ExecutionError{ExecID: created.ID, CleanupConfirmed: created.CleanupConfirmed, Cause: ErrExecutionInProgress}
	}
	starting, err := e.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: e.meta(domain.DeriveIdempotencyKey(request.IdempotencyKey, "starting"), request.CorrelationID, deadline), ExecID: created.ID, ExpectedRevision: created.Revision, State: domain.ExecStarting})
	if err != nil {
		return Result{}, err
	}
	execModel, err := domainExec(starting)
	if err != nil {
		return Result{}, err
	}
	start := transport.ExecStart{ExecID: starting.ID, IdempotencyKey: request.IdempotencyKey, Executable: starting.Executable, Argv: append([]string(nil), starting.Argv...), WorkingDirectory: starting.WorkingDirectory, Environment: cloneStringMap(request.Environment), TemporaryInputs: cloneTemporaryInputs(request.TemporaryInputs), Terminal: request.Terminal, Deadline: deadline, MaxOutputBytes: request.MaxOutputBytes, CleanupGrace: request.CleanupGrace}
	stream, err := e.driver.OpenExec(ctx, ports.ExecPlan{LeaseID: e.leaseID, AgentWorkspaceID: e.agentWorkspaceID, AgentGeneration: e.agentGeneration, Exec: execModel, Start: start})
	if err != nil {
		return Result{}, e.finalizeFailure(ctx, request, starting, domain.ExecFailed, false, err)
	}
	defer stream.Close()
	running, err := e.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: e.meta(domain.DeriveIdempotencyKey(request.IdempotencyKey, "running"), request.CorrelationID, deadline), ExecID: starting.ID, ExpectedRevision: starting.Revision, State: domain.ExecRunning})
	if err != nil {
		return Result{}, e.finalizeFailure(ctx, request, starting, domain.ExecFailed, false, err)
	}
	terminal, exchangeErr := exchange(ctx, stream, request, e.heartbeatInterval)
	closeErr := stream.Close()
	if exchangeErr != nil {
		state := domain.ExecLost
		if errors.Is(exchangeErr, context.Canceled) || errors.Is(exchangeErr, context.DeadlineExceeded) {
			state = domain.ExecCancelled
		}
		if closeErr != nil {
			exchangeErr = errors.Join(exchangeErr, closeErr)
		}
		return Result{}, e.finalizeFailure(ctx, request, running, state, false, exchangeErr)
	}
	if closeErr != nil {
		terminal.CleanupConfirmed = false
		if terminal.Error == "" {
			terminal.Error = closeErr.Error()
		}
	}
	state := domain.ExecCompleted
	if terminal.ExitCode != 0 || terminal.Signal != "" || terminal.Error != "" {
		state = domain.ExecFailed
	}
	exitCode := terminal.ExitCode
	incidentIDs := make([]string, 0, 1)
	if terminal.IncidentID != "" {
		incidentIDs = append(incidentIDs, terminal.IncidentID)
	}
	finalized, err := e.finalize(ctx, request, running, application.FinalizeExecRequest{State: state, ExitCode: &exitCode, Signal: terminal.Signal, IncidentIDs: incidentIDs, CleanupConfirmed: terminal.CleanupConfirmed, Error: terminal.Error})
	if err != nil {
		return Result{}, err
	}
	result, resultErr := resultFromRecord(finalized)
	if resultErr != nil {
		return Result{}, resultErr
	}
	if state != domain.ExecCompleted {
		return result, &ExecutionError{ExecID: finalized.ID, IncidentID: result.IncidentID, CleanupConfirmed: finalized.CleanupConfirmed, Cause: errors.New(terminal.Error)}
	}
	return result, nil
}

func exchange(ctx context.Context, stream ports.ExecTransport, request Request, heartbeatInterval time.Duration) (transport.Terminal, error) {
	inputContext, cancelInput := context.WithCancel(ctx)
	defer cancelInput()
	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	heartbeatDone := make(chan error, 1)
	go sendHeartbeats(heartbeatContext, stream, heartbeatInterval, heartbeatDone)
	inputDone := make(chan error, 1)
	stdin := append([]byte(nil), request.Stdin...)
	go func() {
		for len(stdin) > 0 {
			chunkSize := transport.DefaultChunkSize
			if len(stdin) < chunkSize {
				chunkSize = len(stdin)
			}
			if _, err := stream.Send(inputContext, transport.KindStdin, stdin[:chunkSize]); err != nil {
				inputDone <- err
				return
			}
			stdin = stdin[chunkSize:]
		}
		_, err := stream.Send(inputContext, transport.KindCloseInput, nil)
		inputDone <- err
	}()
	var outputBytes int64
	for {
		frame, err := stream.Receive(ctx)
		if err != nil {
			cancelInput()
			cancelHeartbeat()
			select {
			case heartbeatErr := <-heartbeatDone:
				if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
					return transport.Terminal{}, heartbeatErr
				}
			default:
			}
			return transport.Terminal{}, err
		}
		frame.Data = append([]byte(nil), frame.Data...)
		switch frame.Kind {
		case transport.KindStdout, transport.KindStderr:
			if int64(len(frame.Data)) > request.MaxOutputBytes-outputBytes {
				cancelInput()
				return transport.Terminal{}, transport.ErrOutputLimit
			}
			outputBytes += int64(len(frame.Data))
			callback := request.OnStdout
			if frame.Kind == transport.KindStderr {
				callback = request.OnStderr
			}
			if callback != nil {
				if err := callback(frame.Data); err != nil {
					cancelInput()
					return transport.Terminal{}, err
				}
			}
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			cancelInput()
			cancelHeartbeat()
			inputErr := <-inputDone
			heartbeatErr := <-heartbeatDone
			if inputErr != nil && !errors.Is(inputErr, context.Canceled) {
				return transport.Terminal{}, inputErr
			}
			if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
				return transport.Terminal{}, heartbeatErr
			}
			if err != nil {
				return transport.Terminal{}, err
			}
			if request.OnControl != nil {
				if err := request.OnControl(frame); err != nil {
					return transport.Terminal{}, err
				}
			}
			return terminal, nil
		default:
			if request.OnControl != nil {
				if err := request.OnControl(frame); err != nil {
					cancelInput()
					return transport.Terminal{}, err
				}
			}
			if frame.Kind == transport.KindError {
				cancelInput()
				return transport.Terminal{}, fmt.Errorf("guest protocol: %s", frame.Data)
			}
		}
	}
}

func sendHeartbeats(ctx context.Context, stream ports.ExecTransport, interval time.Duration, done chan<- error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
			if _, err := stream.Send(ctx, transport.KindHeartbeat, nil); err != nil {
				done <- err
				_ = stream.Close()
				return
			}
		}
	}
}

func (e *Environment) finalizeFailure(ctx context.Context, request Request, execution application.ExecRecord, state domain.ExecState, cleanupConfirmed bool, cause error) error {
	finalized, err := e.finalize(ctx, request, execution, application.FinalizeExecRequest{State: state, CleanupConfirmed: cleanupConfirmed, Error: cause.Error()})
	if err != nil {
		return errors.Join(cause, err)
	}
	result, _ := resultFromRecord(finalized)
	return &ExecutionError{ExecID: execution.ID, IncidentID: result.IncidentID, CleanupConfirmed: cleanupConfirmed, Cause: cause}
}

func (e *Environment) finalize(ctx context.Context, request Request, execution application.ExecRecord, terminal application.FinalizeExecRequest) (application.ExecRecord, error) {
	controlContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.controlTimeout)
	defer cancel()
	deadline, _ := controlContext.Deadline()
	terminal.Meta = e.meta(domain.DeriveIdempotencyKey(request.IdempotencyKey, "finalize"), request.CorrelationID, deadline)
	terminal.ExecID = execution.ID
	terminal.ExpectedRevision = execution.Revision
	return e.core.FinalizeExec(controlContext, terminal)
}

func (e *Environment) meta(key, correlationID string, deadline time.Time) application.MutationMeta {
	return application.MutationMeta{IdempotencyKey: key, CorrelationID: correlationID, AuthorizedPolicyReference: e.authorizedPolicyReference, Deadline: deadline}
}

func domainExec(record application.ExecRecord) (domain.Exec, error) {
	execID, err := domain.ParseExecID(record.ID)
	if err != nil {
		return domain.Exec{}, err
	}
	leaseID, err := domain.ParseLeaseID(record.LeaseID)
	if err != nil {
		return domain.Exec{}, err
	}
	agentID, err := domain.ParseAgentWorkspaceID(record.AgentWorkspaceID)
	if err != nil {
		return domain.Exec{}, err
	}
	return domain.NewExec(domain.ExecSpec{ID: execID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(record.AgentGeneration), Kind: record.Kind, Executable: record.Executable, Argv: record.Argv, WorkingDirectory: record.WorkingDirectory, CreatedAt: record.CreatedAt})
}

func resultFromRecord(record application.ExecRecord) (Result, error) {
	result := Result{ExecID: record.ID, Signal: record.Signal, CleanupConfirmed: record.CleanupConfirmed}
	if record.ExitCode != nil {
		result.ExitCode = *record.ExitCode
	}
	if len(record.IncidentIDs) > 0 {
		result.IncidentID = record.IncidentIDs[0]
	}
	if record.State == domain.ExecCompleted {
		return result, nil
	}
	if record.State.Terminal() {
		return result, &ExecutionError{ExecID: record.ID, IncidentID: result.IncidentID, CleanupConfirmed: record.CleanupConfirmed, Cause: errors.New(record.Error)}
	}
	return result, ErrExecutionInProgress
}

func cloneStringMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneTemporaryInputs(values []transport.TemporaryInput) []transport.TemporaryInput {
	result := make([]transport.TemporaryInput, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Bytes = append([]byte(nil), value.Bytes...)
	}
	return result
}
