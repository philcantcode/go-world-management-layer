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
	"github.com/philcantcode/go-world-management-layer/internal/research"
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
	// ActionEvidence optionally records multi-dimensional action bundles for
	// each Execute call. Nil disables capture (no-op).
	ActionEvidence *research.Store
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
	actionEvidence            *research.Store
}

func New(options Options) (*Environment, error) {
	if options.Core == nil || options.Driver == nil || options.LeaseID.IsZero() || options.AgentWorkspaceID.IsZero() || !options.AgentGeneration.IsValid() || options.CapabilityDigest.IsZero() || options.AuthorizedPolicyReference == "" {
		return nil, fmt.Errorf("core, driver, lease, agent generation, and capability digest are required")
	}
	if options.ControlTimeout <= 0 {
		options.ControlTimeout = 10 * time.Second
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = ports.DefaultExecHeartbeatInterval
	}
	if options.ProtocolVersion == 0 {
		options.ProtocolVersion = EnvironmentProtocolVersion
	}
	return &Environment{core: options.Core, driver: options.Driver, leaseID: options.LeaseID, agentWorkspaceID: options.AgentWorkspaceID, agentGeneration: options.AgentGeneration, capabilityDigest: options.CapabilityDigest, authorizedPolicyReference: options.AuthorizedPolicyReference, controlTimeout: options.ControlTimeout, heartbeatInterval: options.HeartbeatInterval, protocolVersion: options.ProtocolVersion, actionEvidence: options.ActionEvidence}, nil
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
	// Begin failures are fail-open: marker recorded inside beginAction.
	actionSession, _ := e.beginAction(ctx, starting, request)
	execModel, err := domainExec(starting)
	if err != nil {
		return Result{}, e.finalizeWithActionEvidence(ctx, request, starting, domain.ExecFailed, err, actionSession, transport.ProcessLifecycle{})
	}
	start := transport.ExecStart{ExecID: starting.ID, IdempotencyKey: request.IdempotencyKey, Executable: starting.Executable, Argv: append([]string(nil), starting.Argv...), WorkingDirectory: starting.WorkingDirectory, Environment: cloneStringMap(request.Environment), TemporaryInputs: cloneTemporaryInputs(request.TemporaryInputs), Terminal: request.Terminal, Deadline: deadline, MaxOutputBytes: request.MaxOutputBytes, CleanupGrace: request.CleanupGrace}
	stream, err := e.driver.OpenExec(ctx, ports.ExecPlan{LeaseID: e.leaseID, AgentWorkspaceID: e.agentWorkspaceID, AgentGeneration: e.agentGeneration, Exec: execModel, Start: start})
	if err != nil {
		return Result{}, e.finalizeWithActionEvidence(ctx, request, starting, domain.ExecFailed, err, actionSession, transport.ProcessLifecycle{})
	}
	defer stream.Close()
	running, err := e.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: e.meta(domain.DeriveIdempotencyKey(request.IdempotencyKey, "running"), request.CorrelationID, deadline), ExecID: starting.ID, ExpectedRevision: starting.Revision, State: domain.ExecRunning})
	if err != nil {
		return Result{}, e.finalizeWithActionEvidence(ctx, request, starting, domain.ExecFailed, errors.Join(err, stream.Close()), actionSession, transport.ProcessLifecycle{})
	}
	if actionSession != nil {
		originalStdout, originalStderr := request.OnStdout, request.OnStderr
		request.OnStdout = func(data []byte) error {
			actionSession.AppendStdout(data)
			if originalStdout != nil {
				return originalStdout(data)
			}
			return nil
		}
		request.OnStderr = func(data []byte) error {
			actionSession.AppendStderr(data)
			if originalStderr != nil {
				return originalStderr(data)
			}
			return nil
		}
	}
	terminal, lifecycle, exchangeErr := exchange(ctx, stream, request, e.heartbeatInterval)
	closeErr := stream.Close()
	if exchangeErr != nil {
		state := domain.ExecLost
		if errors.Is(exchangeErr, context.Canceled) || errors.Is(exchangeErr, context.DeadlineExceeded) {
			state = domain.ExecCancelled
		}
		return Result{}, e.finalizeWithActionEvidence(ctx, request, running, state, errors.Join(exchangeErr, closeErr), actionSession, lifecycle)
	}
	if closeErr != nil {
		terminal.CleanupConfirmed = false
		if terminal.Error == "" {
			terminal.Error = closeErr.Error()
		}
	}
	// Fail-open at API boundary: seal errors never fail a successful Execute.
	_ = e.sealAction(ctx, actionSession, terminal, lifecycle)
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
		return result, resultErr
	}
	if state != domain.ExecCompleted {
		return result, &ExecutionError{ExecID: finalized.ID, IncidentID: result.IncidentID, CleanupConfirmed: finalized.CleanupConfirmed, Cause: errors.New(terminal.Error)}
	}
	return result, nil
}

func (e *Environment) beginAction(ctx context.Context, record application.ExecRecord, request Request) (*research.Session, error) {
	if e.actionEvidence == nil {
		return nil, nil
	}
	startedAt := record.CreatedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	start := research.StartFromCommand(
		record.ID, research.ActionScopeAgentExec, record.Executable, record.Argv, record.WorkingDirectory,
		startedAt, research.ResolveObservationLevel(false, "", false),
	)
	start.LeaseID = record.LeaseID
	start.ResearchSessionID = record.SessionID
	start.AgentWorkspaceID = record.AgentWorkspaceID
	start.AgentGeneration = record.AgentGeneration
	start.ExecID = record.ID
	start.CorrelationID = request.CorrelationID
	start.IdempotencyKey = request.IdempotencyKey
	start.EnvironmentKeys = research.EnvironmentKeys(request.Environment)
	session, err := e.actionEvidence.Begin(ctx, start)
	if err != nil {
		e.actionEvidence.RecordBeginFailure(record.ID, research.ReasonBeginConflict)
		// Callers fail-open and continue with a nil session.
		return nil, fmt.Errorf("begin action evidence for %s: %w", record.ID, err)
	}
	return session, nil
}

func (e *Environment) sealAction(ctx context.Context, session *research.Session, terminal transport.Terminal, lifecycle transport.ProcessLifecycle) error {
	if session == nil {
		return nil
	}
	outcome := research.ActionOutcome{
		EndedAt: time.Now().UTC(), Signal: terminal.Signal, Error: terminal.Error,
		CleanupConfirmed: terminal.CleanupConfirmed,
	}
	if started := lifecycle.Started(); started != nil {
		outcome.ProcessID = started.PID
		outcome.ProcessStartNS = started.ProcessStartNS
		outcome.ParentPID = started.ParentPID
	}
	if lifecycle.Started() != nil || terminal.CleanupConfirmed {
		exit := terminal.ExitCode
		outcome.ExitCode = &exit
	}
	sealCtx, cancel := actionEvidenceContext(ctx, e.controlTimeout)
	defer cancel()
	if _, err := session.Seal(sealCtx, outcome); err != nil {
		return fmt.Errorf("seal action evidence for %s: %w", session.ActionID(), err)
	}
	return nil
}

func (e *Environment) finalizeWithActionEvidence(ctx context.Context, request Request, execution application.ExecRecord, state domain.ExecState, cause error, session *research.Session, lifecycle transport.ProcessLifecycle) error {
	if cause == nil {
		cause = errors.New("execution failed")
	}
	// Best-effort seal; never join seal errors into the execution cause.
	_ = e.sealAction(ctx, session, transport.Terminal{Error: cause.Error(), CleanupConfirmed: false}, lifecycle)
	return e.finalizeFailure(ctx, request, execution, state, false, cause)
}

func actionEvidenceContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func exchange(ctx context.Context, stream ports.ExecTransport, request Request, heartbeatInterval time.Duration) (transport.Terminal, transport.ProcessLifecycle, error) {
	inputContext, cancelInput := context.WithCancel(ctx)
	defer cancelInput()
	stopHeartbeat := ports.MaintainExecHeartbeat(ctx, stream, heartbeatInterval)
	defer stopHeartbeat()
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
	var lifecycle transport.ProcessLifecycle
	for {
		frame, err := stream.Receive(ctx)
		if err != nil {
			cancelInput()
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return transport.Terminal{}, lifecycle, heartbeatErr
			}
			return transport.Terminal{}, lifecycle, err
		}
		frame.Data = append([]byte(nil), frame.Data...)
		switch frame.Kind {
		case transport.KindStdout, transport.KindStderr:
			if int64(len(frame.Data)) > request.MaxOutputBytes-outputBytes {
				cancelInput()
				return transport.Terminal{}, lifecycle, transport.ErrOutputLimit
			}
			outputBytes += int64(len(frame.Data))
			callback := request.OnStdout
			if frame.Kind == transport.KindStderr {
				callback = request.OnStderr
			}
			if callback != nil {
				if err := callback(frame.Data); err != nil {
					cancelInput()
					return transport.Terminal{}, lifecycle, err
				}
			}
		case transport.KindProcess:
			if err := lifecycle.Observe(frame); err != nil {
				cancelInput()
				return transport.Terminal{}, lifecycle, fmt.Errorf("process lifecycle: %w", err)
			}
			if request.OnControl != nil {
				if err := request.OnControl(frame); err != nil {
					cancelInput()
					return transport.Terminal{}, lifecycle, err
				}
			}
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			cancelInput()
			inputErr := <-inputDone
			heartbeatErr := stopHeartbeat()
			if inputErr != nil && !errors.Is(inputErr, context.Canceled) {
				return transport.Terminal{}, lifecycle, inputErr
			}
			if heartbeatErr != nil {
				return transport.Terminal{}, lifecycle, heartbeatErr
			}
			if err != nil {
				return transport.Terminal{}, lifecycle, err
			}
			if err := lifecycle.ValidateTerminal(terminal); err != nil {
				// Non-fatal for evidence: still return terminal; lifecycle PID may be partial.
				_ = err
			}
			if request.OnControl != nil {
				if err := request.OnControl(frame); err != nil {
					return transport.Terminal{}, lifecycle, err
				}
			}
			return terminal, lifecycle, nil
		default:
			if request.OnControl != nil {
				if err := request.OnControl(frame); err != nil {
					cancelInput()
					return transport.Terminal{}, lifecycle, err
				}
			}
			if frame.Kind == transport.KindError {
				cancelInput()
				return transport.Terminal{}, lifecycle, fmt.Errorf("guest protocol: %s", frame.Data)
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
