package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type execInput struct {
	stdin     []byte
	signal    string
	resize    *worldv1.TerminalSettings
	heartbeat bool
}

type execWire interface {
	Context() context.Context
	Receive() (execInput, error)
	SendOutput(transport.Kind, []byte) error
	SendOutcome(*worldv1.ExecOutcome) error
}

type agentExecWire struct {
	stream worldv1.WorldService_OpenExecServer
}

func (w agentExecWire) Context() context.Context { return w.stream.Context() }
func (w agentExecWire) Receive() (execInput, error) {
	frame, err := w.stream.Recv()
	if err != nil {
		return execInput{}, err
	}
	if frame == nil || frame.Start != nil || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Outcome != nil {
		return execInput{}, status.Error(codes.InvalidArgument, "client exec frame contains a server-only or repeated start field")
	}
	if err := requireExactlyOneExecInput("exec", populatedExecFields(frame.Stdin, frame.Signal, frame.Resize, frame.Heartbeat)); err != nil {
		return execInput{}, err
	}
	return execInput{stdin: append([]byte(nil), frame.Stdin...), signal: frame.Signal, resize: frame.Resize, heartbeat: frame.Heartbeat}, nil
}
func (w agentExecWire) SendOutput(kind transport.Kind, data []byte) error {
	frame := &worldv1.ExecFrame{}
	switch kind {
	case transport.KindStdout:
		frame.Stdout = append([]byte(nil), data...)
	case transport.KindStderr:
		frame.Stderr = append([]byte(nil), data...)
	case transport.KindHeartbeat:
		frame.Heartbeat = true
	default:
		return status.Errorf(codes.Internal, "exec driver returned unknown output kind %d", kind)
	}
	return w.stream.Send(frame)
}
func (w agentExecWire) SendOutcome(outcome *worldv1.ExecOutcome) error {
	return w.stream.Send(&worldv1.ExecFrame{Outcome: outcome})
}

type targetExecWire struct {
	stream worldv1.WorldService_OpenTargetExecServer
}

func (w targetExecWire) Context() context.Context { return w.stream.Context() }
func (w targetExecWire) Receive() (execInput, error) {
	frame, err := w.stream.Recv()
	if err != nil {
		return execInput{}, err
	}
	if frame == nil || frame.Start != nil || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Outcome != nil {
		return execInput{}, status.Error(codes.InvalidArgument, "client target exec frame contains a server-only or repeated start field")
	}
	if err := requireExactlyOneExecInput("target exec", populatedExecFields(frame.Stdin, frame.Signal, frame.Resize, frame.Heartbeat)); err != nil {
		return execInput{}, err
	}
	return execInput{stdin: append([]byte(nil), frame.Stdin...), signal: frame.Signal, resize: frame.Resize, heartbeat: frame.Heartbeat}, nil
}
func (w targetExecWire) SendOutput(kind transport.Kind, data []byte) error {
	frame := &worldv1.TargetExecFrame{}
	switch kind {
	case transport.KindStdout:
		frame.Stdout = append([]byte(nil), data...)
	case transport.KindStderr:
		frame.Stderr = append([]byte(nil), data...)
	case transport.KindHeartbeat:
		frame.Heartbeat = true
	default:
		return status.Errorf(codes.Internal, "target driver returned unknown output kind %d", kind)
	}
	return w.stream.Send(frame)
}
func (w targetExecWire) SendOutcome(outcome *worldv1.ExecOutcome) error {
	return w.stream.Send(&worldv1.TargetExecFrame{Outcome: outcome})
}

func (s *Service) OpenExec(stream worldv1.WorldService_OpenExecServer) error {
	if s.agent == nil {
		return status.Error(codes.FailedPrecondition, "agent execution is unavailable because no production agent driver is configured")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := requireExecStartFrame(first); err != nil {
		return err
	}
	start := first.Start
	ctx, cancel, meta, err := mutationContext(stream.Context(), start.Mutation)
	if err != nil {
		return err
	}
	defer cancel()
	if err := s.authorize(ctx, meta.AuthorizedPolicyReference, application.AuthorizationRequest{LeaseID: start.LeaseId}); err != nil {
		return err
	}
	if err := validatePublicExecStart(start); err != nil {
		return err
	}
	temporaryInputs, err := mapTemporaryInputs(start.TemporaryInputs, len(start.Argv), s.maxExecBytes)
	if err != nil {
		return err
	}
	created, err := s.core.CreateExec(ctx, application.CreateExecRequest{
		Meta: childMeta(meta, "create", deadline(ctx)), LeaseID: start.LeaseId,
		Kind: domain.ExecProvider, Executable: start.ProviderExecutable,
		Argv: append([]string(nil), start.Argv...), WorkingDirectory: defaultDirectory(start.WorkspaceRelativeWorkingDirectory),
	})
	if err != nil {
		return err
	}
	if created.State.Terminal() {
		return (agentExecWire{stream: stream}).SendOutcome(execOutcomeFromRecord(created))
	}
	if created.State != domain.ExecRequested {
		return status.Errorf(codes.FailedPrecondition, "exec %s is already in progress", created.ID)
	}
	starting, err := s.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: childMeta(meta, "starting", deadline(ctx)), ExecID: created.ID, ExpectedRevision: created.Revision, State: domain.ExecStarting})
	if err != nil {
		return err
	}
	model, err := domainExec(starting)
	if err != nil {
		return s.finalizeExecFailure(ctx, meta, starting, err)
	}
	transportStart := transport.ExecStart{
		ExecID: starting.ID, IdempotencyKey: meta.IdempotencyKey, Executable: starting.Executable,
		Argv: append([]string(nil), starting.Argv...), WorkingDirectory: starting.WorkingDirectory,
		Terminal:        terminalEnabled(start.Terminal),
		TemporaryInputs: temporaryInputs, Deadline: deadline(ctx), MaxOutputBytes: s.maxExecBytes, CleanupGrace: s.controlTimeout,
	}
	modelSpec := model.Spec()
	connection, err := s.agent.OpenExec(ctx, ports.ExecPlan{
		LeaseID: modelSpec.LeaseID, AgentWorkspaceID: modelSpec.AgentWorkspaceID,
		AgentGeneration: domain.AgentGeneration(starting.AgentGeneration), Exec: model, Start: transportStart,
	})
	if err != nil {
		return s.finalizeExecFailure(ctx, meta, starting, err)
	}
	defer connection.Close()
	running, err := s.core.TransitionExec(ctx, application.TransitionExecRequest{Meta: childMeta(meta, "running", deadline(ctx)), ExecID: starting.ID, ExpectedRevision: starting.Revision, State: domain.ExecRunning})
	if err != nil {
		return errors.Join(err, connection.Close())
	}
	terminal, exchangeErr := exchangeExec(ctx, connection, agentExecWire{stream: stream}, s.maxExecBytes)
	closeErr := connection.Close()
	if exchangeErr != nil {
		return s.finalizeExecFailure(ctx, meta, running, errors.Join(exchangeErr, closeErr))
	}
	if closeErr != nil {
		terminal.CleanupConfirmed = false
		terminal.Error = joinMessage(terminal.Error, closeErr)
	}
	finalized, err := s.finalizeExec(ctx, meta, running, terminal)
	if err != nil {
		return err
	}
	return (agentExecWire{stream: stream}).SendOutcome(execOutcomeFromRecord(finalized))
}

func (s *Service) OpenTargetExec(stream worldv1.WorldService_OpenTargetExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := requireTargetExecStartFrame(first); err != nil {
		return err
	}
	start := first.Start
	ctx, cancel, meta, err := mutationContext(stream.Context(), start.Mutation)
	if err != nil {
		return err
	}
	defer cancel()
	target, run, driver, err := s.scopedTarget(ctx, start.TargetId, start.TargetRunId, meta.AuthorizedPolicyReference)
	if err != nil {
		return err
	}
	if run.State != domain.TargetRunRunning {
		return status.Errorf(codes.FailedPrecondition, "target run is %s, not running", run.State)
	}
	kind, commandDisplay, contentDigest, transportStart, err := s.targetExecStart(start, meta, ctx)
	if err != nil {
		return err
	}
	operation, err := s.core.CreateTargetOperation(ctx, application.CreateTargetOperationRequest{
		Meta: childMeta(meta, "operation-create", deadline(ctx)), TargetID: target.ID, RunID: run.ID,
		Kind: kind, CommandDisplay: commandDisplay, ContentDigest: contentDigest,
	})
	if err != nil {
		return err
	}
	if operation.State.Terminal() {
		return (targetExecWire{stream: stream}).SendOutcome(operationOutcome(operation))
	}
	if operation.State != domain.TargetOperationRequested {
		return status.Errorf(codes.FailedPrecondition, "target operation %s is already in progress", operation.ID)
	}
	model, err := domainTargetOperation(target, operation)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	transportStart.ExecID = operation.ID
	connection, err := driver.OpenTransport(ctx, model.Spec().TargetRunID)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	defer connection.Close()
	execTransport, err := connection.OpenExec(ctx, ports.TargetExecPlan{Operation: model, Start: transportStart})
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, errors.Join(err, connection.Close()))
	}
	defer execTransport.Close()
	running, err := s.core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{Meta: childMeta(meta, "operation-running", deadline(ctx)), TargetID: target.ID, OperationID: operation.ID, ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning})
	if err != nil {
		return errors.Join(err, execTransport.Close(), connection.Close())
	}
	terminal, exchangeErr := exchangeExec(ctx, execTransport, targetExecWire{stream: stream}, s.maxExecBytes)
	closeErr := errors.Join(execTransport.Close(), connection.Close())
	if exchangeErr != nil || closeErr != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, errors.Join(exchangeErr, closeErr))
	}
	terminalState := domain.TargetOperationCompleted
	if terminal.ExitCode != 0 || terminal.Signal != "" || terminal.Error != "" || !terminal.CleanupConfirmed {
		terminalState = domain.TargetOperationFailed
	}
	finalized, err := s.transitionOperation(ctx, meta, target.ID, running, terminalState, "operation-terminal")
	if err != nil {
		return err
	}
	if err := (targetExecWire{stream: stream}).SendOutcome(operationOutcomeWithTerminal(finalized, terminal)); err != nil {
		return err
	}
	return nil
}

func requireStartOnly(kind string, hasStart, hasOtherFields bool) error {
	if !hasStart || hasOtherFields {
		return status.Errorf(codes.InvalidArgument, "first %s frame must contain only start", kind)
	}
	return nil
}

func requireExactlyOneExecInput(kind string, populated int) error {
	if populated != 1 {
		return status.Errorf(codes.InvalidArgument, "%s frame must contain exactly one input field", kind)
	}
	return nil
}

func requireExecStartFrame(frame *worldv1.ExecFrame) error {
	return requireStartOnly("exec", frame != nil && frame.Start != nil, frame != nil && (len(frame.Stdin) > 0 || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Signal != "" || frame.Resize != nil || frame.Heartbeat || frame.Outcome != nil))
}

func requireTargetExecStartFrame(frame *worldv1.TargetExecFrame) error {
	return requireStartOnly("target exec", frame != nil && frame.Start != nil, frame != nil && (len(frame.Stdin) > 0 || len(frame.Stdout) > 0 || len(frame.Stderr) > 0 || frame.Signal != "" || frame.Resize != nil || frame.Heartbeat || frame.Outcome != nil))
}

func (s *Service) targetExecStart(start *worldv1.TargetExecStart, meta application.MutationMeta, ctx context.Context) (domain.TargetOperationKind, string, string, transport.ExecStart, error) {
	if len(start.Argv) == 0 && len(start.ExplicitShellBytes) == 0 || len(start.Argv) > 0 && len(start.ExplicitShellBytes) > 0 {
		return "", "", "", transport.ExecStart{}, status.Error(codes.InvalidArgument, "exactly one of argv or explicit_shell_bytes is required")
	}
	workingDirectory := defaultDirectory(start.TargetRelativeWorkingDirectory)
	result := transport.ExecStart{IdempotencyKey: meta.IdempotencyKey, WorkingDirectory: workingDirectory, Terminal: terminalEnabled(start.Terminal), Deadline: deadline(ctx), MaxOutputBytes: s.maxExecBytes, CleanupGrace: s.controlTimeout}
	if len(start.ExplicitShellBytes) > 0 {
		if int64(len(start.ExplicitShellBytes)) > s.maxExecBytes {
			return "", "", "", transport.ExecStart{}, status.Error(codes.ResourceExhausted, "explicit shell input exceeds the exec byte limit")
		}
		result.Executable = "/bin/sh"
		result.Argv = []string{"world-script.sh"}
		result.TemporaryInputs = []transport.TemporaryInput{{NameHint: "world-script.sh", ArgvIndex: 0, Mode: 0o700, Bytes: append([]byte(nil), start.ExplicitShellBytes...)}}
		return domain.TargetOperationShell, "explicit shell content", domain.NewDigest(start.ExplicitShellBytes).String(), result, nil
	}
	result.Executable = start.Argv[0]
	result.Argv = append([]string(nil), start.Argv[1:]...)
	display := strings.Join(start.Argv, " ")
	if len(display) > 4096 {
		display = display[:4096]
	}
	return domain.TargetOperationExec, display, "", result, nil
}

func exchangeExec(ctx context.Context, connection ports.ExecTransport, wire execWire, maxBytes int64) (transport.Terminal, error) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	inputErrors := make(chan error, 1)
	go func() {
		err := forwardExecInput(child, connection, wire, maxBytes)
		if err != nil {
			cancel()
		}
		inputErrors <- err
	}()
	var outputBytes int64
	var lifecycle transport.ProcessLifecycle
	for {
		frame, err := connection.Receive(child)
		if err != nil {
			select {
			case inputErr := <-inputErrors:
				if inputErr != nil {
					return transport.Terminal{}, inputErr
				}
			default:
			}
			return transport.Terminal{}, err
		}
		switch frame.Kind {
		case transport.KindStdout, transport.KindStderr:
			if int64(len(frame.Data)) > maxBytes-outputBytes {
				return transport.Terminal{}, transport.ErrOutputLimit
			}
			outputBytes += int64(len(frame.Data))
			if err := wire.SendOutput(frame.Kind, frame.Data); err != nil {
				return transport.Terminal{}, err
			}
		case transport.KindHeartbeat:
			if err := wire.SendOutput(frame.Kind, nil); err != nil {
				return transport.Terminal{}, err
			}
		case transport.KindProcess:
			if err := lifecycle.Observe(frame); err != nil {
				return transport.Terminal{}, status.Errorf(codes.Internal, "exec driver returned invalid process lifecycle: %v", err)
			}
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			if err != nil {
				return transport.Terminal{}, err
			}
			if err := lifecycle.ValidateTerminal(terminal); err != nil {
				return transport.Terminal{}, status.Errorf(codes.Internal, "exec driver returned invalid process lifecycle: %v", err)
			}
			return terminal, nil
		case transport.KindError:
			return transport.Terminal{}, fmt.Errorf("exec transport: %s", frame.Data)
		default:
			return transport.Terminal{}, status.Errorf(codes.Internal, "exec driver returned unknown control frame kind %d", frame.Kind)
		}
	}
}

func forwardExecInput(ctx context.Context, connection ports.ExecTransport, wire execWire, maxBytes int64) error {
	var inputBytes int64
	for {
		input, err := wire.Receive()
		if errors.Is(err, io.EOF) {
			_, closeErr := connection.Send(ctx, transport.KindCloseInput, nil)
			return closeErr
		}
		if err != nil {
			return err
		}
		var kind transport.Kind
		var payload []byte
		switch {
		case input.stdin != nil:
			if int64(len(input.stdin)) > maxBytes-inputBytes {
				return status.Error(codes.ResourceExhausted, "exec input exceeds the byte limit")
			}
			inputBytes += int64(len(input.stdin))
			kind, payload = transport.KindStdin, input.stdin
		case input.signal != "":
			kind = transport.KindSignal
			payload, err = json.Marshal(transport.Signal{Name: input.signal})
		case input.resize != nil:
			if input.resize.Rows == 0 || input.resize.Columns == 0 {
				return status.Error(codes.InvalidArgument, "terminal resize requires positive rows and columns")
			}
			kind = transport.KindResize
			payload, err = json.Marshal(transport.Resize{Columns: input.resize.Columns, Rows: input.resize.Rows})
		case input.heartbeat:
			kind = transport.KindHeartbeat
		default:
			return status.Error(codes.InvalidArgument, "empty exec input frame")
		}
		if err != nil {
			return err
		}
		if _, err := connection.Send(ctx, kind, payload); err != nil {
			return err
		}
	}
}

func (s *Service) finalizeExec(ctx context.Context, meta application.MutationMeta, record application.ExecRecord, terminal transport.Terminal) (application.ExecRecord, error) {
	state := domain.ExecCompleted
	if terminal.ExitCode != 0 || terminal.Signal != "" || terminal.Error != "" || !terminal.CleanupConfirmed {
		state = domain.ExecFailed
	}
	exitCode := terminal.ExitCode
	request := application.FinalizeExecRequest{Meta: childMeta(meta, "terminal", deadline(ctx)), ExecID: record.ID, ExpectedRevision: record.Revision, State: state, ExitCode: &exitCode, Signal: terminal.Signal, CleanupConfirmed: terminal.CleanupConfirmed, Error: terminal.Error}
	if terminal.IncidentID != "" {
		request.IncidentIDs = []string{terminal.IncidentID}
	}
	return s.core.FinalizeExec(ctx, request)
}

func (s *Service) finalizeExecFailure(ctx context.Context, meta application.MutationMeta, record application.ExecRecord, cause error) error {
	if cause == nil {
		cause = errors.New("exec failed")
	}
	state := domain.ExecFailed
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		state = domain.ExecCancelled
	}
	finalizeCtx, finalizeMeta, cancel := s.finalizationContext(ctx, meta, "failure")
	defer cancel()
	_, finalizeErr := s.core.FinalizeExec(finalizeCtx, application.FinalizeExecRequest{Meta: finalizeMeta, ExecID: record.ID, ExpectedRevision: record.Revision, State: state, CleanupConfirmed: false, Error: boundedError(cause)})
	return errors.Join(cause, finalizeErr)
}

func (s *Service) transitionOperation(ctx context.Context, meta application.MutationMeta, targetID string, record application.TargetOperationRecord, state domain.TargetOperationState, suffix string) (application.TargetOperationRecord, error) {
	transitionCtx, transitionMeta, cancel := s.finalizationContext(ctx, meta, suffix)
	defer cancel()
	return s.core.TransitionTargetOperation(transitionCtx, application.TransitionTargetOperationRequest{Meta: transitionMeta, TargetID: targetID, OperationID: record.ID, ExpectedRevision: record.Revision, State: state})
}

func (s *Service) finalizeOperationFailure(ctx context.Context, meta application.MutationMeta, targetID string, record application.TargetOperationRecord, cause error) error {
	state := domain.TargetOperationFailed
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		state = domain.TargetOperationCancelled
	}
	_, transitionErr := s.transitionOperation(ctx, meta, targetID, record, state, "operation-failure")
	return errors.Join(cause, transitionErr)
}

func (s *Service) finalizationContext(ctx context.Context, meta application.MutationMeta, suffix string) (context.Context, application.MutationMeta, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, childMeta(meta, suffix, deadline(ctx)), func() {}
	}
	recovery, cancel, recoveryDeadline := cleanupContext(s.controlTimeout)
	return recovery, childMeta(meta, suffix+"-recovery", recoveryDeadline), cancel
}

func validatePublicExecStart(value *worldv1.ExecStart) error {
	if value == nil || strings.TrimSpace(value.LeaseId) == "" || strings.TrimSpace(value.ProviderExecutable) == "" {
		return status.Error(codes.InvalidArgument, "lease_id and provider_executable are required")
	}
	if value.Terminal != nil && value.Terminal.Enabled && (value.Terminal.Rows == 0 || value.Terminal.Columns == 0) {
		return status.Error(codes.InvalidArgument, "enabled terminal requires positive rows and columns")
	}
	return nil
}

func mapTemporaryInputs(values []*worldv1.TemporaryInput, argvCount int, maximumBytes int64) ([]transport.TemporaryInput, error) {
	result := make([]transport.TemporaryInput, 0, len(values))
	usedIndexes := make(map[uint32]struct{}, len(values))
	var total int64
	for index, value := range values {
		if value == nil {
			return nil, status.Errorf(codes.InvalidArgument, "temporary_inputs[%d] is required", index)
		}
		name, err := safepath.Normalize(value.NameHint)
		if err != nil || strings.Contains(name, "/") {
			return nil, status.Errorf(codes.InvalidArgument, "temporary_inputs[%d].name_hint must be a safe file name", index)
		}
		if uint64(value.ArgvIndex) >= uint64(argvCount) {
			return nil, status.Errorf(codes.InvalidArgument, "temporary_inputs[%d].argv_index is outside argv", index)
		}
		if _, duplicate := usedIndexes[value.ArgvIndex]; duplicate {
			return nil, status.Errorf(codes.InvalidArgument, "temporary_inputs share argv_index %d", value.ArgvIndex)
		}
		usedIndexes[value.ArgvIndex] = struct{}{}
		if int64(len(value.Content)) > maximumBytes-total {
			return nil, status.Error(codes.ResourceExhausted, "temporary inputs exceed the configured byte limit")
		}
		total += int64(len(value.Content))
		mode := value.Mode
		if mode == 0 {
			mode = 0o600
		}
		if mode&^uint32(0o777) != 0 || mode&0o400 == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "temporary_inputs[%d].mode must be owner-readable and contain only permission bits", index)
		}
		result = append(result, transport.TemporaryInput{NameHint: name, ArgvIndex: int(value.ArgvIndex), Mode: mode, Bytes: append([]byte(nil), value.Content...)})
	}
	return result, nil
}

func domainExec(record application.ExecRecord) (domain.Exec, error) {
	const operation = "orchestration.domain_exec"
	id, err := domain.ParseExecID(record.ID)
	if err != nil {
		return domain.Exec{}, domain.NewError(domain.CodeIntegrityViolation, operation, "exec_id", "persisted identifier is invalid", err)
	}
	leaseID, err := requireStoredID(operation, "lease_id", record.LeaseID, domain.ParseLeaseID)
	if err != nil {
		return domain.Exec{}, err
	}
	agentID, err := requireStoredID(operation, "agent_workspace_id", record.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return domain.Exec{}, err
	}
	return domain.NewExec(domain.ExecSpec{
		ID: id, LeaseID: leaseID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.AgentGeneration(record.AgentGeneration), Kind: record.Kind,
		Executable: record.Executable, Argv: append([]string(nil), record.Argv...),
		WorkingDirectory: record.WorkingDirectory, CreatedAt: record.CreatedAt,
	})
}

func domainTargetOperation(target application.TargetRecord, record application.TargetOperationRecord) (domain.TargetOperation, error) {
	const operation = "orchestration.domain_target_operation"
	id, err := domain.ParseTargetOperationID(record.ID)
	if err != nil {
		return domain.TargetOperation{}, domain.NewError(domain.CodeIntegrityViolation, operation, "target_operation_id", "persisted identifier is invalid", err)
	}
	leaseID, err := requireStoredID(operation, "lease_id", target.LeaseID, domain.ParseLeaseID)
	if err != nil {
		return domain.TargetOperation{}, err
	}
	targetID, err := requireStoredID(operation, "target_id", target.ID, domain.ParseTargetID)
	if err != nil {
		return domain.TargetOperation{}, err
	}
	runID, err := requireStoredID(operation, "target_run_id", record.RunID, domain.ParseTargetRunID)
	if err != nil {
		return domain.TargetOperation{}, err
	}
	var digest domain.Digest
	if record.ContentDigest != "" {
		digest, err = domain.ParseDigest(record.ContentDigest)
		if err != nil {
			return domain.TargetOperation{}, err
		}
	}
	return domain.NewTargetOperation(domain.TargetOperationSpec{
		ID: id, LeaseID: leaseID, TargetID: targetID,
		TargetGeneration: domain.TargetGeneration(record.Generation), TargetRunID: runID,
		Kind: record.Kind, CommandDisplay: record.CommandDisplay, ContentDigest: digest, CreatedAt: record.CreatedAt,
	})
}

func populatedExecFields(stdin []byte, signal string, resize *worldv1.TerminalSettings, heartbeat bool) int {
	count := 0
	if stdin != nil {
		count++
	}
	if signal != "" {
		count++
	}
	if resize != nil {
		count++
	}
	if heartbeat {
		count++
	}
	return count
}

func terminalEnabled(value *worldv1.TerminalSettings) bool { return value != nil && value.Enabled }
func defaultDirectory(value string) string {
	if value == "" {
		return "."
	}
	return value
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 4096 {
		value = value[:4096]
	}
	return value
}

func joinMessage(value string, err error) string {
	if err == nil {
		return value
	}
	if value == "" {
		return boundedError(err)
	}
	return boundedError(errors.New(value + "; " + err.Error()))
}

func execOutcomeFromRecord(value application.ExecRecord) *worldv1.ExecOutcome {
	result := &worldv1.ExecOutcome{Termination: string(value.State), Error: value.Error}
	if value.ExitCode != nil {
		result.ExitCode = int32(*value.ExitCode)
	}
	if value.Signal != "" {
		result.Termination = value.Signal
	}
	return result
}

func operationOutcome(value application.TargetOperationRecord) *worldv1.ExecOutcome {
	return &worldv1.ExecOutcome{Termination: string(value.State)}
}

func operationOutcomeWithTerminal(value application.TargetOperationRecord, terminal transport.Terminal) *worldv1.ExecOutcome {
	result := &worldv1.ExecOutcome{ExitCode: int32(terminal.ExitCode), Termination: string(value.State), Error: terminal.Error}
	if terminal.Signal != "" {
		result.Termination = terminal.Signal
	}
	return result
}
