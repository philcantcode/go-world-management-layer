package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

// ActionEvidence returns the durable action evidence store.
func (s *Service) ActionEvidence() *research.Store {
	if s == nil {
		return nil
	}
	return s.actionEvidence
}

// Material returns the composed material authority, if any.
func (s *Service) Material() ports.MaterialAuthority {
	if s == nil {
		return nil
	}
	return s.material
}

func (s *Service) beginAgentAction(ctx context.Context, record application.ExecRecord, meta application.MutationMeta) (*research.Session, error) {
	if s == nil || s.actionEvidence == nil {
		return nil, nil
	}
	startedAt := record.CreatedAt
	if startedAt.IsZero() {
		startedAt = s.clock().UTC()
	}
	// Production wiring is baseline-only until policy/escalate is plumbed
	// through OpenExec (see ADR 0011).
	start := research.StartFromCommand(
		record.ID, research.ActionScopeAgentExec, record.Executable, record.Argv, record.WorkingDirectory,
		startedAt, research.ResolveObservationLevel(false, "", false),
	)
	start.LeaseID = record.LeaseID
	start.ResearchSessionID = record.SessionID
	start.AgentWorkspaceID = record.AgentWorkspaceID
	start.AgentGeneration = record.AgentGeneration
	start.ExecID = record.ID
	start.CorrelationID = meta.CorrelationID
	start.IdempotencyKey = meta.IdempotencyKey
	session, err := s.actionEvidence.Begin(ctx, start)
	if err != nil {
		s.actionEvidence.RecordBeginFailure(record.ID, research.ReasonBeginConflict)
		// Return the error for optional diagnostics; callers must fail-open
		// and continue the command with a nil session.
		return nil, fmt.Errorf("begin agent action evidence for %s: %w", record.ID, err)
	}
	return session, nil
}

func (s *Service) beginTargetAction(ctx context.Context, target application.TargetRecord, run application.TargetRunRecord, operation application.TargetOperationRecord, executable string, argv []string, workingDirectory string, meta application.MutationMeta) (*research.Session, error) {
	if s == nil || s.actionEvidence == nil {
		return nil, nil
	}
	startedAt := operation.CreatedAt
	if startedAt.IsZero() {
		startedAt = s.clock().UTC()
	}
	// Production wiring is baseline-only until policy/escalate is plumbed
	// through OpenTargetExec (see ADR 0011).
	start := research.StartFromCommand(
		operation.ID, research.ActionScopeTargetOperation, executable, argv, workingDirectory,
		startedAt, research.ResolveObservationLevel(false, "", false),
	)
	start.LeaseID = target.LeaseID
	start.ResearchSessionID = target.SessionID
	start.TargetID = target.ID
	start.TargetGeneration = run.Generation
	start.TargetRunID = run.ID
	start.TargetOperationID = operation.ID
	start.CorrelationID = meta.CorrelationID
	start.IdempotencyKey = meta.IdempotencyKey
	session, err := s.actionEvidence.Begin(ctx, start)
	if err != nil {
		s.actionEvidence.RecordBeginFailure(operation.ID, research.ReasonBeginConflict)
		return nil, fmt.Errorf("begin target action evidence for %s: %w", operation.ID, err)
	}
	return session, nil
}

func (s *Service) sealActionSession(ctx context.Context, session *research.Session, terminal transport.Terminal, lifecycle transport.ProcessLifecycle) error {
	if session == nil {
		return nil
	}
	endedAt := time.Now().UTC()
	if s != nil && s.clock != nil {
		endedAt = s.clock().UTC()
	}
	outcome := research.ActionOutcome{
		EndedAt: endedAt, Signal: terminal.Signal, Error: terminal.Error,
		CleanupConfirmed: terminal.CleanupConfirmed,
	}
	if started := lifecycle.Started(); started != nil {
		outcome.ProcessID = started.PID
		outcome.ProcessStartNS = started.ProcessStartNS
		outcome.ParentPID = started.ParentPID
	}
	// ExitCode is set only when a real process exit is known. Pure transport
	// failures leave exit_code omitted (nil), not zero.
	if lifecycle.Started() != nil || terminal.CleanupConfirmed {
		exit := terminal.ExitCode
		outcome.ExitCode = &exit
	}
	sealCtx, cancel := orchestrationEvidenceContext(ctx, s.controlTimeout)
	defer cancel()
	if _, err := session.Seal(sealCtx, outcome); err != nil {
		return fmt.Errorf("seal action evidence for %s: %w", session.ActionID(), err)
	}
	return nil
}

func orchestrationEvidenceContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

// sealActionOnFailure best-effort seals evidence for a failed exec. Seal and
// begin evidence errors never replace the execution cause.
func (s *Service) sealActionOnFailure(ctx context.Context, session *research.Session, lifecycle transport.ProcessLifecycle, cause error) error {
	if cause == nil {
		cause = errors.New("execution failed")
	}
	_ = s.sealActionSession(ctx, session, transport.Terminal{Error: cause.Error(), CleanupConfirmed: false}, lifecycle)
	return cause
}

// capturingExecWire tees stdout/stderr into an action evidence session.
type capturingExecWire struct {
	inner   execWire
	session *research.Session
}

func (w capturingExecWire) Context() context.Context { return w.inner.Context() }
func (w capturingExecWire) Receive() (execInput, error) {
	return w.inner.Receive()
}
func (w capturingExecWire) SendOutput(kind transport.Kind, data []byte) error {
	if w.session != nil {
		switch kind {
		case transport.KindStdout:
			w.session.AppendStdout(data)
		case transport.KindStderr:
			w.session.AppendStderr(data)
		}
	}
	return w.inner.SendOutput(kind, data)
}
func (w capturingExecWire) SendOutcome(outcome *worldv1.ExecOutcome) error {
	return w.inner.SendOutcome(outcome)
}
