package linuxcontainer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

const (
	targetGuestBinary    = "/usr/local/bin/world-guest"
	targetReadinessGrace = 10 * time.Second
	targetReadinessBytes = int64(64 << 10)
	targetCleanupGrace   = 10 * time.Second
)

// requireGuestReadiness proves that the exact framed guest endpoint used by
// target operations can launch a process, report its identity and cleanup it.
func (d *Driver) requireGuestReadiness(ctx context.Context, runtimeID string, plan ContainerPlan) error {
	readinessCtx, cancel := context.WithTimeout(ctx, targetReadinessGrace)
	defer cancel()
	execPlan, err := d.guestReadinessPlan(readinessCtx, plan)
	if err != nil {
		return err
	}
	session, err := d.runtime.OpenExec(readinessCtx, runtimeID, execPlan)
	if err != nil {
		return err
	}
	probeErr := receiveTargetGuestReadiness(readinessCtx, session)
	return errors.Join(probeErr, session.Close())
}

func (d *Driver) guestReadinessPlan(ctx context.Context, plan ContainerPlan) (ports.TargetExecPlan, error) {
	runID, err := domain.NewTargetRunID()
	if err != nil {
		return ports.TargetExecPlan{}, fmt.Errorf("create readiness run identity: %w", err)
	}
	operationID, err := domain.NewTargetOperationID()
	if err != nil {
		return ports.TargetExecPlan{}, fmt.Errorf("create readiness operation identity: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return ports.TargetExecPlan{}, fmt.Errorf("readiness context has no deadline")
	}
	createdAt := d.now().UTC()
	operation, err := domain.NewTargetOperation(domain.TargetOperationSpec{
		ID: operationID, LeaseID: plan.LeaseID, TargetID: plan.TargetID, TargetGeneration: plan.Generation,
		TargetRunID: runID, Kind: domain.TargetOperationExec,
		CommandDisplay: targetGuestBinary + " " + transport.GuestSelfTestArgument, CreatedAt: createdAt,
	})
	if err != nil {
		return ports.TargetExecPlan{}, err
	}
	execPlan := ports.TargetExecPlan{
		Operation: operation,
		Start: transport.ExecStart{
			ExecID: operationID.String(), IdempotencyKey: "world-target-readiness-" + operationID.String(),
			Executable: targetGuestBinary, Argv: []string{transport.GuestSelfTestArgument}, WorkingDirectory: TargetMount,
			Deadline: deadline, MaxOutputBytes: targetReadinessBytes, CleanupGrace: time.Second,
		},
	}
	if err := execPlan.Validate(); err != nil {
		return ports.TargetExecPlan{}, err
	}
	return execPlan, nil
}

func receiveTargetGuestReadiness(ctx context.Context, session ports.ExecTransport) error {
	var outputBytes int64
	var lifecycle transport.ProcessLifecycle
	for {
		frame, err := session.Receive(ctx)
		if err != nil {
			return err
		}
		switch frame.Kind {
		case transport.KindStdout, transport.KindStderr:
			if int64(len(frame.Data)) > targetReadinessBytes-outputBytes {
				return transport.ErrOutputLimit
			}
			outputBytes += int64(len(frame.Data))
		case transport.KindProcess:
			if err := lifecycle.Observe(frame); err != nil {
				return fmt.Errorf("invalid readiness process event: %w", err)
			}
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			if err != nil {
				return err
			}
			if err := lifecycle.ValidateTerminal(terminal); err != nil {
				return fmt.Errorf("readiness process lifecycle: %w", err)
			}
			if terminal.ExitCode != 0 || !terminal.CleanupConfirmed || terminal.Error != "" {
				return fmt.Errorf("readiness outcome is not authoritative: exit=%d cleanup=%t error=%q", terminal.ExitCode, terminal.CleanupConfirmed, terminal.Error)
			}
			return nil
		default:
			return fmt.Errorf("unexpected readiness frame kind %d", frame.Kind)
		}
	}
}
