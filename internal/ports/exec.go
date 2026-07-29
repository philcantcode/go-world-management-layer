package ports

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

// ExecTransport is a framed, ordered, byte-transparent stream. Implementations
// must serialize concurrent sends, return exactly one terminal frame, unblock
// Receive and Send on Close, and honor context cancellation.
type ExecTransport interface {
	Send(context.Context, transport.Kind, []byte) (transport.Frame, error)
	Receive(context.Context) (transport.Frame, error)
	Close() error
}

const DefaultExecHeartbeatInterval = 10 * time.Second

// MaintainExecHeartbeat keeps a guest exec lease alive until the returned
// stop function is called or ctx ends. A heartbeat send failure closes the
// transport to unblock a concurrent Receive and is returned by stop.
func MaintainExecHeartbeat(ctx context.Context, stream ExecTransport, interval time.Duration) func() error {
	if interval <= 0 {
		interval = DefaultExecHeartbeatInterval
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var heartbeatErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if _, err := stream.Send(heartbeatCtx, transport.KindHeartbeat, nil); err != nil {
					if heartbeatCtx.Err() != nil && errors.Is(err, heartbeatCtx.Err()) {
						return
					}
					heartbeatErr = err
					_ = stream.Close()
					return
				}
			}
		}
	}()
	return func() error {
		cancel()
		<-done
		return heartbeatErr
	}
}

type ExecPlan struct {
	LeaseID          domain.LeaseID
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	Exec             domain.Exec
	Start            transport.ExecStart
}

func (p ExecPlan) Validate() error {
	const operation = "ports.exec_plan.validate"
	if p.LeaseID.IsZero() || p.AgentWorkspaceID.IsZero() || !p.AgentGeneration.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "scope", "lease, agent workspace, and generation are required", nil)
	}
	if p.Exec.ID().IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "exec", "must be initialized", nil)
	}
	spec := p.Exec.Spec()
	if spec.LeaseID != p.LeaseID || spec.AgentWorkspaceID != p.AgentWorkspaceID || spec.AgentGeneration != p.AgentGeneration {
		return domain.NewError(domain.CodeConflict, operation, "exec", "scope does not match the plan", nil)
	}
	if p.Start.ExecID != p.Exec.ID().String() {
		return domain.NewError(domain.CodeConflict, operation, "start.exec_id", "does not match the domain exec", nil)
	}
	if err := p.Start.Validate(64 << 20); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "start", "is invalid", err)
	}
	return nil
}

type TargetExecPlan struct {
	Operation domain.TargetOperation
	Start     transport.ExecStart
}

func (p TargetExecPlan) Validate() error {
	const operation = "ports.target_exec_plan.validate"
	if p.Operation.ID().IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "operation", "must be initialized", nil)
	}
	if p.Operation.Spec().Kind != domain.TargetOperationExec && p.Operation.Spec().Kind != domain.TargetOperationShell {
		return domain.NewError(domain.CodeInvalidArgument, operation, "operation.kind", "must be exec or shell", nil)
	}
	if err := p.Start.Validate(64 << 20); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "start", "is invalid", err)
	}
	return nil
}

type TargetTransferPlan struct {
	Operation    domain.TargetOperation
	RelativePath string
	// Mode is the exact portable permission mode to apply to a pushed file.
	// Zero selects the safe 0600 default. It must be zero for pulls. Only the
	// low nine Unix permission bits are accepted; setuid, setgid, sticky, and
	// file-type bits are never allowed.
	Mode         uint32
	MaximumBytes int64
}

func (p TargetTransferPlan) Validate(kind domain.TargetOperationKind) error {
	const operation = "ports.target_transfer_plan.validate"
	if p.Operation.ID().IsZero() || p.Operation.Spec().Kind != kind {
		return domain.NewError(domain.CodeInvalidArgument, operation, "operation", "has the wrong or uninitialized kind", nil)
	}
	if _, err := safepath.Normalize(p.RelativePath); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "relative_path", "must be a safe logical relative path", err)
	}
	if p.MaximumBytes <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "maximum_bytes", "must be positive", nil)
	}
	if kind == domain.TargetOperationPull && p.Mode != 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "mode", "must be zero for pulls", nil)
	}
	if kind == domain.TargetOperationPush && p.Mode&^uint32(0o777) != 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "mode", "may contain only user/group/other permission bits", nil)
	}
	return nil
}

type TransferResult struct {
	OperationID domain.TargetOperationID
	Digest      domain.Digest
	Bytes       int64
}

// ContentReader exposes verified immutable content without revealing its host
// or repository path.
type ContentReader interface {
	io.ReadCloser
	Digest() domain.Digest
	Size() int64
}
