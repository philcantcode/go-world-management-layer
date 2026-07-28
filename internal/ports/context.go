// Package ports defines stable, world-owned interfaces between the logical
// control core and node/runtime adapters. Vendor SDK types must not cross these
// interfaces.
package ports

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// RequireDeadline enforces the contract for every potentially blocking driver
// operation. Drivers must also continue observing ctx after the call starts.
func RequireDeadline(ctx context.Context, operation string) error {
	if ctx == nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "context", "must be provided", nil)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return domain.NewError(domain.CodeInvalidArgument, operation, "deadline", "context deadline is required", nil)
	}
	if err := ctx.Err(); err != nil {
		code := domain.CodeUnavailable
		if errors.Is(err, context.DeadlineExceeded) || !deadline.After(time.Now()) {
			code = domain.CodeDeadlineExceeded
		}
		return domain.NewError(code, operation, "context", "operation context is not active", err)
	}
	return nil
}

type StopMode string

const (
	StopGraceful  StopMode = "graceful"
	StopImmediate StopMode = "immediate"
	StopForce     StopMode = "force"
)

func (m StopMode) IsValid() bool {
	return m == StopGraceful || m == StopImmediate || m == StopForce
}

type ResetMode string

const (
	ResetRecreate ResetMode = "recreate"
	ResetBaseline ResetMode = "baseline"
	ResetSnapshot ResetMode = "snapshot"
)

func (m ResetMode) IsValid() bool {
	return m == ResetRecreate || m == ResetBaseline || m == ResetSnapshot
}

// ValidateResetSelection guarantees that a reset mode and its optional
// snapshot selector form one unambiguous driver request.
func ValidateResetSelection(mode ResetMode, snapshotName string) error {
	const operation = "ports.validate_reset_selection"
	if !mode.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "mode", "must be baseline, recreate, or snapshot", nil)
	}
	if mode == ResetSnapshot {
		if strings.TrimSpace(snapshotName) == "" {
			return domain.NewError(domain.CodeInvalidArgument, operation, "snapshot_name", "is required for snapshot reset", nil)
		}
		if snapshotName != strings.TrimSpace(snapshotName) {
			return domain.NewError(domain.CodeInvalidArgument, operation, "snapshot_name", "must not have surrounding whitespace", nil)
		}
		return nil
	}
	if snapshotName != "" {
		return domain.NewError(domain.CodeInvalidArgument, operation, "snapshot_name", "is only valid for snapshot reset", nil)
	}
	return nil
}

func requireIdempotency(operation, value string) error {
	if !domain.IsCanonicalIdempotencyKey(value) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "idempotency_key", "must be a canonical non-blank value of at most 1024 bytes", nil)
	}
	return nil
}
