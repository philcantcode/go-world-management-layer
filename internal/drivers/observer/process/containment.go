package process

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

// collectorContainment is implemented by platform processes whose direct
// process handle is not sufficient proof that their complete process tree is
// gone. The authority stays open until ConfirmCleanup proves that the tree is
// empty; only then may CloseContainment release it.
type collectorContainment interface {
	ConfirmCleanup(context.Context) (bool, error)
	CloseContainment() error
}

func hasCollectorContainment(process command.Process) bool {
	_, ok := process.(collectorContainment)
	return ok
}

// confirmAndCloseCollectorContainment is the one successful-release boundary
// for an owned collector tree. A failed or timed-out proof deliberately keeps
// the authority open so a later Stop retry can still terminate and inspect it.
func confirmAndCloseCollectorContainment(ctx context.Context, process command.Process) (bool, error) {
	authority, ok := process.(collectorContainment)
	if !ok {
		return true, nil
	}
	confirmed, err := authority.ConfirmCleanup(ctx)
	if err != nil {
		return false, fmt.Errorf("confirm collector process-tree cleanup: %w", err)
	}
	if !confirmed {
		return false, nil
	}
	if err := authority.CloseContainment(); err != nil {
		return false, fmt.Errorf("close collector process-tree authority: %w", err)
	}
	return true, nil
}
