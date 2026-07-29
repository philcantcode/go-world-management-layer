//go:build !linux && !windows

package research

import (
	"context"
)

func captureProcessEvidence(ctx context.Context, pid, parentHint int64, identity processIdentityStatus) (ProcessEvidence, []SocketEvidence, []string, error) {
	if err := ctx.Err(); err != nil {
		return ProcessEvidence{}, nil, nil, err
	}
	_ = identity
	return ProcessEvidence{
		PID:       pid,
		ParentPID: parentHint,
		Alive:     false,
		Status:    "unsupported_platform",
	}, nil, []string{"process_detail_unsupported_on_platform"}, nil
}
