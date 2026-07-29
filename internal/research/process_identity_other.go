//go:build !linux && !windows

package research

import "context"

func processExists(pid int64) (bool, error) {
	return false, nil
}

func verifyProcessIdentityPlatform(ctx context.Context, pid, parentPID, processStartNS int64) processIdentityStatus {
	if err := ctx.Err(); err != nil {
		return processIdentityStatus{Reason: ReasonHostCaptureFailed}
	}
	_ = pid
	_ = parentPID
	_ = processStartNS
	// No OS process APIs — treat as unverified for live detail; lifecycle PID only.
	return processIdentityStatus{Alive: false, Verified: true, Reason: "process_detail_unsupported_on_platform"}
}
