package research

import (
	"context"
)

// processIdentityStatus is the result of verifying a lifecycle PID before
// reading process-sensitive data (cmdline, sockets, strace attach).
type processIdentityStatus struct {
	Alive           bool
	Verified        bool
	Reason          string
	KernelStartTime string // platform-specific start marker when available
}

// verifyProcessIdentity checks that the live OS process still matches the
// sealed lifecycle identity. ProcessStartNS is required for live attach to
// reduce PID-reuse TOCTOU. A missing process (already exited) is verified for
// the purpose of recording the lifecycle PID without reading a reused process.
func verifyProcessIdentity(ctx context.Context, pid, parentPID, processStartNS int64) processIdentityStatus {
	if err := ctx.Err(); err != nil {
		return processIdentityStatus{Reason: ReasonHostCaptureFailed}
	}
	if pid <= 0 {
		return processIdentityStatus{Reason: ReasonProcessLifecycleMissing}
	}
	if processStartNS <= 0 {
		// Without start identity we refuse live detail capture (reuse risk).
		alive, _ := processExists(pid)
		if !alive {
			// Exited and no start ns: still safe — nothing to mis-attribute.
			return processIdentityStatus{Alive: false, Verified: true, Reason: "process_exited_before_capture"}
		}
		return processIdentityStatus{Alive: true, Verified: false, Reason: ReasonProcessStartNSMissing}
	}
	return verifyProcessIdentityPlatform(ctx, pid, parentPID, processStartNS)
}
