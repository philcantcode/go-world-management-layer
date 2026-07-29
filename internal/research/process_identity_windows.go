//go:build windows

package research

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// processStartNSToleranceWindows allows skew between lifecycle ProcessStartNS
// and WMIC/CIM CreationDate (second resolution, timezone quirks).
const processStartNSToleranceWindows = 5 * time.Second

func processExists(pid int64) (bool, error) {
	name, alive, err := windowsProcessName(context.Background(), pid)
	_ = name
	return alive, err
}

func verifyProcessIdentityPlatform(ctx context.Context, pid, parentPID, processStartNS int64) processIdentityStatus {
	if err := ctx.Err(); err != nil {
		return processIdentityStatus{Reason: ReasonHostCaptureFailed}
	}
	_, alive, err := windowsProcessName(ctx, pid)
	if err != nil {
		return processIdentityStatus{Alive: false, Verified: false, Reason: ReasonHostUnavailable}
	}
	if !alive {
		return processIdentityStatus{Alive: false, Verified: true, Reason: "process_exited_before_capture"}
	}
	_ = parentPID
	creation, createNS, createErr := windowsProcessCreationNS(ctx, pid)
	if createErr != nil || createNS <= 0 {
		// Creation time unavailable: refuse live attach rather than discard ProcessStartNS.
		return processIdentityStatus{
			Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch, KernelStartTime: creation,
		}
	}
	delta := math.Abs(float64(createNS - processStartNS))
	if delta > float64(processStartNSToleranceWindows.Nanoseconds()) {
		return processIdentityStatus{
			Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch, KernelStartTime: creation,
		}
	}
	return processIdentityStatus{Alive: true, Verified: true, KernelStartTime: creation}
}

func windowsProcessCreation(ctx context.Context, pid int64) string {
	marker, _, _ := windowsProcessCreationNS(ctx, pid)
	return marker
}

func windowsProcessCreationNS(ctx context.Context, pid int64) (marker string, unixNS int64, err error) {
	cmd := exec.CommandContext(ctx, "wmic", "process", "where", fmt.Sprintf("ProcessId=%d", pid), "get", "CreationDate", "/value")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	out, err := cmd.Output()
	if err != nil {
		return "", 0, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "CreationDate=") {
			continue
		}
		raw := strings.TrimPrefix(line, "CreationDate=")
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		ns, parseErr := parseWmicCreationDate(raw)
		if parseErr != nil {
			return boundText(raw, 64), 0, parseErr
		}
		return boundText(raw, 64), ns, nil
	}
	return "", 0, fmt.Errorf("creation date missing")
}

// parseWmicCreationDate parses yyyyMMddHHmmss.mmmmmm±UUU where UUU is offset minutes.
func parseWmicCreationDate(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if len(value) < 14 {
		return 0, fmt.Errorf("short creation date")
	}
	base := value[:14]
	t, err := time.ParseInLocation("20060102150405", base, time.Local)
	if err != nil {
		return 0, err
	}
	// Optional fractional seconds and timezone offset in minutes.
	rest := value[14:]
	if len(rest) > 0 && rest[0] == '.' {
		// Skip .mmmmmm
		i := 1
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		// Offset like -420 or +000
		if i < len(rest) && (rest[i] == '+' || rest[i] == '-') {
			sign := 1
			if rest[i] == '-' {
				sign = -1
			}
			if mins, err := strconv.Atoi(rest[i+1:]); err == nil {
				t = t.Add(time.Duration(-sign*mins) * time.Minute)
				// WMIC local time with offset: convert toward UTC interpretation.
				// CreationDate is local wall time with bias; applying bias yields UTC-ish.
				_ = mins
			}
		}
	}
	return t.UnixNano(), nil
}
