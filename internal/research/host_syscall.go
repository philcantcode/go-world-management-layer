package research

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// HostSyscallOptions configures syscall boundary capture.
type HostSyscallOptions struct {
	LookPath        func(file string) (string, error)
	MaxSyscallBytes int64
	Disable         bool
}

// hostSyscallCollector records syscall-boundary evidence when platform tools
// are available. Without attachable tools it seals an explicit gap.
type hostSyscallCollector struct {
	opts HostSyscallOptions

	mu        sync.Mutex
	started   bool
	cmd       *exec.Cmd
	tracePath string
	method    string
	startErr  string
}

// NewHostSyscallCollector builds a host_syscall companion.
func NewHostSyscallCollector(opts HostSyscallOptions) SyscallCollector {
	if opts.LookPath == nil {
		opts.LookPath = exec.LookPath
	}
	opts.MaxSyscallBytes = boundedByteLimit(opts.MaxSyscallBytes, defaultMaxSyscallBytes, maximumMaxSyscallBytes)
	return &hostSyscallCollector{opts: opts}
}

func (c *hostSyscallCollector) Role() CompanionRole { return CompanionHostSyscall }

// Start prepares the syscall artifact directory. Actual attach usually needs a
// PID and therefore happens in CaptureAfter; Start only probes capability.
func (c *hostSyscallCollector) Start(ctx context.Context, start ActionStart, actionDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	c.started = true
	if err := os.MkdirAll(filepath.Join(actionDir, "host"), 0o700); err != nil {
		c.startErr = ReasonSyscallCaptureFailed
		return nil
	}
	if c.opts.Disable {
		c.startErr = ReasonSyscallToolMissing
		return nil
	}
	if runtime.GOOS == "linux" {
		if path, err := c.opts.LookPath("strace"); err == nil && path != "" {
			c.method = "strace"
			c.tracePath = filepath.Join(actionDir, "host", "syscalls.strace")
			return nil
		}
		c.startErr = ReasonSyscallToolMissing
		return nil
	}
	// Windows/other: no non-admin lightweight attach in MVP.
	c.startErr = ReasonSyscallToolMissing
	c.method = runtime.GOOS + "_unsupported"
	return nil
}

// Stop terminates any attach process started during CaptureAfter.
func (c *hostSyscallCollector) Stop(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Kill()
	_ = c.cmd.Wait()
	c.cmd = nil
	return nil
}

// CaptureAfter attempts a brief strace attach when the process is still alive,
// otherwise records a structured gap with capability reason.
func (c *hostSyscallCollector) CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (SyscallSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return SyscallSnapshot{}, err
	}
	c.mu.Lock()
	method := c.method
	tracePath := c.tracePath
	startErr := c.startErr
	c.mu.Unlock()

	if outcome.ProcessID <= 0 {
		return SyscallSnapshot{
			Available: false, Attributed: false, Reason: ReasonProcessLifecycleMissing, Method: method,
		}, nil
	}
	identity := verifyProcessIdentity(ctx, outcome.ProcessID, outcome.ParentPID, outcome.ProcessStartNS)
	if !identity.Verified || !identity.Alive {
		reason := identity.Reason
		if reason == "" {
			reason = ReasonSyscallUnavailable
		}
		if !identity.Alive && identity.Verified {
			reason = ReasonSyscallUnavailable
		}
		marker := SyscallSnapshot{
			Available: false, Attributed: false, Reason: reason, Method: method, Scope: "action_process",
		}
		_ = writeJSON(filepath.Join(actionDir, "host", "syscalls.json"), marker)
		return marker, nil
	}
	if method != "strace" || tracePath == "" {
		reason := startErr
		if reason == "" {
			reason = ReasonSyscallToolMissing
		}
		marker := SyscallSnapshot{
			Available: false, Attributed: false, Reason: reason, Method: method,
			Scope: "action_process",
		}
		_ = writeJSON(filepath.Join(actionDir, "host", "syscalls.json"), marker)
		return marker, nil
	}

	// Brief attach only after identity verification.
	stracePath, err := c.opts.LookPath("strace")
	if err != nil || stracePath == "" {
		snap := SyscallSnapshot{Available: false, Attributed: false, Reason: ReasonSyscallToolMissing, Method: "strace"}
		_ = writeJSON(filepath.Join(actionDir, "host", "syscalls.json"), snap)
		return snap, nil
	}
	attachCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(attachCtx, stracePath, "-p", formatInt64(outcome.ProcessID), "-o", tracePath, "-qq")
	_ = cmd.Start()
	c.mu.Lock()
	c.cmd = cmd
	c.mu.Unlock()
	_ = cmd.Wait()
	c.mu.Lock()
	c.cmd = nil
	c.mu.Unlock()

	info, err := os.Stat(tracePath)
	if err != nil || info.Size() == 0 {
		snap := SyscallSnapshot{
			Available: false, Attributed: false, Reason: ReasonSyscallUnavailable, Method: "strace",
			Scope: "action_process",
		}
		_ = writeJSON(filepath.Join(actionDir, "host", "syscalls.json"), snap)
		return snap, nil
	}
	if info.Size() > c.opts.MaxSyscallBytes {
		_ = os.Truncate(tracePath, c.opts.MaxSyscallBytes)
		info, _ = os.Stat(tracePath)
	}
	_ = os.Chmod(tracePath, 0o600)
	rel := filepath.ToSlash(filepath.Join("host", "syscalls.strace"))
	events := map[string]any{
		"bytes": info.Size(),
		"pid":   outcome.ProcessID,
		"note":  "brief_post_start_attach",
	}
	snap := SyscallSnapshot{
		Events: events, Scope: "action_process", Available: true, Attributed: true,
		Method: "strace", ArtifactPath: rel,
	}
	if info.Size() >= c.opts.MaxSyscallBytes {
		snap.Truncated = true
	}
	_ = writeJSON(filepath.Join(actionDir, "host", "syscalls.json"), snap)
	return snap, nil
}

func formatInt64(v int64) string {
	// Avoid importing strconv solely for this small helper in hot paths â€” use fmt via small local.
	return strconvFormatInt(v)
}

// strconvFormatInt is a tiny indirection used by host_syscall.
func strconvFormatInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

var (
	_ SyscallCollector   = (*hostSyscallCollector)(nil)
	_ StartableCollector = (*hostSyscallCollector)(nil)
)
