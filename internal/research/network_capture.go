package research

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// NetworkCaptureOptions configures packet/connection capture.
type NetworkCaptureOptions struct {
	LookPath     func(file string) (string, error)
	MaxPcapBytes int64
	DisablePcap  bool
}

// NetworkCaptureCollector spans the action window: Start may launch dumpcap/
// tcpdump when available; CaptureAfter prefers PID-attributed connection tables
// and only claims pcap when a real capture file was produced for this action.
type NetworkCaptureCollector struct {
	opts NetworkCaptureOptions

	mu         sync.Mutex
	started    bool
	cmd        *exec.Cmd
	pcapPath   string
	pcapTool   string
	startErr   string
	ambient    LocalNetworkCollector
	beginIndex NetworkIndex
}

// NewNetworkCaptureCollector builds a network_capture companion.
func NewNetworkCaptureCollector(opts NetworkCaptureOptions) *NetworkCaptureCollector {
	if opts.LookPath == nil {
		opts.LookPath = exec.LookPath
	}
	opts.MaxPcapBytes = boundedByteLimit(opts.MaxPcapBytes, defaultMaxPcapBytes, maximumMaxPcapBytes)
	return &NetworkCaptureCollector{opts: opts, ambient: LocalNetworkCollector{}}
}

func (c *NetworkCaptureCollector) Role() CompanionRole { return CompanionNetworkCapture }

// Start begins best-effort packet capture under the action network directory.
// Capture lifetime is owned by Stop/Seal — the command is not bound to Begin's
// cancelable context.
func (c *NetworkCaptureCollector) Start(ctx context.Context, start ActionStart, actionDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	c.started = true
	if c.opts.DisablePcap {
		c.startErr = ReasonNetworkToolMissing
		return nil
	}
	networkDir := filepath.Join(actionDir, "network")
	if err := os.MkdirAll(networkDir, 0o700); err != nil {
		c.startErr = ReasonNetworkCaptureFailed
		return nil
	}
	tool, args, path, ok := c.resolvePcapCommand(networkDir)
	if !ok {
		c.startErr = ReasonNetworkToolMissing
		return nil
	}
	// Pre-create 0600 so the capture file is never world-readable during write.
	if handle, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); err == nil {
		_ = handle.Close()
	} else {
		c.startErr = ReasonNetworkCaptureFailed
		return nil
	}
	// Detach from Begin cancel so a cancelled Begin ctx does not kill dumpcap.
	// Stop/Seal remain the only teardown path.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), tool, args...)
	if err := cmd.Start(); err != nil {
		c.startErr = ReasonNetworkCaptureFailed
		return nil
	}
	c.cmd = cmd
	c.pcapPath = path
	c.pcapTool = filepath.Base(tool)
	return nil
}

// Stop terminates any running packet capture process (idempotent).
func (c *NetworkCaptureCollector) Stop(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	proc := c.cmd.Process
	// Windows does not deliver Interrupt reliably to console-less children.
	if runtime.GOOS == "windows" {
		_ = proc.Kill()
	} else {
		_ = proc.Signal(os.Interrupt)
	}
	done := make(chan struct{})
	go func() {
		_ = c.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = proc.Kill()
		<-done
	case <-ctx.Done():
		_ = proc.Kill()
		<-done
	}
	c.cmd = nil
	if c.pcapPath != "" {
		_ = os.Chmod(c.pcapPath, 0o600)
		if info, err := os.Stat(c.pcapPath); err == nil && info.Size() > c.opts.MaxPcapBytes {
			_ = os.Truncate(c.pcapPath, c.opts.MaxPcapBytes)
		}
	}
	return nil
}

// Capture records ambient inventory at Begin (never attributed as semantic).
func (c *NetworkCaptureCollector) Capture(ctx context.Context, start ActionStart) (NetworkIndex, error) {
	index, err := c.ambient.Capture(ctx, start)
	if err != nil {
		return index, err
	}
	index.Attributed = false
	index.CaptureMethod = "ambient"
	c.mu.Lock()
	c.beginIndex = index
	c.mu.Unlock()
	return index, nil
}

// CaptureAfter finalizes action-window network evidence.
func (c *NetworkCaptureCollector) CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (NetworkIndex, error) {
	if err := ctx.Err(); err != nil {
		return NetworkIndex{}, err
	}
	_ = c.Stop(ctx, start, outcome, actionDir)

	c.mu.Lock()
	pcapPath := c.pcapPath
	pcapTool := c.pcapTool
	startErr := c.startErr
	begin := c.beginIndex
	c.mu.Unlock()

	observation := NetworkCaptureObservation{
		ObservedAt: time.Now().UTC(),
		Ambient:    begin.Flows,
	}

	// Prefer PID-attributed connection table when identity is verified.
	if outcome.ProcessID > 0 {
		identity := verifyProcessIdentity(ctx, outcome.ProcessID, outcome.ParentPID, outcome.ProcessStartNS)
		if identity.Verified {
			_, sockets, warnings, err := captureProcessEvidence(ctx, outcome.ProcessID, outcome.ParentPID, identity)
			if err == nil {
				observation.PIDConnections = sockets
				observation.Warnings = append(observation.Warnings, warnings...)
			}
		} else if identity.Reason != "" {
			observation.Warnings = append(observation.Warnings, identity.Reason)
		}
	}

	hasPcap := false
	relPcap := ""
	if pcapPath != "" {
		if info, err := os.Stat(pcapPath); err == nil && info.Size() > 0 {
			hasPcap = true
			relPcap = filepath.ToSlash(filepath.Join("network", filepath.Base(pcapPath)))
			observation.Pcap = &PcapArtifact{
				Tool: pcapTool, Path: relPcap, Bytes: info.Size(), Captured: true,
			}
		} else if startErr == "" {
			startErr = ReasonNetworkCaptureFailed
		}
	}

	hasPIDConns := len(observation.PIDConnections) > 0
	switch {
	case hasPcap && hasPIDConns:
		// Full action attribution: window capture joined to process sockets.
		return NetworkIndex{
			Flows: observation, Scope: "action_process", Available: true, Attributed: true,
			CaptureMethod: "pcap+conn_table", ArtifactPath: relPcap,
		}, nil
	case hasPIDConns:
		return NetworkIndex{
			Flows: observation, Scope: "action_process", Available: true, Attributed: true,
			CaptureMethod: "conn_table",
		}, nil
	case hasPcap:
		// Interface-wide pcap without PID join is window-scoped, not process-attributed.
		return NetworkIndex{
			Flows: observation, Scope: "action_window", Available: true, Attributed: false,
			CaptureMethod: "pcap", ArtifactPath: relPcap, Reason: ReasonNetworkWindowUnjoined,
		}, nil
	case begin.Available:
		reason := startErr
		if reason == "" {
			reason = ReasonNetworkNotAttributed
		}
		observation.Warnings = append(observation.Warnings, reason)
		return NetworkIndex{
			Flows: observation, Scope: "collector_host", Available: true, Attributed: false,
			CaptureMethod: "ambient", Reason: reason,
		}, nil
	default:
		reason := startErr
		if reason == "" {
			reason = ReasonNetworkUnavailable
		}
		return NetworkIndex{Available: false, Attributed: false, Reason: reason, CaptureMethod: "none"}, nil
	}
}

func (c *NetworkCaptureCollector) resolvePcapCommand(networkDir string) (tool string, args []string, path string, ok bool) {
	path = filepath.Join(networkDir, "packets.pcap")
	for _, name := range []string{"dumpcap", "tcpdump"} {
		resolved, err := c.opts.LookPath(name)
		if err != nil || resolved == "" {
			continue
		}
		switch name {
		case "dumpcap":
			kb := c.opts.MaxPcapBytes / 1024
			if kb < 64 {
				kb = 64
			}
			return resolved, []string{"-q", "-a", fmt.Sprintf("filesize:%d", kb), "-w", path}, path, true
		case "tcpdump":
			// -C is file size in millions of bytes; -W 1 stops after one file.
			// -s 128 bounds snaplen; packet count is a secondary backstop.
			mb := c.opts.MaxPcapBytes / 1_000_000
			if mb < 1 {
				mb = 1
			}
			// ~200 byte average * N ≈ MaxPcapBytes as a soft packet cap.
			packets := c.opts.MaxPcapBytes / 200
			if packets < 100 {
				packets = 100
			}
			if packets > 50000 {
				packets = 50000
			}
			return resolved, []string{
				"-U", "-s", "128",
				"-C", fmt.Sprintf("%d", mb),
				"-W", "1",
				"-c", fmt.Sprintf("%d", packets),
				"-w", path,
			}, path, true
		}
	}
	return "", nil, "", false
}

// NetworkCaptureObservation is the structured network_capture payload.
type NetworkCaptureObservation struct {
	ObservedAt     time.Time        `json:"observed_at"`
	Pcap           *PcapArtifact    `json:"pcap,omitempty"`
	PIDConnections []SocketEvidence `json:"pid_connections,omitempty"`
	Ambient        any              `json:"ambient,omitempty"`
	Warnings       []string         `json:"warnings,omitempty"`
}

// PcapArtifact references a packet capture under the action directory.
type PcapArtifact struct {
	Tool     string `json:"tool,omitempty"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Captured bool   `json:"captured"`
}

var (
	_ NetworkCollector      = (*NetworkCaptureCollector)(nil)
	_ AfterNetworkCollector = (*NetworkCaptureCollector)(nil)
	_ StartableCollector    = (*NetworkCaptureCollector)(nil)
)
