package research

import (
	"context"
	"time"
)

// HostProcessCollector captures ambient collector identity at Begin and
// PID-attributed process/socket evidence at Seal when ProcessID is known.
type HostProcessCollector struct {
	ambient LocalHostCollector
}

// NewHostProcessCollector returns a host_process companion collector.
func NewHostProcessCollector() *HostProcessCollector {
	return &HostProcessCollector{}
}

// Capture records ambient collector/action identity before launch.
func (c *HostProcessCollector) Capture(ctx context.Context, start ActionStart) (HostSnapshot, error) {
	snap, err := c.ambient.Capture(ctx, start)
	if err != nil {
		return snap, err
	}
	// Ambient pre-start snapshot is available but not action-process attributed.
	snap.Attributed = false
	if snap.Scope == "" {
		snap.Scope = "collector_process"
	}
	return snap, nil
}

// CaptureAfter records process tree / sockets for the lifecycle PID when the OS
// allows. Live detail (cmdline/sockets) requires ProcessStartNS verification to
// reduce PID-reuse risk. Failure is an unavailable snapshot, never a hard error.
func (c *HostProcessCollector) CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return HostSnapshot{}, err
	}
	if outcome.ProcessID <= 0 {
		return HostSnapshot{
			Available: false, Attributed: false, Scope: "action_process",
			Reason: ReasonProcessLifecycleMissing,
		}, nil
	}
	identity := verifyProcessIdentity(ctx, outcome.ProcessID, outcome.ParentPID, outcome.ProcessStartNS)
	if !identity.Verified {
		return HostSnapshot{
			Available: false, Attributed: false, Scope: "action_process",
			Reason: reasonOr(identity.Reason, ReasonProcessIdentityMismatch),
		}, nil
	}

	tree, sockets, warnings, err := captureProcessEvidence(ctx, outcome.ProcessID, outcome.ParentPID, identity)
	if err != nil {
		return HostSnapshot{
			Available: false, Attributed: false, Scope: "action_process",
			Reason: ReasonHostUnavailable,
		}, nil
	}
	if identity.KernelStartTime != "" {
		tree.StartTime = identity.KernelStartTime
	}
	payload := AttributedHostProcess{
		ObservedAt:     time.Now().UTC(),
		PID:            outcome.ProcessID,
		ParentPID:      outcome.ParentPID,
		ProcessStartNS: outcome.ProcessStartNS,
		Process:        tree,
		Warnings:       warnings,
		Action: LocalActionIdentity{
			ActionID: start.ActionID, Scope: start.Scope,
			Executable:       boundText(start.Executable, maximumEvidenceTextBytes),
			WorkingDirectory: boundText(start.WorkingDirectory, maximumEvidenceTextBytes),
			ArgumentCount:    len(start.Argv),
			EnvironmentKeys:  len(start.EnvironmentKeys),
		},
	}
	return HostSnapshot{
		ProcessTree: payload,
		Sockets:     sockets,
		Scope:       "action_process",
		Available:   true,
		Attributed:  true,
	}, nil
}

// AttributedHostProcess is the typed payload for PID-linked host evidence.
type AttributedHostProcess struct {
	ObservedAt     time.Time           `json:"observed_at"`
	PID            int64               `json:"pid"`
	ParentPID      int64               `json:"parent_pid,omitempty"`
	ProcessStartNS int64               `json:"process_start_ns,omitempty"`
	Process        ProcessEvidence     `json:"process"`
	Warnings       []string            `json:"warnings,omitempty"`
	Action         LocalActionIdentity `json:"action"`
}

// ProcessEvidence is a cross-platform process snapshot.
// CommandLine may contain secrets; agent-facing MCP surfaces redact it.
type ProcessEvidence struct {
	PID              int64   `json:"pid"`
	ParentPID        int64   `json:"parent_pid,omitempty"`
	Executable       string  `json:"executable,omitempty"`
	CommandLine      string  `json:"command_line,omitempty"`
	WorkingDirectory string  `json:"working_directory,omitempty"`
	Status           string  `json:"status,omitempty"`
	StartTime        string  `json:"start_time,omitempty"`
	Children         []int64 `json:"children,omitempty"`
	Alive            bool    `json:"alive"`
}

// SocketEvidence is a single socket/connection attributed to a process.
type SocketEvidence struct {
	Protocol      string `json:"protocol"`
	LocalAddress  string `json:"local_address,omitempty"`
	RemoteAddress string `json:"remote_address,omitempty"`
	State         string `json:"state,omitempty"`
	PID           int64  `json:"pid,omitempty"`
}

var (
	_ HostCollector      = (*HostProcessCollector)(nil)
	_ AfterHostCollector = (*HostProcessCollector)(nil)
)
