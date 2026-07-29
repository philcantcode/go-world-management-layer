package research

import (
	"context"
	"time"
)

// HostSnapshot is a best-effort host observation around an action.
type HostSnapshot struct {
	ProcessTree any    `json:"process_tree,omitempty"`
	Sockets     any    `json:"sockets,omitempty"`
	Scope       string `json:"scope,omitempty"`
	Available   bool   `json:"available"`
	// Attributed is true when the snapshot is linked to the action process
	// (lifecycle PID), not merely the collector ambient identity.
	Attributed bool   `json:"attributed,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// NetworkIndex is a best-effort network observation index.
// Full pcap paths (when present) live under the action network/ directory and
// are referenced from the structured payload; Available+Attributed must not be
// claimed for ambient interface inventory alone.
type NetworkIndex struct {
	Flows      any    `json:"flows,omitempty"`
	Scope      string `json:"scope,omitempty"`
	Available  bool   `json:"available"`
	Attributed bool   `json:"attributed"`
	Reason     string `json:"reason,omitempty"`
	// CaptureMethod records how the observation was obtained (pcap, conn_table, ambient).
	CaptureMethod string `json:"capture_method,omitempty"`
	// ArtifactPath is a path relative to the action directory when a capture file exists.
	ArtifactPath string `json:"artifact_path,omitempty"`
}

// StateSnapshot is a scoped before/after state capture.
type StateSnapshot struct {
	Root       string         `json:"root,omitempty"`
	Scope      string         `json:"scope,omitempty"`
	Paths      []string       `json:"paths,omitempty"`
	Entries    map[string]any `json:"entries,omitempty"`
	CapturedAt time.Time      `json:"captured_at,omitempty"`
	Available  bool           `json:"available"`
	Attributed bool           `json:"attributed"`
	Truncated  bool           `json:"truncated,omitempty"`
	Reason     string         `json:"reason,omitempty"`
}

// StateDiff is the derived difference when both sides exist.
type StateDiff struct {
	Changed    []string `json:"changed,omitempty"`
	Created    []string `json:"created,omitempty"`
	Modified   []string `json:"modified,omitempty"`
	Deleted    []string `json:"deleted,omitempty"`
	Available  bool     `json:"available"`
	Attributed bool     `json:"attributed"`
	Truncated  bool     `json:"truncated,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

// SyscallSnapshot is a best-effort host syscall boundary observation.
type SyscallSnapshot struct {
	Events       any    `json:"events,omitempty"`
	Scope        string `json:"scope,omitempty"`
	Available    bool   `json:"available"`
	Attributed   bool   `json:"attributed"`
	Method       string `json:"method,omitempty"`
	ArtifactPath string `json:"artifact_path,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Truncated    bool   `json:"truncated,omitempty"`
}

// StaticContextSnapshot holds binary/static metadata for the action executable.
type StaticContextSnapshot struct {
	Executable   string         `json:"executable,omitempty"`
	FileType     string         `json:"file_type,omitempty"`
	SHA256       string         `json:"sha256,omitempty"`
	Size         int64          `json:"size,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Available    bool           `json:"available"`
	Attributed   bool           `json:"attributed"`
	ArtifactPath string         `json:"artifact_path,omitempty"`
	Reason       string         `json:"reason,omitempty"`
}

// TargetOracleSnapshot is target-side confirmation evidence when configured.
type TargetOracleSnapshot struct {
	Records      any    `json:"records,omitempty"`
	Scope        string `json:"scope,omitempty"`
	Available    bool   `json:"available"`
	Attributed   bool   `json:"attributed"`
	ArtifactPath string `json:"artifact_path,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Truncated    bool   `json:"truncated,omitempty"`
}

// ReplayPackage is a minimal reproducibility package (no secret env values).
type ReplayPackage struct {
	ActionID         string   `json:"action_id"`
	Executable       string   `json:"executable"`
	Argv             []string `json:"argv,omitempty"`
	WorkingDirectory string   `json:"working_directory,omitempty"`
	EnvironmentKeys  []string `json:"environment_keys,omitempty"`
	StdinSHA256      string   `json:"stdin_sha256,omitempty"`
	CaptureRefs      []string `json:"capture_refs,omitempty"`
	StimulusClass    string   `json:"stimulus_class,omitempty"`
	Available        bool     `json:"available"`
	Reason           string   `json:"reason,omitempty"`
}

// NetworkDecodeResult is structured protocol metadata derived from capture or flows.
type NetworkDecodeResult struct {
	Records      any    `json:"records,omitempty"`
	Scope        string `json:"scope,omitempty"`
	Available    bool   `json:"available"`
	Attributed   bool   `json:"attributed"`
	Method       string `json:"method,omitempty"`
	ArtifactPath string `json:"artifact_path,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// HostCollector captures process/socket evidence. Implementations must fail
// closed with an explicit reason rather than inventing data.
type HostCollector interface {
	Capture(ctx context.Context, start ActionStart) (HostSnapshot, error)
}

// AfterHostCollector optionally re-captures after process lifecycle is known.
type AfterHostCollector interface {
	CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (HostSnapshot, error)
}

// NetworkCollector captures network index metadata for the action window.
type NetworkCollector interface {
	Capture(ctx context.Context, start ActionStart) (NetworkIndex, error)
}

// AfterNetworkCollector optionally finalizes network evidence at Seal.
type AfterNetworkCollector interface {
	CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (NetworkIndex, error)
}

// StateCollector captures scoped state before/after when policy allows.
type StateCollector interface {
	CaptureBefore(ctx context.Context, start ActionStart) (StateSnapshot, error)
	CaptureAfter(ctx context.Context, start ActionStart) (StateSnapshot, error)
	Diff(before, after StateSnapshot) StateDiff
}

// SyscallCollector captures syscall-boundary evidence when available.
type SyscallCollector interface {
	// Start begins observation when the platform supports pre-start attach.
	Start(ctx context.Context, start ActionStart, actionDir string) error
	// CaptureAfter finalizes or attaches using the sealed process identity.
	CaptureAfter(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) (SyscallSnapshot, error)
}

// StaticContextCollector gathers binary/static metadata for the executable.
type StaticContextCollector interface {
	Capture(ctx context.Context, start ActionStart, actionDir string) (StaticContextSnapshot, error)
}

// TargetOracleCollector reads configured target-side logs/traces.
type TargetOracleCollector interface {
	Capture(ctx context.Context, start ActionStart, actionDir string) (TargetOracleSnapshot, error)
}

// ReplayCollector writes a minimal replay package for the action.
type ReplayCollector interface {
	Capture(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string, captureRefs []string) (ReplayPackage, error)
}

// NetworkDecodeCollector derives semantic records from capture or flow tables.
type NetworkDecodeCollector interface {
	Decode(ctx context.Context, start ActionStart, network NetworkIndex, actionDir string) (NetworkDecodeResult, error)
}

// StartableCollector spans the action window (Start at Begin, Stop at Seal).
type StartableCollector interface {
	Role() CompanionRole
	Start(ctx context.Context, start ActionStart, actionDir string) error
	Stop(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string) error
}

// FixedHostCollector returns a predetermined snapshot (tests / fakes).
type FixedHostCollector struct {
	Snapshot HostSnapshot
	After    HostSnapshot
	Err      error
	AfterErr error
}

func (c FixedHostCollector) Capture(context.Context, ActionStart) (HostSnapshot, error) {
	return c.Snapshot, c.Err
}

func (c FixedHostCollector) CaptureAfter(context.Context, ActionStart, ActionOutcome, string) (HostSnapshot, error) {
	if c.After.Available || c.After.Reason != "" || c.After.ProcessTree != nil {
		return c.After, c.AfterErr
	}
	return c.Snapshot, c.AfterErr
}

// FixedNetworkCollector returns a predetermined index (tests / fakes).
type FixedNetworkCollector struct {
	Index    NetworkIndex
	After    NetworkIndex
	Err      error
	AfterErr error
}

func (c FixedNetworkCollector) Capture(context.Context, ActionStart) (NetworkIndex, error) {
	return c.Index, c.Err
}

func (c FixedNetworkCollector) CaptureAfter(context.Context, ActionStart, ActionOutcome, string) (NetworkIndex, error) {
	if c.After.Available || c.After.Reason != "" || c.After.Flows != nil {
		return c.After, c.AfterErr
	}
	return c.Index, c.AfterErr
}

// FixedStateCollector returns predetermined before/after snapshots.
type FixedStateCollector struct {
	Before     StateSnapshot
	After      StateSnapshot
	DiffResult StateDiff
	Err        error
}

func (c FixedStateCollector) CaptureBefore(context.Context, ActionStart) (StateSnapshot, error) {
	return c.Before, c.Err
}
func (c FixedStateCollector) CaptureAfter(context.Context, ActionStart) (StateSnapshot, error) {
	return c.After, c.Err
}
func (c FixedStateCollector) Diff(StateSnapshot, StateSnapshot) StateDiff {
	if c.DiffResult.Reason != "" || c.DiffResult.Available || c.DiffResult.Truncated || len(c.DiffResult.Changed) > 0 || len(c.DiffResult.Created) > 0 || len(c.DiffResult.Modified) > 0 || len(c.DiffResult.Deleted) > 0 {
		return c.DiffResult
	}
	return StateDiff{Available: false, Reason: "state diff not computed"}
}

// FixedSyscallCollector returns a predetermined syscall snapshot.
type FixedSyscallCollector struct {
	Snapshot SyscallSnapshot
	Err      error
}

func (c FixedSyscallCollector) Start(context.Context, ActionStart, string) error { return nil }
func (c FixedSyscallCollector) CaptureAfter(context.Context, ActionStart, ActionOutcome, string) (SyscallSnapshot, error) {
	return c.Snapshot, c.Err
}

// FixedStaticContextCollector returns predetermined static context.
type FixedStaticContextCollector struct {
	Snapshot StaticContextSnapshot
	Err      error
}

func (c FixedStaticContextCollector) Capture(context.Context, ActionStart, string) (StaticContextSnapshot, error) {
	return c.Snapshot, c.Err
}

// FixedTargetOracleCollector returns predetermined oracle evidence.
type FixedTargetOracleCollector struct {
	Snapshot TargetOracleSnapshot
	Err      error
}

func (c FixedTargetOracleCollector) Capture(context.Context, ActionStart, string) (TargetOracleSnapshot, error) {
	return c.Snapshot, c.Err
}

// FixedReplayCollector returns a predetermined replay package.
type FixedReplayCollector struct {
	Package ReplayPackage
	Err     error
}

func (c FixedReplayCollector) Capture(context.Context, ActionStart, ActionOutcome, string, []string) (ReplayPackage, error) {
	return c.Package, c.Err
}

// FixedNetworkDecodeCollector returns predetermined decode results.
type FixedNetworkDecodeCollector struct {
	Result NetworkDecodeResult
	Err    error
}

func (c FixedNetworkDecodeCollector) Decode(context.Context, ActionStart, NetworkIndex, string) (NetworkDecodeResult, error) {
	return c.Result, c.Err
}
