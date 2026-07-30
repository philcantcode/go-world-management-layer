package world

import (
	"fmt"
	"strings"
	"time"
)

// Config is the host-owned Open configuration. Paths, drivers, and the local
// subject replace former daemon flags and network credentials.
type Config struct {
	// Paths holds exclusive control-state and material roots.
	Paths LocalPaths

	// Subject is the fixed in-process policy subject for all Manager calls.
	// Required. No bearer token or mTLS client identity is accepted.
	Subject Subject

	// DeploymentProfile is the absolute path to an immutable version-3
	// deployment profile, or empty for logical-only composition.
	DeploymentProfile string

	// Drivers selects physical composition (none | docker/directory/process/…).
	Drivers DriverConfig

	// Bounds and timeouts previously exposed as WORLD_* daemon flags.
	// Zero values receive production defaults at Open.
	ControlTimeout         time.Duration
	ReconciliationInterval time.Duration
	ReconciliationTimeout  time.Duration
	ShutdownTimeout        time.Duration
	MaxTransferBytes       int64
	MaxExecBytes           int64
	MaxADBBytes            int64
	MaxBundleBytes         int64
	MaxCaptureRecords      int
	AllowRemoteADB         bool
	ProbeTimeout           time.Duration

	// DefaultTimeout bounds unary Manager calls when the parent context has no
	// tighter deadline. Zero means 30s.
	DefaultTimeout time.Duration
}

// LocalPaths holds exclusive control-state and material roots for one Open.
type LocalPaths struct {
	StatePath              string // SQLite control DB; processlock.Acquire target
	LedgerDirectory        string
	OrchestrationStateRoot string
	BundleRoot             string
	MaterialRoot           string
}

// Subject is the fixed policy identity installed for every Manager call.
type Subject struct {
	// Name is the policy owner subject (formerly WORLD_BEARER_SUBJECT /
	// mTLS CN). Default conceptual value for operator CLIs: "local-operator".
	Name string
	// Role optionally restricts which transitions the Manager may invoke
	// (operator vs internal). Empty defaults to RoleOperator.
	Role SubjectRole
}

// SubjectRole distinguishes operator-scoped host ops from former trusted-node
// transitions that still require RoleInternal.
type SubjectRole string

const (
	// RoleOperator is the default: lease-scoped host operations.
	RoleOperator SubjectRole = "operator"
	// RoleInternal is required for transitions that formerly demanded a
	// trusted-node subject (generation/exec node reports).
	RoleInternal SubjectRole = "internal"
)

// DriverConfig selects physical composition drivers. Empty string fields
// default to "none" (or "local" for MaterialDriver) at Open.
type DriverConfig struct {
	AgentDriver     string // none | docker
	LinuxTarget     string // none | docker
	AndroidTarget   string // none | android-emulator
	WorkspaceDriver string // none | directory
	MaterialDriver  string // none | local (default local)
	ObserverDriver  string // none | process
	CaptureDriver   string // none | ledger

	DockerBinary         string
	AgentWorkspaceRoot   string
	AgentImageRepository string
	AgentGuestBinary     string
	AgentContainerUser   string

	TargetRoot            string
	TargetImageRepository string
	TargetAllowPtrace     bool

	AndroidTargetRoot       string
	AndroidSystemImageRoot  string
	AndroidADBBinary        string
	AndroidADBServer        string
	AndroidEmulatorBinary   string
	AndroidSDKRoot          string
	AndroidSDKManagerBinary string
	AndroidAVDManagerBinary string
	AndroidADBBasePort      int
	AndroidBackendVersion   string
	AndroidRuntimeVersion   string

	ObserverOutputRoot string
	CaptureRoot        string
}

func (c Config) validate() error {
	if strings.TrimSpace(c.Paths.StatePath) == "" {
		return fmt.Errorf("paths.state_path is required")
	}
	if strings.TrimSpace(c.Paths.LedgerDirectory) == "" {
		return fmt.Errorf("paths.ledger_directory is required")
	}
	if strings.TrimSpace(c.Paths.OrchestrationStateRoot) == "" {
		return fmt.Errorf("paths.orchestration_state_root is required")
	}
	if strings.TrimSpace(c.Paths.BundleRoot) == "" {
		return fmt.Errorf("paths.bundle_root is required")
	}
	if strings.TrimSpace(c.Paths.MaterialRoot) == "" {
		return fmt.Errorf("paths.material_root is required")
	}
	if strings.TrimSpace(c.Subject.Name) == "" {
		return fmt.Errorf("subject.name is required")
	}
	switch c.Subject.Role {
	case "", RoleOperator, RoleInternal:
	default:
		return fmt.Errorf("subject.role %q is invalid; allowed values: %q, %q", c.Subject.Role, RoleOperator, RoleInternal)
	}
	return nil
}

func (c Config) effectiveRole() SubjectRole {
	if c.Subject.Role == "" {
		return RoleOperator
	}
	return c.Subject.Role
}
