package ports

import (
	"context"
	"time"
)

// PhysicalSupport states whether a fact is enforced and independently
// observable by the selected physical backend. Unsupported is a known backend
// limitation; unknown means the backend could not prove the fact.
type PhysicalSupport string

const (
	PhysicalSupportEnforced    PhysicalSupport = "enforced"
	PhysicalSupportUnsupported PhysicalSupport = "unsupported"
	PhysicalSupportUnknown     PhysicalSupport = "unknown"
)

func (s PhysicalSupport) IsValid() bool {
	return s == PhysicalSupportEnforced || s == PhysicalSupportUnsupported || s == PhysicalSupportUnknown
}

type PhysicalLimitFact struct {
	Value   int64           `json:"value"`
	Support PhysicalSupport `json:"support"`
	Detail  string          `json:"detail,omitempty"`
}

func (f PhysicalLimitFact) Enforced() bool { return f.Support == PhysicalSupportEnforced }

// ContainerRuntimePhysicalFacts uses the same field semantics as physical
// policy admission. ImageDigest is empty for a config-level report and exact
// for a plan-level report.
type ContainerRuntimePhysicalFacts struct {
	Driver                 string          `json:"driver"`
	Runtime                string          `json:"runtime,omitempty"`
	ImageDigest            string          `json:"image_digest,omitempty"`
	IsolationProfile       string          `json:"isolation_profile"`
	RootFilesystem         string          `json:"root_filesystem"`
	BaseImage              string          `json:"base_image,omitempty"`
	User                   string          `json:"user"`
	CapabilityDrop         []string        `json:"capability_drop"`
	CapabilityAdd          []string        `json:"capability_add"`
	NoNewPrivileges        bool            `json:"no_new_privileges"`
	SeccompProfile         string          `json:"seccomp_profile"`
	UserEnforced           bool            `json:"user_enforced"`
	SeccompEnforced        bool            `json:"seccomp_enforced"`
	CapabilitySupport      PhysicalSupport `json:"capability_support"`
	NoNewPrivilegesSupport PhysicalSupport `json:"no_new_privileges_support"`
	UserSupport            PhysicalSupport `json:"user_support"`
	SeccompSupport         PhysicalSupport `json:"seccomp_support"`
}

type ContainerNetworkPhysicalFacts struct {
	Mode              string          `json:"mode"`
	AllowDNS          bool            `json:"allow_dns"`
	AllowedCIDRs      []string        `json:"allowed_cidrs"`
	AllowedDomains    []string        `json:"allowed_domains"`
	DenyPrivateRanges bool            `json:"deny_private_ranges"`
	TargetAccess      string          `json:"target_access"`
	Support           PhysicalSupport `json:"support"`
}

type ContainerResourcePhysicalFacts struct {
	CPUMilli           PhysicalLimitFact `json:"cpu_milli"`
	MemoryBytes        PhysicalLimitFact `json:"memory_bytes"`
	SwapBytes          PhysicalLimitFact `json:"swap_bytes"`
	WorkspaceBytes     PhysicalLimitFact `json:"workspace_bytes"`
	WritableStateBytes PhysicalLimitFact `json:"writable_state_bytes"`
	CaptureBytes       PhysicalLimitFact `json:"capture_bytes"`
	Inodes             PhysicalLimitFact `json:"inodes"`
	PIDs               PhysicalLimitFact `json:"pids"`
}

// AndroidRuntimePhysicalFacts records the Android-specific controls that are
// absent from a container runtime. SystemImageDigest is empty in a
// configuration-level report and exact in a plan-level report.
type AndroidRuntimePhysicalFacts struct {
	SystemImageDigest           string          `json:"system_image_digest,omitempty"`
	BaselineState               string          `json:"baseline_state,omitempty"`
	HardwareAcceleration        bool            `json:"hardware_acceleration"`
	HardwareAccelerationSupport PhysicalSupport `json:"hardware_acceleration_support,omitempty"`
	Headless                    bool            `json:"headless"`
	Rooted                      bool            `json:"rooted"`
	Debuggable                  bool            `json:"debuggable"`
	GuestMemoryBytes            int64           `json:"guest_memory_bytes"`
	BootTimeout                 time.Duration   `json:"boot_timeout"`
}

type AgentWorkspacePhysicalPolicyReport struct {
	Runtime   ContainerRuntimePhysicalFacts  `json:"runtime"`
	Network   ContainerNetworkPhysicalFacts  `json:"network"`
	Resources ContainerResourcePhysicalFacts `json:"resources"`
}

type TargetPhysicalPolicyReport struct {
	Template                      string                         `json:"template"`
	Kind                          string                         `json:"kind"`
	Runtime                       ContainerRuntimePhysicalFacts  `json:"runtime"`
	MaterialMountPoint            string                         `json:"material_mount_point"`
	WritableStateMode             string                         `json:"writable_state_mode"`
	WritableStateEnforced         bool                           `json:"writable_state_enforced"`
	CommandAuthority              string                         `json:"command_authority"`
	ExecTransport                 string                         `json:"exec_transport"`
	FileTransfer                  string                         `json:"file_transfer"`
	NetworkEndpoints              string                         `json:"network_endpoints"`
	ADB                           string                         `json:"adb,omitempty"`
	DeviceScopedADBServices       string                         `json:"device_scoped_adb_services,omitempty"`
	DeniedInfrastructureAuthority []string                       `json:"denied_infrastructure_authority"`
	ResetAfterEveryRun            bool                           `json:"reset_after_every_run"`
	ResetMode                     string                         `json:"reset_mode"`
	InteractionSupport            PhysicalSupport                `json:"interaction_support"`
	ResetSupport                  PhysicalSupport                `json:"reset_support"`
	Resources                     ContainerResourcePhysicalFacts `json:"resources"`
	Android                       AndroidRuntimePhysicalFacts    `json:"android,omitempty"`
}

// AgentWorkspacePhysicalPolicyReporter is optional. The config-level method
// publishes immutable backend facts before IDs exist; the plan-level method
// binds exact image and quota facts before mutation.
type AgentWorkspacePhysicalPolicyReporter interface {
	AgentWorkspacePhysicalPolicy(context.Context) (AgentWorkspacePhysicalPolicyReport, error)
	AgentWorkspacePlanPhysicalPolicy(context.Context, AgentWorkspacePlan) (AgentWorkspacePhysicalPolicyReport, error)
}

// TargetPhysicalPolicyReporter provides the equivalent two-stage report for a
// selected target template and its later bound plan.
type TargetPhysicalPolicyReporter interface {
	TargetPhysicalPolicy(context.Context, TargetTemplate) (TargetPhysicalPolicyReport, error)
	TargetPlanPhysicalPolicy(context.Context, TargetPlan) (TargetPhysicalPolicyReport, error)
}
