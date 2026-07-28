// Package policy defines the public research-session policy contract and its
// strict compiler.
package policy

const (
	// APIVersion is the only policy API version accepted by this compiler.
	APIVersion = "world.philcantcode.dev/v1alpha1"
	// Kind is the only policy kind accepted by this compiler.
	Kind = "ResearchSessionPolicy"
)

// Policy is the source and effective ResearchSessionPolicy document.
//
// Source policies are decoded strictly. Effective policies have defaults
// applied and are returned through EffectivePolicy.Policy as defensive copies.
type Policy struct {
	APIVersion string         `yaml:"apiVersion" json:"apiVersion"`
	Kind       string         `yaml:"kind" json:"kind"`
	Metadata   PolicyMetadata `yaml:"metadata" json:"metadata"`
	Spec       PolicySpec     `yaml:"spec" json:"spec"`
}

type PolicyMetadata struct {
	Name     string `yaml:"name" json:"name"`
	Revision int64  `yaml:"revision" json:"revision"`
}

type PolicySpec struct {
	Lease          LeasePolicy          `yaml:"lease" json:"lease"`
	AgentWorkspace AgentWorkspacePolicy `yaml:"agentWorkspace" json:"agentWorkspace"`
	Workspace      WorkspacePolicy      `yaml:"workspace" json:"workspace"`
	Targets        TargetsPolicy        `yaml:"targets" json:"targets"`
	Resources      AggregateResources   `yaml:"resources" json:"resources"`
	Observation    ObservationPolicy    `yaml:"observation" json:"observation"`
	Incidents      IncidentsPolicy      `yaml:"incidents" json:"incidents"`
	Pressure       PressurePolicy       `yaml:"pressure" json:"pressure"`
}

type LeasePolicy struct {
	TTL             Duration `yaml:"ttl" json:"ttl"`
	Priority        int      `yaml:"priority" json:"priority"`
	Preemptible     bool     `yaml:"preemptible" json:"preemptible"`
	QuiesceDeadline Duration `yaml:"quiesceDeadline" json:"quiesceDeadline"`
}

type AgentWorkspacePolicy struct {
	Runtime   AgentRuntimePolicy   `yaml:"runtime" json:"runtime"`
	Network   AgentNetworkPolicy   `yaml:"network" json:"network"`
	Resources AgentResourcesPolicy `yaml:"resources" json:"resources"`
}

type AgentRuntimePolicy struct {
	Driver           string            `yaml:"driver" json:"driver"`
	Image            string            `yaml:"image" json:"image"`
	IsolationProfile string            `yaml:"isolationProfile" json:"isolationProfile"`
	RootFilesystem   string            `yaml:"rootFilesystem" json:"rootFilesystem"`
	User             string            `yaml:"user" json:"user"`
	Capabilities     LinuxCapabilities `yaml:"capabilities" json:"capabilities"`
	NoNewPrivileges  bool              `yaml:"noNewPrivileges" json:"noNewPrivileges"`
	SeccompProfile   string            `yaml:"seccompProfile" json:"seccompProfile"`
}

type LinuxCapabilities struct {
	Drop []string `yaml:"drop" json:"drop"`
	Add  []string `yaml:"add" json:"add"`
}

type AgentNetworkPolicy struct {
	Mode              string   `yaml:"mode" json:"mode"`
	AllowDNS          bool     `yaml:"allowDNS" json:"allowDNS"`
	AllowedCIDRs      []string `yaml:"allowedCIDRs" json:"allowedCIDRs"`
	AllowedDomains    []string `yaml:"allowedDomains" json:"allowedDomains"`
	DenyPrivateRanges bool     `yaml:"denyPrivateRanges" json:"denyPrivateRanges"`
	TargetAccess      string   `yaml:"targetAccess" json:"targetAccess"`
}

type AgentResourcesPolicy struct {
	Requests AgentResourceRequests `yaml:"requests" json:"requests"`
	Limits   AgentResourceLimits   `yaml:"limits" json:"limits"`
}

type AgentResourceRequests struct {
	CPU       CPUQuantity  `yaml:"cpu" json:"cpu"`
	Memory    ByteQuantity `yaml:"memory" json:"memory"`
	Workspace ByteQuantity `yaml:"workspace" json:"workspace"`
}

type AgentResourceLimits struct {
	CPU             CPUQuantity  `yaml:"cpu" json:"cpu"`
	Memory          ByteQuantity `yaml:"memory" json:"memory"`
	Swap            ByteQuantity `yaml:"swap" json:"swap"`
	PIDs            int64        `yaml:"pids" json:"pids"`
	Workspace       ByteQuantity `yaml:"workspace" json:"workspace"`
	WorkspaceInodes int64        `yaml:"workspaceInodes" json:"workspaceInodes"`
}

type WorkspacePolicy struct {
	Mode      string                `yaml:"mode" json:"mode"`
	InputView InputViewPolicy       `yaml:"inputView" json:"inputView"`
	Cache     WorkspaceCachePolicy  `yaml:"cache" json:"cache"`
	Export    WorkspaceExportPolicy `yaml:"export" json:"export"`
}

type InputViewPolicy struct {
	Source            string   `yaml:"source" json:"source"`
	Layout            string   `yaml:"layout" json:"layout"`
	CacheScope        string   `yaml:"cacheScope" json:"cacheScope"`
	Construction      string   `yaml:"construction" json:"construction"`
	ReuseExactView    bool     `yaml:"reuseExactView" json:"reuseExactView"`
	VerifyBeforeMount bool     `yaml:"verifyBeforeMount" json:"verifyBeforeMount"`
	VerifyAfterRun    bool     `yaml:"verifyAfterRun" json:"verifyAfterRun"`
	ViewRetention     Duration `yaml:"viewRetention" json:"viewRetention"`
}

type WorkspaceCachePolicy struct {
	MaxBytes         ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	HighWaterPercent Percent      `yaml:"highWaterPercent" json:"highWaterPercent"`
	LowWaterPercent  Percent      `yaml:"lowWaterPercent" json:"lowWaterPercent"`
	VerifyOnPopulate bool         `yaml:"verifyOnPopulate" json:"verifyOnPopulate"`
	Reverify         string       `yaml:"reverify" json:"reverify"`
}

type WorkspaceExportPolicy struct {
	Declaration              string       `yaml:"declaration" json:"declaration"`
	RegularFilesOnly         bool         `yaml:"regularFilesOnly" json:"regularFilesOnly"`
	MaxFiles                 int64        `yaml:"maxFiles" json:"maxFiles"`
	MaxBytes                 ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	RetainFullChangeManifest bool         `yaml:"retainFullChangeManifest" json:"retainFullChangeManifest"`
}

type TargetsPolicy struct {
	MaxConcurrent    int64                  `yaml:"maxConcurrent" json:"maxConcurrent"`
	MaterialTransfer MaterialTransferPolicy `yaml:"materialTransfer" json:"materialTransfer"`
	Templates        []TargetTemplate       `yaml:"templates" json:"templates"`
}

type MaterialTransferPolicy struct {
	Source                 string       `yaml:"source" json:"source"`
	AgentWorkspacePush     string       `yaml:"agentWorkspacePush" json:"agentWorkspacePush"`
	MaxTransferBytesPerRun ByteQuantity `yaml:"maxTransferBytesPerRun" json:"maxTransferBytesPerRun"`
	VerifyBeforeRun        bool         `yaml:"verifyBeforeRun" json:"verifyBeforeRun"`
}

type TargetTemplate struct {
	Name        string                `yaml:"name" json:"name"`
	Kind        string                `yaml:"kind" json:"kind"`
	Runtime     TargetRuntimePolicy   `yaml:"runtime" json:"runtime"`
	Material    TargetMaterialPolicy  `yaml:"material" json:"material,omitempty"`
	Interaction TargetInteraction     `yaml:"interaction" json:"interaction"`
	Resources   TargetResourcesPolicy `yaml:"resources" json:"resources"`
	Reset       TargetResetPolicy     `yaml:"reset" json:"reset"`
}

// TargetRuntimePolicy is a tagged union selected by TargetTemplate.Kind.
type TargetRuntimePolicy struct {
	Driver                      string            `yaml:"driver" json:"driver"`
	Runtime                     string            `yaml:"runtime" json:"runtime,omitempty"`
	Image                       string            `yaml:"image" json:"image,omitempty"`
	IsolationProfile            string            `yaml:"isolationProfile" json:"isolationProfile"`
	BaseImage                   string            `yaml:"baseImage" json:"baseImage,omitempty"`
	User                        string            `yaml:"user" json:"user,omitempty"`
	Capabilities                LinuxCapabilities `yaml:"capabilities" json:"capabilities,omitempty"`
	NoNewPrivileges             bool              `yaml:"noNewPrivileges" json:"noNewPrivileges,omitempty"`
	SeccompProfile              string            `yaml:"seccompProfile" json:"seccompProfile,omitempty"`
	SystemImageDigest           string            `yaml:"systemImageDigest" json:"systemImageDigest,omitempty"`
	BaselineState               string            `yaml:"baselineState" json:"baselineState,omitempty"`
	RequireHardwareAcceleration bool              `yaml:"requireHardwareAcceleration" json:"requireHardwareAcceleration,omitempty"`
	Headless                    bool              `yaml:"headless" json:"headless,omitempty"`
	Rooted                      bool              `yaml:"rooted" json:"rooted,omitempty"`
	Debuggable                  bool              `yaml:"debuggable" json:"debuggable,omitempty"`
	BootTimeout                 Duration          `yaml:"bootTimeout" json:"bootTimeout,omitempty"`
}

type TargetMaterialPolicy struct {
	MountPoint    string `yaml:"mountPoint" json:"mountPoint,omitempty"`
	WritableState string `yaml:"writableState" json:"writableState,omitempty"`
}

// TargetInteraction is also a tagged union selected by TargetTemplate.Kind.
type TargetInteraction struct {
	CommandAuthority              string   `yaml:"commandAuthority" json:"commandAuthority"`
	ExecTransport                 string   `yaml:"execTransport" json:"execTransport,omitempty"`
	FileTransfer                  string   `yaml:"fileTransfer" json:"fileTransfer"`
	NetworkEndpoints              string   `yaml:"networkEndpoints" json:"networkEndpoints,omitempty"`
	ADB                           string   `yaml:"adb" json:"adb,omitempty"`
	DeviceScopedADBServices       string   `yaml:"deviceScopedADBServices" json:"deviceScopedADBServices,omitempty"`
	DeniedInfrastructureAuthority []string `yaml:"deniedInfrastructureAuthority" json:"deniedInfrastructureAuthority"`
}

type TargetResourcesPolicy struct {
	Limits TargetResourceLimits `yaml:"limits" json:"limits"`
}

type TargetResourceLimits struct {
	CPU           CPUQuantity  `yaml:"cpu" json:"cpu"`
	Memory        ByteQuantity `yaml:"memory" json:"memory"`
	Swap          ByteQuantity `yaml:"swap" json:"swap,omitempty"`
	PIDs          int64        `yaml:"pids" json:"pids,omitempty"`
	WritableState ByteQuantity `yaml:"writableState" json:"writableState"`
}

type TargetResetPolicy struct {
	AfterEveryRun bool   `yaml:"afterEveryRun" json:"afterEveryRun"`
	Mode          string `yaml:"mode" json:"mode"`
}

type AggregateResources struct {
	AggregateLimits AggregateResourceLimits `yaml:"aggregateLimits" json:"aggregateLimits"`
}

type AggregateResourceLimits struct {
	CPU          CPUQuantity  `yaml:"cpu" json:"cpu"`
	Memory       ByteQuantity `yaml:"memory" json:"memory"`
	CaptureBytes ByteQuantity `yaml:"captureBytes" json:"captureBytes"`
}

type ObservationPolicy struct {
	Priority         string                     `yaml:"priority" json:"priority"`
	RequiredCoverage map[string][]string        `yaml:"requiredCoverage" json:"requiredCoverage"`
	Profiles         ObservationProfiles        `yaml:"profiles" json:"profiles"`
	Baseline         []string                   `yaml:"baseline" json:"baseline"`
	Collectors       CollectorPolicies          `yaml:"collectors" json:"collectors"`
	Metrics          MetricsPolicy              `yaml:"metrics" json:"metrics"`
	LiveAccess       LiveAccessPolicy           `yaml:"liveAccess" json:"liveAccess"`
	AgentRequests    AgentRequestsPolicy        `yaml:"agentRequests" json:"agentRequests"`
	Buffers          ObservationBuffers         `yaml:"buffers" json:"buffers"`
	AllowedOnDemand  AllowedOnDemandPolicy      `yaml:"allowedOnDemand" json:"allowedOnDemand"`
	Triggers         []ObservationTrigger       `yaml:"triggers" json:"triggers"`
	Bundles          BundlePolicy               `yaml:"bundles" json:"bundles"`
	Sensitivity      SensitivityPolicy          `yaml:"sensitivity" json:"sensitivity"`
	Retention        ObservationRetentionPolicy `yaml:"retention" json:"retention"`
}

type ObservationProfiles struct {
	Default  string          `yaml:"default" json:"default"`
	Metadata MetadataProfile `yaml:"metadata" json:"metadata"`
	Deep     BoundedProfile  `yaml:"deep" json:"deep"`
	Payload  PayloadProfile  `yaml:"payload" json:"payload"`
}

type MetadataProfile struct {
	CapturePayloads  bool     `yaml:"capturePayloads" json:"capturePayloads"`
	FileEvents       []string `yaml:"fileEvents" json:"fileEvents"`
	SyscallArguments string   `yaml:"syscallArguments" json:"syscallArguments"`
}

type BoundedProfile struct {
	MaxDuration    Duration     `yaml:"maxDuration" json:"maxDuration"`
	MaxBytes       ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	SignalFamilies []string     `yaml:"signalFamilies" json:"signalFamilies"`
}

type PayloadProfile struct {
	Enabled                    string       `yaml:"enabled" json:"enabled"`
	RequireProcessOrPathFilter bool         `yaml:"requireProcessOrPathFilter" json:"requireProcessOrPathFilter"`
	MaxDuration                Duration     `yaml:"maxDuration" json:"maxDuration"`
	MaxBytes                   ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	Sensitivity                string       `yaml:"sensitivity" json:"sensitivity"`
	SignalFamilies             []string     `yaml:"signalFamilies" json:"signalFamilies"`
}

type CollectorPolicies struct {
	LinuxMetadata   SingleCollectorPolicy  `yaml:"linuxMetadata" json:"linuxMetadata"`
	AndroidSystem   MultiCollectorPolicy   `yaml:"androidSystem" json:"androidSystem"`
	AndroidAppHooks AppHookCollectorPolicy `yaml:"androidAppHooks" json:"androidAppHooks"`
	PacketCapture   PacketCollectorPolicy  `yaml:"packetCapture" json:"packetCapture"`
	ProtocolSummary ModeCollectorPolicy    `yaml:"protocolSummary" json:"protocolSummary"`
	MobileAnalysis  ModeCollectorPolicy    `yaml:"mobileAnalysis" json:"mobileAnalysis"`
}

type SingleCollectorPolicy struct {
	Adapter               string `yaml:"adapter" json:"adapter"`
	Placement             string `yaml:"placement" json:"placement"`
	FailRunOnCoverageLoss bool   `yaml:"failRunOnCoverageLoss" json:"failRunOnCoverageLoss"`
}

type MultiCollectorPolicy struct {
	Adapters              []string `yaml:"adapters" json:"adapters"`
	Placement             string   `yaml:"placement" json:"placement"`
	FailRunOnCoverageLoss bool     `yaml:"failRunOnCoverageLoss" json:"failRunOnCoverageLoss"`
}

type AppHookCollectorPolicy struct {
	Adapter   string `yaml:"adapter" json:"adapter"`
	Placement string `yaml:"placement" json:"placement"`
	Coverage  string `yaml:"coverage" json:"coverage"`
}

type PacketCollectorPolicy struct {
	Adapter   string       `yaml:"adapter" json:"adapter"`
	Placement string       `yaml:"placement" json:"placement"`
	RingBytes ByteQuantity `yaml:"ringBytes" json:"ringBytes"`
}

type ModeCollectorPolicy struct {
	Adapter string `yaml:"adapter" json:"adapter"`
	Mode    string `yaml:"mode" json:"mode"`
}

type MetricsPolicy struct {
	NormalInterval   Duration   `yaml:"normalInterval" json:"normalInterval"`
	IncidentInterval Duration   `yaml:"incidentInterval" json:"incidentInterval"`
	StaleAfter       Duration   `yaml:"staleAfter" json:"staleAfter"`
	RateWindows      []Duration `yaml:"rateWindows" json:"rateWindows"`
	Dimensions       []string   `yaml:"dimensions" json:"dimensions"`
}

type LiveAccessPolicy struct {
	Agent  AgentLiveAccessPolicy `yaml:"agent" json:"agent"`
	Visual VisualAccessPolicy    `yaml:"visual" json:"visual"`
}

type AgentLiveAccessPolicy struct {
	Enabled               bool     `yaml:"enabled" json:"enabled"`
	MinimumMetricInterval Duration `yaml:"minimumMetricInterval" json:"minimumMetricInterval"`
	Signals               []string `yaml:"signals" json:"signals"`
}

type VisualAccessPolicy struct {
	Screenshots      string  `yaml:"screenshots" json:"screenshots"`
	LiveScreenMaxFPS float64 `yaml:"liveScreenMaxFps" json:"liveScreenMaxFps"`
}

type AgentRequestsPolicy struct {
	Profiles      []string `yaml:"profiles" json:"profiles"`
	NamedCaptures []string `yaml:"namedCaptures" json:"namedCaptures"`
}

type ObservationBuffers struct {
	LiveSubscriberEvents int64        `yaml:"liveSubscriberEvents" json:"liveSubscriberEvents"`
	SegmentBytes         ByteQuantity `yaml:"segmentBytes" json:"segmentBytes"`
	PacketRingBytes      ByteQuantity `yaml:"packetRingBytes" json:"packetRingBytes"`
}

type AllowedOnDemandPolicy struct {
	WorldLifecycle BoundedProfile         `yaml:"worldLifecycle" json:"worldLifecycle"`
	Strace         BoundedProfile         `yaml:"strace" json:"strace"`
	Perfetto       BoundedProfile         `yaml:"perfetto" json:"perfetto"`
	PacketPayload  FilteredBoundedProfile `yaml:"packetPayload" json:"packetPayload"`
	Mitmproxy      MitmproxyCapturePolicy `yaml:"mitmproxy" json:"mitmproxy"`
	Frida          FridaCapturePolicy     `yaml:"frida" json:"frida"`
}

type FilteredBoundedProfile struct {
	MaxDuration       Duration     `yaml:"maxDuration" json:"maxDuration"`
	MaxBytes          ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	RequireFlowFilter bool         `yaml:"requireFlowFilter" json:"requireFlowFilter"`
	SignalFamilies    []string     `yaml:"signalFamilies" json:"signalFamilies"`
}

type MitmproxyCapturePolicy struct {
	MaxDuration       Duration     `yaml:"maxDuration" json:"maxDuration"`
	MaxBytes          ByteQuantity `yaml:"maxBytes" json:"maxBytes"`
	DecryptedPayloads string       `yaml:"decryptedPayloads" json:"decryptedPayloads"`
	SignalFamilies    []string     `yaml:"signalFamilies" json:"signalFamilies"`
}

type FridaCapturePolicy struct {
	MaxDuration                   Duration `yaml:"maxDuration" json:"maxDuration"`
	HostManagedCollectorScripts   string   `yaml:"hostManagedCollectorScripts" json:"hostManagedCollectorScripts"`
	AgentInstalledInstrumentation string   `yaml:"agentInstalledInstrumentation" json:"agentInstalledInstrumentation"`
	SignalFamilies                []string `yaml:"signalFamilies" json:"signalFamilies"`
}

type ObservationTrigger struct {
	Name    string           `yaml:"name" json:"name"`
	When    TriggerCondition `yaml:"when" json:"when"`
	Actions []string         `yaml:"actions" json:"actions"`
}

type TriggerCondition struct {
	Metric string   `yaml:"metric" json:"metric,omitempty"`
	Above  *float64 `yaml:"above" json:"above,omitempty"`
	For    Duration `yaml:"for" json:"for,omitempty"`
	Event  string   `yaml:"event" json:"event,omitempty"`
}

type BundlePolicy struct {
	RawCollectorOutputs      bool `yaml:"rawCollectorOutputs" json:"rawCollectorOutputs"`
	NormalizedEvents         bool `yaml:"normalizedEvents" json:"normalizedEvents"`
	TargetChangeManifest     bool `yaml:"targetChangeManifest" json:"targetChangeManifest"`
	CollectorCoverageAndGaps bool `yaml:"collectorCoverageAndGaps" json:"collectorCoverageAndGaps"`
	DerivedAgentSummary      bool `yaml:"derivedAgentSummary" json:"derivedAgentSummary"`
	SummaryMustCiteEvidence  bool `yaml:"summaryMustCiteEvidence" json:"summaryMustCiteEvidence"`
}

type SensitivityPolicy struct {
	Default          string `yaml:"default" json:"default"`
	DecryptedTraffic string `yaml:"decryptedTraffic" json:"decryptedTraffic"`
	Screenshots      string `yaml:"screenshots" json:"screenshots"`
	Payloads         string `yaml:"payloads" json:"payloads"`
}

type ObservationRetentionPolicy struct {
	LiveLocal                   Duration `yaml:"liveLocal" json:"liveLocal"`
	FinalizeToArtifactStore     bool     `yaml:"finalizeToArtifactStore" json:"finalizeToArtifactStore"`
	DeleteLocalAfterArtifactAck bool     `yaml:"deleteLocalAfterArtifactAck" json:"deleteLocalAfterArtifactAck"`
}

type IncidentsPolicy struct {
	MinimumEvidence []string               `yaml:"minimumEvidence" json:"minimumEvidence"`
	Recovery        IncidentRecoveryPolicy `yaml:"recovery" json:"recovery"`
}

type IncidentRecoveryPolicy struct {
	AgentWorkspaceFailure string `yaml:"agentWorkspaceFailure" json:"agentWorkspaceFailure"`
	LinuxTargetFailure    string `yaml:"linuxTargetFailure" json:"linuxTargetFailure"`
	AndroidRuntimeFailure string `yaml:"androidRuntimeFailure" json:"androidRuntimeFailure"`
	TargetWorkloadExit    string `yaml:"targetWorkloadExit" json:"targetWorkloadExit"`
	AndroidAppCrash       string `yaml:"androidAppCrash" json:"androidAppCrash"`
	ObserverFailure       string `yaml:"observerFailure" json:"observerFailure"`
}

type PressurePolicy struct {
	ReserveHostMemory ByteQuantity        `yaml:"reserveHostMemory" json:"reserveHostMemory"`
	StopAdmissionOn   StopAdmissionPolicy `yaml:"stopAdmissionOn" json:"stopAdmissionOn"`
	ShedOrder         []string            `yaml:"shedOrder" json:"shedOrder"`
}

type StopAdmissionPolicy struct {
	MemoryPSIFull   Ratio   `yaml:"memoryPSIFull" json:"memoryPSIFull"`
	IOPSIFull       Ratio   `yaml:"ioPSIFull" json:"ioPSIFull"`
	FreeDiskPercent Percent `yaml:"freeDiskPercent" json:"freeDiskPercent"`
}
