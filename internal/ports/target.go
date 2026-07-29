package ports

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/androidcontract"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

type TargetTemplate struct {
	Name             string
	Kind             domain.TargetKind
	Driver           string
	Runtime          string
	ImageDigest      domain.Digest
	IsolationProfile string
	// Android virtual-device fields are carried in the resolved physical plan
	// rather than being re-read from policy inside a driver. They are zero for
	// Linux-container templates.
	BaselineState               string
	RequireHardwareAcceleration bool
	Headless                    bool
	Rooted                      bool
	Debuggable                  bool
	GuestMemoryBytes            int64
	BootTimeout                 time.Duration
}

const AndroidBaselineCleanBoot = "clean-boot"

func (t TargetTemplate) Validate() error {
	const operation = "ports.target_template.validate"
	if t.Name == "" || !t.Kind.IsValid() || t.Driver == "" || t.IsolationProfile == "" {
		return domain.NewError(domain.CodeInvalidArgument, operation, "template", "name, kind, driver, and isolation profile are required", nil)
	}
	if t.ImageDigest.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "image_digest", "must be set", nil)
	}
	switch t.Kind {
	case domain.TargetLinuxContainer:
		if strings.TrimSpace(t.Runtime) == "" || t.Runtime != strings.TrimSpace(t.Runtime) {
			return domain.NewError(domain.CodeInvalidArgument, operation, "runtime", "must be non-blank and trimmed for a Linux container", nil)
		}
		if t.BaselineState != "" || t.RequireHardwareAcceleration || t.Headless || t.Rooted || t.Debuggable || t.GuestMemoryBytes != 0 || t.BootTimeout != 0 {
			return domain.NewError(domain.CodeInvalidArgument, operation, "template", "contains Android-only runtime fields", nil)
		}
	case domain.TargetAndroidVirtualDevice:
		if t.Runtime != "" {
			return domain.NewError(domain.CodeInvalidArgument, operation, "runtime", "must be empty for an Android virtual device", nil)
		}
		if t.BaselineState != AndroidBaselineCleanBoot {
			return domain.NewError(domain.CodeInvalidArgument, operation, "baseline_state", "must be clean-boot", nil)
		}
		if !t.RequireHardwareAcceleration || !t.Headless || !t.Rooted || !t.Debuggable || t.GuestMemoryBytes <= 0 || t.BootTimeout <= 0 {
			return domain.NewError(domain.CodeInvalidArgument, operation, "template", "Android hardware acceleration, headless, rooted, debuggable, positive guest memory, and a positive boot timeout are required", nil)
		}
		if err := androidcontract.ValidateGuestMemoryBytes(t.GuestMemoryBytes); err != nil {
			return domain.NewError(domain.CodeInvalidArgument, operation, "guest_memory_bytes", "is invalid", err)
		}
	}
	return nil
}

type TargetPlan struct {
	IdempotencyKey              string
	LeaseID                     domain.LeaseID
	Target                      domain.Target
	Generation                  domain.TargetGenerationRecord
	Template                    TargetTemplate
	PolicyDigest                domain.Digest
	CapabilityFingerprintDigest domain.Digest
	Resources                   admission.Resources
}

func (p TargetPlan) Validate() error {
	const operation = "ports.target_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.LeaseID.IsZero() || p.Target.ID().IsZero() || p.Generation.Spec().TargetID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "scope", "lease, target, and generation are required", nil)
	}
	if err := p.Template.Validate(); err != nil {
		return err
	}
	generation := p.Generation.Spec()
	if generation.TargetID != p.Target.ID() || generation.Generation != p.Target.CurrentGeneration() || p.Target.Kind() != p.Template.Kind {
		return domain.NewError(domain.CodeConflict, operation, "generation", "does not match target or template", nil)
	}
	if p.PolicyDigest.IsZero() || p.CapabilityFingerprintDigest.IsZero() || generation.PolicyDigest != p.PolicyDigest || generation.CapabilityFingerprintDigest != p.CapabilityFingerprintDigest {
		return domain.NewError(domain.CodeConflict, operation, "digests", "policy and capability digests must match the generation", nil)
	}
	if err := p.Resources.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "resources", "is invalid", err)
	}
	return nil
}

type TargetStatus struct {
	TargetID     domain.TargetID
	Generation   domain.TargetGeneration
	Kind         domain.TargetKind
	State        domain.TargetGenerationState
	Ready        bool
	RuntimeID    string
	DeviceSerial string
	CgroupID     string
	ObservedAt   time.Time
}

type TargetResult struct {
	Status  TargetStatus
	Created bool
}

type TargetRef struct {
	ID         domain.TargetID
	Generation domain.TargetGeneration
}

// TargetQuarantinePlan identifies the one physical generation that must be
// contained. Quarantine preserves the generation's durable state; it is not a
// synonym for Destroy.
type TargetQuarantinePlan struct {
	IdempotencyKey string
	Target         TargetRef
	Reason         string
}

func (p TargetQuarantinePlan) Validate() error {
	const operation = "ports.target_quarantine_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if err := p.Target.Validate(); err != nil {
		return err
	}
	if p.Reason != strings.TrimSpace(p.Reason) || p.Reason == "" || len(p.Reason) > 4096 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "reason", "must be trimmed, non-empty, and at most 4096 bytes", nil)
	}
	return nil
}

// TargetQuarantineEvidence is a backend observation, not an intended state.
// All three confirmations are required so callers cannot manufacture a
// quarantined control state from a successful stop request alone.
type TargetQuarantineEvidence struct {
	Target             TargetRef
	RuntimeID          string
	ExecutionStopped   bool
	NetworkUnreachable bool
	StatePreserved     bool
	ObservedAt         time.Time
}

func (e TargetQuarantineEvidence) Validate(expected TargetRef) error {
	const operation = "ports.target_quarantine_evidence.validate"
	if err := expected.Validate(); err != nil {
		return err
	}
	if e.Target != expected {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "target", "evidence identifies a different target generation", nil)
	}
	if strings.TrimSpace(e.RuntimeID) == "" || e.ObservedAt.IsZero() {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "observation", "runtime identity and observation time are required", nil)
	}
	if !e.ExecutionStopped || !e.NetworkUnreachable || !e.StatePreserved {
		return domain.NewError(domain.CodeFailedPrecondition, operation, "confirmation", "execution stop, network isolation, and state preservation must all be confirmed", nil)
	}
	return nil
}

func (r TargetRef) Validate() error {
	if r.ID.IsZero() || !r.Generation.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "ports.target_ref.validate", "scope", "target and generation are required", nil)
	}
	return nil
}

// TargetMaterialPlan is one explicitly authorized immutable file in a target
// run. Drivers receive reopenable bytes rather than artifact-store paths.
type TargetMaterialPlan struct {
	Artifact    domain.ArtifactReference
	LogicalPath string
	Mode        uint32
	Content     ContentSource
}

func (p TargetMaterialPlan) Validate() error {
	const operation = "ports.target_material_plan.validate"
	spec := p.Artifact.Spec()
	if spec.Reference == "" || spec.Digest.IsZero() || spec.Size < 0 || spec.Role == "" || !spec.Sensitivity.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "artifact", "must be initialized", nil)
	}
	if _, err := safepath.Normalize(p.LogicalPath); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "logical_path", "must be a safe target-relative path", err)
	}
	if p.Mode == 0 || p.Mode&^uint32(0o777) != 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "mode", "must contain only non-zero user/group/other permission bits", nil)
	}
	if p.Content == nil || p.Content.Digest() != spec.Digest || p.Content.Size() != spec.Size {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "content", "declared bytes do not match the artifact identity", nil)
	}
	return nil
}

// TargetMaterializationDigest returns the canonical identity of an exact target
// projection. Entry ordering never changes the digest.
func TargetMaterializationDigest(material []TargetMaterialPlan) (domain.Digest, error) {
	if len(material) == 0 {
		return domain.Digest{}, domain.NewError(domain.CodeInvalidArgument, "ports.target_materialization_digest", "material", "must not be empty", nil)
	}
	entries := append([]TargetMaterialPlan(nil), material...)
	seen := make(map[string]struct{}, len(entries))
	for index := range entries {
		if err := entries[index].Validate(); err != nil {
			return domain.Digest{}, fmt.Errorf("material %d: %w", index, err)
		}
		if _, duplicate := seen[entries[index].LogicalPath]; duplicate {
			return domain.Digest{}, domain.NewError(domain.CodeConflict, "ports.target_materialization_digest", "logical_path", "must be unique", nil)
		}
		seen[entries[index].LogicalPath] = struct{}{}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalPath < entries[j].LogicalPath })
	var canonical bytes.Buffer
	writeTargetMaterialString(&canonical, "world.target-materialization.v1")
	for _, entry := range entries {
		spec := entry.Artifact.Spec()
		writeTargetMaterialString(&canonical, entry.LogicalPath)
		writeTargetMaterialString(&canonical, spec.Reference)
		writeTargetMaterialString(&canonical, spec.Digest.String())
		_ = binary.Write(&canonical, binary.BigEndian, spec.Size)
		_ = binary.Write(&canonical, binary.BigEndian, entry.Mode)
		writeTargetMaterialString(&canonical, spec.Role)
		writeTargetMaterialString(&canonical, string(spec.Sensitivity))
	}
	return domain.NewDigest(canonical.Bytes()), nil
}

func writeTargetMaterialString(buffer *bytes.Buffer, value string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	_, _ = buffer.WriteString(value)
}

type TargetRunPlan struct {
	IdempotencyKey   string
	Run              domain.TargetRun
	RequiredCoverage []string
	Collectors       []CollectorSpec
	Material         []TargetMaterialPlan
	MaximumDuration  time.Duration
}

func (p TargetRunPlan) Validate() error {
	const operation = "ports.target_run_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.Run.ID().IsZero() || p.MaximumDuration <= 0 || len(p.Material) == 0 || len(p.RequiredCoverage) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "run", "initialized run, material, required coverage, and positive duration are required", nil)
	}
	seen := make(map[string]struct{}, len(p.RequiredCoverage))
	for _, family := range p.RequiredCoverage {
		if family == "" || family != strings.TrimSpace(family) {
			return domain.NewError(domain.CodeInvalidArgument, operation, "required_coverage", "must contain trimmed, non-blank values", nil)
		}
		if _, duplicate := seen[family]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, operation, "required_coverage", "must not contain duplicates", nil)
		}
		seen[family] = struct{}{}
	}
	configured := make(map[string]struct{}, len(p.Collectors))
	collectorNames := make(map[string]struct{}, len(p.Collectors))
	collectorFamilies := make(map[string]struct{}, len(p.Collectors))
	for index, collector := range p.Collectors {
		if err := collector.Validate(); err != nil {
			return fmt.Errorf("collector %d: %w", index, err)
		}
		if key := DeriveCollectorIdempotencyKey(p.IdempotencyKey, collector.Name); !domain.IsCanonicalIdempotencyKey(key) {
			return domain.NewError(domain.CodeInvalidArgument, operation, "collectors", "must derive canonical collector idempotency keys", nil)
		}
		if _, duplicate := collectorNames[collector.Name]; duplicate {
			return domain.NewError(domain.CodeConflict, operation, "collectors", "must not contain duplicate names", nil)
		}
		collectorNames[collector.Name] = struct{}{}
		if collector.Requirement.SignalFamily == TargetLifecycleSignal {
			return domain.NewError(domain.CodeConflict, operation, "collectors", "target.lifecycle is intrinsic and must not name an external collector", nil)
		}
		if _, duplicate := collectorFamilies[collector.Requirement.SignalFamily]; duplicate {
			return domain.NewError(domain.CodeConflict, operation, "collectors", "must contain at most one collector for each signal family", nil)
		}
		collectorFamilies[collector.Requirement.SignalFamily] = struct{}{}
		if collector.Requirement.Required {
			if _, required := seen[collector.Requirement.SignalFamily]; !required {
				return domain.NewError(domain.CodeConflict, operation, "collectors", "a required collector family must be present in required coverage", nil)
			}
			configured[collector.Requirement.SignalFamily] = struct{}{}
		}
	}
	externalRequired := len(seen)
	if _, intrinsic := seen[TargetLifecycleSignal]; intrinsic {
		externalRequired--
	}
	if len(configured) != externalRequired {
		return domain.NewError(domain.CodeConflict, operation, "required_coverage", "must exactly match required collector families", nil)
	}
	for family := range seen {
		if family == TargetLifecycleSignal {
			continue
		}
		if _, found := configured[family]; !found {
			return domain.NewError(domain.CodeConflict, operation, "required_coverage", "must exactly match required collector families", nil)
		}
	}
	materializationDigest, err := TargetMaterializationDigest(p.Material)
	if err != nil {
		return err
	}
	if materializationDigest != p.Run.Spec().MaterializationDigest {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "materialization_digest", "does not identify the exact material projection", nil)
	}
	return nil
}

type PreparedTargetRun struct {
	RunID                 domain.TargetRunID
	TargetID              domain.TargetID
	TargetGeneration      domain.TargetGeneration
	MaterializationDigest domain.Digest
	RequiredCoverage      []string
	Attachment            ObservationAttachment
	PreparedAt            time.Time
}

type TargetRunFailureKind string

const (
	TargetRunFailureNone             TargetRunFailureKind = ""
	TargetRunFailureNeverStarted     TargetRunFailureKind = "never_started"
	TargetRunFailureDurationExceeded TargetRunFailureKind = "duration_exceeded"
	TargetRunFailureTarget           TargetRunFailureKind = "target_failure"
)

func (k TargetRunFailureKind) IsValid() bool {
	return k == TargetRunFailureNone || k == TargetRunFailureNeverStarted || k == TargetRunFailureDurationExceeded || k == TargetRunFailureTarget
}

// TargetRunObservation is an intrinsic target fact awaiting assignment of an
// authoritative observation-ledger cursor by orchestration.
type TargetRunObservation struct {
	Kind              string
	ObservedAt        time.Time
	TargetOperationID domain.TargetOperationID
	Payload           json.RawMessage
}

// TargetRunStopReceipt contains only facts owned by the target driver. Raw
// collector evidence is merged later by the run observer coordinator.
type TargetRunStopReceipt struct {
	RunID         domain.TargetRunID
	Outcome       RunOutcome
	FailureKind   TargetRunFailureKind
	StartedAt     time.Time
	StoppedAt     time.Time
	Observations  []TargetRunObservation
	TargetChanges domain.ChangeSet
}

func (r TargetRunStopReceipt) Validate() error {
	const operation = "ports.target_run_stop_receipt.validate"
	if r.RunID.IsZero() || !r.Outcome.IsValid() || !r.FailureKind.IsValid() || r.StoppedAt.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "receipt", "run, outcome, failure kind, and stop time are required", nil)
	}
	if r.Outcome == RunCompleted && r.FailureKind != TargetRunFailureNone || r.Outcome == RunFailed && r.FailureKind == TargetRunFailureNone {
		return domain.NewError(domain.CodeConflict, operation, "outcome", "must agree with the failure kind", nil)
	}
	if r.StartedAt.IsZero() && r.FailureKind != TargetRunFailureNeverStarted || !r.StartedAt.IsZero() && r.StoppedAt.Before(r.StartedAt) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "time_range", "must describe a consistent target run interval", nil)
	}
	if r.TargetChanges.Scope() != domain.ChangeScopeTarget || !r.TargetChanges.WorkspaceRevision().IsValid() || r.TargetChanges.SealedAt().IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "target_changes", "must be an initialized target change set", nil)
	}
	if len(r.Observations) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "observations", "must contain intrinsic target evidence", nil)
	}
	for index, observation := range r.Observations {
		if strings.TrimSpace(observation.Kind) == "" || observation.ObservedAt.IsZero() || observation.ObservedAt.After(r.StoppedAt) || len(observation.Payload) > 0 && !json.Valid(observation.Payload) {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("observations[%d]", index), "is invalid or outside the run interval", nil)
		}
	}
	return nil
}

type RunOutcome string

const (
	RunCompleted RunOutcome = "completed"
	RunFailed    RunOutcome = "failed"
)

func (o RunOutcome) IsValid() bool { return o == RunCompleted || o == RunFailed }

type TargetRunResult struct {
	RunID            domain.TargetRunID
	Outcome          RunOutcome
	FirstCursor      domain.ObservationCursor
	LastCursor       domain.ObservationCursor
	RawArtifacts     []domain.ArtifactReference
	NormalizedEvents []domain.EventEnvelope
	Metrics          []domain.MetricSample
	Coverage         []domain.CollectorCoverage
	Gaps             []domain.Gap
	TargetChanges    domain.ChangeSet
	IncidentIDs      []domain.IncidentID
	Summary          domain.DerivedSummary
	StoppedAt        time.Time
}

type ResetPlan struct {
	IdempotencyKey string
	LeaseID        domain.LeaseID
	Previous       TargetRef
	NextGeneration domain.TargetGeneration
	Mode           ResetMode
	SnapshotName   string
	IncidentID     domain.IncidentID
}

func (p ResetPlan) Validate() error {
	const operation = "ports.reset_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.LeaseID.IsZero() || p.Previous.Validate() != nil || !p.NextGeneration.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "scope", "lease, generations, and reset mode are required", nil)
	}
	if err := ValidateResetSelection(p.Mode, p.SnapshotName); err != nil {
		return err
	}
	next, err := p.Previous.Generation.Next()
	if err != nil || p.NextGeneration != next {
		return domain.NewError(domain.CodeConflict, operation, "next_generation", "must advance by exactly one", nil)
	}
	return nil
}

type ScopedADBEndpoint interface {
	Serial() string
	Address() string
	Close() error
}

type TargetTransport interface {
	OpenExec(context.Context, TargetExecPlan) (ExecTransport, error)
	PushFile(context.Context, TargetTransferPlan, io.Reader) (TransferResult, error)
	PullFile(context.Context, TargetTransferPlan) (io.ReadCloser, error)
	OpenADB(context.Context) (ScopedADBEndpoint, error)
	Close() error
}

// TargetDriver owns target generations and bounded run windows. StartRun is
// idempotent for the prepared run; StopRun returns target-owned facts only.
type TargetDriver interface {
	Probe(context.Context, TargetTemplate) (domain.CapabilityFingerprint, error)
	Create(context.Context, TargetPlan) (TargetResult, error)
	PrepareRun(context.Context, TargetRunPlan) (PreparedTargetRun, error)
	StartRun(context.Context, domain.TargetRunID) error
	OpenTransport(context.Context, domain.TargetRunID) (TargetTransport, error)
	StopRun(context.Context, domain.TargetRunID, StopMode) (TargetRunStopReceipt, error)
	Quarantine(context.Context, TargetQuarantinePlan) (TargetQuarantineEvidence, error)
	Reset(context.Context, domain.TargetID, ResetPlan) (TargetResult, error)
	Destroy(context.Context, TargetRef) error
}
