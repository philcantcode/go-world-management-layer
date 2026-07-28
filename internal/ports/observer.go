package ports

import (
	"context"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// TargetLifecycleSignal is produced intrinsically by a target driver from
// facts it directly owns. It never names an external observer adapter.
const TargetLifecycleSignal = "target.lifecycle"

const (
	CollectorStdoutArtifactRole = "collector.stdout"
	CollectorStderrArtifactRole = "collector.stderr"
)

// MaximumCollectorNameBytes leaves ample room for namespacing collector
// identities beneath a maximum-sized parent idempotency key.
const MaximumCollectorNameBytes = 128

const collectorIdempotencySuffixPrefix = "collector/"

type ObservationRequirement struct {
	SignalFamily string
	Placement    domain.CollectorPlacement
	MinimumLevel domain.CoverageLevel
	Required     bool
}

// CollectorSpec is the immutable, authority-selected collector configuration
// attached to a target-run plan. Callers never supply executable arguments at
// run time; Adapter identifies one adapter that was probed during composition.
type CollectorSpec struct {
	Name                string
	Requirement         ObservationRequirement
	Adapter             string
	Version             string
	ConfigurationDigest domain.Digest
	Resources           admission.Resources
	MaximumBytes        int64
}

func (s CollectorSpec) Validate() error {
	const operation = "ports.collector_spec.validate"
	if err := ValidateCollectorName(s.Name); err != nil {
		return err
	}
	if s.Adapter == "" || s.Version == "" || s.ConfigurationDigest.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "collector", "adapter, version, and configuration digest are required", nil)
	}
	if err := s.Requirement.Validate(); err != nil {
		return err
	}
	if err := s.Resources.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "resources", "is invalid", err)
	}
	if s.MaximumBytes <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "maximum_bytes", "must be positive", nil)
	}
	return nil
}

// ValidateCollectorName accepts only a bounded, portable identifier. Names
// begin and end with an ASCII letter or digit; '.', '_', and '-' are permitted
// only inside the name. This keeps profile references, plan identities, and
// future storage labels free of separators and normalization ambiguity.
func ValidateCollectorName(name string) error {
	const operation = "ports.collector_name.validate"
	if !isCanonicalCollectorName(name) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "name", "must be 1 to 128 ASCII bytes, begin and end with a letter or digit, and contain only letters, digits, '.', '_', or '-'", nil)
	}
	return nil
}

// DeriveCollectorIdempotencyKey is the single canonical mapping from a target
// run provisioning identity and collector name to the collector child key.
func DeriveCollectorIdempotencyKey(parent, name string) string {
	if !isCanonicalCollectorName(name) {
		return ""
	}
	return domain.DeriveIdempotencyKey(parent, collectorIdempotencySuffixPrefix+name)
}

func isCanonicalCollectorName(name string) bool {
	if len(name) == 0 || len(name) > MaximumCollectorNameBytes || !collectorNameEdge(name[0]) || !collectorNameEdge(name[len(name)-1]) {
		return false
	}
	for index := 1; index < len(name)-1; index++ {
		if !collectorNameEdge(name[index]) && name[index] != '.' && name[index] != '_' && name[index] != '-' {
			return false
		}
	}
	return true
}

func collectorNameEdge(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

// ObservationAttachment is a target-driver observation handle. RuntimeID is
// authority-produced by PrepareRun and is never accepted from a public run
// request.
type ObservationAttachment struct {
	TargetKind domain.TargetKind
	RuntimeID  string
}

func (a ObservationAttachment) Validate() error {
	if !a.TargetKind.IsValid() || a.RuntimeID == "" {
		return domain.NewError(domain.CodeInvalidArgument, "ports.observation_attachment.validate", "attachment", "target kind and runtime identity are required", nil)
	}
	return nil
}

func (r ObservationRequirement) Validate() error {
	if r.SignalFamily == "" || !r.Placement.IsValid() || !r.MinimumLevel.IsValid() || r.MinimumLevel == domain.CoverageLevelUnknown {
		return domain.NewError(domain.CodeInvalidArgument, "ports.observation_requirement.validate", "requirement", "signal, placement, and concrete level are required", nil)
	}
	return nil
}

type CollectorPlan struct {
	IdempotencyKey      string
	CollectorID         domain.CollectorID
	ResearchSessionID   domain.ResearchSessionID
	LeaseID             domain.LeaseID
	AgentWorkspaceID    domain.AgentWorkspaceID
	AgentGeneration     domain.AgentGeneration
	TargetID            domain.TargetID
	TargetGeneration    domain.TargetGeneration
	TargetRunID         domain.TargetRunID
	Attachment          ObservationAttachment
	Requirement         ObservationRequirement
	Adapter             string
	Version             string
	ConfigurationDigest domain.Digest
	Resources           admission.Resources
	MaximumBytes        int64
	StartedAt           time.Time
}

func (p CollectorPlan) Validate() error {
	const operation = "ports.collector_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.CollectorID.IsZero() || p.ResearchSessionID.IsZero() || p.LeaseID.IsZero() || p.AgentWorkspaceID.IsZero() || !p.AgentGeneration.IsValid() || p.TargetID.IsZero() || !p.TargetGeneration.IsValid() || p.TargetRunID.IsZero() || p.Adapter == "" || p.Version == "" || p.ConfigurationDigest.IsZero() || p.StartedAt.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "collector", "complete run scope, adapter, version, configuration, and start time are required", nil)
	}
	if err := p.Attachment.Validate(); err != nil {
		return err
	}
	if err := p.Requirement.Validate(); err != nil {
		return err
	}
	if err := p.Resources.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "resources", "is invalid", err)
	}
	if p.MaximumBytes <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "maximum_bytes", "must be positive", nil)
	}
	return nil
}

type Collector struct {
	ID           domain.CollectorID
	TargetRunID  domain.TargetRunID
	SignalFamily string
	StartedAt    time.Time
}

type CollectorResult struct {
	CollectorID       domain.CollectorID
	Coverage          domain.CollectorCoverage
	Artifacts         []domain.ArtifactReference
	StoppedAt         time.Time
	TeardownConfirmed bool
}

// ObserverDriver keeps privileged collector mechanics outside the target
// compromise domain. Coverage is queried independently of collector output.
type ObserverDriver interface {
	Probe(context.Context, ObservationRequirement) (domain.CapabilityFingerprint, error)
	Start(context.Context, CollectorPlan) (Collector, error)
	Stop(context.Context, domain.CollectorID) (CollectorResult, error)
	Coverage(context.Context, domain.CollectorID) (domain.CollectorCoverage, error)
}
