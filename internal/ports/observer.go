package ports

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
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

// ADBServerEndpoint is the canonical identity of one explicitly selected ADB
// server. Only literal loopback endpoints are representable: observation must
// never escape to DNS, a remote server, or adb's ambient server selection.
type ADBServerEndpoint struct {
	Host string
	Port uint16
}

func ParseADBServerEndpoint(value string) (ADBServerEndpoint, error) {
	if value == "" || value != strings.TrimSpace(value) {
		return ADBServerEndpoint{}, fmt.Errorf("ADB server endpoint is required and must be trimmed")
	}
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return ADBServerEndpoint{}, err
	}
	ip := net.ParseIP(host)
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 || ip == nil || !ip.IsLoopback() {
		return ADBServerEndpoint{}, fmt.Errorf("ADB server endpoint must use a literal loopback address and valid port")
	}
	return ADBServerEndpoint{Host: ip.String(), Port: uint16(port)}, nil
}

func (e ADBServerEndpoint) Validate() error {
	ip := net.ParseIP(e.Host)
	if e.Port == 0 || ip == nil || !ip.IsLoopback() || e.Host != ip.String() {
		return fmt.Errorf("ADB server endpoint must use a canonical literal loopback address and valid port")
	}
	return nil
}

// ADBDeviceSelection is the exact target-driver-owned ADB authority supplied
// to a trusted observer. Serial is an argv value, never an option or template.
type ADBDeviceSelection struct {
	Server ADBServerEndpoint
	Serial string
}

func NewADBDeviceSelection(server ADBServerEndpoint, serial string) (ADBDeviceSelection, error) {
	selection := ADBDeviceSelection{Server: server, Serial: serial}
	if err := selection.Validate(); err != nil {
		return ADBDeviceSelection{}, err
	}
	return selection, nil
}

func (s ADBDeviceSelection) Validate() error {
	if err := s.Server.Validate(); err != nil {
		return err
	}
	return ValidateExactADBSerial(s.Serial)
}

func (s ADBDeviceSelection) IsZero() bool {
	return s.Server == (ADBServerEndpoint{}) && s.Serial == ""
}

// ValidateExactADBSerial accepts one bounded literal selector which cannot be
// interpreted as an adb option. It is shared by target and observer drivers so
// the authority producer and consumer cannot drift on selector safety.
func ValidateExactADBSerial(serial string) error {
	if serial == "" || len(serial) > 1024 || strings.HasPrefix(serial, "-") {
		return fmt.Errorf("safe exact ADB serial is required")
	}
	for _, character := range serial {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '.', '_', '-', ':':
			continue
		default:
			return fmt.Errorf("safe exact ADB serial is required")
		}
	}
	return nil
}

// ObservationAttachment is a target-driver observation handle. RuntimeID and
// any ADB device selection are authority-produced by PrepareRun and are never
// accepted from a public run request.
type ObservationAttachment struct {
	TargetKind domain.TargetKind
	RuntimeID  string
	ADBDevice  ADBDeviceSelection
}

func (a ObservationAttachment) Validate() error {
	if !a.TargetKind.IsValid() || a.RuntimeID == "" {
		return domain.NewError(domain.CodeInvalidArgument, "ports.observation_attachment.validate", "attachment", "target kind and runtime identity are required", nil)
	}
	if a.ADBDevice.IsZero() {
		return nil
	}
	if a.TargetKind != domain.TargetAndroidVirtualDevice {
		return domain.NewError(domain.CodeInvalidArgument, "ports.observation_attachment.validate", "adb_device", "is valid only for an Android virtual-device target", nil)
	}
	if err := a.ADBDevice.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, "ports.observation_attachment.validate", "adb_device", "exact ADB server and serial are invalid", err)
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
// compromise domain. PrepareStop arms the target's terminal boundary while
// leaving collection active; CancelStopPreparation rolls that arm back when
// the target produces no stop receipt. Coverage is queried independently of
// collector output.
type ObserverDriver interface {
	Probe(context.Context, ObservationRequirement) (domain.CapabilityFingerprint, error)
	Start(context.Context, CollectorPlan) (Collector, error)
	PrepareStop(context.Context, domain.CollectorID) error
	CancelStopPreparation(context.Context, domain.CollectorID) error
	Stop(context.Context, domain.CollectorID) (CollectorResult, error)
	Coverage(context.Context, domain.CollectorID) (domain.CollectorCoverage, error)
}
