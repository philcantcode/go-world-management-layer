// Package ledger implements the durable, append-only observation ledger and its
// resumable, non-blocking live fan-out.
package ledger

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

const (
	segmentFrameVersion   = uint16(1)
	recordEncodingVersion = uint16(1)
)

var (
	ErrClosed               = errors.New("ledger is closed")
	ErrCursorOutOfRange     = errors.New("cursor is beyond the ledger head")
	ErrCursorOrder          = errors.New("ledger cursor is not contiguous")
	ErrHashChain            = errors.New("ledger hash chain is invalid")
	ErrCorruptSegment       = errors.New("completed ledger frame is corrupt")
	ErrUnsupportedVersion   = errors.New("unsupported ledger frame version")
	ErrInvalidRecord        = errors.New("invalid ledger record")
	ErrMultipleOpenSegments = errors.New("multiple open ledger segments")
	ErrRecordTooLarge       = errors.New("record cannot fit in a segment")
	ErrRecoveryRequired     = errors.New("ledger requires close and reopen after an ambiguous durable write")
)

// Cursor is a stable, monotonically increasing position in the accepted
// ledger. Zero means that no record has yet been accepted.
type Cursor = domain.ObservationCursor

// RecordKind describes the semantic payload carried by a record.
type RecordKind uint8

const (
	RecordObservation RecordKind = iota + 1
	RecordMetric
	RecordControl
	RecordIncident
	RecordCoverage
	RecordPressure
	RecordTopology
	RecordGap
	RecordDuplicate
)

// OriginClass separates evidence-backed origins without deleting raw events.
type OriginClass uint8

const (
	OriginUnknown OriginClass = iota
	OriginAgentControl
	OriginAgentInstrumentation
	OriginSpecimen
	OriginSystem
	OriginMixedOrUnknown
)

// CollectorPlacement records where a collector executes.
type CollectorPlacement uint8

const (
	PlacementUnknown CollectorPlacement = iota
	PlacementHost
	PlacementRuntime
	PlacementGuest
	PlacementInjectedApp
)

// GapCause is a typed explanation for an absent interval.
type GapCause uint8

const (
	GapUnknown GapCause = iota
	GapCollectorOverflow
	GapCollectorRestart
	GapCompaction
	GapCollectorLoss
	GapSourceSequence
	GapSubscriberOverflow
	GapSegmentRepair
)

// Identity associates an event with independent agent and target generations.
// Empty fields and zero generations mean not applicable.
type Identity struct {
	ResearchSessionID string
	LeaseID           string
	AgentWorkspaceID  string
	AgentGeneration   uint64
	ExecID            string
	TargetID          string
	TargetGeneration  uint64
	TargetRunID       string
	TargetOperationID string
}

// CollectorContext describes coverage provenance.
type CollectorContext struct {
	ID        string
	Placement CollectorPlacement
	Coverage  string
}

// ProcessIdentity prevents a reused PID from being treated as the same process.
type ProcessIdentity struct {
	ID                int64
	StartTimeUnixNano int64
}

// CausalContext separates defined causation from evidence-based correlation.
type CausalContext struct {
	CausationID       string
	CorrelationID     string
	CorrelationMethod string
	Confidence        float64
}

// Gap describes an absent sequence or cursor interval. Through values are
// inclusive. Cursor ranges may be synthetic for one subscriber while the
// underlying durable records remain available for replay.
type Gap struct {
	Cause           GapCause
	Source          string
	SourceInstance  string
	FromSequence    uint64
	ThroughSequence uint64
	FromCursor      Cursor
	ThroughCursor   Cursor
	Detail          string
}

// Duplicate identifies a repeated source-local sequence.
type Duplicate struct {
	Source             string
	SourceInstance     string
	SourceSequence     uint64
	OriginalCursor     Cursor
	ConflictingPayload bool
}

// Record is the canonical unit persisted in a segment. Cursor and ChainHash
// are assigned by the ledger and must be zero on append.
type Record struct {
	Cursor                 Cursor
	Kind                   RecordKind
	EventID                string
	Identity               Identity
	SignalFamily           string
	SubjectID              string
	Source                 string
	SourceInstance         string
	HasSourceSequence      bool
	SourceSequence         uint64
	ObservedWallUnixNano   int64
	ObservedMonotonicNanos int64
	ClockDomain            string
	ClockSyncEpoch         uint64
	Collector              CollectorContext
	Process                ProcessIdentity
	PolicyDigest           string
	CapabilityDigest       string
	Origin                 OriginClass
	OriginEvidence         string
	Causal                 CausalContext
	Payload                []byte
	Gap                    *Gap
	Duplicate              *Duplicate
	ChainHash              [sha256.Size]byte
}

// AppendResult reports every durable record produced for one input. A source
// sequence jump produces a gap followed by the input record. A duplicate input
// produces a duplicate record instead of applying the observation twice.
type AppendResult struct {
	Records   []Record
	Duplicate bool
}

// SegmentMetadata is reconstructed from segment contents and persisted as an
// index sidecar. Path is a base name relative to the ledger directory.
type SegmentMetadata struct {
	Path         string            `json:"path"`
	FirstCursor  Cursor            `json:"firstCursor"`
	LastCursor   Cursor            `json:"lastCursor"`
	FrameCount   uint64            `json:"frameCount"`
	ByteSize     int64             `json:"byteSize"`
	PreviousHash [sha256.Size]byte `json:"previousHash"`
	FirstHash    [sha256.Size]byte `json:"firstHash"`
	LastHash     [sha256.Size]byte `json:"lastHash"`
	Finalized    bool              `json:"finalized"`
}

// Repair records a safe recovery action. Only an incomplete tail of an open
// segment is eligible for automatic truncation.
type Repair struct {
	Segment      string
	Offset       int64
	RemovedBytes int64
	Cause        GapCause
}

// OpenReport describes recovery and the authoritative reconstructed index.
type OpenReport struct {
	Repairs  []Repair
	Segments []SegmentMetadata
}

// CorruptionError pinpoints a completed frame that cannot be accepted.
type CorruptionError struct {
	Segment string
	Offset  int64
	Err     error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("%v %s at byte %d: %v", ErrCorruptSegment, e.Segment, e.Offset, e.Err)
}

func (e *CorruptionError) Unwrap() error { return ErrCorruptSegment }

// Validate checks invariants that can be established without interpreting the
// opaque payload.
func (record Record) Validate() error {
	if record.Cursor != 0 || record.ChainHash != ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: cursor and chain hash are ledger-assigned", ErrInvalidRecord)
	}
	return validateRecordShape(record)
}

func validateRecordShape(record Record) error {
	if record.Kind < RecordObservation || record.Kind > RecordDuplicate {
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidRecord, record.Kind)
	}
	if record.Kind == RecordGap {
		if record.Gap == nil || record.Duplicate != nil {
			return fmt.Errorf("%w: gap record requires only gap metadata", ErrInvalidRecord)
		}
	} else if record.Gap != nil {
		return fmt.Errorf("%w: non-gap record contains gap metadata", ErrInvalidRecord)
	}
	if record.Kind == RecordDuplicate {
		if record.Duplicate == nil || record.Gap != nil {
			return fmt.Errorf("%w: duplicate record requires only duplicate metadata", ErrInvalidRecord)
		}
	} else if record.Duplicate != nil {
		return fmt.Errorf("%w: non-duplicate record contains duplicate metadata", ErrInvalidRecord)
	}
	if record.Causal.CorrelationID != "" {
		if record.Causal.CorrelationMethod == "" || math.IsNaN(record.Causal.Confidence) || record.Causal.Confidence < 0 || record.Causal.Confidence > 1 {
			return fmt.Errorf("%w: correlation requires method and confidence in [0,1]", ErrInvalidRecord)
		}
	}
	if record.HasSourceSequence && (record.Source == "" || record.SourceInstance == "") {
		return fmt.Errorf("%w: source sequence requires source and source instance", ErrInvalidRecord)
	}
	return nil
}

func cloneRecord(record Record) Record {
	record.Payload = append([]byte(nil), record.Payload...)
	if record.Gap != nil {
		gap := *record.Gap
		record.Gap = &gap
	}
	if record.Duplicate != nil {
		duplicate := *record.Duplicate
		record.Duplicate = &duplicate
	}
	return record
}
