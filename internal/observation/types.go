// Package observation implements deterministic live-snapshot reduction and
// authorization projections over the shared durable ledger.
package observation

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
)

var (
	ErrCursorOrder    = errors.New("observation cursor must increase")
	ErrInvalidEvent   = errors.New("invalid observation event")
	ErrUnknownSubject = errors.New("observation subject is unknown")
	ErrSubjectCycle   = errors.New("subject topology would contain a cycle")
)

const TopologySignalFamily = "topology"

// EventKind determines which snapshot projection is changed.
type EventKind uint8

const (
	EventCheckpoint EventKind = iota + 1
	EventSubjectUpsert
	EventSubjectRemove
	EventMetricSet
	EventCoverageSet
	EventIncidentSet
	EventIncidentRemove
	EventPressureSet
	EventPressureRemove
)

// MetricStateKind is deliberately tagged so present numeric zero can never be
// confused with missing, unsupported, stale, or gap.
type MetricStateKind uint8

const (
	MetricPresent MetricStateKind = iota + 1
	MetricMissing
	MetricUnsupported
	MetricStale
	MetricGap
)

// MetricState is the latest state of one metric. Value is non-nil only when the
// state is present. LastValue is optional context for a stale state.
type MetricState struct {
	Kind          MetricStateKind
	Value         *float64
	LastValue     *float64
	CollectedAt   time.Time
	SampleAge     time.Duration
	Gap           *ledger.Gap
	Detail        string
	UpdatedCursor domain.ObservationCursor
}

// Present returns a numeric metric state, including an unambiguous zero.
func Present(value float64, collectedAt time.Time) MetricState {
	return MetricState{Kind: MetricPresent, Value: floatPointer(value), CollectedAt: collectedAt}
}

func Missing(detail string) MetricState {
	return MetricState{Kind: MetricMissing, Detail: detail}
}

func Unsupported(detail string) MetricState {
	return MetricState{Kind: MetricUnsupported, Detail: detail}
}

func Stale(lastValue *float64, age time.Duration, detail string) MetricState {
	return MetricState{Kind: MetricStale, LastValue: cloneFloat(lastValue), SampleAge: age, Detail: detail}
}

func GapState(gap ledger.Gap, detail string) MetricState {
	copy := gap
	return MetricState{Kind: MetricGap, Gap: &copy, Detail: detail}
}

func (state MetricState) validate() error {
	switch state.Kind {
	case MetricPresent:
		if state.Value == nil || !finite(*state.Value) || state.LastValue != nil || state.Gap != nil {
			return fmt.Errorf("%w: present metric requires one finite value", ErrInvalidEvent)
		}
	case MetricMissing, MetricUnsupported:
		if state.Value != nil || state.LastValue != nil || state.Gap != nil {
			return fmt.Errorf("%w: unavailable metric cannot carry a value or gap", ErrInvalidEvent)
		}
	case MetricStale:
		if state.Value != nil || state.Gap != nil || state.SampleAge < 0 || (state.LastValue != nil && !finite(*state.LastValue)) {
			return fmt.Errorf("%w: invalid stale metric", ErrInvalidEvent)
		}
	case MetricGap:
		if state.Value != nil || state.LastValue != nil || state.Gap == nil {
			return fmt.Errorf("%w: gap metric requires gap metadata and no value", ErrInvalidEvent)
		}
	default:
		return fmt.Errorf("%w: metric state %d", ErrInvalidEvent, state.Kind)
	}
	return nil
}

// Subject is one node in the current topology.
type Subject struct {
	ID           domain.SubjectID
	ParentID     domain.SubjectID
	LeaseID      domain.LeaseID
	Kind         domain.SubjectKind
	SignalFamily string
	Labels       map[string]string
}

// MetricKey identifies one measure for one subject.
type MetricKey struct {
	SubjectID domain.SubjectID
	Name      string
}

// MetricView includes authorization provenance with the tagged state.
type MetricView struct {
	LeaseID      domain.LeaseID
	SignalFamily string
	SubjectID    domain.SubjectID
	Name         string
	State        MetricState
}

type CoverageKey struct {
	CollectorID  domain.CollectorID
	SubjectID    domain.SubjectID
	SignalFamily string
}

// CoverageView is the current policy/collector coverage projection.
type CoverageView struct {
	LeaseID       domain.LeaseID
	CollectorID   domain.CollectorID
	SubjectID     domain.SubjectID
	SignalFamily  string
	Placement     domain.CollectorPlacement
	Level         domain.CoverageLevel
	Status        domain.CoverageStatus
	Required      bool
	Dropped       uint64
	Gap           *ledger.Gap
	UpdatedCursor domain.ObservationCursor
}

// IncidentView tracks active and recently resolved incidents at the snapshot
// cursor. State uses the domain transition vocabulary.
type IncidentView struct {
	ID            domain.IncidentID
	LeaseID       domain.LeaseID
	SubjectID     domain.SubjectID
	SignalFamily  string
	State         domain.IncidentState
	Summary       string
	UpdatedCursor domain.ObservationCursor
}

type PressureLevel uint8

const (
	PressureNormal PressureLevel = iota + 1
	PressureObserved
	PressureAdmissionStopped
	PressureShedding
	PressureCritical
)

type PressureKey struct {
	SubjectID domain.SubjectID
	Resource  string
}

// PressureView preserves the latest measured state and rationale.
type PressureView struct {
	LeaseID       domain.LeaseID
	SubjectID     domain.SubjectID
	SignalFamily  string
	Resource      string
	Level         PressureLevel
	Value         float64
	Detail        string
	UpdatedCursor domain.ObservationCursor
}

// Event is one deterministic reducer input. Cursor must be strictly greater
// than the prior applied event. Checkpoints advance the cursor without changing
// a projection, allowing every ledger prefix to be represented.
type Event struct {
	Cursor       domain.ObservationCursor
	Kind         EventKind
	LeaseID      domain.LeaseID
	SignalFamily string
	SubjectID    domain.SubjectID
	Subject      *Subject
	MetricName   string
	Metric       *MetricState
	Coverage     *CoverageView
	Incident     *IncidentView
	Pressure     *PressureView
}

// LiveSnapshot is the complete current projection at Cursor.
type LiveSnapshot struct {
	Cursor    domain.ObservationCursor
	Subjects  map[domain.SubjectID]Subject
	Metrics   map[MetricKey]MetricView
	Coverage  map[CoverageKey]CoverageView
	Incidents map[domain.IncidentID]IncidentView
	Pressure  map[PressureKey]PressureView
}

func newSnapshot() LiveSnapshot {
	return LiveSnapshot{
		Subjects:  make(map[domain.SubjectID]Subject),
		Metrics:   make(map[MetricKey]MetricView),
		Coverage:  make(map[CoverageKey]CoverageView),
		Incidents: make(map[domain.IncidentID]IncidentView),
		Pressure:  make(map[PressureKey]PressureView),
	}
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func floatPointer(value float64) *float64 { return &value }

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
