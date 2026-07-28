package observation

import (
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// Reducer applies accepted events in cursor order. It uses no wall clock or
// external state; stale transitions are explicit MetricStale events.
type Reducer struct {
	snapshot LiveSnapshot
}

func NewReducer() *Reducer {
	return &Reducer{snapshot: newSnapshot()}
}

// Reduce replays a prefix into a new snapshot.
func Reduce(events []Event) (LiveSnapshot, error) {
	reducer := NewReducer()
	for _, event := range events {
		if err := reducer.Apply(event); err != nil {
			return LiveSnapshot{}, err
		}
	}
	return reducer.Snapshot(), nil
}

// Apply changes the snapshot only after the complete event has validated.
func (reducer *Reducer) Apply(event Event) error {
	if event.Cursor == 0 || event.Cursor <= reducer.snapshot.Cursor {
		return fmt.Errorf("%w: got %d after %d", ErrCursorOrder, event.Cursor, reducer.snapshot.Cursor)
	}
	if err := reducer.validateEvent(event); err != nil {
		return err
	}

	switch event.Kind {
	case EventCheckpoint:
	case EventSubjectUpsert:
		subject := cloneSubject(*event.Subject)
		reducer.snapshot.Subjects[subject.ID] = subject
	case EventSubjectRemove:
		reducer.removeSubjectTree(event.SubjectID)
	case EventMetricSet:
		state := cloneMetricState(*event.Metric)
		state.UpdatedCursor = event.Cursor
		key := MetricKey{SubjectID: event.SubjectID, Name: event.MetricName}
		reducer.snapshot.Metrics[key] = MetricView{
			LeaseID:      event.LeaseID,
			SignalFamily: event.SignalFamily,
			SubjectID:    event.SubjectID,
			Name:         event.MetricName,
			State:        state,
		}
	case EventCoverageSet:
		coverage := cloneCoverage(*event.Coverage)
		coverage.UpdatedCursor = event.Cursor
		key := CoverageKey{CollectorID: coverage.CollectorID, SubjectID: coverage.SubjectID, SignalFamily: coverage.SignalFamily}
		reducer.snapshot.Coverage[key] = coverage
	case EventIncidentSet:
		incident := *event.Incident
		incident.UpdatedCursor = event.Cursor
		reducer.snapshot.Incidents[incident.ID] = incident
	case EventIncidentRemove:
		delete(reducer.snapshot.Incidents, event.Incident.ID)
	case EventPressureSet:
		pressure := *event.Pressure
		pressure.UpdatedCursor = event.Cursor
		key := PressureKey{SubjectID: pressure.SubjectID, Resource: pressure.Resource}
		reducer.snapshot.Pressure[key] = pressure
	case EventPressureRemove:
		key := PressureKey{SubjectID: event.Pressure.SubjectID, Resource: event.Pressure.Resource}
		delete(reducer.snapshot.Pressure, key)
	}
	reducer.snapshot.Cursor = event.Cursor
	return nil
}

func (reducer *Reducer) validateEvent(event Event) error {
	if event.Kind < EventCheckpoint || event.Kind > EventPressureRemove {
		return fmt.Errorf("%w: kind %d", ErrInvalidEvent, event.Kind)
	}
	if event.Kind == EventCheckpoint {
		return nil
	}
	if event.LeaseID.IsZero() {
		return fmt.Errorf("%w: lease ID is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.SignalFamily) == "" {
		return fmt.Errorf("%w: signal family is required", ErrInvalidEvent)
	}

	switch event.Kind {
	case EventSubjectUpsert:
		return reducer.validateSubjectUpsert(event)
	case EventSubjectRemove:
		return reducer.requireSubject(event.SubjectID, event.LeaseID)
	case EventMetricSet:
		if err := reducer.requireSubject(event.SubjectID, event.LeaseID); err != nil {
			return err
		}
		if strings.TrimSpace(event.MetricName) == "" || event.Metric == nil {
			return fmt.Errorf("%w: metric name and state are required", ErrInvalidEvent)
		}
		return event.Metric.validate()
	case EventCoverageSet:
		return reducer.validateCoverage(event)
	case EventIncidentSet, EventIncidentRemove:
		return reducer.validateIncident(event)
	case EventPressureSet, EventPressureRemove:
		return reducer.validatePressure(event)
	default:
		return fmt.Errorf("%w: unsupported event kind", ErrInvalidEvent)
	}
}

func (reducer *Reducer) validateSubjectUpsert(event Event) error {
	if event.Subject == nil {
		return fmt.Errorf("%w: subject is required", ErrInvalidEvent)
	}
	subject := event.Subject
	if subject.ID.IsZero() || subject.LeaseID != event.LeaseID || subject.SignalFamily != event.SignalFamily {
		return fmt.Errorf("%w: subject identity/envelope mismatch", ErrInvalidEvent)
	}
	if !subject.Kind.IsValid() {
		return fmt.Errorf("%w: invalid subject kind", ErrInvalidEvent)
	}
	for key := range subject.Labels {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%w: subject label key is blank", ErrInvalidEvent)
		}
	}
	if existing, found := reducer.snapshot.Subjects[subject.ID]; found && existing.LeaseID != subject.LeaseID {
		return fmt.Errorf("%w: subject lease cannot change", ErrInvalidEvent)
	}
	if subject.ParentID.IsZero() {
		return nil
	}
	parent, found := reducer.snapshot.Subjects[subject.ParentID]
	if !found {
		return fmt.Errorf("%w: parent", ErrUnknownSubject)
	}
	if parent.LeaseID != subject.LeaseID {
		return fmt.Errorf("%w: parent belongs to another lease", ErrInvalidEvent)
	}
	for current := subject.ParentID; !current.IsZero(); {
		if current == subject.ID {
			return ErrSubjectCycle
		}
		node, found := reducer.snapshot.Subjects[current]
		if !found {
			break
		}
		current = node.ParentID
	}
	return nil
}

func (reducer *Reducer) validateCoverage(event Event) error {
	coverage := event.Coverage
	if coverage == nil || coverage.CollectorID.IsZero() {
		return fmt.Errorf("%w: collector coverage is required", ErrInvalidEvent)
	}
	if coverage.LeaseID != event.LeaseID || coverage.SignalFamily != event.SignalFamily || coverage.SubjectID != event.SubjectID {
		return fmt.Errorf("%w: coverage/envelope mismatch", ErrInvalidEvent)
	}
	if !coverage.Placement.IsValid() || !coverage.Level.IsValid() || !coverage.Status.IsValid() {
		return fmt.Errorf("%w: invalid coverage placement, level, or status", ErrInvalidEvent)
	}
	if !coverage.SubjectID.IsZero() {
		if err := reducer.requireSubject(coverage.SubjectID, coverage.LeaseID); err != nil {
			return err
		}
	}
	if coverage.Status == domain.CoverageUnsupported && coverage.Level != domain.CoverageLevelNone {
		return fmt.Errorf("%w: unsupported coverage must have level none", ErrInvalidEvent)
	}
	if coverage.Level == domain.CoverageLevelComplete && (coverage.Dropped > 0 || coverage.Gap != nil || coverage.Status != domain.CoverageAvailable) {
		return fmt.Errorf("%w: complete coverage cannot contain loss", ErrInvalidEvent)
	}
	return nil
}

func (reducer *Reducer) validateIncident(event Event) error {
	incident := event.Incident
	if incident == nil || incident.ID.IsZero() {
		return fmt.Errorf("%w: incident is required", ErrInvalidEvent)
	}
	if incident.LeaseID != event.LeaseID || incident.SignalFamily != event.SignalFamily || incident.SubjectID != event.SubjectID {
		return fmt.Errorf("%w: incident/envelope mismatch", ErrInvalidEvent)
	}
	if event.Kind == EventIncidentSet && !incident.State.IsValid() {
		return fmt.Errorf("%w: invalid incident state", ErrInvalidEvent)
	}
	if !incident.SubjectID.IsZero() {
		return reducer.requireSubject(incident.SubjectID, incident.LeaseID)
	}
	return nil
}

func (reducer *Reducer) validatePressure(event Event) error {
	pressure := event.Pressure
	if pressure == nil || strings.TrimSpace(pressure.Resource) == "" {
		return fmt.Errorf("%w: pressure resource is required", ErrInvalidEvent)
	}
	if pressure.LeaseID != event.LeaseID || pressure.SignalFamily != event.SignalFamily || pressure.SubjectID != event.SubjectID {
		return fmt.Errorf("%w: pressure/envelope mismatch", ErrInvalidEvent)
	}
	if event.Kind == EventPressureSet {
		if pressure.Level < PressureNormal || pressure.Level > PressureCritical || !finite(pressure.Value) {
			return fmt.Errorf("%w: invalid pressure state", ErrInvalidEvent)
		}
	}
	if !pressure.SubjectID.IsZero() {
		return reducer.requireSubject(pressure.SubjectID, pressure.LeaseID)
	}
	return nil
}

func (reducer *Reducer) requireSubject(id domain.SubjectID, lease domain.LeaseID) error {
	if id.IsZero() {
		return fmt.Errorf("%w: zero subject ID", ErrUnknownSubject)
	}
	subject, found := reducer.snapshot.Subjects[id]
	if !found {
		return ErrUnknownSubject
	}
	if subject.LeaseID != lease {
		return fmt.Errorf("%w: subject belongs to another lease", ErrInvalidEvent)
	}
	return nil
}

func (reducer *Reducer) removeSubjectTree(root domain.SubjectID) {
	removed := map[domain.SubjectID]struct{}{root: {}}
	changed := true
	for changed {
		changed = false
		for id, subject := range reducer.snapshot.Subjects {
			if _, parentRemoved := removed[subject.ParentID]; parentRemoved {
				if _, alreadyRemoved := removed[id]; !alreadyRemoved {
					removed[id] = struct{}{}
					changed = true
				}
			}
		}
	}
	for id := range removed {
		delete(reducer.snapshot.Subjects, id)
	}
	for key := range reducer.snapshot.Metrics {
		if _, found := removed[key.SubjectID]; found {
			delete(reducer.snapshot.Metrics, key)
		}
	}
	for key := range reducer.snapshot.Coverage {
		if _, found := removed[key.SubjectID]; found {
			delete(reducer.snapshot.Coverage, key)
		}
	}
	for key := range reducer.snapshot.Pressure {
		if _, found := removed[key.SubjectID]; found {
			delete(reducer.snapshot.Pressure, key)
		}
	}
}

// Snapshot returns a defensive copy safe for concurrent clients to retain.
func (reducer *Reducer) Snapshot() LiveSnapshot {
	return cloneSnapshot(reducer.snapshot)
}

func cloneSnapshot(snapshot LiveSnapshot) LiveSnapshot {
	cloned := newSnapshot()
	cloned.Cursor = snapshot.Cursor
	for id, subject := range snapshot.Subjects {
		cloned.Subjects[id] = cloneSubject(subject)
	}
	for key, metric := range snapshot.Metrics {
		metric.State = cloneMetricState(metric.State)
		cloned.Metrics[key] = metric
	}
	for key, coverage := range snapshot.Coverage {
		cloned.Coverage[key] = cloneCoverage(coverage)
	}
	for id, incident := range snapshot.Incidents {
		cloned.Incidents[id] = incident
	}
	for key, pressure := range snapshot.Pressure {
		cloned.Pressure[key] = pressure
	}
	return cloned
}

func cloneSubject(subject Subject) Subject {
	subject.Labels = cloneStrings(subject.Labels)
	return subject
}

func cloneMetricState(state MetricState) MetricState {
	state.Value = cloneFloat(state.Value)
	state.LastValue = cloneFloat(state.LastValue)
	if state.Gap != nil {
		gap := *state.Gap
		state.Gap = &gap
	}
	return state
}

func cloneCoverage(coverage CoverageView) CoverageView {
	if coverage.Gap != nil {
		gap := *coverage.Gap
		coverage.Gap = &gap
	}
	return coverage
}
