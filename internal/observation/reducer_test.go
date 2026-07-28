package observation

import (
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
)

func TestReducerDistinguishesMetricStatesAndIsDeterministic(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	subjectID, _ := domain.NewSubjectID()
	collectorID, _ := domain.NewCollectorID()
	incidentID, _ := domain.NewIncidentID()
	subject := Subject{ID: subjectID, LeaseID: lease, Kind: domain.SubjectLinuxTarget, SignalFamily: TopologySignalFamily}
	zero := Present(0, time.Unix(10, 0))
	last := 7.0
	gap := ledger.Gap{Cause: ledger.GapCollectorLoss, FromCursor: 5, ThroughCursor: 5}
	events := []Event{
		{Cursor: 1, Kind: EventSubjectUpsert, LeaseID: lease, SignalFamily: TopologySignalFamily, SubjectID: subjectID, Subject: &subject},
		metricEvent(2, lease, subjectID, "zero", zero),
		metricEvent(3, lease, subjectID, "missing", Missing("not sampled")),
		metricEvent(4, lease, subjectID, "unsupported", Unsupported("not available")),
		metricEvent(5, lease, subjectID, "stale", Stale(&last, time.Second, "late")),
		metricEvent(6, lease, subjectID, "gap", GapState(gap, "collector lost")),
		{Cursor: 7, Kind: EventCoverageSet, LeaseID: lease, SignalFamily: "coverage", SubjectID: subjectID, Coverage: &CoverageView{LeaseID: lease, CollectorID: collectorID, SubjectID: subjectID, SignalFamily: "coverage", Placement: domain.CollectorPlacementHost, Level: domain.CoverageLevelPartial, Status: domain.CoverageLost, Gap: &gap}},
		{Cursor: 8, Kind: EventIncidentSet, LeaseID: lease, SignalFamily: "incidents", SubjectID: subjectID, Incident: &IncidentView{ID: incidentID, LeaseID: lease, SubjectID: subjectID, SignalFamily: "incidents", State: domain.IncidentOpen}},
		{Cursor: 9, Kind: EventPressureSet, LeaseID: lease, SignalFamily: "pressure", SubjectID: subjectID, Pressure: &PressureView{LeaseID: lease, SubjectID: subjectID, SignalFamily: "pressure", Resource: "memory", Level: PressureObserved, Value: 0.5}},
	}
	first, err := Reduce(events)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Reduce(events)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same prefix produced different snapshots")
	}
	wantKinds := map[string]MetricStateKind{"zero": MetricPresent, "missing": MetricMissing, "unsupported": MetricUnsupported, "stale": MetricStale, "gap": MetricGap}
	for name, want := range wantKinds {
		got := first.Metrics[MetricKey{SubjectID: subjectID, Name: name}].State
		if got.Kind != want {
			t.Fatalf("%s kind = %v, want %v", name, got.Kind, want)
		}
	}
	if value := first.Metrics[MetricKey{SubjectID: subjectID, Name: "zero"}].State.Value; value == nil || *value != 0 {
		t.Fatalf("numeric zero was not retained: %v", value)
	}
	projection := first.Project(NewAuthorization([]domain.LeaseID{lease}, []string{TopologySignalFamily, "metrics"}))
	if len(projection.Subjects) != 1 || len(projection.Metrics) != 5 || len(projection.Coverage) != 0 || len(projection.Incidents) != 0 || len(projection.Pressure) != 0 || projection.Cursor != first.Cursor {
		t.Fatalf("unexpected projection: %#v", projection)
	}
}

func TestProjectionDoesNotRevealAnotherLease(t *testing.T) {
	leaseA, _ := domain.NewLeaseID()
	leaseB, _ := domain.NewLeaseID()
	subjectA, _ := domain.NewSubjectID()
	subjectB, _ := domain.NewSubjectID()
	events := []Event{
		subjectEvent(1, leaseA, subjectA),
		subjectEvent(2, leaseB, subjectB),
		metricEvent(3, leaseA, subjectA, "cpu", Present(1, time.Now())),
		metricEvent(4, leaseB, subjectB, "cpu", Present(2, time.Now())),
	}
	snapshot, err := Reduce(events)
	if err != nil {
		t.Fatal(err)
	}
	projected := snapshot.Project(NewAuthorization([]domain.LeaseID{leaseA}, []string{"*"}))
	if len(projected.Subjects) != 1 || len(projected.Metrics) != 1 {
		t.Fatalf("projection leaked state: %#v", projected)
	}
	if _, found := projected.Subjects[subjectB]; found {
		t.Fatal("projection revealed another lease")
	}
}

func metricEvent(cursor domain.ObservationCursor, lease domain.LeaseID, subject domain.SubjectID, name string, state MetricState) Event {
	return Event{Cursor: cursor, Kind: EventMetricSet, LeaseID: lease, SignalFamily: "metrics", SubjectID: subject, MetricName: name, Metric: &state}
}

func subjectEvent(cursor domain.ObservationCursor, lease domain.LeaseID, id domain.SubjectID) Event {
	subject := Subject{ID: id, LeaseID: lease, Kind: domain.SubjectLinuxTarget, SignalFamily: TopologySignalFamily}
	return Event{Cursor: cursor, Kind: EventSubjectUpsert, LeaseID: lease, SignalFamily: TopologySignalFamily, SubjectID: id, Subject: &subject}
}
