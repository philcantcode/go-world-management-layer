package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type fakeCollectorRecord struct {
	plan         ports.CollectorPlan
	collector    ports.Collector
	coverage     domain.CollectorCoverage
	result       ports.CollectorResult
	stopPrepared bool
	stopped      bool
}

// FakeObserverDriver models collectors as resources owned by their target run.
// Stop is idempotent and preserves the final coverage record for later queries.
type FakeObserverDriver struct {
	mu           sync.Mutex
	capabilities domain.CapabilityFingerprint
	clock        *Clock
	faults       *FaultInjector
	tracker      *OwnershipTracker
	requests     map[string]string
	collectors   map[domain.CollectorID]*fakeCollectorRecord
}

func NewFakeObserverDriver(capabilities domain.CapabilityFingerprint, clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeObserverDriver {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeObserverDriver{
		capabilities: capabilities, clock: clock, faults: faults, tracker: tracker,
		requests: make(map[string]string), collectors: make(map[domain.CollectorID]*fakeCollectorRecord),
	}
}

func (d *FakeObserverDriver) Probe(ctx context.Context, requirement ports.ObservationRequirement) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "fake_observer.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := requirement.Validate(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := d.faults.Check("observer.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	return d.capabilities, nil
}

func (d *FakeObserverDriver) Start(ctx context.Context, plan ports.CollectorPlan) (ports.Collector, error) {
	if err := ports.RequireDeadline(ctx, "fake_observer.start"); err != nil {
		return ports.Collector{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.Collector{}, err
	}
	if err := d.faults.Check("observer.start.before"); err != nil {
		return ports.Collector{}, err
	}
	signature := fmt.Sprintf("%s/%s/%s/%s/%s", plan.CollectorID, plan.TargetRunID, plan.Requirement.SignalFamily, plan.Adapter, plan.ConfigurationDigest)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, found := d.requests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.Collector{}, idempotencyConflict("fake_observer.start")
		}
		return d.collectors[plan.CollectorID].collector, nil
	}
	if _, found := d.collectors[plan.CollectorID]; found {
		return ports.Collector{}, domain.NewError(domain.CodeAlreadyExists, "fake_observer.start", "collector_id", "already exists", nil)
	}
	if err := d.tracker.Acquire("collector", plan.CollectorID.String(), plan.TargetRunID.String()); err != nil {
		return ports.Collector{}, err
	}
	coverage, err := domain.NewCollectorCoverage(domain.CollectorCoverageSpec{
		CollectorID: plan.CollectorID, SignalFamily: plan.Requirement.SignalFamily,
		Placement: plan.Requirement.Placement, Level: plan.Requirement.MinimumLevel,
		Status: collectorCoverageStatus(plan.Requirement.MinimumLevel), Required: plan.Requirement.Required,
		StartedAt: plan.StartedAt, EndedAt: plan.StartedAt,
	})
	if err != nil {
		_ = d.tracker.Release("collector", plan.CollectorID.String(), plan.TargetRunID.String())
		return ports.Collector{}, err
	}
	collector := ports.Collector{ID: plan.CollectorID, TargetRunID: plan.TargetRunID, SignalFamily: plan.Requirement.SignalFamily, StartedAt: plan.StartedAt}
	d.collectors[plan.CollectorID] = &fakeCollectorRecord{plan: plan, collector: collector, coverage: coverage}
	d.requests[plan.IdempotencyKey] = signature
	if err := d.faults.Check("observer.start.after"); err != nil {
		return ports.Collector{}, err
	}
	return collector, nil
}

func (d *FakeObserverDriver) PrepareStop(ctx context.Context, collectorID domain.CollectorID) error {
	if err := requireFakeCollectorOperation(ctx, collectorID, "fake_observer.prepare_stop"); err != nil {
		return err
	}
	if err := d.faults.Check("observer.prepare_stop.before"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireCollector(collectorID, "fake_observer.prepare_stop")
	if err != nil {
		return err
	}
	if record.stopped {
		return nil
	}
	record.stopPrepared = true
	return d.faults.Check("observer.prepare_stop.after")
}

func (d *FakeObserverDriver) CancelStopPreparation(ctx context.Context, collectorID domain.CollectorID) error {
	if err := requireFakeCollectorOperation(ctx, collectorID, "fake_observer.cancel_stop_preparation"); err != nil {
		return err
	}
	if err := d.faults.Check("observer.cancel_stop_preparation.before"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireCollector(collectorID, "fake_observer.cancel_stop_preparation")
	if err != nil {
		return err
	}
	if record.stopped {
		return nil
	}
	record.stopPrepared = false
	return d.faults.Check("observer.cancel_stop_preparation.after")
}

func (d *FakeObserverDriver) Stop(ctx context.Context, collectorID domain.CollectorID) (ports.CollectorResult, error) {
	if err := requireFakeCollectorOperation(ctx, collectorID, "fake_observer.stop"); err != nil {
		return ports.CollectorResult{}, err
	}
	if err := d.faults.Check("observer.stop.before"); err != nil {
		return ports.CollectorResult{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireCollector(collectorID, "fake_observer.stop")
	if err != nil {
		return ports.CollectorResult{}, err
	}
	if record.stopped {
		return record.result, nil
	}
	stoppedAt := d.clock.Now()
	coverageSpec := record.coverage.Spec()
	coverageSpec.EndedAt = stoppedAt
	coverage, err := domain.NewCollectorCoverage(coverageSpec)
	if err != nil {
		return ports.CollectorResult{}, err
	}
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "memory://collector/" + collectorID.String(), Digest: record.plan.ConfigurationDigest,
		Size: 0, Role: "raw-observation", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		return ports.CollectorResult{}, err
	}
	record.coverage = coverage
	record.result = ports.CollectorResult{
		CollectorID: collectorID, Coverage: coverage, Artifacts: []domain.ArtifactReference{artifact},
		StoppedAt: stoppedAt, TeardownConfirmed: true,
	}
	record.stopPrepared = false
	record.stopped = true
	_ = d.tracker.Release("collector", collectorID.String(), record.plan.TargetRunID.String())
	if err := d.faults.Check("observer.stop.after"); err != nil {
		return ports.CollectorResult{}, err
	}
	return record.result, nil
}

func (d *FakeObserverDriver) Coverage(ctx context.Context, collectorID domain.CollectorID) (domain.CollectorCoverage, error) {
	if err := requireFakeCollectorOperation(ctx, collectorID, "fake_observer.coverage"); err != nil {
		return domain.CollectorCoverage{}, err
	}
	if err := d.faults.Check("observer.coverage"); err != nil {
		return domain.CollectorCoverage{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, err := d.requireCollector(collectorID, "fake_observer.coverage")
	if err != nil {
		return domain.CollectorCoverage{}, err
	}
	return record.coverage, nil
}

func requireFakeCollectorOperation(ctx context.Context, collectorID domain.CollectorID, operation string) error {
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return err
	}
	if collectorID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "collector_id", "must be set", nil)
	}
	return nil
}

func (d *FakeObserverDriver) requireCollector(collectorID domain.CollectorID, operation string) (*fakeCollectorRecord, error) {
	record, found := d.collectors[collectorID]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, operation, "collector_id", "not found", nil)
	}
	return record, nil
}

func collectorCoverageStatus(level domain.CoverageLevel) domain.CoverageStatus {
	if level == domain.CoverageLevelNone {
		return domain.CoverageUnsupported
	}
	return domain.CoverageAvailable
}

var _ ports.ObserverDriver = (*FakeObserverDriver)(nil)
