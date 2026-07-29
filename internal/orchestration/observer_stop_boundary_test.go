package orchestration

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestRunObserverCoordinatorRollsBackAmbiguousStopPreparationWithoutLosingEvidence(t *testing.T) {
	clock := testkit.NewClock(time.Unix(1_800_090_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	faults := testkit.NewFaultInjector()
	tracker := testkit.NewOwnershipTracker()
	trace := &stopBoundaryTrace{}
	driver := &stopBoundaryObserverDriver{
		inner: testkit.NewFakeObserverDriver(observerTestFingerprint(t), clock, faults, tracker),
		trace: trace,
	}
	coordinator, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: driver, Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now,
		StateRoot: filepath.Join(t.TempDir(), "observer-state"), CleanupTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := observerTestStart(t, clock.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.Start(ctx, input); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("prepare returned after arming the collector")
	faults.FailNext("observer.prepare_stop.after", injected)
	if err := coordinator.PrepareStop(ctx, input.Plan.Run.ID()); !errors.Is(err, injected) {
		t.Fatalf("PrepareStop() error = %v, want %v", err, injected)
	}
	if got, want := trace.snapshot(), []string{"observer.prepare.failed", "observer.cancel"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ambiguous preparation rollback order = %v, want %v", got, want)
	}
	markers, err := coordinator.loadMarkers()
	if err != nil || len(markers) != 1 || markers[0].Phase != "active" {
		t.Fatalf("durable phase after rollback = %#v, %v", markers, err)
	}
	collectorID := driver.collectorID(t)
	coverage, err := driver.Coverage(ctx, collectorID)
	if err != nil || coverage.Spec().Status != domain.CoverageAvailable {
		t.Fatalf("collector coverage after rollback = %#v, %v", coverage, err)
	}

	if err := coordinator.PrepareStop(ctx, input.Plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	markers, err = coordinator.loadMarkers()
	if err != nil || len(markers) != 1 || markers[0].Phase != "stop-prepared" {
		t.Fatalf("durable prepared phase = %#v, %v", markers, err)
	}
	clock.Advance(time.Second)
	evidence, err := coordinator.Finalize(ctx, observerTestReceipt(t, input, clock.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Failures) != 0 || len(evidence.Artifacts) < 2 {
		t.Fatalf("evidence after rolled-back preparation = %#v", evidence)
	}
	if got, want := trace.snapshot(), []string{"observer.prepare.failed", "observer.cancel", "observer.prepare", "observer.stop"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry boundary order = %v, want %v", got, want)
	}
	if ownership := tracker.Snapshot(); len(ownership) != 0 {
		t.Fatalf("finalized collector ownership = %#v", ownership)
	}
}

func TestServiceArmsCollectorsBeforeTargetStopAndCancelsOnTargetFailure(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	trace := &stopBoundaryTrace{}
	observer := &stopBoundaryObserverDriver{
		inner: testkit.NewFakeObserverDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, harness.tracker),
		trace: trace,
	}
	harness.controller.observers.driver = observer

	digest, err := domain.ParseDigest(harness.materialDigest)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedTargetRun(harness.resolver.runs[harness.materialDigest], digest)
	resolved.RequiredCoverage = []string{ports.TargetLifecycleSignal, "process"}
	resolved.Collectors = []ports.CollectorSpec{{
		Name: "process-trace",
		Requirement: ports.ObservationRequirement{
			SignalFamily: "process", Placement: domain.CollectorPlacementHost,
			MinimumLevel: domain.CoverageLevelComplete, Required: true,
		},
		Adapter: "fake", Version: "1", ConfigurationDigest: domain.NewDigest([]byte("boundary-observer-config")), MaximumBytes: 1024,
	}}
	harness.controller.resolver = uncheckedTargetRunResolver{ProvisioningResolver: harness.controller.resolver, resolved: resolved}

	injected := errors.New("target stop failed before a receipt")
	targetDriver := &stopBoundaryTargetDriver{TargetDriver: harness.target, trace: trace, failNext: injected}
	harness.controller.targets[domain.TargetLinuxContainer] = targetDriver
	harness.capabilities.targets[domain.TargetLinuxContainer] = targetDriver
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("observer-boundary-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &worldv1.StopTargetRunRequest{
		Mutation: fixture.wireMeta("observer-boundary-stop"), TargetId: target.ID,
		TargetRunId: run.ID, ExpectedRevision: run.Revision, Reason: "prove two-phase observer stop ordering",
	}
	if _, err := harness.capabilities.StopTargetRun(context.Background(), request); !errors.Is(err, injected) {
		t.Fatalf("first StopTargetRun() error = %v, want %v", err, injected)
	}
	if got, want := trace.snapshot(), []string{"observer.prepare", "target.stop.failed", "observer.cancel"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failed target stop order = %v, want %v", got, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	coverage, err := observer.Coverage(ctx, observer.collectorID(t))
	if err != nil || coverage.Spec().Status != domain.CoverageAvailable {
		t.Fatalf("collector did not remain available after target rollback: %#v, %v", coverage, err)
	}

	bundle, err := harness.capabilities.StopTargetRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := trace.snapshot(), []string{
		"observer.prepare", "target.stop.failed", "observer.cancel",
		"observer.prepare", "target.stop", "observer.stop",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("successful target stop order = %v, want %v", got, want)
	}
	if !bundleHasCompleteRequiredCoverage(bundle, "process") || !bundleHasCollectorArtifact(bundle) {
		t.Fatalf("retry silently lost collector evidence: %#v", bundle)
	}
}

type stopBoundaryTrace struct {
	mu    sync.Mutex
	calls []string
}

func (t *stopBoundaryTrace) add(call string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, call)
}

func (t *stopBoundaryTrace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.calls...)
}

type stopBoundaryObserverDriver struct {
	inner ports.ObserverDriver
	trace *stopBoundaryTrace

	mu         sync.Mutex
	collectors []domain.CollectorID
}

func (d *stopBoundaryObserverDriver) Probe(ctx context.Context, requirement ports.ObservationRequirement) (domain.CapabilityFingerprint, error) {
	return d.inner.Probe(ctx, requirement)
}

func (d *stopBoundaryObserverDriver) Start(ctx context.Context, plan ports.CollectorPlan) (ports.Collector, error) {
	collector, err := d.inner.Start(ctx, plan)
	if err == nil {
		d.mu.Lock()
		d.collectors = append(d.collectors, collector.ID)
		d.mu.Unlock()
	}
	return collector, err
}

func (d *stopBoundaryObserverDriver) PrepareStop(ctx context.Context, id domain.CollectorID) error {
	err := d.inner.PrepareStop(ctx, id)
	if err != nil {
		d.trace.add("observer.prepare.failed")
		return err
	}
	d.trace.add("observer.prepare")
	return nil
}

func (d *stopBoundaryObserverDriver) CancelStopPreparation(ctx context.Context, id domain.CollectorID) error {
	err := d.inner.CancelStopPreparation(ctx, id)
	if err != nil {
		d.trace.add("observer.cancel.failed")
		return err
	}
	d.trace.add("observer.cancel")
	return nil
}

func (d *stopBoundaryObserverDriver) Stop(ctx context.Context, id domain.CollectorID) (ports.CollectorResult, error) {
	result, err := d.inner.Stop(ctx, id)
	if err != nil {
		d.trace.add("observer.stop.failed")
		return result, err
	}
	d.trace.add("observer.stop")
	return result, nil
}

func (d *stopBoundaryObserverDriver) Coverage(ctx context.Context, id domain.CollectorID) (domain.CollectorCoverage, error) {
	return d.inner.Coverage(ctx, id)
}

func (d *stopBoundaryObserverDriver) collectorID(t *testing.T) domain.CollectorID {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.collectors) != 1 {
		t.Fatalf("started collector IDs = %v, want exactly one", d.collectors)
	}
	return d.collectors[0]
}

type stopBoundaryTargetDriver struct {
	ports.TargetDriver
	trace *stopBoundaryTrace

	mu       sync.Mutex
	failNext error
}

func (d *stopBoundaryTargetDriver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	d.mu.Lock()
	failure := d.failNext
	d.failNext = nil
	d.mu.Unlock()
	if failure != nil {
		d.trace.add("target.stop.failed")
		return ports.TargetRunStopReceipt{}, failure
	}
	receipt, err := d.TargetDriver.StopRun(ctx, runID, mode)
	if err != nil {
		d.trace.add("target.stop.failed")
		return receipt, err
	}
	d.trace.add("target.stop")
	return receipt, nil
}

func bundleHasCompleteRequiredCoverage(bundle *worldv1.ObservationBundle, family string) bool {
	for _, coverage := range bundle.Coverage {
		if coverage.SignalFamily == family && coverage.Required && coverage.Level == string(domain.CoverageLevelComplete) && coverage.Status == string(domain.CoverageAvailable) && coverage.Gap == nil {
			return true
		}
	}
	return false
}

func bundleHasCollectorArtifact(bundle *worldv1.ObservationBundle) bool {
	for _, artifact := range bundle.RawArtifacts {
		if artifact.Role == "raw-observation" {
			return true
		}
	}
	return false
}
