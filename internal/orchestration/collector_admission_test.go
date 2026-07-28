package orchestration

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

type uncheckedTargetRunResolver struct {
	ProvisioningResolver
	resolved ResolvedTargetRun
}

func (r uncheckedTargetRunResolver) ResolveTargetMaterial(context.Context, application.StartTargetRunRequest, application.TargetRecord) (ResolvedTargetRun, error) {
	return r.resolved, nil
}

func TestControllerRejectsInvalidCollectorNameBeforeAnyRunMutation(t *testing.T) {
	for name, collectorName := range map[string]string{
		"separator": "process/trace",
		"overlong":  strings.Repeat("x", ports.MaximumCollectorNameBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			targetFaults := testkit.NewFaultInjector()
			observerFaults := testkit.NewFaultInjector()
			harness := newControllerHarness(t, fixture, nil, targetFaults)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)

			harness.controller.observers.driver = testkit.NewFakeObserverDriver(domain.CapabilityFingerprint{}, fixture.clock, observerFaults, harness.tracker)
			digest, err := domain.ParseDigest(harness.materialDigest)
			if err != nil {
				t.Fatal(err)
			}
			resolved := resolvedTargetRun(harness.resolver.runs[harness.materialDigest], digest)
			resolved.RequiredCoverage = []string{ports.TargetLifecycleSignal, "process"}
			resolved.Collectors = []ports.CollectorSpec{{
				Name: collectorName,
				Requirement: ports.ObservationRequirement{
					SignalFamily: "process", Placement: domain.CollectorPlacementHost,
					MinimumLevel: domain.CoverageLevelComplete, Required: true,
				},
				Adapter: "fake", Version: "1", ConfigurationDigest: domain.NewDigest([]byte("observer config")), MaximumBytes: 1024,
			}}
			harness.controller.resolver = uncheckedTargetRunResolver{ProvisioningResolver: harness.controller.resolver, resolved: resolved}

			beforeTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeRecords, err := fixture.store.Records(context.Background(), 0, 10_000)
			if err != nil {
				t.Fatal(err)
			}
			beforeOwnership := harness.tracker.Snapshot()
			beforePrepareHits := targetFaults.Hits("target.prepare_run.before")
			beforeObserverHits := observerFaults.Hits("observer.start.before")

			run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
				Meta: fixture.meta("invalid-collector-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
			})
			if !domain.IsCode(err, domain.CodeInvalidArgument) {
				t.Fatalf("StartTargetRun() error = %v, want invalid argument", err)
			}
			if run.ID != "" {
				t.Fatalf("rejected collector returned logical run %#v", run)
			}

			afterTarget, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if !reflect.DeepEqual(afterTarget, beforeTarget) {
				t.Fatalf("invalid collector changed target control state:\nbefore=%#v\nafter=%#v", beforeTarget, afterTarget)
			}
			afterRecords, recordsErr := fixture.store.Records(context.Background(), 0, 10_000)
			if recordsErr != nil {
				t.Fatal(recordsErr)
			}
			if len(afterRecords) != len(beforeRecords) {
				t.Fatalf("invalid collector persisted %d unexpected control records", len(afterRecords)-len(beforeRecords))
			}
			if got := targetFaults.Hits("target.prepare_run.before"); got != beforePrepareHits {
				t.Fatalf("invalid collector reached target PrepareRun: before=%d after=%d", beforePrepareHits, got)
			}
			if got := observerFaults.Hits("observer.start.before"); got != beforeObserverHits {
				t.Fatalf("invalid collector reached observer Start: before=%d after=%d", beforeObserverHits, got)
			}
			if after := harness.tracker.Snapshot(); !reflect.DeepEqual(after, beforeOwnership) {
				t.Fatalf("invalid collector changed physical ownership:\nbefore=%#v\nafter=%#v", beforeOwnership, after)
			}

			releaseControllerSession(t, fixture, harness, view)
		})
	}
}
