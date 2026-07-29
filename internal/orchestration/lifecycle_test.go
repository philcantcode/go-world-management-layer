package orchestration

import (
	"context"
	"io"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestQuarantineTargetRequiresPhysicalProofBeforeAtomicControlTransition(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	driver := newLifecycleSagaTargetDriver(harness.target)
	harness.capabilities.targets[domain.TargetLinuxContainer] = driver
	harness.controller.targets[domain.TargetLinuxContainer] = driver
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("quarantine-active-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Keep transport checks on a separate, longer deadline so quarantine work
	// itself cannot expire the proof context under slow CI runners.
	transportCtx, transportCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer transportCancel()
	runID, _ := domain.ParseTargetRunID(run.ID)
	transport, err := driver.OpenTransport(transportCtx, runID)
	if err != nil {
		t.Fatal(err)
	}
	request := &worldv1.QuarantineTargetRequest{
		Mutation: fixture.wireMeta("quarantine"), TargetId: target.ID,
		ExpectedRevision: target.Revision, Reason: "contain active specimen",
	}
	result, err := harness.capabilities.QuarantineTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Generations) == 0 || result.Generations[0].State != string(domain.TargetGenerationQuarantined) {
		t.Fatalf("quarantine response generation = %#v", result.Generations)
	}
	if len(result.Runs) == 0 || result.Runs[0].State != string(domain.TargetRunCompleted) || result.Runs[0].BundleId == "" {
		t.Fatalf("quarantine response run = %#v", result.Runs)
	}
	if err := harness.capabilities.observers.RequireCommitted(runID); err != nil {
		t.Fatalf("quarantine did not commit observer finalization: %v", err)
	}
	if _, err := harness.capabilities.loadBundle(context.Background(), run.ID); err != nil {
		t.Fatalf("quarantine did not publish a retrievable observation bundle: %v", err)
	}
	if got := driver.callOrder(); len(got) != 2 || got[0] != "stop" || got[1] != "quarantine" {
		t.Fatalf("physical call order = %v, want [stop quarantine]", got)
	}
	if _, err := transport.OpenADB(transportCtx); !domain.IsCode(err, domain.CodeCapabilityUnavailable) && err != io.ErrClosedPipe {
		// The fake transport normally reports closed-pipe after revocation. Keep
		// the capability result acceptable for transports that check kind first.
		t.Fatalf("quarantine left transport usable: %v", err)
	}
	if _, err := driver.OpenTransport(transportCtx, runID); err == nil {
		t.Fatal("quarantined physical run opened a new transport")
	}
	replayed, err := harness.capabilities.QuarantineTarget(context.Background(), request)
	if err != nil || replayed.Revision != result.Revision {
		t.Fatalf("QuarantineTarget(replay) = %#v, %v", replayed, err)
	}
	newKey := proto.Clone(request).(*worldv1.QuarantineTargetRequest)
	newKey.Mutation = fixture.wireMeta("quarantine-new-key")
	if _, err := harness.capabilities.QuarantineTarget(context.Background(), newKey); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("QuarantineTarget(new key) = %v, want AlreadyExists", err)
	}
	changed := proto.Clone(request).(*worldv1.QuarantineTargetRequest)
	changed.Reason = "different containment reason"
	if _, err := harness.capabilities.QuarantineTarget(context.Background(), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("QuarantineTarget(changed request) = %v, want AlreadyExists", err)
	}
	_, controller := reloadLifecycleSaga(t, fixture, harness, driver)
	report := reconcileLifecycleSaga(t, controller)
	quarantines, destroys := driver.counts()
	if quarantines != 1 || destroys != 0 || len(report.RecoveredRuns) != 0 || len(report.RecoveredTargetQuarantines) != 0 {
		t.Fatalf("restart replayed completed quarantine work: calls=%d destroys=%d report=%#v", quarantines, destroys, report)
	}
}

func TestQuarantineTargetNeverAdvancesWithoutValidBackendEvidence(t *testing.T) {
	tests := []struct {
		name      string
		result    ports.TargetQuarantineEvidence
		driverErr error
		wantCode  codes.Code
	}{
		{name: "unsupported", driverErr: domain.NewError(domain.CodeCapabilityUnavailable, "test.quarantine", "backend", "unsupported", nil), wantCode: codes.FailedPrecondition},
		{name: "unconfirmed", result: ports.TargetQuarantineEvidence{ExecutionStopped: true, StatePreserved: true, ObservedAt: time.Unix(1, 0).UTC()}, wantCode: codes.DataLoss},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			driver := &quarantineOverrideDriver{TargetDriver: harness.target, result: test.result, err: test.driverErr}
			harness.capabilities.targets[domain.TargetLinuxContainer] = driver
			if test.name == "unconfirmed" {
				targetID, parseErr := domain.ParseTargetID(target.ID)
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				driver.result.Target = ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)}
				driver.result.RuntimeID = "runtime-exact"
			}
			_, err := harness.capabilities.QuarantineTarget(context.Background(), &worldv1.QuarantineTargetRequest{
				Mutation: fixture.wireMeta("quarantine-failure"), TargetId: target.ID,
				ExpectedRevision: target.Revision, Reason: "must not manufacture state",
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("QuarantineTarget() code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			stored, getErr := fixture.core.GetTarget(context.Background(), target.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			generation, _ := targetGeneration(stored)
			if generation.State == domain.TargetGenerationQuarantined {
				t.Fatal("control state advanced without valid physical evidence")
			}
		})
	}
}

type quarantineOverrideDriver struct {
	ports.TargetDriver
	result ports.TargetQuarantineEvidence
	err    error
}

func (d *quarantineOverrideDriver) Quarantine(context.Context, ports.TargetQuarantinePlan) (ports.TargetQuarantineEvidence, error) {
	return d.result, d.err
}
