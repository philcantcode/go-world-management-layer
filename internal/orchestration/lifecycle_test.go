package orchestration

import (
	"context"
	"io"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestQuarantineTargetRequiresPhysicalProofBeforeAtomicControlTransition(t *testing.T) {
	fixture := newIntegrationFixture(t)
	target, run := fixture.readyTargetAndRun()
	driver := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, nil)
	preparePhysicalTarget(t, fixture, driver, target, run)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runID, _ := domain.ParseTargetRunID(run.ID)
	transport, err := driver.OpenTransport(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	service := fixture.service(Config{Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver}})
	request := &worldv1.QuarantineTargetRequest{
		Mutation: fixture.wireMeta("quarantine"), TargetId: target.ID,
		ExpectedRevision: target.Revision, Reason: "contain active specimen",
	}
	result, err := service.QuarantineTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Generations) == 0 || result.Generations[0].State != string(domain.TargetGenerationQuarantined) {
		t.Fatalf("quarantine response generation = %#v", result.Generations)
	}
	if len(result.Runs) == 0 || result.Runs[0].State != string(domain.TargetRunQuarantined) {
		t.Fatalf("quarantine response run = %#v", result.Runs)
	}
	if _, err := transport.OpenADB(ctx); !domain.IsCode(err, domain.CodeCapabilityUnavailable) && err != io.ErrClosedPipe {
		// The fake transport normally reports closed-pipe after revocation. Keep
		// the capability result acceptable for transports that check kind first.
		t.Fatalf("quarantine left transport usable: %v", err)
	}
	if _, err := driver.OpenTransport(ctx, runID); err == nil {
		t.Fatal("quarantined physical run opened a new transport")
	}
	replayed, err := service.QuarantineTarget(context.Background(), request)
	if err != nil || replayed.Revision != result.Revision {
		t.Fatalf("QuarantineTarget(replay) = %#v, %v", replayed, err)
	}
	newKey := proto.Clone(request).(*worldv1.QuarantineTargetRequest)
	newKey.Mutation = fixture.wireMeta("quarantine-new-key")
	if _, err := service.QuarantineTarget(context.Background(), newKey); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("QuarantineTarget(new key) = %v, want AlreadyExists", err)
	}
	changed := proto.Clone(request).(*worldv1.QuarantineTargetRequest)
	changed.Reason = "different containment reason"
	if _, err := service.QuarantineTarget(context.Background(), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("QuarantineTarget(changed request) = %v, want AlreadyExists", err)
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
			target, _ := fixture.readyTargetAndRun()
			base := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, nil)
			driver := &quarantineOverrideDriver{TargetDriver: base, result: test.result, err: test.driverErr}
			if test.name == "unconfirmed" {
				targetID, parseErr := domain.ParseTargetID(target.ID)
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				driver.result.Target = ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)}
				driver.result.RuntimeID = "runtime-exact"
			}
			service := fixture.service(Config{Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver}})
			_, err := service.QuarantineTarget(context.Background(), &worldv1.QuarantineTargetRequest{
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
