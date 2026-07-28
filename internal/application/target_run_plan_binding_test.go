package application

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestBindTargetRunPlanIsAtomicImmutableAndRestartSafe(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.readyAgent(t, view.Agent)
	target := fixture.readyTarget(t, view)
	run, err := fixture.core.StartTargetRun(context.Background(), StartTargetRunRequest{
		Meta: fixture.meta(t, "target-run-plan-start"), TargetID: target.ID,
		MaterializationDigest: domain.NewDigest([]byte("bound material")).String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := BindTargetRunPlanRequest{
		Meta: fixture.meta(t, "bind-target-run-plan"), TargetID: target.ID, RunID: run.ID, ExpectedRevision: run.Revision,
		ProvisioningPlanDigest: domain.NewDigest([]byte("exact target run plan")).String(), ProvisioningKey: "run/physical/prepare",
	}
	requireZeroProvisioningPlanDigestRejected(t, func(value string) error {
		invalid := request
		invalid.ProvisioningPlanDigest = value
		_, err := fixture.core.BindTargetRunPlan(context.Background(), invalid)
		return err
	})
	bound, err := fixture.core.BindTargetRunPlan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if bound.ProvisioningPlanDigest != request.ProvisioningPlanDigest || bound.ProvisioningKey != request.ProvisioningKey || bound.Revision != run.Revision+1 {
		t.Fatalf("bound run = %#v", bound)
	}

	replay := request
	replay.Meta.Deadline = fixture.now.Add(2 * time.Minute)
	replayed, err := fixture.core.BindTargetRunPlan(context.Background(), replay)
	if err != nil || replayed.Revision != bound.Revision {
		t.Fatalf("exact replay = %#v, %v", replayed, err)
	}
	changed := request
	changed.ProvisioningPlanDigest = domain.NewDigest([]byte("changed run plan")).String()
	if _, err := fixture.core.BindTargetRunPlan(context.Background(), changed); err == nil {
		t.Fatal("changed run plan reused the original idempotency key")
	}
	newKey := request
	newKey.Meta = fixture.meta(t, "second-target-run-bind-key")
	newKey.ExpectedRevision = bound.Revision
	if _, err := fixture.core.BindTargetRunPlan(context.Background(), newKey); err == nil {
		t.Fatal("target run accepted a second binding request")
	}

	reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatalf("restart rejected the one-time target run binding: %v", err)
	}
	recovered, err := reopened.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	recoveredRun, err := findRun(&recovered, run.ID)
	if err != nil || recoveredRun.ProvisioningPlanDigest != request.ProvisioningPlanDigest {
		t.Fatalf("restart run binding = %#v, %v", recoveredRun, err)
	}
}
