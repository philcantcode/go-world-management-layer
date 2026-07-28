package application

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestBindTargetGenerationPlanIsAtomicImmutableAndRestartSafe(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	createMeta := fixture.meta(t, "target-plan-create")
	target, err := fixture.core.CreateTarget(context.Background(), CreateTargetRequest{
		Meta: createMeta, LeaseID: view.Lease.ID, Template: "linux-visible",
		Kind: domain.TargetLinuxContainer, PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if target.CreationIdempotencyKey != createMeta.IdempotencyKey {
		t.Fatalf("target creation identity = %q, want %q", target.CreationIdempotencyKey, createMeta.IdempotencyKey)
	}
	generation, err := findTargetGeneration(&target, target.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	request := BindTargetGenerationPlanRequest{
		Meta: fixture.meta(t, "bind-target-plan"), TargetID: target.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision,
		ProvisioningPlanDigest: domain.NewDigest([]byte("exact target plan")).String(), ProvisioningKey: "create/physical/target",
	}
	requireZeroProvisioningPlanDigestRejected(t, func(value string) error {
		invalid := request
		invalid.ProvisioningPlanDigest = value
		_, err := fixture.core.BindTargetGenerationPlan(context.Background(), invalid)
		return err
	})
	bound, err := fixture.core.BindTargetGenerationPlan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	boundGeneration, err := findTargetGeneration(&bound, bound.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if boundGeneration.ProvisioningPlanDigest != request.ProvisioningPlanDigest || boundGeneration.ProvisioningKey != request.ProvisioningKey || boundGeneration.Revision != generation.Revision+1 {
		t.Fatalf("bound generation = %#v", boundGeneration)
	}

	replay := request
	replay.Meta.Deadline = fixture.now.Add(2 * time.Minute)
	replayed, err := fixture.core.BindTargetGenerationPlan(context.Background(), replay)
	if err != nil || replayed.Revision != bound.Revision {
		t.Fatalf("exact replay = %#v, %v", replayed, err)
	}
	changed := request
	changed.ProvisioningPlanDigest = domain.NewDigest([]byte("changed target plan")).String()
	if _, err := fixture.core.BindTargetGenerationPlan(context.Background(), changed); err == nil {
		t.Fatal("changed plan reused the original idempotency key")
	}
	newKey := request
	newKey.Meta = fixture.meta(t, "second-target-bind-key")
	newKey.ExpectedRevision = boundGeneration.Revision
	if _, err := fixture.core.BindTargetGenerationPlan(context.Background(), newKey); err == nil {
		t.Fatal("generation accepted a second binding request")
	}

	reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatalf("restart rejected the one-time target plan binding: %v", err)
	}
	recovered, err := reopened.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	recoveredGeneration, err := findTargetGeneration(&recovered, recovered.CurrentGeneration)
	if err != nil || recoveredGeneration.ProvisioningPlanDigest != request.ProvisioningPlanDigest {
		t.Fatalf("restart binding = %#v, %v", recoveredGeneration, err)
	}
}
