package orchestration

import (
	"context"
	"reflect"
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestBoundTargetResetPlanRejectsProfileDriftBeforePhysicalRetry(t *testing.T) {
	fixture := newIntegrationFixture(t)
	faults := testkit.NewFaultInjector()
	harness := newControllerHarness(t, fixture, nil, faults)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("bound-reset-run"), TargetID: target.ID,
		SpecimenOccurrenceRefs: append([]string(nil), harness.specimenRefs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.capabilities.StopTargetRun(context.Background(), &worldv1.StopTargetRunRequest{
		Mutation: fixture.wireMeta("bound-reset-stop"), TargetId: target.ID, TargetRunId: run.ID,
		ExpectedRevision: run.Revision, Reason: "prepare reset binding test",
	}); err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := application.ResetTargetRequest{
		Meta: fixture.meta("bound-reset"), TargetID: target.ID,
		ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
	}
	target, err = harness.controller.ResetTarget(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	if generation.Generation != 2 || generation.State != domain.TargetGenerationReady || generation.ProvisioningPlanDigest == "" || generation.ProvisioningKey != domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/reset") {
		t.Fatalf("reset generation has no exact immutable physical binding: %#v", generation)
	}

	beforeHits := faults.Hits("target.reset.before")
	beforeOwnership := harness.tracker.Snapshot()
	configured := harness.resolver.targets[target.Template]
	configured.Template.ImageDigest = domain.NewDigest([]byte("silently changed reset image"))
	harness.resolver.targets[target.Template] = configured
	if _, err := harness.controller.ResetTarget(context.Background(), request); err == nil {
		t.Fatal("exact reset retry accepted a changed target profile")
	}
	if got := faults.Hits("target.reset.before"); got != beforeHits {
		t.Fatalf("reset-plan drift reached the physical driver: before=%d after=%d", beforeHits, got)
	}
	if after := harness.tracker.Snapshot(); !reflect.DeepEqual(after, beforeOwnership) {
		t.Fatalf("reset-plan drift changed physical ownership:\nbefore=%#v\nafter=%#v", beforeOwnership, after)
	}
	persisted, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedGeneration, err := targetGeneration(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.CurrentGeneration != 2 || persistedGeneration.State != domain.TargetGenerationReady || persistedGeneration.ProvisioningPlanDigest != generation.ProvisioningPlanDigest || persistedGeneration.ProvisioningKey != generation.ProvisioningKey {
		t.Fatalf("profile drift changed the durable reset generation: %#v", persistedGeneration)
	}
}
