package orchestration

import (
	"context"
	"strings"
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type invalidResetResultDriver struct {
	ports.TargetDriver
	mutate func(*ports.TargetStatus)
}

func (d invalidResetResultDriver) Reset(ctx context.Context, targetID domain.TargetID, plan ports.ResetPlan) (ports.TargetResult, error) {
	result, err := d.TargetDriver.Reset(ctx, targetID, plan)
	if err == nil {
		d.mutate(&result.Status)
	}
	return result, err
}

func TestControllerRejectsContradictoryReadyResetStatus(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ports.TargetStatus)
	}{
		{name: "wrong target kind", mutate: func(status *ports.TargetStatus) { status.Kind = domain.TargetAndroidVirtualDevice }},
		{name: "non-ready lifecycle state", mutate: func(status *ports.TargetStatus) { status.State = domain.TargetGenerationQuarantined }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
				Meta: fixture.meta("invalid-reset-status-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := harness.capabilities.StopTargetRun(context.Background(), &worldv1.StopTargetRunRequest{
				Mutation: fixture.wireMeta("invalid-reset-status-stop"), TargetId: target.ID, TargetRunId: run.ID,
				ExpectedRevision: run.Revision, Reason: "prepare reset validation",
			}); err != nil {
				t.Fatal(err)
			}
			target, err = fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			harness.controller.targets[domain.TargetLinuxContainer] = invalidResetResultDriver{TargetDriver: harness.target, mutate: test.mutate}
			_, err = harness.controller.ResetTarget(context.Background(), application.ResetTargetRequest{
				Meta: fixture.meta("invalid-reset-status-reset"), TargetID: target.ID,
				ExpectedRevision: target.Revision, Mode: ports.ResetBaseline,
			})
			if err == nil || !strings.Contains(err.Error(), "target reset returned an invalid generation") {
				t.Fatalf("ResetTarget error = %v, want contradictory result rejection", err)
			}
			persisted, loadErr := fixture.core.GetTarget(context.Background(), target.ID)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			generation, generationErr := targetGeneration(persisted)
			if generationErr != nil {
				t.Fatal(generationErr)
			}
			if generation.State != domain.TargetGenerationFailed {
				t.Fatalf("contradictory physical reset advanced logical generation to %s, want failed", generation.State)
			}
		})
	}
}

var _ ports.TargetDriver = invalidResetResultDriver{}
