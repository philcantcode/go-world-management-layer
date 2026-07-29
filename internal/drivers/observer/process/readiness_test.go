package process

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestCommandReadinessUsesScopedNonAmbientEnvironment(t *testing.T) {
	t.Setenv("WORLD_AMBIENT_SECRET", "must-not-be-inherited")
	plan := validCollectorPlan(t)
	var invocation command.Invocation
	readiness := CommandReadiness{
		Runner: runnerFunc(func(_ context.Context, value command.Invocation) (command.Result, error) {
			invocation = value
			return command.Result{}, nil
		}),
		Program: "trusted-readiness", Args: []string{"get-state"}, Interval: time.Millisecond,
	}
	ctx, cancel := testContext(t)
	defer cancel()
	if err := readiness.Await(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if invocation.Program != readiness.Program || !slices.Equal(invocation.Args, readiness.Args) ||
		!slices.Contains(invocation.Environment, "WORLD_TARGET_RUN_ID="+plan.TargetRunID.String()) ||
		slices.Contains(invocation.Environment, "WORLD_AMBIENT_SECRET=must-not-be-inherited") {
		t.Fatalf("readiness invocation = %#v", invocation)
	}
}
