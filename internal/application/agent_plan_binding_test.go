package application

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestBindAgentGenerationPlanIsAtomicImmutableAndRestartSafe(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	generation, err := findAgentGeneration(&view.Agent, view.Agent.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	request := BindAgentGenerationPlanRequest{
		Meta: fixture.meta(t, "bind-agent-plan"), AgentWorkspaceID: view.Agent.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision,
		ProvisioningPlanDigest:   domain.NewDigest([]byte("exact agent plan")).String(),
		WorkspaceProvisioningKey: "acquire/physical/workspace", AgentProvisioningKey: "acquire/physical/agent",
	}
	requireZeroProvisioningPlanDigestRejected(t, func(value string) error {
		invalid := request
		invalid.ProvisioningPlanDigest = value
		_, err := fixture.core.BindAgentGenerationPlan(context.Background(), invalid)
		return err
	})
	bound, err := fixture.core.BindAgentGenerationPlan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	boundGeneration, err := findAgentGeneration(&bound, bound.CurrentGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if boundGeneration.ProvisioningPlanDigest != request.ProvisioningPlanDigest ||
		boundGeneration.WorkspaceProvisioningKey != request.WorkspaceProvisioningKey ||
		boundGeneration.AgentProvisioningKey != request.AgentProvisioningKey || boundGeneration.Revision != generation.Revision+1 {
		t.Fatalf("bound generation = %#v", boundGeneration)
	}

	replay := request
	replay.Meta.Deadline = fixture.now.Add(2 * time.Minute)
	replayed, err := fixture.core.BindAgentGenerationPlan(context.Background(), replay)
	if err != nil || replayed.Revision != bound.Revision {
		t.Fatalf("exact replay = %#v, %v", replayed, err)
	}
	changed := request
	changed.ProvisioningPlanDigest = domain.NewDigest([]byte("changed plan")).String()
	if _, err := fixture.core.BindAgentGenerationPlan(context.Background(), changed); err == nil {
		t.Fatal("changed plan reused the original idempotency key")
	}
	newKey := request
	newKey.Meta = fixture.meta(t, "second-bind-key")
	newKey.ExpectedRevision = boundGeneration.Revision
	if _, err := fixture.core.BindAgentGenerationPlan(context.Background(), newKey); err == nil {
		t.Fatal("generation accepted a second binding request")
	}

	reopened, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatalf("restart rejected the one-time plan binding: %v", err)
	}
	recovered, err := reopened.GetResearchSession(context.Background(), view.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	recoveredGeneration, err := findAgentGeneration(&recovered.Agent, recovered.Agent.CurrentGeneration)
	if err != nil || recoveredGeneration.ProvisioningPlanDigest != request.ProvisioningPlanDigest {
		t.Fatalf("restart binding = %#v, %v", recoveredGeneration, err)
	}
}
