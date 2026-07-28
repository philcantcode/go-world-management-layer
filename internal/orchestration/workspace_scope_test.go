package orchestration

import (
	"context"
	"testing"
)

func TestCoreWorkspaceResolverUsesActiveLeaseGeneration(t *testing.T) {
	fixture := newIntegrationFixture(t)
	resolver, err := NewCoreWorkspaceResolver(fixture.core)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resolver.ResolveWorkspace(context.Background(), fixture.view.Lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation := fixture.view.Agent.Generations[0]
	if scope.WorkspaceID.String() != generation.WorkspaceID || scope.AgentWorkspaceID.String() != fixture.view.Agent.ID || uint64(scope.AgentGeneration) != fixture.view.Lease.AgentGeneration || scope.AgentState != generation.State {
		t.Fatalf("scope = %#v", scope)
	}
}

func TestCoreWorkspaceResolverRejectsInvalidLease(t *testing.T) {
	fixture := newIntegrationFixture(t)
	resolver, err := NewCoreWorkspaceResolver(fixture.core)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveWorkspace(context.Background(), "not-a-lease"); err == nil {
		t.Fatal("invalid lease was accepted")
	}
}
