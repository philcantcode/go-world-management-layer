package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestProvisioningResultValidationRequiresAuthoritativePhysicalIdentity(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resolved, err := harness.resolver.ResolvePersistedAgent(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := currentAgentGeneration(view.Agent)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := bindAgentProvisioning(application.AcquireRequest{Meta: application.MutationMeta{IdempotencyKey: generation.AgentProvisioningKey}}, resolved, view)
	if err != nil {
		t.Fatal(err)
	}
	status, err := harness.agent.Inspect(ctx, ports.AgentWorkspaceRef{ID: plan.Agent.Generation.Spec().AgentWorkspaceID, Generation: plan.Agent.Generation.Spec().Generation})
	if err != nil {
		t.Fatal(err)
	}
	validAgent := ports.AgentWorkspaceResult{Status: status}
	if err := validateAgentProvisioningResult(plan.Agent, validAgent); err != nil {
		t.Fatalf("valid agent result: %v", err)
	}
	agentCases := map[string]func(*ports.AgentWorkspaceResult){
		"blank container": func(result *ports.AgentWorkspaceResult) { result.Status.ContainerID = "" },
		"zero protocol":   func(result *ports.AgentWorkspaceResult) { result.Status.GuestProtocol = 0 },
		"zero observed":   func(result *ports.AgentWorkspaceResult) { result.Status.ObservedAt = time.Time{} },
	}
	for name, mutate := range agentCases {
		t.Run("agent_"+name, func(t *testing.T) {
			result := validAgent
			mutate(&result)
			if err := validateAgentProvisioningResult(plan.Agent, result); err == nil {
				t.Fatal("invalid agent result was accepted")
			}
		})
	}
	agentWithoutHostCgroup := validAgent
	agentWithoutHostCgroup.Status.CgroupID = ""
	if err := validateAgentProvisioningResult(plan.Agent, agentWithoutHostCgroup); err != nil {
		t.Fatalf("agent result without host-visible cgroup observation: %v", err)
	}

	target := harness.createTarget(t, fixture, view)
	targetGeneration, err := targetGeneration(target)
	if err != nil {
		t.Fatal(err)
	}
	targetRef := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(targetGeneration.Generation)}
	targetPlan, err := harness.controller.resolvePersistedTargetProvisioningPlan(ctx, application.MutationMeta{
		IdempotencyKey: targetGeneration.ProvisioningKey, AuthorizedPolicyReference: targetGeneration.PolicyDigest, Deadline: time.Now().Add(time.Minute),
	}, target, targetGeneration.ProvisioningKey)
	if err != nil {
		t.Fatal(err)
	}
	targetResult, err := harness.target.Create(ctx, targetPlan)
	if err != nil {
		t.Fatal(err)
	}
	targetStatus := targetResult.Status
	if err := requireReadyTargetStatus(targetStatus, targetRef.ID, targetRef.Generation, domain.TargetLinuxContainer); err != nil {
		t.Fatalf("valid target result: %v", err)
	}
	targetCases := map[string]func(*ports.TargetStatus){
		"blank runtime": func(status *ports.TargetStatus) { status.RuntimeID = "" },
		"zero observed": func(status *ports.TargetStatus) { status.ObservedAt = time.Time{} },
	}
	for name, mutate := range targetCases {
		t.Run("target_"+name, func(t *testing.T) {
			status := targetStatus
			mutate(&status)
			if err := requireReadyTargetStatus(status, targetRef.ID, targetRef.Generation, domain.TargetLinuxContainer); err == nil {
				t.Fatal("invalid target result was accepted")
			}
		})
	}
	targetWithoutHostCgroup := targetStatus
	targetWithoutHostCgroup.CgroupID = ""
	if err := requireReadyTargetStatus(targetWithoutHostCgroup, targetRef.ID, targetRef.Generation, domain.TargetLinuxContainer); err != nil {
		t.Fatalf("Linux target result without host-visible cgroup observation: %v", err)
	}
	android := targetStatus
	android.Kind = domain.TargetAndroidVirtualDevice
	android.DeviceSerial = "serial"
	if err := requireReadyTargetStatus(android, targetRef.ID, targetRef.Generation, domain.TargetAndroidVirtualDevice); err != nil {
		t.Fatalf("valid Android result: %v", err)
	}
	android.DeviceSerial = ""
	if err := requireReadyTargetStatus(android, targetRef.ID, targetRef.Generation, domain.TargetAndroidVirtualDevice); err == nil {
		t.Fatal("Android result without device serial was accepted")
	}
}
