package docker

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAgentPhysicalPolicyReportsOnlyObservedEnforcement(t *testing.T) {
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent"},
		Engine: &recordingEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.AgentWorkspacePhysicalPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.Runtime != dockercli.RuncRuntime || report.Runtime.User != defaultGuestUser || report.Runtime.SeccompProfile != dockercli.RuntimeDefaultSeccompProfile {
		t.Fatalf("runtime facts = %#v", report.Runtime)
	}
	for name, support := range map[string]ports.PhysicalSupport{
		"capabilities": report.Runtime.CapabilitySupport, "user": report.Runtime.UserSupport,
		"no-new-privileges": report.Runtime.NoNewPrivilegesSupport, "seccomp": report.Runtime.SeccompSupport,
		"network": report.Network.Support, "cpu": report.Resources.CPUMilli.Support,
		"memory": report.Resources.MemoryBytes.Support, "swap": report.Resources.SwapBytes.Support, "pids": report.Resources.PIDs.Support,
	} {
		if support != ports.PhysicalSupportEnforced {
			t.Fatalf("%s support = %q", name, support)
		}
	}
	if report.Resources.WorkspaceBytes.Support != ports.PhysicalSupportUnsupported || report.Resources.Inodes.Support != ports.PhysicalSupportUnsupported {
		t.Fatalf("bind-mount quotas were overstated: %#v", report.Resources)
	}
}

func TestAgentPlanPhysicalPolicyBindsExactResourcesAndImage(t *testing.T) {
	input := testAgentWorkspacePlan(t)
	input.Resources.SwapBytes = 8 << 20
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: t.TempDir(), ImageRepository: "example.invalid/agent"},
		Engine: &recordingEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.AgentWorkspacePlanPhysicalPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.ImageDigest != input.ImageDigest.String() || report.Resources.CPUMilli.Value != input.Resources.CPUMilli || report.Resources.SwapBytes.Value != input.Resources.SwapBytes {
		t.Fatalf("plan report = %#v", report)
	}
}
