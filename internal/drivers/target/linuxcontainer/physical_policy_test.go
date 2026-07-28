package linuxcontainer

import (
	"context"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestTargetPhysicalPolicyReportsInteractionAndStorageBoundaries(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("target-image")), IsolationProfile: "observable-container",
	}
	driver := physicalPolicyTargetDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.TargetPhysicalPolicy(ctx, template)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.User != defaultTargetUser || report.Runtime.BaseImage != "readOnly" || report.Runtime.SeccompProfile != dockercli.RuntimeDefaultSeccompProfile {
		t.Fatalf("target runtime facts = %#v", report.Runtime)
	}
	if report.WritableStateMode != "private-directory-non-production" || report.WritableStateEnforced || report.Resources.WritableStateBytes.Support != ports.PhysicalSupportUnsupported {
		t.Fatalf("writable-state facts = %#v", report)
	}
	if report.NetworkEndpoints != "none" || report.ResetAfterEveryRun || report.InteractionSupport != ports.PhysicalSupportEnforced || report.ResetSupport != ports.PhysicalSupportEnforced {
		t.Fatalf("interaction/reset facts = %#v", report)
	}
	for name, support := range map[string]ports.PhysicalSupport{
		"capabilities": report.Runtime.CapabilitySupport, "user": report.Runtime.UserSupport,
		"no-new-privileges": report.Runtime.NoNewPrivilegesSupport, "seccomp": report.Runtime.SeccompSupport,
		"cpu": report.Resources.CPUMilli.Support, "memory": report.Resources.MemoryBytes.Support,
		"swap": report.Resources.SwapBytes.Support, "pids": report.Resources.PIDs.Support,
	} {
		if support != ports.PhysicalSupportEnforced {
			t.Fatalf("%s support = %q", name, support)
		}
	}
}

func TestTargetPlanPhysicalPolicyBindsExactResources(t *testing.T) {
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("target-image")))
	input.Resources.SwapBytes = 8 << 20
	input.Resources.StorageBytes = 16 << 20
	input.Resources.Inodes = 1024
	driver := physicalPolicyTargetDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.TargetPlanPhysicalPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.ImageDigest != input.Template.ImageDigest.String() || report.Resources.SwapBytes.Value != input.Resources.SwapBytes || report.Resources.WritableStateBytes.Value != input.Resources.StorageBytes || report.Resources.Inodes.Value != input.Resources.Inodes {
		t.Fatalf("target plan report = %#v", report)
	}
}

func TestTargetPhysicalPolicyDoesNotClaimMissingSeccomp(t *testing.T) {
	template := ports.TargetTemplate{
		Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: dockercli.RuncRuntime,
		ImageDigest: domain.NewDigest([]byte("target-image")), IsolationProfile: "observable-container",
	}
	runtime := physicalPolicyRuntime{capabilities: supportedTargetRuntimeCapabilities()}
	runtime.capabilities.SecurityOptions = nil
	driver, err := New(Config{
		Build: BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target"}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.TargetPhysicalPolicy(ctx, template)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.SeccompEnforced || report.Runtime.SeccompSupport != ports.PhysicalSupportUnsupported {
		t.Fatalf("missing seccomp was overstated: %#v", report.Runtime)
	}
}

func physicalPolicyTargetDriver(t *testing.T) *Driver {
	t.Helper()
	driver, err := New(Config{
		Build:      BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target"},
		Runtime:    physicalPolicyRuntime{capabilities: supportedTargetRuntimeCapabilities()},
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func supportedTargetRuntimeCapabilities() RuntimeCapabilities {
	return RuntimeCapabilities{
		OSType: "linux", Runtimes: []string{dockercli.RuncRuntime}, SecurityOptions: []string{"name=seccomp,profile=builtin"},
		CPUCFSQuota: true, MemoryLimit: true, SwapLimit: true, PIDsLimit: true,
	}
}

type physicalPolicyRuntime struct {
	noopRuntime
	capabilities RuntimeCapabilities
}

func (r physicalPolicyRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return r.capabilities, nil
}
