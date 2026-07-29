package orchestration

import (
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestTargetProvisioningPlanDigestBindsAndroidBootTimeout(t *testing.T) {
	plan := validAndroidProvisioningPlan(t)
	want, err := TargetProvisioningPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	changed := plan
	changed.Template.BootTimeout += time.Minute
	got, err := TargetProvisioningPlanDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if got == want {
		t.Fatal("Android boot timeout was omitted from the immutable provisioning identity")
	}
}

func TestTargetProvisioningPlanDigestBindsAndroidGuestMemory(t *testing.T) {
	plan := validAndroidProvisioningPlan(t)
	want, err := TargetProvisioningPlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	changed := plan
	changed.Template.GuestMemoryBytes = 3 << 30
	got, err := TargetProvisioningPlanDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if got == want {
		t.Fatal("Android guest memory was omitted from the immutable provisioning identity")
	}
}

func TestTargetProvisioningPlanDigestRejectsInvalidAndroidBaseline(t *testing.T) {
	plan := validAndroidProvisioningPlan(t)
	plan.Template.BaselineState = "clean-v2"
	if _, err := TargetProvisioningPlanDigest(plan); err == nil {
		t.Fatal("unsupported Android baseline entered an immutable provisioning identity")
	}
}

func validAndroidProvisioningPlan(t *testing.T) ports.TargetPlan {
	t.Helper()
	leaseID, err := domain.NewLeaseID()
	if err != nil {
		t.Fatal(err)
	}
	sessionID, err := domain.NewResearchSessionID()
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Unix(1_000, 0).UTC()
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, domain.InitialTargetGeneration, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := domain.NewDigest([]byte("android-policy"))
	capabilityDigest := domain.NewDigest([]byte("android-capabilities"))
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: domain.InitialTargetGeneration, PolicyDigest: policyDigest,
		CapabilityFingerprintDigest: capabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetPlan{
		IdempotencyKey: "android-provisioning", LeaseID: leaseID, Target: target, Generation: generation,
		Template: ports.TargetTemplate{
			Name: "android-managed", Kind: domain.TargetAndroidVirtualDevice, Driver: "android-emulator",
			ImageDigest: domain.NewDigest([]byte("android-system-image")), IsolationProfile: "instrumented-android",
			BaselineState: ports.AndroidBaselineCleanBoot, RequireHardwareAcceleration: true, Headless: true,
			Rooted: true, Debuggable: true, GuestMemoryBytes: 2 << 30, BootTimeout: 3 * time.Minute,
		},
		PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
		Resources: admission.Resources{CPUMilli: 2_000, MemoryBytes: 2 << 30, StorageBytes: 1 << 30},
	}
}
