package policyauthority

import (
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestBuildCapabilityFingerprintPreservesComponentsWithoutInventingFilesystemSupport(t *testing.T) {
	agent := componentFingerprint(t, map[string]policy.Capability{
		"agent.docker":             supportedCapability(t, nil),
		"agent.hardened-isolation": supportedCapability(t, nil),
	})
	target := componentFingerprint(t, map[string]policy.Capability{
		"target.linux-container":  supportedCapability(t, nil),
		"target.visibility-first": supportedCapability(t, map[string]string{"runtime": "runc"}),
	})
	facts := CapabilityFacts{
		HostOS: "windows", HostArchitecture: "amd64", WorkspaceMode: "directory-copy-non-production", DirectoryCopy: true,
		Components:        []CapabilityComponent{{Name: "linux-target", Fingerprint: target}, {Name: "agent", Fingerprint: agent}},
		IntrinsicCoverage: map[string][]string{"linux-container": {ports.TargetLifecycleSignal}},
	}
	first, err := BuildCapabilityFingerprint(facts)
	if err != nil {
		t.Fatal(err)
	}
	facts.Components[0], facts.Components[1] = facts.Components[1], facts.Components[0]
	second, err := BuildCapabilityFingerprint(facts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() {
		t.Fatal("component iteration order changed the combined fingerprint")
	}
	for name, expected := range map[string]policy.CapabilityStatus{
		"filesystem.directory-copy.non-production":  policy.CapabilitySupported,
		"filesystem.overlayfs":                      policy.CapabilityUnsupported,
		"filesystem.reflink":                        policy.CapabilityUnsupported,
		"runtime.oci.runc":                          policy.CapabilitySupported,
		"coverage.linux-container.target.lifecycle": policy.CapabilitySupported,
		"component.agent.agent.docker":              policy.CapabilitySupported,
	} {
		capability, found := first.Capability(name)
		if !found || capability.Status() != expected {
			t.Fatalf("capability %q = %#v, found=%t, want %s", name, capability, found, expected)
		}
	}
}

func TestBuildCapabilityFingerprintDerivesManagedAndroidPolicyVocabulary(t *testing.T) {
	virtual := supportedCapability(t, map[string]string{"hardware_acceleration": "true"})
	android, err := policy.NewCapabilityFingerprint(
		map[string]policy.Capability{"target.android-virtual": virtual},
		map[string]string{"managed": "true", "os": "android"},
	)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := BuildCapabilityFingerprint(CapabilityFacts{
		HostOS: "windows", HostArchitecture: "amd64", WorkspaceMode: "directory-copy-non-production", DirectoryCopy: true,
		Components:        []CapabilityComponent{{Name: "android-target", Fingerprint: android}},
		IntrinsicCoverage: map[string][]string{"android-virtual-device": {ports.TargetLifecycleSignal}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"target.kind.android-virtual-device", "runtime.driver.android-emulator",
		"runtime.isolation.instrumented-android", "android.hardware-acceleration",
		"coverage.android-virtual-device.target.lifecycle",
	} {
		capability, found := fingerprint.Capability(name)
		if !found || capability.Status() != policy.CapabilitySupported {
			t.Fatalf("managed Android capability %q = %#v, found=%t", name, capability, found)
		}
	}
}

func TestPhysicalPolicyFingerprintsNormalizeIdentityButBindEnforcementFacts(t *testing.T) {
	target := ports.TargetPhysicalPolicyReport{
		Template: "first", Kind: "linux-container", Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: "docker", Runtime: "runc", ImageDigest: "sha256:first", IsolationProfile: "observable-container",
			CapabilityDrop: []string{"NET_RAW", "ALL"},
		},
		DeniedInfrastructureAuthority: []string{"host-mounts", "docker-api"},
	}
	first, err := TargetPhysicalPolicyFingerprint(target)
	if err != nil {
		t.Fatal(err)
	}
	target.Template = "second"
	target.Runtime.ImageDigest = "sha256:second"
	target.Runtime.CapabilityDrop = []string{"ALL", "NET_RAW"}
	target.DeniedInfrastructureAuthority = []string{"docker-api", "host-mounts"}
	second, err := TargetPhysicalPolicyFingerprint(target)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() {
		t.Fatal("template identity or set ordering changed config-level physical fingerprint")
	}
	target.Runtime.NoNewPrivileges = true
	changed, err := TargetPhysicalPolicyFingerprint(target)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() == changed.Digest() {
		t.Fatal("an enforcement-fact change did not change the physical fingerprint")
	}
}

func TestTargetConfiguredResourcesPopulateAdmissionWithoutChangingCapabilityIdentity(t *testing.T) {
	report := ports.TargetPhysicalPolicyReport{
		Template: "linux-visible", Kind: "linux_container",
		Resources: ports.ContainerResourcePhysicalFacts{
			CPUMilli:           ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
			MemoryBytes:        ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
			SwapBytes:          ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
			WritableStateBytes: ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported},
			PIDs:               ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
		},
	}
	before, err := TargetPhysicalPolicyFingerprint(report)
	if err != nil {
		t.Fatal(err)
	}
	resources := admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, SwapBytes: 32 << 20, StorageBytes: 128 << 20, PIDs: 64}
	bound := WithTargetConfiguredResources(report, resources)
	if bound.Resources.CPUMilli.Value != resources.CPUMilli || bound.Resources.MemoryBytes.Value != resources.MemoryBytes || bound.Resources.SwapBytes.Value != resources.SwapBytes || bound.Resources.WritableStateBytes.Value != resources.StorageBytes || bound.Resources.PIDs.Value != resources.PIDs {
		t.Fatalf("configured target resource values were not propagated: %#v", bound.Resources)
	}
	after, err := TargetPhysicalPolicyFingerprint(bound)
	if err != nil {
		t.Fatal(err)
	}
	if before.Digest() != after.Digest() {
		t.Fatal("configured quota values changed the backend capability fingerprint")
	}
}

func TestAndroidTemplateGuestMemoryAndBootTimeoutDoNotChangePhysicalCapabilityIdentity(t *testing.T) {
	report := ports.TargetPhysicalPolicyReport{
		Template: "android-short-boot", Kind: "android_virtual_device",
		Android: ports.AndroidRuntimePhysicalFacts{
			SystemImageDigest: "sha256:image", BaselineState: ports.AndroidBaselineCleanBoot,
			HardwareAcceleration: true, HardwareAccelerationSupport: ports.PhysicalSupportEnforced,
			Headless: true, Rooted: true, Debuggable: true, GuestMemoryBytes: 2 << 30, BootTimeout: time.Minute,
		},
	}
	short, err := TargetPhysicalPolicyFingerprint(report)
	if err != nil {
		t.Fatal(err)
	}
	report.Template = "android-long-boot"
	report.Android.SystemImageDigest = "sha256:other-image"
	report.Android.GuestMemoryBytes = 3 << 30
	report.Android.BootTimeout = 5 * time.Minute
	long, err := TargetPhysicalPolicyFingerprint(report)
	if err != nil {
		t.Fatal(err)
	}
	if short.Digest() != long.Digest() {
		t.Fatal("per-template Android image, guest memory, or boot deadline changed the backend physical fingerprint")
	}
	report.Android.HardwareAccelerationSupport = ports.PhysicalSupportUnsupported
	changed, err := TargetPhysicalPolicyFingerprint(report)
	if err != nil {
		t.Fatal(err)
	}
	if short.Digest() == changed.Digest() {
		t.Fatal("an Android backend enforcement change did not change the physical fingerprint")
	}
}

func componentFingerprint(t *testing.T, capabilities map[string]policy.Capability) policy.CapabilityFingerprint {
	t.Helper()
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"component": "test"})
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func supportedCapability(t *testing.T, constraints map[string]string) policy.Capability {
	t.Helper()
	capability, err := policy.NewCapability(policy.CapabilitySupported, constraints, nil)
	if err != nil {
		t.Fatal(err)
	}
	return capability
}
