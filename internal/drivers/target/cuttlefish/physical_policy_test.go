package cuttlefish

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestManagedAndroidPhysicalReportPassesActualPolicyAdmission(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "examples", "environment-policy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	effective := compilePolicyWithAllCapabilities(t, source)
	document := effective.Policy()
	var selected policy.TargetTemplate
	for _, template := range document.Spec.Targets.Templates {
		if template.Kind == "android-virtual-device" {
			selected = template
			break
		}
	}
	imageDigest, err := domain.ParseDigest(selected.Runtime.SystemImageDigest)
	if err != nil {
		t.Fatal(err)
	}
	template := ports.TargetTemplate{
		Name: selected.Name, Kind: domain.TargetAndroidVirtualDevice, Driver: selected.Runtime.Driver,
		ImageDigest: imageDigest, IsolationProfile: selected.Runtime.IsolationProfile, BaselineState: selected.Runtime.BaselineState,
		RequireHardwareAcceleration: selected.Runtime.RequireHardwareAcceleration, Headless: selected.Runtime.Headless,
		Rooted: selected.Runtime.Rooted, Debuggable: selected.Runtime.Debuggable,
		GuestMemoryBytes: selected.Runtime.GuestMemory.Bytes(), BootTimeout: selected.Runtime.BootTimeout.Duration(),
	}
	resources := admission.Resources{
		CPUMilli: selected.Resources.Limits.CPU.MilliCPU(), MemoryBytes: selected.Resources.Limits.Memory.Bytes(),
		StorageBytes: selected.Resources.Limits.WritableState.Bytes(), SwapBytes: selected.Resources.Limits.Swap.Bytes(), PIDs: selected.Resources.Limits.PIDs,
	}
	input := physicalTargetPlan(t, template, resources)
	backendVersion, runtimeVersion := "Android emulator version 35.2.10", "google/sdk_gphone64_x86_64/emu35:userdebug/test-keys"
	driver := &Driver{
		build: BuildConfig{BackendVersion: backendVersion, RuntimeVersion: runtimeVersion},
		backend: physicalPolicyBackend{capabilities: BackendCapabilities{
			BackendKind: "android-sdk-emulator", BackendVersion: backendVersion, RuntimeVersion: runtimeVersion,
			Managed: true, KVM: true, KVMKnown: true, HardwareAcceleration: true, HardwareAccelerationKnown: true,
			Headless: true, HeadlessKnown: true, Rooted: true, RootedKnown: true, Debuggable: true, DebuggableKnown: true,
			CPUEnforced: true, MemoryEnforced: true, WritableStateEnforced: true,
			ResetModes: []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline}, Evidence: map[string]string{"os": "android", "managed": "true"},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := driver.TargetPlanPhysicalPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if report.Runtime.ImageDigest != imageDigest.String() || report.Android.SystemImageDigest != imageDigest.String() {
		t.Fatalf("physical report lost exact system image identity: %#v", report)
	}
	if err := policyauthority.ValidateTarget(effective, targetAdmissionFromReport(report)); err != nil {
		t.Fatalf("managed Android report failed actual policy admission: %v\nreport=%#v", err, report)
	}
}

func TestAttachedAndroidPhysicalReportCannotPassProductionEnforcement(t *testing.T) {
	template := completeAndroidTemplate("attached", "android-emulator", domain.NewDigest([]byte("attached-image")))
	capabilities := BackendCapabilities{
		BackendKind: "android-sdk-emulator", BackendVersion: "attached", RuntimeVersion: "device-build",
		Managed: false, Rooted: true, RootedKnown: true, Debuggable: true, DebuggableKnown: true,
	}
	report := androidPhysicalPolicyReport(template, capabilities, admission.Resources{CPUMilli: 1000, MemoryBytes: 1 << 30, StorageBytes: 1 << 30})
	if report.ResetSupport != ports.PhysicalSupportUnsupported || report.Resources.CPUMilli.Support == ports.PhysicalSupportEnforced || report.Resources.MemoryBytes.Support == ports.PhysicalSupportEnforced || report.WritableStateEnforced || report.Android.HardwareAccelerationSupport == ports.PhysicalSupportEnforced {
		t.Fatalf("attached backend claimed production enforcement: %#v", report)
	}
}

func TestAndroidProbeFingerprintNormalizesPerImageEvidenceToManagedConfig(t *testing.T) {
	backendVersion := "Android emulator version 36.3.10"
	runtimeVersion := "google/sdk_gphone64_x86_64/emu64xa:userdebug/dev-keys"
	configDigest := domain.NewDigest([]byte("complete managed image mapping"))
	driver := &Driver{
		build:   BuildConfig{BackendVersion: backendVersion, RuntimeVersion: runtimeVersion, DeviceConfigDigest: configDigest},
		backend: perImageEvidenceBackend{backendVersion: backendVersion, runtimeVersion: runtimeVersion},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := driver.Probe(ctx, completeAndroidTemplate("api-34", "android-emulator", domain.NewDigest([]byte("image-34"))))
	if err != nil {
		t.Fatal(err)
	}
	second, err := driver.Probe(ctx, completeAndroidTemplate("api-35", "android-emulator", domain.NewDigest([]byte("image-35"))))
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() {
		t.Fatalf("per-image observations changed common driver fingerprint: %s != %s", first.Digest(), second.Digest())
	}
	evidence := first.Evidence()
	if evidence["device_config_digest"] != configDigest.String() {
		t.Fatalf("probe evidence omitted managed configuration identity: %#v", evidence)
	}
	for _, key := range []string{"system_image_package", "system_image_directory", "system_image_digest"} {
		if _, found := evidence[key]; found {
			t.Fatalf("probe evidence retained per-template %s: %#v", key, evidence)
		}
	}
}

type physicalPolicyBackend struct {
	Backend
	capabilities BackendCapabilities
}

func (b physicalPolicyBackend) ValidateResourceEnforcement(context.Context, admission.Resources) error {
	return nil
}

type perImageEvidenceBackend struct {
	Backend
	backendVersion string
	runtimeVersion string
}

func (b perImageEvidenceBackend) Probe(_ context.Context, template ports.TargetTemplate) (BackendCapabilities, error) {
	return BackendCapabilities{
		BackendKind: "android-sdk-emulator", BackendVersion: b.backendVersion, RuntimeVersion: b.runtimeVersion,
		Managed: true, HardwareAcceleration: true, HardwareAccelerationKnown: true,
		Headless: true, HeadlessKnown: true, Rooted: true, RootedKnown: true, Debuggable: true, DebuggableKnown: true,
		ResetModes: []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline},
		Evidence: map[string]string{
			"os": "android", "managed": "true", "runtime_fingerprint": b.runtimeVersion,
			"system_image_package":   "system-images;" + template.Name,
			"system_image_directory": filepath.Join("sdk", template.Name), "system_image_digest": template.ImageDigest.String(),
		},
	}, nil
}

func (b physicalPolicyBackend) Probe(context.Context, ports.TargetTemplate) (BackendCapabilities, error) {
	return b.capabilities, nil
}

func physicalTargetPlan(t *testing.T, template ports.TargetTemplate, resources admission.Resources) ports.TargetPlan {
	t.Helper()
	targetID, _ := domain.NewTargetID()
	sessionID, _ := domain.NewResearchSessionID()
	leaseID, _ := domain.NewLeaseID()
	createdAt := time.Now().UTC()
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, 1, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := domain.NewDigest([]byte("physical-policy"))
	capabilityDigest := domain.NewDigest([]byte("physical-capability"))
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID: targetID, Generation: 1, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetPlan{
		IdempotencyKey: "android-physical-plan", LeaseID: leaseID, Target: target, Generation: generation, Template: template,
		PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, Resources: resources,
	}
}

func compilePolicyWithAllCapabilities(t *testing.T, source []byte) *policy.EffectivePolicy {
	t.Helper()
	requirements, err := policy.Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := make(map[string]policy.Capability, len(requirements))
	for _, requirement := range requirements {
		capability, err := policy.NewCapability(policy.CapabilitySupported, requirement.Constraints, nil)
		if err != nil {
			t.Fatal(err)
		}
		capabilities[requirement.Name] = capability
	}
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"test": "physical-admission"})
	if err != nil {
		t.Fatal(err)
	}
	effective, err := policy.Compile(source, policy.CompileOptions{Capabilities: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	return effective
}

func targetAdmissionFromReport(report ports.TargetPhysicalPolicyReport) policyauthority.TargetAdmission {
	return policyauthority.TargetAdmission{
		Template: report.Template, Kind: report.Kind, Driver: report.Runtime.Driver, Runtime: report.Runtime.Runtime,
		ImageDigest: report.Runtime.ImageDigest, IsolationProfile: report.Runtime.IsolationProfile, BaseImage: report.Runtime.BaseImage,
		User: report.Runtime.User, CapabilityDrop: append([]string(nil), report.Runtime.CapabilityDrop...), CapabilityAdd: append([]string(nil), report.Runtime.CapabilityAdd...),
		NoNewPrivileges: report.Runtime.NoNewPrivileges, SeccompProfile: report.Runtime.SeccompProfile,
		UserEnforced:       report.Runtime.UserEnforced && report.Runtime.UserSupport == ports.PhysicalSupportEnforced,
		SeccompEnforced:    report.Runtime.SeccompEnforced && report.Runtime.SeccompSupport == ports.PhysicalSupportEnforced,
		MaterialMountPoint: report.MaterialMountPoint, WritableStateMode: report.WritableStateMode,
		WritableStateEnforced: report.WritableStateEnforced && report.Resources.WritableStateBytes.Support == ports.PhysicalSupportEnforced,
		CommandAuthority:      report.CommandAuthority, ExecTransport: report.ExecTransport, FileTransfer: report.FileTransfer,
		NetworkEndpoints: report.NetworkEndpoints, ADB: report.ADB, DeviceScopedADBServices: report.DeviceScopedADBServices,
		DeniedInfrastructureAuthority: append([]string(nil), report.DeniedInfrastructureAuthority...), ResetAfterEveryRun: report.ResetAfterEveryRun,
		ResetMode: report.ResetMode, BaselineState: report.Android.BaselineState, RequireHardwareAcceleration: report.Android.HardwareAcceleration,
		HardwareAccelerationEnforced: report.Android.HardwareAccelerationSupport == ports.PhysicalSupportEnforced,
		Headless:                     report.Android.Headless, Rooted: report.Android.Rooted, Debuggable: report.Android.Debuggable,
		GuestMemoryBytes: report.Android.GuestMemoryBytes, BootTimeout: report.Android.BootTimeout,
		Resources: policyauthority.RuntimeResources{
			CPUMilli: report.Resources.CPUMilli.Value, MemoryBytes: report.Resources.MemoryBytes.Value, SwapBytes: report.Resources.SwapBytes.Value,
			WritableStateBytes: report.Resources.WritableStateBytes.Value, CaptureBytes: report.Resources.CaptureBytes.Value,
			Inodes: report.Resources.Inodes.Value, PIDs: report.Resources.PIDs.Value,
		},
	}
}

var _ Backend = physicalPolicyBackend{}
var _ Backend = perImageEvidenceBackend{}
