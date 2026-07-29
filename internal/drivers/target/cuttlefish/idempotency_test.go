package cuttlefish

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAndroidCreateIdempotencyBindsFullTargetRequest(t *testing.T) {
	root := t.TempDir()
	port := findFreeEvenPortPair(t)
	allocator, err := NewDurableEmulatorAllocator(DurableEmulatorAllocatorConfig{
		StateRoot: filepath.Join(root, "allocator"), FirstConsolePort: port, LastConsolePort: port,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = allocator.Close() })
	input, build := reconciliationTargetPlan(t, root)
	backend := newStatefulBackend(Instance{})
	delete(backend.instances, "")
	driver := reconciliationDriver(t, build, backend, allocator, newRecordingFileGateway(emulatorAllocation(port).Serial))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Create(ctx, input); err != nil {
		t.Fatal(err)
	}
	actions := len(backend.Actions())
	if replay, err := driver.Create(ctx, input); err != nil || replay.Created {
		t.Fatalf("exact create replay = %#v, %v", replay, err)
	}

	otherLease, _ := domain.NewLeaseID()
	otherSession, _ := domain.NewResearchSessionID()
	otherTarget, err := domain.NewTarget(input.Target.ID(), otherSession, input.Target.Kind(), input.Target.CurrentGeneration(), input.Target.UpdatedAt())
	if err != nil {
		t.Fatal(err)
	}
	otherGenerationSpec := input.Generation.Spec()
	otherGenerationSpec.CreatedAt = otherGenerationSpec.CreatedAt.Add(time.Second)
	otherGeneration, err := domain.NewTargetGeneration(otherGenerationSpec)
	if err != nil {
		t.Fatal(err)
	}
	otherPolicy := domain.NewDigest([]byte("other-android-policy"))
	otherPolicySpec := input.Generation.Spec()
	otherPolicySpec.PolicyDigest = otherPolicy
	otherPolicyGeneration, err := domain.NewTargetGeneration(otherPolicySpec)
	if err != nil {
		t.Fatal(err)
	}
	otherCapability := domain.NewDigest([]byte("other-android-capability"))
	otherCapabilitySpec := input.Generation.Spec()
	otherCapabilitySpec.CapabilityFingerprintDigest = otherCapability
	otherCapabilityGeneration, err := domain.NewTargetGeneration(otherCapabilitySpec)
	if err != nil {
		t.Fatal(err)
	}

	variants := map[string]ports.TargetPlan{}
	changed := input
	changed.LeaseID = otherLease
	variants["lease"] = changed
	changed = input
	changed.Target = otherTarget
	variants["target_model"] = changed
	changed = input
	changed.Generation = otherGeneration
	variants["generation_model"] = changed
	changed = input
	changed.Template.Name += "-other"
	variants["template_name"] = changed
	changed = input
	changed.Template.Driver += "-other"
	variants["template_driver"] = changed
	changed = input
	changed.Template.ImageDigest = domain.NewDigest([]byte("other-android-image"))
	variants["template_image"] = changed
	changed = input
	changed.Template.BootTimeout += time.Second
	variants["boot_timeout"] = changed
	changed = input
	changed.PolicyDigest = otherPolicy
	changed.Generation = otherPolicyGeneration
	variants["policy_digest"] = changed
	changed = input
	changed.CapabilityFingerprintDigest = otherCapability
	changed.Generation = otherCapabilityGeneration
	variants["capability_digest"] = changed
	changed = input
	changed.Resources.CaptureBytes++
	variants["resources"] = changed

	for name, variant := range variants {
		t.Run(name, func(t *testing.T) {
			if err := variant.Validate(); err != nil {
				t.Fatalf("test variant is invalid: %v", err)
			}
			if _, err := driver.Create(ctx, variant); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed create replay error = %v", err)
			}
		})
	}
	if len(backend.Actions()) != actions {
		t.Fatalf("conflicting create replays mutated backend: %v", backend.Actions())
	}
}

func TestAndroidPrepareRunIdempotencyBindsFullRunRequest(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6570")
	driver, _ := materializationTestDriver(t, lease, target, files)
	input := targetRunPlanForMaterial(t, lease, target, nil, "android-run-idempotency")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, input); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.PrepareRun(ctx, input); err != nil {
		t.Fatalf("exact prepare replay: %v", err)
	}
	putCount := files.PutCount()

	longer := input
	longer.MaximumDuration += time.Second
	withCollector := input
	withCollector.RequiredCoverage = append([]string(nil), input.RequiredCoverage...)
	withCollector.RequiredCoverage = append(withCollector.RequiredCoverage, "android.logcat")
	withCollector.Collectors = []ports.CollectorSpec{requiredCollectorSpec("logcat", "android.logcat")}
	otherMaterial := input
	otherMaterial.Material = []ports.TargetMaterialPlan{targetMaterial(t, "fixture/other.bin", 0o640, []byte("fixture"), nil)}
	materialDigest, err := ports.TargetMaterializationDigest(otherMaterial.Material)
	if err != nil {
		t.Fatal(err)
	}
	runSpec := input.Run.Spec()
	runSpec.MaterializationDigest = materialDigest
	otherMaterial.Run, err = domain.NewTargetRun(runSpec)
	if err != nil {
		t.Fatal(err)
	}

	for name, variant := range map[string]ports.TargetRunPlan{
		"maximum_duration":    longer,
		"coverage_collectors": withCollector,
		"material":            otherMaterial,
	} {
		t.Run(name, func(t *testing.T) {
			if err := variant.Validate(); err != nil {
				t.Fatalf("test variant is invalid: %v", err)
			}
			if _, err := driver.PrepareRun(ctx, variant); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("changed prepare replay error = %v", err)
			}
		})
	}
	if files.PutCount() != putCount {
		t.Fatal("conflicting prepare replay re-materialized Android content")
	}
}
