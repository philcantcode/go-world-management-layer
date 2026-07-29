package ports

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestTargetTemplateSeparatesLinuxAndAndroidRuntimeFields(t *testing.T) {
	digest := domain.NewDigest([]byte("system-image"))
	linux := TargetTemplate{
		Name: "linux", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: "runc",
		ImageDigest: digest, IsolationProfile: "observable-container",
	}
	if err := linux.Validate(); err != nil {
		t.Fatal(err)
	}
	missingRuntime := linux
	missingRuntime.Runtime = ""
	if err := missingRuntime.Validate(); err == nil {
		t.Fatal("Linux template without an OCI runtime was accepted")
	}
	androidField := linux
	androidField.BaselineState = AndroidBaselineCleanBoot
	if err := androidField.Validate(); err == nil {
		t.Fatal("Linux template with an Android-only field was accepted")
	}

	android := TargetTemplate{
		Name: "android", Kind: domain.TargetAndroidVirtualDevice, Driver: "android-emulator",
		ImageDigest: digest, IsolationProfile: "instrumented-android", BaselineState: AndroidBaselineCleanBoot,
		RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true,
		GuestMemoryBytes: 2 << 30, BootTimeout: time.Minute,
	}
	if err := android.Validate(); err != nil {
		t.Fatal(err)
	}
	linuxRuntime := android
	linuxRuntime.Runtime = "runc"
	if err := linuxRuntime.Validate(); err == nil {
		t.Fatal("Android template with a Linux runtime was accepted")
	}
	for _, baseline := range []string{"", "snapshot-v1", " clean-boot", "clean-boot "} {
		invalidBaseline := android
		invalidBaseline.BaselineState = baseline
		if err := invalidBaseline.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Errorf("Android template with baseline %q error = %v, want invalid argument", baseline, err)
		}
	}
}

func TestTargetMaterializationDigestIsExactAndOrderStable(t *testing.T) {
	first := targetMaterial(t, "artifact://one", "one.bin", []byte("one"))
	second := targetMaterial(t, "artifact://two", "nested/two.bin", []byte("two"))
	want, err := TargetMaterializationDigest([]TargetMaterialPlan{first, second})
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := TargetMaterializationDigest([]TargetMaterialPlan{second, first})
	if err != nil || reordered != want {
		t.Fatalf("reordered digest = %s, %v; want %s", reordered, err, want)
	}
	changed := first
	changed.LogicalPath = "renamed.bin"
	changedDigest, err := TargetMaterializationDigest([]TargetMaterialPlan{changed, second})
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == want {
		t.Fatal("logical-path change did not change the materialization digest")
	}
	if _, err := TargetMaterializationDigest([]TargetMaterialPlan{first, first}); err == nil {
		t.Fatal("duplicate logical path was accepted")
	}
}

func TestTargetMaterialRejectsContentIdentityMismatch(t *testing.T) {
	material := targetMaterial(t, "artifact://one", "one.bin", []byte("one"))
	material.Content = testContentSource{content: []byte("different")}
	if err := material.Validate(); err == nil {
		t.Fatal("content with a different digest and size was accepted")
	}
}

func TestTargetRunPlanValidatesCanonicalCollectorChildIdentities(t *testing.T) {
	material := []TargetMaterialPlan{targetMaterial(t, "artifact://one", "one.bin", []byte("one"))}
	materializationDigest, err := TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := domain.NewTargetRunID()
	leaseID, _ := domain.NewLeaseID()
	targetID, _ := domain.NewTargetID()
	agentID, _ := domain.NewAgentWorkspaceID()
	run, err := domain.NewTargetRun(domain.TargetRunSpec{
		ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: 1,
		AgentWorkspaceID: agentID, AgentGeneration: 1, MaterializationDigest: materializationDigest,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := TargetRunPlan{
		IdempotencyKey: strings.Repeat("p", domain.MaximumIdempotencyKeyBytes), Run: run,
		RequiredCoverage: []string{TargetLifecycleSignal, "process"},
		Collectors: []CollectorSpec{{
			Name: strings.Repeat("n", MaximumCollectorNameBytes),
			Requirement: ObservationRequirement{
				SignalFamily: "process", Placement: domain.CollectorPlacementHost,
				MinimumLevel: domain.CoverageLevelComplete, Required: true,
			},
			Adapter: "test.process", Version: "1", ConfigurationDigest: domain.NewDigest([]byte("config")), MaximumBytes: 1024,
		}},
		Material: material, MaximumDuration: time.Minute,
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("maximum parent and collector name did not derive a valid plan identity: %v", err)
	}
	if key := DeriveCollectorIdempotencyKey(plan.IdempotencyKey, plan.Collectors[0].Name); !domain.IsCanonicalIdempotencyKey(key) {
		t.Fatalf("validated plan produced non-canonical collector key %q", key)
	}
	plan.Collectors[0].Name = "process/trace"
	if err := plan.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("plan with ambiguous collector name error = %v, want invalid argument", err)
	}
}

func TestValidateResetSelectionAcceptsOnlyUnambiguousPublicModes(t *testing.T) {
	valid := []struct {
		mode     ResetMode
		snapshot string
	}{
		{mode: ResetBaseline},
		{mode: ResetRecreate},
		{mode: ResetSnapshot, snapshot: "known-good"},
	}
	for _, value := range valid {
		if err := ValidateResetSelection(value.mode, value.snapshot); err != nil {
			t.Errorf("%s/%q rejected: %v", value.mode, value.snapshot, err)
		}
	}
	invalid := []struct {
		mode     ResetMode
		snapshot string
	}{
		{mode: ""},
		{mode: "unknown"},
		{mode: ResetSnapshot},
		{mode: ResetSnapshot, snapshot: "  "},
		{mode: ResetSnapshot, snapshot: " known-good"},
		{mode: ResetBaseline, snapshot: "ignored"},
		{mode: ResetRecreate, snapshot: "ignored"},
	}
	for _, value := range invalid {
		if err := ValidateResetSelection(value.mode, value.snapshot); err == nil {
			t.Errorf("%s/%q was accepted", value.mode, value.snapshot)
		}
	}
}

func TestTargetQuarantineEvidenceRequiresExactConfirmedGeneration(t *testing.T) {
	target, _ := domain.NewTargetID()
	other, _ := domain.NewTargetID()
	ref := TargetRef{ID: target, Generation: 2}
	plan := TargetQuarantinePlan{IdempotencyKey: "quarantine-1", Target: ref, Reason: "suspected compromise"}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	evidence := TargetQuarantineEvidence{
		Target: ref, RuntimeID: "runtime-exact", ExecutionStopped: true,
		NetworkUnreachable: true, StatePreserved: true, ObservedAt: time.Now().UTC(),
	}
	if err := evidence.Validate(ref); err != nil {
		t.Fatal(err)
	}
	evidence.Target.ID = other
	if err := evidence.Validate(ref); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("wrong-generation evidence error = %v", err)
	}
	evidence.Target = ref
	evidence.NetworkUnreachable = false
	if err := evidence.Validate(ref); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("unconfirmed containment error = %v", err)
	}
}

func targetMaterial(t *testing.T, reference, logicalPath string, content []byte) TargetMaterialPlan {
	t.Helper()
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: reference, Digest: domain.NewDigest(content), Size: int64(len(content)),
		Role: "target-input", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		t.Fatal(err)
	}
	return TargetMaterialPlan{Artifact: artifact, LogicalPath: logicalPath, Mode: 0o444, Content: testContentSource{content: append([]byte(nil), content...)}}
}

type testContentSource struct{ content []byte }

func (s testContentSource) Digest() domain.Digest { return domain.NewDigest(s.content) }
func (s testContentSource) Size() int64           { return int64(len(s.content)) }
func (s testContentSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}
