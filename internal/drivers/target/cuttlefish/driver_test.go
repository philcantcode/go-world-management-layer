package cuttlefish

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestPrepareRunMaterializesExactArtifactsInCanonicalOrder(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6550")
	driver, _ := materializationTestDriver(t, lease, target, files)
	material := []ports.TargetMaterialPlan{
		targetMaterial(t, "z/second.bin", 0o640, []byte("second"), nil),
		targetMaterial(t, "a/first.bin", 0o750, []byte("first"), nil),
	}
	plan := targetRunPlanForMaterial(t, lease, target, material, "android-materialize")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	prepared, err := driver.PrepareRun(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.MaterializationDigest != plan.Run.Spec().MaterializationDigest || prepared.RunID != plan.Run.ID() {
		t.Fatalf("prepared run = %#v", prepared)
	}
	if prepared.Attachment.ADBDevice.Server != (ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5037}) || prepared.Attachment.ADBDevice.Serial != files.serial {
		t.Fatalf("prepared run did not bind the authority-owned ADB server/serial: %#v", prepared.Attachment)
	}
	files.mu.Lock()
	paths := []string{files.putPlans[0].LogicalPath, files.putPlans[1].LogicalPath}
	modes := []uint32{files.putPlans[0].Mode, files.putPlans[1].Mode}
	areas := []DeviceFileArea{files.putPlans[0].Area, files.putPlans[1].Area}
	scopes := append([]deviceproxy.Scope(nil), files.putScopes...)
	files.mu.Unlock()
	if len(paths) != 2 || paths[0] != "a/first.bin" || paths[1] != "z/second.bin" || modes[0] != 0o750 || modes[1] != 0o640 {
		t.Fatalf("material projection order/modes = %v/%#o", paths, modes)
	}
	if areas[0] != DeviceFileMaterial || areas[1] != DeviceFileMaterial {
		t.Fatalf("prepared artifacts were not isolated in the material area: %v", areas)
	}
	for _, scope := range scopes {
		if scope.LeaseID != lease || scope.TargetID != target || scope.Generation != 1 || scope.RunID != plan.Run.ID() {
			t.Fatalf("material escaped run scope: %#v", scope)
		}
	}
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	files.mu.Lock()
	preparedCalls := files.prepared
	files.mu.Unlock()
	if files.PutCount() != 2 || preparedCalls != 1 {
		t.Fatal("idempotent prepare re-materialized device content")
	}
}

func TestAndroidTargetGenerationIsSingleUseAfterEveryTerminalRun(t *testing.T) {
	for _, test := range []struct {
		name  string
		start bool
		want  ports.RunOutcome
	}{
		{name: "completed", start: true, want: ports.RunCompleted},
		{name: "never-started", start: false, want: ports.RunFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			lease, _ := domain.NewLeaseID()
			target, _ := domain.NewTargetID()
			files := newRecordingFileGateway("127.0.0.1:6552")
			driver, _ := materializationTestDriver(t, lease, target, files)
			first := targetRunPlanForMaterial(t, lease, target, nil, "android-single-use-first")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := driver.PrepareRun(ctx, first); err != nil {
				t.Fatal(err)
			}
			if test.start {
				if err := driver.StartRun(ctx, first.Run.ID()); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := driver.StopRun(ctx, first.Run.ID(), ports.StopGraceful)
			if err != nil || receipt.Outcome != test.want {
				t.Fatalf("terminal receipt = %#v, %v", receipt, err)
			}
			second := targetRunPlanForMaterial(t, lease, target, nil, "android-single-use-second")
			if _, err := driver.PrepareRun(ctx, second); !domain.IsCode(err, domain.CodeFailedPrecondition) {
				t.Fatalf("second run on spent Android generation error = %v", err)
			}
			device := driver.targets[deviceKey(target, 1)]
			use, found, err := loadGenerationUseManifest(device.plan)
			if err != nil || !found || use.RunID != first.Run.ID().String() || use.Reason != generationUseRunPrepared {
				t.Fatalf("durable generation-use gate = %#v, %v, found=%t", use, err, found)
			}
		})
	}
}

func TestPrepareRunRemovesPartialProjectionWhenSourceBytesLie(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6551")
	driver, stateDirectory := materializationTestDriver(t, lease, target, files)
	declared := []byte("declared")
	material := []ports.TargetMaterialPlan{targetMaterial(t, "bad/content.bin", 0o600, declared, []byte("different"))}
	plan := targetRunPlanForMaterial(t, lease, target, material, "android-materialize-fail")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err == nil {
		t.Fatal("content source whose bytes disagree with its digest was accepted")
	}
	files.mu.Lock()
	removed := files.removed
	files.mu.Unlock()
	if removed != 1 {
		t.Fatalf("partial device projection cleanup calls = %d", removed)
	}
	if _, err := os.Stat(filepath.Join(stateDirectory, "runs", plan.Run.ID().String())); !os.IsNotExist(err) {
		t.Fatalf("partial host run directory remains: %v", err)
	}
	if _, found := driver.runs[plan.Run.ID().String()]; found {
		t.Fatal("failed run was registered")
	}
	if _, found := driver.idempotency[plan.IdempotencyKey]; found {
		t.Fatal("failed idempotency key was committed")
	}
}

func TestStopPreparedRunReturnsIntrinsicNeverStartedReceipt(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6552")
	driver, _ := materializationTestDriver(t, lease, target, files)
	material := []ports.TargetMaterialPlan{targetMaterial(t, "input.bin", 0o600, []byte("input"), nil)}
	plan := targetRunPlanForMaterial(t, lease, target, material, "android-stop-prepared")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	prepared, err := driver.PrepareRun(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	prepared.RequiredCoverage[0] = "caller-mutated"
	material[0].Content.(*materialContentSource).digest = domain.Digest{}
	result, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, plan, ports.RunFailed, ports.TargetRunFailureNeverStarted)
	kind := result.Observations[0].Kind
	result.Observations[0].Kind = "caller-mutated"
	if len(result.Observations[0].Payload) > 0 {
		result.Observations[0].Payload[0] ^= 0xff
	}
	replay, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopForce)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Observations[0].Kind != kind || replay.Observations[0].Kind == result.Observations[0].Kind {
		t.Fatal("idempotent stop exposed stored receipt slices")
	}
}

func TestStopStartedRunReturnsOnlyIntrinsicTargetFacts(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	files := newRecordingFileGateway("127.0.0.1:6553")
	driver, _ := materializationTestDriver(t, lease, target, files)
	material := []ports.TargetMaterialPlan{targetMaterial(t, "input.bin", 0o600, []byte("input"), nil)}
	plan := targetRunPlanForMaterial(t, lease, target, material, "android-stop-started")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := driver.StartRun(ctx, plan.Run.ID()); err != nil {
		t.Fatal(err)
	}
	result, err := driver.StopRun(ctx, plan.Run.ID(), ports.StopGraceful)
	if err != nil {
		t.Fatal(err)
	}
	assertStopReceipt(t, result, plan, ports.RunCompleted, ports.TargetRunFailureNone)
}

func assertStopReceipt(t *testing.T, result ports.TargetRunStopReceipt, plan ports.TargetRunPlan, outcome ports.RunOutcome, failure ports.TargetRunFailureKind) {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("receipt validation failed: %v", err)
	}
	if result.RunID != plan.Run.ID() || result.Outcome != outcome || result.FailureKind != failure ||
		result.StoppedAt.Before(plan.Run.Spec().CreatedAt) || result.TargetChanges.Scope() != domain.ChangeScopeTarget ||
		len(result.Observations) == 0 {
		t.Fatalf("intrinsic target receipt = %#v", result)
	}
	last := result.Observations[len(result.Observations)-1]
	if !last.ObservedAt.Equal(result.StoppedAt) {
		t.Fatalf("terminal observation = %#v, stop = %s", last, result.StoppedAt)
	}
	if outcome == ports.RunCompleted && result.StartedAt.IsZero() {
		t.Fatal("completed receipt has no start time")
	}
}

func requiredCollectorSpec(name, family string) ports.CollectorSpec {
	return ports.CollectorSpec{
		Name: name,
		Requirement: ports.ObservationRequirement{
			SignalFamily: family, Placement: domain.CollectorPlacementHost,
			MinimumLevel: domain.CoverageLevelComplete, Required: true,
		},
		Adapter: "fake", Version: "1", ConfigurationDigest: domain.NewDigest([]byte(name)),
		MaximumBytes: 1 << 20,
	}
}

func materializationTestDriver(t *testing.T, lease domain.LeaseID, target domain.TargetID, files *recordingFileGateway) (*Driver, string) {
	t.Helper()
	stateDirectory := cuttlefishTempDir(t, "world-cuttlefish-state-")
	allocation := Allocation{InstanceNumber: 1, InstanceName: "cvd-1", Serial: files.serial, ADBAddress: files.serial}
	session, _ := domain.NewResearchSessionID()
	targetModel, err := domain.NewTarget(target, session, domain.TargetAndroidVirtualDevice, 1, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := ResetFingerprint{
		BackendVersion: "cvd-test", RuntimeVersion: "aosp-test",
		SystemImageDigest: domain.NewDigest([]byte("material-system-image")), DeviceConfigDigest: domain.NewDigest([]byte("material-device-config")),
		Features: []string{"root", "headless"},
	}
	instance := Instance{RuntimeID: allocation.InstanceName, StateDirectory: stateDirectory, Allocation: allocation}
	driver := &Driver{
		backend:    newStatefulBackend(instance),
		files:      files,
		collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
		random:     bytes.NewReader(bytes.Repeat([]byte{0x42}, 128)),
		now:        func() time.Time { return time.Unix(100, 0).UTC() },
		targets: map[string]deviceRecord{
			deviceKey(target, 1): {
				input: ports.TargetPlan{Target: targetModel},
				plan: VirtualDevicePlan{
					Name: "world-android-" + target.UUID() + "-g1", LeaseID: lease, TargetID: target, Generation: 1,
					StateDirectory: stateDirectory, SystemImageDirectory: filepath.Join(stateDirectory, "system-image"), Allocation: allocation,
					ADBServer:   ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5037},
					Fingerprint: fingerprint, Labels: map[string]string{"world.target-generation": "1"}, BaselineState: ports.AndroidBaselineCleanBoot,
					RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true,
					GuestMemoryBytes: 2 << 30, BootTimeout: time.Minute,
				},
				instance: instance,
				status: ports.TargetStatus{
					TargetID: target, Generation: 1, Kind: domain.TargetAndroidVirtualDevice,
					State: domain.TargetGenerationReady, Ready: true, RuntimeID: instance.RuntimeID,
					DeviceSerial: allocation.Serial, ObservedAt: time.Unix(99, 0).UTC(),
				},
			},
		},
		runs:         make(map[string]*runRecord),
		idempotency:  make(map[string]string),
		resetResults: make(map[string]resetOutcome),
	}
	return driver, stateDirectory
}

func targetRunPlanForMaterial(t *testing.T, lease domain.LeaseID, target domain.TargetID, material []ports.TargetMaterialPlan, key string) ports.TargetRunPlan {
	return targetRunPlanForGeneration(t, lease, target, 1, material, key)
}

func targetRunPlanForGeneration(t *testing.T, lease domain.LeaseID, target domain.TargetID, generation domain.TargetGeneration, material []ports.TargetMaterialPlan, key string) ports.TargetRunPlan {
	t.Helper()
	if len(material) == 0 {
		material = []ports.TargetMaterialPlan{targetMaterial(t, "fixture/payload.bin", 0o600, []byte("fixture"), nil)}
	}
	runID, _ := domain.NewTargetRunID()
	agent, _ := domain.NewAgentWorkspaceID()
	digest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.NewTargetRun(domain.TargetRunSpec{
		ID: runID, LeaseID: lease, TargetID: target, TargetGeneration: generation, AgentWorkspaceID: agent, AgentGeneration: 1,
		MaterializationDigest: digest, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetRunPlan{IdempotencyKey: key, Run: run, RequiredCoverage: []string{ports.TargetLifecycleSignal}, Material: material, MaximumDuration: time.Minute}
}

func targetMaterial(t *testing.T, logicalPath string, mode uint32, declared, opened []byte) ports.TargetMaterialPlan {
	t.Helper()
	if opened == nil {
		opened = declared
	}
	digest := domain.NewDigest(declared)
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "artifact://android/" + logicalPath, Digest: digest, Size: int64(len(declared)), Role: "target-input", Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.TargetMaterialPlan{
		Artifact: artifact, LogicalPath: logicalPath, Mode: mode,
		Content: &materialContentSource{digest: digest, size: int64(len(declared)), content: append([]byte(nil), opened...)},
	}
}

type materialContentSource struct {
	digest  domain.Digest
	size    int64
	content []byte
}

func (s *materialContentSource) Digest() domain.Digest { return s.digest }
func (s *materialContentSource) Size() int64           { return s.size }
func (s *materialContentSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

var _ ports.ContentSource = (*materialContentSource)(nil)
