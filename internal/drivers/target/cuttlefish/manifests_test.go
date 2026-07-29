package cuttlefish

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestRunManifestRejectsChangedADBObservationAuthority(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	driver, _ := materializationTestDriver(t, lease, target, newRecordingFileGateway("127.0.0.1:6572"))
	plan := targetRunPlanForMaterial(t, lease, target, nil, "android-run-adb-authority")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := driver.PrepareRun(ctx, plan); err != nil {
		t.Fatal(err)
	}
	run := driver.runs[plan.Run.ID().String()]
	path := filepath.Join(run.directory, runPlanManifestFilename)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest runPlanManifest
	if err := json.Unmarshal(original, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Prepared.Attachment.ADBDevice.Serial = "emulator-5580"
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadExpectedRunManifest(run.directory, plan); err == nil {
		t.Fatal("run manifest accepted an ADB serial different from its exact allocation")
	}
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(original, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Prepared.Attachment.ADBDevice.Server.Port++
	encoded, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	device := driver.targets[deviceKey(target, 1)]
	if _, _, _, err := loadRunPreparation(device.plan, plan); err == nil {
		t.Fatal("run recovery accepted an ADB server different from its exact target plan")
	}
}

func TestRunStartManifestIsImmutableAndBindsExactExecution(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	driver, _ := materializationTestDriver(t, lease, target, newRecordingFileGateway("127.0.0.1:6566"))
	plan := targetRunPlanForMaterial(t, lease, target, nil, "android-run-start-manifest")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	prepared, err := driver.PrepareRun(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	run := driver.runs[plan.Run.ID().String()]
	startedAt := prepared.PreparedAt.Add(time.Second)
	committed, err := commitExpectedRunStart(run.directory, plan, run.allocation, run.sourceInstance, startedAt, prepared.PreparedAt)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := commitExpectedRunStart(run.directory, plan, run.allocation, run.sourceInstance, startedAt.Add(time.Hour), prepared.PreparedAt)
	if err != nil || replayed != committed {
		t.Fatalf("immutable start replay = %#v, %v; want %#v", replayed, err, committed)
	}
	loaded, found, err := loadExpectedRunStart(run.directory, plan, run.allocation, run.sourceInstance, prepared.PreparedAt)
	if err != nil || !found || loaded != committed {
		t.Fatalf("loaded start = %#v, found=%t, err=%v", loaded, found, err)
	}
	wrong := run.allocation
	wrong.Serial = "127.0.0.1:7777"
	wrong.ADBAddress = wrong.Serial
	if _, _, err := loadExpectedRunStart(run.directory, plan, wrong, run.sourceInstance, prepared.PreparedAt); err == nil {
		t.Fatal("run-start manifest accepted another exact allocation")
	}

	path := filepath.Join(run.directory, runStartManifestFilename)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var tampered runStartManifest
	if err := json.Unmarshal(payload, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.RuntimeID = "foreign-runtime"
	payload, err = json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadExpectedRunStart(run.directory, plan, run.allocation, run.sourceInstance, prepared.PreparedAt); err == nil {
		t.Fatal("tampered run-start manifest was accepted")
	}
}

func TestRuntimeManifestRejectsEveryPlanCriticalInstanceMutation(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*Instance)
	}{
		{name: "runtime id", mutate: func(value *Instance) { value.RuntimeID += "-foreign" }},
		{name: "state directory", mutate: func(value *Instance) { value.StateDirectory += "-foreign" }},
		{name: "system image directory", mutate: func(value *Instance) { value.SystemImageDirectory += "-foreign" }},
		{name: "allocation", mutate: func(value *Instance) { value.Allocation = emulatorAllocation(5580) }},
		{name: "fingerprint", mutate: func(value *Instance) { value.Fingerprint.RuntimeVersion += "-foreign" }},
		{name: "host cpu", mutate: func(value *Instance) { value.Resources.CPUMilli += 1000 }},
		{name: "host memory", mutate: func(value *Instance) { value.Resources.MemoryBytes += 1 << 20 }},
		{name: "storage", mutate: func(value *Instance) { value.Resources.StorageBytes += 1 << 20 }},
		{name: "baseline", mutate: func(value *Instance) { value.BaselineState = "foreign-baseline" }},
		{name: "hardware acceleration", mutate: func(value *Instance) { value.RequireHardwareAcceleration = !value.RequireHardwareAcceleration }},
		{name: "headless", mutate: func(value *Instance) { value.Headless = !value.Headless }},
		{name: "rooted", mutate: func(value *Instance) { value.Rooted = !value.Rooted }},
		{name: "debuggable", mutate: func(value *Instance) { value.Debuggable = !value.Debuggable }},
		{name: "guest memory", mutate: func(value *Instance) { value.GuestMemoryBytes += 1 << 20 }},
		{name: "boot timeout", mutate: func(value *Instance) { value.BootTimeout += time.Second }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			plan := managedBackendTestPlan(t, root, domain.NewDigest([]byte("runtime-manifest-"+test.name)))
			instance := instanceFromPlan(plan)
			if err := persistTargetRuntimeManifests(plan, instance, readyState(), time.Unix(3_000, 0)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(plan.StateDirectory, runtimePlanManifestFilename)
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var manifest runtimePlanManifest
			if err := json.Unmarshal(encoded, &manifest); err != nil {
				t.Fatal(err)
			}
			test.mutate(&manifest.Instance)
			encoded, err = json.MarshalIndent(manifest, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := loadTargetRuntimeManifests(plan.StateDirectory); err == nil {
				t.Fatal("runtime manifest accepted a plan-critical Instance mutation")
			}
		})
	}
}
