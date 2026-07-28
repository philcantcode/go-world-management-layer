package admission

import (
	"errors"
	"testing"
	"time"
)

func baseResources() Resources {
	return Resources{CPUMilli: 1000, MemoryBytes: 1000, SwapBytes: 1000, StorageBytes: 1000, CaptureBytes: 1000, Inodes: 1000, PIDs: 1000, Devices: map[string]int64{"kvm": 1}}
}
func safeThresholds() PressureThresholds {
	return PressureThresholds{MemoryPSIFull: .5, IOPSIFull: .5, CPUPSIFull: .5, MinimumFreeDiskPercent: 10, MinimumFreeInodesPercent: 10}
}
func safePressure() Pressure { return Pressure{FreeDiskPercent: 50, FreeInodesPercent: 50} }

func TestEvaluateAccountsForAllCostsAndReserve(t *testing.T) {
	now := time.Unix(10, 0)
	request := LeaseRequest{LeaseKey: "lease", Requests: Resources{CPUMilli: 100, MemoryBytes: 100}, Limits: Resources{CPUMilli: 300, MemoryBytes: 300}, ObserverCost: Resources{CPUMilli: 50}, SnapshotCost: Resources{MemoryBytes: 50}, TTL: time.Hour}
	snapshot := CapacitySnapshot{Allocatable: baseResources(), ControlReserve: Resources{CPUMilli: 100, MemoryBytes: 100}, Pressure: safePressure(), ObservedAt: now}
	decision, err := Evaluate(request, snapshot, safeThresholds(), now)
	if err != nil || !decision.Admitted {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	if decision.AvailableAfter.CPUMilli != 750 || decision.AvailableAfter.MemoryBytes != 750 {
		t.Fatalf("available after = %#v", decision.AvailableAfter)
	}
	second, _ := Evaluate(request, snapshot, safeThresholds(), now)
	if second.InputsDigest != decision.InputsDigest {
		t.Fatal("replay changed input digest")
	}
}

func TestEvaluateRejectsPressureAndEveryResourceShortage(t *testing.T) {
	request := LeaseRequest{LeaseKey: "lease", Requests: Resources{Devices: map[string]int64{"kvm": 2}}, Limits: Resources{Devices: map[string]int64{"kvm": 2}}, TTL: time.Hour}
	snapshot := CapacitySnapshot{Allocatable: baseResources(), Pressure: safePressure()}
	if _, err := Evaluate(request, snapshot, safeThresholds(), time.Now()); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("error=%v", err)
	}
	request.Requests = Resources{MemoryBytes: 10}
	request.Limits = Resources{MemoryBytes: 10}
	snapshot.Pressure.MemoryFull.Current = .6
	if _, err := Evaluate(request, snapshot, safeThresholds(), time.Now()); !errors.Is(err, ErrPressure) {
		t.Fatalf("error=%v", err)
	}
}

func TestResourcesAccountForSwap(t *testing.T) {
	available := Resources{MemoryBytes: 1024, SwapBytes: 512}
	remaining, err := available.Subtract(Resources{MemoryBytes: 256, SwapBytes: 128})
	if err != nil || remaining.MemoryBytes != 768 || remaining.SwapBytes != 384 {
		t.Fatalf("remaining resources = %#v, %v", remaining, err)
	}
	if (Resources{SwapBytes: 513}).FitsWithin(available) {
		t.Fatal("swap request exceeding capacity was accepted")
	}
	if _, err := available.Add(Resources{SwapBytes: -1}); !errors.Is(err, ErrInvalidResources) {
		t.Fatalf("negative swap error = %v", err)
	}
}

func TestPressureOrderAndTargetBeforeAgent(t *testing.T) {
	input := PressureInput{Pressure: Pressure{MemoryFull: PSI{Current: .8}, FreeDiskPercent: 50, FreeInodesPercent: 50}, Thresholds: safeThresholds(), Candidates: []RevocationCandidate{
		{ResourceKey: "agent", Kind: CandidateAgentWorkspace, Priority: 1, Preemptible: true},
		{ResourceKey: "target-high", Kind: CandidateTargetRun, Priority: 10, Preemptible: true},
		{ResourceKey: "target-low", Kind: CandidateTargetRun, Priority: 1, Preemptible: true},
	}}
	want := []PressureStage{StageRaiseObservation, StageStopAdmission, StageExpireReservations, StageShrinkWarmPools, StageQuiesceIdleTargets, StageRevokeTargetRuns, StageRevokeAgentWorkspaces, StageQuarantineNode}
	for _, stage := range want {
		decision, err := DecidePressure(input)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Stage != stage {
			t.Fatalf("stage=%s want=%s", decision.Stage, stage)
		}
		if stage == StageRevokeTargetRuns && decision.ResourceKey != "target-low" {
			t.Fatalf("target selection=%q", decision.ResourceKey)
		}
		if stage == StageRevokeAgentWorkspaces && decision.ResourceKey != "agent" {
			t.Fatalf("agent selection=%q", decision.ResourceKey)
		}
		input.Stage = decision.Stage
	}
}

func TestZeroPressureThresholdsDisableSignals(t *testing.T) {
	decision, err := DecidePressure(PressureInput{Pressure: Pressure{FreeDiskPercent: 0, FreeInodesPercent: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Stage != StageNormal || decision.Action != "resume_admission" {
		t.Fatalf("zero thresholds unexpectedly triggered pressure: %#v", decision)
	}
	invalid := PressureInput{Pressure: safePressure(), Thresholds: PressureThresholds{MemoryPSIFull: 2}}
	if _, err := DecidePressure(invalid); err == nil {
		t.Fatal("invalid pressure threshold was accepted")
	}
}

func TestQueueAgingPreventsStarvation(t *testing.T) {
	now := time.Unix(1000, 0)
	ordered, err := OrderQueue([]QueuedLease{{LeaseKey: "new-high", Priority: 10, EnqueuedAt: now}, {LeaseKey: "old-low", Priority: 1, EnqueuedAt: now.Add(-20 * time.Minute)}}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ordered[0].LeaseKey != "old-low" {
		t.Fatalf("order=%#v", ordered)
	}
}
