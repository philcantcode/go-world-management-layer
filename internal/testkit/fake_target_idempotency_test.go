package testkit

import (
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestFakeTargetCreateIdempotencyBindsCompleteAndroidPlan(t *testing.T) {
	type planMutation struct {
		name   string
		mutate func(*testing.T, ports.TargetPlan, domain.LeaseID, domain.ResearchSessionID) ports.TargetPlan
	}
	mutations := []planMutation{
		{
			name: "boot timeout",
			mutate: func(_ *testing.T, plan ports.TargetPlan, _ domain.LeaseID, _ domain.ResearchSessionID) ports.TargetPlan {
				plan.Template.BootTimeout += time.Second
				return plan
			},
		},
		{
			name: "resources",
			mutate: func(_ *testing.T, plan ports.TargetPlan, _ domain.LeaseID, _ domain.ResearchSessionID) ports.TargetPlan {
				plan.Resources = plan.Resources.Clone()
				plan.Resources.StorageBytes += 4096
				return plan
			},
		},
		{
			name: "capability digest",
			mutate: func(t *testing.T, plan ports.TargetPlan, _ domain.LeaseID, _ domain.ResearchSessionID) ports.TargetPlan {
				capabilityDigest := domain.NewDigest([]byte("different-capability"))
				spec := plan.Generation.Spec()
				spec.CapabilityFingerprintDigest = capabilityDigest
				generation, err := domain.NewTargetGeneration(spec)
				if err != nil {
					t.Fatal(err)
				}
				plan.Generation = generation
				plan.CapabilityFingerprintDigest = capabilityDigest
				return plan
			},
		},
		{
			name: "lease provenance",
			mutate: func(_ *testing.T, plan ports.TargetPlan, leaseID domain.LeaseID, _ domain.ResearchSessionID) ports.TargetPlan {
				plan.LeaseID = leaseID
				return plan
			},
		},
		{
			name: "research session provenance",
			mutate: func(t *testing.T, plan ports.TargetPlan, _ domain.LeaseID, sessionID domain.ResearchSessionID) ports.TargetPlan {
				target, err := domain.NewTarget(plan.Target.ID(), sessionID, plan.Target.Kind(), plan.Target.CurrentGeneration(), plan.Target.UpdatedAt())
				if err != nil {
					t.Fatal(err)
				}
				plan.Target = target
				return plan
			},
		},
	}

	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			plan, otherLeaseID, otherSessionID := androidTargetIdempotencyFixture(t)
			tracker := NewOwnershipTracker()
			driver := NewFakeTargetDriver(domain.CapabilityFingerprint{}, nil, nil, tracker)
			ctx, cancel := contractContext(t)
			defer cancel()

			created, err := driver.Create(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := driver.Create(ctx, plan)
			if err != nil || replayed != created {
				t.Fatalf("Create(identical retry) = %#v, %v; want %#v", replayed, err, created)
			}

			changed := test.mutate(t, plan, otherLeaseID, otherSessionID)
			if err := changed.Validate(); err != nil {
				t.Fatalf("changed plan is not a valid contract input: %v", err)
			}
			if _, err := driver.Create(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("Create(changed %s with reused key) error = %v, want conflict", test.name, err)
			}

			if err := driver.Destroy(ctx, ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation}); err != nil {
				t.Fatal(err)
			}
			if err := tracker.RequireNoLeaks(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFakeTargetResetIdempotencyBindsLeaseAndIncident(t *testing.T) {
	for _, field := range []string{"lease", "incident"} {
		t.Run(field, func(t *testing.T) {
			fixture := newContractFixture(t)
			tracker := NewOwnershipTracker()
			driver := NewFakeTargetDriver(fixture.capabilities, fixture.clock, nil, tracker)
			ctx, cancel := contractContext(t)
			defer cancel()
			created, err := driver.Create(ctx, fixture.targetPlan)
			if err != nil {
				t.Fatal(err)
			}
			plan := ports.ResetPlan{
				IdempotencyKey: "target-reset-idempotency",
				LeaseID:        fixture.leaseID,
				Previous:       ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation},
				NextGeneration: created.Status.Generation + 1,
				Mode:           ports.ResetRecreate,
			}
			if err := plan.Validate(); err != nil {
				t.Fatal(err)
			}

			reset, err := driver.Reset(ctx, created.Status.TargetID, plan)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := driver.Reset(ctx, created.Status.TargetID, plan)
			if err != nil || replayed != reset {
				t.Fatalf("Reset(identical retry) = %#v, %v; want %#v", replayed, err, reset)
			}

			otherClock := NewClock(fixture.clock.Now().Add(time.Hour))
			otherIDs := NewIDGenerator(otherClock)
			changed := plan
			switch field {
			case "lease":
				changed.LeaseID = mustID(t, otherIDs.LeaseID)
			case "incident":
				changed.IncidentID = mustID(t, otherIDs.IncidentID)
			}
			if err := changed.Validate(); err != nil {
				t.Fatalf("changed reset plan is not a valid contract input: %v", err)
			}
			if _, err := driver.Reset(ctx, created.Status.TargetID, changed); !domain.IsCode(err, domain.CodeConflict) {
				t.Fatalf("Reset(changed %s with reused key) error = %v, want conflict", field, err)
			}

			if err := driver.Destroy(ctx, ports.TargetRef{ID: reset.Status.TargetID, Generation: reset.Status.Generation}); err != nil {
				t.Fatal(err)
			}
			if err := tracker.RequireNoLeaks(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFakeTargetPrepareRunIdempotencyBindsMaximumDuration(t *testing.T) {
	fixture := newContractFixture(t)
	tracker := NewOwnershipTracker()
	driver := NewFakeTargetDriver(fixture.capabilities, fixture.clock, nil, tracker)
	ctx, cancel := contractContext(t)
	defer cancel()
	created, err := driver.Create(ctx, fixture.targetPlan)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := driver.PrepareRun(ctx, fixture.runPlan)
	if err != nil {
		t.Fatal(err)
	}
	if replayed, err := driver.PrepareRun(ctx, fixture.runPlan); err != nil || replayed.RunID != prepared.RunID || replayed.PreparedAt != prepared.PreparedAt {
		t.Fatalf("PrepareRun(identical retry) = %#v, %v; want %#v", replayed, err, prepared)
	}
	changed := fixture.runPlan
	changed.MaximumDuration += time.Second
	if err := changed.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.PrepareRun(ctx, changed); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("PrepareRun(changed maximum duration with reused key) error = %v, want conflict", err)
	}
	if _, err := driver.StopRun(ctx, prepared.RunID, ports.StopForce); err != nil {
		t.Fatal(err)
	}
	if err := driver.Destroy(ctx, ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.RequireNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

func androidTargetIdempotencyFixture(t *testing.T) (ports.TargetPlan, domain.LeaseID, domain.ResearchSessionID) {
	t.Helper()
	clock := NewClock(time.Unix(1_900_000_000, 0).UTC())
	ids := NewIDGenerator(clock)
	sessionID := mustID(t, ids.ResearchSessionID)
	otherSessionID := mustID(t, ids.ResearchSessionID)
	leaseID := mustID(t, ids.LeaseID)
	otherLeaseID := mustID(t, ids.LeaseID)
	targetID := mustID(t, ids.TargetID)
	policyDigest := domain.NewDigest([]byte("android-policy"))
	capabilityDigest := domain.NewDigest([]byte("android-capability"))
	target, err := domain.NewTarget(targetID, sessionID, domain.TargetAndroidVirtualDevice, domain.InitialTargetGeneration, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	generation, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{
		TargetID:                    targetID,
		Generation:                  domain.InitialTargetGeneration,
		PolicyDigest:                policyDigest,
		CapabilityFingerprintDigest: capabilityDigest,
		CreatedAt:                   clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ports.TargetPlan{
		IdempotencyKey: "android-target-create",
		LeaseID:        leaseID,
		Target:         target,
		Generation:     generation,
		Template: ports.TargetTemplate{
			Name:                        "android-api-35",
			Kind:                        domain.TargetAndroidVirtualDevice,
			Driver:                      "android-emulator",
			ImageDigest:                 domain.NewDigest([]byte("android-system-image")),
			IsolationProfile:            "instrumented-android",
			BaselineState:               ports.AndroidBaselineCleanBoot,
			RequireHardwareAcceleration: true,
			Headless:                    true,
			Rooted:                      true,
			Debuggable:                  true,
			GuestMemoryBytes:            2 << 30,
			BootTimeout:                 3 * time.Minute,
		},
		PolicyDigest:                policyDigest,
		CapabilityFingerprintDigest: capabilityDigest,
		Resources: admission.Resources{
			CPUMilli: 2000, MemoryBytes: 2 << 30, StorageBytes: 1 << 30, PIDs: 512,
			Devices: map[string]int64{"kvm": 1},
		},
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	return plan, otherLeaseID, otherSessionID
}
