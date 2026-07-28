package cuttlefish

import (
	"context"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestResetFingerprintCompatibilityIsOrderStableAndExact(t *testing.T) {
	base := ResetFingerprint{BackendVersion: "cvd-1", RuntimeVersion: "aosp-36", SystemImageDigest: domain.NewDigest([]byte("system")), DeviceConfigDigest: domain.NewDigest([]byte("config")), Features: []string{"root", "framework-hooks"}}
	reordered := base
	reordered.Features = []string{"framework-hooks", "root"}
	if !base.Compatible(reordered) {
		t.Fatal("feature ordering changed fingerprint")
	}
	changed := base
	changed.RuntimeVersion = "aosp-37"
	if base.Compatible(changed) {
		t.Fatal("runtime-incompatible snapshot accepted")
	}
	changed = base
	changed.Features = []string{"root"}
	if base.Compatible(changed) {
		t.Fatal("feature-incompatible snapshot accepted")
	}
}

func TestAllocatorKeepsGenerationsAndTargetsCollisionFree(t *testing.T) {
	allocator, err := NewMemoryAllocator(1, 6500)
	if err != nil {
		t.Fatal(err)
	}
	targetA, _ := domain.NewTargetID()
	targetB, _ := domain.NewTargetID()
	a1, _ := allocator.Reserve(context.Background(), targetA, 1)
	a1Again, _ := allocator.Reserve(context.Background(), targetA, 1)
	a2, _ := allocator.Reserve(context.Background(), targetA, 2)
	b1, _ := allocator.Reserve(context.Background(), targetB, 1)
	if a1 != a1Again {
		t.Fatal("idempotent allocation changed")
	}
	if a1.Serial == a2.Serial || a1.Serial == b1.Serial || a2.Serial == b1.Serial {
		t.Fatalf("serial collision: %#v %#v %#v", a1, a2, b1)
	}
}
