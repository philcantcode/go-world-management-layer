package world_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/processlock"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/world"
	"google.golang.org/protobuf/types/known/durationpb"
)

func requireProductionHostPlatform(t *testing.T) {
	t.Helper()
	namespace, err := safepath.OpenNamespace(t.TempDir(), "probe")
	if err != nil {
		if errors.Is(err, safepath.ErrUnsupported) {
			t.Skip("production Open requires durable safepath namespaces (linux/windows/darwin)")
		}
		t.Fatal(err)
	}
	_ = namespace.Close()
}

func TestOpenReportsStructuredPlatformSupport(t *testing.T) {
	requireProductionHostPlatform(t)
	manager := openLogicalManager(t)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Fatalf("close manager: %v", err)
		}
	}()
	support := manager.PlatformSupport()
	if support.GOOS == "" || support.GOARCH == "" {
		t.Fatalf("platform support missing host identity: %#v", support)
	}
	found := false
	for _, feature := range support.Features {
		if feature.ID == "safepath.namespace" {
			found = true
			if feature.Status != world.PlatformFeatureSupported {
				t.Fatalf("safepath status = %s, want supported", feature.Status)
			}
		}
		if feature.ID == "target.android_emulator.managed" && feature.Status == world.PlatformFeatureUnsupported {
			// Darwin and other non-managed hosts must surface explicit warnings.
			if len(support.Warnings) == 0 {
				t.Fatal("expected platform support warnings when Android is unsupported")
			}
		}
	}
	if !found {
		t.Fatal("expected safepath.namespace in platform support")
	}
}

func TestOpenLogicalAcquireGetRelease(t *testing.T) {
	requireProductionHostPlatform(t)
	ctx := context.Background()
	manager := openLogicalManager(t)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Fatalf("close manager: %v", err)
		}
	}()

	policy := domain.NewDigest([]byte("library-policy")).String()
	capabilities := domain.NewDigest([]byte("library-capabilities")).String()
	meta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	acquire, err := manager.AcquireResearchSession(ctx, &worldv1.AcquireResearchSessionRequest{
		Mutation:         meta,
		InputView:        &worldv1.InputViewSpec{ResolvedInputViewId: domain.NewInputViewID([]byte("library-input")).String()},
		PolicyDigest:     policy,
		CapabilityDigest: capabilities,
		Ttl:              durationpb.New(time.Hour),
	})
	if err != nil {
		t.Fatalf("AcquireResearchSession: %v", err)
	}
	if acquire.View == nil || acquire.Lease == nil {
		t.Fatalf("acquire response missing view/lease: %#v", acquire)
	}
	if acquire.View.Session.OwnerSubject != "library-operator" {
		t.Fatalf("owner subject = %q, want library-operator", acquire.View.Session.OwnerSubject)
	}
	if acquire.Lease.State != "active" {
		t.Fatalf("lease state = %q, want active", acquire.Lease.State)
	}

	loaded, err := manager.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{
		ResearchSessionId: acquire.View.Session.ResearchSessionId,
	})
	if err != nil {
		t.Fatalf("GetResearchSession: %v", err)
	}
	if loaded.Session.ResearchSessionId != acquire.View.Session.ResearchSessionId {
		t.Fatalf("loaded session = %q, want %q", loaded.Session.ResearchSessionId, acquire.View.Session.ResearchSessionId)
	}

	renewMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := manager.RenewLease(ctx, &worldv1.RenewLeaseRequest{
		Mutation:         renewMeta,
		LeaseId:          acquire.Lease.LeaseId,
		ExpectedRevision: acquire.Lease.Revision,
		Ttl:              durationpb.New(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if renewed.Revision <= acquire.Lease.Revision {
		t.Fatalf("renewed revision = %d, want > %d", renewed.Revision, acquire.Lease.Revision)
	}

	releaseMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.ReleaseResearchSession(ctx, &worldv1.ReleaseResearchSessionRequest{
		Mutation:         releaseMeta,
		LeaseId:          acquire.Lease.LeaseId,
		ExpectedRevision: renewed.Revision,
		Reason:           "library test complete",
	})
	if err != nil {
		t.Fatalf("ReleaseResearchSession: %v", err)
	}
	if outcome.LeaseId != acquire.Lease.LeaseId {
		t.Fatalf("release lease = %q, want %q", outcome.LeaseId, acquire.Lease.LeaseId)
	}
}

func TestOpenFailsClosedWhenStateOwned(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	owner, err := processlock.Acquire(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })

	_, err = world.Open(context.Background(), logicalConfig(root, statePath))
	if !errors.Is(err, processlock.ErrAlreadyHeld) {
		t.Fatalf("Open error = %v, want processlock.ErrAlreadyHeld", err)
	}
}

func TestOpenSecondManagerRejectsSharedState(t *testing.T) {
	requireProductionHostPlatform(t)
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	first, err := world.Open(context.Background(), logicalConfig(root, statePath))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	_, err = world.Open(context.Background(), logicalConfig(root, statePath))
	if !errors.Is(err, processlock.ErrAlreadyHeld) {
		t.Fatalf("second Open error = %v, want processlock.ErrAlreadyHeld", err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Ownership is released; a subsequent Open must succeed.
	second, err := world.Open(context.Background(), logicalConfig(root, statePath))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCloseIsIdempotent(t *testing.T) {
	requireProductionHostPlatform(t)
	manager := openLogicalManager(t)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err := manager.GetResearchSession(context.Background(), &worldv1.GetResearchSessionRequest{ResearchSessionId: "missing"})
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestOpenRequiresSubjectAndPaths(t *testing.T) {
	root := t.TempDir()
	cfg := logicalConfig(root, filepath.Join(root, "control.db"))
	cfg.Subject.Name = ""
	if _, err := world.Open(context.Background(), cfg); err == nil {
		t.Fatal("expected missing subject error")
	}
	cfg = logicalConfig(root, "")
	if _, err := world.Open(context.Background(), cfg); err == nil {
		t.Fatal("expected missing state path error")
	}
}

func openLogicalManager(t *testing.T) *world.Manager {
	t.Helper()
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	manager, err := world.Open(context.Background(), logicalConfig(root, statePath))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return manager
}

func logicalConfig(root, statePath string) world.Config {
	return world.Config{
		Paths: world.LocalPaths{
			StatePath:              statePath,
			LedgerDirectory:        filepath.Join(root, "ledger"),
			OrchestrationStateRoot: filepath.Join(root, "orchestration"),
			BundleRoot:             filepath.Join(root, "bundles"),
			MaterialRoot:           filepath.Join(root, "material"),
		},
		Subject: world.Subject{Name: "library-operator", Role: world.RoleOperator},
		Drivers: world.DriverConfig{
			AgentDriver:     "none",
			LinuxTarget:     "none",
			AndroidTarget:   "none",
			WorkspaceDriver: "none",
			MaterialDriver:  "local",
			ObserverDriver:  "none",
			CaptureDriver:   "none",
		},
		DefaultTimeout: 5 * time.Second,
	}
}
