//go:build !aix

package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/processlock"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

func requireProductionHostPlatform(t *testing.T) {
	t.Helper()
	namespace, err := safepath.OpenNamespace(t.TempDir(), "probe")
	if err != nil {
		if errors.Is(err, safepath.ErrUnsupported) {
			t.Skip("production OpenHost requires durable safepath namespaces (linux/windows)")
		}
		t.Fatal(err)
	}
	_ = namespace.Close()
}

func TestOpenHostLogicalCompositionAndOwnership(t *testing.T) {
	requireProductionHostPlatform(t)
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	host, err := OpenHost(context.Background(), logicalHostConfig(root, statePath))
	if err != nil {
		t.Fatal(err)
	}
	if host.Controller() == nil || host.Service() == nil || host.Core() == nil {
		t.Fatal("OpenHost returned incomplete production wiring")
	}
	if host.Subject != "host-operator" {
		t.Fatalf("subject = %q, want host-operator", host.Subject)
	}

	_, err = OpenHost(context.Background(), logicalHostConfig(root, statePath))
	if !errors.Is(err, processlock.ErrAlreadyHeld) {
		t.Fatalf("second OpenHost error = %v, want processlock.ErrAlreadyHeld", err)
	}

	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}

	// Lock released; reopen should succeed.
	reopened, err := OpenHost(context.Background(), logicalHostConfig(root, statePath))
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenHostFailsClosedWhenStateOwned(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	owner, err := processlock.Acquire(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })

	_, err = OpenHost(context.Background(), logicalHostConfig(root, statePath))
	if !errors.Is(err, processlock.ErrAlreadyHeld) {
		t.Fatalf("OpenHost error = %v, want processlock.ErrAlreadyHeld", err)
	}
}

func logicalHostConfig(root, statePath string) HostConfig {
	return HostConfig{
		StatePath:              statePath,
		LedgerDirectory:        filepath.Join(root, "ledger"),
		OrchestrationStateRoot: filepath.Join(root, "orchestration"),
		BundleRoot:             filepath.Join(root, "bundles"),
		MaterialRoot:           filepath.Join(root, "material"),
		SubjectName:            "host-operator",
		AgentDriver:            "none",
		LinuxTargetDriver:      "none",
		AndroidTargetDriver:    "none",
		WorkspaceDriver:        "none",
		MaterialDriver:         "local",
		ObserverDriver:         "none",
		CaptureDriver:          "none",
		PhysicalTargetDriver:   "none",
		ControlTimeout:         time.Second,
		ReconciliationInterval: time.Second,
		ReconciliationTimeout:  time.Second,
		ShutdownTimeout:        time.Second,
		ProbeTimeout:           time.Second,
		MaxTransferBytes:       1 << 20,
		MaxExecBytes:           1 << 20,
		MaxADBBytes:            1 << 20,
		MaxBundleBytes:         1 << 20,
	}
}
