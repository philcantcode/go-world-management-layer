//go:build !aix

package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/processlock"
)

func TestOpenHostCannotOpenStateOwnedByAnotherProcess(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	owner, err := processlock.Acquire(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })

	cfg, managedRoots := minimalHostConfig(root, statePath)
	_, err = OpenHost(context.Background(), cfg)
	if !errors.Is(err, processlock.ErrAlreadyHeld) || !strings.Contains(err.Error(), owner.LockPath()) {
		t.Fatalf("OpenHost error = %v", err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control store was opened or created under another owner: %v", err)
	}
	for _, managedRoot := range managedRoots {
		if _, err := os.Stat(managedRoot); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("managed root %q was mutated under another owner: %v", managedRoot, err)
		}
	}
}

func TestConcurrentOpenHostRejectsSharedState(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	firstCfg, _ := minimalHostConfig(root, statePath)
	secondCfg, _ := minimalHostConfig(root, statePath)

	first, err := OpenHost(context.Background(), firstCfg)
	if err != nil {
		// Production OpenHost may skip on platforms without durable safepath.
		if strings.Contains(err.Error(), "unsupported") || strings.Contains(err.Error(), "safepath") {
			t.Skipf("OpenHost unavailable on this platform: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	_, secondErr := OpenHost(context.Background(), secondCfg)
	if !errors.Is(secondErr, processlock.ErrAlreadyHeld) {
		t.Fatalf("second OpenHost returned %v, want processlock.ErrAlreadyHeld", secondErr)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first host close: %v", err)
	}

	owner, err := processlock.Acquire(statePath)
	if err != nil {
		t.Fatalf("ownership was not released after Close: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenHostRejectsHardLinkedControlStateBeforeOpeningManagedRoots(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	if err := os.MkdirAll(filepath.Dir(statePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, []byte("must never be opened as sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	aliasPath := filepath.Join(root, "control-alias.db")
	if err := os.Link(statePath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	cfg, managedRoots := minimalHostConfig(root, statePath)
	_, err := OpenHost(context.Background(), cfg)
	if err == nil || errors.Is(err, processlock.ErrAlreadyHeld) || !strings.Contains(err.Error(), "exactly one link") {
		t.Fatalf("OpenHost error = %v", err)
	}
	for _, managedRoot := range managedRoots {
		if _, err := os.Stat(managedRoot); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("managed root %q was opened after unsafe control identity: %v", managedRoot, err)
		}
	}
}

func minimalHostConfig(root, statePath string) (HostConfig, []string) {
	managedRoots := []string{
		filepath.Join(root, "ledger"),
		filepath.Join(root, "orchestration"),
		filepath.Join(root, "bundles"),
		filepath.Join(root, "material"),
	}
	return HostConfig{
		StatePath:              statePath,
		LedgerDirectory:        managedRoots[0],
		OrchestrationStateRoot: managedRoots[1],
		BundleRoot:             managedRoots[2],
		MaterialRoot:           managedRoots[3],
		SubjectName:            "test-operator",
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
	}, managedRoots
}
