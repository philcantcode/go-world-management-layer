package orchestration

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestServiceRestartRejectsRedirectedDurableEvidenceNamespaces(t *testing.T) {
	for _, logicalDirectory := range []string{bundleStopPreparationDirectory, bundlePublicationDirectory, "bundles"} {
		t.Run(logicalDirectory, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			config := fixture.serviceConfig(Config{})
			if _, err := New(config); err != nil {
				t.Fatal(err)
			}
			namespacePath := filepath.Join(fixture.stateRoot, logicalDirectory)
			if err := os.Remove(namespacePath); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			want := []byte("must remain untouched")
			if err := os.WriteFile(sentinel, want, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, namespacePath); err != nil {
				t.Skipf("directory links unavailable: %v", err)
			}
			if _, err := New(config); err == nil {
				t.Fatal("redirected durable evidence namespace was accepted")
			}
			assertFileContent(t, sentinel, want)
		})
	}
}

func TestServiceRestartRejectsHardLinkedPreparationWithoutChangingSource(t *testing.T) {
	fixture := newIntegrationFixture(t)
	config := fixture.serviceConfig(Config{})
	if _, err := New(config); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	want := []byte(`{"untrusted":true}`)
	if err := os.WriteFile(outside, want, 0o600); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(fixture.stateRoot, bundleStopPreparationDirectory, "untrusted.json")
	if err := os.Link(outside, linked); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if _, err := New(config); err == nil {
		t.Fatal("hard-linked durable preparation was accepted")
	}
	assertFileContent(t, outside, want)
}

func TestRunObserverCoordinatorRejectsRedirectedStateNamespaces(t *testing.T) {
	for _, logicalDirectory := range []string{"runs", "journals", "artifacts"} {
		t.Run(logicalDirectory, func(t *testing.T) {
			stateRoot, config := observerNamespaceSecurityConfig(t)
			if _, err := NewRunObserverCoordinator(config); err != nil {
				t.Fatal(err)
			}
			namespacePath := filepath.Join(stateRoot, logicalDirectory)
			if err := os.Remove(namespacePath); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			want := []byte("observer boundary")
			if err := os.WriteFile(sentinel, want, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, namespacePath); err != nil {
				t.Skipf("directory links unavailable: %v", err)
			}
			if _, err := NewRunObserverCoordinator(config); err == nil {
				t.Fatal("redirected observer namespace was accepted")
			}
			assertFileContent(t, sentinel, want)
		})
	}
}

func TestRunObserverReconcileRejectsHardLinkedMarkerWithoutChangingSource(t *testing.T) {
	stateRoot, config := observerNamespaceSecurityConfig(t)
	coordinator, err := NewRunObserverCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	want := []byte(`{"version":4}`)
	if err := os.WriteFile(outside, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outside, filepath.Join(stateRoot, "runs", "untrusted.json")); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if err := coordinator.Reconcile(context.Background()); err == nil {
		t.Fatal("hard-linked observer marker was accepted")
	}
	assertFileContent(t, outside, want)
}

func TestBundleAuthorityRejectsRedirectedPublicationNamespaces(t *testing.T) {
	for _, logicalDirectory := range []string{"objects", "requests"} {
		t.Run(logicalDirectory, func(t *testing.T) {
			root := t.TempDir()
			if _, err := NewBundleAuthority(root, 1<<20); err != nil {
				t.Fatal(err)
			}
			namespacePath := filepath.Join(root, logicalDirectory)
			if err := os.Remove(namespacePath); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			want := []byte("material boundary")
			if err := os.WriteFile(sentinel, want, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, namespacePath); err != nil {
				t.Skipf("directory links unavailable: %v", err)
			}
			if _, err := NewBundleAuthority(root, 1<<20); err == nil {
				t.Fatal("redirected bundle-authority namespace was accepted")
			}
			assertFileContent(t, sentinel, want)
		})
	}
}

func TestLedgerCaptureRejectsRedirectedAndHardLinkedState(t *testing.T) {
	newConfig := func(t *testing.T, root string) LedgerCaptureConfig {
		t.Helper()
		observations, _, err := ledger.Open(ledger.Options{Directory: filepath.Join(t.TempDir(), "ledger")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = observations.Close() })
		return LedgerCaptureConfig{Root: root, Ledger: observations, Material: newRecordingCaptureAuthority(), MaxBytes: 1 << 20}
	}
	t.Run("redirected namespace", func(t *testing.T) {
		root := t.TempDir()
		config := newConfig(t, root)
		if _, err := NewLedgerCaptureController(config); err != nil {
			t.Fatal(err)
		}
		statePath := filepath.Join(root, "records")
		if err := os.Remove(statePath); err != nil {
			t.Fatal(err)
		}
		outside := t.TempDir()
		if err := os.Symlink(outside, statePath); err != nil {
			t.Skipf("directory links unavailable: %v", err)
		}
		if _, err := NewLedgerCaptureController(config); err == nil {
			t.Fatal("redirected capture state namespace was accepted")
		}
	})
	t.Run("hard-linked record", func(t *testing.T) {
		root := t.TempDir()
		config := newConfig(t, root)
		if _, err := NewLedgerCaptureController(config); err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "outside.json")
		want := []byte(`{"untrusted":true}`)
		if err := os.WriteFile(outside, want, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(outside, filepath.Join(root, "records", "untrusted.json")); err != nil {
			t.Skipf("hard links unavailable: %v", err)
		}
		if _, err := NewLedgerCaptureController(config); err == nil {
			t.Fatal("hard-linked capture state was accepted")
		}
		assertFileContent(t, outside, want)
	})
}

func observerNamespaceSecurityConfig(t *testing.T) (string, RunObserverCoordinatorConfig) {
	t.Helper()
	clock := testkit.NewClock(time.Unix(1_800_000_000, 0).UTC())
	observations, _, err := ledger.Open(ledger.Options{Directory: filepath.Join(t.TempDir(), "ledger")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observations.Close() })
	stateRoot := filepath.Join(t.TempDir(), "observer-state")
	return stateRoot, RunObserverCoordinatorConfig{
		Ledger: observations, IDs: testkit.NewIDGenerator(clock), Clock: clock.Now,
		StateRoot: stateRoot, CleanupTimeout: time.Second,
	}
}

func assertFileContent(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s changed: got %q, want %q", path, got, want)
	}
}
