//go:build !aix

package daemon

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/processlock"
)

func TestRunCannotOpenOrReconcileStateOwnedByAnotherDaemon(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "control", "control.db")
	owner, err := processlock.Acquire(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })

	args, managedRoots := minimalDaemonArgs(root, statePath, "127.0.0.1:0")
	err = Run(context.Background(), args, ModeController)
	if !errors.Is(err, processlock.ErrAlreadyHeld) || !strings.Contains(err.Error(), owner.LockPath()) {
		t.Fatalf("Run error = %v", err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control store was opened or created under another owner: %v", err)
	}
	for _, root := range managedRoots {
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("managed root %q was mutated under another owner: %v", root, err)
		}
	}
}

func TestConcurrentDaemonsRejectSharedStateBeforeListenerSelection(t *testing.T) {
	tests := []struct {
		name         string
		sameListener bool
	}{
		{name: "same listener", sameListener: true},
		{name: "different listeners", sameListener: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			firstAddress := unusedTCPAddress(t)
			secondAddress := firstAddress
			if !test.sameListener {
				for secondAddress == firstAddress {
					secondAddress = unusedTCPAddress(t)
				}
			}
			statePath := filepath.Join(root, "control", "control.db")
			firstArgs, _ := minimalDaemonArgs(root, statePath, firstAddress)
			secondArgs, _ := minimalDaemonArgs(root, statePath, secondAddress)

			firstContext, stopFirst := context.WithCancel(context.Background())
			t.Cleanup(stopFirst)
			firstDone := make(chan error, 1)
			go func() { firstDone <- Run(firstContext, firstArgs, ModeController) }()
			waitForTCPListener(t, firstAddress, firstDone)

			secondContext, stopSecond := context.WithTimeout(context.Background(), 2*time.Second)
			secondErr := Run(secondContext, secondArgs, ModeController)
			stopSecond()
			if !errors.Is(secondErr, processlock.ErrAlreadyHeld) {
				stopFirst()
				t.Fatalf("second daemon with same roots and listener %q returned %v", secondAddress, secondErr)
			}

			stopFirst()
			select {
			case err := <-firstDone:
				if err != nil {
					t.Fatalf("first daemon shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("first daemon did not release ownership during shutdown")
			}

			owner, err := processlock.Acquire(statePath)
			if err != nil {
				t.Fatalf("ownership was not released after shutdown: %v", err)
			}
			if err := owner.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunRejectsHardLinkedControlStateBeforeOpeningManagedRoots(t *testing.T) {
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
	args, managedRoots := minimalDaemonArgs(root, statePath, "127.0.0.1:0")
	err := Run(context.Background(), args, ModeController)
	if err == nil || errors.Is(err, processlock.ErrAlreadyHeld) || !strings.Contains(err.Error(), "exactly one link") {
		t.Fatalf("Run error = %v", err)
	}
	for _, managedRoot := range managedRoots {
		if _, err := os.Stat(managedRoot); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("managed root %q was opened after unsafe control identity: %v", managedRoot, err)
		}
	}
}

func minimalDaemonArgs(root, statePath, address string) ([]string, []string) {
	managedRoots := []string{
		filepath.Join(root, "ledger"),
		filepath.Join(root, "orchestration"),
		filepath.Join(root, "bundles"),
		filepath.Join(root, "material"),
	}
	return []string{
		"-state", statePath,
		"-ledger-dir", managedRoots[0],
		"-orchestration-state-dir", managedRoots[1],
		"-bundle-dir", managedRoots[2],
		"-material-dir", managedRoots[3],
		"-unix-socket=", "-listen", address,
		"-bearer-token", "test-token", "-bearer-subject", "test-operator",
		"-tls-cert=", "-tls-key=", "-client-ca=",
		"-deployment-profile=", "-agent-driver", "none", "-linux-target-driver", "none",
		"-workspace-driver", "none", "-android-target-driver", "none", "-physical-target-driver", "none",
		"-observer-driver", "none", "-observer-output-dir=", "-capture-driver", "none", "-capture-dir=",
		"-agent-workspace-root=", "-agent-image-repository=", "-target-root=", "-target-image-repository=",
		"-target-allow-ptrace=false", "-material-driver", "local",
	}, managedRoots
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForTCPListener(t *testing.T, address string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		select {
		case daemonErr := <-done:
			t.Fatalf("daemon exited before listening: %v", daemonErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("daemon did not listen on %s", address)
}
