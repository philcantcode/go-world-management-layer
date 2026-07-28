//go:build !windows

package rpc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestListenRefusesActiveUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "world.sock")
	active, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = active.Close() })

	second, err := Listen(ListenOptions{UnixSocket: path})
	if second != nil {
		_ = second.Close()
		t.Fatal("second listener replaced an active unix socket")
	}
	if err == nil || !strings.Contains(err.Error(), "active unix socket") {
		t.Fatalf("active socket error = %v", err)
	}

	connection, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("original listener was no longer reachable: %v", err)
	}
	_ = connection.Close()
}

func TestListenReplacesStaleUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "world.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale socket setup failed: %v, %v", info, err)
	}

	listener, err := Listen(ListenOptions{UnixSocket: path})
	if err != nil {
		t.Fatalf("replace stale socket: %v", err)
	}
	defer listener.Close()
	connection, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("replacement listener is unreachable: %v", err)
	}
	_ = connection.Close()
}

func TestListenRefusesNonSocketPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "world.sock")
	if err := os.WriteFile(path, []byte("do not remove"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := Listen(ListenOptions{UnixSocket: path})
	if listener != nil {
		_ = listener.Close()
		t.Fatal("listener replaced a regular file")
	}
	if err == nil || !strings.Contains(err.Error(), "non-socket") {
		t.Fatalf("non-socket error = %v", err)
	}
	payload, readErr := os.ReadFile(path)
	if readErr != nil || string(payload) != "do not remove" {
		t.Fatalf("non-socket path changed: payload=%q err=%v", payload, readErr)
	}
}

func TestListenCreatesPrivateParentAndSocket(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "private", "rpc")
	path := filepath.Join(parent, "world.sock")
	listener, err := Listen(ListenOptions{UnixSocket: path})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	parentInfo, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if got := parentInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("created parent permissions = %04o, want 0700", got)
	}
	socketInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, want socket 0600", socketInfo.Mode())
	}
}

func TestUnixListenerRemovesOnlyItsOwnedSocket(t *testing.T) {
	t.Run("normal cleanup", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "world.sock")
		listener, err := Listen(ListenOptions{UnixSocket: path})
		if err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned socket remains after close: %v", err)
		}
	})

	t.Run("replacement preserved", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "world.sock")
		listener, err := Listen(ListenOptions{UnixSocket: path})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		payload, err := os.ReadFile(path)
		if err != nil || string(payload) != "replacement" {
			t.Fatalf("replacement path changed: payload=%q err=%v", payload, err)
		}
	})
}
