package rpc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

const unixSocketProbeTimeout = 250 * time.Millisecond

type ListenOptions struct {
	UnixSocket string
	TCPAddress string
}

// Listen chooses an explicit TCP endpoint when configured (including Windows
// and tests), otherwise a local Unix-domain socket.
func Listen(options ListenOptions) (net.Listener, error) {
	if address := strings.TrimSpace(options.TCPAddress); address != "" {
		return net.Listen("tcp", address)
	}
	path := strings.TrimSpace(options.UnixSocket)
	if path == "" {
		return nil, fmt.Errorf("unix_socket or tcp_address is required")
	}
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf("unix sockets are not selected on Windows; configure tcp_address")
	}
	if err := ensureSocketParent(path); err != nil {
		return nil, err
	}
	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on unix socket: %w", err)
	}
	// UnixListener otherwise unlinks whatever currently occupies path on close,
	// even if another actor replaced the socket after this listener was bound.
	listener.SetUnlinkOnClose(false)
	owned, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("inspect created unix socket: %w", err)
	}
	if owned.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, fmt.Errorf("created unix listener path is not a socket: %s", path)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = removeOwnedSocket(path, owned)
		return nil, fmt.Errorf("secure unix socket permissions: %w", err)
	}
	return &ownedUnixListener{UnixListener: listener, path: path, owned: owned}, nil
}

func removeStaleSocket(path string) error {
	observed, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect unix socket: %w", err)
	}
	if observed.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace non-socket at %s", path)
	}
	connection, probeErr := net.DialTimeout("unix", path, unixSocketProbeTimeout)
	if probeErr == nil {
		_ = connection.Close()
		return fmt.Errorf("refusing to replace active unix socket at %s", path)
	}
	if errors.Is(probeErr, os.ErrNotExist) {
		return nil
	}
	if !errors.Is(probeErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("refusing to replace unix socket at %s after inconclusive liveness probe: %w", path, probeErr)
	}
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reinspect stale unix socket: %w", err)
	}
	if current.Mode()&os.ModeSocket == 0 || !os.SameFile(observed, current) {
		return fmt.Errorf("refusing to replace unix socket changed during liveness probe at %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale unix socket: %w", err)
	}
	return nil
}

func ensureSocketParent(path string) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create unix socket parent: %w", err)
	}
	info, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("inspect unix socket parent: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("unix socket parent is not a directory: %s", parent)
	}
	return nil
}

type ownedUnixListener struct {
	*net.UnixListener
	path     string
	owned    os.FileInfo
	close    sync.Once
	closeErr error
}

func (listener *ownedUnixListener) Close() error {
	listener.close.Do(func() {
		listener.closeErr = errors.Join(listener.UnixListener.Close(), removeOwnedSocket(listener.path, listener.owned))
	})
	return listener.closeErr
}

func removeOwnedSocket(path string, owned os.FileInfo) error {
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned unix socket: %w", err)
	}
	if current.Mode()&os.ModeSocket == 0 || !os.SameFile(owned, current) {
		return nil
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove owned unix socket: %w", err)
	}
	return nil
}
