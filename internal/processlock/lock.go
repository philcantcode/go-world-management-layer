// Package processlock provides non-blocking, process-wide exclusive ownership
// of a durable control database. Locks are advisory and remain held until the
// returned Owner is released or its process exits. Flock-based systems also
// serialize the canonical parent directory to stabilize the sibling lock name
// for conforming acquirers.
package processlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const lockSuffix = ".worldd.lock"

// ErrAlreadyHeld reports that another owner holds the control database lock.
var ErrAlreadyHeld = errors.New("control state is already owned by another process")

// Owner holds exclusive ownership of one canonical control database path.
type Owner struct {
	mu            sync.Mutex
	file          *os.File
	namespaceFile *os.File
	controlPath   string
	lockPath      string
}

var processClaims = struct {
	sync.Mutex
	paths map[string]struct{}
}{paths: make(map[string]struct{})}

// Acquire claims the sibling lock for controlPath without waiting. The parent
// directory is created when necessary, then resolved before the lock path is
// derived so relative paths and directory symlink aliases converge.
func Acquire(controlPath string) (*Owner, error) {
	canonical, err := canonicalControlPath(controlPath)
	if err != nil {
		return nil, fmt.Errorf("canonicalize control state path: %w", err)
	}
	lockPath := canonical + lockSuffix
	if !claimProcessPath(lockPath) {
		return nil, alreadyHeldError(lockPath)
	}
	file, namespaceFile, err := tryAcquire(canonical, lockPath)
	if err != nil {
		releaseProcessPath(lockPath)
		if errors.Is(err, ErrAlreadyHeld) {
			return nil, alreadyHeldError(lockPath)
		}
		return nil, fmt.Errorf("acquire control state lock %s: %w", lockPath, err)
	}
	return &Owner{file: file, namespaceFile: namespaceFile, controlPath: canonical, lockPath: lockPath}, nil
}

// ControlPath is the canonical absolute path protected by this owner.
func (o *Owner) ControlPath() string {
	if o == nil {
		return ""
	}
	return o.controlPath
}

// LockPath is the canonical sibling file used for inter-process locking.
func (o *Owner) LockPath() string {
	if o == nil {
		return ""
	}
	return o.lockPath
}

// Release relinquishes ownership. It is safe to call more than once.
func (o *Owner) Release() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil
	}
	file := o.file
	namespaceFile := o.namespaceFile
	o.file = nil
	o.namespaceFile = nil
	err := unlockFiles(file, namespaceFile)
	releaseProcessPath(o.lockPath)
	if err != nil {
		return fmt.Errorf("release control state lock %s: %w", o.lockPath, err)
	}
	return nil
}

func requireSameOpenedPath(path string, file *os.File, directory bool) (os.FileInfo, error) {
	handleInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened handle for %q: %w", path, err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect opened path %q: %w", path, err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || pathInfo.IsDir() != directory || (!directory && !pathInfo.Mode().IsRegular()) {
		return nil, fmt.Errorf("opened path %q changed type", path)
	}
	if handleInfo.IsDir() != directory || (!directory && !handleInfo.Mode().IsRegular()) {
		return nil, fmt.Errorf("opened handle for %q has an invalid type", path)
	}
	if !os.SameFile(pathInfo, handleInfo) {
		return nil, fmt.Errorf("opened handle for %q no longer matches its path", path)
	}
	return handleInfo, nil
}

func alreadyHeldError(lockPath string) error {
	return fmt.Errorf("%w (lock %s)", ErrAlreadyHeld, lockPath)
}

func claimProcessPath(lockPath string) bool {
	processClaims.Lock()
	defer processClaims.Unlock()
	if _, held := processClaims.paths[lockPath]; held {
		return false
	}
	processClaims.paths[lockPath] = struct{}{}
	return true
}

func releaseProcessPath(lockPath string) {
	processClaims.Lock()
	delete(processClaims.paths, lockPath)
	processClaims.Unlock()
}

func requireRegularIfPresent(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("path %q is not a regular file", path)
	}
	return nil
}

func canonicalControlPath(controlPath string) (string, error) {
	if strings.TrimSpace(controlPath) == "" {
		return "", fmt.Errorf("control path is required")
	}
	if controlPath == ":memory:" {
		return "", fmt.Errorf("in-memory control state has no process-ownable database path")
	}
	absolute, err := filepath.Abs(controlPath)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	parent := filepath.Dir(absolute)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create control state parent %q: %w", parent, err)
	}

	if info, err := os.Lstat(absolute); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("control state path %q must not be a symlink", absolute)
		}
		resolved, resolveErr := filepath.EvalSymlinks(absolute)
		if resolveErr != nil {
			return "", fmt.Errorf("resolve existing control state %q: %w", absolute, resolveErr)
		}
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect control state %q: %w", absolute, err)
	}

	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("resolve control state parent %q: %w", parent, err)
	}
	return filepath.Join(resolvedParent, filepath.Base(absolute)), nil
}
