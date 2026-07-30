//go:build darwin

package safepath

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Darwin durable namespaces use openat(O_NOFOLLOW) directory pinning and
// renameatx_np(RENAME_EXCL) for non-replace publication. macOS lacks Linux
// openat2 RESOLVE_* flags, so every component is opened with O_NOFOLLOW and
// revalidated by device/inode identity while the directory fd is held.

type namespaceState struct {
	rootFD  int
	dirFD   int
	logical string
	rootID  unix.Stat_t
	dirID   unix.Stat_t
}

func openNamespaceState(root, logical string) (*namespaceState, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(absolute, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open namespace root: %w", err)
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = unix.Close(rootFD)
		}
	}()
	var rootID unix.Stat_t
	if err := unix.Fstat(rootFD, &rootID); err != nil {
		return nil, fmt.Errorf("inspect namespace root: %w", err)
	}
	if err := requireSafeNamespaceDirectory(rootID); err != nil {
		return nil, err
	}

	parentFD := rootFD
	opened := make([]int, 0, len(strings.Split(logical, "/")))
	defer func() {
		for _, fd := range opened {
			if fd != parentFD || closeRoot {
				_ = unix.Close(fd)
			}
		}
	}()
	for _, part := range strings.Split(logical, "/") {
		nextFD, openErr := unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(parentFD, part, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				return nil, fmt.Errorf("create namespace directory %q: %w", part, mkdirErr)
			}
			if syncErr := unix.Fsync(parentFD); syncErr != nil {
				return nil, fmt.Errorf("sync namespace parent: %w", syncErr)
			}
			nextFD, openErr = unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		if openErr != nil {
			return nil, fmt.Errorf("open namespace directory %q: %w", part, openErr)
		}
		var info unix.Stat_t
		if err := unix.Fstat(nextFD, &info); err != nil {
			_ = unix.Close(nextFD)
			return nil, err
		}
		if err := requireSafeNamespaceDirectory(info); err != nil {
			_ = unix.Close(nextFD)
			return nil, err
		}
		opened = append(opened, nextFD)
		parentFD = nextFD
	}
	var dirID unix.Stat_t
	if err := unix.Fstat(parentFD, &dirID); err != nil {
		return nil, err
	}
	state := &namespaceState{rootFD: rootFD, dirFD: parentFD, logical: logical, rootID: rootID, dirID: dirID}
	closeRoot = false
	for _, fd := range opened[:len(opened)-1] {
		_ = unix.Close(fd)
	}
	opened = nil
	return state, nil
}

func requireSafeNamespaceDirectory(info unix.Stat_t) error {
	if info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Mode&0o022 != 0 {
		return fmt.Errorf("%w: namespace directory is not a private directory", ErrUnsafe)
	}
	return nil
}

func (n *namespaceState) revalidate() error {
	var root unix.Stat_t
	if err := unix.Fstat(n.rootFD, &root); err != nil {
		return err
	}
	if !sameDarwinObject(root, n.rootID) {
		return fmt.Errorf("%w: namespace root identity changed", ErrUnsafe)
	}
	currentFD, err := openDarwinDirectoryPath(n.rootFD, n.logical)
	if err != nil {
		return fmt.Errorf("%w: namespace directory cannot be re-opened: %v", ErrUnsafe, err)
	}
	defer unix.Close(currentFD)
	var current, held unix.Stat_t
	if err := unix.Fstat(currentFD, &current); err != nil {
		return err
	}
	if err := unix.Fstat(n.dirFD, &held); err != nil {
		return err
	}
	if !sameDarwinObject(current, n.dirID) || !sameDarwinObject(held, n.dirID) {
		return fmt.Errorf("%w: namespace directory identity changed", ErrUnsafe)
	}
	return requireSafeNamespaceDirectory(current)
}

func openDarwinDirectoryPath(rootFD int, logical string) (int, error) {
	parentFD := rootFD
	owned := -1
	for _, part := range strings.Split(logical, "/") {
		nextFD, err := unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if owned >= 0 {
			_ = unix.Close(owned)
		}
		if err != nil {
			return -1, err
		}
		owned, parentFD = nextFD, nextFD
	}
	return owned, nil
}

func (n *namespaceState) listNames() ([]string, error) {
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(n.dirFD, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), "safe-namespace")
	if directory == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("wrap namespace directory")
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	return names, nil
}

func (n *namespaceState) readRegularBounded(name string, maximum int64) ([]byte, error) {
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	file, before, err := n.openRegular(name, unix.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if before.Size > maximum {
		return nil, ErrTooLarge
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maximum {
		return nil, ErrTooLarge
	}
	var after unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &after); err != nil {
		return nil, err
	}
	if !sameDarwinRegularSnapshot(before, after) || int64(len(content)) != after.Size {
		return nil, fmt.Errorf("%w: regular file changed while being read", ErrUnsafe)
	}
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	return content, nil
}

func (n *namespaceState) openRegular(name string, access int) (*os.File, unix.Stat_t, error) {
	fd, err := unix.Openat(n.dirFD, name, access|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, unix.Stat_t{}, err
	}
	file := os.NewFile(uintptr(fd), "safe-namespace-file")
	if file == nil {
		_ = unix.Close(fd)
		return nil, unix.Stat_t{}, fmt.Errorf("wrap namespace file")
	}
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		_ = file.Close()
		return nil, unix.Stat_t{}, err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Nlink != 1 {
		_ = file.Close()
		return nil, unix.Stat_t{}, fmt.Errorf("%w: namespace entry is not a single-link regular file", ErrUnsafe)
	}
	return file, info, nil
}

func (n *namespaceState) writeRegularAtomic(name string, content []byte, mode fs.FileMode, replace bool) error {
	if err := n.revalidate(); err != nil {
		return err
	}
	if replace {
		if file, _, err := n.openRegular(name, unix.O_RDONLY); err == nil {
			_ = file.Close()
		} else if !errors.Is(err, unix.ENOENT) {
			return err
		}
	} else if existing, err := n.readRegularBounded(name, int64(len(content))); err == nil {
		if bytes.Equal(existing, content) {
			return nil
		}
		return ErrConflict
	} else if !errors.Is(err, unix.ENOENT) {
		if errors.Is(err, ErrTooLarge) {
			return ErrConflict
		}
		return err
	}

	temporary, err := namespaceTemporaryName()
	if err != nil {
		return err
	}
	fd, err := unix.Openat(n.dirFD, temporary, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return fmt.Errorf("create namespace staging file: %w", err)
	}
	file := os.NewFile(uintptr(fd), "safe-namespace-staging")
	if file == nil {
		_ = unix.Close(fd)
		_ = unix.Unlinkat(n.dirFD, temporary, 0)
		return fmt.Errorf("wrap namespace staging file")
	}
	published := false
	defer func() {
		_ = file.Close()
		if !published {
			_ = unix.Unlinkat(n.dirFD, temporary, 0)
		}
	}()
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := file.Chmod(mode.Perm()); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if replace {
		err = unix.Renameat(n.dirFD, temporary, n.dirFD, name)
	} else {
		err = unix.RenameatxNp(n.dirFD, temporary, n.dirFD, name, unix.RENAME_EXCL)
	}
	if err != nil {
		if !replace && (errors.Is(err, unix.EEXIST) || errors.Is(err, os.ErrExist)) {
			existing, readErr := n.readRegularBounded(name, int64(len(content)))
			if readErr == nil && bytes.Equal(existing, content) {
				return nil
			}
			if readErr == nil || errors.Is(readErr, ErrTooLarge) {
				return ErrConflict
			}
			return readErr
		}
		return fmt.Errorf("publish namespace file: %w", err)
	}
	published = true
	if err := unix.Fsync(n.dirFD); err != nil {
		return err
	}
	return n.revalidate()
}

func (n *namespaceState) removeRegular(name string) error {
	if err := n.revalidate(); err != nil {
		return err
	}
	file, before, err := n.openRegular(name, unix.O_RDONLY)
	if err != nil {
		return err
	}
	defer file.Close()
	quarantine, err := namespaceTemporaryName()
	if err != nil {
		return err
	}
	if err := unix.RenameatxNp(n.dirFD, name, n.dirFD, quarantine, unix.RENAME_EXCL); err != nil {
		return err
	}
	quarantined, after, err := n.openRegular(quarantine, unix.O_RDONLY)
	if err != nil {
		return fmt.Errorf("%w: quarantined namespace entry cannot be verified: %v", ErrUnsafe, err)
	}
	_ = quarantined.Close()
	if !sameDarwinObject(before, after) || before.Nlink != after.Nlink {
		return fmt.Errorf("%w: namespace entry changed before deletion", ErrUnsafe)
	}
	if err := unix.Unlinkat(n.dirFD, quarantine, 0); err != nil {
		return err
	}
	if err := unix.Fsync(n.dirFD); err != nil {
		return err
	}
	return n.revalidate()
}

func (n *namespaceState) close() error {
	dirErr := unix.Close(n.dirFD)
	rootErr := unix.Close(n.rootFD)
	return errors.Join(dirErr, rootErr)
}

func sameDarwinObject(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino
}

func sameDarwinRegularSnapshot(left, right unix.Stat_t) bool {
	return sameDarwinObject(left, right) && left.Mode == right.Mode && left.Nlink == right.Nlink && left.Size == right.Size &&
		left.Mtim.Sec == right.Mtim.Sec && left.Mtim.Nsec == right.Mtim.Nsec && left.Ctim.Sec == right.Ctim.Sec && left.Ctim.Nsec == right.Ctim.Nsec
}
