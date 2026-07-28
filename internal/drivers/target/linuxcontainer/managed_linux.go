//go:build linux

package linuxcontainer

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const managedDirectoryMode fs.FileMode = 0o700

func materialModeMatches(actual, expected fs.FileMode) bool {
	return actual.Perm() == expected.Perm()
}

// prepareManagedDirectory creates a directory without following any symlink in
// either the configured root or the managed relative path.
func prepareManagedDirectory(root, directory string) error {
	fd, err := openManagedDirectory(root, directory, true)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Fchmod(fd, uint32(managedDirectoryMode.Perm()))
}

func setManagedDirectoryOwner(root, directory string, uid, gid int) error {
	fd, err := openManagedDirectory(root, directory, false)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return setDescriptorOwner(fd, uid, gid)
}

func setManagedTreeOwner(root, directory string, uid, gid int) error {
	fd, err := openManagedDirectory(root, directory, false)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return setDirectoryTreeOwner(fd, uid, gid)
}

func setManagedFileOwner(root, normalized string, uid, gid int) error {
	parts := strings.Split(normalized, "/")
	if len(parts) == 0 {
		return fmt.Errorf("managed file path is empty")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("managed file path has an unsafe component")
		}
	}
	fd, err := openAbsoluteDirectory(root, false)
	if err != nil {
		return err
	}
	if err := setDescriptorOwner(fd, uid, gid); err != nil {
		_ = unix.Close(fd)
		return err
	}
	for _, part := range parts[:len(parts)-1] {
		next, openErr := openDirectoryAt(fd, part, false)
		if openErr != nil {
			_ = unix.Close(fd)
			return openErr
		}
		if ownerErr := setDescriptorOwner(next, uid, gid); ownerErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)
			return ownerErr
		}
		_ = unix.Close(fd)
		fd = next
	}
	file, err := unix.Openat(fd, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	_ = unix.Close(fd)
	if err != nil {
		return fmt.Errorf("open managed file for ownership: %w", err)
	}
	defer unix.Close(file)
	var stat unix.Stat_t
	if err := unix.Fstat(file, &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("managed ownership target is not a regular file")
	}
	return setDescriptorOwner(file, uid, gid)
}

// clearManagedDirectory deliberately preserves directory's inode. Docker bind
// mounts pin that inode when the target is created, so replacing it would make
// subsequently materialized bytes invisible inside the container.
func clearManagedDirectory(root, directory string) error {
	fd, err := openManagedDirectory(root, directory, true)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Fchmod(fd, uint32(managedDirectoryMode.Perm())); err != nil {
		return err
	}
	return clearDirectoryContents(fd)
}

func sealManagedDirectory(root, directory string) error {
	fd, err := openManagedDirectory(root, directory, false)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return sealDirectoryTree(fd)
}

func removeManagedDirectory(root, directory string) error {
	parent, name, err := openManagedParent(root, directory)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	child, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open managed directory for removal: %w", err)
	}
	if err := unix.Fchmod(child, uint32(managedDirectoryMode.Perm())); err != nil {
		_ = unix.Close(child)
		return err
	}
	if err := clearDirectoryContents(child); err != nil {
		_ = unix.Close(child)
		return err
	}
	if err := unix.Close(child); err != nil {
		return err
	}
	if err := unix.Unlinkat(parent, name, unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("remove managed directory: %w", err)
	}
	return nil
}

func openManagedDirectory(root, directory string, create bool) (int, error) {
	parts, err := managedRelativeParts(root, directory)
	if err != nil {
		return -1, err
	}
	fd, err := openAbsoluteDirectory(root, create)
	if err != nil {
		return -1, err
	}
	for _, part := range parts {
		next, openErr := openDirectoryAt(fd, part, create)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func openManagedParent(root, directory string) (int, string, error) {
	parts, err := managedRelativeParts(root, directory)
	if err != nil {
		return -1, "", err
	}
	name := parts[len(parts)-1]
	parent := root
	if len(parts) > 1 {
		parent = filepath.Join(append([]string{root}, parts[:len(parts)-1]...)...)
	}
	fd, err := openManagedDirectoryOrRoot(root, parent)
	return fd, name, err
}

func openManagedDirectoryOrRoot(root, directory string) (int, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return -1, err
	}
	directoryAbsolute, err := filepath.Abs(directory)
	if err != nil {
		return -1, err
	}
	if filepath.Clean(rootAbsolute) == filepath.Clean(directoryAbsolute) {
		return openAbsoluteDirectory(rootAbsolute, false)
	}
	return openManagedDirectory(rootAbsolute, directoryAbsolute, false)
}

func openAbsoluteDirectory(value string, create bool) (int, error) {
	absolute, err := filepath.Abs(value)
	if err != nil {
		return -1, err
	}
	if !filepath.IsAbs(absolute) {
		return -1, fmt.Errorf("managed root is not absolute")
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	parts := strings.FieldsFunc(strings.TrimPrefix(filepath.Clean(absolute), string(filepath.Separator)), func(value rune) bool {
		return value == filepath.Separator
	})
	for _, part := range parts {
		next, openErr := openDirectoryAt(fd, part, create)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func openDirectoryAt(parent int, name string, create bool) (int, error) {
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) && create {
		if mkdirErr := unix.Mkdirat(parent, name, uint32(managedDirectoryMode.Perm())); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
			return -1, fmt.Errorf("create managed directory %q: %w", name, mkdirErr)
		}
		fd, err = unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return -1, fmt.Errorf("open managed directory %q: %w", name, err)
	}
	return fd, nil
}

func managedRelativeParts(root, directory string) ([]string, error) {
	if err := requirePathBeneath(root, directory); err != nil {
		return nil, err
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	directoryAbsolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(rootAbsolute, directoryAbsolute)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(relative, string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("managed directory has an unsafe component")
		}
	}
	return parts, nil
}

func clearDirectoryContents(fd int) error {
	entries, err := readDirectoryEntries(fd)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
			continue
		} else if err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			child, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			if err := unix.Fchmod(child, uint32(managedDirectoryMode.Perm())); err != nil {
				_ = unix.Close(child)
				return err
			}
			if err := clearDirectoryContents(child); err != nil {
				_ = unix.Close(child)
				return err
			}
			if err := unix.Close(child); err != nil {
				return err
			}
			if err := unix.Unlinkat(fd, entry.Name(), unix.AT_REMOVEDIR); err != nil && !errors.Is(err, unix.ENOENT) {
				return err
			}
			continue
		}
		if err := unix.Unlinkat(fd, entry.Name(), 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return unix.Fsync(fd)
}

func sealDirectoryTree(fd int) error {
	entries, err := readDirectoryEntries(fd)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		switch stat.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			child, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			err = sealDirectoryTree(child)
			closeErr := unix.Close(child)
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		case unix.S_IFREG:
			// File permissions are part of TargetMaterialPlan and were applied by
			// the descriptor-anchored atomic writer.
		default:
			return fmt.Errorf("material projection contains a non-regular entry %q", entry.Name())
		}
	}
	return unix.Fchmod(fd, 0o555)
}

func setDirectoryTreeOwner(fd, uid, gid int) error {
	entries, err := readDirectoryEntries(fd)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		switch stat.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			child, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			err = setDirectoryTreeOwner(child, uid, gid)
			closeErr := unix.Close(child)
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		case unix.S_IFREG:
			file, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			err = setDescriptorOwner(file, uid, gid)
			closeErr := unix.Close(file)
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("managed tree contains a non-regular entry %q", entry.Name())
		}
	}
	return setDescriptorOwner(fd, uid, gid)
}

func setDescriptorOwner(fd, uid, gid int) error {
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return err
	}
	if int(before.Uid) == uid && int(before.Gid) == gid {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("managed ownership differs from %d:%d and the node is not root", uid, gid)
	}
	mode := before.Mode
	if err := unix.Fchown(fd, uid, gid); err != nil {
		return err
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return err
	}
	if int(after.Uid) != uid || int(after.Gid) != gid || after.Mode != mode {
		return fmt.Errorf("managed ownership or mode verification failed")
	}
	return nil
}

func readDirectoryEntries(fd int) ([]os.DirEntry, error) {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(duplicate), "world-managed-directory")
	if file == nil {
		_ = unix.Close(duplicate)
		return nil, fmt.Errorf("wrap managed directory descriptor")
	}
	defer file.Close()
	return file.ReadDir(-1)
}
