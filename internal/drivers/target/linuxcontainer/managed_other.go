//go:build !linux

package linuxcontainer

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func materialModeMatches(actual, expected fs.FileMode) bool {
	if runtime.GOOS == "windows" {
		// Windows exposes only a read-only bit through os.FileMode. Production
		// Docker targets require a Linux host; this preserves meaningful local
		// verification without pretending Windows can report POSIX group bits.
		return actual.Perm()&0o200 == expected.Perm()&0o200
	}
	return actual.Perm() == expected.Perm()
}

func prepareManagedDirectory(root, directory string) error {
	if err := ensureManagedRoot(root); err != nil {
		return err
	}
	parts, err := managedRelativePartsOther(root, directory)
	if err != nil {
		return err
	}
	current, _ := filepath.Abs(root)
	for _, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if os.IsNotExist(statErr) {
			if mkdirErr := os.Mkdir(current, 0o700); mkdirErr != nil && !os.IsExist(mkdirErr) {
				return mkdirErr
			}
			info, statErr = os.Lstat(current)
		}
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed path component %q is not a real directory", part)
		}
	}
	return os.Chmod(current, 0o700)
}

// Docker Desktop projects host ACLs into its Linux VM. Numeric ownership is
// therefore not portable on non-Linux hosts, while the descriptor/path
// boundary remains enforced by the surrounding helpers.
func setManagedDirectoryOwner(_ string, _ string, _, _ int) error { return nil }
func setManagedTreeOwner(_ string, _ string, _, _ int) error      { return nil }
func setManagedFileOwner(_ string, _ string, _, _ int) error      { return nil }

func clearManagedDirectory(root, directory string) error {
	if err := prepareManagedDirectory(root, directory); err != nil {
		return err
	}
	if err := makeTreeWritable(directory); err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(directory, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func sealManagedDirectory(root, directory string) error {
	if _, err := managedRelativePartsOther(root, directory); err != nil {
		return err
	}
	if err := requireRealDirectoryPath(directory); err != nil {
		return err
	}
	return filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("material projection contains a symlink")
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o555)
		}
		return nil
	})
}

func removeManagedDirectory(root, directory string) error {
	if _, err := managedRelativePartsOther(root, directory); err != nil {
		return err
	}
	if err := requireRealDirectoryPath(filepath.Dir(directory)); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("managed removal target is not a real directory")
	}
	if err := makeTreeWritable(directory); err != nil {
		return err
	}
	return os.RemoveAll(directory)
}

func ensureManagedRoot(root string) error {
	return walkRealDirectoryPath(root, true)
}

// requireRealDirectoryPath checks every existing path component rather than
// only the leaf. This is the conservative non-Linux containment boundary: a
// configured root beneath a symlink or junction must not redirect managed I/O.
func requireRealDirectoryPath(directory string) error {
	return walkRealDirectoryPath(directory, false)
}

func walkRealDirectoryPath(directory string, create bool) error {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	volume := filepath.VolumeName(absolute)
	current := volume + string(filepath.Separator)
	remainder := strings.TrimPrefix(absolute, current)
	for _, part := range strings.Split(remainder, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if os.IsNotExist(statErr) && create {
			if mkdirErr := os.Mkdir(current, 0o700); mkdirErr != nil && !os.IsExist(mkdirErr) {
				return mkdirErr
			}
			info, statErr = os.Lstat(current)
		}
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("managed path component %q is not a real directory", part)
		}
	}
	return nil
}

func managedRelativePartsOther(root, directory string) ([]string, error) {
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

func makeTreeWritable(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return os.Chmod(path, 0o700)
		}
		return nil
	})
}
