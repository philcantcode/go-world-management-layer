//go:build !linux

package safepath

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func writeRegularAtomic(root, normalized string, mode fs.FileMode, write func(io.Writer) error) error {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return fmt.Errorf("resolve write root: %w", err)
	}
	parts := strings.Split(normalized, "/")
	parent := rootResolved
	for _, part := range parts[:len(parts)-1] {
		parent = filepath.Join(parent, part)
		info, statErr := os.Lstat(parent)
		if os.IsNotExist(statErr) {
			if mkdirErr := os.Mkdir(parent, 0o700); mkdirErr != nil && !os.IsExist(mkdirErr) {
				return mkdirErr
			}
			info, statErr = os.Lstat(parent)
		}
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrUnsafe
		}
	}
	parentHandle, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer parentHandle.Close()
	parentIdentity, err := parentHandle.Stat()
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(parent, ".world-write-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := requireSameDirectory(parent, parentIdentity); err != nil {
		return err
	}
	if err := write(temporary); err != nil {
		return err
	}
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := requireSameDirectory(parent, parentIdentity); err != nil {
		return err
	}
	destination := filepath.Join(parent, parts[len(parts)-1])
	if info, statErr := os.Lstat(destination); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return ErrUnsafe
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return statErr
	}
	if err := os.Rename(temporaryName, destination); err != nil {
		return err
	}
	committed = true
	return nil
}

func requireSameDirectory(path string, identity fs.FileInfo) error {
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(identity, current) {
		return fmt.Errorf("%w: destination directory identity changed", ErrUnsafe)
	}
	return nil
}
