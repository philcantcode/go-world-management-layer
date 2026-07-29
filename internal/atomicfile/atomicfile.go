// Package atomicfile publishes small control files only after their complete
// contents have been flushed. It centralizes the crash-safe staging pattern
// used by cache manifests, pins, and sealed bundle metadata.
package atomicfile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func WriteJSON(path string, value any, mode os.FileMode) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	encoded = append(encoded, '\n')
	return Write(path, encoded, mode)
}

func Write(path string, content []byte, mode os.FileMode) error {
	return write(path, content, mode, publishReplace)
}

// WriteExclusive durably publishes content only when path does not already
// exist. The completed staging file, rather than the final path, receives all
// writes so a process death can never expose a partial final file.
func WriteExclusive(path string, content []byte, mode os.FileMode) error {
	return write(path, content, mode, publishExclusive)
}

// PublishExclusive atomically publishes a completed staging file without
// replacing an existing destination. Both paths must share one directory so
// publication cannot cross a filesystem boundary. The caller retains the
// staging file when publication fails.
func PublishExclusive(stagedPath, path string) error {
	stagedAbs, err := filepath.Abs(stagedPath)
	if err != nil {
		return fmt.Errorf("canonicalize staging path: %w", err)
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("canonicalize publication path: %w", err)
	}
	if filepath.Clean(filepath.Dir(stagedAbs)) != filepath.Clean(filepath.Dir(pathAbs)) || filepath.Clean(stagedAbs) == filepath.Clean(pathAbs) {
		return fmt.Errorf("exclusive publication requires distinct paths in one directory")
	}
	info, err := os.Lstat(stagedAbs)
	if err != nil {
		return fmt.Errorf("inspect completed staging file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("completed staging path is not a regular file")
	}
	if err := publishExclusive(stagedAbs, pathAbs); err != nil {
		return fmt.Errorf("publish completed staging file: %w", err)
	}
	return syncDirectory(filepath.Dir(pathAbs))
}

func write(path string, content []byte, mode os.FileMode, publish func(string, string) error) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create parent: %w", err)
	}
	staged, err := os.CreateTemp(directory, ".staging-*")
	if err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}
	stagedPath := staged.Name()
	keep := false
	defer func() {
		_ = staged.Close()
		if !keep {
			_ = os.Remove(stagedPath)
		}
	}()
	if err := staged.Chmod(mode); err != nil {
		return fmt.Errorf("chmod staging file: %w", err)
	}
	if _, err := staged.Write(content); err != nil {
		return fmt.Errorf("write staging file: %w", err)
	}
	if err := staged.Sync(); err != nil {
		return fmt.Errorf("sync staging file: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close staging file: %w", err)
	}
	if err := publish(stagedPath, path); err != nil {
		return fmt.Errorf("publish staging file: %w", err)
	}
	keep = true
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open parent for sync: %w", err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		// Windows does not support flushing directory handles through os.File.
		// The rename is still atomic; Linux production nodes take the durable path.
		if isDirectorySyncUnsupported(err) {
			return nil
		}
		return fmt.Errorf("sync parent: %w", err)
	}
	return nil
}
