//go:build linux

package safepath

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func writeRegularAtomic(root, normalized string, mode fs.FileMode, write func(io.Writer) error) error {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open write root: %w", err)
	}
	fds := []int{rootFD}
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()

	parts := strings.Split(normalized, "/")
	parentFD := rootFD
	for _, part := range parts[:len(parts)-1] {
		nextFD, openErr := unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(parentFD, part, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				return fmt.Errorf("create parent %q: %w", part, mkdirErr)
			}
			nextFD, openErr = unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		if openErr != nil {
			return fmt.Errorf("open parent %q: %w", part, openErr)
		}
		fds = append(fds, nextFD)
		parentFD = nextFD
	}

	destination := parts[len(parts)-1]
	var existing unix.Stat_t
	if statErr := unix.Fstatat(parentFD, destination, &existing, unix.AT_SYMLINK_NOFOLLOW); statErr == nil {
		if existing.Mode&unix.S_IFMT != unix.S_IFREG {
			return ErrUnsafe
		}
	} else if !errors.Is(statErr, unix.ENOENT) {
		return fmt.Errorf("inspect destination: %w", statErr)
	}

	temporary, err := randomTemporaryName()
	if err != nil {
		return err
	}
	fd, err := unix.Openat(parentFD, temporary, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	file := os.NewFile(uintptr(fd), "world-atomic-write")
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("wrap temporary file")
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = unix.Unlinkat(parentFD, temporary, 0)
		}
	}()
	if err := write(file); err != nil {
		return err
	}
	if err := file.Chmod(mode); err != nil {
		return fmt.Errorf("set file mode: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := unix.Renameat(parentFD, temporary, parentFD, destination); err != nil {
		return fmt.Errorf("publish destination: %w", err)
	}
	committed = true
	if err := unix.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync destination directory: %w", err)
	}
	return nil
}

func randomTemporaryName() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create temporary name: %w", err)
	}
	return ".world-write-" + hex.EncodeToString(value[:]), nil
}
