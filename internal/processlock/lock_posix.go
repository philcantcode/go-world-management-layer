//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package processlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockRegularFile(path string, claim func(int) error) (*os.File, error) {
	if err := requireRegularIfPresent(path); err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("create file handle")
	}
	failed := true
	defer func() {
		if failed {
			_ = file.Close()
		}
	}()

	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil {
		return nil, err
	}
	if status.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, fmt.Errorf("lock path is not a regular file")
	}
	if err := claim(fd); err != nil {
		return nil, err
	}
	if _, err := requireSameOpenedPath(path, file, false); err != nil {
		return nil, err
	}
	if err := requireSingleLink(fd, path); err != nil {
		return nil, err
	}
	failed = false
	return file, nil
}

func validateExistingControlFile(path string) error {
	if err := requireRegularIfPresent(path); err != nil {
		return fmt.Errorf("validate control state path: %w", err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open existing control state %q: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("create control state file handle")
	}
	validationErr := validateOpenedRegularFile(path, file, fd)
	return errors.Join(validationErr, file.Close())
}

func validateOpenedRegularFile(path string, file *os.File, fd int) error {
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil {
		return err
	}
	if status.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("path %q is not a regular file", path)
	}
	if _, err := requireSameOpenedPath(path, file, false); err != nil {
		return err
	}
	return requireSingleLink(fd, path)
}

func requireSingleLink(fd int, path string) error {
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil {
		return err
	}
	if status.Nlink != 1 {
		return fmt.Errorf("path %q must have exactly one link; found %d", path, status.Nlink)
	}
	return nil
}
