//go:build linux

package inputcache

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func cloneFile(source, destination string, mode os.FileMode) (bool, error) {
	input, err := os.Open(source)
	if err != nil {
		return false, err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false, err
	}
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = os.Remove(destination)
		}
	}()
	if err := unix.IoctlFileClone(int(output.Fd()), int(input.Fd())); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.ENOTTY) || errors.Is(err, syscall.EINVAL) {
			return false, nil
		}
		return false, err
	}
	if err := output.Chmod(mode); err != nil {
		return false, err
	}
	if err := output.Sync(); err != nil {
		return false, err
	}
	keep = true
	return true, nil
}
