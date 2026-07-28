//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package processlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func tryLockFile(path string) (*os.File, error) {
	return lockRegularFile(path, flockExclusive)
}

func unlockFile(file *os.File) error {
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return errors.Join(unlockErr, file.Close())
}

func tryAcquire(controlPath, lockPath string) (*os.File, *os.File, error) {
	namespaceFile, err := tryLockDirectory(filepath.Dir(lockPath))
	if err != nil {
		return nil, nil, err
	}
	file, err := tryLockFile(lockPath)
	if err != nil {
		return nil, nil, errors.Join(err, unlockDirectory(namespaceFile))
	}
	if err := validateExistingControlFile(controlPath); err != nil {
		return nil, nil, errors.Join(err, unlockFiles(file, namespaceFile))
	}
	return file, namespaceFile, nil
}

func tryLockDirectory(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("lock namespace %q is not a directory", path)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("create namespace directory handle")
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
	if status.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, fmt.Errorf("lock namespace %q is not a directory", path)
	}
	if err := flockExclusive(fd); err != nil {
		return nil, err
	}
	if _, err := requireSameOpenedPath(path, file, true); err != nil {
		return nil, err
	}
	failed = false
	return file, nil
}

func flockExclusive(fd int) error {
	err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrAlreadyHeld
	}
	return err
}

func unlockDirectory(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return errors.Join(unlockErr, file.Close())
}

func unlockFiles(file, namespaceFile *os.File) error {
	fileErr := unlockFile(file)
	namespaceErr := unlockDirectory(namespaceFile)
	return errors.Join(fileErr, namespaceErr)
}
