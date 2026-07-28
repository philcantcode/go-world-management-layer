//go:build windows

package processlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

const lockRange = ^uint32(0)

func tryLockFile(path string) (*os.File, error) {
	file, handle, err := openWindowsRegular(path, windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_ALWAYS)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = file.Close()
		}
	}()

	var overlapped windows.Overlapped
	if err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, lockRange, lockRange, &overlapped); err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			return nil, ErrAlreadyHeld
		}
		return nil, err
	}
	if err := validateWindowsOpenedFile(path, file, handle); err != nil {
		var unlockOverlapped windows.Overlapped
		_ = windows.UnlockFileEx(handle, 0, lockRange, lockRange, &unlockOverlapped)
		return nil, err
	}
	failed = false
	return file, nil
}

func tryAcquire(controlPath, lockPath string) (*os.File, *os.File, error) {
	file, err := tryLockFile(lockPath)
	if err != nil {
		return nil, nil, err
	}
	if err := validateExistingControlFile(controlPath); err != nil {
		return nil, nil, errors.Join(err, unlockFile(file))
	}
	return file, nil, nil
}

func validateExistingControlFile(path string) error {
	if err := requireRegularIfPresent(path); err != nil {
		return fmt.Errorf("validate control state path: %w", err)
	}
	file, handle, err := openWindowsRegular(path, windows.GENERIC_READ, windows.OPEN_EXISTING)
	if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open existing control state %q: %w", path, err)
	}
	return errors.Join(validateWindowsOpenedFile(path, file, handle), file.Close())
}

func openWindowsRegular(path string, access, disposition uint32) (*os.File, windows.Handle, error) {
	if err := requireRegularIfPresent(path); err != nil {
		return nil, 0, err
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, 0, err
	}
	handle, err := windows.CreateFile(
		name,
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		disposition,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, 0, ErrAlreadyHeld
		}
		return nil, 0, err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, 0, fmt.Errorf("create file handle")
	}
	fileType, err := windows.GetFileType(handle)
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &information); err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	if fileType != windows.FILE_TYPE_DISK || information.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || information.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		_ = file.Close()
		return nil, 0, fmt.Errorf("path %q is not a regular non-reparse file", path)
	}
	return file, handle, nil
}

func validateWindowsOpenedFile(path string, file *os.File, handle windows.Handle) error {
	if _, err := requireSameOpenedPath(path, file, false); err != nil {
		return err
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &information); err != nil {
		return err
	}
	if information.NumberOfLinks != 1 {
		return fmt.Errorf("path %q must have exactly one link; found %d", path, information.NumberOfLinks)
	}
	return nil
}

func unlockFile(file *os.File) error {
	var overlapped windows.Overlapped
	unlockErr := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, lockRange, lockRange, &overlapped)
	return errors.Join(unlockErr, file.Close())
}

func unlockFiles(file, _ *os.File) error {
	return unlockFile(file)
}
