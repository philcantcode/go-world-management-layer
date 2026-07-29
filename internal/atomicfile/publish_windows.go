//go:build windows

package atomicfile

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

const (
	windowsReplaceRetryLimit = time.Second
	windowsReplaceRetryDelay = time.Millisecond
	windowsReplaceRetryMax   = 25 * time.Millisecond
)

func publishReplace(stagedPath, path string) error {
	return moveFile(stagedPath, path, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func publishExclusive(stagedPath, path string) error {
	return moveFile(stagedPath, path, windows.MOVEFILE_WRITE_THROUGH)
}

func moveFile(stagedPath, path string, flags uint32) error {
	from, err := windows.UTF16PtrFromString(stagedPath)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	started := time.Now()
	delay := windowsReplaceRetryDelay
	for {
		err = windows.MoveFileEx(from, to, flags)
		if err == nil || !retryableWindowsReplacement(err, path, flags) {
			return err
		}
		remaining := windowsReplaceRetryLimit - time.Since(started)
		if remaining <= 0 {
			return err
		}
		if delay > remaining {
			delay = remaining
		}
		time.Sleep(delay)
		if delay < windowsReplaceRetryMax {
			delay *= 2
			if delay > windowsReplaceRetryMax {
				delay = windowsReplaceRetryMax
			}
		}
	}
}

// Windows scanners and other readers can briefly deny replacement even after
// the writer has closed and flushed both files. Retry only the observed
// sharing/access failures, only for replacement, and only while the exact
// destination remains a regular file. Missing, redirected, or permanently
// inaccessible destinations still fail closed.
func retryableWindowsReplacement(err error, path string, flags uint32) bool {
	if flags&windows.MOVEFILE_REPLACE_EXISTING == 0 ||
		(!errors.Is(err, windows.ERROR_ACCESS_DENIED) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION)) {
		return false
	}
	info, statErr := os.Lstat(path)
	return statErr == nil && info.Mode().IsRegular()
}
