//go:build windows

package atomicfile

import "golang.org/x/sys/windows"

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
	return windows.MoveFileEx(from, to, flags)
}
