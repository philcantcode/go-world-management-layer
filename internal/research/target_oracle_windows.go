//go:build windows

package research

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// readOracleFileTail opens an absolute path without following reparse points
// (symlinks, junctions, mount points). Reparse targets are refused.
func readOracleFileTail(path string, limit int64) ([]byte, int64, error) {
	pathUTF16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, 0, err
	}
	// Refuse known reparse attributes before open (symlink/junction).
	attrs, err := windows.GetFileAttributes(pathUTF16)
	if err != nil {
		return nil, 0, err
	}
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return nil, 0, errOracleNotRegular
	}
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		return nil, 0, errOracleNotRegular
	}

	before, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, 0, errOracleNotRegular
	}

	// Open with FILE_FLAG_OPEN_REPARSE_POINT so we open the named object itself
	// rather than following a reparse that appeared after GetFileAttributes.
	handle, err := windows.CreateFile(
		pathUTF16,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, 0, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		windows.CloseHandle(handle)
		return nil, 0, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		windows.CloseHandle(handle)
		return nil, 0, errOracleNotRegular
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		windows.CloseHandle(handle)
		return nil, 0, errOracleNotRegular
	}

	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		windows.CloseHandle(handle)
		return nil, 0, fmt.Errorf("wrap oracle handle")
	}
	defer file.Close()

	after, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !os.SameFile(before, after) {
		return nil, 0, fmt.Errorf("oracle file identity changed")
	}
	if !after.Mode().IsRegular() {
		return nil, 0, errOracleNotRegular
	}
	return readTailFromHandle(file, limit)
}
