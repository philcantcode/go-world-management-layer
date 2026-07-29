//go:build unix

package research

import (
	"fmt"
	"os"
	"syscall"
)

// readOracleFileTail opens an absolute path with O_NOFOLLOW and fstats after open.
func readOracleFileTail(path string, limit int64) ([]byte, int64, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return nil, 0, errOracleNotRegular
	}
	if !before.Mode().IsRegular() {
		return nil, 0, errOracleNotRegular
	}
	handle, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, 0, err
	}
	defer handle.Close()
	after, err := handle.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !os.SameFile(before, after) {
		return nil, 0, fmt.Errorf("oracle file identity changed")
	}
	if !after.Mode().IsRegular() {
		return nil, 0, errOracleNotRegular
	}
	return readTailFromHandle(handle, limit)
}
