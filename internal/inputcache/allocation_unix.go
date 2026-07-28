//go:build !windows

package inputcache

import (
	"io/fs"
	"syscall"
)

func allocatedBytes(info fs.FileInfo) int64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return stat.Blocks * 512
	}
	return info.Size()
}
