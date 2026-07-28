//go:build windows

package inputcache

import "io/fs"

func allocatedBytes(info fs.FileInfo) int64 { return info.Size() }
