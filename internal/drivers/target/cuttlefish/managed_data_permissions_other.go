//go:build !windows

package cuttlefish

import "os"

func managedDataBackingReadOnly(_ string, info os.FileInfo) (bool, error) {
	return info.Mode().Perm()&0o222 == 0, nil
}
