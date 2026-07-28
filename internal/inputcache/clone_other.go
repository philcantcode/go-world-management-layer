//go:build !linux && !windows

package inputcache

import "os"

func cloneFile(string, string, os.FileMode) (bool, error) { return false, nil }
