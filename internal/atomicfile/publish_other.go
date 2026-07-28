//go:build !windows

package atomicfile

import "os"

func publishReplace(stagedPath, path string) error {
	return os.Rename(stagedPath, path)
}

func publishExclusive(stagedPath, path string) error {
	if err := os.Link(stagedPath, path); err != nil {
		return err
	}
	return os.Remove(stagedPath)
}
