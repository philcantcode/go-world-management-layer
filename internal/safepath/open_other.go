//go:build !linux && !windows && !darwin

package safepath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Non-Linux clients are supported, but production physical hosts are Linux. This
// conservative fallback rejects every symlink component and verifies that the
// opened descriptor still identifies the inspected file.
func openRegular(root, normalized string) (*File, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	current := rootAbsolute
	var before os.FileInfo
	for index, part := range strings.Split(normalized, "/") {
		current = filepath.Join(current, part)
		before, err = os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if before.Mode()&os.ModeSymlink != 0 || (index < len(strings.Split(normalized, "/"))-1 && !before.IsDir()) {
			return nil, ErrUnsafe
		}
	}
	opened, err := os.Open(current)
	if err != nil {
		return nil, err
	}
	after, err := opened.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = opened.Close()
		return nil, fmt.Errorf("%w: file identity changed", ErrUnsafe)
	}
	return newFile(opened, after)
}
