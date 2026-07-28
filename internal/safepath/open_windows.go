//go:build windows

package safepath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func openRegular(root, normalized string) (*File, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return nil, fmt.Errorf("resolve root links: %w", err)
	}

	current := rootResolved
	parts := strings.Split(normalized, "/")
	var before os.FileInfo
	for index, part := range parts {
		current = filepath.Join(current, part)
		before, err = os.Lstat(current)
		if err != nil {
			return nil, fmt.Errorf("inspect component %q: %w", part, err)
		}
		if before.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: symlink component %q", ErrUnsafe, part)
		}
		if index < len(parts)-1 && !before.IsDir() {
			return nil, fmt.Errorf("%w: non-directory component %q", ErrUnsafe, part)
		}
	}

	opened, err := os.Open(current)
	if err != nil {
		return nil, fmt.Errorf("open beneath root: %w", err)
	}
	after, err := opened.Stat()
	if err != nil {
		_ = opened.Close()
		return nil, fmt.Errorf("stat open file: %w", err)
	}
	if before == nil || !os.SameFile(before, after) {
		_ = opened.Close()
		return nil, fmt.Errorf("%w: file identity changed while opening", ErrUnsafe)
	}
	return newFile(opened, after)
}
