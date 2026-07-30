//go:build darwin

package safepath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openRegular walks each path component with openat(O_NOFOLLOW) beneath the
// trusted root. Darwin has no openat2 RESOLVE_* flags, so component opens and
// post-open identity checks provide the symlink safety boundary.
func openRegular(root, normalized string) (*File, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(absolute, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open root: %w", err)
	}
	defer unix.Close(rootFD)

	parts := strings.Split(normalized, "/")
	parentFD := rootFD
	owned := -1
	closeOwned := func() {
		if owned >= 0 {
			_ = unix.Close(owned)
			owned = -1
		}
	}
	defer closeOwned()

	for index, part := range parts {
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if index < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		nextFD, openErr := unix.Openat(parentFD, part, flags, 0)
		if openErr != nil {
			return nil, fmt.Errorf("open beneath root: %w", openErr)
		}
		if owned >= 0 {
			_ = unix.Close(owned)
		}
		owned = nextFD
		parentFD = nextFD
	}

	var info unix.Stat_t
	if err := unix.Fstat(owned, &info); err != nil {
		return nil, fmt.Errorf("stat open descriptor: %w", err)
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, ErrNotRegular
	}
	opened := os.NewFile(uintptr(owned), "workspace-export")
	if opened == nil {
		return nil, fmt.Errorf("wrap open descriptor")
	}
	owned = -1
	stat, err := opened.Stat()
	if err != nil {
		_ = opened.Close()
		return nil, fmt.Errorf("stat open descriptor: %w", err)
	}
	return newFile(opened, stat)
}
