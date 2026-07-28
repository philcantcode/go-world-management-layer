//go:build linux

package safepath

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openRegular(root, normalized string) (*File, error) {
	rootFD, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open root: %w", err)
	}
	defer unix.Close(rootFD)

	how := &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS,
	}
	fd, err := unix.Openat2(rootFD, normalized, how)
	if err != nil {
		return nil, fmt.Errorf("open beneath root: %w", err)
	}
	opened := os.NewFile(uintptr(fd), "workspace-export")
	if opened == nil {
		unix.Close(fd)
		return nil, fmt.Errorf("wrap open descriptor")
	}
	info, err := opened.Stat()
	if err != nil {
		_ = opened.Close()
		return nil, fmt.Errorf("stat open descriptor: %w", err)
	}
	return newFile(opened, info)
}
