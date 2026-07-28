package docker

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
)

type workspaceAccessEntry struct {
	path string
	info fs.FileInfo
}

// prepareWorkspaceAccess hands only the validated bind root to the numeric
// guest identity. Permission bits are preserved exactly: ownership, rather
// than world-readable/world-writable modes, makes the tree usable by the
// unprivileged container process.
func prepareWorkspaceAccess(root, user string) error {
	uid, gid, err := dockercli.ParseNumericUser(user)
	if err != nil {
		return err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve workspace mount: %w", err)
	}
	// Canonicalize through EvalSymlinks so host path aliases (Windows TEMP
	// junctions, path rewrites) do not fail closed when the leaf tree itself
	// is clean. The walk below still rejects symlinks inside the mount.
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return fmt.Errorf("resolve workspace mount components: %w", err)
	}
	absolute = filepath.Clean(resolved)
	entries := make([]workspaceAccessEntry, 0, 16)
	err = filepath.WalkDir(absolute, func(hostPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("workspace contains a symlink or special entry at %q", hostPath)
		}
		entries = append(entries, workspaceAccessEntry{path: hostPath, info: info})
		return nil
	})
	if err != nil {
		return fmt.Errorf("inspect workspace mount: %w", err)
	}
	if len(entries) == 0 || !entries[0].info.IsDir() {
		return fmt.Errorf("workspace mount is not a directory")
	}
	return applyWorkspaceOwnership(entries, uid, gid)
}
