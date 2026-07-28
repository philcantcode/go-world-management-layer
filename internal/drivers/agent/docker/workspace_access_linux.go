//go:build linux

package docker

import (
	"fmt"
	"os"
	"syscall"
)

func applyWorkspaceOwnership(entries []workspaceAccessEntry, uid, gid int) error {
	needsOwnershipChange := false
	for _, entry := range entries {
		stat, ok := entry.info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("workspace entry %q has no Unix ownership metadata", entry.path)
		}
		if int(stat.Uid) != uid || int(stat.Gid) != gid {
			needsOwnershipChange = true
		}
	}
	if !needsOwnershipChange {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("workspace ownership differs from %d:%d and the node is not root", uid, gid)
	}
	// Descendants are handed over before their parents so no intermediate
	// directory becomes inaccessible part-way through the operation.
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		mode := entry.info.Mode().Perm()
		if err := os.Lchown(entry.path, uid, gid); err != nil {
			return fmt.Errorf("set workspace ownership for %q: %w", entry.path, err)
		}
		current, err := os.Lstat(entry.path)
		if err != nil {
			return fmt.Errorf("verify workspace ownership for %q: %w", entry.path, err)
		}
		stat, ok := current.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != uid || int(stat.Gid) != gid || current.Mode().Perm() != mode {
			return fmt.Errorf("workspace ownership or mode verification failed for %q", entry.path)
		}
	}
	return nil
}
