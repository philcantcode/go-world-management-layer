package docker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareWorkspaceAccessRejectsLinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := prepareWorkspaceAccess(root, defaultGuestUser); err == nil {
		t.Fatal("workspace containing a symlink was accepted")
	}
}

func TestPrepareWorkspaceAccessAcceptsNumericGuestIdentity(t *testing.T) {
	root := t.TempDir()
	if err := prepareWorkspaceAccess(root, defaultGuestUser); err != nil {
		t.Fatal(err)
	}
}
