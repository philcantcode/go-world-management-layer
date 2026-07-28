package docker

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPrepareWorkspaceAccessRejectsLinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := prepareWorkspaceAccess(root, testGuestUser(t)); err == nil {
		t.Fatal("workspace containing a symlink was accepted")
	}
}

func TestPrepareWorkspaceAccessAcceptsNumericGuestIdentity(t *testing.T) {
	root := t.TempDir()
	if err := prepareWorkspaceAccess(root, testGuestUser(t)); err != nil {
		t.Fatal(err)
	}
}

// testGuestUser returns an identity the current process can already own.
// Production defaults to 65532:65532 and requires root handoff on Linux; unit
// tests on non-root CI runners use the current uid/gid so ownership is a no-op.
func testGuestUser(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "linux" && os.Geteuid() != 0 {
		return fmt.Sprintf("%d:%d", os.Geteuid(), os.Getegid())
	}
	return defaultGuestUser
}
