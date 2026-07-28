//go:build linux

package docker

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPrepareWorkspaceAccessChangesOwnershipWithoutWideningModes(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("ownership handoff requires root")
	}
	root := t.TempDir()
	nested := filepath.Join(root, "input")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(nested, "specimen.bin")
	if err := os.WriteFile(input, []byte("specimen"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := prepareWorkspaceAccess(root, defaultGuestUser); err != nil {
		t.Fatal(err)
	}
	for path, wantMode := range map[string]os.FileMode{root: 0o700, nested: 0o700, input: 0o400} {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 65532 || stat.Gid != 65532 {
			t.Fatalf("%s ownership = %#v", path, info.Sys())
		}
		if info.Mode().Perm() != wantMode {
			t.Fatalf("%s mode = %04o, want %04o", path, info.Mode().Perm(), wantMode)
		}
	}
}
