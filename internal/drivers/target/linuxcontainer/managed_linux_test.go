//go:build linux

package linuxcontainer

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestTargetOwnershipHandoffPreservesMaterialModes(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("numeric target ownership handoff requires a root production node")
	}
	root := t.TempDir()
	plan := ContainerPlan{TargetDirectory: filepath.Join(root, "target", "generations", "1"), User: defaultTargetUser}
	if err := prepareTargetDirectories(root, plan); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(plan.materialRoot(), "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	material := filepath.Join(nested, "specimen")
	if err := os.WriteFile(material, []byte("specimen"), 0o440); err != nil {
		t.Fatal(err)
	}
	if err := applyTargetTreeOwnership(root, plan.materialRoot(), plan.User); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{plan.TargetDirectory, plan.writableRoot(), plan.materialRoot(), nested, material} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 65532 || stat.Gid != 65532 {
			t.Fatalf("ownership for %q = %#v", path, info.Sys())
		}
	}
	info, err := os.Stat(material)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o440 {
		t.Fatalf("material mode = %v", info.Mode().Perm())
	}
	pushDirectory := filepath.Join(plan.writableRoot(), "nested-push")
	if err := os.Mkdir(pushDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	pushFile := filepath.Join(pushDirectory, "tool")
	if err := os.WriteFile(pushFile, []byte("tool"), 0o550); err != nil {
		t.Fatal(err)
	}
	if err := setManagedFileOwner(plan.writableRoot(), "nested-push/tool", 65532, 65532); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{pushDirectory, pushFile} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		if stat.Uid != 65532 || stat.Gid != 65532 {
			t.Fatalf("pushed ownership for %q = %d:%d", path, stat.Uid, stat.Gid)
		}
	}
}
