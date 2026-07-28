package linuxcontainer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareTargetDirectoriesRejectsSymlinkedManagedComponent(t *testing.T) {
	root := writableTempDir(t)
	outside := writableTempDir(t)
	linkedTarget := filepath.Join(root, "linked-target")
	if err := os.Symlink(outside, linkedTarget); err != nil {
		t.Skipf("directory symlink unavailable: %v", err)
	}
	plan := ContainerPlan{TargetDirectory: filepath.Join(linkedTarget, "generations", "1")}
	if err := prepareTargetDirectories(root, plan); err == nil {
		t.Fatal("symlinked target component was accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "generations")); !os.IsNotExist(err) {
		t.Fatalf("managed directory creation escaped through a symlink: %v", err)
	}
}

func TestPrepareTargetDirectoriesRejectsSymlinkedConfiguredRootAncestor(t *testing.T) {
	base := writableTempDir(t)
	outside := writableTempDir(t)
	linkedRoot := filepath.Join(base, "linked-root")
	if err := os.Symlink(outside, linkedRoot); err != nil {
		t.Skipf("directory symlink unavailable: %v", err)
	}
	root := filepath.Join(linkedRoot, "targets")
	plan := ContainerPlan{TargetDirectory: filepath.Join(root, "target", "generations", "1")}
	if err := prepareTargetDirectories(root, plan); err == nil {
		t.Fatal("configured root beneath a symlink was accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "targets")); !os.IsNotExist(err) {
		t.Fatalf("managed root creation escaped through a symlink: %v", err)
	}
}

func TestClearManagedDirectoryRemovesSymlinkWithoutFollowingIt(t *testing.T) {
	root := writableTempDir(t)
	outside := writableTempDir(t)
	plan := ContainerPlan{TargetDirectory: filepath.Join(root, "target", "generations", "1")}
	if err := prepareTargetDirectories(root, plan); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "must-survive")
	if err := os.WriteFile(marker, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(plan.materialRoot(), "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("directory symlink unavailable: %v", err)
	}
	before, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	if err := clearManagedDirectory(root, plan.materialRoot()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("clear replaced the managed root")
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("managed symlink remains: %v", err)
	}
	if content, err := os.ReadFile(marker); err != nil || string(content) != "outside" {
		t.Fatalf("clear followed the symlink outside its root: %q, %v", content, err)
	}
}
