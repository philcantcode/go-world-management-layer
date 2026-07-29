package directory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func prepareRoot(configured string) (string, error) {
	if strings.TrimSpace(configured) == "" {
		return "", fmt.Errorf("workspace root is required")
	}
	absolute, err := filepath.Abs(configured)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return "", fmt.Errorf("create workspace root: %w", err)
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf("inspect workspace root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("workspace root must be a real directory, not a symlink or special file")
	}
	resolved := absolute
	if runtime.GOOS != "windows" {
		resolved, err = filepath.EvalSymlinks(absolute)
		if err != nil {
			return "", fmt.Errorf("resolve workspace root components: %w", err)
		}
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("normalize workspace root: %w", err)
	}
	if err := os.Chmod(resolved, 0o700); err != nil {
		return "", fmt.Errorf("restrict workspace root: %w", err)
	}
	return filepath.Clean(resolved), nil
}

func requireExactWorkspaceDirectory(root, candidate string, workspaceID domain.WorkspaceID) error {
	if workspaceID.IsZero() {
		return fmt.Errorf("workspace id is empty")
	}
	expected := filepath.Join(root, workspaceID.String())
	if filepath.Clean(candidate) != filepath.Clean(expected) {
		return fmt.Errorf("workspace path is not the exact id-derived directory")
	}
	if err := requireContainedChild(root, candidate); err != nil {
		return err
	}
	info, err := os.Lstat(candidate)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("workspace path is not a real directory")
	}
	return nil
}

func requireManagedDirectory(parent, candidate string) error {
	if err := requireContainedChild(parent, candidate); err != nil {
		return err
	}
	info, err := os.Lstat(candidate)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("managed path is not a real directory")
	}
	return nil
}

func isWorkspaceStagingName(name string) bool {
	if !strings.HasPrefix(name, ".") {
		return false
	}
	workspaceText, suffix, found := strings.Cut(strings.TrimPrefix(name, "."), ".prepare-")
	if !found || !safeStagingSuffix(suffix) {
		return false
	}
	workspaceID, err := domain.ParseWorkspaceID(workspaceText)
	return err == nil && name == "."+workspaceID.String()+".prepare-"+suffix
}

func isSnapshotStagingName(name string) bool {
	const prefix = ".sealed-"
	return strings.HasPrefix(name, prefix) && safeStagingSuffix(strings.TrimPrefix(name, prefix))
}

// removeSnapshotStagingDirectories removes only the exact private directory
// namespace created by os.MkdirTemp during sealed-snapshot publication. A
// staging-shaped file, link, or malformed name is an integrity error and is
// never followed or removed.
func removeSnapshotStagingDirectories(workspaceRoot string) error {
	entries, err := os.ReadDir(workspaceRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".sealed") {
			continue
		}
		if !isSnapshotStagingName(name) {
			return fmt.Errorf("workspace contains malformed sealed-snapshot staging entry %q", name)
		}
		candidate := filepath.Join(workspaceRoot, name)
		if err := removeStagingDirectory(workspaceRoot, candidate); err != nil {
			return fmt.Errorf("remove incomplete sealed-snapshot staging directory %q: %w", name, err)
		}
	}
	return nil
}

func safeStagingSuffix(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func removeStagingDirectory(root, candidate string) error {
	if err := requireManagedDirectory(root, candidate); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := removeTreeContext(ctx, candidate); err != nil {
		return err
	}
	if _, err := os.Lstat(candidate); !os.IsNotExist(err) {
		if err == nil {
			return fmt.Errorf("staging directory remains present after removal")
		}
		return fmt.Errorf("prove staging directory absence: %w", err)
	}
	return syncDirectory(root)
}

func requireContainedChild(root, candidate string) error {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	candidateAbsolute, err := filepath.Abs(candidate)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbsolute, candidateAbsolute)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("managed path is outside its configured root")
	}
	return nil
}

func removeWorkspaceTree(ctx context.Context, workspaceRoot string) error {
	entries, err := os.ReadDir(workspaceRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.Name() == recordFilename {
			continue
		}
		if err := removeTreeContext(ctx, filepath.Join(workspaceRoot, entry.Name())); err != nil {
			return err
		}
	}
	if err := contextError(ctx, "workspace.directory.release"); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(workspaceRoot, recordFilename)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := contextError(ctx, "workspace.directory.release"); err != nil {
		return err
	}
	if err := os.Remove(workspaceRoot); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func removeTreeContext(ctx context.Context, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return os.Remove(target)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := removeTreeContext(ctx, filepath.Join(target, entry.Name())); err != nil {
			return err
		}
	}
	return os.Remove(target)
}

func cleanupManagedDirectory(root, candidate string) {
	if err := requireContainedChild(root, candidate); err != nil {
		return
	}
	info, err := os.Lstat(candidate)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = removeTreeContext(ctx, candidate)
}

func publishDirectory(ctx context.Context, staging, destination string) error {
	const retryWindow = time.Second
	deadline := time.NewTimer(retryWindow)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if err := os.Rename(staging, destination); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if _, err := os.Lstat(destination); err == nil || !os.IsNotExist(err) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return lastErr
		case <-ticker.C:
		}
	}
}

func syncDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}
