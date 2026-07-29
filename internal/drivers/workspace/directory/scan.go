package directory

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

type ManifestAlias = workspacepkg.Manifest

type workspaceUsage struct {
	bytes  int64
	inodes int64
}

type scanResult struct {
	manifest workspacepkg.Manifest
	err      error
}

func scanManaged(ctx context.Context, root string, byteLimit, inodeLimit int64, at time.Time) (workspacepkg.Manifest, workspaceUsage, error) {
	if byteLimit <= 0 || inodeLimit <= 0 {
		return workspacepkg.Manifest{}, workspaceUsage{}, fmt.Errorf("%w: workspace limits must be positive", workspacepkg.ErrQuotaExceeded)
	}
	limits := workspacepkg.ScanLimits{MaxFiles: boundedInt(inodeLimit), MaxBytes: byteLimit}
	first, err := scanOnce(ctx, root, limits, at)
	if err != nil {
		return workspacepkg.Manifest{}, workspaceUsage{}, err
	}
	firstUsage, err := measureUsage(ctx, root, first, inodeLimit)
	if err != nil {
		return workspacepkg.Manifest{}, workspaceUsage{}, err
	}
	second, err := scanOnce(ctx, root, limits, at)
	if err != nil {
		return workspacepkg.Manifest{}, workspaceUsage{}, err
	}
	secondUsage, err := measureUsage(ctx, root, second, inodeLimit)
	if err != nil {
		return workspacepkg.Manifest{}, workspaceUsage{}, err
	}
	if first.Digest != second.Digest || firstUsage != secondUsage {
		return workspacepkg.Manifest{}, workspaceUsage{}, workspacepkg.ErrConcurrentMutation
	}
	return second, secondUsage, nil
}

func scanOnce(ctx context.Context, root string, limits workspacepkg.ScanLimits, at time.Time) (workspacepkg.Manifest, error) {
	result := make(chan scanResult, 1)
	go func() {
		manifest, err := workspacepkg.Scan(root, limits, at)
		result <- scanResult{manifest: manifest, err: err}
	}()
	select {
	case <-ctx.Done():
		return workspacepkg.Manifest{}, ctx.Err()
	case completed := <-result:
		return completed.manifest, completed.err
	}
}

func measureUsage(ctx context.Context, root string, manifest workspacepkg.Manifest, inodeLimit int64) (workspaceUsage, error) {
	usage := workspaceUsage{}
	for _, entry := range manifest.Entries {
		if entry.Size > int64(^uint64(0)>>1)-usage.bytes {
			return workspaceUsage{}, fmt.Errorf("%w: byte total overflow", workspacepkg.ErrQuotaExceeded)
		}
		usage.bytes += entry.Size
	}
	err := filepath.WalkDir(root, func(hostPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if hostPath == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink %q", safepath.ErrUnsafe, hostPath)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("%w: special entry %q", safepath.ErrNotRegular, hostPath)
		}
		usage.inodes++
		if usage.inodes > inodeLimit {
			return fmt.Errorf("%w: more than %d inodes", workspacepkg.ErrQuotaExceeded, inodeLimit)
		}
		return nil
	})
	if err != nil {
		return workspaceUsage{}, err
	}
	return usage, nil
}

func verifyBaseline(input domain.InputViewManifest, baseline workspacepkg.Manifest) error {
	entries := input.Entries()
	if len(entries) != len(baseline.Entries) {
		return fmt.Errorf("file count is %d, want %d", len(baseline.Entries), len(entries))
	}
	indexed := make(map[string]workspacepkg.Entry, len(baseline.Entries))
	for _, entry := range baseline.Entries {
		indexed[entry.Path] = entry
	}
	for _, inputEntry := range entries {
		spec := inputEntry.Spec()
		actual, found := indexed[spec.LogicalPath]
		if !found {
			return fmt.Errorf("missing %q", spec.LogicalPath)
		}
		digest, err := domain.ParseDigest(actual.Digest)
		if err != nil {
			return err
		}
		if digest != spec.Digest || actual.Size != spec.Size || !projectedModeMatches(spec.Mode, actual.Mode) {
			return fmt.Errorf("identity or mode mismatch for %q", spec.LogicalPath)
		}
	}
	return nil
}

func projectedModeMatches(expected, actual uint32) bool {
	if runtime.GOOS != "windows" {
		return expected == actual
	}
	projected := uint32(0o444)
	if expected&0o222 != 0 {
		projected = 0o666
	}
	return actual == projected
}

func requireUnchanged(expected, actual workspacepkg.Manifest) error {
	changes, err := workspacepkg.Diff(expected, actual)
	if err != nil {
		return err
	}
	if len(changes) != 0 {
		return fmt.Errorf("workspace has %d unexpected changes", len(changes))
	}
	return nil
}

func changesBetween(before, after workspacepkg.Manifest) ([]domain.ChangeEntry, error) {
	return workspacepkg.DomainChanges(before, after)
}

func classifyScanError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if domain.ErrorCodeOf(err) != domain.CodeInternal {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return domain.NewError(domain.CodeDeadlineExceeded, operation, "context", "workspace scan exceeded its deadline", err)
	case errors.Is(err, context.Canceled):
		return domain.NewError(domain.CodeUnavailable, operation, "context", "workspace scan was cancelled", err)
	case errors.Is(err, workspacepkg.ErrQuotaExceeded), errors.Is(err, safepath.ErrTooLarge):
		return domain.NewError(domain.CodeResourceExhausted, operation, "quota", "workspace exceeds its byte or inode limit", err)
	case errors.Is(err, workspacepkg.ErrConcurrentMutation):
		return domain.NewError(domain.CodeConflict, operation, "workspace", "workspace changed while it was being scanned", err)
	case errors.Is(err, safepath.ErrUnsafe), errors.Is(err, safepath.ErrNotRegular):
		return domain.NewError(domain.CodeIntegrityViolation, operation, "workspace", "workspace contains a symlink or special file", err)
	case errors.Is(err, fs.ErrNotExist):
		return domain.NewError(domain.CodeNotFound, operation, "workspace", "workspace directory or file disappeared", err)
	default:
		return domain.NewError(domain.CodeUnavailable, operation, "workspace", "workspace could not be scanned", err)
	}
}

func boundedInt(value int64) int {
	maximum := int64(^uint(0) >> 1)
	if value > maximum {
		return int(maximum)
	}
	return int(value)
}
