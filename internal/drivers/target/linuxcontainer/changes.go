package linuxcontainer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

const (
	defaultTargetChangeBytes  = int64(64 << 20)
	defaultTargetChangeInodes = int64(4096)
)

type targetTreeUsage struct {
	bytes  int64
	inodes int64
}

type targetScanResult struct {
	manifest workspacepkg.Manifest
	err      error
}

func scanTargetWritable(ctx context.Context, plan ContainerPlan, at time.Time) (workspacepkg.Manifest, error) {
	byteLimit, inodeLimit := plan.Resources.StorageBytes, plan.Resources.Inodes
	if byteLimit == 0 {
		byteLimit = defaultTargetChangeBytes
	}
	if inodeLimit == 0 {
		inodeLimit = defaultTargetChangeInodes
	}
	limits := workspacepkg.ScanLimits{MaxFiles: boundedTargetInt(inodeLimit), MaxBytes: byteLimit}
	first, err := scanTargetOnce(ctx, plan.writableRoot(), limits, at)
	if err != nil {
		return workspacepkg.Manifest{}, err
	}
	firstUsage, err := measureTargetTree(ctx, plan.writableRoot(), first, inodeLimit)
	if err != nil {
		return workspacepkg.Manifest{}, err
	}
	second, err := scanTargetOnce(ctx, plan.writableRoot(), limits, at)
	if err != nil {
		return workspacepkg.Manifest{}, err
	}
	secondUsage, err := measureTargetTree(ctx, plan.writableRoot(), second, inodeLimit)
	if err != nil {
		return workspacepkg.Manifest{}, err
	}
	if first.Digest != second.Digest || firstUsage != secondUsage {
		return workspacepkg.Manifest{}, workspacepkg.ErrConcurrentMutation
	}
	return second, nil
}

func scanTargetOnce(ctx context.Context, root string, limits workspacepkg.ScanLimits, at time.Time) (workspacepkg.Manifest, error) {
	completed := make(chan targetScanResult, 1)
	go func() {
		manifest, err := workspacepkg.Scan(root, limits, at)
		completed <- targetScanResult{manifest: manifest, err: err}
	}()
	select {
	case <-ctx.Done():
		return workspacepkg.Manifest{}, ctx.Err()
	case result := <-completed:
		return result.manifest, result.err
	}
}

func measureTargetTree(ctx context.Context, root string, manifest workspacepkg.Manifest, inodeLimit int64) (targetTreeUsage, error) {
	usage := targetTreeUsage{}
	for _, entry := range manifest.Entries {
		if entry.Size > int64(^uint64(0)>>1)-usage.bytes {
			return targetTreeUsage{}, fmt.Errorf("%w: byte total overflow", workspacepkg.ErrQuotaExceeded)
		}
		usage.bytes += entry.Size
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink %q", safepath.ErrUnsafe, path)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("%w: special entry %q", safepath.ErrNotRegular, path)
		}
		usage.inodes++
		if usage.inodes > inodeLimit {
			return fmt.Errorf("%w: more than %d inodes", workspacepkg.ErrQuotaExceeded, inodeLimit)
		}
		return nil
	})
	return usage, err
}

func classifyTargetScanError(operation string, err error) error {
	if err == nil || domain.ErrorCodeOf(err) != domain.CodeInternal {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return domain.NewError(domain.CodeDeadlineExceeded, operation, "target_changes", "target writable-tree scan exceeded its deadline", err)
	case errors.Is(err, context.Canceled):
		return domain.NewError(domain.CodeUnavailable, operation, "target_changes", "target writable-tree scan was cancelled", err)
	case errors.Is(err, workspacepkg.ErrQuotaExceeded), errors.Is(err, safepath.ErrTooLarge):
		return domain.NewError(domain.CodeResourceExhausted, operation, "target_changes", "target writable tree exceeds its byte or inode limit", err)
	case errors.Is(err, workspacepkg.ErrConcurrentMutation):
		return domain.NewError(domain.CodeConflict, operation, "target_changes", "target writable tree changed while it was being scanned", err)
	case errors.Is(err, safepath.ErrUnsafe), errors.Is(err, safepath.ErrNotRegular):
		return domain.NewError(domain.CodeIntegrityViolation, operation, "target_changes", "target writable tree contains a symlink or special file", err)
	case errors.Is(err, fs.ErrNotExist):
		return domain.NewError(domain.CodeNotFound, operation, "target_changes", "target writable tree disappeared", err)
	default:
		return domain.NewError(domain.CodeUnavailable, operation, "target_changes", "target writable tree could not be scanned", err)
	}
}

func boundedTargetInt(value int64) int {
	maximum := int64(^uint(0) >> 1)
	if value > maximum {
		return int(maximum)
	}
	return int(value)
}
