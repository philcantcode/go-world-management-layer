package directory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

func validatePlanCapacity(plan ports.WorkspacePlan) error {
	const operation = "workspace.directory.prepare"
	var totalBytes int64
	inodes := int64(0)
	directories := make(map[string]struct{})
	for index, entry := range plan.InputView.Entries() {
		spec := entry.Spec()
		if spec.Mode == 0 || spec.Mode&^uint32(0o777) != 0 {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("input_view.entries[%d].mode", index), "copy-backed workspaces require non-zero regular Unix permission bits", nil)
		}
		if spec.Size > plan.UpperByteLimit-totalBytes {
			return domain.NewError(domain.CodeResourceExhausted, operation, "upper_byte_limit", "input view exceeds the workspace byte limit", nil)
		}
		totalBytes += spec.Size
		inodes++
		for parent := path.Dir(spec.LogicalPath); parent != "."; parent = path.Dir(parent) {
			directories[parent] = struct{}{}
		}
	}
	if inodes+int64(len(directories)) > plan.UpperInodeLimit {
		return domain.NewError(domain.CodeResourceExhausted, operation, "upper_inode_limit", "input view exceeds the workspace inode limit", nil)
	}
	return nil
}

func materialize(ctx context.Context, root string, plan ports.WorkspacePlan) error {
	entries := plan.InputView.Entries()
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalPath() < entries[j].LogicalPath() })
	for _, entry := range entries {
		if err := contextError(ctx, "workspace.directory.prepare"); err != nil {
			return err
		}
		spec := entry.Spec()
		source := plan.Content[spec.LogicalPath]
		if err := writeInput(ctx, "workspace.directory.prepare", root, spec, source); err != nil {
			if domain.ErrorCodeOf(err) != domain.CodeInternal {
				return err
			}
			return domain.NewError(domain.CodeIntegrityViolation, "workspace.directory.prepare", fmt.Sprintf("content[%q]", spec.LogicalPath), "content could not be safely materialized", err)
		}
	}
	return nil
}

func writeInput(ctx context.Context, operation, root string, entry domain.InputViewEntrySpec, source ports.ContentSource) error {
	err := safepath.WriteRegularAtomic(root, entry.LogicalPath, fs.FileMode(entry.Mode), func(destination io.Writer) error {
		reader, err := source.Open(ctx)
		if err != nil {
			if contextErr := contextError(ctx, operation); contextErr != nil {
				return contextErr
			}
			return domain.NewError(domain.CodeUnavailable, operation, "content", "immutable content source could not be opened", err)
		}
		if reader == nil {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "content", "immutable content source returned a nil reader", nil)
		}
		hash := sha256.New()
		written, copyErr := safepath.CopyBounded(io.MultiWriter(destination, hash), &contextReader{ctx: ctx, reader: reader}, entry.Size)
		closeErr := reader.Close()
		if contextErr := contextError(ctx, operation); contextErr != nil {
			return errors.Join(contextErr, copyErr, closeErr)
		}
		if copyErr != nil {
			code := domain.CodeUnavailable
			if errors.Is(copyErr, safepath.ErrTooLarge) {
				code = domain.CodeIntegrityViolation
			}
			return domain.NewError(code, operation, "content", "immutable content stream could not be copied exactly", errors.Join(copyErr, closeErr))
		}
		if closeErr != nil {
			return domain.NewError(domain.CodeUnavailable, operation, "content", "immutable content stream could not be closed", closeErr)
		}
		actual, parseErr := domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
		if parseErr != nil {
			return parseErr
		}
		if written != entry.Size || actual != entry.Digest {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "content", "streamed bytes do not match the input-view digest and size", nil)
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if domain.ErrorCodeOf(err) != domain.CodeInternal {
		return err
	}
	if errors.Is(err, safepath.ErrUnsafe) || errors.Is(err, safepath.ErrNotRegular) {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "logical_path", "destination path is not a safe regular file", err)
	}
	return domain.NewError(domain.CodeUnavailable, operation, "workspace", "could not atomically publish input content", err)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}
