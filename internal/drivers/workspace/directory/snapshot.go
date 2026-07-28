package directory

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

// ensureSnapshot publishes a private immutable-content view beside the merged
// directory. Only merged is bind-mounted into the agent container, so the
// snapshot is outside the untrusted writer's namespace. Every copied file is
// checked against the authoritative seal manifest before publication.
func (d *Driver) ensureSnapshot(ctx context.Context, workspaceID domain.WorkspaceID, record diskRecord, manifest workspacepkg.Manifest) (string, error) {
	const operation = "workspace.directory.seal"
	workspaceRoot := d.workspacePath(workspaceID)
	snapshotRoot := filepath.Join(workspaceRoot, sealedDirectory)
	if _, err := os.Lstat(snapshotRoot); err == nil {
		if err := verifySnapshot(ctx, snapshotRoot, record, manifest); err != nil {
			return "", domain.NewError(domain.CodeIntegrityViolation, operation, "snapshot", "persisted sealed snapshot is invalid", err)
		}
		if err := syncDirectory(workspaceRoot); err != nil {
			return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "sealed snapshot could not be synchronized", err)
		}
		return snapshotRoot, nil
	} else if !os.IsNotExist(err) {
		return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "could not inspect the sealed snapshot", err)
	}

	staging, err := os.MkdirTemp(workspaceRoot, ".sealed-")
	if err != nil {
		return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "could not create sealed snapshot staging", err)
	}
	defer cleanupManagedDirectory(workspaceRoot, staging)
	if err := os.Chmod(staging, 0o700); err != nil {
		return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "could not restrict sealed snapshot staging", err)
	}
	mergedRoot := filepath.Join(workspaceRoot, mergedDirectory)
	for index, entry := range manifest.Entries {
		digest, err := domain.ParseDigest(entry.Digest)
		if err != nil {
			return "", domain.NewError(domain.CodeIntegrityViolation, operation, fmt.Sprintf("manifest.entries[%d].digest", index), "is invalid", err)
		}
		source := &snapshotSource{root: mergedRoot, relativePath: entry.Path, digest: digest, size: entry.Size}
		input := domain.InputViewEntrySpec{LogicalPath: entry.Path, Digest: digest, Size: entry.Size, Mode: entry.Mode}
		if err := writeInput(ctx, operation, staging, input, source); err != nil {
			return "", err
		}
	}
	if err := verifySnapshot(ctx, staging, record, manifest); err != nil {
		return "", domain.NewError(domain.CodeIntegrityViolation, operation, "snapshot", "staged sealed snapshot does not match the authoritative manifest", err)
	}
	if err := publishDirectory(ctx, staging, snapshotRoot); err != nil {
		if _, statErr := os.Lstat(snapshotRoot); statErr == nil {
			if verifyErr := verifySnapshot(ctx, snapshotRoot, record, manifest); verifyErr == nil {
				return snapshotRoot, nil
			}
		}
		return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "could not publish the sealed snapshot", err)
	}
	if err := syncDirectory(workspaceRoot); err != nil {
		return "", domain.NewError(domain.CodeUnavailable, operation, "snapshot", "sealed snapshot was published but could not be synchronized", err)
	}
	return snapshotRoot, nil
}

func verifySnapshot(ctx context.Context, root string, record diskRecord, expected workspacepkg.Manifest) error {
	if err := requireManagedDirectory(filepath.Dir(root), root); err != nil {
		return err
	}
	actual, usage, err := scanManaged(ctx, root, record.UpperByteLimit, record.UpperInodeLimit, expected.SealedAt)
	if err != nil {
		return err
	}
	if len(actual.Entries) != len(expected.Entries) {
		return fmt.Errorf("file count is %d, want %d", len(actual.Entries), len(expected.Entries))
	}
	expectedEntries := make(map[string]workspacepkg.Entry, len(expected.Entries))
	expectedDirectories := make(map[string]struct{})
	var expectedBytes int64
	for _, entry := range expected.Entries {
		expectedEntries[entry.Path] = entry
		expectedBytes += entry.Size
		for parent := path.Dir(entry.Path); parent != "."; parent = path.Dir(parent) {
			expectedDirectories[parent] = struct{}{}
		}
	}
	if usage.bytes != expectedBytes || usage.inodes != int64(len(expected.Entries)+len(expectedDirectories)) {
		return fmt.Errorf("snapshot allocation does not exactly match the sealed manifest")
	}
	for _, entry := range actual.Entries {
		want, found := expectedEntries[entry.Path]
		if !found || entry.Digest != want.Digest || entry.Size != want.Size || !projectedModeMatches(want.Mode, entry.Mode) {
			return fmt.Errorf("snapshot entry %q does not match the sealed manifest", entry.Path)
		}
	}
	return nil
}

type snapshotSource struct {
	root         string
	relativePath string
	digest       domain.Digest
	size         int64
}

func (s *snapshotSource) Digest() domain.Digest { return s.digest }
func (s *snapshotSource) Size() int64           { return s.size }
func (s *snapshotSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return safepath.OpenRegular(s.root, s.relativePath)
}

var _ ports.ContentSource = (*snapshotSource)(nil)
