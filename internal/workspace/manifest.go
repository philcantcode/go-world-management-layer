// Package workspace seals authoritative workspace manifests, computes changes,
// and exports explicitly selected files through the descriptor-safe path
// boundary. It is deliberately independent of OverlayFS and artifact vendors.
package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

var (
	ErrConcurrentMutation = errors.New("workspace changed while it was being sealed")
	ErrManifestConflict   = errors.New("manifest contains conflicting paths")
	ErrQuotaExceeded      = errors.New("workspace quota exceeded")
	ErrCaptureMismatch    = errors.New("capture sink returned inconsistent bytes")
)

type Entry struct {
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
	Mode       uint32 `json:"mode"`
	ModifiedNS int64  `json:"modified_ns,omitempty"`
}

type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	SealedAt      time.Time `json:"sealed_at"`
	Entries       []Entry   `json:"entries"`
	Digest        string    `json:"digest"`
}

type ScanLimits struct {
	MaxFiles int
	MaxBytes int64
}

func (l ScanLimits) validate() error {
	if l.MaxFiles <= 0 || l.MaxBytes < 0 {
		return fmt.Errorf("%w: max files must be positive and max bytes non-negative", ErrQuotaExceeded)
	}
	return nil
}

// Scan seals all regular files below root. Symlinks and special files are
// rejected because untyped exports cannot faithfully or safely represent them.
func Scan(root string, limits ScanLimits, now time.Time) (Manifest, error) {
	if err := limits.validate(); err != nil {
		return Manifest{}, err
	}
	entries := make([]Entry, 0)
	var total int64
	err := filepath.WalkDir(root, func(hostPath string, item fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if hostPath == root {
			return nil
		}
		relative, err := filepath.Rel(root, hostPath)
		if err != nil {
			return err
		}
		logical := filepath.ToSlash(relative)
		if item.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink %q", safepath.ErrUnsafe, logical)
		}
		if item.IsDir() {
			return nil
		}
		if !item.Type().IsRegular() {
			return fmt.Errorf("%w: special file %q", safepath.ErrNotRegular, logical)
		}
		if len(entries) >= limits.MaxFiles {
			return fmt.Errorf("%w: more than %d files", ErrQuotaExceeded, limits.MaxFiles)
		}
		entry, err := hashEntry(root, logical)
		if err != nil {
			return err
		}
		if entry.Size > limits.MaxBytes-total {
			return fmt.Errorf("%w: more than %d bytes", ErrQuotaExceeded, limits.MaxBytes)
		}
		total += entry.Size
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return Manifest{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	manifest := Manifest{SchemaVersion: 1, SealedAt: now.UTC(), Entries: entries}
	digest, err := manifestDigest(manifest)
	if err != nil {
		return Manifest{}, err
	}
	manifest.Digest = digest
	return manifest, nil
}

func hashEntry(root, logical string) (Entry, error) {
	opened, err := safepath.OpenRegular(root, logical)
	if err != nil {
		return Entry{}, err
	}
	defer opened.Close()
	before := opened.Info()
	hash := sha256.New()
	n, err := io.Copy(hash, opened)
	if err != nil {
		return Entry{}, fmt.Errorf("hash %q: %w", logical, err)
	}
	after, err := opened.Stat()
	if err != nil {
		return Entry{}, fmt.Errorf("restat %q: %w", logical, err)
	}
	if n != before.Size() || before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
		return Entry{}, fmt.Errorf("%w: %q", ErrConcurrentMutation, logical)
	}
	return Entry{
		Path:       logical,
		Digest:     "sha256:" + hex.EncodeToString(hash.Sum(nil)),
		Size:       n,
		Mode:       uint32(after.Mode().Perm()),
		ModifiedNS: after.ModTime().UnixNano(),
	}, nil
}

func manifestDigest(manifest Manifest) (string, error) {
	manifest.Digest = ""
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("world-workspace-manifest-v1\x00"), encoded...))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ValidateManifest rejects unsorted, duplicate, unsafe, or digest-inconsistent
// manifests before they become lower-layer or change-set authority.
func ValidateManifest(manifest Manifest) error {
	if manifest.SchemaVersion != 1 {
		return fmt.Errorf("unsupported manifest schema %d", manifest.SchemaVersion)
	}
	previous := ""
	for index, entry := range manifest.Entries {
		normalized, err := safepath.Normalize(entry.Path)
		if err != nil || normalized != entry.Path {
			return fmt.Errorf("entry %d path: %w", index, err)
		}
		if entry.Path <= previous {
			return fmt.Errorf("%w at %q", ErrManifestConflict, entry.Path)
		}
		if entry.Size < 0 || !strings.HasPrefix(entry.Digest, "sha256:") || len(entry.Digest) != len("sha256:")+sha256.Size*2 {
			return fmt.Errorf("entry %q has invalid size or digest", entry.Path)
		}
		previous = entry.Path
	}
	want, err := manifestDigest(manifest)
	if err != nil {
		return err
	}
	if manifest.Digest != want {
		return fmt.Errorf("manifest digest %q does not match %q", manifest.Digest, want)
	}
	return nil
}

type ChangeKind string

const (
	ChangeAdded    ChangeKind = "added"
	ChangeModified ChangeKind = "modified"
	ChangeDeleted  ChangeKind = "deleted"
	ChangeRenamed  ChangeKind = "renamed"
	ChangeMetadata ChangeKind = "metadata"
	ChangeOpaque   ChangeKind = "opaque"
)

type Change struct {
	Kind         ChangeKind `json:"kind"`
	Path         string     `json:"path"`
	PreviousPath string     `json:"previous_path,omitempty"`
	Before       *Entry     `json:"before,omitempty"`
	After        *Entry     `json:"after,omitempty"`
}

// Diff compares two sealed manifests and detects unambiguous renames by exact
// content and metadata identity. Ambiguous equal-content additions/deletions
// remain separate so the change record does not invent provenance.
func Diff(before, after Manifest) ([]Change, error) {
	if err := ValidateManifest(before); err != nil {
		return nil, fmt.Errorf("before manifest: %w", err)
	}
	if err := ValidateManifest(after); err != nil {
		return nil, fmt.Errorf("after manifest: %w", err)
	}
	oldEntries := indexEntries(before.Entries)
	newEntries := indexEntries(after.Entries)
	changes := make([]Change, 0)
	deleted := make([]Entry, 0)
	added := make([]Entry, 0)
	for path, oldEntry := range oldEntries {
		newEntry, exists := newEntries[path]
		if !exists {
			deleted = append(deleted, oldEntry)
			continue
		}
		oldCopy, newCopy := oldEntry, newEntry
		switch {
		case oldEntry.Digest != newEntry.Digest || oldEntry.Size != newEntry.Size:
			changes = append(changes, Change{Kind: ChangeModified, Path: path, Before: &oldCopy, After: &newCopy})
		case oldEntry.Mode != newEntry.Mode || oldEntry.ModifiedNS != newEntry.ModifiedNS:
			changes = append(changes, Change{Kind: ChangeMetadata, Path: path, Before: &oldCopy, After: &newCopy})
		}
	}
	for path, entry := range newEntries {
		if _, exists := oldEntries[path]; !exists {
			added = append(added, entry)
		}
	}

	usedAdded := make(map[string]bool)
	usedDeleted := make(map[string]bool)
	for _, oldEntry := range deleted {
		matches := make([]Entry, 0, 1)
		for _, newEntry := range added {
			if !usedAdded[newEntry.Path] && sameIdentity(oldEntry, newEntry) {
				matches = append(matches, newEntry)
			}
		}
		if len(matches) == 1 && countIdentity(deleted, oldEntry) == 1 && countIdentity(added, matches[0]) == 1 {
			oldCopy, newCopy := oldEntry, matches[0]
			changes = append(changes, Change{Kind: ChangeRenamed, Path: newCopy.Path, PreviousPath: oldCopy.Path, Before: &oldCopy, After: &newCopy})
			usedAdded[newCopy.Path] = true
			usedDeleted[oldCopy.Path] = true
		}
	}
	for _, entry := range deleted {
		if !usedDeleted[entry.Path] {
			copy := entry
			changes = append(changes, Change{Kind: ChangeDeleted, Path: entry.Path, Before: &copy})
		}
	}
	for _, entry := range added {
		if !usedAdded[entry.Path] {
			copy := entry
			changes = append(changes, Change{Kind: ChangeAdded, Path: entry.Path, After: &copy})
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Path == changes[j].Path {
			return changes[i].Kind < changes[j].Kind
		}
		return changes[i].Path < changes[j].Path
	})
	return changes, nil
}

func indexEntries(entries []Entry) map[string]Entry {
	indexed := make(map[string]Entry, len(entries))
	for _, entry := range entries {
		indexed[entry.Path] = entry
	}
	return indexed
}

func sameIdentity(a, b Entry) bool {
	return a.Digest == b.Digest && a.Size == b.Size && a.Mode == b.Mode && a.ModifiedNS == b.ModifiedNS
}

func countIdentity(entries []Entry, want Entry) int {
	count := 0
	for _, entry := range entries {
		if sameIdentity(entry, want) {
			count++
		}
	}
	return count
}

type ExportSelection struct {
	Path string `json:"path"`
	Role string `json:"role"`
}

type CaptureRequest struct {
	LogicalPath string
	Role        string
	Mode        fs.FileMode
	Size        int64
}

type CaptureResult struct {
	Reference string
	Digest    string
	Size      int64
}

type CaptureSink interface {
	Capture(context.Context, CaptureRequest, io.Reader) (CaptureResult, error)
}

// Export validates all declarations before performing side effects, then opens
// each selected file beneath root and streams it once to the artifact boundary.
func Export(ctx context.Context, root string, selections []ExportSelection, limits ScanLimits, sink CaptureSink) ([]CaptureResult, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if len(selections) == 0 || len(selections) > limits.MaxFiles {
		return nil, ErrQuotaExceeded
	}
	seen := make(map[string]struct{}, len(selections))
	normalized := make([]ExportSelection, len(selections))
	for index, selection := range selections {
		path, err := safepath.Normalize(selection.Path)
		if err != nil {
			return nil, fmt.Errorf("selection %d: %w", index, err)
		}
		if strings.TrimSpace(selection.Role) == "" {
			return nil, fmt.Errorf("selection %d has empty role", index)
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, fmt.Errorf("%w: duplicate export %q", ErrManifestConflict, path)
		}
		seen[path] = struct{}{}
		normalized[index] = ExportSelection{Path: path, Role: selection.Role}
	}

	results := make([]CaptureResult, 0, len(normalized))
	var total int64
	for _, selection := range normalized {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		opened, err := safepath.OpenRegular(root, selection.Path)
		if err != nil {
			return nil, err
		}
		if opened.Size() > limits.MaxBytes-total {
			_ = opened.Close()
			return nil, ErrQuotaExceeded
		}
		hasher := sha256.New()
		limited := &io.LimitedReader{R: opened, N: opened.Size() + 1}
		result, captureErr := sink.Capture(ctx, CaptureRequest{LogicalPath: selection.Path, Role: selection.Role, Mode: opened.Mode(), Size: opened.Size()}, io.TeeReader(limited, hasher))
		closeErr := opened.Close()
		if captureErr != nil {
			return nil, captureErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		consumed := opened.Size() + 1 - limited.N
		if consumed != opened.Size() || result.Size != consumed || result.Digest != digest {
			return nil, fmt.Errorf("%w for %q", ErrCaptureMismatch, selection.Path)
		}
		total += consumed
		results = append(results, result)
	}
	return results, nil
}
