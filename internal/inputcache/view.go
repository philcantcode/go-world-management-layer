package inputcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

func CanonicalizeManifest(manifest InputViewManifest) (InputViewManifest, error) {
	if manifest.SchemaVersion == 0 {
		manifest.SchemaVersion = 1
	}
	if manifest.SchemaVersion != 1 || strings.TrimSpace(manifest.Selection) == "" || strings.TrimSpace(manifest.Layout) == "" {
		return InputViewManifest{}, fmt.Errorf("schema version 1, selection, and layout are required")
	}
	canonical := InputViewManifest{SchemaVersion: 1, Selection: manifest.Selection, Layout: manifest.Layout, Entries: make([]InputEntry, len(manifest.Entries))}
	copy(canonical.Entries, manifest.Entries)
	for index := range canonical.Entries {
		entry := &canonical.Entries[index]
		normalized, err := safepath.Normalize(entry.Path)
		if err != nil {
			return InputViewManifest{}, fmt.Errorf("entry %d path: %w", index, err)
		}
		entry.Path = normalized
		if _, err := validateObject(Object{Occurrence: entry.Occurrence, Digest: entry.Digest, Size: entry.Size}); err != nil {
			return InputViewManifest{}, fmt.Errorf("entry %q: %w", entry.Path, err)
		}
		if entry.Mode == 0 {
			entry.Mode = 0o444
		}
		if entry.Mode > 0o777 {
			return InputViewManifest{}, fmt.Errorf("entry %q mode must contain only regular Unix permission bits", entry.Path)
		}
		entry.Digest = "sha256:" + strings.ToLower(strings.TrimPrefix(entry.Digest, "sha256:"))
		entry.Sidecars = append([]string(nil), entry.Sidecars...)
		seenSidecars := make(map[string]struct{}, len(entry.Sidecars))
		for sidecarIndex, sidecar := range entry.Sidecars {
			if strings.TrimSpace(sidecar) == "" {
				return InputViewManifest{}, fmt.Errorf("entry %q sidecar %d must not be blank", entry.Path, sidecarIndex)
			}
			if _, duplicate := seenSidecars[sidecar]; duplicate {
				return InputViewManifest{}, fmt.Errorf("entry %q contains duplicate sidecar %q", entry.Path, sidecar)
			}
			seenSidecars[sidecar] = struct{}{}
		}
		sort.Strings(entry.Sidecars)
	}
	sort.Slice(canonical.Entries, func(i, j int) bool { return canonical.Entries[i].Path < canonical.Entries[j].Path })
	seenPaths := make(map[string]struct{}, len(canonical.Entries))
	for _, entry := range canonical.Entries {
		if _, duplicate := seenPaths[entry.Path]; duplicate {
			return InputViewManifest{}, fmt.Errorf("%w: %q", ErrViewConflict, entry.Path)
		}
		for parent := path.Dir(entry.Path); parent != "."; parent = path.Dir(parent) {
			if _, conflict := seenPaths[parent]; conflict {
				return InputViewManifest{}, fmt.Errorf("%w: file %q is an ancestor of %q", ErrViewConflict, parent, entry.Path)
			}
		}
		seenPaths[entry.Path] = struct{}{}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return InputViewManifest{}, err
	}
	digest := sha256.Sum256(append([]byte("world-input-view-v1\x00"), encoded...))
	canonical.ViewID = "input_view_sha256_" + hex.EncodeToString(digest[:])
	return canonical, nil
}

// BuildView constructs or reuses the exact read-only logical tree described by
// manifest. It never hardlinks content because view entries require independent
// inode and metadata semantics.
func (c *Cache) BuildView(ctx context.Context, manifest InputViewManifest) (string, InputViewManifest, error) {
	c.operations.RLock()
	defer c.operations.RUnlock()
	canonical, err := CanonicalizeManifest(manifest)
	if err != nil {
		return "", InputViewManifest{}, err
	}
	built, err := c.runPathBuild(ctx, c.viewBuilds, canonical.ViewID, func() (string, error) {
		return c.buildView(ctx, canonical)
	})
	return built, canonical, err
}

func (c *Cache) buildView(ctx context.Context, canonical InputViewManifest) (string, error) {
	destination := filepath.Join(c.scopeRoot, "views", canonical.ViewID)
	if c.validView(destination, canonical.ViewID) {
		_ = os.Chtimes(destination, c.clock(), c.clock())
		return filepath.Join(destination, "root"), nil
	}
	if err := removeCacheTree(destination); err != nil {
		return "", fmt.Errorf("remove invalid cached view: %w", err)
	}
	staging, err := os.MkdirTemp(filepath.Join(c.scopeRoot, "staging"), "view-*")
	if err != nil {
		return "", err
	}
	defer removeCacheTree(staging) // best-effort cleanup; a returned build error remains authoritative
	viewRoot := filepath.Join(staging, "root")
	if err := os.MkdirAll(viewRoot, 0o500); err != nil {
		return "", err
	}
	for _, entry := range canonical.Entries {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		contentPath, err := c.ensureContent(ctx, Object{Occurrence: entry.Occurrence, Digest: entry.Digest, Size: entry.Size})
		if err != nil {
			return "", err
		}
		viewPath := filepath.Join(viewRoot, filepath.FromSlash(entry.Path))
		if err := os.MkdirAll(filepath.Dir(viewPath), 0o500); err != nil {
			return "", err
		}
		cloned, err := cloneFile(contentPath, viewPath, os.FileMode(entry.Mode)&0o777)
		if err != nil {
			return "", err
		}
		if !cloned {
			if c.construction == RequireReflink {
				return "", ErrReflinkUnavailable
			}
			if err := copyIndependent(contentPath, viewPath, os.FileMode(entry.Mode)&0o777); err != nil {
				return "", err
			}
		}
	}
	if err := atomicfile.WriteJSON(filepath.Join(staging, "input-view-manifest.json"), canonical, 0o400); err != nil {
		return "", err
	}
	if err := os.Rename(staging, destination); err != nil {
		if c.validView(destination, canonical.ViewID) {
			return filepath.Join(destination, "root"), nil
		}
		return "", err
	}
	if err := os.Chmod(filepath.Join(destination, "root"), 0o500); err != nil {
		return "", err
	}
	return filepath.Join(destination, "root"), nil
}

func (c *Cache) validView(directory, expectedID string) bool {
	encoded, err := os.ReadFile(filepath.Join(directory, "input-view-manifest.json"))
	if err != nil {
		return false
	}
	var manifest InputViewManifest
	if json.Unmarshal(encoded, &manifest) != nil {
		return false
	}
	canonical, err := CanonicalizeManifest(manifest)
	if err != nil || manifest.ViewID != expectedID || canonical.ViewID != expectedID || !reflect.DeepEqual(manifest, canonical) {
		return false
	}
	root := filepath.Join(directory, "root")
	for _, entry := range canonical.Entries {
		opened, err := safepath.OpenRegular(root, entry.Path)
		if err != nil {
			return false
		}
		valid := opened.Size() == entry.Size && viewModeMatches(opened.Mode(), entry.Mode)
		if valid && c.verifyCacheHits {
			digest, size, hashErr := hashReader(opened, c.maxContentBytes)
			valid = hashErr == nil && size == entry.Size && digest == entry.Digest
		}
		_ = opened.Close()
		if !valid {
			return false
		}
	}
	return true
}

func removeCacheTree(root string) error {
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o700)
		}
		if entry.Type().IsRegular() {
			return os.Chmod(path, 0o600)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return os.RemoveAll(root)
}

func copyIndependent(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = os.Remove(destination)
		}
	}()
	if _, err := io.Copy(output, input); err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	if err := output.Chmod(mode); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	keep = true
	return nil
}

type pinsFile struct {
	Views map[string][]string `json:"views"`
}

func (c *Cache) pinsPath() string { return filepath.Join(c.scopeRoot, "pins.json") }

func (c *Cache) loadPins() error {
	encoded, err := os.ReadFile(c.pinsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var stored pinsFile
	if err := json.Unmarshal(encoded, &stored); err != nil {
		return fmt.Errorf("decode pins: %w", err)
	}
	for viewID, owners := range stored.Views {
		set := make(map[string]struct{}, len(owners))
		for _, owner := range owners {
			if owner != "" {
				set[owner] = struct{}{}
			}
		}
		if len(set) > 0 {
			c.pins[viewID] = set
		}
	}
	return nil
}

func (c *Cache) savePinsLocked() error {
	stored := pinsFile{Views: make(map[string][]string, len(c.pins))}
	for viewID, owners := range c.pins {
		values := make([]string, 0, len(owners))
		for owner := range owners {
			values = append(values, owner)
		}
		sort.Strings(values)
		stored.Views[viewID] = values
	}
	return atomicfile.WriteJSON(c.pinsPath(), stored, 0o600)
}

func (c *Cache) Pin(viewID, owner string) error {
	c.operations.RLock()
	defer c.operations.RUnlock()
	if owner == "" || !c.validView(filepath.Join(c.scopeRoot, "views", viewID), viewID) {
		return ErrUnknownView
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pins[viewID] == nil {
		c.pins[viewID] = make(map[string]struct{})
	}
	c.pins[viewID][owner] = struct{}{}
	return c.savePinsLocked()
}

func (c *Cache) Release(viewID, owner string) error {
	c.operations.RLock()
	defer c.operations.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	owners := c.pins[viewID]
	delete(owners, owner)
	if len(owners) == 0 {
		delete(c.pins, viewID)
	}
	return c.savePinsLocked()
}
