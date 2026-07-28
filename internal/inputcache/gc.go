package inputcache

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Stats struct {
	LogicalBytes   int64 `json:"logical_bytes"`
	PhysicalBytes  int64 `json:"physical_bytes"`
	ContentEntries int   `json:"content_entries"`
	Views          int   `json:"views"`
	PinnedViews    int   `json:"pinned_views"`
}

type GCResult struct {
	Before         Stats    `json:"before"`
	After          Stats    `json:"after"`
	RemovedViews   []string `json:"removed_views"`
	RemovedContent []string `json:"removed_content"`
}

type cacheCandidate struct {
	name     string
	path     string
	modified time.Time
	size     int64
}

func (c *Cache) Stats() (Stats, error) {
	c.operations.RLock()
	defer c.operations.RUnlock()
	return c.stats()
}

func (c *Cache) stats() (Stats, error) {
	var result Stats
	err := filepath.WalkDir(c.scopeRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		result.LogicalBytes += info.Size()
		result.PhysicalBytes += allocatedBytes(info)
		if strings.Contains(path, string(filepath.Separator)+"content"+string(filepath.Separator)+"sha256"+string(filepath.Separator)) {
			result.ContentEntries++
		}
		return nil
	})
	if err != nil {
		return Stats{}, err
	}
	viewEntries, err := os.ReadDir(filepath.Join(c.scopeRoot, "views"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Stats{}, err
	}
	for _, entry := range viewEntries {
		if entry.IsDir() {
			result.Views++
		}
	}
	c.mu.Lock()
	result.PinnedViews = len(c.pins)
	c.mu.Unlock()
	return result, nil
}

// Collect enforces retention and high/low watermarks. It first removes expired,
// unpinned view trees, then removes content no remaining view references. Pins
// and in-flight operations are protected by the operations lock.
func (c *Cache) Collect(ctx context.Context) (GCResult, error) {
	c.operations.Lock()
	defer c.operations.Unlock()
	before, err := c.stats()
	if err != nil {
		return GCResult{}, err
	}
	result := GCResult{Before: before}
	high := c.maxCacheBytes * int64(c.highWater) / 100
	low := c.maxCacheBytes * int64(c.lowWater) / 100
	now := c.clock()

	c.mu.Lock()
	pinned := make(map[string]bool, len(c.pins))
	for viewID, owners := range c.pins {
		pinned[viewID] = len(owners) > 0
	}
	c.mu.Unlock()

	views, err := candidates(filepath.Join(c.scopeRoot, "views"))
	if err != nil {
		return GCResult{}, err
	}
	current := before.LogicalBytes
	for _, view := range views {
		if err := ctx.Err(); err != nil {
			return GCResult{}, err
		}
		if pinned[view.name] {
			continue
		}
		expired := now.Sub(view.modified) >= c.viewRetention
		if !expired && current <= high {
			continue
		}
		// Views are sealed read-only; chmod the tree before removal.
		if err := removeCacheTree(view.path); err != nil {
			return GCResult{}, err
		}
		result.RemovedViews = append(result.RemovedViews, view.name)
		current -= view.size
		if current <= low && !expired {
			break
		}
	}

	referenced, err := c.referencedDigests()
	if err != nil {
		return GCResult{}, err
	}
	content, err := contentCandidates(filepath.Join(c.scopeRoot, "content", "sha256"))
	if err != nil {
		return GCResult{}, err
	}
	for _, object := range content {
		if err := ctx.Err(); err != nil {
			return GCResult{}, err
		}
		if referenced[object.name] || (current <= high && now.Sub(object.modified) < c.viewRetention) {
			continue
		}
		if err := os.Remove(object.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return GCResult{}, err
		}
		result.RemovedContent = append(result.RemovedContent, object.name)
		current -= object.size
		if current <= low {
			break
		}
	}
	after, err := c.stats()
	if err != nil {
		return GCResult{}, err
	}
	result.After = after
	return result, nil
}

func candidates(root string) ([]cacheCandidate, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]cacheCandidate, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		size, err := treeSize(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, err
		}
		result = append(result, cacheCandidate{name: entry.Name(), path: filepath.Join(root, entry.Name()), modified: info.ModTime(), size: size})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].modified.Before(result[j].modified) })
	return result, nil
}

func contentCandidates(root string) ([]cacheCandidate, error) {
	result := make([]cacheCandidate, 0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		result = append(result, cacheCandidate{name: entry.Name(), path: path, modified: info.ModTime(), size: info.Size()})
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].modified.Before(result[j].modified) })
	return result, err
}

func treeSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func (c *Cache) referencedDigests() (map[string]bool, error) {
	referenced := make(map[string]bool)
	views, err := os.ReadDir(filepath.Join(c.scopeRoot, "views"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	for _, view := range views {
		if !view.IsDir() {
			continue
		}
		encoded, err := os.ReadFile(filepath.Join(c.scopeRoot, "views", view.Name(), "input-view-manifest.json"))
		if err != nil {
			return nil, err
		}
		var manifest InputViewManifest
		if err := json.Unmarshal(encoded, &manifest); err != nil {
			return nil, err
		}
		for _, entry := range manifest.Entries {
			referenced[strings.TrimPrefix(entry.Digest, "sha256:")] = true
		}
	}
	return referenced, nil
}
