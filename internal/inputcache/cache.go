// Package inputcache implements the scoped, verified content and exact-view
// cache. It is an expendable acceleration layer: all bytes enter through a
// ContentSource, every publication is verified, and callers pin views while a
// generation or target materialization can still reference them.
package inputcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

var (
	ErrInvalidDigest      = errors.New("invalid sha256 digest")
	ErrDigestMismatch     = errors.New("content digest mismatch")
	ErrSizeMismatch       = errors.New("content size mismatch")
	ErrViewConflict       = errors.New("input view has conflicting paths")
	ErrReflinkUnavailable = errors.New("reflink construction is required but unavailable")
	ErrUnknownView        = errors.New("unknown input view")
)

type Construction string

const (
	RequireReflink Construction = "require-reflink"
	AllowCopy      Construction = "allow-copy"
)

type Object struct {
	Occurrence string `json:"occurrence"`
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
}

type ContentSource interface {
	OpenContent(context.Context, Object) (io.ReadCloser, error)
}

type InputEntry struct {
	Path       string   `json:"path"`
	Occurrence string   `json:"occurrence"`
	Digest     string   `json:"digest"`
	Size       int64    `json:"size"`
	Mode       uint32   `json:"mode"`
	Sidecars   []string `json:"sidecars,omitempty"`
}

type InputViewManifest struct {
	SchemaVersion int          `json:"schema_version"`
	Selection     string       `json:"selection"`
	Layout        string       `json:"layout"`
	Entries       []InputEntry `json:"entries"`
	ViewID        string       `json:"view_id"`
}

type Options struct {
	Root             string
	SecurityScope    string
	Construction     Construction
	VerifyCacheHits  bool
	MaxContentBytes  int64
	MaxCacheBytes    int64
	ViewRetention    time.Duration
	HighWaterPercent int
	LowWaterPercent  int
	Clock            func() time.Time
}

type Cache struct {
	root            string
	scopeRoot       string
	construction    Construction
	verifyCacheHits bool
	maxContentBytes int64
	maxCacheBytes   int64
	viewRetention   time.Duration
	highWater       int
	lowWater        int
	source          ContentSource
	clock           func() time.Time

	operations sync.RWMutex
	mu         sync.Mutex
	builds     map[string]*pathBuild
	viewBuilds map[string]*pathBuild
	pins       map[string]map[string]struct{}
}

type pathBuild struct {
	done chan struct{}
	path string
	err  error
}

func New(options Options, source ContentSource) (*Cache, error) {
	if strings.TrimSpace(options.Root) == "" || strings.TrimSpace(options.SecurityScope) == "" {
		return nil, fmt.Errorf("cache root and security scope are required")
	}
	if source == nil {
		return nil, fmt.Errorf("content source is required")
	}
	if options.Construction == "" {
		options.Construction = RequireReflink
	}
	if options.Construction != RequireReflink && options.Construction != AllowCopy {
		return nil, fmt.Errorf("unknown construction %q", options.Construction)
	}
	if options.MaxContentBytes <= 0 {
		options.MaxContentBytes = 1 << 40
	}
	if options.MaxCacheBytes <= 0 {
		options.MaxCacheBytes = 500 << 30
	}
	if options.ViewRetention <= 0 {
		options.ViewRetention = 24 * time.Hour
	}
	if options.HighWaterPercent == 0 {
		options.HighWaterPercent = 85
	}
	if options.LowWaterPercent == 0 {
		options.LowWaterPercent = 70
	}
	if options.LowWaterPercent < 0 || options.HighWaterPercent > 100 || options.LowWaterPercent >= options.HighWaterPercent {
		return nil, fmt.Errorf("cache watermarks must satisfy 0 <= low < high <= 100")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	scopeHash := sha256.Sum256(append([]byte("world-cache-scope-v1\x00"), []byte(options.SecurityScope)...))
	scopeRoot := filepath.Join(options.Root, "cache", hex.EncodeToString(scopeHash[:]))
	for _, directory := range []string{filepath.Join(scopeRoot, "content", "sha256"), filepath.Join(scopeRoot, "views"), filepath.Join(scopeRoot, "staging")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create cache directory: %w", err)
		}
	}
	cache := &Cache{
		root: options.Root, scopeRoot: scopeRoot, construction: options.Construction,
		verifyCacheHits: options.VerifyCacheHits, maxContentBytes: options.MaxContentBytes,
		maxCacheBytes: options.MaxCacheBytes, viewRetention: options.ViewRetention,
		highWater: options.HighWaterPercent, lowWater: options.LowWaterPercent,
		source: source, clock: options.Clock, builds: make(map[string]*pathBuild),
		viewBuilds: make(map[string]*pathBuild), pins: make(map[string]map[string]struct{}),
	}
	if err := cache.loadPins(); err != nil {
		return nil, err
	}
	return cache, nil
}

func validateObject(object Object) (string, error) {
	if object.Size < 0 || object.Occurrence == "" {
		return "", fmt.Errorf("occurrence and non-negative size are required")
	}
	raw := strings.TrimPrefix(object.Digest, "sha256:")
	if len(raw) != sha256.Size*2 {
		return "", ErrInvalidDigest
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != sha256.Size {
		return "", ErrInvalidDigest
	}
	return strings.ToLower(raw), nil
}

func (c *Cache) contentPath(rawDigest string) string {
	return filepath.Join(c.scopeRoot, "content", "sha256", rawDigest[:2], rawDigest)
}

// EnsureContent joins concurrent misses for the same scoped digest and returns
// only after a verified immutable publication exists.
func (c *Cache) EnsureContent(ctx context.Context, object Object) (string, error) {
	c.operations.RLock()
	defer c.operations.RUnlock()
	return c.ensureContent(ctx, object)
}

func (c *Cache) ensureContent(ctx context.Context, object Object) (string, error) {
	rawDigest, err := validateObject(object)
	if err != nil {
		return "", err
	}
	path, err := c.runPathBuild(ctx, c.builds, rawDigest, func() (string, error) {
		destination := c.contentPath(rawDigest)
		if exists, verifyErr := c.validHit(destination, object); exists {
			return destination, verifyErr
		}
		return c.populate(ctx, object, rawDigest, destination)
	})
	if err != nil {
		return "", err
	}
	// Concurrent callers join by digest because that is the physical object
	// identity. Revalidate the caller's declared size after the shared build so
	// a conflicting declaration cannot inherit another caller's success.
	if exists, verifyErr := c.validHit(path, object); exists {
		return path, verifyErr
	}
	return "", fmt.Errorf("published cache object %s is unavailable", object.Digest)
}

// runPathBuild makes construction of a deterministic cache path single-flight.
// A waiting caller may abandon its wait without cancelling the caller that owns
// the build; a later call retries after a failed build.
func (c *Cache) runPathBuild(ctx context.Context, builds map[string]*pathBuild, key string, buildPath func() (string, error)) (string, error) {
	c.mu.Lock()
	if existing, ok := builds[key]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-existing.done:
			return existing.path, existing.err
		}
	}
	build := &pathBuild{done: make(chan struct{})}
	builds[key] = build
	c.mu.Unlock()

	build.path, build.err = buildPath()
	c.mu.Lock()
	delete(builds, key)
	close(build.done)
	c.mu.Unlock()
	return build.path, build.err
}

func (c *Cache) validHit(path string, object Object) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !info.Mode().IsRegular() {
		_ = os.Remove(path)
		return false, nil
	}
	if info.Size() != object.Size {
		digest, _, hashErr := hashFile(path, c.maxContentBytes)
		if hashErr == nil && digest == "sha256:"+strings.ToLower(strings.TrimPrefix(object.Digest, "sha256:")) {
			return true, fmt.Errorf("%w: cached digest has %d bytes, declaration says %d", ErrSizeMismatch, info.Size(), object.Size)
		}
		_ = os.Remove(path)
		return false, nil
	}
	if !c.verifyCacheHits {
		return true, nil
	}
	digest, _, err := hashFile(path, c.maxContentBytes)
	if err != nil || digest != strings.ToLower(object.Digest) {
		_ = os.Remove(path)
		return false, nil
	}
	return true, nil
}

func (c *Cache) populate(ctx context.Context, object Object, rawDigest, destination string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return "", err
	}
	reader, err := c.source.OpenContent(ctx, object)
	if err != nil {
		return "", fmt.Errorf("open occurrence: %w", err)
	}
	defer reader.Close()
	staged, err := os.CreateTemp(filepath.Join(c.scopeRoot, "staging"), "content-*")
	if err != nil {
		return "", err
	}
	stagedPath := staged.Name()
	defer os.Remove(stagedPath)
	hash := sha256.New()
	limited := &io.LimitedReader{R: reader, N: minInt64(object.Size, c.maxContentBytes) + 1}
	n, copyErr := io.Copy(io.MultiWriter(staged, hash), limited)
	if copyErr != nil {
		_ = staged.Close()
		return "", copyErr
	}
	if n != object.Size {
		_ = staged.Close()
		return "", fmt.Errorf("%w: got %d, want %d", ErrSizeMismatch, n, object.Size)
	}
	gotDigest := hex.EncodeToString(hash.Sum(nil))
	if gotDigest != rawDigest {
		_ = staged.Close()
		return "", fmt.Errorf("%w: got sha256:%s", ErrDigestMismatch, gotDigest)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return "", err
	}
	if err := staged.Chmod(0o400); err != nil {
		_ = staged.Close()
		return "", err
	}
	if err := staged.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(stagedPath, destination); err != nil {
		if exists, hitErr := c.validHit(destination, object); exists {
			return destination, hitErr
		}
		return "", fmt.Errorf("publish content: %w", err)
	}
	return destination, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func hashFile(path string, maximum int64) (string, int64, error) {
	opened, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer opened.Close()
	return hashReader(opened, maximum)
}

func hashReader(reader io.Reader, maximum int64) (string, int64, error) {
	hash := sha256.New()
	n, err := io.Copy(hash, &io.LimitedReader{R: reader, N: maximum + 1})
	if err != nil {
		return "", n, err
	}
	if n > maximum {
		return "", n, safepath.ErrTooLarge
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), n, nil
}
