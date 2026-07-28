package inputcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type countingSource struct {
	mu      sync.Mutex
	content []byte
	opens   int
}

type gatedSource struct {
	content []byte
	opened  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedSource) OpenContent(context.Context, Object) (io.ReadCloser, error) {
	s.once.Do(func() { close(s.opened) })
	<-s.release
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func (s *countingSource) OpenContent(context.Context, Object) (io.ReadCloser, error) {
	s.mu.Lock()
	s.opens++
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func objectFor(content []byte) Object {
	sum := sha256.Sum256(content)
	return Object{Occurrence: "case:occurrence:1", Digest: "sha256:" + hex.EncodeToString(sum[:]), Size: int64(len(content))}
}

func newTestCache(t *testing.T, source ContentSource, construction Construction) *Cache {
	t.Helper()
	cache, err := New(Options{Root: t.TempDir(), SecurityScope: "campaign-one", Construction: construction, VerifyCacheHits: true, MaxContentBytes: 1024, MaxCacheBytes: 1024, ViewRetention: time.Nanosecond, HighWaterPercent: 80, LowWaterPercent: 50}, source)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestCollectKeepsPinnedViewThenRemovesReleasedView(t *testing.T) {
	content := []byte("collectable")
	source := &countingSource{content: content}
	cache := newTestCache(t, source, AllowCopy)
	object := objectFor(content)
	manifest := InputViewManifest{Selection: "selection:gc", Layout: "flat", Entries: []InputEntry{{Path: "one", Occurrence: object.Occurrence, Digest: object.Digest, Size: object.Size, Mode: 0o400}}}
	_, canonical, err := cache.BuildView(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Pin(canonical.ViewID, "owner"); err != nil {
		t.Fatal(err)
	}
	first, err := cache.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.RemovedViews) != 0 {
		t.Fatal("pinned view was removed")
	}
	if err := cache.Release(canonical.ViewID, "owner"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := cache.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(second.RemovedViews) != 1 {
		t.Fatalf("removed views = %v, want one", second.RemovedViews)
	}
}

func TestConcurrentMissFetchedOnce(t *testing.T) {
	content := []byte("immutable evidence")
	source := &countingSource{content: content}
	cache := newTestCache(t, source, AllowCopy)
	object := objectFor(content)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for index := 0; index < 32; index++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := cache.EnsureContent(context.Background(), object); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if source.opens != 1 {
		t.Fatalf("source opened %d times, want once", source.opens)
	}
}

func TestConcurrentDigestJoinRevalidatesEachDeclaredSize(t *testing.T) {
	content := []byte("one immutable object")
	source := &gatedSource{content: content, opened: make(chan struct{}), release: make(chan struct{})}
	cache := newTestCache(t, source, AllowCopy)
	correct := objectFor(content)
	wrong := correct
	wrong.Size++

	correctResult := make(chan error, 1)
	go func() {
		_, err := cache.EnsureContent(context.Background(), correct)
		correctResult <- err
	}()
	<-source.opened
	wrongResult := make(chan error, 1)
	go func() {
		_, err := cache.EnsureContent(context.Background(), wrong)
		wrongResult <- err
	}()
	close(source.release)
	if err := <-correctResult; err != nil {
		t.Fatalf("correct declaration failed: %v", err)
	}
	if err := <-wrongResult; !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("conflicting declaration error = %v, want size mismatch", err)
	}
	if _, err := cache.EnsureContent(context.Background(), correct); err != nil {
		t.Fatalf("conflicting declaration removed valid object: %v", err)
	}
}

func TestDigestMismatchIsNotPublished(t *testing.T) {
	source := &countingSource{content: []byte("wrong")}
	cache := newTestCache(t, source, AllowCopy)
	object := objectFor([]byte("expected"))
	if _, err := cache.EnsureContent(context.Background(), object); err == nil {
		t.Fatal("digest mismatch unexpectedly accepted")
	}
}

func TestExactViewReuseAndPin(t *testing.T) {
	content := []byte("one")
	source := &countingSource{content: content}
	cache := newTestCache(t, source, AllowCopy)
	object := objectFor(content)
	manifest := InputViewManifest{Selection: "selection:1", Layout: "by-evidence-path", Entries: []InputEntry{{Path: "inputs/one.bin", Occurrence: object.Occurrence, Digest: object.Digest, Size: object.Size, Mode: 0o400}}}
	firstPath, canonical, err := cache.BuildView(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	secondPath, second, err := cache.BuildView(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath != secondPath || canonical.ViewID != second.ViewID {
		t.Fatal("exact view was not reused")
	}
	if err := cache.Pin(canonical.ViewID, "agent-generation:1"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Release(canonical.ViewID, "agent-generation:1"); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalManifestRejectsConflictAndTraversal(t *testing.T) {
	object := objectFor([]byte("x"))
	base := InputEntry{Path: "same", Occurrence: object.Occurrence, Digest: object.Digest, Size: object.Size, Mode: 0o400}
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base, base}}); err == nil {
		t.Fatal("duplicate path accepted")
	}
	base.Path = "../escape"
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base}}); err == nil {
		t.Fatal("traversal accepted")
	}
	base.Path = "tree"
	child := base
	child.Path = "tree/child"
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base, child}}); err == nil {
		t.Fatal("file/directory ancestor conflict accepted")
	}
	base.Path = "sidecars"
	base.Sidecars = []string{"metadata", "metadata"}
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base}}); err == nil {
		t.Fatal("duplicate sidecar accepted")
	}
	base.Sidecars = []string{" "}
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base}}); err == nil {
		t.Fatal("blank sidecar accepted")
	}
	base.Sidecars = nil
	base.Mode = 0
	canonical, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base}})
	if err != nil {
		t.Fatal(err)
	}
	if canonical.Entries[0].Mode != 0o444 {
		t.Fatalf("default mode = %#o, want 0444", canonical.Entries[0].Mode)
	}
	base.Mode = 0o1000
	if _, err := CanonicalizeManifest(InputViewManifest{Selection: "s", Layout: "l", Entries: []InputEntry{base}}); err == nil {
		t.Fatal("special mode bits accepted")
	}
}

func TestBuildViewReconstructsTamperedManifestAndEntry(t *testing.T) {
	content := []byte("trusted")
	cache := newTestCache(t, &countingSource{content: content}, AllowCopy)
	object := objectFor(content)
	requested := InputViewManifest{Selection: "selection:integrity", Layout: "flat", Entries: []InputEntry{{
		Path: "specimen.bin", Occurrence: object.Occurrence, Digest: object.Digest, Size: object.Size, Mode: 0o400,
	}}}
	root, canonical, err := cache.BuildView(context.Background(), requested)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(filepath.Dir(root), "input-view-manifest.json")
	encoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var tampered InputViewManifest
	if err := json.Unmarshal(encoded, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.Selection = "selection:tampered"
	encoded, err = json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifestPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	rebuiltRoot, rebuilt, err := cache.BuildView(context.Background(), requested)
	if err != nil {
		t.Fatal(err)
	}
	if rebuiltRoot != root || rebuilt.ViewID != canonical.ViewID {
		t.Fatalf("rebuilt view = (%q, %q), want (%q, %q)", rebuiltRoot, rebuilt.ViewID, root, canonical.ViewID)
	}
	restoredManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(restoredManifest, []byte("selection:tampered")) {
		t.Fatal("tampered manifest survived exact-view reconstruction")
	}

	entryPath := filepath.Join(root, "specimen.bin")
	if err := os.Chmod(entryPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entryPath, []byte("hostile"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.BuildView(context.Background(), requested); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(entryPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, content) {
		t.Fatalf("rebuilt entry = %q, want %q", restored, content)
	}
}
