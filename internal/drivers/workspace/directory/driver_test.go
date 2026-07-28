package directory

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

type byteSource struct {
	digest  domain.Digest
	size    int64
	content []byte
	openErr error
}

type blockingSource struct {
	digest domain.Digest
	size   int64
}

func (s blockingSource) Digest() domain.Digest { return s.digest }
func (s blockingSource) Size() int64           { return s.size }
func (s blockingSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return blockingReader{ctx: ctx}, nil
}

type blockingReader struct{ ctx context.Context }

func (r blockingReader) Read([]byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}
func (blockingReader) Close() error { return nil }

func sourceFor(content []byte) *byteSource {
	owned := append([]byte(nil), content...)
	return &byteSource{digest: domain.NewDigest(owned), size: int64(len(owned)), content: owned}
}

func (s *byteSource) Digest() domain.Digest { return s.digest }
func (s *byteSource) Size() int64           { return s.size }
func (s *byteSource) Open(context.Context) (io.ReadCloser, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func newTestPlan(t *testing.T, files map[string][]byte) ports.WorkspacePlan {
	t.Helper()
	leaseID, err := domain.NewLeaseID()
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := domain.NewAgentWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, err := domain.NewWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]domain.InputViewEntry, 0, len(files))
	content := make(map[string]ports.ContentSource, len(files))
	for logicalPath, value := range files {
		source := sourceFor(value)
		entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
			LogicalPath: logicalPath, OccurrenceRef: "artifact://test/" + logicalPath,
			Digest: source.Digest(), Size: source.Size(), Mode: 0o600,
		})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
		content[logicalPath] = source
	}
	manifest, err := domain.NewInputViewManifest(entries)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.InitialAgentGeneration, InputViewID: manifest.ID(), CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.WorkspacePlan{
		IdempotencyKey: "prepare-" + workspaceID.String(), Workspace: workspace, InputView: manifest, Content: content,
		Construction: domain.InputViewAllowCopy, UpperByteLimit: 1 << 20, UpperInodeLimit: 100,
	}
}

func testDriver(t *testing.T, root string) *Driver {
	t.Helper()
	driver, err := New(Config{Root: root, Now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestDriverContract(t *testing.T) {
	plan := newTestPlan(t, map[string][]byte{"input/specimen.bin": []byte("contract specimen")})
	testkit.RunWorkspaceDriverContract(t, testkit.WorkspaceDriverContract{
		Driver: testDriver(t, t.TempDir()), Plan: plan,
	})
}

func TestWorkspacePlanRequiresExactContentIdentities(t *testing.T) {
	plan := newTestPlan(t, map[string][]byte{"input/a.bin": []byte("a")})
	if err := plan.Validate(); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}

	t.Run("missing", func(t *testing.T) {
		invalid := plan
		invalid.Content = nil
		if err := invalid.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Fatalf("Validate() error = %v, want invalid argument", err)
		}
	})

	t.Run("extra", func(t *testing.T) {
		invalid := plan
		invalid.Content = cloneSources(plan.Content)
		invalid.Content["extra.bin"] = sourceFor([]byte("extra"))
		if err := invalid.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Fatalf("Validate() error = %v, want invalid argument", err)
		}
	})

	t.Run("identity mismatch", func(t *testing.T) {
		invalid := plan
		invalid.Content = cloneSources(plan.Content)
		invalid.Content["input/a.bin"] = sourceFor([]byte("different"))
		if err := invalid.Validate(); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("Validate() error = %v, want integrity violation", err)
		}
	})
}

func TestPrepareVerifiesStreamAndCleansStaging(t *testing.T) {
	root := t.TempDir()
	plan := newTestPlan(t, map[string][]byte{"input/specimen.bin": []byte("expected")})
	declared := plan.Content["input/specimen.bin"].(*byteSource)
	plan.Content = cloneSources(plan.Content)
	plan.Content["input/specimen.bin"] = &byteSource{digest: declared.digest, size: declared.size, content: []byte("corrupt!")}
	_, err := testDriver(t, root).Prepare(testContext(t), plan)
	if !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("Prepare() error = %v, want integrity violation", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed prepare leaked entries: %#v", entries)
	}
}

func TestPrepareHonorsActiveDeadlineAndReflinkRequirement(t *testing.T) {
	t.Run("active deadline", func(t *testing.T) {
		plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("x")})
		declared := plan.Content["input.bin"]
		plan.Content = map[string]ports.ContentSource{
			"input.bin": blockingSource{digest: declared.Digest(), size: declared.Size()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if _, err := testDriver(t, t.TempDir()).Prepare(ctx, plan); !domain.IsCode(err, domain.CodeDeadlineExceeded) {
			t.Fatalf("Prepare() error = %v, want deadline exceeded", err)
		}
	})

	t.Run("reflink", func(t *testing.T) {
		plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("x")})
		plan.Construction = domain.InputViewRequireReflink
		if _, err := testDriver(t, t.TempDir()).Prepare(testContext(t), plan); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("Prepare() error = %v, want capability unavailable", err)
		}
	})
}

func TestPrepareEnforcesByteAndInodeQuotas(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("1234")})
		plan.UpperByteLimit = 3
		_, err := testDriver(t, t.TempDir()).Prepare(testContext(t), plan)
		if !domain.IsCode(err, domain.CodeResourceExhausted) {
			t.Fatalf("Prepare() error = %v, want resource exhausted", err)
		}
	})

	t.Run("inodes include directories", func(t *testing.T) {
		plan := newTestPlan(t, map[string][]byte{"one/two/input.bin": []byte("x")})
		plan.UpperInodeLimit = 2
		_, err := testDriver(t, t.TempDir()).Prepare(testContext(t), plan)
		if !domain.IsCode(err, domain.CodeResourceExhausted) {
			t.Fatalf("Prepare() error = %v, want resource exhausted", err)
		}
	})
}

func TestPrepareIsPersistentAndConflictAware(t *testing.T) {
	root := t.TempDir()
	plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
	driver := testDriver(t, root)
	first, err := driver.Prepare(testContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := driver.Prepare(testContext(t), plan)
	if err != nil || replayed.MergedPath != first.MergedPath {
		t.Fatalf("Prepare(replay) = %#v, %v", replayed, err)
	}

	restarted := testDriver(t, root)
	restartedReplay, err := restarted.Prepare(testContext(t), plan)
	if err != nil || restartedReplay.MergedPath != first.MergedPath {
		t.Fatalf("Prepare(restart replay) = %#v, %v", restartedReplay, err)
	}

	differentWorkspace := planWithNewWorkspace(t, plan)
	if _, err := restarted.Prepare(testContext(t), differentWorkspace); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("reused idempotency key error = %v, want conflict", err)
	}
	differentKey := plan
	differentKey.IdempotencyKey += "-different"
	if _, err := restarted.Prepare(testContext(t), differentKey); !domain.IsCode(err, domain.CodeAlreadyExists) {
		t.Fatalf("reused workspace id error = %v, want already exists", err)
	}
}

func TestSealProducesDigestAndMetadataChanges(t *testing.T) {
	root := t.TempDir()
	plan := newTestPlan(t, map[string][]byte{
		"input/modified.txt": []byte("before"),
		"input/deleted.txt":  []byte("deleted"),
		"input/renamed.txt":  []byte("renamed"),
		"input/metadata.txt": []byte("metadata"),
	})
	driver := testDriver(t, root)
	handle, err := driver.Prepare(testContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handle.MergedPath, "input", "modified.txt"), []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(handle.MergedPath, "input", "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(handle.MergedPath, "input", "renamed.txt"), filepath.Join(handle.MergedPath, "input", "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(handle.MergedPath, "input", "metadata.txt"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handle.MergedPath, "added.txt"), []byte("added"), 0o600); err != nil {
		t.Fatal(err)
	}

	preview := requirePreview(t, driver, handle.WorkspaceID)
	sealed, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[domain.ChangeKind]int)
	for _, entry := range sealed.ChangeSet.Entries() {
		spec := entry.Spec()
		kinds[spec.Kind]++
		if spec.Kind == domain.ChangeModified {
			if spec.BeforeDigest.IsZero() || spec.AfterDigest.IsZero() || len(spec.Metadata) == 0 {
				t.Fatalf("modified change lacks digests or metadata: %#v", spec)
			}
		}
	}
	if kinds[domain.ChangeAdded] != 1 || kinds[domain.ChangeModified] != 1 || kinds[domain.ChangeDeleted] != 1 || kinds[domain.ChangeRenamed] != 1 {
		t.Fatalf("unexpected change kinds: %#v", kinds)
	}
	if runtime.GOOS != "windows" && kinds[domain.ChangeMetadataOnly] != 1 {
		t.Fatalf("metadata-only change missing: %#v", kinds)
	}
	replayed, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil || !replayed.SealedAt.Equal(sealed.SealedAt) {
		t.Fatalf("Seal(replay) = %#v, %v", replayed, err)
	}
	if _, err := testDriver(t, root).Inspect(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatalf("Inspect(after restart) error = %v", err)
	}
}

func TestMountRejectsMutationAndSealRejectsUnsafeEntries(t *testing.T) {
	t.Run("pre-mount mutation", func(t *testing.T) {
		plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
		driver := testDriver(t, t.TempDir())
		handle, err := driver.Prepare(testContext(t), plan)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(handle.MergedPath, "unexpected.bin"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := driver.Mount(testContext(t), handle.WorkspaceID); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("Mount() error = %v, want integrity violation", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		outside := t.TempDir()
		plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
		driver := testDriver(t, t.TempDir())
		handle, err := driver.Prepare(testContext(t), plan)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(handle.MergedPath, "escape")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := driver.Preview(testContext(t), handle.WorkspaceID); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("Preview() error = %v, want integrity violation", err)
		}
	})
}

func TestSealEnforcesPostMountQuota(t *testing.T) {
	plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("1234")})
	plan.UpperByteLimit = 4
	driver := testDriver(t, t.TempDir())
	handle, err := driver.Prepare(testContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handle.MergedPath, "extra.bin"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Preview(testContext(t), handle.WorkspaceID); !domain.IsCode(err, domain.CodeResourceExhausted) {
		t.Fatalf("Preview() error = %v, want resource exhausted", err)
	}
}

func TestReleaseRejectsWorkspaceRootSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	marker := filepath.Join(outside, "keep.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
	driver := testDriver(t, root)
	handle, err := driver.Prepare(testContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	preview := requirePreview(t, driver, handle.WorkspaceID)
	if _, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision()); err != nil {
		t.Fatal(err)
	}
	workspaceRoot := filepath.Dir(handle.MergedPath)
	saved := workspaceRoot + ".saved"
	if err := os.Rename(workspaceRoot, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, workspaceRoot); err != nil {
		_ = os.Rename(saved, workspaceRoot)
		t.Skipf("symlinks unavailable: %v", err)
	}
	defer func() {
		_ = os.Remove(workspaceRoot)
		_ = os.Rename(saved, workspaceRoot)
	}()
	if err := driver.Release(testContext(t), handle.WorkspaceID); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("Release() error = %v, want integrity violation", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("outside marker was removed: %v", err)
	}
}

func TestCorruptAuthorityFailsClosedOnRestart(t *testing.T) {
	root := t.TempDir()
	plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
	handle, err := testDriver(t, root).Prepare(testContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(handle.MergedPath), recordFilename), []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Root: root}); err == nil {
		t.Fatal("New() accepted corrupt workspace authority")
	}
}

func TestAllOperationsRequireDeadlines(t *testing.T) {
	plan := newTestPlan(t, map[string][]byte{"input.bin": []byte("input")})
	driver := testDriver(t, t.TempDir())
	ctx := context.Background()
	if _, err := driver.Prepare(ctx, plan); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Prepare() error = %v", err)
	}
	workspaceID := plan.Workspace.ID()
	if _, err := driver.Mount(ctx, workspaceID); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Mount() error = %v", err)
	}
	if _, err := driver.Inspect(ctx, workspaceID); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Inspect() error = %v", err)
	}
	if _, err := driver.Preview(ctx, workspaceID); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Preview() error = %v", err)
	}
	if _, err := driver.Seal(ctx, workspaceID, domain.InitialRevision); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Seal() error = %v", err)
	}
	if err := driver.Release(ctx, workspaceID); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestSealRejectsWorkspaceChangedAfterPreview(t *testing.T) {
	driver := testDriver(t, t.TempDir())
	handle, err := driver.Prepare(testContext(t), newTestPlan(t, map[string][]byte{"input.bin": []byte("input")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	first := requirePreview(t, driver, handle.WorkspaceID)
	if err := os.WriteFile(filepath.Join(handle.MergedPath, "result.bin"), []byte("result"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Seal(testContext(t), handle.WorkspaceID, first.ChangeSet.WorkspaceRevision()); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("Seal(stale preview) error = %v, want conflict", err)
	}
	second := requirePreview(t, driver, handle.WorkspaceID)
	if second.ChangeSet.WorkspaceRevision() != first.ChangeSet.WorkspaceRevision()+1 {
		t.Fatalf("second preview revision = %d, want %d", second.ChangeSet.WorkspaceRevision(), first.ChangeSet.WorkspaceRevision()+1)
	}
	if _, err := driver.Seal(testContext(t), handle.WorkspaceID, second.ChangeSet.WorkspaceRevision()); err != nil {
		t.Fatalf("Seal(current preview) error = %v", err)
	}
}

func TestSealPublishesPrivateImmutableSnapshot(t *testing.T) {
	driver := testDriver(t, t.TempDir())
	handle, err := driver.Prepare(testContext(t), newTestPlan(t, map[string][]byte{"input.bin": []byte("input")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(handle.MergedPath, "result.bin")
	if err := os.WriteFile(resultPath, []byte("sealed-result"), 0o600); err != nil {
		t.Fatal(err)
	}
	preview := requirePreview(t, driver, handle.WorkspaceID)
	sealed, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil {
		t.Fatal(err)
	}
	if sealed.ImmutablePath == "" || filepath.Clean(sealed.ImmutablePath) == filepath.Clean(handle.MergedPath) {
		t.Fatalf("immutable path = %q, merged path = %q", sealed.ImmutablePath, handle.MergedPath)
	}
	snapshotPath := filepath.Join(sealed.ImmutablePath, "result.bin")
	if err := os.WriteFile(resultPath, []byte("mutated-after-seal"), 0o600); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "sealed-result" {
		t.Fatalf("snapshot content = %q", content)
	}
	replayed, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ImmutablePath != sealed.ImmutablePath {
		t.Fatalf("replayed immutable path = %q, want %q", replayed.ImmutablePath, sealed.ImmutablePath)
	}
}

func TestSealRejectsTamperedPersistedSnapshot(t *testing.T) {
	driver := testDriver(t, t.TempDir())
	handle, err := driver.Prepare(testContext(t), newTestPlan(t, map[string][]byte{"input.bin": []byte("input")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Mount(testContext(t), handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
	preview := requirePreview(t, driver, handle.WorkspaceID)
	sealed, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sealed.ImmutablePath, "input.bin"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Seal(testContext(t), handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision()); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("Seal(tampered snapshot) error = %v, want integrity violation", err)
	}
}

func TestPersistedWorkspaceIdentityMismatchIsAnIntegrityError(t *testing.T) {
	requested, err := domain.NewWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := domain.NewWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}

	err = requireRecordIdentity(requested, diskRecord{WorkspaceID: persisted.String()}, "workspace.directory.test")
	if !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("expected integrity violation, got %v", err)
	}
}

func requirePreview(t *testing.T, driver *Driver, workspaceID domain.WorkspaceID) ports.WorkspacePreviewResult {
	t.Helper()
	preview, err := driver.Preview(testContext(t), workspaceID)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	return preview
}

func planWithNewWorkspace(t *testing.T, plan ports.WorkspacePlan) ports.WorkspacePlan {
	t.Helper()
	spec := plan.Workspace.Spec()
	workspaceID, err := domain.NewWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	spec.ID = workspaceID
	workspace, err := domain.NewWorkspace(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan.Workspace = workspace
	return plan
}

func cloneSources(values map[string]ports.ContentSource) map[string]ports.ContentSource {
	result := make(map[string]ports.ContentSource, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
