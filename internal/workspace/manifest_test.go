package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func writeFixture(t *testing.T, root, name, value string, mode os.FileMode) {
	t.Helper()
	host := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(host), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host, []byte(value), mode); err != nil {
		t.Fatal(err)
	}
}

func TestScanDiffAndRename(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "same.txt", "same", 0o600)
	writeFixture(t, root, "modified.txt", "old", 0o600)
	writeFixture(t, root, "renamed.txt", "move", 0o600)
	before, err := Scan(root, ScanLimits{MaxFiles: 10, MaxBytes: 1024}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "modified.txt"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "renamed.txt"), filepath.Join(root, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, "added.txt", "added", 0o600)
	after, err := Scan(root, ScanLimits{MaxFiles: 10, MaxBytes: 1024}, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	changes, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[ChangeKind]int)
	for _, change := range changes {
		kinds[change.Kind]++
	}
	if kinds[ChangeAdded] != 1 || kinds[ChangeModified] != 1 || kinds[ChangeRenamed] != 1 {
		t.Fatalf("unexpected changes: %#v", changes)
	}
	domainChanges, err := DomainChanges(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if len(domainChanges) != 3 {
		t.Fatalf("domain changes = %#v", domainChanges)
	}
	byPath := make(map[string]domain.ChangeEntrySpec, len(domainChanges))
	for _, change := range domainChanges {
		byPath[change.Path()] = change.Spec()
	}
	if modified := byPath["modified.txt"]; modified.Kind != domain.ChangeModified || modified.BeforeDigest != domain.NewDigest([]byte("old")) || modified.AfterDigest != domain.NewDigest([]byte("new")) || modified.Metadata["before_size_bytes"] != "3" {
		t.Fatalf("modified domain change = %#v", modified)
	}
	if renamed := byPath["moved.txt"]; renamed.Kind != domain.ChangeRenamed || renamed.PreviousPath != "renamed.txt" {
		t.Fatalf("renamed domain change = %#v", renamed)
	}
	if added := byPath["added.txt"]; added.Kind != domain.ChangeAdded || !added.BeforeDigest.IsZero() || added.AfterDigest != domain.NewDigest([]byte("added")) {
		t.Fatalf("added domain change = %#v", added)
	}
}

func TestDiffReportsModificationTimeAsMetadata(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "result.txt", "same bytes", 0o600)
	before, err := Scan(root, ScanLimits{MaxFiles: 2, MaxBytes: 1024}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	after := before
	after.Entries = append([]Entry(nil), before.Entries...)
	after.SealedAt = time.Unix(2, 0)
	after.Entries[0].ModifiedNS++
	after.Digest, err = manifestDigest(after)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Kind != ChangeMetadata {
		t.Fatalf("changes = %#v, want one metadata change", changes)
	}
}

type memorySink struct{ data map[string][]byte }

func (s *memorySink) Capture(_ context.Context, request CaptureRequest, reader io.Reader) (CaptureResult, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return CaptureResult{}, err
	}
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[request.LogicalPath] = append([]byte(nil), content...)
	digest := sha256.Sum256(content)
	return CaptureResult{Reference: "occurrence:" + request.LogicalPath, Digest: "sha256:" + hex.EncodeToString(digest[:]), Size: int64(len(content))}, nil
}

func TestExportUsesValidatedOpenFile(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "results/report.json", `{"ok":true}`, 0o600)
	sink := &memorySink{}
	results, err := Export(context.Background(), root, []ExportSelection{{Path: "results/report.json", Role: "derived-report"}}, ScanLimits{MaxFiles: 2, MaxBytes: 100}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !bytes.Equal(sink.data["results/report.json"], []byte(`{"ok":true}`)) {
		t.Fatalf("unexpected export: %#v %#v", results, sink.data)
	}
}

func TestScanRejectsSpecialEntriesAndQuota(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "large", "12345", 0o600)
	if _, err := Scan(root, ScanLimits{MaxFiles: 1, MaxBytes: 4}, time.Now()); err == nil {
		t.Fatal("byte quota unexpectedly accepted")
	}
	if err := os.Symlink(filepath.Join(root, "large"), filepath.Join(root, "link")); err == nil {
		if _, err := Scan(root, ScanLimits{MaxFiles: 10, MaxBytes: 100}, time.Now()); err == nil {
			t.Fatal("symlink unexpectedly accepted")
		}
	}
}
