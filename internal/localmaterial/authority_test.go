package localmaterial_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/localmaterial"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

const (
	testScope     = "case/acme"
	testReference = "occurrence:specimen-1"
)

func TestAuthorityContractAndScopedSelection(t *testing.T) {
	authority, occurrence, content, _, _ := newAuthority(t, 1<<20)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	selection, err := authority.ResolveSelection(ctx, testScope, "selection:primary")
	if err != nil || len(selection) != 1 || selection[0].Occurrence != occurrence || selection[0].LogicalPath != "inputs/specimen.bin" {
		t.Fatalf("ResolveSelection() = %#v, %v", selection, err)
	}
	if _, err := authority.ResolveOccurrence(ctx, "another-scope", testReference); !domain.IsCode(err, domain.CodeForbidden) {
		t.Fatalf("cross-scope ResolveOccurrence() error = %v", err)
	}

	clock := testkit.NewClock(time.Now().UTC())
	ids := testkit.NewIDGenerator(clock)
	leaseID, _ := ids.LeaseID()
	workspaceID, _ := ids.WorkspaceID()
	agentID, _ := ids.AgentWorkspaceID()
	input := ports.InputPlan{SecurityScope: testScope, Entries: []ports.InputEntryPlan{{Occurrence: occurrence, LogicalPath: "inputs/specimen.bin", Mode: 0o400, PermittedSidecars: []string{"symbols"}}}}
	export, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: "results/report.json", Roles: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	output := ports.OutputPlan{
		IdempotencyKey: "publish-report", LeaseID: leaseID, WorkspaceID: workspaceID,
		AgentWorkspaceID: agentID, AgentGeneration: domain.InitialAgentGeneration,
		Selections: []domain.ExportSelection{export}, Content: map[string]ports.ContentSource{"results/report.json": testkit.NewMemoryContentSource([]byte(`{"ok":true}`))},
		Provenance: map[string]string{"test": "contract"},
	}
	testkit.RunMaterialAuthorityContract(t, testkit.MaterialAuthorityContract{Authority: authority, Input: input, Occurrence: occurrence, Output: output})

	reader, err := authority.OpenContent(ctx, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != string(content) {
		t.Fatalf("OpenContent() bytes = %q, %v", got, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityKeepsStagedOccurrenceWhenSourceMutates(t *testing.T) {
	authority, occurrence, original, source, _ := newAuthority(t, 1<<20)
	entry, err := authority.Entry(testScope, testReference)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, filepath.FromSlash(entry.SourcePath)), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if resolved, err := authority.ResolveOccurrence(ctx, testScope, testReference); err != nil || resolved != occurrence {
		t.Fatalf("ResolveOccurrence() after source mutation = %#v, %v", resolved, err)
	}
	reader, err := authority.OpenContent(ctx, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(reader)
	if closeErr := reader.Close(); readErr != nil || closeErr != nil || string(got) != string(original) {
		t.Fatalf("OpenContent() after source mutation = %q, read=%v, close=%v", got, readErr, closeErr)
	}
}

func TestAuthorityDetectsStagedObjectCorruption(t *testing.T) {
	authority, occurrence, _, _, publication := newAuthority(t, 1<<20)
	rawDigest := strings.TrimPrefix(occurrence.Digest.String(), "sha256:")
	objectPath := filepath.Join(publication, "objects", "sha256", rawDigest[:2], rawDigest)
	if err := os.WriteFile(objectPath, make([]byte, occurrence.Size), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := authority.ResolveOccurrence(ctx, testScope, testReference); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("ResolveOccurrence() after staged corruption error = %v", err)
	}
	if _, err := authority.OpenContent(ctx, occurrence); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("OpenContent() after staged corruption error = %v", err)
	}
}

func TestAuthorityRejectsUnsafeAndOversizedSources(t *testing.T) {
	root := t.TempDir()
	publication := t.TempDir()
	if _, err := localmaterial.New(localmaterial.Config{SourceRoot: root, PublicationRoot: publication, MaxObjectBytes: 8, Entries: []localmaterial.EntryConfig{{
		Reference: testReference, SecurityScope: testScope, SourcePath: "../escape", LogicalPath: "specimen.bin", Role: "specimen", Sensitivity: domain.SensitivityInternal,
	}}}); err == nil {
		t.Fatal("unsafe source path was accepted")
	}
	if err := os.WriteFile(filepath.Join(root, "large.bin"), []byte("123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := localmaterial.New(localmaterial.Config{SourceRoot: root, PublicationRoot: publication, MaxObjectBytes: 8, Entries: []localmaterial.EntryConfig{{
		Reference: testReference, SecurityScope: testScope, SourcePath: "large.bin", LogicalPath: "specimen.bin", Role: "specimen", Sensitivity: domain.SensitivityInternal,
	}}}); !domain.IsCode(err, domain.CodeResourceExhausted) {
		t.Fatalf("oversized source error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "regular.bin"), []byte("regular"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := localmaterial.New(localmaterial.Config{SourceRoot: root, PublicationRoot: publication, MaxObjectBytes: 8, Entries: []localmaterial.EntryConfig{{
		Reference: testReference, SecurityScope: testScope, SourcePath: "regular.bin", LogicalPath: "specimen.bin", Mode: 0o1000, Role: "specimen", Sensitivity: domain.SensitivityInternal,
	}}}); err == nil {
		t.Fatal("special mode bits were accepted")
	}
}

func TestCaptureOutputsRejectsConflictingIdempotency(t *testing.T) {
	authority, _, _, _, _ := newAuthority(t, 1<<20)
	clock := testkit.NewClock(time.Now().UTC())
	ids := testkit.NewIDGenerator(clock)
	leaseID, _ := ids.LeaseID()
	workspaceID, _ := ids.WorkspaceID()
	agentID, _ := ids.AgentWorkspaceID()
	selection, _ := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: "report.txt", Roles: []string{"report"}})
	plan := ports.OutputPlan{IdempotencyKey: "same", LeaseID: leaseID, WorkspaceID: workspaceID, AgentWorkspaceID: agentID, AgentGeneration: 1, Selections: []domain.ExportSelection{selection}, Content: map[string]ports.ContentSource{"report.txt": testkit.NewMemoryContentSource([]byte("first"))}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := authority.CaptureOutputs(ctx, plan); err != nil {
		t.Fatal(err)
	}
	plan.Content["report.txt"] = testkit.NewMemoryContentSource([]byte("second"))
	if _, err := authority.CaptureOutputs(ctx, plan); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}
}

func TestAuthorityRejectsRedirectedPublicationNamespaces(t *testing.T) {
	for _, logicalDirectory := range []string{"objects/sha256", "requests"} {
		t.Run(logicalDirectory, func(t *testing.T) {
			source := t.TempDir()
			publication := t.TempDir()
			config := localmaterial.Config{SourceRoot: source, PublicationRoot: publication, MaxObjectBytes: 1 << 20}
			if _, err := localmaterial.New(config); err != nil {
				t.Fatal(err)
			}
			namespacePath := filepath.Join(publication, filepath.FromSlash(logicalDirectory))
			if err := os.Remove(namespacePath); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			if err := os.WriteFile(sentinel, []byte("outside"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, namespacePath); err != nil {
				t.Skipf("directory links unavailable: %v", err)
			}
			if _, err := localmaterial.New(config); err == nil {
				t.Fatal("redirected publication namespace was accepted")
			}
			if content, err := os.ReadFile(sentinel); err != nil || string(content) != "outside" {
				t.Fatalf("outside sentinel changed: %q, %v", content, err)
			}
		})
	}
}

func TestAuthorityRejectsHardLinkedImmutableObject(t *testing.T) {
	authority, occurrence, want, _, publication := newAuthority(t, 1<<20)
	rawDigest := strings.TrimPrefix(occurrence.Digest.String(), "sha256:")
	objectPath := filepath.Join(publication, "objects", "sha256", rawDigest[:2], rawDigest)
	outside := filepath.Join(t.TempDir(), "object-alias")
	if err := os.Link(objectPath, outside); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := authority.OpenContent(ctx, occurrence); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("hard-linked object error = %v, want integrity violation", err)
	}
	if content, err := os.ReadFile(outside); err != nil || string(content) != string(want) {
		t.Fatalf("hard-linked object changed: %q, %v", content, err)
	}
}

func newAuthority(t *testing.T, maxBytes int64) (*localmaterial.Authority, ports.ArtifactOccurrence, []byte, string, string) {
	t.Helper()
	source := t.TempDir()
	publication := t.TempDir()
	content := []byte("deterministic specimen\n")
	if err := os.MkdirAll(filepath.Join(source, "registered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "registered", "specimen.bin"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	authority, err := localmaterial.New(localmaterial.Config{
		SourceRoot: source, PublicationRoot: publication, MaxObjectBytes: maxBytes,
		Entries:    []localmaterial.EntryConfig{{Reference: testReference, SecurityScope: testScope, SourcePath: "registered/specimen.bin", LogicalPath: "inputs/specimen.bin", Mode: 0o400, Role: "specimen", Sensitivity: domain.SensitivityInternal, Sidecars: []string{"symbols"}}},
		Selections: []localmaterial.SelectionConfig{{Reference: "selection:primary", SecurityScope: testScope, Occurrences: []string{testReference}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := authority.Entry(testScope, testReference)
	if err != nil {
		t.Fatal(err)
	}
	return authority, entry.Occurrence, content, source, publication
}
