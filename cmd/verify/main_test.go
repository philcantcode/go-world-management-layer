package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExecuteRecordsPassingAndFailingGates(t *testing.T) {
	passed, err := execute("pass", func() error { return nil })
	if err != nil || passed.Status != "passed" || passed.Error != "" || passed.DurationNanoseconds < 0 || passed.FinishedAt.Before(passed.StartedAt) {
		t.Fatalf("passing gate = %#v, %v", passed, err)
	}
	wantErr := errors.New("deliberate failure")
	failed, err := execute("fail", func() error { return wantErr })
	if !errors.Is(err, wantErr) || failed.Status != "failed" || failed.Error != wantErr.Error() || failed.DurationNanoseconds < 0 || failed.FinishedAt.Before(failed.StartedAt) {
		t.Fatalf("failing gate = %#v, %v", failed, err)
	}
}

func TestWriteSummaryPublishesMachineReadableResult(t *testing.T) {
	started := time.Unix(100, 0).UTC()
	want := verificationSummary{
		SchemaVersion: verificationSummaryVersion,
		StartedAt:     started,
		FinishedAt:    started.Add(time.Second),
		Success:       true,
		SelectedGate:  "format",
		Gates: []gateResult{{
			Name: "format", Status: "passed", StartedAt: started,
			FinishedAt: started.Add(time.Second), DurationNanoseconds: int64(time.Second),
		}},
	}
	path := filepath.Join(t.TempDir(), "nested", "summary.json")
	if err := writeSummary(path, want); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got verificationSummary
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("summary is not JSON: %v", err)
	}
	if got.SchemaVersion != want.SchemaVersion || !got.Success || got.SelectedGate != want.SelectedGate || len(got.Gates) != 1 || got.Gates[0].Status != "passed" {
		t.Fatalf("summary = %#v", got)
	}
}

func TestWriteSummaryRejectsBlankPath(t *testing.T) {
	if err := writeSummary("  ", verificationSummary{}); err == nil {
		t.Fatal("blank summary path was accepted")
	}
}

func TestRequireEqualFilesDetectsGenerationDrift(t *testing.T) {
	directory := t.TempDir()
	want := filepath.Join(directory, "want.go")
	got := filepath.Join(directory, "got.go")
	if err := os.WriteFile(want, []byte("generated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(got, []byte("generated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireEqualFiles(want, got); err != nil {
		t.Fatalf("equal generated files: %v", err)
	}
	if err := os.WriteFile(got, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireEqualFiles(want, got); err == nil {
		t.Fatal("generation drift was not detected")
	}
}
