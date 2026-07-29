package linuxcontainer

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

func TestRunRecordPublicationSurvivesInterruptedPartialWindow(t *testing.T) {
	directory := t.TempDir()
	payload := []byte(`{"schema_version":1,"complete":true}`)
	interrupted := errors.New("injected crash before publication")
	err := publishCompleteRunRecord(directory, "record.json", payload, 1024, func(temporary, final string) error {
		staged, readErr := os.ReadFile(temporary)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !reflect.DeepEqual(staged, payload) {
			t.Fatalf("staged record = %q, want %q", staged, payload)
		}
		if _, statErr := os.Stat(final); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("final record became visible before atomic publication: %v", statErr)
		}
		return interrupted
	})
	if !errors.Is(err, interrupted) {
		t.Fatalf("interrupted publication error = %v", err)
	}
	if _, err := loadRunRecord(directory, "record.json", 1024); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("interrupted publication exposed a final record: %v", err)
	}
	// A real process loss can leave its private temporary name behind. Readers
	// must ignore that truncated staging file and an exact retry must still be
	// able to publish the complete immutable record.
	if err := os.WriteFile(filepath.Join(directory, ".record.json.tmp-interrupted"), payload[:5], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRunRecord(directory, "record.json", 1024); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("truncated staging record was treated as final: %v", err)
	}
	if err := persistRunRecord(directory, "record.json", payload, 1024); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadRunRecord(directory, "record.json", 1024)
	if err != nil || !reflect.DeepEqual(loaded, payload) {
		t.Fatalf("retried record = %q, %v", loaded, err)
	}
	if err := persistRunRecord(directory, "record.json", []byte("replacement"), 1024); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("published record was replaceable: %v", err)
	}
}

func TestRunBaselinePersistsExactlyAndRejectsReplacement(t *testing.T) {
	directory := t.TempDir()
	writeRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(writeRoot, "result.bin"), []byte("durable result"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := scanTargetOnce(context.Background(), writeRoot, workspacepkg.ScanLimits{MaxFiles: 4, MaxBytes: 1024}, time.Unix(12, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := persistRunBaseline(directory, manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadRunBaseline(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, manifest) {
		t.Fatalf("loaded baseline = %#v, want %#v", loaded, manifest)
	}
	if err := persistRunBaseline(directory, manifest); err == nil {
		t.Fatal("durable baseline was replaceable")
	}
	if err := os.WriteFile(filepath.Join(directory, runBaselineFile), []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRunBaseline(directory); err == nil {
		t.Fatal("tampered baseline was accepted")
	}
}

func TestRunStartRecordBindsExactAuthority(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 3, RunID: run}
	startedAt := time.Unix(33, 0).UTC()
	materialization := domain.NewDigest([]byte("materialization"))
	directory := t.TempDir()
	if err := persistRunStart(directory, authority, startedAt, testRuntimeID("runtime-3"), "cgroup/old", materialization); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := loadRunStart(directory, authority)
	if err != nil || !found || loaded.StartedAt != startedAt || loaded.RuntimeID != testRuntimeID("runtime-3") || loaded.Materialization != materialization {
		t.Fatalf("loaded start = %v, %t, %v", loaded, found, err)
	}
	wrong := authority
	wrong.Generation++
	if _, _, err := loadRunStart(directory, wrong); err == nil {
		t.Fatal("start record was accepted for another generation")
	}
	if err := persistRunStart(directory, authority, startedAt, testRuntimeID("runtime-3"), "cgroup/old", materialization); err == nil {
		t.Fatal("durable run start was replaceable")
	}
}
