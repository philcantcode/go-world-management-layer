package ledger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppendRollsBackPartialWriteBeforeRetry(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	injected := errors.New("injected partial write")
	failed := false
	store.hooks = &ledgerHooks{write: func(file *os.File, value []byte) (int64, error) {
		if !failed {
			failed = true
			count, err := file.Write(value[:len(value)/2])
			if err != nil {
				return int64(count), err
			}
			return int64(count), injected
		}
		return writeAll(file, value)
	}}

	input := Record{Kind: RecordControl, Payload: []byte("retry me")}
	if result, err := store.Append(context.Background(), input); !errors.Is(err, injected) || len(result.Records) != 0 || store.Head() != 0 {
		t.Fatalf("failed append = %#v, %v; head=%d", result, err, store.Head())
	}
	if size := activeSegmentSize(t, store); size != 0 {
		t.Fatalf("partial frame remains after rollback: %d bytes", size)
	}
	result, err := store.Append(context.Background(), input)
	if err != nil || len(result.Records) != 1 || result.Records[0].Cursor != 1 {
		t.Fatalf("retry append = %#v, %v", result, err)
	}
}

func TestAppendRollsBackFrameWhenFsyncReportsFailure(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	injected := errors.New("injected append fsync failure")
	failed := false
	store.hooks = &ledgerHooks{sync: func(file *os.File, stage string) error {
		if stage == "append" && !failed {
			failed = true
			if err := file.Sync(); err != nil {
				return err
			}
			return injected
		}
		return file.Sync()
	}}

	input := Record{Kind: RecordControl, Payload: []byte("durable before reported failure")}
	if _, err := store.Append(context.Background(), input); !errors.Is(err, injected) || store.Head() != 0 {
		t.Fatalf("fsync failure = %v; head=%d", err, store.Head())
	}
	if size := activeSegmentSize(t, store); size != 0 {
		t.Fatalf("fsynced frame remains after durable rollback: %d bytes", size)
	}
	result, err := store.Append(context.Background(), input)
	if err != nil || result.Records[0].Cursor != 1 {
		t.Fatalf("retry append = %#v, %v", result, err)
	}
}

func TestAppendFailsClosedWhenRollbackCannotBeSynced(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	store.hooks = &ledgerHooks{sync: func(file *os.File, stage string) error {
		if stage == "append" {
			if err := file.Sync(); err != nil {
				return err
			}
			return errors.New("append sync state is ambiguous")
		}
		if stage == "rollback" {
			return errors.New("rollback sync failed")
		}
		return file.Sync()
	}}

	input := Record{Kind: RecordControl}
	if _, err := store.Append(context.Background(), input); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ambiguous append error = %v", err)
	}
	if _, err := store.Append(context.Background(), input); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("append after ambiguous rollback = %v", err)
	}
	if _, err := store.ReadAfter(0, 0); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("read after ambiguous rollback = %v", err)
	}
	if err := store.Close(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("close after ambiguous rollback = %v", err)
	}
}

func TestDurableAppendSurvivesIndexFailureWithoutDuplicateObservation(t *testing.T) {
	directory := t.TempDir()
	store := openTestLedger(t, Options{Directory: directory})
	injected := errors.New("injected index publication failure")
	store.hooks = &ledgerHooks{index: func(string, []SegmentMetadata) error { return injected }}
	input := sourceRecord(1, "one")
	result, err := store.Append(context.Background(), input)
	if err != nil || len(result.Records) != 1 || result.Records[0].Cursor != 1 || !errors.Is(store.indexErr, injected) {
		t.Fatalf("durable append with index failure = %#v, %v; index=%v", result, err, store.indexErr)
	}

	// Simulate process loss before Close can repair the rebuildable sidecar.
	crashOpenSegment(t, store)
	reopened := openTestLedger(t, Options{Directory: directory})
	defer reopened.Close()
	retry, err := reopened.Append(context.Background(), input)
	if err != nil || !retry.Duplicate || len(retry.Records) != 1 || retry.Records[0].Duplicate.OriginalCursor != 1 {
		t.Fatalf("post-recovery retry = %#v, %v", retry, err)
	}
	records, err := reopened.ReadAfter(0, 0)
	if err != nil || len(records) != 2 || records[0].Kind != RecordObservation || records[1].Kind != RecordDuplicate {
		t.Fatalf("recovered records = %#v, %v", records, err)
	}
}

func TestCloseRetriesDirtyIndexPublication(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	failIndex := true
	store.hooks = &ledgerHooks{index: func(directory string, segments []SegmentMetadata) error {
		if failIndex {
			return errors.New("index unavailable")
		}
		return writeSegmentIndex(directory, segments)
	}}
	if _, err := store.Append(context.Background(), Record{Kind: RecordControl}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err == nil || !strings.Contains(err.Error(), "persist segment index") {
		t.Fatalf("first Close error = %v", err)
	}
	failIndex = false
	if err := store.Close(); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
}

func TestRotateReopensActiveSegmentAfterRenameFailure(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	if _, err := store.Append(context.Background(), Record{Kind: RecordControl, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected rename failure")
	failed := false
	store.hooks = &ledgerHooks{rename: func(oldPath, newPath string) error {
		if !failed {
			failed = true
			return injected
		}
		return os.Rename(oldPath, newPath)
	}}
	if _, err := store.Rotate(); !errors.Is(err, injected) {
		t.Fatalf("first Rotate error = %v", err)
	}
	if store.active == nil {
		t.Fatal("rename failure left no retryable active segment")
	}
	if _, err := store.Rotate(); err != nil {
		t.Fatalf("retry Rotate: %v", err)
	}
	result, err := store.Append(context.Background(), Record{Kind: RecordControl, Payload: []byte("second")})
	if err != nil || result.Records[0].Cursor != 2 {
		t.Fatalf("append after rotate retry = %#v, %v", result, err)
	}
}

func TestGapAndObservationCommitAsOneRetryableBatch(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir()})
	if _, err := store.Append(context.Background(), sourceRecord(1, "one")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("batch write failed")
	failed := false
	store.hooks = &ledgerHooks{write: func(file *os.File, value []byte) (int64, error) {
		if !failed {
			failed = true
			return 0, injected
		}
		return writeAll(file, value)
	}}
	if result, err := store.Append(context.Background(), sourceRecord(3, "three")); !errors.Is(err, injected) || len(result.Records) != 0 || store.Head() != 1 {
		t.Fatalf("failed batch = %#v, %v; head=%d", result, err, store.Head())
	}
	retry, err := store.Append(context.Background(), sourceRecord(3, "three"))
	if err != nil || len(retry.Records) != 2 || retry.Records[0].Kind != RecordGap || retry.Records[0].Cursor != 2 || retry.Records[1].Cursor != 3 {
		t.Fatalf("batch retry = %#v, %v", retry, err)
	}
	records, err := store.ReadAfter(0, 0)
	if err != nil || len(records) != 3 {
		t.Fatalf("durable batch records = %#v, %v", records, err)
	}
}

func TestSourceDuplicateWindowIsBoundedAndFallsBackToDurableLookup(t *testing.T) {
	directory := t.TempDir()
	store := openTestLedger(t, Options{Directory: directory, MaxSourceSequences: 2})
	for sequence := uint64(1); sequence <= 5; sequence++ {
		if _, err := store.Append(context.Background(), sourceRecord(sequence, string(rune('a'+sequence)))); err != nil {
			t.Fatal(err)
		}
	}
	state := store.sourceState("collector", "boot-1")
	if len(state.seen) != 2 || len(state.order) != 2 {
		t.Fatalf("duplicate window grew beyond bound: seen=%d order=%d", len(state.seen), len(state.order))
	}
	old := sourceRecord(1, string(rune('a'+1)))
	result, err := store.Append(context.Background(), old)
	if err != nil || !result.Duplicate || result.Records[0].Duplicate.OriginalCursor != 1 || result.Records[0].Duplicate.ConflictingPayload {
		t.Fatalf("evicted duplicate lookup = %#v, %v", result, err)
	}
	if len(state.seen) != 2 || len(state.order) != 2 {
		t.Fatalf("duplicate lookup expanded window: seen=%d order=%d", len(state.seen), len(state.order))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestLedger(t, Options{Directory: directory, MaxSourceSequences: 2})
	defer reopened.Close()
	conflict := sourceRecord(1, "different")
	result, err = reopened.Append(context.Background(), conflict)
	if err != nil || !result.Duplicate || result.Records[0].Duplicate.OriginalCursor != 1 || !result.Records[0].Duplicate.ConflictingPayload {
		t.Fatalf("recovered evicted duplicate lookup = %#v, %v", result, err)
	}
}

func TestLateSequenceInsidePriorGapIsNotMisclassifiedAsDuplicate(t *testing.T) {
	store := openTestLedger(t, Options{Directory: t.TempDir(), MaxSourceSequences: 1})
	defer store.Close()
	if _, err := store.Append(context.Background(), sourceRecord(1, "one")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), sourceRecord(4, "four")); err != nil {
		t.Fatal(err)
	}
	late, err := store.Append(context.Background(), sourceRecord(2, "late-two"))
	if err != nil || late.Duplicate || len(late.Records) != 1 || late.Records[0].Kind != RecordObservation {
		t.Fatalf("late sequence = %#v, %v", late, err)
	}
}

func openTestLedger(t *testing.T, options Options) *Ledger {
	t.Helper()
	store, _, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func activeSegmentSize(t *testing.T, store *Ledger) int64 {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.active == nil {
		return 0
	}
	info, err := os.Stat(filepath.Join(store.options.Directory, store.segments[store.active.index].Path))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func sourceRecord(sequence uint64, payload string) Record {
	return Record{
		Kind: RecordObservation, Source: "collector", SourceInstance: "boot-1",
		HasSourceSequence: true, SourceSequence: sequence, Payload: []byte(payload),
	}
}
