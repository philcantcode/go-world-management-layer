package ledger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendSequenceGapDuplicateRotateAndReopen(t *testing.T) {
	directory := t.TempDir()
	store, _, err := Open(Options{Directory: directory, MaxSegmentBytes: 2048, MaxFramePayload: 4096})
	if err != nil {
		t.Fatal(err)
	}
	appendSource := func(sequence uint64, payload string) AppendResult {
		t.Helper()
		result, appendErr := store.Append(context.Background(), Record{
			Kind:              RecordObservation,
			Source:            "collector",
			SourceInstance:    "boot-1",
			HasSourceSequence: true,
			SourceSequence:    sequence,
			Payload:           []byte(payload),
		})
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		return result
	}

	if got := appendSource(1, "one"); len(got.Records) != 1 || got.Records[0].Cursor != 1 {
		t.Fatalf("first append = %#v", got)
	}
	jump := appendSource(3, "three")
	if len(jump.Records) != 2 || jump.Records[0].Kind != RecordGap || jump.Records[0].Gap.FromSequence != 2 || jump.Records[1].Cursor != 3 {
		t.Fatalf("sequence jump = %#v", jump)
	}
	duplicate := appendSource(3, "different-three")
	if !duplicate.Duplicate || len(duplicate.Records) != 1 || duplicate.Records[0].Kind != RecordDuplicate || !duplicate.Records[0].Duplicate.ConflictingPayload {
		t.Fatalf("duplicate = %#v", duplicate)
	}
	if _, err = store.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, report, err := Open(Options{Directory: directory, MaxSegmentBytes: 2048, MaxFramePayload: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(report.Repairs) != 0 || reopened.Head() != 4 {
		t.Fatalf("reopen report = %#v, head = %d", report, reopened.Head())
	}
	records, err := reopened.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("read %d records, want 4", len(records))
	}
	result, err := reopened.Append(context.Background(), Record{Kind: RecordObservation, Source: "collector", SourceInstance: "boot-1", HasSourceSequence: true, SourceSequence: 4})
	if err != nil || result.Records[0].Cursor != 5 {
		t.Fatalf("post-recovery append = %#v, %v", result, err)
	}
}

func TestRecoveryTruncatesOnlyIncompleteOpenTail(t *testing.T) {
	directory := t.TempDir()
	store, _, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), Record{Kind: RecordControl, Payload: []byte("accepted")}); err != nil {
		t.Fatal(err)
	}
	path := crashOpenSegment(t, store)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte{'G', 'W', 'F'}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	recovered, report, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if len(report.Repairs) != 1 || report.Repairs[0].RemovedBytes != 3 || recovered.Head() != 1 {
		t.Fatalf("report = %#v, head = %d", report, recovered.Head())
	}
}

func TestRecoveryRejectsCompletedFrameCorruptionWithoutTruncating(t *testing.T) {
	directory := t.TempDir()
	store, _, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), Record{Kind: RecordControl, Payload: []byte("accepted")}); err != nil {
		t.Fatal(err)
	}
	path := crashOpenSegment(t, store)
	before, _ := os.Stat(path)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, before.Size()-1); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	_, _, err = Open(Options{Directory: directory})
	if !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("error = %v, want ErrCorruptSegment", err)
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatalf("corrupt segment was truncated from %d to %d", before.Size(), after.Size())
	}
}

func TestSlowSubscriptionGetsExplicitGapAndDoesNotBlockAppend(t *testing.T) {
	store, _, err := Open(Options{Directory: t.TempDir(), SubscriberBuffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	subscription, err := store.Subscribe(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	for index := 0; index < 32; index++ {
		if _, err = store.Append(context.Background(), Record{Kind: RecordMetric, Payload: []byte{byte(index)}}); err != nil {
			t.Fatal(err)
		}
	}
	delivery, err := subscription.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Gap == nil || delivery.Gap.Cause != GapSubscriberOverflow || delivery.Gap.FromCursor != 1 || delivery.Gap.ThroughCursor != 32 {
		t.Fatalf("delivery = %#v", delivery)
	}
	if _, err = store.Append(context.Background(), Record{Kind: RecordMetric}); err != nil {
		t.Fatal(err)
	}
	delivery, err = subscription.Next(context.Background())
	if err != nil || delivery.Record == nil || delivery.Record.Cursor != 33 {
		t.Fatalf("post-gap delivery = %#v, %v", delivery, err)
	}
}

func crashOpenSegment(t *testing.T, store *Ledger) string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	path := filepath.Join(store.options.Directory, store.segments[store.active.index].Path)
	if err := store.active.file.Close(); err != nil {
		t.Fatal(err)
	}
	store.closed = true
	store.hub.close(ErrClosed)
	return path
}
