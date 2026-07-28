package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/framing"
)

const (
	defaultMaxSegmentBytes  = int64(64 << 20)
	defaultMaxFramePayload  = uint32(8 << 20)
	defaultSubscriberBuffer = 128
	defaultSourceSequences  = 4096
)

// Options configures bounded segment and subscription resources.
type Options struct {
	Directory        string
	MaxSegmentBytes  int64
	MaxFramePayload  uint32
	SubscriberBuffer int
	// MaxSourceSequences bounds the in-memory duplicate window for each
	// source instance. Older occurrences remain discoverable from segments.
	MaxSourceSequences int
	hooks              *ledgerHooks
}

type ledgerHooks struct {
	write  func(*os.File, []byte) (int64, error)
	sync   func(*os.File, string) error
	index  func(string, []SegmentMetadata) error
	rename func(string, string) error
}

type activeSegment struct {
	file  *os.File
	index int
}

type sourceKey struct {
	source   string
	instance string
}

type sourceOccurrence struct {
	cursor      Cursor
	fingerprint [sha256.Size]byte
}

type sourceState struct {
	hasMaximum bool
	maximum    uint64
	seen       map[uint64]sourceOccurrence
	order      []uint64
	next       int
}

// Ledger owns segment persistence and live subscriptions. Append operations are
// serialized so durable cursor order and fan-out order are identical.
type Ledger struct {
	mu       sync.Mutex
	options  Options
	segments []SegmentMetadata
	active   *activeSegment
	head     Cursor
	lastHash [sha256.Size]byte
	sources  map[sourceKey]*sourceState
	repairs  []Repair
	hub      *fanoutHub
	hooks    *ledgerHooks
	indexErr error
	failed   error
	closed   bool
}

// Open reconstructs authoritative metadata from segment contents. It truncates
// only an incomplete tail of the sole open segment and reports that repair.
func Open(options Options) (*Ledger, OpenReport, error) {
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, OpenReport{}, err
	}
	if err = os.MkdirAll(normalized.Directory, 0o700); err != nil {
		return nil, OpenReport{}, err
	}
	ledger := &Ledger{
		options: normalized,
		sources: make(map[sourceKey]*sourceState),
		hub:     newFanoutHub(),
		hooks:   normalized.hooks,
	}
	paths, err := discoverSegmentPaths(normalized.Directory)
	if err != nil {
		return nil, OpenReport{}, err
	}

	expectedCursor := Cursor(1)
	var expectedHash [sha256.Size]byte
	for pathIndex, path := range paths {
		isOpen := strings.HasSuffix(path, ".open")
		if isOpen && pathIndex != len(paths)-1 {
			return nil, OpenReport{}, fmt.Errorf("%w: open segment is not last", ErrCorruptSegment)
		}
		scanned, scanErr := scanSegment(path, normalized.MaxFramePayload, expectedCursor, expectedHash)
		if scanErr != nil {
			return nil, OpenReport{}, scanErr
		}
		if scanned.incompleteTail {
			if !isOpen {
				return nil, OpenReport{}, corruption(path, scanned.lastGoodOffset, framing.ErrIncompleteFrame)
			}
			removed := scanned.fileSize - scanned.lastGoodOffset
			if truncateErr := truncateAndSync(path, scanned.lastGoodOffset); truncateErr != nil {
				return nil, OpenReport{}, truncateErr
			}
			repair := Repair{
				Segment:      filepath.Base(path),
				Offset:       scanned.lastGoodOffset,
				RemovedBytes: removed,
				Cause:        GapSegmentRepair,
			}
			ledger.repairs = append(ledger.repairs, repair)
			scanned.metadata.ByteSize = scanned.lastGoodOffset
		}
		ledger.segments = append(ledger.segments, scanned.metadata)
		for _, record := range scanned.records {
			ledger.indexSourceRecord(record)
		}
		if scanned.metadata.FrameCount > 0 {
			expectedCursor = scanned.metadata.LastCursor + 1
			expectedHash = scanned.metadata.LastHash
			ledger.head = scanned.metadata.LastCursor
			ledger.lastHash = scanned.metadata.LastHash
		}
		if isOpen {
			file, openErr := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
			if openErr != nil {
				return nil, OpenReport{}, openErr
			}
			ledger.active = &activeSegment{file: file, index: len(ledger.segments) - 1}
		}
	}
	if err = ledger.flushIndexLocked(); err != nil {
		if ledger.active != nil {
			_ = ledger.active.file.Close()
		}
		return nil, OpenReport{}, err
	}
	report := OpenReport{Repairs: append([]Repair(nil), ledger.repairs...), Segments: ledger.Segments()}
	return ledger, report, nil
}

func normalizeOptions(options Options) (Options, error) {
	if options.Directory == "" {
		return Options{}, fmt.Errorf("ledger directory is required")
	}
	if options.MaxSegmentBytes == 0 {
		options.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if options.MaxFramePayload == 0 {
		options.MaxFramePayload = defaultMaxFramePayload
	}
	if options.SubscriberBuffer == 0 {
		options.SubscriberBuffer = defaultSubscriberBuffer
	}
	if options.MaxSourceSequences == 0 {
		options.MaxSourceSequences = defaultSourceSequences
	}
	if options.MaxSegmentBytes <= 0 || options.MaxFramePayload == 0 || options.SubscriberBuffer <= 0 || options.MaxSourceSequences <= 0 {
		return Options{}, fmt.Errorf("ledger bounds must be positive")
	}
	abs, err := filepath.Abs(options.Directory)
	if err != nil {
		return Options{}, err
	}
	options.Directory = abs
	return options, nil
}

// Append validates and durably appends a record. Source jumps create a durable
// gap before the record. Repeated source sequences create a duplicate record and
// do not re-apply the payload.
func (l *Ledger) Append(ctx context.Context, input Record) (AppendResult, error) {
	if err := input.Validate(); err != nil {
		return AppendResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return AppendResult{}, ErrClosed
	}
	if l.failed != nil {
		return AppendResult{}, l.failed
	}
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}

	records := make([]Record, 0, 2)
	duplicate := false
	if input.HasSourceSequence {
		state := l.sourceState(input.Source, input.SourceInstance)
		occurrence, found := state.seen[input.SourceSequence]
		if !found && state.hasMaximum && input.SourceSequence <= state.maximum {
			var lookupErr error
			occurrence, found, lookupErr = l.findSourceOccurrenceLocked(ctx, input.Source, input.SourceInstance, input.SourceSequence)
			if lookupErr != nil {
				return AppendResult{}, lookupErr
			}
		}
		if found {
			records = append(records, makeDuplicateRecord(input, occurrence))
			duplicate = true
		} else if state.hasMaximum && state.maximum < math.MaxUint64 && input.SourceSequence > state.maximum+1 {
			records = append(records, makeSequenceGapRecord(input, state.maximum+1, input.SourceSequence-1))
		}
	}
	if !duplicate {
		records = append(records, input)
	}
	stored, err := l.appendBatchLocked(records)
	if err != nil {
		return AppendResult{}, err
	}
	return AppendResult{Records: stored, Duplicate: duplicate}, nil
}

// AppendGap records an explicit collector/restart/compaction/loss gap.
func (l *Ledger) AppendGap(ctx context.Context, envelope Record, gap Gap) (Record, error) {
	envelope.Kind = RecordGap
	envelope.Cursor = 0
	envelope.ChainHash = [sha256.Size]byte{}
	envelope.HasSourceSequence = false
	envelope.SourceSequence = 0
	envelope.Payload = nil
	envelope.Duplicate = nil
	envelope.Gap = &gap
	result, err := l.Append(ctx, envelope)
	if err != nil {
		return Record{}, err
	}
	return result.Records[len(result.Records)-1], nil
}

func (l *Ledger) appendBatchLocked(records []Record) ([]Record, error) {
	if len(records) == 0 {
		return nil, nil
	}
	stored := make([]Record, len(records))
	var encoded bytes.Buffer
	previousHash := l.lastHash
	for index, record := range records {
		record.Cursor = l.head + Cursor(index) + 1
		wire, chainHash, err := marshalWireRecord(record, previousHash)
		if err != nil {
			return nil, err
		}
		frame, err := framing.Marshal(framing.Frame{Version: segmentFrameVersion, Payload: wire}, l.options.MaxFramePayload)
		if err != nil {
			return nil, err
		}
		if int64(encoded.Len()+len(frame)) > l.options.MaxSegmentBytes {
			return nil, fmt.Errorf("%w: append transaction is %d bytes, segment maximum is %d", ErrRecordTooLarge, encoded.Len()+len(frame), l.options.MaxSegmentBytes)
		}
		_, _ = encoded.Write(frame)
		record.ChainHash = chainHash
		stored[index] = record
		previousHash = chainHash
	}
	if l.active != nil {
		metadata := l.segments[l.active.index]
		if metadata.FrameCount > 0 && metadata.ByteSize+int64(encoded.Len()) > l.options.MaxSegmentBytes {
			if err := l.finalizeActiveLocked(); err != nil {
				return nil, err
			}
		}
	}
	if err := l.ensureActiveLocked(stored[0].Cursor); err != nil {
		return nil, err
	}

	metadata := &l.segments[l.active.index]
	offset := metadata.ByteSize
	written, err := l.writeActive(encoded.Bytes())
	if err == nil && written != int64(encoded.Len()) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return nil, l.failAfterRollbackLocked(offset, err)
	}
	if err := l.syncActive("append"); err != nil {
		return nil, l.failAfterRollbackLocked(offset, err)
	}

	for _, record := range stored {
		if metadata.FrameCount == 0 {
			metadata.FirstCursor = record.Cursor
			metadata.FirstHash = record.ChainHash
		}
		metadata.LastCursor = record.Cursor
		metadata.LastHash = record.ChainHash
		metadata.FrameCount++
		l.indexSourceRecord(record)
	}
	metadata.ByteSize += written
	l.head = stored[len(stored)-1].Cursor
	l.lastHash = stored[len(stored)-1].ChainHash
	// The segment is authoritative once its fsync succeeds. The index is a
	// rebuildable sidecar, so a transient index failure must not invite the
	// caller to append the same durable observation again.
	_ = l.flushIndexLocked()
	for _, record := range stored {
		l.hub.publish(record)
	}
	result := make([]Record, len(stored))
	for index, record := range stored {
		result[index] = cloneRecord(record)
	}
	return result, nil
}

func (l *Ledger) ensureActiveLocked(first Cursor) error {
	if l.active != nil {
		return nil
	}
	name := segmentName(first, false)
	path := filepath.Join(l.options.Directory, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if err := l.syncDirectory("segment-create"); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("persist new segment directory entry: %w", err)
	}
	metadata := SegmentMetadata{Path: name, PreviousHash: l.lastHash}
	l.segments = append(l.segments, metadata)
	l.active = &activeSegment{file: file, index: len(l.segments) - 1}
	return nil
}

func (l *Ledger) writeActive(value []byte) (int64, error) {
	if l.hooks != nil && l.hooks.write != nil {
		return l.hooks.write(l.active.file, value)
	}
	return writeAll(l.active.file, value)
}

func (l *Ledger) syncActive(stage string) error {
	return l.syncFile(l.active.file, stage)
}

func (l *Ledger) syncFile(file *os.File, stage string) error {
	if l.hooks != nil && l.hooks.sync != nil {
		return l.hooks.sync(file, stage)
	}
	return file.Sync()
}

func (l *Ledger) syncDirectory(stage string) error {
	// Windows does not expose directory fsync through os.File.Sync. Segment
	// recovery still validates contents there, but the stronger Unix directory
	// entry durability guarantee is not available through this implementation.
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(l.options.Directory)
	if err != nil {
		return err
	}
	defer directory.Close()
	return l.syncFile(directory, stage)
}

func (l *Ledger) failAfterRollbackLocked(offset int64, appendErr error) error {
	rollbackErr := l.rollbackActiveLocked(offset)
	if rollbackErr == nil {
		return appendErr
	}
	l.failed = errors.Join(ErrRecoveryRequired, appendErr, fmt.Errorf("durable append rollback: %w", rollbackErr))
	return l.failed
}

func (l *Ledger) rollbackActiveLocked(offset int64) error {
	path := filepath.Join(l.options.Directory, l.segments[l.active.index].Path)
	closeErr := l.active.file.Close()
	file, openErr := os.OpenFile(path, os.O_RDWR, 0)
	if openErr != nil {
		return errors.Join(closeErr, openErr)
	}
	truncateErr := file.Truncate(offset)
	var syncErr error
	if truncateErr == nil {
		syncErr = l.syncFile(file, "rollback")
	}
	fileCloseErr := file.Close()
	reopened, reopenErr := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if reopenErr == nil {
		l.active.file = reopened
	}
	return errors.Join(closeErr, truncateErr, syncErr, fileCloseErr, reopenErr)
}

func (l *Ledger) flushIndexLocked() error {
	var err error
	if l.hooks != nil && l.hooks.index != nil {
		err = l.hooks.index(l.options.Directory, l.segments)
	} else {
		err = writeSegmentIndex(l.options.Directory, l.segments)
	}
	if err == nil {
		err = l.syncDirectory("index")
	}
	l.indexErr = err
	return err
}

// Rotate syncs and finalizes the current segment. The next append lazily creates
// a new open segment.
func (l *Ledger) Rotate() (SegmentMetadata, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return SegmentMetadata{}, ErrClosed
	}
	if l.failed != nil {
		return SegmentMetadata{}, l.failed
	}
	if l.active == nil {
		if l.indexErr != nil {
			return SegmentMetadata{}, l.flushIndexLocked()
		}
		return SegmentMetadata{}, nil
	}
	metadata := l.segments[l.active.index]
	if err := l.finalizeActiveLocked(); err != nil {
		return SegmentMetadata{}, err
	}
	return metadataWithFinalizedName(metadata), nil
}

func (l *Ledger) finalizeActiveLocked() error {
	if l.active == nil {
		return nil
	}
	metadata := &l.segments[l.active.index]
	if err := l.syncActive("finalize"); err != nil {
		return err
	}
	if err := l.active.file.Close(); err != nil {
		l.failed = errors.Join(ErrRecoveryRequired, fmt.Errorf("close active segment before finalization: %w", err))
		return l.failed
	}
	oldPath := filepath.Join(l.options.Directory, metadata.Path)
	newName := strings.TrimSuffix(metadata.Path, ".open") + ".seg"
	newPath := filepath.Join(l.options.Directory, newName)
	rename := os.Rename
	if l.hooks != nil && l.hooks.rename != nil {
		rename = l.hooks.rename
	}
	if err := rename(oldPath, newPath); err != nil {
		reopened, reopenErr := os.OpenFile(oldPath, os.O_RDWR|os.O_APPEND, 0)
		if reopenErr == nil {
			l.active.file = reopened
			return err
		}
		l.failed = errors.Join(ErrRecoveryRequired, fmt.Errorf("finalize active segment: %w", err), fmt.Errorf("reopen active segment: %w", reopenErr))
		return l.failed
	}
	metadata.Path = newName
	metadata.Finalized = true
	l.active = nil
	_ = l.flushIndexLocked()
	return nil
}

func metadataWithFinalizedName(metadata SegmentMetadata) SegmentMetadata {
	metadata.Path = strings.TrimSuffix(metadata.Path, ".open") + ".seg"
	metadata.Finalized = true
	return metadata
}

// Head returns the latest durable cursor.
func (l *Ledger) Head() Cursor {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.head
}

// Segments returns a defensive copy of reconstructed segment metadata.
func (l *Ledger) Segments() []SegmentMetadata {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]SegmentMetadata(nil), l.segments...)
}

// Repairs returns safe recovery actions performed by Open.
func (l *Ledger) Repairs() []Repair {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Repair(nil), l.repairs...)
}

// ReadAfter validates and returns durable records after a cursor. A non-positive
// limit means no caller-imposed record limit.
func (l *Ledger) ReadAfter(after Cursor, limit int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if after > l.head {
		return nil, ErrCursorOutOfRange
	}
	if l.failed != nil {
		return nil, l.failed
	}
	return l.readAfterLocked(after, l.head, limit)
}

func (l *Ledger) readAfterLocked(after, through Cursor, limit int) ([]Record, error) {
	records := make([]Record, 0)
	expectedCursor := Cursor(1)
	var expectedHash [sha256.Size]byte
	for _, metadata := range l.segments {
		path := filepath.Join(l.options.Directory, metadata.Path)
		scanned, err := scanSegment(path, l.options.MaxFramePayload, expectedCursor, expectedHash)
		if err != nil {
			return nil, err
		}
		if scanned.incompleteTail {
			return nil, corruption(path, scanned.lastGoodOffset, framing.ErrIncompleteFrame)
		}
		for _, record := range scanned.records {
			if record.Cursor > after && record.Cursor <= through {
				records = append(records, cloneRecord(record))
				if limit > 0 && len(records) >= limit {
					return records, nil
				}
			}
		}
		if scanned.metadata.FrameCount > 0 {
			expectedCursor = scanned.metadata.LastCursor + 1
			expectedHash = scanned.metadata.LastHash
		}
	}
	return records, nil
}

// Close finalizes the open segment and closes every subscription.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if l.failed != nil {
		var closeErr error
		if l.active != nil {
			closeErr = l.active.file.Close()
			l.active = nil
		}
		l.closed = true
		l.hub.close(l.failed)
		return errors.Join(l.failed, closeErr)
	}
	if err := l.finalizeActiveLocked(); err != nil {
		return err
	}
	if l.indexErr != nil {
		if err := l.flushIndexLocked(); err != nil {
			return fmt.Errorf("persist segment index: %w", err)
		}
	}
	l.closed = true
	l.hub.close(ErrClosed)
	return nil
}

func (l *Ledger) sourceState(source, instance string) *sourceState {
	key := sourceKey{source: source, instance: instance}
	state := l.sources[key]
	if state == nil {
		state = &sourceState{seen: make(map[uint64]sourceOccurrence)}
		l.sources[key] = state
	}
	return state
}

func (l *Ledger) indexSourceRecord(record Record) {
	if !record.HasSourceSequence || record.Kind == RecordDuplicate || record.Kind == RecordGap {
		return
	}
	state := l.sourceState(record.Source, record.SourceInstance)
	if _, exists := state.seen[record.SourceSequence]; !exists {
		if len(state.order) < l.options.MaxSourceSequences {
			state.order = append(state.order, record.SourceSequence)
		} else {
			evicted := state.order[state.next]
			delete(state.seen, evicted)
			state.order[state.next] = record.SourceSequence
			state.next = (state.next + 1) % len(state.order)
		}
	}
	state.seen[record.SourceSequence] = sourceOccurrence{
		cursor:      record.Cursor,
		fingerprint: sha256.Sum256(record.Payload),
	}
	if !state.hasMaximum || record.SourceSequence > state.maximum {
		state.hasMaximum = true
		state.maximum = record.SourceSequence
	}
}

func (l *Ledger) findSourceOccurrenceLocked(ctx context.Context, source, instance string, sequence uint64) (sourceOccurrence, bool, error) {
	expectedCursor := Cursor(1)
	var expectedHash [sha256.Size]byte
	for _, metadata := range l.segments {
		if err := ctx.Err(); err != nil {
			return sourceOccurrence{}, false, err
		}
		path := filepath.Join(l.options.Directory, metadata.Path)
		scanned, err := scanSegment(path, l.options.MaxFramePayload, expectedCursor, expectedHash)
		if err != nil {
			return sourceOccurrence{}, false, err
		}
		if scanned.incompleteTail {
			return sourceOccurrence{}, false, corruption(path, scanned.lastGoodOffset, framing.ErrIncompleteFrame)
		}
		for _, record := range scanned.records {
			if record.HasSourceSequence && record.Kind != RecordDuplicate && record.Kind != RecordGap && record.Source == source && record.SourceInstance == instance && record.SourceSequence == sequence {
				return sourceOccurrence{cursor: record.Cursor, fingerprint: sha256.Sum256(record.Payload)}, true, nil
			}
		}
		if scanned.metadata.FrameCount > 0 {
			expectedCursor = scanned.metadata.LastCursor + 1
			expectedHash = scanned.metadata.LastHash
		}
	}
	return sourceOccurrence{}, false, nil
}

func makeDuplicateRecord(input Record, occurrence sourceOccurrence) Record {
	sourceSequence := input.SourceSequence
	fingerprint := sha256.Sum256(input.Payload)
	input.Kind = RecordDuplicate
	input.HasSourceSequence = false
	input.SourceSequence = 0
	input.Payload = nil
	input.Gap = nil
	input.Duplicate = &Duplicate{
		Source:             input.Source,
		SourceInstance:     input.SourceInstance,
		SourceSequence:     sourceSequence,
		OriginalCursor:     occurrence.cursor,
		ConflictingPayload: occurrence.fingerprint != fingerprint,
	}
	return input
}

func makeSequenceGapRecord(input Record, from, through uint64) Record {
	return Record{
		Kind:                   RecordGap,
		Identity:               input.Identity,
		SignalFamily:           input.SignalFamily,
		SubjectID:              input.SubjectID,
		Source:                 input.Source,
		SourceInstance:         input.SourceInstance,
		ObservedWallUnixNano:   input.ObservedWallUnixNano,
		ObservedMonotonicNanos: input.ObservedMonotonicNanos,
		ClockDomain:            input.ClockDomain,
		ClockSyncEpoch:         input.ClockSyncEpoch,
		Collector:              input.Collector,
		PolicyDigest:           input.PolicyDigest,
		CapabilityDigest:       input.CapabilityDigest,
		Origin:                 input.Origin,
		OriginEvidence:         input.OriginEvidence,
		Gap: &Gap{
			Cause:           GapSourceSequence,
			Source:          input.Source,
			SourceInstance:  input.SourceInstance,
			FromSequence:    from,
			ThroughSequence: through,
			Detail:          "source-local sequence discontinuity",
		},
	}
}

func truncateAndSync(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = file.Truncate(size); err != nil {
		return err
	}
	return file.Sync()
}

func writeAll(writer io.Writer, value []byte) (int64, error) {
	written := 0
	for written < len(value) {
		count, err := writer.Write(value[written:])
		written += count
		if err != nil {
			return int64(written), err
		}
		if count == 0 {
			return int64(written), io.ErrShortWrite
		}
	}
	return int64(written), nil
}
