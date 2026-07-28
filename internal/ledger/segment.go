package ledger

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/framing"
)

const indexVersion = 1

type segmentIndexFile struct {
	Version  int               `json:"version"`
	Segments []SegmentMetadata `json:"segments"`
}

type scannedSegment struct {
	metadata       SegmentMetadata
	records        []Record
	lastGoodOffset int64
	incompleteTail bool
	fileSize       int64
}

func segmentName(first Cursor, finalized bool) string {
	extension := ".open"
	if finalized {
		extension = ".seg"
	}
	return fmt.Sprintf("segment-%020d%s", first, extension)
}

func discoverSegmentPaths(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	openCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "segment-") {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".open") {
			openCount++
		} else if !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}
		paths = append(paths, filepath.Join(directory, entry.Name()))
	}
	if openCount > 1 {
		return nil, ErrMultipleOpenSegments
	}
	sort.Strings(paths)
	return paths, nil
}

func scanSegment(path string, maxFramePayload uint32, expectedCursor Cursor, expectedHash [sha256.Size]byte) (scannedSegment, error) {
	file, err := os.Open(path)
	if err != nil {
		return scannedSegment{}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return scannedSegment{}, err
	}

	result := scannedSegment{
		metadata: SegmentMetadata{
			Path:         filepath.Base(path),
			PreviousHash: expectedHash,
			Finalized:    strings.HasSuffix(path, ".seg"),
		},
		fileSize: stat.Size(),
	}
	decoder := framing.NewDecoder(file, maxFramePayload)
	currentHash := expectedHash
	offset := int64(0)
	for {
		frameOffset := offset
		frame, consumed, readErr := decoder.Read()
		offset += consumed
		if errors.Is(readErr, io.EOF) {
			result.lastGoodOffset = frameOffset
			break
		}
		if errors.Is(readErr, framing.ErrIncompleteFrame) {
			result.lastGoodOffset = frameOffset
			result.incompleteTail = true
			break
		}
		if readErr != nil {
			return scannedSegment{}, corruption(path, frameOffset, readErr)
		}
		if frame.Version != segmentFrameVersion {
			return scannedSegment{}, corruption(path, frameOffset, fmt.Errorf("%w: frame %d", ErrUnsupportedVersion, frame.Version))
		}
		record, previousHash, decodeErr := unmarshalWireRecord(frame.Payload)
		if decodeErr != nil {
			return scannedSegment{}, corruption(path, frameOffset, decodeErr)
		}
		if previousHash != currentHash {
			return scannedSegment{}, corruption(path, frameOffset, ErrHashChain)
		}
		if record.Cursor != expectedCursor {
			return scannedSegment{}, corruption(path, frameOffset, fmt.Errorf("%w: got %d, want %d", ErrCursorOrder, record.Cursor, expectedCursor))
		}

		if result.metadata.FrameCount == 0 {
			result.metadata.FirstCursor = record.Cursor
			result.metadata.FirstHash = record.ChainHash
		}
		result.metadata.LastCursor = record.Cursor
		result.metadata.LastHash = record.ChainHash
		result.metadata.FrameCount++
		result.records = append(result.records, record)
		result.lastGoodOffset = offset
		currentHash = record.ChainHash
		expectedCursor++
	}
	result.metadata.ByteSize = result.lastGoodOffset
	return result, nil
}

func corruption(path string, offset int64, err error) error {
	return &CorruptionError{Segment: filepath.Base(path), Offset: offset, Err: err}
}

func writeSegmentIndex(directory string, segments []SegmentMetadata) error {
	index := segmentIndexFile{Version: indexVersion, Segments: append([]SegmentMetadata(nil), segments...)}
	encoded, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(directory, ".segments-index-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if _, err = temporary.Write(encoded); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporaryPath, filepath.Join(directory, "segments.index.json")); err != nil {
		return err
	}
	committed = true
	return nil
}
