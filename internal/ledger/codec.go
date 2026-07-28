package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

type binaryEncoder struct {
	buffer bytes.Buffer
	err    error
}

func (e *binaryEncoder) uint8(value uint8) {
	if e.err == nil {
		e.err = e.buffer.WriteByte(value)
	}
}

func (e *binaryEncoder) uint16(value uint16)   { e.fixed(value) }
func (e *binaryEncoder) uint32(value uint32)   { e.fixed(value) }
func (e *binaryEncoder) uint64(value uint64)   { e.fixed(value) }
func (e *binaryEncoder) int64(value int64)     { e.uint64(uint64(value)) }
func (e *binaryEncoder) float64(value float64) { e.uint64(math.Float64bits(value)) }

func (e *binaryEncoder) fixed(value any) {
	if e.err == nil {
		e.err = binary.Write(&e.buffer, binary.BigEndian, value)
	}
}

func (e *binaryEncoder) boolean(value bool) {
	if value {
		e.uint8(1)
		return
	}
	e.uint8(0)
}

func (e *binaryEncoder) bytes(value []byte) {
	if uint64(len(value)) > math.MaxUint32 {
		e.err = fmt.Errorf("field is too large: %d", len(value))
		return
	}
	e.uint32(uint32(len(value)))
	if e.err == nil {
		_, e.err = e.buffer.Write(value)
	}
}

func (e *binaryEncoder) string(value string) { e.bytes([]byte(value)) }

func (e *binaryEncoder) hash(value [sha256.Size]byte) {
	if e.err == nil {
		_, e.err = e.buffer.Write(value[:])
	}
}

func (e *binaryEncoder) result() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.buffer.Bytes(), nil
}

type binaryDecoder struct {
	reader *bytes.Reader
	err    error
}

func newBinaryDecoder(value []byte) *binaryDecoder {
	return &binaryDecoder{reader: bytes.NewReader(value)}
}

func (d *binaryDecoder) fixed(target any) {
	if d.err == nil {
		d.err = binary.Read(d.reader, binary.BigEndian, target)
	}
}

func (d *binaryDecoder) uint8() uint8 {
	if d.err != nil {
		return 0
	}
	value, err := d.reader.ReadByte()
	d.err = err
	return value
}

func (d *binaryDecoder) uint16() (value uint16) { d.fixed(&value); return }
func (d *binaryDecoder) uint32() (value uint32) { d.fixed(&value); return }
func (d *binaryDecoder) uint64() (value uint64) { d.fixed(&value); return }
func (d *binaryDecoder) int64() int64           { return int64(d.uint64()) }
func (d *binaryDecoder) float64() float64       { return math.Float64frombits(d.uint64()) }

func (d *binaryDecoder) boolean() bool {
	value := d.uint8()
	if d.err == nil && value > 1 {
		d.err = fmt.Errorf("invalid boolean %d", value)
	}
	return value == 1
}

func (d *binaryDecoder) bytes() []byte {
	length := d.uint32()
	if d.err != nil {
		return nil
	}
	if uint64(length) > uint64(d.reader.Len()) {
		d.err = fmt.Errorf("field length %d exceeds remaining payload %d", length, d.reader.Len())
		return nil
	}
	value := make([]byte, length)
	_, d.err = d.reader.Read(value)
	return value
}

func (d *binaryDecoder) string() string { return string(d.bytes()) }

func (d *binaryDecoder) hash() (value [sha256.Size]byte) {
	if d.err == nil {
		_, d.err = d.reader.Read(value[:])
	}
	return
}

func (d *binaryDecoder) done() error {
	if d.err != nil {
		return d.err
	}
	if d.reader.Len() != 0 {
		return fmt.Errorf("%d trailing bytes", d.reader.Len())
	}
	return nil
}

func marshalRecord(record Record) ([]byte, error) {
	var encoder binaryEncoder
	encoder.uint8(uint8(record.Kind))
	encoder.uint64(uint64(record.Cursor))
	encoder.string(record.EventID)
	encodeIdentity(&encoder, record.Identity)
	encoder.string(record.SignalFamily)
	encoder.string(record.SubjectID)
	encoder.string(record.Source)
	encoder.string(record.SourceInstance)
	encoder.boolean(record.HasSourceSequence)
	encoder.uint64(record.SourceSequence)
	encoder.int64(record.ObservedWallUnixNano)
	encoder.int64(record.ObservedMonotonicNanos)
	encoder.string(record.ClockDomain)
	encoder.uint64(record.ClockSyncEpoch)
	encoder.string(record.Collector.ID)
	encoder.uint8(uint8(record.Collector.Placement))
	encoder.string(record.Collector.Coverage)
	encoder.int64(record.Process.ID)
	encoder.int64(record.Process.StartTimeUnixNano)
	encoder.string(record.PolicyDigest)
	encoder.string(record.CapabilityDigest)
	encoder.uint8(uint8(record.Origin))
	encoder.string(record.OriginEvidence)
	encodeCausalContext(&encoder, record.Causal)
	encoder.bytes(record.Payload)
	encodeGap(&encoder, record.Gap)
	encodeDuplicate(&encoder, record.Duplicate)
	return encoder.result()
}

func encodeIdentity(encoder *binaryEncoder, identity Identity) {
	encoder.string(identity.ResearchSessionID)
	encoder.string(identity.LeaseID)
	encoder.string(identity.AgentWorkspaceID)
	encoder.uint64(identity.AgentGeneration)
	encoder.string(identity.ExecID)
	encoder.string(identity.TargetID)
	encoder.uint64(identity.TargetGeneration)
	encoder.string(identity.TargetRunID)
	encoder.string(identity.TargetOperationID)
}

func encodeCausalContext(encoder *binaryEncoder, causal CausalContext) {
	encoder.string(causal.CausationID)
	encoder.string(causal.CorrelationID)
	encoder.string(causal.CorrelationMethod)
	encoder.float64(causal.Confidence)
}

func encodeGap(encoder *binaryEncoder, gap *Gap) {
	encoder.boolean(gap != nil)
	if gap == nil {
		return
	}
	encoder.uint8(uint8(gap.Cause))
	encoder.string(gap.Source)
	encoder.string(gap.SourceInstance)
	encoder.uint64(gap.FromSequence)
	encoder.uint64(gap.ThroughSequence)
	encoder.uint64(uint64(gap.FromCursor))
	encoder.uint64(uint64(gap.ThroughCursor))
	encoder.string(gap.Detail)
}

func encodeDuplicate(encoder *binaryEncoder, duplicate *Duplicate) {
	encoder.boolean(duplicate != nil)
	if duplicate == nil {
		return
	}
	encoder.string(duplicate.Source)
	encoder.string(duplicate.SourceInstance)
	encoder.uint64(duplicate.SourceSequence)
	encoder.uint64(uint64(duplicate.OriginalCursor))
	encoder.boolean(duplicate.ConflictingPayload)
}

func unmarshalRecord(encoded []byte) (Record, error) {
	decoder := newBinaryDecoder(encoded)
	record := Record{
		Kind:                   RecordKind(decoder.uint8()),
		Cursor:                 Cursor(decoder.uint64()),
		EventID:                decoder.string(),
		Identity:               decodeIdentity(decoder),
		SignalFamily:           decoder.string(),
		SubjectID:              decoder.string(),
		Source:                 decoder.string(),
		SourceInstance:         decoder.string(),
		HasSourceSequence:      decoder.boolean(),
		SourceSequence:         decoder.uint64(),
		ObservedWallUnixNano:   decoder.int64(),
		ObservedMonotonicNanos: decoder.int64(),
		ClockDomain:            decoder.string(),
		ClockSyncEpoch:         decoder.uint64(),
	}
	record.Collector = CollectorContext{
		ID:        decoder.string(),
		Placement: CollectorPlacement(decoder.uint8()),
		Coverage:  decoder.string(),
	}
	record.Process = ProcessIdentity{ID: decoder.int64(), StartTimeUnixNano: decoder.int64()}
	record.PolicyDigest = decoder.string()
	record.CapabilityDigest = decoder.string()
	record.Origin = OriginClass(decoder.uint8())
	record.OriginEvidence = decoder.string()
	record.Causal = decodeCausalContext(decoder)
	record.Payload = decoder.bytes()
	record.Gap = decodeGap(decoder)
	record.Duplicate = decodeDuplicate(decoder)
	if err := decoder.done(); err != nil {
		return Record{}, err
	}
	if record.Cursor == 0 {
		return Record{}, fmt.Errorf("%w: zero persisted cursor", ErrInvalidRecord)
	}
	if err := validateRecordShape(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func decodeIdentity(decoder *binaryDecoder) Identity {
	return Identity{
		ResearchSessionID: decoder.string(),
		LeaseID:           decoder.string(),
		AgentWorkspaceID:  decoder.string(),
		AgentGeneration:   decoder.uint64(),
		ExecID:            decoder.string(),
		TargetID:          decoder.string(),
		TargetGeneration:  decoder.uint64(),
		TargetRunID:       decoder.string(),
		TargetOperationID: decoder.string(),
	}
}

func decodeCausalContext(decoder *binaryDecoder) CausalContext {
	return CausalContext{
		CausationID:       decoder.string(),
		CorrelationID:     decoder.string(),
		CorrelationMethod: decoder.string(),
		Confidence:        decoder.float64(),
	}
}

func decodeGap(decoder *binaryDecoder) *Gap {
	if !decoder.boolean() {
		return nil
	}
	return &Gap{
		Cause:           GapCause(decoder.uint8()),
		Source:          decoder.string(),
		SourceInstance:  decoder.string(),
		FromSequence:    decoder.uint64(),
		ThroughSequence: decoder.uint64(),
		FromCursor:      Cursor(decoder.uint64()),
		ThroughCursor:   Cursor(decoder.uint64()),
		Detail:          decoder.string(),
	}
}

func decodeDuplicate(decoder *binaryDecoder) *Duplicate {
	if !decoder.boolean() {
		return nil
	}
	return &Duplicate{
		Source:             decoder.string(),
		SourceInstance:     decoder.string(),
		SourceSequence:     decoder.uint64(),
		OriginalCursor:     Cursor(decoder.uint64()),
		ConflictingPayload: decoder.boolean(),
	}
}

func marshalWireRecord(record Record, previousHash [sha256.Size]byte) ([]byte, [sha256.Size]byte, error) {
	recordBytes, err := marshalRecord(record)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write(previousHash[:])
	_, _ = hasher.Write(recordBytes)
	var chainHash [sha256.Size]byte
	copy(chainHash[:], hasher.Sum(nil))

	var encoder binaryEncoder
	encoder.uint16(recordEncodingVersion)
	encoder.hash(previousHash)
	encoder.hash(chainHash)
	encoder.bytes(recordBytes)
	wire, err := encoder.result()
	return wire, chainHash, err
}

func unmarshalWireRecord(encoded []byte) (Record, [sha256.Size]byte, error) {
	decoder := newBinaryDecoder(encoded)
	version := decoder.uint16()
	previousHash := decoder.hash()
	storedHash := decoder.hash()
	recordBytes := decoder.bytes()
	if err := decoder.done(); err != nil {
		return Record{}, [sha256.Size]byte{}, err
	}
	if version != recordEncodingVersion {
		return Record{}, [sha256.Size]byte{}, fmt.Errorf("%w: record encoding %d", ErrUnsupportedVersion, version)
	}

	hasher := sha256.New()
	_, _ = hasher.Write(previousHash[:])
	_, _ = hasher.Write(recordBytes)
	var computedHash [sha256.Size]byte
	copy(computedHash[:], hasher.Sum(nil))
	if computedHash != storedHash {
		return Record{}, [sha256.Size]byte{}, ErrHashChain
	}
	record, err := unmarshalRecord(recordBytes)
	if err != nil {
		return Record{}, [sha256.Size]byte{}, err
	}
	record.ChainHash = storedHash
	return record, previousHash, nil
}
