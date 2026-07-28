// Package framing implements a small, bounded, versioned binary frame format.
//
// A frame is encoded as:
//
//	magic[4] | version(u16) | flags(u16) | payload_length(u32) | payload | crc32c(u32)
//
// The CRC covers the complete header and payload. Readers distinguish a clean
// end of stream from an incomplete trailing frame so callers can safely repair
// append-only files without treating completed-frame corruption as truncation.
package framing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	HeaderSize  = 12
	TrailerSize = 4
)

var (
	magic = [4]byte{'G', 'W', 'F', 'R'}

	ErrIncompleteFrame = errors.New("incomplete frame")
	ErrInvalidMagic    = errors.New("invalid frame magic")
	ErrChecksum        = errors.New("frame checksum mismatch")
	ErrFrameTooLarge   = errors.New("frame payload exceeds configured maximum")
	ErrInvalidVersion  = errors.New("frame version must be non-zero")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Frame is the payload and routing metadata carried by one binary frame.
type Frame struct {
	Version uint16
	Flags   uint16
	Payload []byte
}

// Decoder reads frames while enforcing a payload allocation bound.
type Decoder struct {
	r          io.Reader
	maxPayload uint32
}

// NewDecoder constructs a decoder. maxPayload must be greater than zero.
func NewDecoder(r io.Reader, maxPayload uint32) *Decoder {
	return &Decoder{r: r, maxPayload: maxPayload}
}

// Read returns one frame and the number of bytes consumed from the underlying
// reader. A clean boundary returns io.EOF and zero bytes. Any partial header,
// payload, or trailer returns ErrIncompleteFrame.
func (d *Decoder) Read() (Frame, int64, error) {
	if d.maxPayload == 0 {
		return Frame{}, 0, fmt.Errorf("%w: maximum is zero", ErrFrameTooLarge)
	}

	header := make([]byte, HeaderSize)
	n, err := io.ReadFull(d.r, header)
	consumed := int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return Frame{}, 0, io.EOF
		}
		return Frame{}, consumed, fmt.Errorf("%w: header: %v", ErrIncompleteFrame, err)
	}

	if string(header[:len(magic)]) != string(magic[:]) {
		return Frame{}, consumed, ErrInvalidMagic
	}
	version := binary.BigEndian.Uint16(header[4:6])
	if version == 0 {
		return Frame{}, consumed, ErrInvalidVersion
	}
	flags := binary.BigEndian.Uint16(header[6:8])
	payloadLength := binary.BigEndian.Uint32(header[8:12])
	if payloadLength > d.maxPayload {
		return Frame{}, consumed, fmt.Errorf("%w: got %d, maximum %d", ErrFrameTooLarge, payloadLength, d.maxPayload)
	}

	payload := make([]byte, payloadLength)
	n, err = io.ReadFull(d.r, payload)
	consumed += int64(n)
	if err != nil {
		return Frame{}, consumed, fmt.Errorf("%w: payload: %v", ErrIncompleteFrame, err)
	}

	trailer := make([]byte, TrailerSize)
	n, err = io.ReadFull(d.r, trailer)
	consumed += int64(n)
	if err != nil {
		return Frame{}, consumed, fmt.Errorf("%w: checksum: %v", ErrIncompleteFrame, err)
	}

	want := binary.BigEndian.Uint32(trailer)
	checksum := crc32.New(crcTable)
	_, _ = checksum.Write(header)
	_, _ = checksum.Write(payload)
	if got := checksum.Sum32(); got != want {
		return Frame{}, consumed, fmt.Errorf("%w: got %08x, want %08x", ErrChecksum, got, want)
	}

	return Frame{Version: version, Flags: flags, Payload: payload}, consumed, nil
}

// Marshal encodes a frame and enforces maxPayload before allocating output.
func Marshal(frame Frame, maxPayload uint32) ([]byte, error) {
	if frame.Version == 0 {
		return nil, ErrInvalidVersion
	}
	if uint64(len(frame.Payload)) > uint64(maxPayload) {
		return nil, fmt.Errorf("%w: got %d, maximum %d", ErrFrameTooLarge, len(frame.Payload), maxPayload)
	}

	encoded := make([]byte, HeaderSize+len(frame.Payload)+TrailerSize)
	copy(encoded[:4], magic[:])
	binary.BigEndian.PutUint16(encoded[4:6], frame.Version)
	binary.BigEndian.PutUint16(encoded[6:8], frame.Flags)
	binary.BigEndian.PutUint32(encoded[8:12], uint32(len(frame.Payload)))
	copy(encoded[HeaderSize:], frame.Payload)
	checksum := crc32.Checksum(encoded[:HeaderSize+len(frame.Payload)], crcTable)
	binary.BigEndian.PutUint32(encoded[HeaderSize+len(frame.Payload):], checksum)
	return encoded, nil
}

// Write marshals and completely writes a frame. It never treats a short write
// as success.
func Write(w io.Writer, frame Frame, maxPayload uint32) (int64, error) {
	encoded, err := Marshal(frame, maxPayload)
	if err != nil {
		return 0, err
	}
	written := 0
	for written < len(encoded) {
		n, writeErr := w.Write(encoded[written:])
		written += n
		if writeErr != nil {
			return int64(written), writeErr
		}
		if n == 0 {
			return int64(written), io.ErrShortWrite
		}
	}
	return int64(written), nil
}
