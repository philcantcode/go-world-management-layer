package framing

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	want := Frame{Version: 3, Flags: 7, Payload: []byte("payload")}
	encoded, err := Marshal(want, 1024)
	if err != nil {
		t.Fatal(err)
	}
	got, consumed, err := NewDecoder(bytes.NewReader(encoded), 1024).Read()
	if err != nil {
		t.Fatal(err)
	}
	if consumed != int64(len(encoded)) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
	}
	if got.Version != want.Version || got.Flags != want.Flags || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestIncompleteFrameAtEveryBoundary(t *testing.T) {
	encoded, err := Marshal(Frame{Version: 1, Payload: []byte("abcdef")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for length := 1; length < len(encoded); length++ {
		_, _, readErr := NewDecoder(bytes.NewReader(encoded[:length]), 1024).Read()
		if !errors.Is(readErr, ErrIncompleteFrame) {
			t.Fatalf("length %d: error = %v, want ErrIncompleteFrame", length, readErr)
		}
	}
	_, consumed, err := NewDecoder(bytes.NewReader(nil), 1024).Read()
	if !errors.Is(err, io.EOF) || consumed != 0 {
		t.Fatalf("empty read = (%d, %v), want (0, io.EOF)", consumed, err)
	}
}

func TestCompletedFrameCorruptionIsChecksumError(t *testing.T) {
	encoded, err := Marshal(Frame{Version: 1, Payload: []byte("abcdef")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	encoded[HeaderSize+2] ^= 0xff
	_, _, err = NewDecoder(bytes.NewReader(encoded), 1024).Read()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("error = %v, want ErrChecksum", err)
	}
}

func TestPayloadBoundIsCheckedBeforePayloadRead(t *testing.T) {
	encoded, err := Marshal(Frame{Version: 1, Payload: bytes.Repeat([]byte{'x'}, 16)}, 16)
	if err != nil {
		t.Fatal(err)
	}
	_, consumed, err := NewDecoder(bytes.NewReader(encoded), 8).Read()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("error = %v, want ErrFrameTooLarge", err)
	}
	if consumed != HeaderSize {
		t.Fatalf("consumed = %d, want header size %d", consumed, HeaderSize)
	}
}
