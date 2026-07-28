package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

// MaximumIdempotencyKeyBytes is the byte bound shared by logical mutations,
// physical plans, and persisted replay identities.
const MaximumIdempotencyKeyBytes = 1024

const (
	derivedIdempotencyDigestMarker = "//sha256:"
	derivedIdempotencyDomain       = "world-management-layer.idempotency-child.v1\x00"
)

// IsCanonicalIdempotencyKey reports whether value can safely cross every
// idempotency boundary without normalization or replay-time rejection.
func IsCanonicalIdempotencyKey(value string) bool {
	return value != "" && len(value) <= MaximumIdempotencyKeyBytes &&
		strings.TrimSpace(value) == value && utf8.ValidString(value)
}

// DeriveIdempotencyKey creates a deterministic child identity without ever
// exceeding the shared idempotency-key bound. An unambiguous pair retains the
// exact parent + "/" + suffix form. Pairs containing a slash, and pairs whose
// readable form would overflow, carry a reserved double-slash digest marker.
// The digest commits to the exact, length-framed pair rather than its readable
// rendering, so different component boundaries cannot collapse onto one key.
//
// An empty result means that the parent or suffix was not canonical.
func DeriveIdempotencyKey(parent, suffix string) string {
	if !IsCanonicalIdempotencyKey(parent) || !IsCanonicalIdempotencyKey(suffix) {
		return ""
	}

	candidate := parent + "/" + suffix
	if !strings.ContainsRune(parent, '/') && !strings.ContainsRune(suffix, '/') && len(candidate) <= MaximumIdempotencyKeyBytes {
		return candidate
	}

	marker := derivedIdempotencyDigestMarker + hex.EncodeToString(derivedIdempotencyPairDigest(parent, suffix))
	prefix := truncateValidUTF8(candidate, MaximumIdempotencyKeyBytes-len(marker))
	return prefix + marker
}

func derivedIdempotencyPairDigest(parent, suffix string) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(derivedIdempotencyDomain))
	writeIdempotencyFrame(hash, parent)
	writeIdempotencyFrame(hash, suffix)
	return hash.Sum(nil)
}

type idempotencyDigestWriter interface {
	Write([]byte) (int, error)
}

func writeIdempotencyFrame(writer idempotencyDigestWriter, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write([]byte(value))
}

func truncateValidUTF8(value string, maximumBytes int) string {
	if len(value) <= maximumBytes {
		return value
	}
	value = value[:maximumBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
