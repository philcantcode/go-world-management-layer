package domain

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDeriveIdempotencyKeyKeepsOnlyUnambiguousPairsReadable(t *testing.T) {
	ordinary := DeriveIdempotencyKey("parent", "child")
	if ordinary != "parent/child" || strings.Count(ordinary, "/") != 1 {
		t.Fatalf("ordinary derivation = %q, want exact one-slash form", ordinary)
	}

	percent := DeriveIdempotencyKey("parent%2Fencoded", "child")
	if percent != "parent%2Fencoded/child" {
		t.Fatalf("percent-bearing derivation was normalized: %q", percent)
	}
	if percent == DeriveIdempotencyKey("parent/encoded", "child") {
		t.Fatal("literal percent encoding collided with a slash-bearing component")
	}

	ambiguousLeft := DeriveIdempotencyKey("a/b", "c")
	ambiguousRight := DeriveIdempotencyKey("a", "b/c")
	for name, value := range map[string]string{"left": ambiguousLeft, "right": ambiguousRight} {
		if !strings.Contains(value, derivedIdempotencyDigestMarker) || !IsCanonicalIdempotencyKey(value) {
			t.Errorf("%s ambiguous derivation did not use the reserved hashed class: %q", name, value)
		}
	}
	if ambiguousLeft == ambiguousRight {
		t.Fatal("different (parent, suffix) boundaries collapsed onto one key")
	}
	if ordinary == ambiguousLeft || strings.Contains(ordinary, derivedIdempotencyDigestMarker) {
		t.Fatal("ordinary and hashed derivations did not remain in disjoint key classes")
	}
}

func TestDeriveIdempotencyKeyBoundsOverflowAndUnicode(t *testing.T) {
	exactParent := strings.Repeat("p", 512)
	exactSuffix := strings.Repeat("s", MaximumIdempotencyKeyBytes-len(exactParent)-1)
	exact := DeriveIdempotencyKey(exactParent, exactSuffix)
	if exact != exactParent+"/"+exactSuffix || len(exact) != MaximumIdempotencyKeyBytes {
		t.Fatalf("exact-boundary derivation was not retained: %d bytes", len(exact))
	}

	maximumParent := strings.Repeat("m", MaximumIdempotencyKeyBytes)
	overflow := DeriveIdempotencyKey(maximumParent, "child")
	if len(overflow) != MaximumIdempotencyKeyBytes || !utf8.ValidString(overflow) || !strings.Contains(overflow, derivedIdempotencyDigestMarker) {
		t.Fatalf("overflow derivation is not a canonical reserved key: %d bytes, %q", len(overflow), overflow)
	}
	ordinary := DeriveIdempotencyKey("m", "child")
	if ordinary == overflow || strings.Count(ordinary, "/") != 1 || strings.Contains(ordinary, derivedIdempotencyDigestMarker) {
		t.Fatal("ordinary and overflowing derivations did not remain in disjoint key classes")
	}
	if overflow == DeriveIdempotencyKey(maximumParent, "other") {
		t.Fatal("different overflowing suffixes collapsed onto one key")
	}

	unicodeParent := strings.Repeat("€", 341) + "x"
	if len(unicodeParent) != MaximumIdempotencyKeyBytes {
		t.Fatalf("unicode fixture = %d bytes, want %d", len(unicodeParent), MaximumIdempotencyKeyBytes)
	}
	unicode := DeriveIdempotencyKey(unicodeParent, "child")
	if len(unicode) != MaximumIdempotencyKeyBytes || !utf8.ValidString(unicode) || !IsCanonicalIdempotencyKey(unicode) {
		t.Fatalf("unicode overflow split a rune or exceeded the bound: %d bytes", len(unicode))
	}
}

func TestDeriveIdempotencyKeyIsDeterministicForNestedKeys(t *testing.T) {
	first := DeriveIdempotencyKey("root", "child")
	nested := DeriveIdempotencyKey(first, "nested")
	deeper := DeriveIdempotencyKey(nested, "deeper/branch")
	for name, value := range map[string]string{"nested": nested, "deeper": deeper} {
		if !IsCanonicalIdempotencyKey(value) || !strings.Contains(value, derivedIdempotencyDigestMarker) {
			t.Errorf("%s key is not canonical and hashed: %q", name, value)
		}
	}
	for index := 0; index < 10; index++ {
		if got := DeriveIdempotencyKey(first, "nested"); got != nested {
			t.Fatalf("repeat %d produced %q, want %q", index, got, nested)
		}
	}
	if nested == DeriveIdempotencyKey("root", "child/nested") {
		t.Fatal("nested derivation collided with a differently framed pair")
	}
}
