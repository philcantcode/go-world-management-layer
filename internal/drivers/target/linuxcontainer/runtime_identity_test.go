package linuxcontainer

import (
	"crypto/sha256"
	"encoding/hex"
)

// testRuntimeID keeps test fixtures readable while honoring the same full
// Docker identity contract as production inventory and inspect operations.
func testRuntimeID(label string) string {
	digest := sha256.Sum256([]byte(label))
	return hex.EncodeToString(digest[:])
}
