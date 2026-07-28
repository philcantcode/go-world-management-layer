package ports

import (
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestRequireIdempotencyUsesCanonicalSharedBoundary(t *testing.T) {
	if err := requireIdempotency("test", strings.Repeat("k", domain.MaximumIdempotencyKeyBytes)); err != nil {
		t.Fatalf("maximum canonical key rejected: %v", err)
	}
	for name, value := range map[string]string{
		"empty": "", "whitespace": " \t ", "leading": " invalid", "trailing": "invalid ",
		"oversized": strings.Repeat("k", domain.MaximumIdempotencyKeyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := requireIdempotency("test", value); !domain.IsCode(err, domain.CodeInvalidArgument) {
				t.Fatalf("requireIdempotency() error = %v, want invalid argument", err)
			}
		})
	}
}
