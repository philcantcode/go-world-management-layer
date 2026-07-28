package ports

import (
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestValidateCollectorNameUsesBoundedPortableGrammar(t *testing.T) {
	valid := []string{
		"a",
		"Process-Trace.v1_2",
		strings.Repeat("x", MaximumCollectorNameBytes),
	}
	for _, name := range valid {
		if err := ValidateCollectorName(name); err != nil {
			t.Errorf("valid collector name %q rejected: %v", name, err)
		}
	}

	invalid := []string{
		"",
		" collector",
		"collector ",
		"collector/name",
		"collector%2Fname",
		"collector\\name",
		".collector",
		"collector-",
		"collectör",
		strings.Repeat("x", MaximumCollectorNameBytes+1),
	}
	for _, name := range invalid {
		if err := ValidateCollectorName(name); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Errorf("invalid collector name %q error = %v, want invalid argument", name, err)
		}
	}
}

func TestDeriveCollectorIdempotencyKeyPreservesHeadroom(t *testing.T) {
	parent := strings.Repeat("p", domain.MaximumIdempotencyKeyBytes)
	name := strings.Repeat("n", MaximumCollectorNameBytes)
	key := DeriveCollectorIdempotencyKey(parent, name)
	if !domain.IsCanonicalIdempotencyKey(key) || len(key) != domain.MaximumIdempotencyKeyBytes || !strings.Contains(key, "//sha256:") {
		t.Fatalf("collector child key is not bounded and canonical: %d bytes, %q", len(key), key)
	}
	if repeated := DeriveCollectorIdempotencyKey(parent, name); repeated != key {
		t.Fatalf("collector child derivation changed between calls: %q != %q", repeated, key)
	}
	if got := DeriveCollectorIdempotencyKey(parent, "invalid/name"); got != "" {
		t.Fatalf("invalid collector name derived key %q", got)
	}
}

func TestCollectorSpecRejectsMalformedName(t *testing.T) {
	spec := CollectorSpec{
		Name: "process-trace",
		Requirement: ObservationRequirement{
			SignalFamily: "process", Placement: domain.CollectorPlacementHost,
			MinimumLevel: domain.CoverageLevelComplete, Required: true,
		},
		Adapter: "test.process", Version: "1", ConfigurationDigest: domain.NewDigest([]byte("config")), MaximumBytes: 1024,
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid collector spec rejected: %v", err)
	}
	spec.Name = strings.Repeat("x", MaximumCollectorNameBytes+1)
	if err := spec.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("overlong collector spec error = %v, want invalid argument", err)
	}
}
