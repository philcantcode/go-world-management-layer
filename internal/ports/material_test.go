package ports

import (
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestInputPlanModeContractMatchesRegularFileWriters(t *testing.T) {
	occurrence := ArtifactOccurrence{Reference: "artifact:input", Digest: domain.NewDigest([]byte("x")), Size: 1}
	plan := InputPlan{SecurityScope: "campaign", Entries: []InputEntryPlan{{Occurrence: occurrence, LogicalPath: "input.bin"}}}
	if err := plan.Validate(); err != nil {
		t.Fatalf("zero mode default was rejected: %v", err)
	}
	plan.Entries[0].Mode = 0o777
	if err := plan.Validate(); err != nil {
		t.Fatalf("regular permission bits were rejected: %v", err)
	}
	plan.Entries[0].Mode = 0o1000
	if err := plan.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("special mode error = %v, want invalid argument", err)
	}
}
