package application

import (
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func zeroProvisioningPlanDigest() string {
	return "sha256:" + strings.Repeat("0", 64)
}

func requireZeroProvisioningPlanDigestRejected(t *testing.T, bind func(string) error) {
	t.Helper()
	if err := bind(zeroProvisioningPlanDigest()); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("zero provisioning plan digest error = %v, want invalid_argument", err)
	}
}
