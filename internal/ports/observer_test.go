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

func TestADBObservationAuthorityRequiresLiteralExactSelection(t *testing.T) {
	endpoint, err := ParseADBServerEndpoint("127.0.0.1:5041")
	if err != nil || endpoint != (ADBServerEndpoint{Host: "127.0.0.1", Port: 5041}) {
		t.Fatalf("parsed endpoint = %#v, %v", endpoint, err)
	}
	selection, err := NewADBDeviceSelection(endpoint, "emulator-5578")
	if err != nil {
		t.Fatal(err)
	}
	attachment := ObservationAttachment{
		TargetKind: domain.TargetAndroidVirtualDevice, RuntimeID: "world-android-generation-2", ADBDevice: selection,
	}
	if err := attachment.Validate(); err != nil {
		t.Fatalf("valid Android attachment rejected: %v", err)
	}

	for _, value := range []string{"localhost:5037", "192.0.2.1:5037", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1"} {
		if _, err := ParseADBServerEndpoint(value); err == nil {
			t.Errorf("unsafe ADB server %q accepted", value)
		}
	}
	for _, serial := range []string{"", "-e", "emulator-5578\n-s", strings.Repeat("a", 1025)} {
		if err := ValidateExactADBSerial(serial); err == nil {
			t.Errorf("unsafe ADB serial %q accepted", serial)
		}
	}

	wrongKind := attachment
	wrongKind.TargetKind = domain.TargetLinuxContainer
	if err := wrongKind.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("non-Android ADB attachment error = %v", err)
	}
	invalidPort := attachment
	invalidPort.ADBDevice.Server.Port = 0
	if err := invalidPort.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("zero-port ADB attachment error = %v", err)
	}
	remote := attachment
	remote.ADBDevice.Server.Host = "192.0.2.1"
	if err := remote.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("remote ADB attachment error = %v", err)
	}
	injected := attachment
	injected.ADBDevice.Serial = "-s"
	if err := injected.Validate(); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("option-shaped ADB attachment error = %v", err)
	}
}
