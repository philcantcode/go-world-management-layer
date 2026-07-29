package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCompileExamplePolicyRoundTripAndDeterminism(t *testing.T) {
	source := examplePolicy(t)
	fingerprint := fingerprintFor(t, source, nil, nil)
	first := compilePolicy(t, source, fingerprint)
	second := compilePolicy(t, source, fingerprint)

	if first.Digest() != second.Digest() {
		t.Fatalf("digest is not deterministic: %s != %s", first.Digest(), second.Digest())
	}
	if !bytes.Equal(first.CanonicalJSON(), second.CanonicalJSON()) {
		t.Fatal("canonical JSON is not deterministic")
	}
	if first.Digest().String() == "" || !strings.HasPrefix(first.Digest().String(), "sha256:") {
		t.Fatalf("unexpected digest %q", first.Digest().String())
	}
	var roundTripped Policy
	if err := json.Unmarshal(first.CanonicalJSON(), &roundTripped); err != nil {
		t.Fatalf("canonical JSON did not round-trip: %v", err)
	}
	if roundTripped.Metadata.Name != "mixed-vr-visibility-first" || len(roundTripped.Spec.Targets.Templates) != 2 {
		t.Fatalf("round-trip lost policy data: %#v", roundTripped.Metadata)
	}

	canonicalFingerprint := fingerprintFor(t, first.CanonicalJSON(), nil, nil)
	canonical := compilePolicy(t, first.CanonicalJSON(), canonicalFingerprint)
	if canonical.Digest() != first.Digest() {
		t.Fatalf("canonical round-trip changed digest: %s != %s", canonical.Digest(), first.Digest())
	}
}

func FuzzCompileDeterministic(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte("apiVersion: world.philcantcode.dev/v1\nkind: WorldPolicy\n"))
	if example, err := os.ReadFile("../docs/examples/environment-policy.yaml"); err == nil {
		f.Add(example)
	}
	f.Fuzz(func(t *testing.T, source []byte) {
		if len(source) > 64<<10 {
			t.Skip()
		}
		compiled, err := Compile(source, CompileOptions{})
		if err != nil {
			return
		}
		canonical := compiled.CanonicalJSON()
		var decoded any
		if err := json.Unmarshal(canonical, &decoded); err != nil {
			t.Fatalf("successful compile produced invalid canonical JSON: %v", err)
		}
		repeated, err := Compile(source, CompileOptions{})
		if err != nil {
			t.Fatalf("repeated compile failed: %v", err)
		}
		if repeated.Digest() != compiled.Digest() || !bytes.Equal(repeated.CanonicalJSON(), canonical) {
			t.Fatal("repeated compile was not deterministic")
		}
		if len(canonical) > 0 {
			canonical[0] ^= 0xff
			if bytes.Equal(compiled.CanonicalJSON(), canonical) {
				t.Fatal("CanonicalJSON exposed mutable internal storage")
			}
		}
	})
}

func TestCompileRejectsUnknownFieldWithFullPath(t *testing.T) {
	source := replaceOnce(t, examplePolicy(t), "    ttl: 2h\n", "    ttl: 2h\n    mysteryLimit: 3\n")
	_, err := Compile(source, CompileOptions{})
	assertPolicyErrorContains(t, err, "spec.lease.mysteryLimit", "unknown field")
}

func TestCompileRejectsWrongEnvelope(t *testing.T) {
	tests := []struct {
		name         string
		oldValue     string
		newValue     string
		expectedPath string
	}{
		{"api version", APIVersion, "world.philcantcode.dev/v9", "apiVersion"},
		{"kind", Kind, "OtherPolicy", "kind"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := replaceOnce(t, examplePolicy(t), test.oldValue, test.newValue)
			_, err := Compile(source, CompileOptions{})
			assertPolicyErrorContains(t, err, test.expectedPath, "must be")
		})
	}
}

func TestCompileAppliesDefaults(t *testing.T) {
	source := examplePolicy(t)
	source = replaceOnce(t, source, "    ttl: 2h\n", "")
	source = replaceOnce(t, source, "    quiesceDeadline: 30s\n", "")
	source = replaceOnce(t, source, "    mode: overlayfs\n", "")
	source = replaceOnce(t, source, "      construction: require-reflink\n", "")
	fingerprint := fingerprintFor(t, source, nil, nil)
	effective := compilePolicy(t, source, fingerprint)
	policy := effective.Policy()
	if policy.Spec.Lease.TTL != DefaultLeaseTTL || policy.Spec.Lease.QuiesceDeadline != DefaultQuiesceDeadline {
		t.Fatalf("lease defaults not applied: %#v", policy.Spec.Lease)
	}
	if policy.Spec.Workspace.Mode != "overlayfs" || policy.Spec.Workspace.InputView.Construction != "require-reflink" {
		t.Fatalf("workspace defaults not applied: %#v", policy.Spec.Workspace.InputView)
	}
}

func TestQuantityParsing(t *testing.T) {
	cpuTests := map[string]int64{"2": 2000, "0.5": 500, "250m": 250}
	for source, expected := range cpuTests {
		value, err := parseCPU(source)
		if err != nil || value.MilliCPU() != expected {
			t.Errorf("parseCPU(%q) = %d, %v; want %d", source, value, err, expected)
		}
	}
	byteTests := map[string]int64{"0B": 0, "1Ki": 1024, "1.5Gi": 1610612736, "2GB": 2_000_000_000}
	for source, expected := range byteTests {
		value, err := parseBytes(source)
		if err != nil || value.Bytes() != expected {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", source, value, err, expected)
		}
	}
	for _, source := range []string{"-1Gi", "0.1B", "1G", "nan"} {
		if _, err := parseBytes(source); err == nil {
			t.Errorf("parseBytes(%q) unexpectedly succeeded", source)
		}
	}
}

func TestCompileValidatesResourceRequestsAndLimits(t *testing.T) {
	source := replaceOnce(t, examplePolicy(t), "        cpu: \"2\"\n        memory: 4Gi", "        cpu: \"5\"\n        memory: 4Gi")
	_, err := Compile(source, CompileOptions{})
	assertPolicyErrorContains(t, err, "spec.agentWorkspace.resources.requests.cpu", "must not exceed")
}

func TestCompileValidatesWatermarksAndRetention(t *testing.T) {
	t.Run("watermarks", func(t *testing.T) {
		source := replaceOnce(t, examplePolicy(t), "      highWaterPercent: 85", "      highWaterPercent: 60")
		_, err := Compile(source, CompileOptions{})
		assertPolicyErrorContains(t, err, "spec.workspace.cache.lowWaterPercent", "must be less")
	})
	t.Run("artifact acknowledgement", func(t *testing.T) {
		source := replaceOnce(t, examplePolicy(t), "      finalizeToArtifactStore: true", "      finalizeToArtifactStore: false")
		_, err := Compile(source, CompileOptions{})
		assertPolicyErrorContains(t, err, "spec.observation.retention.deleteLocalAfterArtifactAck", "requires")
	})
}

func TestCompileEnforcesCommandAuthorityInvariants(t *testing.T) {
	tests := []struct {
		name     string
		oldValue string
		newValue string
		path     string
	}{
		{
			"semantic allowlist forbidden",
			"commandAuthority: arbitrary-inside-assigned-target",
			"commandAuthority: approved-command-list",
			"spec.targets.templates[0].interaction.commandAuthority",
		},
		{
			"host authority denial required",
			"            - host-exec\n            - docker-api",
			"            - docker-api",
			"spec.targets.templates[0].interaction.deniedInfrastructureAuthority",
		},
		{
			"ADB must remain scoped",
			"          adb: scoped-gateway",
			"          adb: raw-host-adb",
			"spec.targets.templates[1].interaction.adb",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := replaceOnce(t, examplePolicy(t), test.oldValue, test.newValue)
			_, err := Compile(source, CompileOptions{})
			assertPolicyErrorContains(t, err, test.path, "")
		})
	}
}

func TestCompileRequiresImplementedAndroidRuntimeContract(t *testing.T) {
	tests := []struct {
		name     string
		oldValue string
		newValue string
		path     string
	}{
		{
			name:     "unsupported driver",
			oldValue: "driver: android-emulator",
			newValue: "driver: unsupported-driver",
			path:     "spec.targets.templates[1].runtime.driver",
		},
		{
			name:     "unsupported baseline",
			oldValue: "baselineState: clean-boot",
			newValue: "baselineState: snapshot-v1",
			path:     "spec.targets.templates[1].runtime.baselineState",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := replaceOnce(t, examplePolicy(t), test.oldValue, test.newValue)
			_, err := Compile(source, CompileOptions{})
			assertPolicyErrorContains(t, err, test.path, "must be")
		})
	}
}

func TestCompileRejectsUnimplementedLinuxContainerRuntimes(t *testing.T) {
	for _, runtimeName := range []string{"gvisor", "kata"} {
		t.Run(runtimeName, func(t *testing.T) {
			source := replaceOnce(t, examplePolicy(t), "          runtime: runc", "          runtime: "+runtimeName)
			source = replaceOnce(t, source, "          isolationProfile: observable-container", "          isolationProfile: sandboxed-kernel")
			_, err := Compile(source, CompileOptions{})
			assertPolicyErrorContains(t, err, "spec.targets.templates[0].runtime.runtime", "must be")
		})
	}
}

func TestCompileRequiresEnforceableAndroidResources(t *testing.T) {
	tests := []struct {
		name     string
		oldValue string
		newValue string
		path     string
		message  string
	}{
		{
			name:     "fractional host CPU",
			oldValue: "            cpu: \"4\"\n            memory: 6Gi\n            writableState: 1Gi",
			newValue: "            cpu: \"1.5\"\n            memory: 6Gi\n            writableState: 1Gi",
			path:     "spec.targets.templates[1].resources.limits.cpu",
			message:  "whole-vCPU",
		},
		{
			name:     "unaligned data partition",
			oldValue: "            writableState: 1Gi",
			newValue: "            writableState: 1073741825B",
			path:     "spec.targets.templates[1].resources.limits.writableState",
			message:  "MiB-aligned",
		},
		{
			name:     "oversized data partition",
			oldValue: "            writableState: 1Gi",
			newValue: "            writableState: 2Gi",
			path:     "spec.targets.templates[1].resources.limits.writableState",
			message:  "64 to 2047 MiB",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := replaceOnce(t, examplePolicy(t), test.oldValue, test.newValue)
			_, err := Compile(source, CompileOptions{})
			assertPolicyErrorContains(t, err, test.path, test.message)
		})
	}
}

func TestCompileCapabilityResolution(t *testing.T) {
	t.Run("missing required reflink", func(t *testing.T) {
		source := examplePolicy(t)
		fingerprint := fingerprintFor(t, source, nil, map[string]bool{"filesystem.reflink": true})
		_, err := Compile(source, CompileOptions{Capabilities: fingerprint})
		assertPolicyErrorContains(t, err, "spec.workspace.inputView.construction", "required capability \"filesystem.reflink\" is unknown")
	})
	t.Run("unknown required coverage", func(t *testing.T) {
		source := examplePolicy(t)
		name := "coverage.linux-container.syscall-results"
		fingerprint := fingerprintFor(t, source, map[string]CapabilityStatus{name: CapabilityUnknown}, nil)
		_, err := Compile(source, CompileOptions{Capabilities: fingerprint})
		assertPolicyErrorContains(t, err, "spec.observation.requiredCoverage.linux-container[1]", "is unknown")
	})
	t.Run("allow-copy makes reflink an explicit downgrade", func(t *testing.T) {
		source := replaceOnce(t, examplePolicy(t), "construction: require-reflink", "construction: allow-copy")
		fingerprint := fingerprintFor(t, source, map[string]CapabilityStatus{"filesystem.reflink": CapabilityUnsupported}, nil)
		effective := compilePolicy(t, source, fingerprint)
		warnings := effective.Warnings()
		if len(warnings) != 1 || warnings[0].Capability != "filesystem.reflink" || warnings[0].Path != "spec.workspace.inputView.construction" {
			t.Fatalf("unexpected warnings: %#v", warnings)
		}
	})
	t.Run("preferred collector downgrade", func(t *testing.T) {
		source := examplePolicy(t)
		name := "collector.adapter.mobsf"
		fingerprint := fingerprintFor(t, source, map[string]CapabilityStatus{name: CapabilityUnsupported}, nil)
		effective := compilePolicy(t, source, fingerprint)
		if !containsWarning(effective.Warnings(), name) {
			t.Fatalf("missing downgrade for %s: %#v", name, effective.Warnings())
		}
	})
}

func TestCompileRequiresPayloadFilters(t *testing.T) {
	tests := []struct {
		oldValue string
		newValue string
		path     string
	}{
		{"requireProcessOrPathFilter: true", "requireProcessOrPathFilter: false", "spec.observation.profiles.payload.requireProcessOrPathFilter"},
		{"requireFlowFilter: true", "requireFlowFilter: false", "spec.observation.allowedOnDemand.packetPayload.requireFlowFilter"},
	}
	for _, test := range tests {
		source := replaceOnce(t, examplePolicy(t), test.oldValue, test.newValue)
		_, err := Compile(source, CompileOptions{})
		assertPolicyErrorContains(t, err, test.path, "must be true")
	}
}

func TestEffectivePolicyIsDeeplyImmutable(t *testing.T) {
	source := examplePolicy(t)
	fingerprint := fingerprintFor(t, source, map[string]CapabilityStatus{"collector.adapter.mobsf": CapabilityUnsupported}, nil)
	effective := compilePolicy(t, source, fingerprint)
	originalDigest := effective.Digest()
	originalCanonical := effective.CanonicalJSON()

	copyPolicy := effective.Policy()
	copyPolicy.Spec.Targets.Templates[0].Interaction.DeniedInfrastructureAuthority[0] = "mutated"
	copyPolicy.Spec.Observation.RequiredCoverage["linux-container"][0] = "mutated"
	canonical := effective.CanonicalJSON()
	canonical[0] = '['
	requirements := effective.CapabilityRequirements()
	requirements[0].Constraints["mutated"] = "yes"
	warnings := effective.Warnings()
	warnings[0].Message = "mutated"

	if effective.Digest() != originalDigest || !bytes.Equal(effective.CanonicalJSON(), originalCanonical) {
		t.Fatal("effective policy changed through a returned value")
	}
	if effective.Policy().Spec.Targets.Templates[0].Interaction.DeniedInfrastructureAuthority[0] == "mutated" {
		t.Fatal("Policy returned shared slice state")
	}
	if _, found := effective.CapabilityRequirements()[0].Constraints["mutated"]; found {
		t.Fatal("CapabilityRequirements returned shared map state")
	}
	if effective.Warnings()[0].Message == "mutated" {
		t.Fatal("Warnings returned shared state")
	}
}

func TestCompileRejectsAmbiguousYAML(t *testing.T) {
	t.Run("multiple documents", func(t *testing.T) {
		source := append(examplePolicy(t), []byte("\n---\nkind: ResearchSessionPolicy\n")...)
		_, err := Compile(source, CompileOptions{})
		assertPolicyErrorContains(t, err, "$", "more than one YAML document")
	})
	t.Run("anchor", func(t *testing.T) {
		source := replaceOnce(t, examplePolicy(t), "    ttl: 2h", "    ttl: &shared 2h")
		_, err := Compile(source, CompileOptions{})
		assertPolicyErrorContains(t, err, "spec.lease.ttl", "anchors and aliases")
	})
}

func TestInvalidQuantityReportsFieldPath(t *testing.T) {
	source := replaceOnce(t, examplePolicy(t), "        memory: 4Gi", "        memory: not-bytes")
	_, err := Compile(source, CompileOptions{})
	assertPolicyErrorContains(t, err, "spec.agentWorkspace.resources.requests.memory", "invalid byte quantity")
}

func examplePolicy(t *testing.T) []byte {
	t.Helper()
	source, err := os.ReadFile("../docs/examples/environment-policy.yaml")
	if err != nil {
		t.Fatalf("read example policy: %v", err)
	}
	// Fixtures and replaceOnce helpers assume Unix newlines. Normalize so
	// Windows checkouts with CRLF still match the documented replacements.
	return bytes.ReplaceAll(source, []byte("\r\n"), []byte("\n"))
}

func fingerprintFor(t *testing.T, source []byte, overrides map[string]CapabilityStatus, omit map[string]bool) CapabilityFingerprint {
	t.Helper()
	parsed, positions, err := decodeStrict(source)
	if err != nil {
		t.Fatalf("decode fixture for capabilities: %v", err)
	}
	applyDefaults(&parsed, positions)
	if err := validatePolicy(&parsed, positions); err != nil {
		t.Fatalf("validate fixture for capabilities: %v", err)
	}
	requirements := deriveCapabilityRequirements(&parsed)
	capabilities := make(map[string]Capability, len(requirements))
	for _, requirement := range requirements {
		if omit[requirement.Name] {
			continue
		}
		status := CapabilitySupported
		if override, found := overrides[requirement.Name]; found {
			status = override
		}
		capability, err := NewCapability(status, nil, map[string]string{"fixture": "test"})
		if err != nil {
			t.Fatalf("new capability %s: %v", requirement.Name, err)
		}
		capabilities[requirement.Name] = capability
	}
	if len(capabilities) == 0 {
		capability, err := NewCapability(CapabilitySupported, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		capabilities["fixture.unrelated"] = capability
	}
	fingerprint, err := NewCapabilityFingerprint(capabilities, map[string]string{"node": "test"})
	if err != nil {
		t.Fatalf("new capability fingerprint: %v", err)
	}
	return fingerprint
}

func compilePolicy(t *testing.T, source []byte, fingerprint CapabilityFingerprint) *EffectivePolicy {
	t.Helper()
	effective, err := Compile(source, CompileOptions{Capabilities: fingerprint})
	if err != nil {
		t.Fatalf("Compile() error: %v", err)
	}
	return effective
}

func replaceOnce(t *testing.T, source []byte, oldValue, newValue string) []byte {
	t.Helper()
	if strings.Count(string(source), oldValue) != 1 {
		t.Fatalf("fixture replacement %q matched %d times", oldValue, strings.Count(string(source), oldValue))
	}
	return []byte(strings.Replace(string(source), oldValue, newValue, 1))
}

func assertPolicyErrorContains(t *testing.T, err error, path, message string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected policy error, got nil")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
	for _, problem := range validation.Problems {
		if problem.Path == path && strings.Contains(problem.Message, message) {
			return
		}
	}
	t.Fatalf("error did not contain path %q and message %q: %v", path, message, err)
}

func containsWarning(warnings []Warning, capability string) bool {
	for _, warning := range warnings {
		if warning.Capability == capability {
			return true
		}
	}
	return false
}

func TestDefaultDurationConstants(t *testing.T) {
	// Guard accidental unit mistakes in exported defaults.
	if DefaultLeaseTTL.Duration() != 2*time.Hour || DefaultIncidentMetricInterval.Duration() != 250*time.Millisecond {
		t.Fatal("unexpected duration defaults")
	}
}
