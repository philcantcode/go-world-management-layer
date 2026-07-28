package domain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTypedUUIDv7IDsRoundTripAndRejectCrossType(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 34, 56, 789_000_000, time.UTC)
	generator, err := NewIDGenerator(func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{0xab}, 256)))
	if err != nil {
		t.Fatal(err)
	}
	sessionID, err := generator.ResearchSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sessionID.String(), "rs_") {
		t.Fatalf("wrong prefix: %s", sessionID)
	}
	uuid := sessionID.UUID()
	if uuid[14] != '7' {
		t.Fatalf("not UUIDv7: %s", uuid)
	}
	if uuid[19] != 'a' {
		t.Fatalf("wrong RFC variant: %s", uuid)
	}
	parsed, err := ParseResearchSessionID(sessionID.String())
	if err != nil || parsed != sessionID {
		t.Fatalf("round trip: %v, %v", parsed, err)
	}
	if _, err := ParseLeaseID(sessionID.String()); !IsCode(err, CodeInvalidID) {
		t.Fatalf("cross-type parse: %v", err)
	}
	if _, err := ParseResearchSessionID("rs_" + strings.ToUpper(uuid)); !IsCode(err, CodeInvalidID) {
		t.Fatalf("upper case parse: %v", err)
	}
	invalidVersion := "rs_" + uuid[:14] + "4" + uuid[15:]
	if _, err := ParseResearchSessionID(invalidVersion); !IsCode(err, CodeInvalidID) {
		t.Fatalf("wrong version parse: %v", err)
	}

	data, err := json.Marshal(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ResearchSessionID
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != sessionID {
		t.Fatalf("json round trip changed ID")
	}
}

func TestEveryIDPrefixParsesOnlyItsOwnType(t *testing.T) {
	generator, _ := NewIDGenerator(func() time.Time { return time.UnixMilli(1_700_000_000_123) }, bytes.NewReader(bytes.Repeat([]byte{0x42}, 2048)))
	tests := []struct {
		name  string
		make  func() (string, error)
		parse func(string) error
	}{
		{"session", func() (string, error) { v, e := generator.ResearchSessionID(); return v.String(), e }, func(s string) error { _, e := ParseResearchSessionID(s); return e }},
		{"lease", func() (string, error) { v, e := generator.LeaseID(); return v.String(), e }, func(s string) error { _, e := ParseLeaseID(s); return e }},
		{"agent workspace", func() (string, error) { v, e := generator.AgentWorkspaceID(); return v.String(), e }, func(s string) error { _, e := ParseAgentWorkspaceID(s); return e }},
		{"exec", func() (string, error) { v, e := generator.ExecID(); return v.String(), e }, func(s string) error { _, e := ParseExecID(s); return e }},
		{"target", func() (string, error) { v, e := generator.TargetID(); return v.String(), e }, func(s string) error { _, e := ParseTargetID(s); return e }},
		{"run", func() (string, error) { v, e := generator.TargetRunID(); return v.String(), e }, func(s string) error { _, e := ParseTargetRunID(s); return e }},
		{"operation", func() (string, error) { v, e := generator.TargetOperationID(); return v.String(), e }, func(s string) error { _, e := ParseTargetOperationID(s); return e }},
		{"workspace", func() (string, error) { v, e := generator.WorkspaceID(); return v.String(), e }, func(s string) error { _, e := ParseWorkspaceID(s); return e }},
		{"incident", func() (string, error) { v, e := generator.IncidentID(); return v.String(), e }, func(s string) error { _, e := ParseIncidentID(s); return e }},
		{"capture", func() (string, error) { v, e := generator.CaptureID(); return v.String(), e }, func(s string) error { _, e := ParseCaptureID(s); return e }},
		{"bundle", func() (string, error) { v, e := generator.ObservationBundleID(); return v.String(), e }, func(s string) error { _, e := ParseObservationBundleID(s); return e }},
		{"export", func() (string, error) { v, e := generator.ExportID(); return v.String(), e }, func(s string) error { _, e := ParseExportID(s); return e }},
		{"event", func() (string, error) { v, e := generator.EventID(); return v.String(), e }, func(s string) error { _, e := ParseEventID(s); return e }},
		{"correlation", func() (string, error) { v, e := generator.CorrelationID(); return v.String(), e }, func(s string) error { _, e := ParseCorrelationID(s); return e }},
		{"collector", func() (string, error) { v, e := generator.CollectorID(); return v.String(), e }, func(s string) error { _, e := ParseCollectorID(s); return e }},
		{"subject", func() (string, error) { v, e := generator.SubjectID(); return v.String(), e }, func(s string) error { _, e := ParseSubjectID(s); return e }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := test.make()
			if err != nil {
				t.Fatal(err)
			}
			if err := test.parse(value); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInputViewIDIsDomainSeparatedAndCanonical(t *testing.T) {
	id := NewInputViewID([]byte("manifest"))
	if id.IsZero() || !strings.HasPrefix(id.String(), "iv_") {
		t.Fatalf("invalid ID: %s", id)
	}
	if id.Digest() == NewDigest([]byte("manifest")) {
		t.Fatal("input-view digest was not domain separated")
	}
	parsed, err := ParseInputViewID(id.String())
	if err != nil || parsed != id {
		t.Fatalf("round trip: %v, %v", parsed, err)
	}
}

func TestDigestTextRejectsTheReservedZeroValue(t *testing.T) {
	valid := NewDigest([]byte("canonical digest"))
	parsed, err := ParseDigest(valid.String())
	if err != nil || parsed != valid {
		t.Fatalf("valid digest round trip = %v, %v", parsed, err)
	}

	zero := "sha256:" + strings.Repeat("0", 64)
	if _, err := ParseDigest(zero); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("zero digest parse error = %v, want invalid_argument", err)
	}
	var decoded Digest
	if err := decoded.UnmarshalText([]byte(zero)); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("zero digest unmarshal error = %v, want invalid_argument", err)
	}
}

func TestCapabilityEvaluationTriStateConstraintsAndCopies(t *testing.T) {
	constraints := map[string]string{"runtime": "runc", "version": "1.2"}
	evidence := map[string]string{"binary_digest": "abc"}
	supported, err := NewCapability(CapabilitySupported, constraints, evidence)
	if err != nil {
		t.Fatal(err)
	}
	unsupported, _ := NewCapability(CapabilityUnsupported, nil, map[string]string{"probe": "v1"})
	fingerprint, err := NewCapabilityFingerprint(map[string]Capability{"ebpf": supported, "kvm": unsupported}, map[string]string{"kernel": "6.8"})
	if err != nil {
		t.Fatal(err)
	}
	constraints["runtime"] = "changed"
	evidence["binary_digest"] = "changed"
	capability, _ := fingerprint.Capability("ebpf")
	if capability.Constraints()["runtime"] != "runc" || capability.Evidence()["binary_digest"] != "abc" {
		t.Fatal("constructor retained mutable maps")
	}

	requirements := []CapabilityRequirement{
		{Name: "ebpf", Level: RequirementRequired, Constraints: map[string]string{"runtime": "runc"}},
		{Name: "kvm", Level: RequirementPreferred},
		{Name: "openat2", Level: RequirementRequired},
	}
	evaluation, err := EvaluateCapabilityRequirements(fingerprint, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.Admitted() {
		t.Fatal("required unknown capability admitted")
	}
	if len(evaluation.Downgrades()) != 1 || len(evaluation.Failures()) != 1 {
		t.Fatalf("unexpected results: %#v", evaluation.Resolutions())
	}
	requirements[0].Constraints["runtime"] = "mutated"
	if evaluation.Resolutions()[0].Requirement().Constraints["runtime"] != "runc" {
		t.Fatal("evaluation retained requirement map")
	}

	fingerprintAgain, err := NewCapabilityFingerprint(map[string]Capability{"kvm": unsupported, "ebpf": supported}, map[string]string{"kernel": "6.8"})
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint.Digest() != fingerprintAgain.Digest() {
		t.Fatal("fingerprint digest depends on map iteration order")
	}
}

func TestRevisionErrorsRemainStructured(t *testing.T) {
	if err := RequireRevision(3, 2); !IsCode(err, CodeStaleRevision) {
		t.Fatalf("got %v", err)
	}
	err := NewDetailedError(CodeConflict, "test", "field", "message", map[string]string{"key": "value"}, nil)
	details := err.Details()
	details["key"] = "mutated"
	if err.Details()["key"] != "value" {
		t.Fatal("error details were mutable")
	}
}
