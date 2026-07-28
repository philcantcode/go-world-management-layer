package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestMapCapabilityReportPreservesFingerprintEvidence(t *testing.T) {
	capability, err := domain.NewCapability(domain.CapabilitySupported, map[string]string{"bounded": "true"}, map[string]string{"version": "1"})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := domain.NewCapabilityFingerprint(map[string]domain.Capability{"example": capability}, map[string]string{"os": "linux"})
	if err != nil {
		t.Fatal(err)
	}
	mapped := mapCapabilityReport(fingerprint)
	if mapped.Digest != fingerprint.Digest().String() || mapped.Evidence["os"] != "linux" || mapped.Capabilities["example"].Status != "supported" || mapped.Capabilities["example"].Constraints["bounded"] != "true" {
		t.Fatalf("mapped report = %#v", mapped)
	}
	// Public accessors and the report must not alias the immutable domain model.
	mapped.Evidence["os"] = "mutated"
	if fingerprint.Evidence()["os"] != "linux" {
		t.Fatal("mapped report aliases capability fingerprint evidence")
	}
}

func TestReadPolicySourceRequiresNonEmptyBoundedRegularFile(t *testing.T) {
	root := t.TempDir()
	validPath := filepath.Join(root, "policy.yaml")
	if err := os.WriteFile(validPath, []byte("policy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if source, err := readPolicySource(validPath); err != nil || string(source) != "policy" {
		t.Fatalf("read valid policy = %q, %v", source, err)
	}
	emptyPath := filepath.Join(root, "empty.yaml")
	if err := os.WriteFile(emptyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPolicySource(emptyPath); err == nil {
		t.Fatal("empty policy was accepted")
	}
	largePath := filepath.Join(root, "large.yaml")
	if err := os.WriteFile(largePath, bytes.Repeat([]byte{'x'}, int(maximumPolicyBytes)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPolicySource(largePath); err == nil {
		t.Fatal("oversized policy was accepted")
	}
	if _, err := readPolicySource(root); err == nil {
		t.Fatal("directory policy source was accepted")
	}
}

func TestRunRejectsUnprobeableCompositionBeforeContactingDocker(t *testing.T) {
	for _, arguments := range [][]string{{"--observer-driver", "process"}, {"--workspace-driver", "overlayfs"}} {
		if err := run(arguments); err == nil || !strings.Contains(err.Error(), "cannot be truthfully") {
			t.Fatalf("run(%v) error = %v", arguments, err)
		}
	}
}
