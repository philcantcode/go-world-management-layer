package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestVerifyRecordedResultRequiresExactDigestAndDeniedBoundaries(t *testing.T) {
	const digest = "sha256:payload"
	probes := make([]probe, 0, len(requiredBoundaryPaths))
	for _, path := range requiredBoundaryPaths {
		probes = append(probes, probe{Path: path, Accessible: false})
	}
	content, err := json.Marshal(result{InputDigest: digest, Probes: probes})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRecordedResult(content, digest); err != nil {
		t.Fatalf("valid recorded result: %v", err)
	}

	tests := []struct {
		name    string
		digest  string
		mutate  func([]probe) []probe
		message string
	}{
		{name: "wrong digest", digest: "sha256:other", mutate: identityProbes, message: "does not match"},
		{name: "accessible", digest: digest, mutate: func(values []probe) []probe { values[0].Accessible = true; return values }, message: "was accessible"},
		{name: "missing", digest: digest, mutate: func(values []probe) []probe { return values[1:] }, message: "is missing"},
		{name: "duplicate", digest: digest, mutate: func(values []probe) []probe { return append(values, values[0]) }, message: "is duplicated"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated, err := json.Marshal(result{InputDigest: digest, Probes: test.mutate(append([]probe(nil), probes...))})
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyRecordedResult(mutated, test.digest); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v, want message containing %q", err, test.message)
			}
		})
	}
}

func identityProbes(values []probe) []probe { return values }
