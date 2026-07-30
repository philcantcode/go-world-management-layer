package policy

import (
	"os"
	"strings"
	"testing"
)

func TestDirectoryCopyDeploymentPolicyCompilesAgainstExplicitCapabilities(t *testing.T) {
	source, err := os.ReadFile("deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	requirements, err := Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	levels := make(map[string]RequirementLevel, len(requirements))
	for _, requirement := range requirements {
		levels[requirement.Name] = requirement.Level
	}
	for _, required := range []string{"host.profile.directory-copy-non-production", "filesystem.directory-copy.non-production", "runtime.oci.runc", "coverage.linux-container.target.lifecycle"} {
		if levels[required] != RequirementRequired {
			t.Fatalf("capability %q level = %q, want required", required, levels[required])
		}
	}
	for _, forbidden := range []string{"filesystem.overlayfs", "filesystem.reflink"} {
		if _, found := levels[forbidden]; found {
			t.Fatalf("directory-copy deployment unexpectedly requires %q", forbidden)
		}
	}
	compilePolicy(t, source, fingerprintFor(t, source, nil, nil))
}

func TestPolicyRejectsMalformedDigestShapedIdentities(t *testing.T) {
	source, err := os.ReadFile("deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	malformedImage := []byte(strings.Replace(string(source),
		"@sha256:6105d6cc76af4009c44e4692f219054456e7111487afb0c71077d9f887668fef",
		"@sha256:not-a-digest", 1))
	if _, err := Requirements(malformedImage); err == nil {
		t.Fatal("malformed digest-shaped image identity was accepted")
	}
}
