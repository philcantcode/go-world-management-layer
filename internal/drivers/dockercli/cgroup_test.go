package dockercli

import (
	"strings"
	"testing"
)

func TestRequireSupportedCgroupVersionFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "unified v2", version: "2"},
		{name: "legacy v1", version: "1"},
		{name: "unreported", version: "", wantErr: true},
		{name: "noncanonical", version: "v2", wantErr: true},
		{name: "surrounding whitespace", version: " 2 ", wantErr: true},
		{name: "future unknown", version: "3", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := RequireSupportedCgroupVersion(test.version)
			if (err != nil) != test.wantErr {
				t.Fatalf("RequireSupportedCgroupVersion(%q) error = %v, wantErr %t", test.version, err, test.wantErr)
			}
		})
	}
}

func TestParseExactCgroupV2PathBindsFullContainerIdentity(t *testing.T) {
	containerID := strings.Repeat("a", 64)
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "cgroupfs leaf", content: "0::/docker/" + containerID + "\n", want: "/docker/" + containerID},
		{name: "systemd leaf", content: "0::/system.slice/docker-" + containerID + ".scope\n", want: "/system.slice/docker-" + containerID + ".scope"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseExactCgroupV2Path(strings.NewReader(test.content), containerID)
			if err != nil || got != test.want {
				t.Fatalf("cgroup path = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestParseExactCgroupV2PathRejectsAmbiguousOrUnboundMembership(t *testing.T) {
	containerID := strings.Repeat("b", 64)
	otherID := strings.Repeat("c", 64)
	tests := []struct {
		name        string
		content     string
		identity    string
		wantMessage string
	}{
		{name: "short Docker ID", content: "0::/docker/short\n", identity: "short", wantMessage: "exactly 64 lower-case hexadecimal"},
		{name: "uppercase Docker ID", content: "0::/docker/" + strings.ToUpper(containerID) + "\n", identity: strings.ToUpper(containerID), wantMessage: "exactly 64 lower-case hexadecimal"},
		{name: "v1 record", content: "5:memory:/docker/" + containerID + "\n", identity: containerID, wantMessage: "unified v2"},
		{name: "hybrid records", content: "0::/docker/" + containerID + "\n5:memory:/docker/" + containerID + "\n", identity: containerID, wantMessage: "unified v2"},
		{name: "root", content: "0::/\n", identity: containerID, wantMessage: "non-root absolute"},
		{name: "relative", content: "0::docker/" + containerID + "\n", identity: containerID, wantMessage: "non-root absolute"},
		{name: "duplicate separator", content: "0::/docker//" + containerID + "\n", identity: containerID, wantMessage: "canonical non-root absolute"},
		{name: "traversal", content: "0::/docker/other/../" + containerID + "\n", identity: containerID, wantMessage: "canonical non-root absolute"},
		{name: "different full ID", content: "0::/docker/" + otherID + "\n", identity: containerID, wantMessage: "not bound to full Docker container ID"},
		{name: "prefix-only ID", content: "0::/docker/" + containerID[:12] + "\n", identity: containerID, wantMessage: "not bound to full Docker container ID"},
		{name: "deleted leaf", content: "0::/docker/" + containerID + " (deleted)\n", identity: containerID, wantMessage: "not bound to full Docker container ID"},
		{name: "CRLF", content: "0::/docker/" + containerID + "\r\n", identity: containerID, wantMessage: "non-canonical"},
		{name: "missing terminator", content: "0::/docker/" + containerID, identity: containerID, wantMessage: "canonical newline terminator"},
		{name: "NUL", content: "0::/docker/" + containerID + "\x00\n", identity: containerID, wantMessage: "invalid text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseExactCgroupV2Path(strings.NewReader(test.content), test.identity)
			if err == nil || got != "" || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("cgroup path = %q, %v; want error containing %q", got, err, test.wantMessage)
			}
		})
	}
}

func TestParseExactDockerCgroupMembershipAcceptsCompleteV1WithoutSynthesizingIdentity(t *testing.T) {
	containerID := strings.Repeat("e", 64)
	containerPath := "/docker/" + containerID
	content := strings.Join([]string{
		"11:memory:" + containerPath,
		"10:cpu,cpuacct:" + containerPath,
		"9:pids:" + containerPath,
		"1:name=systemd:" + containerPath,
	}, "\n") + "\n"
	membership, err := parseExactDockerCgroupMembership(strings.NewReader(content), containerID)
	if err != nil {
		t.Fatal(err)
	}
	if membership.Version != "1" || membership.Path != "" {
		t.Fatalf("cgroup v1 membership = %#v", membership)
	}
}

func TestParseExactDockerCgroupMembershipReportsV2Identity(t *testing.T) {
	containerID := strings.Repeat("f", 64)
	want := "/system.slice/docker-" + containerID + ".scope"
	membership, err := parseExactDockerCgroupMembership(strings.NewReader("0::"+want+"\n"), containerID)
	if err != nil || membership.Version != "2" || membership.Path != want {
		t.Fatalf("cgroup v2 membership = %#v, %v; want %q", membership, err, want)
	}
}

func TestParseExactDockerCgroupMembershipAcceptsDockerDesktopV1Hybrid(t *testing.T) {
	containerID := strings.Repeat("d", 64)
	containerPath := "/docker/" + containerID
	content := strings.Join([]string{
		"11:memory:" + containerPath,
		"10:cpu,cpuacct:" + containerPath,
		"9:pids:" + containerPath,
		"1:name=systemd:" + containerPath,
		"0::" + containerPath,
	}, "\n") + "\n"
	membership, err := parseExactDockerCgroupMembership(strings.NewReader(content), containerID)
	if err != nil || membership.Version != "1" || membership.Path != "" {
		t.Fatalf("Docker Desktop hybrid membership = %#v, %v", membership, err)
	}
}

func TestParseExactDockerCgroupMembershipRejectsIncompleteOrAmbiguousV1(t *testing.T) {
	containerID := strings.Repeat("a", 64)
	otherID := strings.Repeat("b", 64)
	containerPath := "/docker/" + containerID
	tests := map[string]string{
		"missing pids":              "2:memory:" + containerPath + "\n1:cpu,cpuacct:" + containerPath + "\n",
		"duplicate cpu":             "3:memory:" + containerPath + "\n2:cpu:" + containerPath + "\n1:cpu,pids:" + containerPath + "\n",
		"duplicate hierarchy":       "2:memory:" + containerPath + "\n2:cpu,pids:" + containerPath + "\n",
		"different identity":        "3:memory:" + containerPath + "\n2:cpu,cpuacct:" + containerPath + "\n1:pids:/docker/" + otherID + "\n",
		"hybrid path mismatch":      "0::/alternate/" + containerID + "\n3:memory:" + containerPath + "\n2:cpu,cpuacct:" + containerPath + "\n1:pids:" + containerPath + "\n",
		"hybrid duplicate unified":  "0::" + containerPath + "\n0::" + containerPath + "\n3:memory:" + containerPath + "\n2:cpu,cpuacct:" + containerPath + "\n1:pids:" + containerPath + "\n",
		"hybrid partial v1":         "0::" + containerPath + "\n2:memory:" + containerPath + "\n1:cpu,cpuacct:" + containerPath + "\n",
		"hybrid ambiguous v1 paths": "0::" + containerPath + "\n3:memory:" + containerPath + "\n2:cpu,cpuacct:/alternate/" + containerID + "\n1:pids:" + containerPath + "\n",
		"root membership":           "3:memory:/\n2:cpu,cpuacct:/\n1:pids:/\n",
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			if membership, err := parseExactDockerCgroupMembership(strings.NewReader(content), containerID); err == nil {
				t.Fatalf("invalid membership was accepted: %#v", membership)
			}
		})
	}
}

func TestParseExactCgroupV2PathRejectsOversizedMembership(t *testing.T) {
	containerID := strings.Repeat("d", 64)
	content := strings.Repeat("x", int(maximumProcCgroupBytes)+1)
	if got, err := parseExactCgroupV2Path(strings.NewReader(content), containerID); err == nil || got != "" || !strings.Contains(err.Error(), "safety bound") {
		t.Fatalf("oversized cgroup path = %q, %v", got, err)
	}
}
