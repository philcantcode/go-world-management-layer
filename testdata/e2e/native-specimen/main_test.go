package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestDetachedChildWritesReadinessBeforeDelayedMutation(t *testing.T) {
	root := t.TempDir()
	ready := filepath.Join(root, "ready.txt")
	output := filepath.Join(root, "output.txt")
	if err := runDetachedChild(ready, output, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(ready); err != nil || string(content) != "ready\n" {
		t.Fatalf("readiness marker = %q, %v", content, err)
	}
	if content, err := os.ReadFile(output); err != nil || string(content) != "escaped process survived\n" {
		t.Fatalf("output marker = %q, %v", content, err)
	}
	if err := runDetachedChild(ready, ready, time.Millisecond); err == nil {
		t.Fatal("invalid detached paths were accepted")
	}
}

var exactCgroupExpectation = cgroupExpectation{
	CPUMilli: 500, MemoryBytes: 256 << 20, SwapBytes: 0, PIDs: 128,
}

const canonicalV1Membership = "11:memory:/world/memory\n10:cpu,cpuacct:/world/cpu\n9:pids:/world/tasks\n8:cpuset:/world/other\n"

const canonicalV1Mountinfo = "31 20 0:31 / /sys/fs/cgroup/cpu,cpuacct ro - cgroup cgroup rw,cpu,cpuacct\n" +
	"32 20 0:32 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n" +
	"33 20 0:33 / /sys/fs/cgroup/pids ro - cgroup cgroup rw,pids\n"

const canonicalV2Mountinfo = "41 20 0:41 / /sys/fs/cgroup ro - cgroup2 cgroup rw\n"

func TestResultUsesVersionedGenericCgroupReport(t *testing.T) {
	report := cgroupReport{Version: 1, Paths: cgroupPaths{CPU: "/cpu", Memory: "/memory", PIDs: "/pids"}}
	encoded, err := json.Marshal(result{Cgroup: &report})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["cgroup_v2"]; exists {
		t.Fatal("legacy version-specific cgroup report key was emitted")
	}
	var decoded cgroupReport
	if err := json.Unmarshal(fields["cgroup"], &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 1 || decoded.Paths != report.Paths {
		t.Fatalf("generic cgroup report = %#v", decoded)
	}
}

func TestVerifyCgroupLimitsReadsExactV1ControllersAndDerivesZeroSwap(t *testing.T) {
	membership, mountinfo, root := writeCgroupFixture(t, canonicalV1Membership, canonicalV1Mountinfo, exactV1Files())
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != 1 || report.Paths.CPU != "/world/cpu" ||
		report.Paths.Memory != "/world/memory" || report.Paths.PIDs != "/world/tasks" ||
		report.CPUQuota != 50000 || report.CPUPeriod != 100000 ||
		report.CPUMilli != exactCgroupExpectation.CPUMilli || report.MemoryBytes != exactCgroupExpectation.MemoryBytes ||
		report.SwapBytes != 0 || report.PIDs != exactCgroupExpectation.PIDs {
		t.Fatalf("cgroup v1 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsAcceptsDockerDesktopV1HybridAndMountRoots(t *testing.T) {
	const dockerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	group := "/docker/" + dockerID
	membershipText := "11:memory:" + group + "\n" +
		"10:cpuacct,cpu:" + group + "\n" +
		"9:pids:" + group + "\n" +
		"0::" + group + "\n"
	mountinfoText := "637 620 0:37 " + group + " /sys/fs/cgroup/cpu ro - cgroup cgroup rw,cpu,cpuacct\n" +
		"638 620 0:38 " + group + " /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n" +
		"639 620 0:39 " + group + " /sys/fs/cgroup/pids ro - cgroup cgroup rw,pids\n"
	files := map[string]string{
		"cpu/cpu.cfs_quota_us":               "50000\n",
		"cpu/cpu.cfs_period_us":              "100000\n",
		"memory/memory.limit_in_bytes":       "268435456\n",
		"memory/memory.memsw.limit_in_bytes": "268435456\n",
		"pids/pids.max":                      "128\n",
	}
	membership, mountinfo, root := writeCgroupFixture(t, membershipText, mountinfoText, files)
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := cgroupPaths{CPU: group, Memory: group, PIDs: group}
	if report.Version != 1 || report.Paths != wantPaths || report.CPUMilli != 500 || report.SwapBytes != 0 {
		t.Fatalf("Docker Desktop cgroup v1 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsUsesV1MountControllerOptionsInsteadOfDirectoryNames(t *testing.T) {
	mounts := "31 20 0:31 / /sys/fs/cgroup/limits-a ro - cgroup cgroup rw,cpuacct,cpu\n" +
		"32 20 0:32 / /sys/fs/cgroup/limits-b ro - cgroup cgroup rw,memory\n" +
		"33 20 0:33 / /sys/fs/cgroup/limits-c ro - cgroup cgroup rw,pids\n"
	files := map[string]string{
		"limits-a/world/cpu/cpu.cfs_quota_us":               "50000\n",
		"limits-a/world/cpu/cpu.cfs_period_us":              "100000\n",
		"limits-b/world/memory/memory.limit_in_bytes":       "268435456\n",
		"limits-b/world/memory/memory.memsw.limit_in_bytes": "268435456\n",
		"limits-c/world/tasks/pids.max":                     "128\n",
	}
	membership, mountinfo, root := writeCgroupFixture(t, canonicalV1Membership, mounts, files)
	if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyCgroupLimitsAcceptsPrivateV1NamespaceWithIndividualMountAlias(t *testing.T) {
	files := map[string]string{
		"cpu/cpu.cfs_quota_us":               "50000\n",
		"cpu/cpu.cfs_period_us":              "100000\n",
		"memory/memory.limit_in_bytes":       "268435456\n",
		"memory/memory.memsw.limit_in_bytes": "268435456\n",
		"pids/pids.max":                      "128\n",
	}
	mounts := "31 20 0:31 / /sys/fs/cgroup/cpu ro - cgroup cgroup rw,cpu,cpuacct\n" +
		"32 20 0:32 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n" +
		"33 20 0:33 / /sys/fs/cgroup/pids ro - cgroup cgroup rw,pids\n"
	membership, mountinfo, root := writeCgroupFixture(t, "3:cpuacct,cpu:/\n2:memory:/\n1:pids:/\n", mounts, files)
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != 1 || report.Paths != (cgroupPaths{CPU: "/", Memory: "/", PIDs: "/"}) || report.SwapBytes != 0 {
		t.Fatalf("private cgroup v1 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsAcceptsPrivateV2NamespaceRoot(t *testing.T) {
	membership, mountinfo, root := writeCgroupFixture(t, "0::/\n", canonicalV2Mountinfo, exactV2Files(""))
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != 2 || report.Paths != (cgroupPaths{CPU: "/", Memory: "/", PIDs: "/"}) ||
		report.CPUQuota != 50000 || report.CPUPeriod != 100000 || report.SwapBytes != 0 {
		t.Fatalf("private cgroup v2 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsReadsNestedV2Controllers(t *testing.T) {
	membership, mountinfo, root := writeCgroupFixture(t, "0::/world/test\n", canonicalV2Mountinfo, exactV2Files("world/test"))
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != 2 || report.Paths.CPU != "/world/test" || report.CPUMilli != 500 ||
		report.MemoryBytes != 268435456 || report.SwapBytes != 0 || report.PIDs != 128 {
		t.Fatalf("nested cgroup v2 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsMapsV2MembershipRelativeToSubtreeMountRoot(t *testing.T) {
	mounts := "41 20 0:41 /docker/container /sys/fs/cgroup ro - cgroup2 cgroup rw\n"
	membership, mountinfo, root := writeCgroupFixture(
		t, "0::/docker/container/world/test\n", mounts, exactV2Files("world/test"),
	)
	report, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != 2 || report.Paths.CPU != "/docker/container/world/test" || report.CPUMilli != 500 {
		t.Fatalf("subtree-mounted cgroup v2 report = %#v", report)
	}
}

func TestVerifyCgroupLimitsRejectsMalformedOrContradictoryMembership(t *testing.T) {
	tests := []struct {
		name       string
		membership string
	}{
		{name: "empty", membership: ""},
		{name: "missing newline", membership: "0::/world/test"},
		{name: "duplicate unified", membership: "0::/world/one\n0::/world/two\n"},
		{name: "mixed v1 v2", membership: "0::/world/unified\n1:cpu:/world/legacy\n"},
		{name: "complete mixed paths differ", membership: canonicalV1Membership + "0::/world/unified\n"},
		{name: "partial same-path legacy", membership: "0::/world/test\n1:cpu:/world/test\n"},
		{name: "v2 names controller", membership: "0:cpu:/world/test\n"},
		{name: "missing pids", membership: "2:cpu:/world/cpu\n1:memory:/world/memory\n"},
		{name: "duplicate required controller", membership: "4:cpu:/world/one\n3:cpu,cpuacct:/world/two\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "duplicate controller in hierarchy", membership: "3:cpu,cpu:/world/cpu\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "duplicate hierarchy", membership: "2:cpu:/world/cpu\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "noncanonical hierarchy", membership: "03:cpu:/world/cpu\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "empty controller", membership: "3:cpu,:/world/cpu\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "uppercase controller", membership: "4:CPU:/world/other\n3:cpu:/world/cpu\n2:memory:/world/memory\n1:pids:/world/pids\n"},
		{name: "relative path", membership: "0::world/test\n"},
		{name: "dot traversal", membership: "0::/world/../escape\n"},
		{name: "backslash traversal", membership: "0::/world\\..\\escape\n"},
		{name: "noncanonical slash", membership: "0::/world//test\n"},
		{name: "invalid utf8", membership: "0::/world/\xff\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			membership, mountinfo, root := writeCgroupFixture(t, test.membership, canonicalV2Mountinfo, nil)
			if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil {
				t.Fatal("malformed or contradictory membership was accepted")
			}
		})
	}
}

func TestVerifyCgroupLimitsRejectsOversizedMembership(t *testing.T) {
	membershipText := "0::/" + strings.Repeat("a", int(maximumCgroupMembershipBytes)) + "\n"
	membership, mountinfo, root := writeCgroupFixture(t, membershipText, canonicalV2Mountinfo, nil)
	if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil {
		t.Fatal("oversized cgroup membership was accepted")
	}
}

func TestVerifyCgroupLimitsRejectsOversizedOrMalformedMountinfo(t *testing.T) {
	tests := []struct {
		name      string
		mountinfo string
	}{
		{name: "oversized", mountinfo: strings.Repeat("x", int(maximumCgroupMountinfoBytes)+1)},
		{name: "missing newline", mountinfo: strings.TrimSuffix(canonicalV2Mountinfo, "\n")},
		{name: "noncanonical spacing", mountinfo: "41  20 0:41 / /sys/fs/cgroup ro - cgroup2 cgroup rw\n"},
		{name: "missing separator", mountinfo: "41 20 0:41 / /sys/fs/cgroup ro cgroup2 cgroup rw\n"},
		{name: "duplicate mount ID", mountinfo: canonicalV2Mountinfo + "41 20 0:42 / /elsewhere ro - tmpfs tmpfs rw\n"},
		{name: "traversing mountpoint", mountinfo: "41 20 0:41 / /sys/fs/cgroup/../escape ro - cgroup2 cgroup rw\n"},
		{name: "traversing mount root", mountinfo: "41 20 0:41 /docker/../escape /sys/fs/cgroup ro - cgroup2 cgroup rw\n"},
		{name: "unsafe decoded mountpoint", mountinfo: "41 20 0:41 / /sys/fs/cgroup/evil\\134name ro - cgroup2 cgroup rw\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			membership, mountinfo, root := writeCgroupFixture(t, "0::/world/test\n", test.mountinfo, nil)
			if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil {
				t.Fatal("oversized or malformed mountinfo was accepted")
			}
		})
	}
}

func TestVerifyCgroupLimitsRejectsMembershipOutsideMountedSubtree(t *testing.T) {
	mounts := "41 20 0:41 /docker/other /sys/fs/cgroup ro - cgroup2 cgroup rw\n"
	membership, mountinfo, root := writeCgroupFixture(t, "0::/docker/container\n", mounts, exactV2Files(""))
	if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("membership outside mounted subtree error = %v", err)
	}
}

func TestVerifyCgroupLimitsRejectsAmbiguousV1ControllerMounts(t *testing.T) {
	files := exactV1Files()
	files["cpu/world/cpu/cpu.cfs_quota_us"] = "25000\n"
	files["cpu/world/cpu/cpu.cfs_period_us"] = "100000\n"
	mounts := canonicalV1Mountinfo +
		"34 20 0:31 / /sys/fs/cgroup/cpu ro - cgroup cgroup rw,cpu,cpuacct\n"
	membership, mountinfo, root := writeCgroupFixture(t, canonicalV1Membership, mounts, files)
	if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous cgroup v1 CPU mounts error = %v", err)
	}
}

func TestVerifyCgroupLimitsRejectsAmbiguousV2Mounts(t *testing.T) {
	files := exactV2Files("world/test")
	for relative, value := range exactV2Files("unified/world/test") {
		files[relative] = value
	}
	mounts := canonicalV2Mountinfo +
		"42 20 0:41 / /sys/fs/cgroup/unified ro - cgroup2 cgroup rw\n"
	membership, mountinfo, root := writeCgroupFixture(t, "0::/world/test\n", mounts, files)
	if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous cgroup v2 mounts error = %v", err)
	}
}

func TestVerifyCgroupLimitsRejectsMalformedUnboundedOrMismatchedV2Controls(t *testing.T) {
	tests := []struct {
		name, control, value string
	}{
		{name: "unbounded cpu", control: "cpu.max", value: "max 100000\n"},
		{name: "noncanonical cpu spacing", control: "cpu.max", value: "50000  100000\n"},
		{name: "fractional milli cpu", control: "cpu.max", value: "50001 100000\n"},
		{name: "different cpu", control: "cpu.max", value: "25000 100000\n"},
		{name: "unbounded memory", control: "memory.max", value: "max\n"},
		{name: "different memory", control: "memory.max", value: "134217728\n"},
		{name: "unbounded swap", control: "memory.swap.max", value: "max\n"},
		{name: "different swap", control: "memory.swap.max", value: "1\n"},
		{name: "unbounded pids", control: "pids.max", value: "max\n"},
		{name: "different pids", control: "pids.max", value: "64\n"},
		{name: "missing newline", control: "pids.max", value: "128"},
		{name: "oversized", control: "memory.max", value: strings.Repeat("1", int(maximumCgroupControlBytes)+1) + "\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := exactV2Files("world/test")
			files["world/test/"+test.control] = test.value
			membership, mountinfo, root := writeCgroupFixture(t, "0::/world/test\n", canonicalV2Mountinfo, files)
			if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil {
				t.Fatal("malformed, unbounded, or mismatched cgroup v2 control was accepted")
			}
		})
	}
}

func TestVerifyCgroupLimitsRejectsMalformedUnboundedOrMismatchedV1Controls(t *testing.T) {
	tests := []struct {
		name, control, value string
	}{
		{name: "unbounded cpu", control: "cpu,cpuacct/world/cpu/cpu.cfs_quota_us", value: "-1\n"},
		{name: "different cpu", control: "cpu,cpuacct/world/cpu/cpu.cfs_quota_us", value: "25000\n"},
		{name: "zero period", control: "cpu,cpuacct/world/cpu/cpu.cfs_period_us", value: "0\n"},
		{name: "unbounded memory sentinel", control: "memory/world/memory/memory.limit_in_bytes", value: "9223372036854771712\n"},
		{name: "different memory", control: "memory/world/memory/memory.limit_in_bytes", value: "134217728\n"},
		{name: "unbounded memsw sentinel", control: "memory/world/memory/memory.memsw.limit_in_bytes", value: "9223372036854771712\n"},
		{name: "memsw below memory", control: "memory/world/memory/memory.memsw.limit_in_bytes", value: "134217728\n"},
		{name: "nonzero derived swap", control: "memory/world/memory/memory.memsw.limit_in_bytes", value: "268435457\n"},
		{name: "unbounded pids", control: "pids/world/tasks/pids.max", value: "max\n"},
		{name: "different pids", control: "pids/world/tasks/pids.max", value: "64\n"},
		{name: "multiple records", control: "pids/world/tasks/pids.max", value: "128\n128\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := exactV1Files()
			files[test.control] = test.value
			membership, mountinfo, root := writeCgroupFixture(t, canonicalV1Membership, canonicalV1Mountinfo, files)
			if _, err := verifyCgroupLimits(exactCgroupExpectation, membership, mountinfo, root); err == nil {
				t.Fatal("malformed, unbounded, or mismatched cgroup v1 control was accepted")
			}
		})
	}
}

func exactV2Files(prefix string) map[string]string {
	return map[string]string{
		joinFixturePath(prefix, "cpu.max"):         "50000 100000\n",
		joinFixturePath(prefix, "memory.max"):      "268435456\n",
		joinFixturePath(prefix, "memory.swap.max"): "0\n",
		joinFixturePath(prefix, "pids.max"):        "128\n",
	}
}

func exactV1Files() map[string]string {
	return map[string]string{
		"cpu,cpuacct/world/cpu/cpu.cfs_quota_us":          "50000\n",
		"cpu,cpuacct/world/cpu/cpu.cfs_period_us":         "100000\n",
		"memory/world/memory/memory.limit_in_bytes":       "268435456\n",
		"memory/world/memory/memory.memsw.limit_in_bytes": "268435456\n",
		"pids/world/tasks/pids.max":                       "128\n",
	}
}

func joinFixturePath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

func writeCgroupFixture(t *testing.T, membershipText, mountinfoText string, files map[string]string) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	membership := filepath.Join(root, "self.cgroup")
	if err := os.WriteFile(membership, []byte(membershipText), 0o600); err != nil {
		t.Fatal(err)
	}
	mountinfo := filepath.Join(root, "self.mountinfo")
	if err := os.WriteFile(mountinfo, []byte(mountinfoText), 0o600); err != nil {
		t.Fatal(err)
	}
	cgroupRoot := filepath.Join(root, "cgroup")
	if err := os.MkdirAll(cgroupRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for relative, value := range files {
		filename := filepath.Join(cgroupRoot, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return membership, mountinfo, cgroupRoot
}
