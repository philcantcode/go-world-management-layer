// native-specimen is a deterministic hostile-input fixture used only by the
// end-to-end Docker and Android qualification harnesses.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

type probe struct {
	Path       string `json:"path"`
	Accessible bool   `json:"accessible"`
	Error      string `json:"error,omitempty"`
}

type result struct {
	InputPath   string        `json:"input_path"`
	InputDigest string        `json:"input_digest"`
	InputSize   int           `json:"input_size"`
	Probes      []probe       `json:"boundary_probes"`
	Cgroup      *cgroupReport `json:"cgroup,omitempty"`
	StartedAt   time.Time     `json:"started_at"`
	FinishedAt  time.Time     `json:"finished_at"`
}

type cgroupExpectation struct {
	CPUMilli    int64
	MemoryBytes int64
	SwapBytes   int64
	PIDs        int64
}

type cgroupPaths struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	PIDs   string `json:"pids"`
}

type cgroupReport struct {
	Version     int         `json:"version"`
	Paths       cgroupPaths `json:"paths"`
	CPUQuota    int64       `json:"cpu_quota"`
	CPUPeriod   int64       `json:"cpu_period"`
	CPUMilli    int64       `json:"cpu_milli"`
	MemoryBytes int64       `json:"memory_bytes"`
	SwapBytes   int64       `json:"swap_bytes"`
	PIDs        int64       `json:"pids"`
}

var requiredBoundaryPaths = []string{
	"/workspace",
	"/var/run/docker.sock",
	"/run/containerd/containerd.sock",
	"/proc/1/root/workspace",
}

func main() {
	input := flag.String("input", "/target/input/payload.txt", "input file")
	output := flag.String("output", "/target/result.json", "result file")
	delay := flag.Duration("sleep", 0, "bounded cancellation fixture delay")
	exitCode := flag.Int("exit", 0, "requested terminal exit code")
	outputBytes := flag.Int("output-bytes", 0, "append deterministic padding bytes")
	verifyResult := flag.Bool("verify-result", false, "verify a prior specimen result and its isolation probes")
	expectedInputDigest := flag.String("expected-input-digest", "", "required input digest for -verify-result")
	detachedReady := flag.String("detached-ready", "", "write this readiness marker from a detached setsid child")
	detachedOutput := flag.String("detached-output", "", "write this delayed mutation from a detached setsid child")
	detachedDelay := flag.Duration("detached-delay", 0, "delay before the detached child writes its mutation")
	detachedChild := flag.Bool("detached-child", false, "internal detached child mode")
	verifyCgroup := flag.Bool("verify-cgroup", false, "verify the exact live cgroup resource limits")
	expectedCPUMilli := flag.Int64("expected-cpu-milli", 0, "expected live cgroup CPU quota in milli-CPU")
	expectedMemoryBytes := flag.Int64("expected-memory-bytes", 0, "expected live cgroup memory limit in bytes")
	expectedSwapBytes := flag.Int64("expected-swap-bytes", 0, "expected live cgroup swap-only limit in bytes")
	expectedPIDs := flag.Int64("expected-pids", 0, "expected live cgroup PID limit")
	flag.Parse()
	if *detachedChild {
		if err := runDetachedChild(*detachedReady, *detachedOutput, *detachedDelay); err != nil {
			fatal(err, 7)
		}
		return
	}
	if *detachedReady != "" || *detachedOutput != "" {
		pid, err := startDetachedChild(*detachedReady, *detachedOutput, *detachedDelay)
		if err != nil {
			fatal(err, 7)
		}
		fmt.Printf("detached_pid=%d\n", pid)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now().UTC()
	if *delay > 0 {
		timer := time.NewTimer(*delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			os.Exit(124)
		case <-timer.C:
		}
	}
	content, err := os.ReadFile(*input)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *verifyResult {
		if err := verifyRecordedResult(content, *expectedInputDigest); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(6)
		}
	}
	sum := sha256.Sum256(content)
	value := result{InputPath: *input, InputDigest: "sha256:" + hex.EncodeToString(sum[:]), InputSize: len(content), StartedAt: started}
	if *verifyCgroup {
		report, err := verifyCgroupLimits(cgroupExpectation{
			CPUMilli: *expectedCPUMilli, MemoryBytes: *expectedMemoryBytes,
			SwapBytes: *expectedSwapBytes, PIDs: *expectedPIDs,
		}, "/proc/self/cgroup", "/proc/self/mountinfo", "/sys/fs/cgroup")
		if err != nil {
			fatal(err, 8)
		}
		value.Cgroup = &report
	}
	for _, path := range requiredBoundaryPaths {
		_, probeErr := os.Stat(path)
		item := probe{Path: path, Accessible: probeErr == nil}
		if probeErr != nil {
			item.Error = probeErr.Error()
		}
		value.Probes = append(value.Probes, item)
	}
	value.FinishedAt = time.Now().UTC()
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if *outputBytes > 0 {
		encoded = append(encoded, []byte(strings.Repeat("x", *outputBytes))...)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	if err := os.WriteFile(*output, append(encoded, '\n'), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}
	fmt.Printf("%s\n", value.InputDigest)
	os.Exit(*exitCode)
}

const (
	maximumCgroupMembershipBytes = int64(64 << 10)
	maximumCgroupMountinfoBytes  = int64(1 << 20)
	maximumCgroupControlBytes    = int64(256)
	cgroupFilesystemMountpoint   = "/sys/fs/cgroup"
)

type cgroupV1Record struct {
	path string
}

type cgroupLayout struct {
	version    int
	unified    string
	controller map[string]cgroupV1Record
}

type cgroupMount struct {
	root        string
	mountpoint  string
	filesystem  string
	superOption map[string]bool
}

type cgroupControlReader func(string) (string, error)

var requiredCgroupV1Controllers = []string{"cpu", "memory", "pids"}

func verifyCgroupLimits(expected cgroupExpectation, membershipPath, mountinfoPath, filesystemRoot string) (cgroupReport, error) {
	if expected.CPUMilli <= 0 || expected.MemoryBytes <= 0 || expected.SwapBytes < 0 || expected.PIDs <= 0 {
		return cgroupReport{}, fmt.Errorf("cgroup verification requires positive CPU, memory, and PID limits plus non-negative swap")
	}
	if filesystemRoot == "" || !filepath.IsAbs(filesystemRoot) || filepath.Clean(filesystemRoot) != filesystemRoot {
		return cgroupReport{}, fmt.Errorf("cgroup filesystem root %q is not canonical and absolute", filesystemRoot)
	}
	membership, err := readBoundedControlFile(membershipPath, maximumCgroupMembershipBytes)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("read cgroup membership: %w", err)
	}
	layout, err := parseCanonicalCgroupMembership(membership)
	if err != nil {
		return cgroupReport{}, err
	}
	mountinfo, err := readBoundedControlFile(mountinfoPath, maximumCgroupMountinfoBytes)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("read cgroup mountinfo: %w", err)
	}
	mounts, err := parseCanonicalCgroupMounts(mountinfo, filesystemRoot)
	if err != nil {
		return cgroupReport{}, err
	}
	var report cgroupReport
	switch layout.version {
	case 1:
		report, err = readCgroupV1Report(layout, mounts)
	case 2:
		report, err = readCgroupV2Report(layout, mounts)
	default:
		return cgroupReport{}, fmt.Errorf("unsupported cgroup layout version %d", layout.version)
	}
	if err != nil {
		return cgroupReport{}, err
	}
	if report.CPUMilli != expected.CPUMilli || report.MemoryBytes != expected.MemoryBytes || report.SwapBytes != expected.SwapBytes || report.PIDs != expected.PIDs {
		return report, fmt.Errorf(
			"live cgroup v%d limits cpu=%d memory=%d swap=%d pids=%d differ from exact plan cpu=%d memory=%d swap=%d pids=%d",
			report.Version, report.CPUMilli, report.MemoryBytes, report.SwapBytes, report.PIDs,
			expected.CPUMilli, expected.MemoryBytes, expected.SwapBytes, expected.PIDs,
		)
	}
	return report, nil
}

func parseCanonicalCgroupMembership(content string) (cgroupLayout, error) {
	body, err := canonicalNewlineTerminatedBody(content, "process cgroup membership")
	if err != nil {
		return cgroupLayout{}, err
	}
	lines := strings.Split(body, "\n")
	layout := cgroupLayout{controller: make(map[string]cgroupV1Record)}
	seenHierarchies := make(map[int64]bool)
	seenUnified := false
	for _, line := range lines {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			return cgroupLayout{}, fmt.Errorf("cgroup membership record %q does not have three fields", line)
		}
		hierarchyText, controllerText, group := fields[0], fields[1], fields[2]
		if err := requireCanonicalCgroupPath(group); err != nil {
			return cgroupLayout{}, err
		}
		if hierarchyText == "0" {
			if controllerText != "" {
				return cgroupLayout{}, fmt.Errorf("unified cgroup record %q has legacy controllers", line)
			}
			if seenUnified {
				return cgroupLayout{}, fmt.Errorf("cgroup membership contains duplicate unified records")
			}
			seenUnified = true
			layout.unified = group
			continue
		}
		hierarchy, err := parseCanonicalCgroupInteger(hierarchyText, false)
		if err != nil {
			return cgroupLayout{}, fmt.Errorf("parse cgroup hierarchy %q: %w", hierarchyText, err)
		}
		if seenHierarchies[hierarchy] {
			return cgroupLayout{}, fmt.Errorf("cgroup membership contains duplicate hierarchy %d", hierarchy)
		}
		seenHierarchies[hierarchy] = true
		if controllerText == "" {
			return cgroupLayout{}, fmt.Errorf("legacy cgroup hierarchy %d has no controllers", hierarchy)
		}
		controllers := strings.Split(controllerText, ",")
		record := cgroupV1Record{path: group}
		for _, controller := range controllers {
			if !isCanonicalControllerName(controller) {
				return cgroupLayout{}, fmt.Errorf("cgroup controller %q is not canonical", controller)
			}
			if _, exists := layout.controller[controller]; exists {
				return cgroupLayout{}, fmt.Errorf("cgroup controller %q is duplicated or ambiguous", controller)
			}
			layout.controller[controller] = record
		}
	}
	if seenUnified && len(seenHierarchies) == 0 {
		layout.version = 2
		return layout, nil
	}
	for _, required := range requiredCgroupV1Controllers {
		record, exists := layout.controller[required]
		if !exists {
			return cgroupLayout{}, fmt.Errorf("cgroup v1 membership is missing the %q controller", required)
		}
		if seenUnified && record.path != layout.unified {
			return cgroupLayout{}, fmt.Errorf(
				"mixed cgroup membership has unified path %q but %s path %q",
				layout.unified, required, record.path,
			)
		}
	}
	layout.version = 1
	return layout, nil
}

func readCgroupV2Report(layout cgroupLayout, mounts []cgroupMount) (cgroupReport, error) {
	directory, err := resolveCgroupDirectory(mounts, "cgroup2", "", layout.unified)
	if err != nil {
		return cgroupReport{}, err
	}
	read := newCgroupControlReader(directory)
	cpuLine, err := readCanonicalControlLine(read, "cpu.max")
	if err != nil {
		return cgroupReport{}, err
	}
	cpuFields := strings.Split(cpuLine, " ")
	if len(cpuFields) != 2 || cpuFields[0] == "" || cpuFields[1] == "" {
		return cgroupReport{}, fmt.Errorf("cpu.max %q is not one canonical quota/period pair", cpuLine)
	}
	if cpuFields[0] == "max" {
		return cgroupReport{}, fmt.Errorf("cpu.max is unbounded")
	}
	quota, err := parseCanonicalCgroupInteger(cpuFields[0], false)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("parse cpu.max quota: %w", err)
	}
	period, err := parseCanonicalCgroupInteger(cpuFields[1], false)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("parse cpu.max period: %w", err)
	}
	cpuMilli, err := exactCPUMilli(quota, period)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("cpu.max: %w", err)
	}
	memory, err := readFiniteCgroupLimit(read, "memory.max", false, "max")
	if err != nil {
		return cgroupReport{}, err
	}
	swap, err := readFiniteCgroupLimit(read, "memory.swap.max", true, "max")
	if err != nil {
		return cgroupReport{}, err
	}
	pids, err := readFiniteCgroupLimit(read, "pids.max", false, "max")
	if err != nil {
		return cgroupReport{}, err
	}
	return cgroupReport{
		Version:  2,
		Paths:    cgroupPaths{CPU: layout.unified, Memory: layout.unified, PIDs: layout.unified},
		CPUQuota: quota, CPUPeriod: period, CPUMilli: cpuMilli,
		MemoryBytes: memory, SwapBytes: swap, PIDs: pids,
	}, nil
}

func readCgroupV1Report(layout cgroupLayout, mounts []cgroupMount) (cgroupReport, error) {
	cpuRecord := layout.controller["cpu"]
	memoryRecord := layout.controller["memory"]
	pidsRecord := layout.controller["pids"]
	cpuDirectory, err := resolveCgroupDirectory(mounts, "cgroup", "cpu", cpuRecord.path)
	if err != nil {
		return cgroupReport{}, err
	}
	memoryDirectory, err := resolveCgroupDirectory(mounts, "cgroup", "memory", memoryRecord.path)
	if err != nil {
		return cgroupReport{}, err
	}
	pidsDirectory, err := resolveCgroupDirectory(mounts, "cgroup", "pids", pidsRecord.path)
	if err != nil {
		return cgroupReport{}, err
	}
	cpuRead := newCgroupControlReader(cpuDirectory)
	memoryRead := newCgroupControlReader(memoryDirectory)
	pidsRead := newCgroupControlReader(pidsDirectory)
	quota, err := readFiniteCgroupLimit(cpuRead, "cpu.cfs_quota_us", false, "-1")
	if err != nil {
		return cgroupReport{}, err
	}
	period, err := readFiniteCgroupLimit(cpuRead, "cpu.cfs_period_us", false)
	if err != nil {
		return cgroupReport{}, err
	}
	cpuMilli, err := exactCPUMilli(quota, period)
	if err != nil {
		return cgroupReport{}, fmt.Errorf("cgroup v1 CPU quota/period: %w", err)
	}
	memory, err := readFiniteV1MemoryLimit(memoryRead, "memory.limit_in_bytes")
	if err != nil {
		return cgroupReport{}, err
	}
	memsw, err := readFiniteV1MemoryLimit(memoryRead, "memory.memsw.limit_in_bytes")
	if err != nil {
		return cgroupReport{}, err
	}
	if memsw < memory {
		return cgroupReport{}, fmt.Errorf("memory.memsw.limit_in_bytes %d is smaller than memory.limit_in_bytes %d", memsw, memory)
	}
	swap := memsw - memory
	pids, err := readFiniteCgroupLimit(pidsRead, "pids.max", false, "max")
	if err != nil {
		return cgroupReport{}, err
	}
	return cgroupReport{
		Version:  1,
		Paths:    cgroupPaths{CPU: cpuRecord.path, Memory: memoryRecord.path, PIDs: pidsRecord.path},
		CPUQuota: quota, CPUPeriod: period, CPUMilli: cpuMilli,
		MemoryBytes: memory, SwapBytes: swap, PIDs: pids,
	}, nil
}

func parseCanonicalCgroupMounts(content, filesystemRoot string) ([]cgroupMount, error) {
	body, err := canonicalNewlineTerminatedBody(content, "process mountinfo")
	if err != nil {
		return nil, err
	}
	seenMountIDs := make(map[int64]bool)
	var mounts []cgroupMount
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Split(line, " ")
		for _, field := range fields {
			if field == "" {
				return nil, fmt.Errorf("mountinfo record %q does not use canonical field spacing", line)
			}
		}
		separator := -1
		for index := 6; index < len(fields); index++ {
			if fields[index] == "-" {
				separator = index
				break
			}
		}
		if separator < 6 || len(fields) != separator+4 {
			return nil, fmt.Errorf("mountinfo record %q does not have canonical fields", line)
		}
		mountID, err := parseCanonicalCgroupInteger(fields[0], false)
		if err != nil {
			return nil, fmt.Errorf("parse mountinfo mount ID %q: %w", fields[0], err)
		}
		if seenMountIDs[mountID] {
			return nil, fmt.Errorf("mountinfo contains duplicate mount ID %d", mountID)
		}
		seenMountIDs[mountID] = true
		if _, err := parseCanonicalCgroupInteger(fields[1], false); err != nil {
			return nil, fmt.Errorf("parse mountinfo parent ID %q: %w", fields[1], err)
		}
		if err := requireCanonicalMountDevice(fields[2]); err != nil {
			return nil, err
		}
		filesystem := fields[separator+1]
		if filesystem != "cgroup" && filesystem != "cgroup2" {
			continue
		}
		root, err := decodeMountInfoPath(fields[3])
		if err != nil {
			return nil, fmt.Errorf("decode mountinfo root %q: %w", fields[3], err)
		}
		mountpoint, err := decodeMountInfoPath(fields[4])
		if err != nil {
			return nil, fmt.Errorf("decode mountinfo mountpoint %q: %w", fields[4], err)
		}
		physicalMountpoint, included, err := mapCgroupMountpoint(filesystemRoot, mountpoint)
		if err != nil {
			return nil, err
		}
		if !included {
			continue
		}
		if err := requireCanonicalCgroupPath(root); err != nil {
			return nil, fmt.Errorf("mountinfo cgroup root: %w", err)
		}
		superOptions := make(map[string]bool)
		for _, option := range strings.Split(fields[separator+3], ",") {
			if option == "" {
				return nil, fmt.Errorf("mountinfo cgroup record %q contains an empty super option", line)
			}
			superOptions[option] = true
		}
		mounts = append(mounts, cgroupMount{
			root: root, mountpoint: physicalMountpoint,
			filesystem: filesystem, superOption: superOptions,
		})
	}
	return mounts, nil
}

func requireCanonicalMountDevice(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return fmt.Errorf("mountinfo device %q is not one major:minor pair", value)
	}
	for _, part := range parts {
		if _, err := parseCanonicalCgroupInteger(part, true); err != nil {
			return fmt.Errorf("parse mountinfo device %q: %w", value, err)
		}
	}
	return nil
}

func decodeMountInfoPath(value string) (string, error) {
	var decoded strings.Builder
	decoded.Grow(len(value))
	for index := 0; index < len(value); {
		if value[index] != '\\' {
			decoded.WriteByte(value[index])
			index++
			continue
		}
		if index+4 > len(value) {
			return "", fmt.Errorf("path %q ends in a truncated escape", value)
		}
		escape := value[index+1 : index+4]
		switch escape {
		case "040":
			decoded.WriteByte(' ')
		case "011":
			decoded.WriteByte('\t')
		case "012":
			decoded.WriteByte('\n')
		case "134":
			decoded.WriteByte('\\')
		default:
			return "", fmt.Errorf("path %q contains unsupported escape \\%s", value, escape)
		}
		index += 4
	}
	return decoded.String(), nil
}

func mapCgroupMountpoint(filesystemRoot, mountpoint string) (string, bool, error) {
	if mountpoint == "" || !path.IsAbs(mountpoint) || path.Clean(mountpoint) != mountpoint {
		return "", false, fmt.Errorf("mountinfo cgroup mountpoint %q is not canonical and absolute", mountpoint)
	}
	var relative string
	switch {
	case mountpoint == cgroupFilesystemMountpoint:
		relative = "/"
	case strings.HasPrefix(mountpoint, cgroupFilesystemMountpoint+"/"):
		relative = strings.TrimPrefix(mountpoint, cgroupFilesystemMountpoint)
	default:
		return "", false, nil
	}
	if err := requireCanonicalCgroupPath(mountpoint); err != nil {
		return "", false, fmt.Errorf("mountinfo cgroup mountpoint: %w", err)
	}
	physical, err := joinCgroupDirectory(filesystemRoot, relative)
	if err != nil {
		return "", false, err
	}
	return physical, true, nil
}

func resolveCgroupDirectory(mounts []cgroupMount, filesystem, controller, membership string) (string, error) {
	type candidate struct {
		path string
		info os.FileInfo
	}
	var candidates []candidate
	for _, mount := range mounts {
		if mount.filesystem != filesystem || (controller != "" && !mount.superOption[controller]) {
			continue
		}
		relative, included := cgroupPathRelativeToMount(membership, mount.root)
		if !included {
			continue
		}
		directory, err := joinCgroupDirectory(mount.mountpoint, relative)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("inspect %s %s hierarchy %s: %w", filesystem, controller, directory, err)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("%s %s hierarchy %s is not a directory", filesystem, controller, directory)
		}
		duplicate := false
		for _, existing := range candidates {
			if os.SameFile(existing.info, info) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			candidates = append(candidates, candidate{path: directory, info: info})
		}
	}
	description := "cgroup v2"
	if controller != "" {
		description = "cgroup v1 " + controller
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("%s hierarchy for %q is missing", description, membership)
	}
	if len(candidates) != 1 {
		return "", fmt.Errorf("%s hierarchy for %q is ambiguous", description, membership)
	}
	return candidates[0].path, nil
}

func cgroupPathRelativeToMount(membership, root string) (string, bool) {
	if root == "/" {
		return membership, true
	}
	if membership == root {
		return "/", true
	}
	if strings.HasPrefix(membership, root+"/") {
		return strings.TrimPrefix(membership, root), true
	}
	return "", false
}

func newCgroupControlReader(directory string) cgroupControlReader {
	return func(name string) (string, error) {
		return readBoundedControlFile(filepath.Join(directory, name), maximumCgroupControlBytes)
	}
}

func readCanonicalControlLine(read cgroupControlReader, name string) (string, error) {
	value, err := read(name)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", name, err)
	}
	line, err := canonicalNewlineTerminatedBody(value, name)
	if err != nil {
		return "", err
	}
	if strings.Contains(line, "\n") {
		return "", fmt.Errorf("%s contains more than one record", name)
	}
	return line, nil
}

func readFiniteCgroupLimit(read cgroupControlReader, name string, allowZero bool, unboundedValues ...string) (int64, error) {
	value, err := readCanonicalControlLine(read, name)
	if err != nil {
		return 0, err
	}
	for _, unbounded := range unboundedValues {
		if value == unbounded {
			return 0, fmt.Errorf("%s is unbounded", name)
		}
	}
	parsed, err := parseCanonicalCgroupInteger(value, allowZero)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func readFiniteV1MemoryLimit(read cgroupControlReader, name string) (int64, error) {
	value, err := readFiniteCgroupLimit(read, name, false, "-1", "max")
	if err != nil {
		return 0, err
	}
	// Cgroup v1 represents an unbounded memory counter as LONG_MAX rounded
	// down to a page boundary (normally 9223372036854771712 on amd64).
	if value >= math.MaxInt64-(1<<20) {
		return 0, fmt.Errorf("%s is unbounded", name)
	}
	return value, nil
}

func exactCPUMilli(quota, period int64) (int64, error) {
	if quota <= 0 || period <= 0 || quota > math.MaxInt64/1000 {
		return 0, fmt.Errorf("quota=%d period=%d is not a finite supported CPU limit", quota, period)
	}
	scaled := quota * 1000
	if scaled%period != 0 {
		return 0, fmt.Errorf("quota=%d period=%d does not map to exact milli-CPU", quota, period)
	}
	milli := scaled / period
	if milli <= 0 {
		return 0, fmt.Errorf("quota=%d period=%d does not produce a positive milli-CPU limit", quota, period)
	}
	return milli, nil
}

func canonicalNewlineTerminatedBody(value, description string) (string, error) {
	if value == "" || value[len(value)-1] != '\n' {
		return "", fmt.Errorf("%s is empty or lacks its canonical newline terminator", description)
	}
	body := value[:len(value)-1]
	if body == "" || strings.ContainsRune(body, '\r') || strings.HasSuffix(body, "\n") {
		return "", fmt.Errorf("%s is not canonical newline-terminated text", description)
	}
	return body, nil
}

func requireCanonicalCgroupPath(group string) error {
	if group == "" || !path.IsAbs(group) || path.Clean(group) != group {
		return fmt.Errorf("cgroup path %q is not canonical and absolute", group)
	}
	for _, segment := range strings.Split(strings.TrimPrefix(group, "/"), "/") {
		if strings.ContainsAny(segment, "\\:") {
			return fmt.Errorf("cgroup path %q contains an unsafe path segment", group)
		}
		for _, character := range segment {
			if character < 0x20 || character == 0x7f {
				return fmt.Errorf("cgroup path %q contains control characters", group)
			}
		}
	}
	return nil
}

func joinCgroupDirectory(filesystemRoot, group string) (string, error) {
	if filesystemRoot == "" || !filepath.IsAbs(filesystemRoot) || filepath.Clean(filesystemRoot) != filesystemRoot {
		return "", fmt.Errorf("cgroup hierarchy root %q is not canonical and absolute", filesystemRoot)
	}
	if err := requireCanonicalCgroupPath(group); err != nil {
		return "", err
	}
	directory := filesystemRoot
	for _, segment := range strings.Split(strings.TrimPrefix(group, "/"), "/") {
		if segment != "" {
			directory = filepath.Join(directory, segment)
		}
	}
	relative, err := filepath.Rel(filesystemRoot, directory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("cgroup path %q escapes hierarchy root %q", group, filesystemRoot)
	}
	return directory, nil
}

func isCanonicalControllerName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
			character == '_' || character == '.' || character == '=' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func parseCanonicalCgroupInteger(value string, allowZero bool) (int64, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return 0, fmt.Errorf("value %q is not canonical decimal", value)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value || parsed < 0 || (!allowZero && parsed == 0) {
		return 0, fmt.Errorf("value %q is not a supported canonical limit", value)
	}
	return parsed, nil
}

func readBoundedControlFile(filename string, maximum int64) (string, error) {
	if maximum <= 0 {
		return "", fmt.Errorf("invalid read bound %d for %s", maximum, filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular control file", filename)
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return "", err
	}
	if int64(len(content)) > maximum || bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content) {
		return "", fmt.Errorf("%s exceeds its bounded UTF-8 text contract", filename)
	}
	return string(content), nil
}

func runDetachedChild(readyPath, outputPath string, delay time.Duration) error {
	if readyPath == "" || outputPath == "" || readyPath == outputPath || delay <= 0 {
		return fmt.Errorf("detached child requires distinct readiness/output paths and a positive delay")
	}
	if err := writeDetachedMarker(readyPath, []byte("ready\n")); err != nil {
		return err
	}
	time.Sleep(delay)
	return writeDetachedMarker(outputPath, []byte("escaped process survived\n"))
}

func writeDetachedMarker(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, content, 0o600)
}

func fatal(err error, code int) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(code)
}

func verifyRecordedResult(content []byte, expectedInputDigest string) error {
	if expectedInputDigest == "" {
		return fmt.Errorf("expected input digest is required")
	}
	var recorded result
	if err := json.Unmarshal(content, &recorded); err != nil {
		return fmt.Errorf("decode recorded specimen result: %w", err)
	}
	if recorded.InputDigest != expectedInputDigest {
		return fmt.Errorf("recorded input digest %q does not match %q", recorded.InputDigest, expectedInputDigest)
	}
	required := make(map[string]bool, len(requiredBoundaryPaths))
	for _, path := range requiredBoundaryPaths {
		required[path] = false
	}
	for _, item := range recorded.Probes {
		seen, expected := required[item.Path]
		if !expected {
			continue
		}
		if seen {
			return fmt.Errorf("boundary probe %q is duplicated", item.Path)
		}
		if item.Accessible {
			return fmt.Errorf("boundary probe %q was accessible", item.Path)
		}
		required[item.Path] = true
	}
	for path, seen := range required {
		if !seen {
			return fmt.Errorf("boundary probe %q is missing", path)
		}
	}
	return nil
}
