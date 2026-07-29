package dockercli

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maximumProcCgroupBytes = int64(64 << 10)

type exactCgroupMembership struct {
	Version string
	Path    string
}

// RequireSupportedCgroupVersion accepts the two Docker resource-controller
// layouts for which the drivers bind exact HostConfig limits and the
// qualification specimen independently measures the live controller files.
func RequireSupportedCgroupVersion(reportedVersion string) error {
	if reportedVersion == "1" || reportedVersion == "2" {
		return nil
	}
	if strings.TrimSpace(reportedVersion) == "" {
		return fmt.Errorf("container engine did not report a cgroup version")
	}
	return fmt.Errorf("container engine reported unsupported cgroup version %q", reportedVersion)
}

// parseExactCgroupV2Path accepts only the single unified hierarchy record
// emitted by a native cgroup-v2 host and binds its leaf to Docker's full,
// untruncated container ID. A parent configuration is not a runtime identity.
func parseExactCgroupV2Path(reader io.Reader, containerID string) (string, error) {
	if err := RequireCanonicalContainerID(containerID); err != nil {
		return "", err
	}
	lines, err := readCanonicalCgroupMembership(reader)
	if err != nil {
		return "", err
	}
	return parseExactCgroupV2Lines(lines, containerID)
}

// parseExactDockerCgroupMembership recognizes either a single unified-v2
// membership or a complete v1 CPU/memory/PID controller set. Docker Desktop
// can expose the latter together with one compatibility unified record; that
// hybrid is accepted only when every record names one identical canonical
// full-container-ID path. A v1 process can otherwise have several physical
// hierarchy paths, so it is validated but deliberately does not synthesize one
// misleading CgroupID.
func parseExactDockerCgroupMembership(reader io.Reader, containerID string) (exactCgroupMembership, error) {
	if err := RequireCanonicalContainerID(containerID); err != nil {
		return exactCgroupMembership{}, err
	}
	lines, err := readCanonicalCgroupMembership(reader)
	if err != nil {
		return exactCgroupMembership{}, err
	}
	unifiedLines := make([]string, 0, 1)
	v1Lines := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(line, "0::") {
			unifiedLines = append(unifiedLines, line)
			continue
		}
		v1Lines = append(v1Lines, line)
	}
	if len(v1Lines) == 0 {
		cgroupPath, err := parseExactCgroupV2Lines(unifiedLines, containerID)
		if err != nil {
			return exactCgroupMembership{}, err
		}
		return exactCgroupMembership{Version: "2", Path: cgroupPath}, nil
	}
	if len(unifiedLines) > 1 {
		return exactCgroupMembership{}, fmt.Errorf("hybrid cgroup membership contains multiple unified records")
	}
	v1Paths, err := validateExactCgroupV1LinesAndPaths(v1Lines, containerID)
	if err != nil {
		return exactCgroupMembership{}, err
	}
	if len(unifiedLines) == 1 {
		unifiedPath, err := parseExactCgroupV2Lines(unifiedLines, containerID)
		if err != nil {
			return exactCgroupMembership{}, fmt.Errorf("hybrid unified cgroup record: %w", err)
		}
		if len(v1Paths) != 1 {
			return exactCgroupMembership{}, fmt.Errorf("hybrid cgroup membership has ambiguous v1 hierarchy paths")
		}
		if _, exact := v1Paths[unifiedPath]; !exact {
			return exactCgroupMembership{}, fmt.Errorf("hybrid unified cgroup path does not exactly match the v1 hierarchy path")
		}
	}
	return exactCgroupMembership{Version: "1"}, nil
}

func readCanonicalCgroupMembership(reader io.Reader) ([]string, error) {
	content, err := io.ReadAll(io.LimitReader(reader, maximumProcCgroupBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read cgroup membership: %w", err)
	}
	if int64(len(content)) > maximumProcCgroupBytes {
		return nil, fmt.Errorf("cgroup membership exceeds the %d-byte safety bound", maximumProcCgroupBytes)
	}
	if len(content) == 0 || content[len(content)-1] != '\n' {
		return nil, fmt.Errorf("cgroup membership is empty or lacks its canonical newline terminator")
	}
	if bytes.IndexByte(content, 0) >= 0 || !utf8.Valid(content) {
		return nil, fmt.Errorf("cgroup membership contains invalid text")
	}
	body := string(content[:len(content)-1])
	if body == "" {
		return nil, fmt.Errorf("cgroup membership contains no records")
	}
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if line == "" || strings.ContainsRune(line, '\r') {
			return nil, fmt.Errorf("cgroup membership has non-canonical line endings or empty records")
		}
	}
	return lines, nil
}

func parseExactCgroupV2Lines(lines []string, containerID string) (string, error) {
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "0::") {
		return "", fmt.Errorf("cgroup membership is not one exact unified v2 record")
	}
	line := lines[0]
	cgroupPath := strings.TrimPrefix(line, "0::")
	if cgroupPath == "" || !path.IsAbs(cgroupPath) || cgroupPath == "/" || path.Clean(cgroupPath) != cgroupPath {
		return "", fmt.Errorf("cgroup v2 path %q is not a canonical non-root absolute path", cgroupPath)
	}
	if err := requireCgroupLeafBound(cgroupPath, containerID, "cgroup v2"); err != nil {
		return "", err
	}
	return cgroupPath, nil
}

func validateExactCgroupV1Lines(lines []string, containerID string) error {
	_, err := validateExactCgroupV1LinesAndPaths(lines, containerID)
	return err
}

func validateExactCgroupV1LinesAndPaths(lines []string, containerID string) (map[string]struct{}, error) {
	seenHierarchies := make(map[int]struct{}, len(lines))
	seenControllers := make(map[string]struct{})
	seenPaths := make(map[string]struct{})
	required := map[string]bool{"cpu": false, "memory": false, "pids": false}
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 3 {
			return nil, fmt.Errorf("cgroup v1 membership record is malformed")
		}
		hierarchy, err := strconv.Atoi(fields[0])
		if err != nil || hierarchy <= 0 || strconv.Itoa(hierarchy) != fields[0] {
			return nil, fmt.Errorf("cgroup v1 hierarchy ID %q is not canonical", fields[0])
		}
		if _, duplicate := seenHierarchies[hierarchy]; duplicate {
			return nil, fmt.Errorf("cgroup v1 hierarchy ID %d is duplicated", hierarchy)
		}
		seenHierarchies[hierarchy] = struct{}{}
		controllers := strings.Split(fields[1], ",")
		if fields[1] == "" {
			return nil, fmt.Errorf("cgroup v1 hierarchy %d has no controllers", hierarchy)
		}
		for _, controller := range controllers {
			if !isCanonicalCgroupController(controller) {
				return nil, fmt.Errorf("cgroup v1 controller %q is not canonical", controller)
			}
			if _, duplicate := seenControllers[controller]; duplicate {
				return nil, fmt.Errorf("cgroup v1 controller %q is duplicated", controller)
			}
			seenControllers[controller] = struct{}{}
			if _, tracked := required[controller]; tracked {
				required[controller] = true
			}
		}
		cgroupPath := fields[2]
		if cgroupPath == "" || !path.IsAbs(cgroupPath) || cgroupPath == "/" || path.Clean(cgroupPath) != cgroupPath {
			return nil, fmt.Errorf("cgroup v1 path %q is not a canonical non-root absolute path", cgroupPath)
		}
		if err := requireCgroupLeafBound(cgroupPath, containerID, "cgroup v1"); err != nil {
			return nil, err
		}
		seenPaths[cgroupPath] = struct{}{}
	}
	for _, controller := range []string{"cpu", "memory", "pids"} {
		if !required[controller] {
			return nil, fmt.Errorf("cgroup v1 membership does not contain the required %s controller", controller)
		}
	}
	return seenPaths, nil
}

func isCanonicalCgroupController(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' || character == '=' {
			continue
		}
		return false
	}
	return true
}

func requireCgroupLeafBound(cgroupPath, containerID, version string) error {
	leaf := path.Base(cgroupPath)
	if leaf != containerID && leaf != "docker-"+containerID+".scope" {
		return fmt.Errorf("%s leaf %q is not bound to full Docker container ID %q", version, leaf, containerID)
	}
	return nil
}
