package dockercli

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const (
	// RuntimeDefaultSeccompProfile is the policy-facing name for Docker's
	// independently selected built-in default profile. DockerBuiltinSeccompProfile
	// is the exact CLI/inspect spelling that avoids an unverifiable host path.
	RuntimeDefaultSeccompProfile = "runtime-default"
	DockerBuiltinSeccompProfile  = "builtin"
	NoNewPrivilegesOption        = "no-new-privileges:true"
	RuncRuntime                  = "runc"
)

// ExactWorldLabels requires every expected label and rejects additional labels
// in the reserved world namespace. Labels owned by another namespace do not
// alter world identity.
func ExactWorldLabels(actual, expected map[string]string) bool {
	for name, value := range expected {
		if actual[name] != value {
			return false
		}
	}
	for name := range actual {
		if strings.HasPrefix(name, "world.") {
			if _, found := expected[name]; !found {
				return false
			}
		}
	}
	return true
}

// NanoCPUs converts the admission CPU unit into Docker's physical limit unit.
func NanoCPUs(cpuMilli int64) int64 {
	if cpuMilli > math.MaxInt64/1_000_000 {
		return math.MaxInt64
	}
	return cpuMilli * 1_000_000
}

// MemorySwapTotal converts a swap-only policy limit into Docker's combined
// memory+swap value. Equal memory and total values explicitly disable swap.
func MemorySwapTotal(memoryBytes, swapBytes int64) (int64, error) {
	if memoryBytes <= 0 {
		return 0, fmt.Errorf("a positive memory limit is required")
	}
	if swapBytes < 0 || swapBytes > math.MaxInt64-memoryBytes {
		return 0, fmt.Errorf("swap limit is negative or overflows Docker's combined limit")
	}
	return memoryBytes + swapBytes, nil
}

func ParseNumericUser(value string) (int, int, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("must be an explicit numeric uid:gid pair")
	}
	values := [2]int{}
	for index, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 31)
		if err != nil || parsed == 0 {
			return 0, 0, fmt.Errorf("uid and gid must be positive 31-bit decimal numbers")
		}
		values[index] = int(parsed)
	}
	return values[0], values[1], nil
}

func HardenedSecurityOptions() []string {
	return []string{NoNewPrivilegesOption, "seccomp=" + DockerBuiltinSeccompProfile}
}

func HardenedSecurityArguments() []string {
	options := HardenedSecurityOptions()
	arguments := make([]string, 0, len(options)*2)
	for _, option := range options {
		arguments = append(arguments, "--security-opt", option)
	}
	return arguments
}

// PrivateNamespaceArguments pins namespace settings for which Docker accepts
// an explicit private value. Docker represents a private PID namespace only
// as its empty default and rejects "--pid private".
func PrivateNamespaceArguments() []string {
	return []string{
		"--network", "none",
		"--ipc", "private",
		"--cgroupns", "private",
	}
}

// ResourceLimitArguments produces the one shared Docker resource-controller
// spelling used by agent and target containers. Zero swap is an explicit
// no-swap limit, not an omitted/unbounded setting.
func ResourceLimitArguments(cpuMilli, memoryBytes, swapBytes, pids int64) ([]string, error) {
	if cpuMilli <= 0 || pids <= 0 {
		return nil, fmt.Errorf("positive CPU and PID limits are required")
	}
	memorySwap, err := MemorySwapTotal(memoryBytes, swapBytes)
	if err != nil {
		return nil, err
	}
	return []string{
		"--memory", strconv.FormatInt(memoryBytes, 10),
		"--memory-swap", strconv.FormatInt(memorySwap, 10),
		"--cpus", strconv.FormatFloat(float64(cpuMilli)/1000, 'f', 3, 64),
		"--pids-limit", strconv.FormatInt(pids, 10),
	}, nil
}

// SupportsSecurityOption recognizes Docker info entries such as
// "name=seccomp,profile=builtin" without treating an unrelated substring as
// proof of support.
func SupportsSecurityOption(options []string, name string) bool {
	want := strings.ToLower(strings.TrimSpace(name))
	if want == "" {
		return false
	}
	for _, option := range options {
		for _, field := range strings.Split(strings.ToLower(option), ",") {
			key, value, found := strings.Cut(strings.TrimSpace(field), "=")
			if found && key == "name" && value == want {
				return true
			}
			if !found && key == want {
				return true
			}
		}
	}
	return false
}

// ConfigurationDifference compares the security- and execution-relevant
// Docker configuration. Docker represents a private namespace as either an
// empty default or the explicit value "private"; those spellings are the only
// normalization performed beyond order-insensitive option sets.
func ConfigurationDifference(actual, expected Configuration) error {
	actual = normalizeConfiguration(actual)
	expected = normalizeConfiguration(expected)
	if fields := differingConfigurationFields(actual, expected); len(fields) != 0 {
		return fmt.Errorf("observed Docker configuration differs from the expected plan in fields: %s", strings.Join(fields, ", "))
	}
	return nil
}

func differingConfigurationFields(actual, expected Configuration) []string {
	actualValue := reflect.ValueOf(actual)
	expectedValue := reflect.ValueOf(expected)
	configurationType := actualValue.Type()
	fields := make([]string, 0)
	for index := 0; index < actualValue.NumField(); index++ {
		if !reflect.DeepEqual(actualValue.Field(index).Interface(), expectedValue.Field(index).Interface()) {
			fields = append(fields, configurationType.Field(index).Name)
		}
	}
	return fields
}

func normalizeConfiguration(value Configuration) Configuration {
	value.Entrypoint = nonNilStrings(value.Entrypoint)
	value.Command = nonNilStrings(value.Command)
	value.PIDMode = normalizePrivateMode(value.PIDMode)
	value.IPCMode = normalizePrivateMode(value.IPCMode)
	value.CgroupMode = normalizePrivateMode(value.CgroupMode)
	value.CapabilitiesAdd = normalizeUpperSet(value.CapabilitiesAdd)
	value.CapabilitiesDrop = normalizeUpperSet(value.CapabilitiesDrop)
	value.SecurityOptions = normalizeSecurityOptions(value.SecurityOptions)
	value.Tmpfs = normalizeTmpfs(value.Tmpfs)
	value.Devices = append([]Device(nil), value.Devices...)
	sort.Slice(value.Devices, func(i, j int) bool {
		left, right := value.Devices[i], value.Devices[j]
		return left.ContainerPath+"\x00"+left.HostPath+"\x00"+left.Permissions < right.ContainerPath+"\x00"+right.HostPath+"\x00"+right.Permissions
	})
	value.Mounts = append([]Mount(nil), value.Mounts...)
	for index := range value.Mounts {
		value.Mounts[index].Source = filepath.Clean(value.Mounts[index].Source)
		value.Mounts[index].Destination = filepath.Clean(value.Mounts[index].Destination)
	}
	sort.Slice(value.Mounts, func(i, j int) bool {
		left, right := value.Mounts[i], value.Mounts[j]
		return left.Destination+"\x00"+left.Source < right.Destination+"\x00"+right.Source
	})
	return value
}

func normalizePrivateMode(value string) string {
	if value == "" {
		return "private"
	}
	return value
}

func normalizeUpperSet(values []string) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = strings.ToUpper(value)
	}
	sort.Strings(result)
	return result
}

func normalizeSecurityOptions(values []string) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = strings.ToLower(strings.ReplaceAll(value, "=", ":"))
	}
	sort.Strings(result)
	return result
}

func normalizeTmpfs(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for path, value := range values {
		options := strings.Split(value, ",")
		sort.Strings(options)
		result[filepath.Clean(path)] = strings.Join(options, ",")
	}
	return result
}

func nonNilStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	return append([]string(nil), values...)
}
