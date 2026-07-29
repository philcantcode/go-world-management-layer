package dockercli

import (
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
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
	RestrictedShmSize            = int64(64 << 20)
	RestrictedPathEnvironment    = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	RestrictedWorkingDirectory   = "/"
	StoppedStatusCreated         = "created"
	StoppedStatusExited          = "exited"
)

// RequireCanonicalContainerID accepts only the full identity emitted by
// Docker's no-trunc inventory and create operations. Short IDs, upper-case
// hexadecimal, names, and surrounding whitespace are never adoption
// authority.
func RequireCanonicalContainerID(value string) error {
	if len(value) != 64 {
		return fmt.Errorf("container ID must contain exactly 64 lower-case hexadecimal characters")
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return fmt.Errorf("container ID must contain exactly 64 lower-case hexadecimal characters")
			}
		}
	}
	return nil
}

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

// RestrictedNamespaceFacts describes the namespace choices made by the
// restricted create arguments. An empty Docker UsernsMode delegates to the
// daemon: Docker reports userns remapping through its security options, while
// absence of both userns and rootless reports the host user namespace.
func RestrictedNamespaceFacts(securityOptions []string) map[string]string {
	userNamespace := "host"
	if SupportsSecurityOption(securityOptions, "userns") || SupportsSecurityOption(securityOptions, "rootless") {
		userNamespace = "remapped"
	}
	return map[string]string{
		"pid_namespace":     "private",
		"ipc_namespace":     "private",
		"cgroup_namespace":  "private",
		"uts_namespace":     "private",
		"network_namespace": "none",
		"user_namespace":    userNamespace,
	}
}

// RestrictedContainerConfiguration returns the common Docker configuration
// guaranteed by both hardened container plans. Callers add only their
// identity, command, resource values, and exact bind mounts.
func RestrictedContainerConfiguration() Configuration {
	return Configuration{
		Environment:     []string{RestrictedPathEnvironment},
		WorkingDir:      RestrictedWorkingDirectory,
		AttachStdout:    true,
		AttachStderr:    true,
		ExposedPorts:    []string{},
		DeclaredVolumes: []string{},
		ReadOnlyRoot:    true,
		RestartPolicy:   RestartPolicy{Name: "no"},
		NetworkMode:     "none",
		PIDMode:         "private",
		IPCMode:         "private",
		CgroupMode:      "private",
		// Docker's empty UsernsMode means the daemon-selected user namespace,
		// not a private namespace. Keep that spelling exact instead of claiming
		// an isolation guarantee Docker did not make.
		UsernsMode:           "",
		UTSMode:              "private",
		Init:                 true,
		InitKnown:            true,
		CapabilitiesAdd:      []string{},
		CapabilitiesDrop:     []string{"ALL"},
		GroupAdd:             []string{},
		SecurityOptions:      HardenedSecurityOptions(),
		Tmpfs:                map[string]string{"/tmp": "rw,nosuid,nodev,noexec,mode=1777"},
		Devices:              []Device{},
		DeviceRequests:       []DeviceRequest{},
		DeviceCgroupRules:    []string{},
		Mounts:               []Mount{},
		ConfiguredMounts:     []ConfiguredMount{},
		Binds:                []string{},
		VolumesFrom:          []string{},
		PortBindings:         map[string][]PortBinding{},
		NetworkAttachments:   []string{"none"},
		ExtraHosts:           []string{},
		DNS:                  []string{},
		DNSOptions:           []string{},
		DNSSearch:            []string{},
		Links:                []string{},
		BlkioWeightDevice:    []WeightDevice{},
		BlkioDeviceReadBps:   []ThrottleDevice{},
		BlkioDeviceWriteBps:  []ThrottleDevice{},
		BlkioDeviceReadIOps:  []ThrottleDevice{},
		BlkioDeviceWriteIOps: []ThrottleDevice{},
		Ulimits:              []Ulimit{},
		Sysctls:              map[string]string{},
		MaskedPaths:          defaultMaskedPaths(),
		ReadonlyPaths:        defaultReadonlyPaths(),
		ShmSize:              RestrictedShmSize,
		LogConfig:            LogConfiguration{Type: "none", Config: map[string]string{}},
		StorageOptions:       map[string]string{},
		Annotations:          map[string]string{},
	}
}

// RestrictedLifecycleArguments pins liveness, hostname, shared-memory,
// logging, and image-derived execution defaults that Docker would otherwise
// inherit from daemon or image settings.
func RestrictedLifecycleArguments(hostname string) []string {
	return []string{
		"--hostname", hostname,
		"--restart", "no",
		"--log-driver", "none",
		"--shm-size", strconv.FormatInt(RestrictedShmSize, 10),
		"--env", RestrictedPathEnvironment,
		"--workdir", RestrictedWorkingDirectory,
	}
}

func RestrictedBindMount(source, destination string, readOnly bool) Mount {
	return Mount{Type: "bind", Source: source, Destination: destination, ReadOnly: readOnly, Propagation: "rprivate"}
}

func RestrictedConfiguredBindMount(source, destination string, readOnly bool) ConfiguredMount {
	return ConfiguredMount{Type: "bind", Source: source, Target: destination, ReadOnly: readOnly}
}

// AddRestrictedBindMount keeps the expected configured request and realized
// mount observations in lockstep. Both are required because Docker's realized
// Mounts view omits advanced HostConfig.Mounts options.
func AddRestrictedBindMount(configuration *Configuration, source, destination string, readOnly bool) {
	configuration.Mounts = append(configuration.Mounts, RestrictedBindMount(source, destination, readOnly))
	configuration.ConfiguredMounts = append(configuration.ConfiguredMounts, RestrictedConfiguredBindMount(source, destination, readOnly))
}

// RestrictedBindMountArgument builds one Docker --mount value without ever
// allowing CSV metacharacters to reinterpret a validated host path or guest
// destination as an additional mount option.
func RestrictedBindMountArgument(source, destination string, readOnly bool) (string, error) {
	if source == "" || destination == "" {
		return "", fmt.Errorf("bind mount source and destination are required")
	}
	if strings.ContainsAny(source, ",\x00\r\n\"") || strings.ContainsAny(destination, ",\x00\r\n\"") {
		return "", fmt.Errorf("bind mount source and destination cannot contain Docker --mount CSV metacharacters")
	}
	value := "type=bind,src=" + source + ",dst=" + destination
	if readOnly {
		value += ",readonly"
	}
	return value, nil
}

// CanonicalHostBindSourceEqual compares already-validated host bind sources.
// Windows host paths are case-insensitive; Unix host paths are not.
func CanonicalHostBindSourceEqual(left, right string) bool {
	return canonicalHostBindSourceEqual(runtime.GOOS, left, right)
}

func canonicalHostBindSourceEqual(goos, left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	if goos == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

// RequireExactStoppedState rejects Docker's transitional, terminal-dead, and
// contradictory state combinations. Callers supply only the stopped statuses
// that are valid for their lifecycle operation (normally created and/or
// exited).
func RequireExactStoppedState(running, paused, restarting, dead bool, status string, allowedStatuses ...string) error {
	if running || paused || restarting || dead {
		return fmt.Errorf("Docker state flags do not describe an exact stopped container")
	}
	for _, allowed := range allowedStatuses {
		if status == allowed {
			return nil
		}
	}
	return fmt.Errorf("Docker status %q is not allowed for this stopped-container boundary", status)
}

func RequireExactRunningState(running, paused, restarting, dead bool, status string) error {
	if !running || paused || restarting || dead || status != "running" {
		return fmt.Errorf("Docker state does not describe an exact running container")
	}
	return nil
}

func defaultMaskedPaths() []string {
	return []string{
		"/proc/acpi", "/proc/asound", "/proc/interrupts", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
		"/proc/sched_debug", "/proc/scsi", "/proc/timer_list", "/proc/timer_stats",
		"/sys/devices/virtual/powercap", "/sys/firmware",
	}
}

func defaultReadonlyPaths() []string {
	return []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}
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
// Docker configuration. Docker represents private PID, IPC, cgroup, and UTS
// namespaces as either an empty default or the explicit value "private";
// those spellings are the only normalization performed beyond
// order-insensitive option sets. UsernsMode is deliberately excluded: its
// empty value is daemon-selected and is not semantically "private".
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
		field := configurationType.Field(index)
		equal := reflect.DeepEqual(actualValue.Field(index).Interface(), expectedValue.Field(index).Interface())
		switch field.Name {
		case "Mounts":
			equal = equalMounts(actual.Mounts, expected.Mounts)
		case "ConfiguredMounts":
			equal = equalConfiguredMounts(actual.ConfiguredMounts, expected.ConfiguredMounts)
		}
		if !equal {
			fields = append(fields, field.Name)
		}
	}
	return fields
}

func normalizeConfiguration(value Configuration) Configuration {
	value.Entrypoint = nonNilStrings(value.Entrypoint)
	value.Command = nonNilStrings(value.Command)
	value.Environment = nonNilStrings(value.Environment)
	value.ExposedPorts = normalizeStringSet(value.ExposedPorts)
	value.DeclaredVolumes = normalizeStringSet(value.DeclaredVolumes)
	value.Healthcheck.Test = nonNilStrings(value.Healthcheck.Test)
	value.PIDMode = normalizePrivateMode(value.PIDMode)
	value.IPCMode = normalizePrivateMode(value.IPCMode)
	value.CgroupMode = normalizePrivateMode(value.CgroupMode)
	value.UTSMode = normalizePrivateMode(value.UTSMode)
	value.CapabilitiesAdd = normalizeUpperSet(value.CapabilitiesAdd)
	value.CapabilitiesDrop = normalizeUpperSet(value.CapabilitiesDrop)
	value.GroupAdd = normalizeStringSet(value.GroupAdd)
	value.SecurityOptions = normalizeSecurityOptions(value.SecurityOptions)
	value.Tmpfs = normalizeTmpfs(value.Tmpfs)
	value.Devices = append([]Device{}, value.Devices...)
	sort.Slice(value.Devices, func(i, j int) bool {
		left, right := value.Devices[i], value.Devices[j]
		return left.ContainerPath+"\x00"+left.HostPath+"\x00"+left.Permissions < right.ContainerPath+"\x00"+right.HostPath+"\x00"+right.Permissions
	})
	value.DeviceRequests = normalizeDeviceRequests(value.DeviceRequests)
	value.DeviceCgroupRules = normalizeStringSet(value.DeviceCgroupRules)
	value.Mounts = append([]Mount{}, value.Mounts...)
	for index := range value.Mounts {
		value.Mounts[index].Source = normalizeMountSource(value.Mounts[index].Type, value.Mounts[index].Source)
		value.Mounts[index].Destination = normalizeGuestMountPath(value.Mounts[index].Destination)
	}
	sort.Slice(value.Mounts, func(i, j int) bool {
		left, right := value.Mounts[i], value.Mounts[j]
		return left.Destination+"\x00"+left.Type+"\x00"+left.Name+"\x00"+mountSourceSortKey(left.Type, left.Source)+"\x00"+left.Mode+"\x00"+left.Propagation+"\x00"+left.Driver <
			right.Destination+"\x00"+right.Type+"\x00"+right.Name+"\x00"+mountSourceSortKey(right.Type, right.Source)+"\x00"+right.Mode+"\x00"+right.Propagation+"\x00"+right.Driver
	})
	value.ConfiguredMounts = normalizeConfiguredMounts(value.ConfiguredMounts)
	value.Binds = normalizeStringSet(value.Binds)
	value.VolumesFrom = normalizeStringSet(value.VolumesFrom)
	value.PortBindings = normalizePortBindings(value.PortBindings)
	value.NetworkAttachments = normalizeStringSet(value.NetworkAttachments)
	value.ExtraHosts = normalizeStringSet(value.ExtraHosts)
	value.DNS = nonNilStrings(value.DNS)
	value.DNSOptions = nonNilStrings(value.DNSOptions)
	value.DNSSearch = nonNilStrings(value.DNSSearch)
	value.Links = normalizeStringSet(value.Links)
	value.BlkioWeightDevice = normalizeWeightDevices(value.BlkioWeightDevice)
	value.BlkioDeviceReadBps = normalizeThrottleDevices(value.BlkioDeviceReadBps)
	value.BlkioDeviceWriteBps = normalizeThrottleDevices(value.BlkioDeviceWriteBps)
	value.BlkioDeviceReadIOps = normalizeThrottleDevices(value.BlkioDeviceReadIOps)
	value.BlkioDeviceWriteIOps = normalizeThrottleDevices(value.BlkioDeviceWriteIOps)
	value.Ulimits = normalizeUlimits(value.Ulimits)
	value.Sysctls = cloneMap(value.Sysctls)
	value.MaskedPaths = normalizeStringSet(value.MaskedPaths)
	value.ReadonlyPaths = normalizeStringSet(value.ReadonlyPaths)
	value.LogConfig.Config = cloneMap(value.LogConfig.Config)
	value.StorageOptions = cloneMap(value.StorageOptions)
	value.Annotations = cloneMap(value.Annotations)
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

func normalizeStringSet(values []string) []string {
	result := nonNilStrings(values)
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
	for mountPath, value := range values {
		options := strings.Split(value, ",")
		sort.Strings(options)
		result[path.Clean(mountPath)] = strings.Join(options, ",")
	}
	return result
}

func normalizeConfiguredMounts(values []ConfiguredMount) []ConfiguredMount {
	result := append([]ConfiguredMount{}, values...)
	for index := range result {
		mount := &result[index]
		mount.Source = normalizeMountSource(mount.Type, mount.Source)
		mount.Target = normalizeGuestMountPath(mount.Target)
		mount.VolumeOptions.Labels = cloneMap(mount.VolumeOptions.Labels)
		mount.VolumeOptions.Driver.Options = cloneMap(mount.VolumeOptions.Driver.Options)
		mount.TmpfsOptions.Options = cloneNestedSlice(mount.TmpfsOptions.Options)
		for optionIndex := range mount.TmpfsOptions.Options {
			mount.TmpfsOptions.Options[optionIndex] = normalizeStringSet(mount.TmpfsOptions.Options[optionIndex])
		}
		sort.Slice(mount.TmpfsOptions.Options, func(i, j int) bool {
			return strings.Join(mount.TmpfsOptions.Options[i], "\x00") < strings.Join(mount.TmpfsOptions.Options[j], "\x00")
		})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		return left.Target+"\x00"+left.Type+"\x00"+mountSourceSortKey(left.Type, left.Source) <
			right.Target+"\x00"+right.Type+"\x00"+mountSourceSortKey(right.Type, right.Source)
	})
	return result
}

func normalizeMountSource(mountType, source string) string {
	if mountType != "bind" || source == "" {
		return source
	}
	return filepath.Clean(source)
}

func normalizeGuestMountPath(value string) string {
	if value == "" {
		return ""
	}
	return path.Clean(value)
}

func mountSourceSortKey(mountType, source string) string {
	if mountType == "bind" && runtime.GOOS == "windows" {
		return strings.ToLower(source)
	}
	return source
}

func equalMounts(left, right []Mount) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftMount, rightMount := left[index], right[index]
		if leftMount.Type == "bind" && rightMount.Type == "bind" {
			if !CanonicalHostBindSourceEqual(leftMount.Source, rightMount.Source) {
				return false
			}
			leftMount.Source, rightMount.Source = "", ""
		}
		if !reflect.DeepEqual(leftMount, rightMount) {
			return false
		}
	}
	return true
}

func equalConfiguredMounts(left, right []ConfiguredMount) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftMount, rightMount := left[index], right[index]
		if leftMount.Type == "bind" && rightMount.Type == "bind" {
			if !CanonicalHostBindSourceEqual(leftMount.Source, rightMount.Source) {
				return false
			}
			leftMount.Source, rightMount.Source = "", ""
		}
		if !reflect.DeepEqual(leftMount, rightMount) {
			return false
		}
	}
	return true
}

func normalizeDeviceRequests(values []DeviceRequest) []DeviceRequest {
	result := make([]DeviceRequest, len(values))
	for index, value := range values {
		value.DeviceIDs = normalizeStringSet(value.DeviceIDs)
		value.Capabilities = cloneNestedSlice(value.Capabilities)
		for capabilityIndex := range value.Capabilities {
			value.Capabilities[capabilityIndex] = normalizeStringSet(value.Capabilities[capabilityIndex])
		}
		sort.Slice(value.Capabilities, func(i, j int) bool {
			return strings.Join(value.Capabilities[i], "\x00") < strings.Join(value.Capabilities[j], "\x00")
		})
		value.Options = cloneMap(value.Options)
		result[index] = value
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		return left.Driver+"\x00"+strings.Join(left.DeviceIDs, "\x00") < right.Driver+"\x00"+strings.Join(right.DeviceIDs, "\x00")
	})
	return result
}

func normalizePortBindings(values map[string][]PortBinding) map[string][]PortBinding {
	result := make(map[string][]PortBinding, len(values))
	for port, bindings := range values {
		cloned := append([]PortBinding{}, bindings...)
		sort.Slice(cloned, func(i, j int) bool {
			return cloned[i].HostIP+"\x00"+cloned[i].HostPort < cloned[j].HostIP+"\x00"+cloned[j].HostPort
		})
		result[port] = cloned
	}
	return result
}

func normalizeWeightDevices(values []WeightDevice) []WeightDevice {
	result := append([]WeightDevice{}, values...)
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}

func normalizeThrottleDevices(values []ThrottleDevice) []ThrottleDevice {
	result := append([]ThrottleDevice{}, values...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Path == result[j].Path {
			return result[i].Rate < result[j].Rate
		}
		return result[i].Path < result[j].Path
	})
	return result
}

func normalizeUlimits(values []Ulimit) []Ulimit {
	result := append([]Ulimit{}, values...)
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func nonNilStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	return append([]string(nil), values...)
}
