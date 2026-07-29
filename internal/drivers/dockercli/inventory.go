// Package dockercli contains the bounded, shared Docker inspection surface
// used by both container-backed world drivers.
package dockercli

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

const (
	MaximumInventoryContainers = 4096
	maximumInspectBatch        = 32
)

type Mount struct {
	Type        string
	Name        string
	Source      string
	Destination string
	ReadOnly    bool
	Mode        string
	Propagation string
	Driver      string
}

// ConfiguredMount is the exact immutable mount request retained in
// HostConfig.Mounts. Docker's top-level Mounts observation describes the
// realized mount, but does not retain security-relevant request options such
// as recursive-read-only behavior, bind source creation, volume copy-up, or
// tmpfs sizing.
type ConfiguredMount struct {
	Type                string
	Source              string
	Target              string
	ReadOnly            bool
	Consistency         string
	BindOptions         BindOptions
	BindOptionsKnown    bool
	VolumeOptions       VolumeOptions
	VolumeOptionsKnown  bool
	TmpfsOptions        TmpfsOptions
	TmpfsOptionsKnown   bool
	ImageOptions        ImageOptions
	ImageOptionsKnown   bool
	ClusterOptionsKnown bool
}

type BindOptions struct {
	Propagation            string
	NonRecursive           bool
	CreateMountpoint       bool
	ReadOnlyNonRecursive   bool
	ReadOnlyForceRecursive bool
}

type VolumeOptions struct {
	NoCopy      bool
	Labels      map[string]string
	Subpath     string
	Driver      MountDriver
	DriverKnown bool
}

type MountDriver struct {
	Name    string
	Options map[string]string
}

type TmpfsOptions struct {
	SizeBytes int64
	Mode      uint32
	Options   [][]string
}

type ImageOptions struct {
	Subpath string
}

type Device struct {
	HostPath      string
	ContainerPath string
	Permissions   string
}

type RestartPolicy struct {
	Name              string
	MaximumRetryCount int
}

type DeviceRequest struct {
	Driver       string
	Count        int
	DeviceIDs    []string
	Capabilities [][]string
	Options      map[string]string
}

type PortBinding struct {
	HostIP   string
	HostPort string
}

type Healthcheck struct {
	Test          []string
	Interval      int64
	Timeout       int64
	StartPeriod   int64
	StartInterval int64
	Retries       int
}

type Ulimit struct {
	Name string
	Soft int64
	Hard int64
}

type WeightDevice struct {
	Path   string
	Weight uint16
}

type ThrottleDevice struct {
	Path string
	Rate uint64
}

type LogConfiguration struct {
	Type   string
	Config map[string]string
}

type Configuration struct {
	Image                 string
	Runtime               string
	Hostname              string
	Domainname            string
	Entrypoint            []string
	Command               []string
	Environment           []string
	WorkingDir            string
	User                  string
	AttachStdin           bool
	AttachStdout          bool
	AttachStderr          bool
	OpenStdin             bool
	StdinOnce             bool
	TTY                   bool
	NetworkDisabled       bool
	MacAddress            string
	ExposedPorts          []string
	DeclaredVolumes       []string
	Healthcheck           Healthcheck
	HealthcheckKnown      bool
	StopSignal            string
	StopTimeout           int
	StopTimeoutKnown      bool
	ReadOnlyRoot          bool
	Privileged            bool
	RestartPolicy         RestartPolicy
	AutoRemove            bool
	NetworkMode           string
	PIDMode               string
	IPCMode               string
	CgroupMode            string
	UsernsMode            string
	UTSMode               string
	Init                  bool
	InitKnown             bool
	CapabilitiesAdd       []string
	CapabilitiesDrop      []string
	GroupAdd              []string
	SecurityOptions       []string
	Tmpfs                 map[string]string
	Devices               []Device
	DeviceRequests        []DeviceRequest
	DeviceCgroupRules     []string
	Mounts                []Mount
	ConfiguredMounts      []ConfiguredMount
	Binds                 []string
	VolumesFrom           []string
	ContainerIDFile       string
	PublishAllPorts       bool
	PortBindings          map[string][]PortBinding
	NetworkAttachments    []string
	ExtraHosts            []string
	DNS                   []string
	DNSOptions            []string
	DNSSearch             []string
	Links                 []string
	OomKillDisable        bool
	OomScoreAdj           int
	Cgroup                string
	CgroupParent          string
	MemoryReservation     int64
	KernelMemory          int64
	KernelMemoryTCP       int64
	MemorySwappiness      int64
	MemorySwappinessKnown bool
	CPUShares             int64
	CPUPeriod             int64
	CPUQuota              int64
	CPURealtimePeriod     int64
	CPURealtimeRuntime    int64
	CpusetCPUs            string
	CpusetMems            string
	BlkioWeight           uint16
	BlkioWeightDevice     []WeightDevice
	BlkioDeviceReadBps    []ThrottleDevice
	BlkioDeviceWriteBps   []ThrottleDevice
	BlkioDeviceReadIOps   []ThrottleDevice
	BlkioDeviceWriteIOps  []ThrottleDevice
	CPUCount              int64
	CPUPercent            int64
	IOMaximumBandwidth    uint64
	IOMaximumIOps         uint64
	Ulimits               []Ulimit
	Sysctls               map[string]string
	MaskedPaths           []string
	ReadonlyPaths         []string
	ShmSize               int64
	LogConfig             LogConfiguration
	VolumeDriver          string
	StorageOptions        map[string]string
	Isolation             string
	Annotations           map[string]string
	MemoryBytes           int64
	MemorySwapBytes       int64
	NanoCPUs              int64
	PIDs                  int64
}

type Container struct {
	ID            string
	Name          string
	Running       bool
	Paused        bool
	Restarting    bool
	Dead          bool
	Status        string
	Labels        map[string]string
	CgroupID      string
	Configuration Configuration
}

type cgroupIDResolver func(pid int, containerID string) (string, error)

// Inventory lists every Docker container and inspects each one. Returning a
// partial inventory would let callers misclassify an uninspected resource as
// absent, so any list or inspect failure fails the whole observation.
func Inventory(ctx context.Context, binary string, runner command.Runner) ([]Container, error) {
	return inventoryWithCgroupIDResolver(ctx, binary, runner, resolveContainerCgroupID)
}

func inventoryWithCgroupIDResolver(ctx context.Context, binary string, runner command.Runner, resolveCgroupID cgroupIDResolver) ([]Container, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, fmt.Errorf("Docker inventory requires a context deadline")
	}
	if resolveCgroupID == nil {
		return nil, fmt.Errorf("Docker inventory requires a cgroup identity resolver")
	}
	result, err := runner.Run(ctx, command.Invocation{
		Program:       binary,
		Args:          []string{"ps", "--all", "--no-trunc", "--format", "{{.ID}}"},
		MaximumOutput: int64(MaximumInventoryContainers+1) * 129,
	})
	if err != nil {
		return nil, err
	}
	ids := nonEmptyExactLines(result.Stdout)
	if len(ids) > MaximumInventoryContainers {
		return nil, fmt.Errorf("Docker inventory exceeds the %d-container safety bound", MaximumInventoryContainers)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if err := RequireCanonicalContainerID(id); err != nil {
			return nil, fmt.Errorf("Docker inventory returned invalid container ID %q: %w", id, err)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("Docker inventory returned duplicate container ID %q", id)
		}
		seen[id] = struct{}{}
	}
	containers := make([]Container, 0, len(ids))
	for start := 0; start < len(ids); start += maximumInspectBatch {
		end := min(start+maximumInspectBatch, len(ids))
		batchIDs := ids[start:end]
		batch, err := inspectContainersWithCgroupIDResolver(ctx, binary, runner, batchIDs, resolveCgroupID)
		if err != nil {
			return nil, fmt.Errorf("inspect Docker inventory batch: %w", err)
		}
		if err := requireExactInspectSet(batchIDs, batch); err != nil {
			return nil, err
		}
		containers = append(containers, batch...)
	}
	sort.Slice(containers, func(i, j int) bool { return containers[i].ID < containers[j].ID })
	return containers, nil
}

func Inspect(ctx context.Context, binary string, runner command.Runner, id string) (Container, error) {
	containers, err := inspectContainersWithCgroupIDResolver(ctx, binary, runner, []string{id}, resolveContainerCgroupID)
	if err != nil {
		return Container{}, err
	}
	if err := requireExactInspectSet([]string{id}, containers); err != nil {
		return Container{}, err
	}
	return containers[0], nil
}

// inspectContainers amortizes host process startup across a bounded group of
// resources. Docker accepts multiple object IDs and returns one JSON array;
// keeping the batch bounded retains the command runner's output limit while
// avoiding one host process per container on large shared Docker engines.
func inspectContainers(ctx context.Context, binary string, runner command.Runner, ids []string) ([]Container, error) {
	return inspectContainersWithCgroupIDResolver(ctx, binary, runner, ids, resolveContainerCgroupID)
}

func inspectContainersWithCgroupIDResolver(ctx context.Context, binary string, runner command.Runner, ids []string, resolveCgroupID cgroupIDResolver) ([]Container, error) {
	if len(ids) == 0 || len(ids) > maximumInspectBatch {
		return nil, fmt.Errorf("Docker inspect batch size %d is outside 1..%d", len(ids), maximumInspectBatch)
	}
	for _, id := range ids {
		if err := RequireCanonicalContainerID(id); err != nil {
			return nil, fmt.Errorf("Docker inspect requires a canonical container ID %q: %w", id, err)
		}
	}
	if resolveCgroupID == nil {
		return nil, fmt.Errorf("Docker inspect requires a cgroup identity resolver")
	}
	args := append([]string{"inspect"}, ids...)
	result, err := runner.Run(ctx, command.Invocation{Program: binary, Args: args})
	if err != nil {
		return nil, err
	}
	var values []dockerInspect
	if err := json.Unmarshal(result.Stdout, &values); err != nil {
		return nil, fmt.Errorf("decode Docker inspect: %w", err)
	}
	if len(values) != len(ids) {
		return nil, fmt.Errorf("Docker inspect returned %d resources for %d IDs", len(values), len(ids))
	}
	containers := make([]Container, 0, len(values))
	for _, value := range values {
		if err := RequireCanonicalContainerID(value.ID); err != nil {
			return nil, fmt.Errorf("Docker inspect returned invalid resource ID %q: %w", value.ID, err)
		}
		if value.State.PID < 0 {
			return nil, fmt.Errorf("Docker inspect returned invalid negative PID %d for resource %q", value.State.PID, value.ID)
		}
		if value.State.Running && value.State.PID == 0 {
			return nil, fmt.Errorf("Docker inspect returned running resource %q without a positive PID", value.ID)
		}
		cgroupID := ""
		if value.State.Running {
			cgroupID, err = resolveCgroupID(value.State.PID, value.ID)
			if err != nil {
				return nil, fmt.Errorf("resolve exact cgroup identity for Docker container %q: %w", value.ID, err)
			}
		}
		containers = append(containers, containerFromInspect(value, cgroupID))
	}
	return containers, nil
}

func requireExactInspectSet(ids []string, containers []Container) error {
	expected := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		expected[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		if _, ok := expected[container.ID]; !ok {
			return fmt.Errorf("Docker inspect returned unrequested resource %q", container.ID)
		}
		if _, duplicate := seen[container.ID]; duplicate {
			return fmt.Errorf("Docker inspect returned duplicate resource %q", container.ID)
		}
		seen[container.ID] = struct{}{}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("Docker inspect omitted %d requested resources", len(expected)-len(seen))
	}
	return nil
}

func containerFromInspect(value dockerInspect, cgroupID string) Container {
	configuration := Configuration{
		Image: value.Config.Image, Runtime: value.HostConfig.Runtime, Hostname: value.Config.Hostname, Domainname: value.Config.Domainname,
		Entrypoint: cloneSlice(value.Config.Entrypoint), Command: cloneSlice(value.Config.Cmd), Environment: cloneSlice(value.Config.Env),
		WorkingDir: value.Config.WorkingDir, User: value.Config.User, AttachStdin: value.Config.AttachStdin, AttachStdout: value.Config.AttachStdout,
		AttachStderr: value.Config.AttachStderr, OpenStdin: value.Config.OpenStdin, StdinOnce: value.Config.StdinOnce,
		TTY: value.Config.Tty, NetworkDisabled: value.Config.NetworkDisabled,
		MacAddress: value.Config.MacAddress, ExposedPorts: mapKeys(value.Config.ExposedPorts), DeclaredVolumes: mapKeys(value.Config.Volumes),
		StopSignal: value.Config.StopSignal, ReadOnlyRoot: value.HostConfig.ReadonlyRootfs,
		Privileged: value.HostConfig.Privileged, RestartPolicy: RestartPolicy{
			Name: value.HostConfig.RestartPolicy.Name, MaximumRetryCount: value.HostConfig.RestartPolicy.MaximumRetryCount,
		}, AutoRemove: value.HostConfig.AutoRemove, NetworkMode: value.HostConfig.NetworkMode,
		PIDMode: value.HostConfig.PidMode, IPCMode: value.HostConfig.IpcMode, CgroupMode: value.HostConfig.CgroupnsMode,
		UsernsMode: value.HostConfig.UsernsMode, UTSMode: value.HostConfig.UTSMode,
		CapabilitiesAdd: cloneSlice(value.HostConfig.CapAdd), CapabilitiesDrop: cloneSlice(value.HostConfig.CapDrop),
		GroupAdd: cloneSlice(value.HostConfig.GroupAdd), SecurityOptions: cloneSlice(value.HostConfig.SecurityOpt), Tmpfs: cloneMap(value.HostConfig.Tmpfs),
		DeviceCgroupRules: cloneSlice(value.HostConfig.DeviceCgroupRules), ConfiguredMounts: cloneConfiguredMounts(value.HostConfig.Mounts),
		Binds: cloneSlice(value.HostConfig.Binds), VolumesFrom: cloneSlice(value.HostConfig.VolumesFrom),
		ContainerIDFile: value.HostConfig.ContainerIDFile,
		PublishAllPorts: value.HostConfig.PublishAllPorts, PortBindings: clonePortBindings(value.HostConfig.PortBindings),
		ExtraHosts: cloneSlice(value.HostConfig.ExtraHosts), DNS: cloneSlice(value.HostConfig.DNS), DNSOptions: cloneSlice(value.HostConfig.DNSOptions),
		DNSSearch: cloneSlice(value.HostConfig.DNSSearch), Links: cloneSlice(value.HostConfig.Links), OomScoreAdj: value.HostConfig.OomScoreAdj,
		Cgroup: value.HostConfig.Cgroup, CgroupParent: value.HostConfig.CgroupParent,
		MemoryReservation: value.HostConfig.MemoryReservation, KernelMemory: value.HostConfig.KernelMemory,
		KernelMemoryTCP: value.HostConfig.KernelMemoryTCP, CPUShares: value.HostConfig.CPUShares, CPUPeriod: value.HostConfig.CPUPeriod,
		CPUQuota: value.HostConfig.CPUQuota, CPURealtimePeriod: value.HostConfig.CPURealtimePeriod, CPURealtimeRuntime: value.HostConfig.CPURealtimeRuntime,
		CpusetCPUs: value.HostConfig.CpusetCpus, CpusetMems: value.HostConfig.CpusetMems, BlkioWeight: value.HostConfig.BlkioWeight,
		BlkioWeightDevice: cloneWeightDevices(value.HostConfig.BlkioWeightDevice), BlkioDeviceReadBps: cloneThrottleDevices(value.HostConfig.BlkioDeviceReadBps),
		BlkioDeviceWriteBps: cloneThrottleDevices(value.HostConfig.BlkioDeviceWriteBps), BlkioDeviceReadIOps: cloneThrottleDevices(value.HostConfig.BlkioDeviceReadIOps),
		BlkioDeviceWriteIOps: cloneThrottleDevices(value.HostConfig.BlkioDeviceWriteIOps), CPUCount: value.HostConfig.CPUCount, CPUPercent: value.HostConfig.CPUPercent,
		IOMaximumBandwidth: value.HostConfig.IOMaximumBandwidth, IOMaximumIOps: value.HostConfig.IOMaximumIOps, Ulimits: cloneUlimits(value.HostConfig.Ulimits),
		Sysctls: cloneMap(value.HostConfig.Sysctls), MaskedPaths: cloneSlice(value.HostConfig.MaskedPaths), ReadonlyPaths: cloneSlice(value.HostConfig.ReadonlyPaths),
		ShmSize: value.HostConfig.ShmSize, LogConfig: LogConfiguration{Type: value.HostConfig.LogConfig.Type, Config: cloneMap(value.HostConfig.LogConfig.Config)},
		VolumeDriver: value.HostConfig.VolumeDriver, StorageOptions: cloneMap(value.HostConfig.StorageOpt), Isolation: value.HostConfig.Isolation,
		Annotations: cloneMap(value.HostConfig.Annotations),
		MemoryBytes: value.HostConfig.Memory, MemorySwapBytes: value.HostConfig.MemorySwap, NanoCPUs: value.HostConfig.NanoCPUs,
	}
	if value.Config.Healthcheck != nil {
		configuration.HealthcheckKnown = true
		configuration.Healthcheck = Healthcheck{
			Test: cloneSlice(value.Config.Healthcheck.Test), Interval: value.Config.Healthcheck.Interval, Timeout: value.Config.Healthcheck.Timeout,
			StartPeriod: value.Config.Healthcheck.StartPeriod, StartInterval: value.Config.Healthcheck.StartInterval, Retries: value.Config.Healthcheck.Retries,
		}
	}
	if value.Config.StopTimeout != nil {
		configuration.StopTimeout, configuration.StopTimeoutKnown = *value.Config.StopTimeout, true
	}
	if value.HostConfig.OomKillDisable != nil {
		configuration.OomKillDisable = *value.HostConfig.OomKillDisable
	}
	if value.HostConfig.MemorySwappiness != nil {
		configuration.MemorySwappiness, configuration.MemorySwappinessKnown = *value.HostConfig.MemorySwappiness, true
	}
	if value.HostConfig.Init != nil {
		configuration.Init, configuration.InitKnown = *value.HostConfig.Init, true
	}
	if value.HostConfig.PidsLimit != nil {
		configuration.PIDs = *value.HostConfig.PidsLimit
	}
	for _, device := range value.HostConfig.Devices {
		configuration.Devices = append(configuration.Devices, Device{HostPath: device.PathOnHost, ContainerPath: device.PathInContainer, Permissions: device.CgroupPermissions})
	}
	for _, request := range value.HostConfig.DeviceRequests {
		configuration.DeviceRequests = append(configuration.DeviceRequests, DeviceRequest{
			Driver: request.Driver, Count: request.Count, DeviceIDs: cloneSlice(request.DeviceIDs),
			Capabilities: cloneNestedSlice(request.Capabilities), Options: cloneMap(request.Options),
		})
	}
	for _, mount := range value.Mounts {
		configuration.Mounts = append(configuration.Mounts, Mount{
			Type: mount.Type, Name: mount.Name, Source: mount.Source, Destination: mount.Destination, ReadOnly: !mount.RW,
			Mode: mount.Mode, Propagation: mount.Propagation, Driver: mount.Driver,
		})
	}
	for name := range value.NetworkSettings.Networks {
		configuration.NetworkAttachments = append(configuration.NetworkAttachments, name)
	}
	return Container{
		ID: value.ID, Name: strings.TrimPrefix(value.Name, "/"), Running: value.State.Running, Paused: value.State.Paused,
		Restarting: value.State.Restarting, Dead: value.State.Dead, Status: value.State.Status,
		Labels: cloneMap(value.Config.Labels), CgroupID: cgroupID, Configuration: configuration,
	}
}

// PlanDigest binds an immutable driver plan to a world label. Payloads must be
// structs or otherwise have deterministic JSON encoding.
func PlanDigest(kind string, payload any) (domain.Digest, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(append([]byte("world-physical-plan-v1\x00"+kind+"\x00"), encoded...)), nil
}

func RuntimeNames(values map[string]json.RawMessage) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func SupportsRuntime(values []string, name string) bool {
	for _, value := range values {
		if value == name {
			return true
		}
	}
	return false
}

func nonEmptyExactLines(value []byte) []string {
	lines := strings.Split(strings.ReplaceAll(string(value), "\r\n", "\n"), "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			result = append(result, line)
		}
	}
	return result
}

func cloneSlice(values []string) []string { return append([]string(nil), values...) }

func cloneNestedSlice(values [][]string) [][]string {
	result := make([][]string, len(values))
	for index := range values {
		result[index] = cloneSlice(values[index])
	}
	return result
}

func cloneMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

func mapKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	return result
}

func cloneWeightDevices(values []struct {
	Path   string
	Weight uint16
}) []WeightDevice {
	result := make([]WeightDevice, len(values))
	for index, value := range values {
		result[index] = WeightDevice{Path: value.Path, Weight: value.Weight}
	}
	return result
}

func cloneThrottleDevices(values []struct {
	Path string
	Rate uint64
}) []ThrottleDevice {
	result := make([]ThrottleDevice, len(values))
	for index, value := range values {
		result[index] = ThrottleDevice{Path: value.Path, Rate: value.Rate}
	}
	return result
}

func cloneUlimits(values []struct {
	Name string
	Soft int64
	Hard int64
}) []Ulimit {
	result := make([]Ulimit, len(values))
	for index, value := range values {
		result[index] = Ulimit{Name: value.Name, Soft: value.Soft, Hard: value.Hard}
	}
	return result
}

func clonePortBindings(values map[string][]struct {
	HostIP   string
	HostPort string
}) map[string][]PortBinding {
	result := make(map[string][]PortBinding, len(values))
	for port, bindings := range values {
		cloned := make([]PortBinding, len(bindings))
		for index, binding := range bindings {
			cloned[index] = PortBinding{HostIP: binding.HostIP, HostPort: binding.HostPort}
		}
		result[port] = cloned
	}
	return result
}

func cloneConfiguredMounts(values []dockerConfiguredMount) []ConfiguredMount {
	result := make([]ConfiguredMount, len(values))
	for index, value := range values {
		mount := ConfiguredMount{
			Type: value.Type, Source: value.Source, Target: value.Target, ReadOnly: value.ReadOnly, Consistency: value.Consistency,
		}
		if value.BindOptions != nil {
			mount.BindOptionsKnown = true
			mount.BindOptions = BindOptions{
				Propagation: value.BindOptions.Propagation, NonRecursive: value.BindOptions.NonRecursive,
				CreateMountpoint: value.BindOptions.CreateMountpoint, ReadOnlyNonRecursive: value.BindOptions.ReadOnlyNonRecursive,
				ReadOnlyForceRecursive: value.BindOptions.ReadOnlyForceRecursive,
			}
		}
		if value.VolumeOptions != nil {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions = VolumeOptions{
				NoCopy: value.VolumeOptions.NoCopy, Labels: cloneMap(value.VolumeOptions.Labels), Subpath: value.VolumeOptions.Subpath,
			}
			if value.VolumeOptions.DriverConfig != nil {
				mount.VolumeOptions.DriverKnown = true
				mount.VolumeOptions.Driver = MountDriver{Name: value.VolumeOptions.DriverConfig.Name, Options: cloneMap(value.VolumeOptions.DriverConfig.Options)}
			}
		}
		if value.TmpfsOptions != nil {
			mount.TmpfsOptionsKnown = true
			mount.TmpfsOptions = TmpfsOptions{SizeBytes: value.TmpfsOptions.SizeBytes, Mode: value.TmpfsOptions.Mode, Options: cloneNestedSlice(value.TmpfsOptions.Options)}
		}
		if value.ImageOptions != nil {
			mount.ImageOptionsKnown = true
			mount.ImageOptions = ImageOptions{Subpath: value.ImageOptions.Subpath}
		}
		mount.ClusterOptionsKnown = value.ClusterOptions != nil
		result[index] = mount
	}
	return result
}

type dockerConfiguredMount struct {
	Type        string
	Source      string
	Target      string
	ReadOnly    bool
	Consistency string
	BindOptions *struct {
		Propagation            string
		NonRecursive           bool
		CreateMountpoint       bool
		ReadOnlyNonRecursive   bool
		ReadOnlyForceRecursive bool
	}
	VolumeOptions *struct {
		NoCopy       bool
		Labels       map[string]string
		Subpath      string
		DriverConfig *struct {
			Name    string
			Options map[string]string
		}
	}
	TmpfsOptions *struct {
		SizeBytes int64
		Mode      uint32
		Options   [][]string
	}
	ImageOptions *struct {
		Subpath string
	}
	ClusterOptions *struct{}
}

type dockerInspect struct {
	ID    string
	Name  string
	State struct {
		Running    bool
		Paused     bool
		Restarting bool
		Dead       bool
		Status     string
		PID        int `json:"Pid"`
	}
	Config struct {
		Image           string
		Labels          map[string]string
		Hostname        string
		Domainname      string
		Entrypoint      []string
		Cmd             []string
		Env             []string
		WorkingDir      string
		User            string
		AttachStdin     bool
		AttachStdout    bool
		AttachStderr    bool
		OpenStdin       bool
		StdinOnce       bool
		Tty             bool
		NetworkDisabled bool
		MacAddress      string
		ExposedPorts    map[string]struct{}
		Volumes         map[string]struct{}
		Healthcheck     *struct {
			Test          []string
			Interval      int64
			Timeout       int64
			StartPeriod   int64
			StartInterval int64
			Retries       int
		}
		StopSignal  string
		StopTimeout *int
	}
	HostConfig struct {
		Runtime        string
		ReadonlyRootfs bool
		Privileged     bool
		RestartPolicy  struct {
			Name              string
			MaximumRetryCount int
		}
		AutoRemove         bool
		NetworkMode        string
		PidMode            string
		IpcMode            string
		CgroupnsMode       string
		UsernsMode         string
		UTSMode            string
		CapAdd             []string
		CapDrop            []string
		GroupAdd           []string
		SecurityOpt        []string
		Tmpfs              map[string]string
		Memory             int64
		MemoryReservation  int64
		MemorySwap         int64
		MemorySwappiness   *int64
		KernelMemory       int64
		KernelMemoryTCP    int64
		CPUShares          int64 `json:"CpuShares"`
		CPUPeriod          int64 `json:"CpuPeriod"`
		CPUQuota           int64 `json:"CpuQuota"`
		CPURealtimePeriod  int64 `json:"CpuRealtimePeriod"`
		CPURealtimeRuntime int64 `json:"CpuRealtimeRuntime"`
		CpusetCpus         string
		CpusetMems         string
		NanoCPUs           int64
		PidsLimit          *int64
		Init               *bool
		Mounts             []dockerConfiguredMount
		Binds              []string
		VolumesFrom        []string
		ContainerIDFile    string
		DeviceCgroupRules  []string
		DeviceRequests     []struct {
			Driver       string
			Count        int
			DeviceIDs    []string
			Capabilities [][]string
			Options      map[string]string
		}
		PublishAllPorts bool
		PortBindings    map[string][]struct {
			HostIP   string
			HostPort string
		}
		ExtraHosts        []string
		DNS               []string `json:"Dns"`
		DNSOptions        []string `json:"DnsOptions"`
		DNSSearch         []string `json:"DnsSearch"`
		Links             []string
		OomKillDisable    *bool
		OomScoreAdj       int
		Cgroup            string
		CgroupParent      string
		BlkioWeight       uint16
		BlkioWeightDevice []struct {
			Path   string
			Weight uint16
		}
		BlkioDeviceReadBps []struct {
			Path string
			Rate uint64
		}
		BlkioDeviceWriteBps []struct {
			Path string
			Rate uint64
		}
		BlkioDeviceReadIOps []struct {
			Path string
			Rate uint64
		}
		BlkioDeviceWriteIOps []struct {
			Path string
			Rate uint64
		}
		CPUCount           int64 `json:"CpuCount"`
		CPUPercent         int64 `json:"CpuPercent"`
		IOMaximumBandwidth uint64
		IOMaximumIOps      uint64
		Ulimits            []struct {
			Name string
			Soft int64
			Hard int64
		}
		Sysctls       map[string]string
		MaskedPaths   []string
		ReadonlyPaths []string
		ShmSize       int64
		LogConfig     struct {
			Type   string
			Config map[string]string
		}
		VolumeDriver string
		StorageOpt   map[string]string
		Isolation    string
		Annotations  map[string]string
		Devices      []struct {
			PathOnHost        string
			PathInContainer   string
			CgroupPermissions string
		}
	}
	NetworkSettings struct {
		Networks map[string]json.RawMessage
	}
	Mounts []struct {
		Type        string
		Name        string
		Source      string
		Destination string
		RW          bool
		Mode        string
		Propagation string
		Driver      string
	}
}
