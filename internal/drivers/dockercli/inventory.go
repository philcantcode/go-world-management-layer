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
	Source      string
	Destination string
	ReadOnly    bool
}

type Device struct {
	HostPath      string
	ContainerPath string
	Permissions   string
}

type Configuration struct {
	Image            string
	Runtime          string
	Entrypoint       []string
	Command          []string
	User             string
	OpenStdin        bool
	ReadOnlyRoot     bool
	Privileged       bool
	NetworkMode      string
	PIDMode          string
	IPCMode          string
	CgroupMode       string
	Init             bool
	InitKnown        bool
	CapabilitiesAdd  []string
	CapabilitiesDrop []string
	SecurityOptions  []string
	Tmpfs            map[string]string
	Devices          []Device
	Mounts           []Mount
	MemoryBytes      int64
	MemorySwapBytes  int64
	NanoCPUs         int64
	PIDs             int64
}

type Container struct {
	ID            string
	Name          string
	Running       bool
	Status        string
	Labels        map[string]string
	CgroupID      string
	Configuration Configuration
}

// Inventory lists every Docker container and inspects each one. Returning a
// partial inventory would let callers misclassify an uninspected resource as
// absent, so any list or inspect failure fails the whole observation.
func Inventory(ctx context.Context, binary string, runner command.Runner) ([]Container, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, fmt.Errorf("Docker inventory requires a context deadline")
	}
	result, err := runner.Run(ctx, command.Invocation{
		Program:       binary,
		Args:          []string{"ps", "--all", "--no-trunc", "--format", "{{.ID}}"},
		MaximumOutput: int64(MaximumInventoryContainers+1) * 129,
	})
	if err != nil {
		return nil, err
	}
	ids := nonEmptyLines(result.Stdout)
	if len(ids) > MaximumInventoryContainers {
		return nil, fmt.Errorf("Docker inventory exceeds the %d-container safety bound", MaximumInventoryContainers)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("Docker inventory returned duplicate container ID %q", id)
		}
		seen[id] = struct{}{}
	}
	containers := make([]Container, 0, len(ids))
	for start := 0; start < len(ids); start += maximumInspectBatch {
		end := min(start+maximumInspectBatch, len(ids))
		batchIDs := ids[start:end]
		batch, err := inspectContainers(ctx, binary, runner, batchIDs)
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
	containers, err := inspectContainers(ctx, binary, runner, []string{id})
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
	if len(ids) == 0 || len(ids) > maximumInspectBatch {
		return nil, fmt.Errorf("Docker inspect batch size %d is outside 1..%d", len(ids), maximumInspectBatch)
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
		if strings.TrimSpace(value.ID) == "" {
			return nil, fmt.Errorf("Docker inspect returned an empty resource ID")
		}
		containers = append(containers, containerFromInspect(value))
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

func containerFromInspect(value dockerInspect) Container {
	configuration := Configuration{
		Image: value.Config.Image, Runtime: value.HostConfig.Runtime, Entrypoint: cloneSlice(value.Config.Entrypoint), Command: cloneSlice(value.Config.Cmd),
		User: value.Config.User, OpenStdin: value.Config.OpenStdin, ReadOnlyRoot: value.HostConfig.ReadonlyRootfs,
		Privileged: value.HostConfig.Privileged, NetworkMode: value.HostConfig.NetworkMode,
		PIDMode: value.HostConfig.PidMode, IPCMode: value.HostConfig.IpcMode, CgroupMode: value.HostConfig.CgroupnsMode,
		CapabilitiesAdd: cloneSlice(value.HostConfig.CapAdd), CapabilitiesDrop: cloneSlice(value.HostConfig.CapDrop),
		SecurityOptions: cloneSlice(value.HostConfig.SecurityOpt), Tmpfs: cloneMap(value.HostConfig.Tmpfs),
		MemoryBytes: value.HostConfig.Memory, MemorySwapBytes: value.HostConfig.MemorySwap, NanoCPUs: value.HostConfig.NanoCPUs,
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
	for _, mount := range value.Mounts {
		configuration.Mounts = append(configuration.Mounts, Mount{Type: mount.Type, Source: mount.Source, Destination: mount.Destination, ReadOnly: !mount.RW})
	}
	return Container{
		ID: value.ID, Name: strings.TrimPrefix(value.Name, "/"), Running: value.State.Running, Status: value.State.Status,
		Labels: cloneMap(value.Config.Labels), CgroupID: value.HostConfig.CgroupParent, Configuration: configuration,
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

func nonEmptyLines(value []byte) []string {
	lines := strings.Split(strings.ReplaceAll(string(value), "\r\n", "\n"), "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

func cloneSlice(values []string) []string { return append([]string(nil), values...) }

func cloneMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

type dockerInspect struct {
	ID    string
	Name  string
	State struct {
		Running bool
		Status  string
	}
	Config struct {
		Image      string
		Labels     map[string]string
		Entrypoint []string
		Cmd        []string
		User       string
		OpenStdin  bool
	}
	HostConfig struct {
		CgroupParent   string
		Runtime        string
		ReadonlyRootfs bool
		Privileged     bool
		NetworkMode    string
		PidMode        string
		IpcMode        string
		CgroupnsMode   string
		CapAdd         []string
		CapDrop        []string
		SecurityOpt    []string
		Tmpfs          map[string]string
		Memory         int64
		MemorySwap     int64
		NanoCPUs       int64
		PidsLimit      *int64
		Init           *bool
		Devices        []struct {
			PathOnHost        string
			PathInContainer   string
			CgroupPermissions string
		}
	}
	Mounts []struct {
		Type        string
		Source      string
		Destination string
		RW          bool
	}
}
