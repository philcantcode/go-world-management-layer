package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type EngineCapabilities struct {
	EngineVersion   string
	APIVersion      string
	OSType          string
	Architecture    string
	CgroupVersion   string
	SecurityOptions []string
	DefaultRuntime  string
	Runtimes        []string
	CPUCFSQuota     bool
	MemoryLimit     bool
	SwapLimit       bool
	PIDsLimit       bool
}

type ContainerState struct {
	ID            string
	Name          string
	Running       bool
	Paused        bool
	Restarting    bool
	Dead          bool
	Status        string
	Labels        map[string]string
	CgroupID      string
	Configuration dockercli.Configuration
}

type Engine interface {
	Probe(context.Context) (EngineCapabilities, error)
	Create(context.Context, ContainerPlan) (string, error)
	Start(context.Context, string) error
	Inspect(context.Context, string) (ContainerState, error)
	Stop(context.Context, string, ports.StopMode) error
	Remove(context.Context, string) error
	OpenExec(context.Context, string, string, ports.ExecPlan) (ports.ExecTransport, error)
}

// EngineInventory is a separate capability so probe-only engines do not need
// to claim physical authority. Provision, reconciliation, and restart-safe
// destruction require it and fail closed when it is unavailable.
type EngineInventory interface {
	ListContainers(context.Context) ([]ContainerState, error)
}

type CLIEngine struct {
	Binary  string
	Runner  command.Runner
	Starter command.Starter
}

func NewCLIEngine(binary string, runner command.Runner, starter command.Starter) *CLIEngine {
	if binary == "" {
		binary = "docker"
	}
	if runner == nil {
		runner = command.OS{}
	}
	if starter == nil {
		starter = command.OS{}
	}
	return &CLIEngine{Binary: binary, Runner: runner, Starter: starter}
}

func (e *CLIEngine) Probe(ctx context.Context) (EngineCapabilities, error) {
	result, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: []string{"version", "--format", "{{json .Server}}"}})
	if err != nil {
		return EngineCapabilities{}, err
	}
	var server struct {
		Version    string
		APIVersion string
		Os         string
		Arch       string
	}
	if err := json.Unmarshal(result.Stdout, &server); err != nil {
		return EngineCapabilities{}, fmt.Errorf("decode Docker version: %w", err)
	}
	info, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: []string{"info", "--format", "{{json .}}"}})
	if err != nil {
		return EngineCapabilities{}, err
	}
	var details struct {
		CgroupVersion   string
		SecurityOptions []string
		DefaultRuntime  string
		Runtimes        map[string]json.RawMessage
		CPUCFSQuota     bool `json:"CpuCfsQuota"`
		MemoryLimit     bool `json:"MemoryLimit"`
		SwapLimit       bool `json:"SwapLimit"`
		PIDsLimit       bool `json:"PidsLimit"`
	}
	if err := json.Unmarshal(info.Stdout, &details); err != nil {
		return EngineCapabilities{}, fmt.Errorf("decode Docker info: %w", err)
	}
	return EngineCapabilities{
		EngineVersion: server.Version, APIVersion: server.APIVersion, OSType: server.Os, Architecture: server.Arch,
		CgroupVersion: details.CgroupVersion, SecurityOptions: append([]string(nil), details.SecurityOptions...),
		DefaultRuntime: details.DefaultRuntime, Runtimes: dockercli.RuntimeNames(details.Runtimes),
		CPUCFSQuota: details.CPUCFSQuota, MemoryLimit: details.MemoryLimit, SwapLimit: details.SwapLimit, PIDsLimit: details.PIDsLimit,
	}, nil
}

func (e *CLIEngine) Create(ctx context.Context, plan ContainerPlan) (string, error) {
	arguments, err := plan.DockerCreateArgs()
	if err != nil {
		return "", err
	}
	result, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: arguments})
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(result.Stdout))
	if err := dockercli.RequireCanonicalContainerID(id); err != nil {
		return "", fmt.Errorf("Docker create returned an invalid container ID: %w", err)
	}
	return id, nil
}

func (e *CLIEngine) Start(ctx context.Context, id string) error {
	_, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: []string{"start", id}})
	return err
}

func (e *CLIEngine) Inspect(ctx context.Context, id string) (ContainerState, error) {
	container, err := dockercli.Inspect(ctx, e.Binary, e.Runner, id)
	if err != nil {
		return ContainerState{}, err
	}
	return containerState(container), nil
}

func (e *CLIEngine) ListContainers(ctx context.Context) ([]ContainerState, error) {
	containers, err := dockercli.Inventory(ctx, e.Binary, e.Runner)
	if err != nil {
		return nil, err
	}
	states := make([]ContainerState, 0, len(containers))
	for _, container := range containers {
		states = append(states, containerState(container))
	}
	return states, nil
}

func (e *CLIEngine) Stop(ctx context.Context, id string, mode ports.StopMode) error {
	args := []string{"stop", "--time", "10", id}
	if mode == ports.StopImmediate {
		args = []string{"stop", "--time", "1", id}
	}
	if mode == ports.StopForce {
		args = []string{"kill", "--signal", "KILL", id}
	}
	_, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: args})
	return err
}

func (e *CLIEngine) Remove(ctx context.Context, id string) error {
	_, err := e.Runner.Run(ctx, command.Invocation{Program: e.Binary, Args: []string{"rm", "--force", "--volumes", id}})
	return err
}

func (e *CLIEngine) OpenExec(ctx context.Context, id, guestBinary string, plan ports.ExecPlan) (ports.ExecTransport, error) {
	start := plan.Start
	args := []string{"exec", "--interactive", "--workdir", WorkspaceMount}
	names := make([]string, 0, len(start.Environment))
	for name := range start.Environment {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--env", name+"="+start.Environment[name])
	}
	args = append(args, id, guestBinary)
	invocation := command.Invocation{Program: e.Binary, Args: args}
	return command.StartGuestTransport(ctx, e.Starter, invocation, start, start.CleanupGrace)
}

var _ Engine = (*CLIEngine)(nil)
var _ EngineInventory = (*CLIEngine)(nil)

func containerState(container dockercli.Container) ContainerState {
	return ContainerState{
		ID: container.ID, Name: container.Name, Running: container.Running, Paused: container.Paused,
		Restarting: container.Restarting, Dead: container.Dead, Status: container.Status,
		Labels: container.Labels, CgroupID: container.CgroupID, Configuration: container.Configuration,
	}
}
