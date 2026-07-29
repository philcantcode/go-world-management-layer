package linuxcontainer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type RuntimeCapabilities struct {
	Version         string
	APIVersion      string
	CgroupVersion   string
	OSType          string
	SecurityOptions []string
	DefaultRuntime  string
	Runtimes        []string
	CPUCFSQuota     bool
	MemoryLimit     bool
	SwapLimit       bool
	PIDsLimit       bool
}

type RuntimeState struct {
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

type Runtime interface {
	Probe(context.Context) (RuntimeCapabilities, error)
	Create(context.Context, ContainerPlan) (string, error)
	Start(context.Context, string) error
	Inspect(context.Context, string) (RuntimeState, error)
	Stop(context.Context, string, ports.StopMode) error
	Remove(context.Context, string) error
	OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error)
}

// RuntimeInventory is optional. A runtime that implements it promises that a
// successful call is a complete, bounded observation of live resources.
type RuntimeInventory interface {
	ListContainers(context.Context) ([]RuntimeState, error)
}

// RuntimeContainment is implemented only by runtimes that can both contain an
// existing runtime and authoritatively observe the result. Keeping it separate
// from Runtime makes lack of containment an explicit capability failure.
type RuntimeContainment interface {
	Quarantine(context.Context, string) (RuntimeContainmentEvidence, error)
}

type RuntimeContainmentEvidence struct {
	RuntimeID          string
	ExecutionStopped   bool
	NetworkUnreachable bool
	StatePreserved     bool
	ObservedAt         time.Time
}

type DockerRuntime struct {
	Binary  string
	Runner  command.Runner
	Starter command.Starter
}

func NewDockerRuntime(binary string, runner command.Runner, starter command.Starter) *DockerRuntime {
	if binary == "" {
		binary = "docker"
	}
	if runner == nil {
		runner = command.OS{}
	}
	if starter == nil {
		starter = command.OS{}
	}
	return &DockerRuntime{Binary: binary, Runner: runner, Starter: starter}
}

func (r *DockerRuntime) Probe(ctx context.Context) (RuntimeCapabilities, error) {
	version, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: []string{"version", "--format", "{{json .Server}}"}})
	if err != nil {
		return RuntimeCapabilities{}, err
	}
	var server struct{ Version, APIVersion, Os string }
	if err := json.Unmarshal(version.Stdout, &server); err != nil {
		return RuntimeCapabilities{}, err
	}
	info, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: []string{"info", "--format", "{{json .}}"}})
	if err != nil {
		return RuntimeCapabilities{}, err
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
		return RuntimeCapabilities{}, err
	}
	return RuntimeCapabilities{
		Version: server.Version, APIVersion: server.APIVersion, CgroupVersion: details.CgroupVersion, OSType: server.Os,
		SecurityOptions: append([]string(nil), details.SecurityOptions...), DefaultRuntime: details.DefaultRuntime,
		Runtimes: dockercli.RuntimeNames(details.Runtimes), CPUCFSQuota: details.CPUCFSQuota,
		MemoryLimit: details.MemoryLimit, SwapLimit: details.SwapLimit, PIDsLimit: details.PIDsLimit,
	}, nil
}

func (r *DockerRuntime) Create(ctx context.Context, plan ContainerPlan) (string, error) {
	arguments, err := plan.DockerCreateArgs()
	if err != nil {
		return "", err
	}
	result, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: arguments})
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(result.Stdout))
	if err := dockercli.RequireCanonicalContainerID(id); err != nil {
		return "", fmt.Errorf("Docker create returned an invalid container ID: %w", err)
	}
	return id, nil
}

func (r *DockerRuntime) Start(ctx context.Context, id string) error {
	_, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: []string{"start", id}})
	return err
}

func (r *DockerRuntime) Inspect(ctx context.Context, id string) (RuntimeState, error) {
	container, err := dockercli.Inspect(ctx, r.Binary, r.Runner, id)
	if err != nil {
		return RuntimeState{}, err
	}
	return runtimeState(container), nil
}

func (r *DockerRuntime) ListContainers(ctx context.Context) ([]RuntimeState, error) {
	containers, err := dockercli.Inventory(ctx, r.Binary, r.Runner)
	if err != nil {
		return nil, err
	}
	states := make([]RuntimeState, 0, len(containers))
	for _, container := range containers {
		states = append(states, runtimeState(container))
	}
	return states, nil
}

func (r *DockerRuntime) Stop(ctx context.Context, id string, mode ports.StopMode) error {
	args := []string{"stop", "--time", "10", id}
	if mode == ports.StopImmediate {
		args = []string{"stop", "--time", "1", id}
	}
	if mode == ports.StopForce {
		args = []string{"kill", "--signal", "KILL", id}
	}
	_, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: args})
	return err
}

func (r *DockerRuntime) Quarantine(ctx context.Context, id string) (RuntimeContainmentEvidence, error) {
	if err := dockercli.RequireCanonicalContainerID(id); err != nil {
		return RuntimeContainmentEvidence{}, fmt.Errorf("canonical runtime ID is required: %w", err)
	}
	state, err := r.Inspect(ctx, id)
	if err != nil {
		return RuntimeContainmentEvidence{}, err
	}
	if state.ID != id {
		return RuntimeContainmentEvidence{}, fmt.Errorf("Docker inspect returned runtime %q for %q", state.ID, id)
	}
	if state.Running {
		if err := r.Stop(ctx, id, ports.StopForce); err != nil {
			return RuntimeContainmentEvidence{}, err
		}
		state, err = r.Inspect(ctx, id)
		if err != nil {
			return RuntimeContainmentEvidence{}, err
		}
		if state.ID != id {
			return RuntimeContainmentEvidence{}, fmt.Errorf("Docker inspect returned runtime %q for %q after containment", state.ID, id)
		}
		if err := dockercli.RequireExactStoppedState(state.Running, state.Paused, state.Restarting, state.Dead, state.Status, dockercli.StoppedStatusExited); err != nil {
			return RuntimeContainmentEvidence{}, fmt.Errorf("Docker runtime %q did not reach an exact stopped state after containment: %w", id, err)
		}
	} else if err := dockercli.RequireExactStoppedState(state.Running, state.Paused, state.Restarting, state.Dead, state.Status, dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
		return RuntimeContainmentEvidence{}, fmt.Errorf("Docker runtime %q is not in an exact containable stopped state: %w", id, err)
	}
	// A confirmed stopped container has no executing network namespace, while
	// successful inspect proves Docker retained its writable layer and metadata.
	return RuntimeContainmentEvidence{
		RuntimeID: id, ExecutionStopped: true, NetworkUnreachable: true,
		StatePreserved: true, ObservedAt: time.Now().UTC(),
	}, nil
}

func (r *DockerRuntime) Remove(ctx context.Context, id string) error {
	_, err := r.Runner.Run(ctx, command.Invocation{Program: r.Binary, Args: []string{"rm", "--force", "--volumes", id}})
	return err
}

func (r *DockerRuntime) OpenExec(ctx context.Context, id string, plan ports.TargetExecPlan) (ports.ExecTransport, error) {
	invocation := command.Invocation{Program: r.Binary, Args: []string{"exec", "--interactive", id, targetGuestBinary}}
	return command.StartGuestTransport(ctx, r.Starter, invocation, plan.Start, plan.Start.CleanupGrace)
}

var _ Runtime = (*DockerRuntime)(nil)
var _ RuntimeContainment = (*DockerRuntime)(nil)
var _ RuntimeInventory = (*DockerRuntime)(nil)

func runtimeState(container dockercli.Container) RuntimeState {
	return RuntimeState{
		ID: container.ID, Name: container.Name, Running: container.Running, Paused: container.Paused,
		Restarting: container.Restarting, Dead: container.Dead, Status: container.Status,
		Labels: container.Labels, CgroupID: container.CgroupID, Configuration: container.Configuration,
	}
}
