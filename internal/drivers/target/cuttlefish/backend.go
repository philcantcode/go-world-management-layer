package cuttlefish

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type BackendCapabilities struct {
	BackendKind    string
	BackendVersion string
	RuntimeVersion string
	KVM            bool
	KVMKnown       bool
	ResetModes     []ports.ResetMode
	Evidence       map[string]string
}

type Instance struct {
	RuntimeID            string
	StateDirectory       string
	SystemImageDirectory string
	Allocation           Allocation
	Fingerprint          ResetFingerprint
}

type ReadinessState struct {
	ProcessRunning bool
	ADBReady       bool
	BootCompleted  bool
	FrameworkReady bool
	ObservedAt     time.Time
}

func (r ReadinessState) Ready() bool {
	return r.ProcessRunning && r.ADBReady && r.BootCompleted && r.FrameworkReady
}

type Backend interface {
	Probe(context.Context, ports.TargetTemplate) (BackendCapabilities, error)
	Create(context.Context, VirtualDevicePlan) (Instance, error)
	Start(context.Context, Instance) error
	WaitReady(context.Context, Instance) (ReadinessState, error)
	Inspect(context.Context, Instance) (ReadinessState, error)
	Stop(context.Context, Instance, ports.StopMode) error
	Destroy(context.Context, Instance) error
}

// BackendQuarantiner is optional because an attached external emulator cannot
// be stopped or isolated without violating its ownership contract.
type BackendQuarantiner interface {
	Quarantine(context.Context, Instance) (BackendQuarantineState, error)
}

type BackendQuarantineState struct {
	RuntimeID          string
	ExecutionStopped   bool
	NetworkUnreachable bool
	StatePreserved     bool
	ObservedAt         time.Time
}

type CommandBackendConfig struct {
	Runner         command.Runner
	LaunchBinary   string
	StopBinary     string
	CVDBinary      string
	ADBBinary      string
	PollInterval   time.Duration
	BackendVersion string
	RuntimeVersion string
}

type CommandBackend struct{ config CommandBackendConfig }

func NewCommandBackend(config CommandBackendConfig) *CommandBackend {
	if config.Runner == nil {
		config.Runner = command.OS{}
	}
	if config.LaunchBinary == "" {
		config.LaunchBinary = "launch_cvd"
	}
	if config.StopBinary == "" {
		config.StopBinary = "stop_cvd"
	}
	if config.CVDBinary == "" {
		config.CVDBinary = "cvd"
	}
	if config.ADBBinary == "" {
		config.ADBBinary = "adb"
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 500 * time.Millisecond
	}
	return &CommandBackend{config: config}
}

func (b *CommandBackend) Probe(ctx context.Context, _ ports.TargetTemplate) (BackendCapabilities, error) {
	result, err := b.config.Runner.Run(ctx, command.Invocation{Program: b.config.CVDBinary, Args: []string{"version"}})
	if err != nil {
		return BackendCapabilities{}, err
	}
	backendVersion := b.config.BackendVersion
	if backendVersion == "" {
		backendVersion = strings.TrimSpace(string(result.Stdout))
	}
	return BackendCapabilities{BackendKind: "cuttlefish", BackendVersion: backendVersion, RuntimeVersion: b.config.RuntimeVersion, KVM: true, KVMKnown: true, ResetModes: []ports.ResetMode{ports.ResetRecreate, ports.ResetBaseline}}, nil
}

func (b *CommandBackend) Create(_ context.Context, plan VirtualDevicePlan) (Instance, error) {
	if err := os.MkdirAll(plan.StateDirectory, 0o700); err != nil {
		return Instance{}, err
	}
	return Instance{RuntimeID: plan.Allocation.InstanceName, StateDirectory: plan.StateDirectory, SystemImageDirectory: plan.SystemImageDirectory, Allocation: plan.Allocation, Fingerprint: plan.Fingerprint}, nil
}

func (b *CommandBackend) Start(ctx context.Context, instance Instance) error {
	_, err := b.config.Runner.Run(ctx, command.Invocation{Program: b.config.LaunchBinary, Args: []string{"--daemon", "--instance_dir=" + instance.StateDirectory, "--system_image_dir=" + instance.SystemImageDirectory, "--base_instance_num=" + strconv.Itoa(instance.Allocation.InstanceNumber)}})
	return err
}

func (b *CommandBackend) WaitReady(ctx context.Context, instance Instance) (ReadinessState, error) {
	for {
		state, err := b.Inspect(ctx, instance)
		if err == nil && state.Ready() {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return ReadinessState{}, ctx.Err()
		case <-time.After(b.config.PollInterval):
		}
	}
}

func (b *CommandBackend) Inspect(ctx context.Context, instance Instance) (ReadinessState, error) {
	state := ReadinessState{ProcessRunning: true, ObservedAt: time.Now().UTC()}
	if _, err := b.runADB(ctx, instance, "get-state"); err != nil {
		return state, err
	}
	state.ADBReady = true
	boot, err := b.runADB(ctx, instance, "shell", "getprop", "sys.boot_completed")
	if err != nil {
		return state, err
	}
	state.BootCompleted = strings.TrimSpace(string(boot.Stdout)) == "1"
	framework, err := b.runADB(ctx, instance, "shell", "getprop", "init.svc.bootanim")
	if err != nil {
		return state, err
	}
	state.FrameworkReady = strings.TrimSpace(string(framework.Stdout)) == "stopped"
	return state, nil
}

func (b *CommandBackend) runADB(ctx context.Context, instance Instance, args ...string) (command.Result, error) {
	return runExactSerialADB(ctx, b.config.Runner, b.config.ADBBinary, instance.Allocation.Serial, command.DefaultOutputLimit, args...)
}

func (b *CommandBackend) Stop(ctx context.Context, instance Instance, mode ports.StopMode) error {
	args := []string{"--instance_dir=" + instance.StateDirectory}
	if mode == ports.StopForce {
		args = append(args, "--force")
	}
	_, err := b.config.Runner.Run(ctx, command.Invocation{Program: b.config.StopBinary, Args: args})
	return err
}

func (b *CommandBackend) Destroy(ctx context.Context, instance Instance) error {
	if err := b.Stop(ctx, instance, ports.StopForce); err != nil {
		return err
	}
	return os.RemoveAll(instance.StateDirectory)
}

func (b *CommandBackend) Quarantine(ctx context.Context, instance Instance) (BackendQuarantineState, error) {
	state := BackendQuarantineState{RuntimeID: instance.RuntimeID, ObservedAt: time.Now().UTC()}
	if strings.TrimSpace(instance.RuntimeID) == "" || strings.TrimSpace(instance.StateDirectory) == "" || !safeExactADBSerial(instance.Allocation.Serial) {
		return state, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.backend.quarantine", "instance", "runtime, state directory, and safe exact serial are required", nil)
	}
	if err := b.Stop(ctx, instance, ports.StopForce); err != nil {
		return state, err
	}
	state.ExecutionStopped = true
	probe, probeErr := b.runADB(ctx, instance, "get-state")
	if probeErr == nil {
		return state, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.backend.quarantine", "adb", "stopped exact device serial remains reachable", nil)
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if !confirmedADBUnreachable(probe, probeErr) {
		return state, domain.NewError(domain.CodeUnavailable, "cuttlefish.backend.quarantine", "adb", "ADB probe failed without confirming that the exact serial is unreachable", probeErr)
	}
	state.NetworkUnreachable = true
	info, err := os.Stat(instance.StateDirectory)
	if err != nil {
		return state, err
	}
	if !info.IsDir() {
		return state, fmt.Errorf("Cuttlefish state path is not a directory")
	}
	state.StatePreserved = true
	return state, nil
}

func confirmedADBUnreachable(result command.Result, err error) bool {
	if err == nil || result.ExitCode <= 0 {
		return false
	}
	output := strings.ToLower(string(result.Stderr) + "\n" + string(result.Stdout) + "\n" + err.Error())
	for _, marker := range []string{"not found", "offline", "no devices/emulators", "cannot connect", "failed to connect", "connection refused", "connection closed"} {
		if strings.Contains(output, marker) {
			return true
		}
	}
	return false
}

var _ Backend = (*CommandBackend)(nil)
var _ BackendQuarantiner = (*CommandBackend)(nil)
