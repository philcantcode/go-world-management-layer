package process

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAndroidLogcatBindsCollectorAndReadinessToAuthorityDevice(t *testing.T) {
	configuration := exactAdapterConfiguration()
	adapter, err := BuildAdapter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	var readinessInvocation command.Invocation
	readiness := adapter.Readiness.(CommandReadiness)
	readiness.Runner = runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		readinessInvocation = invocation
		return command.Result{}, nil
	})
	adapter.Readiness = readiness
	process := newObserverProcess()
	var collectorInvocation command.Invocation
	driver, err := New(Config{
		Starter: starterFunc(func(_ context.Context, invocation command.Invocation) (command.Process, error) {
			collectorInvocation = invocation
			return process, nil
		}),
		Adapters: []Adapter{adapter}, Outputs: &memoryOutputFactory{capture: newMemoryCapture()},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := androidLogcatCollectorPlan(t, configuration, adapter.ConfigurationDigest)
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	prefix := []string{"-H", "127.0.0.1", "-P", "5041", "-s", "emulator-5578"}
	wantCollector := append(append([]string(nil), prefix...), configuration.Args...)
	wantReadiness := append(append([]string(nil), prefix...), configuration.ReadinessArgs...)
	if !slices.Equal(collectorInvocation.Args, wantCollector) || !slices.Equal(readinessInvocation.Args, wantReadiness) {
		t.Fatalf("collector args = %v, readiness args = %v", collectorInvocation.Args, readinessInvocation.Args)
	}
	if !reflect.DeepEqual(adapter.Args, configuration.Args) || !reflect.DeepEqual(readiness.Args, configuration.ReadinessArgs) {
		t.Fatalf("runtime binding mutated immutable adapter configuration: adapter=%v readiness=%v", adapter.Args, readiness.Args)
	}
	if _, err := driver.Stop(ctx, plan.CollectorID); err != nil {
		t.Fatal(err)
	}
}

func TestAndroidLogcatRejectsMissingMismatchedOrInjectedAuthorityBeforeStart(t *testing.T) {
	configuration := exactAdapterConfiguration()
	adapter, err := BuildAdapter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	readiness := adapter.Readiness.(CommandReadiness)
	readiness.Runner = runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
		return command.Result{}, nil
	})
	adapter.Readiness = readiness

	tests := []struct {
		name   string
		mutate func(*ports.CollectorPlan)
	}{
		{name: "missing device", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.ADBDevice = ports.ADBDeviceSelection{} }},
		{name: "non Android target", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.TargetKind = domain.TargetLinuxContainer }},
		{name: "remote server", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.ADBDevice.Server.Host = "192.0.2.1" }},
		{name: "zero server port", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.ADBDevice.Server.Port = 0 }},
		{name: "option serial", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.ADBDevice.Serial = "-e" }},
		{name: "newline serial", mutate: func(plan *ports.CollectorPlan) { plan.Attachment.ADBDevice.Serial = "emulator-5578\n-e" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			starts := 0
			driver, err := New(Config{
				Starter: starterFunc(func(context.Context, command.Invocation) (command.Process, error) {
					starts++
					return newObserverProcess(), nil
				}),
				Adapters: []Adapter{adapter}, Outputs: &memoryOutputFactory{capture: newMemoryCapture()},
			})
			if err != nil {
				t.Fatal(err)
			}
			plan := androidLogcatCollectorPlan(t, configuration, adapter.ConfigurationDigest)
			test.mutate(&plan)
			ctx, cancel := testContext(t)
			defer cancel()
			if _, err := driver.Start(ctx, plan); err == nil {
				t.Fatalf("Start() error = %v", err)
			}
			if starts != 0 {
				t.Fatalf("invalid authority started %d collector processes", starts)
			}
		})
	}
}

func TestAndroidRuntimeBoundAdapterRejectsManualInvariantBypass(t *testing.T) {
	base, err := BuildAdapter(exactAdapterConfiguration())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Adapter){
		"ambient device environment": func(value *Adapter) { value.Environment["ANDROID_SERIAL"] = "emulator-5580" },
		"ambient server environment": func(value *Adapter) { value.Environment["ADB_SERVER_SOCKET"] = "tcp:127.0.0.1:5038" },
		"selected version probe":     func(value *Adapter) { value.VersionArgs = []string{"-e", "version"} },
		"untyped readiness": func(value *Adapter) {
			readiness := value.Readiness.(CommandReadiness)
			readiness.RuntimeBinding = RuntimeBindingNone
			value.Readiness = readiness
		},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := base
			adapter.Args = append([]string(nil), base.Args...)
			adapter.VersionArgs = append([]string(nil), base.VersionArgs...)
			adapter.Environment = cloneMap(base.Environment)
			mutate(&adapter)
			if _, err := New(Config{Adapters: []Adapter{adapter}, Outputs: &memoryOutputFactory{capture: newMemoryCapture()}}); err == nil {
				t.Fatal("manually constructed runtime-bound adapter bypassed exact ADB validation")
			}
		})
	}
}

func androidLogcatCollectorPlan(t *testing.T, configuration AdapterConfiguration, digest domain.Digest) ports.CollectorPlan {
	t.Helper()
	plan := validCollectorPlan(t)
	plan.TargetGeneration = 2
	plan.Attachment = ports.ObservationAttachment{
		TargetKind: domain.TargetAndroidVirtualDevice,
		RuntimeID:  "world-android-generation-2",
		ADBDevice: ports.ADBDeviceSelection{
			Server: ports.ADBServerEndpoint{Host: "127.0.0.1", Port: 5041},
			Serial: "emulator-5578",
		},
	}
	plan.Requirement = ports.ObservationRequirement{
		SignalFamily: configuration.SignalFamily, Placement: configuration.Placement,
		MinimumLevel: configuration.CoverageLevel, Required: true,
	}
	plan.Adapter = configuration.Adapter
	plan.Version = configuration.Version
	plan.ConfigurationDigest = digest
	return plan
}
