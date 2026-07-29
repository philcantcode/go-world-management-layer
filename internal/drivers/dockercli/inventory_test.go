package dockercli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestInventoryReturnsOnlyCompleteInspectedSnapshot(t *testing.T) {
	containerA := canonicalContainerID(1)
	containerB := canonicalContainerID(2)
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		switch invocation.Args[0] {
		case "ps":
			return command.Result{Stdout: []byte(containerB + "\n" + containerA + "\n")}, nil
		case "inspect":
			return command.Result{Stdout: dockerInspectJSON(invocation.Args[1:]...)}, nil
		default:
			return command.Result{}, errors.New("unexpected invocation")
		}
	})
	containers, err := Inventory(inventoryDeadline(t), "docker-test", runner)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != 2 || containers[0].ID != containerA || containers[1].ID != containerB {
		t.Fatalf("inventory = %#v", containers)
	}
	configuration := containers[0].Configuration
	if containers[0].Name != containerA || configuration.Image != "repo@sha256:image" || !configuration.InitKnown || !configuration.Init || configuration.MemorySwapBytes != 1536 || len(configuration.Mounts) != 1 {
		t.Fatalf("decoded container = %#v", containers[0])
	}
}

func TestInventoryUsesExactRuntimeCgroupResolverAndIgnoresConfiguredParent(t *testing.T) {
	containerID := strings.Repeat("a", 64)
	const pid = 4242
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		switch invocation.Args[0] {
		case "ps":
			return command.Result{Stdout: []byte(containerID + "\n")}, nil
		case "inspect":
			return command.Result{Stdout: dockerInspectStateJSON(containerID, true, pid, "configured-parent-is-not-runtime-identity")}, nil
		default:
			return command.Result{}, errors.New("unexpected invocation")
		}
	})
	wantCgroup := "/system.slice/docker-" + containerID + ".scope"
	resolverCalls := 0
	containers, err := inventoryWithCgroupIDResolver(inventoryDeadline(t), "docker-test", runner, func(gotPID int, gotID string) (string, error) {
		resolverCalls++
		if gotPID != pid || gotID != containerID {
			t.Fatalf("cgroup resolver identity = pid %d, ID %q", gotPID, gotID)
		}
		return wantCgroup, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolverCalls != 1 || len(containers) != 1 || containers[0].CgroupID != wantCgroup {
		t.Fatalf("inventory = %#v, resolver calls = %d", containers, resolverCalls)
	}
}

func TestInventoryCgroupResolutionFailureInvalidatesCompleteSnapshot(t *testing.T) {
	containerIDs := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if invocation.Args[0] == "ps" {
			return command.Result{Stdout: []byte(strings.Join(containerIDs, "\n") + "\n")}, nil
		}
		return command.Result{Stdout: dockerInspectStateJSON(containerIDs[0], true, 101, "parent-a", containerIDs[1], true, 102, "parent-b")}, nil
	})
	containers, err := inventoryWithCgroupIDResolver(inventoryDeadline(t), "docker", runner, func(pid int, containerID string) (string, error) {
		if pid == 102 {
			return "", errors.New("ambiguous cgroup membership")
		}
		return "/docker/" + containerID, nil
	})
	if err == nil || containers != nil || !strings.Contains(err.Error(), "ambiguous cgroup membership") {
		t.Fatalf("partial inventory = %#v, %v", containers, err)
	}
}

func TestStoppedContainerHasNoCgroupIdentityAndDoesNotInvokeResolver(t *testing.T) {
	containerID := strings.Repeat("c", 64)
	runner := runnerFunc(func(_ context.Context, _ command.Invocation) (command.Result, error) {
		return command.Result{Stdout: dockerInspectStateJSON(containerID, false, 0, "configured-parent")}, nil
	})
	containers, err := inspectContainersWithCgroupIDResolver(inventoryDeadline(t), "docker", runner, []string{containerID}, func(int, string) (string, error) {
		return "", errors.New("resolver must not run for stopped PID zero")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != 1 || containers[0].Running || containers[0].CgroupID != "" {
		t.Fatalf("stopped container = %#v", containers)
	}
}

func TestInventoryBatchesContainerInspection(t *testing.T) {
	ids := make([]string, maximumInspectBatch+1)
	for index := range ids {
		ids[index] = canonicalContainerID(index + 1)
	}
	var batchSizes []int
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if invocation.Args[0] == "ps" {
			return command.Result{Stdout: []byte(strings.Join(ids, "\n") + "\n")}, nil
		}
		batchSizes = append(batchSizes, len(invocation.Args)-1)
		return command.Result{Stdout: dockerInspectJSON(invocation.Args[1:]...)}, nil
	})
	containers, err := Inventory(inventoryDeadline(t), "docker", runner)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != len(ids) || fmt.Sprint(batchSizes) != fmt.Sprint([]int{maximumInspectBatch, 1}) {
		t.Fatalf("containers=%d batches=%v", len(containers), batchSizes)
	}
}

func TestInventoryRejectsAmbiguousOrUnboundedSnapshots(t *testing.T) {
	t.Run("duplicate IDs", func(t *testing.T) {
		duplicate := canonicalContainerID(1)
		runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte(duplicate + "\n" + duplicate + "\n")}, nil
		})
		if _, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil {
			t.Fatal("duplicate inventory was accepted")
		}
	})
	t.Run("safety bound", func(t *testing.T) {
		listing := strings.Repeat("container\n", MaximumInventoryContainers+1)
		runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte(listing)}, nil
		})
		if _, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil {
			t.Fatal("unbounded inventory was accepted")
		}
	})
	t.Run("inspect failure", func(t *testing.T) {
		requested := canonicalContainerID(1)
		runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
			if invocation.Args[0] == "ps" {
				return command.Result{Stdout: []byte(requested + "\n")}, nil
			}
			return command.Result{}, errors.New("container disappeared")
		})
		if containers, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil || containers != nil {
			t.Fatalf("partial inventory = %#v, %v", containers, err)
		}
	})
	t.Run("inspect substitution", func(t *testing.T) {
		requested := canonicalContainerID(1)
		different := canonicalContainerID(2)
		runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
			if invocation.Args[0] == "ps" {
				return command.Result{Stdout: []byte(requested + "\n")}, nil
			}
			return command.Result{Stdout: dockerInspectJSON(different)}, nil
		})
		if containers, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil || containers != nil {
			t.Fatalf("substituted inventory = %#v, %v", containers, err)
		}
	})
}

func TestInspectRejectsSubstitutedResource(t *testing.T) {
	requested := canonicalContainerID(1)
	different := canonicalContainerID(2)
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if fmt.Sprint(invocation.Args) != fmt.Sprint([]string{"inspect", requested}) {
			t.Fatalf("inspect arguments = %v", invocation.Args)
		}
		return command.Result{Stdout: dockerInspectJSON(different)}, nil
	})
	if container, err := Inspect(inventoryDeadline(t), "docker", runner, requested); err == nil || container.ID != "" {
		t.Fatalf("substituted inspect = %#v, %v", container, err)
	}
}

func TestInventoryAndInspectRejectNoncanonicalContainerIDs(t *testing.T) {
	valid := strings.Repeat("a", 64)
	for _, invalid := range []string{"short", strings.ToUpper(valid), " " + valid, valid + " "} {
		t.Run(fmt.Sprintf("%q", invalid), func(t *testing.T) {
			inventoryRunner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
				return command.Result{Stdout: []byte(invalid + "\n")}, nil
			})
			if containers, err := Inventory(inventoryDeadline(t), "docker", inventoryRunner); err == nil || containers != nil {
				t.Fatalf("Inventory accepted %q: %#v, %v", invalid, containers, err)
			}

			inspectRunner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
				t.Fatal("noncanonical inspect ID reached the command runner")
				return command.Result{}, nil
			})
			if container, err := Inspect(inventoryDeadline(t), "docker", inspectRunner, invalid); err == nil || container.ID != "" {
				t.Fatalf("Inspect accepted %q: %#v, %v", invalid, container, err)
			}
		})
	}
}

func TestInspectRejectsRunningContainerWithoutPositivePID(t *testing.T) {
	containerID := canonicalContainerID(1)
	runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
		return command.Result{Stdout: dockerInspectStateJSON(containerID, true, 0, "")}, nil
	})
	containers, err := inspectContainersWithCgroupIDResolver(inventoryDeadline(t), "docker", runner, []string{containerID}, func(int, string) (string, error) {
		t.Fatal("invalid running state reached the cgroup resolver")
		return "", nil
	})
	if err == nil || containers != nil || !strings.Contains(err.Error(), "positive PID") {
		t.Fatalf("running PID-zero inspect = %#v, %v", containers, err)
	}
}

func TestInspectDecodesSecurityAndExecutionConfiguration(t *testing.T) {
	containerID := canonicalContainerID(7)
	document := fmt.Sprintf(`[{
		"Id":%q,"Name":"/decoded","State":{"Running":false,"Paused":true,"Restarting":true,"Dead":true,"Status":"paused","Pid":0},
		"Config":{
			"Image":"repo@sha256:decoded","Labels":{"world.role":"agent"},"Hostname":"host","Domainname":"domain",
			"Entrypoint":["/entry"],"Cmd":["arg"],"Env":["KEY=value"],"WorkingDir":"/work","User":"7:8",
			"AttachStdin":true,"AttachStdout":true,"AttachStderr":true,"OpenStdin":true,"StdinOnce":true,"Tty":true,
			"NetworkDisabled":true,"MacAddress":"02:42:ac:11:00:02","ExposedPorts":{"80/tcp":{}},"Volumes":{"/data":{}},
			"Healthcheck":{"Test":["CMD","true"],"Interval":11,"Timeout":12,"StartPeriod":13,"StartInterval":14,"Retries":15},
			"StopSignal":"SIGTERM","StopTimeout":16
		},
		"HostConfig":{
			"Runtime":"alternate","ReadonlyRootfs":true,"Privileged":true,"RestartPolicy":{"Name":"always","MaximumRetryCount":2},"AutoRemove":true,
			"NetworkMode":"bridge","PidMode":"host","IpcMode":"host","CgroupnsMode":"host","UsernsMode":"host","UTSMode":"host",
			"CapAdd":["NET_ADMIN"],"CapDrop":["MKNOD"],"GroupAdd":["9"],"SecurityOpt":["label=disable"],"Tmpfs":{"/tmp":"rw"},
			"Memory":101,"MemoryReservation":102,"MemorySwap":103,"MemorySwappiness":104,"KernelMemory":105,"KernelMemoryTCP":106,
			"CpuShares":107,"CpuPeriod":108,"CpuQuota":109,"CpuRealtimePeriod":110,"CpuRealtimeRuntime":111,"CpusetCpus":"0","CpusetMems":"1",
			"NanoCpus":112,"PidsLimit":113,"Init":true,
			"Mounts":[
				{"Type":"bind","Source":"/configured-bind","Target":"/bind","ReadOnly":true,"Consistency":"consistent","BindOptions":{"Propagation":"rshared","NonRecursive":true,"CreateMountpoint":true,"ReadOnlyNonRecursive":true,"ReadOnlyForceRecursive":true}},
				{"Type":"volume","Source":"configured-volume","Target":"/volume","VolumeOptions":{"NoCopy":true,"Labels":{"purpose":"test"},"Subpath":"child","DriverConfig":{"Name":"local","Options":{"type":"tmpfs"}}}},
				{"Type":"tmpfs","Target":"/memory","TmpfsOptions":{"SizeBytes":4096,"Mode":493,"Options":[["nodev"],["nosuid","strictatime"]]}},
				{"Type":"image","Source":"repo@sha256:mount","Target":"/image","ImageOptions":{"Subpath":"content"}},
				{"Type":"cluster","Source":"cluster","Target":"/cluster","ClusterOptions":{}}
			],
			"Binds":["/a:/b"],"VolumesFrom":["parent"],"ContainerIDFile":"/id",
			"Devices":[{"PathOnHost":"/dev/a","PathInContainer":"/dev/b","CgroupPermissions":"r"}],"DeviceCgroupRules":["c 1:3 r"],
			"DeviceRequests":[{"Driver":"gpu","Count":1,"DeviceIDs":["device"],"Capabilities":[["compute"]],"Options":{"key":"value"}}],
			"PublishAllPorts":true,"PortBindings":{"80/tcp":[{"HostIp":"127.0.0.1","HostPort":"8080"}]},
			"ExtraHosts":["host:127.0.0.1"],"Dns":["1.1.1.1"],"DnsOptions":["timeout:1"],"DnsSearch":["example"],"Links":["peer"],
			"OomKillDisable":true,"OomScoreAdj":114,"Cgroup":"named-group","CgroupParent":"parent","BlkioWeight":115,
			"BlkioWeightDevice":[{"Path":"/dev/a","Weight":116}],"BlkioDeviceReadBps":[{"Path":"/dev/a","Rate":117}],
			"BlkioDeviceWriteBps":[{"Path":"/dev/a","Rate":118}],"BlkioDeviceReadIOps":[{"Path":"/dev/a","Rate":119}],
			"BlkioDeviceWriteIOps":[{"Path":"/dev/a","Rate":120}],"CpuCount":121,"CpuPercent":122,
			"IOMaximumBandwidth":123,"IOMaximumIOps":124,"Ulimits":[{"Name":"nofile","Soft":125,"Hard":126}],
			"Sysctls":{"kernel.domainname":"value"},"MaskedPaths":["/masked"],"ReadonlyPaths":["/readonly"],"ShmSize":127,
			"LogConfig":{"Type":"json-file","Config":{"max-size":"1m"}},"VolumeDriver":"driver","StorageOpt":{"size":"1G"},
			"Isolation":"process","Annotations":{"annotation":"value"}
		},
		"NetworkSettings":{"Networks":{"isolated":{}}},
		"Mounts":[{"Type":"volume","Name":"volume-name","Source":"source","Destination":"/destination","RW":false,"Mode":"z","Propagation":"rshared","Driver":"local"}]
	}]`, containerID)
	runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
		return command.Result{Stdout: []byte(document)}, nil
	})
	containers, err := inspectContainersWithCgroupIDResolver(inventoryDeadline(t), "docker", runner, []string{containerID}, func(int, string) (string, error) {
		t.Fatal("stopped inspect unexpectedly invoked the cgroup resolver")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := Configuration{
		Image: "repo@sha256:decoded", Runtime: "alternate", Hostname: "host", Domainname: "domain",
		Entrypoint: []string{"/entry"}, Command: []string{"arg"}, Environment: []string{"KEY=value"}, WorkingDir: "/work", User: "7:8",
		AttachStdin: true, AttachStdout: true, AttachStderr: true, OpenStdin: true, StdinOnce: true, TTY: true, NetworkDisabled: true,
		MacAddress: "02:42:ac:11:00:02", ExposedPorts: []string{"80/tcp"}, DeclaredVolumes: []string{"/data"},
		Healthcheck:      Healthcheck{Test: []string{"CMD", "true"}, Interval: 11, Timeout: 12, StartPeriod: 13, StartInterval: 14, Retries: 15},
		HealthcheckKnown: true, StopSignal: "SIGTERM", StopTimeout: 16, StopTimeoutKnown: true,
		ReadOnlyRoot: true, Privileged: true, RestartPolicy: RestartPolicy{Name: "always", MaximumRetryCount: 2}, AutoRemove: true,
		NetworkMode: "bridge", PIDMode: "host", IPCMode: "host", CgroupMode: "host", UsernsMode: "host", UTSMode: "host",
		Init: true, InitKnown: true, CapabilitiesAdd: []string{"NET_ADMIN"}, CapabilitiesDrop: []string{"MKNOD"}, GroupAdd: []string{"9"},
		SecurityOptions: []string{"label=disable"}, Tmpfs: map[string]string{"/tmp": "rw"},
		Devices:           []Device{{HostPath: "/dev/a", ContainerPath: "/dev/b", Permissions: "r"}},
		DeviceRequests:    []DeviceRequest{{Driver: "gpu", Count: 1, DeviceIDs: []string{"device"}, Capabilities: [][]string{{"compute"}}, Options: map[string]string{"key": "value"}}},
		DeviceCgroupRules: []string{"c 1:3 r"},
		Mounts:            []Mount{{Type: "volume", Name: "volume-name", Source: "source", Destination: "/destination", ReadOnly: true, Mode: "z", Propagation: "rshared", Driver: "local"}},
		ConfiguredMounts: []ConfiguredMount{
			{
				Type: "bind", Source: "/configured-bind", Target: "/bind", ReadOnly: true, Consistency: "consistent",
				BindOptionsKnown: true, BindOptions: BindOptions{
					Propagation: "rshared", NonRecursive: true, CreateMountpoint: true, ReadOnlyNonRecursive: true, ReadOnlyForceRecursive: true,
				},
			},
			{
				Type: "volume", Source: "configured-volume", Target: "/volume", VolumeOptionsKnown: true,
				VolumeOptions: VolumeOptions{
					NoCopy: true, Labels: map[string]string{"purpose": "test"}, Subpath: "child", DriverKnown: true,
					Driver: MountDriver{Name: "local", Options: map[string]string{"type": "tmpfs"}},
				},
			},
			{Type: "tmpfs", Target: "/memory", TmpfsOptionsKnown: true, TmpfsOptions: TmpfsOptions{SizeBytes: 4096, Mode: 493, Options: [][]string{{"nodev"}, {"nosuid", "strictatime"}}}},
			{Type: "image", Source: "repo@sha256:mount", Target: "/image", ImageOptionsKnown: true, ImageOptions: ImageOptions{Subpath: "content"}},
			{Type: "cluster", Source: "cluster", Target: "/cluster", ClusterOptionsKnown: true},
		},
		Binds: []string{"/a:/b"}, VolumesFrom: []string{"parent"}, ContainerIDFile: "/id", PublishAllPorts: true,
		PortBindings: map[string][]PortBinding{"80/tcp": {{HostIP: "127.0.0.1", HostPort: "8080"}}}, NetworkAttachments: []string{"isolated"},
		ExtraHosts: []string{"host:127.0.0.1"}, DNS: []string{"1.1.1.1"}, DNSOptions: []string{"timeout:1"}, DNSSearch: []string{"example"}, Links: []string{"peer"},
		OomKillDisable: true, OomScoreAdj: 114, Cgroup: "named-group", CgroupParent: "parent", MemoryReservation: 102, KernelMemory: 105, KernelMemoryTCP: 106,
		MemorySwappiness: 104, MemorySwappinessKnown: true, CPUShares: 107, CPUPeriod: 108, CPUQuota: 109, CPURealtimePeriod: 110,
		CPURealtimeRuntime: 111, CpusetCPUs: "0", CpusetMems: "1", BlkioWeight: 115,
		BlkioWeightDevice: []WeightDevice{{Path: "/dev/a", Weight: 116}}, BlkioDeviceReadBps: []ThrottleDevice{{Path: "/dev/a", Rate: 117}},
		BlkioDeviceWriteBps: []ThrottleDevice{{Path: "/dev/a", Rate: 118}}, BlkioDeviceReadIOps: []ThrottleDevice{{Path: "/dev/a", Rate: 119}},
		BlkioDeviceWriteIOps: []ThrottleDevice{{Path: "/dev/a", Rate: 120}}, CPUCount: 121, CPUPercent: 122,
		IOMaximumBandwidth: 123, IOMaximumIOps: 124, Ulimits: []Ulimit{{Name: "nofile", Soft: 125, Hard: 126}},
		Sysctls: map[string]string{"kernel.domainname": "value"}, MaskedPaths: []string{"/masked"}, ReadonlyPaths: []string{"/readonly"}, ShmSize: 127,
		LogConfig: LogConfiguration{Type: "json-file", Config: map[string]string{"max-size": "1m"}}, VolumeDriver: "driver",
		StorageOptions: map[string]string{"size": "1G"}, Isolation: "process", Annotations: map[string]string{"annotation": "value"},
		MemoryBytes: 101, MemorySwapBytes: 103, NanoCPUs: 112, PIDs: 113,
	}
	if len(containers) != 1 {
		t.Fatalf("decoded containers = %#v", containers)
	}
	container := containers[0]
	if err := ConfigurationDifference(container.Configuration, expected); err != nil {
		t.Fatalf("decoded configuration: %v\nactual = %#v", err, container.Configuration)
	}
	for _, check := range []struct {
		name string
		got  bool
		want bool
	}{
		{name: "paused", got: container.Paused, want: true},
		{name: "restarting", got: container.Restarting, want: true},
		{name: "dead", got: container.Dead, want: true},
	} {
		t.Run(check.name, func(t *testing.T) {
			if check.got != check.want {
				t.Fatalf("decoded %s = %t; want %t", check.name, check.got, check.want)
			}
		})
	}
}

func dockerInspectJSON(ids ...string) []byte {
	values := make([]any, 0, len(ids)*4)
	for _, id := range ids {
		values = append(values, id, false, 0, "configured-parent")
	}
	return dockerInspectStateJSON(values...)
}

// dockerInspectStateJSON accepts repeating id, running, pid, cgroup-parent
// groups so tests can exercise Docker State.Pid without duplicating inspect
// fixtures. CgroupParent remains deliberately present to prove it is ignored.
func dockerInspectStateJSON(values ...any) []byte {
	if len(values)%4 != 0 {
		panic("dockerInspectStateJSON requires id/running/pid/cgroup-parent groups")
	}
	documents := make([]string, 0, len(values)/4)
	for index := 0; index < len(values); index += 4 {
		id, ok := values[index].(string)
		if !ok {
			panic("dockerInspectStateJSON ID must be a string")
		}
		running, ok := values[index+1].(bool)
		if !ok {
			panic("dockerInspectStateJSON running must be a bool")
		}
		pid, ok := values[index+2].(int)
		if !ok {
			panic("dockerInspectStateJSON PID must be an int")
		}
		cgroupParent, ok := values[index+3].(string)
		if !ok {
			panic("dockerInspectStateJSON cgroup parent must be a string")
		}
		status := "exited"
		if running {
			status = "running"
		}
		documents = append(documents, fmt.Sprintf(`{
			"Id":%q,"Name":%q,"State":{"Running":%t,"Status":%q,"Pid":%d},
			"Config":{"Image":"repo@sha256:image","Labels":{"world.role":"test"},"Entrypoint":["/guest"],"OpenStdin":true},
			"HostConfig":{"CgroupParent":%q,"ReadonlyRootfs":true,"NetworkMode":"none","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true","seccomp=builtin"],"Memory":1024,"MemorySwap":1536,"Init":true},
			"Mounts":[{"Type":"bind","Source":"/source","Destination":"/target","RW":true}]
		}`, id, "/"+id, running, status, pid, cgroupParent))
	}
	return []byte("[" + strings.Join(documents, ",") + "]")
}

func inventoryDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

type runnerFunc func(context.Context, command.Invocation) (command.Result, error)

func (f runnerFunc) Run(ctx context.Context, invocation command.Invocation) (command.Result, error) {
	return f(ctx, invocation)
}

var _ command.Runner = runnerFunc(nil)

func canonicalContainerID(index int) string {
	return fmt.Sprintf("%064x", index)
}
