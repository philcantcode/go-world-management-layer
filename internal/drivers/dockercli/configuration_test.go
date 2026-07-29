package dockercli

import (
	"math"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestExactWorldLabelsReservesOnlyWorldNamespace(t *testing.T) {
	expected := map[string]string{"world.role": "agent-workspace"}
	tests := []struct {
		name   string
		actual map[string]string
		want   bool
	}{
		{name: "exact", actual: map[string]string{"world.role": "agent-workspace"}, want: true},
		{name: "unrelated image label allowed", actual: map[string]string{"world.role": "agent-workspace", "dev.philcantcode.world-e2e.run": "run-token"}, want: true},
		{name: "unexpected reserved label rejected", actual: map[string]string{"world.role": "agent-workspace", "world.e2e.run": "run-token"}},
		{name: "expected value mismatch rejected", actual: map[string]string{"world.role": "linux-target"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ExactWorldLabels(test.actual, expected); got != test.want {
				t.Fatalf("ExactWorldLabels() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestMemorySwapTotalUsesDockerCombinedLimit(t *testing.T) {
	tests := []struct {
		name      string
		memory    int64
		swap      int64
		want      int64
		wantError bool
	}{
		{name: "swap disabled", memory: 1024, want: 1024},
		{name: "bounded swap", memory: 1024, swap: 512, want: 1536},
		{name: "memory required", swap: 1, wantError: true},
		{name: "negative swap", memory: 1, swap: -1, wantError: true},
		{name: "overflow", memory: math.MaxInt64, swap: 1, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := MemorySwapTotal(test.memory, test.swap)
			if test.wantError {
				if err == nil {
					t.Fatalf("MemorySwapTotal(%d, %d) = %d, nil", test.memory, test.swap, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("MemorySwapTotal(%d, %d) = %d, %v; want %d", test.memory, test.swap, got, err, test.want)
			}
		})
	}
}

func TestConfigurationDifferenceNamesFieldsWithoutLeakingValues(t *testing.T) {
	actual := Configuration{Image: "sensitive.actual", MemoryBytes: 1024}
	expected := Configuration{Image: "sensitive.expected", MemoryBytes: 2048}
	err := ConfigurationDifference(actual, expected)
	if err == nil {
		t.Fatal("different configurations were accepted")
	}
	message := err.Error()
	if !strings.Contains(message, "Image") || !strings.Contains(message, "MemoryBytes") {
		t.Fatalf("difference did not identify fields: %v", err)
	}
	if strings.Contains(message, "sensitive") || strings.Contains(message, "1024") || strings.Contains(message, "2048") {
		t.Fatalf("difference leaked configuration values: %v", err)
	}
}

func TestConfigurationDifferenceDoesNotTreatDaemonUserNamespaceAsPrivate(t *testing.T) {
	actual := Configuration{UsernsMode: ""}
	expected := Configuration{UsernsMode: "private"}
	if err := ConfigurationDifference(actual, expected); err == nil || !strings.Contains(err.Error(), "UsernsMode") {
		t.Fatalf("daemon-default and private user namespaces were treated as equivalent: %v", err)
	}
}

func TestRequireExactStoppedStateRejectsContradictoryAndUnknownStates(t *testing.T) {
	tests := []struct {
		name                              string
		running, paused, restarting, dead bool
		status                            string
		wantError                         bool
	}{
		{name: "created", status: StoppedStatusCreated},
		{name: "exited", status: StoppedStatusExited},
		{name: "running flag", running: true, status: StoppedStatusExited, wantError: true},
		{name: "paused", paused: true, status: "paused", wantError: true},
		{name: "restarting", restarting: true, status: "restarting", wantError: true},
		{name: "dead", dead: true, status: "dead", wantError: true},
		{name: "running status", status: "running", wantError: true},
		{name: "unknown status", status: "", wantError: true},
		{name: "removing status", status: "removing", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RequireExactStoppedState(test.running, test.paused, test.restarting, test.dead, test.status, StoppedStatusCreated, StoppedStatusExited)
			if (err != nil) != test.wantError {
				t.Fatalf("RequireExactStoppedState() error = %v, wantError %t", err, test.wantError)
			}
		})
	}
	if err := RequireExactStoppedState(false, false, false, false, StoppedStatusCreated, StoppedStatusExited); err == nil {
		t.Fatal("created state was accepted for an exited-only boundary")
	}
}

func TestRestrictedNamespaceFactsReportObservedUserNamespaceMode(t *testing.T) {
	for _, test := range []struct {
		name     string
		options  []string
		wantUser string
	}{
		{name: "empty means host", wantUser: "host"},
		{name: "unrelated security", options: []string{"name=seccomp,profile=builtin"}, wantUser: "host"},
		{name: "userns remap", options: []string{"name=userns"}, wantUser: "remapped"},
		{name: "rootless", options: []string{"name=rootless"}, wantUser: "remapped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := RestrictedNamespaceFacts(test.options)
			if facts["user_namespace"] != test.wantUser {
				t.Fatalf("user namespace = %q; want %q", facts["user_namespace"], test.wantUser)
			}
			for name, want := range map[string]string{
				"pid_namespace": "private", "ipc_namespace": "private", "cgroup_namespace": "private",
				"uts_namespace": "private", "network_namespace": "none",
			} {
				if facts[name] != want {
					t.Fatalf("%s = %q; want %q", name, facts[name], want)
				}
			}
		})
	}
}

func TestRestrictedBindMountArgumentRejectsCSVReinterpretation(t *testing.T) {
	value, err := RestrictedBindMountArgument(filepath.Join("safe", "source"), "/target", true)
	if err != nil || !strings.HasSuffix(value, ",readonly") {
		t.Fatalf("safe mount argument = %q, %v", value, err)
	}
	for _, test := range []struct{ source, target string }{
		{source: "source,readonly", target: "/target"},
		{source: "source", target: "/target,bind-nonrecursive"},
		{source: "source\"quoted", target: "/target"},
		{source: "source\noption", target: "/target"},
	} {
		if value, err := RestrictedBindMountArgument(test.source, test.target, false); err == nil || value != "" {
			t.Fatalf("unsafe mount %q -> %q = %q, %v", test.source, test.target, value, err)
		}
	}
}

func TestCanonicalHostBindSourceComparisonIsOSAware(t *testing.T) {
	if !canonicalHostBindSourceEqual("windows", `C:\\World\\Target`, `c:\\world\\target`) {
		t.Fatal("Windows-equivalent bind sources did not compare equal")
	}
	if canonicalHostBindSourceEqual("linux", "/World/Target", "/world/target") {
		t.Fatal("case-distinct Linux bind sources compared equal")
	}
	left := Configuration{}
	right := Configuration{}
	AddRestrictedBindMount(&left, filepath.Join("root", "directory", "..", "target"), "/target", false)
	AddRestrictedBindMount(&right, filepath.Join("root", "target"), "/target", false)
	if err := ConfigurationDifference(left, right); err != nil {
		t.Fatalf("clean-equivalent production bind comparison failed: %v", err)
	}
	caseLeft := Configuration{}
	caseRight := Configuration{}
	AddRestrictedBindMount(&caseLeft, filepath.Join("root", "Target"), "/target", false)
	AddRestrictedBindMount(&caseRight, filepath.Join("root", "target"), "/target", false)
	err := ConfigurationDifference(caseLeft, caseRight)
	if runtime.GOOS == "windows" && err != nil {
		t.Fatalf("Windows case-equivalent production bind comparison failed: %v", err)
	}
	if runtime.GOOS != "windows" && err == nil {
		t.Fatal("case-distinct Unix production bind sources compared equal")
	}
}

func TestConfigurationDifferenceCanonicalizesOnlyBindMountPaths(t *testing.T) {
	for _, test := range []struct {
		name     string
		actual   ConfiguredMount
		expected ConfiguredMount
	}{
		{
			name:     "volume source remains an exact name",
			actual:   ConfiguredMount{Type: "volume", Source: "prefix/../volume", Target: "/target"},
			expected: ConfiguredMount{Type: "volume", Source: "volume", Target: "/target"},
		},
		{
			name:     "empty target remains distinct from dot",
			actual:   ConfiguredMount{Type: "tmpfs"},
			expected: ConfiguredMount{Type: "tmpfs", Target: "."},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := Configuration{ConfiguredMounts: []ConfiguredMount{test.actual}}
			expected := Configuration{ConfiguredMounts: []ConfiguredMount{test.expected}}
			if err := ConfigurationDifference(actual, expected); err == nil || !strings.Contains(err.Error(), "ConfiguredMounts") {
				t.Fatalf("distinct configured mount paths were accepted: %v", err)
			}
		})
	}
}

func TestConfigurationDifferenceCoversConfiguredMountAdvancedOptions(t *testing.T) {
	mutations := map[string]func(*ConfiguredMount){
		"type":        func(mount *ConfiguredMount) { mount.Type = "volume" },
		"source":      func(mount *ConfiguredMount) { mount.Source = "/different" },
		"target":      func(mount *ConfiguredMount) { mount.Target = "/different" },
		"read only":   func(mount *ConfiguredMount) { mount.ReadOnly = true },
		"consistency": func(mount *ConfiguredMount) { mount.Consistency = "delegated" },
		"bind options": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
		},
		"bind propagation": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
			mount.BindOptions.Propagation = "rshared"
		},
		"bind nonrecursive": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
			mount.BindOptions.NonRecursive = true
		},
		"bind create mountpoint": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
			mount.BindOptions.CreateMountpoint = true
		},
		"bind readonly nonrecursive": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
			mount.BindOptions.ReadOnlyNonRecursive = true
		},
		"bind readonly force": func(mount *ConfiguredMount) {
			mount.BindOptionsKnown = true
			mount.BindOptions.ReadOnlyForceRecursive = true
		},
		"volume options": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
		},
		"volume nocopy": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.NoCopy = true
		},
		"volume labels": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.Labels = map[string]string{"unexpected": "label"}
		},
		"volume subpath": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.Subpath = "unexpected"
		},
		"volume driver presence": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.DriverKnown = true
		},
		"volume driver name": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.DriverKnown = true
			mount.VolumeOptions.Driver.Name = "unexpected"
		},
		"volume driver options": func(mount *ConfiguredMount) {
			mount.VolumeOptionsKnown = true
			mount.VolumeOptions.DriverKnown = true
			mount.VolumeOptions.Driver = MountDriver{Name: "unexpected", Options: map[string]string{"key": "value"}}
		},
		"tmpfs options presence": func(mount *ConfiguredMount) {
			mount.TmpfsOptionsKnown = true
		},
		"tmpfs size": func(mount *ConfiguredMount) {
			mount.TmpfsOptionsKnown = true
			mount.TmpfsOptions.SizeBytes = 1
		},
		"tmpfs mode": func(mount *ConfiguredMount) {
			mount.TmpfsOptionsKnown = true
			mount.TmpfsOptions.Mode = 1
		},
		"tmpfs option list": func(mount *ConfiguredMount) {
			mount.TmpfsOptionsKnown = true
			mount.TmpfsOptions.Options = [][]string{{"unexpected"}}
		},
		"image options presence": func(mount *ConfiguredMount) {
			mount.ImageOptionsKnown = true
		},
		"image subpath": func(mount *ConfiguredMount) {
			mount.ImageOptionsKnown = true
			mount.ImageOptions.Subpath = "unexpected"
		},
		"cluster options": func(mount *ConfiguredMount) { mount.ClusterOptionsKnown = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			expected := Configuration{}
			AddRestrictedBindMount(&expected, "/source", "/target", false)
			actual := expected
			actual.Mounts = append([]Mount(nil), expected.Mounts...)
			actual.ConfiguredMounts = append([]ConfiguredMount(nil), expected.ConfiguredMounts...)
			mutate(&actual.ConfiguredMounts[0])
			if err := ConfigurationDifference(actual, expected); err == nil || !strings.Contains(err.Error(), "ConfiguredMounts") {
				t.Fatalf("advanced configured mount mutation was accepted: %v", err)
			}
		})
	}
}

func TestRestrictedConfiguredBindMountMatchesPlainDockerMountRequest(t *testing.T) {
	mount := RestrictedConfiguredBindMount("/source", "/target", true)
	if !reflect.DeepEqual(mount, ConfiguredMount{Type: "bind", Source: "/source", Target: "/target", ReadOnly: true}) {
		t.Fatalf("restricted configured bind mount = %#v", mount)
	}
}

func TestRestrictedLifecycleArgumentsPinImageExecutionDefaults(t *testing.T) {
	arguments := RestrictedLifecycleArguments("world-test")
	for _, expected := range [][]string{
		{"--env", RestrictedPathEnvironment},
		{"--workdir", RestrictedWorkingDirectory},
	} {
		found := false
		for index := 0; index+1 < len(arguments); index++ {
			if arguments[index] == expected[0] && arguments[index+1] == expected[1] {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("RestrictedLifecycleArguments() = %v; missing %v", arguments, expected)
		}
	}
	configuration := RestrictedContainerConfiguration()
	if !slices.Equal(configuration.Environment, []string{RestrictedPathEnvironment}) || configuration.WorkingDir != RestrictedWorkingDirectory {
		t.Fatalf("restricted execution configuration = env %v, workdir %q", configuration.Environment, configuration.WorkingDir)
	}
}

func TestConfigurationDifferenceCoversEveryConfigurationField(t *testing.T) {
	configurationType := reflect.TypeOf(Configuration{})
	for index := 0; index < configurationType.NumField(); index++ {
		field := configurationType.Field(index)
		t.Run(field.Name, func(t *testing.T) {
			actual := reflect.New(configurationType).Elem()
			setDifferentConfigurationValue(t, actual.Field(index))
			err := ConfigurationDifference(actual.Interface().(Configuration), Configuration{})
			if err == nil || !strings.Contains(err.Error(), field.Name) {
				t.Fatalf("ConfigurationDifference did not report mutated field %s: %v", field.Name, err)
			}
		})
	}
}

func setDifferentConfigurationValue(t *testing.T, value reflect.Value) {
	t.Helper()
	switch value.Kind() {
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(1)
	case reflect.String:
		value.SetString("different")
	case reflect.Slice:
		result := reflect.MakeSlice(value.Type(), 1, 1)
		setDifferentConfigurationValue(t, result.Index(0))
		value.Set(result)
	case reflect.Map:
		result := reflect.MakeMap(value.Type())
		key := reflect.New(value.Type().Key()).Elem()
		setDifferentConfigurationValue(t, key)
		entry := reflect.New(value.Type().Elem()).Elem()
		setDifferentConfigurationValue(t, entry)
		result.SetMapIndex(key, entry)
		value.Set(result)
	case reflect.Struct:
		if value.NumField() == 0 {
			t.Fatalf("cannot mutate empty struct %s", value.Type())
		}
		setDifferentConfigurationValue(t, value.Field(0))
	default:
		t.Fatalf("unsupported Configuration field kind %s for %s", value.Kind(), value.Type())
	}
}

func TestAssessPhysicalSupportDistinguishesUnsupportedAndUnknown(t *testing.T) {
	capabilities := PhysicalCapabilities{
		OSType: "linux", Runtimes: []string{RuncRuntime}, SecurityOptions: []string{"name=seccomp,profile=builtin"},
		CPUCFSQuota: true, MemoryLimit: true, SwapLimit: true, PIDsLimit: true,
	}
	support := AssessPhysicalSupport(capabilities, RuncRuntime)
	if support.Container != ports.PhysicalSupportEnforced || support.Seccomp != ports.PhysicalSupportEnforced || support.Swap != ports.PhysicalSupportEnforced {
		t.Fatalf("supported assessment = %#v", support)
	}
	capabilities.Runtimes = nil
	if got := AssessPhysicalSupport(capabilities, RuncRuntime).Container; got != ports.PhysicalSupportUnknown {
		t.Fatalf("unreported runtime support = %q", got)
	}
	capabilities.Runtimes = []string{"alternate-runtime"}
	if got := AssessPhysicalSupport(capabilities, RuncRuntime).Container; got != ports.PhysicalSupportUnsupported {
		t.Fatalf("missing runtime support = %q", got)
	}
	if got := AssessPhysicalSupport(capabilities, "alternate-runtime").Seccomp; got != ports.PhysicalSupportUnknown {
		t.Fatalf("alternative runtime seccomp support = %q", got)
	}
}

func TestParseNumericUserRequiresPositiveUIDAndGID(t *testing.T) {
	uid, gid, err := ParseNumericUser("65532:65531")
	if err != nil || uid != 65532 || gid != 65531 {
		t.Fatalf("ParseNumericUser = %d:%d, %v", uid, gid, err)
	}
	for _, value := range []string{"root", "0:1", "1:0", "-1:1", "1:2147483648", "1:2:3"} {
		if _, _, err := ParseNumericUser(value); err == nil {
			t.Fatalf("ParseNumericUser(%q) succeeded", value)
		}
	}
}

func TestHardenedSecurityOptionsSelectBuiltinSeccomp(t *testing.T) {
	want := []string{NoNewPrivilegesOption, "seccomp=" + DockerBuiltinSeccompProfile}
	if got := HardenedSecurityOptions(); !slices.Equal(got, want) {
		t.Fatalf("HardenedSecurityOptions = %v; want %v", got, want)
	}
	if !SupportsSecurityOption([]string{"name=apparmor", "name=seccomp,profile=builtin"}, "seccomp") {
		t.Fatal("Docker seccomp support was not recognized")
	}
	if SupportsSecurityOption([]string{"name=not-seccomp"}, "seccomp") {
		t.Fatal("unrelated security option was accepted")
	}
}

func TestPrivateNamespaceArgumentsPinEveryConfigurableNamespace(t *testing.T) {
	want := []string{
		"--network", "none",
		"--ipc", "private",
		"--cgroupns", "private",
	}
	if got := PrivateNamespaceArguments(); !slices.Equal(got, want) {
		t.Fatalf("PrivateNamespaceArguments = %v; want %v", got, want)
	}
}
