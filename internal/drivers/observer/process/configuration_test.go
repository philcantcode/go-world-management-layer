package process

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestRuntimeBindingCanonicalExternalRepresentationRoundTrips(t *testing.T) {
	if got := RuntimeBindingNone.String(); got != "none" {
		t.Fatalf("RuntimeBindingNone.String() = %q, want canonical external value %q", got, "none")
	}
	for _, test := range []struct {
		external string
		want     RuntimeBinding
	}{
		{external: "none", want: RuntimeBindingNone},
		{external: "android-exact-adb", want: RuntimeBindingAndroidExactADB},
	} {
		parsed, err := ParseRuntimeBinding(test.external)
		if err != nil || parsed != test.want {
			t.Fatalf("ParseRuntimeBinding(%q) = %q, %v", test.external, parsed, err)
		}
		var decoded struct {
			Binding RuntimeBinding `json:"binding"`
		}
		if err := json.Unmarshal([]byte(`{"binding":"`+test.external+`"}`), &decoded); err != nil || decoded.Binding != test.want {
			t.Fatalf("JSON runtime binding %q = %q, %v", test.external, decoded.Binding, err)
		}
	}
	for _, invalid := range []string{"", "NONE", "template"} {
		if _, err := ParseRuntimeBinding(invalid); err == nil {
			t.Fatalf("non-canonical runtime binding %q was accepted", invalid)
		}
	}
}

func TestConfigurationDigestBindsEveryVariableExecutableField(t *testing.T) {
	base := exactAdapterConfiguration()
	baseDigest, err := ConfigurationDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	const expected = "sha256:c7da45267f01a49d9811dc2f45e16b9c9d74fd538c5935e468a9cda1e3c2329c"
	if baseDigest.String() != expected {
		t.Fatalf("configuration digest = %s, want %s", baseDigest.String(), expected)
	}
	mutations := map[string]func(*AdapterConfiguration){
		"version": func(value *AdapterConfiguration) { value.Version = "2" },
		"signal family": func(value *AdapterConfiguration) {
			value.SignalFamily, value.Adapter, value.RuntimeBinding = "process.stdout", "process", RuntimeBindingNone
		},
		"placement":          func(value *AdapterConfiguration) { value.Placement = domain.CollectorPlacementHost },
		"coverage":           func(value *AdapterConfiguration) { value.CoverageLevel = domain.CoverageLevelComplete },
		"program":            func(value *AdapterConfiguration) { value.Program += ".alt"; value.ReadinessProgram += ".alt" },
		"args":               func(value *AdapterConfiguration) { value.Args = append(value.Args, "WORLD_EXTRA:I") },
		"environment":        func(value *AdapterConfiguration) { value.Environment["LANG"] = "en_GB.UTF-8" },
		"readiness interval": func(value *AdapterConfiguration) { value.ReadinessInterval = 500 * time.Millisecond },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := exactAdapterConfiguration()
			mutate(&changed)
			digest, err := ConfigurationDigest(changed)
			if err != nil {
				t.Fatal(err)
			}
			if digest == baseDigest {
				t.Fatalf("configuration mutation did not change digest %s", digest.String())
			}
		})
	}
}

func TestConfigurationDigestPreservesNilAndEmptyIdentityAndCanonicalizesMapOrder(t *testing.T) {
	nilValues := genericAdapterConfiguration()
	nilValues.Args, nilValues.Environment = nil, nil
	emptyValues := nilValues
	emptyValues.Args, emptyValues.Environment = []string{}, map[string]string{}
	nilDigest, err := ConfigurationDigest(nilValues)
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, err := ConfigurationDigest(emptyValues)
	if err != nil {
		t.Fatal(err)
	}
	if nilDigest == emptyDigest {
		t.Fatal("nil and explicitly empty configuration values collapsed to one identity")
	}

	first := genericAdapterConfiguration()
	first.Environment = map[string]string{"LANG": "C", "TZ": "UTC"}
	second := genericAdapterConfiguration()
	second.Environment = map[string]string{"TZ": "UTC", "LANG": "C"}
	firstDigest, err := ConfigurationDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := ConfigurationDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("map insertion order changed digest: %s != %s", firstDigest.String(), secondDigest.String())
	}
}

func TestBuildAdapterUsesTheConfigurationItFingerprints(t *testing.T) {
	configuration := exactAdapterConfiguration()
	adapter, err := BuildAdapter(configuration)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := ConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	readiness, ok := adapter.Readiness.(CommandReadiness)
	if !ok {
		t.Fatalf("readiness type = %T", adapter.Readiness)
	}
	if adapter.ConfigurationDigest != digest || adapter.Name != configuration.Adapter || adapter.Version != configuration.Version ||
		adapter.SignalFamily != configuration.SignalFamily || adapter.Placement != configuration.Placement || adapter.CoverageLevel != configuration.CoverageLevel ||
		adapter.RuntimeBinding != configuration.RuntimeBinding || adapter.Program != configuration.Program || !reflect.DeepEqual(adapter.Args, configuration.Args) ||
		!reflect.DeepEqual(adapter.Environment, configuration.Environment) || !reflect.DeepEqual(adapter.VersionArgs, configuration.VersionArgs) ||
		readiness.Program != configuration.ReadinessProgram || !reflect.DeepEqual(readiness.Args, configuration.ReadinessArgs) || readiness.Interval != configuration.ReadinessInterval || readiness.RuntimeBinding != configuration.RuntimeBinding {
		t.Fatalf("built adapter does not preserve exact configuration: %#v readiness=%#v", adapter, readiness)
	}
	configuration.Args[0] = "mutated"
	configuration.Environment["LC_ALL"] = "mutated"
	if adapter.Args[0] == "mutated" || adapter.Environment["LC_ALL"] == "mutated" {
		t.Fatal("built adapter aliases caller-owned configuration")
	}
}

func TestAdapterConfigurationRejectsAmbiguousOrReservedValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AdapterConfiguration)
		want   string
	}{
		{name: "untrimmed identity", mutate: func(value *AdapterConfiguration) { value.Version = " 1" }, want: "trimmed"},
		{name: "reserved environment", mutate: func(value *AdapterConfiguration) { value.Environment["WORLD_TARGET_RUN_ID"] = "spoofed" }, want: "reserved"},
		{name: "case-insensitive reserved environment", mutate: func(value *AdapterConfiguration) { value.Environment["world_target_run_id"] = "spoofed" }, want: "reserved"},
		{name: "nul argument", mutate: func(value *AdapterConfiguration) { value.Args[0] = "bad\x00arg" }, want: "NUL"},
		{name: "invalid interval", mutate: func(value *AdapterConfiguration) { value.ReadinessInterval = 0 }, want: "readiness interval"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := exactAdapterConfiguration()
			test.mutate(&configuration)
			if err := configuration.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAndroidLogcatConfigurationRequiresOneMatchingExactADBSelection(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AdapterConfiguration)
	}{
		{name: "missing runtime binding", mutate: func(value *AdapterConfiguration) { value.RuntimeBinding = RuntimeBindingNone }},
		{name: "unknown runtime binding", mutate: func(value *AdapterConfiguration) { value.RuntimeBinding = "template" }},
		{name: "collector emulator selector", mutate: func(value *AdapterConfiguration) { value.Args = []string{"-e", "logcat"} }},
		{name: "collector usb selector", mutate: func(value *AdapterConfiguration) { value.Args = []string{"-d", "logcat"} }},
		{name: "readiness emulator selector", mutate: func(value *AdapterConfiguration) { value.ReadinessArgs = []string{"-e", "get-state"} }},
		{name: "fixed collector selection", mutate: func(value *AdapterConfiguration) {
			value.Args = []string{"-H", "127.0.0.1", "-P", "5037", "-s", "emulator-5580", "logcat"}
		}},
		{name: "wrong collector action", mutate: func(value *AdapterConfiguration) { value.Args[0] = "shell" }},
		{name: "wrong readiness action", mutate: func(value *AdapterConfiguration) { value.ReadinessArgs[0] = "wait-for-device" }},
		{name: "readiness trailing arguments", mutate: func(value *AdapterConfiguration) { value.ReadinessArgs = append(value.ReadinessArgs, "other") }},
		{name: "mismatched program", mutate: func(value *AdapterConfiguration) { value.ReadinessProgram += ".other" }},
		{name: "selector in version probe", mutate: func(value *AdapterConfiguration) { value.VersionArgs = []string{"-e", "version"} }},
		{name: "ambient device environment", mutate: func(value *AdapterConfiguration) { value.Environment["ANDROID_SERIAL"] = "emulator-5580" }},
		{name: "ambient server environment", mutate: func(value *AdapterConfiguration) { value.Environment["adb_server_socket"] = "tcp:127.0.0.1:5038" }},
		{name: "wrong adapter", mutate: func(value *AdapterConfiguration) { value.Adapter = "process" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := exactAdapterConfiguration()
			test.mutate(&configuration)
			if err := configuration.Validate(); err == nil {
				t.Fatal("ambiguous or mismatched Android logcat ADB configuration was accepted")
			}
		})
	}
}

func TestAndroidLogcatConfigurationAllowsDeviceLocalLogcatOptionsAfterExactSelection(t *testing.T) {
	configuration := exactAdapterConfiguration()
	configuration.Args = append(configuration.Args, "-s", "WORLD_TEST:I", "*:S")
	if err := configuration.Validate(); err != nil {
		t.Fatalf("exact Android logcat configuration: %v", err)
	}

	generic := genericAdapterConfiguration()
	generic.Args = []string{"-e", "opaque-generic-argument"}
	if err := generic.Validate(); err != nil {
		t.Fatalf("generic process observer was subjected to Android ADB validation: %v", err)
	}
}

func exactAdapterConfiguration() AdapterConfiguration {
	return AdapterConfiguration{
		Adapter: "logcat", Version: "1", SignalFamily: "android.logcat",
		Placement: domain.CollectorPlacementGuest, CoverageLevel: domain.CoverageLevelPartial,
		RuntimeBinding: RuntimeBindingAndroidExactADB,
		Program:        `C:\Android\Sdk\platform-tools\adb.exe`,
		Args:           []string{"logcat", "-v", "threadtime"},
		Environment:    map[string]string{"LC_ALL": "C"}, VersionArgs: []string{"version"},
		ReadinessProgram:  `C:\Android\Sdk\platform-tools\adb.exe`,
		ReadinessArgs:     []string{"get-state"},
		ReadinessInterval: 250 * time.Millisecond,
	}
}

func genericAdapterConfiguration() AdapterConfiguration {
	return AdapterConfiguration{
		Adapter: "process", Version: "1", SignalFamily: "process.stdout",
		Placement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelPartial,
		Program: "observer", VersionArgs: []string{"--version"}, ReadinessProgram: "observer-ready",
		ReadinessArgs: []string{"ready"}, ReadinessInterval: 250 * time.Millisecond,
	}
}
