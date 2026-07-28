package dockercli

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

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
	capabilities.Runtimes = []string{"gvisor"}
	if got := AssessPhysicalSupport(capabilities, RuncRuntime).Container; got != ports.PhysicalSupportUnsupported {
		t.Fatalf("missing runtime support = %q", got)
	}
	if got := AssessPhysicalSupport(capabilities, "gvisor").Seccomp; got != ports.PhysicalSupportUnknown {
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
