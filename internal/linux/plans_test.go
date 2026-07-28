package linux

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestCgroupAndOverlayPlansAreExplicitAndBounded(t *testing.T) {
	cgroup := CgroupPlan{Root: "/sys/fs/cgroup/world", Parent: "lease-1", Owner: "target-1", Resources: admission.Resources{CPUMilli: 500, MemoryBytes: 1024, PIDs: 12}, MemoryHighBytes: 768}
	values, err := cgroup.ControllerValues()
	if err != nil {
		t.Fatal(err)
	}
	if values["cpu.max"] != "50000 100000" || values["memory.max"] != "1024" || values["memory.high"] != "768" || values["pids.max"] != "12" {
		t.Fatalf("controller values = %#v", values)
	}
	if path, _ := cgroup.Path(); path != "/sys/fs/cgroup/world/lease-1/target-1" {
		t.Fatalf("cgroup path = %q", path)
	}
	unsafe := cgroup
	unsafe.Owner = "../../host"
	if unsafe.Validate() == nil {
		t.Fatal("unsafe cgroup owner accepted")
	}
	overlay := OverlayPlan{LowerDirectories: []string{"/views/selection", "/views/tools"}, UpperDirectory: "/workspaces/ws/upper", WorkDirectory: "/workspaces/ws/work", MergedDirectory: "/workspaces/ws/merged", ExtraOptions: []string{"nodev"}}
	options, err := overlay.MountOptions()
	if err != nil {
		t.Fatal(err)
	}
	if options != "lowerdir=/views/selection:/views/tools,upperdir=/workspaces/ws/upper,workdir=/workspaces/ws/work,nodev" {
		t.Fatalf("mount options = %q", options)
	}
	overlay.ExtraOptions = []string{"upperdir=/host"}
	if overlay.Validate() == nil {
		t.Fatal("caller override of authority-bearing overlay option accepted")
	}
}

func TestPSIParserAndOffLinuxCapabilityResult(t *testing.T) {
	sample, err := ParsePSI("some avg10=1.25 avg60=2.50 avg300=3.75 total=100\nfull avg10=0.50 avg60=1.00 avg300=2.00 total=50\n")
	if err != nil {
		t.Fatal(err)
	}
	if !sample.HasFull || sample.Some.TotalMicros != 100 || sample.AdmissionFull(nil).Current != .005 {
		t.Fatalf("sample = %#v", sample)
	}
	if _, err := ParsePSI("some avg10=101 avg60=0 avg300=0 total=1"); err == nil {
		t.Fatal("invalid PSI percentage accepted")
	}
	if runtime.GOOS != "linux" {
		result := PlatformSupport()
		if result.Supported || !strings.Contains(result.Reason, runtime.GOOS) {
			t.Fatalf("platform support = %#v", result)
		}
		fingerprint, err := ProbeCapabilities(context.Background(), ProbePlan{})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"linux.cgroup-v2", "linux.psi", "linux.overlayfs", "linux.kvm", "linux.btf"} {
			capability, found := fingerprint.Capability(name)
			if !found || capability.Status() != domain.CapabilityUnsupported {
				t.Fatalf("%s = %#v, found=%v", name, capability, found)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := ApplyCgroup(ctx, CgroupPlan{}); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("off-Linux cgroup error = %v", err)
		}
		if err := MountOverlay(ctx, OverlayPlan{}); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("off-Linux overlay error = %v", err)
		}
		if _, err := ReadHostPSI(ctx, ProbePlan{}, "memory"); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
			t.Fatalf("off-Linux PSI error = %v", err)
		}
	}
}
