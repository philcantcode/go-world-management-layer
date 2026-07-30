package platform_test

import (
	"encoding/json"
	"runtime"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/platform"
)

func TestReportIncludesCoreFeatures(t *testing.T) {
	report := platform.Report()
	if report.GOOS != runtime.GOOS || report.GOARCH != runtime.GOARCH {
		t.Fatalf("report host = %s/%s, want %s/%s", report.GOOS, report.GOARCH, runtime.GOOS, runtime.GOARCH)
	}
	required := []string{
		platform.FeatureLogicalControlPlane,
		platform.FeatureSafePathNamespace,
		platform.FeatureProcessLock,
		platform.FeatureDirectoryCopyWorkspace,
		platform.FeatureAndroidManagedEmulator,
		platform.FeatureAndroidResourceContainment,
	}
	for _, id := range required {
		if _, ok := report.Feature(id); !ok {
			t.Fatalf("missing feature %q", id)
		}
	}
	if len(report.Features) == 0 {
		t.Fatal("expected features")
	}
}

func TestReportJSONRoundTrip(t *testing.T) {
	report := platform.Report()
	encoded, err := report.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded platform.SupportReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GOOS != report.GOOS || len(decoded.Features) != len(report.Features) {
		t.Fatalf("decoded report mismatch: %#v", decoded)
	}
}

func TestSafePathAndAndroidMatchHost(t *testing.T) {
	report := platform.Report()
	switch runtime.GOOS {
	case "linux", "windows", "darwin":
		if report.StatusOf(platform.FeatureSafePathNamespace) != platform.StatusSupported {
			t.Fatalf("safepath status = %s, want supported on %s", report.StatusOf(platform.FeatureSafePathNamespace), runtime.GOOS)
		}
	}
	switch runtime.GOOS {
	case "windows":
		if report.StatusOf(platform.FeatureAndroidManagedEmulator) != platform.StatusSupported {
			t.Fatal("windows should fully support managed Android")
		}
		if report.StatusOf(platform.FeatureAndroidResourceContainment) != platform.StatusSupported {
			t.Fatal("windows should support Android Job containment")
		}
	case "linux":
		if report.StatusOf(platform.FeatureAndroidManagedEmulator) != platform.StatusPartial {
			t.Fatalf("linux Android status = %s, want partial", report.StatusOf(platform.FeatureAndroidManagedEmulator))
		}
		if report.StatusOf(platform.FeatureAndroidResourceContainment) != platform.StatusUnsupported {
			t.Fatal("linux should not claim Android resource containment")
		}
	case "darwin":
		if report.StatusOf(platform.FeatureAndroidManagedEmulator) != platform.StatusUnsupported {
			t.Fatalf("darwin Android status = %s, want unsupported", report.StatusOf(platform.FeatureAndroidManagedEmulator))
		}
		if report.StatusOf(platform.FeatureDirectoryCopyWorkspace) != platform.StatusSupported {
			t.Fatal("darwin should support directory-copy non-production")
		}
		if err := platform.RequireAndroidManagedHost(); err == nil {
			t.Fatal("expected RequireAndroidManagedHost to fail on darwin")
		}
	}
	if platform.DirectoryCopyHost() != (runtime.GOOS == "windows" || runtime.GOOS == "darwin") {
		t.Fatalf("DirectoryCopyHost() = %v on %s", platform.DirectoryCopyHost(), runtime.GOOS)
	}
}

func TestEnabledDriverNotesForAndroid(t *testing.T) {
	report := platform.Report()
	notes := report.EnabledDriverNotes("android-emulator", "none", "none")
	if runtime.GOOS == "windows" {
		// Resource containment is supported; managed is supported — may still be empty.
		return
	}
	if len(notes) == 0 {
		t.Fatal("expected driver notes when Android is selected on non-Windows")
	}
}
