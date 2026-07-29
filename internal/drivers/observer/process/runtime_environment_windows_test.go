//go:build windows

package process

import (
	"slices"
	"strings"
	"testing"
)

func TestWindowsRuntimeEnvironmentSuppliesOnlyCanonicalSystemRoot(t *testing.T) {
	const systemRoot = `C:\Windows-World-Test`
	t.Setenv("SystemRoot", systemRoot)
	platform := platformRuntimeEnvironment()
	if len(platform) != 1 || platform["SystemRoot"] != systemRoot {
		t.Fatalf("platform runtime environment = %#v", platform)
	}
	merged := serializedEnvironment(trustedRuntimeEnvironment(map[string]string{
		"SYSTEMROOT": `C:\untrusted`, "EXPLICIT_SETTING": "value",
	}))
	if !slices.Contains(merged, "SystemRoot="+systemRoot) || !slices.Contains(merged, "EXPLICIT_SETTING=value") ||
		slices.Contains(merged, `SYSTEMROOT=C:\untrusted`) {
		t.Fatalf("merged runtime environment = %v", merged)
	}
}

func TestWindowsRuntimeEnvironmentKeyCannotBeConfiguredCaseInsensitively(t *testing.T) {
	for _, name := range []string{"SystemRoot", "SYSTEMROOT", "systemroot"} {
		t.Run(name, func(t *testing.T) {
			configuration := exactAdapterConfiguration()
			configuration.Environment = map[string]string{name: `C:\untrusted`}
			if err := configuration.Validate(); err == nil || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("configuration environment %q error = %v", name, err)
			}
			adapter := testAdapters(nil)[0]
			adapter.Environment = map[string]string{name: `C:\untrusted`}
			if err := adapter.Validate(); err == nil || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("adapter environment %q error = %v", name, err)
			}
		})
	}
}
