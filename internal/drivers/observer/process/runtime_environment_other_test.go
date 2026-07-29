//go:build !windows

package process

import "testing"

func TestNonWindowsRuntimeEnvironmentAddsNoAmbientState(t *testing.T) {
	if environment := platformRuntimeEnvironment(); len(environment) != 0 {
		t.Fatalf("non-Windows platform runtime environment = %#v", environment)
	}
	if isPlatformRuntimeEnvironmentName("SystemRoot") {
		t.Fatal("non-Windows platform reserved a Windows-only runtime key")
	}
}
