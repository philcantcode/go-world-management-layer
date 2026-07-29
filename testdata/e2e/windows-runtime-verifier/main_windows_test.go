//go:build windows

package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestExpectedWindowsCPURateUsesExactJobScale(t *testing.T) {
	rate, err := expectedWindowsCPURate(2000, 16)
	if err != nil {
		t.Fatal(err)
	}
	if rate != 1250 {
		t.Fatalf("CPU rate = %d, want 1250", rate)
	}
	for _, test := range []struct {
		milli int64
		cpus  int
	}{
		{milli: 1500, cpus: 16},
		{milli: 17000, cpus: 16},
		{milli: 2000, cpus: 0},
	} {
		if _, err := expectedWindowsCPURate(test.milli, test.cpus); err == nil {
			t.Fatalf("invalid CPU contract (%d, %d) was accepted", test.milli, test.cpus)
		}
	}
}

func TestReadExpectedArgvRequiresNonemptyJSONArray(t *testing.T) {
	path := filepath.Join(t.TempDir(), "argv.json")
	want := []string{`C:\Android\emulator.exe`, "-avd", "world-emulator-5554", "-partition-size", "1024"}
	if err := os.WriteFile(path, []byte(`["C:\\Android\\emulator.exe","-avd","world-emulator-5554","-partition-size","1024"]`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readExpectedArgv(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("argv = %#v, want %#v", got, want)
	}
	if err := os.WriteFile(path, []byte(`[]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readExpectedArgv(path); err == nil {
		t.Fatal("empty expected argv was accepted")
	}
}
