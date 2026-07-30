package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func TestParseOpenExecKeepsArgumentOnlyIndexContract(t *testing.T) {
	source := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	options, err := parseOpenExec([]string{
		"-lease", "lease_1", "-executable", "/workspace/provider", "-policy", "sha256:policy",
		"-temporary-input", "1:payload.bin=" + source, "--", "-input", "placeholder", "-output", "result.json",
	}, &bytes.Buffer{}, worldcli.OpenConfig{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	wantArgv := []string{"-input", "placeholder", "-output", "result.json"}
	if len(options.start.Argv) != len(wantArgv) {
		t.Fatalf("argv = %#v", options.start.Argv)
	}
	for index := range wantArgv {
		if options.start.Argv[index] != wantArgv[index] {
			t.Fatalf("argv = %#v, want %#v", options.start.Argv, wantArgv)
		}
	}
	if len(options.start.TemporaryInputs) != 1 || options.start.TemporaryInputs[0].ArgvIndex != 1 || string(options.start.TemporaryInputs[0].Content) != "payload" {
		t.Fatalf("temporary inputs = %#v", options.start.TemporaryInputs)
	}
}

func TestReadTemporaryInputsParsesExplicitArgvBinding(t *testing.T) {
	source := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(source, []byte{0, 1, 2}, 0o600); err != nil {
		t.Fatal(err)
	}
	values, err := readTemporaryInputs([]string{"2:prompt.bin=" + source}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].ArgvIndex != 2 || values[0].NameHint != "prompt.bin" || values[0].Mode != 0o600 || len(values[0].Content) != 3 {
		t.Fatalf("values = %#v", values)
	}
}

func TestReadTemporaryInputsRejectsImplicitOrUnsafeBinding(t *testing.T) {
	for _, value := range []string{"prompt.bin=file", "x:prompt.bin=file", "1:../prompt.bin=file", "1:=file"} {
		if _, err := readTemporaryInputs([]string{value}, 1); err == nil {
			t.Fatalf("unsafe binding %q was accepted", value)
		}
	}
}

func TestTemporaryInputBindingsMustBeUniqueAndInsideArgv(t *testing.T) {
	if err := validateTemporaryInputBindings([]*worldv1.TemporaryInput{{ArgvIndex: 1}}, 1); err == nil {
		t.Fatal("out-of-range temporary input was accepted")
	}
	if err := validateTemporaryInputBindings([]*worldv1.TemporaryInput{{ArgvIndex: 0}, {ArgvIndex: 0}}, 2); err == nil {
		t.Fatal("duplicate temporary input index was accepted")
	}
	if err := validateTemporaryInputBindings([]*worldv1.TemporaryInput{{ArgvIndex: 1}, {ArgvIndex: 0}}, 2); err != nil {
		t.Fatalf("valid temporary input bindings failed: %v", err)
	}
}
