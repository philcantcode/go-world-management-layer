package main

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func targetTestConfig() worldcli.OpenConfig {
	return worldcli.OpenConfig{Timeout: time.Second}
}

func scopedArguments(arguments ...string) []string {
	prefix := []string{"-target", "target_1", "-run", "run_1", "-policy", "sha256:policy"}
	return append(prefix, arguments...)
}

func TestTargetCommandCatalogue(t *testing.T) {
	rpcCommands := map[string]string{
		"OpenTargetExec": "exec",
		"PushTargetFile": "push",
		"PullTargetFile": "pull",
		"OpenTargetADB":  "adb",
	}
	methods := worldv1.File_world_v1_world_proto.Services().ByName("WorldService").Methods()
	for rpc, command := range rpcCommands {
		if methods.ByName(protoreflect.Name(rpc)) == nil {
			t.Errorf("unknown target RPC %s", rpc)
		}
		if targetCommands[command] == nil {
			t.Errorf("target RPC %s maps to missing command %q", rpc, command)
		}
	}
	for _, alias := range []string{"shell", "adb-proxy"} {
		if targetCommands[alias] == nil {
			t.Errorf("missing target command alias %q", alias)
		}
	}
}

func TestParseTargetExecPreservesArbitraryArgv(t *testing.T) {
	options, err := parseTargetExec(scopedArguments("--", "/bin/sh", "-c", "printf '%s' arbitrary"), &bytes.Buffer{}, targetTestConfig(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.start.Argv) != 3 || options.start.Argv[1] != "-c" || len(options.start.ExplicitShellBytes) != 0 {
		t.Fatalf("unexpected start: %#v", options.start)
	}
}

func TestParseTargetShellPreservesExactBytes(t *testing.T) {
	options, err := parseTargetExec(scopedArguments("-script", "printf 'a b\\n'"), &bytes.Buffer{}, targetTestConfig(), true)
	if err != nil {
		t.Fatal(err)
	}
	if string(options.start.ExplicitShellBytes) != "printf 'a b\\n'" || len(options.start.Argv) != 0 {
		t.Fatalf("unexpected start: %#v", options.start)
	}
}

func TestTransferParsersEnforceScopedPaths(t *testing.T) {
	push, err := parsePush(scopedArguments("samples/input.bin", "tmp/input.bin"), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if push.workspaceSource != "samples/input.bin" || push.targetDestination != "tmp/input.bin" || push.mode != 0o600 {
		t.Fatalf("unexpected push: %#v", push)
	}
	executable, err := parsePush(scopedArguments("-mode", "0750", "samples/tool", "tmp/tool"), &bytes.Buffer{})
	if err != nil || executable.mode != 0o750 {
		t.Fatalf("executable push = %#v, %v", executable, err)
	}
	if _, err := parsePush(scopedArguments("-mode", "01000", "samples/tool", "tmp/tool"), &bytes.Buffer{}); err == nil {
		t.Fatal("expected unsupported target mode rejection")
	}
	if _, err := parsePull(scopedArguments("../host", "result.bin"), &bytes.Buffer{}); err == nil {
		t.Fatal("expected target path escape rejection")
	}
	if _, err := parsePull(scopedArguments("tmp/result.bin", "../host"), &bytes.Buffer{}); err == nil {
		t.Fatal("expected workspace path escape rejection")
	}
	pull, err := parsePull(scopedArguments("tmp/result.bin", "derived/result.bin"), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if pull.targetSource != "tmp/result.bin" || pull.workspaceDestination != "derived/result.bin" {
		t.Fatalf("unexpected pull: %#v", pull)
	}
	meta := &worldv1.MutationMetadata{IdempotencyKey: "idem_1"}
	start := push.start(meta)
	if start.Start == nil || start.Start.Mutation != meta || start.Start.WorkspaceRelativePath != "samples/input.bin" || start.Start.TargetRelativePath != "tmp/input.bin" {
		t.Fatalf("workspace-backed push start = %#v", start)
	}
	request := pull.request(meta)
	if request.Mutation != meta || request.TargetRelativePath != "tmp/result.bin" || request.WorkspaceRelativePath != "derived/result.bin" {
		t.Fatalf("workspace-backed pull request = %#v", request)
	}
}

func TestWorkspaceTransferRequiresProtocolCompletion(t *testing.T) {
	operation := &worldv1.TargetOperation{TargetOperationId: "operation_1"}
	frames := []*worldv1.FileTransferFrame{{Complete: true, Digest: "sha256:abc", Operation: operation}}
	receive := func() (*worldv1.FileTransferFrame, error) {
		if len(frames) == 0 {
			return nil, io.EOF
		}
		frame := frames[0]
		frames = frames[1:]
		return frame, nil
	}
	got, err := receiveWorkspaceTransfer(receive, "push")
	if err != nil || got.TargetOperationId != operation.TargetOperationId || got == operation {
		t.Fatalf("completion = %#v, %v", got, err)
	}

	protocolErrors := []struct {
		name   string
		frames []*worldv1.FileTransferFrame
	}{
		{name: "missing completion"},
		{name: "streamed data", frames: []*worldv1.FileTransferFrame{{Data: []byte("unexpected")}}},
		{name: "missing digest", frames: []*worldv1.FileTransferFrame{{Complete: true, Operation: operation}}},
		{name: "missing operation", frames: []*worldv1.FileTransferFrame{{Complete: true, Digest: "sha256:abc"}}},
	}
	for _, test := range protocolErrors {
		t.Run(test.name, func(t *testing.T) {
			index := 0
			_, err := receiveWorkspaceTransfer(func() (*worldv1.FileTransferFrame, error) {
				if index >= len(test.frames) {
					return nil, io.EOF
				}
				frame := test.frames[index]
				index++
				return frame, nil
			}, "pull")
			if err == nil {
				t.Fatal("protocol error was accepted")
			}
		})
	}
}

func TestADBResponseRequiresStableSerialAndCompletion(t *testing.T) {
	var stderr bytes.Buffer
	response := adbResponse{stderr: &stderr}
	if err := response.handle(&worldv1.ADBFrame{AssignedSerial: "serial_1"}); err != nil {
		t.Fatal(err)
	}
	if err := response.handle(&worldv1.ADBFrame{Complete: true}); err != nil {
		t.Fatal(err)
	}
	if err := response.finish(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "serial_1") {
		t.Fatalf("serial output = %q", stderr.String())
	}
	if err := response.handle(&worldv1.ADBFrame{}); err == nil {
		t.Fatal("frame after completion was accepted")
	}
	changed := adbResponse{stderr: io.Discard}
	if err := changed.handle(&worldv1.ADBFrame{AssignedSerial: "serial_1"}); err != nil {
		t.Fatal(err)
	}
	if err := changed.handle(&worldv1.ADBFrame{AssignedSerial: "serial_2"}); err == nil {
		t.Fatal("assigned serial change was accepted")
	}
	if err := (&adbResponse{serial: "serial_1"}).finish(); err == nil {
		t.Fatal("missing ADB completion was accepted")
	}
}

func TestADBProxyRequiresLoopbackAndConnectionBound(t *testing.T) {
	if _, err := parseADBProxy(scopedArguments("-listen", "0.0.0.0:5037"), &bytes.Buffer{}); err == nil {
		t.Fatal("expected non-loopback rejection")
	}
	if _, err := parseADBProxy(scopedArguments("-connections", strconv.Itoa(maxADBProxyConnections+1)), &bytes.Buffer{}); err == nil {
		t.Fatal("expected connection bound rejection")
	}
	options, err := parseADBProxy(scopedArguments("-listen", "127.0.0.1:0", "-connections", strconv.Itoa(maxADBProxyConnections)), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if options.connections != maxADBProxyConnections {
		t.Fatalf("unexpected options: %#v", options)
	}
}

func TestADBProxyRejectsPositionalArguments(t *testing.T) {
	_, err := parseADBProxy(scopedArguments("unexpected"), &bytes.Buffer{})
	if err == nil || !errors.As(err, new(worldcli.UsageError)) {
		t.Fatalf("positional argument error = %v", err)
	}
}
