package cuttlefish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
)

const fileGatewayTestADBServerEndpoint = "127.0.0.1:5041"

func TestValidateManagedADBServerEndpointRequiresLiteralLoopback(t *testing.T) {
	for _, valid := range []string{"127.0.0.1:5037", "[::1]:5037"} {
		if err := ValidateManagedADBServerEndpoint(valid); err != nil {
			t.Fatalf("valid endpoint %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "localhost:5037", "192.0.2.10:5037", "127.0.0.1:0", " 127.0.0.1:5037"} {
		if err := ValidateManagedADBServerEndpoint(invalid); err == nil {
			t.Fatalf("invalid endpoint %q was accepted", invalid)
		}
	}
}

func TestCommandFileGatewayUsesOnlyExactSerialAndVerifiesBytes(t *testing.T) {
	scope, allocation := adbTestScope(t)
	runner := newFakeADBRunner(t, allocation.Serial)
	staging := cuttlefishTempDir(t, "world-adb-staging-")
	gateway, err := NewCommandFileGateway(CommandFileGatewayConfig{Runner: runner, ADBBinary: "test-adb", ADBServerEndpoint: fileGatewayTestADBServerEndpoint, StagingRoot: staging, MaximumTransferBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gateway.PrepareRun(ctx, scope, allocation); err != nil {
		t.Fatal(err)
	}
	content := []byte("opaque android bytes")
	digest := domain.NewDigest(content)
	file, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{
		Area: DeviceFileWritable, LogicalPath: "nested/odd_name.bin", Mode: 0o640, MaximumBytes: int64(len(content)), ExpectedDigest: digest, ExpectedSize: int64(len(content)),
	}, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if file.Digest != digest || file.Size != int64(len(content)) {
		t.Fatalf("put result = %#v", file)
	}
	reader, err := gateway.Get(ctx, scope, allocation, "nested/odd_name.bin", 64)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Digest() != digest || reader.Size() != int64(len(content)) {
		t.Fatalf("reader identity = %s/%d", reader.Digest(), reader.Size())
	}
	pulled, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(pulled, content) {
		t.Fatalf("pulled = %q", pulled)
	}

	invocations := runner.Invocations()
	if len(invocations) == 0 {
		t.Fatal("ADB was not invoked")
	}
	var pushedHostPath string
	for _, invocation := range invocations {
		action, exact := exactADBTestAction(invocation.Args, fileGatewayTestADBServerEndpoint, allocation.Serial)
		if invocation.Program != "test-adb" || !exact {
			t.Fatalf("invocation escaped exact serial scope: %#v", invocation)
		}
		joined := strings.Join(action, "\x00")
		if strings.Contains(joined, "host:") || strings.Contains(joined, "host-serial:") || containsArgument(action, "sh") || containsArgument(action, "bash") {
			t.Fatalf("host-global or shell authority used: %#v", invocation.Args)
		}
		if action[0] == "push" {
			pushedHostPath = action[1]
		}
	}
	stagingAbs, _ := filepath.Abs(staging)
	pushedAbs, _ := filepath.Abs(pushedHostPath)
	relative, relErr := filepath.Rel(stagingAbs, pushedAbs)
	if pushedHostPath == "" || relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("push exposed a caller-controlled host path: %q", pushedHostPath)
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging files leaked: %v", entries)
	}
}

func TestCommandFileGatewayRejectsCrossDeviceOversizeAndCorruption(t *testing.T) {
	scope, allocation := adbTestScope(t)
	runner := newFakeADBRunner(t, allocation.Serial)
	staging := cuttlefishTempDir(t, "world-adb-reject-")
	gateway, err := NewCommandFileGateway(CommandFileGatewayConfig{Runner: runner, ADBServerEndpoint: fileGatewayTestADBServerEndpoint, StagingRoot: staging, MaximumTransferBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wrong := allocation
	wrong.Serial = "127.0.0.1:7777"
	wrong.ADBAddress = wrong.Serial
	if err := gateway.PrepareRun(ctx, scope, wrong); err == nil {
		t.Fatal("cross-device scope accepted")
	}
	if len(runner.Invocations()) != 0 {
		t.Fatal("ADB invoked before scope rejection")
	}
	if _, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "../escape", Mode: 0o600, MaximumBytes: 1, ExpectedSize: -1}, bytes.NewReader(nil)); err == nil {
		t.Fatal("path traversal accepted")
	}
	if _, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "large", Mode: 0o600, MaximumBytes: 3, ExpectedSize: -1}, bytes.NewReader([]byte("four"))); err == nil {
		t.Fatal("oversize stream accepted")
	}
	stable := []byte("stable")
	if _, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "growing", Mode: 0o600, MaximumBytes: 8, ExpectedDigest: domain.NewDigest(stable), ExpectedSize: int64(len(stable))}, bytes.NewReader(stable)); err != nil {
		t.Fatal(err)
	}
	runner.growOnRead = true
	if _, err := gateway.Get(ctx, scope, allocation, "growing", int64(len(stable))); err == nil {
		t.Fatal("file growth during bounded pull was accepted")
	}
	runner.growOnRead = false
	runner.corruptPush = true
	value := []byte("exact")
	if _, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "corrupt", Mode: 0o600, MaximumBytes: 8, ExpectedDigest: domain.NewDigest(value), ExpectedSize: int64(len(value))}, bytes.NewReader(value)); err == nil {
		t.Fatal("corrupt device write accepted")
	}
	if runner.HasRemoteSuffix(".world-upload") {
		t.Fatal("failed remote upload leaked")
	}
	runner.corruptPush = false
	runner.corruptAfterMove = true
	if _, err := gateway.Put(ctx, scope, allocation, DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "post-move", Mode: 0o600, MaximumBytes: 8, ExpectedDigest: domain.NewDigest(value), ExpectedSize: int64(len(value))}, bytes.NewReader(value)); err == nil {
		t.Fatal("post-commit corruption was accepted")
	}
	if runner.HasRemoteSuffix("post-move") {
		t.Fatal("failed published file leaked")
	}
}

func TestCommandFileGatewayRejectsRemoteShellPathsAndSymlinkParents(t *testing.T) {
	scope, allocation := adbTestScope(t)
	runner := newFakeADBRunner(t, allocation.Serial)
	gateway, err := NewCommandFileGateway(CommandFileGatewayConfig{Runner: runner, ADBServerEndpoint: fileGatewayTestADBServerEndpoint, StagingRoot: cuttlefishTempDir(t, "world-adb-path-")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gateway.PrepareRun(ctx, scope, allocation); err != nil {
		t.Fatal(err)
	}
	before := len(runner.Invocations())
	for _, logicalPath := range []string{"odd name", "semi;colon", "line\nbreak", "sub/$(id)", "unicode/λ"} {
		plan := DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: logicalPath, Mode: 0o600, MaximumBytes: 1, ExpectedSize: -1}
		if _, err := gateway.Put(ctx, scope, allocation, plan, bytes.NewReader(nil)); !domain.IsCode(err, domain.CodeInvalidArgument) {
			t.Fatalf("path %q error = %v, want invalid argument", logicalPath, err)
		}
	}
	if len(runner.Invocations()) != before {
		t.Fatal("remote-shell path reached ADB")
	}

	link := scopedDeviceAreaRoot(scope, DeviceFileWritable) + "/link"
	runner.SetSymlink(link, "/data/local/tmp/outside")
	plan := DeviceFileWritePlan{Area: DeviceFileWritable, LogicalPath: "link/file.bin", Mode: 0o600, MaximumBytes: 1, ExpectedSize: -1}
	if _, err := gateway.Put(ctx, scope, allocation, plan, bytes.NewReader(nil)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("symlink parent error = %v, want integrity violation", err)
	}
	if runner.HasRemotePrefix("/data/local/tmp/outside") {
		t.Fatal("symlink parent escaped the device run root")
	}
}

func TestExactSerialADBRejectsSelectionEscapesBeforeRunner(t *testing.T) {
	runner := newFakeADBRunner(t, "serial-1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server, err := parseADBServerEndpoint(fileGatewayTestADBServerEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{
		"devices", "start-server", "host:version",
		"-a", "-d", "-e", "-s", "-t", "-H", "-P", "-L", "--one-device", "--exit-on-write-error",
		"wait-for-any", "wait-for-any-device", "wait-for-local", "wait-for-local-device", "wait-for-usb", "wait-for-usb-device",
	} {
		if _, err := runExactSerialADBAt(ctx, runner, "adb", server, "serial-1", 128, action); err == nil {
			t.Fatalf("selection-escaping action %q was accepted", action)
		}
	}
	for _, serial := range []string{"serial-1\nother", "-e", "-s", "--help"} {
		if _, err := runExactSerialADBAt(ctx, runner, "adb", server, serial, 128, "get-state"); err == nil {
			t.Fatalf("unsafe or option-shaped serial %q was accepted", serial)
		}
	}
	if len(runner.Invocations()) != 0 {
		t.Fatal("runner was called for a rejected ADB command")
	}
	exactAction := []string{"shell", "mkdir", "-p", "--", "/data/local/tmp/exact"}
	if _, err := runExactSerialADBAt(ctx, runner, "adb", server, "serial-1", 128, exactAction...); err != nil {
		t.Fatalf("exact serial command was rejected: %v", err)
	}
	invocations := runner.Invocations()
	if len(invocations) != 1 {
		t.Fatalf("runner invocation count = %d, want 1", len(invocations))
	}
	if action, exact := exactADBTestAction(invocations[0].Args, fileGatewayTestADBServerEndpoint, "serial-1"); !exact || !reflect.DeepEqual(action, exactAction) {
		t.Fatalf("exact ADB invocation = %#v", invocations[0])
	}
}

func adbTestScope(t *testing.T) (deviceproxy.Scope, Allocation) {
	t.Helper()
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	serial := "127.0.0.1:6520"
	scope, err := deviceproxy.IssueScope(lease, target, 3, run, serial, bytes.NewReader(bytes.Repeat([]byte{0x5a}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return scope, Allocation{InstanceNumber: 3, InstanceName: "cvd-3", Serial: serial, ADBAddress: serial}
}

func containsArgument(arguments []string, expected string) bool {
	for _, argument := range arguments {
		if argument == expected {
			return true
		}
	}
	return false
}

func cuttlefishTempDir(t *testing.T, pattern string) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", pattern)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}

type fakeADBRunner struct {
	t                *testing.T
	serial           string
	mu               sync.Mutex
	files            map[string][]byte
	modes            map[string]string
	directories      map[string]struct{}
	symlinks         map[string]string
	invocations      []command.Invocation
	corruptPush      bool
	growOnRead       bool
	corruptAfterMove bool
}

func newFakeADBRunner(t *testing.T, serial string) *fakeADBRunner {
	return &fakeADBRunner{t: t, serial: serial, files: make(map[string][]byte), modes: make(map[string]string), directories: map[string]struct{}{"/data": {}}, symlinks: make(map[string]string)}
}

func (r *fakeADBRunner) Run(ctx context.Context, invocation command.Invocation) (command.Result, error) {
	r.t.Helper()
	if err := ctx.Err(); err != nil {
		return command.Result{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	invocation.Args = append([]string(nil), invocation.Args...)
	r.invocations = append(r.invocations, invocation)
	args, exact := exactADBTestAction(invocation.Args, fileGatewayTestADBServerEndpoint, r.serial)
	if !exact {
		return command.Result{}, fmt.Errorf("ADB invocation is not exact-serial: %v", invocation.Args)
	}
	switch args[0] {
	case "push":
		if len(args) != 3 {
			return command.Result{}, fmt.Errorf("bad push args: %v", args)
		}
		content, err := os.ReadFile(args[1])
		if err != nil {
			return command.Result{}, err
		}
		if r.corruptPush {
			content = append(content, '!')
		}
		r.files[args[2]] = append([]byte(nil), content...)
	case "exec-out":
		if len(args) != 6 || args[1] != "head" || args[2] != "-c" || args[4] != "--" {
			return command.Result{}, fmt.Errorf("bad exec-out args: %v", args)
		}
		maximum, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return command.Result{}, err
		}
		content, found := r.files[args[5]]
		if !found {
			return command.Result{}, os.ErrNotExist
		}
		if r.growOnRead {
			content = append(append([]byte(nil), content...), '!')
		}
		if int64(len(content)) > maximum {
			content = content[:maximum]
		}
		return command.Result{Stdout: append([]byte(nil), content...)}, nil
	case "shell":
		return r.runShell(args[1:])
	default:
		return command.Result{}, fmt.Errorf("unexpected ADB action %q", args[0])
	}
	return command.Result{}, nil
}

func (r *fakeADBRunner) runShell(args []string) (command.Result, error) {
	if len(args) == 0 {
		return command.Result{}, fmt.Errorf("missing device command")
	}
	devicePath := args[len(args)-1]
	switch args[0] {
	case "mkdir":
		if _, linked := r.symlinks[devicePath]; !linked {
			r.directories[devicePath] = struct{}{}
		}
		return command.Result{}, nil
	case "rm":
		if containsArgument(args, "-rf") {
			for name := range r.files {
				if name == devicePath || strings.HasPrefix(name, devicePath+"/") {
					delete(r.files, name)
					delete(r.modes, name)
				}
			}
			for name := range r.directories {
				if name == devicePath || strings.HasPrefix(name, devicePath+"/") {
					delete(r.directories, name)
				}
			}
		} else {
			delete(r.files, devicePath)
			delete(r.modes, devicePath)
		}
		return command.Result{}, nil
	case "realpath":
		resolved, found := r.resolve(devicePath)
		if !found {
			return command.Result{}, os.ErrNotExist
		}
		return command.Result{Stdout: []byte(resolved + "\n")}, nil
	case "stat":
		content, found := r.files[devicePath]
		if !found {
			return command.Result{}, os.ErrNotExist
		}
		if len(args) >= 3 && args[2] == "%a" {
			mode, found := r.modes[devicePath]
			if !found {
				return command.Result{}, fmt.Errorf("mode unavailable")
			}
			return command.Result{Stdout: []byte(strings.TrimLeft(mode, "0") + "\n")}, nil
		}
		return command.Result{Stdout: []byte(strconv.Itoa(len(content)) + "\n")}, nil
	case "sha256sum":
		content, found := r.files[devicePath]
		if !found {
			return command.Result{}, os.ErrNotExist
		}
		sum := sha256.Sum256(content)
		return command.Result{Stdout: []byte(hex.EncodeToString(sum[:]) + "  " + devicePath + "\n")}, nil
	case "chmod":
		if len(args) != 4 {
			return command.Result{}, fmt.Errorf("bad chmod args: %v", args)
		}
		r.modes[devicePath] = args[1]
		return command.Result{}, nil
	case "mv":
		if len(args) != 5 {
			return command.Result{}, fmt.Errorf("bad mv args: %v", args)
		}
		source, destination := args[3], args[4]
		content, found := r.files[source]
		if !found {
			return command.Result{}, os.ErrNotExist
		}
		r.files[destination] = content
		if r.corruptAfterMove {
			r.files[destination] = append(append([]byte(nil), content...), '!')
		}
		delete(r.files, source)
		r.modes[destination] = r.modes[source]
		delete(r.modes, source)
		return command.Result{}, nil
	default:
		return command.Result{}, fmt.Errorf("unexpected device command %q", args[0])
	}
}

func (r *fakeADBRunner) Invocations() []command.Invocation {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]command.Invocation, len(r.invocations))
	copy(result, r.invocations)
	return result
}

func (r *fakeADBRunner) HasRemoteSuffix(suffix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name := range r.files {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func (r *fakeADBRunner) HasRemotePrefix(prefix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name := range r.files {
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			return true
		}
	}
	return false
}

func (r *fakeADBRunner) SetSymlink(name, target string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.symlinks[name] = target
}

func (r *fakeADBRunner) resolve(name string) (string, bool) {
	for link, target := range r.symlinks {
		if name == link {
			return target, true
		}
		if strings.HasPrefix(name, link+"/") {
			return target + strings.TrimPrefix(name, link), true
		}
	}
	if _, found := r.directories[name]; found {
		return name, true
	}
	if _, found := r.files[name]; found {
		return name, true
	}
	return "", false
}

var _ command.Runner = (*fakeADBRunner)(nil)

func exactADBTestAction(arguments []string, endpoint, serial string) ([]string, bool) {
	server, err := parseADBServerEndpoint(endpoint)
	if err != nil {
		return nil, false
	}
	prefix := server.globalArgs("-s", serial)
	if len(arguments) <= len(prefix) || !reflect.DeepEqual(arguments[:len(prefix)], prefix) {
		return nil, false
	}
	return arguments[len(prefix):], true
}
