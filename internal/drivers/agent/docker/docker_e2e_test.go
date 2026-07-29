package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	workspacedirectory "github.com/philcantcode/go-world-management-layer/internal/drivers/workspace/directory"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestDockerAgentWorkspaceLifecycleEndToEnd(t *testing.T) {
	image := os.Getenv("WORLD_AGENT_DOCKER_E2E_IMAGE")
	repository, imageDigest, ok := splitPinnedAgentImage(image)
	if !ok {
		t.Skip("WORLD_AGENT_DOCKER_E2E_IMAGE must be a local repository@sha256:digest reference")
	}
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nativePath, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "testdata", "e2e", "build", "linux-amd64", "native-specimen"))
	if err != nil {
		t.Fatal(err)
	}
	native, err := os.ReadFile(nativePath)
	if err != nil {
		t.Fatalf("read native specimen: %v", err)
	}
	fixture := newAgentFixture(t, imageDigest, map[string]fixtureInput{"tools/native-specimen": {bytes: native, mode: 0o555}})
	workspaceDriver, err := workspacedirectory.New(workspacedirectory.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	handle, err := workspaceDriver.Prepare(ctx, fixture.workspace)
	if err != nil {
		t.Fatal(err)
	}
	handle, err = workspaceDriver.Mount(ctx, handle.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewCLIEngine("docker", nil, nil)
	driver, err := New(Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: repository, GuestBinary: defaultGuestBinary, ContainerUser: defaultGuestUser},
		Engine: engine,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := driver.Provision(ctx, fixture.agent)
	if err != nil {
		for _, cause := range flattenedErrors(err) {
			t.Logf("provision cause: %T: %v", cause, cause)
		}
		t.Fatal(err)
	}
	containerID := created.Status.ContainerID
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = engine.Remove(cleanupCtx, containerID)
	})
	if !created.Status.Ready || created.Status.GuestProtocol != uint32(transport.ProtocolVersion) {
		t.Fatalf("agent readiness = %#v", created.Status)
	}
	requireSingleWorkspaceMount(t, ctx, containerID, handle.MergedPath)

	payload := []byte("opaque temporary payload with spaces ; $() and a trailing zero\x00")
	executable := "/workspace/tools/native-specimen"
	argv := []string{"-input", "temporary-placeholder", "-output", "/workspace/result.json"}
	execPlan := newTestExecPlan(t, fixture.agent, executable, argv, []transport.TemporaryInput{{NameHint: "payload.bin", ArgvIndex: 1, Mode: 0o400, Bytes: payload}})
	session, err := driver.OpenExec(ctx, execPlan)
	if err != nil {
		t.Fatal(err)
	}
	stdout, terminal, receiveErr := receiveAgentExec(ctx, session)
	closeErr := session.Close()
	if err := errors.Join(receiveErr, closeErr); err != nil {
		t.Fatal(err)
	}
	if terminal.ExitCode != 0 || !terminal.CleanupConfirmed || terminal.Error != "" {
		t.Fatalf("exec terminal = %#v", terminal)
	}
	wantDigest := domain.NewDigest(payload)
	if !strings.Contains(string(stdout), wantDigest.String()) {
		t.Fatalf("stdout = %q, want digest %s", stdout, wantDigest)
	}
	resultBytes, err := os.ReadFile(filepath.Join(handle.MergedPath, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var specimen struct {
		InputDigest string `json:"input_digest"`
		Probes      []struct {
			Path       string `json:"path"`
			Accessible bool   `json:"accessible"`
		} `json:"boundary_probes"`
	}
	if err := json.Unmarshal(resultBytes, &specimen); err != nil {
		t.Fatal(err)
	}
	if specimen.InputDigest != wantDigest.String() {
		t.Fatalf("specimen digest = %q, want %q", specimen.InputDigest, wantDigest)
	}
	for _, probe := range specimen.Probes {
		if (probe.Path == "/var/run/docker.sock" || probe.Path == "/run/containerd/containerd.sock") && probe.Accessible {
			t.Fatalf("runtime authority crossed the agent boundary: %s", probe.Path)
		}
	}

	crashProof, err := driver.RecoverInterruptedExecs(ctx, fixture.agent)
	if err != nil {
		t.Fatalf("cross real Docker crash-recovery boundary: %v", err)
	}
	if err := crashProof.ValidateFor(fixture.agent); err != nil {
		t.Fatalf("real Docker crash-recovery proof = %#v: %v", crashProof, err)
	}
	if crashProof.Status.ContainerID != containerID {
		t.Fatalf("crash recovery changed container identity: got %q, want %q", crashProof.Status.ContainerID, containerID)
	}
	requireSingleWorkspaceMount(t, ctx, containerID, handle.MergedPath)

	ref := ports.AgentWorkspaceRef{ID: fixture.agent.Generation.Spec().AgentWorkspaceID, Generation: fixture.agent.Generation.Spec().Generation}
	if err := driver.Stop(ctx, ref, ports.StopGraceful); err != nil {
		t.Fatal(err)
	}
	if err := driver.Destroy(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := (command.OS{}).Run(ctx, command.Invocation{Program: "docker", Args: []string{"inspect", containerID}}); err == nil {
		t.Fatal("destroy left an orphaned agent container")
	}
	preview, err := workspaceDriver.Preview(ctx, handle.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workspaceDriver.Seal(ctx, handle.WorkspaceID, preview.ChangeSet.WorkspaceRevision()); err != nil {
		t.Fatal(err)
	}
	if err := workspaceDriver.Release(ctx, handle.WorkspaceID); err != nil {
		t.Fatal(err)
	}
}

func flattenedErrors(root error) []error {
	if root == nil {
		return nil
	}
	result := []error{root}
	switch value := root.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range value.Unwrap() {
			result = append(result, flattenedErrors(child)...)
		}
	case interface{ Unwrap() error }:
		result = append(result, flattenedErrors(value.Unwrap())...)
	}
	return result
}

func splitPinnedAgentImage(value string) (string, domain.Digest, bool) {
	separator := strings.LastIndex(value, "@sha256:")
	if separator <= 0 {
		return "", domain.Digest{}, false
	}
	digest, err := domain.ParseDigest(value[separator+1:])
	if err != nil {
		return "", domain.Digest{}, false
	}
	return value[:separator], digest, true
}

func receiveAgentExec(ctx context.Context, session ports.ExecTransport) ([]byte, transport.Terminal, error) {
	var stdout bytes.Buffer
	for {
		frame, err := session.Receive(ctx)
		if err != nil {
			return stdout.Bytes(), transport.Terminal{}, err
		}
		switch frame.Kind {
		case transport.KindStdout:
			if _, err := stdout.Write(frame.Data); err != nil {
				return stdout.Bytes(), transport.Terminal{}, err
			}
		case transport.KindStderr, transport.KindProcess:
		case transport.KindTerminal:
			terminal, err := transport.DecodeJSON[transport.Terminal](frame)
			return stdout.Bytes(), terminal, err
		default:
			return stdout.Bytes(), transport.Terminal{}, io.ErrUnexpectedEOF
		}
	}
}

func requireSingleWorkspaceMount(t *testing.T, ctx context.Context, containerID, expectedSource string) {
	t.Helper()
	result, err := (command.OS{}).Run(ctx, command.Invocation{Program: "docker", Args: []string{"inspect", "--format", "{{json .Mounts}}", containerID}})
	if err != nil {
		t.Fatal(err)
	}
	var mounts []struct {
		Source      string
		Destination string
		RW          bool
	}
	if err := json.Unmarshal(bytes.TrimSpace(result.Stdout), &mounts); err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 1 || mounts[0].Destination != WorkspaceMount || !mounts[0].RW {
		t.Fatalf("container mounts = %#v", mounts)
	}
	if runtime.GOOS == "windows" {
		// Docker Desktop projects the Windows source through a Linux VM path;
		// exact host-string comparison is not meaningful there.
		return
	}
	want, err := filepath.Abs(expectedSource)
	if err != nil {
		t.Fatal(err)
	}
	got, err := filepath.Abs(mounts[0].Source)
	if err != nil || !dockercli.CanonicalHostBindSourceEqual(got, want) {
		t.Fatalf("workspace mount source = %q, want %q (resolve error %v)", mounts[0].Source, want, err)
	}
}
