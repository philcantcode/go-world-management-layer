package linuxcontainer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// TestDockerMaterializationRemainsVisibleAcrossBoundRoot is opt-in because it
// needs a local Docker Engine and prebuilt scratch target image. It creates the
// container before publishing material, reproducing the bind-mount ordering
// that originally exposed root-inode replacement.
func TestDockerMaterializationRemainsVisibleAcrossBoundRoot(t *testing.T) {
	image := os.Getenv("WORLD_LINUX_TARGET_E2E_IMAGE")
	if image == "" {
		t.Skip("WORLD_LINUX_TARGET_E2E_IMAGE is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := filepath.Abs(writableTempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	lease, _ := domain.NewLeaseID()
	targetID, _ := domain.NewTargetID()
	policy := domain.NewDigest([]byte("docker-e2e-policy"))
	capability := domain.NewDigest([]byte("docker-e2e-capability"))
	plan := ContainerPlan{
		Name:             targetContainerName(targetID, 1),
		LeaseID:          lease,
		TargetID:         targetID,
		Generation:       1,
		Image:            image,
		Runtime:          dockercli.RuncRuntime,
		TargetDirectory:  filepath.Join(root, targetID.String(), "generations", "1"),
		PolicyDigest:     policy,
		CapabilityDigest: capability,
		Resources:        admission.Resources{CPUMilli: 250, MemoryBytes: 64 << 20, PIDs: 64},
		User:             defaultTargetUser,
		ReadOnlyRoot:     true,
		NoNewPrivileges:  true,
		SeccompProfile:   dockercli.RuntimeDefaultSeccompProfile,
		Labels: map[string]string{
			"world.lease":             lease.String(),
			"world.target":            targetID.String(),
			"world.target-generation": "1",
			"world.policy-digest":     policy.String(),
			"world.capability-digest": capability.String(),
		},
	}
	plan.MountSources = []string{plan.writableRoot(), plan.materialRoot()}
	if err := prepareTargetDirectories(root, plan); err != nil {
		t.Fatal(err)
	}
	boundRoot, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewDockerRuntime("docker", nil, nil)
	runtimeID, err := runtime.Create(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = runtime.Remove(cleanup, runtimeID)
	})
	if err := runtime.Start(ctx, runtimeID); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.Inspect(ctx, runtimeID)
	if err != nil || !state.Running {
		t.Fatalf("target state = %#v, %v", state, err)
	}
	if err := validateRuntimeIdentity(state, plan); err != nil {
		t.Fatal(err)
	}

	first := []byte("first material published after container creation")
	materializeDockerFixture(t, ctx, root, plan, "first.bin", first)
	requireSameBoundRoot(t, boundRoot, plan.materialRoot())
	requireDockerCopy(t, ctx, runtimeID, "/target/input/first.bin", filepath.Join(root, "copied-first.bin"), first)

	second := []byte("replacement exact projection on the same bound inode")
	materializeDockerFixture(t, ctx, root, plan, "second.bin", second)
	requireSameBoundRoot(t, boundRoot, plan.materialRoot())
	requireDockerCopy(t, ctx, runtimeID, "/target/input/second.bin", filepath.Join(root, "copied-second.bin"), second)
	if err := dockerCopy(ctx, runtimeID, "/target/input/first.bin", filepath.Join(root, "stale-first.bin")); err == nil {
		t.Fatal("second exact projection left the first material visible in the running container")
	}
}

func materializeDockerFixture(t *testing.T, ctx context.Context, root string, plan ContainerPlan, logicalPath string, content []byte) {
	t.Helper()
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference:   "artifact://docker-e2e/" + logicalPath,
		Digest:      domain.NewDigest(content),
		Size:        int64(len(content)),
		Role:        "target-input",
		Sensitivity: domain.SensitivityInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	material := ports.TargetMaterialPlan{Artifact: artifact, LogicalPath: logicalPath, Mode: 0o444, Content: memorySource{content: content, digest: domain.NewDigest(content)}}
	if err := materializeTarget(ctx, root, plan, []ports.TargetMaterialPlan{material}); err != nil {
		t.Fatal(err)
	}
}

func requireSameBoundRoot(t *testing.T, before os.FileInfo, root string) {
	t.Helper()
	after, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("materialization replaced the container's bind-mounted root")
	}
}

func requireDockerCopy(t *testing.T, ctx context.Context, runtimeID, source, destination string, expected []byte) {
	t.Helper()
	if err := dockerCopy(ctx, runtimeID, source, destination); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(content, expected) {
		t.Fatalf("container material = %q, %v; want %q", content, err, expected)
	}
}

func dockerCopy(ctx context.Context, runtimeID, source, destination string) error {
	_, err := (command.OS{}).Run(ctx, command.Invocation{Program: "docker", Args: []string{"cp", fmt.Sprintf("%s:%s", runtimeID, source), destination}})
	return err
}
