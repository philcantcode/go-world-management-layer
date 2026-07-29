package linuxcontainer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestTargetTransportScopesOperationsAndCleansFailedPush(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 4, RunID: run}
	root := writableTempDir(t)
	transport := &targetTransport{runtime: noopRuntime{}, runtimeID: "runtime", root: root, authority: authority}
	push := transferPlan(t, authority, domain.TargetOperationPush, "nested/tool.bin", 32)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := transport.PushFile(ctx, push, bytes.NewReader([]byte("opaque bytes")))
	if err != nil {
		t.Fatal(err)
	}
	if result.Bytes != int64(len("opaque bytes")) || result.Digest.IsZero() {
		t.Fatalf("push result = %#v", result)
	}
	pull := transferPlan(t, authority, domain.TargetOperationPull, "nested/tool.bin", 32)
	reader, err := transport.PullFile(ctx, pull)
	if err != nil {
		t.Fatalf("%v: %v", err, errors.Unwrap(err))
	}
	value, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Equal(value, []byte("opaque bytes")) {
		t.Fatalf("pulled = %q", value)
	}
	traversal := push
	traversal.RelativePath = "../escape"
	if _, err := transport.PushFile(ctx, traversal, bytes.NewReader(nil)); err == nil {
		t.Fatal("path traversal accepted")
	}
	otherRun, _ := domain.NewTargetRunID()
	wrong := authority
	wrong.RunID = otherRun
	wrongScope := transferPlan(t, wrong, domain.TargetOperationPush, "wrong", 10)
	if _, err := transport.PushFile(ctx, wrongScope, bytes.NewReader(nil)); err == nil {
		t.Fatal("other run operation accepted")
	}
	overflow := transferPlan(t, authority, domain.TargetOperationPush, "overflow", 3)
	if _, err := transport.PushFile(ctx, overflow, bytes.NewReader([]byte("too large"))); err == nil {
		t.Fatal("oversize push accepted")
	}
	entries, err := temporaryWrites(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary push files leaked: %v", entries)
	}
}

func TestTargetTransportCloseWaitsForInFlightMutation(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 1, RunID: run}
	root := writableTempDir(t)
	scoped := &targetTransport{runtime: noopRuntime{}, runtimeID: "runtime", root: root, authority: authority}
	reader := &gatedTargetReader{entered: make(chan struct{}), release: make(chan struct{})}
	push := transferPlan(t, authority, domain.TargetOperationPush, "late.bin", 64)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pushDone := make(chan error, 1)
	go func() {
		_, err := scoped.PushFile(ctx, push, reader)
		pushDone <- err
	}()
	<-reader.entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- scoped.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the active mutation drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(reader.release)
	if err := <-pushDone; err == nil {
		t.Fatal("in-flight push succeeded after transport revocation")
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "late.bin")); !os.IsNotExist(err) {
		t.Fatalf("revoked in-flight push published target state: %v", err)
	}
}

type gatedTargetReader struct {
	entered chan struct{}
	release chan struct{}
	sent    bool
}

func (r *gatedTargetReader) Read(buffer []byte) (int, error) {
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	close(r.entered)
	<-r.release
	return copy(buffer, []byte("late mutation")), nil
}

func TestMaterializationPublishesOnlyVerifiedBytes(t *testing.T) {
	targetRoot := writableTempDir(t)
	plan := ContainerPlan{TargetDirectory: filepath.Join(targetRoot, "target", "generations", "1")}
	if err := prepareTargetDirectories(targetRoot, plan); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("verified specimen")
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "artifact://specimen", Digest: domain.NewDigest(content), Size: int64(len(content)),
		Role: "specimen", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		t.Fatal(err)
	}
	material := ports.TargetMaterialPlan{Artifact: artifact, LogicalPath: "nested/specimen.bin", Mode: 0o440, Content: memorySource{content: content, digest: domain.NewDigest(content)}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := materializeTarget(ctx, targetRoot, plan, []ports.TargetMaterialPlan{material}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("materialization replaced the bind-mounted root inode")
	}
	got, err := os.ReadFile(filepath.Join(plan.materialRoot(), "nested", "specimen.bin"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("materialized content = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(plan.writableRoot(), "nested", "specimen.bin")); !os.IsNotExist(err) {
		t.Fatalf("material leaked into writable target state: %v", err)
	}
	corrupt := material
	corrupt.Content = memorySource{content: []byte("corrupt specimen"), digest: domain.NewDigest(content), size: int64(len(content))}
	if err := materializeTarget(ctx, targetRoot, plan, []ports.TargetMaterialPlan{corrupt}); err == nil {
		t.Fatal("materialization accepted bytes that did not match the artifact digest")
	}
	afterFailure, err := os.Stat(plan.materialRoot())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, afterFailure) {
		t.Fatal("failed materialization replaced the bind-mounted root inode")
	}
	if _, err := os.Stat(filepath.Join(plan.materialRoot(), "nested", "specimen.bin")); !os.IsNotExist(err) {
		t.Fatalf("failed materialization left partial input: %v", err)
	}
}

func TestTargetTransportEnforcesContentDigestAndReturnsImmutablePullSnapshot(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 2, RunID: run}
	root := writableTempDir(t)
	transport := &targetTransport{runtime: noopRuntime{}, runtimeID: "runtime", root: root, authority: authority}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	original := []byte("original verified bytes")
	originalDigest := domain.NewDigest(original)
	push := transferPlanWithDigest(t, authority, domain.TargetOperationPush, "result.bin", 64, originalDigest)
	if _, err := transport.PushFile(ctx, push, bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}
	wrong := transferPlanWithDigest(t, authority, domain.TargetOperationPush, "result.bin", 64, domain.NewDigest([]byte("different")))
	if _, err := transport.PushFile(ctx, wrong, bytes.NewReader([]byte("replacement"))); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("wrong push digest error = %v", err)
	}
	unchanged, err := os.ReadFile(filepath.Join(root, "result.bin"))
	if err != nil || !bytes.Equal(unchanged, original) {
		t.Fatalf("failed push replaced verified destination: %q, %v", unchanged, err)
	}

	pull := transferPlanWithDigest(t, authority, domain.TargetOperationPull, "result.bin", 64, originalDigest)
	reader, err := transport.PullFile(ctx, pull)
	if err != nil {
		t.Fatal(err)
	}
	verified, ok := reader.(ports.ContentReader)
	if !ok || verified.Digest() != originalDigest || verified.Size() != int64(len(original)) {
		t.Fatalf("pull identity = %#v", reader)
	}
	if err := os.WriteFile(filepath.Join(root, "result.bin"), []byte("mutated after open"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(snapshot, original) {
		t.Fatalf("pull did not return its verified snapshot: %q, %v", snapshot, err)
	}
	badPull := transferPlanWithDigest(t, authority, domain.TargetOperationPull, "result.bin", 64, originalDigest)
	if _, err := transport.PullFile(ctx, badPull); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("wrong pull digest error = %v", err)
	}
}

func TestTargetTransportAppliesExactPushModeOnLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Unix mode fidelity is verified by the cross-compiled Linux suite")
	}
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	authority := RunAuthority{LeaseID: lease, TargetID: target, Generation: 1, RunID: run}
	root := writableTempDir(t)
	transport := &targetTransport{root: root, authority: authority}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := transferPlanWithDigest(t, authority, domain.TargetOperationPush, "mode.bin", 64, domain.NewDigest([]byte("mode")))
	plan.Mode = 0o550
	if _, err := transport.PushFile(ctx, plan, strings.NewReader("mode")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "mode.bin"))
	if err != nil || info.Mode().Perm() != 0o550 {
		t.Fatalf("pushed mode = %v, %v; want 0550", info.Mode().Perm(), err)
	}
	defaultPlan := transferPlanWithDigest(t, authority, domain.TargetOperationPush, "default-mode.bin", 64, domain.NewDigest([]byte("default")))
	defaultPlan.Mode = 0
	if _, err := transport.PushFile(ctx, defaultPlan, strings.NewReader("default")); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(filepath.Join(root, "default-mode.bin"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("default pushed mode = %v, %v; want 0600", info.Mode().Perm(), err)
	}
}

func transferPlan(t *testing.T, authority RunAuthority, kind domain.TargetOperationKind, path string, maximum int64) ports.TargetTransferPlan {
	return transferPlanWithDigest(t, authority, kind, path, maximum, domain.Digest{})
}

func transferPlanWithDigest(t *testing.T, authority RunAuthority, kind domain.TargetOperationKind, path string, maximum int64, digest domain.Digest) ports.TargetTransferPlan {
	t.Helper()
	operationID, _ := domain.NewTargetOperationID()
	operation, err := domain.NewTargetOperation(domain.TargetOperationSpec{ID: operationID, LeaseID: authority.LeaseID, TargetID: authority.TargetID, TargetGeneration: authority.Generation, TargetRunID: authority.RunID, Kind: kind, CommandDisplay: string(kind), ContentDigest: digest, CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(0)
	if kind == domain.TargetOperationPush {
		mode = 0o640
	}
	return ports.TargetTransferPlan{Operation: operation, RelativePath: path, Mode: mode, MaximumBytes: maximum}
}

func temporaryWrites(root string) ([]string, error) {
	var matches []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(entry.Name(), ".world-write-") {
			matches = append(matches, path)
		}
		return nil
	})
	return matches, err
}

func writableTempDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", "world-linux-target-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}

type noopRuntime struct{}

type memorySource struct {
	content []byte
	digest  domain.Digest
	size    int64
}

func (s memorySource) Digest() domain.Digest { return s.digest }
func (s memorySource) Size() int64 {
	if s.size != 0 {
		return s.size
	}
	return int64(len(s.content))
}
func (s memorySource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func (noopRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{}, nil
}
func (noopRuntime) Create(context.Context, ContainerPlan) (string, error) { return "", nil }
func (noopRuntime) Start(context.Context, string) error                   { return nil }
func (noopRuntime) Inspect(context.Context, string) (RuntimeState, error) { return RuntimeState{}, nil }
func (noopRuntime) Stop(context.Context, string, ports.StopMode) error    { return nil }
func (noopRuntime) Remove(context.Context, string) error                  { return nil }
func (noopRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	return successfulTargetReadinessTransport(), nil
}
