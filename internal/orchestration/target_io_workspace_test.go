package orchestration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkspaceBackedTargetPushAndPull(t *testing.T) {
	fixture := newIntegrationFixture(t)
	target, run := fixture.readyTargetAndRun()
	physical := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, nil)
	preparePhysicalTarget(t, fixture, physical, target, run)
	shared := &sharedTargetFiles{files: make(map[string][]byte), modes: make(map[string]uint32)}
	driver := &workspaceTransferTargetDriver{TargetDriver: physical, shared: shared}

	workspaceRoot := t.TempDir()
	workspaceID, err := currentWorkspaceID(fixture.view)
	if err != nil {
		t.Fatal(err)
	}
	workspaceScope, err := NewCoreWorkspaceResolver(fixture.core)
	if err != nil {
		t.Fatal(err)
	}
	service := fixture.service(Config{
		Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver},
		Workspace: &inspectOnlyWorkspace{handle: ports.WorkspaceHandle{
			WorkspaceID: workspaceID, State: domain.WorkspaceMounted, MergedPath: workspaceRoot, ObservedAt: fixture.clock.Now(),
		}},
		WorkspaceScope:   workspaceScope,
		MaxTransferBytes: 1 << 20,
	})

	pushBytes := []byte("workspace-owned tool\x00bytes")
	if err := os.MkdirAll(filepath.Join(workspaceRoot, "tools"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "tools", "probe.bin"), pushBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	push := &recordingFileTransferStream{ctx: context.Background(), received: []*worldv1.FileTransferFrame{{Start: &worldv1.FileTransferStart{
		Mutation: fixture.wireMeta("workspace-push"), TargetId: target.ID, TargetRunId: run.ID,
		TargetRelativePath: "opt/probe.bin", WorkspaceRelativePath: "tools/probe.bin", Mode: 0o750,
	}}}}
	if err := service.PushTargetFile(push); err != nil {
		t.Fatal(err)
	}
	if got, mode := shared.file("opt/probe.bin"); string(got) != string(pushBytes) || mode != 0o750 {
		t.Fatalf("pushed target file = %q mode %#o", got, mode)
	}
	if len(push.sent) != 1 || !push.sent[0].Complete || push.sent[0].Operation == nil || push.sent[0].Digest != domain.NewDigest(pushBytes).String() {
		t.Fatalf("push completion = %#v", push.sent)
	}

	pullBytes := []byte("target-produced evidence\n")
	shared.put("results/evidence.bin", pullBytes, 0o400)
	pull := &recordingFileTransferStream{ctx: context.Background()}
	if err := service.PullTargetFile(&worldv1.PullTargetFileRequest{
		Mutation: fixture.wireMeta("workspace-pull"), TargetId: target.ID, TargetRunId: run.ID,
		TargetRelativePath: "results/evidence.bin", WorkspaceRelativePath: "derived/evidence.bin",
	}, pull); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(workspaceRoot, "derived", "evidence.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(pullBytes) {
		t.Fatalf("workspace pull bytes = %q", written)
	}
	if len(pull.sent) != 1 || len(pull.sent[0].Data) != 0 || !pull.sent[0].Complete || pull.sent[0].Operation == nil || pull.sent[0].Digest != domain.NewDigest(pullBytes).String() {
		t.Fatalf("pull completion = %#v", pull.sent)
	}
}

func TestFileTransferEndRejectsTrailingFrames(t *testing.T) {
	closed := &recordingFileTransferStream{ctx: context.Background()}
	if err := requireFileTransferEnd(closed); err != nil {
		t.Fatalf("closed transfer error = %v", err)
	}
	trailing := &recordingFileTransferStream{ctx: context.Background(), received: []*worldv1.FileTransferFrame{{Data: []byte("trailing")}}}
	if code := status.Code(requireFileTransferEnd(trailing)); code != codes.InvalidArgument {
		t.Fatalf("trailing transfer code = %s, want %s", code, codes.InvalidArgument)
	}
}

func currentWorkspaceID(view application.ResearchSessionView) (domain.WorkspaceID, error) {
	for _, generation := range view.Agent.Generations {
		if generation.Generation == view.Agent.CurrentGeneration {
			return domain.ParseWorkspaceID(generation.WorkspaceID)
		}
	}
	return domain.WorkspaceID{}, fmt.Errorf("current agent generation is missing")
}

type inspectOnlyWorkspace struct {
	ports.WorkspaceDriver
	handle ports.WorkspaceHandle
}

func (d *inspectOnlyWorkspace) Inspect(context.Context, domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	return d.handle, nil
}

type workspaceTransferTargetDriver struct {
	ports.TargetDriver
	shared *sharedTargetFiles
}

func (d *workspaceTransferTargetDriver) OpenTransport(context.Context, domain.TargetRunID) (ports.TargetTransport, error) {
	return &sharedTargetTransport{shared: d.shared}, nil
}

type sharedTargetFiles struct {
	mu    sync.Mutex
	files map[string][]byte
	modes map[string]uint32
}

func (s *sharedTargetFiles) put(path string, content []byte, mode uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = append([]byte(nil), content...)
	s.modes[path] = mode
}

func (s *sharedTargetFiles) file(path string) ([]byte, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.files[path]...), s.modes[path]
}

type sharedTargetTransport struct{ shared *sharedTargetFiles }

func (t *sharedTargetTransport) OpenExec(context.Context, ports.TargetExecPlan) (ports.ExecTransport, error) {
	return nil, fmt.Errorf("exec is not used by this test")
}

func (t *sharedTargetTransport) PushFile(_ context.Context, plan ports.TargetTransferPlan, reader io.Reader) (ports.TransferResult, error) {
	content, err := io.ReadAll(io.LimitReader(reader, plan.MaximumBytes+1))
	if err != nil {
		return ports.TransferResult{}, err
	}
	if int64(len(content)) > plan.MaximumBytes {
		return ports.TransferResult{}, fmt.Errorf("transfer exceeded maximum")
	}
	t.shared.put(plan.RelativePath, content, plan.Mode)
	return ports.TransferResult{OperationID: plan.Operation.ID(), Digest: domain.NewDigest(content), Bytes: int64(len(content))}, nil
}

func (t *sharedTargetTransport) PullFile(_ context.Context, plan ports.TargetTransferPlan) (io.ReadCloser, error) {
	content, _ := t.shared.file(plan.RelativePath)
	if content == nil {
		return nil, fmt.Errorf("target file not found")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (*sharedTargetTransport) OpenADB(context.Context) (ports.ScopedADBEndpoint, error) {
	return nil, fmt.Errorf("ADB is not used by this test")
}

func (*sharedTargetTransport) Close() error { return nil }

type recordingFileTransferStream struct {
	grpc.ServerStream
	ctx      context.Context
	received []*worldv1.FileTransferFrame
	sent     []*worldv1.FileTransferFrame
	index    int
}

func (s *recordingFileTransferStream) Context() context.Context { return s.ctx }
func (s *recordingFileTransferStream) Recv() (*worldv1.FileTransferFrame, error) {
	if s.index >= len(s.received) {
		return nil, io.EOF
	}
	frame := s.received[s.index]
	s.index++
	return frame, nil
}
func (s *recordingFileTransferStream) Send(frame *worldv1.FileTransferFrame) error {
	s.sent = append(s.sent, frame)
	return nil
}
