package cuttlefish

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

func TestAndroidTransportAuthorizesOperationsAndUsesScopedGateways(t *testing.T) {
	scope, allocation := adbTestScope(t)
	files := newRecordingFileGateway(allocation.Serial)
	endpoint := &recordingEndpointGateway{expectedScope: scope, expectedAllocation: allocation}
	transport := &androidTransport{gateway: endpoint, files: files, scope: scope, allocation: allocation}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	content := []byte("authorized push")
	digest := domain.NewDigest(content)
	push := androidTransferPlan(t, scope, domain.TargetOperationPush, "results/file.bin", 64, digest)
	result, err := transport.PushFile(ctx, push, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if result.OperationID != push.Operation.ID() || result.Digest != digest || result.Bytes != int64(len(content)) {
		t.Fatalf("push result = %#v", result)
	}
	pull := androidTransferPlan(t, scope, domain.TargetOperationPull, "results/file.bin", 64, digest)
	reader, err := transport.PullFile(ctx, pull)
	if err != nil {
		t.Fatal(err)
	}
	verified, ok := reader.(ports.ContentReader)
	if !ok || verified.Digest() != digest || verified.Size() != int64(len(content)) {
		t.Fatalf("pull did not preserve verified identity: %#v", reader)
	}
	pulled, _ := io.ReadAll(reader)
	_ = reader.Close()
	if !bytes.Equal(pulled, content) {
		t.Fatalf("pulled = %q", pulled)
	}
	files.mu.Lock()
	files.corruptRead = true
	files.mu.Unlock()
	if _, err := transport.PullFile(ctx, pull); err == nil {
		t.Fatal("pull whose verified digest differs from the operation was accepted")
	}
	files.mu.Lock()
	files.corruptRead = false
	files.mu.Unlock()
	adb, err := transport.OpenADB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if adb.Serial() != allocation.Serial || adb.Address() != "127.0.0.1:19000" {
		t.Fatalf("endpoint = %s/%s", adb.Serial(), adb.Address())
	}

	otherRun, _ := domain.NewTargetRunID()
	wrongScope := scope
	wrongScope.RunID = otherRun
	wrong := androidTransferPlan(t, wrongScope, domain.TargetOperationPush, "wrong", 64, domain.Digest{})
	before := files.PutCount()
	if _, err := transport.PushFile(ctx, wrong, bytes.NewReader(nil)); err == nil {
		t.Fatal("operation from another run was accepted")
	}
	if files.PutCount() != before {
		t.Fatal("file gateway invoked before operation authorization")
	}
	traversal := push
	traversal.RelativePath = "../escape"
	if _, err := transport.PushFile(ctx, traversal, bytes.NewReader(nil)); err == nil {
		t.Fatal("traversal was accepted")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if !endpoint.Closed() {
		t.Fatal("scoped endpoint was not closed with transport")
	}
	if _, err := transport.PushFile(ctx, push, bytes.NewReader(content)); err == nil {
		t.Fatal("closed transport accepted a push")
	}
}

func androidTransferPlan(t *testing.T, scope deviceproxy.Scope, kind domain.TargetOperationKind, logicalPath string, maximum int64, digest domain.Digest) ports.TargetTransferPlan {
	t.Helper()
	id, _ := domain.NewTargetOperationID()
	operation, err := domain.NewTargetOperation(domain.TargetOperationSpec{
		ID: id, LeaseID: scope.LeaseID, TargetID: scope.TargetID, TargetGeneration: scope.Generation, TargetRunID: scope.RunID,
		Kind: kind, CommandDisplay: string(kind) + " " + logicalPath, ContentDigest: digest, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(0)
	if kind == domain.TargetOperationPush {
		mode = 0o600
	}
	return ports.TargetTransferPlan{Operation: operation, RelativePath: logicalPath, Mode: mode, MaximumBytes: maximum}
}

type recordingEndpointGateway struct {
	mu                 sync.Mutex
	expectedScope      deviceproxy.Scope
	expectedAllocation Allocation
	closed             bool
}

func (g *recordingEndpointGateway) Open(_ context.Context, scope deviceproxy.Scope, allocation Allocation) (ports.ScopedADBEndpoint, error) {
	if scope != g.expectedScope || allocation != g.expectedAllocation {
		return nil, fmt.Errorf("endpoint scope changed")
	}
	return deviceproxy.NewEndpoint(allocation.Serial, "127.0.0.1:19000", func() error {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.closed = true
		return nil
	}), nil
}

func (g *recordingEndpointGateway) Closed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

type recordingFileGateway struct {
	serial string

	mu          sync.Mutex
	files       map[string][]byte
	putPlans    []DeviceFileWritePlan
	putScopes   []deviceproxy.Scope
	prepared    int
	removed     int
	corruptRead bool
}

func newRecordingFileGateway(serial string) *recordingFileGateway {
	return &recordingFileGateway{serial: serial, files: make(map[string][]byte)}
}

func (g *recordingFileGateway) PrepareRun(_ context.Context, scope deviceproxy.Scope, allocation Allocation) error {
	if err := g.authorize(scope, allocation); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.files = make(map[string][]byte)
	g.prepared++
	return nil
}

func (g *recordingFileGateway) Put(_ context.Context, scope deviceproxy.Scope, allocation Allocation, plan DeviceFileWritePlan, reader io.Reader) (DeviceFile, error) {
	if err := g.authorize(scope, allocation); err != nil {
		return DeviceFile{}, err
	}
	if _, err := plan.validate(1 << 30); err != nil {
		return DeviceFile{}, err
	}
	var buffer bytes.Buffer
	if _, err := safepath.CopyBounded(&buffer, reader, plan.MaximumBytes); err != nil {
		return DeviceFile{}, err
	}
	file := deviceFileForBytes(buffer.Bytes())
	if err := requireExpectedDeviceFile(file, plan.ExpectedDigest, plan.ExpectedSize); err != nil {
		return DeviceFile{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.files[deviceFileKey(plan.Area, plan.LogicalPath)] = append([]byte(nil), buffer.Bytes()...)
	g.putPlans = append(g.putPlans, plan)
	g.putScopes = append(g.putScopes, scope)
	return file, nil
}

func (g *recordingFileGateway) Get(_ context.Context, scope deviceproxy.Scope, allocation Allocation, logicalPath string, maximum int64) (ports.ContentReader, error) {
	if err := g.authorize(scope, allocation); err != nil {
		return nil, err
	}
	if _, err := safepath.Normalize(logicalPath); err != nil {
		return nil, err
	}
	g.mu.Lock()
	content, found := g.files[deviceFileKey(DeviceFileWritable, logicalPath)]
	corrupt := g.corruptRead
	g.mu.Unlock()
	if !found {
		return nil, osErrNotExist(logicalPath)
	}
	if int64(len(content)) > maximum {
		return nil, safepath.ErrTooLarge
	}
	if corrupt {
		content = append(append([]byte(nil), content...), '!')
	}
	file := deviceFileForBytes(content)
	return &verifiedADBContent{reader: bytes.NewReader(content), digest: file.Digest, size: file.Size}, nil
}

func (g *recordingFileGateway) RemoveRun(_ context.Context, scope deviceproxy.Scope, allocation Allocation) error {
	if err := g.authorize(scope, allocation); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.files = make(map[string][]byte)
	g.removed++
	return nil
}

func (g *recordingFileGateway) authorize(scope deviceproxy.Scope, allocation Allocation) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if allocation.Serial != g.serial || scope.Serial != g.serial {
		return fmt.Errorf("file scope selected another serial")
	}
	return nil
}

func (g *recordingFileGateway) PutCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.putPlans)
}

func deviceFileKey(area DeviceFileArea, logicalPath string) string {
	return string(area) + "/" + logicalPath
}

func deviceFileForBytes(content []byte) DeviceFile {
	return DeviceFile{Digest: domain.NewDigest(content), Size: int64(len(content))}
}

func osErrNotExist(path string) error { return fmt.Errorf("%s: file does not exist", path) }

var _ Gateway = (*recordingEndpointGateway)(nil)
var _ ScopedFileGateway = (*recordingFileGateway)(nil)
