package cuttlefish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const (
	defaultMaximumADBTransferBytes int64 = 1 << 30
	adbMetadataOutputLimit         int64 = 4 << 10
	deviceRunBase                        = "/data/local/tmp/world/runs"
	adbCleanupTimeout                    = 5 * time.Second
	deviceMaterialDirectory              = "material"
	deviceWritableDirectory              = "writable"
)

// DeviceFile is the verified identity of bytes stored on, or read from, the
// one Android device named by a Scope.
type DeviceFile struct {
	Digest domain.Digest
	Size   int64
}

type DeviceFileArea string

const (
	// DeviceFileMaterial is populated only while a run is prepared. This split
	// prevents ordinary PushFile/PullFile operations from addressing material;
	// it is namespacing, not a guest security boundary. A caller holding the
	// scoped arbitrary-service ADB endpoint can still mutate Android guest state.
	DeviceFileMaterial DeviceFileArea = "material"
	// DeviceFileWritable is the push/pull area exposed to run operations.
	DeviceFileWritable DeviceFileArea = "writable"
)

func (a DeviceFileArea) valid() bool {
	return a == DeviceFileMaterial || a == DeviceFileWritable
}

// DeviceFileWritePlan contains no host or device path. LogicalPath is always
// resolved beneath a run-specific device directory derived from Scope.
type DeviceFileWritePlan struct {
	Area           DeviceFileArea
	LogicalPath    string
	Mode           uint32
	MaximumBytes   int64
	ExpectedDigest domain.Digest
	ExpectedSize   int64
}

func (p DeviceFileWritePlan) validate(maximum int64) (string, error) {
	normalized, err := normalizeDeviceLogicalPath(p.LogicalPath)
	if err != nil {
		return "", domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb_file_plan.validate", "logical_path", "must be a safe run-relative path", err)
	}
	if !p.Area.valid() {
		return "", domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb_file_plan.validate", "area", "must select the material or writable run area", nil)
	}
	if p.Mode == 0 || p.Mode&^uint32(0o777) != 0 {
		return "", domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb_file_plan.validate", "mode", "must contain non-zero permission bits only", nil)
	}
	if p.MaximumBytes < 0 || p.MaximumBytes > maximum {
		return "", domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb_file_plan.validate", "maximum_bytes", "is negative or exceeds the configured ADB transfer limit", nil)
	}
	if p.ExpectedSize < -1 || (p.ExpectedDigest.IsZero() && p.ExpectedSize >= 0) {
		return "", domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb_file_plan.validate", "expected_content", "size requires a digest and must be non-negative or unspecified", nil)
	}
	if p.ExpectedSize > p.MaximumBytes {
		return "", domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb_file_plan.validate", "expected_size", "exceeds the transfer limit", nil)
	}
	return normalized, nil
}

// ScopedFileGateway is the narrow privileged boundary used for Android file
// projection. Implementations must reject scope/allocation serial mismatches
// and must never execute host-global ADB services.
type ScopedFileGateway interface {
	PrepareRun(context.Context, deviceproxy.Scope, Allocation) error
	Put(context.Context, deviceproxy.Scope, Allocation, DeviceFileWritePlan, io.Reader) (DeviceFile, error)
	Get(context.Context, deviceproxy.Scope, Allocation, string, int64) (ports.ContentReader, error)
	RemoveRun(context.Context, deviceproxy.Scope, Allocation) error
}

type CommandFileGatewayConfig struct {
	Runner               command.Runner
	ADBBinary            string
	StagingRoot          string
	MaximumTransferBytes int64
}

// CommandFileGateway invokes ADB without a host shell. Every invocation starts
// with an exact -s serial selector, and every device path is derived internally
// from the authorized run scope.
type CommandFileGateway struct {
	runner      command.Runner
	adbBinary   string
	stagingRoot string
	maximum     int64

	mu sync.Mutex
}

func NewCommandFileGateway(config CommandFileGatewayConfig) (*CommandFileGateway, error) {
	if config.Runner == nil {
		config.Runner = command.OS{}
	}
	if config.ADBBinary == "" {
		config.ADBBinary = "adb"
	}
	if config.StagingRoot == "" {
		return nil, fmt.Errorf("ADB staging root is required")
	}
	if config.MaximumTransferBytes == 0 {
		config.MaximumTransferBytes = defaultMaximumADBTransferBytes
	}
	if config.MaximumTransferBytes < 0 || config.MaximumTransferBytes == math.MaxInt64 {
		return nil, fmt.Errorf("maximum ADB transfer bytes must leave room for an overflow sentinel byte")
	}
	return &CommandFileGateway{runner: config.Runner, adbBinary: config.ADBBinary, stagingRoot: config.StagingRoot, maximum: config.MaximumTransferBytes}, nil
}

func (g *CommandFileGateway) PrepareRun(ctx context.Context, scope deviceproxy.Scope, allocation Allocation) error {
	if err := requireExactDeviceScope(scope, allocation); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	root := scopedDeviceRunRoot(scope)
	if err := g.ensureConfinedDirectoryLocked(ctx, allocation, path.Dir(root)); err != nil {
		return err
	}
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "rm", "-rf", "--", root); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.prepare_run", "remove", "could not clear the scoped device run directory", err)
	}
	if err := g.ensureConfinedDirectoryLocked(ctx, allocation, scopedDeviceAreaRoot(scope, DeviceFileMaterial)); err != nil {
		return err
	}
	if err := g.ensureConfinedDirectoryLocked(ctx, allocation, scopedDeviceAreaRoot(scope, DeviceFileWritable)); err != nil {
		return err
	}
	return nil
}

func (g *CommandFileGateway) Put(ctx context.Context, scope deviceproxy.Scope, allocation Allocation, plan DeviceFileWritePlan, reader io.Reader) (DeviceFile, error) {
	if err := requireExactDeviceScope(scope, allocation); err != nil {
		return DeviceFile{}, err
	}
	normalized, err := plan.validate(g.maximum)
	if err != nil {
		return DeviceFile{}, err
	}
	if reader == nil {
		return DeviceFile{}, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb.put", "reader", "is required", nil)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.putLocked(ctx, scope, allocation, normalized, plan, reader)
}

func (g *CommandFileGateway) putLocked(ctx context.Context, scope deviceproxy.Scope, allocation Allocation, normalized string, plan DeviceFileWritePlan, reader io.Reader) (DeviceFile, error) {
	if err := os.MkdirAll(g.stagingRoot, 0o700); err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "staging_root", "could not create private ADB staging root", err)
	}
	temporary, err := os.CreateTemp(g.stagingRoot, ".world-adb-upload-*")
	if err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "staging_file", "could not create private ADB staging file", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return DeviceFile{}, err
	}
	hash := sha256.New()
	written, err := safepath.CopyBounded(io.MultiWriter(temporary, hash), &adbContextReader{ctx: ctx, reader: reader}, plan.MaximumBytes)
	if err != nil {
		if errors.Is(err, safepath.ErrTooLarge) {
			return DeviceFile{}, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb.put", "maximum_bytes", "input exceeded the bounded ADB transfer", err)
		}
		if ctx.Err() != nil {
			return DeviceFile{}, domain.NewError(domain.CodeDeadlineExceeded, "cuttlefish.adb.put", "content", "bounded input read was cancelled", err)
		}
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "content", "bounded input could not be read", err)
	}
	digest, err := digestFromHash(hash.Sum(nil))
	if err != nil {
		return DeviceFile{}, err
	}
	local := DeviceFile{Digest: digest, Size: written}
	if err := requireExpectedDeviceFile(local, plan.ExpectedDigest, plan.ExpectedSize); err != nil {
		return DeviceFile{}, err
	}
	if err := temporary.Sync(); err != nil {
		return DeviceFile{}, err
	}
	if err := temporary.Close(); err != nil {
		return DeviceFile{}, err
	}

	destination := scopedDevicePath(scope, plan.Area, normalized)
	remoteTemporary := destination + ".world-upload"
	if err := g.ensureConfinedDirectoryLocked(ctx, allocation, path.Dir(destination)); err != nil {
		return DeviceFile{}, err
	}
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "rm", "-f", "--", remoteTemporary); err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "temporary", "could not clear the scoped temporary path", err)
	}
	committed := false
	publishedPath := false
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.Background(), adbCleanupTimeout)
			defer cancel()
			_, _ = g.run(cleanup, allocation.Serial, adbMetadataOutputLimit, "shell", "rm", "-f", "--", remoteTemporary)
			if publishedPath {
				_, _ = g.run(cleanup, allocation.Serial, adbMetadataOutputLimit, "shell", "rm", "-f", "--", destination)
			}
		}
	}()
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "push", temporaryName, remoteTemporary); err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "push", "exact-serial ADB push failed", err)
	}
	if err := g.requireConfinedPathLocked(ctx, allocation, remoteTemporary); err != nil {
		return DeviceFile{}, err
	}
	remote, err := g.inspectLocked(ctx, allocation, remoteTemporary, plan.MaximumBytes)
	if err != nil {
		return DeviceFile{}, err
	}
	if remote != local {
		return DeviceFile{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.put", "remote_content", "device bytes do not match the verified input bytes", nil)
	}
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "chmod", fmt.Sprintf("%04o", plan.Mode), "--", remoteTemporary); err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "chmod", "could not apply the authorized device mode", err)
	}
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "mv", "-f", "--", remoteTemporary, destination); err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.put", "commit", "could not atomically publish scoped device bytes", err)
	}
	publishedPath = true
	if err := g.requireConfinedPathLocked(ctx, allocation, destination); err != nil {
		return DeviceFile{}, err
	}
	published, err := g.inspectLocked(ctx, allocation, destination, plan.MaximumBytes)
	if err != nil {
		return DeviceFile{}, err
	}
	if published != local {
		return DeviceFile{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.put", "published_content", "published device bytes changed during commit", nil)
	}
	if err := g.requireModeLocked(ctx, allocation, destination, plan.Mode); err != nil {
		return DeviceFile{}, err
	}
	committed = true
	return published, nil
}

func (g *CommandFileGateway) Get(ctx context.Context, scope deviceproxy.Scope, allocation Allocation, logicalPath string, maximumBytes int64) (ports.ContentReader, error) {
	if err := requireExactDeviceScope(scope, allocation); err != nil {
		return nil, err
	}
	normalized, err := normalizeDeviceLogicalPath(logicalPath)
	if err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb.get", "logical_path", "must be a safe run-relative path", err)
	}
	if maximumBytes <= 0 || maximumBytes > g.maximum {
		return nil, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb.get", "maximum_bytes", "must be positive and within the configured ADB transfer limit", nil)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	devicePath := scopedDevicePath(scope, DeviceFileWritable, normalized)
	if err := g.requireConfinedPathLocked(ctx, allocation, devicePath); err != nil {
		return nil, err
	}
	expected, err := g.inspectLocked(ctx, allocation, devicePath, maximumBytes)
	if err != nil {
		return nil, err
	}
	// Ask the device for at most one byte beyond the limit. This bounds the ADB
	// stream even if an untrusted process replaces or grows the file after stat.
	result, err := g.run(ctx, allocation.Serial, maximumBytes+1, "exec-out", "head", "-c", strconv.FormatInt(maximumBytes+1, 10), "--", devicePath)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.get", "pull", "bounded exact-serial ADB read failed", err)
	}
	if int64(len(result.Stdout)) > maximumBytes {
		return nil, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb.get", "maximum_bytes", "device file grew beyond the bounded transfer", safepath.ErrTooLarge)
	}
	actual := DeviceFile{Digest: domain.NewDigest(result.Stdout), Size: int64(len(result.Stdout))}
	if actual != expected {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.get", "content", "device content changed or was corrupted while reading", nil)
	}
	return &verifiedADBContent{reader: bytes.NewReader(result.Stdout), digest: actual.Digest, size: actual.Size}, nil
}

func (g *CommandFileGateway) RemoveRun(ctx context.Context, scope deviceproxy.Scope, allocation Allocation) error {
	if err := requireExactDeviceScope(scope, allocation); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.requireConfinedPathLocked(ctx, allocation, scopedDeviceRunRoot(scope)); err != nil {
		return err
	}
	if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "rm", "-rf", "--", scopedDeviceRunRoot(scope)); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.remove_run", "remove", "could not remove the scoped device run directory", err)
	}
	return nil
}

func (g *CommandFileGateway) inspectLocked(ctx context.Context, allocation Allocation, devicePath string, maximumBytes int64) (DeviceFile, error) {
	stat, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "stat", "-c", "%s", "--", devicePath)
	if err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.inspect", "stat", "could not read exact-serial device file size", err)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(stat.Stdout)), 10, 64)
	if err != nil || size < 0 {
		return DeviceFile{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.inspect", "size", "device returned an invalid file size", err)
	}
	if size > maximumBytes {
		return DeviceFile{}, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.adb.inspect", "maximum_bytes", "device file exceeds the bounded transfer", safepath.ErrTooLarge)
	}
	hashed, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "sha256sum", "--", devicePath)
	if err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.inspect", "digest", "could not read exact-serial device file digest", err)
	}
	fields := strings.Fields(string(hashed.Stdout))
	if len(fields) < 1 || len(fields[0]) != sha256.Size*2 {
		return DeviceFile{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.inspect", "digest", "device returned an invalid SHA-256 digest", nil)
	}
	digest, err := domain.ParseDigest("sha256:" + strings.ToLower(fields[0]))
	if err != nil {
		return DeviceFile{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.inspect", "digest", "device returned an invalid SHA-256 digest", err)
	}
	return DeviceFile{Digest: digest, Size: size}, nil
}

func (g *CommandFileGateway) requireModeLocked(ctx context.Context, allocation Allocation, devicePath string, expected uint32) error {
	result, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "stat", "-c", "%a", "--", devicePath)
	if err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.inspect", "mode", "could not read exact-serial device file mode", err)
	}
	mode, err := strconv.ParseUint(strings.TrimSpace(string(result.Stdout)), 8, 32)
	if err != nil || uint32(mode) != expected {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.inspect", "mode", "device file does not have the authorized permission mode", err)
	}
	return nil
}

func (g *CommandFileGateway) ensureConfinedDirectoryLocked(ctx context.Context, allocation Allocation, directory string) error {
	if directory != deviceRunBase && !strings.HasPrefix(directory, deviceRunBase+"/") {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.confine", "directory", "device directory is outside the managed run root", nil)
	}
	components := strings.Split(strings.TrimPrefix(directory, "/"), "/")
	current := ""
	for _, component := range components {
		current += "/" + component
		if current == "/data" {
			if err := g.requireConfinedPathLocked(ctx, allocation, current); err != nil {
				return err
			}
			continue
		}
		// Resolve the parent before mkdir so an existing symlink cannot make
		// directory creation mutate a location outside the managed hierarchy.
		if err := g.requireConfinedPathLocked(ctx, allocation, path.Dir(current)); err != nil {
			return err
		}
		if _, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "mkdir", "-p", "--", current); err != nil {
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.confine", "mkdir", "could not create a managed device directory component", err)
		}
		if err := g.requireConfinedPathLocked(ctx, allocation, current); err != nil {
			return err
		}
	}
	return nil
}

func (g *CommandFileGateway) requireConfinedPathLocked(ctx context.Context, allocation Allocation, expected string) error {
	result, err := g.run(ctx, allocation.Serial, adbMetadataOutputLimit, "shell", "realpath", "--", expected)
	if err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.adb.confine", "realpath", "could not resolve a managed device path", err)
	}
	resolved := strings.TrimSpace(string(result.Stdout))
	if resolved != expected {
		return domain.NewDetailedError(domain.CodeIntegrityViolation, "cuttlefish.adb.confine", "path", "device path traverses a symlink or escaped its managed root", map[string]string{"expected": expected, "resolved": resolved}, nil)
	}
	return nil
}

func (g *CommandFileGateway) run(ctx context.Context, serial string, maximumOutput int64, args ...string) (command.Result, error) {
	return runExactSerialADB(ctx, g.runner, g.adbBinary, serial, maximumOutput, args...)
}

func requireExactDeviceScope(scope deviceproxy.Scope, allocation Allocation) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := allocation.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, "cuttlefish.adb.scope", "allocation", "is invalid", err)
	}
	if scope.Serial != allocation.Serial {
		return domain.NewError(domain.CodeForbidden, "cuttlefish.adb.scope", "serial", "scope cannot select a different Android device", nil)
	}
	return nil
}

func scopedDeviceRunRoot(scope deviceproxy.Scope) string {
	return path.Join(deviceRunBase, scope.TargetID.String(), "g"+strconv.FormatUint(uint64(scope.Generation), 10), scope.RunID.String())
}

func scopedDeviceAreaRoot(scope deviceproxy.Scope, area DeviceFileArea) string {
	return path.Join(scopedDeviceRunRoot(scope), string(area))
}

func scopedDevicePath(scope deviceproxy.Scope, area DeviceFileArea, normalized string) string {
	return path.Join(scopedDeviceAreaRoot(scope, area), normalized)
}

func normalizeDeviceLogicalPath(logicalPath string) (string, error) {
	normalized, err := safepath.Normalize(logicalPath)
	if err != nil {
		return "", err
	}
	for _, character := range normalized {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '/', '.', '_', '-':
			continue
		default:
			return "", fmt.Errorf("logical path contains a remote-shell delimiter or non-ASCII character")
		}
	}
	return normalized, nil
}

func requireExpectedDeviceFile(actual DeviceFile, digest domain.Digest, size int64) error {
	if digest.IsZero() {
		return nil
	}
	if actual.Digest != digest || (size >= 0 && actual.Size != size) {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.adb.verify", "content", "bytes do not match the declared digest and size", nil)
	}
	return nil
}

func digestFromHash(sum []byte) (domain.Digest, error) {
	return domain.ParseDigest("sha256:" + hex.EncodeToString(sum))
}

type adbContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *adbContextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

type verifiedADBContent struct {
	reader *bytes.Reader
	digest domain.Digest
	size   int64
	closed bool
	mu     sync.Mutex
}

func (r *verifiedADBContent) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	return r.reader.Read(buffer)
}

func (r *verifiedADBContent) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *verifiedADBContent) Digest() domain.Digest { return r.digest }
func (r *verifiedADBContent) Size() int64           { return r.size }

var _ ScopedFileGateway = (*CommandFileGateway)(nil)
var _ ports.ContentReader = (*verifiedADBContent)(nil)
