package process

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

var ErrCaptureLimit = errors.New("collector output exceeded its authorized byte limit")

const maximumLocalCaptureManifestBytes = int64(64 << 10)

const localCaptureManifestVersion = 2

type LocalOutputConfig struct {
	Root        string
	Sensitivity domain.Sensitivity
}

// LocalOutputFactory is a durable, collector-scoped output authority. Open
// syncs both empty stream files and their collector directory before exposing
// the transaction to Driver's process starter. Content objects are immutable
// by digest and the run manifest makes Finalize idempotent without trusting
// collector-produced paths or metadata.
type LocalOutputFactory struct {
	root        string
	sensitivity domain.Sensitivity
	syncFile    func(*os.File) error
	syncDir     func(string) error
	mu          sync.Mutex
	active      map[string]*localCapture
}

type localCapture struct {
	factory   *LocalOutputFactory
	plan      ports.CollectorPlan
	signature string
	directory string
	stdout    *boundedCaptureWriter
	stderr    *boundedCaptureWriter
	budget    *captureBudget

	mu        sync.Mutex
	finalized []domain.ArtifactReference
	finalErr  error
	aborted   bool
}

type captureBudget struct {
	mu        sync.Mutex
	remaining int64
	exceeded  bool
}

type boundedCaptureWriter struct {
	file   *os.File
	budget *captureBudget
	mu     sync.Mutex
	closed bool
}

type localCaptureManifest struct {
	Version   uint32                  `json:"version"`
	Signature string                  `json:"signature"`
	Exceeded  bool                    `json:"exceeded,omitempty"`
	Artifacts []localArtifactManifest `json:"artifacts"`
}

type localArtifactManifest struct {
	Reference   string `json:"reference"`
	Digest      string `json:"digest"`
	Size        int64  `json:"size"`
	Role        string `json:"role"`
	Sensitivity string `json:"sensitivity"`
}

func NewLocalOutputFactory(config LocalOutputConfig) (*LocalOutputFactory, error) {
	configuredRoot := strings.TrimSpace(config.Root)
	if configuredRoot == "" {
		return nil, fmt.Errorf("resolve observer output root: path is empty")
	}
	root, err := filepath.Abs(configuredRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve observer output root: %w", err)
	}
	if config.Sensitivity == "" {
		config.Sensitivity = domain.SensitivityInternal
	}
	if !config.Sensitivity.IsValid() {
		return nil, fmt.Errorf("observer output sensitivity is invalid")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create observer output root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve observer output root links: %w", err)
	}
	root = filepath.Clean(root)
	if _, err := verifiedDirectoryEntries(root); err != nil {
		return nil, fmt.Errorf("verify observer output root: %w", err)
	}
	if _, _, err := ensureChildDirectory(root, "objects"); err != nil {
		return nil, fmt.Errorf("create observer object root: %w", err)
	}
	return &LocalOutputFactory{
		root:        root,
		sensitivity: config.Sensitivity,
		syncFile:    syncCaptureFile,
		syncDir:     syncCaptureDirectory,
		active:      make(map[string]*localCapture),
	}, nil
}

func (f *LocalOutputFactory) Open(ctx context.Context, plan ports.CollectorPlan) (OutputCapture, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	signature, err := localCaptureSignature(plan)
	if err != nil {
		return nil, err
	}
	key := plan.CollectorID.String()
	f.mu.Lock()
	defer f.mu.Unlock()
	if active := f.active[key]; active != nil {
		if active.signature != signature {
			return nil, domain.NewError(domain.CodeConflict, "observer.local_output.open", "collector_id", "already has a different output transaction", nil)
		}
		return active, nil
	}
	runsDirectory, _, err := ensureChildDirectory(f.root, "runs")
	if err != nil {
		return nil, err
	}
	runDirectory, _, err := ensureChildDirectory(runsDirectory, plan.TargetRunID.String())
	if err != nil {
		return nil, err
	}
	directory, _, err := ensureChildDirectory(runDirectory, plan.CollectorID.String())
	if err != nil {
		return nil, err
	}
	if artifacts, finalErr, found, err := f.loadFinalized(directory, signature, plan); err != nil {
		return nil, err
	} else if found {
		capture := &localCapture{factory: f, plan: plan, signature: signature, directory: directory, finalized: artifacts, finalErr: finalErr}
		capture.stdout = closedCaptureWriter()
		capture.stderr = closedCaptureWriter()
		f.active[key] = capture
		return capture, nil
	}
	if err := f.prepareCaptureDirectoryForOpen(directory, signature); err != nil {
		return nil, err
	}
	budget := &captureBudget{remaining: plan.MaximumBytes}
	stdout, stderr, err := f.openDurableCaptureWriters(directory, budget)
	if err != nil {
		return nil, err
	}
	capture := &localCapture{factory: f, plan: plan, signature: signature, directory: directory, stdout: stdout, stderr: stderr, budget: budget}
	f.active[key] = capture
	return capture, nil
}

type namedCaptureWriter struct {
	name   string
	writer *boundedCaptureWriter
}

func (f *LocalOutputFactory) openDurableCaptureWriters(directory string, budget *captureBudget) (*boundedCaptureWriter, *boundedCaptureWriter, error) {
	writers := []namedCaptureWriter{{name: "stdout.partial"}, {name: "stderr.partial"}}
	for index := range writers {
		writer, err := openBoundedWriter(filepath.Join(directory, writers[index].name), budget)
		if err != nil {
			cleanupErr := f.discardOpenCapture(directory, writers)
			return nil, nil, errors.Join(err, cleanupErr)
		}
		writers[index].writer = writer
	}
	if err := f.syncNewCapture(directory, writers); err != nil {
		cleanupErr := f.discardOpenCapture(directory, writers)
		return nil, nil, errors.Join(fmt.Errorf("make collector output transaction durable: %w", err), cleanupErr)
	}
	return writers[0].writer, writers[1].writer, nil
}

func (f *LocalOutputFactory) syncNewCapture(directory string, writers []namedCaptureWriter) error {
	for _, value := range writers {
		path := filepath.Join(directory, value.name)
		if err := requireEmptyCaptureFile(path, value.writer); err != nil {
			return err
		}
		if err := f.syncFile(value.writer.file); err != nil {
			return fmt.Errorf("sync %s: %w", value.name, err)
		}
		if err := requireEmptyCaptureFile(path, value.writer); err != nil {
			return err
		}
	}
	if err := f.syncDir(directory); err != nil {
		return fmt.Errorf("sync collector output directory: %w", err)
	}
	for _, value := range writers {
		if err := requireEmptyCaptureFile(filepath.Join(directory, value.name), value.writer); err != nil {
			return err
		}
	}
	return requireExactCollectorFiles(directory, writers[0].name, writers[1].name)
}

func requireEmptyCaptureFile(path string, writer *boundedCaptureWriter) error {
	if writer == nil || writer.file == nil {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "partial_output", "capture file is missing", nil)
	}
	fromDescriptor, err := writer.file.Stat()
	if err != nil {
		return err
	}
	fromPath, pathErr := os.Lstat(path)
	if pathErr != nil || !fromDescriptor.Mode().IsRegular() || fromDescriptor.Size() != 0 || !fromPath.Mode().IsRegular() || fromPath.Mode()&os.ModeSymlink != 0 || fromPath.Size() != 0 || !os.SameFile(fromDescriptor, fromPath) {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "partial_output", "must be the exact empty regular file created for this transaction", pathErr)
	}
	return nil
}

func (f *LocalOutputFactory) discardOpenCapture(directory string, writers []namedCaptureWriter) error {
	identities := make([]os.FileInfo, len(writers))
	values := make([]*boundedCaptureWriter, 0, len(writers))
	var result []error
	for index, value := range writers {
		if value.writer == nil || value.writer.file == nil {
			continue
		}
		identity, err := value.writer.file.Stat()
		if err != nil {
			result = append(result, fmt.Errorf("identify uncommitted %s: %w", value.name, err))
		} else {
			identities[index] = identity
		}
		values = append(values, value.writer)
	}
	result = append(result, closeCaptureWriters(values...))
	for index, identity := range identities {
		if identity == nil {
			continue
		}
		if err := removeExactRegularFile(filepath.Join(directory, writers[index].name), identity); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = append(result, err)
		}
	}
	if f.syncDir == nil {
		result = append(result, errors.New("capture directory sync is nil"))
	} else {
		result = append(result, f.syncDir(directory))
	}
	return errors.Join(result...)
}

func openBoundedWriter(path string, budget *captureBudget) (*boundedCaptureWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &boundedCaptureWriter{file: file, budget: budget}, nil
}

func closedCaptureWriter() *boundedCaptureWriter { return &boundedCaptureWriter{closed: true} }

func closeCaptureWriters(writers ...*boundedCaptureWriter) error {
	result := make([]error, 0, len(writers))
	for _, writer := range writers {
		if writer != nil {
			result = append(result, writer.Close())
		}
	}
	return errors.Join(result...)
}

func (w *boundedCaptureWriter) Write(value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.file == nil {
		return 0, os.ErrClosed
	}
	w.budget.mu.Lock()
	allowed := int64(len(value))
	if allowed > w.budget.remaining {
		allowed = w.budget.remaining
	}
	w.budget.remaining -= allowed
	w.budget.mu.Unlock()
	written := 0
	var err error
	if allowed > 0 {
		written, err = w.file.Write(value[:allowed])
	}
	w.budget.mu.Lock()
	w.budget.remaining += allowed - int64(written)
	limitExceeded := int64(written) == allowed && allowed < int64(len(value))
	if limitExceeded {
		w.budget.exceeded = true
	}
	w.budget.mu.Unlock()
	if err == nil && int64(written) < allowed {
		err = io.ErrShortWrite
	}
	if err != nil {
		return written, err
	}
	if limitExceeded {
		return written, ErrCaptureLimit
	}
	return written, nil
}

func (w *boundedCaptureWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}
	return errors.Join(w.file.Sync(), w.file.Close())
}

func (c *localCapture) Stdout() io.WriteCloser { return c.stdout }
func (c *localCapture) Stderr() io.WriteCloser { return c.stderr }

func (c *localCapture) Finalize(ctx context.Context) ([]domain.ArtifactReference, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return nil, domain.NewError(domain.CodeInvalidState, "observer.local_output.finalize", "capture", "was aborted", nil)
	}
	if len(c.finalized) > 0 {
		cleanupErr := removeCapturePartials(c.directory)
		c.factory.release(c.plan.CollectorID.String(), c)
		return append([]domain.ArtifactReference(nil), c.finalized...), errors.Join(c.finalErr, cleanupErr)
	}
	if err := closeCaptureWriters(c.stdout, c.stderr); err != nil {
		return nil, err
	}
	streams := []struct{ file, role string }{{"stdout.partial", CollectorStdoutRole}, {"stderr.partial", CollectorStderrRole}}
	contents := make([][]byte, len(streams))
	var totalSize int64
	for index, stream := range streams {
		content, err := readBoundedRegularFile(filepath.Join(c.directory, stream.file), c.plan.MaximumBytes)
		if err != nil {
			return nil, err
		}
		if int64(len(content)) > c.plan.MaximumBytes-totalSize {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.finalize", "partial_output", "exceeds the collector's shared byte limit", nil)
		}
		totalSize += int64(len(content))
		contents[index] = content
	}
	c.budget.mu.Lock()
	remaining := c.budget.remaining
	limitAttempted := c.budget.exceeded
	c.budget.mu.Unlock()
	if remaining < 0 || remaining > c.plan.MaximumBytes || totalSize != c.plan.MaximumBytes-remaining {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.finalize", "capture_budget", "retained bytes do not match the authority-owned budget", nil)
	}
	exceeded := limitAttempted && totalSize == c.plan.MaximumBytes
	artifacts := make([]domain.ArtifactReference, 0, 2)
	for index, stream := range streams {
		content := contents[index]
		digest := domain.NewDigest(content)
		objectPath := filepath.Join(c.factory.root, "objects", strings.TrimPrefix(digest.String(), "sha256:"))
		if err := ensureLocalObject(objectPath, content); err != nil {
			return nil, err
		}
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
			Reference: "observer://collectors/" + c.plan.CollectorID.String() + "/" + strings.TrimPrefix(stream.role, "collector.") + "/" + digest.String(),
			Digest:    digest, Size: int64(len(content)), Role: stream.role, Sensitivity: c.factory.sensitivity,
		})
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	manifest := localCaptureManifest{Version: localCaptureManifestVersion, Signature: c.signature, Exceeded: exceeded}
	for _, artifact := range artifacts {
		spec := artifact.Spec()
		manifest.Artifacts = append(manifest.Artifacts, localArtifactManifest{spec.Reference, spec.Digest.String(), spec.Size, spec.Role, string(spec.Sensitivity)})
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if err := publishCaptureControlFile(c.directory, "finalized.json", encoded, maximumLocalCaptureManifestBytes); err != nil {
		return nil, err
	}
	c.finalized = append([]domain.ArtifactReference(nil), artifacts...)
	if exceeded {
		c.finalErr = ErrCaptureLimit
	}
	cleanupErr := removeCapturePartials(c.directory)
	c.factory.release(c.plan.CollectorID.String(), c)
	return append([]domain.ArtifactReference(nil), artifacts...), errors.Join(c.finalErr, cleanupErr)
}

func (c *localCapture) Abort(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.finalized) > 0 {
		return domain.NewError(domain.CodeInvalidState, "observer.local_output.abort", "capture", "is already finalized", nil)
	}
	closeErr := closeCaptureWriters(c.stdout, c.stderr)
	markerErr := publishCaptureControlFile(c.directory, "aborted", []byte(c.signature+"\n"), maximumLocalCaptureManifestBytes)
	if markerErr == nil {
		c.aborted = true
	}
	cleanupErr := error(nil)
	if c.aborted {
		cleanupErr = removeCapturePartials(c.directory)
	}
	if c.aborted {
		c.factory.release(c.plan.CollectorID.String(), c)
	}
	return errors.Join(closeErr, markerErr, cleanupErr)
}

func (f *LocalOutputFactory) release(key string, capture *localCapture) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active[key] == capture {
		delete(f.active, key)
	}
}

func (f *LocalOutputFactory) loadFinalized(directory, signature string, plan ports.CollectorPlan) ([]domain.ArtifactReference, error, bool, error) {
	encoded, err := readBoundedRegularFile(filepath.Join(directory, "finalized.json"), maximumLocalCaptureManifestBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	artifacts, finalErr, err := f.decodeFinalized(encoded, signature, plan)
	if err != nil {
		return nil, nil, false, err
	}
	return artifacts, finalErr, true, nil
}

func (f *LocalOutputFactory) decodeFinalized(encoded []byte, signature string, plan ports.CollectorPlan) ([]domain.ArtifactReference, error, error) {
	manifest, decodeErr := decodeLocalCaptureManifest(encoded)
	if decodeErr != nil || manifest.Version != localCaptureManifestVersion || manifest.Signature != signature || len(manifest.Artifacts) != 2 {
		return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "manifest", "is invalid or belongs to another plan", decodeErr)
	}
	artifacts := make([]domain.ArtifactReference, 0, len(manifest.Artifacts))
	var totalSize int64
	for _, value := range manifest.Artifacts {
		digest, err := domain.ParseDigest(value.Digest)
		if err != nil {
			return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "manifest.digest", "is invalid", err)
		}
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: value.Reference, Digest: digest, Size: value.Size, Role: value.Role, Sensitivity: domain.Sensitivity(value.Sensitivity)})
		if err != nil {
			return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "manifest.artifact", "is invalid", err)
		}
		expectedReference := "observer://collectors/" + plan.CollectorID.String() + "/" + strings.TrimPrefix(value.Role, "collector.") + "/" + digest.String()
		if value.Reference != expectedReference || value.Sensitivity != string(f.sensitivity) || value.Size > plan.MaximumBytes-totalSize {
			return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "manifest.artifact", "does not match its authorized collector", nil)
		}
		totalSize += value.Size
		content, err := readBoundedRegularFile(filepath.Join(f.root, "objects", strings.TrimPrefix(digest.String(), "sha256:")), plan.MaximumBytes)
		if err != nil || int64(len(content)) != value.Size || domain.NewDigest(content) != digest {
			return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "object", "content does not match the finalized artifact", err)
		}
		artifacts = append(artifacts, artifact)
	}
	if err := validateArtifacts(artifacts); err != nil {
		return nil, nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.open", "manifest.artifacts", "required stream artifacts are invalid", err)
	}
	var finalErr error
	if manifest.Exceeded {
		finalErr = ErrCaptureLimit
	}
	return artifacts, finalErr, nil
}

func decodeLocalCaptureManifest(encoded []byte) (localCaptureManifest, error) {
	var manifest localCaptureManifest
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return localCaptureManifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("manifest contains more than one JSON value")
		}
		return localCaptureManifest{}, err
	}
	return manifest, nil
}

func localCaptureSignature(plan ports.CollectorPlan) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	return domain.NewDigest(encoded).String(), nil
}

func ensureLocalObject(path string, desired []byte) error {
	existing, err := readBoundedRegularFile(path, int64(len(desired)))
	if err == nil {
		if bytes.Equal(existing, desired) {
			return nil
		}
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.publish", "object", "immutable content conflicts", nil)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := publishCaptureControlFile(filepath.Dir(path), filepath.Base(path), desired, int64(len(desired))); err != nil {
		return err
	}
	return nil
}

func readBoundedRegularFile(path string, maximum int64) ([]byte, error) {
	file, before, err := openVerifiedRegularFile(path)
	if err != nil {
		return nil, err
	}
	if before.Size() < 0 || before.Size() > maximum {
		_ = file.Close()
		return nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.read", "file", "must be a bounded regular file", nil)
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	closeErr := closeVerifiedRegularFile(file, before)
	if err != nil || closeErr != nil || int64(len(content)) != before.Size() || int64(len(content)) > maximum {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.read", "file", "changed while reading or exceeded its limit", errors.Join(err, closeErr))
	}
	return content, nil
}

func publishCaptureControlFile(directory, name string, desired []byte, maximum int64) error {
	if int64(len(desired)) > maximum {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.publish", "control_file", "exceeds its authorized limit", nil)
	}
	destination := filepath.Join(directory, name)
	if existing, err := readBoundedRegularFile(destination, maximum); err == nil {
		if bytes.Equal(existing, desired) {
			return nil
		}
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.publish", "control_file", "immutable content conflicts", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	pending := destination + ".pending"
	if existing, err := readBoundedRegularFile(pending, maximum); err == nil {
		if !bytes.Equal(existing, desired) {
			return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.publish", "pending_control_file", "content conflicts", nil)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		file, createErr := os.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil {
			return createErr
		}
		writeErr := writeAndSync(file, desired)
		if writeErr != nil {
			return writeErr
		}
	} else {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.publish", "control_file", "appeared during publication", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(pending, destination); err != nil {
		return err
	}
	return syncCaptureDirectory(directory)
}

func writeAndSync(file *os.File, content []byte) error {
	if file == nil {
		return errors.New("control file is nil")
	}
	_, writeErr := file.Write(content)
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func syncCaptureFile(file *os.File) error {
	if file == nil {
		return errors.New("capture file is nil")
	}
	return file.Sync()
}

func syncCaptureDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}

func removeCapturePartials(directory string) error {
	var result []error
	for _, name := range []string{"stdout.partial", "stderr.partial"} {
		if err := removeVerifiedRegularFile(filepath.Join(directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = append(result, err)
		}
	}
	result = append(result, syncCaptureDirectory(directory))
	return errors.Join(result...)
}

func removeVerifiedRegularFile(path string) error {
	file, before, err := openVerifiedRegularFile(path)
	if err != nil {
		return err
	}
	if err := closeVerifiedRegularFile(file, before); err != nil {
		return err
	}
	return removeExactRegularFile(path, before)
}

func removeExactRegularFile(path string, expected os.FileInfo) error {
	if expected == nil || !expected.Mode().IsRegular() {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.remove", "file", "expected identity is not a regular file", nil)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, current) {
		return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.remove", "file", "identity changed before removal", nil)
	}
	return os.Remove(path)
}

var _ OutputFactory = (*LocalOutputFactory)(nil)
