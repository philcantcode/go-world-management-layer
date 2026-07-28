// Package observationbundle validates and atomically seals exactly one
// observation bundle for each target run.
package observationbundle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const SchemaVersion uint32 = 1

type FinalizeRequest struct {
	BundleID         domain.ObservationBundleID
	TargetID         domain.TargetID
	TargetGeneration domain.TargetGeneration
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	RequiredCoverage []string
	Result           ports.TargetRunResult
	CreatedAt        time.Time
	FinalizedAt      time.Time
}

type Metadata struct {
	SchemaVersion uint32          `json:"schema_version"`
	ContentDigest string          `json:"content_digest"`
	Bundle        MetadataPayload `json:"bundle"`
}

type MetadataPayload struct {
	BundleID         string             `json:"bundle_id"`
	TargetRunID      string             `json:"target_run_id"`
	TargetID         string             `json:"target_id"`
	TargetGeneration uint64             `json:"target_generation"`
	AgentWorkspaceID string             `json:"agent_workspace_id"`
	AgentGeneration  uint64             `json:"agent_generation"`
	Outcome          ports.RunOutcome   `json:"outcome"`
	FirstCursor      uint64             `json:"first_cursor"`
	LastCursor       uint64             `json:"last_cursor"`
	RequiredCoverage []string           `json:"required_coverage"`
	RawArtifacts     []ArtifactMetadata `json:"raw_artifacts"`
	NormalizedEvents []EventMetadata    `json:"normalized_events"`
	Metrics          []MetricMetadata   `json:"metrics"`
	Coverage         []CoverageMetadata `json:"coverage"`
	Gaps             []GapMetadata      `json:"gaps"`
	TargetChanges    ChangeSetMetadata  `json:"target_changes"`
	IncidentIDs      []string           `json:"incident_ids"`
	Summary          SummaryMetadata    `json:"summary"`
	CreatedAt        time.Time          `json:"created_at"`
	FinalizedAt      time.Time          `json:"finalized_at"`
}

type ArtifactMetadata struct {
	Reference   string `json:"reference"`
	Digest      string `json:"digest"`
	Size        int64  `json:"size"`
	Role        string `json:"role"`
	Sensitivity string `json:"sensitivity"`
}

type EventMetadata struct {
	EventID      string `json:"event_id"`
	Kind         string `json:"kind"`
	Source       string `json:"source"`
	SourceCursor uint64 `json:"source_cursor"`
	Digest       string `json:"digest"`
}

type MetricMetadata struct {
	SubjectID   string    `json:"subject_id"`
	Name        string    `json:"name"`
	Cursor      uint64    `json:"cursor"`
	CollectedAt time.Time `json:"collected_at"`
	Digest      string    `json:"digest"`
}

type GapMetadata struct {
	Kind                string    `json:"kind"`
	Source              string    `json:"source"`
	SourceInstance      string    `json:"source_instance,omitempty"`
	FirstSourceSequence uint64    `json:"first_source_sequence,omitempty"`
	LastSourceSequence  uint64    `json:"last_source_sequence,omitempty"`
	FirstCursor         uint64    `json:"first_cursor,omitempty"`
	LastCursor          uint64    `json:"last_cursor,omitempty"`
	StartedAt           time.Time `json:"started_at,omitempty"`
	EndedAt             time.Time `json:"ended_at,omitempty"`
	LostRecords         uint64    `json:"lost_records,omitempty"`
	Reason              string    `json:"reason"`
}

type CoverageMetadata struct {
	CollectorID    string        `json:"collector_id"`
	SignalFamily   string        `json:"signal_family"`
	Placement      string        `json:"placement"`
	Level          string        `json:"level"`
	Status         string        `json:"status"`
	Required       bool          `json:"required"`
	StartedAt      time.Time     `json:"started_at,omitempty"`
	EndedAt        time.Time     `json:"ended_at,omitempty"`
	DroppedRecords uint64        `json:"dropped_records,omitempty"`
	Gaps           []GapMetadata `json:"gaps"`
}

type ChangeMetadata struct {
	Kind         string            `json:"kind"`
	Path         string            `json:"path"`
	PreviousPath string            `json:"previous_path,omitempty"`
	BeforeDigest string            `json:"before_digest,omitempty"`
	AfterDigest  string            `json:"after_digest,omitempty"`
	Metadata     map[string]string `json:"metadata"`
}

type ChangeSetMetadata struct {
	Scope             string           `json:"scope"`
	WorkspaceRevision uint64           `json:"workspace_revision"`
	SealedAt          time.Time        `json:"sealed_at"`
	Entries           []ChangeMetadata `json:"entries"`
}

type CitationMetadata struct {
	FirstCursor uint64            `json:"first_cursor,omitempty"`
	LastCursor  uint64            `json:"last_cursor,omitempty"`
	Artifact    *ArtifactMetadata `json:"artifact,omitempty"`
}

type SummaryMetadata struct {
	Text       string             `json:"text"`
	Citations  []CitationMetadata `json:"citations"`
	Inferences []string           `json:"inferences"`
}

type Result struct {
	Bundle   domain.ObservationBundle
	Metadata Metadata
	Path     string
	Content  ports.ContentSource
	Created  bool
}

type Finalizer struct {
	root string
	gate chan struct{}
}

func New(root string) (*Finalizer, error) {
	if strings.TrimSpace(root) == "" {
		return nil, domain.NewError(domain.CodeInvalidArgument, "observation_bundle_finalizer.new", "root", "must not be blank", nil)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, "observation_bundle_finalizer.new", "root", "cannot be resolved", err)
	}
	absolute = filepath.Clean(absolute)
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "observation_bundle_finalizer.new", "root", "cannot be created", err)
	}
	for _, logicalDirectory := range []string{"objects", "runs"} {
		namespace, err := safepath.OpenNamespace(absolute, logicalDirectory)
		if err != nil {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle_finalizer.new", logicalDirectory, "namespace is unsafe", err)
		}
		cleanupErr := cleanupNamespaceStages(namespace)
		closeErr := namespace.Close()
		if cleanupErr != nil || closeErr != nil {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle_finalizer.new", logicalDirectory, "namespace cannot be initialized", errors.Join(cleanupErr, closeErr))
		}
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &Finalizer{root: absolute, gate: gate}, nil
}

func (f *Finalizer) Finalize(ctx context.Context, request FinalizeRequest) (Result, error) {
	const operation = "observation_bundle.finalize"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return Result{}, err
	}
	select {
	case <-ctx.Done():
		return Result{}, domain.NewError(domain.CodeDeadlineExceeded, operation, "context", "cancelled while waiting to finalize", ctx.Err())
	case <-f.gate:
	}
	defer func() { f.gate <- struct{}{} }()
	bundle, payload, err := build(request)
	if err != nil {
		return Result{}, err
	}
	payloadBytes, err := canonicalJSON(payload)
	if err != nil {
		return Result{}, domain.NewError(domain.CodeInternal, operation, "metadata", "cannot be encoded", err)
	}
	contentDigest := domain.NewDigest(payloadBytes)
	metadata := Metadata{SchemaVersion: SchemaVersion, ContentDigest: contentDigest.String(), Bundle: payload}
	encoded, err := canonicalJSON(metadata)
	if err != nil {
		return Result{}, domain.NewError(domain.CodeInternal, operation, "metadata", "cannot be encoded", err)
	}
	artifactDigest := domain.NewDigest(encoded)
	artifactSize := int64(len(encoded))

	runLogicalDirectory := "runs/" + request.Result.RunID.String()
	runNamespace, err := safepath.OpenNamespace(f.root, runLogicalDirectory)
	if err != nil {
		return Result{}, domain.NewError(domain.CodeIntegrityViolation, operation, "run_directory", "cannot be opened safely", err)
	}
	defer runNamespace.Close()
	runDirectory := filepath.Join(f.root, filepath.FromSlash(runLogicalDirectory))
	sealedPath := filepath.Join(runDirectory, "sealed.json")
	if existing, found, err := loadMetadata(runNamespace, "sealed.json", int64(len(encoded))); err != nil {
		return Result{}, err
	} else if found {
		if existing.ContentDigest != metadata.ContentDigest {
			return Result{}, domain.NewDetailedError(domain.CodeConflict, operation, "target_run_id", "a different bundle is already sealed for this run", map[string]string{"existing_digest": existing.ContentDigest, "requested_digest": metadata.ContentDigest}, nil)
		}
		if err := ensureCommitMarker(runNamespace, artifactDigest.String()); err != nil {
			return Result{}, err
		}
		return Result{Bundle: bundle, Metadata: existing, Path: sealedPath, Content: sealedContent{root: f.root, logicalDirectory: runLogicalDirectory, name: "sealed.json", digest: artifactDigest, size: artifactSize}}, nil
	}
	if err := ctx.Err(); err != nil {
		return Result{}, domain.NewError(domain.CodeDeadlineExceeded, operation, "context", "cancelled before publication", err)
	}

	objectName := strings.TrimPrefix(artifactDigest.String(), "sha256:") + ".json"
	objectNamespace, err := safepath.OpenNamespace(f.root, "objects")
	if err != nil {
		return Result{}, domain.NewError(domain.CodeIntegrityViolation, operation, "object", "namespace is unsafe", err)
	}
	if err := ensureImmutableFile(objectNamespace, objectName, encoded, "object"); err != nil {
		_ = objectNamespace.Close()
		return Result{}, err
	}
	if err := objectNamespace.Close(); err != nil {
		return Result{}, domain.NewError(domain.CodeUnavailable, operation, "object", "namespace cannot be closed", err)
	}
	if err := runNamespace.EnsureRegularAtomically("sealed.json", encoded, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return Result{}, domain.NewError(domain.CodeConflict, operation, "target_run_id", "a conflicting finalization won publication", err)
		}
		return Result{}, domain.NewError(domain.CodeUnavailable, operation, "sealed_metadata", "cannot be atomically published", err)
	}
	if err := ensureCommitMarker(runNamespace, artifactDigest.String()); err != nil {
		return Result{}, err
	}
	return Result{Bundle: bundle, Metadata: metadata, Path: sealedPath, Content: sealedContent{root: f.root, logicalDirectory: runLogicalDirectory, name: "sealed.json", digest: artifactDigest, size: artifactSize}, Created: true}, nil
}

type sealedContent struct {
	root             string
	logicalDirectory string
	name             string
	digest           domain.Digest
	size             int64
}

func (s sealedContent) Digest() domain.Digest { return s.digest }
func (s sealedContent) Size() int64           { return s.size }
func (s sealedContent) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespace, err := safepath.OpenNamespace(s.root, s.logicalDirectory)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "observation_bundle.content.open", "content", "sealed content cannot be opened", err)
	}
	content, readErr := namespace.ReadRegularBounded(s.name, s.size)
	closeErr := namespace.Close()
	if readErr != nil || closeErr != nil || int64(len(content)) != s.size || domain.NewDigest(content) != s.digest {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.content.open", "content", "sealed content identity changed", errors.Join(readErr, closeErr))
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func ensureCommitMarker(namespace *safepath.Namespace, digest string) error {
	return ensureImmutableFile(namespace, "committed", []byte(digest+"\n"), "commit_marker")
}

func ensureImmutableFile(namespace *safepath.Namespace, name string, desired []byte, field string) error {
	if err := namespace.EnsureRegularAtomically(name, desired, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.finalize", field, "contains bytes that conflict with the sealed bundle", err)
		}
		return domain.NewError(domain.CodeUnavailable, "observation_bundle.finalize", field, "cannot be durably published", err)
	}
	return nil
}

func loadMetadata(namespace *safepath.Namespace, name string, maximum int64) (Metadata, bool, error) {
	var metadata Metadata
	encoded, err := namespace.ReadRegularBounded(name, maximum)
	if errors.Is(err, os.ErrNotExist) {
		return metadata, false, nil
	}
	if err != nil {
		if errors.Is(err, safepath.ErrTooLarge) || errors.Is(err, safepath.ErrUnsafe) || errors.Is(err, safepath.ErrNotRegular) {
			return metadata, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "metadata", "is not a bounded single-link regular file", err)
		}
		return metadata, false, domain.NewError(domain.CodeUnavailable, "observation_bundle.load", "metadata", "cannot be read", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return Metadata{}, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "metadata", "is invalid", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Metadata{}, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "metadata", "contains trailing JSON", nil)
	}
	if metadata.SchemaVersion != SchemaVersion || metadata.ContentDigest == "" {
		return Metadata{}, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "metadata", "has an unsupported schema or missing digest", nil)
	}
	canonical, err := canonicalJSON(metadata)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return Metadata{}, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "metadata", "is not canonical", err)
	}
	payload, err := canonicalJSON(metadata.Bundle)
	if err != nil || domain.NewDigest(payload).String() != metadata.ContentDigest {
		return Metadata{}, false, domain.NewError(domain.CodeIntegrityViolation, "observation_bundle.load", "content_digest", "does not match metadata", err)
	}
	return metadata, true, nil
}

func cleanupNamespaceStages(namespace *safepath.Namespace) error {
	for _, prefix := range []string{".staging-", ".world-ns-"} {
		if err := namespace.CleanupPrefix(prefix); err != nil {
			return err
		}
	}
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}
