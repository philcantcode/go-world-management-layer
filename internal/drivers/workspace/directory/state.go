package directory

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

const (
	recordSchemaVersion = 2
	recordFilename      = "workspace.json"
	mergedDirectory     = "merged"
	sealedDirectory     = "sealed"
	maximumRecordBytes  = 64 << 20
)

type diskRecord struct {
	SchemaVersion   int                   `json:"schema_version"`
	WorkspaceID     string                `json:"workspace_id"`
	IdempotencyKey  string                `json:"idempotency_key"`
	PlanDigest      string                `json:"plan_digest"`
	InputViewID     string                `json:"input_view_id"`
	State           domain.WorkspaceState `json:"state"`
	UpperByteLimit  int64                 `json:"upper_byte_limit"`
	UpperInodeLimit int64                 `json:"upper_inode_limit"`
	ObservedAt      time.Time             `json:"observed_at"`
	Baseline        workspacepkg.Manifest `json:"baseline"`
	Preview         *diskPreview          `json:"preview,omitempty"`
	Seal            *diskSeal             `json:"seal,omitempty"`
}

type diskPreview struct {
	Revision   domain.Revision       `json:"revision"`
	ObservedAt time.Time             `json:"observed_at"`
	Manifest   workspacepkg.Manifest `json:"manifest"`
	Changes    []diskChange          `json:"changes"`
}

type diskSeal struct {
	Revision domain.Revision       `json:"revision"`
	SealedAt time.Time             `json:"sealed_at"`
	Manifest workspacepkg.Manifest `json:"manifest"`
	Changes  []diskChange          `json:"changes"`
}

type diskChange struct {
	Kind         domain.ChangeKind `json:"kind"`
	Path         string            `json:"path"`
	PreviousPath string            `json:"previous_path,omitempty"`
	BeforeDigest string            `json:"before_digest,omitempty"`
	AfterDigest  string            `json:"after_digest,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type planIdentity struct {
	WorkspaceID      string    `json:"workspace_id"`
	LeaseID          string    `json:"lease_id"`
	AgentWorkspaceID string    `json:"agent_workspace_id"`
	AgentGeneration  uint64    `json:"agent_generation"`
	InputViewID      string    `json:"input_view_id"`
	WorkspaceCreated time.Time `json:"workspace_created_at"`
	Construction     string    `json:"construction"`
	UpperByteLimit   int64     `json:"upper_byte_limit"`
	UpperInodeLimit  int64     `json:"upper_inode_limit"`
}

func workspacePlanDigest(plan ports.WorkspacePlan) (string, error) {
	spec := plan.Workspace.Spec()
	identity := planIdentity{
		WorkspaceID: spec.ID.String(), LeaseID: spec.LeaseID.String(), AgentWorkspaceID: spec.AgentWorkspaceID.String(),
		AgentGeneration: uint64(spec.AgentGeneration), InputViewID: plan.InputView.ID().String(),
		WorkspaceCreated: spec.CreatedAt.UTC(), Construction: string(plan.Construction),
		UpperByteLimit: plan.UpperByteLimit, UpperInodeLimit: plan.UpperInodeLimit,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("world.directory-workspace-plan.v1\x00"), encoded...))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func writeDiskRecord(workspaceRoot string, record diskRecord) error {
	if err := validateDiskRecord(record); err != nil {
		return err
	}
	return atomicfile.WriteJSON(filepath.Join(workspaceRoot, recordFilename), record, 0o600)
}

func readDiskRecord(workspaceRoot string) (diskRecord, error) {
	opened, err := safepath.OpenRegular(workspaceRoot, recordFilename)
	if err != nil {
		return diskRecord{}, fmt.Errorf("open workspace record: %w", err)
	}
	defer opened.Close()
	var content bytes.Buffer
	if _, err := safepath.CopyBounded(&content, opened, maximumRecordBytes); err != nil {
		return diskRecord{}, fmt.Errorf("read workspace record: %w", err)
	}
	decoder := json.NewDecoder(&content)
	decoder.DisallowUnknownFields()
	var record diskRecord
	if err := decoder.Decode(&record); err != nil {
		return diskRecord{}, fmt.Errorf("decode workspace record: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return diskRecord{}, err
	}
	if err := validateDiskRecord(record); err != nil {
		return diskRecord{}, err
	}
	return record, nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("workspace record has trailing JSON")
		}
		return fmt.Errorf("decode trailing workspace record data: %w", err)
	}
	return nil
}

func validateDiskRecord(record diskRecord) error {
	if record.SchemaVersion != recordSchemaVersion {
		return fmt.Errorf("unsupported workspace record schema %d", record.SchemaVersion)
	}
	if _, err := domain.ParseWorkspaceID(record.WorkspaceID); err != nil {
		return fmt.Errorf("workspace id: %w", err)
	}
	if !domain.IsCanonicalIdempotencyKey(record.IdempotencyKey) {
		return fmt.Errorf("idempotency key is not canonical")
	}
	if _, err := domain.ParseDigest(record.PlanDigest); err != nil {
		return fmt.Errorf("plan digest: %w", err)
	}
	if _, err := domain.ParseInputViewID(record.InputViewID); err != nil {
		return fmt.Errorf("input view id: %w", err)
	}
	if record.State != domain.WorkspaceReady && record.State != domain.WorkspaceMounted && record.State != domain.WorkspaceSealed && record.State != domain.WorkspaceReleased {
		return fmt.Errorf("unsupported persisted workspace state %q", record.State)
	}
	if record.UpperByteLimit <= 0 || record.UpperInodeLimit <= 0 || record.ObservedAt.IsZero() {
		return fmt.Errorf("workspace limits and observation time are required")
	}
	if err := workspacepkg.ValidateManifest(record.Baseline); err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	if err := validateStoredManifest(record.Baseline); err != nil {
		return fmt.Errorf("baseline manifest: %w", err)
	}
	if err := validateManifestCapacity(record.Baseline, record.UpperByteLimit, record.UpperInodeLimit); err != nil {
		return err
	}
	if record.Preview != nil {
		if err := validatePreview(record, *record.Preview); err != nil {
			return err
		}
	}
	if record.State == domain.WorkspaceSealed || record.State == domain.WorkspaceReleased {
		if record.Seal == nil {
			return fmt.Errorf("sealed workspace record has no seal")
		}
		if _, err := sealResult(*record.Seal); err != nil {
			return fmt.Errorf("sealed result: %w", err)
		}
		if err := workspacepkg.ValidateManifest(record.Seal.Manifest); err != nil {
			return fmt.Errorf("sealed manifest: %w", err)
		}
		if err := validateStoredManifest(record.Seal.Manifest); err != nil {
			return fmt.Errorf("sealed manifest: %w", err)
		}
		if !record.Seal.SealedAt.Equal(record.Seal.Manifest.SealedAt) {
			return fmt.Errorf("seal time does not match the sealed manifest")
		}
		expected, err := changesBetween(record.Baseline, record.Seal.Manifest)
		if err != nil {
			return fmt.Errorf("sealed changes: %w", err)
		}
		actual, err := domainChanges(record.Seal.Changes)
		if err != nil {
			return err
		}
		if !equalChanges(expected, actual) {
			return fmt.Errorf("persisted changes do not match the sealed manifest")
		}
		if record.Preview == nil || record.Preview.Revision != record.Seal.Revision || requireUnchanged(record.Preview.Manifest, record.Seal.Manifest) != nil {
			return fmt.Errorf("sealed workspace does not match its optimistic preview")
		}
	} else if record.Seal != nil {
		return fmt.Errorf("unsealed workspace record unexpectedly contains a seal")
	}
	return nil
}

func validatePreview(record diskRecord, preview diskPreview) error {
	if !preview.Revision.IsValid() || preview.ObservedAt.IsZero() {
		return fmt.Errorf("preview revision and observation time are required")
	}
	if err := workspacepkg.ValidateManifest(preview.Manifest); err != nil {
		return fmt.Errorf("preview manifest: %w", err)
	}
	if err := validateStoredManifest(preview.Manifest); err != nil {
		return fmt.Errorf("preview manifest: %w", err)
	}
	if !preview.ObservedAt.Equal(preview.Manifest.SealedAt) {
		return fmt.Errorf("preview observation time does not match its manifest")
	}
	if err := validateManifestCapacity(preview.Manifest, record.UpperByteLimit, record.UpperInodeLimit); err != nil {
		return err
	}
	expected, err := changesBetween(record.Baseline, preview.Manifest)
	if err != nil {
		return fmt.Errorf("preview changes: %w", err)
	}
	actual, err := domainChanges(preview.Changes)
	if err != nil {
		return err
	}
	if !equalChanges(expected, actual) {
		return fmt.Errorf("persisted preview changes do not match the preview manifest")
	}
	return nil
}

func validateStoredManifest(manifest workspacepkg.Manifest) error {
	for index, entry := range manifest.Entries {
		if _, err := domain.ParseDigest(entry.Digest); err != nil {
			return fmt.Errorf("entry %d digest: %w", index, err)
		}
		if entry.Mode == 0 || entry.Mode&^uint32(0o777) != 0 {
			return fmt.Errorf("entry %d has unsupported mode %#o", index, entry.Mode)
		}
	}
	return nil
}

func validateManifestCapacity(manifest workspacepkg.Manifest, byteLimit, inodeLimit int64) error {
	var total int64
	directories := make(map[string]struct{})
	for _, entry := range manifest.Entries {
		if entry.Size > byteLimit-total {
			return fmt.Errorf("baseline exceeds byte limit")
		}
		total += entry.Size
		for parent := path.Dir(entry.Path); parent != "."; parent = path.Dir(parent) {
			directories[parent] = struct{}{}
		}
	}
	if int64(len(manifest.Entries))+int64(len(directories)) > inodeLimit {
		return fmt.Errorf("baseline exceeds inode limit")
	}
	return nil
}

func (d *Driver) loadRecords() error {
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return fmt.Errorf("read workspace root: %w", err)
	}
	for _, entry := range entries {
		workspaceID, parseErr := domain.ParseWorkspaceID(entry.Name())
		if parseErr != nil {
			if isWorkspaceStagingName(entry.Name()) {
				stagingPath := filepath.Join(d.root, entry.Name())
				if err := removeStagingDirectory(d.root, stagingPath); err != nil {
					return fmt.Errorf("remove incomplete workspace staging directory %q: %w", entry.Name(), err)
				}
				continue
			}
			return fmt.Errorf("workspace root contains unrecognized entry %q", entry.Name())
		}
		workspacePath := d.workspacePath(workspaceID)
		if err := requireExactWorkspaceDirectory(d.root, workspacePath, workspaceID); err != nil {
			return fmt.Errorf("load workspace %s: %w", workspaceID, err)
		}
		record, err := readDiskRecord(workspacePath)
		if err != nil {
			return fmt.Errorf("load workspace %s: %w", workspaceID, err)
		}
		if record.WorkspaceID != workspaceID.String() {
			return fmt.Errorf("workspace directory %s contains authority for %s", workspaceID, record.WorkspaceID)
		}
		if err := removeSnapshotStagingDirectories(workspacePath); err != nil {
			return fmt.Errorf("recover workspace %s snapshot staging: %w", workspaceID, err)
		}
		if prior, duplicate := d.requests[record.IdempotencyKey]; duplicate && prior.workspaceID != workspaceID {
			return fmt.Errorf("idempotency key %q is persisted for multiple workspaces", record.IdempotencyKey)
		}
		if err := d.registerRecord(workspaceID, record, "workspace.directory.load"); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) loadRecordIfPresent(workspaceID domain.WorkspaceID) (*diskRecord, error) {
	workspacePath := d.workspacePath(workspaceID)
	if _, err := os.Lstat(workspacePath); os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "workspace.directory.load", "workspace", "could not inspect the workspace directory", err)
	}
	if err := requireExactWorkspaceDirectory(d.root, workspacePath, workspaceID); err != nil {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "workspace.directory.load", "workspace", "workspace path failed containment validation", err)
	}
	record, err := readDiskRecord(workspacePath)
	if err != nil {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "workspace.directory.load", "metadata", "workspace authority is missing or corrupt", err)
	}
	if record.WorkspaceID != workspaceID.String() {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "workspace.directory.load", "workspace_id", "persisted authority does not match its directory", nil)
	}
	if err := removeSnapshotStagingDirectories(workspacePath); err != nil {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "workspace.directory.load", "snapshot_staging", "workspace contains invalid sealed-snapshot staging state", err)
	}
	return &record, nil
}

func diskChanges(entries []domain.ChangeEntry) []diskChange {
	result := make([]diskChange, 0, len(entries))
	for _, entry := range entries {
		spec := entry.Spec()
		result = append(result, diskChange{
			Kind: spec.Kind, Path: spec.Path, PreviousPath: spec.PreviousPath,
			BeforeDigest: spec.BeforeDigest.String(), AfterDigest: spec.AfterDigest.String(), Metadata: cloneMetadata(spec.Metadata),
		})
	}
	return result
}

func domainChanges(entries []diskChange) ([]domain.ChangeEntry, error) {
	result := make([]domain.ChangeEntry, 0, len(entries))
	for index, entry := range entries {
		before, err := parseOptionalDigest(entry.BeforeDigest)
		if err != nil {
			return nil, fmt.Errorf("change %d before digest: %w", index, err)
		}
		after, err := parseOptionalDigest(entry.AfterDigest)
		if err != nil {
			return nil, fmt.Errorf("change %d after digest: %w", index, err)
		}
		change, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
			Kind: entry.Kind, Path: entry.Path, PreviousPath: entry.PreviousPath,
			BeforeDigest: before, AfterDigest: after, Metadata: cloneMetadata(entry.Metadata),
		})
		if err != nil {
			return nil, fmt.Errorf("change %d: %w", index, err)
		}
		result = append(result, change)
	}
	return result, nil
}

func sealResult(seal diskSeal) (ports.WorkspaceSealResult, error) {
	if !seal.Revision.IsValid() || seal.SealedAt.IsZero() {
		return ports.WorkspaceSealResult{}, fmt.Errorf("seal revision and time are required")
	}
	changes, err := domainChanges(seal.Changes)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	changeSet, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, changes, seal.Revision, seal.SealedAt)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	return ports.WorkspaceSealResult{ChangeSet: changeSet, SealedAt: seal.SealedAt}, nil
}

func previewResult(preview diskPreview) (ports.WorkspacePreviewResult, error) {
	if !preview.Revision.IsValid() || preview.ObservedAt.IsZero() {
		return ports.WorkspacePreviewResult{}, fmt.Errorf("preview revision and observation time are required")
	}
	changes, err := domainChanges(preview.Changes)
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	changeSet, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, changes, preview.Revision, preview.ObservedAt)
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	return ports.WorkspacePreviewResult{ChangeSet: changeSet, ObservedAt: preview.ObservedAt}, nil
}

func parseOptionalDigest(value string) (domain.Digest, error) {
	if value == "" {
		return domain.Digest{}, nil
	}
	return domain.ParseDigest(value)
}

func equalChanges(left, right []domain.ChangeEntry) bool {
	if len(left) != len(right) {
		return false
	}
	leftSpecs := make([]domain.ChangeEntrySpec, len(left))
	rightSpecs := make([]domain.ChangeEntrySpec, len(right))
	for index := range left {
		leftSpecs[index] = left[index].Spec()
		rightSpecs[index] = right[index].Spec()
	}
	sort.Slice(leftSpecs, func(i, j int) bool { return leftSpecs[i].Path < leftSpecs[j].Path })
	sort.Slice(rightSpecs, func(i, j int) bool { return rightSpecs[i].Path < rightSpecs[j].Path })
	return reflect.DeepEqual(leftSpecs, rightSpecs)
}

func cloneMetadata(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
