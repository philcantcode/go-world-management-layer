package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	defaultCaptureRecords = 10000
	maximumCaptureState   = 1 << 20
)

type LedgerCaptureConfig struct {
	Root       string
	Ledger     *ledger.Ledger
	Material   ports.MaterialAuthority
	MaxBytes   int64
	MaxRecords int
	Now        func() time.Time
}

// LedgerCaptureController publishes a bounded, lease-filtered immutable slice
// of the durable observation ledger. It is a concrete capture backend for
// single-node deployments; it does not claim to start a new kernel collector.
type LedgerCaptureController struct {
	root       string
	ledger     *ledger.Ledger
	material   ports.MaterialAuthority
	maxBytes   int64
	maxRecords int
	now        func() time.Time

	mu      sync.Mutex
	records map[string]ledgerCaptureRecord
}

type ledgerCaptureRecord struct {
	CaptureID       string                  `json:"capture_id"`
	IdempotencyKey  string                  `json:"idempotency_key"`
	LeaseID         string                  `json:"lease_id"`
	WorkspaceID     string                  `json:"workspace_id"`
	AgentWorkspace  string                  `json:"agent_workspace_id"`
	AgentGeneration uint64                  `json:"agent_generation"`
	Spec            ledgerCaptureSpec       `json:"spec"`
	StartedAt       time.Time               `json:"started_at"`
	AfterCursor     uint64                  `json:"after_cursor"`
	StoppedAt       time.Time               `json:"stopped_at,omitempty"`
	Artifacts       []captureArtifactRecord `json:"artifacts,omitempty"`
}

type ledgerCaptureSpec struct {
	Profile        string        `json:"profile"`
	SignalFamilies []string      `json:"signal_families"`
	Duration       time.Duration `json:"duration"`
	ByteLimit      uint64        `json:"byte_limit"`
}

type captureArtifactRecord struct {
	Reference   string             `json:"reference"`
	Digest      string             `json:"digest"`
	Size        int64              `json:"size"`
	Role        string             `json:"role"`
	Sensitivity domain.Sensitivity `json:"sensitivity"`
}

type captureDocument struct {
	SchemaVersion  int             `json:"schema_version"`
	CaptureID      string          `json:"capture_id"`
	LeaseID        string          `json:"lease_id"`
	Profile        string          `json:"profile"`
	SignalFamilies []string        `json:"signal_families"`
	StartedAt      time.Time       `json:"started_at"`
	StoppedAt      time.Time       `json:"stopped_at"`
	ThroughCursor  uint64          `json:"through_cursor"`
	Truncated      bool            `json:"truncated"`
	Records        []ledger.Record `json:"records"`
}

func NewLedgerCaptureController(config LedgerCaptureConfig) (*LedgerCaptureController, error) {
	if strings.TrimSpace(config.Root) == "" || config.Ledger == nil || config.Material == nil || config.MaxBytes <= 0 {
		return nil, fmt.Errorf("capture root, ledger, material authority, and positive byte limit are required")
	}
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve capture root: %w", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create capture root: %w", err)
	}
	if config.MaxRecords <= 0 {
		config.MaxRecords = defaultCaptureRecords
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	controller := &LedgerCaptureController{
		root: filepath.Clean(root), ledger: config.Ledger, material: config.Material,
		maxBytes: config.MaxBytes, maxRecords: config.MaxRecords, now: config.Now,
		records: make(map[string]ledgerCaptureRecord),
	}
	if err := controller.load(); err != nil {
		return nil, err
	}
	return controller, nil
}

func (c *LedgerCaptureController) Start(ctx context.Context, plan CapturePlan) error {
	const operation = "orchestration.ledger_capture.start"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return err
	}
	spec, err := normalizeLedgerCapturePlan(plan, c.maxBytes)
	if err != nil {
		return err
	}
	record := ledgerCaptureRecord{
		CaptureID: plan.CaptureID, IdempotencyKey: plan.IdempotencyKey, LeaseID: plan.LeaseID,
		WorkspaceID: plan.Workspace.WorkspaceID.String(), AgentWorkspace: plan.Workspace.AgentWorkspaceID.String(),
		AgentGeneration: uint64(plan.Workspace.AgentGeneration), Spec: spec,
		StartedAt: plan.StartedAt.UTC(), AfterCursor: uint64(c.ledger.Head()),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, found := c.records[plan.CaptureID]; found {
		if !sameCaptureStart(existing, record) {
			return domain.NewError(domain.CodeConflict, operation, "capture_id", "is already bound to another capture plan", nil)
		}
		return nil
	}
	for _, existing := range c.records {
		if existing.IdempotencyKey == plan.IdempotencyKey {
			return domain.NewError(domain.CodeConflict, operation, "idempotency_key", "was used for another capture", nil)
		}
	}
	if err := c.persist(record); err != nil {
		return domain.NewError(domain.CodeUnavailable, operation, "state", "could not persist capture start", err)
	}
	c.records[plan.CaptureID] = record
	return nil
}

func (c *LedgerCaptureController) Stop(ctx context.Context, plan CaptureStopPlan) ([]domain.ArtifactReference, error) {
	const operation = "orchestration.ledger_capture.stop"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return nil, err
	}
	if !domain.IsCanonicalIdempotencyKey(plan.IdempotencyKey) {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "idempotency_key", "must be canonical and at most 1024 bytes", nil)
	}
	if _, err := domain.ParseCaptureID(plan.CaptureID); err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "capture_id", "is invalid", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, found := c.records[plan.CaptureID]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, operation, "capture_id", "was not started by this controller", nil)
	}
	if len(record.Artifacts) > 0 {
		return restoreCaptureArtifacts(record.Artifacts)
	}
	stoppedAt := c.now().UTC()
	through := c.ledger.Head()
	records, truncated, err := c.readCaptureRecords(ctx, record, through, stoppedAt)
	if err != nil {
		return nil, err
	}
	document := captureDocument{
		SchemaVersion: 1, CaptureID: record.CaptureID, LeaseID: record.LeaseID, Profile: record.Spec.Profile,
		SignalFamilies: append([]string(nil), record.Spec.SignalFamilies...), StartedAt: record.StartedAt,
		StoppedAt: stoppedAt, ThroughCursor: uint64(through), Truncated: truncated, Records: records,
	}
	content, err := marshalCaptureDocument(document, captureByteLimit(record.Spec, c.maxBytes))
	if err != nil {
		return nil, err
	}
	path := "captures/" + record.CaptureID + ".json"
	selection, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: path, Roles: []string{"observation-capture"}})
	if err != nil {
		return nil, err
	}
	leaseID, _ := domain.ParseLeaseID(record.LeaseID)
	workspaceID, _ := domain.ParseWorkspaceID(record.WorkspaceID)
	agentID, _ := domain.ParseAgentWorkspaceID(record.AgentWorkspace)
	source := newImmutableContentSource(content)
	artifacts, err := c.material.CaptureOutputs(ctx, ports.OutputPlan{
		IdempotencyKey: domain.DeriveIdempotencyKey(plan.IdempotencyKey, "capture-output"), LeaseID: leaseID, WorkspaceID: workspaceID,
		AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(record.AgentGeneration),
		Selections: []domain.ExportSelection{selection}, Content: map[string]ports.ContentSource{path: source},
		Provenance: map[string]string{"world.capture_id": record.CaptureID, "world.capture_profile": record.Spec.Profile},
	})
	if err != nil {
		return nil, err
	}
	if len(artifacts) == 0 {
		return nil, domain.NewError(domain.CodeIntegrityViolation, operation, "artifacts", "material authority returned no capture artifact", nil)
	}
	record.StoppedAt = stoppedAt
	record.Artifacts = storeCaptureArtifacts(artifacts)
	if err := c.persist(record); err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, operation, "state", "capture published but completion state could not be persisted", err)
	}
	c.records[plan.CaptureID] = record
	return append([]domain.ArtifactReference(nil), artifacts...), nil
}

func normalizeLedgerCapturePlan(plan CapturePlan, maximum int64) (ledgerCaptureSpec, error) {
	if !domain.IsCanonicalIdempotencyKey(plan.IdempotencyKey) || strings.TrimSpace(plan.LeaseID) == "" || plan.StartedAt.IsZero() {
		return ledgerCaptureSpec{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.plan", "identity", "idempotency, lease, and start time are required", nil)
	}
	if _, err := domain.ParseCaptureID(plan.CaptureID); err != nil {
		return ledgerCaptureSpec{}, err
	}
	if _, err := domain.ParseLeaseID(plan.LeaseID); err != nil {
		return ledgerCaptureSpec{}, err
	}
	if plan.Workspace.WorkspaceID.IsZero() || plan.Workspace.AgentWorkspaceID.IsZero() || !plan.Workspace.AgentGeneration.IsValid() {
		return ledgerCaptureSpec{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.plan", "workspace", "complete workspace scope is required", nil)
	}
	if plan.Spec == nil {
		return ledgerCaptureSpec{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.plan", "spec", "is required", nil)
	}
	duration, durationErr := nativeDuration(plan.Spec.Duration, "capture_spec.duration", true)
	if durationErr != nil {
		return ledgerCaptureSpec{}, domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.plan", "spec", "profile, duration, and an in-bounds byte limit are required", durationErr)
	}
	families := append([]string(nil), plan.Spec.SignalFamilies...)
	sort.Strings(families)
	spec := ledgerCaptureSpec{Profile: plan.Spec.Profile, SignalFamilies: families, Duration: duration, ByteLimit: plan.Spec.ByteLimit}
	if err := validateLedgerCaptureSpec(spec, uint64(maximum)); err != nil {
		return ledgerCaptureSpec{}, err
	}
	return spec, nil
}

func validateLedgerCaptureSpec(spec ledgerCaptureSpec, maximum uint64) error {
	if strings.TrimSpace(spec.Profile) == "" || spec.Duration <= 0 || spec.ByteLimit == 0 || maximum > 0 && spec.ByteLimit > maximum {
		return domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.spec", "bounds", "profile, duration, and an in-bounds byte limit are required", nil)
	}
	if len(spec.SignalFamilies) == 0 || len(spec.SignalFamilies) > maxFilterValues {
		return domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.spec", "signal_families", "must be bounded and non-empty", nil)
	}
	for index, family := range spec.SignalFamilies {
		if strings.TrimSpace(family) == "" || len(family) > 256 || index > 0 && family <= spec.SignalFamilies[index-1] {
			return domain.NewError(domain.CodeInvalidArgument, "orchestration.ledger_capture.spec", "signal_families", "must contain unique, sorted, non-blank bounded values", nil)
		}
	}
	return nil
}

func (c *LedgerCaptureController) readCaptureRecords(ctx context.Context, capture ledgerCaptureRecord, through ledger.Cursor, stoppedAt time.Time) ([]ledger.Record, bool, error) {
	after := ledger.Cursor(capture.AfterCursor)
	result := make([]ledger.Record, 0)
	truncated := false
	cutoff := capture.StartedAt.Add(capture.Spec.Duration)
	if stoppedAt.Before(cutoff) {
		cutoff = stoppedAt
	}
	families := make(map[string]struct{}, len(capture.Spec.SignalFamilies))
	for _, family := range capture.Spec.SignalFamilies {
		families[family] = struct{}{}
	}
	for after < through {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		batch, err := c.ledger.ReadAfter(after, minInt(512, c.maxRecords-len(result)+1))
		if err != nil {
			return nil, false, domain.NewError(domain.CodeUnavailable, "orchestration.ledger_capture.read", "ledger", "could not read capture window", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, item := range batch {
			if item.Cursor > through {
				break
			}
			after = item.Cursor
			if item.Identity.LeaseID != capture.LeaseID || item.ObservedWallUnixNano > cutoff.UnixNano() {
				continue
			}
			if item.Kind != ledger.RecordGap {
				if _, wanted := families[item.SignalFamily]; !wanted {
					continue
				}
			}
			if len(result) >= c.maxRecords {
				truncated = true
				continue
			}
			result = append(result, item)
		}
	}
	return result, truncated, nil
}

func marshalCaptureDocument(document captureDocument, maximum int64) ([]byte, error) {
	for {
		encoded, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		if int64(len(encoded)) <= maximum {
			return encoded, nil
		}
		if len(document.Records) == 0 {
			return nil, domain.NewError(domain.CodeResourceExhausted, "orchestration.ledger_capture.encode", "byte_limit", "is too small for capture metadata", nil)
		}
		document.Records = document.Records[:len(document.Records)-1]
		document.Truncated = true
	}
}

func captureByteLimit(spec ledgerCaptureSpec, configured int64) int64 {
	if spec.ByteLimit < uint64(configured) {
		return int64(spec.ByteLimit)
	}
	return configured
}

func sameCaptureStart(left, right ledgerCaptureRecord) bool {
	left.StoppedAt, right.StoppedAt = time.Time{}, time.Time{}
	left.Artifacts, right.Artifacts = nil, nil
	left.AfterCursor, right.AfterCursor = 0, 0
	return left.CaptureID == right.CaptureID && left.IdempotencyKey == right.IdempotencyKey && left.LeaseID == right.LeaseID &&
		left.WorkspaceID == right.WorkspaceID && left.AgentWorkspace == right.AgentWorkspace && left.AgentGeneration == right.AgentGeneration &&
		left.StartedAt.Equal(right.StartedAt) && captureSpecsEqual(left.Spec, right.Spec)
}

func captureSpecsEqual(left, right ledgerCaptureSpec) bool {
	if left.Profile != right.Profile || left.Duration != right.Duration || left.ByteLimit != right.ByteLimit || len(left.SignalFamilies) != len(right.SignalFamilies) {
		return false
	}
	for index := range left.SignalFamilies {
		if left.SignalFamilies[index] != right.SignalFamilies[index] {
			return false
		}
	}
	return true
}

func (c *LedgerCaptureController) persist(record ledgerCaptureRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(encoded) > maximumCaptureState {
		return fmt.Errorf("capture state exceeds %d bytes", maximumCaptureState)
	}
	namespace, err := openDurableNamespace(c.root, "records")
	if err != nil {
		return err
	}
	defer namespace.Close()
	return namespace.ReplaceRegularAtomically(record.CaptureID+".json", encoded, 0o600)
}

func (c *LedgerCaptureController) load() error {
	namespace, err := openDurableNamespace(c.root, "records")
	if err != nil {
		return err
	}
	defer namespace.Close()
	if err := cleanupDurableNamespaceStages(namespace); err != nil {
		return err
	}
	names, err := namespace.ListNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("capture state namespace contains unsupported entry %q", name)
		}
		content, err := namespace.ReadRegularBounded(name, maximumCaptureState)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.DisallowUnknownFields()
		var record ledgerCaptureRecord
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("decode capture state %q: %w", name, err)
		}
		if err := requireCaptureJSONEnd(decoder); err != nil {
			return fmt.Errorf("decode capture state %q: %w", name, err)
		}
		if record.CaptureID+".json" != name {
			return fmt.Errorf("capture state %q has mismatched identity", name)
		}
		if _, exists := c.records[record.CaptureID]; exists {
			return fmt.Errorf("duplicate capture state %q", record.CaptureID)
		}
		if err := validateLoadedCapture(record); err != nil {
			return fmt.Errorf("capture state %q: %w", record.CaptureID, err)
		}
		c.records[record.CaptureID] = record
	}
	return nil
}

func validateLoadedCapture(record ledgerCaptureRecord) error {
	if _, err := domain.ParseCaptureID(record.CaptureID); err != nil {
		return err
	}
	if _, err := domain.ParseLeaseID(record.LeaseID); err != nil {
		return err
	}
	if _, err := domain.ParseWorkspaceID(record.WorkspaceID); err != nil {
		return err
	}
	if _, err := domain.ParseAgentWorkspaceID(record.AgentWorkspace); err != nil {
		return err
	}
	if !domain.AgentGeneration(record.AgentGeneration).IsValid() || !domain.IsCanonicalIdempotencyKey(record.IdempotencyKey) || record.StartedAt.IsZero() {
		return fmt.Errorf("capture authority is incomplete")
	}
	if err := validateLedgerCaptureSpec(record.Spec, 0); err != nil {
		return err
	}
	if len(record.Artifacts) > 0 && record.StoppedAt.IsZero() {
		return fmt.Errorf("published capture has no stop time")
	}
	_, err := restoreCaptureArtifacts(record.Artifacts)
	return err
}

func requireCaptureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func storeCaptureArtifacts(values []domain.ArtifactReference) []captureArtifactRecord {
	result := make([]captureArtifactRecord, 0, len(values))
	for _, value := range values {
		spec := value.Spec()
		result = append(result, captureArtifactRecord{Reference: spec.Reference, Digest: spec.Digest.String(), Size: spec.Size, Role: spec.Role, Sensitivity: spec.Sensitivity})
	}
	return result
}

func restoreCaptureArtifacts(values []captureArtifactRecord) ([]domain.ArtifactReference, error) {
	result := make([]domain.ArtifactReference, 0, len(values))
	for index, value := range values {
		digest, err := domain.ParseDigest(value.Digest)
		if err != nil {
			return nil, fmt.Errorf("artifact %d digest: %w", index, err)
		}
		artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: value.Reference, Digest: digest, Size: value.Size, Role: value.Role, Sensitivity: value.Sensitivity})
		if err != nil {
			return nil, fmt.Errorf("artifact %d: %w", index, err)
		}
		result = append(result, artifact)
	}
	return result, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

var _ CaptureController = (*LedgerCaptureController)(nil)
