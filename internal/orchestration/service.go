// Package orchestration composes the logical application core with concrete
// node drivers. It is deliberately the only layer that is allowed to turn a
// successful driver result into a durable control transition.
package orchestration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	stateSource         = "world.orchestration.state"
	stateSourceInstance = "v1"
	stateVersion        = uint32(3)

	defaultMaxTransferBytes = int64(64 << 20)
	defaultMaxExecBytes     = int64(64 << 20)
	defaultMaxADBBytes      = int64(64 << 20)
	defaultMaxStateBytes    = 4 << 20
	defaultControlTimeout   = 10 * time.Second
	defaultStreamBuffer     = 128
	maxFilterValues         = 256
)

// SubjectResolver returns the authenticated policy subject installed by the
// transport. A service without one would be unable to enforce lease scope and
// is therefore rejected by New.
type SubjectResolver func(context.Context) (string, bool)

// Dialer is injected so ADB tunnels are testable and so the service only ever
// dials an endpoint issued by the selected target driver.
type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type CapturePlan struct {
	IdempotencyKey string
	CaptureID      string
	LeaseID        string
	Workspace      WorkspaceScope
	Spec           *worldv1.CaptureSpec
	StartedAt      time.Time
}

type CaptureStopPlan struct {
	IdempotencyKey string
	CaptureID      string
}

// CaptureController owns the actual collector mechanics. Persisting capture
// intent is not evidence that collection started, so state changes only after
// these methods succeed.
type CaptureController interface {
	Start(context.Context, CapturePlan) error
	Stop(context.Context, CaptureStopPlan) ([]domain.ArtifactReference, error)
}

type WorkspaceScope struct {
	WorkspaceID      domain.WorkspaceID
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	AgentState       domain.AgentGenerationState
}

// WorkspaceResolver bridges the current Core read-model gap: Core authorizes a
// lease but does not expose a lease-to-workspace projection. Production export
// composition must inject an authoritative resolver rather than infer paths.
type WorkspaceResolver interface {
	ResolveWorkspace(context.Context, string) (WorkspaceScope, error)
}

func (s *Service) requireWorkspaceScope(ctx context.Context, leaseID string) (WorkspaceScope, error) {
	if s.workspace == nil || s.workspaceScope == nil {
		return WorkspaceScope{}, status.Error(codes.FailedPrecondition, "workspace access requires a workspace driver and authoritative lease workspace resolver")
	}
	scope, err := s.workspaceScope.ResolveWorkspace(ctx, leaseID)
	if err != nil {
		return WorkspaceScope{}, err
	}
	if scope.WorkspaceID.IsZero() || scope.AgentWorkspaceID.IsZero() || !scope.AgentGeneration.IsValid() || !scope.AgentState.IsValid() {
		return WorkspaceScope{}, status.Error(codes.DataLoss, "workspace resolver returned incomplete scope")
	}
	return scope, nil
}

func requireWritableWorkspaceScope(scope WorkspaceScope) error {
	if scope.AgentState != domain.AgentGenerationReady && scope.AgentState != domain.AgentGenerationRunning {
		return status.Errorf(codes.FailedPrecondition, "agent workspace generation in %s is not writable", scope.AgentState)
	}
	return nil
}

type Config struct {
	Core            *application.Core
	Ledger          *ledger.Ledger
	Finalization    *application.RunFinalizationService
	Agent           ports.AgentWorkspaceDriver
	Targets         map[domain.TargetKind]ports.TargetDriver
	Workspace       ports.WorkspaceDriver
	WorkspaceScope  WorkspaceResolver
	Material        ports.MaterialAuthority
	Captures        CaptureController
	PolicyAdmission LeaseOperationPolicyAdmission
	Observers       *RunObserverCoordinator
	CaptureProfiles map[string]*worldv1.CaptureSpec
	Subject         SubjectResolver
	Dialer          Dialer
	IDs             *domain.IDGenerator
	Clock           func() time.Time
	StateRoot       string

	MaxTransferBytes int64
	MaxExecBytes     int64
	MaxADBBytes      int64
	MaxStateBytes    int
	ControlTimeout   time.Duration
	StreamBuffer     int
	AllowRemoteADB   bool
}

// Service is safe for concurrent RPC use. The mutex only protects compact
// control indexes; the ledger and drivers provide their own concurrency.
type Service struct {
	worldv1.UnimplementedWorldServiceServer

	core            *application.Core
	ledger          *ledger.Ledger
	finalization    *application.RunFinalizationService
	agent           ports.AgentWorkspaceDriver
	targets         map[domain.TargetKind]ports.TargetDriver
	workspace       ports.WorkspaceDriver
	workspaceScope  WorkspaceResolver
	material        ports.MaterialAuthority
	captures        CaptureController
	policyAdmission LeaseOperationPolicyAdmission
	observers       *RunObserverCoordinator
	profiles        map[string]*worldv1.CaptureSpec
	subject         SubjectResolver
	dialer          Dialer
	ids             *domain.IDGenerator
	clock           func() time.Time
	stateRoot       string

	maxTransferBytes   int64
	maxExecBytes       int64
	maxADBBytes        int64
	maxStateBytes      int
	controlTimeout     time.Duration
	streamBuffer       int
	allowRemoteADB     bool
	finalizationFaults *finalizationFaultHooks

	mu                     sync.RWMutex
	captureState           map[string]captureRecord
	exportState            map[string]exportRecord
	bundles                map[string]bundleRecord
	reservations           map[string]bundleReservation
	stopPreparations       map[string]stagedBundleStopPreparation
	stopPreparationRecords map[string]bundleStopPreparationRecord
	publications           map[string]stagedBundlePublication
	publicationRecords     map[string]bundlePublicationRecord
	completions            map[string]bundleCompletion
	operations             map[string]operationReservation
	idempotency            map[string]idempotencyRecord
}

// finalizationFaultHooks are package-private deterministic crash boundaries
// used by restart tests. Production compositions never set them.
type finalizationFaultHooks struct {
	afterBundleReserved   func() error
	afterStopPrepared     func() error
	afterFailureIncident  func() error
	afterEvidencePrepared func() error
	afterPublicationStage func() error
	afterCoreCommit       func() error
	afterBundleFile       func() error
	afterBundleIndexed    func() error
}

type captureRecord struct {
	Capture *worldv1.Capture     `json:"capture"`
	Spec    *worldv1.CaptureSpec `json:"spec"`
	Scope   leaseOperationScope  `json:"scope"`
}

type exportRecord struct {
	Export *worldv1.Export     `json:"export"`
	Scope  leaseOperationScope `json:"scope"`
}

// leaseOperationScope binds a durable capture/export saga to the exact agent
// generation and effective-policy pair admitted before its first mutation.
type leaseOperationScope struct {
	PolicyDigest     string `json:"policy_digest"`
	CapabilityDigest string `json:"capability_digest"`
	WorkspaceID      string `json:"workspace_id"`
	AgentWorkspaceID string `json:"agent_workspace_id"`
	AgentGeneration  uint64 `json:"agent_generation"`
}

type bundleRecord struct {
	LeaseID    string `json:"lease_id"`
	TargetID   string `json:"target_id"`
	RunID      string `json:"run_id"`
	BundleID   string `json:"bundle_id"`
	File       string `json:"file"`
	WireDigest string `json:"wire_digest"`
	Size       int64  `json:"size"`
}

type bundleReservation struct {
	Namespace      string `json:"namespace"`
	LeaseID        string `json:"lease_id"`
	TargetID       string `json:"target_id"`
	RunID          string `json:"run_id"`
	BundleID       string `json:"bundle_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Signature      string `json:"signature"`
}

const bundleStopPreparationVersion = uint32(2)

type runFailureIncidentIntent struct {
	Classification domain.IncidentClassification `json:"classification"`
	Trigger        string                        `json:"trigger"`
	Cause          application.CauseRecord       `json:"cause"`
}

// stagedBundleStopPreparation is the first evidence-bearing durable handoff.
// It is anchored before an incident, run transition, local seal, or material
// publication is allowed, so every later retry consumes byte-identical input.
type stagedBundleStopPreparation struct {
	Version          uint32                    `json:"version"`
	Reservation      bundleReservation         `json:"reservation"`
	Meta             application.MutationMeta  `json:"meta"`
	InitialRunState  domain.TargetRunState     `json:"initial_run_state"`
	InitialRevision  uint64                    `json:"initial_revision"`
	TargetGeneration uint64                    `json:"target_generation"`
	AgentWorkspaceID string                    `json:"agent_workspace_id"`
	AgentGeneration  uint64                    `json:"agent_generation"`
	RunCreatedAt     time.Time                 `json:"run_created_at"`
	RequiredCoverage []string                  `json:"required_coverage"`
	Result           persistedTargetRunResult  `json:"result"`
	Incident         *runFailureIncidentIntent `json:"incident,omitempty"`
	ObserverDigest   string                    `json:"observer_digest"`
}

type bundleStopPreparationRecord struct {
	LeaseID        string `json:"lease_id"`
	TargetID       string `json:"target_id"`
	RunID          string `json:"run_id"`
	BundleID       string `json:"bundle_id"`
	File           string `json:"file"`
	WireDigest     string `json:"wire_digest"`
	ObserverDigest string `json:"observer_digest"`
	Size           int64  `json:"size"`
}

const bundlePublicationVersion = uint32(2)

// bundlePublication is an independently durable hand-off between immutable
// evidence publication and the terminal Core mutation. Its canonical file is
// retained so a restart can reproduce the exact public wire bundle without
// rebuilding or inventing evidence.
type stagedBundlePublication struct {
	Version     uint32                               `json:"version"`
	Reservation bundleReservation                    `json:"reservation"`
	Commit      application.FinalizeTargetRunRequest `json:"commit"`
	Artifact    application.IncidentArtifactRecord   `json:"artifact"`
	Bundle      *worldv1.ObservationBundle           `json:"bundle"`
}

type bundlePublicationRecord struct {
	LeaseID    string `json:"lease_id"`
	TargetID   string `json:"target_id"`
	RunID      string `json:"run_id"`
	BundleID   string `json:"bundle_id"`
	File       string `json:"file"`
	WireDigest string `json:"wire_digest"`
	Size       int64  `json:"size"`
}

// bundleCompletion is written only after the indexed wire bundle and the run
// observer's committed marker have both been verified.
type bundleCompletion struct {
	RunID      string `json:"run_id"`
	BundleID   string `json:"bundle_id"`
	WireDigest string `json:"wire_digest"`
}

// operationReservation is the durable ownership boundary for an irreversible
// service operation. A resource may have only one reservation per namespace;
// retries must present the exact original key and request signature.
type operationReservation struct {
	Namespace      string `json:"namespace"`
	ResourceID     string `json:"resource_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Signature      string `json:"signature"`
}

type idempotencyRecord struct {
	Signature  string `json:"signature"`
	ResourceID string `json:"resource_id"`
}

type stateEvent struct {
	Version         uint32                       `json:"version"`
	Kind            string                       `json:"kind"`
	Namespace       string                       `json:"namespace,omitempty"`
	IdempotencyKey  string                       `json:"idempotency_key,omitempty"`
	Signature       string                       `json:"signature,omitempty"`
	Capture         *captureRecord               `json:"capture,omitempty"`
	Export          *exportRecord                `json:"export,omitempty"`
	Bundle          *bundleRecord                `json:"bundle,omitempty"`
	Reservation     *bundleReservation           `json:"reservation,omitempty"`
	Publication     *bundlePublicationRecord     `json:"publication,omitempty"`
	StopPreparation *bundleStopPreparationRecord `json:"stop_preparation,omitempty"`
	Completion      *bundleCompletion            `json:"completion,omitempty"`
	Operation       *operationReservation        `json:"operation,omitempty"`
}

func New(config Config) (*Service, error) {
	if config.Core == nil || config.Ledger == nil {
		return nil, fmt.Errorf("application core and observation ledger are required")
	}
	if config.Subject == nil {
		return nil, fmt.Errorf("authenticated subject resolver is required")
	}
	if (config.Captures != nil || config.Agent != nil && config.Workspace != nil && config.Material != nil) && config.PolicyAdmission == nil {
		return nil, fmt.Errorf("capture/export capabilities require effective-policy admission")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.IDs == nil {
		generator, err := domain.NewIDGenerator(config.Clock, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("create orchestration ID generator: %w", err)
		}
		config.IDs = generator
	}
	if config.Dialer == nil {
		config.Dialer = &net.Dialer{}
	}
	if config.MaxTransferBytes <= 0 {
		config.MaxTransferBytes = defaultMaxTransferBytes
	}
	if config.MaxExecBytes <= 0 {
		config.MaxExecBytes = defaultMaxExecBytes
	}
	if config.MaxADBBytes <= 0 {
		config.MaxADBBytes = defaultMaxADBBytes
	}
	if config.MaxStateBytes <= 0 {
		config.MaxStateBytes = defaultMaxStateBytes
	}
	config.ControlTimeout = configuredControlTimeout(config.ControlTimeout)
	if config.StreamBuffer <= 0 {
		config.StreamBuffer = defaultStreamBuffer
	}
	stateRoot := strings.TrimSpace(config.StateRoot)
	if stateRoot == "" {
		return nil, fmt.Errorf("orchestration state root is required")
	}
	absoluteStateRoot, err := filepath.Abs(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve orchestration state root: %w", err)
	}
	if err := os.MkdirAll(filepath.Clean(absoluteStateRoot), 0o700); err != nil {
		return nil, fmt.Errorf("create orchestration state root: %w", err)
	}
	targets := make(map[domain.TargetKind]ports.TargetDriver, len(config.Targets))
	for kind, driver := range config.Targets {
		if !kind.IsValid() || driver == nil {
			return nil, fmt.Errorf("target driver map contains an invalid kind or nil driver")
		}
		targets[kind] = driver
	}
	profiles := make(map[string]*worldv1.CaptureSpec, len(config.CaptureProfiles))
	for name, profile := range config.CaptureProfiles {
		if strings.TrimSpace(name) == "" || profile == nil {
			return nil, fmt.Errorf("capture profile name and specification are required")
		}
		profiles[name] = cloneCaptureSpec(profile)
	}
	service := &Service{
		core: config.Core, ledger: config.Ledger, finalization: config.Finalization,
		agent: config.Agent, targets: targets, workspace: config.Workspace, workspaceScope: config.WorkspaceScope,
		material: config.Material, captures: config.Captures, policyAdmission: config.PolicyAdmission, observers: config.Observers, profiles: profiles,
		subject: config.Subject, dialer: config.Dialer, ids: config.IDs,
		clock: config.Clock, stateRoot: filepath.Clean(absoluteStateRoot),
		maxTransferBytes: config.MaxTransferBytes, maxExecBytes: config.MaxExecBytes,
		maxADBBytes: config.MaxADBBytes, maxStateBytes: config.MaxStateBytes,
		controlTimeout: config.ControlTimeout, streamBuffer: config.StreamBuffer,
		allowRemoteADB: config.AllowRemoteADB,
		captureState:   make(map[string]captureRecord), exportState: make(map[string]exportRecord),
		bundles: make(map[string]bundleRecord), reservations: make(map[string]bundleReservation),
		stopPreparations: make(map[string]stagedBundleStopPreparation), stopPreparationRecords: make(map[string]bundleStopPreparationRecord),
		publications: make(map[string]stagedBundlePublication), publicationRecords: make(map[string]bundlePublicationRecord),
		completions: make(map[string]bundleCompletion),
		operations:  make(map[string]operationReservation), idempotency: make(map[string]idempotencyRecord),
	}
	if err := service.replayState(); err != nil {
		return nil, fmt.Errorf("replay orchestration state: %w", err)
	}
	if err := service.loadBundleStopPreparations(); err != nil {
		return nil, fmt.Errorf("load stopped-run preparations: %w", err)
	}
	if err := service.loadBundlePublications(); err != nil {
		return nil, fmt.Errorf("load staged observation bundles: %w", err)
	}
	if err := service.reconcileBundleFiles(); err != nil {
		return nil, fmt.Errorf("reconcile observation bundle files: %w", err)
	}
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), config.ControlTimeout)
	defer verifyCancel()
	if err := service.verifyOperationIndexes(verifyCtx); err != nil {
		return nil, fmt.Errorf("verify persisted operation reservations: %w", err)
	}
	if err := service.verifyBundleIndexes(verifyCtx); err != nil {
		return nil, fmt.Errorf("verify persisted observation bundles: %w", err)
	}
	return service, nil
}

func configuredControlTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultControlTimeout
	}
	return value
}

func (s *Service) replayState() error {
	records, err := s.ledger.ReadAfter(0, 0)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Kind != ledger.RecordControl || record.Source != stateSource || record.SourceInstance != stateSourceInstance {
			continue
		}
		if len(record.Payload) == 0 || len(record.Payload) > s.maxStateBytes {
			return fmt.Errorf("state record at cursor %d exceeds bounds", record.Cursor)
		}
		var event stateEvent
		decoder := json.NewDecoder(bytes.NewReader(record.Payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return fmt.Errorf("decode state cursor %d: %w", record.Cursor, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return fmt.Errorf("decode state cursor %d: trailing JSON value", record.Cursor)
			}
			return fmt.Errorf("decode state cursor %d trailing JSON: %w", record.Cursor, err)
		}
		if event.Version != stateVersion {
			return fmt.Errorf("state cursor %d has unsupported version %d", record.Cursor, event.Version)
		}
		if err := s.applyStateEvent(event); err != nil {
			return fmt.Errorf("apply state cursor %d: %w", record.Cursor, err)
		}
	}
	return nil
}

func (s *Service) applyStateEvent(event stateEvent) error {
	if err := validateStateEventIdempotency(event); err != nil {
		return err
	}
	if event.IdempotencyKey != "" {
		if event.Namespace == "" || event.Signature == "" {
			return fmt.Errorf("idempotent state event is incomplete")
		}
		resourceID := ""
		switch {
		case event.Capture != nil:
			if event.Capture.Capture == nil {
				return fmt.Errorf("capture state event has no capture")
			}
			resourceID = event.Capture.Capture.CaptureId
		case event.Export != nil:
			if event.Export.Export == nil {
				return fmt.Errorf("export state event has no export")
			}
			resourceID = event.Export.Export.ExportId
		case event.Reservation != nil:
			resourceID = event.Reservation.BundleID
		case event.Operation != nil:
			resourceID = event.Operation.ResourceID
		}
		if resourceID == "" {
			return fmt.Errorf("idempotent state event has no resource identity")
		}
		if event.Operation != nil && event.Operation.ResourceID != resourceID {
			return fmt.Errorf("operation reservation resource does not match its state event")
		}
		if previous, found := s.idempotency[idempotencyIndex(event.Namespace, event.IdempotencyKey)]; found && (previous.Signature != event.Signature || previous.ResourceID != resourceID) {
			return fmt.Errorf("idempotency index conflicts with an earlier state event")
		}
		s.idempotency[idempotencyIndex(event.Namespace, event.IdempotencyKey)] = idempotencyRecord{Signature: event.Signature, ResourceID: resourceID}
	}
	if event.Operation != nil {
		reservation := *event.Operation
		if reservation.Namespace == "" || reservation.ResourceID == "" || !domain.IsCanonicalIdempotencyKey(reservation.IdempotencyKey) || reservation.Signature == "" {
			return fmt.Errorf("operation reservation is incomplete")
		}
		if reservation.Namespace != event.Namespace || reservation.IdempotencyKey != event.IdempotencyKey || reservation.Signature != event.Signature {
			return fmt.Errorf("operation reservation does not match its state envelope")
		}
		index := operationIndex(reservation.Namespace, reservation.ResourceID)
		if previous, found := s.operations[index]; found && previous != reservation {
			return fmt.Errorf("operation reservation conflicts with an earlier state event")
		}
		s.operations[index] = reservation
	}
	switch event.Kind {
	case "capture.upserted":
		if event.Capture == nil || event.Capture.Capture == nil || event.Capture.Spec == nil || event.Capture.Capture.CaptureId == "" || event.Capture.Capture.LeaseId == "" {
			return fmt.Errorf("capture event is incomplete")
		}
		if err := event.Capture.Scope.validate(); err != nil {
			return fmt.Errorf("capture scope is invalid: %w", err)
		}
		s.captureState[event.Capture.Capture.CaptureId] = cloneCaptureRecord(*event.Capture)
	case "export.upserted":
		if event.Export == nil || event.Export.Export == nil || event.Export.Export.ExportId == "" || event.Export.Export.LeaseId == "" {
			return fmt.Errorf("export event is incomplete")
		}
		if err := event.Export.Scope.validate(); err != nil {
			return fmt.Errorf("export scope is invalid: %w", err)
		}
		s.exportState[event.Export.Export.ExportId] = cloneExportRecord(*event.Export)
	case "bundle.reserved":
		if event.Reservation == nil || event.Reservation.Namespace != event.Namespace || event.Reservation.IdempotencyKey != event.IdempotencyKey || event.Reservation.Signature != event.Signature {
			return fmt.Errorf("bundle reservation is incomplete")
		}
		if err := validateBundleReservation(*event.Reservation); err != nil {
			return err
		}
		if previous, found := s.reservations[event.Reservation.RunID]; found && previous != *event.Reservation {
			return fmt.Errorf("bundle reservation conflicts with an earlier state event")
		}
		s.reservations[event.Reservation.RunID] = *event.Reservation
	case "bundle.stop_prepared":
		if event.StopPreparation == nil {
			return fmt.Errorf("bundle stop preparation is incomplete")
		}
		if err := validateBundleStopPreparationRecord(*event.StopPreparation, s.maxBundlePublicationBytes()); err != nil {
			return err
		}
		reservation, found := s.reservations[event.StopPreparation.RunID]
		if !found || reservation.LeaseID != event.StopPreparation.LeaseID || reservation.TargetID != event.StopPreparation.TargetID || reservation.BundleID != event.StopPreparation.BundleID {
			return fmt.Errorf("bundle stop preparation has no exact durable reservation")
		}
		if previous, found := s.stopPreparationRecords[event.StopPreparation.RunID]; found && previous != *event.StopPreparation {
			return fmt.Errorf("bundle stop preparation conflicts with an earlier state event")
		}
		s.stopPreparationRecords[event.StopPreparation.RunID] = *event.StopPreparation
	case "bundle.indexed":
		if event.Bundle == nil {
			return fmt.Errorf("bundle index is incomplete")
		}
		if err := validateBundleRecord(*event.Bundle, s.maxTransferBytes); err != nil {
			return err
		}
		if previous, found := s.bundles[event.Bundle.RunID]; found && previous != *event.Bundle {
			return fmt.Errorf("bundle index conflicts with an earlier state event")
		}
		s.bundles[event.Bundle.RunID] = *event.Bundle
	case "bundle.publication_staged":
		if event.Publication == nil {
			return fmt.Errorf("bundle publication stage is incomplete")
		}
		if err := validateBundlePublicationRecord(*event.Publication, s.maxBundlePublicationBytes()); err != nil {
			return err
		}
		reservation, found := s.reservations[event.Publication.RunID]
		if !found || reservation.LeaseID != event.Publication.LeaseID || reservation.TargetID != event.Publication.TargetID || reservation.BundleID != event.Publication.BundleID {
			return fmt.Errorf("bundle publication stage has no exact durable reservation")
		}
		if previous, found := s.publicationRecords[event.Publication.RunID]; found && previous != *event.Publication {
			return fmt.Errorf("bundle publication stage conflicts with an earlier state event")
		}
		s.publicationRecords[event.Publication.RunID] = *event.Publication
	case "bundle.completed":
		if event.Completion == nil || event.Completion.RunID == "" || event.Completion.BundleID == "" || event.Completion.WireDigest == "" {
			return fmt.Errorf("bundle completion is incomplete")
		}
		if _, err := domain.ParseTargetRunID(event.Completion.RunID); err != nil {
			return fmt.Errorf("bundle completion run id: %w", err)
		}
		if _, err := domain.ParseObservationBundleID(event.Completion.BundleID); err != nil {
			return fmt.Errorf("bundle completion bundle id: %w", err)
		}
		if _, err := domain.ParseDigest(event.Completion.WireDigest); err != nil {
			return fmt.Errorf("bundle completion wire digest: %w", err)
		}
		if previous, found := s.completions[event.Completion.RunID]; found && previous != *event.Completion {
			return fmt.Errorf("bundle completion conflicts with an earlier state event")
		}
		s.completions[event.Completion.RunID] = *event.Completion
	case "operation.reserved":
		if event.Operation == nil {
			return fmt.Errorf("operation reservation event is incomplete")
		}
	default:
		return fmt.Errorf("unknown state event kind %q", event.Kind)
	}
	return nil
}

func (s *Service) persistStateLocked(ctx context.Context, event stateEvent, identity ledger.Identity) error {
	if err := validateStateEventIdempotency(event); err != nil {
		return err
	}
	event.Version = stateVersion
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(payload) > s.maxStateBytes {
		return status.Errorf(codes.ResourceExhausted, "orchestration state exceeds %d bytes", s.maxStateBytes)
	}
	_, err = s.ledger.Append(ctx, ledger.Record{
		Kind: ledger.RecordControl, Identity: identity, Source: stateSource,
		SourceInstance: stateSourceInstance, ObservedWallUnixNano: s.clock().UTC().UnixNano(),
		Origin: ledger.OriginSystem, Payload: payload,
	})
	if err != nil {
		return err
	}
	return s.applyStateEvent(event)
}

func validateStateEventIdempotency(event stateEvent) error {
	if event.IdempotencyKey == "" {
		return nil
	}
	if !domain.IsCanonicalIdempotencyKey(event.IdempotencyKey) {
		return fmt.Errorf("state event idempotency key is invalid")
	}
	if event.Reservation != nil && !domain.IsCanonicalIdempotencyKey(event.Reservation.IdempotencyKey) {
		return fmt.Errorf("bundle reservation idempotency key is invalid")
	}
	if event.Operation != nil && !domain.IsCanonicalIdempotencyKey(event.Operation.IdempotencyKey) {
		return fmt.Errorf("operation reservation idempotency key is invalid")
	}
	return nil
}

func (s *Service) authorize(ctx context.Context, policy string, request application.AuthorizationRequest) error {
	subject, ok := s.subject(ctx)
	if !ok || strings.TrimSpace(subject) == "" {
		return status.Error(codes.Unauthenticated, "authenticated subject is unavailable")
	}
	request.Subject = subject
	request.PolicyReference = policy
	if err := s.core.Authorize(ctx, request); err != nil {
		if errors.Is(err, application.ErrNotFound) || errors.Is(err, application.ErrScope) {
			// Do not reveal whether another subject owns the identifier.
			return status.Error(codes.PermissionDenied, "resource is outside the authenticated scope")
		}
		return err
	}
	return nil
}

func mutationContext(parent context.Context, value *worldv1.MutationMetadata) (context.Context, context.CancelFunc, application.MutationMeta, error) {
	if value == nil {
		return nil, nil, application.MutationMeta{}, status.Error(codes.InvalidArgument, "mutation metadata is required")
	}
	declaredDeadline, err := nativeTimestamp(value.Deadline, "mutation.deadline", true)
	if err != nil {
		return nil, nil, application.MutationMeta{}, status.Error(codes.InvalidArgument, err.Error())
	}
	meta := application.MutationMeta{
		IdempotencyKey: value.IdempotencyKey, CorrelationID: value.CorrelationId,
		CausationID: value.CausationId, AuthorizedPolicyReference: value.AuthorizedPolicyReference,
		Deadline: declaredDeadline,
	}
	ctx, cancel := context.WithDeadline(parent, declaredDeadline)
	if err := meta.Validate(ctx, time.Now()); err != nil {
		cancel()
		return nil, nil, application.MutationMeta{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return ctx, cancel, meta, nil
}

func childMeta(meta application.MutationMeta, suffix string, deadline time.Time) application.MutationMeta {
	meta.IdempotencyKey = domain.DeriveIdempotencyKey(meta.IdempotencyKey, suffix)
	meta.Deadline = deadline
	return meta
}

func cleanupContext(timeout time.Duration) (context.Context, context.CancelFunc, time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	deadline, _ := ctx.Deadline()
	return ctx, cancel, deadline
}

func requestSignature(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (s *Service) existingIdempotencyLocked(namespace, key, signature string) (string, bool, error) {
	record, found := s.idempotency[idempotencyIndex(namespace, key)]
	if !found {
		return "", false, nil
	}
	if record.Signature != signature {
		return "", true, status.Error(codes.AlreadyExists, "idempotency key was reused with a different request")
	}
	return record.ResourceID, true, nil
}

func idempotencyIndex(namespace, key string) string { return namespace + "\x00" + key }

func operationIndex(namespace, resourceID string) string { return namespace + "\x00" + resourceID }

func cloneCaptureSpec(value *worldv1.CaptureSpec) *worldv1.CaptureSpec {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*worldv1.CaptureSpec)
}

func cloneCaptureRecord(value captureRecord) captureRecord {
	value.Spec = cloneCaptureSpec(value.Spec)
	if value.Capture != nil {
		value.Capture = proto.Clone(value.Capture).(*worldv1.Capture)
	}
	return value
}

func cloneExportRecord(value exportRecord) exportRecord {
	if value.Export != nil {
		value.Export = proto.Clone(value.Export).(*worldv1.Export)
	}
	return value
}

func targetGeneration(value application.TargetRecord) (application.TargetGenerationRecord, error) {
	for _, generation := range value.Generations {
		if generation.Generation == value.CurrentGeneration {
			return generation, nil
		}
	}
	return application.TargetGenerationRecord{}, status.Error(codes.DataLoss, "target current generation is missing")
}

func targetRun(value application.TargetRecord, runID string) (application.TargetRunRecord, error) {
	for _, run := range value.Runs {
		if run.ID == runID {
			return run, nil
		}
	}
	return application.TargetRunRecord{}, status.Error(codes.NotFound, "target run not found")
}

func (s *Service) scopedTarget(ctx context.Context, targetID, runID, policy string) (application.TargetRecord, application.TargetRunRecord, ports.TargetDriver, error) {
	if _, err := domain.ParseTargetID(targetID); err != nil {
		return application.TargetRecord{}, application.TargetRunRecord{}, nil, status.Error(codes.InvalidArgument, "valid target_id is required")
	}
	if err := s.authorize(ctx, policy, application.AuthorizationRequest{TargetID: targetID}); err != nil {
		return application.TargetRecord{}, application.TargetRunRecord{}, nil, err
	}
	target, err := s.core.GetTarget(ctx, targetID)
	if err != nil {
		return application.TargetRecord{}, application.TargetRunRecord{}, nil, err
	}
	run, err := targetRun(target, runID)
	if err != nil {
		return application.TargetRecord{}, application.TargetRunRecord{}, nil, err
	}
	driver := s.targets[target.Kind]
	if driver == nil {
		return application.TargetRecord{}, application.TargetRunRecord{}, nil, status.Errorf(codes.FailedPrecondition, "no production target driver is configured for kind %q", target.Kind)
	}
	return target, run, driver, nil
}

func deadline(ctx context.Context) time.Time {
	value, _ := ctx.Deadline()
	return value
}

var _ worldv1.WorldServiceServer = (*Service)(nil)
