package orchestration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const (
	observerStateVersion               = uint32(6)
	observerStateSource                = "world.run-observer-coordinator"
	maximumObserverStateMarkerBytes    = int64(4 << 20)
	defaultMaximumObserverJournalBytes = int64(64 << 20)
)

type RunObserverStart struct {
	Plan                        ports.TargetRunPlan
	Prepared                    ports.PreparedTargetRun
	TargetKind                  domain.TargetKind
	ResearchSessionID           domain.ResearchSessionID
	PolicyDigest                domain.Digest
	CapabilityFingerprintDigest domain.Digest
}

type RunObserverStop struct {
	RunID           domain.TargetRunID
	TargetStoppedAt time.Time
}

// PersistedRunObserverBinding is the durable control-plane identity used to
// match observer markers before any interrupted run is recovered. PlanDigest
// identifies the exact persisted TargetRunPlan, not a newly resolved variant.
type PersistedRunObserverBinding struct {
	RunID           domain.TargetRunID
	PlanDigest      domain.Digest
	State           domain.TargetRunState
	BundlePublished bool
}

type ObserverFailure struct {
	CollectorID domain.CollectorID
	Family      string
	Required    bool
	Reason      string
}

type RunObservationEvidence struct {
	Required    []string
	FirstCursor domain.ObservationCursor
	LastCursor  domain.ObservationCursor
	Artifacts   []domain.ArtifactReference
	Events      []domain.EventEnvelope
	Metrics     []domain.MetricSample
	Coverage    []domain.CollectorCoverage
	Gaps        []domain.Gap
	StoppedAt   time.Time
	Failures    []ObserverFailure
}

type RunObserverCoordinatorConfig struct {
	Driver          ports.ObserverDriver
	Ledger          *ledger.Ledger
	IDs             *domain.IDGenerator
	Clock           func() time.Time
	StateRoot       string
	CleanupTimeout  time.Duration
	MaxJournalBytes int64
}

// RunObserverCoordinator owns the boundary between target readiness and
// collector evidence. Its small durable marker journal intentionally fails
// startup closed after an interrupted physical run; process and target drivers
// must reconcile their external resources before that run can be adopted.
type RunObserverCoordinator struct {
	driver          ports.ObserverDriver
	ledger          *ledger.Ledger
	ids             *domain.IDGenerator
	clock           func() time.Time
	stateRoot       string
	cleanupTimeout  time.Duration
	maxJournalBytes int64

	mu      sync.Mutex
	records map[string]*runObserverRecord
}

type runObserverRecord struct {
	start                 RunObserverStart
	signature             string
	planDigest            domain.Digest
	crashCleanup          bool
	externalOwnership     bool
	plans                 []ports.CollectorPlan
	started               map[string]bool
	startCommitted        map[string]bool
	coverage              map[string]domain.CollectorCoverage
	artifacts             []domain.ArtifactReference
	events                []domain.EventEnvelope
	metrics               []domain.MetricSample
	gaps                  []domain.Gap
	failures              []ObserverFailure
	first                 domain.ObservationCursor
	last                  domain.ObservationCursor
	stoppedAt             time.Time
	phase                 string
	timer                 *time.Timer
	timerGeneration       uint64
	stopDeadline          time.Time
	stopResumePhase       string
	stopResult            *RunObservationEvidence
	targetEventIDs        []domain.EventID
	targetAppended        int
	intrinsicID           domain.CollectorID
	intrinsicStartedAt    time.Time
	intrinsicAppended     bool
	receiptSignature      string
	stoppedResultDigest   string
	stopPreparationDigest string
	journal               observerEvidenceJournal
	journalRef            *observerJournalReference
	journalDirty          bool
	obsoleteJournalRefs   []observerJournalReference
}

type observerStateMarker struct {
	Version               uint32                              `json:"version"`
	RunID                 string                              `json:"run_id"`
	PlanDigest            string                              `json:"plan_digest"`
	Signature             string                              `json:"signature"`
	Phase                 string                              `json:"phase"`
	CrashCleanup          bool                                `json:"crash_cleanup_guaranteed"`
	ExternalOwnership     bool                                `json:"external_ownership_possible"`
	Collectors            []ports.InterruptedCollectorBinding `json:"collectors"`
	IntrinsicID           string                              `json:"intrinsic_collector_id,omitempty"`
	IntrinsicStartedAt    time.Time                           `json:"intrinsic_started_at,omitempty"`
	StoppedResultDigest   string                              `json:"stopped_result_digest,omitempty"`
	StopPreparationDigest string                              `json:"stop_preparation_digest,omitempty"`
	Journal               *observerJournalReference           `json:"journal,omitempty"`
	UpdatedAt             time.Time                           `json:"updated_at"`
}

func NewRunObserverCoordinator(config RunObserverCoordinatorConfig) (*RunObserverCoordinator, error) {
	if config.Ledger == nil {
		return nil, fmt.Errorf("observation ledger is required")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.IDs == nil {
		ids, err := domain.NewIDGenerator(config.Clock, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("create observer ID generator: %w", err)
		}
		config.IDs = ids
	}
	if config.CleanupTimeout <= 0 {
		config.CleanupTimeout = defaultControllerCleanupTimeout
	}
	if config.MaxJournalBytes <= 0 {
		config.MaxJournalBytes = defaultMaximumObserverJournalBytes
	}
	stateRoot := strings.TrimSpace(config.StateRoot)
	if stateRoot == "" {
		return nil, fmt.Errorf("observer state root is required")
	}
	root, err := filepath.Abs(stateRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve observer state root: %w", err)
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create observer state root: %w", err)
	}
	for _, name := range []string{"runs", "journals", "artifacts"} {
		namespace, err := openDurableNamespace(root, name)
		if err != nil {
			return nil, fmt.Errorf("initialize observer %s namespace: %w", name, err)
		}
		cleanupErr := cleanupDurableNamespaceStages(namespace)
		closeErr := namespace.Close()
		if cleanupErr != nil || closeErr != nil {
			return nil, fmt.Errorf("initialize observer %s namespace: %w", name, errors.Join(cleanupErr, closeErr))
		}
	}
	return &RunObserverCoordinator{
		driver: config.Driver, ledger: config.Ledger, ids: config.IDs, clock: config.Clock,
		stateRoot: root, cleanupTimeout: config.CleanupTimeout, maxJournalBytes: config.MaxJournalBytes,
		records: make(map[string]*runObserverRecord),
	}, nil
}

// Reconcile is the final startup gate. Persisted run reconciliation must have
// explicitly recovered and committed every interrupted lifecycle first.
func (c *RunObserverCoordinator) Reconcile(context.Context) error {
	markers, err := c.loadMarkers()
	if err != nil {
		return err
	}
	if err := c.pruneUnreferencedObserverJournals(markers); err != nil {
		return err
	}
	var unresolved []string
	for _, marker := range markers {
		if marker.Phase != "committed" {
			unresolved = append(unresolved, marker.RunID+"("+marker.Phase+")")
		}
	}
	if len(unresolved) > 0 {
		sort.Strings(unresolved)
		return domain.NewError(domain.CodeFailedPrecondition, "run_observers.reconcile", "runs", "interrupted physical runs require explicit target/process reconciliation: "+strings.Join(unresolved, ", "), nil)
	}
	return nil
}

// ReconcilePersistedRuns matches every marker to a durable run and its exact
// persisted plan digest before physical recovery begins. A stopped marker may
// be committed directly only when the durable run is already terminal, which
// covers a crash after bundle finalization but before marker commit.
func (c *RunObserverCoordinator) ReconcilePersistedRuns(ctx context.Context, bindings []PersistedRunObserverBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	bound := make(map[string]PersistedRunObserverBinding, len(bindings))
	for index, binding := range bindings {
		if binding.RunID.IsZero() || binding.PlanDigest.IsZero() || !binding.State.IsValid() {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", fmt.Sprintf("bindings[%d]", index), "durable observer binding is invalid", nil)
		}
		key := binding.RunID.String()
		if _, duplicate := bound[key]; duplicate {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "bindings", "durable observer bindings contain a duplicate run", nil)
		}
		bound[key] = binding
	}
	markers, err := c.loadMarkers()
	if err != nil {
		return err
	}
	if err := c.pruneUnreferencedObserverJournals(markers); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(markers))
	for _, marker := range markers {
		binding, found := bound[marker.RunID]
		if !found {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "marker", "observer marker has no exact durable run binding: "+marker.RunID, nil)
		}
		if marker.PlanDigest != binding.PlanDigest.String() {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "plan_digest", "observer marker differs from the exact persisted run plan: "+marker.RunID, nil)
		}
		seen[marker.RunID] = struct{}{}
		if !binding.State.Terminal() {
			if binding.BundlePublished {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "bundle", "nonterminal durable run has a published terminal bundle: "+marker.RunID, nil)
			}
			if marker.Phase == "committed" {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "phase", "nonterminal durable run has a committed observer marker: "+marker.RunID, nil)
			}
			continue
		}
		if !binding.BundlePublished {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "bundle", "terminal durable run lacks a verified public bundle publication: "+marker.RunID, nil)
		}
		switch marker.Phase {
		case "committed":
			continue
		case "stopped":
			marker.Phase = "committed"
			marker.UpdatedAt = c.clock().UTC()
			if err := c.writeMarker(marker); err != nil {
				return err
			}
		default:
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "phase", "terminal durable run has an unresolved observer marker: "+marker.RunID+"("+marker.Phase+")", nil)
		}
	}
	for runID, binding := range bound {
		if binding.State.Terminal() {
			if !binding.BundlePublished {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "bundle", "terminal durable run lacks a verified public bundle publication: "+runID, nil)
			}
			if _, found := seen[runID]; !found {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile_persisted", "marker", "terminal durable run has no observer marker: "+runID, nil)
			}
		}
	}
	return nil
}

// RecoverInterrupted reconstructs observer evidence for a durably bound run
// without starting a collector or arming the maximum-duration timer. Any old
// external ownership must first be authoritatively proven dead. Every planned
// signal is then recorded as lost so finalization cannot claim continuity.
func (c *RunObserverCoordinator) RecoverInterrupted(ctx context.Context, input RunObserverStart, durableState domain.TargetRunState) error {
	if err := validateRunObserverStart(input); err != nil {
		return err
	}
	if !durableState.IsValid() || durableState.Terminal() {
		return domain.NewError(domain.CodeInvalidArgument, "run_observers.recover_interrupted", "run_state", "must be a nonterminal durable run state", nil)
	}
	signature, err := observerStartSignature(input)
	if err != nil {
		return err
	}
	planDigest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		return err
	}
	runID := input.Plan.Run.ID().String()
	markers, err := c.loadMarkers()
	if err != nil {
		return err
	}
	var matched *observerStateMarker
	for index := range markers {
		if markers[index].RunID == runID {
			copy := markers[index]
			matched = &copy
			break
		}
	}
	if matched == nil && durableState != domain.TargetRunRequested && durableState != domain.TargetRunPreparing {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "marker", "durable run state proves observer ownership but its exact marker is missing", nil)
	}
	var recovered ports.InterruptedCollectorReconciliationReport
	var recoveredFromJournal bool
	var preview observerEvidenceJournal
	if matched != nil {
		if matched.PlanDigest != planDigest.String() || matched.Signature != signature || matched.Phase == "committed" {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "marker", "interrupted observer marker does not match the exact persisted run realization", nil)
		}
		if err := validatePersistedCollectorBindings(input, *matched); err != nil {
			return err
		}
		if matched.Journal != nil {
			preview, err = c.loadObserverJournal(*matched.Journal, matched.RunID, matched.PlanDigest)
			if err != nil {
				return err
			}
			if preview.Recovery != nil {
				recovered, err = preview.Recovery.restore()
				if err != nil {
					return err
				}
				recoveredFromJournal = true
			}
		}
		if len(matched.Collectors) > 0 {
			if c.driver == nil {
				return domain.NewError(domain.CodeCapabilityUnavailable, "run_observers.recover_interrupted", "observer_driver", "persisted collector outputs require their exact observer driver", nil)
			}
			if matched.ExternalOwnership && !matched.CrashCleanup {
				return domain.NewError(domain.CodeCapabilityUnavailable, "run_observers.recover_interrupted", "collector_cleanup", "the interrupted collector lifecycle did not carry an authoritative crash-cleanup guarantee", nil)
			}
			reconciler, ok := c.driver.(ports.ObserverCrashReconciler)
			if !ok || !reconciler.InterruptedCollectorCleanupGuaranteed() {
				return domain.NewError(domain.CodeCapabilityUnavailable, "run_observers.recover_interrupted", "collector_cleanup", "the configured observer driver cannot prove old collector teardown and reconcile its durable output", nil)
			}
			request := ports.InterruptedCollectorReconciliation{TargetRunID: input.Plan.Run.ID(), Collectors: cloneInterruptedCollectorBindings(matched.Collectors)}
			physicalAlreadyFinalized := matched.Phase == "stopped" && preview.Receipt != nil || preview.StopBatch != nil
			if !recoveredFromJournal && !physicalAlreadyFinalized {
				recovered, err = reconciler.ReconcileInterruptedCollectors(ctx, request)
				if err != nil {
					return fmt.Errorf("reconcile interrupted collector cleanup and output: %w", err)
				}
			}
			if !physicalAlreadyFinalized {
				if err := recovered.ValidateFor(request); err != nil {
					return err
				}
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.records[runID]; existing != nil {
		if existing.signature == signature && existing.phase == "recovering" {
			return nil
		}
		return domain.NewError(domain.CodeConflict, "run_observers.recover_interrupted", "target_run_id", "already has different in-process observer ownership", nil)
	}
	record := &runObserverRecord{
		start: input, signature: signature, planDigest: planDigest, phase: "recovering",
		crashCleanup: true, started: make(map[string]bool), startCommitted: make(map[string]bool),
		coverage: make(map[string]domain.CollectorCoverage),
	}
	if matched != nil {
		record.plans = make([]ports.CollectorPlan, 0, len(matched.Collectors))
		for _, binding := range matched.Collectors {
			record.plans = append(record.plans, cloneCollectorPlan(binding.Plan))
			record.startCommitted[binding.Plan.CollectorID.String()] = binding.StartCommitted
		}
		if matched.IntrinsicID != "" {
			record.intrinsicID, err = domain.ParseCollectorID(matched.IntrinsicID)
			if err != nil {
				return err
			}
			record.intrinsicStartedAt = matched.IntrinsicStartedAt.UTC()
		}
	} else {
		if containsString(input.Plan.RequiredCoverage, ports.TargetLifecycleSignal) {
			record.intrinsicID, err = c.ids.CollectorID()
			if err != nil {
				return err
			}
			record.intrinsicStartedAt = input.Prepared.PreparedAt.UTC()
		}
		record.plans, err = c.bindCollectorPlans(input)
		if err != nil {
			return err
		}
	}
	if matched != nil {
		if err := c.hydrateObserverJournal(ctx, record, *matched); err != nil {
			return err
		}
		if matched.Phase == "stopped" && record.journal.Receipt != nil {
			receipt, restoreErr := record.journal.Receipt.restore()
			if restoreErr != nil || receipt.RunID.String() != runID || record.stoppedAt.IsZero() {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "journal", "stopped marker lacks its exact finalized receipt and evidence", restoreErr)
			}
			record.receiptSignature, err = targetReceiptSignature(receipt)
			if err != nil {
				return err
			}
			record.phase = "stopped"
			record.stoppedResultDigest = matched.StoppedResultDigest
			record.stopPreparationDigest = matched.StopPreparationDigest
			evidence := c.evidence(record)
			record.stopResult = &evidence
			c.records[runID] = record
			return nil
		}
		if record.journal.StopBatch != nil {
			for _, binding := range matched.Collectors {
				if binding.StartCommitted {
					record.started[binding.Plan.CollectorID.String()] = true
				}
			}
			record.phase = "stopping"
			c.records[runID] = record
			return nil
		}
		if len(matched.Collectors) > 0 && record.journal.Recovery == nil {
			persisted := persistRecoveryReport(recovered)
			record.journal.Recovery = &persisted
			if err := c.checkpointObserverJournal(record); err != nil {
				return err
			}
		}
	}
	if err := appendRecoveredCollectorArtifacts(record, recovered); err != nil {
		return err
	}
	c.records[runID] = record
	if err := c.persistMarker(record); err != nil {
		delete(c.records, runID)
		return err
	}
	reason := "control-plane loss interrupted the run; prior collector and specimen continuity was not resumed"
	endedAt := c.nowNotBefore(input.Prepared.PreparedAt)
	for _, plan := range record.plans {
		collectorReason := recoveredCollectorFailureReason(reason, recovered, plan.CollectorID)
		if err := c.recordFailure(ctx, record, plan, collectorReason, c.nowNotBefore(plan.StartedAt)); err != nil {
			delete(c.records, runID)
			return err
		}
	}
	if !record.intrinsicID.IsZero() {
		if err := c.recordFailure(ctx, record, intrinsicCollectorPlan(record), reason, endedAt); err != nil {
			delete(c.records, runID)
			return err
		}
	}
	if err := c.persistMarker(record); err != nil {
		delete(c.records, runID)
		return err
	}
	return nil
}

func (c *RunObserverCoordinator) Start(ctx context.Context, input RunObserverStart) error {
	if err := validateRunObserverStart(input); err != nil {
		return err
	}
	if len(input.Plan.Collectors) > 0 && c.driver == nil {
		return domain.NewError(domain.CodeCapabilityUnavailable, "run_observers.start", "observer_driver", "external collectors are configured but no observer driver is composed", nil)
	}
	signature, err := observerStartSignature(input)
	if err != nil {
		return err
	}
	planDigest, err := TargetRunProvisioningPlanDigest(input.Plan)
	if err != nil {
		return err
	}
	crashCleanup := len(input.Plan.Collectors) == 0
	if reconciler, ok := c.driver.(ports.ObserverCrashReconciler); ok && reconciler.InterruptedCollectorCleanupGuaranteed() {
		crashCleanup = true
	}
	runID := input.Plan.Run.ID().String()

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.records[runID]; existing != nil {
		if existing.signature != signature {
			return domain.NewError(domain.CodeConflict, "run_observers.start", "target_run_id", "already has a different collector plan", nil)
		}
		if existing.phase == "active" {
			return nil
		}
		return domain.NewError(domain.CodeInvalidState, "run_observers.start", "target_run_id", "collector lifecycle is already stopping or stopped", nil)
	}

	record := &runObserverRecord{
		start: input, signature: signature, planDigest: planDigest, crashCleanup: crashCleanup,
		externalOwnership: len(input.Plan.Collectors) > 0,
		phase:             "starting", started: make(map[string]bool), startCommitted: make(map[string]bool),
		coverage: make(map[string]domain.CollectorCoverage),
	}
	if containsString(input.Plan.RequiredCoverage, ports.TargetLifecycleSignal) {
		record.intrinsicID, err = c.ids.CollectorID()
		if err != nil {
			return err
		}
		record.intrinsicStartedAt = input.Prepared.PreparedAt.UTC()
	}
	plans, err := c.bindCollectorPlans(input)
	if err != nil {
		return err
	}
	record.plans = plans
	c.records[runID] = record
	if err := c.persistMarker(record); err != nil {
		delete(c.records, runID)
		return err
	}

	for _, plan := range record.plans {
		collector, startErr := c.driver.Start(ctx, plan)
		if startErr == nil {
			// Start transferred ownership even when the returned receipt or
			// subsequent readiness proof is invalid.
			record.started[plan.CollectorID.String()] = true
			record.startCommitted[plan.CollectorID.String()] = true
			if markerErr := c.persistMarker(record); markerErr != nil {
				cleanupErr := c.stopStartedDetached(record)
				record.phase = "stopped"
				return errors.Join(markerErr, cleanupErr, c.persistMarker(record))
			}
		}
		if startErr == nil {
			startErr = validateStartedCollector(plan, collector)
		}
		if startErr == nil {
			var coverage domain.CollectorCoverage
			coverage, startErr = c.driver.Coverage(ctx, plan.CollectorID)
			if startErr == nil {
				startErr = requireCoverage(plan.Requirement, plan.CollectorID, coverage, false)
			}
			if startErr == nil {
				startErr = c.appendCoverage(ctx, record, plan, coverage)
			}
			if startErr == nil {
				startErr = c.appendLifecycle(ctx, record, plan, "collector.ready", plan.StartedAt, domain.TargetOperationID{}, map[string]any{"signal_family": plan.Requirement.SignalFamily, "adapter": plan.Adapter, "version": plan.Version})
			}
		}
		if startErr != nil {
			failureErr := c.recordFailure(ctx, record, plan, "collector failed before authoritative readiness: "+startErr.Error(), c.nowNotBefore(plan.StartedAt))
			if plan.Requirement.Required {
				cleanupErr := c.stopStartedDetached(record)
				record.phase = "stopped"
				_ = c.persistMarker(record)
				return errors.Join(startErr, failureErr, cleanupErr)
			}
		}
	}
	record.phase = "active"
	if err := c.persistMarker(record); err != nil {
		return errors.Join(err, c.stopStartedDetached(record))
	}
	record.stopDeadline = c.clock().UTC().Add(input.Plan.MaximumDuration)
	c.armMaximumDurationTimer(record)
	return nil
}

// PrepareStop durably arms every external collector before the target is
// stopped. An armed collector keeps running; Stop is the commit point that
// transfers its final output into observer evidence.
func (c *RunObserverCoordinator) PrepareStop(ctx context.Context, runID domain.TargetRunID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.requireRunRecordLocked(runID, "run_observers.prepare_stop")
	if err != nil {
		return err
	}
	switch record.phase {
	case "stop-prepared":
		return nil
	case "stopping", "stopped", "committed":
		// Stop has already committed the boundary. Driver implementations make
		// preparation harmless after commit, and coordinator replay does too.
		return nil
	case "stop-preparing", "stop-canceling":
		if err := c.cancelStopPreparationLocked(ctx, record); err != nil {
			return err
		}
	case "active", "recovering":
	default:
		return domain.NewError(domain.CodeInvalidState, "run_observers.prepare_stop", "phase", "collector lifecycle cannot prepare a stop from "+record.phase, nil)
	}

	record.stopResumePhase = record.phase
	c.disarmMaximumDurationTimer(record)
	record.phase = "stop-preparing"
	if err := c.persistMarker(record); err != nil {
		record.phase = record.stopResumePhase
		record.stopResumePhase = ""
		if record.phase == "active" {
			c.armMaximumDurationTimer(record)
		}
		return err
	}
	for _, plan := range record.plans {
		if !record.started[plan.CollectorID.String()] {
			continue
		}
		if err := c.driver.PrepareStop(ctx, plan.CollectorID); err != nil {
			return errors.Join(err, c.cancelStopPreparationDetachedLocked(record))
		}
	}
	record.phase = "stop-prepared"
	if err := c.persistMarker(record); err != nil {
		return errors.Join(err, c.cancelStopPreparationDetachedLocked(record))
	}
	return nil
}

// CancelStopPreparation restores collection after the target stop failed
// before producing an authoritative receipt. It is idempotent and does not
// roll back a boundary once collector Stop has begun.
func (c *RunObserverCoordinator) CancelStopPreparation(ctx context.Context, runID domain.TargetRunID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.requireRunRecordLocked(runID, "run_observers.cancel_stop_preparation")
	if err != nil {
		return err
	}
	switch record.phase {
	case "active", "recovering", "stopping", "stopped", "committed":
		return nil
	case "stop-preparing", "stop-prepared", "stop-canceling":
		return c.cancelStopPreparationLocked(ctx, record)
	default:
		return domain.NewError(domain.CodeInvalidState, "run_observers.cancel_stop_preparation", "phase", "collector lifecycle cannot cancel a stop preparation from "+record.phase, nil)
	}
}

func (c *RunObserverCoordinator) Stop(ctx context.Context, input RunObserverStop) (RunObservationEvidence, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.requireRunRecordLocked(input.RunID, "run_observers.stop")
	if err != nil {
		return RunObservationEvidence{}, err
	}
	return c.stopLocked(ctx, input, record)
}

func (c *RunObserverCoordinator) stopLocked(ctx context.Context, input RunObserverStop, record *runObserverRecord) (RunObservationEvidence, error) {
	if record.stopResult != nil {
		return cloneRunObservationEvidence(*record.stopResult), nil
	}
	c.disarmMaximumDurationTimer(record)
	record.phase = "stopping"
	if err := c.persistMarker(record); err != nil {
		return RunObservationEvidence{}, err
	}
	if err := c.initializeJournal(record); err != nil {
		return RunObservationEvidence{}, err
	}
	if record.journal.StopBatch == nil {
		batch, collectErr := c.collectStoppedObservers(ctx, record, input.TargetStoppedAt)
		if collectErr != nil {
			// No ledger append has occurred. Keep the marker at stopping and let
			// an explicit call or the bounded timer retry every unconfirmed
			// physical owner.
			return RunObservationEvidence{}, collectErr
		}
		record.journal.StopBatch = &batch
		if err := c.checkpointObserverJournal(record); err != nil {
			return RunObservationEvidence{}, err
		}
	}
	if err := c.applyStoppedObserverBatch(ctx, record, *record.journal.StopBatch); err != nil {
		return RunObservationEvidence{}, err
	}
	record.stoppedAt = c.nowNotBefore(record.journal.StopBatch.StoppedAt)
	record.phase = "stopped"
	if err := c.checkpointObserverJournal(record); err != nil {
		return RunObservationEvidence{}, err
	}
	evidence := c.evidence(record)
	record.stopResult = &evidence
	return cloneRunObservationEvidence(evidence), nil
}

func (c *RunObserverCoordinator) cancelStopPreparationDetachedLocked(record *runObserverRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
	defer cancel()
	return c.cancelStopPreparationLocked(ctx, record)
}

func (c *RunObserverCoordinator) cancelStopPreparationLocked(ctx context.Context, record *runObserverRecord) error {
	var transitionErr error
	if record.phase != "stop-canceling" {
		record.phase = "stop-canceling"
		transitionErr = c.persistMarker(record)
	}
	var cancellationErrors []error
	for index := len(record.plans) - 1; index >= 0; index-- {
		plan := record.plans[index]
		if !record.started[plan.CollectorID.String()] {
			continue
		}
		if err := c.driver.CancelStopPreparation(ctx, plan.CollectorID); err != nil {
			cancellationErrors = append(cancellationErrors, err)
		}
	}
	if cancellationErr := errors.Join(cancellationErrors...); cancellationErr != nil {
		return errors.Join(transitionErr, cancellationErr, c.persistMarker(record))
	}
	resumePhase := record.stopResumePhase
	if resumePhase != "active" && resumePhase != "recovering" {
		resumePhase = "active"
	}
	record.phase = resumePhase
	if err := c.persistMarker(record); err != nil {
		// The physical collectors are active again, but retain the conservative
		// durable phase in memory until a retry can persist that fact.
		record.phase = "stop-canceling"
		return errors.Join(transitionErr, err)
	}
	record.stopResumePhase = ""
	if resumePhase == "active" {
		c.armMaximumDurationTimer(record)
	}
	return transitionErr
}

func (c *RunObserverCoordinator) requireRunRecordLocked(runID domain.TargetRunID, operation string) (*runObserverRecord, error) {
	if runID.IsZero() {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "run_id", "must be set", nil)
	}
	record := c.records[runID.String()]
	if record == nil {
		return nil, domain.NewError(domain.CodeNotFound, operation, "run_id", "has no coordinator state in this process; restart reconciliation is required", nil)
	}
	return record, nil
}

func (c *RunObserverCoordinator) disarmMaximumDurationTimer(record *runObserverRecord) {
	record.timerGeneration++
	if record.timer != nil {
		record.timer.Stop()
		record.timer = nil
	}
}

func (c *RunObserverCoordinator) armMaximumDurationTimer(record *runObserverRecord) {
	if record.stopDeadline.IsZero() {
		return
	}
	c.disarmMaximumDurationTimer(record)
	generation := record.timerGeneration
	runID := record.start.Plan.Run.ID()
	duration := record.stopDeadline.Sub(c.clock().UTC())
	if duration < 0 {
		duration = 0
	}
	record.timer = time.AfterFunc(duration, func() {
		deadlineCtx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
		defer cancel()
		stop := RunObserverStop{RunID: runID, TargetStoppedAt: c.clock().UTC()}
		_, stopErr := c.stopAfterMaximumDuration(deadlineCtx, stop, generation)
		for attempt := 1; stopErr != nil && attempt < 3 && deadlineCtx.Err() == nil; attempt++ {
			_, stopErr = c.Stop(deadlineCtx, stop)
		}
	})
}

func (c *RunObserverCoordinator) stopAfterMaximumDuration(ctx context.Context, input RunObserverStop, generation uint64) (RunObservationEvidence, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, err := c.requireRunRecordLocked(input.RunID, "run_observers.maximum_duration_stop")
	if err != nil {
		return RunObservationEvidence{}, err
	}
	if record.timerGeneration != generation || record.phase != "active" {
		return RunObservationEvidence{}, nil
	}
	return c.stopLocked(ctx, input, record)
}

func (c *RunObserverCoordinator) collectStoppedObservers(ctx context.Context, record *runObserverRecord, targetStoppedAt time.Time) (persistedCollectorStopBatch, error) {
	stoppedAt := targetStoppedAt.UTC()
	if stoppedAt.IsZero() {
		stoppedAt = c.clock().UTC()
	}
	batch := persistedCollectorStopBatch{StoppedAt: stoppedAt, Results: []persistedCollectorStopResult{}}
	stopFailures := make(map[string]string, len(record.journal.StopFailures))
	for _, failure := range record.journal.StopFailures {
		stopFailures[failure.CollectorID] = failure.Reason
	}
	var cleanupErrors []error
	for _, plan := range record.plans {
		if !record.started[plan.CollectorID.String()] {
			continue
		}
		result, stopErr := c.driver.Stop(ctx, plan.CollectorID)
		identityMatches := result.CollectorID == plan.CollectorID
		if !identityMatches {
			stopErr = errors.Join(stopErr, domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", "collector_id", "observer returned a different collector", nil))
		}
		if !result.StoppedAt.IsZero() && result.StoppedAt.After(batch.StoppedAt) {
			batch.StoppedAt = result.StoppedAt.UTC()
		}
		persisted := persistedCollectorStopResult{
			CollectorID: result.CollectorID.String(), Artifacts: persistArtifacts(result.Artifacts),
			StoppedAt: result.StoppedAt.UTC(), TeardownConfirmed: result.TeardownConfirmed,
		}
		if stopErr != nil {
			persisted.StopError = stopErr.Error()
			persisted.FailureReason = stopErr.Error()
		}
		artifacts, artifactErr := validatedCollectorArtifacts(result.Artifacts)
		if artifactErr != nil {
			persisted.FailureReason = errors.Join(stopErr, artifactErr).Error()
		} else {
			persisted.AcceptedArtifacts = persistArtifacts(artifacts)
		}
		if coverageErr := requireCoverage(plan.Requirement, plan.CollectorID, result.Coverage, true); coverageErr != nil {
			if persisted.FailureReason == "" {
				persisted.FailureReason = coverageErr.Error()
			}
		} else {
			coverage := persistCoverage([]domain.CollectorCoverage{result.Coverage})[0]
			persisted.Coverage = &coverage
		}
		if prior := stopFailures[plan.CollectorID.String()]; prior != "" {
			persisted.FailureReason = errors.Join(errors.New(prior), errorFromString(persisted.FailureReason)).Error()
		}
		batch.Results = append(batch.Results, persisted)
		if !identityMatches || !result.TeardownConfirmed {
			cleanupErr := errors.Join(stopErr, domain.NewError(domain.CodeUnavailable, "run_observers.stop", "collector_teardown", "collector teardown was not authoritatively confirmed", nil))
			cleanupErrors = append(cleanupErrors, cleanupErr)
			stopFailures[plan.CollectorID.String()] = cleanupErr.Error()
		}
	}
	if err := errors.Join(cleanupErrors...); err != nil {
		record.journal.StopFailures = make([]persistedCollectorStopFailure, 0, len(stopFailures))
		for collectorID, reason := range stopFailures {
			record.journal.StopFailures = append(record.journal.StopFailures, persistedCollectorStopFailure{CollectorID: collectorID, Reason: reason})
		}
		sort.Slice(record.journal.StopFailures, func(i, j int) bool {
			return record.journal.StopFailures[i].CollectorID < record.journal.StopFailures[j].CollectorID
		})
		if checkpointErr := c.checkpointObserverJournal(record); checkpointErr != nil {
			return persistedCollectorStopBatch{}, errors.Join(err, checkpointErr)
		}
		return persistedCollectorStopBatch{}, err
	}
	return batch, nil
}

func errorFromString(value string) error {
	if value == "" {
		return nil
	}
	return errors.New(value)
}

func (c *RunObserverCoordinator) applyStoppedObserverBatch(ctx context.Context, record *runObserverRecord, batch persistedCollectorStopBatch) error {
	byCollector := make(map[string]persistedCollectorStopResult, len(batch.Results))
	for _, result := range batch.Results {
		byCollector[result.CollectorID] = result
	}
	for _, plan := range record.plans {
		if !record.started[plan.CollectorID.String()] {
			continue
		}
		result, found := byCollector[plan.CollectorID.String()]
		if !found || !result.TeardownConfirmed {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", "collector_stop_batch", "journal lacks an exact confirmed collector result", nil)
		}
		artifacts, err := restoreArtifacts(result.AcceptedArtifacts)
		if err != nil {
			return err
		}
		record.artifacts = appendUniqueArtifacts(record.artifacts, artifacts...)
		if !collectorHasFailure(record, plan.CollectorID) {
			if result.FailureReason != "" {
				if err := c.recordFailure(ctx, record, plan, result.FailureReason, batch.StoppedAt); err != nil {
					return err
				}
			} else if result.Coverage == nil {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", "collector_stop_batch", "successful collector result lacks coverage", nil)
			} else {
				coverage, restoreErr := restoreCoverage([]persistedCoverage{*result.Coverage})
				if restoreErr != nil {
					return restoreErr
				}
				if err := c.appendCoverage(ctx, record, plan, coverage[0]); err != nil {
					return err
				}
			}
		}
		if err := c.appendLifecycle(ctx, record, plan, "collector.stopped", batch.StoppedAt, domain.TargetOperationID{}, map[string]any{"teardown_confirmed": true}); err != nil {
			return err
		}
	}
	return nil
}

// Finalize appends target-owned lifecycle facts only after the target has
// stopped. Ledger cursors are assigned here, never synthesized by a driver.
func (c *RunObserverCoordinator) Finalize(ctx context.Context, receipt ports.TargetRunStopReceipt) (RunObservationEvidence, error) {
	if err := receipt.Validate(); err != nil {
		return RunObservationEvidence{}, err
	}
	signature, err := targetReceiptSignature(receipt)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	if _, err := c.Stop(ctx, RunObserverStop{RunID: receipt.RunID, TargetStoppedAt: receipt.StoppedAt}); err != nil {
		return RunObservationEvidence{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[receipt.RunID.String()]
	if record == nil || record.phase != "stopped" {
		return RunObservationEvidence{}, domain.NewError(domain.CodeInvalidState, "run_observers.finalize", "run_id", "collector evidence is not stopped", nil)
	}
	if err := c.initializeJournal(record); err != nil {
		return RunObservationEvidence{}, err
	}
	if record.journal.Receipt != nil {
		storedReceipt, restoreErr := record.journal.Receipt.restore()
		if restoreErr != nil {
			return RunObservationEvidence{}, restoreErr
		}
		storedSignature, signatureErr := targetReceiptSignature(storedReceipt)
		if signatureErr != nil || storedSignature != signature {
			return RunObservationEvidence{}, domain.NewError(domain.CodeConflict, "run_observers.finalize", "receipt", "target stop receipt changed across retries", signatureErr)
		}
		receipt = storedReceipt
	} else {
		persisted := persistTargetStopReceipt(receipt)
		record.journal.Receipt = &persisted
		if err := c.checkpointObserverJournal(record); err != nil {
			return RunObservationEvidence{}, err
		}
	}
	if record.receiptSignature != "" && record.receiptSignature != signature {
		return RunObservationEvidence{}, domain.NewError(domain.CodeConflict, "run_observers.finalize", "receipt", "target stop receipt changed across retries", nil)
	}
	record.receiptSignature = signature
	for index := range receipt.Observations {
		eventID, idErr := c.ids.EventID()
		if idErr != nil {
			return RunObservationEvidence{}, idErr
		}
		if err := c.appendTargetLifecycle(ctx, record, receipt.Observations[index], index, eventID); err != nil {
			return RunObservationEvidence{}, err
		}
	}
	if !record.intrinsicID.IsZero() {
		if err := c.recordIntrinsicCoverage(ctx, record, receipt); err != nil {
			return RunObservationEvidence{}, err
		}
	}
	if receipt.StoppedAt.After(record.stoppedAt) {
		record.stoppedAt = receipt.StoppedAt.UTC()
	}
	manifest, err := c.writeLifecycleArtifact(record)
	if err != nil {
		return RunObservationEvidence{}, err
	}
	record.artifacts = replaceArtifactRole(record.artifacts, manifest)
	evidence := c.evidence(record)
	record.stopResult = &evidence
	if err := c.checkpointObserverJournal(record); err != nil {
		record.stopResult = nil
		return RunObservationEvidence{}, err
	}
	return cloneRunObservationEvidence(evidence), nil
}

// RestoreFinalized returns the exact target receipt and observer evidence that
// were journaled before service-level stop preparation. It is the restart
// bridge for a crash after physical finalization but before the Service owns a
// bundle-stop-preparation file.
func (c *RunObserverCoordinator) RestoreFinalized(runID domain.TargetRunID) (ports.TargetRunStopReceipt, RunObservationEvidence, bool, error) {
	if runID.IsZero() {
		return ports.TargetRunStopReceipt{}, RunObservationEvidence{}, false, domain.NewError(domain.CodeInvalidArgument, "run_observers.restore_finalized", "run_id", "must be set", nil)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[runID.String()]
	if record == nil || record.phase != "stopped" || record.stopResult == nil || record.journal.Receipt == nil {
		return ports.TargetRunStopReceipt{}, RunObservationEvidence{}, false, nil
	}
	receipt, err := record.journal.Receipt.restore()
	if err != nil || receipt.RunID != runID {
		return ports.TargetRunStopReceipt{}, RunObservationEvidence{}, false, domain.NewError(domain.CodeIntegrityViolation, "run_observers.restore_finalized", "receipt", "journaled target receipt is invalid", err)
	}
	return receipt, cloneRunObservationEvidence(*record.stopResult), true, nil
}

func (c *RunObserverCoordinator) Commit(ctx context.Context, runID domain.TargetRunID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[runID.String()]
	if record == nil || record.stopResult == nil {
		return domain.NewError(domain.CodeInvalidState, "run_observers.commit", "run_id", "does not have stopped authoritative evidence", nil)
	}
	if record.phase == "committed" {
		return nil
	}
	if record.phase != "stopped" {
		return domain.NewError(domain.CodeInvalidState, "run_observers.commit", "run_id", "does not have stopped authoritative evidence", nil)
	}
	record.phase = "committed"
	if err := c.persistMarker(record); err != nil {
		// Keep an unsuccessful transition retryable. A marker that reached disk
		// before a later durability error is harmless: the retry publishes the
		// same committed state again.
		record.phase = "stopped"
		return err
	}
	return nil
}

// BindStoppedPreparation makes the compact observer marker prove both the
// stopped result and the exact Service-owned preparation that may consume it.
func (c *RunObserverCoordinator) BindStoppedPreparation(runID, resultDigest, preparationDigest string) error {
	parsedRunID, err := domain.ParseTargetRunID(runID)
	if err != nil {
		return err
	}
	for _, digest := range []string{resultDigest, preparationDigest} {
		if _, err := domain.ParseDigest(digest); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[parsedRunID.String()]
	if record == nil || record.phase != "stopped" || record.stopResult == nil {
		return domain.NewError(domain.CodeInvalidState, "run_observers.bind_stopped_preparation", "run_id", "does not have finalized stopped evidence", nil)
	}
	if record.stoppedResultDigest != "" && (record.stoppedResultDigest != resultDigest || record.stopPreparationDigest != preparationDigest) {
		return domain.NewError(domain.CodeConflict, "run_observers.bind_stopped_preparation", "digest", "stopped preparation is already bound to different evidence", nil)
	}
	previousResult, previousPreparation := record.stoppedResultDigest, record.stopPreparationDigest
	record.stoppedResultDigest, record.stopPreparationDigest = resultDigest, preparationDigest
	if err := c.persistMarker(record); err != nil {
		record.stoppedResultDigest, record.stopPreparationDigest = previousResult, previousPreparation
		return err
	}
	return nil
}

func (c *RunObserverCoordinator) RequireStoppedPreparation(runID, resultDigest, preparationDigest string) error {
	if _, err := domain.ParseTargetRunID(runID); err != nil {
		return err
	}
	for _, digest := range []string{resultDigest, preparationDigest} {
		if _, err := domain.ParseDigest(digest); err != nil {
			return err
		}
	}
	markers, err := c.loadMarkers()
	if err != nil {
		return err
	}
	for _, marker := range markers {
		if marker.RunID != runID {
			continue
		}
		if marker.Phase != "stopped" && marker.Phase != "committed" {
			return domain.NewError(domain.CodeFailedPrecondition, "run_observers.require_stopped_preparation", "phase", "observer evidence is not stopped", nil)
		}
		if marker.StoppedResultDigest != resultDigest || marker.StopPreparationDigest != preparationDigest {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.require_stopped_preparation", "digest", "observer marker differs from the stopped preparation", nil)
		}
		return nil
	}
	return domain.NewError(domain.CodeIntegrityViolation, "run_observers.require_stopped_preparation", "marker", "observer marker is missing", nil)
}

// RequireCommitted verifies the durable observer side of the finalization
// boundary without relying on an in-process record surviving a restart.
func (c *RunObserverCoordinator) RequireCommitted(runID domain.TargetRunID) error {
	if runID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, "run_observers.require_committed", "run_id", "is required", nil)
	}
	markers, err := c.loadMarkers()
	if err != nil {
		return err
	}
	for _, marker := range markers {
		if marker.RunID != runID.String() {
			continue
		}
		if marker.Phase != "committed" {
			return domain.NewError(domain.CodeFailedPrecondition, "run_observers.require_committed", "phase", "observer marker is not committed", nil)
		}
		return nil
	}
	return domain.NewError(domain.CodeIntegrityViolation, "run_observers.require_committed", "marker", "observer marker is missing", nil)
}

func (c *RunObserverCoordinator) Close(ctx context.Context) error {
	c.mu.Lock()
	runIDs := make([]domain.TargetRunID, 0)
	for _, record := range c.records {
		if observerPhaseMayOwnCollectors(record.phase) {
			runIDs = append(runIDs, record.start.Plan.Run.ID())
		}
	}
	c.mu.Unlock()
	var result []error
	for _, runID := range runIDs {
		_, err := c.Stop(ctx, RunObserverStop{RunID: runID, TargetStoppedAt: c.clock().UTC()})
		result = append(result, err)
	}
	return errors.Join(result...)
}

func validateRunObserverStart(input RunObserverStart) error {
	if err := input.Plan.Validate(); err != nil {
		return err
	}
	if !input.TargetKind.IsValid() || input.ResearchSessionID.IsZero() || input.PolicyDigest.IsZero() || input.CapabilityFingerprintDigest.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, "run_observers.start", "provenance", "target kind, research session, policy, and capability digests are required", nil)
	}
	run := input.Plan.Run.Spec()
	prepared := input.Prepared
	if prepared.RunID != run.ID || prepared.TargetID != run.TargetID || prepared.TargetGeneration != run.TargetGeneration || prepared.MaterializationDigest != run.MaterializationDigest || prepared.PreparedAt.IsZero() {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.start", "prepared_run", "does not match the authoritative run plan", nil)
	}
	if err := prepared.Attachment.Validate(); err != nil {
		return err
	}
	if prepared.Attachment.TargetKind != input.TargetKind {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.start", "attachment", "target kind changed", nil)
	}
	return nil
}

func observerStartSignature(input RunObserverStart) (string, error) {
	type collectorSignature struct {
		Name, Family, Placement, Level, Adapter, Version, Digest string
		Required                                                 bool
		MaximumBytes                                             int64
		Resources                                                any
	}
	collectors := make([]collectorSignature, 0, len(input.Plan.Collectors))
	for _, value := range input.Plan.Collectors {
		collectors = append(collectors, collectorSignature{
			Name: value.Name, Family: value.Requirement.SignalFamily, Placement: string(value.Requirement.Placement),
			Level: string(value.Requirement.MinimumLevel), Required: value.Requirement.Required,
			Adapter: value.Adapter, Version: value.Version, Digest: value.ConfigurationDigest.String(), MaximumBytes: value.MaximumBytes,
			Resources: value.Resources,
		})
	}
	sort.Slice(collectors, func(i, j int) bool { return collectors[i].Name < collectors[j].Name })
	required := sortedCoverageFamilies(input.Plan.RequiredCoverage)
	encoded, err := json.Marshal(struct {
		Run, Material, TargetKind, Policy, Capability string
		Attachment                                    ports.ObservationAttachment
		MaximumDuration                               int64
		Required                                      []string
		Collectors                                    []collectorSignature
	}{
		Run: input.Plan.Run.ID().String(), Material: input.Plan.Run.Spec().MaterializationDigest.String(),
		TargetKind: string(input.TargetKind), Policy: input.PolicyDigest.String(), Capability: input.CapabilityFingerprintDigest.String(),
		Attachment: input.Prepared.Attachment, MaximumDuration: int64(input.Plan.MaximumDuration), Required: required, Collectors: collectors,
	})
	if err != nil {
		return "", err
	}
	return domain.NewDigest(encoded).String(), nil
}

func (c *RunObserverCoordinator) bindCollectorPlans(input RunObserverStart) ([]ports.CollectorPlan, error) {
	specs := cloneCollectorSpecs(input.Plan.Collectors)
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	run := input.Plan.Run.Spec()
	startedAt := c.nowNotBefore(input.Prepared.PreparedAt)
	plans := make([]ports.CollectorPlan, 0, len(specs))
	for _, spec := range specs {
		id, err := c.ids.CollectorID()
		if err != nil {
			return nil, err
		}
		plan := ports.CollectorPlan{
			IdempotencyKey: ports.DeriveCollectorIdempotencyKey(input.Plan.IdempotencyKey, spec.Name),
			CollectorID:    id, ResearchSessionID: input.ResearchSessionID, LeaseID: run.LeaseID,
			AgentWorkspaceID: run.AgentWorkspaceID, AgentGeneration: run.AgentGeneration,
			TargetID: run.TargetID, TargetGeneration: run.TargetGeneration, TargetRunID: run.ID,
			Attachment: input.Prepared.Attachment, Requirement: spec.Requirement,
			Adapter: spec.Adapter, Version: spec.Version, ConfigurationDigest: spec.ConfigurationDigest,
			Resources: spec.Resources.Clone(), MaximumBytes: spec.MaximumBytes, StartedAt: startedAt,
		}
		if err := plan.Validate(); err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func validateStartedCollector(plan ports.CollectorPlan, collector ports.Collector) error {
	if collector.ID != plan.CollectorID || collector.TargetRunID != plan.TargetRunID || collector.SignalFamily != plan.Requirement.SignalFamily || collector.StartedAt != plan.StartedAt {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.start", "collector", "observer returned a mismatched collector receipt", nil)
	}
	return nil
}

func (c *RunObserverCoordinator) appendCoverage(ctx context.Context, record *runObserverRecord, plan ports.CollectorPlan, coverage domain.CollectorCoverage) error {
	return c.appendCoverageEvidence(ctx, record, plan, coverage, nil)
}

func (c *RunObserverCoordinator) appendCoverageEvidence(ctx context.Context, record *runObserverRecord, plan ports.CollectorPlan, coverage domain.CollectorCoverage, failure *ObserverFailure) error {
	payload, err := json.Marshal(mapDomainCoverage(coverage))
	if err != nil {
		return err
	}
	eventID, err := c.ids.EventID()
	if err != nil {
		return err
	}
	persisted := persistCoverage([]domain.CollectorCoverage{coverage})[0]
	key, err := observerJournalStepKey("coverage", struct {
		Coverage persistedCoverage         `json:"coverage"`
		Failure  *persistedObserverFailure `json:"failure,omitempty"`
	}{Coverage: persisted, Failure: persistObserverFailure(failure)})
	if err != nil {
		return err
	}
	return c.runObserverJournalStep(ctx, record, observerJournalStep{
		Key: key,
		Record: ledger.Record{
			Kind: ledger.RecordCoverage, EventID: eventID.String(), Identity: collectorLedgerIdentity(plan), SignalFamily: plan.Requirement.SignalFamily,
			Source: observerStateSource, SourceInstance: plan.CollectorID.String(), ObservedWallUnixNano: coverage.Spec().EndedAt.UTC().UnixNano(),
			Collector:    ledger.CollectorContext{ID: plan.CollectorID.String(), Placement: ledgerPlacement(plan.Requirement.Placement), Coverage: string(coverage.Level())},
			PolicyDigest: record.start.PolicyDigest.String(), CapabilityDigest: record.start.CapabilityFingerprintDigest.String(),
			Origin: ledger.OriginSystem, Payload: payload,
		},
		Coverage: &persisted, Failure: persistObserverFailure(failure),
	})
}

func (c *RunObserverCoordinator) appendIntrinsicCoverage(ctx context.Context, record *runObserverRecord, coverage domain.CollectorCoverage, failure *ObserverFailure) error {
	payload, err := json.Marshal(mapDomainCoverage(coverage))
	if err != nil {
		return err
	}
	run := record.start.Plan.Run.Spec()
	spec := coverage.Spec()
	eventID, err := c.ids.EventID()
	if err != nil {
		return err
	}
	persisted := persistCoverage([]domain.CollectorCoverage{coverage})[0]
	key, err := observerJournalStepKey("intrinsic-coverage", struct {
		Coverage persistedCoverage         `json:"coverage"`
		Failure  *persistedObserverFailure `json:"failure,omitempty"`
	}{Coverage: persisted, Failure: persistObserverFailure(failure)})
	if err != nil {
		return err
	}
	return c.runObserverJournalStep(ctx, record, observerJournalStep{Key: key, Record: ledger.Record{
		Kind:    ledger.RecordCoverage,
		EventID: eventID.String(),
		Identity: ledger.Identity{
			ResearchSessionID: record.start.ResearchSessionID.String(), LeaseID: run.LeaseID.String(),
			AgentWorkspaceID: run.AgentWorkspaceID.String(), AgentGeneration: uint64(run.AgentGeneration),
			TargetID: run.TargetID.String(), TargetGeneration: uint64(run.TargetGeneration), TargetRunID: run.ID.String(),
		},
		SignalFamily: ports.TargetLifecycleSignal, Source: "target-driver", SourceInstance: record.start.Prepared.Attachment.RuntimeID,
		ObservedWallUnixNano: spec.EndedAt.UnixNano(),
		Collector:            ledger.CollectorContext{ID: coverage.CollectorID().String(), Placement: ledgerPlacement(spec.Placement), Coverage: string(spec.Level)},
		PolicyDigest:         record.start.PolicyDigest.String(), CapabilityDigest: record.start.CapabilityFingerprintDigest.String(),
		Origin: ledger.OriginSystem, Payload: payload,
	}, Coverage: &persisted, Failure: persistObserverFailure(failure)})
}

func (c *RunObserverCoordinator) recordIntrinsicCoverage(ctx context.Context, record *runObserverRecord, receipt ports.TargetRunStopReceipt) error {
	if collectorHasFailure(record, record.intrinsicID) {
		// Recovery already recorded the intrinsic continuity gap. The fresh
		// prepared-only stop receipt is still appended as target evidence, but it
		// must not overwrite that lost-coverage classification with Complete.
		return nil
	}
	plan := intrinsicCollectorPlan(record)
	coverageSpec := domain.CollectorCoverageSpec{
		CollectorID: record.intrinsicID, SignalFamily: ports.TargetLifecycleSignal,
		Placement: domain.CollectorPlacementHost, Level: domain.CoverageLevelComplete,
		Status: domain.CoverageAvailable, Required: true,
		StartedAt: record.intrinsicStartedAt, EndedAt: receipt.StoppedAt,
	}
	var failure *ObserverFailure
	if validationErr := validateIntrinsicLifecycle(record.intrinsicStartedAt, receipt); validationErr != nil {
		reason := "intrinsic target lifecycle receipt is incomplete: " + validationErr.Error()
		gap, err := c.appendFailureGap(ctx, record, plan, reason, receipt.StoppedAt)
		if err != nil {
			return err
		}
		coverageSpec.Level = domain.CoverageLevelNone
		coverageSpec.Status = domain.CoverageLost
		coverageSpec.Gaps = []domain.Gap{gap}
		value := ObserverFailure{
			CollectorID: record.intrinsicID, Family: ports.TargetLifecycleSignal, Required: true, Reason: reason,
		}
		failure = &value
	}
	coverage, err := domain.NewCollectorCoverage(coverageSpec)
	if err != nil {
		return err
	}
	if err := c.appendIntrinsicCoverage(ctx, record, coverage, failure); err != nil {
		return err
	}
	return nil
}

func intrinsicCollectorPlan(record *runObserverRecord) ports.CollectorPlan {
	run := record.start.Plan.Run.Spec()
	return ports.CollectorPlan{
		CollectorID: record.intrinsicID, ResearchSessionID: record.start.ResearchSessionID,
		LeaseID: run.LeaseID, AgentWorkspaceID: run.AgentWorkspaceID, AgentGeneration: run.AgentGeneration,
		TargetID: run.TargetID, TargetGeneration: run.TargetGeneration, TargetRunID: run.ID,
		Attachment: record.start.Prepared.Attachment,
		Requirement: ports.ObservationRequirement{
			SignalFamily: ports.TargetLifecycleSignal, Placement: domain.CollectorPlacementHost,
			MinimumLevel: domain.CoverageLevelComplete, Required: true,
		},
		StartedAt: record.intrinsicStartedAt,
	}
}

func validateIntrinsicLifecycle(preparedAt time.Time, receipt ports.TargetRunStopReceipt) error {
	if preparedAt.IsZero() || receipt.StoppedAt.Before(preparedAt) {
		return fmt.Errorf("receipt interval precedes preparation")
	}
	previous := preparedAt
	sawStart := receipt.StartedAt.IsZero()
	sawTerminal := false
	for _, observation := range receipt.Observations {
		if observation.ObservedAt.Before(previous) {
			return fmt.Errorf("observations are not time ordered")
		}
		previous = observation.ObservedAt
		if observation.Kind == "target.run.started" && !receipt.StartedAt.IsZero() && observation.ObservedAt.Equal(receipt.StartedAt) {
			sawStart = true
		}
		if observation.ObservedAt.Equal(receipt.StoppedAt) && terminalObservationMatches(receipt.FailureKind, observation.Kind) {
			sawTerminal = true
		}
	}
	if !sawStart {
		return fmt.Errorf("started run has no exact start observation")
	}
	if !sawTerminal {
		return fmt.Errorf("receipt has no matching terminal observation")
	}
	return nil
}

func terminalObservationMatches(kind ports.TargetRunFailureKind, observationKind string) bool {
	switch kind {
	case ports.TargetRunFailureNone:
		return observationKind == "target.run.stopped"
	case ports.TargetRunFailureNeverStarted:
		return strings.Contains(observationKind, "never-started") || strings.Contains(observationKind, "never_started")
	case ports.TargetRunFailureDurationExceeded:
		return strings.Contains(observationKind, "duration-exceeded") || strings.Contains(observationKind, "duration_exceeded")
	case ports.TargetRunFailureTarget:
		return strings.Contains(observationKind, "failed") || strings.Contains(observationKind, "failure")
	default:
		return false
	}
}

func (c *RunObserverCoordinator) appendLifecycle(ctx context.Context, record *runObserverRecord, plan ports.CollectorPlan, kind string, at time.Time, operationID domain.TargetOperationID, payloadValue any) error {
	payload, err := json.Marshal(payloadValue)
	if err != nil {
		return err
	}
	eventID, err := c.ids.EventID()
	if err != nil {
		return err
	}
	params := domain.EventEnvelopeParams{
		SchemaVersion: 1, EventID: eventID, Kind: kind, ResearchSessionID: plan.ResearchSessionID,
		LeaseID: plan.LeaseID, AgentWorkspaceID: plan.AgentWorkspaceID, AgentGeneration: plan.AgentGeneration,
		TargetID: plan.TargetID, TargetGeneration: plan.TargetGeneration, TargetRunID: plan.TargetRunID,
		TargetOperationID: operationID, Source: observerStateSource, SourceInstance: plan.CollectorID.String(),
		CollectorID: plan.CollectorID, CollectorPlacement: plan.Requirement.Placement,
		CoverageLevel: plan.Requirement.MinimumLevel, ObservedWallTime: at.UTC(), ContainerID: plan.Attachment.RuntimeID,
		PolicyDigest: record.start.PolicyDigest, CapabilityFingerprintDigest: record.start.CapabilityFingerprintDigest,
		Payload: payload, Sensitivity: domain.SensitivityInternal, Completeness: domain.CompletenessComplete,
		Confidence: 1, Origin: domain.OriginSystem,
	}
	persisted := persistEvent(params, ledger.Record{})
	keyMaterial := persisted
	keyMaterial.EventID = ""
	key, err := observerJournalStepKey("event", keyMaterial)
	if err != nil {
		return err
	}
	return c.runObserverJournalStep(ctx, record, observerJournalStep{Key: key, Record: ledger.Record{
		Kind: ledger.RecordObservation, EventID: eventID.String(), Identity: collectorLedgerIdentity(plan),
		SignalFamily: plan.Requirement.SignalFamily, Source: observerStateSource,
		SourceInstance: plan.CollectorID.String(), ObservedWallUnixNano: at.UTC().UnixNano(),
		Collector:    ledger.CollectorContext{ID: plan.CollectorID.String(), Placement: ledgerPlacement(plan.Requirement.Placement), Coverage: string(plan.Requirement.MinimumLevel)},
		PolicyDigest: record.start.PolicyDigest.String(), CapabilityDigest: record.start.CapabilityFingerprintDigest.String(),
		Origin: ledger.OriginSystem, Payload: payload,
	}, Event: &persisted})
}

func (c *RunObserverCoordinator) appendTargetLifecycle(ctx context.Context, record *runObserverRecord, observation ports.TargetRunObservation, index int, eventID domain.EventID) error {
	payload := append([]byte(nil), observation.Payload...)
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	run := record.start.Plan.Run.Spec()
	params := domain.EventEnvelopeParams{
		SchemaVersion: 1, EventID: eventID, Kind: observation.Kind, ResearchSessionID: record.start.ResearchSessionID,
		LeaseID: run.LeaseID, AgentWorkspaceID: run.AgentWorkspaceID, AgentGeneration: run.AgentGeneration,
		TargetID: run.TargetID, TargetGeneration: run.TargetGeneration, TargetRunID: run.ID,
		TargetOperationID: observation.TargetOperationID, Source: "target-driver", SourceInstance: record.start.Prepared.Attachment.RuntimeID,
		ObservedWallTime: observation.ObservedAt.UTC(), ContainerID: record.start.Prepared.Attachment.RuntimeID,
		PolicyDigest: record.start.PolicyDigest, CapabilityFingerprintDigest: record.start.CapabilityFingerprintDigest,
		Payload: payload, Sensitivity: domain.SensitivityInternal, Completeness: domain.CompletenessComplete,
		Confidence: 1, Origin: domain.OriginSystem,
	}
	persisted := persistEvent(params, ledger.Record{})
	keyMaterial := persisted
	keyMaterial.EventID = ""
	key, err := observerJournalStepKey("target-event", struct {
		Index int            `json:"index"`
		Event persistedEvent `json:"event"`
	}{Index: index, Event: keyMaterial})
	if err != nil {
		return err
	}
	return c.runObserverJournalStep(ctx, record, observerJournalStep{Key: key, Record: ledger.Record{
		Kind: ledger.RecordObservation, EventID: eventID.String(),
		Identity: ledger.Identity{
			ResearchSessionID: record.start.ResearchSessionID.String(), LeaseID: run.LeaseID.String(),
			AgentWorkspaceID: run.AgentWorkspaceID.String(), AgentGeneration: uint64(run.AgentGeneration),
			TargetID: run.TargetID.String(), TargetGeneration: uint64(run.TargetGeneration), TargetRunID: run.ID.String(),
			TargetOperationID: observation.TargetOperationID.String(),
		},
		SignalFamily: "target.lifecycle", Source: "target-driver", SourceInstance: record.start.Prepared.Attachment.RuntimeID,
		ObservedWallUnixNano: observation.ObservedAt.UTC().UnixNano(), PolicyDigest: record.start.PolicyDigest.String(),
		CapabilityDigest: record.start.CapabilityFingerprintDigest.String(), Origin: ledger.OriginSystem, Payload: payload,
	}, Event: &persisted})
}

func (c *RunObserverCoordinator) recordFailure(ctx context.Context, record *runObserverRecord, plan ports.CollectorPlan, reason string, endedAt time.Time) error {
	gap, err := c.appendFailureGap(ctx, record, plan, reason, endedAt)
	if err != nil {
		return err
	}
	coverage, err := domain.NewCollectorCoverage(domain.CollectorCoverageSpec{
		CollectorID: plan.CollectorID, SignalFamily: plan.Requirement.SignalFamily,
		Placement: plan.Requirement.Placement, Level: domain.CoverageLevelNone, Status: domain.CoverageLost,
		Required: plan.Requirement.Required, StartedAt: plan.StartedAt, EndedAt: endedAt,
		Gaps: []domain.Gap{gap},
	})
	if err != nil {
		return err
	}
	failure := ObserverFailure{CollectorID: plan.CollectorID, Family: plan.Requirement.SignalFamily, Required: plan.Requirement.Required, Reason: reason}
	if err := c.appendCoverageEvidence(ctx, record, plan, coverage, &failure); err != nil {
		return err
	}
	return c.appendLifecycle(ctx, record, plan, "collector.failed", endedAt, domain.TargetOperationID{}, map[string]any{"reason": reason})
}

func (c *RunObserverCoordinator) appendFailureGap(ctx context.Context, record *runObserverRecord, plan ports.CollectorPlan, reason string, endedAt time.Time) (domain.Gap, error) {
	eventID, err := c.ids.EventID()
	if err != nil {
		return domain.Gap{}, err
	}
	persisted := persistedGap{
		Kind: domain.GapUnavailable, Source: observerStateSource, SourceInstance: plan.CollectorID.String(),
		StartedAt: plan.StartedAt.UTC(), EndedAt: endedAt.UTC(), Reason: reason,
	}
	key, err := observerJournalStepKey("gap", persisted)
	if err != nil {
		return domain.Gap{}, err
	}
	if err := c.runObserverJournalStep(ctx, record, observerJournalStep{Key: key, Record: ledger.Record{
		Kind: ledger.RecordGap, EventID: eventID.String(),
		Identity: collectorLedgerIdentity(plan), SignalFamily: plan.Requirement.SignalFamily,
		Source: observerStateSource, SourceInstance: plan.CollectorID.String(), ObservedWallUnixNano: endedAt.UTC().UnixNano(),
		Collector:    ledger.CollectorContext{ID: plan.CollectorID.String(), Placement: ledgerPlacement(plan.Requirement.Placement), Coverage: string(domain.CoverageLevelNone)},
		PolicyDigest: record.start.PolicyDigest.String(), CapabilityDigest: record.start.CapabilityFingerprintDigest.String(), Origin: ledger.OriginSystem,
		Gap: &ledger.Gap{Cause: ledger.GapCollectorLoss, Source: observerStateSource, SourceInstance: plan.CollectorID.String(), Detail: reason},
	}, Gap: &persisted}); err != nil {
		return domain.Gap{}, err
	}
	for index := len(record.gaps) - 1; index >= 0; index-- {
		spec := record.gaps[index].Spec()
		if spec.Source == persisted.Source && spec.SourceInstance == persisted.SourceInstance && spec.StartedAt.Equal(persisted.StartedAt) && spec.EndedAt.Equal(persisted.EndedAt) && spec.Reason == persisted.Reason {
			return record.gaps[index], nil
		}
	}
	return domain.Gap{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.gap", "journal", "completed gap was not hydrated", nil)
}

func persistObserverFailure(value *ObserverFailure) *persistedObserverFailure {
	if value == nil {
		return nil
	}
	result := persistObserverFailures([]ObserverFailure{*value})[0]
	return &result
}

func collectorLedgerIdentity(plan ports.CollectorPlan) ledger.Identity {
	return ledger.Identity{
		ResearchSessionID: plan.ResearchSessionID.String(), LeaseID: plan.LeaseID.String(),
		AgentWorkspaceID: plan.AgentWorkspaceID.String(), AgentGeneration: uint64(plan.AgentGeneration),
		TargetID: plan.TargetID.String(), TargetGeneration: uint64(plan.TargetGeneration), TargetRunID: plan.TargetRunID.String(),
	}
}

func collectorHasFailure(record *runObserverRecord, collectorID domain.CollectorID) bool {
	for _, failure := range record.failures {
		if failure.CollectorID == collectorID {
			return true
		}
	}
	return false
}

func validatedCollectorArtifacts(values []domain.ArtifactReference) ([]domain.ArtifactReference, error) {
	if len(values) == 0 {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", "artifacts", "observer returned no immutable output artifacts", nil)
	}
	result := make([]domain.ArtifactReference, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, artifact := range values {
		spec := artifact.Spec()
		if _, err := domain.NewArtifactReference(spec); err != nil {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", fmt.Sprintf("artifacts[%d]", index), "observer returned an invalid artifact", err)
		}
		key := spec.Reference + "\x00" + spec.Digest.String() + "\x00" + spec.Role
		if _, duplicate := seen[key]; duplicate {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.stop", "artifacts", "observer returned duplicate artifact identities", nil)
		}
		seen[key] = struct{}{}
		result = append(result, artifact)
	}
	return result, nil
}

func targetReceiptSignature(receipt ports.TargetRunStopReceipt) (string, error) {
	type observationSignature struct {
		Kind        string          `json:"kind"`
		ObservedAt  time.Time       `json:"observed_at"`
		OperationID string          `json:"operation_id,omitempty"`
		Payload     json.RawMessage `json:"payload,omitempty"`
	}
	type changeSignature struct {
		Kind         string            `json:"kind"`
		Path         string            `json:"path"`
		PreviousPath string            `json:"previous_path,omitempty"`
		BeforeDigest string            `json:"before_digest,omitempty"`
		AfterDigest  string            `json:"after_digest,omitempty"`
		Metadata     map[string]string `json:"metadata,omitempty"`
	}
	observations := make([]observationSignature, len(receipt.Observations))
	for index, observation := range receipt.Observations {
		observations[index] = observationSignature{
			Kind: observation.Kind, ObservedAt: observation.ObservedAt.UTC(),
			OperationID: observation.TargetOperationID.String(),
			Payload:     append(json.RawMessage(nil), observation.Payload...),
		}
	}
	entries := receipt.TargetChanges.Entries()
	changes := make([]changeSignature, len(entries))
	for index, entry := range entries {
		spec := entry.Spec()
		changes[index] = changeSignature{
			Kind: string(spec.Kind), Path: spec.Path, PreviousPath: spec.PreviousPath,
			BeforeDigest: spec.BeforeDigest.String(), AfterDigest: spec.AfterDigest.String(),
			Metadata: spec.Metadata,
		}
	}
	return requestSignature(struct {
		RunID        string                     `json:"run_id"`
		Outcome      ports.RunOutcome           `json:"outcome"`
		FailureKind  ports.TargetRunFailureKind `json:"failure_kind"`
		StartedAt    time.Time                  `json:"started_at,omitempty"`
		StoppedAt    time.Time                  `json:"stopped_at"`
		Observations []observationSignature     `json:"observations"`
		ChangeScope  domain.ChangeScope         `json:"change_scope"`
		Revision     domain.Revision            `json:"revision"`
		SealedAt     time.Time                  `json:"sealed_at"`
		Changes      []changeSignature          `json:"changes"`
	}{
		RunID: receipt.RunID.String(), Outcome: receipt.Outcome, FailureKind: receipt.FailureKind,
		StartedAt: receipt.StartedAt.UTC(), StoppedAt: receipt.StoppedAt.UTC(), Observations: observations,
		ChangeScope: receipt.TargetChanges.Scope(), Revision: receipt.TargetChanges.WorkspaceRevision(),
		SealedAt: receipt.TargetChanges.SealedAt().UTC(), Changes: changes,
	})
}

func ledgerPlacement(value domain.CollectorPlacement) ledger.CollectorPlacement {
	switch value {
	case domain.CollectorPlacementHost:
		return ledger.PlacementHost
	case domain.CollectorPlacementObserverNamespace:
		return ledger.PlacementRuntime
	case domain.CollectorPlacementGuest:
		return ledger.PlacementGuest
	case domain.CollectorPlacementInjectedProcess:
		return ledger.PlacementInjectedApp
	default:
		return ledger.PlacementUnknown
	}
}

func (c *RunObserverCoordinator) noteCursors(record *runObserverRecord, records []ledger.Record) {
	for _, stored := range records {
		if record.first == 0 || stored.Cursor < record.first {
			record.first = stored.Cursor
		}
		if stored.Cursor > record.last {
			record.last = stored.Cursor
		}
	}
}

func (c *RunObserverCoordinator) stopStartedDetached(record *runObserverRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
	defer cancel()
	var result []error
	for _, plan := range record.plans {
		if record.started[plan.CollectorID.String()] {
			_, err := c.driver.Stop(ctx, plan.CollectorID)
			result = append(result, err)
		}
	}
	return errors.Join(result...)
}

func (c *RunObserverCoordinator) evidence(record *runObserverRecord) RunObservationEvidence {
	coverage := make([]domain.CollectorCoverage, 0, len(record.coverage))
	for _, value := range record.coverage {
		coverage = append(coverage, value)
	}
	sort.Slice(coverage, func(i, j int) bool {
		left, right := coverage[i].Spec(), coverage[j].Spec()
		if left.SignalFamily == right.SignalFamily {
			return left.CollectorID.String() < right.CollectorID.String()
		}
		return left.SignalFamily < right.SignalFamily
	})
	return RunObservationEvidence{
		Required: append([]string(nil), record.start.Plan.RequiredCoverage...), FirstCursor: record.first, LastCursor: record.last,
		Artifacts: append([]domain.ArtifactReference(nil), record.artifacts...), Events: append([]domain.EventEnvelope(nil), record.events...),
		Metrics: append([]domain.MetricSample(nil), record.metrics...), Coverage: coverage,
		Gaps: append([]domain.Gap(nil), record.gaps...), StoppedAt: record.stoppedAt,
		Failures: append([]ObserverFailure(nil), record.failures...),
	}
}

func cloneRunObservationEvidence(value RunObservationEvidence) RunObservationEvidence {
	value.Required = append([]string(nil), value.Required...)
	value.Artifacts = append([]domain.ArtifactReference(nil), value.Artifacts...)
	value.Events = append([]domain.EventEnvelope(nil), value.Events...)
	value.Metrics = append([]domain.MetricSample(nil), value.Metrics...)
	value.Coverage = append([]domain.CollectorCoverage(nil), value.Coverage...)
	value.Gaps = append([]domain.Gap(nil), value.Gaps...)
	value.Failures = append([]ObserverFailure(nil), value.Failures...)
	return value
}

func appendUniqueArtifacts(existing []domain.ArtifactReference, values ...domain.ArtifactReference) []domain.ArtifactReference {
	seen := make(map[string]struct{}, len(existing)+len(values))
	for _, artifact := range existing {
		spec := artifact.Spec()
		seen[spec.Reference+"\x00"+spec.Digest.String()+"\x00"+spec.Role] = struct{}{}
	}
	for _, artifact := range values {
		spec := artifact.Spec()
		key := spec.Reference + "\x00" + spec.Digest.String() + "\x00" + spec.Role
		if _, found := seen[key]; found {
			continue
		}
		seen[key] = struct{}{}
		existing = append(existing, artifact)
	}
	return existing
}

func replaceArtifactRole(existing []domain.ArtifactReference, replacement domain.ArtifactReference) []domain.ArtifactReference {
	role := replacement.Spec().Role
	result := make([]domain.ArtifactReference, 0, len(existing)+1)
	for _, artifact := range existing {
		if artifact.Spec().Role != role {
			result = append(result, artifact)
		}
	}
	return appendUniqueArtifacts(result, replacement)
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (c *RunObserverCoordinator) writeLifecycleArtifact(record *runObserverRecord) (domain.ArtifactReference, error) {
	type failure struct {
		CollectorID, Family, Reason string
		Required                    bool
	}
	payload := struct {
		Version   uint32    `json:"version"`
		RunID     string    `json:"run_id"`
		StoppedAt time.Time `json:"stopped_at"`
		First     uint64    `json:"first_cursor"`
		Last      uint64    `json:"last_cursor"`
		Failures  []failure `json:"failures,omitempty"`
	}{Version: 1, RunID: record.start.Plan.Run.ID().String(), StoppedAt: record.stoppedAt, First: uint64(record.first), Last: uint64(record.last)}
	for _, value := range record.failures {
		payload.Failures = append(payload.Failures, failure{value.CollectorID.String(), value.Family, value.Reason, value.Required})
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	digest := domain.NewDigest(encoded)
	namespace, err := openDurableNamespace(c.stateRoot, "artifacts")
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	defer namespace.Close()
	file := strings.TrimPrefix(digest.String(), "sha256:") + ".json"
	if err := namespace.EnsureRegularAtomically(file, encoded, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return domain.ArtifactReference{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.artifact", "content", "immutable lifecycle artifact conflicts", err)
		}
		return domain.ArtifactReference{}, err
	}
	return domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "observer://runs/" + record.start.Plan.Run.ID().String() + "/lifecycle/" + digest.String(),
		Digest:    digest, Size: int64(len(encoded)), Role: "collector.lifecycle", Sensitivity: domain.SensitivityInternal,
	})
}

func cloneCollectorPlan(plan ports.CollectorPlan) ports.CollectorPlan {
	plan.Resources = plan.Resources.Clone()
	return plan
}

func cloneInterruptedCollectorBindings(values []ports.InterruptedCollectorBinding) []ports.InterruptedCollectorBinding {
	result := make([]ports.InterruptedCollectorBinding, len(values))
	for index, value := range values {
		result[index] = ports.InterruptedCollectorBinding{Plan: cloneCollectorPlan(value.Plan), StartCommitted: value.StartCommitted}
	}
	return result
}

func validateObserverMarkerBindings(marker observerStateMarker) error {
	runID, err := domain.ParseTargetRunID(marker.RunID)
	if err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "run_id", "observer state marker has an invalid run", err)
	}
	request := ports.InterruptedCollectorReconciliation{TargetRunID: runID, Collectors: cloneInterruptedCollectorBindings(marker.Collectors)}
	if err := request.Validate(); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "collectors", "observer state marker has invalid collector bindings", err)
	}
	if marker.ExternalOwnership && len(marker.Collectors) == 0 {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "external_ownership", "cannot be set without a collector binding", nil)
	}
	if (marker.StoppedResultDigest == "") != (marker.StopPreparationDigest == "") {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "stopped_preparation_digest", "result and preparation digests must be bound together", nil)
	}
	if marker.StoppedResultDigest != "" {
		if marker.Phase != "stopped" && marker.Phase != "committed" {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "stopped_result_digest", "is valid only for stopped or committed evidence", nil)
		}
		if _, err := domain.ParseDigest(marker.StoppedResultDigest); err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "stopped_result_digest", "is invalid", err)
		}
		if _, err := domain.ParseDigest(marker.StopPreparationDigest); err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "stop_preparation_digest", "is invalid", err)
		}
	}
	if marker.Journal != nil {
		if err := validateObserverJournalReference(*marker.Journal, marker.RunID, math.MaxInt64); err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "journal", "observer journal reference is invalid", err)
		}
	}
	if (marker.IntrinsicID == "") != marker.IntrinsicStartedAt.IsZero() {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "intrinsic_collector", "identity and start time must be present together", nil)
	}
	if marker.IntrinsicID == "" {
		return nil
	}
	intrinsicID, err := domain.ParseCollectorID(marker.IntrinsicID)
	if err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "intrinsic_collector_id", "is invalid", err)
	}
	for _, binding := range marker.Collectors {
		if binding.Plan.CollectorID == intrinsicID {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "intrinsic_collector_id", "duplicates an external collector", nil)
		}
	}
	return nil
}

func validatePersistedCollectorBindings(input RunObserverStart, marker observerStateMarker) error {
	if err := validateObserverMarkerBindings(marker); err != nil {
		return err
	}
	expected := make(map[string]ports.CollectorSpec, len(input.Plan.Collectors))
	for _, spec := range input.Plan.Collectors {
		key := ports.DeriveCollectorIdempotencyKey(input.Plan.IdempotencyKey, spec.Name)
		expected[key] = spec
	}
	if len(marker.Collectors) != len(expected) {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "collectors", "persisted collector count differs from the exact run plan", nil)
	}
	run := input.Plan.Run.Spec()
	for _, binding := range marker.Collectors {
		plan := binding.Plan
		spec, found := expected[plan.IdempotencyKey]
		if !found {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "collector.idempotency_key", "does not identify an exact planned collector", nil)
		}
		delete(expected, plan.IdempotencyKey)
		if plan.ResearchSessionID != input.ResearchSessionID || plan.LeaseID != run.LeaseID || plan.AgentWorkspaceID != run.AgentWorkspaceID || plan.AgentGeneration != run.AgentGeneration || plan.TargetID != run.TargetID || plan.TargetGeneration != run.TargetGeneration || plan.TargetRunID != run.ID || plan.Attachment != input.Prepared.Attachment || plan.Requirement != spec.Requirement || plan.Adapter != spec.Adapter || plan.Version != spec.Version || plan.ConfigurationDigest != spec.ConfigurationDigest || plan.MaximumBytes != spec.MaximumBytes || !plan.Resources.FitsWithin(spec.Resources) || !spec.Resources.FitsWithin(plan.Resources) {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "collector", "persisted binding differs from its authority-selected plan", nil)
		}
	}
	wantsIntrinsic := containsString(input.Plan.RequiredCoverage, ports.TargetLifecycleSignal)
	if wantsIntrinsic != (marker.IntrinsicID != "") {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.recover_interrupted", "intrinsic_collector", "persisted binding differs from required target lifecycle coverage", nil)
	}
	return nil
}

func appendRecoveredCollectorArtifacts(record *runObserverRecord, report ports.InterruptedCollectorReconciliationReport) error {
	for _, output := range report.Outputs {
		if output.State != ports.InterruptedCollectorOutputFinalized {
			continue
		}
		artifacts, err := validatedCollectorArtifacts(output.Artifacts)
		if err != nil {
			return err
		}
		record.artifacts = appendUniqueArtifacts(record.artifacts, artifacts...)
	}
	return nil
}

func recoveredCollectorFailureReason(base string, report ports.InterruptedCollectorReconciliationReport, collectorID domain.CollectorID) string {
	for _, output := range report.Outputs {
		if output.CollectorID != collectorID || !output.CaptureLimitExceeded {
			continue
		}
		return base + "; the recovered immutable output is a byte-limit-truncated prefix"
	}
	return base
}

func (c *RunObserverCoordinator) persistMarker(record *runObserverRecord) error {
	marker := observerStateMarker{
		Version: observerStateVersion, RunID: record.start.Plan.Run.ID().String(), PlanDigest: record.planDigest.String(),
		Signature: record.signature, Phase: record.phase, CrashCleanup: record.crashCleanup,
		ExternalOwnership: record.externalOwnership, Collectors: make([]ports.InterruptedCollectorBinding, 0, len(record.plans)),
		UpdatedAt: c.clock().UTC(),
	}
	for _, plan := range record.plans {
		marker.Collectors = append(marker.Collectors, ports.InterruptedCollectorBinding{
			Plan: cloneCollectorPlan(plan), StartCommitted: record.startCommitted[plan.CollectorID.String()],
		})
	}
	if !record.intrinsicID.IsZero() {
		marker.IntrinsicID = record.intrinsicID.String()
		marker.IntrinsicStartedAt = record.intrinsicStartedAt.UTC()
	}
	marker.StoppedResultDigest = record.stoppedResultDigest
	marker.StopPreparationDigest = record.stopPreparationDigest
	if record.journalRef != nil {
		journal := *record.journalRef
		marker.Journal = &journal
	}
	return c.writeMarker(marker)
}

func (c *RunObserverCoordinator) writeMarker(marker observerStateMarker) error {
	encoded, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if int64(len(encoded)) > maximumObserverStateMarkerBytes {
		return domain.NewError(domain.CodeResourceExhausted, "run_observers.marker", "content", "observer state marker exceeds the durable marker byte limit", nil)
	}
	namespace, err := openDurableNamespace(c.stateRoot, "runs")
	if err != nil {
		return err
	}
	defer namespace.Close()
	return namespace.ReplaceRegularAtomically(marker.RunID+".json", encoded, 0o600)
}

func (c *RunObserverCoordinator) loadMarkers() ([]observerStateMarker, error) {
	namespace, err := openDurableNamespace(c.stateRoot, "runs")
	if err != nil {
		return nil, err
	}
	defer namespace.Close()
	if err := cleanupDurableNamespaceStages(namespace); err != nil {
		return nil, err
	}
	names, err := namespace.ListNames()
	if err != nil {
		return nil, err
	}
	markers := make([]observerStateMarker, 0, len(names))
	for _, name := range names {
		if filepath.Ext(name) != ".json" {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "state", "observer state directory contains an unclaimed entry", nil)
		}
		encoded, err := namespace.ReadRegularBounded(name, maximumObserverStateMarkerBytes)
		if err != nil {
			return nil, err
		}
		var marker observerStateMarker
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&marker)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		if decodeErr != nil || !errors.Is(trailingErr, io.EOF) || marker.Version != observerStateVersion || marker.RunID == "" || marker.PlanDigest == "" || marker.Signature == "" || !validObserverPhase(marker.Phase) || marker.UpdatedAt.IsZero() || marker.Collectors == nil {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "state", "observer state marker is invalid", errors.Join(decodeErr, trailingErr))
		}
		if name != marker.RunID+".json" {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "state", "observer marker filename does not match its run", nil)
		}
		if _, err := domain.ParseDigest(marker.PlanDigest); err != nil {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "plan_digest", "observer state marker has an invalid plan digest", err)
		}
		if err := validateObserverMarkerBindings(marker); err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	return markers, nil
}

func validObserverPhase(phase string) bool {
	switch phase {
	case "starting", "active", "recovering", "stop-preparing", "stop-prepared", "stop-canceling", "stopping", "stopped", "committed":
		return true
	default:
		return false
	}
}

func observerPhaseMayOwnCollectors(phase string) bool {
	switch phase {
	case "starting", "active", "recovering", "stop-preparing", "stop-prepared", "stop-canceling", "stopping":
		return true
	default:
		return false
	}
}

func (c *RunObserverCoordinator) nowNotBefore(minimum time.Time) time.Time {
	value := c.clock().UTC()
	if value.Before(minimum) {
		return minimum.UTC()
	}
	return value
}
