package orchestration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const observerEvidenceJournalVersion = uint32(1)

type observerJournalReference struct {
	File   string `json:"file"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type observerEvidenceJournal struct {
	Version      uint32                          `json:"version"`
	RunID        string                          `json:"run_id"`
	PlanDigest   string                          `json:"plan_digest"`
	Evidence     persistedRunEvidence            `json:"evidence"`
	Completed    []observerJournalCompletion     `json:"completed"`
	Pending      *observerJournalStep            `json:"pending,omitempty"`
	Recovery     *persistedRecoveryReport        `json:"recovery,omitempty"`
	Receipt      *persistedTargetStopReceipt     `json:"receipt,omitempty"`
	StopBatch    *persistedCollectorStopBatch    `json:"collector_stop_batch,omitempty"`
	StopFailures []persistedCollectorStopFailure `json:"collector_stop_failures,omitempty"`
}

type observerJournalCompletion struct {
	Key       string `json:"key"`
	EventID   string `json:"event_id"`
	Cursor    uint64 `json:"cursor"`
	ChainHash string `json:"chain_hash"`
}

type observerJournalStep struct {
	Key      string                    `json:"key"`
	Record   ledger.Record             `json:"record"`
	Gap      *persistedGap             `json:"gap,omitempty"`
	Coverage *persistedCoverage        `json:"coverage,omitempty"`
	Event    *persistedEvent           `json:"event,omitempty"`
	Failure  *persistedObserverFailure `json:"failure,omitempty"`
}

type persistedRecoveryReport struct {
	TargetRunID string                    `json:"target_run_id"`
	Outputs     []persistedRecoveryOutput `json:"outputs"`
}

type persistedRecoveryOutput struct {
	CollectorID          string                                `json:"collector_id"`
	State                ports.InterruptedCollectorOutputState `json:"state"`
	Artifacts            []persistedArtifact                   `json:"artifacts"`
	CaptureLimitExceeded bool                                  `json:"capture_limit_exceeded"`
}

type persistedTargetStopReceipt struct {
	RunID        string                       `json:"run_id"`
	Outcome      ports.RunOutcome             `json:"outcome"`
	FailureKind  ports.TargetRunFailureKind   `json:"failure_kind"`
	StartedAt    time.Time                    `json:"started_at,omitempty"`
	StoppedAt    time.Time                    `json:"stopped_at"`
	Observations []persistedTargetObservation `json:"observations"`
	Changes      persistedChangeSet           `json:"target_changes"`
}

type persistedTargetObservation struct {
	Kind              string          `json:"kind"`
	ObservedAt        time.Time       `json:"observed_at"`
	TargetOperationID string          `json:"target_operation_id,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

// persistedCollectorStopBatch is written before any stop-derived ledger
// record. It retains both the exact driver output that can be represented by
// domain value objects and the deterministic validation decision used by the
// evidence layer. Only a batch for which every physical teardown was proven is
// accepted here; an unconfirmed attempt remains retryable and cannot produce
// ledger evidence.
type persistedCollectorStopBatch struct {
	StoppedAt time.Time                      `json:"stopped_at"`
	Results   []persistedCollectorStopResult `json:"results"`
}

type persistedCollectorStopResult struct {
	CollectorID       string              `json:"collector_id"`
	Coverage          *persistedCoverage  `json:"coverage,omitempty"`
	Artifacts         []persistedArtifact `json:"artifacts"`
	AcceptedArtifacts []persistedArtifact `json:"accepted_artifacts"`
	StoppedAt         time.Time           `json:"stopped_at,omitempty"`
	TeardownConfirmed bool                `json:"teardown_confirmed"`
	StopError         string              `json:"stop_error,omitempty"`
	FailureReason     string              `json:"failure_reason,omitempty"`
}

type persistedCollectorStopFailure struct {
	CollectorID string `json:"collector_id"`
	Reason      string `json:"reason"`
}

func persistRecoveryReport(value ports.InterruptedCollectorReconciliationReport) persistedRecoveryReport {
	result := persistedRecoveryReport{TargetRunID: value.TargetRunID.String(), Outputs: make([]persistedRecoveryOutput, len(value.Outputs))}
	for index, output := range value.Outputs {
		result.Outputs[index] = persistedRecoveryOutput{
			CollectorID: output.CollectorID.String(), State: output.State,
			Artifacts: persistArtifacts(output.Artifacts), CaptureLimitExceeded: output.CaptureLimitExceeded,
		}
	}
	return result
}

func (value persistedRecoveryReport) restore() (ports.InterruptedCollectorReconciliationReport, error) {
	runID, err := domain.ParseTargetRunID(value.TargetRunID)
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	result := ports.InterruptedCollectorReconciliationReport{TargetRunID: runID, Outputs: make([]ports.InterruptedCollectorOutput, len(value.Outputs))}
	for index, output := range value.Outputs {
		collectorID, parseErr := domain.ParseCollectorID(output.CollectorID)
		if parseErr != nil {
			return ports.InterruptedCollectorReconciliationReport{}, parseErr
		}
		artifacts, restoreErr := restoreArtifacts(output.Artifacts)
		if restoreErr != nil {
			return ports.InterruptedCollectorReconciliationReport{}, restoreErr
		}
		result.Outputs[index] = ports.InterruptedCollectorOutput{
			CollectorID: collectorID, State: output.State, Artifacts: artifacts,
			CaptureLimitExceeded: output.CaptureLimitExceeded,
		}
	}
	return result, nil
}

func persistTargetStopReceipt(value ports.TargetRunStopReceipt) persistedTargetStopReceipt {
	result := persistedTargetStopReceipt{
		RunID: value.RunID.String(), Outcome: value.Outcome, FailureKind: value.FailureKind,
		StartedAt: value.StartedAt.UTC(), StoppedAt: value.StoppedAt.UTC(),
		Observations: make([]persistedTargetObservation, len(value.Observations)), Changes: persistChangeSet(value.TargetChanges),
	}
	for index, observation := range value.Observations {
		result.Observations[index] = persistedTargetObservation{
			Kind: observation.Kind, ObservedAt: observation.ObservedAt.UTC(),
			TargetOperationID: observation.TargetOperationID.String(), Payload: append(json.RawMessage(nil), observation.Payload...),
		}
	}
	return result
}

func (value persistedTargetStopReceipt) restore() (ports.TargetRunStopReceipt, error) {
	runID, err := domain.ParseTargetRunID(value.RunID)
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	changes, err := value.Changes.restore()
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	result := ports.TargetRunStopReceipt{
		RunID: runID, Outcome: value.Outcome, FailureKind: value.FailureKind,
		StartedAt: value.StartedAt.UTC(), StoppedAt: value.StoppedAt.UTC(),
		Observations: make([]ports.TargetRunObservation, len(value.Observations)), TargetChanges: changes,
	}
	for index, observation := range value.Observations {
		var operationID domain.TargetOperationID
		if observation.TargetOperationID != "" {
			operationID, err = domain.ParseTargetOperationID(observation.TargetOperationID)
			if err != nil {
				return ports.TargetRunStopReceipt{}, err
			}
		}
		result.Observations[index] = ports.TargetRunObservation{
			Kind: observation.Kind, ObservedAt: observation.ObservedAt.UTC(), TargetOperationID: operationID,
			Payload: append(json.RawMessage(nil), observation.Payload...),
		}
	}
	if err := result.Validate(); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	return result, nil
}

func (c *RunObserverCoordinator) initializeJournal(record *runObserverRecord) error {
	if record.journal.Version != 0 {
		return nil
	}
	record.journal = observerEvidenceJournal{
		Version: observerEvidenceJournalVersion, RunID: record.start.Plan.Run.ID().String(), PlanDigest: record.planDigest.String(),
		Completed: []observerJournalCompletion{},
	}
	return c.checkpointObserverJournal(record)
}

func (c *RunObserverCoordinator) checkpointObserverJournal(record *runObserverRecord) error {
	if record.journal.Version == 0 {
		record.journal = observerEvidenceJournal{Completed: []observerJournalCompletion{}}
	}
	evidence, err := persistRunObservationEvidence(c.ledger, c.evidence(record))
	if err != nil {
		return err
	}
	record.journal.Version = observerEvidenceJournalVersion
	record.journal.RunID = record.start.Plan.Run.ID().String()
	record.journal.PlanDigest = record.planDigest.String()
	record.journal.Evidence = evidence
	return c.persistObserverJournal(record)
}

func (c *RunObserverCoordinator) persistObserverJournal(record *runObserverRecord) error {
	if err := c.removeObsoleteObserverJournals(record); err != nil {
		return err
	}
	canonical, err := encodeObserverJournal(record.journal)
	if err != nil {
		return err
	}
	if int64(len(canonical)) > c.maxJournalBytes {
		return domain.NewError(domain.CodeResourceExhausted, "run_observers.journal", "content", "observer evidence journal exceeds configured byte limit", nil)
	}
	digest := domain.NewDigest(canonical)
	file := record.journal.RunID + "-" + strings.TrimPrefix(digest.String(), "sha256:") + ".json"
	namespace, err := openDurableNamespace(c.stateRoot, "journals")
	if err != nil {
		return err
	}
	if err := namespace.EnsureRegularAtomically(file, canonical, 0o600); err != nil {
		_ = namespace.Close()
		if errors.Is(err, safepath.ErrConflict) {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "content", "content-addressed journal conflicts", err)
		}
		return err
	}
	if err := namespace.Close(); err != nil {
		return err
	}
	previous := record.journalRef
	record.journalRef = &observerJournalReference{File: file, Digest: digest.String(), Size: int64(len(canonical))}
	record.journalDirty = true
	if err := c.persistMarker(record); err != nil {
		record.journalRef = previous
		return err
	}
	record.journalDirty = false
	if previous != nil && previous.File != file {
		record.obsoleteJournalRefs = append(record.obsoleteJournalRefs, *previous)
	}
	return c.removeObsoleteObserverJournals(record)
}

func (c *RunObserverCoordinator) removeObsoleteObserverJournals(record *runObserverRecord) error {
	for len(record.obsoleteJournalRefs) > 0 {
		if err := c.removeObserverJournal(record.obsoleteJournalRefs[0]); err != nil {
			return err
		}
		record.obsoleteJournalRefs = record.obsoleteJournalRefs[1:]
	}
	return nil
}

func (c *RunObserverCoordinator) removeObserverJournal(ref observerJournalReference) (resultErr error) {
	runID, digest, err := observerJournalFileIdentity(ref.File)
	if err != nil || digest != ref.Digest {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "obsolete_reference", "obsolete observer journal reference is invalid", err)
	}
	if err := validateObserverJournalReference(ref, runID, c.maxJournalBytes); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "obsolete_reference", "obsolete observer journal reference is invalid", err)
	}
	namespace, err := openDurableNamespace(c.stateRoot, "journals")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, namespace.Close()) }()
	encoded, err := namespace.ReadRegularBounded(ref.File, c.maxJournalBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	journal, err := decodeCanonicalObserverJournal(encoded)
	if err != nil || journal.RunID != runID || int64(len(encoded)) != ref.Size || domain.NewDigest(encoded).String() != ref.Digest {
		return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "obsolete_content", "obsolete observer journal differs from its content address", err)
	}
	return namespace.RemoveRegular(ref.File)
}

// pruneUnreferencedObserverJournals removes only complete, canonical,
// content-addressed checkpoints that no durable marker references. It runs in
// the startup reconciliation path, closing the crash window between publishing
// a new journal and atomically replacing its marker.
func (c *RunObserverCoordinator) pruneUnreferencedObserverJournals(markers []observerStateMarker) (resultErr error) {
	live := make(map[string]struct{}, len(markers))
	for _, marker := range markers {
		if marker.Journal != nil {
			live[marker.Journal.File] = struct{}{}
		}
	}
	namespace, err := openDurableNamespace(c.stateRoot, "journals")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, namespace.Close()) }()
	if err := cleanupDurableNamespaceStages(namespace); err != nil {
		return err
	}
	names, err := namespace.ListNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, retained := live[name]; retained {
			continue
		}
		runID, digest, err := observerJournalFileIdentity(name)
		if err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "journals", "journal directory contains an unclaimed entry", err)
		}
		encoded, err := namespace.ReadRegularBounded(name, c.maxJournalBytes)
		if err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "journals", "unreferenced journal is not a bounded regular file", err)
		}
		journal, err := decodeCanonicalObserverJournal(encoded)
		if err != nil || journal.RunID != runID || domain.NewDigest(encoded).String() != digest {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.reconcile", "journals", "unreferenced journal is not a canonical content-addressed checkpoint", err)
		}
		if err := namespace.RemoveRegular(name); err != nil {
			return err
		}
	}
	return nil
}

func encodeObserverJournal(value observerEvidenceJournal) ([]byte, error) {
	if value.Version != observerEvidenceJournalVersion || value.RunID == "" || value.PlanDigest == "" || value.Completed == nil {
		return nil, fmt.Errorf("observer evidence journal identity is incomplete")
	}
	if _, err := domain.ParseTargetRunID(value.RunID); err != nil {
		return nil, err
	}
	if _, err := domain.ParseDigest(value.PlanDigest); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(value.Completed))
	for _, completion := range value.Completed {
		if strings.TrimSpace(completion.Key) == "" || completion.EventID == "" || completion.Cursor == 0 {
			return nil, fmt.Errorf("observer journal has blank completed step")
		}
		if _, err := domain.ParseEventID(completion.EventID); err != nil {
			return nil, fmt.Errorf("observer journal completed event id: %w", err)
		}
		if len(completion.ChainHash) != 64 {
			return nil, fmt.Errorf("observer journal completed chain hash is invalid")
		}
		if _, ok := seen[completion.Key]; ok {
			return nil, fmt.Errorf("observer journal has duplicate completed step")
		}
		seen[completion.Key] = struct{}{}
	}
	if value.Pending != nil {
		if strings.TrimSpace(value.Pending.Key) == "" || value.Pending.Record.EventID == "" {
			return nil, fmt.Errorf("observer journal pending step is incomplete")
		}
		if _, ok := seen[value.Pending.Key]; ok {
			return nil, fmt.Errorf("observer journal step is both pending and complete")
		}
		if err := value.Pending.Record.Validate(); err != nil {
			return nil, err
		}
		actions := 0
		if value.Pending.Gap != nil {
			actions++
		}
		if value.Pending.Coverage != nil {
			actions++
		}
		if value.Pending.Event != nil {
			actions++
		}
		if actions != 1 {
			return nil, fmt.Errorf("observer journal step must have exactly one evidence action")
		}
	}
	if value.Receipt != nil {
		if _, err := value.Receipt.restore(); err != nil {
			return nil, fmt.Errorf("observer journal target receipt: %w", err)
		}
	}
	if value.Recovery != nil {
		if _, err := value.Recovery.restore(); err != nil {
			return nil, fmt.Errorf("observer journal recovery report: %w", err)
		}
	}
	if value.StopBatch != nil {
		if value.StopBatch.StoppedAt.IsZero() || value.StopBatch.Results == nil {
			return nil, fmt.Errorf("observer journal collector stop batch is incomplete")
		}
		seenCollectors := make(map[string]struct{}, len(value.StopBatch.Results))
		for _, result := range value.StopBatch.Results {
			if _, err := domain.ParseCollectorID(result.CollectorID); err != nil || !result.TeardownConfirmed {
				return nil, fmt.Errorf("observer journal collector stop result is invalid")
			}
			if _, duplicate := seenCollectors[result.CollectorID]; duplicate {
				return nil, fmt.Errorf("observer journal collector stop result is duplicated")
			}
			seenCollectors[result.CollectorID] = struct{}{}
		}
	}
	seenStopFailures := make(map[string]struct{}, len(value.StopFailures))
	for _, failure := range value.StopFailures {
		if _, err := domain.ParseCollectorID(failure.CollectorID); err != nil || strings.TrimSpace(failure.Reason) == "" {
			return nil, fmt.Errorf("observer journal collector stop failure is invalid")
		}
		if _, duplicate := seenStopFailures[failure.CollectorID]; duplicate {
			return nil, fmt.Errorf("observer journal collector stop failure is duplicated")
		}
		seenStopFailures[failure.CollectorID] = struct{}{}
	}
	return json.Marshal(value)
}

func (c *RunObserverCoordinator) loadObserverJournal(ref observerJournalReference, runID, planDigest string) (observerEvidenceJournal, error) {
	if err := validateObserverJournalReference(ref, runID, c.maxJournalBytes); err != nil {
		return observerEvidenceJournal{}, err
	}
	namespace, err := openDurableNamespace(c.stateRoot, "journals")
	if err != nil {
		return observerEvidenceJournal{}, err
	}
	encoded, err := namespace.ReadRegularBounded(ref.File, c.maxJournalBytes)
	closeErr := namespace.Close()
	if err != nil {
		return observerEvidenceJournal{}, err
	}
	if closeErr != nil {
		return observerEvidenceJournal{}, closeErr
	}
	if int64(len(encoded)) != ref.Size || domain.NewDigest(encoded).String() != ref.Digest {
		return observerEvidenceJournal{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "identity", "journal differs from marker", nil)
	}
	value, err := decodeCanonicalObserverJournal(encoded)
	if err != nil {
		return observerEvidenceJournal{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "canonical", "journal is invalid or non-canonical", err)
	}
	if value.RunID != runID || value.PlanDigest != planDigest {
		return observerEvidenceJournal{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "scope", "journal differs from marker scope", nil)
	}
	return value, nil
}

func decodeCanonicalObserverJournal(encoded []byte) (observerEvidenceJournal, error) {
	var value observerEvidenceJournal
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return observerEvidenceJournal{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return observerEvidenceJournal{}, fmt.Errorf("observer journal contains trailing JSON: %w", err)
	}
	canonical, err := encodeObserverJournal(value)
	if err != nil {
		return observerEvidenceJournal{}, err
	}
	if !bytes.Equal(canonical, encoded) {
		return observerEvidenceJournal{}, fmt.Errorf("observer journal is not canonical")
	}
	return value, nil
}

func validateObserverJournalReference(ref observerJournalReference, runID string, maximum int64) error {
	if _, err := domain.ParseDigest(ref.Digest); err != nil {
		return err
	}
	fileRunID, fileDigest, err := observerJournalFileIdentity(ref.File)
	if err != nil || fileRunID != runID || fileDigest != ref.Digest || ref.Size <= 0 || ref.Size > maximum {
		return fmt.Errorf("observer journal reference is invalid")
	}
	return nil
}

func observerJournalFileIdentity(file string) (string, string, error) {
	if !strings.HasSuffix(file, ".json") {
		return "", "", fmt.Errorf("observer journal filename has no JSON suffix")
	}
	base := strings.TrimSuffix(file, ".json")
	separator := len(base) - 65
	if separator <= 0 || base[separator] != '-' {
		return "", "", fmt.Errorf("observer journal filename has no canonical digest suffix")
	}
	runID, digest := base[:separator], "sha256:"+base[separator+1:]
	if _, err := domain.ParseTargetRunID(runID); err != nil {
		return "", "", err
	}
	parsed, err := domain.ParseDigest(digest)
	if err != nil || parsed.String() != digest {
		return "", "", fmt.Errorf("observer journal filename has an invalid digest")
	}
	return runID, digest, nil
}

func (c *RunObserverCoordinator) hydrateObserverJournal(ctx context.Context, record *runObserverRecord, marker observerStateMarker) error {
	if marker.Journal == nil {
		return nil
	}
	journal, err := c.loadObserverJournal(*marker.Journal, marker.RunID, marker.PlanDigest)
	if err != nil {
		return err
	}
	evidence, err := journal.Evidence.restore(c.ledger)
	if err != nil {
		return err
	}
	record.artifacts = append([]domain.ArtifactReference(nil), evidence.Artifacts...)
	record.events = append([]domain.EventEnvelope(nil), evidence.Events...)
	record.metrics = append([]domain.MetricSample(nil), evidence.Metrics...)
	record.gaps = append([]domain.Gap(nil), evidence.Gaps...)
	record.failures = append([]ObserverFailure(nil), evidence.Failures...)
	record.coverage = make(map[string]domain.CollectorCoverage, len(evidence.Coverage))
	for _, item := range evidence.Coverage {
		record.coverage[item.CollectorID().String()] = item
	}
	record.first = evidence.FirstCursor
	record.last = evidence.LastCursor
	record.stoppedAt = evidence.StoppedAt
	record.journal = journal
	copy := *marker.Journal
	record.journalRef = &copy
	if journal.Pending != nil {
		if err := c.resumeObserverJournalStep(ctx, record, *journal.Pending); err != nil {
			return err
		}
	}
	return nil
}

func (c *RunObserverCoordinator) runObserverJournalStep(ctx context.Context, record *runObserverRecord, step observerJournalStep) error {
	if err := c.initializeJournal(record); err != nil {
		return err
	}
	for _, completion := range record.journal.Completed {
		if completion.Key == step.Key {
			expected := step.Record
			expected.EventID = completion.EventID
			stored, err := c.appendExactObserverRecord(ctx, expected)
			if err != nil {
				return err
			}
			if uint64(stored.Cursor) != completion.Cursor || fmt.Sprintf("%x", stored.ChainHash[:]) != completion.ChainHash {
				return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "completed", "completed evidence ledger binding changed", nil)
			}
			// A prior marker durability error can leave the in-memory journal
			// complete while its older durable marker still names the pending
			// version. Re-checkpoint only in that ambiguous state.
			if record.journalDirty {
				return c.checkpointObserverJournal(record)
			}
			return nil
		}
	}
	if record.journal.Pending != nil {
		if record.journal.Pending.Key != step.Key {
			return domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "pending", "different evidence step is pending", nil)
		}
		if err := c.checkpointObserverJournal(record); err != nil {
			return err
		}
		return c.resumeObserverJournalStep(ctx, record, *record.journal.Pending)
	}
	record.journal.Pending = &step
	if err := c.checkpointObserverJournal(record); err != nil {
		return err
	}
	return c.resumeObserverJournalStep(ctx, record, step)
}

func observerJournalStepKey(kind string, value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return kind + "/" + strings.TrimPrefix(domain.NewDigest(encoded).String(), "sha256:"), nil
}

func (c *RunObserverCoordinator) resumeObserverJournalStep(ctx context.Context, record *runObserverRecord, step observerJournalStep) error {
	stored, err := c.appendExactObserverRecord(ctx, step.Record)
	if err != nil {
		return err
	}
	c.noteCursors(record, []ledger.Record{stored})
	if step.Gap != nil {
		value := *step.Gap
		value.FirstCursor = uint64(stored.Cursor)
		value.LastCursor = uint64(stored.Cursor)
		items, e := restoreGaps([]persistedGap{value})
		if e != nil {
			return e
		}
		record.gaps = append(record.gaps, items[0])
	}
	if step.Coverage != nil {
		items, e := restoreCoverage([]persistedCoverage{*step.Coverage})
		if e != nil {
			return e
		}
		record.coverage[items[0].CollectorID().String()] = items[0]
	}
	if step.Event != nil {
		value := *step.Event
		value.LedgerCursor = uint64(stored.Cursor)
		value.LedgerChainHash = fmt.Sprintf("%x", stored.ChainHash[:])
		event, e := value.restore(stored)
		if e != nil {
			return e
		}
		record.events = append(record.events, event)
	}
	if step.Failure != nil {
		items, e := restoreObserverFailures([]persistedObserverFailure{*step.Failure})
		if e != nil {
			return e
		}
		record.failures = append(record.failures, items[0])
	}
	record.journal.Pending = nil
	record.journal.Completed = append(record.journal.Completed, observerJournalCompletion{
		Key: step.Key, EventID: stored.EventID, Cursor: uint64(stored.Cursor), ChainHash: fmt.Sprintf("%x", stored.ChainHash[:]),
	})
	sort.Slice(record.journal.Completed, func(i, j int) bool { return record.journal.Completed[i].Key < record.journal.Completed[j].Key })
	return c.checkpointObserverJournal(record)
}

func (c *RunObserverCoordinator) appendExactObserverRecord(ctx context.Context, expected ledger.Record) (ledger.Record, error) {
	records, err := c.ledger.ReadAfter(0, 0)
	if err != nil {
		return ledger.Record{}, err
	}
	var matched []ledger.Record
	for _, record := range records {
		if record.EventID == expected.EventID {
			matched = append(matched, record)
		}
	}
	if len(matched) > 1 {
		return ledger.Record{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "event_id", "multiple ledger records claim one observer event", nil)
	}
	if len(matched) == 1 {
		if !sameObserverRecord(expected, matched[0]) {
			return ledger.Record{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "ledger_record", "existing observer event differs from pending intent", nil)
		}
		return matched[0], nil
	}
	result, err := c.ledger.Append(ctx, expected)
	if err != nil {
		return ledger.Record{}, err
	}
	if result.Duplicate || len(result.Records) != 1 || !sameObserverRecord(expected, result.Records[0]) {
		return ledger.Record{}, domain.NewError(domain.CodeIntegrityViolation, "run_observers.journal", "append", "ledger did not append the exact observer event", nil)
	}
	return result.Records[0], nil
}

func sameObserverRecord(expected, actual ledger.Record) bool {
	actual.Cursor = 0
	actual.ChainHash = [32]byte{}
	return reflect.DeepEqual(expected, actual)
}
