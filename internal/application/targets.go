package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type CreateTargetRequest struct {
	Meta             MutationMeta      `json:"meta"`
	LeaseID          string            `json:"lease_id"`
	Template         string            `json:"template"`
	Kind             domain.TargetKind `json:"kind"`
	PolicyDigest     string            `json:"policy_digest"`
	CapabilityDigest string            `json:"capability_digest"`
}

type targetResult struct {
	TargetID string `json:"target_id"`
}

func (c *Core) CreateTarget(ctx context.Context, request CreateTargetRequest) (TargetRecord, error) {
	const operation = "target.create"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRecord{}, err
	}
	if _, err := domain.ParseLeaseID(request.LeaseID); err != nil {
		return TargetRecord{}, err
	}
	policyDigest, err := domain.ParseDigest(request.PolicyDigest)
	if err != nil {
		return TargetRecord{}, err
	}
	capabilityDigest, err := domain.ParseDigest(request.CapabilityDigest)
	if err != nil {
		return TargetRecord{}, err
	}
	if strings.TrimSpace(request.Template) == "" || !request.Kind.IsValid() {
		return TargetRecord{}, invalidArgument(operation, "template_kind", "target template and recognized kind are required", nil)
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return TargetRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "create_target", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		targetID, err := c.ids.TargetID()
		if err != nil {
			return nil, err
		}
		sessionID, err := requireStoredID("lease.session_id", lease.SessionID, domain.ParseResearchSessionID)
		if err != nil {
			return nil, err
		}
		targetModel, err := domain.NewTarget(targetID, sessionID, request.Kind, domain.InitialTargetGeneration, now)
		if err != nil {
			return nil, err
		}
		generationModel, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{TargetID: targetID, Generation: domain.InitialTargetGeneration, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, CreatedAt: now})
		if err != nil {
			return nil, err
		}
		target := TargetRecord{ID: targetID.String(), SessionID: lease.SessionID, LeaseID: lease.ID, CreationIdempotencyKey: request.Meta.IdempotencyKey, Template: request.Template, Kind: request.Kind, CurrentGeneration: 1, Revision: uint64(targetModel.Revision()), CreatedAt: now, UpdatedAt: now, Generations: []TargetGenerationRecord{{Generation: 1, PolicyDigest: request.PolicyDigest, CapabilityDigest: request.CapabilityDigest, State: generationModel.State(), Revision: uint64(generationModel.Revision()), CreatedAt: now, UpdatedAt: now}}}
		if err := appendControl(ctx, tx, "target", target.ID, "target.created", target.Revision, target); err != nil {
			return nil, err
		}
		return json.Marshal(targetResult{TargetID: target.ID})
	})
	if err != nil {
		return TargetRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return TargetRecord{}, err
	}
	var result targetResult
	if err := json.Unmarshal(response, &result); err != nil {
		return TargetRecord{}, err
	}
	return cloneTarget(c.targets[result.TargetID]), nil
}

type TransitionTargetGenerationRequest struct {
	Meta             MutationMeta                 `json:"meta"`
	TargetID         string                       `json:"target_id"`
	Generation       uint64                       `json:"generation"`
	ExpectedRevision uint64                       `json:"expected_revision"`
	State            domain.TargetGenerationState `json:"state"`
}

type QuarantineTargetRequest struct {
	Meta             MutationMeta                   `json:"meta"`
	TargetID         string                         `json:"target_id"`
	ExpectedRevision uint64                         `json:"expected_revision"`
	Reason           string                         `json:"reason"`
	Evidence         ports.TargetQuarantineEvidence `json:"evidence"`
}

// QuarantineTarget atomically reflects a previously proven physical
// containment. The orchestration layer must obtain backend evidence before it
// calls this method; this method deliberately performs no physical action.
func (c *Core) QuarantineTarget(ctx context.Context, request QuarantineTargetRequest) (TargetRecord, error) {
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRecord{}, err
	}
	if _, err := domain.ParseTargetID(request.TargetID); err != nil {
		return TargetRecord{}, err
	}
	if request.Reason != strings.TrimSpace(request.Reason) || request.Reason == "" || len(request.Reason) > 4096 {
		return TargetRecord{}, domain.NewError(domain.CodeInvalidArgument, "target.quarantine", "reason", "must be trimmed, non-empty, and at most 4096 bytes", nil)
	}
	return c.mutateTargetJournalDuringTermination(ctx, "quarantine_target", request.Meta, request, request.TargetID, func(target *TargetRecord, record func(string) error) error {
		if target.Revision != request.ExpectedRevision {
			return store.ErrRevisionConflict
		}
		generation, err := findTargetGeneration(target, target.CurrentGeneration)
		if err != nil {
			return err
		}
		targetID, err := requireStoredID("target.id", target.ID, domain.ParseTargetID)
		if err != nil {
			return err
		}
		expected := ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)}
		if err := request.Evidence.Validate(expected); err != nil {
			return err
		}
		if generation.State == domain.TargetGenerationQuarantined {
			return nil
		}
		if err := domain.RequireTargetGenerationTransition(generation.State, domain.TargetGenerationQuarantined); err != nil {
			return err
		}
		now := c.clock().UTC()
		for index := range target.Operations {
			operation := &target.Operations[index]
			if operation.Generation != target.CurrentGeneration || operation.State.Terminal() {
				continue
			}
			if err := domain.RequireTargetOperationTransition(operation.State, domain.TargetOperationCancelled); err != nil {
				return err
			}
			operation.State, operation.Revision, operation.UpdatedAt = domain.TargetOperationCancelled, operation.Revision+1, now
		}
		for index := range target.Runs {
			run := &target.Runs[index]
			if run.Generation != target.CurrentGeneration || run.State.Terminal() {
				continue
			}
			if err := domain.RequireTargetRunTransition(run.State, domain.TargetRunQuarantined); err != nil {
				return err
			}
			run.State, run.Revision, run.UpdatedAt = domain.TargetRunQuarantined, run.Revision+1, now
		}
		generation.State, generation.Revision, generation.UpdatedAt = domain.TargetGenerationQuarantined, generation.Revision+1, now
		return record("target.quarantined")
	})
}

func (c *Core) TransitionTargetGeneration(ctx context.Context, request TransitionTargetGenerationRequest) (TargetRecord, error) {
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRecord{}, err
	}
	return c.mutateTarget(ctx, "transition_target_generation", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		generation, err := findTargetGeneration(target, request.Generation)
		if err != nil {
			return "", err
		}
		if generation.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if err := domain.RequireTargetGenerationTransition(generation.State, request.State); err != nil {
			return "", err
		}
		generation.State, generation.Revision, generation.UpdatedAt = request.State, generation.Revision+1, c.clock().UTC()
		return "target_generation.transitioned", nil
	})
}

type StartTargetRunRequest struct {
	Meta                   MutationMeta `json:"meta"`
	TargetID               string       `json:"target_id"`
	MaterializationDigest  string       `json:"materialization_digest"`
	SpecimenOccurrenceRefs []string     `json:"specimen_occurrence_refs,omitempty"`
	FixtureRefs            []string     `json:"fixture_refs,omitempty"`
}
type runResult struct {
	TargetID string `json:"target_id"`
	RunID    string `json:"run_id"`
}

func (c *Core) StartTargetRun(ctx context.Context, request StartTargetRunRequest) (TargetRunRecord, error) {
	const operation = "target_run.start"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRunRecord{}, err
	}
	materialDigest, err := domain.ParseDigest(request.MaterializationDigest)
	if err != nil {
		return TargetRunRecord{}, err
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return TargetRunRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "start_target_run", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		target, ok := detachedRecord(c.targets, request.TargetID, cloneTarget)
		if !ok {
			return nil, ErrNotFound
		}
		lease := c.leases[target.LeaseID]
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		if _, err := c.requireAgentGenerationAcceptingWork(operation, lease.AgentWorkspaceID, lease.AgentGeneration); err != nil {
			return nil, err
		}
		generation, err := findTargetGeneration(&target, target.CurrentGeneration)
		if err != nil {
			return nil, err
		}
		if generation.State != domain.TargetGenerationReady && generation.State != domain.TargetGenerationResettable {
			return nil, failedPrecondition(operation, "target_generation", "is not ready", nil)
		}
		for _, run := range target.Runs {
			if run.Generation == target.CurrentGeneration && !run.State.Terminal() {
				return nil, failedPrecondition(operation, "target_generation", "already has an active run", nil)
			}
		}
		now := c.clock().UTC()
		runID, err := c.ids.TargetRunID()
		if err != nil {
			return nil, err
		}
		leaseID, err := requireStoredID("target.lease_id", target.LeaseID, domain.ParseLeaseID)
		if err != nil {
			return nil, err
		}
		targetID, err := requireStoredID("target.id", target.ID, domain.ParseTargetID)
		if err != nil {
			return nil, err
		}
		agentID, err := requireStoredID("lease.agent_workspace_id", lease.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
		if err != nil {
			return nil, err
		}
		runModel, err := domain.NewTargetRun(domain.TargetRunSpec{ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(target.CurrentGeneration), AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(lease.AgentGeneration), MaterializationDigest: materialDigest, CreatedAt: now})
		if err != nil {
			return nil, err
		}
		run := TargetRunRecord{ID: runID.String(), Generation: target.CurrentGeneration, AgentWorkspaceID: lease.AgentWorkspaceID, AgentGeneration: lease.AgentGeneration, MaterializationDigest: request.MaterializationDigest, State: runModel.State(), Revision: uint64(runModel.Revision()), CreatedAt: now, UpdatedAt: now}
		target.Runs = append(target.Runs, run)
		target.Revision++
		target.UpdatedAt = now
		if err := appendControl(ctx, tx, "target", target.ID, "target_run.requested", target.Revision, target); err != nil {
			return nil, err
		}
		return json.Marshal(runResult{TargetID: target.ID, RunID: run.ID})
	})
	if err != nil {
		return TargetRunRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return TargetRunRecord{}, err
	}
	var result runResult
	if err := json.Unmarshal(response, &result); err != nil {
		return TargetRunRecord{}, err
	}
	run, err := findRunPointer(c.targets[result.TargetID].Runs, result.RunID)
	if err != nil {
		return TargetRunRecord{}, err
	}
	return *run, nil
}

func findRunPointer(runs []TargetRunRecord, id string) (*TargetRunRecord, error) {
	for index := range runs {
		if runs[index].ID == id {
			return &runs[index], nil
		}
	}
	return nil, ErrNotFound
}

type TransitionTargetRunRequest struct {
	Meta             MutationMeta          `json:"meta"`
	TargetID         string                `json:"target_id"`
	RunID            string                `json:"run_id"`
	ExpectedRevision uint64                `json:"expected_revision"`
	State            domain.TargetRunState `json:"state"`
}

func (c *Core) TransitionTargetRun(ctx context.Context, request TransitionTargetRunRequest) (TargetRunRecord, error) {
	const operation = "target_run.transition"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRunRecord{}, err
	}
	mutate := c.mutateTarget
	if request.State.Terminal() {
		mutate = c.mutateTargetDuringTermination
	}
	target, err := mutate(ctx, "transition_target_run", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		run, err := findRun(target, request.RunID)
		if err != nil {
			return "", err
		}
		if run.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if request.State == domain.TargetRunCompleted || request.State == domain.TargetRunFailed {
			return "", failedPrecondition(operation, "state", "terminal runs must use FinalizeTargetRun with a bundle", nil)
		}
		if err := domain.RequireTargetRunTransition(run.State, request.State); err != nil {
			return "", err
		}
		run.State, run.Revision, run.UpdatedAt = request.State, run.Revision+1, c.clock().UTC()
		return "target_run.transitioned", nil
	})
	if err != nil {
		return TargetRunRecord{}, err
	}
	run, err := findRun(&target, request.RunID)
	if err != nil {
		return TargetRunRecord{}, err
	}
	return *run, nil
}

type FinalizeTargetRunRequest struct {
	Meta             MutationMeta `json:"meta"`
	TargetID         string       `json:"target_id"`
	RunID            string       `json:"run_id"`
	ExpectedRevision uint64       `json:"expected_revision"`
	Failed           bool         `json:"failed"`
	BundleID         string       `json:"bundle_id"`
	BundleArtifact   string       `json:"bundle_artifact,omitempty"`
	BundleDigest     string       `json:"bundle_digest,omitempty"`
	IncidentIDs      []string     `json:"incident_ids,omitempty"`
}

func (c *Core) FinalizeTargetRun(ctx context.Context, request FinalizeTargetRunRequest) (TargetRunRecord, error) {
	const operation = "target_run.finalize"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRunRecord{}, err
	}
	if _, err := domain.ParseObservationBundleID(request.BundleID); err != nil {
		return TargetRunRecord{}, err
	}
	if (request.BundleArtifact == "") != (request.BundleDigest == "") {
		return TargetRunRecord{}, invalidArgument(operation, "bundle", "artifact and digest must be provided together", nil)
	}
	if request.BundleDigest != "" {
		if _, err := domain.ParseDigest(request.BundleDigest); err != nil {
			return TargetRunRecord{}, err
		}
	}
	for _, id := range request.IncidentIDs {
		if _, err := domain.ParseIncidentID(id); err != nil {
			return TargetRunRecord{}, err
		}
	}
	normalizedIncidentIDs, err := normalizedNonBlank(request.IncidentIDs)
	if err != nil {
		return TargetRunRecord{}, err
	}
	request.IncidentIDs = normalizedIncidentIDs
	target, err := c.mutateTargetJournalDuringTermination(ctx, "finalize_target_run", request.Meta, request, request.TargetID, func(target *TargetRecord, record func(string) error) error {
		run, err := findRun(target, request.RunID)
		if err != nil {
			return err
		}
		if run.Revision != request.ExpectedRevision {
			return store.ErrRevisionConflict
		}
		for _, incidentID := range request.IncidentIDs {
			incident, ok := c.incidents[incidentID]
			if !ok {
				return ErrNotFound
			}
			if incident.SessionID != target.SessionID || incident.TargetID != target.ID || incident.TargetRunID != run.ID || incident.TargetGeneration != run.Generation {
				return ErrScope
			}
		}
		terminal := domain.TargetRunCompleted
		if request.Failed {
			terminal = domain.TargetRunFailed
		}
		// A failed run may be finalized with sealed evidence from any active
		// lifecycle state. Provisioning and observation failures can happen
		// before the run reaches Running, so forcing those states through
		// Finalizing would make authoritative compensation impossible.
		if !request.Failed && run.State != domain.TargetRunFinalizing {
			if err := domain.RequireTargetRunTransition(run.State, domain.TargetRunFinalizing); err != nil {
				return err
			}
			run.State, run.Revision, run.UpdatedAt = domain.TargetRunFinalizing, run.Revision+1, c.clock().UTC()
			if err := record("target_run.finalizing"); err != nil {
				return err
			}
		}
		if err := domain.RequireTargetRunTransition(run.State, terminal); err != nil {
			return err
		}
		run.State, run.Revision, run.UpdatedAt, run.BundleID = terminal, run.Revision+1, c.clock().UTC(), request.BundleID
		run.BundleArtifact, run.BundleDigest = request.BundleArtifact, request.BundleDigest
		run.IncidentIDs, err = mergedNonBlank(run.IncidentIDs, request.IncidentIDs...)
		if err != nil {
			return err
		}
		generation, err := findTargetGeneration(target, run.Generation)
		if err == nil && generation.State == domain.TargetGenerationReady {
			if domain.RequireTargetGenerationTransition(generation.State, domain.TargetGenerationResettable) == nil {
				generation.State, generation.Revision, generation.UpdatedAt = domain.TargetGenerationResettable, generation.Revision+1, c.clock().UTC()
			}
		}
		return record("target_run.finalized")
	})
	if err != nil {
		return TargetRunRecord{}, err
	}
	run, err := findRun(&target, request.RunID)
	if err != nil {
		return TargetRunRecord{}, err
	}
	return *run, nil
}

type CreateTargetOperationRequest struct {
	Meta           MutationMeta               `json:"meta"`
	TargetID       string                     `json:"target_id"`
	RunID          string                     `json:"run_id"`
	Kind           domain.TargetOperationKind `json:"kind"`
	CommandDisplay string                     `json:"command_display,omitempty"`
	ContentDigest  string                     `json:"content_digest,omitempty"`
}
type operationResult struct {
	TargetID    string `json:"target_id"`
	OperationID string `json:"operation_id"`
}

func (c *Core) CreateTargetOperation(ctx context.Context, request CreateTargetOperationRequest) (TargetOperationRecord, error) {
	const operationName = "target_operation.create"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetOperationRecord{}, err
	}
	if len(request.CommandDisplay) > 4096 {
		return TargetOperationRecord{}, resourceExhausted(operationName, "command_display", "exceeds 4096 bytes", nil)
	}
	var contentDigest domain.Digest
	var err error
	if request.ContentDigest != "" {
		contentDigest, err = domain.ParseDigest(request.ContentDigest)
		if err != nil {
			return TargetOperationRecord{}, err
		}
	}
	requestBytes, _ := json.Marshal(request)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return TargetOperationRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "create_target_operation", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		target, ok := detachedRecord(c.targets, request.TargetID, cloneTarget)
		if !ok {
			return nil, ErrNotFound
		}
		run, err := findRun(&target, request.RunID)
		if err != nil {
			return nil, err
		}
		if run.State != domain.TargetRunRunning {
			return nil, failedPrecondition(operationName, "target_run", "is not running", nil)
		}
		lease, ok := c.leases[target.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		if _, err := c.requireAgentGenerationAcceptingWork(operationName, run.AgentWorkspaceID, run.AgentGeneration); err != nil {
			return nil, err
		}
		operationID, err := c.ids.TargetOperationID()
		if err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		leaseID, err := requireStoredID("target.lease_id", target.LeaseID, domain.ParseLeaseID)
		if err != nil {
			return nil, err
		}
		targetID, err := requireStoredID("target.id", target.ID, domain.ParseTargetID)
		if err != nil {
			return nil, err
		}
		runID, err := requireStoredID("target_run.id", run.ID, domain.ParseTargetRunID)
		if err != nil {
			return nil, err
		}
		model, err := domain.NewTargetOperation(domain.TargetOperationSpec{ID: operationID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(run.Generation), TargetRunID: runID, Kind: request.Kind, CommandDisplay: request.CommandDisplay, ContentDigest: contentDigest, CreatedAt: now})
		if err != nil {
			return nil, err
		}
		operation := TargetOperationRecord{ID: operationID.String(), RunID: run.ID, Generation: run.Generation, Kind: request.Kind, CommandDisplay: request.CommandDisplay, ContentDigest: request.ContentDigest, State: model.State(), Revision: uint64(model.Revision()), CreatedAt: now, UpdatedAt: now}
		target.Operations = append(target.Operations, operation)
		target.Revision++
		target.UpdatedAt = now
		if err := appendControl(ctx, tx, "target", target.ID, "target_operation.requested", target.Revision, target); err != nil {
			return nil, err
		}
		return json.Marshal(operationResult{TargetID: target.ID, OperationID: operation.ID})
	})
	if err != nil {
		return TargetOperationRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return TargetOperationRecord{}, err
	}
	var result operationResult
	if err := json.Unmarshal(response, &result); err != nil {
		return TargetOperationRecord{}, err
	}
	operation, err := findOperationPointer(c.targets[result.TargetID].Operations, result.OperationID)
	if err != nil {
		return TargetOperationRecord{}, err
	}
	return *operation, nil
}

func findOperationPointer(values []TargetOperationRecord, id string) (*TargetOperationRecord, error) {
	for index := range values {
		if values[index].ID == id {
			return &values[index], nil
		}
	}
	return nil, ErrNotFound
}

type ResetTargetRequest struct {
	Meta               MutationMeta    `json:"meta"`
	TargetID           string          `json:"target_id"`
	ExpectedRevision   uint64          `json:"expected_revision"`
	Mode               ports.ResetMode `json:"reset_mode"`
	SnapshotName       string          `json:"snapshot_name,omitempty"`
	RecoveryIncidentID string          `json:"recovery_incident_id,omitempty"`
}

// ResetTarget seals the old realization and creates exactly the next target
// generation. It deliberately does not touch lease.AgentGeneration or the
// agent-workspace record.
func (c *Core) ResetTarget(ctx context.Context, request ResetTargetRequest) (TargetRecord, error) {
	const operation = "target.reset"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetRecord{}, err
	}
	if err := ports.ValidateResetSelection(request.Mode, request.SnapshotName); err != nil {
		return TargetRecord{}, err
	}
	var incidentID domain.IncidentID
	var err error
	if request.RecoveryIncidentID != "" {
		incidentID, err = domain.ParseIncidentID(request.RecoveryIncidentID)
		if err != nil {
			return TargetRecord{}, err
		}
	}
	return c.mutateTarget(ctx, "reset_target", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		if target.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		for _, run := range target.Runs {
			if run.Generation == target.CurrentGeneration && !run.State.Terminal() {
				return "", failedPrecondition(operation, "target_run", fmt.Sprintf("%s must be finalized before reset", run.ID), nil)
			}
		}
		old, err := findTargetGeneration(target, target.CurrentGeneration)
		if err != nil {
			return "", err
		}
		if old.State == domain.TargetGenerationResettable {
			if err := domain.RequireTargetGenerationTransition(old.State, domain.TargetGenerationDestroyed); err != nil {
				return "", err
			}
			old.State, old.Revision, old.UpdatedAt = domain.TargetGenerationDestroyed, old.Revision+1, c.clock().UTC()
		} else if old.State != domain.TargetGenerationFailed && old.State != domain.TargetGenerationDestroyed {
			return "", failedPrecondition(operation, "target_generation", fmt.Sprintf("in %s cannot be reset", old.State), nil)
		}
		newGeneration := old.Generation + 1
		policyDigest, err := domain.ParseDigest(old.PolicyDigest)
		if err != nil {
			return "", err
		}
		capabilityDigest, err := domain.ParseDigest(old.CapabilityDigest)
		if err != nil {
			return "", err
		}
		now := c.clock().UTC()
		targetID, err := requireStoredID("target.id", target.ID, domain.ParseTargetID)
		if err != nil {
			return "", err
		}
		model, err := domain.NewTargetGeneration(domain.TargetGenerationSpec{TargetID: targetID, Generation: domain.TargetGeneration(newGeneration), PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest, PreviousGeneration: domain.TargetGeneration(old.Generation), RecoveryIncidentID: incidentID, CreatedAt: now})
		if err != nil {
			return "", err
		}
		target.CurrentGeneration = newGeneration
		target.Generations = append(target.Generations, TargetGenerationRecord{Generation: newGeneration, PolicyDigest: old.PolicyDigest, CapabilityDigest: old.CapabilityDigest, Previous: old.Generation, RecoveryIncident: request.RecoveryIncidentID, State: model.State(), Revision: uint64(model.Revision()), CreatedAt: now, UpdatedAt: now})
		return "target.reset", nil
	})
}

type TransitionTargetOperationRequest struct {
	Meta             MutationMeta                `json:"meta"`
	TargetID         string                      `json:"target_id"`
	OperationID      string                      `json:"operation_id"`
	ExpectedRevision uint64                      `json:"expected_revision"`
	State            domain.TargetOperationState `json:"state"`
}

func (c *Core) TransitionTargetOperation(ctx context.Context, request TransitionTargetOperationRequest) (TargetOperationRecord, error) {
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return TargetOperationRecord{}, err
	}
	mutate := c.mutateTarget
	if request.State.Terminal() {
		mutate = c.mutateTargetDuringTermination
	}
	target, err := mutate(ctx, "transition_target_operation", request.Meta, request, request.TargetID, func(target *TargetRecord) (string, error) {
		operation, err := findOperation(target, request.OperationID)
		if err != nil {
			return "", err
		}
		if operation.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if err := domain.RequireTargetOperationTransition(operation.State, request.State); err != nil {
			return "", err
		}
		operation.State, operation.Revision, operation.UpdatedAt = request.State, operation.Revision+1, c.clock().UTC()
		return "target_operation.transitioned", nil
	})
	if err != nil {
		return TargetOperationRecord{}, err
	}
	operation, err := findOperation(&target, request.OperationID)
	if err != nil {
		return TargetOperationRecord{}, err
	}
	return *operation, nil
}

func (c *Core) mutateTarget(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, mutate func(*TargetRecord) (string, error)) (TargetRecord, error) {
	return c.mutateTargetWithLeaseMode(ctx, namespace, meta, request, targetID, targetLeaseActiveOnly, mutate)
}

func (c *Core) mutateTargetDuringTermination(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, mutate func(*TargetRecord) (string, error)) (TargetRecord, error) {
	return c.mutateTargetWithLeaseMode(ctx, namespace, meta, request, targetID, targetLeaseCleanup, mutate)
}

func (c *Core) mutateTargetWithLeaseMode(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, leaseMode targetLeaseMode, mutate func(*TargetRecord) (string, error)) (TargetRecord, error) {
	journal := c.mutateTargetJournal
	if leaseMode == targetLeaseCleanup {
		journal = c.mutateTargetJournalDuringTermination
	}
	return journal(ctx, namespace, meta, request, targetID, func(target *TargetRecord, record func(string) error) error {
		event, err := mutate(target)
		if err != nil {
			return err
		}
		return record(event)
	})
}

func (c *Core) mutateTargetJournal(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, mutate func(*TargetRecord, func(string) error) error) (TargetRecord, error) {
	return c.mutateTargetJournalWithLeaseMode(ctx, namespace, meta, request, targetID, targetLeaseActiveOnly, mutate)
}

func (c *Core) mutateTargetJournalDuringTermination(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, mutate func(*TargetRecord, func(string) error) error) (TargetRecord, error) {
	return c.mutateTargetJournalWithLeaseMode(ctx, namespace, meta, request, targetID, targetLeaseCleanup, mutate)
}

type targetLeaseMode uint8

const (
	targetLeaseActiveOnly targetLeaseMode = iota
	targetLeaseCleanup
)

func (c *Core) mutateTargetJournalWithLeaseMode(ctx context.Context, namespace string, meta MutationMeta, request any, targetID string, leaseMode targetLeaseMode, mutate func(*TargetRecord, func(string) error) error) (TargetRecord, error) {
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return TargetRecord{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return TargetRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, namespace, meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		target, ok := detachedRecord(c.targets, targetID, cloneTarget)
		if !ok {
			return nil, ErrNotFound
		}
		lease := c.leases[target.LeaseID]
		if err := requireTargetMutationLease(lease, c.clock(), leaseMode); err != nil {
			return nil, err
		}
		record := func(event string) error {
			target.Revision++
			target.UpdatedAt = c.clock().UTC()
			if err := appendControl(ctx, tx, "target", target.ID, event, target.Revision, target); err != nil {
				return err
			}
			return nil
		}
		if err := mutate(&target, record); err != nil {
			return nil, err
		}
		return json.Marshal(target)
	})
	if err != nil {
		return TargetRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return TargetRecord{}, err
	}
	var target TargetRecord
	if err := json.Unmarshal(response, &target); err != nil {
		return TargetRecord{}, err
	}
	return cloneTarget(target), nil
}

func requireTargetMutationLease(lease LeaseRecord, now time.Time, mode targetLeaseMode) error {
	if mode == targetLeaseCleanup && lease.Termination.InProgress() {
		return nil
	}
	return requireLeaseActive(lease, now)
}
