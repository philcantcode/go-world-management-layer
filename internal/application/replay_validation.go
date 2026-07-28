package application

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

const replayOperation = "application.replay"

// decodeReplayAggregate treats a control payload as untrusted persisted data.
// It validates the JSON shape and semantic content before the caller projects
// any of it into memory.
func decodeReplayAggregate[T any](record store.ControlRecord, identity func(T) (string, uint64), validate func(T) error) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(record.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, replayViolation("payload", "persisted aggregate is not strict JSON", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return value, err
	}

	payloadID, payloadRevision := identity(value)
	if payloadID != record.AggregateID {
		return value, replayViolation("aggregate_id", "payload identity does not match the control envelope", nil)
	}
	if payloadRevision != record.Revision {
		return value, replayViolation("revision", "payload revision does not match the control envelope", nil)
	}
	if err := validate(value); err != nil {
		if domain.IsCode(err, domain.CodeIntegrityViolation) {
			return value, err
		}
		return value, replayViolation("payload", "persisted aggregate violates semantic invariants", err)
	}
	return value, nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return replayViolation("payload", "persisted aggregate contains trailing JSON values", err)
	}
	return nil
}

func replayViolation(field, message string, cause error) error {
	return integrityViolation(replayOperation, field, message, cause)
}

func parsePersisted[T any](field, value string, parse func(string) (T, error)) (T, error) {
	parsed, err := parse(value)
	if err != nil {
		var zero T
		return zero, replayViolation(field, "persisted identity is invalid", err)
	}
	return parsed, nil
}

func parseOptionalPersisted[T any](field, value string, parse func(string) (T, error)) (T, error) {
	if value == "" {
		var zero T
		return zero, nil
	}
	return parsePersisted(field, value, parse)
}

func parsePersistedDigest(field, value string) (domain.Digest, error) {
	return parsePersisted(field, value, domain.ParseDigest)
}

func requirePersistedState(field string, valid bool) error {
	if !valid {
		return replayViolation(field, "persisted state is not recognized", nil)
	}
	return nil
}

func requirePersistedRevision(field string, revision uint64) error {
	if !domain.Revision(revision).IsValid() {
		return replayViolation(field, "persisted revision must be positive", nil)
	}
	return nil
}

func requirePersistedTimes(prefix, createdName string, created, updated time.Time) error {
	if created.IsZero() {
		return replayViolation(prefix+"."+createdName, "persisted timestamp must be set", nil)
	}
	if updated.IsZero() {
		return replayViolation(prefix+".updated_at", "persisted timestamp must be set", nil)
	}
	if updated.Before(created) {
		return replayViolation(prefix+".updated_at", "persisted update precedes creation", nil)
	}
	return nil
}

func requireContainedTimes(prefix string, parentCreated, parentUpdated, created, updated time.Time) error {
	if err := requirePersistedTimes(prefix, "created_at", created, updated); err != nil {
		return err
	}
	if created.Before(parentCreated) {
		return replayViolation(prefix+".created_at", "persisted child predates its aggregate", nil)
	}
	if updated.After(parentUpdated) {
		return replayViolation(prefix+".updated_at", "persisted child update exceeds its aggregate update", nil)
	}
	return nil
}

func requireNonBlankPersisted(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return replayViolation(field, "persisted value must not be blank", nil)
	}
	return nil
}

func validateGenerationSequence[T any](field string, current uint64, values []T, generation func(T) uint64, validate func(int, T) error) error {
	if current == 0 || uint64(len(values)) != current {
		return replayViolation(field, "persisted generations must contain every generation through the current generation", nil)
	}
	for index, value := range values {
		expected := uint64(index + 1)
		if generation(value) != expected {
			return replayViolation(fmt.Sprintf("%s[%d].generation", field, index), "persisted generations must be ordered and contiguous", nil)
		}
		if err := validate(index, value); err != nil {
			return err
		}
	}
	return nil
}

func rememberPersistedID[T any](seen map[string]T, field, value string, associated T) error {
	if _, exists := seen[value]; exists {
		return replayViolation(field, "persisted identity is duplicated", nil)
	}
	seen[value] = associated
	return nil
}

func validatePersistedIncidentIDs(field string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		itemField := fmt.Sprintf("%s[%d]", field, index)
		if _, err := parsePersisted(itemField, value, domain.ParseIncidentID); err != nil {
			return err
		}
		if err := rememberPersistedID(seen, itemField, value, struct{}{}); err != nil {
			return err
		}
	}
	return nil
}

func validateReplaySession(value SessionRecord) error {
	sessionID, err := parsePersisted("session.id", value.ID, domain.ParseResearchSessionID)
	if err != nil {
		return err
	}
	if _, err := parsePersisted("session.lease_id", value.LeaseID, domain.ParseLeaseID); err != nil {
		return err
	}
	if _, err := parsePersisted("session.agent_workspace_id", value.AgentWorkspaceID, domain.ParseAgentWorkspaceID); err != nil {
		return err
	}
	if _, err := parsePersisted("session.input_view_id", value.InputViewID, domain.ParseInputViewID); err != nil {
		return err
	}
	if _, err := parsePersistedDigest("session.policy_digest", value.PolicyDigest); err != nil {
		return err
	}
	if _, err := parsePersistedDigest("session.capability_digest", value.CapabilityDigest); err != nil {
		return err
	}
	if err := requireNonBlankPersisted("session.owner_subject", value.OwnerSubject); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(value.AcquisitionIdempotencyKey) {
		return replayViolation("session.acquisition_idempotency_key", "persisted acquisition key is invalid", nil)
	}
	if _, err := domain.NewResearchSession(sessionID, value.CreatedAt); err != nil {
		return replayViolation("session", "persisted session creation fields are invalid", err)
	}
	if err := requirePersistedState("session.state", value.State.IsValid()); err != nil {
		return err
	}
	if err := requirePersistedRevision("session.revision", value.Revision); err != nil {
		return err
	}
	return requirePersistedTimes("session", "created_at", value.CreatedAt, value.UpdatedAt)
}

func validateReplayLease(value LeaseRecord) error {
	leaseID, err := parsePersisted("lease.id", value.ID, domain.ParseLeaseID)
	if err != nil {
		return err
	}
	sessionID, err := parsePersisted("lease.session_id", value.SessionID, domain.ParseResearchSessionID)
	if err != nil {
		return err
	}
	agentID, err := parsePersisted("lease.agent_workspace_id", value.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return err
	}
	inputViewID, err := parsePersisted("lease.input_view_id", value.InputViewID, domain.ParseInputViewID)
	if err != nil {
		return err
	}
	policyDigest, err := parsePersistedDigest("lease.policy_digest", value.PolicyDigest)
	if err != nil {
		return err
	}
	capabilityDigest, err := parsePersistedDigest("lease.capability_digest", value.CapabilityDigest)
	if err != nil {
		return err
	}
	_, err = domain.NewLease(domain.LeaseSpec{
		ID: leaseID, ResearchSessionID: sessionID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.AgentGeneration(value.AgentGeneration), InputViewID: inputViewID,
		PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
		ExpiresAt: value.ExpiresAt, CreatedAt: value.CreatedAt,
	})
	if err != nil {
		return replayViolation("lease", "persisted lease creation fields are invalid", err)
	}
	if err := requirePersistedState("lease.state", value.State.IsValid()); err != nil {
		return err
	}
	if err := requirePersistedRevision("lease.revision", value.Revision); err != nil {
		return err
	}
	if err := validateReplayLeaseTermination(value); err != nil {
		return err
	}
	return requirePersistedTimes("lease", "created_at", value.CreatedAt, value.UpdatedAt)
}

func validateReplayLeaseTermination(value LeaseRecord) error {
	termination := value.Termination
	if termination.Empty() {
		if value.State == domain.LeaseReleasing || value.State == domain.LeaseReleased || value.State == domain.LeaseExpired {
			return replayViolation("lease.termination", "persisted terminal or releasing lease lacks a termination intent", nil)
		}
		return nil
	}
	if !termination.Kind.IsValid() {
		return replayViolation("lease.termination.kind", "persisted termination kind is not recognized", nil)
	}
	if !termination.State.IsValid() {
		return replayViolation("lease.termination.state", "persisted termination state is not recognized", nil)
	}
	if err := requireNonBlankPersisted("lease.termination.reason", termination.Reason); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(termination.BeginIdempotencyKey) {
		return replayViolation("lease.termination.begin_idempotency_key", "persisted idempotency key is invalid", nil)
	}
	if _, err := parsePersistedDigest("lease.termination.begin_request_digest", termination.BeginRequestDigest); err != nil {
		return err
	}
	if termination.InitiatedLeaseRevision < 2 || termination.InitiatedLeaseRevision > value.Revision {
		return replayViolation("lease.termination.initiated_lease_revision", "persisted initiating revision is outside the lease history", nil)
	}
	if termination.InitiatedAt.IsZero() || termination.InitiatedAt.Before(value.CreatedAt) || termination.InitiatedAt.After(value.UpdatedAt) {
		return replayViolation("lease.termination.initiated_at", "persisted initiation timestamp is outside the lease lifetime", nil)
	}

	switch termination.Kind {
	case LeaseTerminationRelease:
		if termination.State != LeaseTerminationReleasing && termination.State != LeaseTerminationReleased {
			return replayViolation("lease.termination.state", "release intent has an expiry state", nil)
		}
		if termination.State == LeaseTerminationReleasing && value.State != domain.LeaseReleasing {
			return replayViolation("lease.state", "releasing intent does not match lease state", nil)
		}
		if termination.State == LeaseTerminationReleased && value.State != domain.LeaseReleased {
			return replayViolation("lease.state", "released intent does not match lease state", nil)
		}
		if !termination.InitiatedAt.Before(value.ExpiresAt) {
			return replayViolation("lease.termination.initiated_at", "release began after the lease deadline", nil)
		}
	case LeaseTerminationExpiry:
		if termination.State != LeaseTerminationExpiring && termination.State != LeaseTerminationExpired {
			return replayViolation("lease.termination.state", "expiry intent has a release state", nil)
		}
		if termination.InitiatedAt.Before(value.ExpiresAt) {
			return replayViolation("lease.termination.initiated_at", "expiry began before the lease deadline", nil)
		}
		if termination.State == LeaseTerminationExpiring && value.State != domain.LeaseActive {
			return replayViolation("lease.state", "expiring intent does not match lease state", nil)
		}
		if termination.State == LeaseTerminationExpired && value.State != domain.LeaseExpired {
			return replayViolation("lease.state", "expired intent does not match lease state", nil)
		}
	}

	if termination.State.Terminal() {
		if !domain.IsCanonicalIdempotencyKey(termination.CompleteIdempotencyKey) {
			return replayViolation("lease.termination.complete_idempotency_key", "persisted idempotency key is invalid", nil)
		}
		if _, err := parsePersistedDigest("lease.termination.complete_request_digest", termination.CompleteRequestDigest); err != nil {
			return err
		}
		if termination.CompletedAt.IsZero() || termination.CompletedAt.Before(termination.InitiatedAt) || !termination.CompletedAt.Equal(value.UpdatedAt) {
			return replayViolation("lease.termination.completed_at", "persisted completion timestamp is inconsistent", nil)
		}
		if value.Revision != termination.InitiatedLeaseRevision+1 {
			return replayViolation("lease.revision", "terminal lease revision does not follow its termination intent", nil)
		}
	} else if termination.CompleteIdempotencyKey != "" || termination.CompleteRequestDigest != "" || !termination.CompletedAt.IsZero() {
		return replayViolation("lease.termination", "unfinished termination contains completion identity", nil)
	} else if value.Revision != termination.InitiatedLeaseRevision {
		return replayViolation("lease.revision", "unfinished termination revision changed after initiation", nil)
	}
	return nil
}

func validateReplayAgentWorkspace(value AgentWorkspaceRecord) error {
	agentID, err := parsePersisted("agent_workspace.id", value.ID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return err
	}
	sessionID, err := parsePersisted("agent_workspace.session_id", value.SessionID, domain.ParseResearchSessionID)
	if err != nil {
		return err
	}
	if _, err := domain.NewAgentWorkspace(agentID, sessionID, domain.AgentGeneration(value.CurrentGeneration), value.CreatedAt); err != nil {
		return replayViolation("agent_workspace", "persisted agent workspace creation fields are invalid", err)
	}
	if err := requirePersistedRevision("agent_workspace.revision", value.Revision); err != nil {
		return err
	}
	if err := requirePersistedTimes("agent_workspace", "created_at", value.CreatedAt, value.UpdatedAt); err != nil {
		return err
	}
	workspaceIDs := make(map[string]struct{}, len(value.Generations))
	return validateGenerationSequence("agent_workspace.generations", value.CurrentGeneration, value.Generations,
		func(generation AgentGenerationRecord) uint64 { return generation.Generation },
		func(index int, generation AgentGenerationRecord) error {
			prefix := fmt.Sprintf("agent_workspace.generations[%d]", index)
			workspaceID, err := parsePersisted(prefix+".workspace_id", generation.WorkspaceID, domain.ParseWorkspaceID)
			if err != nil {
				return err
			}
			if err := rememberPersistedID(workspaceIDs, prefix+".workspace_id", generation.WorkspaceID, struct{}{}); err != nil {
				return err
			}
			inputViewID, err := parsePersisted(prefix+".input_view_id", generation.InputViewID, domain.ParseInputViewID)
			if err != nil {
				return err
			}
			policyDigest, err := parsePersistedDigest(prefix+".policy_digest", generation.PolicyDigest)
			if err != nil {
				return err
			}
			capabilityDigest, err := parsePersistedDigest(prefix+".capability_digest", generation.CapabilityDigest)
			if err != nil {
				return err
			}
			if err := validateAgentProvisioningBinding(prefix, generation); err != nil {
				return err
			}
			recoveryIncident, err := parseOptionalPersisted(prefix+".recovery_incident", generation.RecoveryIncident, domain.ParseIncidentID)
			if err != nil {
				return err
			}
			_, err = domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
				AgentWorkspaceID: agentID, Generation: domain.AgentGeneration(generation.Generation), WorkspaceID: workspaceID,
				InputViewID: inputViewID, PolicyDigest: policyDigest, CapabilityFingerprintDigest: capabilityDigest,
				PreviousGeneration: domain.AgentGeneration(generation.Previous), RecoveryIncidentID: recoveryIncident,
				CreatedAt: generation.CreatedAt,
			})
			if err != nil {
				return replayViolation(prefix, "persisted agent generation creation fields are invalid", err)
			}
			if err := requirePersistedState(prefix+".state", generation.State.IsValid()); err != nil {
				return err
			}
			if err := requirePersistedRevision(prefix+".revision", generation.Revision); err != nil {
				return err
			}
			return requireContainedTimes(prefix, value.CreatedAt, value.UpdatedAt, generation.CreatedAt, generation.UpdatedAt)
		})
}

func validateReplayExec(value ExecRecord) error {
	execID, err := parsePersisted("exec.id", value.ID, domain.ParseExecID)
	if err != nil {
		return err
	}
	if _, err := parsePersisted("exec.session_id", value.SessionID, domain.ParseResearchSessionID); err != nil {
		return err
	}
	leaseID, err := parsePersisted("exec.lease_id", value.LeaseID, domain.ParseLeaseID)
	if err != nil {
		return err
	}
	agentID, err := parsePersisted("exec.agent_workspace_id", value.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return err
	}
	if err := validateExecCommand(value.Executable, value.Argv, value.WorkingDirectory); err != nil {
		return replayViolation("exec.command", "persisted exec command is invalid", err)
	}
	if _, err := domain.NewExec(domain.ExecSpec{
		ID: execID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(value.AgentGeneration),
		Kind: value.Kind, Executable: value.Executable, Argv: value.Argv, WorkingDirectory: value.WorkingDirectory,
		CreatedAt: value.CreatedAt,
	}); err != nil {
		return replayViolation("exec", "persisted exec creation fields are invalid", err)
	}
	if err := requirePersistedState("exec.state", value.State.IsValid()); err != nil {
		return err
	}
	if err := requirePersistedRevision("exec.revision", value.Revision); err != nil {
		return err
	}
	if err := requirePersistedTimes("exec", "created_at", value.CreatedAt, value.UpdatedAt); err != nil {
		return err
	}
	if value.State == domain.ExecCompleted && (value.ExitCode == nil || *value.ExitCode != 0 || !value.CleanupConfirmed) {
		return replayViolation("exec.terminal", "completed exec lacks a zero exit code or confirmed cleanup", nil)
	}
	return validatePersistedIncidentIDs("exec.incident_ids", value.IncidentIDs)
}

func validateReplayTarget(value TargetRecord) error {
	targetID, err := parsePersisted("target.id", value.ID, domain.ParseTargetID)
	if err != nil {
		return err
	}
	sessionID, err := parsePersisted("target.session_id", value.SessionID, domain.ParseResearchSessionID)
	if err != nil {
		return err
	}
	leaseID, err := parsePersisted("target.lease_id", value.LeaseID, domain.ParseLeaseID)
	if err != nil {
		return err
	}
	if err := requireNonBlankPersisted("target.template", value.Template); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(value.CreationIdempotencyKey) {
		return replayViolation("target.creation_idempotency_key", "persisted creation key is invalid", nil)
	}
	if _, err := domain.NewTarget(targetID, sessionID, value.Kind, domain.TargetGeneration(value.CurrentGeneration), value.CreatedAt); err != nil {
		return replayViolation("target", "persisted target creation fields are invalid", err)
	}
	if err := requirePersistedRevision("target.revision", value.Revision); err != nil {
		return err
	}
	if err := requirePersistedTimes("target", "created_at", value.CreatedAt, value.UpdatedAt); err != nil {
		return err
	}
	if err := validateGenerationSequence("target.generations", value.CurrentGeneration, value.Generations,
		func(generation TargetGenerationRecord) uint64 { return generation.Generation },
		func(index int, generation TargetGenerationRecord) error {
			prefix := fmt.Sprintf("target.generations[%d]", index)
			policyDigest, err := parsePersistedDigest(prefix+".policy_digest", generation.PolicyDigest)
			if err != nil {
				return err
			}
			capabilityDigest, err := parsePersistedDigest(prefix+".capability_digest", generation.CapabilityDigest)
			if err != nil {
				return err
			}
			recoveryIncident, err := parseOptionalPersisted(prefix+".recovery_incident", generation.RecoveryIncident, domain.ParseIncidentID)
			if err != nil {
				return err
			}
			_, err = domain.NewTargetGeneration(domain.TargetGenerationSpec{
				TargetID: targetID, Generation: domain.TargetGeneration(generation.Generation), PolicyDigest: policyDigest,
				CapabilityFingerprintDigest: capabilityDigest, PreviousGeneration: domain.TargetGeneration(generation.Previous),
				RecoveryIncidentID: recoveryIncident, CreatedAt: generation.CreatedAt,
			})
			if err != nil {
				return replayViolation(prefix, "persisted target generation creation fields are invalid", err)
			}
			if err := requirePersistedState(prefix+".state", generation.State.IsValid()); err != nil {
				return err
			}
			if err := requirePersistedRevision(prefix+".revision", generation.Revision); err != nil {
				return err
			}
			if err := validateTargetProvisioningBinding(prefix, generation); err != nil {
				return err
			}
			return requireContainedTimes(prefix, value.CreatedAt, value.UpdatedAt, generation.CreatedAt, generation.UpdatedAt)
		}); err != nil {
		return err
	}

	runGenerations := make(map[string]uint64, len(value.Runs))
	for index, run := range value.Runs {
		prefix := fmt.Sprintf("target.runs[%d]", index)
		runID, err := parsePersisted(prefix+".id", run.ID, domain.ParseTargetRunID)
		if err != nil {
			return err
		}
		if err := rememberPersistedID(runGenerations, prefix+".id", run.ID, run.Generation); err != nil {
			return err
		}
		if run.Generation == 0 || run.Generation > value.CurrentGeneration {
			return replayViolation(prefix+".generation", "persisted run refers to a missing target generation", nil)
		}
		agentID, err := parsePersisted(prefix+".agent_workspace_id", run.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
		if err != nil {
			return err
		}
		materializationDigest, err := parsePersistedDigest(prefix+".materialization_digest", run.MaterializationDigest)
		if err != nil {
			return err
		}
		if _, err := domain.NewTargetRun(domain.TargetRunSpec{
			ID: runID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(run.Generation),
			AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(run.AgentGeneration),
			MaterializationDigest: materializationDigest, CreatedAt: run.CreatedAt,
		}); err != nil {
			return replayViolation(prefix, "persisted target run creation fields are invalid", err)
		}
		if err := requirePersistedState(prefix+".state", run.State.IsValid()); err != nil {
			return err
		}
		if err := requirePersistedRevision(prefix+".revision", run.Revision); err != nil {
			return err
		}
		if err := validateTargetRunProvisioningBinding(prefix, run); err != nil {
			return err
		}
		if err := requireContainedTimes(prefix, value.CreatedAt, value.UpdatedAt, run.CreatedAt, run.UpdatedAt); err != nil {
			return err
		}
		if _, err := parseOptionalPersisted(prefix+".bundle_id", run.BundleID, domain.ParseObservationBundleID); err != nil {
			return err
		}
		if (run.BundleArtifact == "") != (run.BundleDigest == "") {
			return replayViolation(prefix+".bundle", "persisted bundle artifact and digest must be present together", nil)
		}
		if run.BundleArtifact != "" && run.BundleID == "" {
			return replayViolation(prefix+".bundle", "persisted bundle artifact requires a bundle identity", nil)
		}
		if run.BundleDigest != "" {
			if _, err := parsePersistedDigest(prefix+".bundle_digest", run.BundleDigest); err != nil {
				return err
			}
		}
		if (run.State == domain.TargetRunCompleted || run.State == domain.TargetRunFailed) && run.BundleID == "" {
			return replayViolation(prefix+".bundle_id", "finalized run lacks its observation bundle identity", nil)
		}
		if err := validatePersistedIncidentIDs(prefix+".incident_ids", run.IncidentIDs); err != nil {
			return err
		}
	}

	operationIDs := make(map[string]struct{}, len(value.Operations))
	for index, operation := range value.Operations {
		prefix := fmt.Sprintf("target.operations[%d]", index)
		operationID, err := parsePersisted(prefix+".id", operation.ID, domain.ParseTargetOperationID)
		if err != nil {
			return err
		}
		if err := rememberPersistedID(operationIDs, prefix+".id", operation.ID, struct{}{}); err != nil {
			return err
		}
		runGeneration, exists := runGenerations[operation.RunID]
		if !exists || operation.Generation != runGeneration {
			return replayViolation(prefix+".run_id", "persisted operation does not match an aggregate run generation", nil)
		}
		runID, err := parsePersisted(prefix+".run_id", operation.RunID, domain.ParseTargetRunID)
		if err != nil {
			return err
		}
		var contentDigest domain.Digest
		if operation.ContentDigest != "" {
			contentDigest, err = parsePersistedDigest(prefix+".content_digest", operation.ContentDigest)
			if err != nil {
				return err
			}
		}
		if _, err := domain.NewTargetOperation(domain.TargetOperationSpec{
			ID: operationID, LeaseID: leaseID, TargetID: targetID, TargetGeneration: domain.TargetGeneration(operation.Generation),
			TargetRunID: runID, Kind: operation.Kind, CommandDisplay: operation.CommandDisplay,
			ContentDigest: contentDigest, CreatedAt: operation.CreatedAt,
		}); err != nil {
			return replayViolation(prefix, "persisted target operation creation fields are invalid", err)
		}
		if err := requirePersistedState(prefix+".state", operation.State.IsValid()); err != nil {
			return err
		}
		if err := requirePersistedRevision(prefix+".revision", operation.Revision); err != nil {
			return err
		}
		if err := requireContainedTimes(prefix, value.CreatedAt, value.UpdatedAt, operation.CreatedAt, operation.UpdatedAt); err != nil {
			return err
		}
	}
	return nil
}

func validateReplayIncident(value IncidentRecord) error {
	incidentID, err := parsePersisted("incident.id", value.ID, domain.ParseIncidentID)
	if err != nil {
		return err
	}
	sessionID, err := parsePersisted("incident.session_id", value.SessionID, domain.ParseResearchSessionID)
	if err != nil {
		return err
	}
	leaseID, err := parseOptionalPersisted("incident.lease_id", value.LeaseID, domain.ParseLeaseID)
	if err != nil {
		return err
	}
	agentID, err := parseOptionalPersisted("incident.agent_workspace_id", value.AgentWorkspaceID, domain.ParseAgentWorkspaceID)
	if err != nil {
		return err
	}
	execID, err := parseOptionalPersisted("incident.exec_id", value.ExecID, domain.ParseExecID)
	if err != nil {
		return err
	}
	targetID, err := parseOptionalPersisted("incident.target_id", value.TargetID, domain.ParseTargetID)
	if err != nil {
		return err
	}
	runID, err := parseOptionalPersisted("incident.target_run_id", value.TargetRunID, domain.ParseTargetRunID)
	if err != nil {
		return err
	}
	bundleID, err := parseOptionalPersisted("incident.observation_bundle_id", value.ObservationBundleID, domain.ParseObservationBundleID)
	if err != nil {
		return err
	}
	cause, err := domain.NewCauseAssessment(domain.CauseAssessmentSpec{
		Kind: value.Cause.Kind, Summary: value.Cause.Summary, Method: value.Cause.Method, Confidence: value.Cause.Confidence,
	})
	if err != nil {
		return replayViolation("incident.cause", "persisted cause is invalid", err)
	}
	metrics, err := incidentMetricModels(value.HighWaterMetrics)
	if err != nil {
		return replayViolation("incident.high_water_metrics", "persisted metrics are invalid", err)
	}
	coverage, err := incidentCoverageModels(value.Coverage)
	if err != nil {
		return replayViolation("incident.coverage", "persisted coverage is invalid", err)
	}
	artifacts, err := incidentArtifactModels(value.Artifacts)
	if err != nil {
		return replayViolation("incident.artifacts", "persisted artifacts are invalid", err)
	}
	if _, err := domain.NewIncident(domain.IncidentSpec{
		ID: incidentID, Classification: value.Classification, ResearchSessionID: sessionID, LeaseID: leaseID,
		AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(value.AgentGeneration), ExecID: execID,
		TargetID: targetID, TargetGeneration: domain.TargetGeneration(value.TargetGeneration), TargetRunID: runID,
		Trigger: value.Trigger, LastKnownState: value.LastKnownState, Cause: cause, HighWaterMetrics: metrics,
		FirstRelevantCursor: domain.ObservationCursor(value.FirstRelevantCursor), LastRelevantCursor: domain.ObservationCursor(value.LastRelevantCursor),
		Coverage: coverage, ObservationBundleID: bundleID, Artifacts: artifacts,
		RecoveryActions: value.RecoveryActions, VisibilityAcknowledgements: value.VisibilityAcknowledgements,
		OccurredAt: value.OccurredAt,
	}); err != nil {
		return replayViolation("incident", "persisted incident creation fields are invalid", err)
	}
	if err := requirePersistedState("incident.state", value.State.IsValid()); err != nil {
		return err
	}
	if err := requirePersistedRevision("incident.revision", value.Revision); err != nil {
		return err
	}
	if value.State != domain.IncidentOpen && value.ObservationBundleID == "" && len(value.Artifacts) == 0 {
		return replayViolation("incident.state", "persisted sealed incident has no evidence identity", nil)
	}
	if value.State == domain.IncidentRecovering && (len(value.RecoveryActions) == 0 || len(value.VisibilityAcknowledgements) == 0) {
		return replayViolation("incident.state", "persisted recovering incident lacks recovery and visibility records", nil)
	}
	return requirePersistedTimes("incident", "occurred_at", value.OccurredAt, value.UpdatedAt)
}

func (c *Core) validateReplaySessionProjection(value SessionRecord) error {
	if err := validateReplaySession(value); err != nil {
		return err
	}
	if previous, exists := c.sessions[value.ID]; exists {
		if previous.OwnerSubject != value.OwnerSubject || previous.AcquisitionIdempotencyKey != value.AcquisitionIdempotencyKey || previous.LeaseID != value.LeaseID ||
			previous.AgentWorkspaceID != value.AgentWorkspaceID || previous.InputViewID != value.InputViewID ||
			previous.PolicyDigest != value.PolicyDigest || previous.CapabilityDigest != value.CapabilityDigest ||
			!previous.CreatedAt.Equal(value.CreatedAt) {
			return replayViolation("session", "persisted session identity or provenance changed across revisions", nil)
		}
		if err := requireReplayStateAdvance("session", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt, previous.State, value.State, domain.RequireResearchSessionTransition); err != nil {
			return err
		}
	}
	if lease, exists := c.leases[value.LeaseID]; exists {
		if lease.SessionID != value.ID || lease.AgentWorkspaceID != value.AgentWorkspaceID ||
			lease.InputViewID != value.InputViewID || lease.PolicyDigest != value.PolicyDigest || lease.CapabilityDigest != value.CapabilityDigest {
			return replayViolation("session.lease_id", "persisted session and lease ownership disagree", nil)
		}
	}
	if agent, exists := c.agents[value.AgentWorkspaceID]; exists && agent.SessionID != value.ID {
		return replayViolation("session.agent_workspace_id", "persisted session and agent workspace ownership disagree", nil)
	}
	return nil
}

func (c *Core) validateReplayLeaseProjection(value LeaseRecord) error {
	if err := validateReplayLease(value); err != nil {
		return err
	}
	session, exists := c.sessions[value.SessionID]
	if !exists || session.LeaseID != value.ID || session.AgentWorkspaceID != value.AgentWorkspaceID ||
		session.InputViewID != value.InputViewID || session.PolicyDigest != value.PolicyDigest || session.CapabilityDigest != value.CapabilityDigest {
		return replayViolation("lease.session_id", "persisted lease does not match its session", nil)
	}
	if previous, exists := c.leases[value.ID]; exists {
		if previous.SessionID != value.SessionID || previous.AgentWorkspaceID != value.AgentWorkspaceID ||
			previous.InputViewID != value.InputViewID || previous.PolicyDigest != value.PolicyDigest ||
			previous.CapabilityDigest != value.CapabilityDigest || !previous.CreatedAt.Equal(value.CreatedAt) {
			return replayViolation("lease", "persisted lease identity or provenance changed across revisions", nil)
		}
		if err := requireReplayGenerationAdvance("lease.agent_generation", previous.AgentGeneration, value.AgentGeneration); err != nil {
			return err
		}
		if value.ExpiresAt.Before(previous.ExpiresAt) {
			return replayViolation("lease.expires_at", "persisted lease expiry moved backwards", nil)
		}
		if !value.Termination.Empty() && !value.ExpiresAt.Equal(previous.ExpiresAt) {
			return replayViolation("lease.expires_at", "persisted lease expiry changed after termination began", nil)
		}
		if !value.Termination.Empty() && value.AgentGeneration != previous.AgentGeneration {
			return replayViolation("lease.agent_generation", "persisted agent generation changed after termination began", nil)
		}
		if err := validateReplayLeaseTerminationAdvance(previous.Termination, value.Termination); err != nil {
			return err
		}
		if err := requireReplayStateAdvance("lease", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt, previous.State, value.State, domain.RequireLeaseTransition); err != nil {
			return err
		}
	} else if value.Revision != uint64(domain.InitialRevision) || value.State != domain.LeaseActive || !value.Termination.Empty() {
		return replayViolation("lease", "persisted lease history does not begin at the active initial revision", nil)
	}
	if agent, exists := c.agents[value.AgentWorkspaceID]; exists {
		if agent.SessionID != value.SessionID || value.AgentGeneration > agent.CurrentGeneration || !hasAgentGeneration(agent, value.AgentGeneration) {
			return replayViolation("lease.agent_workspace_id", "persisted lease does not resolve to its agent generation", nil)
		}
	}
	return nil
}

func validateReplayLeaseTerminationAdvance(previous, next LeaseTerminationRecord) error {
	if previous.Empty() {
		if next.Empty() || next.InProgress() {
			return nil
		}
		return replayViolation("lease.termination.state", "persisted termination skipped its in-progress state", nil)
	}
	if next.Empty() {
		return replayViolation("lease.termination", "persisted termination intent was removed", nil)
	}
	if previous.Kind != next.Kind || previous.Reason != next.Reason ||
		previous.BeginIdempotencyKey != next.BeginIdempotencyKey || previous.BeginRequestDigest != next.BeginRequestDigest ||
		previous.InitiatedLeaseRevision != next.InitiatedLeaseRevision || !previous.InitiatedAt.Equal(next.InitiatedAt) {
		return replayViolation("lease.termination", "persisted termination identity changed", nil)
	}
	if previous.State.Terminal() {
		return replayViolation("lease.termination.state", "persisted terminal termination changed", nil)
	}
	want := LeaseTerminationReleased
	if previous.State == LeaseTerminationExpiring {
		want = LeaseTerminationExpired
	}
	if next.State != want {
		return replayViolation("lease.termination.state", "persisted termination transition is invalid", nil)
	}
	return nil
}

func (c *Core) validateReplayAgentWorkspaceProjection(value AgentWorkspaceRecord) error {
	if err := validateReplayAgentWorkspace(value); err != nil {
		return err
	}
	session, sessionExists := c.sessions[value.SessionID]
	lease, leaseExists := c.leases[session.LeaseID]
	if !sessionExists || session.AgentWorkspaceID != value.ID || !leaseExists || lease.AgentWorkspaceID != value.ID || lease.SessionID != value.SessionID {
		return replayViolation("agent_workspace.session_id", "persisted agent workspace does not match its session and lease", nil)
	}
	if lease.AgentGeneration > value.CurrentGeneration || value.CurrentGeneration-lease.AgentGeneration > 1 {
		return replayViolation("agent_workspace.current_generation", "persisted agent and lease generations diverge", nil)
	}
	for index, generation := range value.Generations {
		if generation.InputViewID != lease.InputViewID || generation.PolicyDigest != lease.PolicyDigest || generation.CapabilityDigest != lease.CapabilityDigest {
			return replayViolation(fmt.Sprintf("agent_workspace.generations[%d]", index), "persisted agent generation provenance does not match its lease", nil)
		}
	}
	if previous, exists := c.agents[value.ID]; exists {
		if previous.SessionID != value.SessionID || !previous.CreatedAt.Equal(value.CreatedAt) {
			return replayViolation("agent_workspace", "persisted agent workspace identity changed across revisions", nil)
		}
		if err := requireReplayGenerationAdvance("agent_workspace.current_generation", previous.CurrentGeneration, value.CurrentGeneration); err != nil {
			return err
		}
		if err := requireReplayRevisionAdvance("agent_workspace", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt); err != nil {
			return err
		}
		if err := validateAgentGenerationHistory(previous.Generations, value.Generations); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) validateReplayExecProjection(value ExecRecord) error {
	if err := validateReplayExec(value); err != nil {
		return err
	}
	session, sessionExists := c.sessions[value.SessionID]
	lease, leaseExists := c.leases[value.LeaseID]
	agent, agentExists := c.agents[value.AgentWorkspaceID]
	if !sessionExists || !leaseExists || !agentExists || session.LeaseID != value.LeaseID ||
		lease.SessionID != value.SessionID || lease.AgentWorkspaceID != value.AgentWorkspaceID ||
		agent.SessionID != value.SessionID || !hasAgentGeneration(agent, value.AgentGeneration) {
		return replayViolation("exec.session_id", "persisted exec does not resolve to its session, lease, and agent generation", nil)
	}
	if previous, exists := c.execs[value.ID]; exists {
		if previous.SessionID != value.SessionID || previous.LeaseID != value.LeaseID || previous.AgentWorkspaceID != value.AgentWorkspaceID ||
			previous.AgentGeneration != value.AgentGeneration || previous.Kind != value.Kind || previous.Executable != value.Executable ||
			!reflect.DeepEqual(previous.Argv, value.Argv) || previous.WorkingDirectory != value.WorkingDirectory || !previous.CreatedAt.Equal(value.CreatedAt) {
			return replayViolation("exec", "persisted exec identity or command changed across revisions", nil)
		}
		if err := requireReplayStateAdvance("exec", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt, previous.State, value.State, domain.RequireExecTransition); err != nil {
			return err
		}
	}
	for index, incidentID := range value.IncidentIDs {
		incident, exists := c.incidents[incidentID]
		if !exists || incident.SessionID != value.SessionID || incident.LeaseID != value.LeaseID ||
			incident.AgentWorkspaceID != value.AgentWorkspaceID || incident.AgentGeneration != value.AgentGeneration || incident.ExecID != value.ID {
			return replayViolation(fmt.Sprintf("exec.incident_ids[%d]", index), "persisted exec incident link is inconsistent", nil)
		}
	}
	return nil
}

func (c *Core) validateReplayTargetProjection(value TargetRecord) error {
	if err := validateReplayTarget(value); err != nil {
		return err
	}
	session, sessionExists := c.sessions[value.SessionID]
	lease, leaseExists := c.leases[value.LeaseID]
	if !sessionExists || !leaseExists || session.LeaseID != value.LeaseID || lease.SessionID != value.SessionID {
		return replayViolation("target.session_id", "persisted target does not resolve to its session and lease", nil)
	}
	for runIndex, run := range value.Runs {
		agent, exists := c.agents[run.AgentWorkspaceID]
		if !exists || agent.SessionID != value.SessionID || !hasAgentGeneration(agent, run.AgentGeneration) {
			return replayViolation(fmt.Sprintf("target.runs[%d].agent_workspace_id", runIndex), "persisted run does not resolve to its agent generation", nil)
		}
		for incidentIndex, incidentID := range run.IncidentIDs {
			incident, exists := c.incidents[incidentID]
			if !exists || incident.SessionID != value.SessionID || incident.TargetID != value.ID ||
				incident.TargetGeneration != run.Generation || incident.TargetRunID != run.ID {
				return replayViolation(fmt.Sprintf("target.runs[%d].incident_ids[%d]", runIndex, incidentIndex), "persisted target run incident link is inconsistent", nil)
			}
		}
	}
	if previous, exists := c.targets[value.ID]; exists {
		if previous.SessionID != value.SessionID || previous.LeaseID != value.LeaseID || previous.CreationIdempotencyKey != value.CreationIdempotencyKey || previous.Template != value.Template ||
			previous.Kind != value.Kind || !previous.CreatedAt.Equal(value.CreatedAt) {
			return replayViolation("target", "persisted target identity changed across revisions", nil)
		}
		if err := requireReplayGenerationAdvance("target.current_generation", previous.CurrentGeneration, value.CurrentGeneration); err != nil {
			return err
		}
		if err := requireReplayRevisionAdvance("target", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt); err != nil {
			return err
		}
		if err := validateTargetGenerationHistory(previous.Generations, value.Generations); err != nil {
			return err
		}
		if err := validateTargetRunHistory(previous.Runs, value.Runs); err != nil {
			return err
		}
		if err := validateTargetOperationHistory(previous.Operations, value.Operations); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) validateReplayIncidentProjection(value IncidentRecord) error {
	if err := validateReplayIncident(value); err != nil {
		return err
	}
	session, exists := c.sessions[value.SessionID]
	if !exists {
		return replayViolation("incident.session_id", "persisted incident session does not exist", nil)
	}
	if value.LeaseID != "" {
		lease, exists := c.leases[value.LeaseID]
		if !exists || lease.SessionID != value.SessionID || session.LeaseID != value.LeaseID {
			return replayViolation("incident.lease_id", "persisted incident lease is inconsistent", nil)
		}
	}
	if value.AgentWorkspaceID != "" {
		agent, exists := c.agents[value.AgentWorkspaceID]
		if !exists || agent.SessionID != value.SessionID || (value.AgentGeneration > 0 && !hasAgentGeneration(agent, value.AgentGeneration)) {
			return replayViolation("incident.agent_workspace_id", "persisted incident agent generation is inconsistent", nil)
		}
	}
	if value.ExecID != "" {
		execution, exists := c.execs[value.ExecID]
		if !exists || execution.SessionID != value.SessionID || execution.LeaseID != value.LeaseID ||
			execution.AgentWorkspaceID != value.AgentWorkspaceID || execution.AgentGeneration != value.AgentGeneration {
			return replayViolation("incident.exec_id", "persisted incident exec is inconsistent", nil)
		}
	}
	if value.TargetID != "" {
		target, exists := c.targets[value.TargetID]
		if !exists || target.SessionID != value.SessionID || (value.LeaseID != "" && target.LeaseID != value.LeaseID) ||
			(value.TargetGeneration > 0 && !hasTargetGeneration(target, value.TargetGeneration)) {
			return replayViolation("incident.target_id", "persisted incident target generation is inconsistent", nil)
		}
		if value.TargetRunID != "" {
			run, exists := targetRunByID(target, value.TargetRunID)
			if !exists || run.Generation != value.TargetGeneration ||
				(value.AgentWorkspaceID != "" && (run.AgentWorkspaceID != value.AgentWorkspaceID || run.AgentGeneration != value.AgentGeneration)) {
				return replayViolation("incident.target_run_id", "persisted incident target run is inconsistent", nil)
			}
		}
	}
	if previous, exists := c.incidents[value.ID]; exists {
		if !sameIncidentIdentity(previous, value) {
			return replayViolation("incident", "persisted incident identity changed across revisions", nil)
		}
		if err := requireIncidentEvidenceAdvance(previous, value); err != nil {
			return err
		}
		if err := requireReplayStateAdvance("incident", previous.Revision, value.Revision, previous.UpdatedAt, value.UpdatedAt, previous.State, value.State, domain.RequireIncidentTransition); err != nil {
			return err
		}
	}
	return nil
}

func requireReplayRevisionAdvance(prefix string, previous, next uint64, previousUpdated, nextUpdated time.Time) error {
	if previous == ^uint64(0) || next != previous+1 {
		return replayViolation(prefix+".revision", fmt.Sprintf("persisted aggregate revision %d is not the successor of %d", next, previous), nil)
	}
	if nextUpdated.Before(previousUpdated) {
		return replayViolation(prefix+".updated_at", "persisted aggregate update moved backwards", nil)
	}
	return nil
}

func requireReplayGenerationAdvance(field string, previous, next uint64) error {
	if next < previous || next-previous > 1 {
		return replayViolation(field, "persisted generation did not advance monotonically by at most one", nil)
	}
	return nil
}

func requireReplayStateAdvance[S comparable](prefix string, previousRevision, nextRevision uint64, previousUpdated, nextUpdated time.Time, previousState, nextState S, transition func(S, S) error) error {
	if err := requireReplayRevisionAdvance(prefix, previousRevision, nextRevision, previousUpdated, nextUpdated); err != nil {
		return err
	}
	if previousState != nextState {
		if err := transition(previousState, nextState); err != nil {
			return replayViolation(prefix+".state", "persisted state transition is invalid", err)
		}
	}
	return nil
}

func hasAgentGeneration(agent AgentWorkspaceRecord, generation uint64) bool {
	return generation > 0 && generation <= uint64(len(agent.Generations)) && agent.Generations[generation-1].Generation == generation
}

func hasTargetGeneration(target TargetRecord, generation uint64) bool {
	return generation > 0 && generation <= uint64(len(target.Generations)) && target.Generations[generation-1].Generation == generation
}

func targetRunByID(target TargetRecord, runID string) (TargetRunRecord, bool) {
	for _, run := range target.Runs {
		if run.ID == runID {
			return run, true
		}
	}
	return TargetRunRecord{}, false
}

func validateAgentGenerationHistory(previous, next []AgentGenerationRecord) error {
	if len(next) < len(previous) || len(next) > len(previous)+1 {
		return replayViolation("agent_workspace.generations", "persisted agent generation history was removed or skipped", nil)
	}
	for index := range previous {
		before, after := previous[index], next[index]
		prefix := fmt.Sprintf("agent_workspace.generations[%d]", index)
		if before.Generation != after.Generation || before.WorkspaceID != after.WorkspaceID || before.InputViewID != after.InputViewID ||
			before.PolicyDigest != after.PolicyDigest || before.CapabilityDigest != after.CapabilityDigest || before.Previous != after.Previous ||
			before.RecoveryIncident != after.RecoveryIncident || !before.CreatedAt.Equal(after.CreatedAt) {
			return replayViolation(prefix, "persisted agent generation identity or provenance changed", nil)
		}
		if !sameAgentProvisioningBinding(before, after) && (!agentProvisioningBindingEmpty(before) || !agentProvisioningBindingComplete(after)) {
			return replayViolation(prefix+".provisioning", "persisted agent provisioning binding changed after it was established", nil)
		}
		if err := requireNestedReplayStateAdvance(prefix, before.Revision, after.Revision, before.UpdatedAt, after.UpdatedAt, before.State, after.State, domain.RequireAgentGenerationTransition); err != nil {
			return err
		}
	}
	return nil
}

func agentProvisioningBindingEmpty(generation AgentGenerationRecord) bool {
	return generation.ProvisioningPlanDigest == "" && generation.WorkspaceProvisioningKey == "" && generation.AgentProvisioningKey == ""
}

func agentProvisioningBindingComplete(generation AgentGenerationRecord) bool {
	return generation.ProvisioningPlanDigest != "" && generation.WorkspaceProvisioningKey != "" && generation.AgentProvisioningKey != ""
}

func sameAgentProvisioningBinding(left, right AgentGenerationRecord) bool {
	return left.ProvisioningPlanDigest == right.ProvisioningPlanDigest &&
		left.WorkspaceProvisioningKey == right.WorkspaceProvisioningKey && left.AgentProvisioningKey == right.AgentProvisioningKey
}

func validateAgentProvisioningBinding(prefix string, generation AgentGenerationRecord) error {
	if agentProvisioningBindingEmpty(generation) {
		return nil
	}
	if !agentProvisioningBindingComplete(generation) {
		return replayViolation(prefix+".provisioning", "persisted agent provisioning binding is partial", nil)
	}
	if _, err := parsePersistedDigest(prefix+".provisioning_plan_digest", generation.ProvisioningPlanDigest); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(generation.WorkspaceProvisioningKey) || !domain.IsCanonicalIdempotencyKey(generation.AgentProvisioningKey) {
		return replayViolation(prefix+".provisioning_key", "persisted provisioning keys are blank or have surrounding whitespace", nil)
	}
	return nil
}

func validateTargetGenerationHistory(previous, next []TargetGenerationRecord) error {
	if len(next) < len(previous) || len(next) > len(previous)+1 {
		return replayViolation("target.generations", "persisted target generation history was removed or skipped", nil)
	}
	for index := range previous {
		before, after := previous[index], next[index]
		prefix := fmt.Sprintf("target.generations[%d]", index)
		if before.Generation != after.Generation || before.PolicyDigest != after.PolicyDigest || before.CapabilityDigest != after.CapabilityDigest ||
			before.Previous != after.Previous || before.RecoveryIncident != after.RecoveryIncident || !before.CreatedAt.Equal(after.CreatedAt) {
			return replayViolation(prefix, "persisted target generation identity or provenance changed", nil)
		}
		if !sameTargetProvisioningBinding(before, after) && (!targetProvisioningBindingEmpty(before) || !targetProvisioningBindingComplete(after)) {
			return replayViolation(prefix+".provisioning", "persisted target provisioning binding changed after it was established", nil)
		}
		if err := requireNestedReplayStateAdvance(prefix, before.Revision, after.Revision, before.UpdatedAt, after.UpdatedAt, before.State, after.State, domain.RequireTargetGenerationTransition); err != nil {
			return err
		}
	}
	return nil
}

func targetProvisioningBindingEmpty(generation TargetGenerationRecord) bool {
	return generation.ProvisioningPlanDigest == "" && generation.ProvisioningKey == ""
}

func targetProvisioningBindingComplete(generation TargetGenerationRecord) bool {
	return generation.ProvisioningPlanDigest != "" && generation.ProvisioningKey != ""
}

func sameTargetProvisioningBinding(left, right TargetGenerationRecord) bool {
	return left.ProvisioningPlanDigest == right.ProvisioningPlanDigest && left.ProvisioningKey == right.ProvisioningKey
}

func validateTargetProvisioningBinding(prefix string, generation TargetGenerationRecord) error {
	if targetProvisioningBindingEmpty(generation) {
		return nil
	}
	if !targetProvisioningBindingComplete(generation) {
		return replayViolation(prefix+".provisioning", "persisted target provisioning binding is partial", nil)
	}
	if _, err := parsePersistedDigest(prefix+".provisioning_plan_digest", generation.ProvisioningPlanDigest); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(generation.ProvisioningKey) {
		return replayViolation(prefix+".provisioning_key", "persisted provisioning key is invalid", nil)
	}
	return nil
}

func validateTargetRunHistory(previous, next []TargetRunRecord) error {
	if len(next) < len(previous) {
		return replayViolation("target.runs", "persisted target run history was removed", nil)
	}
	nextByID := make(map[string]TargetRunRecord, len(next))
	for _, run := range next {
		nextByID[run.ID] = run
	}
	for index, before := range previous {
		after, exists := nextByID[before.ID]
		prefix := fmt.Sprintf("target.runs[%d]", index)
		if !exists || before.Generation != after.Generation || before.AgentWorkspaceID != after.AgentWorkspaceID ||
			before.AgentGeneration != after.AgentGeneration || before.MaterializationDigest != after.MaterializationDigest || !before.CreatedAt.Equal(after.CreatedAt) {
			return replayViolation(prefix, "persisted target run identity changed", nil)
		}
		if !sameTargetRunProvisioningBinding(before, after) && (!targetRunProvisioningBindingEmpty(before) || !targetRunProvisioningBindingComplete(after)) {
			return replayViolation(prefix+".provisioning", "persisted target run provisioning binding changed after it was established", nil)
		}
		if err := requireNestedReplayStateAdvance(prefix, before.Revision, after.Revision, before.UpdatedAt, after.UpdatedAt, before.State, after.State, domain.RequireTargetRunTransition); err != nil {
			return err
		}
		if before.Revision == after.Revision && (before.BundleID != after.BundleID || before.BundleArtifact != after.BundleArtifact || before.BundleDigest != after.BundleDigest) {
			return replayViolation(prefix+".bundle", "persisted target run bundle changed without a run revision", nil)
		}
	}
	return nil
}

func targetRunProvisioningBindingEmpty(run TargetRunRecord) bool {
	return run.ProvisioningPlanDigest == "" && run.ProvisioningKey == ""
}

func targetRunProvisioningBindingComplete(run TargetRunRecord) bool {
	return run.ProvisioningPlanDigest != "" && run.ProvisioningKey != ""
}

func sameTargetRunProvisioningBinding(left, right TargetRunRecord) bool {
	return left.ProvisioningPlanDigest == right.ProvisioningPlanDigest && left.ProvisioningKey == right.ProvisioningKey
}

func validateTargetRunProvisioningBinding(prefix string, run TargetRunRecord) error {
	if targetRunProvisioningBindingEmpty(run) {
		return nil
	}
	if !targetRunProvisioningBindingComplete(run) {
		return replayViolation(prefix+".provisioning", "persisted target run provisioning binding is partial", nil)
	}
	if _, err := parsePersistedDigest(prefix+".provisioning_plan_digest", run.ProvisioningPlanDigest); err != nil {
		return err
	}
	if !domain.IsCanonicalIdempotencyKey(run.ProvisioningKey) {
		return replayViolation(prefix+".provisioning_key", "persisted provisioning key is invalid", nil)
	}
	return nil
}

func validateTargetOperationHistory(previous, next []TargetOperationRecord) error {
	if len(next) < len(previous) {
		return replayViolation("target.operations", "persisted target operation history was removed", nil)
	}
	nextByID := make(map[string]TargetOperationRecord, len(next))
	for _, operation := range next {
		nextByID[operation.ID] = operation
	}
	for index, before := range previous {
		after, exists := nextByID[before.ID]
		prefix := fmt.Sprintf("target.operations[%d]", index)
		if !exists || before.RunID != after.RunID || before.Generation != after.Generation || before.Kind != after.Kind ||
			before.CommandDisplay != after.CommandDisplay || before.ContentDigest != after.ContentDigest || !before.CreatedAt.Equal(after.CreatedAt) {
			return replayViolation(prefix, "persisted target operation identity changed", nil)
		}
		if err := requireNestedReplayStateAdvance(prefix, before.Revision, after.Revision, before.UpdatedAt, after.UpdatedAt, before.State, after.State, domain.RequireTargetOperationTransition); err != nil {
			return err
		}
	}
	return nil
}

func requireNestedReplayStateAdvance[S comparable](prefix string, previousRevision, nextRevision uint64, previousUpdated, nextUpdated time.Time, previousState, nextState S, transition func(S, S) error) error {
	if nextRevision == previousRevision {
		if previousState != nextState || !previousUpdated.Equal(nextUpdated) {
			return replayViolation(prefix+".revision", "persisted nested state changed without a revision", nil)
		}
		return nil
	}
	return requireReplayStateAdvance(prefix, previousRevision, nextRevision, previousUpdated, nextUpdated, previousState, nextState, transition)
}

func sameIncidentIdentity(previous, next IncidentRecord) bool {
	previous.State, next.State = "", ""
	previous.Revision, next.Revision = 0, 0
	previous.UpdatedAt, next.UpdatedAt = time.Time{}, time.Time{}
	previous.RecoveryActions, next.RecoveryActions = nil, nil
	previous.VisibilityAcknowledgements, next.VisibilityAcknowledgements = nil, nil
	previous.ObservationBundleID, next.ObservationBundleID = "", ""
	previous.Artifacts, next.Artifacts = nil, nil
	return reflect.DeepEqual(previous, next)
}

func requireIncidentEvidenceAdvance(previous, next IncidentRecord) error {
	if previous.ObservationBundleID == next.ObservationBundleID && reflect.DeepEqual(previous.Artifacts, next.Artifacts) {
		return nil
	}
	if previous.State != domain.IncidentOpen || next.State != domain.IncidentEvidenceSealed || next.ObservationBundleID == "" {
		return replayViolation("incident.evidence", "persisted incident evidence changed outside the open-to-sealed transition", nil)
	}
	if previous.ObservationBundleID != "" && previous.ObservationBundleID != next.ObservationBundleID {
		return replayViolation("incident.observation_bundle_id", "persisted incident evidence binding changed", nil)
	}
	for _, artifact := range previous.Artifacts {
		found := false
		for _, candidate := range next.Artifacts {
			if candidate == artifact {
				found = true
				break
			}
		}
		if !found {
			return replayViolation("incident.artifacts", "persisted incident evidence was removed while sealing", nil)
		}
	}
	if len(next.Artifacts) < len(previous.Artifacts) || (len(next.Artifacts) == len(previous.Artifacts) && previous.ObservationBundleID == next.ObservationBundleID) {
		return replayViolation("incident.artifacts", "persisted incident seal did not add a bundle evidence binding", nil)
	}
	return nil
}
