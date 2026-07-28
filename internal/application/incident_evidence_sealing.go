package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

// SealRunIncidentsRequest binds every listed run-scoped incident to the exact
// immutable observation-bundle artifact. The operation is a single durable,
// idempotent transaction so a crash cannot leave only part of the incident set
// sealed.
type SealRunIncidentsRequest struct {
	Meta           MutationMeta           `json:"meta"`
	TargetID       string                 `json:"target_id"`
	RunID          string                 `json:"run_id"`
	BundleID       string                 `json:"bundle_id"`
	BundleArtifact IncidentArtifactRecord `json:"bundle_artifact"`
	IncidentIDs    []string               `json:"incident_ids"`
}

type sealRunIncidentsResult struct {
	IncidentIDs []string `json:"incident_ids"`
}

func (c *Core) SealRunIncidents(ctx context.Context, request SealRunIncidentsRequest) ([]IncidentRecord, error) {
	const operation = "incident.seal_run_evidence"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return nil, err
	}
	if _, err := domain.ParseTargetID(request.TargetID); err != nil {
		return nil, err
	}
	if _, err := domain.ParseTargetRunID(request.RunID); err != nil {
		return nil, err
	}
	if _, err := domain.ParseObservationBundleID(request.BundleID); err != nil {
		return nil, err
	}
	artifacts, err := incidentArtifactModels([]IncidentArtifactRecord{request.BundleArtifact})
	if err != nil {
		return nil, fmt.Errorf("%s: bundle artifact: %w", operation, err)
	}
	if artifacts[0].Spec().Role != "observation-bundle" {
		return nil, invalidArgument(operation, "bundle_artifact.role", "must identify an observation-bundle artifact", nil)
	}
	request.IncidentIDs, err = normalizedNonBlank(request.IncidentIDs)
	if err != nil {
		return nil, err
	}
	if len(request.IncidentIDs) == 0 {
		return nil, invalidArgument(operation, "incident_ids", "must not be empty", nil)
	}
	for _, incidentID := range request.IncidentIDs {
		if _, err := domain.ParseIncidentID(incidentID); err != nil {
			return nil, err
		}
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return nil, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "seal_run_incidents", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		target, found := c.targets[request.TargetID]
		if !found {
			return nil, ErrNotFound
		}
		run, err := findRun(&target, request.RunID)
		if err != nil {
			return nil, err
		}
		if !run.State.Terminal() || run.BundleID != request.BundleID || run.BundleArtifact != request.BundleArtifact.Reference || run.BundleDigest != request.BundleArtifact.Digest {
			return nil, failedPrecondition(operation, "target_run", "terminal run does not bind the requested bundle artifact", nil)
		}
		now := c.clock().UTC()
		for _, incidentID := range request.IncidentIDs {
			incident, found := detachedRecord(c.incidents, incidentID, cloneIncident)
			if !found {
				return nil, ErrNotFound
			}
			if incident.SessionID != target.SessionID || incident.LeaseID != target.LeaseID || incident.TargetID != target.ID || incident.TargetRunID != run.ID || incident.TargetGeneration != run.Generation {
				return nil, ErrScope
			}
			if incident.State == domain.IncidentEvidenceSealed {
				if !incidentHasBundleEvidence(incident, request.BundleID, request.BundleArtifact) {
					return nil, domain.NewError(domain.CodeIntegrityViolation, operation, "incident", "sealed incident differs from the terminal bundle", nil)
				}
				continue
			}
			if incident.State != domain.IncidentOpen {
				return nil, failedPrecondition(operation, "incident", "only an open incident can receive run evidence", nil)
			}
			if incident.ObservationBundleID != "" && incident.ObservationBundleID != request.BundleID {
				return nil, domain.NewError(domain.CodeIntegrityViolation, operation, "observation_bundle_id", "open incident already names different evidence", nil)
			}
			incident.ObservationBundleID = request.BundleID
			incident.Artifacts, err = appendExactIncidentArtifact(incident.Artifacts, request.BundleArtifact)
			if err != nil {
				return nil, err
			}
			if err := domain.RequireIncidentTransition(incident.State, domain.IncidentEvidenceSealed); err != nil {
				return nil, err
			}
			incident.State, incident.Revision, incident.UpdatedAt = domain.IncidentEvidenceSealed, incident.Revision+1, now
			if err := appendControl(ctx, tx, "incident", incident.ID, "incident.evidence_sealed", incident.Revision, incident); err != nil {
				return nil, err
			}
		}
		return json.Marshal(sealRunIncidentsResult{IncidentIDs: request.IncidentIDs})
	})
	if err != nil {
		return nil, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return nil, err
	}
	var result sealRunIncidentsResult
	if err := json.Unmarshal(response, &result); err != nil {
		return nil, err
	}
	incidents := make([]IncidentRecord, len(result.IncidentIDs))
	for index, incidentID := range result.IncidentIDs {
		incident, found := c.incidents[incidentID]
		if !found {
			return nil, domain.NewError(domain.CodeIntegrityViolation, operation, "incident", "sealed incident is missing after commit", nil)
		}
		incidents[index] = cloneIncident(incident)
	}
	return incidents, nil
}

func appendExactIncidentArtifact(existing []IncidentArtifactRecord, desired IncidentArtifactRecord) ([]IncidentArtifactRecord, error) {
	result := append([]IncidentArtifactRecord(nil), existing...)
	for _, artifact := range result {
		if artifact.Reference != desired.Reference {
			continue
		}
		if artifact != desired {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "incident.seal_run_evidence", "bundle_artifact", "artifact reference already has different immutable metadata", nil)
		}
		return result, nil
	}
	return append(result, desired), nil
}

func incidentHasBundleEvidence(incident IncidentRecord, bundleID string, artifact IncidentArtifactRecord) bool {
	if incident.ObservationBundleID != bundleID {
		return false
	}
	for _, candidate := range incident.Artifacts {
		if candidate == artifact {
			return true
		}
	}
	return false
}
