package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

type CreateExecRequest struct {
	Meta       MutationMeta    `json:"meta"`
	LeaseID    string          `json:"lease_id"`
	Kind       domain.ExecKind `json:"kind"`
	Executable string          `json:"executable"`
	// Argv contains only the arguments after argv[0]. Executable supplies
	// both the program to launch and argv[0].
	Argv             []string `json:"argv,omitempty"`
	WorkingDirectory string   `json:"working_directory,omitempty"`
}

type execResult struct {
	ExecID string `json:"exec_id"`
}

func (c *Core) CreateExec(ctx context.Context, request CreateExecRequest) (ExecRecord, error) {
	const operation = "exec.create"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return ExecRecord{}, err
	}
	if _, err := domain.ParseLeaseID(request.LeaseID); err != nil {
		return ExecRecord{}, err
	}
	if !request.Kind.IsValid() {
		return ExecRecord{}, invalidArgument(operation, "kind", "is not recognized", nil)
	}
	if request.WorkingDirectory == "" {
		request.WorkingDirectory = "."
	}
	if err := validateExecCommand(request.Executable, request.Argv, request.WorkingDirectory); err != nil {
		return ExecRecord{}, err
	}
	request.Argv = append([]string(nil), request.Argv...)
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return ExecRecord{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ExecRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, "create_exec", request.Meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		lease, ok := c.leases[request.LeaseID]
		if !ok {
			return nil, ErrNotFound
		}
		if err := requireLeaseActive(lease, c.clock()); err != nil {
			return nil, err
		}
		agent, err := c.requireAgentGenerationAcceptingWork(operation, lease.AgentWorkspaceID, lease.AgentGeneration)
		if err != nil {
			return nil, err
		}
		execID, err := c.ids.ExecID()
		if err != nil {
			return nil, err
		}
		now := c.clock().UTC()
		leaseID, err := requireStoredID("lease.id", lease.ID, domain.ParseLeaseID)
		if err != nil {
			return nil, err
		}
		agentID, err := requireStoredID("agent_workspace.id", agent.ID, domain.ParseAgentWorkspaceID)
		if err != nil {
			return nil, err
		}
		model, err := domain.NewExec(domain.ExecSpec{ID: execID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: domain.AgentGeneration(agent.CurrentGeneration), Kind: request.Kind, Executable: request.Executable, Argv: request.Argv, WorkingDirectory: request.WorkingDirectory, CreatedAt: now})
		if err != nil {
			return nil, err
		}
		execution := ExecRecord{ID: execID.String(), SessionID: lease.SessionID, LeaseID: lease.ID, AgentWorkspaceID: agent.ID, AgentGeneration: agent.CurrentGeneration, Kind: request.Kind, Executable: request.Executable, Argv: append([]string(nil), request.Argv...), WorkingDirectory: request.WorkingDirectory, State: model.State(), Revision: uint64(model.Revision()), CreatedAt: now, UpdatedAt: now}
		if err := appendControl(ctx, tx, "exec", execution.ID, "exec.requested", execution.Revision, execution); err != nil {
			return nil, err
		}
		return json.Marshal(execResult{ExecID: execution.ID})
	})
	if err != nil {
		return ExecRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return ExecRecord{}, err
	}
	var result execResult
	if err := json.Unmarshal(response, &result); err != nil {
		return ExecRecord{}, err
	}
	return cloneExec(c.execs[result.ExecID]), nil
}

func validateExecCommand(executable string, argv []string, workingDirectory string) error {
	const operation = "exec.validate_command"
	if strings.TrimSpace(executable) == "" || strings.IndexByte(executable, 0) >= 0 {
		return invalidArgument(operation, "executable", "is required and must not contain NUL", nil)
	}
	if len(executable) > 4096 || len(workingDirectory) > 4096 || len(argv) > 4096 {
		return resourceExhausted(operation, "command", "exceeds structural limits", nil)
	}
	if strings.IndexByte(workingDirectory, 0) >= 0 {
		return invalidArgument(operation, "working_directory", "must not contain NUL", nil)
	}
	for index, argument := range argv {
		if len(argument) > 1<<20 || strings.IndexByte(argument, 0) >= 0 {
			return invalidArgument(operation, fmt.Sprintf("argv[%d]", index), "exceeds limits or contains NUL", nil)
		}
	}
	return nil
}

type TransitionExecRequest struct {
	Meta             MutationMeta     `json:"meta"`
	ExecID           string           `json:"exec_id"`
	ExpectedRevision uint64           `json:"expected_revision"`
	State            domain.ExecState `json:"state"`
}

func (c *Core) TransitionExec(ctx context.Context, request TransitionExecRequest) (ExecRecord, error) {
	const operation = "exec.transition"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return ExecRecord{}, err
	}
	if _, err := domain.ParseExecID(request.ExecID); err != nil {
		return ExecRecord{}, err
	}
	if request.State.Terminal() {
		return ExecRecord{}, failedPrecondition(operation, "state", "terminal transitions must use FinalizeExec", nil)
	}
	return c.mutateExec(ctx, "transition_exec", request.Meta, request, request.ExecID, true, func(ctx context.Context, tx *store.Tx, execution *ExecRecord) (string, error) {
		if execution.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		if err := domain.RequireExecTransition(execution.State, request.State); err != nil {
			return "", err
		}
		now := c.clock().UTC()
		if request.State == domain.ExecRunning {
			agent, ok := detachedRecord(c.agents, execution.AgentWorkspaceID, cloneAgent)
			if !ok {
				return "", ErrNotFound
			}
			generation, err := findAgentGeneration(&agent, execution.AgentGeneration)
			if err != nil {
				return "", err
			}
			if generation.State == domain.AgentGenerationReady {
				if err := domain.RequireAgentGenerationTransition(generation.State, domain.AgentGenerationRunning); err != nil {
					return "", err
				}
				generation.State, generation.Revision, generation.UpdatedAt = domain.AgentGenerationRunning, generation.Revision+1, now
				agent.Revision++
				agent.UpdatedAt = now
				if err := appendControl(ctx, tx, "agent_workspace", agent.ID, "agent_generation.exec_started", agent.Revision, agent); err != nil {
					return "", err
				}
			} else if generation.State != domain.AgentGenerationRunning {
				return "", failedPrecondition(operation, "agent_generation", fmt.Sprintf("cannot run an exec in %s", generation.State), nil)
			}
		}
		execution.State, execution.Revision, execution.UpdatedAt = request.State, execution.Revision+1, now
		return "exec.transitioned", nil
	})
}

type FinalizeExecRequest struct {
	Meta             MutationMeta     `json:"meta"`
	ExecID           string           `json:"exec_id"`
	ExpectedRevision uint64           `json:"expected_revision"`
	State            domain.ExecState `json:"state"`
	ExitCode         *int             `json:"exit_code,omitempty"`
	Signal           string           `json:"signal,omitempty"`
	IncidentIDs      []string         `json:"incident_ids,omitempty"`
	CleanupConfirmed bool             `json:"cleanup_confirmed"`
	Error            string           `json:"error,omitempty"`
}

func (c *Core) FinalizeExec(ctx context.Context, request FinalizeExecRequest) (ExecRecord, error) {
	const operation = "exec.finalize"
	if err := request.Meta.Validate(ctx, c.clock()); err != nil {
		return ExecRecord{}, err
	}
	if _, err := domain.ParseExecID(request.ExecID); err != nil {
		return ExecRecord{}, err
	}
	if !request.State.Terminal() {
		return ExecRecord{}, invalidArgument(operation, "state", "must be terminal", nil)
	}
	if request.State == domain.ExecCompleted && (request.ExitCode == nil || *request.ExitCode != 0 || !request.CleanupConfirmed) {
		return ExecRecord{}, invalidArgument(operation, "terminal", "completed exec requires exit code zero and confirmed cleanup", nil)
	}
	if len(request.Signal) > 256 || len(request.Error) > 4096 {
		return ExecRecord{}, resourceExhausted(operation, "terminal", "details exceed limits", nil)
	}
	for _, incidentID := range request.IncidentIDs {
		if _, err := domain.ParseIncidentID(incidentID); err != nil {
			return ExecRecord{}, err
		}
	}
	incidentIDs, err := normalizedNonBlank(request.IncidentIDs)
	if err != nil {
		return ExecRecord{}, err
	}
	request.IncidentIDs = incidentIDs
	return c.mutateExec(ctx, "finalize_exec", request.Meta, request, request.ExecID, false, func(_ context.Context, _ *store.Tx, execution *ExecRecord) (string, error) {
		if execution.Revision != request.ExpectedRevision {
			return "", store.ErrRevisionConflict
		}
		for _, incidentID := range request.IncidentIDs {
			incident, ok := c.incidents[incidentID]
			if !ok {
				return "", ErrNotFound
			}
			if incident.SessionID != execution.SessionID || incident.ExecID != execution.ID || incident.AgentWorkspaceID != execution.AgentWorkspaceID || incident.AgentGeneration != execution.AgentGeneration {
				return "", ErrScope
			}
		}
		if err := domain.RequireExecTransition(execution.State, request.State); err != nil {
			return "", err
		}
		execution.State = request.State
		execution.Revision++
		execution.UpdatedAt = c.clock().UTC()
		if request.ExitCode != nil {
			exitCode := *request.ExitCode
			execution.ExitCode = &exitCode
		}
		execution.Signal = request.Signal
		execution.CleanupConfirmed = request.CleanupConfirmed
		execution.Error = strings.TrimSpace(request.Error)
		execution.IncidentIDs, err = mergedNonBlank(execution.IncidentIDs, request.IncidentIDs...)
		if err != nil {
			return "", err
		}
		return "exec.finalized", nil
	})
}

func (c *Core) mutateExec(ctx context.Context, namespace string, meta MutationMeta, request any, execID string, requireActiveLease bool, mutate func(context.Context, *store.Tx, *ExecRecord) (string, error)) (ExecRecord, error) {
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return ExecRecord{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncLocked(ctx); err != nil {
		return ExecRecord{}, err
	}
	response, _, err := c.store.RunIdempotent(ctx, namespace, meta.IdempotencyKey, requestBytes, func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		execution, ok := detachedRecord(c.execs, execID, cloneExec)
		if !ok {
			return nil, ErrNotFound
		}
		if requireActiveLease {
			lease, ok := c.leases[execution.LeaseID]
			if !ok {
				return nil, ErrNotFound
			}
			if err := requireLeaseActive(lease, c.clock()); err != nil {
				return nil, err
			}
		}
		event, err := mutate(ctx, tx, &execution)
		if err != nil {
			return nil, err
		}
		if err := appendControl(ctx, tx, "exec", execution.ID, event, execution.Revision, execution); err != nil {
			return nil, err
		}
		return json.Marshal(execution)
	})
	if err != nil {
		return ExecRecord{}, err
	}
	if err := c.syncLocked(ctx); err != nil {
		return ExecRecord{}, err
	}
	var execution ExecRecord
	if err := json.Unmarshal(response, &execution); err != nil {
		return ExecRecord{}, err
	}
	return cloneExec(execution), nil
}
