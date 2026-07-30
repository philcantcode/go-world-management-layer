package world

import (
	"context"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
)

// AcquireResearchSession acquires a lease and agent workspace generation.
func (m *Manager) AcquireResearchSession(ctx context.Context, request *worldv1.AcquireResearchSessionRequest) (*worldv1.AcquireResearchSessionResponse, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.AcquireResearchSessionRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.AcquireResearchSessionResponse, error) {
		return m.facade.AcquireResearchSession(bound, request)
	})
}

// GetResearchSession reads the current session view.
func (m *Manager) GetResearchSession(ctx context.Context, request *worldv1.GetResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.ResearchSessionView, error) {
		return m.facade.GetResearchSession(bound, request)
	})
}

// WaitResearchSession blocks until the session reaches the desired state.
func (m *Manager) WaitResearchSession(ctx context.Context, request *worldv1.WaitResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.ResearchSessionView, error) {
		return m.facade.WaitResearchSession(bound, request)
	})
}

// RenewLease extends a lease TTL.
func (m *Manager) RenewLease(ctx context.Context, request *worldv1.RenewLeaseRequest) (*worldv1.Lease, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.RenewLeaseRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Lease, error) {
		return m.facade.RenewLease(bound, request)
	})
}

// ReleaseResearchSession drains and releases a research session lease.
func (m *Manager) ReleaseResearchSession(ctx context.Context, request *worldv1.ReleaseResearchSessionRequest) (*worldv1.ReleaseOutcome, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.ReleaseResearchSessionRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.ReleaseOutcome, error) {
		return m.facade.ReleaseResearchSession(bound, request)
	})
}

// CreateTarget creates a target generation under a lease.
func (m *Manager) CreateTarget(ctx context.Context, request *worldv1.CreateTargetRequest) (*worldv1.Target, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.CreateTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return m.facade.CreateTarget(bound, request)
	})
}

// GetTarget reads a target.
func (m *Manager) GetTarget(ctx context.Context, request *worldv1.GetTargetRequest) (*worldv1.Target, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.Target, error) {
		return m.facade.GetTarget(bound, request)
	})
}

// StartTargetRun starts a mutable target run.
func (m *Manager) StartTargetRun(ctx context.Context, request *worldv1.StartTargetRunRequest) (*worldv1.TargetRun, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.StartTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetRun, error) {
		return m.facade.StartTargetRun(bound, request)
	})
}

// WaitTargetRun blocks until a run reaches the desired state.
func (m *Manager) WaitTargetRun(ctx context.Context, request *worldv1.WaitTargetRunRequest) (*worldv1.TargetRun, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.TargetRun, error) {
		return m.facade.WaitTargetRun(bound, request)
	})
}

// StopTargetRun stops a run and returns the sealed observation bundle when available.
func (m *Manager) StopTargetRun(ctx context.Context, request *worldv1.StopTargetRunRequest) (*worldv1.ObservationBundle, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.StopTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.ObservationBundle, error) {
		return m.facade.StopTargetRun(bound, request)
	})
}

// ResetTarget resets a target generation.
func (m *Manager) ResetTarget(ctx context.Context, request *worldv1.ResetTargetRequest) (*worldv1.Target, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.ResetTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return m.facade.ResetTarget(bound, request)
	})
}

// DestroyTarget destroys a target.
func (m *Manager) DestroyTarget(ctx context.Context, request *worldv1.DestroyTargetRequest) (*worldv1.Outcome, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.DestroyTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Outcome, error) {
		return m.facade.DestroyTarget(bound, request)
	})
}

// RequestRecovery requests recovery of a resource generation.
func (m *Manager) RequestRecovery(ctx context.Context, request *worldv1.RequestRecoveryRequest) (*worldv1.RecoveredResource, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.RequestRecoveryRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.RecoveredResource, error) {
		return m.facade.RequestRecovery(bound, request)
	})
}

// QuarantineTarget quarantines a target.
func (m *Manager) QuarantineTarget(ctx context.Context, request *worldv1.QuarantineTargetRequest) (*worldv1.Target, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.QuarantineTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return m.facade.QuarantineTarget(bound, request)
	})
}

// GetIncident reads an incident.
func (m *Manager) GetIncident(ctx context.Context, request *worldv1.GetIncidentRequest) (*worldv1.Incident, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.Incident, error) {
		return m.facade.GetIncident(bound, request)
	})
}

// CreateIncident creates an incident.
func (m *Manager) CreateIncident(ctx context.Context, request *worldv1.CreateIncidentRequest) (*worldv1.Incident, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.CreateIncidentRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Incident, error) {
		return m.facade.CreateIncident(bound, request)
	})
}

// TransitionIncident transitions an incident.
func (m *Manager) TransitionIncident(ctx context.Context, request *worldv1.TransitionIncidentRequest) (*worldv1.Incident, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionIncidentRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Incident, error) {
		return m.facade.TransitionIncident(bound, request)
	})
}

// GetExec reads a logical exec record.
func (m *Manager) GetExec(ctx context.Context, request *worldv1.GetExecRequest) (*worldv1.Exec, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.Exec, error) {
		return m.facade.GetExec(bound, request)
	})
}

// CreateExec creates a logical exec record.
func (m *Manager) CreateExec(ctx context.Context, request *worldv1.CreateExecRequest) (*worldv1.Exec, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.CreateExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return m.facade.CreateExec(bound, request)
	})
}

// TransitionExec transitions a logical exec record.
func (m *Manager) TransitionExec(ctx context.Context, request *worldv1.TransitionExecRequest) (*worldv1.Exec, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return m.facade.TransitionExec(bound, request)
	})
}

// FinalizeExec finalizes a logical exec record.
func (m *Manager) FinalizeExec(ctx context.Context, request *worldv1.FinalizeExecRequest) (*worldv1.Exec, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.FinalizeExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return m.facade.FinalizeExec(bound, request)
	})
}

// GetLiveSnapshot returns the live observation projection.
func (m *Manager) GetLiveSnapshot(ctx context.Context, request *worldv1.GetLiveSnapshotRequest) (*worldv1.LiveSnapshot, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.LiveSnapshot, error) {
		return m.facade.GetLiveSnapshot(bound, request)
	})
}

// GetObservationBundle reads a sealed observation bundle.
func (m *Manager) GetObservationBundle(ctx context.Context, request *worldv1.GetObservationBundleRequest) (*worldv1.ObservationBundle, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.ObservationBundle, error) {
		return m.facade.GetObservationBundle(bound, request)
	})
}

// StartCapture starts a capture.
func (m *Manager) StartCapture(ctx context.Context, request *worldv1.StartCaptureRequest) (*worldv1.Capture, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.StartCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return m.facade.StartCapture(bound, request)
	})
}

// RequestCapture requests a named capture profile.
func (m *Manager) RequestCapture(ctx context.Context, request *worldv1.RequestCaptureRequest) (*worldv1.Capture, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.RequestCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return m.facade.RequestCapture(bound, request)
	})
}

// StopCapture stops a capture.
func (m *Manager) StopCapture(ctx context.Context, request *worldv1.StopCaptureRequest) (*worldv1.Capture, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.StopCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return m.facade.StopCapture(bound, request)
	})
}

// DeclareExport declares an export.
func (m *Manager) DeclareExport(ctx context.Context, request *worldv1.DeclareExportRequest) (*worldv1.Export, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.DeclareExportRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Export, error) {
		return m.facade.DeclareExport(bound, request)
	})
}

// PreviewChangeSet previews an export change set.
func (m *Manager) PreviewChangeSet(ctx context.Context, request *worldv1.PreviewChangeSetRequest) (*worldv1.ChangeSet, error) {
	return invokeUnary(ctx, m, nil, func(bound context.Context) (*worldv1.ChangeSet, error) {
		return m.facade.PreviewChangeSet(bound, request)
	})
}

// CommitExport commits an export.
func (m *Manager) CommitExport(ctx context.Context, request *worldv1.CommitExportRequest) (*worldv1.Export, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.CommitExportRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Export, error) {
		return m.facade.CommitExport(bound, request)
	})
}

// TransitionAgentGeneration transitions an agent generation (RoleInternal for node-only reports).
func (m *Manager) TransitionAgentGeneration(ctx context.Context, request *worldv1.TransitionAgentGenerationRequest) (*worldv1.AgentWorkspace, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionAgentGenerationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.AgentWorkspace, error) {
		return m.facade.TransitionAgentGeneration(bound, request)
	})
}

// TransitionTargetGeneration transitions a target generation.
func (m *Manager) TransitionTargetGeneration(ctx context.Context, request *worldv1.TransitionTargetGenerationRequest) (*worldv1.Target, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionTargetGenerationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return m.facade.TransitionTargetGeneration(bound, request)
	})
}

// TransitionTargetRun transitions a target run.
func (m *Manager) TransitionTargetRun(ctx context.Context, request *worldv1.TransitionTargetRunRequest) (*worldv1.TargetRun, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetRun, error) {
		return m.facade.TransitionTargetRun(bound, request)
	})
}

// CreateTargetOperation creates a target operation.
func (m *Manager) CreateTargetOperation(ctx context.Context, request *worldv1.CreateTargetOperationRequest) (*worldv1.TargetOperation, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.CreateTargetOperationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetOperation, error) {
		return m.facade.CreateTargetOperation(bound, request)
	})
}

// TransitionTargetOperation transitions a target operation.
func (m *Manager) TransitionTargetOperation(ctx context.Context, request *worldv1.TransitionTargetOperationRequest) (*worldv1.TargetOperation, error) {
	return invokeUnary(ctx, m, mutationOf(request, func(v *worldv1.TransitionTargetOperationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetOperation, error) {
		return m.facade.TransitionTargetOperation(bound, request)
	})
}
