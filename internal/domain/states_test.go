package domain

import "testing"

func TestEveryLegalAndIllegalTransition(t *testing.T) {
	t.Run("research session", func(t *testing.T) {
		states := []ResearchSessionState{ResearchSessionRequested, ResearchSessionAdmitted, ResearchSessionLeased, ResearchSessionReleasing, ResearchSessionReleased, ResearchSessionQuarantined, ResearchSessionLost}
		legal := pairs(
			pair(ResearchSessionRequested, ResearchSessionAdmitted), pair(ResearchSessionAdmitted, ResearchSessionLeased),
			pair(ResearchSessionLeased, ResearchSessionReleasing), pair(ResearchSessionReleasing, ResearchSessionReleased),
		)
		addToAll(legal, states[:4], ResearchSessionQuarantined, ResearchSessionLost)
		assertTransitionMatrix(t, states, legal, RequireResearchSessionTransition)
	})
	t.Run("lease", func(t *testing.T) {
		states := []LeaseState{LeaseActive, LeaseReleasing, LeaseReleased, LeaseExpired, LeaseRevoked, LeaseQuarantined, LeaseLost}
		legal := pairs(pair(LeaseActive, LeaseReleasing), pair(LeaseReleasing, LeaseReleased), pair(LeaseActive, LeaseExpired), pair(LeaseActive, LeaseRevoked), pair(LeaseActive, LeaseQuarantined), pair(LeaseActive, LeaseLost), pair(LeaseReleasing, LeaseRevoked), pair(LeaseReleasing, LeaseQuarantined), pair(LeaseReleasing, LeaseLost))
		assertTransitionMatrix(t, states, legal, RequireLeaseTransition)
	})
	t.Run("agent generation", func(t *testing.T) {
		active := []AgentGenerationState{AgentGenerationProvisioning, AgentGenerationBooting, AgentGenerationReady, AgentGenerationRunning, AgentGenerationQuiescing}
		states := append(cloneSlice(active), AgentGenerationSealed, AgentGenerationFailed, AgentGenerationQuarantined, AgentGenerationLost)
		legal := pairs(pair(AgentGenerationProvisioning, AgentGenerationBooting), pair(AgentGenerationBooting, AgentGenerationReady), pair(AgentGenerationReady, AgentGenerationRunning), pair(AgentGenerationReady, AgentGenerationQuiescing), pair(AgentGenerationRunning, AgentGenerationQuiescing), pair(AgentGenerationQuiescing, AgentGenerationSealed))
		addToAll(legal, active, AgentGenerationFailed, AgentGenerationQuarantined, AgentGenerationLost)
		assertTransitionMatrix(t, states, legal, RequireAgentGenerationTransition)
	})
	t.Run("target generation", func(t *testing.T) {
		active := []TargetGenerationState{TargetGenerationProvisioning, TargetGenerationInstrumenting, TargetGenerationReady, TargetGenerationResettable}
		states := append(cloneSlice(active), TargetGenerationDestroyed, TargetGenerationFailed, TargetGenerationQuarantined, TargetGenerationLost)
		legal := pairs(pair(TargetGenerationProvisioning, TargetGenerationInstrumenting), pair(TargetGenerationInstrumenting, TargetGenerationReady), pair(TargetGenerationReady, TargetGenerationResettable), pair(TargetGenerationResettable, TargetGenerationDestroyed))
		addToAll(legal, active, TargetGenerationFailed, TargetGenerationQuarantined, TargetGenerationLost)
		assertTransitionMatrix(t, states, legal, RequireTargetGenerationTransition)
	})
	t.Run("target run", func(t *testing.T) {
		active := []TargetRunState{TargetRunRequested, TargetRunPreparing, TargetRunObserving, TargetRunRunning, TargetRunFinalizing}
		states := append(cloneSlice(active), TargetRunCompleted, TargetRunFailed, TargetRunQuarantined, TargetRunLost)
		legal := pairs(pair(TargetRunRequested, TargetRunPreparing), pair(TargetRunPreparing, TargetRunObserving), pair(TargetRunObserving, TargetRunRunning), pair(TargetRunRunning, TargetRunFinalizing), pair(TargetRunFinalizing, TargetRunCompleted))
		addToAll(legal, active, TargetRunFailed, TargetRunQuarantined, TargetRunLost)
		assertTransitionMatrix(t, states, legal, RequireTargetRunTransition)
	})
	t.Run("exec", func(t *testing.T) {
		states := []ExecState{ExecRequested, ExecStarting, ExecRunning, ExecCompleted, ExecFailed, ExecCancelled, ExecLost}
		legal := pairs(pair(ExecRequested, ExecStarting), pair(ExecStarting, ExecRunning), pair(ExecRunning, ExecCompleted))
		addToAll(legal, states[:3], ExecFailed, ExecCancelled, ExecLost)
		assertTransitionMatrix(t, states, legal, RequireExecTransition)
	})
	t.Run("target operation", func(t *testing.T) {
		states := []TargetOperationState{TargetOperationRequested, TargetOperationRunning, TargetOperationCompleted, TargetOperationFailed, TargetOperationCancelled, TargetOperationLost}
		legal := pairs(pair(TargetOperationRequested, TargetOperationRunning), pair(TargetOperationRunning, TargetOperationCompleted))
		addToAll(legal, states[:2], TargetOperationFailed, TargetOperationCancelled, TargetOperationLost)
		assertTransitionMatrix(t, states, legal, RequireTargetOperationTransition)
	})
	t.Run("input view", func(t *testing.T) {
		states := []InputViewState{InputViewBuilding, InputViewReady, InputViewRetired, InputViewFailed, InputViewQuarantined, InputViewLost}
		legal := pairs(pair(InputViewBuilding, InputViewReady), pair(InputViewReady, InputViewRetired), pair(InputViewBuilding, InputViewFailed), pair(InputViewReady, InputViewFailed), pair(InputViewBuilding, InputViewQuarantined), pair(InputViewReady, InputViewQuarantined), pair(InputViewBuilding, InputViewLost), pair(InputViewReady, InputViewLost))
		assertTransitionMatrix(t, states, legal, RequireInputViewTransition)
	})
	t.Run("workspace", func(t *testing.T) {
		active := []WorkspaceState{WorkspacePreparing, WorkspaceReady, WorkspaceMounted, WorkspaceQuiescing, WorkspaceSealing, WorkspaceSealed}
		states := append(cloneSlice(active), WorkspaceReleased, WorkspaceFailed, WorkspaceQuarantined, WorkspaceLost)
		legal := pairs(pair(WorkspacePreparing, WorkspaceReady), pair(WorkspaceReady, WorkspaceMounted), pair(WorkspaceMounted, WorkspaceQuiescing), pair(WorkspaceQuiescing, WorkspaceSealing), pair(WorkspaceSealing, WorkspaceSealed), pair(WorkspaceSealed, WorkspaceReleased))
		addToAll(legal, active, WorkspaceFailed, WorkspaceQuarantined, WorkspaceLost)
		assertTransitionMatrix(t, states, legal, RequireWorkspaceTransition)
	})
	t.Run("capture", func(t *testing.T) {
		states := []CaptureState{CaptureRequested, CaptureRunning, CaptureFinalizing, CaptureCompleted, CaptureFailed, CaptureCancelled, CaptureLost}
		legal := pairs(pair(CaptureRequested, CaptureRunning), pair(CaptureRunning, CaptureFinalizing), pair(CaptureFinalizing, CaptureCompleted), pair(CaptureRequested, CaptureCancelled), pair(CaptureRunning, CaptureCancelled), pair(CaptureRequested, CaptureFailed), pair(CaptureRunning, CaptureFailed), pair(CaptureFinalizing, CaptureFailed), pair(CaptureRequested, CaptureLost), pair(CaptureRunning, CaptureLost), pair(CaptureFinalizing, CaptureLost))
		assertTransitionMatrix(t, states, legal, RequireCaptureTransition)
	})
	t.Run("export", func(t *testing.T) {
		states := []ExportState{ExportDeclared, ExportCommitting, ExportCommitted, ExportFailed, ExportCancelled}
		legal := pairs(pair(ExportDeclared, ExportCommitting), pair(ExportCommitting, ExportCommitted), pair(ExportDeclared, ExportCancelled), pair(ExportDeclared, ExportFailed), pair(ExportCommitting, ExportFailed))
		assertTransitionMatrix(t, states, legal, RequireExportTransition)
	})
	t.Run("observation bundle", func(t *testing.T) {
		states := []ObservationBundleState{ObservationBundleBuilding, ObservationBundleSealed, ObservationBundleFailed}
		legal := pairs(pair(ObservationBundleBuilding, ObservationBundleSealed), pair(ObservationBundleBuilding, ObservationBundleFailed))
		assertTransitionMatrix(t, states, legal, RequireObservationBundleTransition)
	})
	t.Run("incident", func(t *testing.T) {
		states := []IncidentState{IncidentOpen, IncidentEvidenceSealed, IncidentRecovering, IncidentResolved}
		legal := pairs(pair(IncidentOpen, IncidentEvidenceSealed), pair(IncidentEvidenceSealed, IncidentRecovering), pair(IncidentEvidenceSealed, IncidentResolved), pair(IncidentRecovering, IncidentResolved))
		assertTransitionMatrix(t, states, legal, RequireIncidentTransition)
	})
}

type statePair[S comparable] struct{ from, to S }

func pair[S comparable](from, to S) statePair[S] { return statePair[S]{from: from, to: to} }
func pairs[S comparable](values ...statePair[S]) map[statePair[S]]struct{} {
	result := make(map[statePair[S]]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
func addToAll[S comparable](legal map[statePair[S]]struct{}, from []S, destinations ...S) {
	for _, source := range from {
		for _, destination := range destinations {
			legal[pair(source, destination)] = struct{}{}
		}
	}
}
func assertTransitionMatrix[S comparable](t *testing.T, states []S, legal map[statePair[S]]struct{}, guard func(S, S) error) {
	t.Helper()
	for _, from := range states {
		for _, to := range states {
			_, expected := legal[pair(from, to)]
			err := guard(from, to)
			if expected && err != nil {
				t.Errorf("expected %v -> %v to be legal: %v", from, to, err)
			}
			if !expected && !IsCode(err, CodeInvalidTransition) {
				t.Errorf("expected %v -> %v to be invalid transition, got %v", from, to, err)
			}
		}
	}
	var zero S
	if err := guard(zero, states[0]); !IsCode(err, CodeInvalidState) {
		t.Errorf("unknown source: got %v", err)
	}
	if err := guard(states[0], zero); !IsCode(err, CodeInvalidState) {
		t.Errorf("unknown destination: got %v", err)
	}
}
