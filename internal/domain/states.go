package domain

type transition[S comparable] struct{ from, to S }
type transitionTable[S comparable] map[S]map[S]struct{}

func newTransitionTable[S comparable](transitions ...transition[S]) transitionTable[S] {
	table := make(transitionTable[S])
	for _, item := range transitions {
		if table[item.from] == nil {
			table[item.from] = make(map[S]struct{})
		}
		table[item.from][item.to] = struct{}{}
	}
	return table
}

// guardTransition is the sole transition decision point for every domain
// state machine. The per-resource tables below remain declarative.
func guardTransition[S comparable](resource string, valid func(S) bool, table transitionTable[S], from, to S) error {
	if !valid(from) {
		return NewError(CodeInvalidState, resource+".transition", "from", "is not a recognized state", nil)
	}
	if !valid(to) {
		return NewError(CodeInvalidState, resource+".transition", "to", "is not a recognized state", nil)
	}
	if _, allowed := table[from][to]; !allowed {
		return NewDetailedError(CodeInvalidTransition, resource+".transition", "state", "transition is not allowed", map[string]string{
			"from": stateString(from), "to": stateString(to),
		}, nil)
	}
	return nil
}

func stateString[S comparable](state S) string {
	if text, ok := any(state).(interface{ String() string }); ok {
		return text.String()
	}
	return "unknown"
}

type ResearchSessionState string

const (
	ResearchSessionRequested   ResearchSessionState = "requested"
	ResearchSessionAdmitted    ResearchSessionState = "admitted"
	ResearchSessionLeased      ResearchSessionState = "leased"
	ResearchSessionReleasing   ResearchSessionState = "releasing"
	ResearchSessionReleased    ResearchSessionState = "released"
	ResearchSessionQuarantined ResearchSessionState = "quarantined"
	ResearchSessionLost        ResearchSessionState = "lost"
)

func (s ResearchSessionState) String() string { return string(s) }
func (s ResearchSessionState) IsValid() bool {
	return sessionStates[s]
}
func (s ResearchSessionState) Terminal() bool {
	return s == ResearchSessionReleased || s == ResearchSessionQuarantined || s == ResearchSessionLost
}

var sessionStates = map[ResearchSessionState]bool{ResearchSessionRequested: true, ResearchSessionAdmitted: true, ResearchSessionLeased: true, ResearchSessionReleasing: true, ResearchSessionReleased: true, ResearchSessionQuarantined: true, ResearchSessionLost: true}
var sessionTransitions = newTransitionTable(
	transition[ResearchSessionState]{ResearchSessionRequested, ResearchSessionAdmitted},
	transition[ResearchSessionState]{ResearchSessionAdmitted, ResearchSessionLeased},
	transition[ResearchSessionState]{ResearchSessionLeased, ResearchSessionReleasing},
	transition[ResearchSessionState]{ResearchSessionReleasing, ResearchSessionReleased},
	transition[ResearchSessionState]{ResearchSessionRequested, ResearchSessionQuarantined}, transition[ResearchSessionState]{ResearchSessionRequested, ResearchSessionLost},
	transition[ResearchSessionState]{ResearchSessionAdmitted, ResearchSessionQuarantined}, transition[ResearchSessionState]{ResearchSessionAdmitted, ResearchSessionLost},
	transition[ResearchSessionState]{ResearchSessionLeased, ResearchSessionQuarantined}, transition[ResearchSessionState]{ResearchSessionLeased, ResearchSessionLost},
	transition[ResearchSessionState]{ResearchSessionReleasing, ResearchSessionQuarantined}, transition[ResearchSessionState]{ResearchSessionReleasing, ResearchSessionLost},
)

func RequireResearchSessionTransition(from, to ResearchSessionState) error {
	return guardTransition("research_session", ResearchSessionState.IsValid, sessionTransitions, from, to)
}
func CanResearchSessionTransition(from, to ResearchSessionState) bool {
	return RequireResearchSessionTransition(from, to) == nil
}

type LeaseState string

const (
	LeaseActive      LeaseState = "active"
	LeaseReleasing   LeaseState = "releasing"
	LeaseReleased    LeaseState = "released"
	LeaseExpired     LeaseState = "expired"
	LeaseRevoked     LeaseState = "revoked"
	LeaseQuarantined LeaseState = "quarantined"
	LeaseLost        LeaseState = "lost"
)

func (s LeaseState) String() string { return string(s) }
func (s LeaseState) IsValid() bool  { return leaseStates[s] }
func (s LeaseState) Terminal() bool {
	return s == LeaseReleased || s == LeaseExpired || s == LeaseRevoked || s == LeaseQuarantined || s == LeaseLost
}

var leaseStates = map[LeaseState]bool{LeaseActive: true, LeaseReleasing: true, LeaseReleased: true, LeaseExpired: true, LeaseRevoked: true, LeaseQuarantined: true, LeaseLost: true}
var leaseTransitions = newTransitionTable(
	transition[LeaseState]{LeaseActive, LeaseReleasing}, transition[LeaseState]{LeaseReleasing, LeaseReleased},
	transition[LeaseState]{LeaseActive, LeaseExpired}, transition[LeaseState]{LeaseActive, LeaseRevoked},
	transition[LeaseState]{LeaseActive, LeaseQuarantined}, transition[LeaseState]{LeaseActive, LeaseLost},
	transition[LeaseState]{LeaseReleasing, LeaseRevoked}, transition[LeaseState]{LeaseReleasing, LeaseQuarantined}, transition[LeaseState]{LeaseReleasing, LeaseLost},
)

func RequireLeaseTransition(from, to LeaseState) error {
	return guardTransition("lease", LeaseState.IsValid, leaseTransitions, from, to)
}
func CanLeaseTransition(from, to LeaseState) bool { return RequireLeaseTransition(from, to) == nil }

type AgentGenerationState string

const (
	AgentGenerationProvisioning AgentGenerationState = "provisioning"
	AgentGenerationBooting      AgentGenerationState = "booting"
	AgentGenerationReady        AgentGenerationState = "ready"
	AgentGenerationRunning      AgentGenerationState = "running"
	AgentGenerationQuiescing    AgentGenerationState = "quiescing"
	AgentGenerationSealed       AgentGenerationState = "sealed"
	AgentGenerationFailed       AgentGenerationState = "failed"
	AgentGenerationQuarantined  AgentGenerationState = "quarantined"
	AgentGenerationLost         AgentGenerationState = "lost"
)

func (s AgentGenerationState) String() string { return string(s) }
func (s AgentGenerationState) IsValid() bool  { return agentGenerationStates[s] }
func (s AgentGenerationState) Terminal() bool {
	return s == AgentGenerationSealed || s == AgentGenerationFailed || s == AgentGenerationQuarantined || s == AgentGenerationLost
}

var agentGenerationStates = map[AgentGenerationState]bool{AgentGenerationProvisioning: true, AgentGenerationBooting: true, AgentGenerationReady: true, AgentGenerationRunning: true, AgentGenerationQuiescing: true, AgentGenerationSealed: true, AgentGenerationFailed: true, AgentGenerationQuarantined: true, AgentGenerationLost: true}
var agentGenerationTransitions = buildFailureAwareAgentTransitions()

func buildFailureAwareAgentTransitions() transitionTable[AgentGenerationState] {
	table := newTransitionTable(
		transition[AgentGenerationState]{AgentGenerationProvisioning, AgentGenerationBooting},
		transition[AgentGenerationState]{AgentGenerationBooting, AgentGenerationReady},
		transition[AgentGenerationState]{AgentGenerationReady, AgentGenerationRunning},
		transition[AgentGenerationState]{AgentGenerationReady, AgentGenerationQuiescing},
		transition[AgentGenerationState]{AgentGenerationRunning, AgentGenerationQuiescing},
		transition[AgentGenerationState]{AgentGenerationQuiescing, AgentGenerationSealed},
	)
	for _, from := range []AgentGenerationState{AgentGenerationProvisioning, AgentGenerationBooting, AgentGenerationReady, AgentGenerationRunning, AgentGenerationQuiescing} {
		for _, to := range []AgentGenerationState{AgentGenerationFailed, AgentGenerationQuarantined, AgentGenerationLost} {
			table[from][to] = struct{}{}
		}
	}
	return table
}
func RequireAgentGenerationTransition(from, to AgentGenerationState) error {
	return guardTransition("agent_generation", AgentGenerationState.IsValid, agentGenerationTransitions, from, to)
}
func CanAgentGenerationTransition(from, to AgentGenerationState) bool {
	return RequireAgentGenerationTransition(from, to) == nil
}

type TargetGenerationState string

const (
	TargetGenerationProvisioning  TargetGenerationState = "provisioning"
	TargetGenerationInstrumenting TargetGenerationState = "instrumenting"
	TargetGenerationReady         TargetGenerationState = "ready"
	TargetGenerationResettable    TargetGenerationState = "resettable"
	TargetGenerationDestroyed     TargetGenerationState = "destroyed"
	TargetGenerationFailed        TargetGenerationState = "failed"
	TargetGenerationQuarantined   TargetGenerationState = "quarantined"
	TargetGenerationLost          TargetGenerationState = "lost"
)

func (s TargetGenerationState) String() string { return string(s) }
func (s TargetGenerationState) IsValid() bool  { return targetGenerationStates[s] }
func (s TargetGenerationState) Terminal() bool {
	return s == TargetGenerationDestroyed || s == TargetGenerationFailed || s == TargetGenerationQuarantined || s == TargetGenerationLost
}

var targetGenerationStates = map[TargetGenerationState]bool{TargetGenerationProvisioning: true, TargetGenerationInstrumenting: true, TargetGenerationReady: true, TargetGenerationResettable: true, TargetGenerationDestroyed: true, TargetGenerationFailed: true, TargetGenerationQuarantined: true, TargetGenerationLost: true}
var targetGenerationTransitions = buildFailureAwareTargetTransitions()

func buildFailureAwareTargetTransitions() transitionTable[TargetGenerationState] {
	table := newTransitionTable(
		transition[TargetGenerationState]{TargetGenerationProvisioning, TargetGenerationInstrumenting},
		transition[TargetGenerationState]{TargetGenerationInstrumenting, TargetGenerationReady},
		transition[TargetGenerationState]{TargetGenerationReady, TargetGenerationResettable},
		transition[TargetGenerationState]{TargetGenerationResettable, TargetGenerationDestroyed},
	)
	for _, from := range []TargetGenerationState{TargetGenerationProvisioning, TargetGenerationInstrumenting, TargetGenerationReady, TargetGenerationResettable} {
		for _, to := range []TargetGenerationState{TargetGenerationFailed, TargetGenerationQuarantined, TargetGenerationLost} {
			table[from][to] = struct{}{}
		}
	}
	return table
}
func RequireTargetGenerationTransition(from, to TargetGenerationState) error {
	return guardTransition("target_generation", TargetGenerationState.IsValid, targetGenerationTransitions, from, to)
}
func CanTargetGenerationTransition(from, to TargetGenerationState) bool {
	return RequireTargetGenerationTransition(from, to) == nil
}

type TargetRunState string

const (
	TargetRunRequested   TargetRunState = "requested"
	TargetRunPreparing   TargetRunState = "preparing"
	TargetRunObserving   TargetRunState = "observing"
	TargetRunRunning     TargetRunState = "running"
	TargetRunFinalizing  TargetRunState = "finalizing"
	TargetRunCompleted   TargetRunState = "completed"
	TargetRunFailed      TargetRunState = "failed"
	TargetRunQuarantined TargetRunState = "quarantined"
	TargetRunLost        TargetRunState = "lost"
)

func (s TargetRunState) String() string { return string(s) }
func (s TargetRunState) IsValid() bool  { return targetRunStates[s] }
func (s TargetRunState) Terminal() bool {
	return s == TargetRunCompleted || s == TargetRunFailed || s == TargetRunQuarantined || s == TargetRunLost
}

var targetRunStates = map[TargetRunState]bool{TargetRunRequested: true, TargetRunPreparing: true, TargetRunObserving: true, TargetRunRunning: true, TargetRunFinalizing: true, TargetRunCompleted: true, TargetRunFailed: true, TargetRunQuarantined: true, TargetRunLost: true}
var targetRunTransitions = buildFailureAwareRunTransitions()

func buildFailureAwareRunTransitions() transitionTable[TargetRunState] {
	table := newTransitionTable(
		transition[TargetRunState]{TargetRunRequested, TargetRunPreparing},
		transition[TargetRunState]{TargetRunPreparing, TargetRunObserving},
		transition[TargetRunState]{TargetRunObserving, TargetRunRunning},
		transition[TargetRunState]{TargetRunRunning, TargetRunFinalizing},
		transition[TargetRunState]{TargetRunFinalizing, TargetRunCompleted},
	)
	for _, from := range []TargetRunState{TargetRunRequested, TargetRunPreparing, TargetRunObserving, TargetRunRunning, TargetRunFinalizing} {
		for _, to := range []TargetRunState{TargetRunFailed, TargetRunQuarantined, TargetRunLost} {
			table[from][to] = struct{}{}
		}
	}
	return table
}
func RequireTargetRunTransition(from, to TargetRunState) error {
	return guardTransition("target_run", TargetRunState.IsValid, targetRunTransitions, from, to)
}
func CanTargetRunTransition(from, to TargetRunState) bool {
	return RequireTargetRunTransition(from, to) == nil
}

type ExecState string

const (
	ExecRequested ExecState = "requested"
	ExecStarting  ExecState = "starting"
	ExecRunning   ExecState = "running"
	ExecCompleted ExecState = "completed"
	ExecFailed    ExecState = "failed"
	ExecCancelled ExecState = "cancelled"
	ExecLost      ExecState = "lost"
)

func (s ExecState) String() string { return string(s) }
func (s ExecState) IsValid() bool  { return execStates[s] }
func (s ExecState) Terminal() bool {
	return s == ExecCompleted || s == ExecFailed || s == ExecCancelled || s == ExecLost
}

var execStates = map[ExecState]bool{ExecRequested: true, ExecStarting: true, ExecRunning: true, ExecCompleted: true, ExecFailed: true, ExecCancelled: true, ExecLost: true}
var execTransitions = newTransitionTable(
	transition[ExecState]{ExecRequested, ExecStarting}, transition[ExecState]{ExecStarting, ExecRunning}, transition[ExecState]{ExecRunning, ExecCompleted},
	transition[ExecState]{ExecRequested, ExecCancelled}, transition[ExecState]{ExecStarting, ExecCancelled}, transition[ExecState]{ExecRunning, ExecCancelled},
	transition[ExecState]{ExecRequested, ExecFailed}, transition[ExecState]{ExecStarting, ExecFailed}, transition[ExecState]{ExecRunning, ExecFailed},
	transition[ExecState]{ExecRequested, ExecLost}, transition[ExecState]{ExecStarting, ExecLost}, transition[ExecState]{ExecRunning, ExecLost},
)

func RequireExecTransition(from, to ExecState) error {
	return guardTransition("exec", ExecState.IsValid, execTransitions, from, to)
}
func CanExecTransition(from, to ExecState) bool { return RequireExecTransition(from, to) == nil }

type TargetOperationState string

const (
	TargetOperationRequested TargetOperationState = "requested"
	TargetOperationRunning   TargetOperationState = "running"
	TargetOperationCompleted TargetOperationState = "completed"
	TargetOperationFailed    TargetOperationState = "failed"
	TargetOperationCancelled TargetOperationState = "cancelled"
	TargetOperationLost      TargetOperationState = "lost"
)

func (s TargetOperationState) String() string { return string(s) }
func (s TargetOperationState) IsValid() bool  { return targetOperationStates[s] }
func (s TargetOperationState) Terminal() bool {
	return s == TargetOperationCompleted || s == TargetOperationFailed || s == TargetOperationCancelled || s == TargetOperationLost
}

var targetOperationStates = map[TargetOperationState]bool{TargetOperationRequested: true, TargetOperationRunning: true, TargetOperationCompleted: true, TargetOperationFailed: true, TargetOperationCancelled: true, TargetOperationLost: true}
var targetOperationTransitions = newTransitionTable(
	transition[TargetOperationState]{TargetOperationRequested, TargetOperationRunning}, transition[TargetOperationState]{TargetOperationRunning, TargetOperationCompleted},
	transition[TargetOperationState]{TargetOperationRequested, TargetOperationFailed}, transition[TargetOperationState]{TargetOperationRunning, TargetOperationFailed},
	transition[TargetOperationState]{TargetOperationRequested, TargetOperationCancelled}, transition[TargetOperationState]{TargetOperationRunning, TargetOperationCancelled},
	transition[TargetOperationState]{TargetOperationRequested, TargetOperationLost}, transition[TargetOperationState]{TargetOperationRunning, TargetOperationLost},
)

func RequireTargetOperationTransition(from, to TargetOperationState) error {
	return guardTransition("target_operation", TargetOperationState.IsValid, targetOperationTransitions, from, to)
}
func CanTargetOperationTransition(from, to TargetOperationState) bool {
	return RequireTargetOperationTransition(from, to) == nil
}

type InputViewState string

const (
	InputViewBuilding    InputViewState = "building"
	InputViewReady       InputViewState = "ready"
	InputViewRetired     InputViewState = "retired"
	InputViewFailed      InputViewState = "failed"
	InputViewQuarantined InputViewState = "quarantined"
	InputViewLost        InputViewState = "lost"
)

func (s InputViewState) String() string { return string(s) }
func (s InputViewState) IsValid() bool  { return inputViewStates[s] }
func (s InputViewState) Terminal() bool {
	return s == InputViewRetired || s == InputViewFailed || s == InputViewQuarantined || s == InputViewLost
}

var inputViewStates = map[InputViewState]bool{InputViewBuilding: true, InputViewReady: true, InputViewRetired: true, InputViewFailed: true, InputViewQuarantined: true, InputViewLost: true}
var inputViewTransitions = newTransitionTable(
	transition[InputViewState]{InputViewBuilding, InputViewReady}, transition[InputViewState]{InputViewReady, InputViewRetired},
	transition[InputViewState]{InputViewBuilding, InputViewFailed}, transition[InputViewState]{InputViewReady, InputViewFailed},
	transition[InputViewState]{InputViewBuilding, InputViewQuarantined}, transition[InputViewState]{InputViewReady, InputViewQuarantined},
	transition[InputViewState]{InputViewBuilding, InputViewLost}, transition[InputViewState]{InputViewReady, InputViewLost},
)

func RequireInputViewTransition(from, to InputViewState) error {
	return guardTransition("input_view", InputViewState.IsValid, inputViewTransitions, from, to)
}
func CanInputViewTransition(from, to InputViewState) bool {
	return RequireInputViewTransition(from, to) == nil
}

type WorkspaceState string

const (
	WorkspacePreparing   WorkspaceState = "preparing"
	WorkspaceReady       WorkspaceState = "ready"
	WorkspaceMounted     WorkspaceState = "mounted"
	WorkspaceQuiescing   WorkspaceState = "quiescing"
	WorkspaceSealing     WorkspaceState = "sealing"
	WorkspaceSealed      WorkspaceState = "sealed"
	WorkspaceReleased    WorkspaceState = "released"
	WorkspaceFailed      WorkspaceState = "failed"
	WorkspaceQuarantined WorkspaceState = "quarantined"
	WorkspaceLost        WorkspaceState = "lost"
)

func (s WorkspaceState) String() string { return string(s) }
func (s WorkspaceState) IsValid() bool  { return workspaceStates[s] }
func (s WorkspaceState) Terminal() bool {
	return s == WorkspaceReleased || s == WorkspaceFailed || s == WorkspaceQuarantined || s == WorkspaceLost
}

var workspaceStates = map[WorkspaceState]bool{WorkspacePreparing: true, WorkspaceReady: true, WorkspaceMounted: true, WorkspaceQuiescing: true, WorkspaceSealing: true, WorkspaceSealed: true, WorkspaceReleased: true, WorkspaceFailed: true, WorkspaceQuarantined: true, WorkspaceLost: true}
var workspaceTransitions = buildWorkspaceTransitions()

func buildWorkspaceTransitions() transitionTable[WorkspaceState] {
	table := newTransitionTable(
		transition[WorkspaceState]{WorkspacePreparing, WorkspaceReady}, transition[WorkspaceState]{WorkspaceReady, WorkspaceMounted},
		transition[WorkspaceState]{WorkspaceMounted, WorkspaceQuiescing}, transition[WorkspaceState]{WorkspaceQuiescing, WorkspaceSealing},
		transition[WorkspaceState]{WorkspaceSealing, WorkspaceSealed}, transition[WorkspaceState]{WorkspaceSealed, WorkspaceReleased},
	)
	for _, from := range []WorkspaceState{WorkspacePreparing, WorkspaceReady, WorkspaceMounted, WorkspaceQuiescing, WorkspaceSealing, WorkspaceSealed} {
		for _, to := range []WorkspaceState{WorkspaceFailed, WorkspaceQuarantined, WorkspaceLost} {
			table[from][to] = struct{}{}
		}
	}
	return table
}
func RequireWorkspaceTransition(from, to WorkspaceState) error {
	return guardTransition("workspace", WorkspaceState.IsValid, workspaceTransitions, from, to)
}
func CanWorkspaceTransition(from, to WorkspaceState) bool {
	return RequireWorkspaceTransition(from, to) == nil
}

type CaptureState string

const (
	CaptureRequested  CaptureState = "requested"
	CaptureRunning    CaptureState = "running"
	CaptureFinalizing CaptureState = "finalizing"
	CaptureCompleted  CaptureState = "completed"
	CaptureFailed     CaptureState = "failed"
	CaptureCancelled  CaptureState = "cancelled"
	CaptureLost       CaptureState = "lost"
)

func (s CaptureState) String() string { return string(s) }
func (s CaptureState) IsValid() bool  { return captureStates[s] }
func (s CaptureState) Terminal() bool {
	return s == CaptureCompleted || s == CaptureFailed || s == CaptureCancelled || s == CaptureLost
}

var captureStates = map[CaptureState]bool{CaptureRequested: true, CaptureRunning: true, CaptureFinalizing: true, CaptureCompleted: true, CaptureFailed: true, CaptureCancelled: true, CaptureLost: true}
var captureTransitions = newTransitionTable(
	transition[CaptureState]{CaptureRequested, CaptureRunning}, transition[CaptureState]{CaptureRunning, CaptureFinalizing}, transition[CaptureState]{CaptureFinalizing, CaptureCompleted},
	transition[CaptureState]{CaptureRequested, CaptureCancelled}, transition[CaptureState]{CaptureRunning, CaptureCancelled},
	transition[CaptureState]{CaptureRequested, CaptureFailed}, transition[CaptureState]{CaptureRunning, CaptureFailed}, transition[CaptureState]{CaptureFinalizing, CaptureFailed},
	transition[CaptureState]{CaptureRequested, CaptureLost}, transition[CaptureState]{CaptureRunning, CaptureLost}, transition[CaptureState]{CaptureFinalizing, CaptureLost},
)

func RequireCaptureTransition(from, to CaptureState) error {
	return guardTransition("capture", CaptureState.IsValid, captureTransitions, from, to)
}
func CanCaptureTransition(from, to CaptureState) bool {
	return RequireCaptureTransition(from, to) == nil
}

type ExportState string

const (
	ExportDeclared   ExportState = "declared"
	ExportCommitting ExportState = "committing"
	ExportCommitted  ExportState = "committed"
	ExportFailed     ExportState = "failed"
	ExportCancelled  ExportState = "cancelled"
)

func (s ExportState) String() string { return string(s) }
func (s ExportState) IsValid() bool  { return exportStates[s] }
func (s ExportState) Terminal() bool {
	return s == ExportCommitted || s == ExportFailed || s == ExportCancelled
}

var exportStates = map[ExportState]bool{ExportDeclared: true, ExportCommitting: true, ExportCommitted: true, ExportFailed: true, ExportCancelled: true}
var exportTransitions = newTransitionTable(
	transition[ExportState]{ExportDeclared, ExportCommitting}, transition[ExportState]{ExportCommitting, ExportCommitted},
	transition[ExportState]{ExportDeclared, ExportCancelled}, transition[ExportState]{ExportDeclared, ExportFailed}, transition[ExportState]{ExportCommitting, ExportFailed},
)

func RequireExportTransition(from, to ExportState) error {
	return guardTransition("export", ExportState.IsValid, exportTransitions, from, to)
}
func CanExportTransition(from, to ExportState) bool { return RequireExportTransition(from, to) == nil }

type ObservationBundleState string

const (
	ObservationBundleBuilding ObservationBundleState = "building"
	ObservationBundleSealed   ObservationBundleState = "sealed"
	ObservationBundleFailed   ObservationBundleState = "failed"
)

func (s ObservationBundleState) String() string { return string(s) }
func (s ObservationBundleState) IsValid() bool  { return observationBundleStates[s] }
func (s ObservationBundleState) Terminal() bool {
	return s == ObservationBundleSealed || s == ObservationBundleFailed
}

var observationBundleStates = map[ObservationBundleState]bool{ObservationBundleBuilding: true, ObservationBundleSealed: true, ObservationBundleFailed: true}
var observationBundleTransitions = newTransitionTable(transition[ObservationBundleState]{ObservationBundleBuilding, ObservationBundleSealed}, transition[ObservationBundleState]{ObservationBundleBuilding, ObservationBundleFailed})

func RequireObservationBundleTransition(from, to ObservationBundleState) error {
	return guardTransition("observation_bundle", ObservationBundleState.IsValid, observationBundleTransitions, from, to)
}
func CanObservationBundleTransition(from, to ObservationBundleState) bool {
	return RequireObservationBundleTransition(from, to) == nil
}

type IncidentState string

const (
	IncidentOpen           IncidentState = "open"
	IncidentEvidenceSealed IncidentState = "evidence_sealed"
	IncidentRecovering     IncidentState = "recovering"
	IncidentResolved       IncidentState = "resolved"
)

func (s IncidentState) String() string { return string(s) }
func (s IncidentState) IsValid() bool  { return incidentStates[s] }
func (s IncidentState) Terminal() bool { return s == IncidentResolved }

var incidentStates = map[IncidentState]bool{IncidentOpen: true, IncidentEvidenceSealed: true, IncidentRecovering: true, IncidentResolved: true}
var incidentTransitions = newTransitionTable(
	transition[IncidentState]{IncidentOpen, IncidentEvidenceSealed}, transition[IncidentState]{IncidentEvidenceSealed, IncidentRecovering},
	transition[IncidentState]{IncidentEvidenceSealed, IncidentResolved}, transition[IncidentState]{IncidentRecovering, IncidentResolved},
)

func RequireIncidentTransition(from, to IncidentState) error {
	return guardTransition("incident", IncidentState.IsValid, incidentTransitions, from, to)
}
func CanIncidentTransition(from, to IncidentState) bool {
	return RequireIncidentTransition(from, to) == nil
}
