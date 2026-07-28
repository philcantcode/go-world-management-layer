package domain

import "time"

type ResearchSession struct {
	id        ResearchSessionID
	state     ResearchSessionState
	revision  Revision
	createdAt time.Time
	updatedAt time.Time
}

func NewResearchSession(id ResearchSessionID, createdAt time.Time) (ResearchSession, error) {
	if err := requireID("research_session_id", id); err != nil {
		return ResearchSession{}, err
	}
	if err := requireTime("created_at", createdAt); err != nil {
		return ResearchSession{}, err
	}
	return ResearchSession{id: id, state: ResearchSessionRequested, revision: InitialRevision, createdAt: createdAt, updatedAt: createdAt}, nil
}
func (s ResearchSession) ID() ResearchSessionID       { return s.id }
func (s ResearchSession) State() ResearchSessionState { return s.state }
func (s ResearchSession) Revision() Revision          { return s.revision }
func (s ResearchSession) CreatedAt() time.Time        { return s.createdAt }
func (s ResearchSession) UpdatedAt() time.Time        { return s.updatedAt }
func (s ResearchSession) Transition(next ResearchSessionState, expected Revision, at time.Time) (ResearchSession, error) {
	if err := RequireResearchSessionTransition(s.state, next); err != nil {
		return ResearchSession{}, err
	}
	revision, err := nextModelRevision(s.revision, expected, s.updatedAt, at)
	if err != nil {
		return ResearchSession{}, err
	}
	s.state, s.revision, s.updatedAt = next, revision, at
	return s, nil
}

type LeaseSpec struct {
	ID                          LeaseID
	ResearchSessionID           ResearchSessionID
	AgentWorkspaceID            AgentWorkspaceID
	AgentGeneration             AgentGeneration
	InputViewID                 InputViewID
	PolicyDigest                Digest
	CapabilityFingerprintDigest Digest
	ExpiresAt                   time.Time
	CreatedAt                   time.Time
}
type Lease struct {
	spec      LeaseSpec
	state     LeaseState
	revision  Revision
	updatedAt time.Time
}

func NewLease(spec LeaseSpec) (Lease, error) {
	if err := requireID("lease_id", spec.ID); err != nil {
		return Lease{}, err
	}
	if err := requireID("research_session_id", spec.ResearchSessionID); err != nil {
		return Lease{}, err
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return Lease{}, err
	}
	if !spec.AgentGeneration.IsValid() {
		return Lease{}, NewError(CodeInvalidArgument, "lease.new", "agent_generation", "must be positive", nil)
	}
	if spec.InputViewID.IsZero() {
		return Lease{}, NewError(CodeInvalidID, "lease.new", "input_view_id", "must be set", nil)
	}
	if spec.PolicyDigest.IsZero() {
		return Lease{}, NewError(CodeInvalidArgument, "lease.new", "policy_digest", "must be set", nil)
	}
	if spec.CapabilityFingerprintDigest.IsZero() {
		return Lease{}, NewError(CodeInvalidArgument, "lease.new", "capability_fingerprint_digest", "must be set", nil)
	}
	if err := requireOrderedTimes("created_at", spec.CreatedAt, "expires_at", spec.ExpiresAt); err != nil {
		return Lease{}, err
	}
	if !spec.ExpiresAt.After(spec.CreatedAt) {
		return Lease{}, NewError(CodeInvalidArgument, "lease.new", "expires_at", "must be after created_at", nil)
	}
	return Lease{spec: spec, state: LeaseActive, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (l Lease) Spec() LeaseSpec                      { return l.spec }
func (l Lease) ID() LeaseID                          { return l.spec.ID }
func (l Lease) ResearchSessionID() ResearchSessionID { return l.spec.ResearchSessionID }
func (l Lease) State() LeaseState                    { return l.state }
func (l Lease) Revision() Revision                   { return l.revision }
func (l Lease) ExpiresAt() time.Time                 { return l.spec.ExpiresAt }
func (l Lease) UpdatedAt() time.Time                 { return l.updatedAt }
func (l Lease) Renew(expected Revision, expiresAt, at time.Time) (Lease, error) {
	if l.state != LeaseActive {
		return Lease{}, NewError(CodeFailedPrecondition, "lease.renew", "state", "lease is not active", nil)
	}
	if !expiresAt.After(l.spec.ExpiresAt) {
		return Lease{}, NewError(CodeInvalidArgument, "lease.renew", "expires_at", "must extend the current expiry", nil)
	}
	if !expiresAt.After(at) {
		return Lease{}, NewError(CodeInvalidArgument, "lease.renew", "expires_at", "must be in the future", nil)
	}
	revision, err := nextModelRevision(l.revision, expected, l.updatedAt, at)
	if err != nil {
		return Lease{}, err
	}
	l.spec.ExpiresAt, l.revision, l.updatedAt = expiresAt, revision, at
	return l, nil
}
func (l Lease) Transition(next LeaseState, expected Revision, at time.Time) (Lease, error) {
	if err := RequireLeaseTransition(l.state, next); err != nil {
		return Lease{}, err
	}
	revision, err := nextModelRevision(l.revision, expected, l.updatedAt, at)
	if err != nil {
		return Lease{}, err
	}
	l.state, l.revision, l.updatedAt = next, revision, at
	return l, nil
}

type AgentWorkspace struct {
	id                AgentWorkspaceID
	researchSessionID ResearchSessionID
	currentGeneration AgentGeneration
	revision          Revision
	createdAt         time.Time
	updatedAt         time.Time
}

func NewAgentWorkspace(id AgentWorkspaceID, sessionID ResearchSessionID, generation AgentGeneration, createdAt time.Time) (AgentWorkspace, error) {
	if err := requireID("agent_workspace_id", id); err != nil {
		return AgentWorkspace{}, err
	}
	if err := requireID("research_session_id", sessionID); err != nil {
		return AgentWorkspace{}, err
	}
	if !generation.IsValid() {
		return AgentWorkspace{}, NewError(CodeInvalidArgument, "agent_workspace.new", "generation", "must be positive", nil)
	}
	if err := requireTime("created_at", createdAt); err != nil {
		return AgentWorkspace{}, err
	}
	return AgentWorkspace{id: id, researchSessionID: sessionID, currentGeneration: generation, revision: InitialRevision, createdAt: createdAt, updatedAt: createdAt}, nil
}
func (w AgentWorkspace) ID() AgentWorkspaceID                 { return w.id }
func (w AgentWorkspace) ResearchSessionID() ResearchSessionID { return w.researchSessionID }
func (w AgentWorkspace) CurrentGeneration() AgentGeneration   { return w.currentGeneration }
func (w AgentWorkspace) Revision() Revision                   { return w.revision }
func (w AgentWorkspace) CreatedAt() time.Time                 { return w.createdAt }
func (w AgentWorkspace) UpdatedAt() time.Time                 { return w.updatedAt }
func (w AgentWorkspace) AdvanceGeneration(expected Revision, next AgentGeneration, at time.Time) (AgentWorkspace, error) {
	wanted, err := w.currentGeneration.Next()
	if err != nil {
		return AgentWorkspace{}, err
	}
	if next != wanted {
		return AgentWorkspace{}, NewDetailedError(CodeConflict, "agent_workspace.advance_generation", "generation", "must advance by exactly one", map[string]string{"current": uintString(uint64(w.currentGeneration)), "requested": uintString(uint64(next))}, nil)
	}
	revision, err := nextModelRevision(w.revision, expected, w.updatedAt, at)
	if err != nil {
		return AgentWorkspace{}, err
	}
	w.currentGeneration, w.revision, w.updatedAt = next, revision, at
	return w, nil
}

type AgentWorkspaceGenerationSpec struct {
	AgentWorkspaceID            AgentWorkspaceID
	Generation                  AgentGeneration
	WorkspaceID                 WorkspaceID
	InputViewID                 InputViewID
	PolicyDigest                Digest
	CapabilityFingerprintDigest Digest
	PreviousGeneration          AgentGeneration
	RecoveryIncidentID          IncidentID
	CreatedAt                   time.Time
}
type AgentWorkspaceGenerationRecord struct {
	spec      AgentWorkspaceGenerationSpec
	state     AgentGenerationState
	revision  Revision
	updatedAt time.Time
}

func NewAgentWorkspaceGeneration(spec AgentWorkspaceGenerationSpec) (AgentWorkspaceGenerationRecord, error) {
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return AgentWorkspaceGenerationRecord{}, err
	}
	if !spec.Generation.IsValid() {
		return AgentWorkspaceGenerationRecord{}, NewError(CodeInvalidArgument, "agent_generation.new", "generation", "must be positive", nil)
	}
	if err := requireID("workspace_id", spec.WorkspaceID); err != nil {
		return AgentWorkspaceGenerationRecord{}, err
	}
	if spec.InputViewID.IsZero() {
		return AgentWorkspaceGenerationRecord{}, NewError(CodeInvalidID, "agent_generation.new", "input_view_id", "must be set", nil)
	}
	if spec.PolicyDigest.IsZero() || spec.CapabilityFingerprintDigest.IsZero() {
		return AgentWorkspaceGenerationRecord{}, NewError(CodeInvalidArgument, "agent_generation.new", "provenance_digest", "policy and capability digests must be set", nil)
	}
	if spec.Generation == InitialAgentGeneration {
		if spec.PreviousGeneration != 0 || !spec.RecoveryIncidentID.IsZero() {
			return AgentWorkspaceGenerationRecord{}, NewError(CodeInvalidArgument, "agent_generation.new", "previous_generation", "initial generation cannot have recovery provenance", nil)
		}
	} else {
		expected := spec.Generation - 1
		if spec.PreviousGeneration != expected {
			return AgentWorkspaceGenerationRecord{}, NewError(CodeInvalidArgument, "agent_generation.new", "previous_generation", "later generations require the immediately previous generation", nil)
		}
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return AgentWorkspaceGenerationRecord{}, err
	}
	return AgentWorkspaceGenerationRecord{spec: spec, state: AgentGenerationProvisioning, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (g AgentWorkspaceGenerationRecord) Spec() AgentWorkspaceGenerationSpec { return g.spec }
func (g AgentWorkspaceGenerationRecord) State() AgentGenerationState        { return g.state }
func (g AgentWorkspaceGenerationRecord) Revision() Revision                 { return g.revision }
func (g AgentWorkspaceGenerationRecord) UpdatedAt() time.Time               { return g.updatedAt }
func (g AgentWorkspaceGenerationRecord) Transition(next AgentGenerationState, expected Revision, at time.Time) (AgentWorkspaceGenerationRecord, error) {
	if err := RequireAgentGenerationTransition(g.state, next); err != nil {
		return AgentWorkspaceGenerationRecord{}, err
	}
	revision, err := nextModelRevision(g.revision, expected, g.updatedAt, at)
	if err != nil {
		return AgentWorkspaceGenerationRecord{}, err
	}
	g.state, g.revision, g.updatedAt = next, revision, at
	return g, nil
}

type ExecKind string

const (
	ExecProvider ExecKind = "provider"
	ExecTool     ExecKind = "tool"
)

func (k ExecKind) IsValid() bool { return k == ExecProvider || k == ExecTool }

type ExecSpec struct {
	ID               ExecID
	LeaseID          LeaseID
	AgentWorkspaceID AgentWorkspaceID
	AgentGeneration  AgentGeneration
	Kind             ExecKind
	Executable       string
	// Argv contains only the arguments after argv[0]. Executable supplies
	// both the program to launch and argv[0].
	Argv             []string
	WorkingDirectory string
	CreatedAt        time.Time
}
type Exec struct {
	spec      ExecSpec
	state     ExecState
	revision  Revision
	updatedAt time.Time
}

func NewExec(spec ExecSpec) (Exec, error) {
	if err := requireID("exec_id", spec.ID); err != nil {
		return Exec{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return Exec{}, err
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return Exec{}, err
	}
	if !spec.AgentGeneration.IsValid() {
		return Exec{}, NewError(CodeInvalidArgument, "exec.new", "agent_generation", "must be positive", nil)
	}
	if !spec.Kind.IsValid() {
		return Exec{}, NewError(CodeInvalidArgument, "exec.new", "kind", "is not recognized", nil)
	}
	if err := requireNonBlank("executable", spec.Executable); err != nil {
		return Exec{}, err
	}
	if err := requireRelativePath("working_directory", spec.WorkingDirectory, true); err != nil {
		return Exec{}, err
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return Exec{}, err
	}
	spec.Argv = cloneSlice(spec.Argv)
	return Exec{spec: spec, state: ExecRequested, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (e Exec) Spec() ExecSpec     { result := e.spec; result.Argv = cloneSlice(e.spec.Argv); return result }
func (e Exec) ID() ExecID         { return e.spec.ID }
func (e Exec) State() ExecState   { return e.state }
func (e Exec) Revision() Revision { return e.revision }
func (e Exec) Transition(next ExecState, expected Revision, at time.Time) (Exec, error) {
	if err := RequireExecTransition(e.state, next); err != nil {
		return Exec{}, err
	}
	revision, err := nextModelRevision(e.revision, expected, e.updatedAt, at)
	if err != nil {
		return Exec{}, err
	}
	e.state, e.revision, e.updatedAt = next, revision, at
	return e, nil
}

type TargetKind string

const (
	TargetLinuxContainer       TargetKind = "linux_container"
	TargetAndroidVirtualDevice TargetKind = "android_virtual_device"
	TargetPhysicalDevice       TargetKind = "physical_device"
)

func (k TargetKind) IsValid() bool {
	return k == TargetLinuxContainer || k == TargetAndroidVirtualDevice || k == TargetPhysicalDevice
}

type Target struct {
	id                TargetID
	researchSessionID ResearchSessionID
	kind              TargetKind
	currentGeneration TargetGeneration
	revision          Revision
	createdAt         time.Time
	updatedAt         time.Time
}

func NewTarget(id TargetID, sessionID ResearchSessionID, kind TargetKind, generation TargetGeneration, createdAt time.Time) (Target, error) {
	if err := requireID("target_id", id); err != nil {
		return Target{}, err
	}
	if err := requireID("research_session_id", sessionID); err != nil {
		return Target{}, err
	}
	if !kind.IsValid() {
		return Target{}, NewError(CodeInvalidArgument, "target.new", "kind", "is not recognized", nil)
	}
	if !generation.IsValid() {
		return Target{}, NewError(CodeInvalidArgument, "target.new", "generation", "must be positive", nil)
	}
	if err := requireTime("created_at", createdAt); err != nil {
		return Target{}, err
	}
	return Target{id: id, researchSessionID: sessionID, kind: kind, currentGeneration: generation, revision: InitialRevision, createdAt: createdAt, updatedAt: createdAt}, nil
}
func (t Target) ID() TargetID                         { return t.id }
func (t Target) ResearchSessionID() ResearchSessionID { return t.researchSessionID }
func (t Target) Kind() TargetKind                     { return t.kind }
func (t Target) CurrentGeneration() TargetGeneration  { return t.currentGeneration }
func (t Target) Revision() Revision                   { return t.revision }
func (t Target) UpdatedAt() time.Time                 { return t.updatedAt }
func (t Target) AdvanceGeneration(expected Revision, next TargetGeneration, at time.Time) (Target, error) {
	wanted, err := t.currentGeneration.Next()
	if err != nil {
		return Target{}, err
	}
	if next != wanted {
		return Target{}, NewDetailedError(CodeConflict, "target.advance_generation", "generation", "must advance by exactly one", map[string]string{"current": uintString(uint64(t.currentGeneration)), "requested": uintString(uint64(next))}, nil)
	}
	revision, err := nextModelRevision(t.revision, expected, t.updatedAt, at)
	if err != nil {
		return Target{}, err
	}
	t.currentGeneration, t.revision, t.updatedAt = next, revision, at
	return t, nil
}

type TargetGenerationSpec struct {
	TargetID                    TargetID
	Generation                  TargetGeneration
	PolicyDigest                Digest
	CapabilityFingerprintDigest Digest
	PreviousGeneration          TargetGeneration
	RecoveryIncidentID          IncidentID
	CreatedAt                   time.Time
}
type TargetGenerationRecord struct {
	spec      TargetGenerationSpec
	state     TargetGenerationState
	revision  Revision
	updatedAt time.Time
}

func NewTargetGeneration(spec TargetGenerationSpec) (TargetGenerationRecord, error) {
	if err := requireID("target_id", spec.TargetID); err != nil {
		return TargetGenerationRecord{}, err
	}
	if !spec.Generation.IsValid() {
		return TargetGenerationRecord{}, NewError(CodeInvalidArgument, "target_generation.new", "generation", "must be positive", nil)
	}
	if spec.PolicyDigest.IsZero() || spec.CapabilityFingerprintDigest.IsZero() {
		return TargetGenerationRecord{}, NewError(CodeInvalidArgument, "target_generation.new", "provenance_digest", "policy and capability digests must be set", nil)
	}
	if spec.Generation == InitialTargetGeneration {
		if spec.PreviousGeneration != 0 || !spec.RecoveryIncidentID.IsZero() {
			return TargetGenerationRecord{}, NewError(CodeInvalidArgument, "target_generation.new", "previous_generation", "initial generation cannot have recovery provenance", nil)
		}
	} else {
		if spec.PreviousGeneration != spec.Generation-1 {
			return TargetGenerationRecord{}, NewError(CodeInvalidArgument, "target_generation.new", "previous_generation", "later generations require the immediately previous generation", nil)
		}
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return TargetGenerationRecord{}, err
	}
	return TargetGenerationRecord{spec: spec, state: TargetGenerationProvisioning, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (g TargetGenerationRecord) Spec() TargetGenerationSpec   { return g.spec }
func (g TargetGenerationRecord) State() TargetGenerationState { return g.state }
func (g TargetGenerationRecord) Revision() Revision           { return g.revision }
func (g TargetGenerationRecord) UpdatedAt() time.Time         { return g.updatedAt }
func (g TargetGenerationRecord) Transition(next TargetGenerationState, expected Revision, at time.Time) (TargetGenerationRecord, error) {
	if err := RequireTargetGenerationTransition(g.state, next); err != nil {
		return TargetGenerationRecord{}, err
	}
	revision, err := nextModelRevision(g.revision, expected, g.updatedAt, at)
	if err != nil {
		return TargetGenerationRecord{}, err
	}
	g.state, g.revision, g.updatedAt = next, revision, at
	return g, nil
}

type TargetRunSpec struct {
	ID                    TargetRunID
	LeaseID               LeaseID
	TargetID              TargetID
	TargetGeneration      TargetGeneration
	AgentWorkspaceID      AgentWorkspaceID
	AgentGeneration       AgentGeneration
	MaterializationDigest Digest
	CreatedAt             time.Time
}
type TargetRun struct {
	spec      TargetRunSpec
	state     TargetRunState
	revision  Revision
	updatedAt time.Time
}

func NewTargetRun(spec TargetRunSpec) (TargetRun, error) {
	if err := requireID("target_run_id", spec.ID); err != nil {
		return TargetRun{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return TargetRun{}, err
	}
	if err := requireID("target_id", spec.TargetID); err != nil {
		return TargetRun{}, err
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return TargetRun{}, err
	}
	if !spec.TargetGeneration.IsValid() {
		return TargetRun{}, NewError(CodeInvalidArgument, "target_run.new", "target_generation", "must be positive", nil)
	}
	if !spec.AgentGeneration.IsValid() {
		return TargetRun{}, NewError(CodeInvalidArgument, "target_run.new", "agent_generation", "must be positive", nil)
	}
	if spec.MaterializationDigest.IsZero() {
		return TargetRun{}, NewError(CodeInvalidArgument, "target_run.new", "materialization_digest", "must be set", nil)
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return TargetRun{}, err
	}
	return TargetRun{spec: spec, state: TargetRunRequested, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (r TargetRun) Spec() TargetRunSpec   { return r.spec }
func (r TargetRun) ID() TargetRunID       { return r.spec.ID }
func (r TargetRun) State() TargetRunState { return r.state }
func (r TargetRun) Revision() Revision    { return r.revision }
func (r TargetRun) UpdatedAt() time.Time  { return r.updatedAt }
func (r TargetRun) Transition(next TargetRunState, expected Revision, at time.Time) (TargetRun, error) {
	if err := RequireTargetRunTransition(r.state, next); err != nil {
		return TargetRun{}, err
	}
	revision, err := nextModelRevision(r.revision, expected, r.updatedAt, at)
	if err != nil {
		return TargetRun{}, err
	}
	r.state, r.revision, r.updatedAt = next, revision, at
	return r, nil
}

type TargetOperationKind string

const (
	TargetOperationExec       TargetOperationKind = "exec"
	TargetOperationShell      TargetOperationKind = "shell"
	TargetOperationPush       TargetOperationKind = "push"
	TargetOperationPull       TargetOperationKind = "pull"
	TargetOperationADBService TargetOperationKind = "adb_service"
	TargetOperationLifecycle  TargetOperationKind = "lifecycle"
)

func (k TargetOperationKind) IsValid() bool {
	switch k {
	case TargetOperationExec, TargetOperationShell, TargetOperationPush, TargetOperationPull, TargetOperationADBService, TargetOperationLifecycle:
		return true
	}
	return false
}

type TargetOperationSpec struct {
	ID               TargetOperationID
	LeaseID          LeaseID
	TargetID         TargetID
	TargetGeneration TargetGeneration
	TargetRunID      TargetRunID
	Kind             TargetOperationKind
	CommandDisplay   string
	ContentDigest    Digest
	CreatedAt        time.Time
}
type TargetOperation struct {
	spec      TargetOperationSpec
	state     TargetOperationState
	revision  Revision
	updatedAt time.Time
}

func NewTargetOperation(spec TargetOperationSpec) (TargetOperation, error) {
	if err := requireID("target_operation_id", spec.ID); err != nil {
		return TargetOperation{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return TargetOperation{}, err
	}
	if err := requireID("target_id", spec.TargetID); err != nil {
		return TargetOperation{}, err
	}
	if err := requireID("target_run_id", spec.TargetRunID); err != nil {
		return TargetOperation{}, err
	}
	if !spec.TargetGeneration.IsValid() {
		return TargetOperation{}, NewError(CodeInvalidArgument, "target_operation.new", "target_generation", "must be positive", nil)
	}
	if !spec.Kind.IsValid() {
		return TargetOperation{}, NewError(CodeInvalidArgument, "target_operation.new", "kind", "is not recognized", nil)
	}
	if spec.CommandDisplay == "" && spec.ContentDigest.IsZero() {
		return TargetOperation{}, NewError(CodeInvalidArgument, "target_operation.new", "description", "command_display or content_digest must be set", nil)
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return TargetOperation{}, err
	}
	return TargetOperation{spec: spec, state: TargetOperationRequested, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}
func (o TargetOperation) Spec() TargetOperationSpec   { return o.spec }
func (o TargetOperation) ID() TargetOperationID       { return o.spec.ID }
func (o TargetOperation) State() TargetOperationState { return o.state }
func (o TargetOperation) Revision() Revision          { return o.revision }
func (o TargetOperation) Transition(next TargetOperationState, expected Revision, at time.Time) (TargetOperation, error) {
	if err := RequireTargetOperationTransition(o.state, next); err != nil {
		return TargetOperation{}, err
	}
	revision, err := nextModelRevision(o.revision, expected, o.updatedAt, at)
	if err != nil {
		return TargetOperation{}, err
	}
	o.state, o.revision, o.updatedAt = next, revision, at
	return o, nil
}
