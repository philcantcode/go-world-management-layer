package ports

import (
	"context"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// PhysicalResourceClassification describes what an authoritative driver
// inventory proved about one durable generation. Unknown or incomplete
// observations are never represented as missing.
type PhysicalResourceClassification string

const (
	PhysicalResourceAdopted   PhysicalResourceClassification = "adopted"
	PhysicalResourceMissing   PhysicalResourceClassification = "missing"
	PhysicalResourceOrphan    PhysicalResourceClassification = "orphan"
	PhysicalResourceForeign   PhysicalResourceClassification = "foreign"
	PhysicalResourceUncertain PhysicalResourceClassification = "uncertain"
)

func (c PhysicalResourceClassification) IsValid() bool {
	switch c {
	case PhysicalResourceAdopted, PhysicalResourceMissing, PhysicalResourceOrphan, PhysicalResourceForeign, PhysicalResourceUncertain:
		return true
	default:
		return false
	}
}

// AgentWorkspaceReconciliation is the physical observation for one expected
// or unclaimed agent workspace generation. ContainerID is empty only when no
// single physical resource could be identified.
type AgentWorkspaceReconciliation struct {
	Ref            AgentWorkspaceRef
	ContainerID    string
	Classification PhysicalResourceClassification
	// PlanMatched is true only when the driver proved that the single
	// identified physical resource matches the complete expected immutable
	// plan. It does not imply guest readiness or authorize execution; startup
	// recovery uses it solely to retire or retain a failed predecessor safely.
	PlanMatched bool
	Diagnostic  string
}

type AgentWorkspaceReconciliationReport struct {
	Expected   []AgentWorkspaceReconciliation
	Unclaimed  []AgentWorkspaceReconciliation
	Conflicts  []PhysicalResourceConflict
	ObservedAt time.Time
}

// AgentWorkspaceReconciliationRequest separates generations that may be
// adopted for work from generations supplied solely as exact cleanup
// authority. Cleanup plans must be inventoried and retained only for
// Stop/Destroy; they must never become executable workspaces.
type AgentWorkspaceReconciliationRequest struct {
	Active      []AgentWorkspacePlan
	CleanupOnly []AgentWorkspacePlan
}

// AgentWorkspaceReconciler is optional. Callers should feature-detect it so
// simple AgentWorkspaceDriver fakes and non-inventory backends remain valid.
type AgentWorkspaceReconciler interface {
	ReconcileAgentWorkspaces(context.Context, AgentWorkspaceReconciliationRequest) (AgentWorkspaceReconciliationReport, error)
}

// AgentExecCrashRecovery is the proof returned after a driver has crossed a
// fresh execution boundary for one exact persisted agent workspace plan. A
// successful result means every process that could have been started through
// the pre-restart container boundary is gone and the same generation has
// completed a new guest readiness handshake.
type AgentExecCrashRecovery struct {
	Status                     AgentWorkspaceStatus
	PreviousBoundaryTerminated bool
}

func (r AgentExecCrashRecovery) ValidateFor(plan AgentWorkspacePlan) error {
	const operation = "ports.agent_exec_crash_recovery.validate"
	if err := plan.Validate(); err != nil {
		return err
	}
	spec := plan.Generation.Spec()
	if !r.PreviousBoundaryTerminated {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "execution_boundary", "driver did not prove termination of the pre-restart execution boundary", nil)
	}
	if r.Status.AgentWorkspaceID != spec.AgentWorkspaceID || r.Status.Generation != spec.Generation {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "generation", "driver returned proof for a different agent generation", nil)
	}
	if r.Status.State != domain.AgentGenerationReady || !r.Status.Ready ||
		strings.TrimSpace(r.Status.ContainerID) == "" ||
		r.Status.GuestProtocol == 0 || r.Status.ObservedAt.IsZero() {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "readiness", "driver did not return a complete fresh-ready agent status", nil)
	}
	return nil
}

// AgentExecCrashReconciler is implemented by agent drivers that can use an
// exact persisted plan to terminate the execution boundary owned by a dead
// daemon and then restore fresh guest readiness without changing generation
// identity. Implementations must fail closed on foreign or ambiguous physical
// resources.
type AgentExecCrashReconciler interface {
	RecoverInterruptedExecs(context.Context, AgentWorkspacePlan) (AgentExecCrashRecovery, error)
}

// TargetReconciliation is the physical observation for one expected or
// unclaimed target generation. RuntimeID is empty only when no single physical
// resource could be identified.
type TargetReconciliation struct {
	Ref            TargetRef
	RuntimeID      string
	Classification PhysicalResourceClassification
	// PlanMatched is true only when the identified resource matches the
	// complete expected immutable plan. It does not imply readiness or permit
	// execution; cleanup-only inventory uses it as deletion authority.
	PlanMatched bool
	// CleanupRequired is true only when the runtime is authoritatively missing
	// but the complete expected plan still owns local driver state that Destroy
	// must retire. It is evidence of residue, not deletion authority by itself:
	// orchestration may act on it only under a terminal or durable-operation
	// cleanup decision, never for an unclaimed resource.
	CleanupRequired bool
	Diagnostic      string
}

type TargetReconciliationReport struct {
	Expected   []TargetReconciliation
	Unclaimed  []TargetReconciliation
	Conflicts  []PhysicalResourceConflict
	ObservedAt time.Time
}

// TargetReconciliationRequest separates live generations from historical
// generations that may only be inspected, stopped, and destroyed.
type TargetReconciliationRequest struct {
	Active      []TargetPlan
	CleanupOnly []TargetPlan
}

// PhysicalResourceConflict reports a world-named or world-labelled resource
// whose labels are too malformed to construct a typed generation reference.
type PhysicalResourceConflict struct {
	ResourceID     string
	Name           string
	Classification PhysicalResourceClassification
	Diagnostic     string
}

// TargetReconciler is optional for the same reason as
// AgentWorkspaceReconciler.
type TargetReconciler interface {
	ReconcileTargets(context.Context, TargetReconciliationRequest) (TargetReconciliationReport, error)
}

// TargetRunCrashReconciler is implemented only by target drivers that can
// authoritatively terminate every execution left behind by a lost controller,
// preserve the adopted generation identity, and rebuild the persisted run in
// a prepared-only state. Implementations must not start specimen execution or
// arm the run's maximum-duration timer.
type TargetRunCrashReconciler interface {
	RecoverInterruptedRun(context.Context, TargetRunPlan) (PreparedTargetRun, error)
}

// ObserverCrashReconciler is implemented only by observer drivers that can
// prove every collector process covered by their platform authority is gone
// after controller loss. The guarantee is recorded before collector ownership
// begins and checked again by the next process; a driver must return false when
// a custom starter or platform cannot provide that invariant. Direct-process
// parent-death signaling cannot cover independently surviving descendants;
// those require a process-tree authority such as a Windows Job or cgroup.
type ObserverCrashReconciler interface {
	InterruptedCollectorCleanupGuaranteed() bool
	ReconcileInterruptedCollectors(context.Context, InterruptedCollectorReconciliation) (InterruptedCollectorReconciliationReport, error)
}

// InterruptedCollectorBinding is the exact durable authority binding for one
// collector that may have crossed a daemon-loss boundary. StartCommitted is
// set only after ObserverDriver.Start returned successfully and that fact was
// durably recorded. A missing output transaction is therefore a valid
// never-started state only when StartCommitted is false.
type InterruptedCollectorBinding struct {
	Plan           CollectorPlan `json:"plan"`
	StartCommitted bool          `json:"start_committed"`
}

// InterruptedCollectorReconciliation carries every collector binding for one
// run. The complete CollectorPlan is intentional: recovery must neither mint a
// fresh collector identity nor guess any authority-selected configuration.
type InterruptedCollectorReconciliation struct {
	TargetRunID domain.TargetRunID
	Collectors  []InterruptedCollectorBinding
}

func (r InterruptedCollectorReconciliation) Validate() error {
	const operation = "ports.interrupted_collector_reconciliation.validate"
	if r.TargetRunID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "target_run_id", "must be set", nil)
	}
	collectorIDs := make(map[string]struct{}, len(r.Collectors))
	idempotencyKeys := make(map[string]struct{}, len(r.Collectors))
	for _, binding := range r.Collectors {
		if err := binding.Plan.Validate(); err != nil {
			return err
		}
		if binding.Plan.TargetRunID != r.TargetRunID {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "collector.target_run_id", "does not match the reconciled run", nil)
		}
		collectorID := binding.Plan.CollectorID.String()
		if _, duplicate := collectorIDs[collectorID]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, operation, "collector_id", "is duplicated", nil)
		}
		collectorIDs[collectorID] = struct{}{}
		if _, duplicate := idempotencyKeys[binding.Plan.IdempotencyKey]; duplicate {
			return domain.NewError(domain.CodeInvalidArgument, operation, "idempotency_key", "is duplicated", nil)
		}
		idempotencyKeys[binding.Plan.IdempotencyKey] = struct{}{}
	}
	return nil
}

type InterruptedCollectorOutputState string

const (
	InterruptedCollectorOutputFinalized InterruptedCollectorOutputState = "finalized"
	InterruptedCollectorOutputAborted   InterruptedCollectorOutputState = "aborted"
)

func (s InterruptedCollectorOutputState) IsValid() bool {
	return s == InterruptedCollectorOutputFinalized || s == InterruptedCollectorOutputAborted
}

// InterruptedCollectorOutput classifies one and only one expected durable
// output transaction. Finalized transactions return their verified immutable
// artifacts; aborted transactions return none. CaptureLimitExceeded preserves
// the finalization boundary without exposing a driver-specific error value.
type InterruptedCollectorOutput struct {
	CollectorID          domain.CollectorID
	State                InterruptedCollectorOutputState
	Artifacts            []domain.ArtifactReference
	CaptureLimitExceeded bool
}

type InterruptedCollectorReconciliationReport struct {
	TargetRunID domain.TargetRunID
	Outputs     []InterruptedCollectorOutput
}

func (r InterruptedCollectorReconciliationReport) ValidateFor(request InterruptedCollectorReconciliation) error {
	const operation = "ports.interrupted_collector_reconciliation_report.validate"
	if err := request.Validate(); err != nil {
		return err
	}
	if r.TargetRunID != request.TargetRunID {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "target_run_id", "does not match the request", nil)
	}
	expected := make(map[string]CollectorPlan, len(request.Collectors))
	for _, binding := range request.Collectors {
		expected[binding.Plan.CollectorID.String()] = binding.Plan
	}
	observed := make(map[string]struct{}, len(r.Outputs))
	for _, output := range r.Outputs {
		collectorID := output.CollectorID.String()
		if output.CollectorID.IsZero() || !output.State.IsValid() {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "output", "collector identity and state must be valid", nil)
		}
		plan, found := expected[collectorID]
		if !found {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "collector_id", "was not requested", nil)
		}
		if _, duplicate := observed[collectorID]; duplicate {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "collector_id", "was reported more than once", nil)
		}
		observed[collectorID] = struct{}{}
		if output.State == InterruptedCollectorOutputAborted && (len(output.Artifacts) != 0 || output.CaptureLimitExceeded) {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "output", "aborted output cannot contain finalized artifacts", nil)
		}
		if output.State == InterruptedCollectorOutputFinalized {
			if err := validateInterruptedCollectorArtifacts(plan, output); err != nil {
				return err
			}
		}
	}
	if len(observed) != len(expected) {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "outputs", "does not classify every requested collector", nil)
	}
	return nil
}

func validateInterruptedCollectorArtifacts(plan CollectorPlan, output InterruptedCollectorOutput) error {
	const operation = "ports.interrupted_collector_reconciliation_report.validate"
	if len(output.Artifacts) != 2 {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "artifacts", "finalized output must contain exactly stdout and stderr", nil)
	}
	requiredRoles := map[string]bool{CollectorStdoutArtifactRole: false, CollectorStderrArtifactRole: false}
	references := make(map[string]struct{}, len(output.Artifacts))
	var totalSize int64
	for _, artifact := range output.Artifacts {
		spec := artifact.Spec()
		if _, err := domain.NewArtifactReference(spec); err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "artifact", "is invalid", err)
		}
		seen, expectedRole := requiredRoles[spec.Role]
		if !expectedRole || seen {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "artifact.role", "must identify each stream exactly once", nil)
		}
		requiredRoles[spec.Role] = true
		expectedReference := "observer://collectors/" + plan.CollectorID.String() + "/" + strings.TrimPrefix(spec.Role, "collector.") + "/" + spec.Digest.String()
		if spec.Reference != expectedReference {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "artifact.reference", "does not match the exact collector, stream, and digest", nil)
		}
		if _, duplicate := references[spec.Reference]; duplicate {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "artifact.reference", "is duplicated", nil)
		}
		references[spec.Reference] = struct{}{}
		if spec.Size > plan.MaximumBytes-totalSize {
			return domain.NewError(domain.CodeIntegrityViolation, operation, "artifacts.size", "exceeds the collector's shared byte limit", nil)
		}
		totalSize += spec.Size
	}
	if output.CaptureLimitExceeded && totalSize != plan.MaximumBytes {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "capture_limit_exceeded", "requires a retained prefix exactly at the authorized byte limit", nil)
	}
	return nil
}
