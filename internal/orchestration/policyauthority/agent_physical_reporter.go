package policyauthority

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// AgentPhysicalEnforcement describes host-owned enforcement that complements
// the agent runtime's own physical report. Applying it through a reporter
// wrapper guarantees that config-level publication and exact-plan admission
// see the same composition facts.
type AgentPhysicalEnforcement struct {
	DirectoryWorkspace   bool
	BoundedLedgerCapture bool
}

// NewAgentPhysicalPolicyReporter applies host-owned physical enforcement to
// both reports exposed by delegate.
func NewAgentPhysicalPolicyReporter(delegate ports.AgentWorkspacePhysicalPolicyReporter, enforcement AgentPhysicalEnforcement) (ports.AgentWorkspacePhysicalPolicyReporter, error) {
	if delegate == nil {
		return nil, fmt.Errorf("agent physical policy reporter is required")
	}
	return &agentPhysicalPolicyReporter{delegate: delegate, enforcement: enforcement}, nil
}

type agentPhysicalPolicyReporter struct {
	delegate    ports.AgentWorkspacePhysicalPolicyReporter
	enforcement AgentPhysicalEnforcement
}

func (r *agentPhysicalPolicyReporter) AgentWorkspacePhysicalPolicy(ctx context.Context) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	report, err := r.delegate.AgentWorkspacePhysicalPolicy(ctx)
	if err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, err
	}
	return r.enforcement.Apply(report), nil
}

func (r *agentPhysicalPolicyReporter) AgentWorkspacePlanPhysicalPolicy(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	report, err := r.delegate.AgentWorkspacePlanPhysicalPolicy(ctx, plan)
	if err != nil {
		return ports.AgentWorkspacePhysicalPolicyReport{}, err
	}
	return r.enforcement.Apply(report), nil
}

// Apply adds only enforcement owned by the composed host services. Runtime
// facts and exact plan values remain supplied by the underlying driver.
func (e AgentPhysicalEnforcement) Apply(report ports.AgentWorkspacePhysicalPolicyReport) ports.AgentWorkspacePhysicalPolicyReport {
	if e.DirectoryWorkspace {
		report = WithDirectoryWorkspaceEnforcement(report)
	}
	if e.BoundedLedgerCapture {
		report = WithBoundedLedgerCaptureEnforcement(report)
	}
	return report
}

var _ ports.AgentWorkspacePhysicalPolicyReporter = (*agentPhysicalPolicyReporter)(nil)
