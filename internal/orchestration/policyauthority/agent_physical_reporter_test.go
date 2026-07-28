package policyauthority

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAgentPhysicalPolicyReporterAppliesCompositionToConfigAndPlan(t *testing.T) {
	rawConfig := rawAgentPhysicalReport()
	rawPlan := rawAgentPhysicalReport()
	rawPlan.Resources.WorkspaceBytes.Value = 4096
	rawPlan.Resources.CaptureBytes.Value = 8192
	rawPlan.Resources.Inodes.Value = 16
	delegate := &physicalReporterStub{config: rawConfig, plan: rawPlan}
	enforcement := AgentPhysicalEnforcement{DirectoryWorkspace: true, BoundedLedgerCapture: true}
	reporter, err := NewAgentPhysicalPolicyReporter(delegate, enforcement)
	if err != nil {
		t.Fatal(err)
	}

	configured, err := reporter.AgentWorkspacePhysicalPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	planned, err := reporter.AgentWorkspacePlanPhysicalPolicy(context.Background(), ports.AgentWorkspacePlan{})
	if err != nil {
		t.Fatal(err)
	}
	if expected := enforcement.Apply(rawConfig); !reflect.DeepEqual(configured, expected) {
		t.Fatalf("config report differs from shared enforcement: %#v", configured)
	}
	if expected := enforcement.Apply(rawPlan); !reflect.DeepEqual(planned, expected) {
		t.Fatalf("plan report differs from shared enforcement: %#v", planned)
	}
	if planned.Resources.WorkspaceBytes.Value != 4096 || planned.Resources.CaptureBytes.Value != 8192 || planned.Resources.Inodes.Value != 16 {
		t.Fatal("composition enforcement changed exact plan resource values")
	}
}

func TestAgentPhysicalPolicyReporterRejectsNilAndPreservesErrors(t *testing.T) {
	if _, err := NewAgentPhysicalPolicyReporter(nil, AgentPhysicalEnforcement{}); err == nil {
		t.Fatal("nil delegate was accepted")
	}
	want := errors.New("report failed")
	reporter, err := NewAgentPhysicalPolicyReporter(&physicalReporterStub{err: want}, AgentPhysicalEnforcement{DirectoryWorkspace: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reporter.AgentWorkspacePhysicalPolicy(context.Background()); !errors.Is(err, want) {
		t.Fatalf("config report error = %v, want %v", err, want)
	}
	if _, err := reporter.AgentWorkspacePlanPhysicalPolicy(context.Background(), ports.AgentWorkspacePlan{}); !errors.Is(err, want) {
		t.Fatalf("plan report error = %v, want %v", err, want)
	}
}

func rawAgentPhysicalReport() ports.AgentWorkspacePhysicalPolicyReport {
	unsupported := func(detail string) ports.PhysicalLimitFact {
		return ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported, Detail: detail}
	}
	return ports.AgentWorkspacePhysicalPolicyReport{Resources: ports.ContainerResourcePhysicalFacts{
		WorkspaceBytes: unsupported("runtime workspace limit unavailable"),
		CaptureBytes:   unsupported("runtime capture limit unavailable"),
		Inodes:         unsupported("runtime inode limit unavailable"),
	}}
}

type physicalReporterStub struct {
	config ports.AgentWorkspacePhysicalPolicyReport
	plan   ports.AgentWorkspacePhysicalPolicyReport
	err    error
}

func (s *physicalReporterStub) AgentWorkspacePhysicalPolicy(context.Context) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	return s.config, s.err
}

func (s *physicalReporterStub) AgentWorkspacePlanPhysicalPolicy(context.Context, ports.AgentWorkspacePlan) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	return s.plan, s.err
}
