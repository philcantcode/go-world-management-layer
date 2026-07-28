package daemon

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
)

type leaseTerminationReconciler interface {
	ReconcileLeaseTerminations(context.Context) (orchestration.LeaseTerminationScanReport, error)
}

type physicalResourceReconciler interface {
	ReconcilePhysicalResources(context.Context) (orchestration.PhysicalReconciliationReport, error)
}

type startupStateReconciler interface {
	physicalResourceReconciler
	leaseTerminationReconciler
}

type startupReconciliationReports struct {
	Physical         orchestration.PhysicalReconciliationReport
	LeaseTermination orchestration.LeaseTerminationScanReport
}

// reconcileStartupStateWithin adopts generations and resolves every
// interrupted run before lease cleanup is resumed. Both stages complete before
// the listener is created, so no RPC can observe or reuse crash-owned work.
func reconcileStartupStateWithin(parent context.Context, reconciler startupStateReconciler, timeout time.Duration) (startupReconciliationReports, error) {
	if reconciler == nil {
		return startupReconciliationReports{}, fmt.Errorf("startup state reconciler is required")
	}
	physical, err := reconcilePhysicalResourcesWithin(parent, reconciler, timeout)
	if err != nil {
		return startupReconciliationReports{Physical: physical}, fmt.Errorf("physical reconciliation: %w", err)
	}
	termination, err := reconcileLeaseTerminationsWithin(parent, reconciler, timeout)
	if err != nil {
		return startupReconciliationReports{Physical: physical, LeaseTermination: termination}, fmt.Errorf("lease termination reconciliation: %w", err)
	}
	return startupReconciliationReports{Physical: physical, LeaseTermination: termination}, nil
}

func reconcilePhysicalResourcesWithin(parent context.Context, reconciler physicalResourceReconciler, timeout time.Duration) (orchestration.PhysicalReconciliationReport, error) {
	if reconciler == nil {
		return orchestration.PhysicalReconciliationReport{}, fmt.Errorf("physical resource reconciler is required")
	}
	if timeout <= 0 {
		return orchestration.PhysicalReconciliationReport{}, fmt.Errorf("positive reconciliation timeout is required")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	report, err := reconciler.ReconcilePhysicalResources(ctx)
	if err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("physical reconciliation exceeded %s: %w", timeout, err)
	}
	return report, nil
}

func reconcileLeaseTerminationsWithin(parent context.Context, reconciler leaseTerminationReconciler, timeout time.Duration) (orchestration.LeaseTerminationScanReport, error) {
	if reconciler == nil {
		return orchestration.LeaseTerminationScanReport{}, fmt.Errorf("lease termination reconciler is required")
	}
	if timeout <= 0 {
		return orchestration.LeaseTerminationScanReport{}, fmt.Errorf("positive reconciliation timeout is required")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	report, err := reconciler.ReconcileLeaseTerminations(ctx)
	if err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("lease termination reconciliation exceeded %s: %w", timeout, err)
	}
	return report, nil
}

func runLeaseTerminationTicker(ctx context.Context, reconciler leaseTerminationReconciler, interval, timeout time.Duration, logf func(string, ...any)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	runLeaseTerminationTicks(ctx, ticker.C, reconciler, timeout, logf)
}

func runLeaseTerminationTicks(ctx context.Context, ticks <-chan time.Time, reconciler leaseTerminationReconciler, timeout time.Duration, logf func(string, ...any)) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			report, err := reconcileLeaseTerminationsWithin(ctx, reconciler, timeout)
			if err != nil {
				logf("periodic lease termination reconciliation failed: %v", err)
				continue
			}
			if report.Examined != 0 {
				logf("periodic lease termination reconciliation examined=%d begun=%d completed=%d", report.Examined, report.Begun, report.Completed)
			}
		}
	}
}

func logPhysicalReconciliation(report orchestration.PhysicalReconciliationReport) {
	targetExpected, targetUnclaimed, targetConflicts, removedTargets := 0, 0, 0, 0
	for kind, item := range report.Targets {
		targetExpected += len(item.Expected)
		targetUnclaimed += len(item.Unclaimed)
		targetConflicts += len(item.Conflicts)
		removedTargets += len(report.RemovedTargetOrphans[kind])
	}
	log.Printf("startup physical reconciliation agent_expected=%d agent_unclaimed=%d agent_conflicts=%d agent_orphans_removed=%d target_expected=%d target_unclaimed=%d target_conflicts=%d target_orphans_removed=%d interrupted_runs_failed=%d target_operations_lost=%d",
		len(report.Agent.Expected), len(report.Agent.Unclaimed), len(report.Agent.Conflicts), len(report.RemovedAgentOrphans),
		targetExpected, targetUnclaimed, targetConflicts, removedTargets, len(report.RecoveredRuns), len(report.LostTargetOperations))
}
