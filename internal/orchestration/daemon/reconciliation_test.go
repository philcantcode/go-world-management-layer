package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
)

type leaseTerminationReconcilerFunc func(context.Context) (orchestration.LeaseTerminationScanReport, error)

func (f leaseTerminationReconcilerFunc) ReconcileLeaseTerminations(ctx context.Context) (orchestration.LeaseTerminationScanReport, error) {
	return f(ctx)
}

type startupReconcilerFunc struct {
	physical    func(context.Context) (orchestration.PhysicalReconciliationReport, error)
	termination func(context.Context) (orchestration.LeaseTerminationScanReport, error)
}

func (f startupReconcilerFunc) ReconcilePhysicalResources(ctx context.Context) (orchestration.PhysicalReconciliationReport, error) {
	return f.physical(ctx)
}

func (f startupReconcilerFunc) ReconcileLeaseTerminations(ctx context.Context) (orchestration.LeaseTerminationScanReport, error) {
	return f.termination(ctx)
}

func TestStartupReconciliationRecoversPhysicalRunsBeforeLeaseCleanup(t *testing.T) {
	var order []string
	reconciler := startupReconcilerFunc{
		physical: func(ctx context.Context) (orchestration.PhysicalReconciliationReport, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("physical startup reconciliation has no deadline")
			}
			order = append(order, "physical")
			return orchestration.PhysicalReconciliationReport{RecoveredRuns: []string{"run_interrupted"}}, nil
		},
		termination: func(ctx context.Context) (orchestration.LeaseTerminationScanReport, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("lease startup reconciliation has no deadline")
			}
			order = append(order, "lease")
			return orchestration.LeaseTerminationScanReport{Examined: 1, Completed: 1}, nil
		},
	}
	reports, err := reconcileStartupStateWithin(context.Background(), reconciler, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "physical,lease" || len(reports.Physical.RecoveredRuns) != 1 || reports.LeaseTermination.Completed != 1 {
		t.Fatalf("startup order=%v reports=%#v", order, reports)
	}
}

func TestStartupReconciliationFailsBeforeLeaseCleanupWhenPhysicalRecoveryIsUnproven(t *testing.T) {
	leaseCalled := false
	reconciler := startupReconcilerFunc{
		physical: func(context.Context) (orchestration.PhysicalReconciliationReport, error) {
			return orchestration.PhysicalReconciliationReport{}, errors.New("collector cleanup unproven")
		},
		termination: func(context.Context) (orchestration.LeaseTerminationScanReport, error) {
			leaseCalled = true
			return orchestration.LeaseTerminationScanReport{}, nil
		},
	}
	if _, err := reconcileStartupStateWithin(context.Background(), reconciler, time.Second); err == nil || !strings.Contains(err.Error(), "collector cleanup unproven") {
		t.Fatalf("startup recovery error = %v", err)
	}
	if leaseCalled {
		t.Fatal("lease cleanup ran before physical recovery was proven")
	}
}

func TestReconcileLeaseTerminationsWithinSuppliesBoundedContext(t *testing.T) {
	timeout := 250 * time.Millisecond
	reconciler := leaseTerminationReconcilerFunc(func(ctx context.Context) (orchestration.LeaseTerminationScanReport, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("reconciliation context has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > timeout+25*time.Millisecond {
			t.Fatalf("reconciliation deadline remaining = %s, timeout = %s", remaining, timeout)
		}
		return orchestration.LeaseTerminationScanReport{Examined: 2, Completed: 2}, nil
	})
	report, err := reconcileLeaseTerminationsWithin(context.Background(), reconciler, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if report.Examined != 2 || report.Completed != 2 {
		t.Fatalf("report = %#v", report)
	}
}

func TestRunLeaseTerminationTicksContinuesAfterFailureAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	calls := make(chan int, 2)
	var mu sync.Mutex
	callCount := 0
	reconciler := leaseTerminationReconcilerFunc(func(context.Context) (orchestration.LeaseTerminationScanReport, error) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		calls <- current
		if current == 1 {
			return orchestration.LeaseTerminationScanReport{}, errors.New("transient")
		}
		return orchestration.LeaseTerminationScanReport{Examined: 1, Completed: 1}, nil
	})
	logs := make(chan string, 2)
	done := make(chan struct{})
	go func() {
		runLeaseTerminationTicks(ctx, ticks, reconciler, time.Second, func(format string, values ...any) {
			logs <- fmt.Sprintf(format, values...)
		})
		close(done)
	}()

	ticks <- time.Now()
	if call := <-calls; call != 1 {
		t.Fatalf("first call = %d", call)
	}
	if message := <-logs; !strings.Contains(message, "failed") {
		t.Fatalf("first log = %q", message)
	}
	ticks <- time.Now()
	if call := <-calls; call != 2 {
		t.Fatalf("second call = %d", call)
	}
	if message := <-logs; !strings.Contains(message, "completed=1") {
		t.Fatalf("second log = %q", message)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ticker loop did not stop after cancellation")
	}
}

func TestDefaultConfigReadsControlAndReconciliationDurations(t *testing.T) {
	t.Setenv("WORLD_CONTROL_TIMEOUT", "12s")
	t.Setenv("WORLD_RECONCILIATION_INTERVAL", "45s")
	t.Setenv("WORLD_RECONCILIATION_TIMEOUT", "4s")
	configuration, err := defaultConfig(ModeController)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.controlTimeout != 12*time.Second || configuration.reconciliationInterval != 45*time.Second || configuration.reconciliationTimeout != 4*time.Second {
		t.Fatalf("control/reconciliation durations = %s/%s/%s", configuration.controlTimeout, configuration.reconciliationInterval, configuration.reconciliationTimeout)
	}

	t.Setenv("WORLD_CONTROL_TIMEOUT", "not-a-duration")
	if _, err := defaultConfig(ModeController); err == nil || !strings.Contains(err.Error(), "WORLD_CONTROL_TIMEOUT") {
		t.Fatalf("invalid control duration error = %v", err)
	}
	t.Setenv("WORLD_CONTROL_TIMEOUT", "12s")

	t.Setenv("WORLD_RECONCILIATION_TIMEOUT", "not-a-duration")
	if _, err := defaultConfig(ModeController); err == nil || !strings.Contains(err.Error(), "WORLD_RECONCILIATION_TIMEOUT") {
		t.Fatalf("invalid duration error = %v", err)
	}
}
