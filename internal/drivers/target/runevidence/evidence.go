package runevidence

import (
	"encoding/json"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// Cause identifies why a target driver cannot truthfully claim required
// collector coverage for a run it prepared.
type Cause string

const (
	CauseCollectorEvidenceUnavailable Cause = "collector-evidence-unavailable"
	CauseNeverStarted                 Cause = "never-started"
	CauseDurationExceeded             Cause = "duration-exceeded"
)

// AtOrAfter normalizes an observed lifecycle time without moving it before
// the model time that established the lifecycle boundary.
func AtOrAfter(observed, minimum time.Time) time.Time {
	observed = observed.UTC()
	minimum = minimum.UTC()
	if observed.IsZero() || observed.Before(minimum) {
		return minimum
	}
	return observed
}

// CloneStopReceipt returns an independently owned target-driver receipt for
// safe idempotent replay before orchestration assigns ledger cursors.
func CloneStopReceipt(receipt ports.TargetRunStopReceipt) ports.TargetRunStopReceipt {
	receipt.Observations = append([]ports.TargetRunObservation(nil), receipt.Observations...)
	for index := range receipt.Observations {
		receipt.Observations[index].Payload = append(json.RawMessage(nil), receipt.Observations[index].Payload...)
	}
	return receipt
}

// ClonePlan protects the driver's prepared-run record from caller mutation.
func ClonePlan(plan ports.TargetRunPlan) ports.TargetRunPlan {
	plan.RequiredCoverage = append([]string(nil), plan.RequiredCoverage...)
	plan.Collectors = append([]ports.CollectorSpec(nil), plan.Collectors...)
	plan.Material = append([]ports.TargetMaterialPlan(nil), plan.Material...)
	return plan
}

// ClonePrepared returns a prepared-run receipt whose coverage slice cannot
// mutate the driver's internal lifecycle record.
func ClonePrepared(prepared ports.PreparedTargetRun) ports.PreparedTargetRun {
	prepared.RequiredCoverage = append([]string(nil), prepared.RequiredCoverage...)
	return prepared
}
