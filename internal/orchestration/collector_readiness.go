package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// LedgerCollectorReadiness turns durable collector coverage records into the
// readiness gate required by target drivers. A target run is allowed to start
// only after every requested signal family has at least one current, gap-free
// collector reporting a ready state for that exact run.
type LedgerCollectorReadiness struct {
	ledger *ledger.Ledger
}

func NewLedgerCollectorReadiness(observations *ledger.Ledger) (*LedgerCollectorReadiness, error) {
	if observations == nil {
		return nil, fmt.Errorf("observation ledger is required")
	}
	return &LedgerCollectorReadiness{ledger: observations}, nil
}

func (r *LedgerCollectorReadiness) AwaitReady(ctx context.Context, runID domain.TargetRunID, requirements []ports.ObservationRequirement) error {
	required, err := normalizedRequirements(requirements)
	if err != nil {
		return err
	}
	if len(required) == 0 {
		return nil
	}
	states := make(map[string]map[string]coverageReadiness, len(required))
	cursor := ledger.Cursor(0)
	applyFrom := func(after ledger.Cursor) error {
		records, readErr := r.ledger.ReadAfter(after, 0)
		if readErr != nil {
			return readErr
		}
		for _, record := range records {
			if err := applyCoverageRecord(states, required, runID.String(), record); err != nil {
				return err
			}
			cursor = record.Cursor
		}
		return nil
	}
	if err := applyFrom(0); err != nil {
		return err
	}
	if coverageReady(states, required) {
		return nil
	}
	subscription, err := r.ledger.Subscribe(cursor, 128)
	if err != nil {
		return err
	}
	defer subscription.Close()
	for {
		delivery, nextErr := subscription.Next(ctx)
		if nextErr != nil {
			return nextErr
		}
		if delivery.Gap != nil {
			if err := applyFrom(cursor); err != nil {
				return err
			}
		} else if delivery.Record != nil {
			if err := applyCoverageRecord(states, required, runID.String(), *delivery.Record); err != nil {
				return err
			}
			cursor = delivery.Record.Cursor
		}
		if coverageReady(states, required) {
			return nil
		}
	}
}

func normalizedRequirements(values []ports.ObservationRequirement) (map[string][]ports.ObservationRequirement, error) {
	result := make(map[string][]ports.ObservationRequirement, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return nil, err
		}
		if !value.Required {
			continue
		}
		result[value.SignalFamily] = append(result[value.SignalFamily], value)
	}
	return result, nil
}

type coverageReadiness struct {
	placement domain.CollectorPlacement
	level     domain.CoverageLevel
	required  bool
	ready     bool
}

func applyCoverageRecord(states map[string]map[string]coverageReadiness, required map[string][]ports.ObservationRequirement, runID string, record ledger.Record) error {
	if record.Kind != ledger.RecordCoverage || record.Identity.TargetRunID != runID {
		return nil
	}
	if _, wanted := required[record.SignalFamily]; !wanted {
		return nil
	}
	var coverage worldv1.CollectorCoverage
	if err := json.Unmarshal(record.Payload, &coverage); err != nil {
		return fmt.Errorf("decode collector coverage at cursor %d: %w", record.Cursor, err)
	}
	collectorID := strings.TrimSpace(coverage.CollectorId)
	if collectorID == "" {
		collectorID = strings.TrimSpace(record.Collector.ID)
	}
	if collectorID == "" {
		return fmt.Errorf("collector coverage at cursor %d has no collector identity", record.Cursor)
	}
	if states[record.SignalFamily] == nil {
		states[record.SignalFamily] = make(map[string]coverageReadiness)
	}
	status := domain.CoverageStatus(strings.ToLower(strings.TrimSpace(coverage.Status)))
	states[record.SignalFamily][collectorID] = coverageReadiness{
		placement: domain.CollectorPlacement(coverage.Placement), level: domain.CoverageLevel(coverage.Level),
		required: coverage.Required,
		ready:    status == domain.CoverageAvailable && coverage.Gap == nil && coverage.DroppedRecords == 0,
	}
	return nil
}

func coverageReady(states map[string]map[string]coverageReadiness, required map[string][]ports.ObservationRequirement) bool {
	for family, requirements := range required {
		for _, requirement := range requirements {
			ready := false
			for _, current := range states[family] {
				ready = ready || current.ready && current.required && current.placement == requirement.Placement && coverageLevelRank(current.level) >= coverageLevelRank(requirement.MinimumLevel)
			}
			if !ready {
				return false
			}
		}
	}
	return true
}
