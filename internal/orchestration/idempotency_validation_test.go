package orchestration

import (
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestStateEventIdempotencyPreflightRejectsMalformedKeys(t *testing.T) {
	valid := stateEvent{
		IdempotencyKey: "event-key",
		Reservation:    &bundleReservation{IdempotencyKey: domain.DeriveIdempotencyKey("event-key", "bundle")},
		Operation:      &operationReservation{IdempotencyKey: domain.DeriveIdempotencyKey("event-key", "operation")},
	}
	if err := validateStateEventIdempotency(valid); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	for name, mutate := range map[string]func(*stateEvent){
		"whitespace envelope": func(event *stateEvent) { event.IdempotencyKey = " invalid" },
		"oversized envelope": func(event *stateEvent) {
			event.IdempotencyKey = strings.Repeat("k", domain.MaximumIdempotencyKeyBytes+1)
		},
		"malformed bundle child":    func(event *stateEvent) { event.Reservation.IdempotencyKey = "invalid " },
		"malformed operation child": func(event *stateEvent) { event.Operation.IdempotencyKey = " invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			event := valid
			reservation, operation := *valid.Reservation, *valid.Operation
			event.Reservation, event.Operation = &reservation, &operation
			mutate(&event)
			if err := validateStateEventIdempotency(event); err == nil {
				t.Fatal("malformed event passed preflight")
			}
		})
	}
	if err := validateStateEventIdempotency(stateEvent{Kind: "bundle.completed", Completion: &bundleCompletion{}}); err != nil {
		t.Fatalf("non-idempotent completion event rejected: %v", err)
	}
}
