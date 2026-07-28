package orchestration

import "github.com/philcantcode/go-world-management-layer/internal/domain"

// requireStoredID turns malformed identifiers read from durable projections
// into a typed integrity failure. Persisted state is never allowed to panic an
// RPC handler or physical cleanup path.
func requireStoredID[T any](operation, field, value string, parse func(string) (T, error)) (T, error) {
	id, err := parse(value)
	if err != nil {
		var zero T
		return zero, domain.NewError(domain.CodeIntegrityViolation, operation, field, "persisted identifier is invalid", err)
	}
	return id, nil
}
