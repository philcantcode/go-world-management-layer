package application

import "github.com/philcantcode/go-world-management-layer/internal/domain"

func invalidArgument(operation, field, message string, cause error) error {
	return domain.NewError(domain.CodeInvalidArgument, operation, field, message, cause)
}

func failedPrecondition(operation, field, message string, cause error) error {
	return domain.NewError(domain.CodeFailedPrecondition, operation, field, message, cause)
}

func resourceExhausted(operation, field, message string, cause error) error {
	return domain.NewError(domain.CodeResourceExhausted, operation, field, message, cause)
}

func integrityViolation(operation, field, message string, cause error) error {
	return domain.NewError(domain.CodeIntegrityViolation, operation, field, message, cause)
}
