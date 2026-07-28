package domain

import (
	"errors"
	"fmt"
)

// ErrorCode is a stable, transport-neutral failure classification.
type ErrorCode string

const (
	CodeInvalidArgument       ErrorCode = "invalid_argument"
	CodeInvalidID             ErrorCode = "invalid_id"
	CodeInvalidState          ErrorCode = "invalid_state"
	CodeInvalidTransition     ErrorCode = "invalid_transition"
	CodeFailedPrecondition    ErrorCode = "failed_precondition"
	CodeNotFound              ErrorCode = "not_found"
	CodeAlreadyExists         ErrorCode = "already_exists"
	CodeConflict              ErrorCode = "conflict"
	CodeStaleRevision         ErrorCode = "stale_revision"
	CodeUnauthorized          ErrorCode = "unauthorized"
	CodeForbidden             ErrorCode = "forbidden"
	CodeDeadlineExceeded      ErrorCode = "deadline_exceeded"
	CodeResourceExhausted     ErrorCode = "resource_exhausted"
	CodeCapabilityUnavailable ErrorCode = "capability_unavailable"
	CodeIntegrityViolation    ErrorCode = "integrity_violation"
	CodeUnavailable           ErrorCode = "unavailable"
	CodeInternal              ErrorCode = "internal"
)

// Error carries structured details without exposing mutable internal state.
type Error struct {
	code    ErrorCode
	op      string
	field   string
	message string
	details map[string]string
	cause   error
}

func NewError(code ErrorCode, op, field, message string, cause error) *Error {
	return &Error{code: code, op: op, field: field, message: message, cause: cause}
}

func NewDetailedError(code ErrorCode, op, field, message string, details map[string]string, cause error) *Error {
	return &Error{code: code, op: op, field: field, message: message, details: cloneMap(details), cause: cause}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	location := e.op
	if e.field != "" {
		if location != "" {
			location += "."
		}
		location += e.field
	}
	if location == "" {
		location = string(e.code)
	}
	if e.message == "" {
		return location
	}
	return fmt.Sprintf("%s: %s", location, e.message)
}

func (e *Error) Unwrap() error { return e.cause }
func (e *Error) Code() ErrorCode {
	if e == nil {
		return ""
	}
	return e.code
}
func (e *Error) Operation() string {
	if e == nil {
		return ""
	}
	return e.op
}
func (e *Error) Field() string {
	if e == nil {
		return ""
	}
	return e.field
}
func (e *Error) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}
func (e *Error) Details() map[string]string {
	if e == nil {
		return nil
	}
	return cloneMap(e.details)
}

func ErrorCodeOf(err error) ErrorCode {
	var domainErr *Error
	if errors.As(err, &domainErr) {
		return domainErr.Code()
	}
	return CodeInternal
}

func IsCode(err error, code ErrorCode) bool {
	return err != nil && ErrorCodeOf(err) == code
}
