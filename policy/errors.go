package policy

import (
	"fmt"
	"strings"
)

// FieldError describes one deterministic policy problem.
type FieldError struct {
	Path    string
	Message string
	Line    int
	Column  int
}

func (e FieldError) Error() string {
	location := e.Path
	if e.Line > 0 {
		location = fmt.Sprintf("%s (line %d, column %d)", location, e.Line, e.Column)
	}
	return location + ": " + e.Message
}

// ValidationError contains all independently detectable policy problems in
// deterministic traversal order.
type ValidationError struct {
	Problems []FieldError
}

func (e *ValidationError) Error() string {
	if e == nil || len(e.Problems) == 0 {
		return "invalid policy"
	}
	var builder strings.Builder
	builder.WriteString("invalid policy: ")
	for index, problem := range e.Problems {
		if index > 0 {
			builder.WriteString("; ")
		}
		builder.WriteString(problem.Error())
	}
	return builder.String()
}

func (e *ValidationError) Unwrap() error { return nil }

type sourcePosition struct {
	line   int
	column int
}

type validationCollector struct {
	positions map[string]sourcePosition
	problems  []FieldError
}

func newValidationCollector(positions map[string]sourcePosition) *validationCollector {
	return &validationCollector{positions: positions}
}

func (v *validationCollector) add(path, format string, args ...any) {
	position := v.positions[path]
	v.problems = append(v.problems, FieldError{
		Path: path, Message: fmt.Sprintf(format, args...),
		Line: position.line, Column: position.column,
	})
}

func (v *validationCollector) err() error {
	if len(v.problems) == 0 {
		return nil
	}
	problems := make([]FieldError, len(v.problems))
	copy(problems, v.problems)
	return &ValidationError{Problems: problems}
}
