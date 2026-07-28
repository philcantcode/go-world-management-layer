package domain

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

func requireNonBlank(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return NewError(CodeInvalidArgument, "validate", field, "must not be blank", nil)
	}
	return nil
}

func requireTime(field string, value time.Time) error {
	if value.IsZero() {
		return NewError(CodeInvalidArgument, "validate", field, "must be set", nil)
	}
	return nil
}

func requireNonNegative(field string, value int64) error {
	if value < 0 {
		return NewError(CodeInvalidArgument, "validate", field, "must not be negative", nil)
	}
	return nil
}

type zeroChecker interface{ IsZero() bool }

func requireID(field string, value zeroChecker) error {
	if value.IsZero() {
		return NewError(CodeInvalidID, "validate", field, "must be set", nil)
	}
	return nil
}

func nextModelRevision(current, expected Revision, updatedAt, at time.Time) (Revision, error) {
	if err := RequireRevision(current, expected); err != nil {
		return 0, err
	}
	if err := requireTime("at", at); err != nil {
		return 0, err
	}
	if at.Before(updatedAt) {
		return 0, NewError(CodeInvalidArgument, "model.transition", "at", "must not precede the previous update", nil)
	}
	return current.Next()
}

func requireRelativePath(field, value string, allowDot bool) error {
	if err := requireNonBlank(field, value); err != nil {
		return err
	}
	if strings.ContainsRune(value, '\x00') {
		return NewError(CodeInvalidArgument, "validate", field, "must not contain NUL", nil)
	}
	if strings.Contains(value, "\\") {
		return NewError(CodeInvalidArgument, "validate", field, "must use forward slashes", nil)
	}
	if strings.HasPrefix(value, "/") || path.IsAbs(value) {
		return NewError(CodeInvalidArgument, "validate", field, "must be relative", nil)
	}
	cleaned := path.Clean(value)
	if cleaned != value || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return NewError(CodeInvalidArgument, "validate", field, "must be a normalized beneath path", nil)
	}
	if !allowDot && cleaned == "." {
		return NewError(CodeInvalidArgument, "validate", field, "must name an entry", nil)
	}
	return nil
}

func requireOrderedTimes(startField string, start time.Time, endField string, end time.Time) error {
	if err := requireTime(startField, start); err != nil {
		return err
	}
	if err := requireTime(endField, end); err != nil {
		return err
	}
	if end.Before(start) {
		return NewError(CodeInvalidArgument, "validate", endField, fmt.Sprintf("must not precede %s", startField), nil)
	}
	return nil
}

func cloneSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	result := make([]T, len(values))
	copy(result, values)
	return result
}

func cloneMap[K comparable, V any](values map[K]V) map[K]V {
	if values == nil {
		return nil
	}
	result := make(map[K]V, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func uniqueNonBlank(values []string, field string) ([]string, error) {
	result := cloneSlice(values)
	seen := make(map[string]struct{}, len(result))
	for i, value := range result {
		if err := requireNonBlank(fmt.Sprintf("%s[%d]", field, i), value); err != nil {
			return nil, err
		}
		if _, exists := seen[value]; exists {
			return nil, NewError(CodeInvalidArgument, "validate", field, "must not contain duplicates", nil)
		}
		seen[value] = struct{}{}
	}
	return result, nil
}
