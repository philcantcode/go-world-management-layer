package rpc

import (
	"errors"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusErrorUsesTypedClassificationOnly(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "invalid argument", err: domain.NewError(domain.CodeInvalidArgument, "test", "field", "bad", nil), want: codes.InvalidArgument},
		{name: "failed precondition", err: domain.NewError(domain.CodeFailedPrecondition, "test", "state", "bad", nil), want: codes.FailedPrecondition},
		{name: "resource exhausted", err: domain.NewError(domain.CodeResourceExhausted, "test", "bytes", "too large", nil), want: codes.ResourceExhausted},
		{name: "wrapped typed", err: errors.Join(errors.New("context"), domain.NewError(domain.CodeIntegrityViolation, "test", "record", "corrupt", nil)), want: codes.DataLoss},
		{name: "wrapped policy denial", err: errors.Join(errors.New("admission"), policyauthority.ErrPolicyDenied), want: codes.PermissionDenied},
		{name: "structured policy violation", err: &policyauthority.Violation{Field: "target.reset.mode", Reason: "does not match"}, want: codes.PermissionDenied},
		{name: "english is not an api", err: errors.New("field is required and state cannot proceed"), want: codes.Internal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := status.Code(StatusError(test.err)); got != test.want {
				t.Fatalf("code = %s, want %s", got, test.want)
			}
		})
	}
}
