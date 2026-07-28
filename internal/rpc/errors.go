package rpc

import (
	"context"
	"errors"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusError is the single domain/store-to-gRPC classification point.
func StatusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, application.ErrNotFound), errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, application.ErrScope):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, store.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrRevisionConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, store.ErrIntegrity):
		return status.Error(codes.DataLoss, err.Error())
	}
	if code := domain.ErrorCodeOf(err); code != domain.CodeInternal {
		return status.Error(grpcCode(code), err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func grpcCode(code domain.ErrorCode) codes.Code {
	switch code {
	case domain.CodeInvalidArgument, domain.CodeInvalidID:
		return codes.InvalidArgument
	case domain.CodeInvalidState, domain.CodeInvalidTransition, domain.CodeFailedPrecondition, domain.CodeCapabilityUnavailable:
		return codes.FailedPrecondition
	case domain.CodeNotFound:
		return codes.NotFound
	case domain.CodeAlreadyExists:
		return codes.AlreadyExists
	case domain.CodeConflict, domain.CodeStaleRevision:
		return codes.Aborted
	case domain.CodeUnauthorized:
		return codes.Unauthenticated
	case domain.CodeForbidden:
		return codes.PermissionDenied
	case domain.CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	case domain.CodeResourceExhausted:
		return codes.ResourceExhausted
	case domain.CodeIntegrityViolation:
		return codes.DataLoss
	case domain.CodeUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
