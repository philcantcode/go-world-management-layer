package rpc

import (
	"context"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testRPCContext() context.Context {
	return context.WithValue(context.Background(), identityKey{}, Identity{Subject: "rpc-test", Method: "test"})
}

func testWireMutation() *worldv1.MutationMetadata {
	return &worldv1.MutationMetadata{
		IdempotencyKey:            "rpc-test",
		CorrelationId:             "correlation-test",
		AuthorizedPolicyReference: "sha256:policy",
		Deadline:                  timestamppb.New(time.Now().Add(time.Minute)),
	}
}
