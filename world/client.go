// Package world is the stable Go embed API for the versioned world.v1 contract.
//
// Integration: world.Open(Config) → *Manager. There is no remote Dial or
// dual-daemon control-plane product.
package world

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ResearchSessionView = worldv1.ResearchSessionView
	Lease               = worldv1.Lease
	Target              = worldv1.Target
	TargetRun           = worldv1.TargetRun
	Incident            = worldv1.Incident
	ObservationBundle   = worldv1.ObservationBundle
)

// NewMutation creates unique idempotency and correlation identities and an
// explicit absolute deadline for a public mutation.
func NewMutation(authorizedPolicyReference string, deadline time.Time) (*worldv1.MutationMetadata, error) {
	if strings.TrimSpace(authorizedPolicyReference) == "" || deadline.IsZero() {
		return nil, fmt.Errorf("authorized policy reference and deadline are required")
	}
	correlation, err := domain.NewCorrelationID()
	if err != nil {
		return nil, err
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, err
	}
	protobufDeadline := timestamppb.New(deadline.UTC())
	if err := protobufDeadline.CheckValid(); err != nil {
		return nil, fmt.Errorf("mutation deadline: %w", err)
	}
	return &worldv1.MutationMetadata{IdempotencyKey: "idem_" + hex.EncodeToString(entropy[:]), CorrelationId: correlation.String(), AuthorizedPolicyReference: authorizedPolicyReference, Deadline: protobufDeadline}, nil
}

func mutationOf[Request any](request *Request, get func(*Request) *worldv1.MutationMetadata) *worldv1.MutationMetadata {
	if request == nil {
		return nil
	}
	return get(request)
}

func defensiveCopy[Value any](value *Value) (*Value, error) {
	if value == nil {
		return nil, nil
	}
	message, ok := any(value).(proto.Message)
	if !ok {
		return nil, fmt.Errorf("copy world response: %T is not a protobuf message", value)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("copy world response: %w", err)
	}
	copy := new(Value)
	copyMessage, ok := any(copy).(proto.Message)
	if !ok {
		return nil, fmt.Errorf("copy world response: %T is not a protobuf message", copy)
	}
	if err := proto.Unmarshal(payload, copyMessage); err != nil {
		return nil, fmt.Errorf("copy world response: %w", err)
	}
	return copy, nil
}
