package rpc

import (
	"context"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type leaseProjectionCoreStub struct {
	Core
	view application.ResearchSessionView
}

func (*leaseProjectionCoreStub) Authorize(context.Context, application.AuthorizationRequest) error {
	return nil
}

func (s *leaseProjectionCoreStub) GetResearchSession(context.Context, string) (application.ResearchSessionView, error) {
	return s.view, nil
}

func TestGetResearchSessionProjectsExpiringLeaseAsClosed(t *testing.T) {
	initiated := time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC)
	core := &leaseProjectionCoreStub{view: application.ResearchSessionView{
		Session: application.SessionRecord{ID: "session_1", State: domain.ResearchSessionReleasing},
		Lease: application.LeaseRecord{
			ID: "lease_1", SessionID: "session_1", State: domain.LeaseActive, Revision: 2,
			ExpiresAt: initiated, CreatedAt: initiated.Add(-time.Hour), UpdatedAt: initiated,
			Termination: application.LeaseTerminationRecord{
				Kind: application.LeaseTerminationExpiry, State: application.LeaseTerminationExpiring,
				Reason: "lease lifetime elapsed", BeginIdempotencyKey: "expiry/lease_1",
				BeginRequestDigest:     domain.NewDigest([]byte("begin")).String(),
				InitiatedLeaseRevision: 2, InitiatedAt: initiated,
			},
		},
	}}
	server, err := NewWorldServer(core, ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	view, err := server.GetResearchSession(testRPCContext(), &worldv1.GetResearchSessionRequest{ResearchSessionId: "session_1"})
	if err != nil {
		t.Fatal(err)
	}
	lease := view.GetLease()
	if lease.GetState() != "expiring" {
		t.Fatalf("wire lease state = %q, want expiring; underlying domain state must not leak as active", lease.GetState())
	}
	termination := lease.GetTermination()
	if termination == nil {
		t.Fatal("wire lease omitted durable termination")
	}
	if termination.GetKind() != "expiry" || termination.GetState() != "expiring" ||
		termination.GetReason() != "lease lifetime elapsed" || termination.GetBeginIdempotencyKey() != "expiry/lease_1" ||
		termination.GetBeginRequestDigest() != core.view.Lease.Termination.BeginRequestDigest || termination.GetInitiatedLeaseRevision() != 2 ||
		!termination.GetInitiatedAt().AsTime().Equal(initiated) || termination.GetCompletedAt() != nil {
		t.Fatalf("wire termination = %#v", termination)
	}
}

func TestLeaseProjectionIncludesTerminalCompletionIdentity(t *testing.T) {
	initiated := time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC)
	completed := initiated.Add(time.Minute)
	mapped := lease(application.LeaseRecord{
		State: domain.LeaseExpired,
		Termination: application.LeaseTerminationRecord{
			Kind: application.LeaseTerminationExpiry, State: application.LeaseTerminationExpired,
			Reason: "lease lifetime elapsed", BeginIdempotencyKey: "expiry/lease_1",
			BeginRequestDigest: domain.NewDigest([]byte("begin")).String(), InitiatedLeaseRevision: 2, InitiatedAt: initiated,
			CompleteIdempotencyKey: "termination/lease_1", CompleteRequestDigest: domain.NewDigest([]byte("complete")).String(), CompletedAt: completed,
		},
	})
	if mapped.GetState() != "expired" || mapped.GetTermination().GetCompleteIdempotencyKey() != "termination/lease_1" ||
		mapped.GetTermination().GetCompleteRequestDigest() == "" || !mapped.GetTermination().GetCompletedAt().AsTime().Equal(completed) {
		t.Fatalf("terminal lease mapping = %#v", mapped)
	}
}
