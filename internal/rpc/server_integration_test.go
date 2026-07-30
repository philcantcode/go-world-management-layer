package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testMutation(t *testing.T, policy string) *worldv1.MutationMetadata {
	t.Helper()
	correlation, err := domain.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	return &worldv1.MutationMetadata{
		IdempotencyKey:            "idem_" + hex.EncodeToString(entropy[:]),
		CorrelationId:             correlation.String(),
		AuthorizedPolicyReference: policy,
		Deadline:                  timestamppb.New(time.Now().Add(time.Minute).UTC()),
	}
}

func TestInProcessAuthIdempotencyAndErrorMapping(t *testing.T) {
	ctx := context.Background()
	controlStore, err := store.Open(ctx, store.Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	core, err := application.NewCore(ctx, application.CoreOptions{Store: controlStore})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewWorldServer(core, ServerOptions{TrustedNodeSubjects: map[string]bool{"integration-test": true}})
	if err != nil {
		t.Fatal(err)
	}

	authorized := ContextWithIdentity(ctx, Identity{Subject: "integration-test", Method: "local_embed"})
	policy := domain.NewDigest([]byte("policy")).String()
	capabilities := domain.NewDigest([]byte("capabilities")).String()
	meta := testMutation(t, policy)
	request := &worldv1.AcquireResearchSessionRequest{
		Mutation:     meta,
		InputView:    &worldv1.InputViewSpec{ResolvedInputViewId: domain.NewInputViewID([]byte("manifest")).String()},
		PolicyDigest: policy, CapabilityDigest: capabilities, Ttl: durationpb.New(time.Hour),
	}
	first, err := server.AcquireResearchSession(authorized, request)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := server.AcquireResearchSession(authorized, request)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if first.View.Session.ResearchSessionId != second.View.Session.ResearchSessionId {
		t.Fatalf("replay returned a different session: %s != %s", first.View.Session.ResearchSessionId, second.View.Session.ResearchSessionId)
	}

	agent := first.View.AgentWorkspace
	generation := agent.Generations[0]
	for _, state := range []string{"booting", "ready"} {
		transitionMeta := testMutation(t, policy)
		agent, err = server.TransitionAgentGeneration(authorized, &worldv1.TransitionAgentGenerationRequest{Mutation: transitionMeta, AgentWorkspaceId: agent.AgentWorkspaceId, Generation: generation.Generation, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			t.Fatalf("transition agent to %s: %v", state, err)
		}
		generation = agent.Generations[0]
	}
	execMeta := testMutation(t, policy)
	execution, err := server.CreateExec(authorized, &worldv1.CreateExecRequest{Mutation: execMeta, LeaseId: first.Lease.LeaseId, Kind: "provider", ProviderExecutable: "provider-cli", Argv: []string{"--version"}, WorkspaceRelativeWorkingDirectory: "."})
	if err != nil {
		t.Fatalf("create exec: %v", err)
	}
	for _, state := range []string{"starting", "running"} {
		transitionMeta := testMutation(t, policy)
		execution, err = server.TransitionExec(authorized, &worldv1.TransitionExecRequest{Mutation: transitionMeta, ExecId: execution.ExecId, ExpectedRevision: execution.Revision, State: state})
		if err != nil {
			t.Fatalf("transition exec to %s: %v", state, err)
		}
	}
	zero := int32(0)
	finalizeMeta := testMutation(t, policy)
	execution, err = server.FinalizeExec(authorized, &worldv1.FinalizeExecRequest{Mutation: finalizeMeta, ExecId: execution.ExecId, ExpectedRevision: execution.Revision, State: "completed", ExitCode: &zero, CleanupConfirmed: true})
	if err != nil {
		t.Fatalf("finalize exec: %v", err)
	}
	loadedExec, err := server.GetExec(authorized, &worldv1.GetExecRequest{ExecId: execution.ExecId})
	if err != nil || loadedExec.ExitCode == nil || *loadedExec.ExitCode != 0 || loadedExec.State != "completed" {
		t.Fatalf("get finalized exec: value=%#v err=%v", loadedExec, err)
	}
	updatedView, err := server.GetResearchSession(authorized, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId})
	if err != nil || len(updatedView.Execs) != 1 || updatedView.Execs[0].ExecId != execution.ExecId {
		t.Fatalf("session view exec projection: value=%#v err=%v", updatedView, err)
	}

	conflict := proto.Clone(request).(*worldv1.AcquireResearchSessionRequest)
	conflict.Ttl = durationpb.New(2 * time.Hour)
	if _, err := server.AcquireResearchSession(authorized, conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("idempotency input conflict code = %s, want %s (err=%v)", status.Code(err), codes.AlreadyExists, err)
	}

	renewMeta := testMutation(t, policy)
	_, err = server.RenewLease(authorized, &worldv1.RenewLeaseRequest{Mutation: renewMeta, LeaseId: first.Lease.LeaseId, ExpectedRevision: first.Lease.Revision + 10, Ttl: durationpb.New(time.Hour)})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("revision conflict code = %s, want %s (err=%v)", status.Code(err), codes.Aborted, err)
	}
	if _, err := server.GetResearchSession(authorized, &worldv1.GetResearchSessionRequest{ResearchSessionId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("not found code = %s, want %s (err=%v)", status.Code(err), codes.NotFound, err)
	}

	if _, err := server.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing identity code = %s, want %s (err=%v)", status.Code(err), codes.Unauthenticated, err)
	}
	if _, err := server.GetLiveSnapshot(authorized, &worldv1.GetLiveSnapshotRequest{Filter: &worldv1.ObservationFilter{LeaseId: first.Lease.LeaseId}}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unwired observation backend code = %s, want %s (err=%v)", status.Code(err), codes.Unimplemented, err)
	}

	otherOwner := ContextWithIdentity(ctx, Identity{Subject: "other-owner", Method: "local_embed"})
	if _, err := server.GetResearchSession(otherOwner, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-owner read code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
	otherAcquire := proto.Clone(request).(*worldv1.AcquireResearchSessionRequest)
	otherAcquire.Mutation = testMutation(t, policy)
	otherSession, err := server.AcquireResearchSession(otherOwner, otherAcquire)
	if err != nil {
		t.Fatalf("acquire non-node owner session: %v", err)
	}
	otherExecMeta := testMutation(t, policy)
	if _, err := server.CreateExec(otherOwner, &worldv1.CreateExecRequest{
		Mutation: otherExecMeta, LeaseId: otherSession.Lease.LeaseId, Kind: "provider",
		ProviderExecutable: "provider-cli", WorkspaceRelativeWorkingDirectory: ".",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-node create exec code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
	wrongPolicy := testMutation(t, domain.NewDigest([]byte("wrong-policy")).String())
	if _, err := server.RenewLease(authorized, &worldv1.RenewLeaseRequest{Mutation: wrongPolicy, LeaseId: first.Lease.LeaseId, ExpectedRevision: first.Lease.Revision, Ttl: durationpb.New(time.Hour)}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-policy mutation code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}
