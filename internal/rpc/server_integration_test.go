package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/world"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestBufconnAuthIdempotencyAndErrorMapping(t *testing.T) {
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
	server, err := NewServer(core, ServerOptions{Authenticator: BearerOrMTLSAuthenticator{BearerSubjects: map[string]string{"test-token": "integration-test", "other-token": "other-owner"}}, TrustedNodeSubjects: map[string]bool{"integration-test": true}})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	go func() { _ = server.Serve(listener) }()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	authorized, err := world.Dial(world.DialOptions{Dialer: dialer, BearerToken: "test-token", DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authorized.Close() })

	policy := domain.NewDigest([]byte("policy")).String()
	capabilities := domain.NewDigest([]byte("capabilities")).String()
	meta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	request := &worldv1.AcquireResearchSessionRequest{
		Mutation:     meta,
		InputView:    &worldv1.InputViewSpec{ResolvedInputViewId: domain.NewInputViewID([]byte("manifest")).String()},
		PolicyDigest: policy, CapabilityDigest: capabilities, Ttl: durationpb.New(time.Hour),
	}
	first, err := authorized.AcquireResearchSession(ctx, request)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := authorized.AcquireResearchSession(ctx, request)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if first.View.Session.ResearchSessionId != second.View.Session.ResearchSessionId {
		t.Fatalf("replay returned a different session: %s != %s", first.View.Session.ResearchSessionId, second.View.Session.ResearchSessionId)
	}

	agent := first.View.AgentWorkspace
	generation := agent.Generations[0]
	for _, state := range []string{"booting", "ready"} {
		transitionMeta, metaErr := world.NewMutation(policy, time.Now().Add(time.Minute))
		if metaErr != nil {
			t.Fatal(metaErr)
		}
		agent, err = authorized.TransitionAgentGeneration(ctx, &worldv1.TransitionAgentGenerationRequest{Mutation: transitionMeta, AgentWorkspaceId: agent.AgentWorkspaceId, Generation: generation.Generation, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			t.Fatalf("transition agent to %s: %v", state, err)
		}
		generation = agent.Generations[0]
	}
	execMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	execution, err := authorized.CreateExec(ctx, &worldv1.CreateExecRequest{Mutation: execMeta, LeaseId: first.Lease.LeaseId, Kind: "provider", ProviderExecutable: "provider-cli", Argv: []string{"--version"}, WorkspaceRelativeWorkingDirectory: "."})
	if err != nil {
		t.Fatalf("create exec: %v", err)
	}
	for _, state := range []string{"starting", "running"} {
		transitionMeta, metaErr := world.NewMutation(policy, time.Now().Add(time.Minute))
		if metaErr != nil {
			t.Fatal(metaErr)
		}
		execution, err = authorized.TransitionExec(ctx, &worldv1.TransitionExecRequest{Mutation: transitionMeta, ExecId: execution.ExecId, ExpectedRevision: execution.Revision, State: state})
		if err != nil {
			t.Fatalf("transition exec to %s: %v", state, err)
		}
	}
	zero := int32(0)
	finalizeMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	execution, err = authorized.FinalizeExec(ctx, &worldv1.FinalizeExecRequest{Mutation: finalizeMeta, ExecId: execution.ExecId, ExpectedRevision: execution.Revision, State: "completed", ExitCode: &zero, CleanupConfirmed: true})
	if err != nil {
		t.Fatalf("finalize exec: %v", err)
	}
	loadedExec, err := authorized.GetExec(ctx, &worldv1.GetExecRequest{ExecId: execution.ExecId})
	if err != nil || loadedExec.ExitCode == nil || *loadedExec.ExitCode != 0 || loadedExec.State != "completed" {
		t.Fatalf("get finalized exec: value=%#v err=%v", loadedExec, err)
	}
	updatedView, err := authorized.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId})
	if err != nil || len(updatedView.Execs) != 1 || updatedView.Execs[0].ExecId != execution.ExecId {
		t.Fatalf("session view exec projection: value=%#v err=%v", updatedView, err)
	}

	conflict := proto.Clone(request).(*worldv1.AcquireResearchSessionRequest)
	conflict.Ttl = durationpb.New(2 * time.Hour)
	if _, err := authorized.AcquireResearchSession(ctx, conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("idempotency input conflict code = %s, want %s (err=%v)", status.Code(err), codes.AlreadyExists, err)
	}

	renewMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, err = authorized.RenewLease(ctx, &worldv1.RenewLeaseRequest{Mutation: renewMeta, LeaseId: first.Lease.LeaseId, ExpectedRevision: first.Lease.Revision + 10, Ttl: durationpb.New(time.Hour)})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("revision conflict code = %s, want %s (err=%v)", status.Code(err), codes.Aborted, err)
	}
	if _, err := authorized.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("not found code = %s, want %s (err=%v)", status.Code(err), codes.NotFound, err)
	}

	unauthorized, err := world.Dial(world.DialOptions{Dialer: dialer, DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unauthorized.Close() })
	if _, err := unauthorized.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing auth code = %s, want %s (err=%v)", status.Code(err), codes.Unauthenticated, err)
	}
	if _, err := authorized.GetLiveSnapshot(ctx, &worldv1.GetLiveSnapshotRequest{Filter: &worldv1.ObservationFilter{LeaseId: first.Lease.LeaseId}}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unwired observation backend code = %s, want %s (err=%v)", status.Code(err), codes.Unimplemented, err)
	}

	otherOwner, err := world.Dial(world.DialOptions{Dialer: dialer, BearerToken: "other-token", DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherOwner.Close() })
	if _, err := otherOwner.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: first.View.Session.ResearchSessionId}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-owner read code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
	otherAcquire := proto.Clone(request).(*worldv1.AcquireResearchSessionRequest)
	otherAcquire.Mutation, err = world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	otherSession, err := otherOwner.AcquireResearchSession(ctx, otherAcquire)
	if err != nil {
		t.Fatalf("acquire non-node owner session: %v", err)
	}
	otherExecMeta, err := world.NewMutation(policy, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherOwner.CreateExec(ctx, &worldv1.CreateExecRequest{
		Mutation: otherExecMeta, LeaseId: otherSession.Lease.LeaseId, Kind: "provider",
		ProviderExecutable: "provider-cli", WorkspaceRelativeWorkingDirectory: ".",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-node create exec code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
	wrongPolicy, err := world.NewMutation(domain.NewDigest([]byte("wrong-policy")).String(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.RenewLease(ctx, &worldv1.RenewLeaseRequest{Mutation: wrongPolicy, LeaseId: first.Lease.LeaseId, ExpectedRevision: first.Lease.Revision, Ttl: durationpb.New(time.Hour)}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-policy mutation code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}
