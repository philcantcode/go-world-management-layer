package rpc

import (
	"context"
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type execStateCoreStub struct {
	Core
	calls int
}

func (s *execStateCoreStub) Authorize(context.Context, application.AuthorizationRequest) error {
	return nil
}

func (s *execStateCoreStub) CreateExec(context.Context, application.CreateExecRequest) (application.ExecRecord, error) {
	s.calls++
	return application.ExecRecord{}, nil
}

func (s *execStateCoreStub) TransitionExec(context.Context, application.TransitionExecRequest) (application.ExecRecord, error) {
	s.calls++
	return application.ExecRecord{}, nil
}

func (s *execStateCoreStub) FinalizeExec(context.Context, application.FinalizeExecRequest) (application.ExecRecord, error) {
	s.calls++
	return application.ExecRecord{}, nil
}

func TestExecStateMutationRequiresTrustedNodeIdentity(t *testing.T) {
	core := &execStateCoreStub{}
	server, err := NewWorldServer(core, ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func() error{
		"create": func() error {
			_, callErr := server.CreateExec(testRPCContext(), &worldv1.CreateExecRequest{Mutation: testWireMutation(), LeaseId: "lease"})
			return callErr
		},
		"transition": func() error {
			_, callErr := server.TransitionExec(testRPCContext(), &worldv1.TransitionExecRequest{Mutation: testWireMutation(), ExecId: "exec"})
			return callErr
		},
		"finalize": func() error {
			_, callErr := server.FinalizeExec(testRPCContext(), &worldv1.FinalizeExecRequest{Mutation: testWireMutation(), ExecId: "exec"})
			return callErr
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if err := call(); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("code = %s, want %s (err=%v)", status.Code(err), codes.PermissionDenied, err)
			}
		})
	}
	if core.calls != 0 {
		t.Fatalf("application exec mutations called %d times without trusted-node authorization", core.calls)
	}
}
