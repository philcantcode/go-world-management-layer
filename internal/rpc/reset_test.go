package rpc

import (
	"context"
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resetCoreStub struct {
	Core
	request application.ResetTargetRequest
	result  application.TargetRecord
	err     error
	calls   int
}

func (s *resetCoreStub) Authorize(context.Context, application.AuthorizationRequest) error {
	return nil
}

func (s *resetCoreStub) ResetTarget(_ context.Context, request application.ResetTargetRequest) (application.TargetRecord, error) {
	s.calls++
	s.request = request
	return s.result, s.err
}

func TestResetTargetPublicModesMatchAndReachApplicationUnchanged(t *testing.T) {
	publicModes := []string{worldv1.ResetModeBaseline, worldv1.ResetModeRecreate, worldv1.ResetModeSnapshot}
	portModes := []ports.ResetMode{ports.ResetBaseline, ports.ResetRecreate, ports.ResetSnapshot}
	for index := range publicModes {
		if publicModes[index] != string(portModes[index]) {
			t.Fatalf("public reset mode %q differs from port mode %q", publicModes[index], portModes[index])
		}
	}

	for _, mode := range portModes {
		snapshotName := ""
		if mode == ports.ResetSnapshot {
			snapshotName = "known-good"
		}
		core := &resetCoreStub{result: application.TargetRecord{ID: "target_1"}}
		server, err := NewWorldServer(core, ServerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = server.ResetTarget(testRPCContext(), &worldv1.ResetTargetRequest{
			Mutation: testWireMutation(), TargetId: "target_1", ExpectedRevision: 7,
			ResetMode: string(mode), SnapshotName: snapshotName, RecoveryIncidentId: "incident_1",
		})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if core.calls != 1 || core.request.Mode != mode || core.request.SnapshotName != snapshotName || core.request.RecoveryIncidentID != "incident_1" {
			t.Fatalf("%s request changed at RPC boundary: %#v", mode, core.request)
		}
	}
}

func TestResetTargetRejectsRemovedAndAmbiguousModesAtPublicBoundary(t *testing.T) {
	cases := []*worldv1.ResetTargetRequest{
		{Mutation: testWireMutation(), TargetId: "target_1", ResetMode: ""},
		{Mutation: testWireMutation(), TargetId: "target_1", ResetMode: "unknown"},
		{Mutation: testWireMutation(), TargetId: "target_1", ResetMode: worldv1.ResetModeSnapshot},
		{Mutation: testWireMutation(), TargetId: "target_1", ResetMode: worldv1.ResetModeBaseline, SnapshotName: "ignored"},
	}
	for index, request := range cases {
		core := &resetCoreStub{}
		server, err := NewWorldServer(core, ServerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.ResetTarget(testRPCContext(), request); status.Code(err) != codes.InvalidArgument {
			t.Errorf("case %d code = %s, want %s (err=%v)", index, status.Code(err), codes.InvalidArgument, err)
		}
		if core.calls != 0 {
			t.Errorf("case %d reached the application core", index)
		}
	}
}

func TestResetTargetMapsUnsupportedDriverModeToFailedPrecondition(t *testing.T) {
	core := &resetCoreStub{err: domain.NewError(domain.CodeCapabilityUnavailable, "target.reset", "mode", "snapshot is unsupported", nil)}
	server, err := NewWorldServer(core, ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.ResetTarget(testRPCContext(), &worldv1.ResetTargetRequest{
		Mutation: testWireMutation(), TargetId: "target_1", ResetMode: worldv1.ResetModeSnapshot, SnapshotName: "known-good",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("capability-unavailable code = %s, want %s (err=%v)", status.Code(err), codes.FailedPrecondition, err)
	}
}
