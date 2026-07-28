package world

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type generatedTransportServer struct {
	worldv1.UnimplementedWorldServiceServer
}

func (generatedTransportServer) GetTarget(_ context.Context, request *worldv1.GetTargetRequest) (*worldv1.Target, error) {
	return &worldv1.Target{TargetId: request.GetTargetId(), Revision: 9}, nil
}

type nilResponseClient struct {
	worldv1.WorldServiceClient
}

func (nilResponseClient) GetTarget(context.Context, *worldv1.GetTargetRequest, ...grpc.CallOption) (*worldv1.Target, error) {
	return nil, nil
}

func TestDefensiveCopyDoesNotShareNestedState(t *testing.T) {
	original := &worldv1.ResearchSessionView{
		Session:   &worldv1.ResearchSession{ResearchSessionId: "session"},
		Targets:   []*worldv1.Target{{TargetId: "target", Runs: []*worldv1.TargetRun{{TargetRunId: "run", IncidentIds: []string{"incident"}}}}},
		Incidents: []*worldv1.Incident{{IncidentId: "incident", HighWaterMetrics: []*worldv1.IncidentMetric{{Labels: map[string]string{"scope": "target"}}}, Coverage: []*worldv1.IncidentCoverage{{Gaps: []*worldv1.IncidentGap{{Reason: "overflow"}}}}, Artifacts: []*worldv1.ArtifactReference{{Reference: "artifact://evidence"}}}},
	}
	copy, err := defensiveCopy(original)
	if err != nil {
		t.Fatal(err)
	}
	copy.Session.ResearchSessionId = "changed"
	copy.Targets[0].Runs[0].IncidentIds[0] = "changed"
	copy.Incidents[0].HighWaterMetrics[0].Labels["scope"] = "changed"
	copy.Incidents[0].Coverage[0].Gaps[0].Reason = "changed"
	copy.Incidents[0].Artifacts[0].Reference = "changed"
	if original.Session.ResearchSessionId != "session" || original.Targets[0].Runs[0].IncidentIds[0] != "incident" || original.Incidents[0].HighWaterMetrics[0].Labels["scope"] != "target" || original.Incidents[0].Coverage[0].Gaps[0].Reason != "overflow" || original.Incidents[0].Artifacts[0].Reference != "artifact://evidence" {
		t.Fatal("returned view shares mutable nested state")
	}
}

func TestDialUsesGeneratedProtobufTransport(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	worldv1.RegisterWorldServiceServer(server, generatedTransportServer{})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(DialOptions{
		Dialer: func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		},
		DefaultTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	target, err := client.GetTarget(context.Background(), &worldv1.GetTargetRequest{TargetId: "target_1"})
	if err != nil {
		t.Fatal(err)
	}
	if target.GetTargetId() != "target_1" || target.GetRevision() != 9 {
		t.Fatalf("target = %#v", target)
	}
}

func TestUnaryRejectsNilGeneratedResponse(t *testing.T) {
	client := NewClient(nilResponseClient{}, time.Second)
	target, err := client.GetTarget(context.Background(), &worldv1.GetTargetRequest{TargetId: "target_1"})
	if target != nil || err == nil || !strings.Contains(err.Error(), "nil response") {
		t.Fatalf("target=%#v err=%v", target, err)
	}
}
