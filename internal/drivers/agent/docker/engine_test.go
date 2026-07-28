package docker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestCLIEngineOpenExecInvokesSelectedGuestBinary(t *testing.T) {
	wantErr := errors.New("stop after recording")
	var got command.Invocation
	starter := recordingStarter(func(_ context.Context, invocation command.Invocation) (command.Process, error) {
		got = invocation
		return nil, wantErr
	})
	engine := NewCLIEngine("docker-custom", nil, starter)
	agent := testAgentWorkspacePlan(t)
	plan := testExecPlan(t, agent, "/workspace/tool")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := engine.OpenExec(ctx, "container-id", "/opt/world/guest-v2", plan); !errors.Is(err, wantErr) {
		t.Fatalf("OpenExec() error = %v, want %v", err, wantErr)
	}
	wantArgs := []string{"exec", "--interactive", "--workdir", WorkspaceMount, "container-id", "/opt/world/guest-v2"}
	if got.Program != "docker-custom" || !reflect.DeepEqual(got.Args, wantArgs) {
		t.Fatalf("invocation = %#v, want program docker-custom args %#v", got, wantArgs)
	}
}

type recordingStarter func(context.Context, command.Invocation) (command.Process, error)

func (f recordingStarter) Start(ctx context.Context, invocation command.Invocation) (command.Process, error) {
	return f(ctx, invocation)
}

var _ command.Starter = recordingStarter(nil)
