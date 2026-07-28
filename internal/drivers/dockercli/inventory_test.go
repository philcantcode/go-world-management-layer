package dockercli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestInventoryReturnsOnlyCompleteInspectedSnapshot(t *testing.T) {
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		switch invocation.Args[0] {
		case "ps":
			return command.Result{Stdout: []byte("container-b\ncontainer-a\n")}, nil
		case "inspect":
			return command.Result{Stdout: dockerInspectJSON(invocation.Args[1:]...)}, nil
		default:
			return command.Result{}, errors.New("unexpected invocation")
		}
	})
	containers, err := Inventory(inventoryDeadline(t), "docker-test", runner)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != 2 || containers[0].ID != "container-a" || containers[1].ID != "container-b" {
		t.Fatalf("inventory = %#v", containers)
	}
	configuration := containers[0].Configuration
	if containers[0].Name != "container-a" || configuration.Image != "repo@sha256:image" || !configuration.InitKnown || !configuration.Init || configuration.MemorySwapBytes != 1536 || len(configuration.Mounts) != 1 {
		t.Fatalf("decoded container = %#v", containers[0])
	}
}

func TestInventoryBatchesContainerInspection(t *testing.T) {
	ids := make([]string, maximumInspectBatch+1)
	for index := range ids {
		ids[index] = fmt.Sprintf("container-%03d", index)
	}
	var batchSizes []int
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if invocation.Args[0] == "ps" {
			return command.Result{Stdout: []byte(strings.Join(ids, "\n") + "\n")}, nil
		}
		batchSizes = append(batchSizes, len(invocation.Args)-1)
		return command.Result{Stdout: dockerInspectJSON(invocation.Args[1:]...)}, nil
	})
	containers, err := Inventory(inventoryDeadline(t), "docker", runner)
	if err != nil {
		t.Fatal(err)
	}
	if len(containers) != len(ids) || fmt.Sprint(batchSizes) != fmt.Sprint([]int{maximumInspectBatch, 1}) {
		t.Fatalf("containers=%d batches=%v", len(containers), batchSizes)
	}
}

func TestInventoryRejectsAmbiguousOrUnboundedSnapshots(t *testing.T) {
	t.Run("duplicate IDs", func(t *testing.T) {
		runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte("duplicate\nduplicate\n")}, nil
		})
		if _, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil {
			t.Fatal("duplicate inventory was accepted")
		}
	})
	t.Run("safety bound", func(t *testing.T) {
		listing := strings.Repeat("container\n", MaximumInventoryContainers+1)
		runner := runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte(listing)}, nil
		})
		if _, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil {
			t.Fatal("unbounded inventory was accepted")
		}
	})
	t.Run("inspect failure", func(t *testing.T) {
		runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
			if invocation.Args[0] == "ps" {
				return command.Result{Stdout: []byte("container\n")}, nil
			}
			return command.Result{}, errors.New("container disappeared")
		})
		if containers, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil || containers != nil {
			t.Fatalf("partial inventory = %#v, %v", containers, err)
		}
	})
	t.Run("inspect substitution", func(t *testing.T) {
		runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
			if invocation.Args[0] == "ps" {
				return command.Result{Stdout: []byte("requested\n")}, nil
			}
			return command.Result{Stdout: dockerInspectJSON("different")}, nil
		})
		if containers, err := Inventory(inventoryDeadline(t), "docker", runner); err == nil || containers != nil {
			t.Fatalf("substituted inventory = %#v, %v", containers, err)
		}
	})
}

func TestInspectRejectsSubstitutedResource(t *testing.T) {
	runner := runnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if fmt.Sprint(invocation.Args) != fmt.Sprint([]string{"inspect", "requested"}) {
			t.Fatalf("inspect arguments = %v", invocation.Args)
		}
		return command.Result{Stdout: dockerInspectJSON("different")}, nil
	})
	if container, err := Inspect(inventoryDeadline(t), "docker", runner, "requested"); err == nil || container.ID != "" {
		t.Fatalf("substituted inspect = %#v, %v", container, err)
	}
}

func dockerInspectJSON(ids ...string) []byte {
	documents := make([]string, len(ids))
	for index, id := range ids {
		documents[index] = fmt.Sprintf(`{
			"Id":%q,"Name":%q,"State":{"Running":true,"Status":"running"},
			"Config":{"Image":"repo@sha256:image","Labels":{"world.role":"test"},"Entrypoint":["/guest"],"OpenStdin":true},
			"HostConfig":{"CgroupParent":"cg","ReadonlyRootfs":true,"NetworkMode":"none","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true","seccomp=builtin"],"Memory":1024,"MemorySwap":1536,"Init":true},
			"Mounts":[{"Type":"bind","Source":"/source","Destination":"/target","RW":true}]
		}`, id, "/"+id)
	}
	return []byte("[" + strings.Join(documents, ",") + "]")
}

func inventoryDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

type runnerFunc func(context.Context, command.Invocation) (command.Result, error)

func (f runnerFunc) Run(ctx context.Context, invocation command.Invocation) (command.Result, error) {
	return f(ctx, invocation)
}

var _ command.Runner = runnerFunc(nil)
