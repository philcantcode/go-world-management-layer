package docker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type fixtureInput struct {
	bytes []byte
	mode  uint32
}

type agentFixture struct {
	agent     ports.AgentWorkspacePlan
	workspace ports.WorkspacePlan
}

func newAgentFixture(t *testing.T, imageDigest domain.Digest, inputs map[string]fixtureInput) agentFixture {
	t.Helper()
	leaseID := requireFixtureID(t, domain.NewLeaseID)
	agentID := requireFixtureID(t, domain.NewAgentWorkspaceID)
	workspaceID := requireFixtureID(t, domain.NewWorkspaceID)
	entries := make([]domain.InputViewEntry, 0, len(inputs))
	content := make(map[string]ports.ContentSource, len(inputs))
	for logicalPath, input := range inputs {
		digest := domain.NewDigest(input.bytes)
		entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
			LogicalPath: logicalPath, OccurrenceRef: "artifact://agent-e2e/" + logicalPath,
			Digest: digest, Size: int64(len(input.bytes)), Mode: input.mode,
		})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
		content[logicalPath] = fixtureSource{content: input.bytes, digest: digest}
	}
	manifest, err := domain.NewInputViewManifest(entries)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID,
		AgentGeneration: domain.InitialAgentGeneration, InputViewID: manifest.ID(), CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.NewDigest([]byte("agent-e2e-policy"))
	capability := domain.NewDigest([]byte("agent-e2e-capability"))
	generation, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
		AgentWorkspaceID: agentID, Generation: domain.InitialAgentGeneration, WorkspaceID: workspaceID,
		InputViewID: manifest.ID(), PolicyDigest: policy, CapabilityFingerprintDigest: capability, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return agentFixture{
		agent: ports.AgentWorkspacePlan{
			IdempotencyKey: "agent-fixture", LeaseID: leaseID, Generation: generation, Workspace: workspace,
			ImageDigest: imageDigest, PolicyDigest: policy, CapabilityFingerprintDigest: capability,
			Resources: admission.Resources{CPUMilli: 100, MemoryBytes: 32 << 20, StorageBytes: 64 << 20, Inodes: 1024, PIDs: 32},
		},
		workspace: ports.WorkspacePlan{
			IdempotencyKey: "workspace-fixture", Workspace: workspace, InputView: manifest, Content: content,
			Construction: domain.InputViewAllowCopy, UpperByteLimit: 64 << 20, UpperInodeLimit: 1024,
		},
	}
}

func requireFixtureID[T any](t *testing.T, create func() (T, error)) T {
	t.Helper()
	value, err := create()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type fixtureSource struct {
	content []byte
	digest  domain.Digest
}

func (s fixtureSource) Digest() domain.Digest { return s.digest }
func (s fixtureSource) Size() int64           { return int64(len(s.content)) }
func (s fixtureSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.content)), nil
}
