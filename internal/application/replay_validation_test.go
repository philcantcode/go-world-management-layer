package application

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
)

func TestReplayAcceptsUntamperedApplicationJournal(t *testing.T) {
	_, records := replayValidationRecords(t)
	projection := emptyReplayCore()
	for _, record := range records {
		if err := projection.apply(record); err != nil {
			t.Fatalf("apply sequence %d %s/%s revision %d: %v", record.Sequence, record.AggregateKind, record.AggregateID, record.Revision, err)
		}
	}
	if len(projection.sessions) != 1 || len(projection.leases) != 1 || len(projection.agents) != 1 ||
		len(projection.execs) != 1 || len(projection.targets) != 1 || len(projection.incidents) != 1 {
		t.Fatalf("incomplete replay projection: sessions=%d leases=%d agents=%d execs=%d targets=%d incidents=%d",
			len(projection.sessions), len(projection.leases), len(projection.agents), len(projection.execs), len(projection.targets), len(projection.incidents))
	}
}

func TestReplayRejectsZeroProvisioningPlanDigests(t *testing.T) {
	zero := zeroProvisioningPlanDigest()
	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "agent generation",
			validate: func() error {
				return validateAgentProvisioningBinding("agent", AgentGenerationRecord{
					ProvisioningPlanDigest: zero, WorkspaceProvisioningKey: "workspace/key", AgentProvisioningKey: "agent/key",
				})
			},
		},
		{
			name: "target generation",
			validate: func() error {
				return validateTargetProvisioningBinding("target", TargetGenerationRecord{
					ProvisioningPlanDigest: zero, ProvisioningKey: "target/key",
				})
			},
		},
		{
			name: "target run",
			validate: func() error {
				return validateTargetRunProvisioningBinding("target_run", TargetRunRecord{
					ProvisioningPlanDigest: zero, ProvisioningKey: "run/key",
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertReplayIntegrityViolation(t, test.validate())
		})
	}
}

func TestReplayRejectsStrictJSONAndEnvelopeTamperingBeforeProjection(t *testing.T) {
	latest, _ := replayValidationRecords(t)
	session := latest["session"]

	unknown := session
	var object map[string]any
	if err := json.Unmarshal(unknown.Payload, &object); err != nil {
		t.Fatal(err)
	}
	object["unknown_field"] = true
	unknown.Payload = mustMarshalReplayTest(t, object)

	trailing := session
	trailing.Payload = append(append([]byte(nil), trailing.Payload...), []byte(` {"second":true}`)...)

	payloadID := tamperReplayRecord[SessionRecord](t, session, func(value *SessionRecord) {
		value.ID = value.LeaseID
	})
	payloadRevision := tamperReplayRecord[SessionRecord](t, session, func(value *SessionRecord) {
		value.Revision++
	})

	for _, test := range []struct {
		name   string
		record store.ControlRecord
	}{
		{name: "unknown field", record: unknown},
		{name: "trailing value", record: trailing},
		{name: "payload aggregate ID", record: payloadID},
		{name: "payload revision", record: payloadRevision},
	} {
		t.Run(test.name, func(t *testing.T) {
			projection := emptyReplayCore()
			assertReplayIntegrityViolation(t, projection.apply(test.record))
			assertEmptyReplayProjection(t, projection)
		})
	}
}

func TestReplayRejectsAggregateSemanticTampering(t *testing.T) {
	latest, _ := replayValidationRecords(t)
	tests := []struct {
		name   string
		record store.ControlRecord
	}{
		{
			name: "session state",
			record: tamperReplayRecord[SessionRecord](t, latest["session"], func(value *SessionRecord) {
				value.State = domain.ResearchSessionState("invented")
			}),
		},
		{
			name: "session acquisition key",
			record: tamperReplayRecord[SessionRecord](t, latest["session"], func(value *SessionRecord) {
				value.AcquisitionIdempotencyKey = " malformed"
			}),
		},
		{
			name: "lease expiry",
			record: tamperReplayRecord[LeaseRecord](t, latest["lease"], func(value *LeaseRecord) {
				value.ExpiresAt = value.CreatedAt
			}),
		},
		{
			name: "agent current generation",
			record: tamperReplayRecord[AgentWorkspaceRecord](t, latest["agent_workspace"], func(value *AgentWorkspaceRecord) {
				value.CurrentGeneration++
			}),
		},
		{
			name: "agent generation state",
			record: tamperReplayRecord[AgentWorkspaceRecord](t, latest["agent_workspace"], func(value *AgentWorkspaceRecord) {
				value.Generations[value.CurrentGeneration-1].State = domain.AgentGenerationState("invented")
			}),
		},
		{
			name: "exec generation",
			record: tamperReplayRecord[ExecRecord](t, latest["exec"], func(value *ExecRecord) {
				value.AgentGeneration = 0
			}),
		},
		{
			name: "exec timestamp",
			record: tamperReplayRecord[ExecRecord](t, latest["exec"], func(value *ExecRecord) {
				value.UpdatedAt = value.CreatedAt.Add(-1)
			}),
		},
		{
			name: "target current generation",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.CurrentGeneration++
			}),
		},
		{
			name: "target creation key",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.CreationIdempotencyKey = strings.Repeat("k", domain.MaximumIdempotencyKeyBytes+1)
			}),
		},
		{
			name: "target operation run identity",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.Operations[0].RunID = value.ID
			}),
		},
		{
			name: "incident target identity",
			record: tamperReplayRecord[IncidentRecord](t, latest["incident"], func(value *IncidentRecord) {
				value.TargetID = ""
			}),
		},
		{
			name: "incident state",
			record: tamperReplayRecord[IncidentRecord](t, latest["incident"], func(value *IncidentRecord) {
				value.State = domain.IncidentState("invented")
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := emptyReplayCore()
			assertReplayIntegrityViolation(t, projection.apply(test.record))
			assertEmptyReplayProjection(t, projection)
		})
	}
}

func TestReplayRejectsLeaseTerminationTampering(t *testing.T) {
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	fixture.now = view.Lease.ExpiresAt
	prepared, err := fixture.core.BeginDueLeaseExpiry(context.Background(), BeginLeaseExpiryRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: view.Lease.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.CompleteLeaseTermination(context.Background(), CompleteLeaseTerminationRequest{
		LeaseID: view.Lease.ID, ExpectedRevision: prepared.TerminatingLeaseRevision,
	}); err != nil {
		t.Fatal(err)
	}
	records, err := fixture.store.Records(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		eventKind string
		mutate    func(*LeaseRecord)
	}{
		{
			name: "invalid begin digest", eventKind: "lease.expiring",
			mutate: func(value *LeaseRecord) { value.Termination.BeginRequestDigest = "not-a-digest" },
		},
		{
			name: "changed begin identity", eventKind: "lease.expired",
			mutate: func(value *LeaseRecord) {
				value.Termination.BeginRequestDigest = domain.NewDigest([]byte("different begin")).String()
			},
		},
		{
			name: "missing completion identity", eventKind: "lease.expired",
			mutate: func(value *LeaseRecord) { value.Termination.CompleteIdempotencyKey = "" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := emptyReplayCore()
			found := false
			for _, record := range records {
				if record.AggregateKind == "lease" && record.Kind == test.eventKind {
					record = tamperReplayRecord[LeaseRecord](t, record, test.mutate)
					found = true
				}
				err := projection.apply(record)
				if found {
					assertReplayIntegrityViolation(t, err)
					break
				}
				if err != nil {
					t.Fatalf("apply sequence %d before tampered record: %v", record.Sequence, err)
				}
			}
			if !found {
				t.Fatalf("event %q not found", test.eventKind)
			}
		})
	}
}

func TestReplayRejectsCrossResourceOwnershipDrift(t *testing.T) {
	latest, records := replayValidationRecords(t)
	tests := []struct {
		name   string
		record store.ControlRecord
	}{
		{
			name: "session immutable lease",
			record: tamperReplayRecord[SessionRecord](t, latest["session"], func(value *SessionRecord) {
				value.LeaseID = mustReplayID(t, domain.NewLeaseID)
			}),
		},
		{
			name: "lease session",
			record: tamperReplayRecord[LeaseRecord](t, latest["lease"], func(value *LeaseRecord) {
				value.SessionID = mustReplayID(t, domain.NewResearchSessionID)
			}),
		},
		{
			name: "agent session",
			record: tamperReplayRecord[AgentWorkspaceRecord](t, latest["agent_workspace"], func(value *AgentWorkspaceRecord) {
				value.SessionID = mustReplayID(t, domain.NewResearchSessionID)
			}),
		},
		{
			name: "agent nested revision skip",
			record: tamperReplayRecord[AgentWorkspaceRecord](t, latest["agent_workspace"], func(value *AgentWorkspaceRecord) {
				value.Generations[value.CurrentGeneration-1].Revision++
			}),
		},
		{
			name: "exec agent workspace",
			record: tamperReplayRecord[ExecRecord](t, latest["exec"], func(value *ExecRecord) {
				value.AgentWorkspaceID = mustReplayID(t, domain.NewAgentWorkspaceID)
			}),
		},
		{
			name: "target lease",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.LeaseID = mustReplayID(t, domain.NewLeaseID)
			}),
		},
		{
			name: "target run incident",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.Runs[0].IncidentIDs = append(value.Runs[0].IncidentIDs, mustReplayID(t, domain.NewIncidentID))
			}),
		},
		{
			name: "target operation nested revision skip",
			record: tamperReplayRecord[TargetRecord](t, latest["target"], func(value *TargetRecord) {
				value.Operations[0].State = domain.TargetOperationRunning
				value.Operations[0].Revision += 2
			}),
		},
		{
			name: "incident exec",
			record: tamperReplayRecord[IncidentRecord](t, latest["incident"], func(value *IncidentRecord) {
				value.ExecID = mustReplayID(t, domain.NewExecID)
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := emptyReplayCore()
			rejected := false
			for _, record := range records {
				if record.Sequence == test.record.Sequence {
					record = test.record
				}
				if err := projection.apply(record); err != nil {
					assertReplayIntegrityViolation(t, err)
					rejected = true
					break
				}
			}
			if !rejected {
				t.Fatal("tampered journal was accepted")
			}
		})
	}
}

func TestNewCoreRejectsSemanticallyInvalidSignedControlPayload(t *testing.T) {
	_, records := replayValidationRecords(t)
	var firstSession store.ControlRecord
	for _, record := range records {
		if record.AggregateKind == "session" && record.Revision == 1 {
			firstSession = record
			break
		}
	}
	if firstSession.AggregateID == "" {
		t.Fatal("first session record not found")
	}
	firstSession = tamperReplayRecord[SessionRecord](t, firstSession, func(value *SessionRecord) {
		value.State = domain.ResearchSessionState("invented")
	})
	firstSession.Sequence = 0
	firstSession.AcceptedAt = firstSession.AcceptedAt.Add(-1)
	firstSession.PreviousHash = [32]byte{}
	firstSession.Hash = [32]byte{}

	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "tampered.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	_, _, err = controlStore.RunIdempotent(context.Background(), "seed", "invalid-signed-payload", []byte("request"), func(ctx context.Context, tx *store.Tx) ([]byte, error) {
		if _, err := tx.AppendControl(ctx, firstSession); err != nil {
			return nil, err
		}
		return []byte(`{"accepted":true}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controlStore.Verify(context.Background()); err != nil {
		t.Fatalf("test record was not validly signed: %v", err)
	}

	_, err = NewCore(context.Background(), CoreOptions{Store: controlStore})
	assertReplayIntegrityViolation(t, err)
}

func replayValidationRecords(t *testing.T) (map[string]store.ControlRecord, []store.ControlRecord) {
	t.Helper()
	fixture := newCoreFixture(t)
	view, _ := fixture.acquire(t)
	agent := fixture.readyAgent(t, view.Agent)
	execution, err := fixture.core.CreateExec(context.Background(), CreateExecRequest{
		Meta: fixture.meta(t, "replay-exec"), LeaseID: view.Lease.ID, Kind: domain.ExecTool,
		Executable: "bin/tool", WorkingDirectory: ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := fixture.readyTarget(t, view)
	run := fixture.runningRun(t, target)
	if _, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
		Meta: fixture.meta(t, "replay-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationShell, CommandDisplay: "inspect specimen",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.core.CreateIncident(context.Background(), CreateIncidentRequest{
		Meta: fixture.meta(t, "replay-incident"), Classification: domain.IncidentTargetWorkloadExit,
		SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID,
		AgentGeneration: agent.CurrentGeneration, ExecID: execution.ID, TargetID: target.ID,
		TargetGeneration: target.CurrentGeneration, TargetRunID: run.ID,
		Trigger: "specimen exited", LastKnownState: "running",
		Cause: CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0},
	}); err != nil {
		t.Fatal(err)
	}

	records, err := fixture.store.Records(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	latest := make(map[string]store.ControlRecord)
	for _, record := range records {
		latest[record.AggregateKind] = record
	}
	return latest, records
}

func tamperReplayRecord[T any](t *testing.T, record store.ControlRecord, mutate func(*T)) store.ControlRecord {
	t.Helper()
	var value T
	if err := json.Unmarshal(record.Payload, &value); err != nil {
		t.Fatal(err)
	}
	mutate(&value)
	record.Payload = mustMarshalReplayTest(t, value)
	return record
}

func mustMarshalReplayTest(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func mustReplayID[T interface{ String() string }](t *testing.T, create func() (T, error)) string {
	t.Helper()
	value, err := create()
	if err != nil {
		t.Fatal(err)
	}
	return value.String()
}

func emptyReplayCore() *Core {
	return &Core{
		sessions: make(map[string]SessionRecord), leases: make(map[string]LeaseRecord),
		agents: make(map[string]AgentWorkspaceRecord), execs: make(map[string]ExecRecord),
		targets: make(map[string]TargetRecord), incidents: make(map[string]IncidentRecord),
	}
}

func assertReplayIntegrityViolation(t *testing.T, err error) {
	t.Helper()
	if !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("replay error = %v, want integrity violation", err)
	}
}

func assertEmptyReplayProjection(t *testing.T, core *Core) {
	t.Helper()
	if len(core.sessions)+len(core.leases)+len(core.agents)+len(core.execs)+len(core.targets)+len(core.incidents) != 0 {
		t.Fatal("invalid record mutated the replay projection")
	}
}
