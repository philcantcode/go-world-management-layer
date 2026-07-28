package application

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type idempotencyTransactionProbe struct {
	beforeBegin int
}

func (p *idempotencyTransactionProbe) Hit(_ context.Context, point string) error {
	if point == "store.before_begin" {
		p.beforeBegin++
	}
	return nil
}

func TestAcquireRejectsMalformedIdempotencyKeysWithoutJournalOrStateMutation(t *testing.T) {
	for name, key := range malformedIdempotencyKeys() {
		t.Run(name, func(t *testing.T) {
			probe := &idempotencyTransactionProbe{}
			fixture := newCoreFixtureWithFaults(t, probe)
			request := validAcquireWithKey(fixture, t, key)

			if _, err := fixture.core.AcquireResearchSession(context.Background(), request); !domain.IsCode(err, domain.CodeInvalidArgument) {
				t.Fatalf("AcquireResearchSession() error = %v, want invalid argument", err)
			}
			assertNoMutationTransaction(t, fixture, probe, 0, 0)
			views, err := fixture.core.ListResearchSessions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(views) != 0 {
				t.Fatalf("malformed acquisition created %d session views", len(views))
			}
		})
	}
}

func TestCreateTargetRejectsMalformedIdempotencyKeysWithoutJournalOrStateMutation(t *testing.T) {
	for name, key := range malformedIdempotencyKeys() {
		t.Run(name, func(t *testing.T) {
			probe := &idempotencyTransactionProbe{}
			fixture := newCoreFixtureWithFaults(t, probe)
			view, _ := fixture.acquire(t)
			beforeTransactions := probe.beforeBegin
			beforeRecords := applicationControlRecordCount(t, fixture)
			meta := fixture.meta(t, "malformed-target")
			meta.IdempotencyKey = key

			_, err := fixture.core.CreateTarget(context.Background(), CreateTargetRequest{
				Meta: meta, LeaseID: view.Lease.ID, Template: "linux-visible", Kind: domain.TargetLinuxContainer,
				PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest,
			})
			if !domain.IsCode(err, domain.CodeInvalidArgument) {
				t.Fatalf("CreateTarget() error = %v, want invalid argument", err)
			}
			assertNoMutationTransaction(t, fixture, probe, beforeTransactions, beforeRecords)
			after, err := fixture.core.GetResearchSession(context.Background(), view.Session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(after.Targets) != 0 {
				t.Fatalf("malformed target creation persisted %d targets", len(after.Targets))
			}
		})
	}
}

func TestMaximumIdempotencyKeyAndEveryChildDerivationRemainCanonical(t *testing.T) {
	root := strings.Repeat("r", domain.MaximumIdempotencyKeyBytes)
	meta := MutationMeta{
		IdempotencyKey: root, CorrelationID: mustIdempotencyCorrelation(t),
		AuthorizedPolicyReference: "policy:test", Deadline: time.Now().Add(time.Minute),
	}
	if err := meta.Validate(context.Background(), time.Now()); err != nil {
		t.Fatalf("maximum root key was rejected: %v", err)
	}

	suffixes := []string{
		"artifact", "control", "physical/workspace", "physical/agent", "physical/target", "physical/run",
		"physical/plan-binding", "physical/target-plan-binding", "physical/run-plan-binding",
		"capture-output", "collector/process", "operation-running", "target-operation/operation_id",
	}
	for _, suffix := range suffixes {
		derived := domain.DeriveIdempotencyKey(root, suffix)
		if !domain.IsCanonicalIdempotencyKey(derived) {
			t.Errorf("DeriveIdempotencyKey(max, %q) produced invalid %d-byte key", suffix, len(derived))
		}
		if repeated := domain.DeriveIdempotencyKey(root, suffix); repeated != derived {
			t.Errorf("DeriveIdempotencyKey(max, %q) is not deterministic", suffix)
		}
	}

	if got := domain.DeriveIdempotencyKey("root", "child"); got != "root/child" {
		t.Fatalf("short derivation = %q, want exact readable form", got)
	}
	if domain.DeriveIdempotencyKey("k", "child") == domain.DeriveIdempotencyKey("k/", "child") {
		t.Fatal("roots that differ by a trailing slash collapsed onto one child identity")
	}
	if domain.DeriveIdempotencyKey("k", "child") == domain.DeriveIdempotencyKey("k", "/child") {
		t.Fatal("suffixes that differ by a leading slash collapsed onto one child identity")
	}
	first := domain.DeriveIdempotencyKey(root, "first")
	second := domain.DeriveIdempotencyKey(root, "second")
	otherParent := domain.DeriveIdempotencyKey(strings.Repeat("r", domain.MaximumIdempotencyKeyBytes-1)+"s", "first")
	if first == second || first == otherParent {
		t.Fatal("distinct overflowing derivations collapsed onto one idempotency key")
	}
	if nested := domain.DeriveIdempotencyKey(first, "nested"); !domain.IsCanonicalIdempotencyKey(nested) {
		t.Fatalf("nested maximum derivation is invalid: %q", nested)
	}
	unicodeRoot := strings.Repeat("€", 341) + "x"
	if len(unicodeRoot) != domain.MaximumIdempotencyKeyBytes {
		t.Fatalf("unicode boundary fixture is %d bytes", len(unicodeRoot))
	}
	if derived := domain.DeriveIdempotencyKey(unicodeRoot, "child"); !domain.IsCanonicalIdempotencyKey(derived) {
		t.Fatalf("UTF-8 boundary derivation is invalid: %q", derived)
	}
	for _, malformed := range []string{"", " root", "root ", strings.Repeat("x", domain.MaximumIdempotencyKeyBytes+1)} {
		if got := domain.DeriveIdempotencyKey(malformed, "child"); got != "" {
			t.Errorf("malformed parent %q derived %q instead of failing closed", malformed, got)
		}
	}
}

func TestMaximumRootKeyPersistsAndBindsMaximumSafePhysicalChildren(t *testing.T) {
	fixture := newCoreFixture(t)
	root := strings.Repeat("m", domain.MaximumIdempotencyKeyBytes)
	view, err := fixture.core.AcquireResearchSession(context.Background(), validAcquireWithKey(fixture, t, root))
	if err != nil {
		t.Fatalf("acquire with maximum key: %v", err)
	}
	if view.Session.AcquisitionIdempotencyKey != root {
		t.Fatal("maximum acquisition identity was not retained exactly")
	}

	generation := view.Agent.Generations[0]
	boundAgent, err := fixture.core.BindAgentGenerationPlan(context.Background(), BindAgentGenerationPlanRequest{
		Meta: mutationChild(view, fixture, t, root, "physical/plan-binding"), AgentWorkspaceID: view.Agent.ID,
		Generation: generation.Generation, ExpectedRevision: generation.Revision,
		ProvisioningPlanDigest:   domain.NewDigest([]byte("maximum agent plan")).String(),
		WorkspaceProvisioningKey: domain.DeriveIdempotencyKey(root, "physical/workspace"),
		AgentProvisioningKey:     domain.DeriveIdempotencyKey(root, "physical/agent"),
	})
	if err != nil {
		t.Fatalf("bind maximum-key agent plan: %v", err)
	}
	if !domain.IsCanonicalIdempotencyKey(boundAgent.Generations[0].WorkspaceProvisioningKey) ||
		!domain.IsCanonicalIdempotencyKey(boundAgent.Generations[0].AgentProvisioningKey) {
		t.Fatal("maximum-key agent plan persisted an invalid physical child identity")
	}

	targetMeta := fixture.meta(t, "maximum-target")
	targetMeta.IdempotencyKey = root
	targetMeta.AuthorizedPolicyReference = view.Session.PolicyDigest
	target, err := fixture.core.CreateTarget(context.Background(), CreateTargetRequest{
		Meta: targetMeta, LeaseID: view.Lease.ID,
		Template: "linux-visible", Kind: domain.TargetLinuxContainer,
		PolicyDigest: view.Session.PolicyDigest, CapabilityDigest: view.Session.CapabilityDigest,
	})
	if err != nil {
		t.Fatalf("create target from maximum key: %v", err)
	}
	if target.CreationIdempotencyKey != root {
		t.Fatal("maximum target creation identity was not retained exactly")
	}
	targetGeneration := target.Generations[0]
	if _, err := fixture.core.BindTargetGenerationPlan(context.Background(), BindTargetGenerationPlanRequest{
		Meta: mutationChild(view, fixture, t, root, "physical/target-plan-binding"), TargetID: target.ID,
		Generation: targetGeneration.Generation, ExpectedRevision: targetGeneration.Revision,
		ProvisioningPlanDigest: domain.NewDigest([]byte("maximum target plan")).String(),
		ProvisioningKey:        domain.DeriveIdempotencyKey(root, "physical/target"),
	}); err != nil {
		t.Fatalf("bind maximum-key target plan: %v", err)
	}
	replayed, err := NewCore(context.Background(), CoreOptions{Store: fixture.store, Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatalf("replay maximum keys: %v", err)
	}
	if _, err := replayed.GetResearchSession(context.Background(), view.Session.ID); err != nil {
		t.Fatalf("read replayed maximum-key session: %v", err)
	}
}

func malformedIdempotencyKeys() map[string]string {
	return map[string]string{
		"whitespace only":        " \t\r\n ",
		"leading whitespace":     " malformed",
		"trailing whitespace":    "malformed ",
		"over maximum byte size": strings.Repeat("x", domain.MaximumIdempotencyKeyBytes+1),
	}
}

func validAcquireWithKey(fixture *coreFixture, t *testing.T, key string) AcquireRequest {
	t.Helper()
	meta := fixture.meta(t, "idempotency-validation")
	meta.IdempotencyKey = key
	return AcquireRequest{
		Meta: meta, OwnerSubject: "test-owner", InputViewID: domain.NewInputViewID([]byte("idempotency-manifest")).String(),
		PolicyDigest:     domain.NewDigest([]byte("idempotency-policy")).String(),
		CapabilityDigest: domain.NewDigest([]byte("idempotency-capability")).String(), TTL: time.Hour,
	}
}

func mutationChild(view ResearchSessionView, fixture *coreFixture, t *testing.T, root, suffix string) MutationMeta {
	t.Helper()
	meta := fixture.meta(t, "maximum-child")
	meta.IdempotencyKey = domain.DeriveIdempotencyKey(root, suffix)
	meta.AuthorizedPolicyReference = view.Session.PolicyDigest
	return meta
}

func mustIdempotencyCorrelation(t *testing.T) string {
	t.Helper()
	correlation, err := domain.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	return correlation.String()
}

func assertNoMutationTransaction(t *testing.T, fixture *coreFixture, probe *idempotencyTransactionProbe, wantTransactions, wantRecords int) {
	t.Helper()
	if probe.beforeBegin != wantTransactions {
		t.Fatalf("mutation began %d idempotent transactions, want %d; journal boundary was crossed", probe.beforeBegin, wantTransactions)
	}
	if got := applicationControlRecordCount(t, fixture); got != wantRecords {
		t.Fatalf("control record count = %d, want %d", got, wantRecords)
	}
}

func applicationControlRecordCount(t *testing.T, fixture *coreFixture) int {
	t.Helper()
	records, err := fixture.store.Records(context.Background(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}
