package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type recordingLeaseOperationAdmission struct {
	captureErr   error
	exportErr    error
	capturePairs [][2]string
	exportPairs  [][2]string
	captures     []policyauthority.CaptureAdmission
	exports      []policyauthority.ExportAdmission
}

func (a *recordingLeaseOperationAdmission) AdmitCapture(_ context.Context, policyDigest, capabilityDigest string, request policyauthority.CaptureAdmission) error {
	a.capturePairs = append(a.capturePairs, [2]string{policyDigest, capabilityDigest})
	a.captures = append(a.captures, request)
	return a.captureErr
}

func (a *recordingLeaseOperationAdmission) AdmitExport(_ context.Context, policyDigest, capabilityDigest string, request policyauthority.ExportAdmission) error {
	a.exportPairs = append(a.exportPairs, [2]string{policyDigest, capabilityDigest})
	a.exports = append(a.exports, request)
	return a.exportErr
}

func TestCapturePolicyAdmissionPrecedesIntentAndPhysicalStart(t *testing.T) {
	fixture := newIntegrationFixture(t)
	fixture.readyAgent()
	denied := errors.New("capture denied")
	admission := &recordingLeaseOperationAdmission{captureErr: denied}
	controller := &captureControllerStub{}
	service := fixture.service(Config{Captures: controller, PolicyAdmission: admission})
	request := &worldv1.StartCaptureRequest{
		Mutation: fixture.wireMeta("capture-policy"), LeaseId: fixture.view.Lease.ID,
		CaptureSpec: &worldv1.CaptureSpec{
			Profile: "strace", SignalFamilies: []string{"process"},
			Duration: durationpb.New(time.Minute), ByteLimit: 1 << 20,
		},
	}
	if _, err := service.StartCapture(context.Background(), request); !errors.Is(err, denied) {
		t.Fatalf("denied capture error = %v", err)
	}
	if len(service.captureState) != 0 || controller.startCalls != 0 || len(admission.captures) != 1 {
		t.Fatalf("denied capture state=%d starts=%d admissions=%d", len(service.captureState), controller.startCalls, len(admission.captures))
	}

	admission.captureErr = nil
	started, err := service.StartCapture(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if started.State != captureStateActive || controller.startCalls != 1 || len(admission.captures) != 3 {
		t.Fatalf("allowed capture=%#v starts=%d admissions=%d", started, controller.startCalls, len(admission.captures))
	}
	for _, capture := range admission.captures {
		if len(capture.SignalFamilies) != 1 || capture.SignalFamilies[0] != "process" {
			t.Fatalf("capture admission lost requested signal-family authority: %#v", capture)
		}
	}
	for _, pair := range admission.capturePairs {
		if pair[0] != fixture.view.Session.PolicyDigest || pair[1] != fixture.view.Session.CapabilityDigest {
			t.Fatalf("capture admitted under pair %v", pair)
		}
	}

	// An active exact replay performs no new mutation or physical start and
	// therefore does not consult mutable admission inputs again.
	admission.captureErr = denied
	replayed, err := service.StartCapture(context.Background(), request)
	if err != nil || replayed.CaptureId != started.CaptureId || controller.startCalls != 1 || len(admission.captures) != 3 {
		t.Fatalf("capture replay=%#v err=%v starts=%d admissions=%d", replayed, err, controller.startCalls, len(admission.captures))
	}
}

func TestExportPolicyAdmissionPrecedesDeclarationAndReservedResumeUsesPersistedScope(t *testing.T) {
	fixture := newIntegrationFixture(t)
	fixture.readyAgent()
	denied := errors.New("export denied")
	admission := &recordingLeaseOperationAdmission{exportErr: denied}
	service := fixture.service(Config{PolicyAdmission: admission})
	declaration := &worldv1.DeclareExportRequest{
		Mutation: fixture.wireMeta("export-policy-declare"), LeaseId: fixture.view.Lease.ID,
		Paths: []*worldv1.ExportPath{{WorkspaceRelativePath: "output/result.txt", Role: "primary"}},
	}
	if _, err := service.DeclareExport(context.Background(), declaration); !errors.Is(err, denied) {
		t.Fatalf("denied declaration error = %v", err)
	}
	if len(service.exportState) != 0 || len(admission.exports) != 1 {
		t.Fatalf("denied declaration state=%d admissions=%d", len(service.exportState), len(admission.exports))
	}

	h := newExportCommitScenario(t)
	h.service.policyAdmission = admission
	if _, err := h.service.CommitExport(context.Background(), h.commitRequest); !errors.Is(err, denied) {
		t.Fatalf("denied commit error = %v", err)
	}
	h.service.mu.RLock()
	deniedRecord := cloneExportRecord(h.service.exportState[h.declared.ExportId])
	_, reserved := h.service.operations[operationIndex("commit_export", h.declared.ExportId)]
	h.service.mu.RUnlock()
	if deniedRecord.Export.State != exportStateDeclared || reserved || h.materialFaults.Hits("material.capture_outputs.before") != 0 {
		t.Fatalf("denied commit state=%s reserved=%t publications=%d", deniedRecord.Export.State, reserved, h.materialFaults.Hits("material.capture_outputs.before"))
	}

	admission.exportErr = nil
	publicationFailure := errors.New("publication response lost")
	h.materialFaults.FailNext("material.capture_outputs.after", publicationFailure)
	if _, err := h.service.CommitExport(context.Background(), h.commitRequest); !errors.Is(err, publicationFailure) {
		t.Fatalf("first admitted commit error = %v", err)
	}
	h.service.mu.RLock()
	committing := cloneExportRecord(h.service.exportState[h.declared.ExportId])
	h.service.mu.RUnlock()
	if committing.Export.State != exportStateCommitting {
		t.Fatalf("reserved export state = %s", committing.Export.State)
	}

	_, restarted, _ := h.restart(t)
	restarted.policyAdmission = admission
	changed := proto.Clone(h.commitRequest).(*worldv1.CommitExportRequest)
	changed.ExpectedWorkspaceRevision++
	beforeChanged := len(admission.exports)
	if _, err := restarted.CommitExport(context.Background(), changed); err == nil {
		t.Fatal("committing reservation accepted changed caller revision")
	}
	if len(admission.exports) != beforeChanged {
		t.Fatal("changed caller data reached policy admission during trusted resume")
	}
	committed, err := restarted.CommitExport(context.Background(), h.commitRequest)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != exportStateCommitted || len(admission.exports) != beforeChanged+1 {
		t.Fatalf("resumed export=%#v admissions=%d", committed, len(admission.exports))
	}
	lastPair := admission.exportPairs[len(admission.exportPairs)-1]
	if lastPair[0] != committing.Scope.PolicyDigest || lastPair[1] != committing.Scope.CapabilityDigest {
		t.Fatalf("resume pair=%v persisted=%s/%s", lastPair, committing.Scope.PolicyDigest, committing.Scope.CapabilityDigest)
	}
	lastAdmission := admission.exports[len(admission.exports)-1]
	if lastAdmission.DeclarationAuthority != "host" || !lastAdmission.FinalPublication || !lastAdmission.RetainsFullChangeManifest {
		t.Fatalf("final export admission did not prove host declaration and full manifest retention: %#v", lastAdmission)
	}
}

var _ LeaseOperationPolicyAdmission = (*recordingLeaseOperationAdmission)(nil)
