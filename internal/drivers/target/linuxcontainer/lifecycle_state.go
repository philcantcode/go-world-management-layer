package linuxcontainer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	lifecycleStateSchemaVersion = 1
	maximumLifecycleRecordSize  = int64(128 << 10)
	resetIntentFile             = "reset-intent.json"
	resetReceiptFile            = "reset-receipt.json"
	quarantineIntentFile        = "quarantine-intent.json"
	quarantineReceiptFile       = "quarantine-receipt.json"
)

var errLifecycleRecordConflict = errors.New("durable lifecycle record has different canonical content")

// persistedResetIntent is the complete destructive authority needed to resume
// a reset after process loss. Both physical plans are retained deliberately:
// reconciliation must never synthesize fields which ResetPlan does not carry.
type persistedResetIntent struct {
	SchemaVersion     int                `json:"schema_version"`
	Reset             persistedResetPlan `json:"reset"`
	PreviousPlan      ContainerPlan      `json:"previous_plan"`
	PreviousRuntimeID string             `json:"previous_runtime_id"`
	NextPlan          ContainerPlan      `json:"next_plan"`
}

type persistedResetPlan struct {
	IdempotencyKey string                  `json:"idempotency_key"`
	LeaseID        domain.LeaseID          `json:"lease_id"`
	Previous       ports.TargetRef         `json:"previous"`
	NextGeneration domain.TargetGeneration `json:"next_generation"`
	Mode           ports.ResetMode         `json:"mode"`
	SnapshotName   string                  `json:"snapshot_name,omitempty"`
	IncidentID     string                  `json:"incident_id,omitempty"`
}

type persistedTargetResult struct {
	TargetID   domain.TargetID              `json:"target_id"`
	Generation domain.TargetGeneration      `json:"generation"`
	Kind       domain.TargetKind            `json:"kind"`
	State      domain.TargetGenerationState `json:"state"`
	Ready      bool                         `json:"ready"`
	RuntimeID  string                       `json:"runtime_id"`
	CgroupID   string                       `json:"cgroup_id,omitempty"`
	ObservedAt time.Time                    `json:"observed_at"`
	Created    bool                         `json:"created"`
}

type persistedOutcomeError struct {
	Code      domain.ErrorCode `json:"code"`
	Operation string           `json:"operation"`
	Field     string           `json:"field,omitempty"`
	Message   string           `json:"message"`
}

type persistedResetReceipt struct {
	SchemaVersion int                    `json:"schema_version"`
	IntentDigest  domain.Digest          `json:"intent_digest"`
	Result        persistedTargetResult  `json:"result"`
	Error         *persistedOutcomeError `json:"error,omitempty"`
}

type persistedQuarantineIntent struct {
	SchemaVersion int                        `json:"schema_version"`
	Plan          ports.TargetQuarantinePlan `json:"plan"`
	TargetPlan    ContainerPlan              `json:"target_plan"`
	RuntimeID     string                     `json:"runtime_id"`
}

type persistedQuarantineReceipt struct {
	SchemaVersion int                            `json:"schema_version"`
	IntentDigest  domain.Digest                  `json:"intent_digest"`
	Evidence      ports.TargetQuarantineEvidence `json:"evidence"`
}

func newResetIntent(reset ports.ResetPlan, previous targetRecord, next ContainerPlan) (persistedResetIntent, error) {
	intent := persistedResetIntent{
		SchemaVersion: lifecycleStateSchemaVersion, Reset: persistedResetPlanFrom(reset),
		PreviousPlan: cloneContainerPlan(previous.plan), PreviousRuntimeID: previous.runtimeID,
		NextPlan: cloneContainerPlan(next),
	}
	return intent, intent.validate(previous.plan.TargetDirectoryRoot(), nil)
}

func (i persistedResetIntent) validate(targetRoot string, expected *ports.TargetPlan) error {
	if i.SchemaVersion != lifecycleStateSchemaVersion || strings.TrimSpace(i.PreviousRuntimeID) == "" {
		return fmt.Errorf("reset intent is incomplete or unsupported")
	}
	reset, err := i.Reset.plan()
	if err != nil {
		return fmt.Errorf("reset intent plan: %w", err)
	}
	if reset.Mode != ports.ResetRecreate {
		return fmt.Errorf("reset intent mode is not supported by the Linux container driver")
	}
	if err := i.PreviousPlan.Validate(targetRoot); err != nil {
		return fmt.Errorf("reset intent previous physical plan: %w", err)
	}
	if err := i.NextPlan.Validate(targetRoot); err != nil {
		return fmt.Errorf("reset intent next physical plan: %w", err)
	}
	if i.PreviousPlan.TargetID != reset.Previous.ID || i.PreviousPlan.Generation != reset.Previous.Generation || i.PreviousPlan.LeaseID != reset.LeaseID ||
		i.NextPlan.TargetID != reset.Previous.ID || i.NextPlan.Generation != reset.NextGeneration || i.NextPlan.LeaseID != reset.LeaseID {
		return fmt.Errorf("reset intent scope does not match its physical plans")
	}
	wantNext, err := replacementContainerPlan(i.PreviousPlan, reset.NextGeneration, targetRoot)
	if err != nil {
		return fmt.Errorf("derive reset replacement identity: %w", err)
	}
	same, err := sameContainerPlanIdentity(wantNext, i.NextPlan)
	if err != nil || !same {
		return fmt.Errorf("reset replacement is not the canonical successor of the previous plan")
	}
	if expected != nil {
		if err := validateResetExpectedPlan(i, *expected); err != nil {
			return err
		}
	}
	return nil
}

func validateResetExpectedPlan(intent persistedResetIntent, expected ports.TargetPlan) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected reset target plan: %w", err)
	}
	spec := expected.Generation.Spec()
	reset, err := intent.Reset.plan()
	if err != nil {
		return err
	}
	if expected.LeaseID != reset.LeaseID || spec.TargetID != reset.Previous.ID || spec.Generation != reset.NextGeneration ||
		spec.PreviousGeneration != reset.Previous.Generation || spec.RecoveryIncidentID != reset.IncidentID {
		return fmt.Errorf("reset intent does not match the expected durable target generation")
	}
	want, err := BuildContainerPlan(expected, BuildConfig{
		TargetRoot: intent.NextPlan.TargetDirectoryRoot(), ImageRepository: imageRepositoryFromPlan(intent.NextPlan),
		AllowPtrace: containsString(intent.NextPlan.Capabilities, "SYS_PTRACE"), ContainerUser: intent.NextPlan.User,
	})
	if err != nil {
		return fmt.Errorf("build expected reset physical plan: %w", err)
	}
	same, err := sameContainerPlanIdentity(want, intent.NextPlan)
	if err != nil || !same {
		return fmt.Errorf("reset intent physical successor differs from the expected durable plan")
	}
	return nil
}

func newResetReceipt(intent persistedResetIntent, result ports.TargetResult, outcomeErr error) (persistedResetReceipt, error) {
	digest, err := canonicalRecordDigest(intent)
	if err != nil {
		return persistedResetReceipt{}, err
	}
	receipt := persistedResetReceipt{
		SchemaVersion: lifecycleStateSchemaVersion, IntentDigest: digest,
		Result: persistedTargetResult{
			TargetID: result.Status.TargetID, Generation: result.Status.Generation, Kind: result.Status.Kind,
			State: result.Status.State, Ready: result.Status.Ready, RuntimeID: result.Status.RuntimeID,
			CgroupID: result.Status.CgroupID, ObservedAt: result.Status.ObservedAt.UTC(), Created: result.Created,
		},
	}
	if outcomeErr != nil {
		var typed *domain.Error
		if !errors.As(outcomeErr, &typed) {
			return persistedResetReceipt{}, fmt.Errorf("reset outcome is not a canonical domain error")
		}
		receipt.Error = &persistedOutcomeError{Code: typed.Code(), Operation: typed.Operation(), Field: typed.Field(), Message: typed.Message()}
	}
	return receipt, receipt.validate(intent)
}

func (r persistedResetReceipt) validate(intent persistedResetIntent) error {
	wantDigest, err := canonicalRecordDigest(intent)
	if err != nil {
		return err
	}
	reset, err := intent.Reset.plan()
	if err != nil {
		return err
	}
	if r.SchemaVersion != lifecycleStateSchemaVersion || r.IntentDigest != wantDigest || r.Result.TargetID != reset.Previous.ID ||
		r.Result.Generation != reset.NextGeneration || r.Result.Kind != domain.TargetLinuxContainer || r.Result.State != domain.TargetGenerationReady ||
		!r.Result.Ready || strings.TrimSpace(r.Result.RuntimeID) == "" || r.Result.ObservedAt.IsZero() || !isUTC(r.Result.ObservedAt) || !r.Result.Created {
		return fmt.Errorf("reset receipt does not match the durable reset intent")
	}
	if r.Error != nil && (r.Error.Code != domain.CodeUnavailable || r.Error.Operation != "linux_target.reset" || r.Error.Field != "cleanup" || r.Error.Message != "replacement is ready but the retired target directory could not be removed") {
		return fmt.Errorf("reset receipt error is not a supported canonical outcome")
	}
	return nil
}

func (r persistedResetReceipt) outcome(intent persistedResetIntent) resetOutcome {
	reset, _ := intent.Reset.plan()
	result := ports.TargetResult{Created: r.Result.Created, Status: ports.TargetStatus{
		TargetID: r.Result.TargetID, Generation: r.Result.Generation, Kind: r.Result.Kind, State: r.Result.State,
		Ready: r.Result.Ready, RuntimeID: r.Result.RuntimeID, CgroupID: r.Result.CgroupID, ObservedAt: r.Result.ObservedAt.UTC(),
	}}
	var outcomeErr error
	if r.Error != nil {
		outcomeErr = domain.NewError(r.Error.Code, r.Error.Operation, r.Error.Field, r.Error.Message, nil)
	}
	return resetOutcome{targetID: reset.Previous.ID, plan: reset, result: result, err: outcomeErr}
}

func persistedResetPlanFrom(plan ports.ResetPlan) persistedResetPlan {
	return persistedResetPlan{
		IdempotencyKey: plan.IdempotencyKey, LeaseID: plan.LeaseID, Previous: plan.Previous,
		NextGeneration: plan.NextGeneration, Mode: plan.Mode, SnapshotName: plan.SnapshotName,
		IncidentID: plan.IncidentID.String(),
	}
}

func (p persistedResetPlan) plan() (ports.ResetPlan, error) {
	var incident domain.IncidentID
	var err error
	if p.IncidentID != "" {
		incident, err = domain.ParseIncidentID(p.IncidentID)
		if err != nil {
			return ports.ResetPlan{}, err
		}
	}
	plan := ports.ResetPlan{
		IdempotencyKey: p.IdempotencyKey, LeaseID: p.LeaseID, Previous: p.Previous,
		NextGeneration: p.NextGeneration, Mode: p.Mode, SnapshotName: p.SnapshotName, IncidentID: incident,
	}
	return plan, plan.Validate()
}

func newQuarantineIntent(plan ports.TargetQuarantinePlan, record targetRecord) (persistedQuarantineIntent, error) {
	intent := persistedQuarantineIntent{
		SchemaVersion: lifecycleStateSchemaVersion, Plan: plan,
		TargetPlan: cloneContainerPlan(record.plan), RuntimeID: record.runtimeID,
	}
	return intent, intent.validate(record.plan.TargetDirectoryRoot(), nil)
}

func (i persistedQuarantineIntent) validate(targetRoot string, expected *ContainerPlan) error {
	if i.SchemaVersion != lifecycleStateSchemaVersion || strings.TrimSpace(i.RuntimeID) == "" {
		return fmt.Errorf("quarantine intent is incomplete or unsupported")
	}
	if err := i.Plan.Validate(); err != nil {
		return fmt.Errorf("quarantine intent plan: %w", err)
	}
	if err := i.TargetPlan.Validate(targetRoot); err != nil {
		return fmt.Errorf("quarantine intent physical plan: %w", err)
	}
	if i.Plan.Target.ID != i.TargetPlan.TargetID || i.Plan.Target.Generation != i.TargetPlan.Generation {
		return fmt.Errorf("quarantine intent scope does not match its physical plan")
	}
	if expected != nil {
		same, err := sameContainerPlanIdentity(i.TargetPlan, *expected)
		if err != nil || !same {
			return fmt.Errorf("quarantine intent differs from the expected durable target plan")
		}
	}
	return nil
}

func newQuarantineReceipt(intent persistedQuarantineIntent, evidence ports.TargetQuarantineEvidence) (persistedQuarantineReceipt, error) {
	digest, err := canonicalRecordDigest(intent)
	if err != nil {
		return persistedQuarantineReceipt{}, err
	}
	receipt := persistedQuarantineReceipt{SchemaVersion: lifecycleStateSchemaVersion, IntentDigest: digest, Evidence: evidence}
	return receipt, receipt.validate(intent)
}

func (r persistedQuarantineReceipt) validate(intent persistedQuarantineIntent) error {
	wantDigest, err := canonicalRecordDigest(intent)
	if err != nil {
		return err
	}
	if r.SchemaVersion != lifecycleStateSchemaVersion || r.IntentDigest != wantDigest {
		return fmt.Errorf("quarantine receipt does not match its durable intent")
	}
	if err := r.Evidence.Validate(intent.Plan.Target); err != nil {
		return err
	}
	if r.Evidence.RuntimeID != intent.RuntimeID || !isUTC(r.Evidence.ObservedAt) {
		return fmt.Errorf("quarantine receipt has a non-canonical observation or identifies another physical runtime")
	}
	return nil
}

func persistResetIntent(directory string, intent persistedResetIntent) error {
	return persistCanonicalLifecycleRecord(directory, resetIntentFile, intent)
}

func persistResetReceipt(directory string, receipt persistedResetReceipt) error {
	return persistCanonicalLifecycleRecord(directory, resetReceiptFile, receipt)
}

func loadResetRecords(directory, targetRoot string, expected *ports.TargetPlan) (persistedResetIntent, persistedResetReceipt, bool, bool, error) {
	var intent persistedResetIntent
	intentFound, err := loadCanonicalLifecycleRecord(directory, resetIntentFile, &intent)
	if err != nil {
		return intent, persistedResetReceipt{}, false, false, err
	}
	var receipt persistedResetReceipt
	receiptFound, receiptErr := loadCanonicalLifecycleRecord(directory, resetReceiptFile, &receipt)
	if receiptErr != nil {
		return intent, receipt, intentFound, false, receiptErr
	}
	if !intentFound {
		if receiptFound {
			return intent, receipt, false, true, fmt.Errorf("reset receipt exists without its intent")
		}
		return intent, receipt, false, false, nil
	}
	if err := intent.validate(targetRoot, expected); err != nil {
		return intent, receipt, true, receiptFound, err
	}
	if receiptFound {
		if err := receipt.validate(intent); err != nil {
			return intent, receipt, true, true, err
		}
	}
	return intent, receipt, true, receiptFound, nil
}

func persistQuarantineIntent(directory string, intent persistedQuarantineIntent) error {
	return persistCanonicalLifecycleRecord(directory, quarantineIntentFile, intent)
}

func persistQuarantineReceipt(directory string, receipt persistedQuarantineReceipt) error {
	return persistCanonicalLifecycleRecord(directory, quarantineReceiptFile, receipt)
}

func loadQuarantineRecords(directory, targetRoot string, expected *ContainerPlan) (persistedQuarantineIntent, persistedQuarantineReceipt, bool, bool, error) {
	var intent persistedQuarantineIntent
	intentFound, err := loadCanonicalLifecycleRecord(directory, quarantineIntentFile, &intent)
	if err != nil {
		return intent, persistedQuarantineReceipt{}, false, false, err
	}
	var receipt persistedQuarantineReceipt
	receiptFound, receiptErr := loadCanonicalLifecycleRecord(directory, quarantineReceiptFile, &receipt)
	if receiptErr != nil {
		return intent, receipt, intentFound, false, receiptErr
	}
	if !intentFound {
		if receiptFound {
			return intent, receipt, false, true, fmt.Errorf("quarantine receipt exists without its intent")
		}
		return intent, receipt, false, false, nil
	}
	if err := intent.validate(targetRoot, expected); err != nil {
		return intent, receipt, true, receiptFound, err
	}
	if receiptFound {
		if err := receipt.validate(intent); err != nil {
			return intent, receipt, true, true, err
		}
	}
	return intent, receipt, true, receiptFound, nil
}

func persistCanonicalLifecycleRecord(directory, name string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := persistRunRecord(directory, name, payload, maximumLifecycleRecordSize); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrExist) {
		return err
	}
	existing, err := loadRunRecord(directory, name, maximumLifecycleRecordSize)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, payload) {
		return errLifecycleRecordConflict
	}
	return nil
}

func loadCanonicalLifecycleRecord(directory, name string, destination any) (bool, error) {
	payload, err := loadRunRecord(directory, name, maximumLifecycleRecordSize)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return false, err
	}
	if err := requireJSONEnd(decoder); err != nil {
		return false, err
	}
	canonical, err := json.Marshal(destination)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(payload, canonical) {
		return false, fmt.Errorf("lifecycle record %q is not canonical JSON", name)
	}
	return true, nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("lifecycle record contains more than one JSON value")
		}
		return err
	}
	return nil
}

func canonicalRecordDigest(value any) (domain.Digest, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(payload), nil
}

func isUTC(value time.Time) bool {
	_, offset := value.Zone()
	return offset == 0
}

func cloneContainerPlan(plan ContainerPlan) ContainerPlan {
	plan.Resources = plan.Resources.Clone()
	plan.Labels = cloneStrings(plan.Labels)
	plan.Devices = append([]string(nil), plan.Devices...)
	plan.MountSources = append([]string(nil), plan.MountSources...)
	plan.Capabilities = append([]string(nil), plan.Capabilities...)
	return plan
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func imageRepositoryFromPlan(plan ContainerPlan) string {
	if index := strings.LastIndex(plan.Image, "@sha256:"); index > 0 {
		return plan.Image[:index]
	}
	return plan.Image
}

// TargetDirectoryRoot returns the configured root encoded by the canonical
// <root>/<target>/generations/<generation> layout.
func (p ContainerPlan) TargetDirectoryRoot() string {
	return filepath.Dir(filepath.Dir(filepath.Dir(p.TargetDirectory)))
}
