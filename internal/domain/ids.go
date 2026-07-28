package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"time"
)

type uuidV7 [16]byte

type idPrefix interface{ prefix() string }
type typedID[P idPrefix] struct{ value uuidV7 }

type researchSessionPrefix struct{}

func (researchSessionPrefix) prefix() string { return "rs" }

type leasePrefix struct{}

func (leasePrefix) prefix() string { return "lease" }

type agentWorkspacePrefix struct{}

func (agentWorkspacePrefix) prefix() string { return "aw" }

type execPrefix struct{}

func (execPrefix) prefix() string { return "exec" }

type targetPrefix struct{}

func (targetPrefix) prefix() string { return "target" }

type targetRunPrefix struct{}

func (targetRunPrefix) prefix() string { return "run" }

type targetOperationPrefix struct{}

func (targetOperationPrefix) prefix() string { return "op" }

type workspacePrefix struct{}

func (workspacePrefix) prefix() string { return "ws" }

type incidentPrefix struct{}

func (incidentPrefix) prefix() string { return "incident" }

type capturePrefix struct{}

func (capturePrefix) prefix() string { return "capture" }

type bundlePrefix struct{}

func (bundlePrefix) prefix() string { return "bundle" }

type exportPrefix struct{}

func (exportPrefix) prefix() string { return "export" }

type eventPrefix struct{}

func (eventPrefix) prefix() string { return "event" }

type correlationPrefix struct{}

func (correlationPrefix) prefix() string { return "corr" }

type collectorPrefix struct{}

func (collectorPrefix) prefix() string { return "collector" }

type subjectPrefix struct{}

func (subjectPrefix) prefix() string { return "subject" }

type ResearchSessionID struct{ typedID[researchSessionPrefix] }
type LeaseID struct{ typedID[leasePrefix] }
type AgentWorkspaceID struct{ typedID[agentWorkspacePrefix] }
type ExecID struct{ typedID[execPrefix] }
type TargetID struct{ typedID[targetPrefix] }
type TargetRunID struct{ typedID[targetRunPrefix] }
type TargetOperationID struct{ typedID[targetOperationPrefix] }
type WorkspaceID struct{ typedID[workspacePrefix] }
type IncidentID struct{ typedID[incidentPrefix] }
type CaptureID struct{ typedID[capturePrefix] }
type ObservationBundleID struct{ typedID[bundlePrefix] }
type ExportID struct{ typedID[exportPrefix] }
type EventID struct{ typedID[eventPrefix] }
type CorrelationID struct{ typedID[correlationPrefix] }
type CollectorID struct{ typedID[collectorPrefix] }
type SubjectID struct{ typedID[subjectPrefix] }

// IDGenerator permits deterministic identity generation in tests while the
// default constructors use the system clock and crypto/rand.
type IDGenerator struct {
	now    func() time.Time
	random io.Reader
}

func NewIDGenerator(now func() time.Time, random io.Reader) (*IDGenerator, error) {
	if now == nil {
		return nil, NewError(CodeInvalidArgument, "id_generator.new", "now", "must be provided", nil)
	}
	if random == nil {
		return nil, NewError(CodeInvalidArgument, "id_generator.new", "random", "must be provided", nil)
	}
	return &IDGenerator{now: now, random: random}, nil
}

func defaultID[P idPrefix]() (typedID[P], error) { return generateID[P](time.Now(), rand.Reader) }

func generateID[P idPrefix](now time.Time, random io.Reader) (typedID[P], error) {
	if now.IsZero() {
		return typedID[P]{}, NewError(CodeInvalidArgument, "id.generate", "time", "must be set", nil)
	}
	if now.UnixMilli() < 0 || uint64(now.UnixMilli()) > 0xffffffffffff {
		return typedID[P]{}, NewError(CodeInvalidArgument, "id.generate", "time", "is outside the UUIDv7 timestamp range", nil)
	}
	var value uuidV7
	millis := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		value[i] = byte(millis)
		millis >>= 8
	}
	if _, err := io.ReadFull(random, value[6:]); err != nil {
		return typedID[P]{}, NewError(CodeUnavailable, "id.generate", "random", "could not read UUID entropy", err)
	}
	value[6] = value[6]&0x0f | 0x70
	value[8] = value[8]&0x3f | 0x80
	return typedID[P]{value: value}, nil
}

func parseID[P idPrefix](text string) (typedID[P], error) {
	prefix := (*new(P)).prefix() + "_"
	if !strings.HasPrefix(text, prefix) {
		return typedID[P]{}, NewDetailedError(CodeInvalidID, "id.parse", "id", "has the wrong type prefix", map[string]string{"expected_prefix": strings.TrimSuffix(prefix, "_")}, nil)
	}
	value, err := parseUUIDv7(strings.TrimPrefix(text, prefix))
	if err != nil {
		return typedID[P]{}, err
	}
	id := typedID[P]{value: value}
	if id.String() != text {
		return typedID[P]{}, NewError(CodeInvalidID, "id.parse", "id", "must use canonical lower-case form", nil)
	}
	return id, nil
}

func parseUUIDv7(text string) (uuidV7, error) {
	var value uuidV7
	if len(text) != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return value, NewError(CodeInvalidID, "id.parse", "id", "must contain a canonical UUID", nil)
	}
	compact := strings.ReplaceAll(text, "-", "")
	if compact != strings.ToLower(compact) {
		return value, NewError(CodeInvalidID, "id.parse", "id", "must use lower-case hexadecimal", nil)
	}
	decoded, err := hex.DecodeString(compact)
	if err != nil || len(decoded) != len(value) {
		return value, NewError(CodeInvalidID, "id.parse", "id", "contains invalid UUID hexadecimal", err)
	}
	copy(value[:], decoded)
	if value[6]>>4 != 7 {
		return uuidV7{}, NewError(CodeInvalidID, "id.parse", "id", "must be UUID version 7", nil)
	}
	if value[8]>>6 != 2 {
		return uuidV7{}, NewError(CodeInvalidID, "id.parse", "id", "must use the RFC 4122 variant", nil)
	}
	return value, nil
}

func (id typedID[P]) IsZero() bool { return id.value == uuidV7{} }
func (id typedID[P]) UUID() string {
	if id.IsZero() {
		return ""
	}
	return formatUUID(id.value)
}
func (id typedID[P]) String() string {
	if id.IsZero() {
		return ""
	}
	return (*new(P)).prefix() + "_" + formatUUID(id.value)
}
func (id typedID[P]) MarshalText() ([]byte, error) {
	if id.IsZero() {
		return nil, NewError(CodeInvalidID, "id.marshal", "id", "must not be zero", nil)
	}
	return []byte(id.String()), nil
}
func (id *typedID[P]) UnmarshalText(text []byte) error {
	parsed, err := parseID[P](string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}
func (id typedID[P]) MarshalJSON() ([]byte, error) {
	if id.IsZero() {
		return nil, NewError(CodeInvalidID, "id.marshal", "id", "must not be zero", nil)
	}
	return json.Marshal(id.String())
}
func (id *typedID[P]) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return NewError(CodeInvalidID, "id.unmarshal", "id", "must be a string", err)
	}
	return id.UnmarshalText([]byte(text))
}

func formatUUID(value uuidV7) string {
	var encoded [36]byte
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])
	return string(encoded[:])
}

func NewResearchSessionID() (ResearchSessionID, error) {
	id, err := defaultID[researchSessionPrefix]()
	return ResearchSessionID{id}, err
}
func NewLeaseID() (LeaseID, error) { id, err := defaultID[leasePrefix](); return LeaseID{id}, err }
func NewAgentWorkspaceID() (AgentWorkspaceID, error) {
	id, err := defaultID[agentWorkspacePrefix]()
	return AgentWorkspaceID{id}, err
}
func NewExecID() (ExecID, error)     { id, err := defaultID[execPrefix](); return ExecID{id}, err }
func NewTargetID() (TargetID, error) { id, err := defaultID[targetPrefix](); return TargetID{id}, err }
func NewTargetRunID() (TargetRunID, error) {
	id, err := defaultID[targetRunPrefix]()
	return TargetRunID{id}, err
}
func NewTargetOperationID() (TargetOperationID, error) {
	id, err := defaultID[targetOperationPrefix]()
	return TargetOperationID{id}, err
}
func NewWorkspaceID() (WorkspaceID, error) {
	id, err := defaultID[workspacePrefix]()
	return WorkspaceID{id}, err
}
func NewIncidentID() (IncidentID, error) {
	id, err := defaultID[incidentPrefix]()
	return IncidentID{id}, err
}
func NewCaptureID() (CaptureID, error) {
	id, err := defaultID[capturePrefix]()
	return CaptureID{id}, err
}
func NewObservationBundleID() (ObservationBundleID, error) {
	id, err := defaultID[bundlePrefix]()
	return ObservationBundleID{id}, err
}
func NewExportID() (ExportID, error) { id, err := defaultID[exportPrefix](); return ExportID{id}, err }
func NewEventID() (EventID, error)   { id, err := defaultID[eventPrefix](); return EventID{id}, err }
func NewCorrelationID() (CorrelationID, error) {
	id, err := defaultID[correlationPrefix]()
	return CorrelationID{id}, err
}
func NewCollectorID() (CollectorID, error) {
	id, err := defaultID[collectorPrefix]()
	return CollectorID{id}, err
}
func NewSubjectID() (SubjectID, error) {
	id, err := defaultID[subjectPrefix]()
	return SubjectID{id}, err
}

func ParseResearchSessionID(v string) (ResearchSessionID, error) {
	id, err := parseID[researchSessionPrefix](v)
	return ResearchSessionID{id}, err
}
func ParseLeaseID(v string) (LeaseID, error) {
	id, err := parseID[leasePrefix](v)
	return LeaseID{id}, err
}
func ParseAgentWorkspaceID(v string) (AgentWorkspaceID, error) {
	id, err := parseID[agentWorkspacePrefix](v)
	return AgentWorkspaceID{id}, err
}
func ParseExecID(v string) (ExecID, error) { id, err := parseID[execPrefix](v); return ExecID{id}, err }
func ParseTargetID(v string) (TargetID, error) {
	id, err := parseID[targetPrefix](v)
	return TargetID{id}, err
}
func ParseTargetRunID(v string) (TargetRunID, error) {
	id, err := parseID[targetRunPrefix](v)
	return TargetRunID{id}, err
}
func ParseTargetOperationID(v string) (TargetOperationID, error) {
	id, err := parseID[targetOperationPrefix](v)
	return TargetOperationID{id}, err
}
func ParseWorkspaceID(v string) (WorkspaceID, error) {
	id, err := parseID[workspacePrefix](v)
	return WorkspaceID{id}, err
}
func ParseIncidentID(v string) (IncidentID, error) {
	id, err := parseID[incidentPrefix](v)
	return IncidentID{id}, err
}
func ParseCaptureID(v string) (CaptureID, error) {
	id, err := parseID[capturePrefix](v)
	return CaptureID{id}, err
}
func ParseObservationBundleID(v string) (ObservationBundleID, error) {
	id, err := parseID[bundlePrefix](v)
	return ObservationBundleID{id}, err
}
func ParseExportID(v string) (ExportID, error) {
	id, err := parseID[exportPrefix](v)
	return ExportID{id}, err
}
func ParseEventID(v string) (EventID, error) {
	id, err := parseID[eventPrefix](v)
	return EventID{id}, err
}
func ParseCorrelationID(v string) (CorrelationID, error) {
	id, err := parseID[correlationPrefix](v)
	return CorrelationID{id}, err
}
func ParseCollectorID(v string) (CollectorID, error) {
	id, err := parseID[collectorPrefix](v)
	return CollectorID{id}, err
}
func ParseSubjectID(v string) (SubjectID, error) {
	id, err := parseID[subjectPrefix](v)
	return SubjectID{id}, err
}

func (g *IDGenerator) ResearchSessionID() (ResearchSessionID, error) {
	id, err := generateID[researchSessionPrefix](g.now(), g.random)
	return ResearchSessionID{id}, err
}
func (g *IDGenerator) LeaseID() (LeaseID, error) {
	id, err := generateID[leasePrefix](g.now(), g.random)
	return LeaseID{id}, err
}
func (g *IDGenerator) AgentWorkspaceID() (AgentWorkspaceID, error) {
	id, err := generateID[agentWorkspacePrefix](g.now(), g.random)
	return AgentWorkspaceID{id}, err
}
func (g *IDGenerator) ExecID() (ExecID, error) {
	id, err := generateID[execPrefix](g.now(), g.random)
	return ExecID{id}, err
}
func (g *IDGenerator) TargetID() (TargetID, error) {
	id, err := generateID[targetPrefix](g.now(), g.random)
	return TargetID{id}, err
}
func (g *IDGenerator) TargetRunID() (TargetRunID, error) {
	id, err := generateID[targetRunPrefix](g.now(), g.random)
	return TargetRunID{id}, err
}
func (g *IDGenerator) TargetOperationID() (TargetOperationID, error) {
	id, err := generateID[targetOperationPrefix](g.now(), g.random)
	return TargetOperationID{id}, err
}
func (g *IDGenerator) WorkspaceID() (WorkspaceID, error) {
	id, err := generateID[workspacePrefix](g.now(), g.random)
	return WorkspaceID{id}, err
}
func (g *IDGenerator) IncidentID() (IncidentID, error) {
	id, err := generateID[incidentPrefix](g.now(), g.random)
	return IncidentID{id}, err
}
func (g *IDGenerator) CaptureID() (CaptureID, error) {
	id, err := generateID[capturePrefix](g.now(), g.random)
	return CaptureID{id}, err
}
func (g *IDGenerator) ObservationBundleID() (ObservationBundleID, error) {
	id, err := generateID[bundlePrefix](g.now(), g.random)
	return ObservationBundleID{id}, err
}
func (g *IDGenerator) ExportID() (ExportID, error) {
	id, err := generateID[exportPrefix](g.now(), g.random)
	return ExportID{id}, err
}
func (g *IDGenerator) EventID() (EventID, error) {
	id, err := generateID[eventPrefix](g.now(), g.random)
	return EventID{id}, err
}
func (g *IDGenerator) CorrelationID() (CorrelationID, error) {
	id, err := generateID[correlationPrefix](g.now(), g.random)
	return CorrelationID{id}, err
}
func (g *IDGenerator) CollectorID() (CollectorID, error) {
	id, err := generateID[collectorPrefix](g.now(), g.random)
	return CollectorID{id}, err
}
func (g *IDGenerator) SubjectID() (SubjectID, error) {
	id, err := generateID[subjectPrefix](g.now(), g.random)
	return SubjectID{id}, err
}

// Digest is a typed SHA-256 value used for immutable content and evidence.
type Digest struct{ value [sha256.Size]byte }

func NewDigest(data []byte) Digest { return Digest{value: sha256.Sum256(data)} }
func ParseDigest(text string) (Digest, error) {
	var digest Digest
	const prefix = "sha256:"
	if !strings.HasPrefix(text, prefix) || len(text) != len(prefix)+sha256.Size*2 {
		return digest, NewError(CodeInvalidArgument, "digest.parse", "digest", "must be sha256:<64 lower-case hex characters>", nil)
	}
	hexText := strings.TrimPrefix(text, prefix)
	if hexText != strings.ToLower(hexText) {
		return digest, NewError(CodeInvalidArgument, "digest.parse", "digest", "must use lower-case hexadecimal", nil)
	}
	decoded, err := hex.DecodeString(hexText)
	if err != nil {
		return digest, NewError(CodeInvalidArgument, "digest.parse", "digest", "contains invalid hexadecimal", err)
	}
	copy(digest.value[:], decoded)
	if digest.IsZero() || digest.String() != text {
		return Digest{}, NewError(CodeInvalidArgument, "digest.parse", "digest", "must be a canonical non-zero SHA-256 digest", nil)
	}
	return digest, nil
}
func (d Digest) IsZero() bool { return d.value == [sha256.Size]byte{} }
func (d Digest) String() string {
	if d.IsZero() {
		return ""
	}
	return "sha256:" + hex.EncodeToString(d.value[:])
}
func (d Digest) Bytes() [sha256.Size]byte { return d.value }
func (d Digest) MarshalText() ([]byte, error) {
	if d.IsZero() {
		return nil, NewError(CodeInvalidArgument, "digest.marshal", "digest", "must not be zero", nil)
	}
	return []byte(d.String()), nil
}
func (d *Digest) UnmarshalText(text []byte) error {
	parsed, err := ParseDigest(string(text))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

type InputViewID struct{ digest Digest }

func NewInputViewID(canonicalManifest []byte) InputViewID {
	framed := make([]byte, 0, len(canonicalManifest)+20)
	framed = append(framed, "world.input-view.v1\x00"...)
	framed = append(framed, canonicalManifest...)
	return InputViewID{digest: NewDigest(framed)}
}
func ParseInputViewID(text string) (InputViewID, error) {
	const prefix = "iv_"
	if !strings.HasPrefix(text, prefix) {
		return InputViewID{}, NewError(CodeInvalidID, "input_view_id.parse", "id", "must use iv_ prefix", nil)
	}
	digest, err := ParseDigest("sha256:" + strings.TrimPrefix(text, prefix))
	if err != nil {
		return InputViewID{}, NewError(CodeInvalidID, "input_view_id.parse", "id", "contains an invalid digest", err)
	}
	id := InputViewID{digest: digest}
	if id.String() != text {
		return InputViewID{}, NewError(CodeInvalidID, "input_view_id.parse", "id", "must use canonical lower-case form", nil)
	}
	return id, nil
}
func (id InputViewID) IsZero() bool { return id.digest.IsZero() }
func (id InputViewID) String() string {
	if id.IsZero() {
		return ""
	}
	return "iv_" + strings.TrimPrefix(id.digest.String(), "sha256:")
}
func (id InputViewID) Digest() Digest { return id.digest }
func (id InputViewID) MarshalText() ([]byte, error) {
	if id.IsZero() {
		return nil, NewError(CodeInvalidID, "input_view_id.marshal", "id", "must not be zero", nil)
	}
	return []byte(id.String()), nil
}
func (id *InputViewID) UnmarshalText(text []byte) error {
	parsed, err := ParseInputViewID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// ObservationCursor is a durable, ordered ledger position. Zero means before
// the first accepted record and is valid for initial subscriptions.
type ObservationCursor uint64

func (c ObservationCursor) Next() (ObservationCursor, error) {
	if c == ObservationCursor(^uint64(0)) {
		return 0, NewError(CodeResourceExhausted, "cursor.next", "cursor", "has reached its maximum", nil)
	}
	return c + 1, nil
}
