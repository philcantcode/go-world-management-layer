package domain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"time"
)

type InputViewEntrySpec struct {
	LogicalPath       string
	OccurrenceRef     string
	Digest            Digest
	Size              int64
	Mode              uint32
	PermittedSidecars []string
}

type InputViewEntry struct{ spec InputViewEntrySpec }

func NewInputViewEntry(spec InputViewEntrySpec) (InputViewEntry, error) {
	if err := requireRelativePath("logical_path", spec.LogicalPath, false); err != nil {
		return InputViewEntry{}, err
	}
	if err := requireNonBlank("occurrence_ref", spec.OccurrenceRef); err != nil {
		return InputViewEntry{}, err
	}
	if spec.Digest.IsZero() {
		return InputViewEntry{}, NewError(CodeInvalidArgument, "input_view_entry.new", "digest", "must be set", nil)
	}
	if err := requireNonNegative("size", spec.Size); err != nil {
		return InputViewEntry{}, err
	}
	if spec.Mode == 0 {
		spec.Mode = 0o444
	}
	if spec.Mode > 0o777 {
		return InputViewEntry{}, NewError(CodeInvalidArgument, "input_view_entry.new", "mode", "must contain only regular Unix permission bits", nil)
	}
	sidecars, err := uniqueNonBlank(spec.PermittedSidecars, "permitted_sidecars")
	if err != nil {
		return InputViewEntry{}, err
	}
	sort.Strings(sidecars)
	spec.PermittedSidecars = sidecars
	return InputViewEntry{spec: spec}, nil
}

func (e InputViewEntry) Spec() InputViewEntrySpec {
	result := e.spec
	result.PermittedSidecars = cloneSlice(e.spec.PermittedSidecars)
	return result
}
func (e InputViewEntry) LogicalPath() string { return e.spec.LogicalPath }
func (e InputViewEntry) Digest() Digest      { return e.spec.Digest }

type InputViewManifest struct {
	id      InputViewID
	entries []InputViewEntry
}

func NewInputViewManifest(entries []InputViewEntry) (InputViewManifest, error) {
	if len(entries) == 0 {
		return InputViewManifest{}, NewError(CodeInvalidArgument, "input_view_manifest.new", "entries", "must not be empty", nil)
	}
	owned := cloneSlice(entries)
	sort.Slice(owned, func(i, j int) bool { return owned[i].spec.LogicalPath < owned[j].spec.LogicalPath })
	seenPaths := make(map[string]struct{}, len(owned))
	for i, entry := range owned {
		if entry.spec.LogicalPath == "" || entry.spec.Digest.IsZero() {
			return InputViewManifest{}, NewError(CodeInvalidArgument, "input_view_manifest.new", fmt.Sprintf("entries[%d]", i), "must be constructed and valid", nil)
		}
		if _, duplicate := seenPaths[entry.spec.LogicalPath]; duplicate {
			return InputViewManifest{}, NewError(CodeConflict, "input_view_manifest.new", "entries", "contains duplicate logical path "+entry.spec.LogicalPath, nil)
		}
		for parent := path.Dir(entry.spec.LogicalPath); parent != "."; parent = path.Dir(parent) {
			if _, conflict := seenPaths[parent]; conflict {
				return InputViewManifest{}, NewError(CodeConflict, "input_view_manifest.new", "entries", "a file path cannot be an ancestor of another entry", nil)
			}
		}
		seenPaths[entry.spec.LogicalPath] = struct{}{}
	}
	canonical := canonicalInputViewManifest(owned)
	return InputViewManifest{id: NewInputViewID(canonical), entries: owned}, nil
}

func canonicalInputViewManifest(entries []InputViewEntry) []byte {
	var buffer bytes.Buffer
	writeCanonicalString(&buffer, "world.input-view-manifest.v1")
	_ = binary.Write(&buffer, binary.BigEndian, uint32(len(entries)))
	for _, entry := range entries {
		writeCanonicalString(&buffer, entry.spec.LogicalPath)
		writeCanonicalString(&buffer, entry.spec.OccurrenceRef)
		writeCanonicalString(&buffer, entry.spec.Digest.String())
		_ = binary.Write(&buffer, binary.BigEndian, entry.spec.Size)
		_ = binary.Write(&buffer, binary.BigEndian, entry.spec.Mode)
		_ = binary.Write(&buffer, binary.BigEndian, uint32(len(entry.spec.PermittedSidecars)))
		for _, sidecar := range entry.spec.PermittedSidecars {
			writeCanonicalString(&buffer, sidecar)
		}
	}
	return buffer.Bytes()
}

func (m InputViewManifest) ID() InputViewID           { return m.id }
func (m InputViewManifest) Entries() []InputViewEntry { return cloneSlice(m.entries) }
func (m InputViewManifest) Len() int                  { return len(m.entries) }

type InputViewConstruction string

const (
	InputViewRequireReflink InputViewConstruction = "require-reflink"
	InputViewAllowCopy      InputViewConstruction = "allow-copy"
)

func (c InputViewConstruction) IsValid() bool {
	return c == InputViewRequireReflink || c == InputViewAllowCopy
}

type InputViewSpec struct {
	Manifest      InputViewManifest
	SecurityScope string
	Construction  InputViewConstruction
	CreatedAt     time.Time
}

type InputView struct {
	spec      InputViewSpec
	state     InputViewState
	revision  Revision
	updatedAt time.Time
}

func NewInputView(spec InputViewSpec) (InputView, error) {
	if spec.Manifest.id.IsZero() || len(spec.Manifest.entries) == 0 {
		return InputView{}, NewError(CodeInvalidArgument, "input_view.new", "manifest", "must be initialized", nil)
	}
	if err := requireNonBlank("security_scope", spec.SecurityScope); err != nil {
		return InputView{}, err
	}
	if !spec.Construction.IsValid() {
		return InputView{}, NewError(CodeInvalidArgument, "input_view.new", "construction", "is not recognized", nil)
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return InputView{}, err
	}
	spec.Manifest.entries = cloneSlice(spec.Manifest.entries)
	return InputView{spec: spec, state: InputViewBuilding, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}

func (v InputView) Spec() InputViewSpec {
	result := v.spec
	result.Manifest.entries = cloneSlice(v.spec.Manifest.entries)
	return result
}
func (v InputView) ID() InputViewID       { return v.spec.Manifest.id }
func (v InputView) State() InputViewState { return v.state }
func (v InputView) Revision() Revision    { return v.revision }
func (v InputView) Transition(next InputViewState, expected Revision, at time.Time) (InputView, error) {
	if err := RequireInputViewTransition(v.state, next); err != nil {
		return InputView{}, err
	}
	revision, err := nextModelRevision(v.revision, expected, v.updatedAt, at)
	if err != nil {
		return InputView{}, err
	}
	v.state, v.revision, v.updatedAt = next, revision, at
	return v, nil
}

type WorkspaceSpec struct {
	ID               WorkspaceID
	LeaseID          LeaseID
	AgentWorkspaceID AgentWorkspaceID
	AgentGeneration  AgentGeneration
	InputViewID      InputViewID
	CreatedAt        time.Time
}

type Workspace struct {
	spec      WorkspaceSpec
	state     WorkspaceState
	revision  Revision
	updatedAt time.Time
}

func NewWorkspace(spec WorkspaceSpec) (Workspace, error) {
	if err := requireID("workspace_id", spec.ID); err != nil {
		return Workspace{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return Workspace{}, err
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return Workspace{}, err
	}
	if !spec.AgentGeneration.IsValid() {
		return Workspace{}, NewError(CodeInvalidArgument, "workspace.new", "agent_generation", "must be positive", nil)
	}
	if spec.InputViewID.IsZero() {
		return Workspace{}, NewError(CodeInvalidID, "workspace.new", "input_view_id", "must be set", nil)
	}
	if err := requireTime("created_at", spec.CreatedAt); err != nil {
		return Workspace{}, err
	}
	return Workspace{spec: spec, state: WorkspacePreparing, revision: InitialRevision, updatedAt: spec.CreatedAt}, nil
}

func (w Workspace) Spec() WorkspaceSpec   { return w.spec }
func (w Workspace) ID() WorkspaceID       { return w.spec.ID }
func (w Workspace) State() WorkspaceState { return w.state }
func (w Workspace) Revision() Revision    { return w.revision }
func (w Workspace) Transition(next WorkspaceState, expected Revision, at time.Time) (Workspace, error) {
	if err := RequireWorkspaceTransition(w.state, next); err != nil {
		return Workspace{}, err
	}
	revision, err := nextModelRevision(w.revision, expected, w.updatedAt, at)
	if err != nil {
		return Workspace{}, err
	}
	w.state, w.revision, w.updatedAt = next, revision, at
	return w, nil
}

type ExportSelectionSpec struct {
	RelativePath string
	Roles        []string
}
type ExportSelection struct{ spec ExportSelectionSpec }

func NewExportSelection(spec ExportSelectionSpec) (ExportSelection, error) {
	if err := requireRelativePath("relative_path", spec.RelativePath, false); err != nil {
		return ExportSelection{}, err
	}
	roles, err := uniqueNonBlank(spec.Roles, "roles")
	if err != nil {
		return ExportSelection{}, err
	}
	if len(roles) == 0 {
		return ExportSelection{}, NewError(CodeInvalidArgument, "export_selection.new", "roles", "must not be empty", nil)
	}
	sort.Strings(roles)
	spec.Roles = roles
	return ExportSelection{spec: spec}, nil
}
func (s ExportSelection) Spec() ExportSelectionSpec {
	result := s.spec
	result.Roles = cloneSlice(s.spec.Roles)
	return result
}

type ExportSpec struct {
	ID                ExportID
	LeaseID           LeaseID
	WorkspaceID       WorkspaceID
	AgentWorkspaceID  AgentWorkspaceID
	AgentGeneration   AgentGeneration
	Selections        []ExportSelection
	WorkspaceRevision Revision
	DeclaredAt        time.Time
}

type Export struct {
	spec      ExportSpec
	state     ExportState
	revision  Revision
	updatedAt time.Time
	artifacts []ArtifactReference
}

func NewExport(spec ExportSpec) (Export, error) {
	if err := requireID("export_id", spec.ID); err != nil {
		return Export{}, err
	}
	if err := requireID("lease_id", spec.LeaseID); err != nil {
		return Export{}, err
	}
	if err := requireID("workspace_id", spec.WorkspaceID); err != nil {
		return Export{}, err
	}
	if err := requireID("agent_workspace_id", spec.AgentWorkspaceID); err != nil {
		return Export{}, err
	}
	if !spec.AgentGeneration.IsValid() {
		return Export{}, NewError(CodeInvalidArgument, "export.new", "agent_generation", "must be positive", nil)
	}
	if !spec.WorkspaceRevision.IsValid() {
		return Export{}, NewError(CodeInvalidArgument, "export.new", "workspace_revision", "must be positive", nil)
	}
	if len(spec.Selections) == 0 {
		return Export{}, NewError(CodeInvalidArgument, "export.new", "selections", "must not be empty", nil)
	}
	if err := requireTime("declared_at", spec.DeclaredAt); err != nil {
		return Export{}, err
	}
	owned := cloneSlice(spec.Selections)
	sort.Slice(owned, func(i, j int) bool { return owned[i].spec.RelativePath < owned[j].spec.RelativePath })
	for i := range owned {
		if owned[i].spec.RelativePath == "" || len(owned[i].spec.Roles) == 0 {
			return Export{}, NewError(CodeInvalidArgument, "export.new", fmt.Sprintf("selections[%d]", i), "must be constructed and valid", nil)
		}
		if i > 0 && owned[i-1].spec.RelativePath == owned[i].spec.RelativePath {
			return Export{}, NewError(CodeConflict, "export.new", "selections", "contains duplicate path "+owned[i].spec.RelativePath, nil)
		}
	}
	spec.Selections = owned
	return Export{spec: spec, state: ExportDeclared, revision: InitialRevision, updatedAt: spec.DeclaredAt}, nil
}

func (e Export) Spec() ExportSpec {
	result := e.spec
	result.Selections = cloneSlice(e.spec.Selections)
	return result
}
func (e Export) ID() ExportID                   { return e.spec.ID }
func (e Export) State() ExportState             { return e.state }
func (e Export) Revision() Revision             { return e.revision }
func (e Export) Artifacts() []ArtifactReference { return cloneSlice(e.artifacts) }
func (e Export) Transition(next ExportState, expected Revision, at time.Time) (Export, error) {
	if err := RequireExportTransition(e.state, next); err != nil {
		return Export{}, err
	}
	revision, err := nextModelRevision(e.revision, expected, e.updatedAt, at)
	if err != nil {
		return Export{}, err
	}
	e.state, e.revision, e.updatedAt = next, revision, at
	return e, nil
}
func (e Export) Commit(artifacts []ArtifactReference, expected Revision, at time.Time) (Export, error) {
	if e.state != ExportCommitting {
		return Export{}, NewError(CodeFailedPrecondition, "export.commit", "state", "must be committing", nil)
	}
	if len(artifacts) == 0 {
		return Export{}, NewError(CodeInvalidArgument, "export.commit", "artifacts", "must not be empty", nil)
	}
	for i, artifact := range artifacts {
		if artifact.spec.Reference == "" || artifact.spec.Digest.IsZero() {
			return Export{}, NewError(CodeInvalidArgument, "export.commit", fmt.Sprintf("artifacts[%d]", i), "must be constructed and valid", nil)
		}
	}
	revision, err := nextModelRevision(e.revision, expected, e.updatedAt, at)
	if err != nil {
		return Export{}, err
	}
	e.artifacts = cloneSlice(artifacts)
	e.state, e.revision, e.updatedAt = ExportCommitted, revision, at
	return e, nil
}
