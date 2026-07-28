// Package forensicartifacts adapts an immutable forensic repository to the
// world-owned material authority port. Repository credentials and physical
// paths remain behind Backend.
package forensicartifacts

import (
	"context"
	"io"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// ResolvePurpose tells the repository why an occurrence is being resolved so
// its authorization policy can distinguish manifest projection from byte
// access.
type ResolvePurpose string

const (
	ResolveForInputView ResolvePurpose = "input-view"
	ResolveForRead      ResolvePurpose = "content-read"
)

// RepositoryOccurrence is public repository metadata. It deliberately has no
// filesystem or storage-location field.
type RepositoryOccurrence struct {
	Reference     string
	Digest        domain.Digest
	Size          int64
	SecurityScope string
	Sidecars      []string
}

type ResolveOccurrenceRequest struct {
	SecurityScope string
	Reference     string
	Purpose       ResolvePurpose
}

type OpenObjectRequest struct {
	SecurityScope string
	Reference     string
}

// OpenedObject couples a stream with the identity the repository authorized.
// The adapter compares this identity with the separately resolved occurrence
// before returning any bytes.
type OpenedObject struct {
	Occurrence RepositoryOccurrence
	Reader     io.ReadCloser
}

type RoleBinding struct {
	Role        string
	Sensitivity domain.Sensitivity
}

type OutputCaptureItem struct {
	LogicalPath string
	Content     ports.ContentSource
	Roles       []RoleBinding
	Provenance  map[string]string
}

// OutputCaptureRequest identifies a workspace structurally. There is no host
// path or caller-selected destination in this contract.
type OutputCaptureRequest struct {
	IdempotencyKey   string
	SecurityScope    string
	LeaseID          domain.LeaseID
	WorkspaceID      domain.WorkspaceID
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	Items            []OutputCaptureItem
}

type PublishedOutput struct {
	LogicalPath string
	Occurrence  RepositoryOccurrence
	Roles       []RoleBinding
	Provenance  map[string]string
	Verified    bool
}

type BundleCaptureRequest struct {
	IdempotencyKey   string
	SecurityScope    string
	BundleID         domain.ObservationBundleID
	TargetRunID      domain.TargetRunID
	TargetID         domain.TargetID
	TargetGeneration domain.TargetGeneration
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	Content          ports.ContentSource
	Role             RoleBinding
	Provenance       map[string]string
}

type PublishedBundle struct {
	Occurrence RepositoryOccurrence
	Role       RoleBinding
	Provenance map[string]string
	Verified   bool
}

// Backend is the narrow seam implemented by the go-forensic-artifacts binding.
// Every method MUST authenticate its context and authorize the complete request
// atomically with the repository operation. Implementations return only opaque
// qualified references and streams, never physical repository paths.
type Backend interface {
	ResolveOccurrence(context.Context, ResolveOccurrenceRequest) (RepositoryOccurrence, error)
	OpenObject(context.Context, OpenObjectRequest) (OpenedObject, error)
	CaptureOutputs(context.Context, OutputCaptureRequest) ([]PublishedOutput, error)
	CaptureObservationBundle(context.Context, BundleCaptureRequest) (PublishedBundle, error)
}
