package ports

import (
	"context"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type ResourceOwnerKind string

const (
	ResourceLease          ResourceOwnerKind = "lease"
	ResourceAgentWorkspace ResourceOwnerKind = "agent_workspace"
	ResourceTarget         ResourceOwnerKind = "target"
	ResourceObserver       ResourceOwnerKind = "observer"
)

func (k ResourceOwnerKind) IsValid() bool {
	return k == ResourceLease || k == ResourceAgentWorkspace || k == ResourceTarget || k == ResourceObserver
}

type ResourcePlan struct {
	IdempotencyKey string
	LeaseID        domain.LeaseID
	OwnerKind      ResourceOwnerKind
	OwnerID        string
	ParentOwnerID  string
	Requests       admission.Resources
	Limits         admission.Resources
}

func (p ResourcePlan) Validate() error {
	const operation = "ports.resource_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.LeaseID.IsZero() || !p.OwnerKind.IsValid() || p.OwnerID == "" {
		return domain.NewError(domain.CodeInvalidArgument, operation, "owner", "lease, owner kind, and owner ID are required", nil)
	}
	if p.OwnerKind != ResourceLease && p.ParentOwnerID == "" {
		return domain.NewError(domain.CodeInvalidArgument, operation, "parent_owner_id", "is required for non-lease resources", nil)
	}
	if err := p.Requests.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "requests", "is invalid", err)
	}
	if err := p.Limits.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "limits", "is invalid", err)
	}
	if p.Requests.IsZero() || !p.Requests.FitsWithin(p.Limits) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "requests", "must be nonzero and fit within limits", nil)
	}
	return nil
}

type ResourceStatus struct {
	LeaseID    domain.LeaseID
	OwnerKind  ResourceOwnerKind
	OwnerID    string
	CgroupID   string
	Limits     admission.Resources
	Usage      admission.Resources
	Pressure   admission.Pressure
	ObservedAt time.Time
}

type ResourceController interface {
	Probe(context.Context) (domain.CapabilityFingerprint, error)
	Reserve(context.Context, ResourcePlan) (ResourceStatus, error)
	Apply(context.Context, ResourcePlan) (ResourceStatus, error)
	Inspect(context.Context, string) (ResourceStatus, error)
	Release(context.Context, string) error
}
