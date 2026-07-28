package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type fakeResourceRecord struct {
	plan   ports.ResourcePlan
	status ports.ResourceStatus
	owner  string
}

type FakeResourceController struct {
	mu           sync.Mutex
	capabilities domain.CapabilityFingerprint
	clock        *Clock
	faults       *FaultInjector
	tracker      *OwnershipTracker
	requests     map[string]string
	results      map[string]ports.ResourceStatus
	resources    map[string]*fakeResourceRecord
}

func NewFakeResourceController(capabilities domain.CapabilityFingerprint, clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeResourceController {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeResourceController{
		capabilities: capabilities, clock: clock, faults: faults, tracker: tracker,
		requests: make(map[string]string), results: make(map[string]ports.ResourceStatus), resources: make(map[string]*fakeResourceRecord),
	}
}

func (c *FakeResourceController) Probe(ctx context.Context) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "fake_resources.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := c.faults.Check("resources.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	return c.capabilities, nil
}

func (c *FakeResourceController) Reserve(ctx context.Context, plan ports.ResourcePlan) (ports.ResourceStatus, error) {
	return c.upsert(ctx, "reserve", plan, false)
}

func (c *FakeResourceController) Apply(ctx context.Context, plan ports.ResourcePlan) (ports.ResourceStatus, error) {
	return c.upsert(ctx, "apply", plan, true)
}

func (c *FakeResourceController) upsert(ctx context.Context, action string, plan ports.ResourcePlan, active bool) (ports.ResourceStatus, error) {
	operation := "fake_resources." + action
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.ResourceStatus{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.ResourceStatus{}, err
	}
	if err := c.faults.Check("resources." + action + ".before"); err != nil {
		return ports.ResourceStatus{}, err
	}
	signature := resourcePlanSignature(plan)
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, found := c.requests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.ResourceStatus{}, idempotencyConflict(operation)
		}
		return cloneResourceStatus(c.results[plan.IdempotencyKey]), nil
	}
	record, exists := c.resources[plan.OwnerID]
	if action == "reserve" && exists {
		return ports.ResourceStatus{}, domain.NewError(domain.CodeAlreadyExists, operation, "owner_id", "resource already exists", nil)
	}
	if action == "apply" && !exists {
		return ports.ResourceStatus{}, domain.NewError(domain.CodeFailedPrecondition, operation, "owner_id", "resource must be reserved first", nil)
	}
	if plan.OwnerKind != ports.ResourceLease {
		if _, found := c.resources[plan.ParentOwnerID]; !found {
			return ports.ResourceStatus{}, domain.NewError(domain.CodeFailedPrecondition, operation, "parent_owner_id", "parent resource is not present", nil)
		}
	}
	owner := plan.LeaseID.String()
	if !exists {
		if err := c.tracker.Acquire("resource", plan.OwnerID, owner); err != nil {
			return ports.ResourceStatus{}, err
		}
		record = &fakeResourceRecord{owner: owner}
		c.resources[plan.OwnerID] = record
	}
	usage := admission.Resources{}
	if active {
		usage = plan.Requests.Clone()
	}
	status := ports.ResourceStatus{
		LeaseID: plan.LeaseID, OwnerKind: plan.OwnerKind, OwnerID: plan.OwnerID,
		CgroupID: "cgroup-" + plan.OwnerID, Limits: plan.Limits.Clone(), Usage: usage,
		Pressure: admission.Pressure{FreeDiskPercent: 100, FreeInodesPercent: 100}, ObservedAt: c.clock.Now(),
	}
	record.plan = cloneResourcePlan(plan)
	record.status = status
	c.requests[plan.IdempotencyKey] = signature
	c.results[plan.IdempotencyKey] = status
	if err := c.faults.Check("resources." + action + ".after"); err != nil {
		return ports.ResourceStatus{}, err
	}
	return cloneResourceStatus(status), nil
}

func (c *FakeResourceController) Inspect(ctx context.Context, ownerID string) (ports.ResourceStatus, error) {
	if err := ports.RequireDeadline(ctx, "fake_resources.inspect"); err != nil {
		return ports.ResourceStatus{}, err
	}
	if ownerID == "" {
		return ports.ResourceStatus{}, domain.NewError(domain.CodeInvalidArgument, "fake_resources.inspect", "owner_id", "must not be blank", nil)
	}
	if err := c.faults.Check("resources.inspect"); err != nil {
		return ports.ResourceStatus{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, found := c.resources[ownerID]
	if !found {
		return ports.ResourceStatus{}, domain.NewError(domain.CodeNotFound, "fake_resources.inspect", "owner_id", "not found", nil)
	}
	return cloneResourceStatus(record.status), nil
}

func (c *FakeResourceController) Release(ctx context.Context, ownerID string) error {
	if err := ports.RequireDeadline(ctx, "fake_resources.release"); err != nil {
		return err
	}
	if ownerID == "" {
		return domain.NewError(domain.CodeInvalidArgument, "fake_resources.release", "owner_id", "must not be blank", nil)
	}
	if err := c.faults.Check("resources.release"); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, found := c.resources[ownerID]
	if !found {
		return nil
	}
	for childID, child := range c.resources {
		if child.plan.ParentOwnerID == ownerID {
			return domain.NewDetailedError(domain.CodeFailedPrecondition, "fake_resources.release", "owner_id", "resource still has a child", map[string]string{"child_id": childID}, nil)
		}
	}
	delete(c.resources, ownerID)
	return c.tracker.Release("resource", ownerID, record.owner)
}

func resourcePlanSignature(plan ports.ResourcePlan) string {
	return fmt.Sprintf("%s/%s/%s/%s/%+v/%+v", plan.LeaseID, plan.OwnerKind, plan.OwnerID, plan.ParentOwnerID, plan.Requests, plan.Limits)
}

func cloneResourcePlan(plan ports.ResourcePlan) ports.ResourcePlan {
	plan.Requests = plan.Requests.Clone()
	plan.Limits = plan.Limits.Clone()
	return plan
}

func cloneResourceStatus(status ports.ResourceStatus) ports.ResourceStatus {
	status.Limits = status.Limits.Clone()
	status.Usage = status.Usage.Clone()
	return status
}

var _ ports.ResourceController = (*FakeResourceController)(nil)
