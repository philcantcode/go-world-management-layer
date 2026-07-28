package testkit

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type FakeInputCache struct {
	mu       sync.Mutex
	clock    *Clock
	faults   *FaultInjector
	tracker  *OwnershipTracker
	contents map[string]ports.CachedContent
	views    map[string]ports.CachedInputView
	pins     map[string]ports.CachePin
}

func NewFakeInputCache(clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeInputCache {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeInputCache{
		clock: clock, faults: faults, tracker: tracker,
		contents: make(map[string]ports.CachedContent), views: make(map[string]ports.CachedInputView), pins: make(map[string]ports.CachePin),
	}
}

func (c *FakeInputCache) EnsureContent(ctx context.Context, plan ports.CacheContentPlan) (ports.CachedContent, error) {
	if plan.Reader != nil {
		defer plan.Reader.Close()
	}
	if err := ports.RequireDeadline(ctx, "fake_cache.ensure_content"); err != nil {
		return ports.CachedContent{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.CachedContent{}, err
	}
	if err := c.faults.Check("cache.ensure_content.before"); err != nil {
		return ports.CachedContent{}, err
	}
	key := cacheContentKey(plan.SecurityScope, plan.Occurrence.Digest)
	c.mu.Lock()
	if existing, found := c.contents[key]; found {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()
	content, err := io.ReadAll(io.LimitReader(plan.Reader, plan.Occurrence.Size+1))
	if err != nil {
		return ports.CachedContent{}, err
	}
	if int64(len(content)) != plan.Occurrence.Size || domain.NewDigest(content) != plan.Occurrence.Digest {
		return ports.CachedContent{}, domain.NewError(domain.CodeIntegrityViolation, "fake_cache.ensure_content", "reader", "bytes do not match the occurrence", nil)
	}
	result := ports.CachedContent{
		SecurityScope: plan.SecurityScope, Digest: plan.Occurrence.Digest, Size: plan.Occurrence.Size,
		PhysicalBytes: plan.Occurrence.Size, VerifiedAt: c.clock.Now(),
	}
	c.mu.Lock()
	c.contents[key] = result
	c.mu.Unlock()
	if err := c.faults.Check("cache.ensure_content.after"); err != nil {
		return ports.CachedContent{}, err
	}
	return result, nil
}

func (c *FakeInputCache) BuildView(ctx context.Context, plan ports.InputViewBuildPlan) (ports.CachedInputView, error) {
	if err := ports.RequireDeadline(ctx, "fake_cache.build_view"); err != nil {
		return ports.CachedInputView{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.CachedInputView{}, err
	}
	if err := c.faults.Check("cache.build_view.before"); err != nil {
		return ports.CachedInputView{}, err
	}
	key := cacheViewKey(plan.SecurityScope, plan.Manifest.ID())
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, found := c.views[key]; found {
		if existing.Construction != plan.Construction {
			return ports.CachedInputView{}, domain.NewError(domain.CodeConflict, "fake_cache.build_view", "construction", "view already uses another construction mode", nil)
		}
		return existing, nil
	}
	unique := make(map[domain.Digest]int64)
	for _, entry := range plan.Manifest.Entries() {
		spec := entry.Spec()
		content, found := c.contents[cacheContentKey(plan.SecurityScope, spec.Digest)]
		if !found || content.Size != spec.Size {
			return ports.CachedInputView{}, domain.NewError(domain.CodeFailedPrecondition, "fake_cache.build_view", "manifest", "content is missing from this security scope", nil)
		}
		unique[spec.Digest] = content.PhysicalBytes
	}
	var physicalBytes int64
	for _, size := range unique {
		physicalBytes += size
	}
	result := ports.CachedInputView{
		SecurityScope: plan.SecurityScope, InputViewID: plan.Manifest.ID(), Construction: plan.Construction,
		PhysicalBytes: physicalBytes, ReadyAt: c.clock.Now(),
	}
	c.views[key] = result
	if err := c.faults.Check("cache.build_view.after"); err != nil {
		return ports.CachedInputView{}, err
	}
	return result, nil
}

func (c *FakeInputCache) Pin(ctx context.Context, pin ports.CachePin) error {
	if err := ports.RequireDeadline(ctx, "fake_cache.pin"); err != nil {
		return err
	}
	if err := pin.Validate(); err != nil {
		return err
	}
	if err := c.faults.Check("cache.pin"); err != nil {
		return err
	}
	key := cachePinKey(pin)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, found := c.views[cacheViewKey(pin.SecurityScope, pin.InputViewID)]; !found {
		return domain.NewError(domain.CodeFailedPrecondition, "fake_cache.pin", "input_view_id", "view is not ready", nil)
	}
	if _, found := c.pins[key]; found {
		return nil
	}
	if err := c.tracker.Acquire("cache_pin", key, pin.Owner); err != nil {
		return err
	}
	c.pins[key] = pin
	return nil
}

func (c *FakeInputCache) Unpin(ctx context.Context, pin ports.CachePin) error {
	if err := ports.RequireDeadline(ctx, "fake_cache.unpin"); err != nil {
		return err
	}
	if err := pin.Validate(); err != nil {
		return err
	}
	if err := c.faults.Check("cache.unpin"); err != nil {
		return err
	}
	key := cachePinKey(pin)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, found := c.pins[key]; !found {
		return nil
	}
	delete(c.pins, key)
	return c.tracker.Release("cache_pin", key, pin.Owner)
}

func (c *FakeInputCache) Reconcile(ctx context.Context) (ports.CacheReconciliation, error) {
	if err := ports.RequireDeadline(ctx, "fake_cache.reconcile"); err != nil {
		return ports.CacheReconciliation{}, err
	}
	if err := c.faults.Check("cache.reconcile"); err != nil {
		return ports.CacheReconciliation{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return ports.CacheReconciliation{PinsRebuilt: len(c.pins), CompletedAt: c.clock.Now()}, nil
}

func cacheContentKey(scope string, digest domain.Digest) string {
	return scope + "\x00" + digest.String()
}
func cacheViewKey(scope string, id domain.InputViewID) string { return scope + "\x00" + id.String() }
func cachePinKey(pin ports.CachePin) string {
	return fmt.Sprintf("%s\x00%s\x00%s", pin.SecurityScope, pin.InputViewID, pin.Owner)
}

var _ ports.InputCache = (*FakeInputCache)(nil)
