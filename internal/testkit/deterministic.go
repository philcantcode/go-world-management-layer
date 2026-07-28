// Package testkit provides deterministic, contract-complete fake ports for
// unit, race, fault-injection, and adapter contract tests.
package testkit

import (
	"io"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type Clock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewClock(start time.Time) *Clock {
	if start.IsZero() {
		start = time.Unix(1_700_000_000, 0).UTC()
	}
	return &Clock{now: start.UTC()}
}

func (c *Clock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *Clock) Advance(delta time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if delta < 0 {
		panic("testkit.Clock cannot move backwards")
	}
	c.now = c.now.Add(delta)
	return c.now
}

func (c *Clock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.Before(c.now) {
		panic("testkit.Clock cannot move backwards")
	}
	c.now = at.UTC()
}

type deterministicReader struct {
	mu      sync.Mutex
	counter byte
}

func (r *deterministicReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := range buffer {
		r.counter++
		buffer[index] = r.counter
	}
	return len(buffer), nil
}

var _ io.Reader = (*deterministicReader)(nil)

func NewIDGenerator(clock *Clock) *domain.IDGenerator {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	generator, err := domain.NewIDGenerator(clock.Now, &deterministicReader{})
	if err != nil {
		panic(err)
	}
	return generator
}
