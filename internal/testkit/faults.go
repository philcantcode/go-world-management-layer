package testkit

import (
	"errors"
	"fmt"
	"sync"
)

var ErrInjected = errors.New("injected test fault")

type FaultInjector struct {
	mu     sync.Mutex
	faults map[string][]error
	hits   map[string]int
}

func NewFaultInjector() *FaultInjector {
	return &FaultInjector{faults: make(map[string][]error), hits: make(map[string]int)}
}

func (f *FaultInjector) FailNext(point string, err error) {
	if point == "" {
		panic("blank fault point")
	}
	if err == nil {
		err = fmt.Errorf("%w at %s", ErrInjected, point)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[point] = append(f.faults[point], err)
}

func (f *FaultInjector) Check(point string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits[point]++
	queued := f.faults[point]
	if len(queued) == 0 {
		return nil
	}
	err := queued[0]
	if len(queued) == 1 {
		delete(f.faults, point)
	} else {
		f.faults[point] = queued[1:]
	}
	return err
}

func (f *FaultInjector) Hits(point string) int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[point]
}

func (f *FaultInjector) Pending() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, queued := range f.faults {
		total += len(queued)
	}
	return total
}
