package testkit

import (
	"fmt"
	"sort"
	"sync"
)

type Ownership struct {
	Kind  string
	ID    string
	Owner string
}

type OwnershipTracker struct {
	mu   sync.Mutex
	live map[string]Ownership
}

func NewOwnershipTracker() *OwnershipTracker {
	return &OwnershipTracker{live: make(map[string]Ownership)}
}

func ownershipKey(kind, id string) string { return kind + "\x00" + id }

func (t *OwnershipTracker) Acquire(kind, id, owner string) error {
	if t == nil {
		return nil
	}
	if kind == "" || id == "" || owner == "" {
		return fmt.Errorf("ownership kind, id, and owner are required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := ownershipKey(kind, id)
	if existing, found := t.live[key]; found {
		if existing.Owner == owner {
			return nil
		}
		return fmt.Errorf("%s %s already owned by %s", kind, id, existing.Owner)
	}
	t.live[key] = Ownership{Kind: kind, ID: id, Owner: owner}
	return nil
}

func (t *OwnershipTracker) Release(kind, id, owner string) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := ownershipKey(kind, id)
	existing, found := t.live[key]
	if !found {
		return fmt.Errorf("%s %s is not owned", kind, id)
	}
	if existing.Owner != owner {
		return fmt.Errorf("%s %s is owned by %s, not %s", kind, id, existing.Owner, owner)
	}
	delete(t.live, key)
	return nil
}

func (t *OwnershipTracker) Snapshot() []Ownership {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]Ownership, 0, len(t.live))
	for _, ownership := range t.live {
		result = append(result, ownership)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Kind+result[i].ID < result[j].Kind+result[j].ID
	})
	return result
}

func (t *OwnershipTracker) RequireNoLeaks() error {
	live := t.Snapshot()
	if len(live) > 0 {
		return fmt.Errorf("leaked ownership: %+v", live)
	}
	return nil
}
