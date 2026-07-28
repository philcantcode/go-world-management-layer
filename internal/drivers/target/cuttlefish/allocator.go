package cuttlefish

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type MemoryAllocator struct {
	mu          sync.Mutex
	next        int
	baseADBPort int
	allocations map[string]Allocation
	used        map[int]struct{}
}

func NewMemoryAllocator(firstInstance, baseADBPort int) (*MemoryAllocator, error) {
	if firstInstance <= 0 || baseADBPort <= 0 || baseADBPort > 65535 {
		return nil, fmt.Errorf("valid first instance and base ADB port are required")
	}
	return &MemoryAllocator{next: firstInstance, baseADBPort: baseADBPort, allocations: make(map[string]Allocation), used: make(map[int]struct{})}, nil
}

func (a *MemoryAllocator) Reserve(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration) (Allocation, error) {
	if err := ctx.Err(); err != nil {
		return Allocation{}, err
	}
	if id.IsZero() || !generation.IsValid() {
		return Allocation{}, fmt.Errorf("target and generation are required")
	}
	key := deviceKey(id, generation)
	a.mu.Lock()
	defer a.mu.Unlock()
	if allocation, found := a.allocations[key]; found {
		return allocation, nil
	}
	instance := a.next
	for {
		if _, used := a.used[instance]; !used {
			break
		}
		instance++
	}
	port := a.baseADBPort + instance - 1
	if port > 65535 {
		return Allocation{}, fmt.Errorf("ADB endpoint range exhausted")
	}
	allocation := Allocation{InstanceNumber: instance, InstanceName: "cvd-" + strconv.Itoa(instance), Serial: "127.0.0.1:" + strconv.Itoa(port), ADBAddress: "127.0.0.1:" + strconv.Itoa(port)}
	a.allocations[key] = allocation
	a.used[instance] = struct{}{}
	a.next = instance + 1
	return allocation, nil
}

func (a *MemoryAllocator) Release(ctx context.Context, allocation Allocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, current := range a.allocations {
		if current.InstanceNumber == allocation.InstanceNumber && current.Serial == allocation.Serial {
			delete(a.allocations, key)
			delete(a.used, current.InstanceNumber)
			return nil
		}
	}
	return nil
}

var _ Allocator = (*MemoryAllocator)(nil)

// FixedAllocator assigns one already-running emulator to one target generation
// at a time. It is intentionally incapable of inventing a replacement serial.
type FixedAllocator struct {
	mu         sync.Mutex
	allocation Allocation
	owner      string
}

func NewFixedAllocator(allocation Allocation) (*FixedAllocator, error) {
	if err := allocation.Validate(); err != nil {
		return nil, err
	}
	if !safeExactADBSerial(allocation.Serial) {
		return nil, fmt.Errorf("fixed allocation serial is unsafe")
	}
	return &FixedAllocator{allocation: allocation}, nil
}

func NewAttachedEmulatorAllocator(serial string) (*FixedAllocator, error) {
	if !safeExactADBSerial(serial) {
		return nil, fmt.Errorf("attached emulator serial is unsafe")
	}
	instanceNumber := 1
	if strings.HasPrefix(serial, "emulator-") {
		port, err := strconv.Atoi(strings.TrimPrefix(serial, "emulator-"))
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("attached emulator serial has an invalid console port")
		}
		instanceNumber = port
	}
	return NewFixedAllocator(Allocation{
		InstanceNumber: instanceNumber, InstanceName: "attached-" + strings.ReplaceAll(serial, ":", "-"),
		Serial: serial, ADBAddress: serial,
	})
}

func (a *FixedAllocator) Reserve(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration) (Allocation, error) {
	if err := ctx.Err(); err != nil {
		return Allocation{}, err
	}
	if id.IsZero() || !generation.IsValid() {
		return Allocation{}, fmt.Errorf("target and generation are required")
	}
	owner := deviceKey(id, generation)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.owner != "" && a.owner != owner {
		return Allocation{}, fmt.Errorf("attached emulator is already assigned to another target generation")
	}
	a.owner = owner
	return a.allocation, nil
}

func (a *FixedAllocator) Release(ctx context.Context, allocation Allocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if allocation != a.allocation {
		return fmt.Errorf("release does not identify the attached emulator allocation")
	}
	a.owner = ""
	return nil
}

var _ Allocator = (*FixedAllocator)(nil)
