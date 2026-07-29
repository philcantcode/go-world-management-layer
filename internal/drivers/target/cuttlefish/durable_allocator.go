package cuttlefish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/processlock"
)

const (
	durableAllocatorFilename = "android-emulator-allocations.json"
	durableAllocatorVersion  = 1
	maximumAllocatorFileSize = int64(4 << 20)
)

type DurableEmulatorAllocatorConfig struct {
	StateRoot        string
	FirstConsolePort int
	LastConsolePort  int
	ListenHost       string
}

// DurableEmulatorAllocator owns one lock-protected endpoint database for its
// lifetime. Close must be called during daemon shutdown to release ownership.
type DurableEmulatorAllocator struct {
	mu          sync.Mutex
	owner       *processlock.Owner
	path        string
	first       int
	last        int
	host        string
	closed      bool
	allocations map[string]durableAllocationEntry
}

type durableAllocatorState struct {
	Version     int                      `json:"version"`
	Allocations []durableAllocationEntry `json:"allocations"`
}

type durableAllocationEntry struct {
	TargetID   string     `json:"target_id"`
	Generation uint64     `json:"generation"`
	Allocation Allocation `json:"allocation"`
}

func NewDurableEmulatorAllocator(config DurableEmulatorAllocatorConfig) (*DurableEmulatorAllocator, error) {
	if strings.TrimSpace(config.StateRoot) == "" {
		return nil, fmt.Errorf("durable emulator allocation state root is required")
	}
	if config.ListenHost == "" {
		config.ListenHost = "127.0.0.1"
	}
	if ip := net.ParseIP(config.ListenHost); ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("durable emulator allocator listen host must be a literal loopback address")
	}
	if err := validateConsolePortRange(config.FirstConsolePort, config.LastConsolePort); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(config.StateRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(root, durableAllocatorFilename)
	owner, err := processlock.Acquire(path)
	if err != nil {
		return nil, err
	}
	allocator := &DurableEmulatorAllocator{
		owner: owner, path: owner.ControlPath(), first: config.FirstConsolePort, last: config.LastConsolePort,
		host: config.ListenHost, allocations: make(map[string]durableAllocationEntry),
	}
	if err := allocator.load(); err != nil {
		_ = owner.Release()
		return nil, err
	}
	return allocator, nil
}

func (a *DurableEmulatorAllocator) Reserve(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration) (Allocation, error) {
	if err := ctx.Err(); err != nil {
		return Allocation{}, err
	}
	if id.IsZero() || !generation.IsValid() {
		return Allocation{}, fmt.Errorf("target and generation are required")
	}
	key := deviceKey(id, generation)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return Allocation{}, fmt.Errorf("durable emulator allocator is closed")
	}
	if entry, found := a.allocations[key]; found {
		return entry.Allocation, nil
	}
	used := make(map[int]struct{}, len(a.allocations))
	for _, entry := range a.allocations {
		used[entry.Allocation.InstanceNumber] = struct{}{}
	}
	for port := a.first; port <= a.last; port += 2 {
		if _, found := used[port]; found {
			continue
		}
		available, err := a.endpointPairAvailable(port)
		if err != nil {
			return Allocation{}, err
		}
		if !available {
			continue
		}
		allocation := emulatorAllocation(port)
		entry := durableAllocationEntry{TargetID: id.String(), Generation: uint64(generation), Allocation: allocation}
		a.allocations[key] = entry
		if err := a.persistLocked(); err != nil {
			delete(a.allocations, key)
			return Allocation{}, err
		}
		return allocation, nil
	}
	return Allocation{}, fmt.Errorf("managed Android emulator console-port range is exhausted")
}

// AdoptExpected restores an exact allocation from a validated target/runtime
// manifest. It deliberately permits an in-use port: reconciliation calls it
// only after proving that the process using that endpoint is the expected AVD.
func (a *DurableEmulatorAllocator) AdoptExpected(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration, allocation Allocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id.IsZero() || !generation.IsValid() {
		return fmt.Errorf("target and generation are required")
	}
	port, err := allocation.EmulatorConsolePort()
	if err != nil || port < a.first || port > a.last || allocation != emulatorAllocation(port) {
		return fmt.Errorf("expected allocation is outside this allocator's exact canonical range")
	}
	key := deviceKey(id, generation)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return fmt.Errorf("durable emulator allocator is closed")
	}
	if prior, found := a.allocations[key]; found {
		if prior.Allocation != allocation {
			return fmt.Errorf("target generation already owns a different durable allocation")
		}
		return nil
	}
	for otherKey, prior := range a.allocations {
		if prior.Allocation.InstanceNumber == port || prior.Allocation.Serial == allocation.Serial {
			return fmt.Errorf("expected allocation collides with durable owner %s", otherKey)
		}
	}
	a.allocations[key] = durableAllocationEntry{TargetID: id.String(), Generation: uint64(generation), Allocation: allocation}
	if err := a.persistLocked(); err != nil {
		delete(a.allocations, key)
		return err
	}
	return nil
}

func (a *DurableEmulatorAllocator) Release(ctx context.Context, allocation Allocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := allocation.EmulatorConsolePort(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return fmt.Errorf("durable emulator allocator is closed")
	}
	for key, current := range a.allocations {
		if current.Allocation == allocation {
			delete(a.allocations, key)
			if err := a.persistLocked(); err != nil {
				a.allocations[key] = current
				return err
			}
			return nil
		}
	}
	return nil
}

func (a *DurableEmulatorAllocator) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	owner := a.owner
	a.owner = nil
	a.mu.Unlock()
	return owner.Release()
}

func (a *DurableEmulatorAllocator) load() error {
	file, err := os.Open(a.path)
	if errors.Is(err, os.ErrNotExist) {
		return a.persistLocked()
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maximumAllocatorFileSize {
		return fmt.Errorf("durable allocation database is not a bounded regular file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maximumAllocatorFileSize+1))
	decoder.DisallowUnknownFields()
	var state durableAllocatorState
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("decode durable allocation database: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	if state.Version != durableAllocatorVersion {
		return fmt.Errorf("unsupported durable allocation database version %d", state.Version)
	}
	ports := make(map[int]struct{}, len(state.Allocations))
	for _, entry := range state.Allocations {
		id, parseErr := domain.ParseTargetID(entry.TargetID)
		generation := domain.TargetGeneration(entry.Generation)
		port, allocationErr := entry.Allocation.EmulatorConsolePort()
		if parseErr != nil || !generation.IsValid() || allocationErr != nil || port < a.first || port > a.last || entry.Allocation != emulatorAllocation(port) {
			return fmt.Errorf("durable allocation database contains an invalid entry")
		}
		key := deviceKey(id, generation)
		if _, duplicate := a.allocations[key]; duplicate {
			return fmt.Errorf("durable allocation database duplicates target generation %s", key)
		}
		if _, duplicate := ports[port]; duplicate {
			return fmt.Errorf("durable allocation database duplicates console port %d", port)
		}
		a.allocations[key], ports[port] = entry, struct{}{}
	}
	return nil
}

func (a *DurableEmulatorAllocator) persistLocked() error {
	entries := make([]durableAllocationEntry, 0, len(a.allocations))
	for _, entry := range a.allocations {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TargetID == entries[j].TargetID {
			return entries[i].Generation < entries[j].Generation
		}
		return entries[i].TargetID < entries[j].TargetID
	})
	return atomicfile.WriteJSON(a.path, durableAllocatorState{Version: durableAllocatorVersion, Allocations: entries}, 0o600)
}

func (a *DurableEmulatorAllocator) endpointPairAvailable(consolePort int) (bool, error) {
	listeners := make([]net.Listener, 0, 2)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for _, port := range []int{consolePort, consolePort + 1} {
		listener, err := net.Listen("tcp", net.JoinHostPort(a.host, strconv.Itoa(port)))
		if err != nil {
			var networkError *net.OpError
			if errors.As(err, &networkError) {
				return false, nil
			}
			return false, fmt.Errorf("probe emulator port %d: %w", port, err)
		}
		listeners = append(listeners, listener)
	}
	return true, nil
}

func emulatorAllocation(consolePort int) Allocation {
	serial := "emulator-" + strconv.Itoa(consolePort)
	return Allocation{InstanceNumber: consolePort, InstanceName: "world-emulator-" + strconv.Itoa(consolePort), Serial: serial, ADBAddress: serial}
}

func validateConsolePortRange(first, last int) error {
	if first < ManagedEmulatorMinConsolePort || first > ManagedEmulatorMaxConsolePort || last < first || last > ManagedEmulatorMaxConsolePort || first%2 != 0 || last%2 != 0 {
		return fmt.Errorf("managed emulator console-port range must contain exact even ports from %d through %d", ManagedEmulatorMinConsolePort, ManagedEmulatorMaxConsolePort)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("durable JSON contains trailing values")
		}
		return err
	}
	return nil
}

type AllocationAdopter interface {
	AdoptExpected(context.Context, domain.TargetID, domain.TargetGeneration, Allocation) error
}

type AllocationLookup interface {
	LookupExpected(context.Context, domain.TargetID, domain.TargetGeneration) (Allocation, bool, error)
}

func (a *DurableEmulatorAllocator) LookupExpected(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration) (Allocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Allocation{}, false, err
	}
	if id.IsZero() || !generation.IsValid() {
		return Allocation{}, false, fmt.Errorf("target and generation are required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return Allocation{}, false, fmt.Errorf("durable emulator allocator is closed")
	}
	entry, found := a.allocations[deviceKey(id, generation)]
	return entry.Allocation, found, nil
}

var _ Allocator = (*DurableEmulatorAllocator)(nil)
var _ AllocationAdopter = (*DurableEmulatorAllocator)(nil)
var _ AllocationLookup = (*DurableEmulatorAllocator)(nil)
