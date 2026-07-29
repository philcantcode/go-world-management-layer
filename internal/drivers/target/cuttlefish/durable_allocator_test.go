package cuttlefish

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestDurableEmulatorAllocatorPersistsAndRejectsInUsePortPairs(t *testing.T) {
	first := findFreeEvenPortPairs(t, 2)
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(first)))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	root := t.TempDir()
	config := DurableEmulatorAllocatorConfig{StateRoot: root, FirstConsolePort: first, LastConsolePort: first + 2, ListenHost: "127.0.0.1"}
	allocator, err := NewDurableEmulatorAllocator(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDurableEmulatorAllocator(config); err == nil {
		t.Fatal("second allocator acquired the same durable database lock")
	}
	target, _ := domain.NewTargetID()
	allocation, err := allocator.Reserve(context.Background(), target, 1)
	if err != nil {
		t.Fatal(err)
	}
	if allocation != emulatorAllocation(first+2) {
		t.Fatalf("allocator selected in-use endpoint pair: %#v", allocation)
	}
	if err := allocator.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewDurableEmulatorAllocator(config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	replayed, err := restarted.Reserve(context.Background(), target, 1)
	if err != nil || replayed != allocation {
		t.Fatalf("durable replay = %#v, %v", replayed, err)
	}
	lookedUp, found, err := restarted.LookupExpected(context.Background(), target, 1)
	if err != nil || !found || lookedUp != allocation {
		t.Fatalf("durable lookup = %#v, %t, %v", lookedUp, found, err)
	}
}

func TestDurableEmulatorAllocatorReadoptsValidatedLiveAssignment(t *testing.T) {
	port := findFreeEvenPortPair(t)
	root := t.TempDir()
	allocator, err := NewDurableEmulatorAllocator(DurableEmulatorAllocatorConfig{StateRoot: root, FirstConsolePort: port, LastConsolePort: port})
	if err != nil {
		t.Fatal(err)
	}
	defer allocator.Close()
	target, _ := domain.NewTargetID()
	console, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	defer console.Close()
	adb, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port+1)))
	if err != nil {
		t.Fatal(err)
	}
	defer adb.Close()
	expected := emulatorAllocation(port)
	if err := allocator.AdoptExpected(context.Background(), target, 3, expected); err != nil {
		t.Fatalf("re-adopt validated live assignment: %v", err)
	}
	if replay, err := allocator.Reserve(context.Background(), target, 3); err != nil || replay != expected {
		t.Fatalf("re-adopt replay = %#v, %v", replay, err)
	}
}

func TestDurableEmulatorAllocatorRejectsPortsOutsideSDKContract(t *testing.T) {
	for _, config := range []DurableEmulatorAllocatorConfig{
		{StateRoot: t.TempDir(), FirstConsolePort: ManagedEmulatorMinConsolePort - 2, LastConsolePort: ManagedEmulatorMinConsolePort},
		{StateRoot: t.TempDir(), FirstConsolePort: ManagedEmulatorMaxConsolePort, LastConsolePort: ManagedEmulatorMaxConsolePort + 2},
	} {
		if _, err := NewDurableEmulatorAllocator(config); err == nil {
			t.Fatalf("out-of-contract emulator range was accepted: %#v", config)
		}
	}
}

func findFreeEvenPortPair(t *testing.T) int {
	return findFreeEvenPortPairs(t, 1)
}

func findFreeEvenPortPairs(t *testing.T, pairs int) int {
	t.Helper()
	for port := ManagedEmulatorMinConsolePort; port+2*pairs-2 <= ManagedEmulatorMaxConsolePort; port += 2 {
		listeners := make([]net.Listener, 0, 2*pairs)
		available := true
		for offset := 0; offset < 2*pairs; offset++ {
			listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port+offset)))
			if err != nil {
				available = false
				break
			}
			listeners = append(listeners, listener)
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		if available {
			return port
		}
	}
	t.Fatalf("no range containing %d free emulator port pairs", pairs)
	return 0
}
