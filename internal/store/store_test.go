package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), Options{Path: filepath.Join(t.TempDir(), "world.db"), Now: func() time.Time { return time.Unix(10, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestIdempotentMutationAndConflict(t *testing.T) {
	store := openTestStore(t)
	var calls int
	handler := func(ctx context.Context, tx *Tx) ([]byte, error) {
		calls++
		payload := []byte(`{"state":"requested"}`)
		if _, err := tx.AppendControl(ctx, ControlRecord{AggregateKind: "session", AggregateID: "rs_1", Revision: 1, Kind: "session.created", Payload: payload}); err != nil {
			return nil, err
		}
		if err := tx.PutSnapshot(ctx, "session", "rs_1", 1, payload); err != nil {
			return nil, err
		}
		return []byte(`{"id":"rs_1"}`), nil
	}
	first, replayed, err := store.RunIdempotent(context.Background(), "acquire", "key", []byte(`{"input":1}`), handler)
	if err != nil || replayed {
		t.Fatalf("first=%s replayed=%v err=%v", first, replayed, err)
	}
	second, replayed, err := store.RunIdempotent(context.Background(), "acquire", "key", []byte(`{"input":1}`), handler)
	if err != nil || !replayed || string(second) != string(first) || calls != 1 {
		t.Fatalf("second=%s replayed=%v calls=%d err=%v", second, replayed, calls, err)
	}
	if _, _, err := store.RunIdempotent(context.Background(), "acquire", "key", []byte(`{"input":2}`), handler); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if revision, snapshot, err := store.Snapshot(context.Background(), "session", "rs_1"); err != nil || revision != 1 || string(snapshot) != `{"state":"requested"}` {
		t.Fatalf("snapshot=%s revision=%d err=%v", snapshot, revision, err)
	}
}

func TestHandlerFailureRollsBack(t *testing.T) {
	store := openTestStore(t)
	_, _, err := store.RunIdempotent(context.Background(), "test", "failure", []byte("request"), func(ctx context.Context, tx *Tx) ([]byte, error) {
		_, appendErr := tx.AppendControl(ctx, ControlRecord{AggregateKind: "session", AggregateID: "rs_fail", Revision: 1, Kind: "created", Payload: []byte("payload")})
		if appendErr != nil {
			return nil, appendErr
		}
		return nil, errors.New("injected")
	})
	if err == nil {
		t.Fatal("failure unexpectedly committed")
	}
	records, err := store.Records(context.Background(), 0, 10)
	if err != nil || len(records) != 0 {
		t.Fatalf("records=%v err=%v", records, err)
	}
}

func TestRevisionAndHashChain(t *testing.T) {
	store := openTestStore(t)
	for revision := uint64(1); revision <= 3; revision++ {
		request, _ := json.Marshal(revision)
		_, _, err := store.RunIdempotent(context.Background(), "transition", string(rune('a'+revision)), request, func(ctx context.Context, tx *Tx) ([]byte, error) {
			_, err := tx.AppendControl(ctx, ControlRecord{AggregateKind: "session", AggregateID: "rs_chain", Revision: revision, Kind: "transition", Payload: request})
			return request, err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE control_records SET payload='corrupt' WHERE sequence=2`); err != nil {
		t.Fatal(err)
	}
	if err := store.Verify(context.Background()); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("verify error=%v", err)
	}
}

func TestConcurrentIdempotencyExecutesOnce(t *testing.T) {
	store := openTestStore(t)
	var calls atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := store.RunIdempotent(context.Background(), "concurrent", "same", []byte("request"), func(context.Context, *Tx) ([]byte, error) { calls.Add(1); return []byte("response"), nil })
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler called %d times", calls.Load())
	}
}

func TestNamedObjectsAreImmutableAtAnIdentity(t *testing.T) {
	controlStore := openTestStore(t)
	if err := controlStore.PutObject(context.Background(), "policy_reference", "stable", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := controlStore.PutObject(context.Background(), "policy_reference", "stable", []byte("one")); err != nil {
		t.Fatalf("identical replay failed: %v", err)
	}
	if err := controlStore.PutObject(context.Background(), "policy_reference", "stable", []byte("two")); !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("conflicting object error = %v", err)
	}
	payload, err := controlStore.Object(context.Background(), "policy_reference", "stable")
	if err != nil || string(payload) != "one" {
		t.Fatalf("object = %q, %v", payload, err)
	}
}

func TestPoliciesAreImmutableAtADigest(t *testing.T) {
	controlStore := openTestStore(t)
	if err := controlStore.PutPolicy(context.Background(), "sha256:stable", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := controlStore.PutPolicy(context.Background(), "sha256:stable", []byte("one")); err != nil {
		t.Fatalf("identical replay failed: %v", err)
	}
	if err := controlStore.PutPolicy(context.Background(), "sha256:stable", []byte("two")); !errors.Is(err, ErrPolicyConflict) {
		t.Fatalf("conflicting policy error = %v", err)
	}
}

func TestIndependentMemoryStoresDoNotShareState(t *testing.T) {
	first, err := Open(context.Background(), Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(context.Background(), Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.PutObject(context.Background(), "scope", "only-first", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Object(context.Background(), "scope", "only-first"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second in-memory store observed first store state: %v", err)
	}
}

func TestStorePathTreatsURICharactersAsLiteralFilenameBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "control#literal.db")
	controlStore, err := Open(context.Background(), Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := controlStore.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("literal database path was not created: info=%v err=%v", info, err)
	}
	if _, err := os.Stat(filepath.Join(root, "control")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("URI fragment changed the database destination: %v", err)
	}
}
