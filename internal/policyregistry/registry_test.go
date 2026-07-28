package policyregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestPublishResolveAndConflict(t *testing.T) {
	ctx := context.Background()
	source := examplePolicy(t)
	registry, controlStore := testRegistry(t)
	first, err := registry.Publish(ctx, source, supportedFingerprint(t, source))
	if err != nil {
		t.Fatal(err)
	}
	if first.Publication.Reference != Reference("mixed-vr-visibility-first", 3) {
		t.Fatalf("reference = %q", first.Publication.Reference)
	}
	resolved, err := registry.Resolve(ctx, first.Publication.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if string(resolved.CanonicalJSON) != string(first.CanonicalJSON) || resolved.Publication.PolicyDigest != first.Publication.PolicyDigest {
		t.Fatalf("resolved publication differs: %#v", resolved.Publication)
	}
	byDigest, err := registry.ResolveDigest(ctx, first.Publication.PolicyDigest, first.Publication.CapabilityFingerprintDigest)
	if err != nil || byDigest.Effective == nil || byDigest.Effective.Digest().String() != first.Publication.PolicyDigest {
		t.Fatalf("digest resolution = %#v, %v", byDigest.Publication, err)
	}
	resolved.CanonicalJSON[0] ^= 0xff
	for name, capability := range resolved.Publication.CapabilityFingerprint.Capabilities {
		capability.Evidence["mutated"] = "yes"
		resolved.Publication.CapabilityFingerprint.Capabilities[name] = capability
		break
	}
	again, err := registry.Resolve(ctx, first.Publication.Reference)
	if err != nil || string(again.CanonicalJSON) != string(first.CanonicalJSON) || again.Publication.CapabilityFingerprint.Evidence["mutated"] != "" {
		t.Fatalf("registry returned shared bytes: %v", err)
	}

	changed := []byte(strings.Replace(string(source), "priority: 50", "priority: 51", 1))
	_, err = registry.Publish(ctx, changed, supportedFingerprint(t, changed))
	if !errors.Is(err, store.ErrObjectConflict) {
		t.Fatalf("conflicting publication error = %v", err)
	}
	if stored, err := controlStore.Policy(ctx, first.Publication.PolicyDigest); err != nil || string(stored) != string(first.CanonicalJSON) {
		t.Fatalf("original policy changed: %v", err)
	}
}

func TestResolveDigestRejectsOpaqueUnknownAndUnverifiedMetadata(t *testing.T) {
	ctx := context.Background()
	registry, _ := testRegistry(t)
	if _, err := registry.ResolveDigest(ctx, "configured-policy", "configured-capabilities"); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("opaque digest error = %v", err)
	}
	unknown := "sha256:" + strings.Repeat("1", 64)
	if _, err := registry.ResolveDigest(ctx, unknown, unknown); !errors.Is(err, ErrUnknownDigest) {
		t.Fatalf("unknown digest error = %v", err)
	}
	source := examplePolicy(t)
	snapshot, err := registry.Publish(ctx, source, supportedFingerprint(t, source))
	if err != nil {
		t.Fatal(err)
	}
	tampered := cloneSnapshot(snapshot)
	tampered.Publication.Resolutions[0].Satisfied = !tampered.Publication.Resolutions[0].Satisfied
	if _, err := verifyPublication(tampered.Publication, tampered.CanonicalJSON); !errors.Is(err, ErrPublicationIntegrity) || !strings.Contains(err.Error(), "capability resolution mismatch") {
		t.Fatalf("tampered resolution error = %v", err)
	}
	tampered = cloneSnapshot(snapshot)
	tampered.Publication.CapabilityFingerprint.Evidence["node"] = "other"
	if _, err := verifyPublication(tampered.Publication, tampered.CanonicalJSON); !errors.Is(err, ErrPublicationIntegrity) || !strings.Contains(err.Error(), "fingerprint digest mismatch") {
		t.Fatalf("tampered fingerprint error = %v", err)
	}
}

func TestPublicationPersistsAndRestoresCompleteCapabilityFingerprint(t *testing.T) {
	source := examplePolicy(t)
	base := supportedFingerprint(t, source)
	capabilities := base.Capabilities()
	extra, err := policy.NewCapability(policy.CapabilitySupported,
		map[string]string{"runtime": "runc", "version": "1.2.3"},
		map[string]string{"probe": "containerd", "observed_at": "fixed"})
	if err != nil {
		t.Fatal(err)
	}
	capabilities["runtime.detail.extra"] = extra
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"node": "node-a", "inventory": "complete"})
	if err != nil {
		t.Fatal(err)
	}
	registry, _ := testRegistry(t)
	published, err := registry.Publish(context.Background(), source, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.ResolveDigest(context.Background(), published.Publication.PolicyDigest, fingerprint.Digest().String())
	if err != nil {
		t.Fatal(err)
	}
	stored := resolved.Publication.CapabilityFingerprint
	if stored.Evidence["node"] != "node-a" || stored.Evidence["inventory"] != "complete" {
		t.Fatalf("fingerprint evidence was not restored: %#v", stored.Evidence)
	}
	actual, found := stored.Capabilities["runtime.detail.extra"]
	if !found || actual.Status != policy.CapabilitySupported || actual.Constraints["runtime"] != "runc" || actual.Constraints["version"] != "1.2.3" || actual.Evidence["probe"] != "containerd" {
		t.Fatalf("complete capability was not restored: %#v", actual)
	}
}

func TestResolveClassifiesEveryPersistedPublicationCorruption(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*memoryRegistryStore, Publication)
	}{
		{
			name: "missing canonical content",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				delete(storage.policies, publication.PolicyDigest)
			},
		},
		{
			name: "changed canonical content",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				storage.policies[publication.PolicyDigest] = append(storage.policies[publication.PolicyDigest], ' ')
			},
		},
		{
			name: "changed complete fingerprint",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				publication.CapabilityFingerprint.Evidence["node"] = "corrupt"
				storage.replaceDigestPublication(t, publication)
			},
		},
		{
			name: "changed resolution",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				publication.Resolutions[0].Reason = "fabricated"
				storage.replaceDigestPublication(t, publication)
			},
		},
		{
			name: "changed requirements",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				publication.Requirements[0].Path = "fabricated.path"
				storage.replaceDigestPublication(t, publication)
			},
		},
		{
			name: "changed warnings",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				publication.Warnings = []policy.Warning{{Code: "fabricated"}}
				storage.replaceDigestPublication(t, publication)
			},
		},
		{
			name: "changed binding identity",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				publication.PolicyDigest = "sha256:" + strings.Repeat("f", 64)
				storage.replaceDigestPublicationAt(t, digestBinding(
					storage.published.PolicyDigest, storage.published.CapabilityFingerprintDigest), publication)
			},
		},
		{
			name: "missing reference index",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				delete(storage.objects, objectKey(referenceObjectKind, publication.Reference))
			},
		},
		{
			name: "disagreeing reference index",
			mutate: func(storage *memoryRegistryStore, publication Publication) {
				storage.objects[objectKey(referenceObjectKind, publication.Reference)] = []byte(`{"reference":"corrupt"}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newMemoryRegistryStore()
			registry, err := New(storage)
			if err != nil {
				t.Fatal(err)
			}
			source := examplePolicy(t)
			snapshot, err := registry.Publish(ctx, source, supportedFingerprint(t, source))
			if err != nil {
				t.Fatal(err)
			}
			storage.published = snapshot.Publication
			test.mutate(storage, cloneSnapshot(snapshot).Publication)
			_, err = registry.ResolveDigest(ctx, snapshot.Publication.PolicyDigest, snapshot.Publication.CapabilityFingerprintDigest)
			if !errors.Is(err, ErrPublicationIntegrity) {
				t.Fatalf("resolve error = %v, want publication integrity failure", err)
			}
		})
	}
}

func TestResolveReferenceRejectsPartialPublication(t *testing.T) {
	ctx := context.Background()
	storage := newMemoryRegistryStore()
	registry, err := New(storage)
	if err != nil {
		t.Fatal(err)
	}
	source := examplePolicy(t)
	snapshot, err := registry.Publish(ctx, source, supportedFingerprint(t, source))
	if err != nil {
		t.Fatal(err)
	}
	delete(storage.objects, objectKey(digestObjectKind, digestBinding(snapshot.Publication.PolicyDigest, snapshot.Publication.CapabilityFingerprintDigest)))
	if _, err := registry.Resolve(ctx, snapshot.Publication.Reference); !errors.Is(err, ErrPublicationIntegrity) {
		t.Fatalf("resolve partial reference error = %v, want publication integrity failure", err)
	}
}

func TestResolveRejectsMissingAndMalformedReferences(t *testing.T) {
	ctx := context.Background()
	registry, controlStore := testRegistry(t)
	if _, err := registry.Resolve(ctx, ""); err == nil {
		t.Fatal("empty reference accepted")
	}
	if err := controlStore.PutObject(ctx, referenceObjectKind, "bad@1", []byte(`{"reference":"bad@1","unexpected":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(ctx, "bad@1"); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("malformed reference error = %v", err)
	}
}

func testRegistry(t *testing.T) (*Registry, *store.Store) {
	t.Helper()
	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "control.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	registry, err := New(controlStore)
	if err != nil {
		t.Fatal(err)
	}
	return registry, controlStore
}

func examplePolicy(t *testing.T) []byte {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("..", "..", "docs", "examples", "environment-policy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func supportedFingerprint(t *testing.T, source []byte) policy.CapabilityFingerprint {
	t.Helper()
	requirements, err := policy.Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := make(map[string]policy.Capability, len(requirements))
	for _, requirement := range requirements {
		capability, err := policy.NewCapability(policy.CapabilitySupported, requirement.Constraints, map[string]string{"test": "supported"})
		if err != nil {
			t.Fatal(err)
		}
		capabilities[requirement.Name] = capability
	}
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"node": "test"})
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

type memoryRegistryStore struct {
	policies  map[string][]byte
	objects   map[string][]byte
	published Publication
}

func newMemoryRegistryStore() *memoryRegistryStore {
	return &memoryRegistryStore{policies: make(map[string][]byte), objects: make(map[string][]byte)}
}

func (s *memoryRegistryStore) PutPolicy(_ context.Context, digest string, payload []byte) error {
	if existing, found := s.policies[digest]; found && !bytes.Equal(existing, payload) {
		return store.ErrPolicyConflict
	}
	s.policies[digest] = append([]byte(nil), payload...)
	return nil
}

func (s *memoryRegistryStore) Policy(_ context.Context, digest string) ([]byte, error) {
	payload, found := s.policies[digest]
	if !found {
		return nil, store.ErrNotFound
	}
	return append([]byte(nil), payload...), nil
}

func (s *memoryRegistryStore) PutObject(_ context.Context, kind, id string, payload []byte) error {
	key := objectKey(kind, id)
	if existing, found := s.objects[key]; found && !bytes.Equal(existing, payload) {
		return store.ErrObjectConflict
	}
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (s *memoryRegistryStore) Object(_ context.Context, kind, id string) ([]byte, error) {
	payload, found := s.objects[objectKey(kind, id)]
	if !found {
		return nil, store.ErrNotFound
	}
	return append([]byte(nil), payload...), nil
}

func (s *memoryRegistryStore) replaceDigestPublication(t *testing.T, publication Publication) {
	t.Helper()
	payload := s.marshalPublication(t, publication)
	s.objects[objectKey(digestObjectKind, digestBinding(publication.PolicyDigest, publication.CapabilityFingerprintDigest))] = append([]byte(nil), payload...)
	s.objects[objectKey(referenceObjectKind, publication.Reference)] = append([]byte(nil), payload...)
}

func (s *memoryRegistryStore) replaceDigestPublicationAt(t *testing.T, binding string, publication Publication) {
	t.Helper()
	s.objects[objectKey(digestObjectKind, binding)] = s.marshalPublication(t, publication)
}

func (s *memoryRegistryStore) marshalPublication(t *testing.T, publication Publication) []byte {
	t.Helper()
	payload, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func objectKey(kind, id string) string { return kind + "\x00" + id }
