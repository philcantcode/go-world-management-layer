package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestLocalOutputReconcileAbortsInterruptedPartialsWithoutOrphans(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	directory := prepareInterruptedPartials(t, root, plan)
	factory := newLocalOutputFactory(t, root)
	request := interruptedCollectorRequest(plan, true)

	report, err := factory.ReconcileInterruptedRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputAborted, nil)
	assertExactDirectoryEntries(t, directory, "aborted")

	replayed, err := factory.ReconcileInterruptedRun(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed, report) {
		t.Fatalf("idempotent reconciliation = %#v, %v; want %#v", replayed, err, report)
	}
	assertNoPartialFiles(t, filepath.Join(root, "runs", plan.TargetRunID.String()))
}

func TestLocalOutputReconcileReusesFinalizedArtifactsAcrossUncommittedStartMarkerRace(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory := newLocalOutputFactory(t, root)
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = capture.Stdout().Write([]byte("final stdout"))
	_, _ = capture.Stderr().Write([]byte("final stderr"))
	artifacts, err := capture.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	directory := collectorOutputDirectory(root, plan)
	for _, name := range []string{"stdout.partial", "stderr.partial"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("crash-window"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	restarted := newLocalOutputFactory(t, root)
	report, err := restarted.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, false))
	if err != nil {
		t.Fatal(err)
	}
	assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputFinalized, artifacts)
	assertExactDirectoryEntries(t, directory, "finalized.json")
}

func TestLocalOutputReconcileAcceptsExactAbortedTransactionOnRetry(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory := newLocalOutputFactory(t, root)
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := newLocalOutputFactory(t, root)
	request := interruptedCollectorRequest(plan, false)
	first, err := restarted.ReconcileInterruptedRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := restarted.ReconcileInterruptedRun(context.Background(), request)
	if err != nil || !reflect.DeepEqual(second, first) {
		t.Fatalf("aborted retry = %#v, %v; want %#v", second, err, first)
	}
	assertInterruptedOutput(t, first, plan.CollectorID, ports.InterruptedCollectorOutputAborted, nil)
}

func TestLocalOutputReconcileClassifiesMissingNeverStartedAndRejectsMissingCommitted(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		report, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, false))
		if err != nil {
			t.Fatal(err)
		}
		assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputAborted, nil)
		assertExactDirectoryEntries(t, collectorOutputDirectory(root, plan), "aborted")
	})

	t.Run("committed", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("missing committed transaction error = %v", err)
		}
		if _, err := os.Lstat(filepath.Join(root, "runs", plan.TargetRunID.String())); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed closed reconciliation mutated the missing run directory: %v", err)
		}
	})
}

func TestLocalOutputReconcileClassifiesOnePartialByStartCommit(t *testing.T) {
	t.Run("never started is safely aborted", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		if err := os.Remove(filepath.Join(directory, "stderr.partial")); err != nil {
			t.Fatal(err)
		}

		factory := newLocalOutputFactory(t, root)
		report, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, false))
		if err != nil {
			t.Fatal(err)
		}
		assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputAborted, nil)
		assertExactDirectoryEntries(t, directory, "aborted")
	})

	t.Run("committed start fails closed", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		if err := os.Remove(filepath.Join(directory, "stderr.partial")); err != nil {
			t.Fatal(err)
		}

		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("one-partial committed transaction error = %v", err)
		}
		assertExactDirectoryEntries(t, directory, "stdout.partial")
	})
}

func TestLocalOutputReconcileRejectsUnclaimedEntries(t *testing.T) {
	t.Run("collector directory", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		runDirectory := filepath.Join(root, "runs", plan.TargetRunID.String())
		if err := os.MkdirAll(filepath.Join(runDirectory, "collector_foreign"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, false)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("unclaimed collector error = %v", err)
		}
	})

	t.Run("collector file", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		if err := os.WriteFile(filepath.Join(directory, "foreign.bin"), []byte("foreign"), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("unclaimed collector file error = %v", err)
		}
	})
}

func TestLocalOutputReconcileRejectsMismatchedPlanSignature(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory := newLocalOutputFactory(t, root)
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed := plan
	changed.StartedAt = changed.StartedAt.Add(1)
	restarted := newLocalOutputFactory(t, root)
	if _, err := restarted.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(changed, false)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("mismatched signature error = %v", err)
	}
}

func TestLocalOutputReconcileRejectsSymlinkAndSpecialEntries(t *testing.T) {
	t.Run("symlink file", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		stdout := filepath.Join(directory, "stdout.partial")
		if err := os.Remove(stdout); err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "outside")
		if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, stdout); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("symlink error = %v", err)
		}
	})

	t.Run("directory in file slot", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		stdout := filepath.Join(directory, "stdout.partial")
		if err := os.Remove(stdout); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(stdout, 0o700); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("special entry error = %v", err)
		}
	})
}

func TestLocalOutputReconcileRecoversKnownControlPublicationWindows(t *testing.T) {
	t.Run("complete abort pending", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		signature, err := localCaptureSignature(plan)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "aborted.pending"), []byte(signature+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); err != nil {
			t.Fatal(err)
		}
		assertExactDirectoryEntries(t, directory, "aborted")
	})

	t.Run("partial abort pending", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		signature, err := localCaptureSignature(plan)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "aborted.pending"), []byte(signature[:len(signature)/2]), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); err != nil {
			t.Fatal(err)
		}
		assertExactDirectoryEntries(t, directory, "aborted")
	})

	t.Run("complete finalization pending", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		capture, err := factory.Open(context.Background(), plan)
		if err != nil {
			t.Fatal(err)
		}
		artifacts, err := capture.Finalize(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		directory := collectorOutputDirectory(root, plan)
		if err := os.Rename(filepath.Join(directory, "finalized.json"), filepath.Join(directory, "finalized.json.pending")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "stdout.partial"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		restarted := newLocalOutputFactory(t, root)
		report, err := restarted.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true))
		if err != nil {
			t.Fatal(err)
		}
		assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputFinalized, artifacts)
		assertExactDirectoryEntries(t, directory, "finalized.json")
	})

	t.Run("incomplete finalization pending", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		directory := prepareInterruptedPartials(t, root, plan)
		if err := os.WriteFile(filepath.Join(directory, "finalized.json.pending"), []byte(`{"version":`), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		report, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true))
		if err != nil {
			t.Fatal(err)
		}
		assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputAborted, nil)
		assertExactDirectoryEntries(t, directory, "aborted")
	})
}

func TestLocalOutputReconcileClassifiesHardCrashObjectPublicationWindows(t *testing.T) {
	t.Run("truncated pending for exact partial", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		stdout := "partial stdout for object"
		prepareInterruptedContent(t, root, plan, stdout, "stderr")
		digest := strings.TrimPrefix(domain.NewDigest([]byte(stdout)).String(), "sha256:")
		pending := filepath.Join(root, "objects", digest+".pending")
		if err := os.WriteFile(pending, []byte(stdout[:len(stdout)/2]), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); err != nil {
			t.Fatal(err)
		}
		assertExactDirectoryEntries(t, filepath.Join(root, "objects"))
	})

	t.Run("renamed but unmanifested object", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		stdout := "renamed object before manifest"
		prepareInterruptedContent(t, root, plan, stdout, "stderr")
		digest := strings.TrimPrefix(domain.NewDigest([]byte(stdout)).String(), "sha256:")
		object := filepath.Join(root, "objects", digest)
		if err := os.WriteFile(object, []byte(stdout), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); err != nil {
			t.Fatal(err)
		}
		assertExactDirectoryEntries(t, filepath.Join(root, "objects"))
	})

	t.Run("object shared by finalized collector", func(t *testing.T) {
		root := t.TempDir()
		shared := "shared immutable bytes"
		finalPlan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		capture, err := factory.Open(context.Background(), finalPlan)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = capture.Stdout().Write([]byte(shared))
		_, _ = capture.Stderr().Write([]byte("final stderr"))
		if _, err := capture.Finalize(context.Background()); err != nil {
			t.Fatal(err)
		}

		interruptedPlan := validCollectorPlan(t)
		prepareInterruptedContent(t, root, interruptedPlan, shared, "orphan bytes")
		orphan := []byte("orphan bytes")
		orphanDigest := strings.TrimPrefix(domain.NewDigest(orphan).String(), "sha256:")
		if err := os.WriteFile(filepath.Join(root, "objects", orphanDigest), orphan, 0o600); err != nil {
			t.Fatal(err)
		}
		restarted := newLocalOutputFactory(t, root)
		if _, err := restarted.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(interruptedPlan, true)); err != nil {
			t.Fatal(err)
		}
		sharedDigest := strings.TrimPrefix(domain.NewDigest([]byte(shared)).String(), "sha256:")
		if _, err := os.Stat(filepath.Join(root, "objects", sharedDigest)); err != nil {
			t.Fatalf("shared referenced object was removed: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, "objects", orphanDigest)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unmanifested object remains: %v", err)
		}
	})

	t.Run("complete pending object referenced by manifest", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		factory := newLocalOutputFactory(t, root)
		capture, err := factory.Open(context.Background(), plan)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = capture.Stdout().Write([]byte("stdout"))
		_, _ = capture.Stderr().Write([]byte("stderr"))
		artifacts, err := capture.Finalize(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		digest := strings.TrimPrefix(artifacts[0].Spec().Digest.String(), "sha256:")
		object := filepath.Join(root, "objects", digest)
		if err := os.Rename(object, object+".pending"); err != nil {
			t.Fatal(err)
		}
		restarted := newLocalOutputFactory(t, root)
		report, err := restarted.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true))
		if err != nil {
			t.Fatal(err)
		}
		assertInterruptedOutput(t, report, plan.CollectorID, ports.InterruptedCollectorOutputFinalized, artifacts)
		if _, err := os.Stat(object); err != nil {
			t.Fatalf("live pending object was not promoted: %v", err)
		}
		assertNoPendingObjectFiles(t, filepath.Join(root, "objects"))
	})
}

func TestLocalOutputReconcileRejectsUnclassifiedObjectEntries(t *testing.T) {
	t.Run("unknown name", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		prepareInterruptedPartials(t, root, plan)
		if err := os.WriteFile(filepath.Join(root, "objects", "foreign"), []byte("foreign"), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("unknown object error = %v", err)
		}
	})

	t.Run("mismatched pending without exact partial", func(t *testing.T) {
		root := t.TempDir()
		plan := validCollectorPlan(t)
		prepareInterruptedPartials(t, root, plan)
		digest := strings.TrimPrefix(domain.NewDigest([]byte("expected other bytes")).String(), "sha256:")
		if err := os.WriteFile(filepath.Join(root, "objects", digest+".pending"), []byte("unrelated"), 0o600); err != nil {
			t.Fatal(err)
		}
		factory := newLocalOutputFactory(t, root)
		if _, err := factory.ReconcileInterruptedRun(context.Background(), interruptedCollectorRequest(plan, true)); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("mismatched pending object error = %v", err)
		}
	})
}

func prepareInterruptedPartials(t *testing.T, root string, plan ports.CollectorPlan) string {
	return prepareInterruptedContent(t, root, plan, "partial stdout", "partial stderr")
}

func prepareInterruptedContent(t *testing.T, root string, plan ports.CollectorPlan, stdout, stderr string) string {
	t.Helper()
	factory := newLocalOutputFactory(t, root)
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Stdout().Write([]byte(stdout)); err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Stderr().Write([]byte(stderr)); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(capture.Stdout().Close(), capture.Stderr().Close()); err != nil {
		t.Fatal(err)
	}
	return collectorOutputDirectory(root, plan)
}

func newLocalOutputFactory(t *testing.T, root string) *LocalOutputFactory {
	t.Helper()
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	return factory
}

func interruptedCollectorRequest(plan ports.CollectorPlan, startCommitted bool) ports.InterruptedCollectorReconciliation {
	return ports.InterruptedCollectorReconciliation{
		TargetRunID: plan.TargetRunID,
		Collectors:  []ports.InterruptedCollectorBinding{{Plan: plan, StartCommitted: startCommitted}},
	}
}

func collectorOutputDirectory(root string, plan ports.CollectorPlan) string {
	return filepath.Join(root, "runs", plan.TargetRunID.String(), plan.CollectorID.String())
}

func assertInterruptedOutput(t *testing.T, report ports.InterruptedCollectorReconciliationReport, collectorID domain.CollectorID, state ports.InterruptedCollectorOutputState, artifacts []domain.ArtifactReference) {
	t.Helper()
	if len(report.Outputs) != 1 || report.Outputs[0].CollectorID != collectorID || report.Outputs[0].State != state || !reflect.DeepEqual(report.Outputs[0].Artifacts, artifacts) {
		t.Fatalf("reconciliation report = %#v", report)
	}
}

func assertExactDirectoryEntries(t *testing.T, directory string, wanted ...string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(entries))
	for _, entry := range entries {
		got[entry.Name()] = true
	}
	if len(got) != len(wanted) {
		t.Fatalf("entries in %s = %v, want %v", directory, got, wanted)
	}
	for _, name := range wanted {
		if !got[name] {
			t.Fatalf("entries in %s = %v, missing %s", directory, got, name)
		}
	}
}

func assertNoPartialFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".partial" {
			t.Errorf("orphan partial remains: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoPendingObjectFiles(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".pending") {
			t.Fatalf("pending object remains after reconciliation: %s", entry.Name())
		}
	}
}
