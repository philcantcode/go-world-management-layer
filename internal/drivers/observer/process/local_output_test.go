package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestLocalOutputOpenDurablyPublishesBothStreamsBeforeReturn(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	events := []string{}
	observeDurableCaptureSync(t, factory, &events)

	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if wanted := []string{"file:stdout.partial", "file:stderr.partial", "directory"}; !reflect.DeepEqual(events, wanted) {
		t.Fatalf("durability order = %v, want %v", events, wanted)
	}
	if len(factory.active) != 1 {
		t.Fatalf("active transactions after durable Open = %d", len(factory.active))
	}
	if err := closeCaptureWriters(capture.Stdout().(*boundedCaptureWriter), capture.Stderr().(*boundedCaptureWriter)); err != nil {
		t.Fatal(err)
	}

	// Closing the descriptors without publishing a terminal marker models the
	// filesystem state available to a replacement daemon after abrupt death.
	restarted, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	request := ports.InterruptedCollectorReconciliation{
		TargetRunID: plan.TargetRunID,
		Collectors:  []ports.InterruptedCollectorBinding{{Plan: plan, StartCommitted: true}},
	}
	report, err := restarted.ReconcileInterruptedRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Outputs) != 1 || report.Outputs[0].State != ports.InterruptedCollectorOutputFinalized || report.Outputs[0].CaptureLimitExceeded {
		t.Fatalf("reconciled durable empty capture = %#v", report)
	}
	assertLocalArtifacts(t, root, plan.CollectorID, report.Outputs[0].Artifacts, map[string]string{
		CollectorStdoutRole: "",
		CollectorStderrRole: "",
	})
	entries, err := verifiedDirectoryEntries(collectorOutputDirectory(root, plan))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := entries["finalized.json"]; len(entries) != 1 || !found {
		t.Fatalf("reconciled collector entries = %v", reflect.ValueOf(entries).MapKeys())
	}
}

func TestLocalOutputOpenSyncFailureCleansAndCanRetry(t *testing.T) {
	tests := []struct {
		name          string
		failedFile    string
		failDirectory bool
		wantFileSyncs int
		wantDirSyncs  int
	}{
		{name: "stdout file", failedFile: "stdout.partial", wantFileSyncs: 1, wantDirSyncs: 1},
		{name: "stderr file", failedFile: "stderr.partial", wantFileSyncs: 2, wantDirSyncs: 1},
		{name: "collector directory", failDirectory: true, wantFileSyncs: 2, wantDirSyncs: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			plan := validCollectorPlan(t)
			factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected durability failure")
			failed, fileSyncs, directorySyncs := false, 0, 0
			factory.syncFile = func(file *os.File) error {
				fileSyncs++
				if !failed && filepath.Base(file.Name()) == test.failedFile {
					failed = true
					return failure
				}
				return syncCaptureFile(file)
			}
			factory.syncDir = func(directory string) error {
				directorySyncs++
				if !failed && test.failDirectory {
					failed = true
					return failure
				}
				return syncCaptureDirectory(directory)
			}

			if _, err := factory.Open(context.Background(), plan); !errors.Is(err, failure) {
				t.Fatalf("Open error = %v", err)
			}
			if !failed || fileSyncs != test.wantFileSyncs || directorySyncs != test.wantDirSyncs {
				t.Fatalf("sync calls = {failed:%t files:%d directories:%d}", failed, fileSyncs, directorySyncs)
			}
			if len(factory.active) != 0 {
				t.Fatalf("failed Open retained %d active transactions", len(factory.active))
			}
			entries, err := verifiedDirectoryEntries(collectorOutputDirectory(root, plan))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("failed Open retained collector files: %v", reflect.ValueOf(entries).MapKeys())
			}

			factory.syncFile = syncCaptureFile
			factory.syncDir = syncCaptureDirectory
			capture, err := factory.Open(context.Background(), plan)
			if err != nil {
				t.Fatalf("retry Open: %v", err)
			}
			if err := capture.Abort(context.Background()); err != nil {
				t.Fatalf("abort retry capture: %v", err)
			}
		})
	}
}

func TestSupervisorNeverStartsProcessBeforeLocalOutputIsDurable(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected collector-directory sync failure")
	directorySyncs := 0
	factory.syncDir = func(directory string) error {
		directorySyncs++
		if directorySyncs == 1 {
			return failure
		}
		return syncCaptureDirectory(directory)
	}

	events := []string{}
	process := newObserverProcess()
	starterCalls := 0
	driver, err := New(Config{
		Runner: runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte("collector-v1")}, nil
		}),
		Starter: starterFunc(func(context.Context, command.Invocation) (command.Process, error) {
			starterCalls++
			events = append(events, "starter")
			return process, nil
		}),
		Adapters: testAdapters(nil),
		Outputs:  factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testContext(t)
	defer cancel()

	if _, err := driver.Start(ctx, plan); !errors.Is(err, failure) {
		t.Fatalf("first Start error = %v", err)
	}
	if starterCalls != 0 || len(factory.active) != 0 {
		t.Fatalf("failed durability reached starter=%d active=%d", starterCalls, len(factory.active))
	}
	entries, err := verifiedDirectoryEntries(collectorOutputDirectory(root, plan))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed Start retained output files: %v", reflect.ValueOf(entries).MapKeys())
	}

	observeDurableCaptureSync(t, factory, &events)
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if wanted := []string{"file:stdout.partial", "file:stderr.partial", "directory", "starter"}; !reflect.DeepEqual(events, wanted) {
		t.Fatalf("successful Start ordering = %v, want %v", events, wanted)
	}
	if _, err := driver.Stop(ctx, plan.CollectorID); err != nil {
		t.Fatal(err)
	}
}

func TestLocalOutputPersistsAndVerifiesImmutableStreamArtifacts(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Stdout().Write([]byte("stdout evidence")); err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Stderr().Write([]byte("stderr evidence")); err != nil {
		t.Fatal(err)
	}
	artifacts, err := capture.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertLocalArtifacts(t, root, plan.CollectorID, artifacts, map[string]string{
		CollectorStdoutRole: "stdout evidence",
		CollectorStderrRole: "stderr evidence",
	})

	reopened, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := reopened.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	replayedArtifacts, err := replay.Finalize(context.Background())
	if err != nil || !reflect.DeepEqual(replayedArtifacts, artifacts) {
		t.Fatalf("durable replay = %#v, %v", replayedArtifacts, err)
	}

	first := artifacts[0].Spec()
	objectPath := filepath.Join(root, "objects", strings.TrimPrefix(first.Digest.String(), "sha256:"))
	if err := os.WriteFile(objectPath, make([]byte, first.Size), 0o600); err != nil {
		t.Fatal(err)
	}
	corruptFactory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corruptFactory.Open(context.Background(), plan); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("corrupt durable object error = %v", err)
	}
}

func TestLocalOutputPublishesBoundedPrefixAndDurablyReportsLimit(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	plan.MaximumBytes = 5
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	capture, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Stdout().Write([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	if written, err := capture.Stderr().Write([]byte("xyz")); written != 1 || !errors.Is(err, ErrCaptureLimit) {
		t.Fatalf("bounded write = %d, %v", written, err)
	}
	artifacts, err := capture.Finalize(context.Background())
	if !errors.Is(err, ErrCaptureLimit) || len(artifacts) != 2 {
		t.Fatalf("bounded finalize = %#v, %v", artifacts, err)
	}
	assertLocalArtifacts(t, root, plan.CollectorID, artifacts, map[string]string{
		CollectorStdoutRole: "1234",
		CollectorStderrRole: "x",
	})

	reopened, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := reopened.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	replayedArtifacts, err := replay.Finalize(context.Background())
	if !errors.Is(err, ErrCaptureLimit) || !reflect.DeepEqual(replayedArtifacts, artifacts) {
		t.Fatalf("bounded durable replay = %#v, %v", replayedArtifacts, err)
	}
}

func TestBoundedCaptureWriterRestoresUnwrittenBudgetAfterWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.partial")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	budget := &captureBudget{remaining: 5}
	writer := &boundedCaptureWriter{file: file, budget: budget}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if written, err := writer.Write([]byte("12345678")); written != 0 || err == nil {
		t.Fatalf("failed write = %d, %v", written, err)
	}
	budget.mu.Lock()
	remaining, exceeded := budget.remaining, budget.exceeded
	budget.mu.Unlock()
	if remaining != 5 || exceeded {
		t.Fatalf("budget after failed write = {remaining:%d exceeded:%t}", remaining, exceeded)
	}
}

func TestLocalOutputFinalizationOnlyPersistsCaptureLimitAtExactBoundary(t *testing.T) {
	root := t.TempDir()
	plan := validCollectorPlan(t)
	plan.MaximumBytes = 5
	factory, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	output, err := factory.Open(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	capture := output.(*localCapture)
	if _, err := capture.Stdout().Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	// Model a limit attempt that raced with another stream's reservation,
	// followed by that stream restoring bytes it failed to write.
	capture.budget.mu.Lock()
	capture.budget.exceeded = true
	capture.budget.mu.Unlock()
	artifacts, err := capture.Finalize(context.Background())
	if err != nil {
		t.Fatalf("finalize below the exact limit = %#v, %v", artifacts, err)
	}

	restarted, err := NewLocalOutputFactory(LocalOutputConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	request := ports.InterruptedCollectorReconciliation{
		TargetRunID: plan.TargetRunID,
		Collectors:  []ports.InterruptedCollectorBinding{{Plan: plan, StartCommitted: true}},
	}
	report, err := restarted.ReconcileInterruptedRun(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Outputs) != 1 || report.Outputs[0].State != ports.InterruptedCollectorOutputFinalized || report.Outputs[0].CaptureLimitExceeded {
		t.Fatalf("recovered below-limit finalization = %#v", report)
	}
}

func assertLocalArtifacts(t *testing.T, root string, collectorID domain.CollectorID, artifacts []domain.ArtifactReference, wanted map[string]string) {
	t.Helper()
	if len(artifacts) != len(wanted) {
		t.Fatalf("artifacts = %#v", artifacts)
	}
	for _, artifact := range artifacts {
		spec := artifact.Spec()
		content, found := wanted[spec.Role]
		if !found {
			t.Fatalf("unexpected artifact role %q", spec.Role)
		}
		objectPath := filepath.Join(root, "objects", strings.TrimPrefix(spec.Digest.String(), "sha256:"))
		stored, err := os.ReadFile(objectPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(stored) != content || int64(len(stored)) != spec.Size || domain.NewDigest(stored) != spec.Digest {
			t.Fatalf("artifact %q does not identify its stored bytes", spec.Role)
		}
		expectedReference := "observer://collectors/" + collectorID.String() + "/" + strings.TrimPrefix(spec.Role, "collector.") + "/" + spec.Digest.String()
		if spec.Reference != expectedReference || spec.Sensitivity != domain.SensitivityInternal {
			t.Fatalf("artifact %q metadata = %#v, want reference %q and internal sensitivity", spec.Role, spec, expectedReference)
		}
	}
}

func observeDurableCaptureSync(t *testing.T, factory *LocalOutputFactory, events *[]string) {
	t.Helper()
	factory.syncFile = func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return fmt.Errorf("capture file %q was not empty and regular before sync", file.Name())
		}
		*events = append(*events, "file:"+filepath.Base(file.Name()))
		return syncCaptureFile(file)
	}
	factory.syncDir = func(directory string) error {
		entries, err := verifiedDirectoryEntries(directory)
		if err != nil {
			return err
		}
		for _, name := range []string{"stdout.partial", "stderr.partial"} {
			info, found := entries[name]
			if !found || !info.Mode().IsRegular() || info.Size() != 0 {
				return fmt.Errorf("collector directory did not contain empty regular %s before sync", name)
			}
		}
		if len(entries) != 2 {
			return fmt.Errorf("collector directory contained %d entries before sync", len(entries))
		}
		*events = append(*events, "directory")
		return syncCaptureDirectory(directory)
	}
}
