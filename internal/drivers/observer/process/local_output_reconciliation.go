package process

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

var collectorTransactionFiles = map[string]struct{}{
	"stdout.partial":         {},
	"stderr.partial":         {},
	"finalized.json":         {},
	"finalized.json.pending": {},
	"aborted":                {},
	"aborted.pending":        {},
}

func (f *LocalOutputFactory) prepareCaptureDirectoryForOpen(directory, signature string) error {
	entries, err := verifiedDirectoryEntries(directory)
	if err != nil {
		return err
	}
	for name, info := range entries {
		if _, found := collectorTransactionFiles[name]; !found {
			return outputIntegrity("collector_directory", "contains an unclaimed file", nil)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return outputIntegrity("collector_file", "must be regular and non-symlink", nil)
		}
	}
	if _, found := entries["finalized.json.pending"]; found {
		return outputIntegrity("pending_finalization", "must be reconciled before the collector can be reopened", nil)
	}
	if _, found := entries["finalized.json"]; found {
		return outputIntegrity("finalized", "could not be validated before reopen", nil)
	}
	if _, found := entries["aborted"]; found {
		if _, pending := entries["aborted.pending"]; pending {
			return outputIntegrity("collector_transaction", "contains conflicting abort states", nil)
		}
		if err := ensureAbortedCollector(directory, signature); err != nil {
			return err
		}
		if err := removeVerifiedRegularFile(filepath.Join(directory, "aborted")); err != nil {
			return err
		}
		return syncCaptureDirectory(directory)
	}
	if _, found := entries["aborted.pending"]; found {
		if err := finishPendingAbort(directory, signature); err != nil {
			return err
		}
		if err := removeVerifiedRegularFile(filepath.Join(directory, "aborted")); err != nil {
			return err
		}
		return syncCaptureDirectory(directory)
	}
	if hasPartial(entries) {
		return outputIntegrity("partial_output", "must be reconciled before the collector can be reopened", nil)
	}
	return requireExactCollectorFiles(directory)
}

// ReconcileInterruptedRun classifies every exact durable collector output
// transaction for a run. It is called only after Driver has proved that the
// prior collector processes are dead, so closing the transaction cannot race
// a legitimate old writer.
func (f *LocalOutputFactory) ReconcileInterruptedRun(ctx context.Context, request ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
	if err := ctx.Err(); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if err := request.Validate(); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}

	bindings := append([]ports.InterruptedCollectorBinding(nil), request.Collectors...)
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].Plan.CollectorID.String() < bindings[j].Plan.CollectorID.String()
	})

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.active) != 0 {
		return ports.InterruptedCollectorReconciliationReport{}, outputIntegrity("collector_id", "an output transaction is still active in this process", nil)
	}
	before, err := f.scanRunObjectReachability()
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	interruptedPending, err := f.classifyInterruptedObjectPending(before.Partials)
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if err := f.reconcileObjectNamespace(before.Referenced, interruptedPending); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}

	runsDirectory, _, err := ensureChildDirectory(f.root, "runs")
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	runDirectory := filepath.Join(runsDirectory, request.TargetRunID.String())
	if _, err := verifiedDirectoryEntries(runDirectory); errors.Is(err, os.ErrNotExist) {
		if hasCommittedCollector(bindings) {
			return ports.InterruptedCollectorReconciliationReport{}, outputIntegrity("run_directory", "is missing after a committed collector start", nil)
		}
		if runDirectory, _, err = ensureChildDirectory(runsDirectory, request.TargetRunID.String()); err != nil {
			return ports.InterruptedCollectorReconciliationReport{}, err
		}
	} else if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}

	runEntries, err := verifiedDirectoryEntries(runDirectory)
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	expected := make(map[string]ports.InterruptedCollectorBinding, len(bindings))
	for _, binding := range bindings {
		expected[binding.Plan.CollectorID.String()] = binding
	}
	for name, info := range runEntries {
		if _, found := expected[name]; !found {
			return ports.InterruptedCollectorReconciliationReport{}, outputIntegrity("run_directory", "contains an unclaimed collector entry", nil)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ports.InterruptedCollectorReconciliationReport{}, outputIntegrity("collector_directory", "must be a non-symlink directory", nil)
		}
	}

	report := ports.InterruptedCollectorReconciliationReport{TargetRunID: request.TargetRunID}
	for _, binding := range bindings {
		collectorID := binding.Plan.CollectorID.String()
		collectorDirectory := filepath.Join(runDirectory, collectorID)
		if _, found := runEntries[collectorID]; !found {
			if binding.StartCommitted {
				return ports.InterruptedCollectorReconciliationReport{}, outputIntegrity("collector_directory", "is missing after its start was committed", nil)
			}
			if _, _, err := ensureChildDirectory(runDirectory, collectorID); err != nil {
				return ports.InterruptedCollectorReconciliationReport{}, err
			}
		}
		output, err := f.reconcileCollectorOutput(collectorDirectory, binding)
		if err != nil {
			return ports.InterruptedCollectorReconciliationReport{}, err
		}
		report.Outputs = append(report.Outputs, output)
	}
	if err := requireExactRunEntries(runDirectory, expected); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	after, err := f.scanRunObjectReachability()
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if err := f.reconcileObjectNamespace(after.Referenced, interruptedPending); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if err := report.ValidateFor(request); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	return report, nil
}

func (f *LocalOutputFactory) reconcileCollectorOutput(directory string, binding ports.InterruptedCollectorBinding) (ports.InterruptedCollectorOutput, error) {
	plan := binding.Plan
	signature, err := localCaptureSignature(plan)
	if err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	entries, err := verifiedDirectoryEntries(directory)
	if err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	for name, info := range entries {
		if _, found := collectorTransactionFiles[name]; !found {
			return ports.InterruptedCollectorOutput{}, outputIntegrity("collector_directory", "contains an unclaimed file", nil)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return ports.InterruptedCollectorOutput{}, outputIntegrity("collector_file", "must be regular and non-symlink", nil)
		}
	}
	if err := validateInterruptedPartials(directory, entries, plan.MaximumBytes); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}

	_, finalized := entries["finalized.json"]
	_, finalizedPending := entries["finalized.json.pending"]
	_, aborted := entries["aborted"]
	_, abortedPending := entries["aborted.pending"]
	if (finalized && aborted) || (finalizedPending && abortedPending) || (finalized && finalizedPending) || (aborted && abortedPending) || (finalized && abortedPending) || (aborted && finalizedPending) {
		return ports.InterruptedCollectorOutput{}, outputIntegrity("collector_transaction", "contains conflicting terminal states", nil)
	}
	if finalized {
		return f.reconcileFinalizedCollector(directory, signature, plan)
	}
	if aborted {
		return reconcileAbortedCollector(directory, signature, plan.CollectorID)
	}
	if finalizedPending {
		return f.reconcilePendingFinalization(directory, signature, binding)
	}
	if abortedPending {
		return reconcilePendingAbort(directory, signature, plan.CollectorID)
	}
	if binding.StartCommitted && !hasCompletePartialSet(entries) {
		return ports.InterruptedCollectorOutput{}, outputIntegrity("collector_transaction", "does not contain both durable stream files after the collector start was committed", nil)
	}
	return abortInterruptedCollector(directory, signature, plan.CollectorID)
}

func (f *LocalOutputFactory) reconcileFinalizedCollector(directory, signature string, plan ports.CollectorPlan) (ports.InterruptedCollectorOutput, error) {
	artifacts, finalErr, found, err := f.loadFinalized(directory, signature, plan)
	if err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	if !found {
		return ports.InterruptedCollectorOutput{}, outputIntegrity("finalized", "disappeared during reconciliation", nil)
	}
	if err := removeCapturePartials(directory); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	if err := requireExactCollectorFiles(directory, "finalized.json"); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	return ports.InterruptedCollectorOutput{
		CollectorID: plan.CollectorID, State: ports.InterruptedCollectorOutputFinalized,
		Artifacts: append([]domain.ArtifactReference(nil), artifacts...), CaptureLimitExceeded: errors.Is(finalErr, ErrCaptureLimit),
	}, nil
}

func reconcileAbortedCollector(directory, signature string, collectorID domain.CollectorID) (ports.InterruptedCollectorOutput, error) {
	if err := ensureAbortedCollector(directory, signature); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	return ports.InterruptedCollectorOutput{CollectorID: collectorID, State: ports.InterruptedCollectorOutputAborted}, nil
}

func ensureAbortedCollector(directory, signature string) error {
	wanted := []byte(signature + "\n")
	content, err := readBoundedRegularFile(filepath.Join(directory, "aborted"), maximumLocalCaptureManifestBytes)
	if err != nil || !bytes.Equal(content, wanted) {
		return outputIntegrity("aborted", "does not match the exact collector plan", err)
	}
	if err := removeCapturePartials(directory); err != nil {
		return err
	}
	if err := requireExactCollectorFiles(directory, "aborted"); err != nil {
		return err
	}
	return nil
}

func (f *LocalOutputFactory) reconcilePendingFinalization(directory, signature string, binding ports.InterruptedCollectorBinding) (ports.InterruptedCollectorOutput, error) {
	pendingPath := filepath.Join(directory, "finalized.json.pending")
	encoded, err := readBoundedRegularFile(pendingPath, maximumLocalCaptureManifestBytes)
	if err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	manifest, decodeErr := decodeLocalCaptureManifest(encoded)
	if decodeErr != nil {
		if err := removeVerifiedRegularFile(pendingPath); err != nil {
			return ports.InterruptedCollectorOutput{}, err
		}
		return abortInterruptedCollector(directory, signature, binding.Plan.CollectorID)
	}
	if manifest.Version != localCaptureManifestVersion || manifest.Signature != signature {
		return ports.InterruptedCollectorOutput{}, outputIntegrity("pending_finalization", "belongs to another collector plan or format", nil)
	}
	if _, _, err := f.decodeFinalized(encoded, signature, binding.Plan); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	if err := promotePendingControlFile(directory, "finalized.json"); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	return f.reconcileFinalizedCollector(directory, signature, binding.Plan)
}

func reconcilePendingAbort(directory, signature string, collectorID domain.CollectorID) (ports.InterruptedCollectorOutput, error) {
	if err := finishPendingAbort(directory, signature); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	return reconcileAbortedCollector(directory, signature, collectorID)
}

func finishPendingAbort(directory, signature string) error {
	pendingPath := filepath.Join(directory, "aborted.pending")
	wanted := []byte(signature + "\n")
	content, err := readBoundedRegularFile(pendingPath, maximumLocalCaptureManifestBytes)
	if err != nil {
		return err
	}
	if bytes.Equal(content, wanted) {
		if err := promotePendingControlFile(directory, "aborted"); err != nil {
			return err
		}
		return nil
	}
	if !bytes.HasPrefix(wanted, content) {
		return outputIntegrity("pending_abort", "does not belong to the exact collector plan", nil)
	}
	if err := removeVerifiedRegularFile(pendingPath); err != nil {
		return err
	}
	return publishCaptureControlFile(directory, "aborted", wanted, maximumLocalCaptureManifestBytes)
}

func abortInterruptedCollector(directory, signature string, collectorID domain.CollectorID) (ports.InterruptedCollectorOutput, error) {
	if err := publishCaptureControlFile(directory, "aborted", []byte(signature+"\n"), maximumLocalCaptureManifestBytes); err != nil {
		return ports.InterruptedCollectorOutput{}, err
	}
	return reconcileAbortedCollector(directory, signature, collectorID)
}

func validateInterruptedPartials(directory string, entries map[string]os.FileInfo, maximum int64) error {
	var total int64
	for _, name := range []string{"stdout.partial", "stderr.partial"} {
		if _, found := entries[name]; !found {
			continue
		}
		size, err := verifiedRegularFileSize(filepath.Join(directory, name), maximum)
		if err != nil {
			return err
		}
		if size > maximum-total {
			return outputIntegrity("partial_output", "exceeds the collector's shared byte limit", nil)
		}
		total += size
	}
	return nil
}

func verifiedRegularFileSize(path string, maximum int64) (int64, error) {
	file, before, err := openVerifiedRegularFile(path)
	if err != nil {
		return 0, err
	}
	if before.Size() < 0 || before.Size() > maximum {
		_ = file.Close()
		return 0, outputIntegrity("collector_file", "must be a bounded regular file", nil)
	}
	if err := closeVerifiedRegularFile(file, before); err != nil {
		return 0, err
	}
	return before.Size(), nil
}

func verifiedDirectoryEntries(directory string) (map[string]os.FileInfo, error) {
	before, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, outputIntegrity("directory", "must be a non-symlink directory", nil)
	}
	handle, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	afterOpen, err := handle.Stat()
	if err != nil || !os.SameFile(before, afterOpen) || !afterOpen.IsDir() {
		_ = handle.Close()
		return nil, outputIntegrity("directory", "identity changed while opening", err)
	}
	entries, readErr := handle.ReadDir(-1)
	afterRead, statErr := handle.Stat()
	closeErr := handle.Close()
	if readErr != nil || statErr != nil || closeErr != nil || !os.SameFile(afterOpen, afterRead) {
		return nil, outputIntegrity("directory", "changed while enumerating", errors.Join(readErr, statErr, closeErr))
	}
	result := make(map[string]os.FileInfo, len(entries))
	for _, entry := range entries {
		fromDescriptor, err := entry.Info()
		if err != nil {
			return nil, err
		}
		fromPath, err := os.Lstat(filepath.Join(directory, entry.Name()))
		if err != nil || !os.SameFile(fromDescriptor, fromPath) || fromDescriptor.Mode() != fromPath.Mode() {
			return nil, outputIntegrity("directory_entry", "identity changed after enumeration", err)
		}
		result[entry.Name()] = fromPath
	}
	return result, nil
}

func ensureChildDirectory(parent, name string) (string, bool, error) {
	if filepath.Base(name) != name || name == "." || name == ".." || name == "" {
		return "", false, outputIntegrity("directory_name", "is not one canonical path component", nil)
	}
	if _, err := verifiedDirectoryEntries(parent); err != nil {
		return "", false, err
	}
	path := filepath.Join(parent, name)
	if _, err := verifiedDirectoryEntries(path); err == nil {
		return path, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return "", false, err
	}
	if _, err := verifiedDirectoryEntries(path); err != nil {
		return "", false, err
	}
	if err := syncCaptureDirectory(parent); err != nil {
		return "", false, err
	}
	return path, true, nil
}

func promotePendingControlFile(directory, name string) error {
	pending := filepath.Join(directory, name+".pending")
	destination := filepath.Join(directory, name)
	if _, err := verifiedRegularFileSize(pending, maximumLocalCaptureManifestBytes); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		return outputIntegrity("control_file", "already exists while its pending form remains", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(pending, destination); err != nil {
		return err
	}
	return syncCaptureDirectory(directory)
}

func requireExactRunEntries(directory string, expected map[string]ports.InterruptedCollectorBinding) error {
	entries, err := verifiedDirectoryEntries(directory)
	if err != nil {
		return err
	}
	if len(entries) != len(expected) {
		return outputIntegrity("run_directory", "does not contain exactly the expected collectors", nil)
	}
	for name, info := range entries {
		if _, found := expected[name]; !found || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return outputIntegrity("run_directory", "contains an invalid or unclaimed collector entry", nil)
		}
	}
	return nil
}

func requireExactCollectorFiles(directory string, wanted ...string) error {
	entries, err := verifiedDirectoryEntries(directory)
	if err != nil {
		return err
	}
	if len(entries) != len(wanted) {
		return outputIntegrity("collector_directory", "contains files outside its terminal state", nil)
	}
	for _, name := range wanted {
		info, found := entries[name]
		if !found || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return outputIntegrity("collector_directory", "does not contain its exact terminal files", nil)
		}
	}
	return nil
}

type objectReachability struct {
	Referenced map[string]struct{}
	Partials   map[string][]string
}

func (f *LocalOutputFactory) scanRunObjectReachability() (objectReachability, error) {
	result := objectReachability{Referenced: make(map[string]struct{}), Partials: make(map[string][]string)}
	runsDirectory := filepath.Join(f.root, "runs")
	runs, err := verifiedDirectoryEntries(runsDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return objectReachability{}, err
	}
	for runName, runInfo := range runs {
		if !runInfo.IsDir() || runInfo.Mode()&os.ModeSymlink != 0 {
			return objectReachability{}, outputIntegrity("run_directory", "contains a non-directory entry", nil)
		}
		if _, err := domain.ParseTargetRunID(runName); err != nil {
			return objectReachability{}, outputIntegrity("run_directory", "contains an invalid run identity", err)
		}
		runDirectory := filepath.Join(runsDirectory, runName)
		collectors, err := verifiedDirectoryEntries(runDirectory)
		if err != nil {
			return objectReachability{}, err
		}
		for collectorName, collectorInfo := range collectors {
			if !collectorInfo.IsDir() || collectorInfo.Mode()&os.ModeSymlink != 0 {
				return objectReachability{}, outputIntegrity("collector_directory", "must be a non-symlink directory", nil)
			}
			if _, err := domain.ParseCollectorID(collectorName); err != nil {
				return objectReachability{}, outputIntegrity("collector_directory", "contains an invalid collector identity", err)
			}
			collectorDirectory := filepath.Join(runDirectory, collectorName)
			files, err := verifiedDirectoryEntries(collectorDirectory)
			if err != nil {
				return objectReachability{}, err
			}
			for name, info := range files {
				if _, found := collectorTransactionFiles[name]; !found {
					return objectReachability{}, outputIntegrity("collector_directory", "contains an unclaimed file", nil)
				}
				if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
					return objectReachability{}, outputIntegrity("collector_file", "must be regular and non-symlink", nil)
				}
			}
			for _, name := range []string{"stdout.partial", "stderr.partial"} {
				if _, found := files[name]; !found {
					continue
				}
				path := filepath.Join(collectorDirectory, name)
				digest, err := hashVerifiedRegularFile(path)
				if err != nil {
					return objectReachability{}, err
				}
				result.Partials[digest] = append(result.Partials[digest], path)
			}
			for _, value := range []struct {
				name    string
				pending bool
			}{{"finalized.json", false}, {"finalized.json.pending", true}} {
				if _, found := files[value.name]; !found {
					continue
				}
				references, err := readManifestObjectReferences(filepath.Join(collectorDirectory, value.name), collectorName, value.pending)
				if err != nil {
					return objectReachability{}, err
				}
				for digest := range references {
					result.Referenced[digest] = struct{}{}
				}
			}
		}
	}
	return result, nil
}

func readManifestObjectReferences(path, collectorID string, pending bool) (map[string]struct{}, error) {
	encoded, err := readBoundedRegularFile(path, maximumLocalCaptureManifestBytes)
	if err != nil {
		return nil, err
	}
	manifest, err := decodeLocalCaptureManifest(encoded)
	if err != nil {
		if pending {
			return nil, nil
		}
		return nil, outputIntegrity("manifest", "finalized manifest is malformed", err)
	}
	if manifest.Version != localCaptureManifestVersion || len(manifest.Artifacts) != 2 {
		return nil, outputIntegrity("manifest", "has an invalid format or artifact count", nil)
	}
	if _, err := domain.ParseDigest(manifest.Signature); err != nil {
		return nil, outputIntegrity("manifest.signature", "is invalid", err)
	}
	references := make(map[string]struct{}, len(manifest.Artifacts))
	roles := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		digest, err := domain.ParseDigest(artifact.Digest)
		if err != nil || artifact.Size < 0 || !domain.Sensitivity(artifact.Sensitivity).IsValid() {
			return nil, outputIntegrity("manifest.artifact", "has invalid digest, size, or sensitivity", err)
		}
		if artifact.Role != CollectorStdoutRole && artifact.Role != CollectorStderrRole {
			return nil, outputIntegrity("manifest.artifact", "has an invalid stream role", nil)
		}
		if _, duplicate := roles[artifact.Role]; duplicate {
			return nil, outputIntegrity("manifest.artifact", "duplicates a stream role", nil)
		}
		roles[artifact.Role] = struct{}{}
		expectedReference := "observer://collectors/" + collectorID + "/" + strings.TrimPrefix(artifact.Role, "collector.") + "/" + digest.String()
		if artifact.Reference != expectedReference {
			return nil, outputIntegrity("manifest.artifact", "reference does not match its collector and digest", nil)
		}
		references[strings.TrimPrefix(digest.String(), "sha256:")] = struct{}{}
	}
	return references, nil
}

func (f *LocalOutputFactory) classifyInterruptedObjectPending(partials map[string][]string) (map[string]struct{}, error) {
	objectsDirectory := filepath.Join(f.root, "objects")
	entries, err := verifiedDirectoryEntries(objectsDirectory)
	if err != nil {
		return nil, err
	}
	approved := make(map[string]struct{})
	for name, info := range entries {
		digest, pending, valid := parseObjectEntryName(name)
		if !valid {
			return nil, outputIntegrity("objects", "contains an unclaimed entry", nil)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, outputIntegrity("object", "must be a regular non-symlink file", nil)
		}
		if !pending {
			continue
		}
		path := filepath.Join(objectsDirectory, name)
		observed, err := hashVerifiedRegularFile(path)
		if err != nil {
			return nil, err
		}
		if observed == digest {
			continue
		}
		for _, partialPath := range partials[digest] {
			prefix, err := verifiedRegularFilePrefix(path, partialPath)
			if err != nil {
				return nil, err
			}
			if prefix {
				approved[digest] = struct{}{}
				break
			}
		}
		if _, found := approved[digest]; !found {
			return nil, outputIntegrity("pending_object", "does not contain its named digest and is not interrupted staging for an exact partial", nil)
		}
	}
	return approved, nil
}

func (f *LocalOutputFactory) reconcileObjectNamespace(referenced, interruptedPending map[string]struct{}) error {
	objectsDirectory := filepath.Join(f.root, "objects")
	entries, err := verifiedDirectoryEntries(objectsDirectory)
	if err != nil {
		return err
	}
	type objectFiles struct {
		final, pending string
		finalDigest    string
		pendingDigest  string
	}
	objects := make(map[string]objectFiles)
	for name, info := range entries {
		digest, pending, valid := parseObjectEntryName(name)
		if !valid {
			return outputIntegrity("objects", "contains an unclaimed entry", nil)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return outputIntegrity("object", "must be a regular non-symlink file", nil)
		}
		path := filepath.Join(objectsDirectory, name)
		observed, err := hashVerifiedRegularFile(path)
		if err != nil {
			return err
		}
		files := objects[digest]
		if pending {
			files.pending, files.pendingDigest = path, observed
		} else {
			files.final, files.finalDigest = path, observed
		}
		objects[digest] = files
	}
	digests := make([]string, 0, len(objects))
	for digest := range objects {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	for _, digest := range digests {
		files := objects[digest]
		_, live := referenced[digest]
		if files.final != "" {
			if files.finalDigest != digest {
				return outputIntegrity("object", "final content does not match its digest filename", nil)
			}
			if files.pending != "" {
				return outputIntegrity("object", "has conflicting final and pending files", nil)
			}
			if !live {
				if err := removeVerifiedRegularFile(files.final); err != nil {
					return err
				}
			}
			continue
		}
		if files.pending == "" {
			continue
		}
		if files.pendingDigest == digest {
			if live {
				if err := promotePendingObject(objectsDirectory, digest); err != nil {
					return err
				}
			} else if err := removeVerifiedRegularFile(files.pending); err != nil {
				return err
			}
			continue
		}
		if _, approved := interruptedPending[digest]; !approved || live {
			return outputIntegrity("pending_object", "is neither valid unreferenced interrupted staging nor a complete live object", nil)
		}
		if err := removeVerifiedRegularFile(files.pending); err != nil {
			return err
		}
	}
	if err := syncCaptureDirectory(objectsDirectory); err != nil {
		return err
	}
	remaining, err := verifiedDirectoryEntries(objectsDirectory)
	if err != nil {
		return err
	}
	if len(remaining) != len(referenced) {
		return outputIntegrity("objects", "does not contain exactly the finalized manifest object set", nil)
	}
	for name := range remaining {
		digest, pending, valid := parseObjectEntryName(name)
		if !valid || pending {
			return outputIntegrity("objects", "contains an unclassified entry after reconciliation", nil)
		}
		if _, live := referenced[digest]; !live {
			return outputIntegrity("objects", "contains an unreferenced final object after reconciliation", nil)
		}
		observed, err := hashVerifiedRegularFile(filepath.Join(objectsDirectory, name))
		if err != nil || observed != digest {
			return outputIntegrity("object", "does not match its finalized manifest digest", err)
		}
	}
	return nil
}

func parseObjectEntryName(name string) (string, bool, bool) {
	pending := strings.HasSuffix(name, ".pending")
	digest := strings.TrimSuffix(name, ".pending")
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return "", false, false
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return "", false, false
	}
	canonical, err := domain.ParseDigest("sha256:" + digest)
	if err != nil || strings.TrimPrefix(canonical.String(), "sha256:") != digest {
		return "", false, false
	}
	return digest, pending, true
}

func hashVerifiedRegularFile(path string) (string, error) {
	file, before, err := openVerifiedRegularFile(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := closeVerifiedRegularFile(file, before)
	if copyErr != nil || closeErr != nil {
		return "", errors.Join(copyErr, closeErr)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func verifiedRegularFilePrefix(prefixPath, fullPath string) (bool, error) {
	prefix, prefixInfo, err := openVerifiedRegularFile(prefixPath)
	if err != nil {
		return false, err
	}
	full, fullInfo, err := openVerifiedRegularFile(fullPath)
	if err != nil {
		_ = prefix.Close()
		return false, err
	}
	if prefixInfo.Size() > fullInfo.Size() {
		return false, errors.Join(closeVerifiedRegularFile(prefix, prefixInfo), closeVerifiedRegularFile(full, fullInfo))
	}
	left := make([]byte, 32<<10)
	right := make([]byte, len(left))
	remaining := prefixInfo.Size()
	matches := true
	for remaining > 0 {
		amount := int64(len(left))
		if amount > remaining {
			amount = remaining
		}
		if _, err := io.ReadFull(prefix, left[:amount]); err != nil {
			matches = false
			break
		}
		if _, err := io.ReadFull(full, right[:amount]); err != nil || !bytes.Equal(left[:amount], right[:amount]) {
			matches = false
			break
		}
		remaining -= amount
	}
	closeErr := errors.Join(closeVerifiedRegularFile(prefix, prefixInfo), closeVerifiedRegularFile(full, fullInfo))
	return matches, closeErr
}

func openVerifiedRegularFile(path string) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, outputIntegrity("file", "must be a regular non-symlink file", nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	afterOpen, err := file.Stat()
	if err != nil || !afterOpen.Mode().IsRegular() || !os.SameFile(before, afterOpen) || afterOpen.Size() != before.Size() {
		_ = file.Close()
		return nil, nil, outputIntegrity("file", "identity changed while opening", err)
	}
	return file, afterOpen, nil
}

func closeVerifiedRegularFile(file *os.File, before os.FileInfo) error {
	after, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || closeErr != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		return outputIntegrity("file", "changed while reading", errors.Join(statErr, closeErr))
	}
	return nil
}

func promotePendingObject(directory, digest string) error {
	pending := filepath.Join(directory, digest+".pending")
	final := filepath.Join(directory, digest)
	if _, err := os.Lstat(final); err == nil {
		return outputIntegrity("object", "final appeared while pending object was promoted", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(pending, final); err != nil {
		return err
	}
	return syncCaptureDirectory(directory)
}

func hasCommittedCollector(bindings []ports.InterruptedCollectorBinding) bool {
	for _, binding := range bindings {
		if binding.StartCommitted {
			return true
		}
	}
	return false
}

func hasPartial(entries map[string]os.FileInfo) bool {
	_, stdout := entries["stdout.partial"]
	_, stderr := entries["stderr.partial"]
	return stdout || stderr
}

func hasCompletePartialSet(entries map[string]os.FileInfo) bool {
	_, stdout := entries["stdout.partial"]
	_, stderr := entries["stderr.partial"]
	return stdout && stderr
}

func outputIntegrity(field, message string, cause error) error {
	return domain.NewError(domain.CodeIntegrityViolation, "observer.local_output.reconcile", field, message, cause)
}

var _ OutputFactory = (*LocalOutputFactory)(nil)
