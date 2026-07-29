package linuxcontainer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	workspacepkg "github.com/philcantcode/go-world-management-layer/internal/workspace"
)

const (
	runBaselineFile     = "writable-baseline.json"
	maximumBaselineSize = int64(32 << 20)
	runStartFile        = "execution-start.json"
	maximumRunStartSize = int64(4096)
)

type persistedRunStart struct {
	SchemaVersion    int                     `json:"schema_version"`
	LeaseID          domain.LeaseID          `json:"lease_id"`
	TargetID         domain.TargetID         `json:"target_id"`
	TargetGeneration domain.TargetGeneration `json:"target_generation"`
	RunID            domain.TargetRunID      `json:"run_id"`
	StartedAt        time.Time               `json:"started_at"`
	RuntimeID        string                  `json:"runtime_id"`
	CgroupID         string                  `json:"cgroup_id,omitempty"`
	Materialization  domain.Digest           `json:"materialization_digest"`
}

func persistRunBaseline(directory string, manifest workspacepkg.Manifest) error {
	if err := workspacepkg.ValidateManifest(manifest); err != nil {
		return err
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return persistRunRecord(directory, runBaselineFile, payload, maximumBaselineSize)
}

func loadRunBaseline(directory string) (workspacepkg.Manifest, error) {
	payload, err := loadRunRecord(directory, runBaselineFile, maximumBaselineSize)
	if err != nil {
		return workspacepkg.Manifest{}, err
	}
	var manifest workspacepkg.Manifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return workspacepkg.Manifest{}, err
	}
	if err := workspacepkg.ValidateManifest(manifest); err != nil {
		return workspacepkg.Manifest{}, err
	}
	return manifest, nil
}

func persistRunStart(directory string, authority RunAuthority, startedAt time.Time, runtimeID, cgroupID string, materialization domain.Digest) error {
	if startedAt.IsZero() || runtimeID == "" || materialization.IsZero() {
		return fmt.Errorf("run start time, runtime identity, and materialization are required")
	}
	record := persistedRunStart{
		SchemaVersion: 1, LeaseID: authority.LeaseID, TargetID: authority.TargetID,
		TargetGeneration: authority.Generation, RunID: authority.RunID, StartedAt: startedAt.UTC(),
		RuntimeID: runtimeID, CgroupID: cgroupID, Materialization: materialization,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return persistRunRecord(directory, runStartFile, payload, maximumRunStartSize)
}

func loadRunStart(directory string, authority RunAuthority) (persistedRunStart, bool, error) {
	payload, err := loadRunRecord(directory, runStartFile, maximumRunStartSize)
	if errors.Is(err, fs.ErrNotExist) {
		return persistedRunStart{}, false, nil
	}
	if err != nil {
		return persistedRunStart{}, false, err
	}
	var record persistedRunStart
	if err := json.Unmarshal(payload, &record); err != nil {
		return persistedRunStart{}, false, err
	}
	if record.SchemaVersion != 1 || record.LeaseID != authority.LeaseID || record.TargetID != authority.TargetID || record.TargetGeneration != authority.Generation || record.RunID != authority.RunID || record.StartedAt.IsZero() || record.RuntimeID == "" || record.Materialization.IsZero() {
		return persistedRunStart{}, false, fmt.Errorf("run start record does not match the recovered authority")
	}
	record.StartedAt = record.StartedAt.UTC()
	return record, true, nil
}

func persistRunRecord(directory, name string, payload []byte, maximumSize int64) error {
	return publishCompleteRunRecord(directory, name, payload, maximumSize, os.Link)
}

func publishCompleteRunRecord(directory, name string, payload []byte, maximumSize int64, publish func(string, string) error) error {
	if int64(len(payload)) > maximumSize {
		return fmt.Errorf("run record %q exceeds %d bytes", name, maximumSize)
	}
	if filepath.Base(name) != name || name == "." {
		return fmt.Errorf("run record name %q is not a single path component", name)
	}
	if publish == nil {
		return fmt.Errorf("run record publisher is required")
	}
	file, err := os.CreateTemp(directory, "."+name+".tmp-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	removeTemporary := func() error {
		err := os.Remove(temporary)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(err, file.Close(), removeTemporary())
	}
	written, writeErr := file.Write(payload)
	if writeErr == nil && written != len(payload) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errors.Join(err, removeTemporary())
	}
	// Hard-linking a fully synced temporary file publishes the immutable final
	// name atomically and without replacement. A crash before this point can
	// leave only an ignored temporary file; a crash afterwards can expose only
	// the complete payload.
	path := filepath.Join(directory, name)
	if err := publish(temporary, path); err != nil {
		return errors.Join(err, removeTemporary())
	}
	return errors.Join(removeTemporary(), syncRunRecordDirectory(directory))
}

func syncRunRecordDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(handle.Sync(), handle.Close())
}

func loadRunRecord(directory, name string, maximumSize int64) ([]byte, error) {
	opened, err := safepath.OpenRegular(directory, name)
	if err != nil {
		return nil, err
	}
	if opened.Size() > maximumSize {
		closeErr := opened.Close()
		return nil, errors.Join(fmt.Errorf("run record %q exceeds %d bytes", name, maximumSize), closeErr)
	}
	payload, readErr := io.ReadAll(io.LimitReader(opened, maximumSize+1))
	closeErr := opened.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if int64(len(payload)) > maximumSize {
		return nil, fmt.Errorf("run record %q exceeds %d bytes", name, maximumSize)
	}
	return payload, nil
}
