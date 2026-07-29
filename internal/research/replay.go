package research

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

// ReplayCollectorImpl writes a minimal reproducibility package for an action.
// Environment values are never retained — only sorted keys from ActionStart.
type ReplayCollectorImpl struct{}

// NewReplayCollector builds a replay companion.
func NewReplayCollector() *ReplayCollectorImpl {
	return &ReplayCollectorImpl{}
}

// Capture persists argv, cwd, env keys, optional stdin hash, and capture refs.
func (c *ReplayCollectorImpl) Capture(ctx context.Context, start ActionStart, outcome ActionOutcome, actionDir string, captureRefs []string) (ReplayPackage, error) {
	if err := ctx.Err(); err != nil {
		return ReplayPackage{}, err
	}
	if err := os.MkdirAll(filepath.Join(actionDir, "replay"), 0o700); err != nil {
		return ReplayPackage{Available: false, Reason: ReasonReplayCaptureFailed}, nil
	}
	pkg := ReplayPackage{
		ActionID:         start.ActionID,
		Executable:       boundText(start.Executable, maximumEvidenceTextBytes),
		Argv:             append([]string(nil), start.Argv...),
		WorkingDirectory: boundText(start.WorkingDirectory, maximumEvidenceTextBytes),
		EnvironmentKeys:  append([]string(nil), start.EnvironmentKeys...),
		CaptureRefs:      append([]string(nil), captureRefs...),
		StimulusClass:    string(start.StimulusClass),
		Available:        true,
	}
	// Bound argv element sizes to avoid retaining huge secret-bearing blobs.
	for i := range pkg.Argv {
		pkg.Argv[i] = boundText(pkg.Argv[i], maximumEvidenceTextBytes)
	}
	// Optional stdin hash from a well-known path if present (callers may place it).
	stdinPath := filepath.Join(actionDir, "replay", "stdin.bin")
	if info, err := os.Stat(stdinPath); err == nil && info.Mode().IsRegular() {
		if digest, err := hashExistingFile(stdinPath, defaultMaxOracleBytes); err == nil {
			pkg.StdinSHA256 = digest
		}
	}
	if err := writeJSON(filepath.Join(actionDir, "replay", "package.json"), pkg); err != nil {
		return ReplayPackage{Available: false, Reason: ReasonReplayCaptureFailed}, nil
	}
	// Manifest of refs only (no secrets).
	_ = writeJSON(filepath.Join(actionDir, "replay", "manifest.json"), map[string]any{
		"action_id":    start.ActionID,
		"capture_refs": pkg.CaptureRefs,
		"exit_code":    outcome.ExitCode,
		"process_id":   outcome.ProcessID,
	})
	return pkg, nil
}

func hashExistingFile(path string, limit int64) (string, error) {
	handle, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer handle.Close()
	hash := sha256.New()
	buf := make([]byte, 32<<10)
	var total int64
	for {
		if total >= limit {
			break
		}
		toRead := len(buf)
		if rem := limit - total; int64(toRead) > rem {
			toRead = int(rem)
		}
		n, err := handle.Read(buf[:toRead])
		if n > 0 {
			_, _ = hash.Write(buf[:n])
			total += int64(n)
		}
		if err != nil {
			break
		}
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}

var _ ReplayCollector = (*ReplayCollectorImpl)(nil)
