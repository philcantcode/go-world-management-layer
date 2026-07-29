package research

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TargetOracleOptions configures target-side log/trace collection.
type TargetOracleOptions struct {
	// Paths are absolute regular files the operator configured for oracle tailing.
	// Relative paths are rejected.
	Paths          []string
	MaxOracleBytes int64
}

// targetOracleCollector reads configured target logs/traces. Without configured
// paths it records an explicit not-configured gap.
type targetOracleCollector struct {
	opts TargetOracleOptions
}

// NewTargetOracleCollector builds a target_oracle companion.
func NewTargetOracleCollector(opts TargetOracleOptions) TargetOracleCollector {
	opts.MaxOracleBytes = boundedByteLimit(opts.MaxOracleBytes, defaultMaxOracleBytes, maximumMaxOracleBytes)
	return &targetOracleCollector{opts: opts}
}

// Capture tails configured oracle paths into the action target/ directory.
func (c *targetOracleCollector) Capture(ctx context.Context, start ActionStart, actionDir string) (TargetOracleSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return TargetOracleSnapshot{}, err
	}
	if err := os.MkdirAll(filepath.Join(actionDir, "target"), 0o700); err != nil {
		return TargetOracleSnapshot{Available: false, Reason: ReasonOracleCaptureFailed}, nil
	}
	if len(c.opts.Paths) == 0 {
		snap := TargetOracleSnapshot{
			Available: false, Attributed: false, Reason: ReasonOracleNotConfigured, Scope: "unconfigured",
		}
		_ = writeJSON(filepath.Join(actionDir, "target", "oracle.json"), snap)
		return snap, nil
	}
	records := make([]map[string]any, 0, len(c.opts.Paths))
	var totalRead int64
	truncated := false
	for i, rawPath := range c.opts.Paths {
		if err := ctx.Err(); err != nil {
			return TargetOracleSnapshot{}, err
		}
		path := strings.TrimSpace(rawPath)
		if path == "" {
			continue
		}
		if strings.Contains(path, "..") || !filepath.IsAbs(path) {
			records = append(records, map[string]any{"index": i, "status": "rejected_path"})
			continue
		}
		clean := filepath.Clean(path)
		if !filepath.IsAbs(clean) {
			records = append(records, map[string]any{"index": i, "status": "rejected_path"})
			continue
		}
		remaining := c.opts.MaxOracleBytes - totalRead
		if remaining <= 0 {
			truncated = true
			records = append(records, map[string]any{"index": i, "status": "budget_exhausted"})
			continue
		}
		content, read, err := readOracleFileTail(clean, remaining)
		if err != nil {
			status := "unreadable"
			if os.IsNotExist(err) {
				status = "missing"
			}
			if err == errOracleNotRegular {
				status = "not_regular_file"
			}
			records = append(records, map[string]any{"index": i, "status": status})
			continue
		}
		totalRead += read
		base := filepath.Base(clean)
		outName := fmtOracleName(i, base)
		outPath := filepath.Join(actionDir, "target", outName)
		if err := os.WriteFile(outPath, content, 0o600); err != nil {
			records = append(records, map[string]any{"index": i, "status": "write_failed"})
			continue
		}
		records = append(records, map[string]any{
			"index":  i,
			"status": "captured",
			"bytes":  read,
			"name":   boundText(base, 256),
			"path":   filepath.ToSlash(filepath.Join("target", outName)),
		})
	}
	if len(records) == 0 {
		snap := TargetOracleSnapshot{Available: false, Attributed: false, Reason: ReasonOracleUnavailable, Scope: "configured"}
		_ = writeJSON(filepath.Join(actionDir, "target", "oracle.json"), snap)
		return snap, nil
	}
	hasCapture := false
	for _, rec := range records {
		if rec["status"] == "captured" {
			hasCapture = true
			break
		}
	}
	snap := TargetOracleSnapshot{
		Records: map[string]any{
			"observed_at": time.Now().UTC(),
			"sources":     records,
		},
		Scope:      "configured_paths",
		Available:  hasCapture,
		Attributed: hasCapture,
		Truncated:  truncated,
	}
	if !hasCapture {
		snap.Reason = ReasonOracleUnavailable
	} else {
		snap.ArtifactPath = filepath.ToSlash(filepath.Join("target", "oracle.json"))
	}
	_ = writeJSON(filepath.Join(actionDir, "target", "oracle.json"), snap)
	return snap, nil
}

var errOracleNotRegular = os.ErrInvalid

func fmtOracleName(index int, base string) string {
	base = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, base)
	if base == "" || base == "." || base == ".." {
		base = "log"
	}
	if len(base) > 64 {
		base = base[:64]
	}
	return "oracle-" + strconvFormatInt(int64(index)) + "-" + base
}

// readOracleFileTail is implemented per-OS for O_NOFOLLOW / SameFile checks.
// Shared tail-from-handle helper:
func readTailFromHandle(handle *os.File, limit int64) ([]byte, int64, error) {
	info, err := handle.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errOracleNotRegular
	}
	size := info.Size()
	if size <= limit {
		data, err := io.ReadAll(io.LimitReader(handle, limit))
		return data, int64(len(data)), err
	}
	if _, err := handle.Seek(size-limit, io.SeekStart); err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(io.LimitReader(handle, limit))
	return data, int64(len(data)), err
}

var _ TargetOracleCollector = (*targetOracleCollector)(nil)
