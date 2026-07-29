package research

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// StaticContextOptions configures binary/static metadata capture.
type StaticContextOptions struct {
	LookPath           func(file string) (string, error)
	MaxStaticFileBytes int64
}

// staticContextCollector gathers file type, hashes, and basic PE/ELF metadata.
type staticContextCollector struct {
	opts StaticContextOptions
}

// NewStaticContextCollector builds a static_context companion.
func NewStaticContextCollector(opts StaticContextOptions) StaticContextCollector {
	if opts.LookPath == nil {
		opts.LookPath = exec.LookPath
	}
	opts.MaxStaticFileBytes = boundedByteLimit(opts.MaxStaticFileBytes, defaultMaxStaticFileBytes, maximumMaxStaticFileBytes)
	return &staticContextCollector{opts: opts}
}

// Capture inspects the action executable path best-effort.
func (c *staticContextCollector) Capture(ctx context.Context, start ActionStart, actionDir string) (StaticContextSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return StaticContextSnapshot{}, err
	}
	if err := os.MkdirAll(filepath.Join(actionDir, "static"), 0o700); err != nil {
		return StaticContextSnapshot{Available: false, Reason: ReasonStaticCaptureFailed}, nil
	}
	executable := strings.TrimSpace(start.Executable)
	if executable == "" {
		return StaticContextSnapshot{Available: false, Reason: ReasonStaticUnavailable}, nil
	}
	// Resolve via LookPath when not absolute.
	resolved := executable
	if !filepath.IsAbs(executable) {
		if path, err := c.opts.LookPath(executable); err == nil && path != "" {
			resolved = path
		}
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return StaticContextSnapshot{
			Executable: boundText(executable, maximumEvidenceTextBytes),
			Available:  false,
			Reason:     ReasonStaticUnavailable,
		}, nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		// Hash the symlink target path string only; do not follow for content
		// when it escapes — still record type.
	}
	if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		return StaticContextSnapshot{
			Executable: boundText(resolved, maximumEvidenceTextBytes),
			Available:  false,
			Reason:     ReasonStaticUnavailable,
		}, nil
	}

	// Open the path carefully: if symlink, open through os.Open after eval within bounds.
	pathToRead := resolved
	if info.Mode()&os.ModeSymlink != 0 {
		if target, err := filepath.EvalSymlinks(resolved); err == nil {
			pathToRead = target
			info, err = os.Stat(pathToRead)
			if err != nil || !info.Mode().IsRegular() {
				return StaticContextSnapshot{
					Executable: boundText(resolved, maximumEvidenceTextBytes),
					Available:  false,
					Reason:     ReasonStaticUnavailable,
				}, nil
			}
		}
	}

	digest, hashed, truncated, hashReason := hashFileBounded(ctx, pathToRead, c.opts.MaxStaticFileBytes)
	fileType, meta := inspectBinaryHeader(pathToRead)
	// Optional `file` tool enrichment.
	if tool, err := c.opts.LookPath("file"); err == nil && tool != "" {
		if out, err := exec.CommandContext(ctx, tool, "-b", "--", pathToRead).Output(); err == nil {
			desc := boundText(strings.TrimSpace(string(out)), 512)
			if desc != "" {
				if fileType == "" || fileType == "unknown" {
					fileType = desc
				}
				if meta == nil {
					meta = map[string]any{}
				}
				meta["file_tool"] = desc
			}
		}
	}
	// Optional checksec-like: presence of tools only as capability notes.
	if meta == nil {
		meta = map[string]any{}
	}
	meta["observed_at"] = time.Now().UTC()
	if hashReason != "" {
		meta["hash_reason"] = hashReason
	}

	snap := StaticContextSnapshot{
		Executable: boundText(resolved, maximumEvidenceTextBytes),
		FileType:   boundText(fileType, 256),
		SHA256:     digest,
		Size:       info.Size(),
		Metadata:   meta,
		Available:  true,
		Attributed: true, // tied to action executable path
	}
	if truncated {
		meta["content_truncated"] = true
	}
	_ = hashed
	rel := filepath.ToSlash(filepath.Join("static", "context.json"))
	snap.ArtifactPath = rel
	if err := writeJSON(filepath.Join(actionDir, "static", "context.json"), snap); err != nil {
		return StaticContextSnapshot{Available: false, Reason: ReasonStaticCaptureFailed}, nil
	}
	return snap, nil
}

func hashFileBounded(ctx context.Context, path string, limit int64) (digest string, hashed int64, truncated bool, reason string) {
	handle, err := os.Open(path)
	if err != nil {
		return "", 0, false, "unreadable"
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return "", 0, false, "stat_failed"
	}
	hash := sha256.New()
	reader := io.LimitReader(handle, limit)
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", total, true, "cancelled"
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
			total += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", total, true, "read_error"
		}
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), total, total < info.Size(), ""
}

func inspectBinaryHeader(path string) (string, map[string]any) {
	handle, err := os.Open(path)
	if err != nil {
		return "unknown", nil
	}
	defer handle.Close()
	header := make([]byte, 64)
	n, _ := io.ReadFull(handle, header)
	if n < 4 {
		return "unknown", nil
	}
	meta := map[string]any{}
	switch {
	case n >= 4 && header[0] == 0x7f && header[1] == 'E' && header[2] == 'L' && header[3] == 'F':
		meta["format"] = "ELF"
		if n >= 5 {
			switch header[4] {
			case 1:
				meta["class"] = "ELF32"
			case 2:
				meta["class"] = "ELF64"
			}
		}
		if n >= 18 {
			machine := binary.LittleEndian.Uint16(header[18:20])
			meta["machine"] = machine
		}
		return "ELF", meta
	case n >= 2 && header[0] == 'M' && header[1] == 'Z':
		meta["format"] = "PE"
		// Optional PE offset at 0x3c
		if n >= 64 {
			peOff := binary.LittleEndian.Uint32(header[0x3c:0x40])
			meta["pe_offset"] = peOff
		}
		return "PE", meta
	case n >= 4 && header[0] == 0xca && header[1] == 0xfe && header[2] == 0xba && header[3] == 0xbe:
		return "Mach-O-fat", map[string]any{"format": "Mach-O-fat"}
	case n >= 4 && header[0] == 0xcf && header[1] == 0xfa && header[2] == 0xed && header[3] == 0xfe:
		return "Mach-O-64", map[string]any{"format": "Mach-O-64"}
	case n >= 4 && header[0] == '#' && header[1] == '!':
		line := string(header[:n])
		if idx := strings.IndexByte(line, '\n'); idx >= 0 {
			line = line[:idx]
		}
		return "script", map[string]any{"shebang": boundText(line, 128)}
	default:
		return "unknown", meta
	}
}

var _ StaticContextCollector = (*staticContextCollector)(nil)
