package research

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const (
	defaultNetworkInterfaceLimit = 64
	defaultNetworkAddressLimit   = 32
	defaultActionEndpointLimit   = 32
	defaultStateEntryLimit       = 256
	defaultStateDepthLimit       = 4
	defaultStateFileBytes        = int64(1 << 20)
	defaultStateTotalBytes       = int64(4 << 20)

	maximumNetworkInterfaceLimit = 256
	maximumNetworkAddressLimit   = 128
	maximumActionEndpointLimit   = 256
	maximumStateEntryLimit       = 4096
	maximumStateDepthLimit       = 32
	maximumStateFileBytes        = int64(16 << 20)
	maximumStateTotalBytes       = int64(64 << 20)
	maximumEvidenceTextBytes     = 4096
)

// LocalHostCollector records a bounded identity snapshot of the collector
// process and the action it is about to observe. It does not claim that the
// collector process is the launched action process; child attribution comes
// from the transport process-lifecycle event at Seal.
type LocalHostCollector struct{}

// HostProcessTree is the typed payload stored in HostSnapshot.ProcessTree.
type HostProcessTree struct {
	ObservedAt time.Time            `json:"observed_at"`
	Host       LocalHostIdentity    `json:"host"`
	Collector  LocalProcessIdentity `json:"collector"`
	Action     LocalActionIdentity  `json:"action"`
	Warnings   []string             `json:"warnings,omitempty"`
}

// LocalHostIdentity describes the machine/runtime without collecting
// environment values.
type LocalHostIdentity struct {
	Hostname   string `json:"hostname,omitempty"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	CPUs       int    `json:"cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
}

// LocalProcessIdentity identifies the process performing the capture.
type LocalProcessIdentity struct {
	PID              int    `json:"pid"`
	ParentPID        int    `json:"parent_pid"`
	Executable       string `json:"executable,omitempty"`
	WorkingDirectory string `json:"working_directory,omitempty"`
}

// LocalActionIdentity is the bounded action context known before launch.
type LocalActionIdentity struct {
	ActionID         string      `json:"action_id"`
	Scope            ActionScope `json:"scope"`
	Executable       string      `json:"executable"`
	WorkingDirectory string      `json:"working_directory,omitempty"`
	ArgumentCount    int         `json:"argument_count"`
	EnvironmentKeys  int         `json:"environment_key_count"`
}

func (LocalHostCollector) Capture(ctx context.Context, start ActionStart) (HostSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return HostSnapshot{}, err
	}
	warnings := make([]string, 0, 3)
	executable, err := os.Executable()
	if err != nil {
		warnings = append(warnings, "collector_executable_unavailable")
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		warnings = append(warnings, "collector_working_directory_unavailable")
	}
	hostname, err := os.Hostname()
	if err != nil {
		warnings = append(warnings, "hostname_unavailable")
	}
	tree := HostProcessTree{
		ObservedAt: time.Now().UTC(),
		Host: LocalHostIdentity{
			Hostname: boundText(hostname, maximumEvidenceTextBytes), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
			CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		},
		Collector: LocalProcessIdentity{
			PID: os.Getpid(), ParentPID: os.Getppid(), Executable: boundText(executable, maximumEvidenceTextBytes),
			WorkingDirectory: boundText(workingDirectory, maximumEvidenceTextBytes),
		},
		Action: LocalActionIdentity{
			ActionID: start.ActionID, Scope: start.Scope, Executable: boundText(start.Executable, maximumEvidenceTextBytes),
			WorkingDirectory: boundText(start.WorkingDirectory, maximumEvidenceTextBytes), ArgumentCount: len(start.Argv),
			EnvironmentKeys: len(start.EnvironmentKeys),
		},
		Warnings: warnings,
	}
	return HostSnapshot{ProcessTree: tree, Scope: "collector_process", Available: true}, nil
}

// LocalNetworkCollector captures bounded interface/address inventory plus
// sanitized endpoint references present in action arguments. These are ambient
// and intent observations, not action-attributed flows.
type LocalNetworkCollector struct {
	MaxInterfaces      int
	MaxAddresses       int
	MaxActionEndpoints int
}

// LocalNetworkObservation is the typed payload stored in NetworkIndex.Flows.
type LocalNetworkObservation struct {
	ObservedAt          time.Time               `json:"observed_at"`
	Interfaces          []LocalNetworkInterface `json:"interfaces"`
	ActionEndpoints     []LocalActionEndpoint   `json:"action_endpoints,omitempty"`
	InterfacesTruncated bool                    `json:"interfaces_truncated,omitempty"`
	EndpointsTruncated  bool                    `json:"endpoints_truncated,omitempty"`
	Warnings            []string                `json:"warnings,omitempty"`
}

type LocalNetworkInterface struct {
	Index           int      `json:"index"`
	Name            string   `json:"name"`
	MTU             int      `json:"mtu"`
	Flags           string   `json:"flags"`
	HardwareAddress string   `json:"hardware_address,omitempty"`
	Addresses       []string `json:"addresses"`
	Truncated       bool     `json:"truncated,omitempty"`
}

// LocalActionEndpoint is a credential-free endpoint reference parsed from an
// action argument. URL paths, queries, fragments, and user information are not
// retained.
type LocalActionEndpoint struct {
	ArgumentIndex int    `json:"argument_index"`
	Scheme        string `json:"scheme"`
	Host          string `json:"host"`
	Port          string `json:"port,omitempty"`
}

func (c LocalNetworkCollector) Capture(ctx context.Context, start ActionStart) (NetworkIndex, error) {
	if err := ctx.Err(); err != nil {
		return NetworkIndex{}, err
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		return NetworkIndex{}, fmt.Errorf("enumerate local network interfaces: %w", err)
	}
	sort.Slice(interfaces, func(i, j int) bool {
		if interfaces[i].Index == interfaces[j].Index {
			return interfaces[i].Name < interfaces[j].Name
		}
		return interfaces[i].Index < interfaces[j].Index
	})
	interfaceLimit := boundedLimit(c.MaxInterfaces, defaultNetworkInterfaceLimit, maximumNetworkInterfaceLimit)
	addressLimit := boundedLimit(c.MaxAddresses, defaultNetworkAddressLimit, maximumNetworkAddressLimit)
	observation := LocalNetworkObservation{ObservedAt: time.Now().UTC()}
	if len(interfaces) > interfaceLimit {
		interfaces = interfaces[:interfaceLimit]
		observation.InterfacesTruncated = true
	}
	observation.Interfaces = make([]LocalNetworkInterface, 0, len(interfaces))
	for _, current := range interfaces {
		if err := ctx.Err(); err != nil {
			return NetworkIndex{}, err
		}
		item := LocalNetworkInterface{
			Index: current.Index, Name: boundText(current.Name, maximumEvidenceTextBytes), MTU: current.MTU,
			Flags: current.Flags.String(), HardwareAddress: current.HardwareAddr.String(),
		}
		addresses, addressErr := current.Addrs()
		if addressErr != nil {
			observation.Warnings = append(observation.Warnings, "interface_addresses_unavailable")
		} else {
			sort.Slice(addresses, func(i, j int) bool { return addresses[i].String() < addresses[j].String() })
			if len(addresses) > addressLimit {
				addresses = addresses[:addressLimit]
				item.Truncated = true
			}
			item.Addresses = make([]string, 0, len(addresses))
			for _, address := range addresses {
				item.Addresses = append(item.Addresses, boundText(address.String(), maximumEvidenceTextBytes))
			}
		}
		observation.Interfaces = append(observation.Interfaces, item)
	}
	endpointLimit := boundedLimit(c.MaxActionEndpoints, defaultActionEndpointLimit, maximumActionEndpointLimit)
	observation.ActionEndpoints, observation.EndpointsTruncated = actionEndpoints(start.Argv, endpointLimit)
	return NetworkIndex{Flows: observation, Scope: "collector_host", Available: true, Attributed: false}, nil
}

// WorkingDirectoryStateCollector captures bounded filesystem metadata and
// bounded content hashes under the action's locally resolvable working
// directory. It never follows directory symlinks.
type WorkingDirectoryStateCollector struct {
	MaxEntries           int
	MaxDepth             int
	MaxFileContentBytes  int64
	MaxTotalContentBytes int64
	// Attributed may be set only when the collector and action share this
	// filesystem namespace. The default collector records ambient host state.
	Attributed bool
}

// LocalStateEntry is the typed value stored in StateSnapshot.Entries.
type LocalStateEntry struct {
	Kind             string    `json:"kind"`
	Size             int64     `json:"size"`
	Mode             string    `json:"mode"`
	Permissions      uint32    `json:"permissions"`
	ModifiedAt       time.Time `json:"modified_at"`
	SHA256           string    `json:"sha256,omitempty"`
	HashedBytes      int64     `json:"hashed_bytes,omitempty"`
	ContentTruncated bool      `json:"content_truncated,omitempty"`
	ContentReason    string    `json:"content_reason,omitempty"`
}

func (c WorkingDirectoryStateCollector) CaptureBefore(ctx context.Context, start ActionStart) (StateSnapshot, error) {
	return c.capture(ctx, start)
}

func (c WorkingDirectoryStateCollector) CaptureAfter(ctx context.Context, start ActionStart) (StateSnapshot, error) {
	return c.capture(ctx, start)
}

func (WorkingDirectoryStateCollector) Diff(before, after StateSnapshot) StateDiff {
	if !before.Available || !after.Available {
		return StateDiff{Available: false, Reason: ReasonStateUnavailable}
	}
	if filepath.Clean(before.Root) != filepath.Clean(after.Root) {
		return StateDiff{Available: false, Reason: ReasonStateScopeChanged}
	}
	keys := make(map[string]struct{}, len(before.Entries)+len(after.Entries))
	for key := range before.Entries {
		keys[key] = struct{}{}
	}
	for key := range after.Entries {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	diff := StateDiff{Available: true, Attributed: before.Attributed && after.Attributed, Truncated: before.Truncated || after.Truncated}
	if diff.Truncated {
		diff.Reason = ReasonStateSnapshotTruncated
	}
	for _, key := range ordered {
		beforeEntry, existedBefore := before.Entries[key]
		afterEntry, existsAfter := after.Entries[key]
		switch {
		case !existedBefore:
			diff.Created = append(diff.Created, key)
			diff.Changed = append(diff.Changed, key)
		case !existsAfter:
			diff.Deleted = append(diff.Deleted, key)
			diff.Changed = append(diff.Changed, key)
		case !reflect.DeepEqual(beforeEntry, afterEntry):
			diff.Modified = append(diff.Modified, key)
			diff.Changed = append(diff.Changed, key)
		}
	}
	return diff
}

func (c WorkingDirectoryStateCollector) capture(ctx context.Context, start ActionStart) (StateSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return StateSnapshot{}, err
	}
	root, err := resolveActionWorkingDirectory(start.WorkingDirectory)
	if err != nil {
		return StateSnapshot{}, err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return StateSnapshot{Root: root, Scope: "collector_host_working_directory", CapturedAt: time.Now().UTC(), Available: false, Reason: ReasonStateScopeUnavailable}, nil
		}
		return StateSnapshot{}, fmt.Errorf("inspect action working directory: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return StateSnapshot{Root: root, Scope: "collector_host_working_directory", CapturedAt: time.Now().UTC(), Available: false, Reason: ReasonStateScopeUnavailable}, nil
	}
	entryLimit := boundedLimit(c.MaxEntries, defaultStateEntryLimit, maximumStateEntryLimit)
	depthLimit := boundedLimit(c.MaxDepth, defaultStateDepthLimit, maximumStateDepthLimit)
	fileBytes := boundedByteLimit(c.MaxFileContentBytes, defaultStateFileBytes, maximumStateFileBytes)
	totalBytes := boundedByteLimit(c.MaxTotalContentBytes, defaultStateTotalBytes, maximumStateTotalBytes)
	snapshot := StateSnapshot{
		Root: root, Scope: "collector_host_working_directory", CapturedAt: time.Now().UTC(), Available: true, Attributed: c.Attributed,
		Entries: make(map[string]any, entryLimit), Paths: make([]string, 0, entryLimit),
	}
	var hashedBytes int64
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			snapshot.Truncated = true
			snapshot.Reason = ReasonStateSnapshotTruncated
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		depth := 0
		if relative != "." {
			depth = strings.Count(relative, "/") + 1
		}
		if depth > depthLimit {
			snapshot.Truncated = true
			snapshot.Reason = ReasonStateSnapshotTruncated
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if len(snapshot.Entries) >= entryLimit {
			snapshot.Truncated = true
			snapshot.Reason = ReasonStateSnapshotTruncated
			return fs.SkipAll
		}
		if len(relative) > maximumEvidenceTextBytes {
			snapshot.Truncated = true
			snapshot.Reason = ReasonStateSnapshotTruncated
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			snapshot.Truncated = true
			snapshot.Reason = ReasonStateSnapshotTruncated
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		stateEntry := stateEntryFromInfo(info)
		if info.Mode().IsRegular() {
			remaining := totalBytes - hashedBytes
			limit := fileBytes
			if remaining < limit {
				limit = remaining
			}
			var read int64
			stateEntry, read = captureRegularStateEntry(ctx, root, relative, info, limit)
			hashedBytes += read
			if stateEntry.ContentTruncated || stateEntry.ContentReason != "" {
				snapshot.Truncated = true
				snapshot.Reason = ReasonStateSnapshotTruncated
			}
		}
		snapshot.Paths = append(snapshot.Paths, relative)
		snapshot.Entries[relative] = stateEntry
		return nil
	})
	if walkErr != nil {
		return StateSnapshot{}, fmt.Errorf("capture action working directory: %w", walkErr)
	}
	sort.Strings(snapshot.Paths)
	return snapshot, nil
}

func actionEndpoints(arguments []string, limit int) ([]LocalActionEndpoint, bool) {
	result := make([]LocalActionEndpoint, 0)
	seen := make(map[string]struct{})
	truncated := false
	for index, argument := range arguments {
		if len(argument) > maximumEvidenceTextBytes {
			truncated = true
			continue
		}
		parsed, err := url.Parse(strings.TrimSpace(argument))
		if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
			continue
		}
		endpoint := LocalActionEndpoint{
			ArgumentIndex: index, Scheme: boundText(strings.ToLower(parsed.Scheme), maximumEvidenceTextBytes),
			Host: boundText(parsed.Hostname(), maximumEvidenceTextBytes), Port: boundText(parsed.Port(), maximumEvidenceTextBytes),
		}
		key := endpoint.Scheme + "\x00" + endpoint.Host + "\x00" + endpoint.Port
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		if len(result) >= limit {
			truncated = true
			continue
		}
		seen[key] = struct{}{}
		result = append(result, endpoint)
	}
	return result, truncated
}

func resolveActionWorkingDirectory(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		current, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve current working directory: %w", err)
		}
		value = current
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve action working directory: %w", err)
	}
	return filepath.Clean(absolute), nil
}

func stateEntryFromInfo(info fs.FileInfo) LocalStateEntry {
	kind := "other"
	switch {
	case info.Mode().IsRegular():
		kind = "file"
	case info.IsDir():
		kind = "directory"
	case info.Mode()&os.ModeSymlink != 0:
		kind = "symlink"
	}
	return LocalStateEntry{
		Kind: kind, Size: info.Size(), Mode: info.Mode().String(), Permissions: uint32(info.Mode().Perm()),
		ModifiedAt: info.ModTime().UTC(),
	}
}

func captureRegularStateEntry(ctx context.Context, root, relative string, fallback fs.FileInfo, limit int64) (LocalStateEntry, int64) {
	entry := stateEntryFromInfo(fallback)
	handle, err := safepath.OpenRegular(root, relative)
	if err != nil {
		entry.ContentTruncated = true
		entry.ContentReason = "state_content_unsafe_or_unreadable"
		return entry, 0
	}
	defer handle.Close()
	entry = stateEntryFromInfo(handle.Info())
	digest, read, truncated, reason := hashStateFile(ctx, handle, entry.Size, limit)
	entry.SHA256 = digest
	entry.HashedBytes = read
	entry.ContentTruncated = truncated
	entry.ContentReason = reason
	after, statErr := handle.Stat()
	if statErr != nil || after.Size() != handle.Info().Size() || after.Mode() != handle.Info().Mode() || !after.ModTime().Equal(handle.Info().ModTime()) {
		entry.SHA256 = ""
		entry.ContentTruncated = true
		entry.ContentReason = "state_content_changed_during_capture"
	}
	return entry, read
}

func hashStateFile(ctx context.Context, reader io.Reader, size, limit int64) (string, int64, bool, string) {
	if size == 0 {
		return fmt.Sprintf("sha256:%x", sha256.Sum256(nil)), 0, false, ""
	}
	if limit <= 0 {
		return "", 0, size > 0, "state_content_budget_exhausted"
	}
	hash := sha256.New()
	reader = io.LimitReader(reader, limit)
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", total, true, "state_content_capture_cancelled"
		}
		read, readErr := reader.Read(buffer)
		if read > 0 {
			_, _ = hash.Write(buffer[:read])
			total += int64(read)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", total, true, "state_content_unreadable"
		}
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), total, total < size, ""
}

func boundedLimit(value, fallback, maximum int) int {
	if value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func boundedByteLimit(value, fallback, maximum int64) int64 {
	if value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

var _ HostCollector = LocalHostCollector{}
var _ NetworkCollector = LocalNetworkCollector{}
var _ StateCollector = WorkingDirectoryStateCollector{}
