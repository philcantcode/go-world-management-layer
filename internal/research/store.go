package research

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
)

// Store is a durable filesystem-backed action evidence store. Each action
// lives under root/actions/<safe-action-id>/.
//
// Collector timing: class-aware companions are built at Begin from stimulus
// class, observation level, and IntendedCompanions. Startable collectors
// (network capture, syscall) span the action window when tools allow. Host
// process attribution and network finalization run at Seal with ProcessID from
// the process lifecycle. Ambient inventory alone never claims semantic network.
//
// Stream bytes on disk (stdout.bounded/stderr.bounded) may contain secrets and
// are a high-sensitivity forensic class. Agent-facing MCP tools must not serve
// raw streams without additional authorization.
type Store struct {
	root               string
	captureBound       int64
	maxCaptureDuration time.Duration
	injects            CollectorInjects
	collectorOpts      CollectorOptions
	mu                 sync.Mutex
	// open maps action IDs to live sessions so abandon/watchdog can stop startables.
	open map[string]*Session
}

// DefaultMaxCaptureDuration bounds startable collectors (e.g. dumpcap) when
// Seal is never called. Zero MaxCaptureDuration in options selects this default
// when startables are present; a negative duration disables the watchdog.
const DefaultMaxCaptureDuration = 30 * time.Minute

// StoreOptions configures a Store.
type StoreOptions struct {
	Root         string
	CaptureBound int64
	// MaxCaptureDuration bounds how long startable collectors may run without
	// Seal. Zero selects DefaultMaxCaptureDuration when startables are present;
	// negative disables the watchdog (tests that assert long-lived startables).
	MaxCaptureDuration time.Duration
	// Host/Network/State inject fixed collectors (tests). When nil, class-aware
	// registry collectors are selected per Begin from stimulus class/level.
	Host    HostCollector
	Network NetworkCollector
	State   StateCollector
	// Optional companion injects (tests / specialized hosts).
	Syscall         SyscallCollector
	Static          StaticContextCollector
	Oracle          TargetOracleCollector
	Replay          ReplayCollector
	NetworkDecode   NetworkDecodeCollector
	ExtraStartables []StartableCollector
	// CollectorOptions configures capability probes and capture bounds.
	CollectorOptions CollectorOptions
}

// NewStore creates an action evidence store. Nil collectors select class-aware
// registry defaults per action; callers may inject alternate implementations
// when capture must occur in a different host or namespace.
func NewStore(options StoreOptions) (*Store, error) {
	root := strings.TrimSpace(options.Root)
	if root == "" {
		return nil, fmt.Errorf("research store root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve research store root: %w", err)
	}
	absolute = filepath.Clean(absolute)
	// Refuse a symlink root so action paths cannot escape via a swapped link.
	if info, err := os.Lstat(absolute); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("research store root must not be a symlink")
		}
	}
	actionsPath := filepath.Join(absolute, "actions")
	if err := os.MkdirAll(actionsPath, 0o700); err != nil {
		return nil, fmt.Errorf("create actions namespace: %w", err)
	}
	if info, err := os.Lstat(actionsPath); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("actions namespace must be a real directory")
	}
	// Re-check after create: ensure actions parent is not a symlink.
	if info, err := os.Lstat(absolute); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("research store root must not be a symlink")
	}
	bound := options.CaptureBound
	if bound <= 0 {
		bound = DefaultCaptureBound
	}
	return &Store{
		root:               absolute,
		captureBound:       bound,
		maxCaptureDuration: options.MaxCaptureDuration,
		collectorOpts:      options.CollectorOptions,
		injects: CollectorInjects{
			Host:            options.Host,
			Network:         options.Network,
			State:           options.State,
			Syscall:         options.Syscall,
			Static:          options.Static,
			Oracle:          options.Oracle,
			Replay:          options.Replay,
			NetworkDecode:   options.NetworkDecode,
			ExtraStartables: options.ExtraStartables,
		},
		open: make(map[string]*Session),
	}, nil
}

// Root returns the store root directory.
func (s *Store) Root() string { return s.root }

// Begin opens a mutable action session and writes the initial action scaffold.
func (s *Store) Begin(ctx context.Context, start ActionStart) (session *Session, err error) {
	if err := validateActionStart(start); err != nil {
		return nil, err
	}
	dir, err := s.actionDir(start.ActionID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, open := s.open[start.ActionID]; open {
		return nil, fmt.Errorf("action %s is already open", start.ActionID)
	}
	if _, err := os.Stat(filepath.Join(dir, "summary.json")); err == nil {
		return nil, fmt.Errorf("action %s is already sealed", start.ActionID)
	}
	if _, err := os.Stat(filepath.Join(dir, "action.json")); err == nil {
		return nil, fmt.Errorf("action %s already begun (unsealed action.json present)", start.ActionID)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create action directory: %w", err)
	}
	for _, sub := range []string{"network", "host", "state", "static", "target", "replay"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			return nil, fmt.Errorf("create action %s directory: %w", sub, err)
		}
	}

	collectors := BuildCollectors(start.StimulusClass, start.ObservationLevel, start.IntendedCompanions, s.collectorOpts, s.injects)

	session = &Session{
		store:      s,
		start:      start,
		dir:        dir,
		collectors: collectors,
		gaps:       nil,
		coverage: []CoverageRecord{
			{Source: "action_metadata", Role: string(RoleStimulus), Status: "available", Required: true, Detail: "command identity and stimulus class"},
		},
	}

	// Fail-open stop of startables if Begin fails after Start (prevents dumpcap leaks).
	startablesRunning := false
	defer func() {
		if err != nil && startablesRunning {
			stopCtx := context.WithoutCancel(ctx)
			for _, startable := range collectors.Startables {
				_ = startable.Stop(stopCtx, start, ActionOutcome{}, dir)
			}
		}
	}()

	// Start window-spanning collectors (fail-open).
	for _, startable := range collectors.Startables {
		if startErr := startable.Start(ctx, start, dir); startErr != nil {
			session.addGap("unavailable", string(startable.Role()), string(startable.Role()), ReasonCompanionUnconfigured)
		}
	}
	startablesRunning = len(collectors.Startables) > 0

	// Best-effort before-state when state_diff is intended or deep+.
	wantState := collectors.HasRole(CompanionStateDiff) || start.ObservationLevel.AtLeast(ObservationLevelDeep)
	if wantState && collectors.State != nil {
		before, capErr := collectors.State.CaptureBefore(ctx, start)
		if capErr != nil {
			session.addGap("unavailable", "state", string(RoleStateDiff), ReasonStateCaptureFailed)
		} else {
			session.before = before
			if err = writeJSON(filepath.Join(dir, "state", "before.json"), before); err != nil {
				return nil, err
			}
			if !before.Available {
				session.addGap("unavailable", "state", string(RoleStateDiff), reasonOr(before.Reason, ReasonStateUnavailable))
				session.coverage = append(session.coverage, CoverageRecord{Source: "state", Role: string(RoleStateDiff), Status: "gap", Required: false, Detail: ReasonStateUnavailable})
			} else if !before.Attributed {
				session.addGap("unavailable", "state", string(RoleStateDiff), ReasonStateNotAttributed)
				session.coverage = append(session.coverage, CoverageRecord{Source: "state", Role: string(RoleStateDiff), Status: "gap", Required: false, Detail: "ambient collector-host state retained without action filesystem attribution"})
			} else {
				session.coverage = append(session.coverage, CoverageRecord{Source: "state", Role: string(RoleStateDiff), Status: "available", Required: false, Detail: "before snapshot retained"})
			}
		}
	} else {
		// Unsupported at baseline: coverage only — do not record a gap for un-intended companions.
		session.coverage = append(session.coverage, CoverageRecord{Source: "state", Role: string(RoleStateDiff), Status: "unsupported", Required: false, Detail: "baseline level"})
		if err = writeJSON(filepath.Join(dir, "state", "before.json"), StateSnapshot{Available: false, Reason: ReasonStateLevelBaseline}); err != nil {
			return nil, err
		}
	}

	// Host pre-start snapshot.
	if collectors.Host != nil {
		hostSnap, hostErr := collectors.Host.Capture(ctx, start)
		if hostErr != nil {
			session.addGap("unavailable", "host", string(RoleCausal), ReasonHostCaptureFailed)
			session.coverage = append(session.coverage, CoverageRecord{Source: "host", Role: string(RoleCausal), Status: "gap", Required: false, Detail: ReasonHostCaptureFailed})
		} else {
			session.host = hostSnap
			if err = writeJSON(filepath.Join(dir, "host", "process-tree.json"), hostSnap); err != nil {
				return nil, err
			}
			if !hostSnap.Available {
				session.addGap("unavailable", "host", string(RoleCausal), ReasonHostUnavailable)
				session.coverage = append(session.coverage, CoverageRecord{Source: "host", Role: string(RoleCausal), Status: "gap", Required: false, Detail: ReasonHostUnavailable})
			} else {
				detail := "pre-start host snapshot retained"
				if !hostSnap.Attributed {
					detail = "pre-start ambient host snapshot retained"
				}
				session.coverage = append(session.coverage, CoverageRecord{Source: "host", Role: string(RoleCausal), Status: "available", Required: false, Detail: detail})
			}
		}
	}

	// Network begin (ambient and/or start capture already launched).
	// Provisional non-attributed gaps are reconciled at Seal if CaptureAfter attributes.
	if collectors.Network != nil {
		netIndex, netErr := collectors.Network.Capture(ctx, start)
		if netErr != nil {
			session.addGap("unavailable", "network", string(RoleSemantic), ReasonNetworkCaptureFailed)
			session.coverage = append(session.coverage, CoverageRecord{Source: "network", Role: string(RoleSemantic), Status: "gap", Required: false, Detail: ReasonNetworkCaptureFailed})
		} else {
			session.network = netIndex
			if err = writeJSON(filepath.Join(dir, "network", "index.json"), netIndex); err != nil {
				return nil, err
			}
			if !netIndex.Available {
				session.addGap("unavailable", "network", string(RoleSemantic), ReasonNetworkUnavailable)
				session.coverage = append(session.coverage, CoverageRecord{Source: "network", Role: string(RoleSemantic), Status: "gap", Required: false, Detail: ReasonNetworkUnavailable})
			} else if !netIndex.Attributed {
				session.addGap("unavailable", "network", string(RoleSemantic), ReasonNetworkNotAttributed)
				session.coverage = append(session.coverage, CoverageRecord{Source: "network", Role: string(RoleSemantic), Status: "gap", Required: false, Detail: "ambient network inventory retained without action flow attribution"})
			} else {
				session.coverage = append(session.coverage, CoverageRecord{Source: "network", Role: string(RoleSemantic), Status: "available", Required: false, Detail: "action-attributed network index retained"})
			}
		}
	}

	// Static context at Begin (executable is known before launch).
	if collectors.Static != nil {
		staticSnap, staticErr := collectors.Static.Capture(ctx, start, dir)
		if staticErr != nil {
			session.addGap("unavailable", "static_context", string(RoleStatic), ReasonStaticCaptureFailed)
			session.coverage = append(session.coverage, CoverageRecord{Source: "static_context", Role: string(RoleStatic), Status: "gap", Required: false, Detail: ReasonStaticCaptureFailed})
		} else {
			session.static = staticSnap
			if !staticSnap.Available {
				session.addGap("unavailable", "static_context", string(RoleStatic), reasonOr(staticSnap.Reason, ReasonStaticUnavailable))
				session.coverage = append(session.coverage, CoverageRecord{Source: "static_context", Role: string(RoleStatic), Status: "gap", Required: false, Detail: reasonOr(staticSnap.Reason, ReasonStaticUnavailable)})
			} else {
				session.coverage = append(session.coverage, CoverageRecord{Source: "static_context", Role: string(RoleStatic), Status: "available", Required: false, Detail: "static context retained"})
			}
		}
	} else if collectors.HasRole(CompanionStaticContext) {
		session.addGap("unavailable", "static_context", string(RoleStatic), ReasonCompanionUnconfigured)
		session.coverage = append(session.coverage, CoverageRecord{Source: "static_context", Role: string(RoleStatic), Status: "gap", Required: false, Detail: ReasonCompanionUnconfigured})
	}

	// Record intended companions that have no collector attempt at all.
	for _, companion := range start.IntendedCompanions {
		switch companion {
		case CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode, CompanionStateDiff,
			CompanionHostSyscall, CompanionStaticContext, CompanionTargetOracle, CompanionReplay:
			continue
		default:
			session.addGap("unavailable", string(companion), string(companion), ReasonCompanionUnconfigured)
			session.coverage = append(session.coverage, CoverageRecord{Source: string(companion), Role: string(companion), Status: "gap", Required: false, Detail: ReasonCompanionUnconfigured})
		}
	}

	if err = writeJSON(filepath.Join(dir, "action.json"), ActionDocument{
		SchemaVersion: ActionSchemaVersion,
		Start:         start,
		Outcome:       ActionOutcome{Sealed: false},
		Gaps:          append([]GapRecord(nil), session.gaps...),
		Coverage:      append([]CoverageRecord(nil), session.coverage...),
	}); err != nil {
		return nil, err
	}
	s.open[start.ActionID] = session
	startablesRunning = false // ownership transferred to session
	session.armCaptureWatchdog()
	return session, nil
}

// RecordBeginFailure writes a durable instrumentation-failure marker when a
// Begin could not be started. It never blocks the caller.
func (s *Store) RecordBeginFailure(actionID, reasonCode string) {
	if s == nil {
		return
	}
	safe, err := sanitizeActionID(actionID)
	if err != nil {
		return
	}
	if reasonCode == "" {
		reasonCode = ReasonBeginConflict
	}
	if err := os.MkdirAll(filepath.Join(s.root, "actions"), 0o700); err != nil {
		return
	}
	path := filepath.Join(s.root, "actions", safe+".begin-failed.json")
	_ = writeJSON(path, map[string]any{
		"action_id": actionID,
		"reason":    reasonCode,
		"at":        time.Now().UTC(),
	})
}

// Get returns the sealed action document and summary for an action ID.
func (s *Store) Get(actionID string) (ActionDocument, ActionSummary, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return ActionDocument{}, ActionSummary{}, err
	}
	var doc ActionDocument
	if err := readJSON(filepath.Join(dir, "action.json"), &doc); err != nil {
		return ActionDocument{}, ActionSummary{}, err
	}
	var summary ActionSummary
	if err := readJSON(filepath.Join(dir, "summary.json"), &summary); err != nil {
		return ActionDocument{}, ActionSummary{}, err
	}
	return doc, summary, nil
}

// ListOptions filters and bounds List.
type ListOptions struct {
	LeaseID     string
	TargetRunID string
	// Limit caps returned actions; 0 selects DefaultListLimit.
	Limit int
}

// ListResult is a bounded list of sealed action summaries.
type ListResult struct {
	Actions   []ActionSummary `json:"actions"`
	Skipped   int             `json:"skipped,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
}

// List returns sealed action summaries. Filters use summary.json fields only
// (no double-load of action.json). Results are hard-capped.
func (s *Store) List(opts ListOptions) (ListResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "actions"))
	if err != nil {
		if os.IsNotExist(err) {
			return ListResult{}, nil
		}
		return ListResult{}, err
	}
	// Sort names for deterministic pagination-friendly order.
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	result := ListResult{Actions: make([]ActionSummary, 0)}
	for _, name := range names {
		summaryPath := filepath.Join(s.root, "actions", name, "summary.json")
		var summary ActionSummary
		if err := readJSON(summaryPath, &summary); err != nil {
			result.Skipped++
			continue
		}
		if opts.LeaseID != "" && summary.LeaseID != opts.LeaseID {
			continue
		}
		if opts.TargetRunID != "" && summary.TargetRunID != opts.TargetRunID {
			continue
		}
		if len(result.Actions) >= limit {
			result.Truncated = true
			break
		}
		result.Actions = append(result.Actions, summary)
	}
	return result, nil
}

// QueryHost returns host artifacts for an action, or gaps.
func (s *Store) QueryHost(actionID string) (HostSnapshot, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return HostSnapshot{}, nil, err
	}
	var snap HostSnapshot
	if err := readJSON(filepath.Join(dir, "host", "process-tree.json"), &snap); err != nil {
		return HostSnapshot{Available: false, Reason: ReasonHostUnavailable}, []GapRecord{{Kind: "unavailable", Source: "host", Role: string(RoleCausal), Reason: ReasonHostUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return snap, nil, err
	}
	return snap, filterGaps(doc.Gaps, "host"), nil
}

// QueryNetwork returns network index artifacts for an action, or gaps.
func (s *Store) QueryNetwork(actionID string) (NetworkIndex, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return NetworkIndex{}, nil, err
	}
	var index NetworkIndex
	if err := readJSON(filepath.Join(dir, "network", "index.json"), &index); err != nil {
		return NetworkIndex{Available: false, Reason: ReasonNetworkUnavailable}, []GapRecord{{Kind: "unavailable", Source: "network", Role: string(RoleSemantic), Reason: ReasonNetworkUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return index, nil, err
	}
	return index, filterGaps(doc.Gaps, "network"), nil
}

// QuerySyscall returns syscall artifacts for an action, or gaps.
func (s *Store) QuerySyscall(actionID string) (SyscallSnapshot, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return SyscallSnapshot{}, nil, err
	}
	var snap SyscallSnapshot
	if err := readJSON(filepath.Join(dir, "host", "syscalls.json"), &snap); err != nil {
		return SyscallSnapshot{Available: false, Reason: ReasonSyscallUnavailable}, []GapRecord{{Kind: "unavailable", Source: "host_syscall", Role: string(RoleCausal), Reason: ReasonSyscallUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return snap, nil, err
	}
	return snap, filterGaps(doc.Gaps, "host_syscall"), nil
}

// QueryStatic returns static context for an action, or gaps.
func (s *Store) QueryStatic(actionID string) (StaticContextSnapshot, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return StaticContextSnapshot{}, nil, err
	}
	var snap StaticContextSnapshot
	if err := readJSON(filepath.Join(dir, "static", "context.json"), &snap); err != nil {
		return StaticContextSnapshot{Available: false, Reason: ReasonStaticUnavailable}, []GapRecord{{Kind: "unavailable", Source: "static_context", Role: string(RoleStatic), Reason: ReasonStaticUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return snap, nil, err
	}
	return snap, filterGaps(doc.Gaps, "static_context"), nil
}

// QueryOracle returns target oracle evidence for an action, or gaps.
func (s *Store) QueryOracle(actionID string) (TargetOracleSnapshot, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return TargetOracleSnapshot{}, nil, err
	}
	var snap TargetOracleSnapshot
	if err := readJSON(filepath.Join(dir, "target", "oracle.json"), &snap); err != nil {
		return TargetOracleSnapshot{Available: false, Reason: ReasonOracleUnavailable}, []GapRecord{{Kind: "unavailable", Source: "target_oracle", Role: string(RoleTargetOracle), Reason: ReasonOracleUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return snap, nil, err
	}
	return snap, filterGaps(doc.Gaps, "target_oracle"), nil
}

// QueryReplay returns the replay package for an action, or gaps.
func (s *Store) QueryReplay(actionID string) (ReplayPackage, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return ReplayPackage{}, nil, err
	}
	var pkg ReplayPackage
	if err := readJSON(filepath.Join(dir, "replay", "package.json"), &pkg); err != nil {
		return ReplayPackage{Available: false, Reason: ReasonReplayUnavailable}, []GapRecord{{Kind: "unavailable", Source: "replay", Role: string(RoleReplay), Reason: ReasonReplayUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return pkg, nil, err
	}
	return pkg, filterGaps(doc.Gaps, "replay"), nil
}

// StateDiff returns the sealed state diff for an action.
func (s *Store) StateDiff(actionID string) (StateDiff, []GapRecord, error) {
	dir, err := s.actionDir(actionID)
	if err != nil {
		return StateDiff{}, nil, err
	}
	var diff StateDiff
	path := filepath.Join(dir, "state", "diff.json")
	if err := readJSON(path, &diff); err != nil {
		return StateDiff{Available: false, Reason: ReasonStateUnavailable}, []GapRecord{{Kind: "unavailable", Source: "state", Role: string(RoleStateDiff), Reason: ReasonStateUnavailable}}, nil
	}
	doc, _, err := s.Get(actionID)
	if err != nil {
		return diff, nil, err
	}
	return diff, filterGaps(doc.Gaps, "state"), nil
}

// EvidenceGraph returns a structured join of action → processes → flows.
func (s *Store) EvidenceGraph(actionID string) (EvidenceGraph, error) {
	doc, summary, err := s.Get(actionID)
	if err != nil {
		return EvidenceGraph{}, err
	}
	host, _, hostErr := s.QueryHost(actionID)
	network, _, networkErr := s.QueryNetwork(actionID)
	diff, _, stateErr := s.StateDiff(actionID)
	if err := errors.Join(hostErr, networkErr, stateErr); err != nil {
		return EvidenceGraph{}, fmt.Errorf("load action evidence sidecars: %w", err)
	}
	// Best-effort optional companions.
	syscallSnap, _, _ := s.QuerySyscall(actionID)
	staticSnap, _, _ := s.QueryStatic(actionID)
	oracleSnap, _, _ := s.QueryOracle(actionID)
	replayPkg, _, _ := s.QueryReplay(actionID)
	return EvidenceGraph{
		ActionID:          actionID,
		StimulusClass:     doc.Start.StimulusClass,
		ExecID:            doc.Start.ExecID,
		TargetRunID:       doc.Start.TargetRunID,
		TargetOperationID: doc.Start.TargetOperationID,
		LeaseID:           doc.Start.LeaseID,
		ProcessID:         doc.Outcome.ProcessID,
		Host:              host,
		Network:           network,
		StateDiff:         diff,
		Syscall:           syscallSnap,
		Static:            staticSnap,
		Oracle:            oracleSnap,
		Replay:            replayPkg,
		ConfidenceFloor:   summary.ConfidenceFloor,
		EvidenceRoles:     summary.EvidenceRoles,
		Gaps:              doc.Gaps,
	}, nil
}

// EvidenceGraph is the structured join for agent/MCP consumers.
type EvidenceGraph struct {
	ActionID          string                `json:"action_id"`
	StimulusClass     StimulusClass         `json:"stimulus_class"`
	ExecID            string                `json:"exec_id,omitempty"`
	TargetRunID       string                `json:"target_run_id,omitempty"`
	TargetOperationID string                `json:"target_operation_id,omitempty"`
	LeaseID           string                `json:"lease_id,omitempty"`
	ProcessID         int64                 `json:"process_id,omitempty"`
	Host              HostSnapshot          `json:"host"`
	Network           NetworkIndex          `json:"network"`
	StateDiff         StateDiff             `json:"state_diff"`
	Syscall           SyscallSnapshot       `json:"syscall,omitempty"`
	Static            StaticContextSnapshot `json:"static,omitempty"`
	Oracle            TargetOracleSnapshot  `json:"oracle,omitempty"`
	Replay            ReplayPackage         `json:"replay,omitempty"`
	ConfidenceFloor   ConfidenceFloor       `json:"confidence_floor"`
	EvidenceRoles     RoleChecklist         `json:"evidence_roles"`
	Gaps              []GapRecord           `json:"gaps"`
}

func (s *Store) actionDir(actionID string) (string, error) {
	safe, err := sanitizeActionID(actionID)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(s.root, "actions", safe)
	// Ensure cleaned path stays under root/actions.
	actionsRoot := filepath.Join(s.root, "actions")
	rel, err := filepath.Rel(actionsRoot, dir)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("action path escapes store root")
	}
	return dir, nil
}

var actionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,200}$`)

func sanitizeActionID(actionID string) (string, error) {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return "", fmt.Errorf("action_id is required")
	}
	if strings.Contains(actionID, "/") || strings.Contains(actionID, "\\") || strings.Contains(actionID, "..") {
		return "", fmt.Errorf("action_id contains path separators")
	}
	if strings.IndexByte(actionID, 0) >= 0 {
		return "", fmt.Errorf("action_id contains NUL")
	}
	if !actionIDPattern.MatchString(actionID) {
		return "", fmt.Errorf("action_id has an invalid charset")
	}
	// filepath.IsLocal rejects absolute and parent-relative forms on Go 1.20+.
	if !filepath.IsLocal(actionID) {
		return "", fmt.Errorf("action_id is not a local path segment")
	}
	return actionID, nil
}

func validateActionStart(start ActionStart) error {
	if _, err := sanitizeActionID(start.ActionID); err != nil {
		return err
	}
	if !start.Scope.IsValid() {
		return fmt.Errorf("action scope is invalid")
	}
	if strings.TrimSpace(start.Executable) == "" {
		return fmt.Errorf("executable is required")
	}
	if !start.StimulusClass.IsValid() {
		return fmt.Errorf("stimulus class is invalid")
	}
	if !start.ObservationLevel.IsValid() {
		return fmt.Errorf("observation level is invalid")
	}
	if start.StartedAt.IsZero() {
		return fmt.Errorf("started_at is required")
	}
	return nil
}

func filterGaps(gaps []GapRecord, source string) []GapRecord {
	result := make([]GapRecord, 0)
	for _, gap := range gaps {
		if gap.Source == source {
			result = append(result, gap)
		}
	}
	return result
}

func reasonOr(reason, fallback string) string {
	if strings.TrimSpace(reason) == "" {
		return fallback
	}
	// Prefer stable codes when the reason already is one; otherwise use fallback
	// to avoid leaking host paths from collector implementations.
	if strings.ContainsAny(reason, `/\`) || strings.Contains(reason, ":") {
		return fallback
	}
	return reason
}

func writeJSON(path string, value any) error {
	return atomicfile.WriteJSON(path, value, 0o600)
}

func readJSON(path string, dest any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(content, dest)
}

// Session is a mutable in-flight action evidence capture.
type Session struct {
	store             *Store
	start             ActionStart
	dir               string
	collectors        ActionCollectors
	stdout            boundedBuffer
	stderr            boundedBuffer
	gaps              []GapRecord
	coverage          []CoverageRecord
	before            StateSnapshot
	host              HostSnapshot
	network           NetworkIndex
	static            StaticContextSnapshot
	syscall           SyscallSnapshot
	oracle            TargetOracleSnapshot
	replay            ReplayPackage
	decode            NetworkDecodeResult
	sealed            bool
	startablesStopped bool
	watchdog          *time.Timer
	startableMu       sync.Mutex // separate from mu so Seal can stop while holding mu
	mu                sync.Mutex
}

// Start returns the session start identity.
func (s *Session) Start() ActionStart { return s.start }

// ActionID returns the action identity.
func (s *Session) ActionID() string { return s.start.ActionID }

// CaptureBound returns the per-stream capture bound.
func (s *Session) CaptureBound() int64 { return s.store.captureBound }

// AppendStdout retains a bounded prefix of stdout for the sealed bundle.
func (s *Session) AppendStdout(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stdout.append(data, s.store.captureBound)
}

// AppendStderr retains a bounded prefix of stderr for the sealed bundle.
func (s *Session) AppendStderr(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stderr.append(data, s.store.captureBound)
}

// Seal finalizes the action bundle with terminal outcome metadata.
// On any failure after Begin, the action_id is released from the open set and
// an abandon marker is written so a later Begin can recover.
func (s *Session) Seal(ctx context.Context, outcome ActionOutcome) (summary ActionSummary, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return ActionSummary{}, fmt.Errorf("action %s is already sealed", s.start.ActionID)
	}
	// Always release process-local open entry; on failure also mark abandoned.
	defer func() {
		if s.sealed {
			s.releaseOpen()
			return
		}
		if err != nil {
			err = errors.Join(err, s.markAbandoned())
			s.releaseOpen()
		}
	}()

	if outcome.EndedAt.IsZero() {
		outcome.EndedAt = time.Now().UTC()
	}
	outcome.StdoutBytes = s.stdout.total
	outcome.StderrBytes = s.stderr.total
	outcome.StdoutTruncated = s.stdout.truncated
	outcome.StderrTruncated = s.stderr.truncated
	outcome.CaptureBound = s.store.captureBound
	outcome.Sealed = true

	// Always stop startables on every Seal path (including stream write failures).
	defer s.forceStopStartables()

	if err := atomicfile.Write(filepath.Join(s.dir, "stdout.bounded"), s.stdout.bytes, 0o600); err != nil {
		return ActionSummary{}, fmt.Errorf("write stdout: %w", err)
	}
	if err := atomicfile.Write(filepath.Join(s.dir, "stderr.bounded"), s.stderr.bytes, 0o600); err != nil {
		return ActionSummary{}, fmt.Errorf("write stderr: %w", err)
	}

	captureRefs := make([]string, 0, 8)

	// Host CaptureAfter with PID attribution. Keep pre-start ambient snapshot
	// when ProcessID is missing so Begin evidence is not clobbered.
	if afterHost, ok := s.collectors.Host.(AfterHostCollector); ok && outcome.ProcessID > 0 {
		hostSnap, hostErr := afterHost.CaptureAfter(ctx, s.start, outcome, s.dir)
		if hostErr != nil {
			s.addGapLocked("unavailable", "host", string(RoleCausal), ReasonHostCaptureFailed)
		} else if hostSnap.Available {
			s.host = hostSnap
			if err := writeJSON(filepath.Join(s.dir, "host", "process-tree.json"), hostSnap); err != nil {
				return ActionSummary{}, fmt.Errorf("write host process-tree: %w", err)
			}
			if hostSnap.Attributed {
				captureRefs = append(captureRefs, "host/process-tree.json")
				s.coverage = append(s.coverage, CoverageRecord{Source: "host", Role: string(RoleCausal), Status: "available", Required: false, Detail: "pid-attributed host snapshot retained"})
			}
		} else {
			s.addGapLocked("unavailable", "host", string(RoleCausal), reasonOr(hostSnap.Reason, ReasonHostUnavailable))
		}
	}

	// Network CaptureAfter (pcap stop + conn table).
	if afterNet, ok := s.collectors.Network.(AfterNetworkCollector); ok {
		netIndex, netErr := afterNet.CaptureAfter(ctx, s.start, outcome, s.dir)
		if netErr != nil {
			s.addGapLocked("unavailable", "network", string(RoleSemantic), ReasonNetworkCaptureFailed)
		} else {
			s.network = netIndex
			if err := writeJSON(filepath.Join(s.dir, "network", "index.json"), netIndex); err != nil {
				return ActionSummary{}, fmt.Errorf("write network index: %w", err)
			}
			// Refresh network gaps for final attribution state.
			if !netIndex.Available {
				s.addGapLocked("unavailable", "network", string(RoleSemantic), reasonOr(netIndex.Reason, ReasonNetworkUnavailable))
			} else if !netIndex.Attributed {
				// Keep a single non-attributed reason (window pcap or ambient).
				reason := ReasonNetworkNotAttributed
				if netIndex.Reason == ReasonNetworkWindowUnjoined {
					reason = ReasonNetworkWindowUnjoined
				}
				s.reconcileNetworkGaps(false)
				s.addGapLocked("unavailable", "network", string(RoleSemantic), reason)
			} else {
				// Drop provisional Begin-time network_not_action_attributed gaps.
				s.reconcileNetworkGaps(true)
				captureRefs = append(captureRefs, "network/index.json")
				if netIndex.ArtifactPath != "" {
					captureRefs = append(captureRefs, netIndex.ArtifactPath)
				}
				s.coverage = append(s.coverage, CoverageRecord{Source: "network", Role: string(RoleSemantic), Status: "available", Required: false, Detail: "action-attributed network index retained"})
			}
		}
	}

	// Network decode.
	if s.collectors.NetworkDecode != nil {
		decoded, decErr := s.collectors.NetworkDecode.Decode(ctx, s.start, s.network, s.dir)
		if decErr != nil {
			s.addGapLocked("unavailable", "network_decode", string(RoleSemantic), ReasonNetworkDecodeFailed)
		} else {
			s.decode = decoded
			if !decoded.Available {
				s.addGapLocked("unavailable", "network_decode", string(RoleSemantic), reasonOr(decoded.Reason, ReasonNetworkDecodeUnavailable))
				s.coverage = append(s.coverage, CoverageRecord{Source: "network_decode", Role: string(RoleSemantic), Status: "gap", Required: false, Detail: reasonOr(decoded.Reason, ReasonNetworkDecodeUnavailable)})
			} else if !decoded.Attributed {
				// Decode records may exist for window pcap; without attribution do not
				// claim semantic coverage (mirrors network index policy).
				s.addGapLocked("unavailable", "network_decode", string(RoleSemantic), reasonOr(decoded.Reason, ReasonNetworkWindowUnjoined))
				s.coverage = append(s.coverage, CoverageRecord{Source: "network_decode", Role: string(RoleSemantic), Status: "gap", Required: false, Detail: reasonOr(decoded.Reason, ReasonNetworkWindowUnjoined)})
				if decoded.ArtifactPath != "" {
					captureRefs = append(captureRefs, decoded.ArtifactPath)
				}
			} else {
				if decoded.ArtifactPath != "" {
					captureRefs = append(captureRefs, decoded.ArtifactPath)
				}
				s.coverage = append(s.coverage, CoverageRecord{Source: "network_decode", Role: string(RoleSemantic), Status: "available", Required: false, Detail: "semantic network decode retained"})
			}
		}
	} else if s.collectors.HasRole(CompanionNetworkDecode) {
		s.addGapLocked("unavailable", "network_decode", string(RoleSemantic), ReasonCompanionUnconfigured)
	}

	// After-state + diff when intended.
	wantState := s.collectors.HasRole(CompanionStateDiff) || s.start.ObservationLevel.AtLeast(ObservationLevelDeep)
	if wantState && s.collectors.State != nil {
		after, err := s.collectors.State.CaptureAfter(ctx, s.start)
		if err != nil {
			s.addGapLocked("unavailable", "state", string(RoleStateDiff), ReasonStateCaptureFailed)
			placeholder := StateSnapshot{Available: false, Reason: ReasonStateCaptureFailed}
			if err := writeJSON(filepath.Join(s.dir, "state", "after.json"), placeholder); err != nil {
				return ActionSummary{}, fmt.Errorf("write state after: %w", err)
			}
			if err := writeJSON(filepath.Join(s.dir, "state", "diff.json"), StateDiff{Available: false, Reason: ReasonStateCaptureFailed}); err != nil {
				return ActionSummary{}, fmt.Errorf("write state diff: %w", err)
			}
		} else {
			if err := writeJSON(filepath.Join(s.dir, "state", "after.json"), after); err != nil {
				return ActionSummary{}, fmt.Errorf("write state after: %w", err)
			}
			diff := s.collectors.State.Diff(s.before, after)
			if !after.Available && diff.Reason == "" {
				diff = StateDiff{Available: false, Reason: reasonOr(after.Reason, ReasonStateUnavailable)}
			}
			if err := writeJSON(filepath.Join(s.dir, "state", "diff.json"), diff); err != nil {
				return ActionSummary{}, fmt.Errorf("write state diff: %w", err)
			}
			if !diff.Available {
				s.addGapLocked("unavailable", "state", string(RoleStateDiff), reasonOr(diff.Reason, ReasonStateUnavailable))
			} else if !diff.Attributed {
				s.addGapLocked("unavailable", "state", string(RoleStateDiff), ReasonStateNotAttributed)
			} else {
				captureRefs = append(captureRefs, "state/diff.json")
			}
		}
	} else {
		if err := writeJSON(filepath.Join(s.dir, "state", "after.json"), StateSnapshot{Available: false, Reason: ReasonStateLevelBaseline}); err != nil {
			return ActionSummary{}, fmt.Errorf("write state after: %w", err)
		}
		if err := writeJSON(filepath.Join(s.dir, "state", "diff.json"), StateDiff{Available: false, Reason: ReasonStateLevelBaseline}); err != nil {
			return ActionSummary{}, fmt.Errorf("write state diff: %w", err)
		}
	}

	// Syscall finalize.
	if s.collectors.Syscall != nil {
		sysSnap, sysErr := s.collectors.Syscall.CaptureAfter(ctx, s.start, outcome, s.dir)
		if sysErr != nil {
			s.addGapLocked("unavailable", "host_syscall", string(RoleCausal), ReasonSyscallCaptureFailed)
		} else {
			s.syscall = sysSnap
			if !sysSnap.Available {
				s.addGapLocked("unavailable", "host_syscall", string(RoleCausal), reasonOr(sysSnap.Reason, ReasonSyscallUnavailable))
				s.coverage = append(s.coverage, CoverageRecord{Source: "host_syscall", Role: string(RoleCausal), Status: "gap", Required: false, Detail: reasonOr(sysSnap.Reason, ReasonSyscallUnavailable)})
			} else {
				if sysSnap.ArtifactPath != "" {
					captureRefs = append(captureRefs, sysSnap.ArtifactPath)
				}
				s.coverage = append(s.coverage, CoverageRecord{Source: "host_syscall", Role: string(RoleCausal), Status: "available", Required: false, Detail: "syscall boundary retained"})
			}
		}
	} else if s.collectors.HasRole(CompanionHostSyscall) {
		s.addGapLocked("unavailable", "host_syscall", string(RoleCausal), ReasonCompanionUnconfigured)
	}

	// Target oracle.
	if s.collectors.Oracle != nil {
		oracleSnap, oracleErr := s.collectors.Oracle.Capture(ctx, s.start, s.dir)
		if oracleErr != nil {
			s.addGapLocked("unavailable", "target_oracle", string(RoleTargetOracle), ReasonOracleCaptureFailed)
		} else {
			s.oracle = oracleSnap
			if !oracleSnap.Available {
				s.addGapLocked("unavailable", "target_oracle", string(RoleTargetOracle), reasonOr(oracleSnap.Reason, ReasonOracleUnavailable))
				s.coverage = append(s.coverage, CoverageRecord{Source: "target_oracle", Role: string(RoleTargetOracle), Status: "gap", Required: false, Detail: reasonOr(oracleSnap.Reason, ReasonOracleUnavailable)})
			} else {
				if oracleSnap.ArtifactPath != "" {
					captureRefs = append(captureRefs, oracleSnap.ArtifactPath)
				}
				s.coverage = append(s.coverage, CoverageRecord{Source: "target_oracle", Role: string(RoleTargetOracle), Status: "available", Required: false, Detail: "target oracle retained"})
			}
		}
	} else if s.collectors.HasRole(CompanionTargetOracle) {
		s.addGapLocked("unavailable", "target_oracle", string(RoleTargetOracle), ReasonCompanionUnconfigured)
	}

	// Replay package.
	if s.collectors.Replay != nil {
		pkg, replayErr := s.collectors.Replay.Capture(ctx, s.start, outcome, s.dir, captureRefs)
		if replayErr != nil {
			s.addGapLocked("unavailable", "replay", string(RoleReplay), ReasonReplayCaptureFailed)
		} else {
			s.replay = pkg
			if !pkg.Available {
				s.addGapLocked("unavailable", "replay", string(RoleReplay), reasonOr(pkg.Reason, ReasonReplayUnavailable))
				s.coverage = append(s.coverage, CoverageRecord{Source: "replay", Role: string(RoleReplay), Status: "gap", Required: false, Detail: reasonOr(pkg.Reason, ReasonReplayUnavailable)})
			} else {
				captureRefs = append(captureRefs, "replay/package.json")
				s.coverage = append(s.coverage, CoverageRecord{Source: "replay", Role: string(RoleReplay), Status: "available", Required: false, Detail: "replay package retained"})
			}
		}
	} else if s.collectors.HasRole(CompanionReplay) {
		s.addGapLocked("unavailable", "replay", string(RoleReplay), ReasonCompanionUnconfigured)
	}

	// Raw role is present when we have exit/stream metadata (always after seal).
	s.coverage = append(s.coverage, CoverageRecord{
		Source: "streams", Role: string(RoleRaw), Status: "available", Required: true,
		Detail: fmt.Sprintf("stdout=%d stderr=%d bytes retained (capture_bound=%d)", len(s.stdout.bytes), len(s.stderr.bytes), s.store.captureBound),
	})

	// Causal confidence: process lifecycle PID or attributed host snapshot.
	hasCausal := outcome.ProcessID > 0 || (s.host.Available && s.host.Attributed)
	if outcome.ProcessID > 0 {
		s.coverage = append(s.coverage, CoverageRecord{Source: "process_lifecycle", Role: string(RoleCausal), Status: "available", Required: false, Detail: fmt.Sprintf("pid=%d", outcome.ProcessID)})
	} else if !hasCausal {
		s.addGapLocked("unavailable", "process_lifecycle", string(RoleCausal), ReasonProcessLifecycleMissing)
	}

	// Semantic: attributed network or successful decode.
	hasSemantic := (s.network.Available && s.network.Attributed) || (s.decode.Available && s.decode.Attributed)
	hasStateDiff := false
	var sealedDiff StateDiff
	if err := readJSON(filepath.Join(s.dir, "state", "diff.json"), &sealedDiff); err == nil && sealedDiff.Available && sealedDiff.Attributed {
		hasStateDiff = true
	}
	hasStatic := s.static.Available && s.static.Attributed
	hasOracle := s.oracle.Available && s.oracle.Attributed
	hasReplay := s.replay.Available

	roles := BuildRoleChecklistForAction(
		s.start.IntendedCompanions,
		true, // stimulus
		true, // raw after seal
		hasSemantic,
		hasCausal,
		hasStatic,
		hasOracle,
		hasStateDiff,
		hasReplay,
	)

	floor := DeriveConfidenceFloor(roles)
	durationMS := outcome.EndedAt.Sub(s.start.StartedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	summary = ActionSummary{
		SchemaVersion:      SummarySchemaVersion,
		ActionID:           s.start.ActionID,
		LeaseID:            s.start.LeaseID,
		TargetRunID:        s.start.TargetRunID,
		StimulusClass:      s.start.StimulusClass,
		ObservationLevel:   s.start.ObservationLevel,
		ExitCode:           outcome.ExitCode,
		Signal:             outcome.Signal,
		Error:              boundText(outcome.Error, 1024),
		ConfidenceFloor:    floor,
		EvidenceRoles:      roles,
		IntendedCompanions: append([]CompanionRole(nil), s.start.IntendedCompanions...),
		Gaps:               append([]GapRecord(nil), s.gaps...),
		Text:               summarizeText(s.start, outcome, floor, roles),
		DurationMS:         durationMS,
	}

	doc := ActionDocument{
		SchemaVersion: ActionSchemaVersion,
		Start:         s.start,
		Outcome:       outcome,
		Gaps:          append([]GapRecord(nil), s.gaps...),
		Coverage:      append([]CoverageRecord(nil), s.coverage...),
	}
	if err := writeJSON(filepath.Join(s.dir, "action.json"), doc); err != nil {
		return ActionSummary{}, err
	}
	if err := writeJSON(filepath.Join(s.dir, "summary.json"), summary); err != nil {
		return ActionSummary{}, err
	}
	s.sealed = true
	return summary, nil
}

func (s *Session) releaseOpen() {
	s.store.mu.Lock()
	delete(s.store.open, s.start.ActionID)
	s.store.mu.Unlock()
}

func (s *Session) markAbandoned() error {
	// Ensure startables (dumpcap) stop even when Seal fails mid-flight.
	s.forceStopStartables()
	payload := map[string]any{
		"action_id": s.start.ActionID,
		"reason":    ReasonSealAbandoned,
		"at":        time.Now().UTC(),
	}
	if err := writeJSON(filepath.Join(s.dir, "abandon.json"), payload); err != nil {
		return fmt.Errorf("write action abandon marker: %w", err)
	}
	return nil
}

// forceStopStartables stops window-spanning collectors and cancels the watchdog.
// Idempotent and safe under concurrent Seal/watchdog/Abandon (uses startableMu,
// not Session.mu, so Seal can call it while holding the session lock).
func (s *Session) forceStopStartables() {
	s.startableMu.Lock()
	if s.startablesStopped {
		s.startableMu.Unlock()
		return
	}
	s.startablesStopped = true
	if s.watchdog != nil {
		s.watchdog.Stop()
		s.watchdog = nil
	}
	startables := s.collectors.Startables
	start := s.start
	dir := s.dir
	s.startableMu.Unlock()
	for _, startable := range startables {
		_ = startable.Stop(context.Background(), start, ActionOutcome{}, dir)
	}
}

// armCaptureWatchdog stops startables if Seal never arrives within the bound.
func (s *Session) armCaptureWatchdog() {
	if len(s.collectors.Startables) == 0 {
		return
	}
	dur := s.store.maxCaptureDuration
	if dur < 0 {
		return // explicitly disabled
	}
	if dur == 0 {
		dur = DefaultMaxCaptureDuration
	}
	s.startableMu.Lock()
	defer s.startableMu.Unlock()
	if s.watchdog != nil || s.startablesStopped {
		return
	}
	s.watchdog = time.AfterFunc(dur, func() {
		s.forceStopStartables()
	})
}

// Abandon stops startables for an open session and writes an abandon marker.
// Used when a caller knows the exec will never Seal (fail-open GC path).
func (s *Store) Abandon(actionID string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	session := s.open[actionID]
	s.mu.Unlock()
	if session == nil {
		return fmt.Errorf("action %s is not open", actionID)
	}
	session.forceStopStartables()
	if err := session.markAbandoned(); err != nil {
		return err
	}
	session.releaseOpen()
	return nil
}

func (s *Session) addGap(kind, source, role, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addGapLocked(kind, source, role, reason)
}

func (s *Session) addGapLocked(kind, source, role, reason string) {
	s.gaps = append(s.gaps, GapRecord{Kind: kind, Source: source, Role: role, Reason: reason})
}

// reconcileNetworkGaps drops provisional Begin-time network attribution gaps
// when the final CaptureAfter result is process-attributed. When attributed is
// false, it still collapses duplicate provisional reasons so Seal can re-add one.
func (s *Session) reconcileNetworkGaps(attributed bool) {
	filtered := make([]GapRecord, 0, len(s.gaps))
	for _, gap := range s.gaps {
		if gap.Source == "network" && (gap.Reason == ReasonNetworkNotAttributed || gap.Reason == ReasonNetworkWindowUnjoined) {
			if attributed {
				continue
			}
			// De-dupe: drop so caller can re-add a single final reason.
			continue
		}
		filtered = append(filtered, gap)
	}
	s.gaps = filtered
}

type boundedBuffer struct {
	bytes     []byte
	total     int64
	truncated bool
}

func (b *boundedBuffer) append(data []byte, bound int64) {
	b.total += int64(len(data))
	if int64(len(b.bytes)) >= bound {
		if len(data) > 0 {
			b.truncated = true
		}
		return
	}
	remaining := bound - int64(len(b.bytes))
	if int64(len(data)) > remaining {
		b.bytes = append(b.bytes, data[:remaining]...)
		b.truncated = true
		return
	}
	b.bytes = append(b.bytes, data...)
}

func summarizeText(start ActionStart, outcome ActionOutcome, floor ConfidenceFloor, roles RoleChecklist) string {
	class := string(start.StimulusClass)
	exit := "unknown"
	if outcome.ExitCode != nil {
		exit = fmt.Sprintf("%d", *outcome.ExitCode)
	}
	present := 0
	for _, status := range roles {
		if status == RolePresent {
			present++
		}
	}
	return boundText(fmt.Sprintf(
		"Action %s classified as %s completed with exit %s at confidence floor %s (%d/%d evidence roles present).",
		start.ActionID, class, exit, floor, present, len(roles),
	), 512)
}

func boundText(value string, max int) string {
	value = strings.TrimSpace(strings.ToValidUTF8(value, "\uFFFD"))
	if len(value) <= max {
		return value
	}
	value = value[:max]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
