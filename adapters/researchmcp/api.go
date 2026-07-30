// Package researchmcp is a thin agent-facing facade over WML action evidence.
// It has no capture authority and does not own collectors; every tool reads
// sealed bundles from a research.Store (or returns explicit gaps).
//
// Authorization: every API requires a non-empty AuthorizedLeases set. Reads
// and lists are restricted to those leases. Unfiltered list_actions is rejected.
// get_action redacts argv by default (secrets may appear in command lines).
// Raw stdout/stderr streams are not served here (forensic class on disk only).
package researchmcp

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/world"
)

// Scope is the caller authorization context for the MCP facade.
type Scope struct {
	// AuthorizedLeases is the set of lease IDs the caller may query. Required.
	AuthorizedLeases []string
	// AllowPayloadEscalate permits payload/invasive escalate when a hook is set.
	AllowPayloadEscalate bool
}

// API is the library surface behind MCP tool handlers.
type API struct {
	store                *research.Store
	leases               map[string]struct{}
	allowPayloadEscalate bool
	// Escalate is an optional hook for observation level changes. When nil,
	// EscalateObservation returns a clear unsupported error. When set, the
	// action's lease must still be in AuthorizedLeases.
	Escalate func(ctx context.Context, actionID string, level research.ObservationLevel, profile string) error
}

// Options configures the MCP research API.
type Options struct {
	Store *research.Store
	Scope Scope
	// Escalate is optional; see API.Escalate.
	Escalate func(ctx context.Context, actionID string, level research.ObservationLevel, profile string) error
}

// New constructs an API over an existing research store with a required scope.
func New(store *research.Store, scope Scope) (*API, error) {
	return NewWithOptions(Options{Store: store, Scope: scope})
}

// NewFromManager binds the MCP facade to Manager.ActionEvidence(). The Manager
// must remain open for the lifetime of the returned API.
func NewFromManager(manager *world.Manager, scope Scope) (*API, error) {
	if manager == nil {
		return nil, fmt.Errorf("manager is required")
	}
	store := manager.ActionEvidence()
	if store == nil {
		return nil, fmt.Errorf("manager action evidence store is not composed")
	}
	return New(store, scope)
}

// NewWithOptions constructs an API with optional escalate hook.
func NewWithOptions(options Options) (*API, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("research store is required")
	}
	if len(options.Scope.AuthorizedLeases) == 0 {
		return nil, fmt.Errorf("authorized leases are required")
	}
	leases := make(map[string]struct{}, len(options.Scope.AuthorizedLeases))
	for _, lease := range options.Scope.AuthorizedLeases {
		lease = strings.TrimSpace(lease)
		if lease == "" {
			return nil, fmt.Errorf("authorized leases must not contain blanks")
		}
		leases[lease] = struct{}{}
	}
	return &API{
		store:                options.Store,
		leases:               leases,
		allowPayloadEscalate: options.Scope.AllowPayloadEscalate,
		Escalate:             options.Escalate,
	}, nil
}

// ToolName enumerates the stable MCP tool identifiers.
const (
	ToolGetAction           = "get_action"
	ToolListActions         = "list_actions"
	ToolQueryHost           = "query_host"
	ToolQueryNetwork        = "query_network"
	ToolStateDiff           = "state_diff"
	ToolEscalateObservation = "escalate_observation"
	ToolAssess              = "assess"
	ToolEvidenceGraph       = "evidence_graph"
)

// ToolNames returns the supported tool list in stable order.
func ToolNames() []string {
	return []string{
		ToolGetAction,
		ToolListActions,
		ToolQueryHost,
		ToolQueryNetwork,
		ToolStateDiff,
		ToolEscalateObservation,
		ToolAssess,
		ToolEvidenceGraph,
	}
}

// GetActionResult is the get_action response (argv redacted for agent views).
type GetActionResult struct {
	Action       research.ActionDocument `json:"action"`
	Summary      research.ActionSummary  `json:"summary"`
	ArgvRedacted bool                    `json:"argv_redacted"`
	ArgvCount    int                     `json:"argv_count,omitempty"`
}

// GetAction returns a sealed action bundle with argv redacted.
func (a *API) GetAction(actionID string) (GetActionResult, error) {
	doc, summary, err := a.store.Get(strings.TrimSpace(actionID))
	if err != nil {
		return GetActionResult{}, fmt.Errorf("%s: %w", ToolGetAction, err)
	}
	if err := a.authorizeLease(doc.Start.LeaseID); err != nil {
		return GetActionResult{}, fmt.Errorf("%s: %w", ToolGetAction, err)
	}
	argvCount := len(doc.Start.Argv)
	doc.Start.Argv = nil
	// Keep executable basename only for stimulus context; drop full paths that
	// may embed workspace layout.
	doc.Start.Executable = path.Base(strings.ReplaceAll(doc.Start.Executable, "\\", "/"))
	return GetActionResult{Action: doc, Summary: summary, ArgvRedacted: true, ArgvCount: argvCount}, nil
}

// ListActionsRequest filters list_actions. LeaseID is required.
type ListActionsRequest struct {
	LeaseID     string `json:"lease_id"`
	TargetRunID string `json:"target_run_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

// ListActions returns sealed action summaries for an authorized lease only.
func (a *API) ListActions(request ListActionsRequest) (research.ListResult, error) {
	leaseID := strings.TrimSpace(request.LeaseID)
	if leaseID == "" {
		return research.ListResult{}, fmt.Errorf("%s: lease_id is required", ToolListActions)
	}
	if err := a.authorizeLease(leaseID); err != nil {
		return research.ListResult{}, fmt.Errorf("%s: %w", ToolListActions, err)
	}
	list, err := a.store.List(research.ListOptions{
		LeaseID: leaseID, TargetRunID: request.TargetRunID, Limit: request.Limit,
	})
	if err != nil {
		return research.ListResult{}, fmt.Errorf("%s: %w", ToolListActions, err)
	}
	return list, nil
}

// QueryHostResult is the query_host response.
type QueryHostResult struct {
	Snapshot research.HostSnapshot `json:"snapshot"`
	Gaps     []research.GapRecord  `json:"gaps"`
}

// QueryHost returns host artifacts or explicit gaps.
// Command lines are redacted for agent views (forensic cmdline remains on disk).
func (a *API) QueryHost(actionID string) (QueryHostResult, error) {
	if err := a.authorizeAction(actionID); err != nil {
		return QueryHostResult{}, fmt.Errorf("%s: %w", ToolQueryHost, err)
	}
	snap, gaps, err := a.store.QueryHost(strings.TrimSpace(actionID))
	if err != nil {
		return QueryHostResult{}, fmt.Errorf("%s: %w", ToolQueryHost, err)
	}
	return QueryHostResult{Snapshot: redactHostSnapshot(snap), Gaps: gaps}, nil
}

// QueryNetworkResult is the query_network response.
type QueryNetworkResult struct {
	Index research.NetworkIndex `json:"index"`
	Gaps  []research.GapRecord  `json:"gaps"`
}

// QueryNetwork returns network index artifacts or explicit gaps.
func (a *API) QueryNetwork(actionID string) (QueryNetworkResult, error) {
	if err := a.authorizeAction(actionID); err != nil {
		return QueryNetworkResult{}, fmt.Errorf("%s: %w", ToolQueryNetwork, err)
	}
	index, gaps, err := a.store.QueryNetwork(strings.TrimSpace(actionID))
	if err != nil {
		return QueryNetworkResult{}, fmt.Errorf("%s: %w", ToolQueryNetwork, err)
	}
	return QueryNetworkResult{Index: index, Gaps: gaps}, nil
}

// StateDiffResult is the state_diff response.
type StateDiffResult struct {
	Diff research.StateDiff   `json:"diff"`
	Gaps []research.GapRecord `json:"gaps"`
}

// StateDiff returns the sealed state difference or gaps.
func (a *API) StateDiff(actionID string) (StateDiffResult, error) {
	if err := a.authorizeAction(actionID); err != nil {
		return StateDiffResult{}, fmt.Errorf("%s: %w", ToolStateDiff, err)
	}
	diff, gaps, err := a.store.StateDiff(strings.TrimSpace(actionID))
	if err != nil {
		return StateDiffResult{}, fmt.Errorf("%s: %w", ToolStateDiff, err)
	}
	return StateDiffResult{Diff: diff, Gaps: gaps}, nil
}

// EscalateRequest is the escalate_observation input.
type EscalateRequest struct {
	ActionID string                    `json:"action_id"`
	Level    research.ObservationLevel `json:"level"`
	Profile  string                    `json:"profile,omitempty"`
}

// EscalateObservation requests a higher observation level for future companions.
// MVP: unsupported unless a hook is installed; always requires lease ownership.
func (a *API) EscalateObservation(ctx context.Context, request EscalateRequest) error {
	actionID := strings.TrimSpace(request.ActionID)
	if actionID == "" {
		return fmt.Errorf("%s: action_id is required", ToolEscalateObservation)
	}
	if err := a.authorizeAction(actionID); err != nil {
		return fmt.Errorf("%s: %w", ToolEscalateObservation, err)
	}
	if a.Escalate == nil {
		return fmt.Errorf("%s: observation escalate is not configured on this node (unsupported)", ToolEscalateObservation)
	}
	level := request.Level
	profile := strings.TrimSpace(request.Profile)
	if !level.IsValid() {
		resolved := research.ResolveObservationLevel(true, profile, a.allowPayloadEscalate)
		level = resolved.Level
	}
	if level == research.ObservationLevelPayload && !a.allowPayloadEscalate {
		return fmt.Errorf("%s: payload observation is not allowed for this caller", ToolEscalateObservation)
	}
	if err := a.Escalate(ctx, actionID, level, profile); err != nil {
		return fmt.Errorf("%s: %w", ToolEscalateObservation, err)
	}
	return nil
}

// AssessResult is the assess response.
type AssessResult struct {
	ActionID        string                   `json:"action_id"`
	ConfidenceFloor research.ConfidenceFloor `json:"confidence_floor"`
	EvidenceRoles   research.RoleChecklist   `json:"evidence_roles"`
	Gaps            []research.GapRecord     `json:"gaps"`
	Text            string                   `json:"text"`
	AssessedAt      time.Time                `json:"assessed_at"`
}

// Assess returns the confidence floor derived from a sealed bundle.
func (a *API) Assess(actionID string) (AssessResult, error) {
	if err := a.authorizeAction(actionID); err != nil {
		return AssessResult{}, fmt.Errorf("%s: %w", ToolAssess, err)
	}
	_, summary, err := a.store.Get(strings.TrimSpace(actionID))
	if err != nil {
		return AssessResult{}, fmt.Errorf("%s: %w", ToolAssess, err)
	}
	return AssessResult{
		ActionID: summary.ActionID, ConfidenceFloor: summary.ConfidenceFloor,
		EvidenceRoles: summary.EvidenceRoles, Gaps: summary.Gaps, Text: summary.Text,
		AssessedAt: time.Now().UTC(),
	}, nil
}

// EvidenceGraph returns the structured action join graph with agent-facing
// redaction of cmdline and replay argv (forensic values remain on disk).
func (a *API) EvidenceGraph(actionID string) (research.EvidenceGraph, error) {
	if err := a.authorizeAction(actionID); err != nil {
		return research.EvidenceGraph{}, fmt.Errorf("%s: %w", ToolEvidenceGraph, err)
	}
	graph, err := a.store.EvidenceGraph(strings.TrimSpace(actionID))
	if err != nil {
		return research.EvidenceGraph{}, fmt.Errorf("%s: %w", ToolEvidenceGraph, err)
	}
	return redactEvidenceGraph(graph), nil
}

// redactHostSnapshot clears command lines that may embed secrets.
func redactHostSnapshot(snap research.HostSnapshot) research.HostSnapshot {
	switch tree := snap.ProcessTree.(type) {
	case research.AttributedHostProcess:
		tree.Process.CommandLine = ""
		snap.ProcessTree = tree
	case map[string]any:
		if process, ok := tree["process"].(map[string]any); ok {
			delete(process, "command_line")
			tree["process"] = process
			snap.ProcessTree = tree
		}
	}
	return snap
}

func redactEvidenceGraph(graph research.EvidenceGraph) research.EvidenceGraph {
	graph.Host = redactHostSnapshot(graph.Host)
	if graph.Replay.Available || len(graph.Replay.Argv) > 0 {
		graph.Replay.Argv = nil
		// Keep argv count out-of-band via executable basename only.
		graph.Replay.Executable = path.Base(strings.ReplaceAll(graph.Replay.Executable, "\\", "/"))
	}
	return graph
}

// Handle dispatches a tool name and JSON-like argument map to the library API.
// Arguments are intentionally simple string maps for easy MCP wiring tests.
func (a *API) Handle(ctx context.Context, tool string, args map[string]string) (any, error) {
	if args == nil {
		args = map[string]string{}
	}
	actionID := strings.TrimSpace(args["action_id"])
	switch tool {
	case ToolGetAction:
		return a.GetAction(actionID)
	case ToolListActions:
		limit := 0
		if args["limit"] != "" {
			fmt.Sscanf(args["limit"], "%d", &limit)
		}
		return a.ListActions(ListActionsRequest{LeaseID: args["lease_id"], TargetRunID: args["target_run_id"], Limit: limit})
	case ToolQueryHost:
		return a.QueryHost(actionID)
	case ToolQueryNetwork:
		return a.QueryNetwork(actionID)
	case ToolStateDiff:
		return a.StateDiff(actionID)
	case ToolEscalateObservation:
		return nil, a.EscalateObservation(ctx, EscalateRequest{
			ActionID: actionID,
			Level:    research.ObservationLevel(args["level"]),
			Profile:  args["profile"],
		})
	case ToolAssess:
		return a.Assess(actionID)
	case ToolEvidenceGraph:
		return a.EvidenceGraph(actionID)
	default:
		return nil, fmt.Errorf("unknown research tool %q", tool)
	}
}

func (a *API) authorizeLease(leaseID string) error {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		return fmt.Errorf("action has no lease_id; access denied")
	}
	if _, ok := a.leases[leaseID]; !ok {
		return fmt.Errorf("lease %s is not authorized for this caller", leaseID)
	}
	return nil
}

func (a *API) authorizeAction(actionID string) error {
	doc, _, err := a.store.Get(strings.TrimSpace(actionID))
	if err != nil {
		return err
	}
	return a.authorizeLease(doc.Start.LeaseID)
}
