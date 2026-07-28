package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

func acquire(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := parseAcquire(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.AcquireResearchSession(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseAcquire(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.AcquireResearchSessionRequest, error) {
	flags := worldcli.NewFlagSet("acquire", stderr)
	inputView := flags.String("input-view", "", "resolved iv_ input view ID")
	frozenSelection := flags.String("frozen-selection", "", "optional frozen selection reference")
	occurrences := flags.String("occurrences", "", "comma-separated immutable occurrence references")
	mappings := flags.String("path-mappings", "", "comma-separated occurrence=workspace/path mappings")
	sidecars := flags.String("sidecars", "", "comma-separated allowed sidecars")
	cacheScope := flags.String("cache-scope", "", "cache security scope")
	requireZeroCopy := flags.Bool("require-zero-copy", false, "require a zero-copy input view")
	mutation := worldcli.AddMutationFlags(flags, defaultEnv("WORLD_POLICY_REFERENCE"))
	policy := &mutation.Policy
	capabilities := flags.String("capabilities", "", "sha256 capability digest")
	ttl := flags.Duration("ttl", time.Hour, "lease TTL")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("policy", *policy, "capabilities", *capabilities); err != nil {
		return nil, err
	}
	selectionRequested := strings.TrimSpace(*frozenSelection) != "" || strings.TrimSpace(*occurrences) != "" || strings.TrimSpace(*mappings) != "" || strings.TrimSpace(*sidecars) != "" || strings.TrimSpace(*cacheScope) != "" || *requireZeroCopy
	if strings.TrimSpace(*inputView) == "" && !selectionRequested {
		return nil, worldcli.UsageError("input-view or an immutable input selection is required")
	}
	if strings.TrimSpace(*inputView) != "" && selectionRequested {
		return nil, worldcli.UsageError("input-view and unresolved selection flags are mutually exclusive")
	}
	if *ttl <= 0 {
		return nil, worldcli.UsageError("ttl must be positive")
	}
	pathMappings, err := parsePathMappings(*mappings)
	if err != nil {
		return nil, err
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	wireTTL, err := worldcli.Duration(*ttl)
	if err != nil {
		return nil, err
	}
	return &worldv1.AcquireResearchSessionRequest{
		Mutation: meta,
		InputView: &worldv1.InputViewSpec{
			ResolvedInputViewId:     *inputView,
			FrozenSelectionRef:      *frozenSelection,
			ImmutableOccurrenceRefs: worldcli.CSV(*occurrences),
			PathMappings:            pathMappings,
			AllowedSidecars:         worldcli.CSV(*sidecars),
			CacheSecurityScope:      *cacheScope,
			RequireZeroCopy:         *requireZeroCopy,
		},
		PolicyDigest:     *policy,
		CapabilityDigest: *capabilities,
		Ttl:              wireTTL,
	}, nil
}

func parsePathMappings(value string) ([]*worldv1.PathMapping, error) {
	items := worldcli.CSV(value)
	result := make([]*worldv1.PathMapping, 0, len(items))
	for _, item := range items {
		occurrence, path, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(occurrence) == "" {
			return nil, fmt.Errorf("path mapping %q must be occurrence=workspace/path", item)
		}
		path, err := worldcli.WorkspacePath(path)
		if err != nil {
			return nil, fmt.Errorf("path mapping %q: %w", item, err)
		}
		result = append(result, &worldv1.PathMapping{OccurrenceRef: strings.TrimSpace(occurrence), WorkspaceRelativePath: path})
	}
	return result, nil
}

func getSession(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("get-session", stderr)
	id := flags.String("session", defaultEnv("WORLD_SESSION_ID"), "research session ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("session", *id); err != nil {
		return err
	}
	result, err := client.GetResearchSession(ctx, &worldv1.GetResearchSessionRequest{ResearchSessionId: *id})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func waitSession(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	request, err := parseWaitSession(arguments, stderr)
	if err != nil {
		return err
	}
	result, err := client.WaitResearchSession(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseWaitSession(arguments []string, stderr io.Writer) (*worldv1.WaitResearchSessionRequest, error) {
	flags := worldcli.NewFlagSet("wait-session", stderr)
	id := flags.String("session", defaultEnv("WORLD_SESSION_ID"), "research session ID")
	desired := flags.String("state", "ready", "desired session state")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("session", *id, "state", *desired); err != nil {
		return nil, err
	}
	return &worldv1.WaitResearchSessionRequest{ResearchSessionId: *id, DesiredState: *desired}, nil
}

func renewLease(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := parseRenewLease(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.RenewLease(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseRenewLease(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.RenewLeaseRequest, error) {
	flags := worldcli.NewFlagSet("renew", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	revision := flags.Uint64("revision", 0, "expected lease revision")
	ttl := flags.Duration("ttl", time.Hour, "new lease TTL")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("lease", *lease); err != nil {
		return nil, err
	}
	if *ttl <= 0 {
		return nil, worldcli.UsageError("ttl must be positive")
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	wireTTL, err := worldcli.Duration(*ttl)
	if err != nil {
		return nil, err
	}
	return &worldv1.RenewLeaseRequest{Mutation: meta, LeaseId: *lease, ExpectedRevision: *revision, Ttl: wireTTL}, nil
}

func releaseSession(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("release", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	revision := flags.Uint64("revision", 0, "expected lease revision")
	reason := flags.String("reason", "requested by operator", "release reason")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("lease", *lease, "reason", *reason); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.ReleaseResearchSession(ctx, &worldv1.ReleaseResearchSessionRequest{Mutation: meta, LeaseId: *lease, ExpectedRevision: *revision, Reason: *reason})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}
