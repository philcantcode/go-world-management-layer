package policy

import (
	"reflect"
	"time"
)

const (
	DefaultLeaseTTL               = Duration(2 * time.Hour)
	DefaultQuiesceDeadline        = Duration(30 * time.Second)
	DefaultViewRetention          = Duration(24 * time.Hour)
	DefaultNormalMetricInterval   = Duration(2 * time.Second)
	DefaultIncidentMetricInterval = Duration(250 * time.Millisecond)
	DefaultMetricStaleAfter       = Duration(6 * time.Second)
	DefaultLiveRetention          = Duration(24 * time.Hour)
	DefaultWorkspaceInodes        = int64(100_000)
)

func applyDefaults(policy *Policy, positions map[string]sourcePosition) {
	if !fieldWasSet(positions, "spec.lease.ttl") {
		policy.Spec.Lease.TTL = DefaultLeaseTTL
	}
	if !fieldWasSet(positions, "spec.lease.quiesceDeadline") {
		policy.Spec.Lease.QuiesceDeadline = DefaultQuiesceDeadline
	}
	if !fieldWasSet(positions, "spec.workspace.mode") {
		policy.Spec.Workspace.Mode = "overlayfs"
	}
	if !fieldWasSet(positions, "spec.agentWorkspace.resources.limits.workspaceInodes") {
		policy.Spec.AgentWorkspace.Resources.Limits.WorkspaceInodes = DefaultWorkspaceInodes
	}
	if !fieldWasSet(positions, "spec.workspace.inputView.construction") {
		policy.Spec.Workspace.InputView.Construction = "require-reflink"
	}
	if !fieldWasSet(positions, "spec.workspace.inputView.viewRetention") {
		policy.Spec.Workspace.InputView.ViewRetention = DefaultViewRetention
	}
	if !fieldWasSet(positions, "spec.workspace.cache.highWaterPercent") {
		policy.Spec.Workspace.Cache.HighWaterPercent = 85
	}
	if !fieldWasSet(positions, "spec.workspace.cache.lowWaterPercent") {
		policy.Spec.Workspace.Cache.LowWaterPercent = 70
	}
	if !fieldWasSet(positions, "spec.targets.maxConcurrent") {
		policy.Spec.Targets.MaxConcurrent = 1
	}
	if !fieldWasSet(positions, "spec.observation.priority") {
		policy.Spec.Observation.Priority = "visibility-first"
	}
	if !fieldWasSet(positions, "spec.observation.profiles.default") {
		policy.Spec.Observation.Profiles.Default = "metadata"
	}
	if !fieldWasSet(positions, "spec.observation.metrics.normalInterval") {
		policy.Spec.Observation.Metrics.NormalInterval = DefaultNormalMetricInterval
	}
	if !fieldWasSet(positions, "spec.observation.metrics.incidentInterval") {
		policy.Spec.Observation.Metrics.IncidentInterval = DefaultIncidentMetricInterval
	}
	if !fieldWasSet(positions, "spec.observation.metrics.staleAfter") {
		policy.Spec.Observation.Metrics.StaleAfter = DefaultMetricStaleAfter
	}
	if policy.Spec.Observation.LiveAccess.Agent.Enabled && !fieldWasSet(positions, "spec.observation.liveAccess.agent.minimumMetricInterval") {
		policy.Spec.Observation.LiveAccess.Agent.MinimumMetricInterval = Duration(time.Second)
	}
	if !fieldWasSet(positions, "spec.observation.retention.liveLocal") {
		policy.Spec.Observation.Retention.LiveLocal = DefaultLiveRetention
	}
	normalizeNilCollections(reflect.ValueOf(policy))
}

func fieldWasSet(positions map[string]sourcePosition, path string) bool {
	_, found := positions[path]
	return found
}

// normalizeNilCollections makes omitted and explicitly empty collections have
// the same effective representation and therefore the same digest.
func normalizeNilCollections(value reflect.Value) {
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		normalizeNilCollections(value.Elem())
		return
	}
	switch value.Kind() {
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Field(index).CanSet() {
				normalizeNilCollections(value.Field(index))
			}
		}
	case reflect.Slice:
		if value.IsNil() && value.CanSet() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		for index := 0; index < value.Len(); index++ {
			normalizeNilCollections(value.Index(index))
		}
	case reflect.Map:
		if value.IsNil() && value.CanSet() {
			value.Set(reflect.MakeMap(value.Type()))
		}
		for _, key := range value.MapKeys() {
			element := reflect.New(value.Type().Elem()).Elem()
			element.Set(value.MapIndex(key))
			normalizeNilCollections(element)
			value.SetMapIndex(key, element)
		}
	}
}
