package observation

import (
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// Authorization is default-deny and immutable after construction. A literal
// "*" signal family grants every family for the selected leases.
type Authorization struct {
	leases         map[domain.LeaseID]struct{}
	signalFamilies map[string]struct{}
}

func NewAuthorization(leases []domain.LeaseID, signalFamilies []string) Authorization {
	authorization := Authorization{
		leases:         make(map[domain.LeaseID]struct{}, len(leases)),
		signalFamilies: make(map[string]struct{}, len(signalFamilies)),
	}
	for _, lease := range leases {
		if !lease.IsZero() {
			authorization.leases[lease] = struct{}{}
		}
	}
	for _, family := range signalFamilies {
		family = strings.TrimSpace(family)
		if family != "" {
			authorization.signalFamilies[family] = struct{}{}
		}
	}
	return authorization
}

func (authorization Authorization) Allows(lease domain.LeaseID, signalFamily string) bool {
	if _, allowed := authorization.leases[lease]; !allowed {
		return false
	}
	if _, all := authorization.signalFamilies["*"]; all {
		return true
	}
	_, allowed := authorization.signalFamilies[signalFamily]
	return allowed
}

// Project returns a deep, authorization-filtered view over the same ledger
// cursor. It never rewrites cursor identity or create a second source of truth.
func (snapshot LiveSnapshot) Project(authorization Authorization) LiveSnapshot {
	projected := newSnapshot()
	projected.Cursor = snapshot.Cursor
	for id, subject := range snapshot.Subjects {
		if authorization.Allows(subject.LeaseID, subject.SignalFamily) {
			projected.Subjects[id] = cloneSubject(subject)
		}
	}
	// Do not expose orphan topology nodes if their parent family was denied.
	changed := true
	for changed {
		changed = false
		for id, subject := range projected.Subjects {
			if !subject.ParentID.IsZero() {
				if _, parentVisible := projected.Subjects[subject.ParentID]; !parentVisible {
					delete(projected.Subjects, id)
					changed = true
				}
			}
		}
	}
	for key, metric := range snapshot.Metrics {
		if authorization.Allows(metric.LeaseID, metric.SignalFamily) && subjectVisible(projected, metric.SubjectID) {
			metric.State = cloneMetricState(metric.State)
			projected.Metrics[key] = metric
		}
	}
	for key, coverage := range snapshot.Coverage {
		if authorization.Allows(coverage.LeaseID, coverage.SignalFamily) && subjectVisible(projected, coverage.SubjectID) {
			projected.Coverage[key] = cloneCoverage(coverage)
		}
	}
	for id, incident := range snapshot.Incidents {
		if authorization.Allows(incident.LeaseID, incident.SignalFamily) && subjectVisible(projected, incident.SubjectID) {
			projected.Incidents[id] = incident
		}
	}
	for key, pressure := range snapshot.Pressure {
		if authorization.Allows(pressure.LeaseID, pressure.SignalFamily) && subjectVisible(projected, pressure.SubjectID) {
			projected.Pressure[key] = pressure
		}
	}
	return projected
}

func subjectVisible(snapshot LiveSnapshot, subject domain.SubjectID) bool {
	if subject.IsZero() {
		return true
	}
	_, visible := snapshot.Subjects[subject]
	return visible
}
