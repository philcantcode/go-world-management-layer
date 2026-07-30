package world

import "github.com/philcantcode/go-world-management-layer/internal/platform"

// PlatformFeatureStatus is the support level for one host feature.
type PlatformFeatureStatus string

const (
	PlatformFeatureSupported   PlatformFeatureStatus = PlatformFeatureStatus(platform.StatusSupported)
	PlatformFeaturePartial     PlatformFeatureStatus = PlatformFeatureStatus(platform.StatusPartial)
	PlatformFeatureUnsupported PlatformFeatureStatus = PlatformFeatureStatus(platform.StatusUnsupported)
)

// PlatformFeature describes one independently reported host capability.
type PlatformFeature struct {
	ID      string                `json:"id"`
	Status  PlatformFeatureStatus `json:"status"`
	Summary string                `json:"summary"`
	Detail  string                `json:"detail,omitempty"`
}

// PlatformSupport is the structured host support matrix captured at Open.
// Partial and unsupported features are also logged as startup warnings.
type PlatformSupport struct {
	GOOS     string            `json:"goos"`
	GOARCH   string            `json:"goarch"`
	Features []PlatformFeature `json:"features"`
	Warnings []string          `json:"warnings"`
}

// PlatformSupport returns the structured host feature matrix recorded when
// this Manager was opened. It is safe to call after Close; the snapshot is
// immutable. A nil Manager returns a zero value.
func (m *Manager) PlatformSupport() PlatformSupport {
	if m == nil || m.host == nil {
		return PlatformSupport{}
	}
	return platformSupportFrom(m.host.PlatformSupport)
}

func platformSupportFrom(report platform.SupportReport) PlatformSupport {
	features := make([]PlatformFeature, len(report.Features))
	for index, feature := range report.Features {
		features[index] = PlatformFeature{
			ID: feature.ID, Status: PlatformFeatureStatus(feature.Status),
			Summary: feature.Summary, Detail: feature.Detail,
		}
	}
	warnings := append([]string(nil), report.Warnings...)
	return PlatformSupport{
		GOOS: report.GOOS, GOARCH: report.GOARCH,
		Features: features, Warnings: warnings,
	}
}
