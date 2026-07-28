package worldcli

import (
	"io"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
)

// ParseRequestCapture builds the intentionally append-only guest request. It
// accepts either -profile NAME or one positional NAME, never collector args.
func ParseRequestCapture(arguments []string, stderr io.Writer, timeout time.Duration, defaultLease, defaultPolicy string) (*worldv1.RequestCaptureRequest, error) {
	flags := NewFlagSet("request", stderr)
	lease := flags.String("lease", defaultLease, "lease ID")
	profile := flags.String("profile", "", "pre-authorized named profile")
	mutation := AddMutationFlags(flags, defaultPolicy)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	remaining := flags.Args()
	if *profile == "" && len(remaining) == 1 {
		*profile = remaining[0]
		remaining = nil
	}
	if len(remaining) != 0 {
		return nil, UsageError("request accepts exactly one named profile")
	}
	if err := Require("lease", *lease, "profile", *profile); err != nil {
		return nil, err
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.RequestCaptureRequest{Mutation: meta, LeaseId: *lease, NamedProfile: *profile}, nil
}
