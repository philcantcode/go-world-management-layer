package worldcli

import (
	"io"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
)

// ParseDeclareExport builds the append-only export intent shared by the host
// CLI and the scoped guest helper.
func ParseDeclareExport(arguments []string, stderr io.Writer, timeout time.Duration, defaultLease, defaultPolicy string) (*worldv1.DeclareExportRequest, error) {
	flags := NewFlagSet("declare-export", stderr)
	lease := flags.String("lease", defaultLease, "lease ID")
	defaultRole := flags.String("role", "result", "default role for paths without =ROLE")
	mutation := AddMutationFlags(flags, defaultPolicy)
	var values StringValues
	flags.Var(&values, "path", "repeatable workspace-relative PATH[=ROLE]")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	values = append(values, flags.Args()...)
	if err := Require("lease", *lease); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, UsageError("at least one export path is required")
	}
	paths := make([]*worldv1.ExportPath, 0, len(values))
	for _, value := range values {
		path, err := ExportPath(value, *defaultRole)
		if err != nil {
			return nil, err
		}
		paths = append(paths, &path)
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.DeclareExportRequest{Mutation: meta, LeaseId: *lease, Paths: paths}, nil
}
