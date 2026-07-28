//go:build !linux

package docker

// Docker Desktop projects host ACLs into its Linux VM rather than exposing
// portable host uid/gid ownership. The tree and mount boundary are still
// validated by prepareWorkspaceAccess; Docker applies the guest view.
func applyWorkspaceOwnership(_ []workspaceAccessEntry, _, _ int) error { return nil }
