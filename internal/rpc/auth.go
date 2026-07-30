package rpc

import (
	"context"
	"crypto/x509"
	"strings"
)

// Identity is policy authentication evidence made available to authorization.
// For the library-only product it is installed by world.Open via
// ContextWithIdentity; there is no bearer-token or mTLS transport authenticator.
type Identity struct {
	Subject string
	Method  string
	Cert    *x509.Certificate
}

type identityKey struct{}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}

// ContextWithIdentity installs a policy identity for in-process Manager calls
// that reuse WorldServer mapping without gRPC interceptors.
func ContextWithIdentity(ctx context.Context, identity Identity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, identityKey{}, identity)
}

// NormalizeSubject trims a policy subject name for local embed configuration.
func NormalizeSubject(subject string) string {
	return strings.TrimSpace(subject)
}
