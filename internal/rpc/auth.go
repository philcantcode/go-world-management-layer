package rpc

import (
	"context"
	"crypto/subtle"
	"crypto/x509"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Identity is transport authentication evidence made available to policy
// authorization. It deliberately contains no mutable host identity object.
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

// Authenticator abstracts local bearer and mTLS identity resolution so policy
// code does not depend on gRPC metadata or TLS implementation details.
type Authenticator interface {
	Authenticate(context.Context) (Identity, error)
}

type AuthenticatorFunc func(context.Context) (Identity, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context) (Identity, error) { return f(ctx) }

// BearerOrMTLSAuthenticator accepts a configured local bearer token or a
// verified client certificate. Tokens map to stable policy subject names.
type BearerOrMTLSAuthenticator struct {
	BearerSubjects map[string]string
	AllowMTLS      bool
}

func (a BearerOrMTLSAuthenticator) Authenticate(ctx context.Context) (Identity, error) {
	authorizations := metadataValues(ctx, "authorization")
	if len(authorizations) > 1 {
		return Identity{}, status.Error(codes.Unauthenticated, "exactly one authorization credential is allowed")
	}
	if len(authorizations) == 1 {
		authorization := authorizations[0]
		const prefix = "Bearer "
		if !strings.HasPrefix(authorization, prefix) {
			return Identity{}, status.Error(codes.Unauthenticated, "authorization must use Bearer scheme")
		}
		provided := strings.TrimPrefix(authorization, prefix)
		for token, subject := range a.BearerSubjects {
			if token == "" {
				continue
			}
			if len(token) == len(provided) && subtle.ConstantTimeCompare([]byte(token), []byte(provided)) == 1 {
				if strings.TrimSpace(subject) == "" {
					return Identity{}, status.Error(codes.Unauthenticated, "bearer identity is not configured")
				}
				return Identity{Subject: subject, Method: "local_bearer"}, nil
			}
		}
		return Identity{}, status.Error(codes.Unauthenticated, "invalid bearer credential")
	}
	if a.AllowMTLS {
		if identity, ok := verifiedTLSIdentity(ctx); ok {
			return identity, nil
		}
	}
	return Identity{}, status.Error(codes.Unauthenticated, "authenticated local bearer or mTLS identity is required")
}

func metadataValues(ctx context.Context, key string) []string {
	values, _ := metadata.FromIncomingContext(ctx)
	return values.Get(key)
}

func verifiedTLSIdentity(ctx context.Context) (Identity, bool) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return Identity{}, false
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return Identity{}, false
	}
	certificate := tlsInfo.State.VerifiedChains[0][0]
	subject := certificate.Subject.CommonName
	if len(certificate.URIs) > 0 {
		subject = certificate.URIs[0].String()
	}
	if strings.TrimSpace(subject) == "" {
		return Identity{}, false
	}
	return Identity{Subject: subject, Method: "mtls", Cert: certificate}, true
}

func unaryAuthInterceptor(authenticator Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		identity, err := authenticate(ctx, authenticator)
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, identityKey{}, identity), request)
	}
}

func streamAuthInterceptor(authenticator Authenticator) grpc.StreamServerInterceptor {
	return func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		identity, err := authenticate(stream.Context(), authenticator)
		if err != nil {
			return err
		}
		return handler(service, &authenticatedServerStream{ServerStream: stream, ctx: context.WithValue(stream.Context(), identityKey{}, identity)})
	}
}

func authenticate(ctx context.Context, authenticator Authenticator) (Identity, error) {
	if authenticator == nil {
		return Identity{}, status.Error(codes.Unauthenticated, "server authenticator is not configured")
	}
	identity, err := authenticator.Authenticate(ctx)
	if err != nil {
		if status.Code(err) == codes.Unknown {
			return Identity{}, status.Error(codes.Unauthenticated, err.Error())
		}
		return Identity{}, err
	}
	if strings.TrimSpace(identity.Subject) == "" || strings.TrimSpace(identity.Method) == "" {
		return Identity{}, status.Error(codes.Unauthenticated, fmt.Sprintf("authenticator returned incomplete identity for %q", identity.Subject))
	}
	return identity, nil
}

type authenticatedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedServerStream) Context() context.Context { return s.ctx }
