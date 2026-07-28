package daemon

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/policy"
)

// compiledPolicyResolver exposes only the exact in-memory pairs compiled for
// startup preflight. It grants no durable authorization; the daemon swaps to
// the registry-backed authority only after every physical plan passes.
type compiledPolicyResolver struct {
	byDigest map[string]*policy.EffectivePolicy
}

func newCompiledPolicyResolver(publications map[string]*policy.EffectivePolicy) (*compiledPolicyResolver, error) {
	if len(publications) == 0 {
		return nil, fmt.Errorf("compiled policies are required")
	}
	resolver := &compiledPolicyResolver{byDigest: make(map[string]*policy.EffectivePolicy, len(publications))}
	for reference, effective := range publications {
		if effective == nil {
			return nil, fmt.Errorf("compiled policy %q is nil", reference)
		}
		key := compiledPolicyDigestKey(effective.Digest().String(), effective.CapabilityFingerprintDigest().String())
		if _, duplicate := resolver.byDigest[key]; duplicate {
			return nil, fmt.Errorf("compiled policy pair %q is duplicated", reference)
		}
		resolver.byDigest[key] = effective
	}
	return resolver, nil
}

func (r *compiledPolicyResolver) Resolve(ctx context.Context, policyDigest, capabilityDigest string) (*policy.EffectivePolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := domain.ParseDigest(policyDigest); err != nil {
		return nil, fmt.Errorf("invalid compiled policy digest: %w", err)
	}
	if _, err := domain.ParseDigest(capabilityDigest); err != nil {
		return nil, fmt.Errorf("invalid compiled capability digest: %w", err)
	}
	effective := r.byDigest[compiledPolicyDigestKey(policyDigest, capabilityDigest)]
	if effective == nil {
		return nil, fmt.Errorf("compiled effective-policy pair is unknown")
	}
	return effective, nil
}

func compiledPolicyDigestKey(policyDigest, capabilityDigest string) string {
	return policyDigest + "\x00" + capabilityDigest
}
