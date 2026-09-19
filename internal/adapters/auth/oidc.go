// Package auth verifies bearer tokens issued by the external OIDC provider
// (Keycloak in the local environment) and derives the caller's identity.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

var (
	ErrMissingToken = errors.New("auth: missing bearer token")
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrForbidden    = errors.New("auth: forbidden")
)

// Role names granted by the IdP as realm roles.
const (
	RoleProvider = "provider"
	RoleInternal = "internal"
)

// Principal is the authenticated caller.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      map[string]struct{}
	ExpiresAt  time.Time
}

// HasRole reports whether the principal carries the realm role.
func (p Principal) HasRole(role string) bool {
	_, ok := p.Roles[role]
	return ok
}

// IsInternal reports whether the caller is the internal wallet service.
func (p Principal) IsInternal() bool { return p.HasRole(RoleInternal) }

// IsProvider reports whether the caller is a game provider with an identity.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// Config for the verifier.
type Config struct {
	Issuer   string
	JWKSURL  string
	Audience string
}

// Verifier validates JWT access tokens against the IdP's JWKS.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
	keySet   *oidc.RemoteKeySet
}

// NewVerifier builds a verifier. The JWKS is fetched lazily and cached; the
// issuer is checked against the configured value, which lets the JWKS be
// reached through an internal host while tokens carry the public issuer.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.JWKSURL == "" || cfg.Audience == "" {
		return nil, errors.New("auth: issuer, jwks url and audience are required")
	}
	keySet := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{ClientID: cfg.Audience, SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256}})
	return &Verifier{verifier: v, keySet: keySet}, nil
}

// Ready fetches the JWKS to prove the IdP is reachable.
func (v *Verifier) Ready(ctx context.Context) error {
	// VerifySignature with garbage input forces a key fetch; the error we
	// care about is a network/HTTP failure, which surfaces as a non-parse error.
	_, err := v.keySet.VerifySignature(ctx, "invalid")
	if err != nil && strings.Contains(err.Error(), "fetching keys") {
		return err
	}
	return nil
}

type claims struct {
	Sub         string `json:"sub"`
	ClientID    string `json:"azp"`
	ProviderID  string `json:"providerId"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify parses and validates a raw JWT, returning the principal.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: claims: %v", ErrInvalidToken, err)
	}
	p := Principal{Subject: tok.Subject, ClientID: c.ClientID, ProviderID: c.ProviderID, Roles: map[string]struct{}{}, ExpiresAt: tok.Expiry}
	for _, r := range c.RealmAccess.Roles {
		p.Roles[r] = struct{}{}
	}
	return p, nil
}

// FromRequest extracts and verifies the Authorization header.
func (v *Verifier) FromRequest(r *http.Request) (Principal, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return Principal{}, ErrMissingToken
	}
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return Principal{}, ErrMissingToken
	}
	return v.Verify(r.Context(), strings.TrimSpace(token))
}

type principalKey struct{}

// WithPrincipal stores the principal in the context.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom reads the principal from the context.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
