package auth

// Google OAuth 2.0 / OIDC ("Continue with Google") and the web half of the
// web-to-desktop handshake.
//
// Flow (RFC 6749 authorization-code, server-side confidential client):
//
//      browser → GET /v1/auth/oauth/google        state + tx cookie set (Redis, TTL)
//      browser → Google consent → callback        state consumed, tx verified,
//                                                   code exchanged, ID token verified
//           API → 302 WebSuccessURL?grant=wgrant_  single-use web grant (Redis, TTL)
//      web JS → POST /v1/auth/web/session {grant}  → short web access token
//                                                          (NO refresh token — the
//                                                           browser is not a vault)
//
// Security properties:
//   - state: 32-byte CSPRNG, single-use (atomic GETDEL), TTL-bounded, and
//     bound to a same-site tx cookie set on the begin hop (login-CSRF defense:
//     a state stolen into another browser fails the cookie match).
//   - ID tokens are verified against Google's JWKS (RS256 only, iss+aud+exp);
//     access tokens from the token endpoint are never parsed (we only trust
//     the ID token delivered over our direct TLS connection to Google).
//   - emails that Google has not verified never create accounts.
//   - no token of any kind ever appears in a URL, a referrer, or a log line;
//     only single-use short-TTL opaque codes (grant/mcode) cross the browser.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/store/repos"
)

// OAuthStore provisions OAuth identities (implemented by repos.OAuthUsersRepo).
type OAuthStore interface {
	// FindOrCreateGoogleUser resolves the identity per the documented cases
	// (login / provision) with race arbitration.
	FindOrCreateGoogleUser(ctx context.Context, sub, email, displayName, avatarURL string) (*domain.User, error)
}

// DevicesStore registers desktop devices (implemented by repos.DevicesRepo).
type DevicesStore interface {
	UpsertDevice(ctx context.Context, userID, deviceID, name, platform string) (string, error)
}

// GoogleIdentity is the verified OIDC identity.
type GoogleIdentity struct {
	Sub           string
	Email         string
	EmailVerified bool
	Name          string
	Picture       string
}

// GoogleProvider is the OAuth client. Zero value is unusable; NewGoogleProvider
// validates. Endpoint URLs are injectable for tests and air-gapped installs.
type GoogleProvider struct {
	clientID     string
	clientSecret string
	redirectURI  string
	authURL      string
	tokenURL     string
	jwksURL      string
	hc           httpClient
	keys         *googleKeys
}

// NewGoogleProvider builds the provider (hc nil → http.DefaultClient).
func NewGoogleProvider(clientID, clientSecret, redirectURI, authURL, tokenURL, jwksURL string, hc httpClient) *GoogleProvider {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &GoogleProvider{
		clientID: clientID, clientSecret: clientSecret, redirectURI: redirectURI,
		authURL: authURL, tokenURL: tokenURL, jwksURL: jwksURL,
		hc: hc, keys: &googleKeys{url: jwksURL, hc: hc},
	}
}

// AuthCodeURL renders the consent-screen redirect (top-level navigation).
// PKCE (RFC 7636, S256) and an OIDC nonce are always sent (audit M1): the
// authorization code alone is then not a bearer credential — redeeming it
// also requires the server-held code_verifier, and the returned ID token
// must carry the nonce bound to this login attempt (replay defense).
func (g *GoogleProvider) AuthCodeURL(state, codeChallenge, nonce string) string {
	q := url.Values{}
	q.Set("client_id", g.clientID)
	q.Set("redirect_uri", g.redirectURI)
	q.Set("response_type", "code")
	q.Set("scope", "openid email profile")
	q.Set("state", state)
	q.Set("code_challenge", codeChallenge)
	q.Set("code_challenge_method", "S256")
	q.Set("nonce", nonce)
	q.Set("access_type", "online")
	q.Set("prompt", "select_account")
	return g.authURL + "?" + q.Encode()
}

// S256CodeChallenge derives the PKCE code challenge (base64url, no padding).
func S256CodeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Exchange trades the authorization code at Google's token endpoint and
// verifies the returned ID token. Only the ID token is trusted (it is
// delivered over our own TLS connection; the access_token is ignored).
func (g *GoogleProvider) Exchange(ctx context.Context, code, codeVerifier, nonce string) (*GoogleIdentity, error) {
	form := url.Values{}
	form.Set("code", code)
	form.Set("client_id", g.clientID)
	form.Set("client_secret", g.clientSecret)
	form.Set("redirect_uri", g.redirectURI)
	form.Set("grant_type", "authorization_code")
	if codeVerifier != "" {
		// PKCE proof-of-possession (audit M1): without the verifier that only
		// this server holds, a stolen authorization code is unusable.
		form.Set("code_verifier", codeVerifier)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, domain.ErrInternal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := g.hc.Do(req)
	if err != nil {
		return nil, domain.ErrOAuthProvider(err)
	}
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)); _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, domain.ErrOAuthProvider(fmt.Errorf("token endpoint: status %d", resp.StatusCode))
	}
	var tok struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tok); err != nil {
		return nil, domain.ErrOAuthProvider(fmt.Errorf("token endpoint decode: %w", err))
	}
	if tok.IDToken == "" {
		return nil, domain.ErrOAuthProvider(errors.New("token endpoint returned no id_token"))
	}
	return g.verifyIDToken(ctx, tok.IDToken, nonce)
}

// googleIssuers are both accepted forms (Google documents both).
var googleIssuers = map[string]bool{
	"accounts.google.com":         true,
	"https://accounts.google.com": true,
}

// verifyIDToken checks RS256 signature against Google's JWKS and validates
// iss / aud / exp; then extracts the identity claims.
func (g *GoogleProvider) verifyIDToken(ctx context.Context, raw, nonce string) (*GoogleIdentity, error) {
	var body struct {
		jwt.RegisteredClaims
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		Nonce         string `json:"nonce"`
	}
	parsed, err := jwt.ParseWithClaims(raw, &body, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected alg %v", t.Header["alg"])
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("id token has no kid")
		}
		return g.keys.key(ctx, kid)
	}, jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second))
	if err != nil {
		return nil, domain.ErrOAuthProvider(fmt.Errorf("id token verify: %w", err))
	}
	if !parsed.Valid {
		return nil, domain.ErrOAuthProvider(errors.New("id token invalid"))
	}
	if !googleIssuers[body.Issuer] {
		return nil, domain.ErrOAuthProvider(fmt.Errorf("id token issuer %q", body.Issuer))
	}
	if len(body.Audience) == 0 || body.Audience[0] != g.clientID {
		return nil, domain.ErrOAuthProvider(errors.New("id token audience mismatch"))
	}
	if body.Subject == "" || body.Email == "" {
		return nil, domain.ErrOAuthProvider(errors.New("id token missing sub/email"))
	}
	// OIDC nonce (audit M1): when the flow sent one, the ID token must carry
	// the SAME value — an ID token minted for a different login attempt (a
	// replay from elsewhere) fails here even with a valid signature.
	if nonce != "" && body.Nonce != nonce {
		return nil, domain.ErrOAuthProvider(errors.New("id token nonce mismatch"))
	}
	return &GoogleIdentity{
		Sub: body.Subject, Email: strings.ToLower(strings.TrimSpace(body.Email)),
		EmailVerified: body.EmailVerified, Name: body.Name, Picture: body.Picture,
	}, nil
}

// googleKeys is a minimal JWKS cache for Google's RSA keys (mirror of
// JWKSVerifier's key resolution, RS256-only, min-refresh throttled).
type googleKeys struct {
	url      string
	hc       httpClient
	mu       sync.RWMutex
	keys     map[string]any
	lastTry  time.Time
	fetching bool
}

const (
	googleKeysFastWindow = 2 * time.Second // unknown-kid fast-refetch floor (audit L14)
)

// key resolves one signing key. Audit L14 fixed two frictions: the HTTP
// fetch no longer holds the write lock (a slow JWKS endpoint used to
// serialize every login behind one network call), and an UNKNOWN kid
// triggers a bounded immediate refetch instead of waiting out the old
// one-minute throttle — right after the provider rotates keys, logins
// presenting the new kid used to be refused for up to a minute.
// A concurrent lookup that lands while a refresh is ALREADY in flight
// JOINS it (single-flight) instead of being throttled out — without this,
// simultaneous first logins raced: the loser read the empty cache, saw the
// winner's just-set lastTry, and was refused for the full 2s floor.
func (k *googleKeys) key(ctx context.Context, kid string) (any, error) {
	k.mu.RLock()
	key, ok := k.keys[kid]
	lastTry := k.lastTry
	inFlight := k.fetching
	k.mu.RUnlock()
	if ok {
		return key, nil
	}
	if inFlight {
		_ = k.waitForInFlight()
		k.mu.RLock()
		defer k.mu.RUnlock()
		if key, ok := k.keys[kid]; ok {
			return key, nil
		}
		return nil, fmt.Errorf("unknown kid %q (after in-flight refresh)", kid)
	}
	if time.Since(lastTry) < googleKeysFastWindow {
		return nil, fmt.Errorf("unknown kid %q (refresh throttled)", kid)
	}
	if err := k.refresh(ctx); err != nil {
		return nil, err
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	if key, ok := k.keys[kid]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("unknown kid %q", kid)
}

// waitForInFlight blocks (bounded, ~1s) until a concurrently running
// refresh finishes. Shared by key() joiners and refresh()'s own late
// arrivals.
func (k *googleKeys) waitForInFlight() bool {
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		k.mu.RLock()
		done := !k.fetching
		k.mu.RUnlock()
		if done {
			return true
		}
	}
	return false
}

// refresh performs one JWKS fetch and swaps the key set atomically. Only
// one refresh runs at a time (single-flight via the fetching flag); the
// lock is NEVER held across the HTTP call. Late arrivals join via
// waitForInFlight.
func (k *googleKeys) refresh(ctx context.Context) error {
	k.mu.Lock()
	if k.fetching {
		k.mu.Unlock()
		k.waitForInFlight()
		return nil
	}
	k.fetching = true
	k.lastTry = time.Now()
	k.mu.Unlock()
	defer func() { k.mu.Lock(); k.fetching = false; k.mu.Unlock() }()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.url, nil)
	if err != nil {
		return err
	}
	resp, err := k.hc.Do(req)
	if err != nil {
		return fmt.Errorf("jwks fetch: %w", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)); _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks fetch: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&set); err != nil {
		return fmt.Errorf("jwks decode: %w", err)
	}
	next := make(map[string]any, len(set.Keys))
	for _, j := range set.Keys {
		pub, err := j.publicKey()
		if err != nil {
			continue
		}
		if j.Kid != "" && j.Kty == "RSA" {
			next[j.Kid] = pub
		}
	}
	k.mu.Lock()
	k.keys = next
	k.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Service methods: state, web grant, lookup
// ---------------------------------------------------------------------------

// oauthTx is the Redis-stored state record (bound to the tx cookie). It also
// carries the PKCE verifier and OIDC nonce for the login it started (M1).
type oauthTx struct {
	Tx        string `json:"tx"`
	Verifier  string `json:"verifier"`
	Nonce     string `json:"nonce"`
	CreatedAt int64  `json:"created_at"`
}

// webGrant is the Redis-stored grant payload.
type webGrant struct {
	UserID   string `json:"user_id"`
	TenantID string `json:"tenant_id"`
}

// WebSession is the browser-side response: a short access token and the
// identity context. Deliberately carries NO refresh token.
type WebSession struct {
	AccessToken string     `json:"access_token"`
	TokenType   string     `json:"token_type"`
	ExpiresIn   int64      `json:"expires_in"`
	User        UserInfo   `json:"user"`
	Tenant      TenantInfo `json:"tenant,omitempty"`
}

// BeginGoogleOAuth mints the anti-CSRF state, binds it to a fresh tx secret
// (the handler sets the matching cookie) and returns the consent URL.
func (s *Service) BeginGoogleOAuth(ctx context.Context) (redirectURL, state, tx string, err error) {
	if s.Google == nil || s.OAuth == nil {
		return "", "", "", domain.ErrOAuthDisabled()
	}
	state = randomHex(32)
	tx = randomHex(32)
	// PKCE verifier (server-held) + OIDC nonce (audit M1).
	verifier := randomHex(32) // 64 hex chars — valid RFC 7636 verifier charset
	nonce := randomHex(16)
	rec, _ := json.Marshal(oauthTx{Tx: tx, Verifier: verifier, Nonce: nonce, CreatedAt: time.Now().Unix()})
	if err := s.Redis.Set(ctx, "auth:oauth:state:"+state, rec, s.OAuthStateTTL).Err(); err != nil {
		return "", "", "", domain.ErrDependencyUnavailable("redis")
	}
	return s.Google.AuthCodeURL(state, S256CodeChallenge(verifier), nonce), state, tx, nil
}

// FinishGoogleOAuth consumes the state (single-use), verifies the tx cookie,
// exchanges the code, resolves the user and mints the web grant.
func (s *Service) FinishGoogleOAuth(ctx context.Context, code, state, txCookie string) (*domain.User, string, error) {
	if s.Google == nil || s.OAuth == nil {
		return nil, "", domain.ErrOAuthDisabled()
	}
	if state == "" || len(state) > 128 || code == "" || len(code) > 2048 {
		return nil, "", domain.ErrOAuthState()
	}
	raw, ok := consumeOneTime(ctx, s.Redis, "auth:oauth:state:"+state)
	if !ok {
		return nil, "", domain.ErrOAuthState()
	}
	var st oauthTx
	// Constant-time comparison (audit L12): the tx secret is a 256-bit
	// bearer value; a plain == leaks how many leading bytes matched.
	if err := json.Unmarshal([]byte(raw), &st); err != nil || st.Tx == "" ||
		subtle.ConstantTimeCompare([]byte(st.Tx), []byte(txCookie)) != 1 {
		// Expired/unknown/tampered state OR a tx cookie from a different flow
		// (login-CSRF attempt) — one generic error, no oracle.
		return nil, "", domain.ErrOAuthState()
	}

	ident, err := s.Google.Exchange(ctx, code, st.Verifier, st.Nonce)
	if err != nil {
		return nil, "", err
	}
	if !ident.EmailVerified {
		return nil, "", domain.ErrOAuthEmailUnverified()
	}
	if !validEmail(ident.Email) {
		return nil, "", domain.ErrOAuthProvider(errors.New("provider returned an invalid email"))
	}

	u, err := s.resolveGoogleUser(ctx, ident)
	if err != nil {
		return nil, "", err
	}

	// Resolve the tenant for the grant (first active membership).
	tenantID, _, err := s.resolveTenant(ctx, u.ID)
	if err != nil {
		return nil, "", err
	}

	grant := "wgrant_" + randomHex(32)
	rec, _ := json.Marshal(webGrant{UserID: u.ID, TenantID: tenantID})
	if err := s.Redis.Set(ctx, "auth:web:grant:"+grant, rec, s.WebGrantTTL).Err(); err != nil {
		return nil, "", domain.ErrDependencyUnavailable("redis")
	}
	return u, grant, nil
}

// resolveGoogleUser applies the documented cases with race retries
// (login / provision). Profile fields are truncated on RUNE boundaries
// (audit M4): a byte-slice at the column limit could split a multi-byte
// character, and PostgreSQL refuses to store the invalid UTF-8 — first
// login then answered 500 for users with long CJK/emoji names.
func (s *Service) resolveGoogleUser(ctx context.Context, ident *GoogleIdentity) (*domain.User, error) {
	name := truncateUTF8(ident.Name, 128)
	avatar := truncateUTF8(ident.Picture, 2048)
	for attempt := 0; attempt < 3; attempt++ {
		u, err := s.OAuth.FindOrCreateGoogleUser(ctx, ident.Sub, ident.Email, name, avatar)
		if err == nil {
			return u, nil
		}
		if errors.Is(err, repos.ErrOAuthSubjectRace) || repos.IsEmailTaken(err) {
			continue // the winner committed; re-run resolves to the login case
		}
		if errors.Is(err, repos.ErrIdentityConflict) {
			return nil, domain.ErrIdentityConflict()
		}
		return nil, repos.AsDomain(err)
	}
	return nil, domain.ErrInternal(errors.New("oauth resolution did not converge"))
}

// resolveTenant picks the first active membership (login parity).
func (s *Service) resolveTenant(ctx context.Context, userID string) (string, string, error) {
	ms, err := s.Tenants.Memberships(ctx, userID)
	if err != nil {
		return "", "", storeMapError(err)
	}
	if len(ms) == 0 {
		return "", "", domain.ErrForbidden("user has no active tenant membership")
	}
	return ms[0].TenantID, ms[0].Role, nil
}

// ExchangeWebGrant consumes the single-use grant and mints the short web
// session. The browser never receives a refresh token.
func (s *Service) ExchangeWebGrant(ctx context.Context, grant string) (*WebSession, error) {
	if grant == "" || len(grant) > 128 || !strings.HasPrefix(grant, "wgrant_") {
		return nil, domain.ErrDesktopCode()
	}
	raw, ok := consumeOneTime(ctx, s.Redis, "auth:web:grant:"+grant)
	if !ok {
		return nil, domain.ErrDesktopCode()
	}
	var g webGrant
	if err := json.Unmarshal([]byte(raw), &g); err != nil || g.UserID == "" {
		return nil, domain.ErrDesktopCode()
	}
	u, err := s.Users.ByID(ctx, g.UserID)
	if err != nil {
		return nil, storeMapError(err)
	}
	if u == nil || u.Status != "active" {
		return nil, domain.ErrUnauthorized(errors.New("account not active"))
	}

	tenantID := g.TenantID
	role := ""
	if tenantID != "" {
		if m, err := s.Tenants.Membership(ctx, tenantID, u.ID); err == nil && m != nil && m.Status == "active" {
			role = m.Role
		}
	}
	if role == "" {
		if tenantID, role, err = s.resolveTenant(ctx, u.ID); err != nil {
			return nil, err
		}
	}

	if s.Signer == nil {
		return nil, &domain.Error{Code: "UNSUPPORTED_AUTH_MODE", Message: "This deployment mints no access tokens; use your identity provider.", HTTP: http.StatusNotImplemented}
	}
	access, err := s.Signer.SignWeb(u.ID, tenantID, role, ids.New("jti"), time.Now(), s.WebSessionTTL)
	if err != nil {
		return nil, domain.ErrInternal(err)
	}

	ws := &WebSession{
		AccessToken: access, TokenType: "Bearer",
		ExpiresIn: int64(s.WebSessionTTL.Seconds()),
		User: UserInfo{ID: u.ID, Email: u.Email, DisplayName: u.DisplayName, Status: u.Status,
			IsPlatformAdmin: u.IsPlatformAdmin, EmailVerified: u.EmailVerified},
	}
	if tenantID != "" {
		if t, _ := s.Tenants.ByID(ctx, tenantID); t != nil {
			ws.Tenant = TenantInfo{ID: t.ID, Slug: t.Slug, Name: t.Name, Status: t.Status}
		}
	}
	return ws, nil
}

// randomHex returns n random bytes hex-encoded (2n chars, crypto/rand).
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is unrecoverable for a security boundary; panic
		// is the honest response (same posture as key generation elsewhere).
		slog.Error("auth: crypto/rand failed", "error", err)
		panic("auth: crypto/rand unavailable")
	}
	return hex.EncodeToString(b)
}
