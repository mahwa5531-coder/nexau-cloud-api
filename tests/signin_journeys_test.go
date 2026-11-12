// Package tests — signin_journeys_test.go (user-journey audit, phase 2).
//
// THE SIGN-IN FAMILY (SCENARIOS.md A01-A24): every journey a person takes
// into and around the session system, driven against the REAL auth.Service —
// real state machine, real single-use consumes, real PKCE/nonce verification
// — with hermetic counterparts for the outside world: an in-memory user/
// tenant/refresh store trio, miniredis, and a scripted fake Google that
// serves its own JWKS and signs real RS256 ID tokens.
//
// What this file pins (test ids reference the SCENARIOS.md rows):
//
//      A01/A02  first Google sign-in provisions; returning sign-in resolves
//      A04      unverified email → grant refused
//      A05      state replay + tx-cookie mismatch (CSRF attempts)
//      A07      ID-token nonce mismatch (replay defense)
//      A08/A09  web grant → web session; grant is single-use
//      A10-A12  desktop pairing: issue, two-channel race, 60s expiry
//      A14      deactivated account cannot pair
//      A15      expired access token is classified, not mysterious
//      A16/A17  refresh rotation; REUSE revokes the whole family (theft)
//      A18      a merely EXPIRED refresh never purges the family
//      A19/A20  logout one device / logout-all
//      A21      web token cannot reach device-only routes (XSS blast radius)
package tests

import (
        "context"
        "crypto/rand"
        "crypto/rsa"
        "encoding/base64"
        "encoding/json"
        "net/http"
        "net/http/httptest"
        "net/url"
        "strings"
        "sync"
        "testing"
        "time"

        "github.com/alicebob/miniredis/v2"
        "github.com/golang-jwt/jwt/v5"
        "github.com/redis/go-redis/v9"

        "github.com/mash-cloud/mash-api/internal/auth"
        "github.com/mash-cloud/mash-api/internal/domain"
        "github.com/mash-cloud/mash-api/internal/store/repos"
)

// ---------------------------------------------------------------------------
// hermetic counterparts for the outside world
// ---------------------------------------------------------------------------

// fakeUsers implements auth.UsersStore.
type fakeUsers struct {
        mu    sync.Mutex
        byID  map[string]*domain.User
        seq   int
        logins int
}

func newFakeUsers() *fakeUsers { return &fakeUsers{byID: map[string]*domain.User{}} }

func (f *fakeUsers) add(u *domain.User) {
        f.mu.Lock()
        defer f.mu.Unlock()
        f.byID[u.ID] = u
}

func (f *fakeUsers) setStatus(id, status string) {
        f.mu.Lock()
        defer f.mu.Unlock()
        if u, ok := f.byID[id]; ok {
                u.Status = status
        }
}

func (f *fakeUsers) ByID(_ context.Context, id string) (*domain.User, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        u := f.byID[id]
        return u, nil
}

func (f *fakeUsers) TouchLogin(_ context.Context, id string) error {
        f.mu.Lock()
        defer f.mu.Unlock()
        f.logins++
        return nil
}

// fakeTenants implements auth.TenantsStore.
type fakeTenants struct {
        mu   sync.Mutex
        ten  map[string]*domain.Tenant
        mems map[string]domain.Membership // "tenant|user" → membership
        seq  int
}

func newFakeTenants() *fakeTenants {
        return &fakeTenants{ten: map[string]*domain.Tenant{}, mems: map[string]domain.Membership{}}
}

func (f *fakeTenants) addMember(tenantID, userID, role, status string) {
        f.mu.Lock()
        defer f.mu.Unlock()
        if _, ok := f.ten[tenantID]; !ok {
                f.seq++
                f.ten[tenantID] = &domain.Tenant{ID: tenantID, Slug: tenantID, Name: tenantID, Status: "active"}
        }
        f.mems[tenantID+"|"+userID] = domain.Membership{TenantID: tenantID, UserID: userID, Role: role, Status: status}
}

func (f *fakeTenants) ByID(_ context.Context, tenantID string) (*domain.Tenant, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        return f.ten[tenantID], nil
}

func (f *fakeTenants) Membership(_ context.Context, tenantID, userID string) (*domain.Membership, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        if m, ok := f.mems[tenantID+"|"+userID]; ok {
                return &m, nil
        }
        return nil, nil
}

func (f *fakeTenants) Memberships(_ context.Context, userID string) ([]domain.Membership, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        var out []domain.Membership
        for _, m := range f.mems {
                if m.UserID == userID && m.Status == "active" {
                        out = append(out, m)
                }
        }
        return out, nil
}

// fakeRefresh implements auth.RefreshStore with the SAME concurrency
// semantics as the SQL: used/revoked/expired rotations return the
// repos.ReuseSignal the real repo returns, so the service's theft logic
// (RFC 6819 §5.2.2.3) runs for real.
type fakeRefresh struct {
        mu     sync.Mutex
        byHash map[string]*repos.RefreshToken
        famRev int
        usrRev int
}

func newFakeRefresh() *fakeRefresh { return &fakeRefresh{byHash: map[string]*repos.RefreshToken{}} }

func (f *fakeRefresh) Issue(_ context.Context, t *repos.RefreshToken) error {
        f.mu.Lock()
        defer f.mu.Unlock()
        f.byHash[t.TokenHash] = t
        return nil
}

func (f *fakeRefresh) ByHash(_ context.Context, hash string) (*repos.RefreshToken, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        return f.byHash[hash], nil
}

func (f *fakeRefresh) Consume(_ context.Context, hash string) (*repos.RefreshToken, bool, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        t, ok := f.byHash[hash]
        if !ok {
                return nil, false, nil
        }
        if t.UsedAt != nil || t.RevokedAt != nil {
                return t, false, repos.ReuseSignal("used")
        }
        now := time.Now().UTC()
        t.UsedAt = &now
        return t, true, nil
}

func (f *fakeRefresh) Rotate(_ context.Context, hash string, successor *repos.RefreshToken) (*repos.RefreshToken, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        t, ok := f.byHash[hash]
        if !ok {
                return nil, repos.ReuseSignal("used") // SQL row-vanished: reads as used
        }
        switch {
        case t.RevokedAt != nil:
                return nil, repos.ReuseSignal("revoked")
        case t.UsedAt != nil:
                return nil, repos.ReuseSignal("used")
        case !t.ExpiresAt.After(time.Now()):
                return nil, repos.ReuseSignal("expired")
        }
        now := time.Now().UTC()
        t.UsedAt = &now
        t.ReplacedBy = successor.ID
        f.byHash[successor.TokenHash] = successor
        return t, nil
}

func (f *fakeRefresh) RevokeFamily(_ context.Context, familyID, _ string) (int64, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        var n int64
        now := time.Now().UTC()
        for _, t := range f.byHash {
                if t.FamilyID == familyID && t.RevokedAt == nil {
                        t.RevokedAt = &now
                        n++
                }
        }
        if n > 0 {
                f.famRev++
        }
        return n, nil
}

func (f *fakeRefresh) RevokeUser(_ context.Context, userID, _ string) (int64, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        var n int64
        now := time.Now().UTC()
        for _, t := range f.byHash {
                if t.UserID == userID && t.RevokedAt == nil {
                        t.RevokedAt = &now
                        n++
                }
        }
        if n > 0 {
                f.usrRev++
        }
        return n, nil
}

// fakeOAuth implements auth.OAuthStore: provisioning mirrors the documented
// login/provision cases, including the email-taken-by-another-subject
// conflict.
type fakeOAuth struct {
        mu      sync.Mutex
        users   *fakeUsers
        tenants *fakeTenants
        bySub   map[string]string // google sub → user id
        byMail  map[string]string // email → user id
        created int
        seq     int
}

func newFakeOAuth(u *fakeUsers, t *fakeTenants) *fakeOAuth {
        return &fakeOAuth{users: u, tenants: t, bySub: map[string]string{}, byMail: map[string]string{}}
}

func (f *fakeOAuth) FindOrCreateGoogleUser(_ context.Context, sub, email, displayName, avatarURL string) (*domain.User, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        if id, ok := f.bySub[sub]; ok { // login case: same subject, never a duplicate
                return f.users.byID[id], nil
        }
        if _, taken := f.byMail[email]; taken { // same email, different subject
                return nil, repos.ErrIdentityConflict
        }
        f.seq++
        id := "usr_journey_" + strings.Repeat("0", 3-len(itoa(f.seq)))+itoa(f.seq)
        tenant := "ten_" + id
        u := &domain.User{
                ID: id, Email: email, DisplayName: displayName, AvatarURL: avatarURL,
                Status: "active", AuthProvider: "google", ExternalSubject: sub, EmailVerified: true,
        }
        f.bySub[sub] = id
        f.byMail[email] = id
        f.users.byID[id] = u
        f.tenants.addMember(tenant, id, "owner", "active") // provision tenant + membership
        f.created++
        return u, nil
}

func itoa(n int) string {
        if n == 0 {
                return "0"
        }
        var b []byte
        for n > 0 {
                b = append([]byte{byte('0' + n%10)}, b...)
                n /= 10
        }
        return string(b)
}

// fakeDevices implements auth.DevicesStore.
type fakeDevices struct {
        mu   sync.Mutex
        byID map[string]string // device id → display name
        seq  int
}

func newFakeDevices() *fakeDevices { return &fakeDevices{byID: map[string]string{}} }

func (f *fakeDevices) UpsertDevice(_ context.Context, _, deviceID, name, _ string) (string, error) {
        f.mu.Lock()
        defer f.mu.Unlock()
        if deviceID == "" {
                f.seq++
                deviceID = "dev_new_" + itoa(f.seq)
        }
        f.byID[deviceID] = name
        return deviceID, nil
}

// fakeGoogle serves the two endpoints the real GoogleProvider talks to: a
// token endpoint returning RS256-signed ID tokens and a JWKS endpoint
// publishing the signing key. The test scripts the identity (sub/email/
// nonce...) before finishing the flow, exactly like a hostile-or-honest
// Google would behave.
type fakeGoogle struct {
        srv  *httptest.Server
        key  *rsa.PrivateKey
        mu   sync.Mutex
        ident struct {
                sub, email, name, picture, nonce string
                emailVerified                    bool
        }
        verifierSeen string
        tokenCalls   int
}

func newFakeGoogle(t *testing.T) *fakeGoogle {
        t.Helper()
        key, err := rsa.GenerateKey(rand.Reader, 2048)
        if err != nil {
                t.Fatalf("rsa keygen: %v", err)
        }
        fg := &fakeGoogle{key: key}
        fg.ident.sub = "google-sub-1"
        fg.ident.email = "dev@example.test"
        fg.ident.emailVerified = true
        fg.ident.name = "Dev Tester"

        mux := http.NewServeMux()
        mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
                _ = r.ParseForm()
                fg.mu.Lock()
                fg.tokenCalls++
                fg.verifierSeen = r.FormValue("code_verifier")
                nonce, sub, email := fg.ident.nonce, fg.ident.sub, fg.ident.email
                name, picture, verified := fg.ident.name, fg.ident.picture, fg.ident.emailVerified
                fg.mu.Unlock()

                now := time.Now()
                tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
                        "iss": "accounts.google.com", "aud": "test-client-id",
                        "sub": sub, "email": email, "email_verified": verified,
                        "name": name, "picture": picture, "nonce": nonce,
                        "iat": now.Unix(), "exp": now.Add(10 * time.Minute).Unix(),
                })
                tok.Header["kid"] = "fk1"
                signed, serr := tok.SignedString(key)
                if serr != nil {
                        w.WriteHeader(500)
                        return
                }
                w.Header().Set("Content-Type", "application/json")
                _ = json.NewEncoder(w).Encode(map[string]string{"id_token": signed})
        })
        mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
                w.Header().Set("Content-Type", "application/json")
                _ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
                        "kty": "RSA", "alg": "RS256", "use": "sig", "kid": "fk1",
                        "n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
                        "e":   "AQAB",
                }}})
        })
        fg.srv = httptest.NewServer(mux)
        t.Cleanup(fg.srv.Close)
        return fg
}

func (f *fakeGoogle) setIdentity(sub, email, name, nonce string, verified bool) {
        f.mu.Lock()
        defer f.mu.Unlock()
        f.ident.sub, f.ident.email, f.ident.name, f.ident.nonce, f.ident.emailVerified = sub, email, name, nonce, verified
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type signinEnv struct {
        svc      *auth.Service
        google   *fakeGoogle
        users    *fakeUsers
        tenants  *fakeTenants
        refresh  *fakeRefresh
        oauth    *fakeOAuth
        devices  *fakeDevices
        mr       *miniredis.Miniredis
        signer   *auth.LocalSigner
}

func newSigninEnv(t *testing.T) *signinEnv {
        t.Helper()
        mr := miniredis.RunT(t)
        rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
        t.Cleanup(func() { _ = rdb.Close() })

        fu, ft, fr := newFakeUsers(), newFakeTenants(), newFakeRefresh()
        fo, fd := newFakeOAuth(fu, ft), newFakeDevices()
        fg := newFakeGoogle(t)
        signer := auth.NewLocalSigner("journey-test-secret-0123456789abcdef", "mash-test", "mash-api", 30*time.Second, 15*time.Minute)

        svc := &auth.Service{
                Users: fu, Tenants: ft, RefreshRepo: fr, Redis: rdb,
                Google: auth.NewGoogleProvider("test-client-id", "test-client-secret",
                        "http://app.test/callback", fg.srv.URL+"/authorize", fg.srv.URL+"/token", fg.srv.URL+"/jwks",
                        fg.srv.Client()),
                OAuth: fo, Devices: fd,
                WebSessionTTL: 15 * time.Minute, WebGrantTTL: 5 * time.Minute, OAuthStateTTL: 10 * time.Minute,
                DesktopCodeTTL: 60 * time.Second,
                Signer: signer, AccessTTL: 15 * time.Minute, RefreshTTL: 30 * 24 * time.Hour,
                RevocationCheck: true,
        }
        return &signinEnv{svc: svc, google: fg, users: fu, tenants: ft, refresh: fr,
                oauth: fo, devices: fd, mr: mr, signer: signer}
}

// completeGoogleLogin drives the full browser leg: begin → (script the fake
// Google with the flow's nonce) → finish. Returns the user + web grant.
func completeGoogleLogin(be *signinEnv, code string) (*domain.User, string, error) {
        ctx := context.Background()
        redirect, state, tx, err := be.svc.BeginGoogleOAuth(ctx)
        if err != nil {
                return nil, "", err
        }
        u, perr := url.Parse(redirect)
        if perr != nil {
                return nil, "", perr
        }
        // The fake Google must echo the flow's nonce in the ID token (this is
        // exactly what the real one does with the authorize-request nonce).
        be.google.setIdentity(be.google.ident.sub, be.google.ident.email,
                be.google.ident.name, u.Query().Get("nonce"), be.google.ident.emailVerified)
        return be.svc.FinishGoogleOAuth(ctx, code, state, tx)
}

// pairDesktop runs the full handshake for an existing user: web-side code
// issue → desktop exchange.
func pairDesktop(be *signinEnv, t *testing.T, userID, tenantID, deviceName string) *auth.TokenPair {
        t.Helper()
        code, _, err := be.svc.IssueDesktopCode(context.Background(), userID, tenantID)
        if err != nil {
                t.Fatalf("issue desktop code: %v", err)
        }
        pair, err := be.svc.ExchangeDesktopCode(context.Background(), code, "dev_x1", deviceName, "macos", "journey-ua")
        if err != nil {
                t.Fatalf("exchange desktop code: %v", err)
        }
        return pair
}

// ---------------------------------------------------------------------------
// the journeys
// ---------------------------------------------------------------------------

// A01 + A02 + A08: first sign-in provisions (user+tenant+membership), the web
// grant becomes a short web session, and a returning sign-in resolves to the
// SAME account — never a duplicate.
func TestJourneyA01_FirstSignInProvisionsAndReturningResolves(t *testing.T) {
        be := newSigninEnv(t)

        u1, grant1, err := completeGoogleLogin(be, "code-one")
        if err != nil {
                t.Fatalf("first sign-in: %v", err)
        }
        if be.oauth.created != 1 {
                t.Fatalf("first sign-in should provision exactly one user, got %d", be.oauth.created)
        }
        if grant1 == "" || !strings.HasPrefix(grant1, "wgrant_") {
                t.Fatalf("expected a web grant, got %q", grant1)
        }
        // A01: tenant + membership were provisioned with the user.
        ms, _ := be.tenants.Memberships(context.Background(), u1.ID)
        if len(ms) != 1 || ms[0].Role != "owner" || ms[0].Status != "active" {
                t.Fatalf("provisioning must create one active owner membership, got %+v", ms)
        }

        // A08: the grant converts to a web session (single-use — A09 below).
        ws, err := be.svc.ExchangeWebGrant(context.Background(), grant1)
        if err != nil {
                t.Fatalf("web grant exchange: %v", err)
        }
        if ws.AccessToken == "" || ws.ExpiresIn <= 0 {
                t.Fatalf("web session must carry a short token, got %+v", ws)
        }

        // A02: returning sign-in (a second begin/finish pair) resolves the same user.
        u2, _, err := completeGoogleLogin(be, "code-two")
        if err != nil {
                t.Fatalf("returning sign-in: %v", err)
        }
        if u2.ID != u1.ID || be.oauth.created != 1 {
                t.Fatalf("returning sign-in must resolve to the same account: %s vs %s (created=%d)", u2.ID, u1.ID, be.oauth.created)
        }
}

// A09: the web grant is single-use — the second exchange fails like any
// unknown code.
func TestJourneyA09_WebGrantIsSingleUse(t *testing.T) {
        be := newSigninEnv(t)
        _, grant, err := completeGoogleLogin(be, "code-g")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        if _, err := be.svc.ExchangeWebGrant(context.Background(), grant); err != nil {
                t.Fatalf("first grant exchange: %v", err)
        }
        if _, err := be.svc.ExchangeWebGrant(context.Background(), grant); err == nil {
                t.Fatal("grant reuse must fail")
        }
}

// A04: Google identity with an unverified email completes OAuth but is
// refused before any user is resolved.
func TestJourneyA04_UnverifiedEmailRefused(t *testing.T) {
        be := newSigninEnv(t)
        ctx := context.Background()
        redirect, state, tx, err := be.svc.BeginGoogleOAuth(ctx)
        if err != nil {
                t.Fatalf("begin: %v", err)
        }
        u, _ := url.Parse(redirect)
        be.google.setIdentity(be.google.ident.sub, be.google.ident.email,
                be.google.ident.name, u.Query().Get("nonce"), false) // unverified

        if _, _, err := be.svc.FinishGoogleOAuth(ctx, "code-u", state, tx); err == nil {
                t.Fatal("unverified email must be refused")
        } else if de := domain.AsError(err); de == nil || de.Code != "OAUTH_EMAIL_UNVERIFIED" {
                t.Fatalf("expected OAUTH_EMAIL_UNVERIFIED, got %v", err)
        }
        if be.oauth.created != 0 {
                t.Fatalf("no user may be provisioned from an unverified email, got %d", be.oauth.created)
        }
}

// A03: same email arriving under a different Google subject is an identity
// conflict — refused, never merged.
func TestJourneyA03_EmailTakenByDifferentSubjectIsConflict(t *testing.T) {
        be := newSigninEnv(t)
        if _, _, err := completeGoogleLogin(be, "code-a"); err != nil {
                t.Fatalf("first sign-in: %v", err)
        }
        // Same email, different Google account (subject changed).
        orig := be.google.ident
        be.google.setIdentity("google-sub-2", orig.email, "Impostor", "", true)
        if _, _, err := completeGoogleLogin(be, "code-b"); err == nil {
                t.Fatal("email taken by another subject must conflict")
        } else if de := domain.AsError(err); de == nil || de.Code != "IDENTITY_CONFLICT" {
                t.Fatalf("expected IDENTITY_CONFLICT, got %v", err)
        }
        if be.oauth.created != 1 {
                t.Fatalf("conflict must not provision anything, created=%d", be.oauth.created)
        }
}

// A05: the OAuth state is single-use and bound to the tx cookie — replays
// and cross-browser injections get one generic error, no oracle.
func TestJourneyA05_StateReplayAndTxMismatch(t *testing.T) {
        be := newSigninEnv(t)
        ctx := context.Background()

        redirect, state, tx, err := be.svc.BeginGoogleOAuth(ctx)
        if err != nil {
                t.Fatalf("begin: %v", err)
        }
        u, _ := url.Parse(redirect)
        be.google.setIdentity(be.google.ident.sub, be.google.ident.email, be.google.ident.name, u.Query().Get("nonce"), true)

        if _, _, err := be.svc.FinishGoogleOAuth(ctx, "code-s", state, tx); err != nil {
                t.Fatalf("honest finish: %v", err)
        }
        // Replay the SAME state+tx (provider double-delivery / attacker echo).
        if _, _, err := be.svc.FinishGoogleOAuth(ctx, "code-s2", state, tx); err == nil {
                t.Fatal("state replay must fail")
        }
        // Fresh state, wrong tx cookie (login-CSRF injection). This fails at the
        // constant-time compare BEFORE any provider call — no nonce needed.
        _, state2, _, err := be.svc.BeginGoogleOAuth(ctx)
        if err != nil {
                t.Fatalf("begin 2: %v", err)
        }
        if _, _, err := be.svc.FinishGoogleOAuth(ctx, "code-s3", state2, "attacker-tx-value"); err == nil {
                t.Fatal("tx cookie mismatch must fail")
        }
}

// A07: an ID token whose nonce does not match THIS login attempt is
// rejected — replay defense (audit M1).
func TestJourneyA07_NonceMismatchRejectsReplayedIDToken(t *testing.T) {
        be := newSigninEnv(t)
        ctx := context.Background()
        _, state, tx, err := be.svc.BeginGoogleOAuth(ctx)
        if err != nil {
                t.Fatalf("begin: %v", err)
        }
        // A DIFFERENT nonce in the ID token: a token minted for another attempt.
        be.google.setIdentity(be.google.ident.sub, be.google.ident.email, be.google.ident.name, "nonce-of-another-login", true)
        if _, _, err := be.svc.FinishGoogleOAuth(ctx, "code-n", state, tx); err == nil {
                t.Fatal("nonce mismatch must be rejected")
        }
        // PKCE proof also flowed: the token endpoint saw the server-held verifier.
        if be.google.verifierSeen == "" {
                t.Fatal("token exchange must present the PKCE code_verifier")
        }
}

// A10 + A11 + A12: the desktop handshake — honest exchange, single-use
// (two-channel race), and the 60-second TTL.
func TestJourneyA10toA12_DesktopPairingSingleUseAndExpiry(t *testing.T) {
        be := newSigninEnv(t)
        u, _, err := completeGoogleLogin(be, "code-d")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        ms, _ := be.tenants.Memberships(context.Background(), u.ID)
        tenant := ms[0].TenantID

        code, ttl, err := be.svc.IssueDesktopCode(context.Background(), u.ID, tenant)
        if err != nil {
                t.Fatalf("issue: %v", err)
        }
        if !strings.HasPrefix(code, "mcode_") || ttl != 60 {
                t.Fatalf("expected mcode with 60s ttl, got %q ttl=%d", code, ttl)
        }

        // Channel 1 (deep link) wins the exchange.
        pair, err := be.svc.ExchangeDesktopCode(context.Background(), code, "dev_1", "My Mac", "macos", "ua")
        if err != nil {
                t.Fatalf("first exchange: %v", err)
        }
        if pair.AccessToken == "" || pair.RefreshToken == "" {
                t.Fatal("desktop exchange must mint a full pair")
        }
        // Channel 2 (loopback) fires the SAME code: must lose cleanly.
        if _, err := be.svc.ExchangeDesktopCode(context.Background(), code, "dev_1", "My Mac", "macos", "ua"); err == nil {
                t.Fatal("code reuse must fail (single-use)")
        }

        // 60s expiry: a fresh code dies with its TTL (miniredis virtual clock).
        code2, _, err := be.svc.IssueDesktopCode(context.Background(), u.ID, tenant)
        if err != nil {
                t.Fatalf("issue 2: %v", err)
        }
        be.mr.FastForward(61 * time.Second)
        if _, err := be.svc.ExchangeDesktopCode(context.Background(), code2, "dev_1", "My Mac", "macos", "ua"); err == nil {
                t.Fatal("expired code must fail")
        }
}

// A14: a deactivated account cannot pair a new device.
func TestJourneyA14_InactiveAccountCannotPair(t *testing.T) {
        be := newSigninEnv(t)
        u, _, err := completeGoogleLogin(be, "code-i")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        ms, _ := be.tenants.Memberships(context.Background(), u.ID)
        be.users.setStatus(u.ID, "suspended")

        code, _, err := be.svc.IssueDesktopCode(context.Background(), u.ID, ms[0].TenantID)
        if err != nil {
                t.Fatalf("issue: %v", err)
        }
        _, err = be.svc.ExchangeDesktopCode(context.Background(), code, "dev_1", "X", "macos", "ua")
        if err == nil {
                t.Fatal("inactive account must not receive tokens")
        }
        if de := domain.AsError(err); de == nil || de.Code != "ACCOUNT_INACTIVE" {
                t.Fatalf("expected ACCOUNT_INACTIVE, got %v", err)
        }
}

// A15: an expired access token is classified as expired — the client knows
// to rotate, not to re-login.
func TestJourneyA15_ExpiredAccessTokenClassified(t *testing.T) {
        be := newSigninEnv(t)
        now := time.Now()
        old, err := be.signer.Sign("usr_x", "ten_x", "owner", "dev_x", "jti_old", now.Add(-20*time.Minute))
        if err != nil {
                t.Fatalf("sign: %v", err)
        }
        _, err = be.signer.Verify(context.Background(), old)
        if err == nil {
                t.Fatal("expired token must fail verification")
        }
        var ec auth.ErrTokenClass
        if !asErrTokenClass(err, &ec) || ec.Kind != "expired" {
                t.Fatalf("expected classified expired, got %v", err)
        }
        // A tampered token is signature-class, and a web-scoped token still
        // verifies (scope is enforced by the middleware, A21).
        forged := old[:len(old)-3] + "abc"
        if _, err := be.signer.Verify(context.Background(), forged); err == nil {
                t.Fatal("tampered token must fail")
        }
}

func asErrTokenClass(err error, target *auth.ErrTokenClass) bool {
        if e, ok := err.(auth.ErrTokenClass); ok {
                *target = e
                return true
        }
        return false
}

// A16 + A17: refresh rotation is atomic, and REUSING a consumed token
// revokes the entire family — the attacker's successor AND the victim's.
func TestJourneyA16andA17_RefreshRotationAndTheftResponse(t *testing.T) {
        be := newSigninEnv(t)
        u, _, err := completeGoogleLogin(be, "code-r")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        ms, _ := be.tenants.Memberships(context.Background(), u.ID)
        pair1 := pairDesktop(be, t, u.ID, ms[0].TenantID, "Laptop")

        // A16: honest rotation.
        pair2, err := be.svc.Refresh(context.Background(), pair1.RefreshToken, "ua")
        if err != nil {
                t.Fatalf("rotation: %v", err)
        }
        if pair2.RefreshToken == "" || pair2.RefreshToken == pair1.RefreshToken {
                t.Fatal("rotation must mint a fresh refresh token")
        }

        // A17: the STOLEN original is replayed → theft detected, family revoked.
        _, err = be.svc.Refresh(context.Background(), pair1.RefreshToken, "attacker-ua")
        if err == nil {
                t.Fatal("replayed refresh must fail")
        }
        if de := domain.AsError(err); de == nil || de.Code != "REFRESH_TOKEN_REUSED" {
                t.Fatalf("expected REFRESH_TOKEN_REUSED, got %v", err)
        }
        if be.refresh.famRev == 0 {
                t.Fatal("reuse must revoke the token family")
        }
        // The victim's (rotated) token is dead too — that is the point of the
        // family response: the attacker cannot keep spinning successors.
        if _, err := be.svc.Refresh(context.Background(), pair2.RefreshToken, "ua"); err == nil {
                t.Fatal("family revocation must kill the successor chain")
        }
}

// A18: a merely EXPIRED refresh token (sleeping laptop) is a clean 401 —
// the family is NOT purged (audit M5).
func TestJourneyA18_ExpiredRefreshDoesNotPurgeFamily(t *testing.T) {
        be := newSigninEnv(t)
        u, _, err := completeGoogleLogin(be, "code-e")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        ms, _ := be.tenants.Memberships(context.Background(), u.ID)
        pair := pairDesktop(be, t, u.ID, ms[0].TenantID, "Old Laptop")

        // Second device, same user — an independent family that must survive.
        code, _, err := be.svc.IssueDesktopCode(context.Background(), u.ID, ms[0].TenantID)
        if err != nil {
                t.Fatalf("issue: %v", err)
        }
        other, err := be.svc.ExchangeDesktopCode(context.Background(), code, "dev_2", "Phone", "ios", "ua")
        if err != nil {
                t.Fatalf("second exchange: %v", err)
        }

        // Age the first device's token past its 30-day life (never used).
        be.refresh.mu.Lock()
        be.refresh.byHash[auth.HashToken(pair.RefreshToken)].ExpiresAt = time.Now().Add(-time.Hour)
        be.refresh.mu.Unlock()

        _, err = be.svc.Refresh(context.Background(), pair.RefreshToken, "ua")
        if err == nil {
                t.Fatal("expired refresh must be refused")
        }
        if de := domain.AsError(err); de == nil || de.Code != "REFRESH_TOKEN_EXPIRED" {
                t.Fatalf("expected REFRESH_TOKEN_EXPIRED, got %v", err)
        }
        if be.refresh.famRev != 0 {
                t.Fatal("expiry is not theft — no family may be purged")
        }
        // The other device still rotates happily.
        if _, err := be.svc.Refresh(context.Background(), other.RefreshToken, "ua"); err != nil {
                t.Fatalf("unrelated family must survive an expiry elsewhere: %v", err)
        }
}

// A19 + A20: logout revokes one device's session; logout-all revokes every
// family (lost-device response).
func TestJourneyA19andA20_LogoutAndLogoutAll(t *testing.T) {
        be := newSigninEnv(t)
        u, _, err := completeGoogleLogin(be, "code-l")
        if err != nil {
                t.Fatalf("sign-in: %v", err)
        }
        ms, _ := be.tenants.Memberships(context.Background(), u.ID)
        tenant := ms[0].TenantID

        laptop := pairDesktop(be, t, u.ID, tenant, "Laptop")
        code, _, err := be.svc.IssueDesktopCode(context.Background(), u.ID, tenant)
        if err != nil {
                t.Fatalf("issue: %v", err)
        }
        phone, err := be.svc.ExchangeDesktopCode(context.Background(), code, "dev_2", "Phone", "ios", "ua")
        if err != nil {
                t.Fatalf("exchange 2: %v", err)
        }

        // A19: logout the laptop only.
        if err := be.svc.Logout(context.Background(), laptop.RefreshToken, "jti_l", time.Minute); err != nil {
                t.Fatalf("logout: %v", err)
        }
        if _, err := be.svc.Refresh(context.Background(), laptop.RefreshToken, "ua"); err == nil {
                t.Fatal("logged-out refresh must be dead")
        }
        if _, err := be.svc.Refresh(context.Background(), phone.RefreshToken, "ua"); err != nil {
                t.Fatalf("phone must survive the laptop logout: %v", err)
        }
        // The logged-out access token's JTI is blacklisted for its remaining life.
        if !be.svc.IsBlacklisted(context.Background(), "jti_l") {
                t.Fatal("logout must blacklist the access JTI")
        }

        // A20: logout-all (lost device).
        n, err := be.svc.LogoutAll(context.Background(), u.ID, "jti_p", time.Minute)
        if err != nil {
                t.Fatalf("logout-all: %v", err)
        }
        if n < 1 {
                t.Fatalf("logout-all must revoke live families, revoked=%d", n)
        }
        if _, err := be.svc.Refresh(context.Background(), phone.RefreshToken, "ua"); err == nil {
                t.Fatal("every session must die on logout-all")
        }
}

// A21 (audit M3): a browser web-session token is refused by device-only
// routes — the web origin's XSS blast radius is a profile view, not the
// tenant's token budget.
func TestJourneyA21_WebTokenCannotReachDeviceRoutes(t *testing.T) {
        be := newSigninEnv(t)
        now := time.Now()
        webTok, err := be.signer.SignWeb("usr_x", "ten_x", "owner", "jti_w", now, 15*time.Minute)
        if err != nil {
                t.Fatalf("sign web: %v", err)
        }
        apiTok, err := be.signer.SignWithTTL("usr_x", "ten_x", "owner", "dev_x", "jti_a", now, 15*time.Minute)
        if err != nil {
                t.Fatalf("sign api: %v", err)
        }

        idn := testIdentity()
        mw := auth.Middleware{Verifier: be.signer, Resolver: &stubResolver{idn: idn},
                AuthTimeout: 2 * time.Second}
        probe := func(tok string, deviceOnly bool) int {
                var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
                if deviceOnly {
                        h = mw.RequireDevice(h)
                } else {
                        h = mw.Require(h)
                }
                req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
                req.Header.Set("Authorization", "Bearer "+tok)
                rec := httptest.NewRecorder()
                h.ServeHTTP(rec, req)
                return rec.Code
        }

        if code := probe(webTok, true); code != http.StatusForbidden {
                t.Fatalf("web token on device-only route: got %d, want 403", code)
        }
        if code := probe(apiTok, true); code != http.StatusOK {
                t.Fatalf("device token on device-only route: got %d, want 200", code)
        }
        if code := probe(webTok, false); code != http.StatusOK {
                t.Fatalf("web token on a web-allowed route: got %d, want 200", code)
        }
}
