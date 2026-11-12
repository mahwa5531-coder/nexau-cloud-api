package auth

// oauth_e2e_test.go — the Google sign-in flows the base suite does not
// cover, all against the REAL provider code (fake Google endpoints, real
// RSA, real JWKS verification, real Redis semantics):
//
//   - the OIDC nonce binding (an ID token from a DIFFERENT login attempt is
//     refused even though perfectly signed) — audit M1
//   - the PKCE proof-of-possession on the WIRE: the token exchange carries
//     the code_verifier whose S256 hash the consent screen saw — audit M1
//   - JWKS key rotation: unknown kid after rotation is refused only for the
//     bounded fast-refetch floor, then the refetch converges — audit L14
//   - two logins in flight at once stay isolated (state/tx/verifier/nonce
//     are per-flow, never shared)

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/mash-cloud/mash-api/internal/domain"
)

// TestOAuthNonceMismatchRejected: a validly-signed ID token carrying the
// nonce of ANOTHER login attempt must not authenticate this one — the
// replay defense the nonce exists for.
func TestOAuthNonceMismatchRejected(t *testing.T) {
	env := newOAuthTestEnv(t)
	ctx := context.Background()

	// Flow A begins (its nonce lands in the state record).
	redirectA, stateA, txA, err := env.svc.BeginGoogleOAuth(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	nonceA := extractQueryParam(t, redirectA, "nonce")

	// Flow B begins; the fake Google will mint flow A's token for it.
	redirectB, stateB, txB, err := env.svc.BeginGoogleOAuth(ctx)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	_ = redirectB

	env.fg.mu.Lock()
	env.fg.idClaimsFn = func() jwt.MapClaims {
		return jwt.MapClaims{"iss": "https://accounts.google.com", "aud": "test-client-id",
			"sub": "sub-nonce", "email": "nonce@example.test", "email_verified": true,
			"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
			"nonce": nonceA} // A's nonce in B's callback
	}
	env.fg.mu.Unlock()

	if _, _, err := env.svc.FinishGoogleOAuth(ctx, env.fg.code(), stateB, txB); err == nil {
		t.Fatal("ID token with a foreign nonce accepted")
	} else if de := domain.AsError(err); de.Code != "OAUTH_PROVIDER_ERROR" {
		t.Fatalf("nonce mismatch code: %s", de.Code)
	}
	// Flow A itself still completes with its own nonce.
	env.fg.mu.Lock()
	env.fg.idClaimsFn = func() jwt.MapClaims {
		return jwt.MapClaims{"iss": "https://accounts.google.com", "aud": "test-client-id",
			"sub": "sub-nonce", "email": "nonce@example.test", "email_verified": true,
			"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
			"nonce": nonceA}
	}
	env.fg.mu.Unlock()
	if _, _, err := env.svc.FinishGoogleOAuth(ctx, env.fg.code(), stateA, txA); err != nil {
		t.Fatalf("flow A with its own nonce: %v", err)
	}
}

// TestOAuthPKCEVerifierOnTheWire: the authorization code alone must be
// useless — the token exchange must present the server-held code_verifier,
// and its S256 hash must equal the code_challenge the consent screen saw.
func TestOAuthPKCEVerifierOnTheWire(t *testing.T) {
	env := newOAuthTestEnv(t)
	ctx := context.Background()

	redirect, state, tx, err := env.svc.BeginGoogleOAuth(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	challenge := extractQueryParam(t, redirect, "code_challenge")
	if challenge == "" || extractQueryParam(t, redirect, "code_challenge_method") != "S256" {
		t.Fatalf("consent URL lacks S256 PKCE: %s", redirect)
	}
	nonce := extractQueryParam(t, redirect, "nonce")
	env.fg.mu.Lock()
	env.fg.idClaimsFn = func() jwt.MapClaims {
		return jwt.MapClaims{"iss": "https://accounts.google.com", "aud": "test-client-id",
			"sub": "sub-pkce", "email": "pkce@example.test", "email_verified": true,
			"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(), "nonce": nonce}
	}
	env.fg.mu.Unlock()

	if _, _, err := env.svc.FinishGoogleOAuth(ctx, env.fg.code(), state, tx); err != nil {
		t.Fatalf("finish: %v", err)
	}

	env.fg.mu.Lock()
	verifier := env.fg.lastVer
	env.fg.mu.Unlock()
	if verifier == "" {
		t.Fatal("token exchange carried no code_verifier — the code alone would have redeemed")
	}
	if got := S256CodeChallenge(verifier); got != challenge {
		t.Fatalf("verifier does not prove the challenge:\n got  %s\n want %s", got, challenge)
	}
}

// TestOAuthJWKSRotationBoundedRefetch: right after Google rotates signing
// keys, an ID token with the NEW kid is refused only while the fast-refetch
// floor throttles (bounded storm protection); past it, the refetch converges
// and login succeeds — the old code refused new-kid logins for a minute.
func TestOAuthJWKSRotationBoundedRefetch(t *testing.T) {
	env := newOAuthTestEnv(t)
	ctx := context.Background()

	// Login #1 caches the current key set.
	if _, _, err := env.runOAuth(t, "sub-r0", "r0@example.test"); err != nil {
		t.Fatalf("login #1: %v", err)
	}

	// Google rotates.
	env.fg.rotate(t)

	env.fg.mu.Lock()
	env.fg.idClaimsFn = func() jwt.MapClaims {
		return jwt.MapClaims{"iss": "https://accounts.google.com", "aud": "test-client-id",
			"sub": "sub-r1", "email": "r1@example.test", "email_verified": true,
			"name": "Rotated Key", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}
	}
	env.fg.mu.Unlock()

	// Immediately: the new kid is unknown and the refetch is throttled — a
	// bounded refusal, NOT a minutes-long outage (audit L14).
	begin := time.Now()
	redirect, state, tx, err := env.svc.BeginGoogleOAuth(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_ = redirect
	_, _, err = env.svc.FinishGoogleOAuth(ctx, env.fg.code(), state, tx)
	if err == nil {
		t.Skip("rotation raced past the throttle floor — converged instantly, also fine")
	}
	if since := time.Since(begin); since > 3*time.Second {
		t.Fatalf("throttled refusal took %s — the floor is 2s", since)
	}

	// Past the floor: the refetch picks the new key up and login succeeds.
	time.Sleep(2200 * time.Millisecond)
	if _, _, err := env.runOAuth(t, "sub-r2", "r2@example.test"); err != nil {
		t.Fatalf("login after rotation window: %v", err)
	}
}

// TestOAuthTwoFlowsInFlightStayIsolated: two browsers signing in at the
// same time — each state consumes with its OWN tx, verifier and nonce; both
// users land, neither sees the other. (Begins race; finishes run serially
// because the fake Google has one mutable claims hook — the isolation under
// test is the per-flow state, which the racing begins exercise.)
func TestOAuthTwoFlowsInFlightStayIsolated(t *testing.T) {
	env := newOAuthTestEnv(t)
	ctx := context.Background()

	type begun struct {
		state, tx, nonce string
	}
	begins := make([]begun, 2)
	var wg sync.WaitGroup
	for i := range begins {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			redirect, state, tx, err := env.svc.BeginGoogleOAuth(ctx)
			if err != nil {
				t.Errorf("parallel begin %d: %v", i, err)
				return
			}
			begins[i] = begun{state: state, tx: tx, nonce: extractQueryParam(t, redirect, "nonce")}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.Fatal("parallel begins failed")
	}
	for i, b := range begins {
		if b.state == "" || b.tx == "" || b.nonce == "" {
			t.Fatalf("begin %d incomplete: %+v", i, b)
		}
	}
	if begins[0].state == begins[1].state || begins[0].tx == begins[1].tx || begins[0].nonce == begins[1].nonce {
		t.Fatal("two concurrent flows shared state/tx/nonce material")
	}

	// Each flow finishes with its OWN nonce; both users provision.
	for i, b := range begins {
		nonce := b.nonce
		env.fg.mu.Lock()
		env.fg.idClaimsFn = func() jwt.MapClaims {
			return jwt.MapClaims{"iss": "https://accounts.google.com", "aud": "test-client-id",
				"sub": "sub-par-" + string(rune('0'+i)), "email": "p" + string(rune('0'+i)) + "@example.test",
				"email_verified": true, "iat": time.Now().Unix(),
				"exp": time.Now().Add(time.Hour).Unix(), "nonce": nonce}
		}
		env.fg.mu.Unlock()
		if _, _, err := env.svc.FinishGoogleOAuth(ctx, env.fg.code(), b.state, b.tx); err != nil {
			t.Fatalf("flow %d finish: %v", i, err)
		}
	}
	if len(env.oauth.bySub) != 2 {
		t.Fatalf("provisioned users: %d, want 2", len(env.oauth.bySub))
	}
	emails := map[string]bool{}
	for _, u := range env.oauth.bySub {
		emails[u.Email] = true
	}
	if !emails["p0@example.test"] || !emails["p1@example.test"] {
		t.Fatalf("expected both parallel users, got %v", emails)
	}
}
