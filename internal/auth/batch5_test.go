package auth

// Batch-5 regression tests: M3 (web tokens cannot reach device routes).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestWebTokenRefusedOnDeviceRoutes is the M3 regression: a web-session
// token must be refused (403) by RequireDevice routes while remaining valid
// on web-safe routes; device tokens keep full access.
func TestWebTokenRefusedOnDeviceRoutes(t *testing.T) {
	signer := NewLocalSigner("0123456789abcdef0123456789abcdef0123456789abcdef", "iss", "aud", 0, time.Hour)
	webTok, err := signer.SignWeb("usr_w", "ten_w", "owner", "jti_w1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	apiTok, err := signer.Sign("usr_w", "ten_w", "owner", "", "jti_a1", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	stub := stubResolverFunc(func(ctx context.Context, userID, tenantID string) (*Identity, error) {
		return &Identity{
			User:       UserInfo{ID: userID, Status: "active"},
			Tenant:     TenantInfo{ID: tenantID, Status: "active"},
			Membership: MembershipInfo{Role: "owner", Status: "active"},
			Plan:       PlanInfo{Code: "free"}, SubscriptionStatus: "active",
		}, nil
	})
	mw := Middleware{Verifier: signer, Resolver: stub}
	deviceRoute := mw.RequireDevice(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	webRoute := mw.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(h http.Handler, tok string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := do(deviceRoute, webTok); got != http.StatusForbidden {
		t.Fatalf("web token on a device route: %d, want 403", got)
	}
	if got := do(deviceRoute, apiTok); got != http.StatusOK {
		t.Fatalf("device token on a device route: %d, want 200", got)
	}
	if got := do(webRoute, webTok); got != http.StatusOK {
		t.Fatalf("web token on a web-safe route: %d, want 200", got)
	}
}

// stubResolverFunc adapts a function into Resolver.
type stubResolverFunc func(ctx context.Context, userID, tenantID string) (*Identity, error)

func (f stubResolverFunc) Resolve(ctx context.Context, userID, tenantID string) (*Identity, error) {
	return f(ctx, userID, tenantID)
}
