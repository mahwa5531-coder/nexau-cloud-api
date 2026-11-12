package auth

// Batch-4 regression tests: M4 (rune-safe truncation) and M5 (expired refresh
// token must not purge the family). M14's atomicity is proven by the repos'
// SQL integration tests (Rotate is one transaction) and by the memRefresh
// fake's single-critical-section Rotate used here.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/store/repos"
)

func TestTruncateUTF8RuneBoundaries(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"short passthrough", "hello", 128, "hello"},
		// 40 CJK chars = 120 bytes; limit 128 would byte-slice into the middle
		// of char #43 — the rune-safe cut lands after 42 chars (126 bytes).
		{"cjk boundary", strings.Repeat("田", 43), 128, strings.Repeat("田", 42)},
		{"exact fit", strings.Repeat("田", 40), 120, strings.Repeat("田", 40)},
		// 4-byte emoji at the boundary.
		{"emoji boundary", strings.Repeat("😀", 33), 128, strings.Repeat("😀", 32)},
		// Invalid bytes are stripped, never stored (PostgreSQL would refuse).
		{"invalid bytes stripped", "ok\xff\xfe", 128, "ok"},
		{"ascii cut", "abcdefgh", 4, "abcd"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncateUTF8(c.in, c.max)
			if got != c.want {
				t.Fatalf("truncateUTF8(%d) = %q (%d bytes), want %q (%d bytes)",
					c.max, got, len(got), c.want, len(c.want))
			}
			if len(got) > c.max {
				t.Fatalf("result exceeds the byte budget: %d > %d", len(got), c.max)
			}
		})
	}
}

// newAuthService builds a Service wired to the in-memory fakes.
func newAuthService(t *testing.T) (*Service, *memRefresh, *memUsers, *memTenants) {
	t.Helper()
	users := newMemUsers()
	tenants := &memTenants{byUser: map[string][]memTenant{}}
	refresh := newMemRefresh()
	tenants.seedMembership("usr_1", "ten_1", "owner")
	users.byID["usr_1"] = &memUser{u: domain.User{ID: "usr_1", Email: "t@example.test", Status: "active"}}
	svc := &Service{
		Users: users, Tenants: tenants, RefreshRepo: refresh,
		Signer:    NewLocalSigner("0123456789abcdef0123456789abcdef0123456789abcdef", "iss", "aud", 0, time.Hour),
		AccessTTL: time.Hour, RefreshTTL: time.Hour,
	}
	return svc, refresh, users, tenants
}

var familyTokenSeq int

func issueFamilyToken(t *testing.T, refresh *memRefresh, family string, expires time.Time) string {
	t.Helper()
	familyTokenSeq++
	secret := "nxr_" + family + "_" + t.Name() + "_" + itoa(familyTokenSeq)
	rt := &repos.RefreshToken{
		ID: "rt_" + family + "_" + t.Name() + "_" + itoa(familyTokenSeq), UserID: "usr_1", TenantID: "ten_1",
		FamilyID: family, TokenHash: HashToken(secret), ExpiresAt: expires,
	}
	if err := refresh.Issue(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	return secret
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestExpiredRefreshDoesNotPurgeFamily is the M5 regression: presenting a
// never-used, expired token (sleeping laptop, weekend, NTP drift) must answer
// a clean 401 REFRESH_TOKEN_EXPIRED and leave every other session alive. The
// old behaviour treated expiry as theft and revoked the entire family.
func TestExpiredRefreshDoesNotPurgeFamily(t *testing.T) {
	svc, refresh, _, _ := newAuthService(t)
	ctx := context.Background()

	family := "fam_m5"
	expired := issueFamilyToken(t, refresh, family, time.Now().Add(-24*time.Hour))
	live := issueFamilyToken(t, refresh, family, time.Now().Add(time.Hour))

	pair, err := svc.Refresh(ctx, expired, "test-agent")
	if err == nil {
		t.Fatalf("expired token must be refused, got a pair for %q", pair.RefreshToken)
	}
	var de *domain.Error
	if !errors.As(err, &de) || de.Code != "REFRESH_TOKEN_EXPIRED" || de.HTTP != 401 {
		t.Fatalf("want 401 REFRESH_TOKEN_EXPIRED, got %v", err)
	}

	// The sibling session in the SAME family must still rotate normally.
	if _, err := svc.Refresh(ctx, live, "test-agent"); err != nil {
		t.Fatalf("M5 regression: sibling session was purged by the expired presentation: %v", err)
	}

	// A used (genuinely replayed) token still purges the family.
	replayed := issueFamilyToken(t, refresh, "fam_replay", time.Now().Add(time.Hour))
	if _, err := svc.Refresh(ctx, replayed, "ua"); err != nil {
		t.Fatalf("first use: %v", err)
	}
	_, err = svc.Refresh(ctx, replayed, "ua")
	if err == nil {
		t.Fatal("replaying a used token must fail")
	}
	if errors.As(err, &de); de == nil || de.Code != "REFRESH_TOKEN_REUSED" {
		t.Fatalf("replay must be REFRESH_TOKEN_REUSED, got %v", err)
	}
}

// TestRotationLeavesOldTokenFullyRotated pins M14's end-state: after one
// refresh the old row is consumed AND linked AND the successor exists — the
// three facts that used to live in three separate transactions.
func TestRotationLeavesOldTokenFullyRotated(t *testing.T) {
	svc, refresh, _, _ := newAuthService(t)
	ctx := context.Background()

	secret := issueFamilyToken(t, refresh, "fam_m14", time.Now().Add(time.Hour))
	pair, err := svc.Refresh(ctx, secret, "ua")
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}

	oldHash := HashToken(secret)
	old, err := refresh.ByHash(ctx, oldHash)
	if err != nil || old == nil {
		t.Fatalf("old row must survive rotation: %v", err)
	}
	if old.UsedAt == nil {
		t.Fatal("old token must be consumed")
	}
	if old.ReplacedBy != pair.RefreshID {
		t.Fatalf("old token must link to its successor: %q vs %q", old.ReplacedBy, pair.RefreshID)
	}
	succ, err := refresh.ByHash(ctx, HashToken(pair.RefreshToken))
	if err != nil || succ == nil {
		t.Fatalf("successor must exist after rotation: %v", err)
	}
	if succ.FamilyID != old.FamilyID {
		t.Fatalf("successor must inherit the family: %q vs %q", succ.FamilyID, old.FamilyID)
	}

	// And the successor actually works.
	if _, err := svc.Refresh(ctx, pair.RefreshToken, "ua"); err != nil {
		t.Fatalf("successor must be refreshable: %v", err)
	}
}
