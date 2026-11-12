// Package tests — auth_pg_test.go
//
// THE AUTH-ROTATION SCENARIO against REAL PostgreSQL. Found live on
// 2026-11-06: RefreshTokensRepo.Rotate wrote replaced_by BEFORE inserting
// the successor row — an immediate-FK violation (SQLSTATE 23503) that every
// hermetic fake was blind to and no PG-gated test covered. Refresh token
// rotation 500'd for 100% of real users. P06 pins the fix.
//
// Gated on NEXAU_TEST_DATABASE_URL (the project's PG-integration
// convention; see docs/DEPLOYMENT.md):
//
//      NEXAU_TEST_DATABASE_URL=postgres://… go test ./tests/ -run TestScenarioP06 -count=1 -v
//
//      P06  refresh rotation on the real engine   (Issue → Rotate succeeds,
//           the successor row lands, the consumed row points at it; a second
//           presentation of the same token reads as THEFT "used", and the
//           family revocation the service performs on that signal works)
package tests

import (
        "context"
        "fmt"
        "os"
        "testing"
        "time"

        "github.com/mash-cloud/mash-api/internal/auth"
        "github.com/mash-cloud/mash-api/internal/store"
        "github.com/mash-cloud/mash-api/internal/store/repos"
)

// authDB provisions a fresh full schema (all migrations) on a disposable
// real PostgreSQL and returns the repos the auth rotation path touches.
func authDB(t *testing.T) (*store.Postgres, *repos.RefreshTokensRepo, *repos.OAuthUsersRepo, string) {
        t.Helper()
        dsn := os.Getenv("NEXAU_TEST_DATABASE_URL")
        if dsn == "" {
                t.Skip("NEXAU_TEST_DATABASE_URL not set — skipping PG integration")
        }
        ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
        defer cancel()

        pg, err := store.NewPostgres(ctx, dsn, 8, 1, time.Hour, 5*time.Second)
        if err != nil {
                t.Fatalf("connect: %v", err)
        }
        t.Cleanup(pg.Close)

        if err := store.Migrate(ctx, pg); err != nil {
                t.Fatalf("migrate: %v", err)
        }

        oauth := repos.NewOAuthUsers(pg.Pool)
        u, err := oauth.FindOrCreateGoogleUser(ctx,
                "p06-google-sub", "p06@example.com", "P06 User", "")
        if err != nil {
                t.Fatalf("seed user: %v", err)
        }
        return pg, repos.NewRefreshTokens(pg.Pool), oauth, u.ID
}

func p06Token(userID string, run string) *repos.RefreshToken {
        id := "rt_p06_" + run
        return &repos.RefreshToken{
                ID:        id,
                UserID:    userID,
                FamilyID:  id,
                TokenHash: auth.HashToken("p06-secret-" + run),
                ExpiresAt: time.Now().Add(24 * time.Hour),
                UserAgent: "p06-test",
        }
}

// TestScenarioP06_RefreshRotationSurvivesRealPG: the rotation transaction
// must commit on the real engine — the immediate FK on replaced_by is the
// trap this scenario exists to keep shut.
func TestScenarioP06_RefreshRotationSurvivesRealPG(t *testing.T) {
        pg, refresh, oauth, _ := authDB(t)
        ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
        defer cancel()

        // A user with a live refresh token, exactly as the desktop exchange
        // leaves things. IDs are unique per run: the seeded user is idempotent
        // (FindOrCreate), but token rows from an earlier run must not collide.
        run := fmt.Sprintf("%d", time.Now().UnixNano())
        u, err := oauth.FindOrCreateGoogleUser(ctx,
                "p06-google-sub", "p06@example.com", "P06 User", "")
        if err != nil || u == nil {
                t.Fatalf("user: %v", err)
        }
        root := p06Token(u.ID, run)
        if err := refresh.Issue(ctx, root); err != nil {
                t.Fatalf("issue root: %v", err)
        }

        // Rotation 1: consume root, install successor. Before the 2026-11-06
        // fix this UPDATE-then-INSERT order died with SQLSTATE 23503.
        successor := &repos.RefreshToken{
                ID:        root.ID + "_s1",
                UserID:    root.UserID,
                FamilyID:  root.FamilyID,
                TokenHash: auth.HashToken("p06-secret-successor-1-" + run),
                ExpiresAt: time.Now().Add(24 * time.Hour),
                UserAgent: "p06-test",
        }
        consumed, err := refresh.Rotate(ctx, root.TokenHash, successor)
        if err != nil {
                t.Fatalf("rotate: %v (regression of the replaced_by FK ordering bug)", err)
        }
        if consumed == nil || consumed.ID != root.ID {
                t.Fatalf("rotate returned wrong row: %+v", consumed)
        }

        // The durable state: successor live, root consumed + linked.
        var rootUsed, rootReplacedBy, succCount int
        err = pg.Pool.QueryRow(ctx, `
                SELECT (used_at IS NOT NULL)::int, COALESCE(replaced_by = $1, false)::int
                FROM refresh_tokens WHERE id = $2`,
                successor.ID, root.ID).Scan(&rootUsed, &rootReplacedBy)
        if err != nil {
                t.Fatalf("root row: %v", err)
        }
        if rootUsed != 1 || rootReplacedBy != 1 {
                t.Fatalf("root not consumed/linked: used=%d linked=%d", rootUsed, rootReplacedBy)
        }
        err = pg.Pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens WHERE id = $1`,
                successor.ID).Scan(&succCount)
        if err != nil || succCount != 1 {
                t.Fatalf("successor row missing: %v count=%d", err, succCount)
        }

        // Rotation 2 on the SAME (now consumed+revoked-by-rotation) token: the
        // theft signal. Rotate's state switch tests RevokedAt before UsedAt, so
        // a rotated token replays as "revoked" — either reuse status triggers
        // the family-wide revocation in the service layer.
        if _, err := refresh.Rotate(ctx, root.TokenHash, &repos.RefreshToken{
                ID: root.ID + "_s2", UserID: root.UserID, FamilyID: root.FamilyID,
                TokenHash: auth.HashToken("p06-secret-successor-2-" + run),
                ExpiresAt: time.Now().Add(24 * time.Hour), UserAgent: "p06-test",
        }); err == nil {
                t.Fatal("replaying a consumed token must fail")
        } else if status, reuse := repos.ReuseStatus(err); !reuse || status != "revoked" {
                t.Fatalf("replay must read as reuse/revoked, got %v", err)
        }

        // The service-level theft response (family revocation) works on the
        // real engine too. The root is already revoked-by-rotation, so the
        // family sweep must kill the remaining LIVE successor (n=1) — and the
        // successor's revoked_reason records the reuse.
        if n, err := refresh.RevokeFamily(ctx, root.FamilyID, "reuse"); err != nil || n != 1 {
                t.Fatalf("revoke family: n=%d err=%v (the live successor must die)", n, err)
        }
}
