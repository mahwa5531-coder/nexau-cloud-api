// Package tests — plan_lifecycle_pg_test.go (user-journey audit, phase 2).
//
// THE PLAN-LIFECYCLE FAMILY (SCENARIOS.md L01-L13): what buying time does,
// what not paying does, and what "weekly" actually is — pinned against REAL
// PostgreSQL with the REAL repos SQL (no store fakes): real ApplyPaid
// extend-or-insert, real Effective() read-side expiry, real plans seed, and
// the real identity resolver that production's hot path uses.
//
// Gated on NEXAU_TEST_DATABASE_URL exactly like the P-suite:
//
//      NEXAU_TEST_DATABASE_URL=postgres://… go test ./tests/ -run TestJourneyL -count=1 -v
//
// The single most important row here is L05: a tenant that stops paying
// simply stops matching the Effective() query — no sweeper, no deletion,
// no "reset" — and the identity resolver hands back the Free plan's
// ceilings on the very next call.
package tests

import (
        "context"
        "os"
        "testing"
        "time"

        "github.com/mash-cloud/mash-api/internal/auth"
        "github.com/mash-cloud/mash-api/internal/payment"
        "github.com/mash-cloud/mash-api/internal/store"
        "github.com/mash-cloud/mash-api/internal/store/repos"
)

// lifecycleDB provisions a FRESH full schema (every migration) plus one
// tenant/user/membership, and wires the real payment service (real SQL
// store) and the real identity resolver over the same pool.
func lifecycleDB(t *testing.T) (*store.Postgres, *auth.IdentityResolver, *payment.Service, *payment.MockProvider, string, string) {
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

        // Full, honest reset: drop the schema, re-run every migration.
        if _, err := pg.Pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
                t.Fatalf("schema reset: %v", err)
        }
        if err := store.Migrate(ctx, pg); err != nil {
                t.Fatalf("migrate: %v", err)
        }

        tenantID, userID := "ten_life", "usr_life"
        if _, err := pg.Pool.Exec(ctx, `
                INSERT INTO tenants (id, slug, name, status, created_at, updated_at)
                VALUES ($1, $1, $1, 'active', now(), now())`, tenantID); err != nil {
                t.Fatalf("seed tenant: %v", err)
        }
        if _, err := pg.Pool.Exec(ctx, `
                INSERT INTO users (id, email, display_name, status, is_platform_admin, auth_provider, external_subject, email_verified, created_at, updated_at)
                VALUES ($1, $1 || '@test', 'L', 'active', FALSE, 'google', 'sub_' || $1, TRUE, now(), now())`, userID); err != nil {
                t.Fatalf("seed user: %v", err)
        }
        if _, err := pg.Pool.Exec(ctx, `
                INSERT INTO tenant_members (tenant_id, user_id, role, status)
                VALUES ($1, $2, 'owner', 'active')`, tenantID, userID); err != nil {
                t.Fatalf("seed membership: %v", err)
        }

        // The real payment service over the REAL SQL store; the mock provider
        // runs the real HMAC paths. Money leg via webhook, like production.
        mock := payment.NewMockProvider()
        svc := payment.New(payment.Config{
                Store: repos.NewPayments(pg.Pool), Provider: mock,
                Catalog: defaultCatalog(t), OrderTTL: 15 * time.Minute,
        })

        // The real identity resolver — Redis nil: every read is authoritative,
        // which isolates the SQL semantics (the ~30s cache is A24/S13's story).
        resolver := &auth.IdentityResolver{
                Users: repos.NewUsers(pg.Pool), Tenants: repos.NewTenants(pg.Pool),
                Subs: repos.NewSubscriptions(pg.Pool), Ents: repos.NewEntitlements(pg.Pool),
        }
        return pg, resolver, svc, mock, tenantID, userID
}

// resolve runs the production identity path (the same Resolve the LLM
// admission gate calls on every request).
func resolve(t *testing.T, r *auth.IdentityResolver, userID, tenantID string) *auth.Identity {
        t.Helper()
        id, err := r.Resolve(context.Background(), userID, tenantID)
        if err != nil {
                t.Fatalf("resolve: %v", err)
        }
        return id
}

// buyPro completes a full purchase through the real service + real SQL:
// checkout → capture → signed webhook → ApplyPaid + activation.
func buyPro(t *testing.T, svc *payment.Service, mock *payment.MockProvider, tenant, sku, tag string) *payment.Order {
        t.Helper()
        ctx := context.Background()
        o, err := svc.Checkout(ctx, tenant, "usr_"+tenant, sku, "idem-"+tag)
        if err != nil {
                t.Fatalf("checkout: %v", err)
        }
        payID := mock.Capture(o.ProviderOrderID)
        body := webhookEnvelopeJSON("payment.captured", "evt_"+tag, payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := svc.HandleWebhook(ctx, body, mock.SignWebhook(body), "127.0.0.1"); err != nil {
                t.Fatalf("webhook: %v", err)
        }
        return o
}

// periodEnd reads the tenant's subscription period end straight from SQL.
func periodEnd(t *testing.T, pg *store.Postgres, tenant string) time.Time {
        t.Helper()
        var end time.Time
        err := pg.Pool.QueryRow(context.Background(),
                `SELECT current_period_end FROM subscriptions WHERE tenant_id = $1 AND status IN ('trialing','active','past_due')`,
                tenant).Scan(&end)
        if err != nil {
                t.Fatalf("period end: %v", err)
        }
        return end
}

func assertNear(t *testing.T, got, want time.Time) {
        t.Helper()
        if got.Before(want.Add(-2*time.Minute)) || got.After(want.Add(2*time.Minute)) {
                t.Fatalf("time drift: got %v, want ~%v", got, want)
        }
}

// L01: a brand-new tenant is Free by default — never plan-less, never
// unbounded. The exact seed ceilings the product promises.
func TestJourneyL01_NewUserIsFreeByDefault(t *testing.T) {
        _, r, _, _, tenant, user := lifecycleDB(t)
        id := resolve(t, r, user, tenant)

        if id.Plan.Code != "free" || id.SubscriptionStatus != "active" {
                t.Fatalf("new tenant must resolve free/active, got plan=%q sub=%q", id.Plan.Code, id.SubscriptionStatus)
        }
        if id.Limits.Window5hTokens != 60000 || id.Limits.WindowWeeklyTokens != 250000 {
                t.Fatalf("free windows must be 60k/250k, got %d/%d", id.Limits.Window5hTokens, id.Limits.WindowWeeklyTokens)
        }
        if id.Limits.RequestsPerMinuteUser != 20 || id.Limits.ConcurrentRequestsUser != 8 {
                // 8, not the 000007 seed's 3: migration 0015 raised Free
                // concurrency for desktop-agent parallelism (1-5 panes × 1-2
                // in-flight streams each) while keeping it as the abuse backstop.
                t.Fatalf("free RPM/concurrency must be 20/8, got %d/%d", id.Limits.RequestsPerMinuteUser, id.Limits.ConcurrentRequestsUser)
        }
}

// L02: buying monthly raises the ceilings through the REAL SQL path —
// checkout, webhook, ApplyPaid's conditional update + activation.
func TestJourneyL02_BuyMonthlyRaisesCeilings(t *testing.T) {
        pg, r, svc, mock, tenant, user := lifecycleDB(t)
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l02")

        id := resolve(t, r, user, tenant)
        if id.Plan.Code != "pro" {
                t.Fatalf("must resolve pro after purchase, got %q", id.Plan.Code)
        }
        if id.Limits.Window5hTokens != 2000000 || id.Limits.WindowWeeklyTokens != 10000000 {
                t.Fatalf("pro windows must be 2M/10M, got %d/%d", id.Limits.Window5hTokens, id.Limits.WindowWeeklyTokens)
        }
        if id.Limits.RequestsPerMinuteUser != 600 || id.Limits.ConcurrentRequestsUser != 16 {
                t.Fatalf("pro RPM/concurrency must be 600/16, got %d/%d", id.Limits.RequestsPerMinuteUser, id.Limits.ConcurrentRequestsUser)
        }
        assertNear(t, periodEnd(t, pg, tenant), time.Now().AddDate(0, 0, 30))
}

// L04: renewals STACK — paying twice back-to-back never loses days
// (GREATEST(now, paid_through) + period, in the same transaction as paid).
func TestJourneyL04_RenewalsStack(t *testing.T) {
        pg, _, svc, mock, tenant, _ := lifecycleDB(t)
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l04a")
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l04b")

        assertNear(t, periodEnd(t, pg, tenant), time.Now().AddDate(0, 0, 60))
        sub, err := svc.Subscription(context.Background(), tenant)
        if err != nil || sub.PlanCode != "pro" || sub.Status != payment.SubActive {
                t.Fatalf("subscription must be active pro: %+v %v", sub, err)
        }
        if sub.CurrentPeriodEnd == nil || sub.DaysRemaining < 55 || sub.DaysRemaining > 65 {
                t.Fatalf("~60 days must remain after stacking, got %+v", sub)
        }
}

// L05 — THE not-paying scenario: the period ends, and the very next
// identity resolution returns the Free ceilings. No deletion, no sweeper,
// no calendar reset: capacity keeps rolling in the same windows.
func TestJourneyL05_StopPayingFallsBackToFree(t *testing.T) {
        pg, r, svc, mock, tenant, user := lifecycleDB(t)
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l05")
        if id := resolve(t, r, user, tenant); id.Plan.Code != "pro" {
                t.Fatalf("precondition: pro, got %q", id.Plan.Code)
        }

        // The period passes (this is ALL that "not paying" does to the row —
        // read-side expiry; nothing flips status, nothing is deleted).
        if _, err := pg.Pool.Exec(context.Background(),
                `UPDATE subscriptions SET current_period_end = now() - interval '1 second' WHERE tenant_id = $1`, tenant); err != nil {
                t.Fatalf("age the period: %v", err)
        }

        id := resolve(t, r, user, tenant)
        if id.Plan.Code != "free" {
                t.Fatalf("expired period must fall back to free, got %q", id.Plan.Code)
        }
        if id.Limits.Window5hTokens != 60000 || id.Limits.WindowWeeklyTokens != 250000 {
                t.Fatalf("free ceilings must apply immediately, got %d/%d", id.Limits.Window5hTokens, id.Limits.WindowWeeklyTokens)
        }
        // The subscription row is still there — untouched, auditable, revivable.
        _ = periodEnd(t, pg, tenant)
}

// L06 — "what about weekly": there is no weekly billing SKU in the catalog,
// but a 7-day SKU (operator-addable via env, bounds allow 1-3660 days)
// behaves exactly like any other period. The 7-day thing users MEET is the
// rolling weekly quota window — which refills continuously and belongs to
// both plans.
func TestJourneyL06_SevenDaySKUActsLikeAPeriod(t *testing.T) {
        pg, r, _, mock, tenant, user := lifecycleDB(t)
        weekCat, err := payment.NewCatalog([]payment.PlanSKU{
                {ID: "plan_pro_7d", Label: "Pro — ₹599 / week", PlanCode: "pro", PeriodDays: 7, AmountPaise: 59900},
        })
        if err != nil {
                t.Fatalf("week catalog: %v", err)
        }
        svc := payment.New(payment.Config{Store: repos.NewPayments(pg.Pool), Provider: mock,
                Catalog: weekCat, OrderTTL: 15 * time.Minute})

        buyPro(t, svc, mock, tenant, "plan_pro_7d", "l06")
        assertNear(t, periodEnd(t, pg, tenant), time.Now().AddDate(0, 0, 7))
        if id := resolve(t, r, user, tenant); id.Plan.Code != "pro" {
                t.Fatalf("a 7-day period must resolve pro while it lasts, got %q", id.Plan.Code)
        }
}

// L07: idling mid-period changes nothing — the period is prepaid; Pro
// ceilings hold until current_period_end, whenever that is.
func TestJourneyL07_IdleMidPeriodKeepsPro(t *testing.T) {
        pg, r, svc, mock, tenant, user := lifecycleDB(t)
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l07")

        // Two weeks of idling: the end is still a week out.
        if _, err := pg.Pool.Exec(context.Background(),
                `UPDATE subscriptions SET current_period_end = now() + interval '7 days' WHERE tenant_id = $1`, tenant); err != nil {
                t.Fatalf("set remaining period: %v", err)
        }
        if id := resolve(t, r, user, tenant); id.Plan.Code != "pro" || id.Limits.Window5hTokens != 2000000 {
                t.Fatalf("prepaid period must hold Pro ceilings while unexpired: %+v", id.Limits)
        }
}

// L08: re-subscribing after a lapse. (a) the read-side-expired row (status
// still 'active') is simply extended from now; (b) a row whose status was
// actually flipped (canceled) no longer holds the unique slot — the INSERT
// path runs, and exactly one effective row remains.
func TestJourneyL08_ResubscribeAfterLapse(t *testing.T) {
        pg, r, svc, mock, tenant, _ := lifecycleDB(t)
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l08a")

        // (a) lapsed (end in the past, status untouched) → re-subscribe extends.
        if _, err := pg.Pool.Exec(context.Background(),
                `UPDATE subscriptions SET current_period_end = now() - interval '1 day' WHERE tenant_id = $1`, tenant); err != nil {
                t.Fatalf("lapse: %v", err)
        }
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l08b")
        assertNear(t, periodEnd(t, pg, tenant), time.Now().AddDate(0, 0, 30))

        // (b) operator-canceled row: the UPDATE guard no longer matches → the
        // INSERT path must run cleanly (the partial unique index arbitrates).
        if _, err := pg.Pool.Exec(context.Background(),
                `UPDATE subscriptions SET status = 'canceled', current_period_end = now() - interval '1 day' WHERE tenant_id = $1`, tenant); err != nil {
                t.Fatalf("cancel: %v", err)
        }
        buyPro(t, svc, mock, tenant, "plan_pro_1m", "l08c")

        var effective int
        if err := pg.Pool.QueryRow(context.Background(),
                `SELECT count(*) FROM subscriptions WHERE tenant_id = $1 AND status IN ('trialing','active','past_due')`,
                tenant).Scan(&effective); err != nil {
                t.Fatalf("count effective: %v", err)
        }
        if effective != 1 {
                t.Fatalf("exactly one effective subscription may exist, got %d", effective)
        }
        assertNear(t, periodEnd(t, pg, tenant), time.Now().AddDate(0, 0, 30))
        _ = r // resolver exercised via the buys above; keep it wired for clarity
}

// L13: an operator hot-changing plan limits via SQL goes live on the next
// resolution — the documented ops lever (seed 000007 comment).
func TestJourneyL13_HotChangedLimitsGoLive(t *testing.T) {
        pg, r, _, _, tenant, user := lifecycleDB(t)
        ctx := context.Background()

        if _, err := pg.Pool.Exec(ctx,
                `UPDATE plans SET limits = jsonb_set(limits, '{window_5h_tokens}', '123456') WHERE code = 'free'`); err != nil {
                t.Fatalf("hot change: %v", err)
        }
        id := resolve(t, r, user, tenant)
        if id.Limits.Window5hTokens != 123456 {
                t.Fatalf("hot-changed limit must be live, got %d", id.Limits.Window5hTokens)
        }
}
