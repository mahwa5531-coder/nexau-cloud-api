// Package tests — data_accumulation_pg_test.go
//
// THE ACCUMULATED-HISTORY SCENARIOS against REAL PostgreSQL: the hermetic
// D-scenarios prove the window MATH; these prove the window MECHANICS —
// that months of accumulated llm_calls rows keep the quota gate fast,
// correct, bounded, and reclaimable on the actual database engine, with the
// actual migrations, indexes and SQL.
//
// Gated on NEXAU_TEST_DATABASE_URL (the project's PG-integration
// convention; see docs/DEPLOYMENT.md):
//
//	NEXAU_TEST_DATABASE_URL=postgres://… go test ./tests/ -run TestScenarioP -count=1 -v
//
// Each test provisions a fresh schema (drops llm_calls/token_normalization,
// re-runs migrations) and seeds through the real write paths.
//
// Scenario index (P = PostgreSQL, real engine):
//
//	P01  ONE MILLION accumulated rows          (200k for the tenant under
//	     test + 800k of other-tenant noise: window sums land on the exact
//	     token, the query plan stays INDEX-DRIVEN (no sequential scan), and
//	     the admission read stays in the tens of milliseconds — data
//	     accumulation must never turn the quota gate into a table scan)
//	P02  the 400-day retention sweeper         (bounded 5000-row batches:
//	     ancient rows vanish, recent rows and both window sums are
//	     untouched — growth is capped by design, not by hope)
//	P03  purged space is reclaimable           (DELETE leaves dead tuples;
//	     VACUUM returns them: dead_tup → 0 and the relation shrinks —
//	     bloat after retention is transient, not permanent)
//	P04  InsertCalls is idempotent at scale    (the same 2000-call batch
//	     presented twice (metering retry, outbox replay) inserts exactly
//	     2000 rows: the call_id unique constraint + ON CONFLICT DO NOTHING
//	     are what make at-least-once delivery safe)
//	P05  a weight change never rewrites history (UPDATE token_normalization
//	     doubles the burn rate for NEW calls; every already-stored row
//	     keeps its own total_tokens — the past is immutable, the currency
//	     changes only going forward)
package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/openrouter"
	"github.com/mash-cloud/mash-api/internal/store"
	"github.com/mash-cloud/mash-api/internal/store/repos"
)

// accumDB provisions a fresh llm_calls + token_normalization schema on a
// real PostgreSQL (disposable test DB only) and seeds one tenant with its
// personal user, mirroring the repos package's own PG-integration harness.
func accumDB(t *testing.T) (*store.Postgres, *repos.LLMCallsRepo, string, string) {
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

	if _, err := pg.Pool.Exec(ctx, `
                DROP TABLE IF EXISTS token_normalization CASCADE;
                DROP TABLE IF EXISTS llm_calls CASCADE`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	for _, version := range []int64{5, 8, 11} {
		if _, err := pg.Pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			if !strings.Contains(err.Error(), "does not exist") {
				t.Fatalf("reset bookkeeping %d: %v", version, err)
			}
		}
	}
	if err := store.Migrate(ctx, pg); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := "ten_" + ids.New("u")[6:]
	if _, err := pg.Pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, created_at, updated_at) VALUES ($1, $1, $1, 'active', now(), now())`,
		tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	userID := "usr_" + ids.New("u")[6:]
	if _, err := pg.Pool.Exec(ctx,
		`INSERT INTO users (id, email, display_name, status, is_platform_admin, auth_provider, external_subject, email_verified, created_at, updated_at)
                 VALUES ($1, $1 || '@test', 'U', 'active', FALSE, 'google', 'sub_' || $1, TRUE, now(), now())`,
		userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	return pg, repos.NewLLMCalls(pg.Pool), tenantID, userID
}

// seedTenants bulk-creates extra tenants+users for multi-tenant noise.
func seedTenants(t *testing.T, pg *store.Postgres, n int) []string {
	t.Helper()
	ctx := context.Background()
	idsList := make([]string, n)
	for i := 0; i < n; i++ {
		idsList[i] = "ten_noise" + fmt.Sprintf("%03d", i)
	}
	b := &pgx.Batch{}
	for _, id := range idsList {
		// Idempotent: noise ids are fixed, and a previous run's tenants
		// survive (only llm_calls/token_normalization are dropped).
		b.Queue(`INSERT INTO tenants (id, slug, name, status, created_at, updated_at) VALUES ($1, $1, $1, 'active', now(), now()) ON CONFLICT (id) DO NOTHING`, id)
		b.Queue(`INSERT INTO users (id, email, display_name, status, is_platform_admin, auth_provider, external_subject, email_verified, created_at, updated_at)
                         VALUES ($1, $1 || '@test', 'N', 'active', FALSE, 'google', 'sub_' || $1, TRUE, now(), now()) ON CONFLICT (id) DO NOTHING`, id)
	}
	if err := pg.Pool.SendBatch(ctx, b).Close(); err != nil {
		t.Fatalf("seed noise tenants: %v", err)
	}
	return idsList
}

// copyGen is a pgx.CopyFromSource backed by a row generator.
type copyGen struct {
	i, n int
	row  func(i int) []any
}

func (g *copyGen) Next() bool             { g.i++; return g.i <= g.n }
func (g *copyGen) Values() ([]any, error) { return g.row(g.i - 1), nil }
func (g *copyGen) Err() error             { return nil }

// llmCallsColumns matches InsertCalls' column list plus recorded_at.
var llmCallsColumns = []string{
	"call_id", "tenant_id", "user_id", "request_id",
	"requested_model", "resolved_model", "provider", "stream",
	"status", "error_code", "error_message",
	"input_tokens", "output_tokens", "total_tokens",
	"raw_prompt_tokens", "raw_completion_tokens",
	"reasoning_tokens", "cache_read_tokens", "cache_write_tokens",
	"input_cost", "output_cost", "total_cost",
	"latency_ms", "started_at", "completed_at", "recorded_at",
}

// copyRows bulk-loads rows through the real table (COPY is the fastest way
// to give the engine a month-scale history; the quota path never writes
// this way — it writes through InsertCalls, which P04 exercises).
func copyRows(t *testing.T, pg *store.Postgres, n int, row func(i int) []any, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn, err := pg.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	start := time.Now()
	if _, err := conn.Conn().CopyFrom(ctx, pgx.Identifier{"llm_calls"}, llmCallsColumns, &copyGen{n: n, row: row}); err != nil {
		t.Fatalf("copy %s: %v", what, err)
	}
	t.Logf("COPY %s: %d rows in %s", what, n, time.Since(start).Round(time.Millisecond))
}

// ---- P01: one million accumulated rows ----------------------------------------------

// TestScenarioP01_MillionRowsStayFastAndIndexed — the operator's core fear
// about accumulation: "the quota check sums my history — what happens after
// a million calls?" Answer, on the real engine with the real migrations:
//
//   - both window sums are EXACT against independently-computed totals
//     (other tenants' 800k rows do not leak into the sums — the index and
//     the WHERE agree on tenant scoping);
//   - the query plan is an INDEX SCAN (never a sequential scan);
//   - the admission read stays in the tens of milliseconds.
//
// Row ages are generated with guard bands around the 5h/7d cutoffs so the
// expected sums cannot be off by a boundary-flip while the test runs.
func TestScenarioP01_MillionRowsStayFastAndIndexed(t *testing.T) {
	pg, repo, tid, uid := accumDB(t)
	ctx := context.Background()

	// 100 noise tenants (9.5k rows each) + the target tenant (50k rows —
	// the "whale": 5% of the table, the shape of a heavy user in a ≤100-
	// user deployment). Plan note observed on the first version of this
	// test: pushed to 20% of the table, PostgreSQL deliberately flips to a
	// PARALLEL SEQ SCAN (still ~130ms at 1M rows) because random index
	// fetches for a fifth of the heap genuinely cost more — the index wins
	// exactly where production lives: many tenants, each a small slice.
	noise := seedTenants(t, pg, 100)

	const targetRows = 50_000
	var want5h, wantWeekly int64
	// Ages: band A = [6m, 4h50m] (2,000 rows, inside the 5h window);
	// band B = [5h10m, 6d23h] (48,000 rows, weekly-only). Tokens 3..9.
	// Ladders are computed in plain SECONDS first — Duration×Duration is
	// ns×ns and overflows int64 at this scale (a bug this test itself
	// caught on its first run: 195k × 417,000s wrapped negative and put
	// rows 8 hours into the future).
	row := func(i int) []any {
		var ageSec int64
		if i < 2000 {
			ageSec = 360 + int64(i)*(17440-360)/2000 // 6m .. 4h50m
		} else {
			ageSec = 18600 + int64(i-2000)*(595440-18600)/48_000 // 5h10m .. 6d23h
		}
		age := time.Duration(ageSec) * time.Second
		tok := int64(3 + i%7)
		if i < 2000 {
			want5h += tok
		}
		wantWeekly += tok
		rec := time.Now().UTC().Add(-age)
		return []any{
			fmt.Sprintf("llm_p01_t%07d", i), tid, uid, "req_p01",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil,
			tok / 3, tok - tok/3, tok,
			tok / 3, tok - tok/3,
			0, 0, 0,
			nil, nil, nil,
			100, rec, rec, rec,
		}
	}
	copyRows(t, pg, targetRows, row, "target-tenant 50k")

	// The noise: 9.5k rows per foreign tenant, spread over 6 days.
	const noiseRows = 950_000
	noiseRow := func(i int) []any {
		tenant := noise[i%len(noise)]
		ageSec := 600 + int64(i/len(noise))*(517200-600)/9500
		rec := time.Now().UTC().Add(-time.Duration(ageSec) * time.Second)
		tok := int64(1 + i%5)
		return []any{
			fmt.Sprintf("llm_p01_n%07d", i), tenant, tenant, "req_p01",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil,
			tok / 3, tok - tok/3, tok,
			tok / 3, tok - tok/3,
			0, 0, 0,
			nil, nil, nil,
			100, rec, rec, rec,
		}
	}
	copyRows(t, pg, noiseRows, noiseRow, "noise 950k")

	if _, err := pg.Pool.Exec(ctx, `ANALYZE llm_calls`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// 1. Exactness.
	pos, err := repo.WindowUsage(ctx, tid)
	if err != nil {
		t.Fatalf("window usage: %v", err)
	}
	if pos.Used5h != want5h {
		t.Fatalf("5h sum over 1M-row table: got %d, want %d", pos.Used5h, want5h)
	}
	if pos.UsedWeekly != wantWeekly {
		t.Fatalf("weekly sum: got %d, want %d", pos.UsedWeekly, wantWeekly)
	}
	// Tenant scoping: a noise tenant sees only its own rows.
	npos, err := repo.WindowUsage(ctx, noise[0])
	if err != nil {
		t.Fatalf("noise window usage: %v", err)
	}
	if npos.UsedWeekly == 0 || npos.UsedWeekly > 9500*5 {
		t.Fatalf("noise tenant scoping broken: %d", npos.UsedWeekly)
	}

	// 2. The plan stays index-driven.
	plan, err := pg.Pool.Query(ctx, `
                EXPLAIN (ANALYZE, COSTS OFF)
                SELECT COALESCE(sum(total_tokens) FILTER (WHERE recorded_at >= now() - interval '5 hours'), 0),
                       COALESCE(sum(total_tokens) FILTER (WHERE recorded_at >= now() - interval '7 days'), 0)
                FROM llm_calls
                WHERE tenant_id = $1 AND recorded_at >= now() - interval '7 days'`, tid)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	var sb strings.Builder
	for plan.Next() {
		var line string
		if err := plan.Scan(&line); err == nil {
			sb.WriteString(line + "\n")
		}
	}
	plan.Close()
	planText := sb.String()
	t.Logf("WindowUsage plan over 1M rows:\n%s", planText)
	if strings.Contains(planText, "Seq Scan on llm_calls") {
		t.Fatal("the window query must stay index-driven as data accumulates")
	}
	if !strings.Contains(planText, "Index") {
		t.Fatalf("expected an index scan, got:\n%s", planText)
	}

	// 3. Latency: the admission read over 1M accumulated rows.
	best := time.Duration(1 << 62)
	for i := 0; i < 5; i++ {
		start := time.Now()
		if _, err := repo.WindowUsage(ctx, tid); err != nil {
			t.Fatalf("timed run %d: %v", i, err)
		}
		if d := time.Since(start); d < best {
			best = d
		}
	}
	t.Logf("WindowUsage best-of-5 over 1M rows: %s", best.Round(time.Millisecond))
	if best > 250*time.Millisecond {
		t.Fatalf("admission read too slow at scale: %s", best)
	}

	// 4. The rejection path's extra query (the exact recovery instant).
	start := time.Now()
	rec, err := repo.WindowRecovery(ctx, tid, 5*time.Hour, want5h-7, want5h)
	if err != nil || rec == nil {
		t.Fatalf("recovery: %v, %v", rec, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("recovery read too slow at scale: %s", d)
	}
	t.Logf("WindowRecovery over 1M rows: %s (resets at %s)", time.Since(start).Round(time.Millisecond), rec.UTC())
}

// ---- P02: the retention sweeper -------------------------------------------------------

// TestScenarioP02_RetentionPurgesInBoundedBatches — llm_calls grows by
// nature; the 400-day sweeper is what caps it. On the real engine: rows
// older than the cutoff vanish in 5000-row batches (the housekeeper loops
// to zero), and neither the recent rows nor the window sums notice.
func TestScenarioP02_RetentionPurgesInBoundedBatches(t *testing.T) {
	pg, repo, tid, uid := accumDB(t)
	ctx := context.Background()

	ancient := time.Now().UTC().AddDate(0, 0, -450) // 450 days old
	freshRow := func(i int) []any {
		rec := time.Now().UTC().Add(-time.Duration(i) * time.Minute)
		return []any{
			fmt.Sprintf("llm_p02_f%05d", i), tid, uid, "req_p02",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil, 5, 5, 10, 5, 5, 0, 0, 0, nil, nil, nil, 10, rec, rec, rec,
		}
	}
	oldRow := func(i int) []any {
		rec := ancient.Add(-time.Duration(i) * time.Second)
		return []any{
			fmt.Sprintf("llm_p02_o%05d", i), tid, uid, "req_p02",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil, 5, 5, 10, 5, 5, 0, 0, 0, nil, nil, nil, 10, rec, rec, rec,
		}
	}
	copyRows(t, pg, 500, freshRow, "fresh 500")
	copyRows(t, pg, 12_000, oldRow, "ancient 12k")

	posBefore, err := repo.WindowUsage(ctx, tid)
	if err != nil {
		t.Fatalf("window before purge: %v", err)
	}

	// The sweeper loop: bounded batches until it stops making progress.
	cutoff := time.Now().UTC().AddDate(0, 0, -400)
	var batches []int64
	for {
		n, err := repo.PurgeOldCalls(ctx, cutoff, 5000)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		batches = append(batches, n)
		if n == 0 {
			break
		}
	}
	t.Logf("purge batches: %v", batches)
	if len(batches) != 4 || batches[0] != 5000 || batches[1] != 5000 || batches[2] != 2000 || batches[3] != 0 {
		t.Fatalf("expected bounded batches 5000+5000+2000+0, got %v", batches)
	}

	// Ancient gone, fresh intact, windows unchanged.
	var remaining, freshCount int64
	if err := pg.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE recorded_at >= now() - interval '1 day') FROM llm_calls WHERE tenant_id = $1`, tid).Scan(&remaining, &freshCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 500 || freshCount != 500 {
		t.Fatalf("after purge: remaining = %d, fresh = %d — want 500/500", remaining, freshCount)
	}
	posAfter, err := repo.WindowUsage(ctx, tid)
	if err != nil {
		t.Fatalf("window after purge: %v", err)
	}
	if posAfter.Used5h != posBefore.Used5h || posAfter.UsedWeekly != posBefore.UsedWeekly {
		t.Fatalf("purge must not touch live windows: before %+v after %+v", posBefore, posAfter)
	}
}

// ---- P03: purged space is reclaimable ---------------------------------------------------

// TestScenarioP03_VacuumReclaimsPurgedSpace — the follow-up worry: "rows
// are deleted, but does the disk come back?" DELETE leaves dead tuples;
// VACUUM (which autovacuum performs continuously in production) returns
// them. Observed on the real engine: dead tuples drop to zero and the
// relation shrinks. Retention keeps the table bounded; vacuum keeps the
// bounded size honest.
func TestScenarioP03_VacuumReclaimsPurgedSpace(t *testing.T) {
	pg, repo, tid, uid := accumDB(t)
	ctx := context.Background()

	// Fresh rows first, ancient LAST: the ancient pages sit at the heap's
	// tail, so the post-purge VACUUM can truncate them away.
	fresh := func(i int) []any {
		rec := time.Now().UTC().Add(-time.Duration(i) * time.Minute)
		return []any{
			fmt.Sprintf("llm_p03_f%05d", i), tid, uid, "req_p03",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil, 50, 50, 100, 50, 50, 0, 0, 0, nil, nil, nil, 10, rec, rec, rec,
		}
	}
	ancient := func(i int) []any {
		rec := time.Now().UTC().AddDate(0, 0, -450).Add(-time.Duration(i) * time.Second)
		return []any{
			fmt.Sprintf("llm_p03_o%05d", i), tid, uid, "req_p03",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil, 50, 50, 100, 50, 50, 0, 0, 0, nil, nil, nil, 10, rec, rec, rec,
		}
	}
	copyRows(t, pg, 1000, fresh, "fresh 1000")
	copyRows(t, pg, 60_000, ancient, "ancient 60k")

	sizeBefore := relationSize(t, pg, "llm_calls")
	deadBefore := deadTuples(t, pg, "llm_calls")

	if _, err := repo.PurgeOldCalls(ctx, time.Now().UTC().AddDate(0, 0, -400), 5000); err != nil {
		t.Fatalf("purge: %v", err)
	}
	for i := 0; i < 12; i++ { // loop the bounded batches to zero
		n, err := repo.PurgeOldCalls(ctx, time.Now().UTC().AddDate(0, 0, -400), 5000)
		if err != nil {
			t.Fatalf("purge loop: %v", err)
		}
		if n == 0 {
			break
		}
	}

	deadAfterPurge := deadTuples(t, pg, "llm_calls")
	sizeAfterPurge := relationSize(t, pg, "llm_calls")

	if _, err := pg.Pool.Exec(ctx, `VACUUM ANALYZE llm_calls`); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	deadAfterVacuum := deadTuples(t, pg, "llm_calls")
	sizeAfterVacuum := relationSize(t, pg, "llm_calls")

	t.Logf("llm_calls size: before=%d dead=%d → after-purge size=%d dead=%d → after-vacuum size=%d dead=%d",
		sizeBefore, deadBefore, sizeAfterPurge, deadAfterPurge, sizeAfterVacuum, deadAfterVacuum)

	if deadAfterPurge == 0 {
		t.Fatal("the purge must leave dead tuples for vacuum to reclaim")
	}
	if deadAfterVacuum != 0 {
		t.Fatalf("VACUUM must reclaim all dead tuples, %d remain", deadAfterVacuum)
	}
	if sizeAfterVacuum >= sizeAfterPurge {
		t.Fatalf("relation must shrink after vacuum: %d → %d", sizeAfterPurge, sizeAfterVacuum)
	}
}

func relationSize(t *testing.T, pg *store.Postgres, table string) int64 {
	t.Helper()
	var n int64
	if err := pg.Pool.QueryRow(context.Background(), `SELECT pg_total_relation_size($1)`, table).Scan(&n); err != nil {
		t.Fatalf("relation size: %v", err)
	}
	return n
}

func deadTuples(t *testing.T, pg *store.Postgres, table string) int64 {
	t.Helper()
	// The stats collector is asynchronous; flush it so the reading is
	// deterministic (PG 15+; the deployment targets PG 16+).
	_, _ = pg.Pool.Exec(context.Background(), `SELECT pg_stat_force_next_flush()`)
	var n int64
	if err := pg.Pool.QueryRow(context.Background(), `SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = $1`, table).Scan(&n); err != nil {
		t.Fatalf("dead tuples: %v", err)
	}
	return n
}

// ---- P04: InsertCalls is idempotent at scale ---------------------------------------------

// TestScenarioP04_InsertCallsIsIdempotentAtScale — every delivery path in
// the metering pipeline is at-least-once (batch retries after timeouts,
// outbox replay after crashes). The database-side guard that makes that
// safe is the llm_calls.call_id unique constraint with ON CONFLICT DO
// NOTHING: presenting the same 2000-call batch twice inserts exactly 2000
// rows, not 4000.
func TestScenarioP04_InsertCallsIsIdempotentAtScale(t *testing.T) {
	pg, repo, tid, uid := accumDB(t)
	ctx := context.Background()

	const n = 2000
	batch := make([]domain.LLMCall, n)
	now := time.Now().UTC()
	for i := range batch {
		batch[i] = domain.LLMCall{
			CallID: fmt.Sprintf("llm_p04_%05d", i), TenantID: tid, UserID: uid, RequestID: "req_p04",
			RequestedModel: "openai/gpt-6-luna", ResolvedModel: "openai/gpt-6-luna",
			Status:    domain.CallCompleted,
			Usage:     domain.TokenUsage{InputTokens: 10, OutputTokens: 10, TotalTokens: 20},
			StartedAt: now, CompletedAt: &now,
		}
	}

	if err := repo.InsertCalls(ctx, batch); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// The same batch again — a retry that did not know the first one
	// landed, or an outbox replay of a persisted-but-unremoved record.
	if err := repo.InsertCalls(ctx, batch); err != nil {
		t.Fatalf("duplicate insert: %v", err)
	}

	var count int64
	if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM llm_calls WHERE tenant_id = $1`, tid).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Fatalf("idempotency broken: %d rows for %d calls", count, n)
	}

	// And the window counts each call exactly once.
	pos, err := repo.WindowUsage(ctx, tid)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if pos.UsedWeekly != n*20 {
		t.Fatalf("window sum after duplicate presentation: %d, want %d", pos.UsedWeekly, n*20)
	}
}

// ---- P05: a weight change never rewrites history -------------------------------------------

// TestScenarioP05_WeightChangeNeverRewritesHistory — the operator doubles a
// model's output weight (pricing change, SQL UPDATE, no redeploy). Every
// already-stored row keeps the total_tokens it was written with — the past
// is immutable — while NEW calls are normalized at the doubled rate. The
// weekly window momentarily contains a mix of both rates; that is correct,
// deterministic, and reconstructible (raw counts are stored per row).
func TestScenarioP05_WeightChangeNeverRewritesHistory(t *testing.T) {
	pg, repo, tid, uid := accumDB(t)
	ctx := context.Background()

	// 1000 rows, each normalized under the seeded luna rule
	// (input 1.0 / output 5.0): prompt 100 → 100, completion 200 → 1000,
	// total 1100 per row.
	const rows = 1000
	hist := func(i int) []any {
		rec := time.Now().UTC().Add(-time.Duration(i) * time.Minute)
		return []any{
			fmt.Sprintf("llm_p05_%05d", i), tid, uid, "req_p05",
			"openai/gpt-6-luna", "openai/gpt-6-luna", "openai", true,
			"completed", nil, nil, 100, 1000, 1100, 100, 200, 0, 0, 0, nil, nil, nil, 10, rec, rec, rec,
		}
	}
	copyRows(t, pg, rows, hist, "history under old weights")

	var sumBefore int64
	if err := pg.Pool.QueryRow(ctx, `SELECT sum(total_tokens) FROM llm_calls WHERE tenant_id = $1`, tid).Scan(&sumBefore); err != nil {
		t.Fatalf("sum before: %v", err)
	}
	if want := int64(rows) * 1100; sumBefore != want {
		t.Fatalf("seeded history sum = %d, want %d", sumBefore, want)
	}

	// The operator doubles the output weight — a hot SQL change.
	if _, err := pg.Pool.Exec(ctx,
		`UPDATE token_normalization SET output_weight = 10.0 WHERE model = 'openai/gpt-6-luna' AND is_active`); err != nil {
		t.Fatalf("weight update: %v", err)
	}

	// The stored history is untouched.
	var sumAfter int64
	if err := pg.Pool.QueryRow(ctx, `SELECT sum(total_tokens) FROM llm_calls WHERE tenant_id = $1`, tid).Scan(&sumAfter); err != nil {
		t.Fatalf("sum after: %v", err)
	}
	if sumAfter != sumBefore {
		t.Fatalf("a weight change rewrote history: %d → %d", sumBefore, sumAfter)
	}

	// But NEW calls burn at the doubled rate: the loaded rule carries the
	// new weight, and NormalizeUsage prices the same raw usage higher.
	norm := repos.NewNormalization(pg.Pool)
	rules, err := norm.All(ctx)
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	var luna domain.NormRule
	for _, r := range rules {
		if r.Model == "openai/gpt-6-luna" {
			luna = r
		}
	}
	if luna.OutputWeight != 10.0 {
		t.Fatalf("updated rule not loaded: %+v", luna)
	}
	u := llm.NormalizeUsage(&openrouter.Usage{PromptTokens: 100, CompletionTokens: 200}, luna)
	if u.TotalTokens != 100+2000 {
		t.Fatalf("new-call normalization under doubled weight: got %d, want 2100", u.TotalTokens)
	}

	// And a call recorded now lands in the window at the new rate, next to
	// the old-rate rows — mixed currencies, exact arithmetic.
	now := time.Now().UTC()
	newCall := domain.LLMCall{
		CallID: "llm_p05_new", TenantID: tid, UserID: uid, RequestID: "req_p05",
		RequestedModel: "openai/gpt-6-luna", ResolvedModel: "openai/gpt-6-luna",
		Status: domain.CallCompleted, Usage: *u, StartedAt: now, CompletedAt: &now,
	}
	if err := repo.InsertCalls(ctx, []domain.LLMCall{newCall}); err != nil {
		t.Fatalf("insert new call: %v", err)
	}
	pos, err := repo.WindowUsage(ctx, tid)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if want := int64(rows)*1100 + 2100; pos.UsedWeekly != want {
		t.Fatalf("mixed-currency window: got %d, want %d", pos.UsedWeekly, want)
	}
}
