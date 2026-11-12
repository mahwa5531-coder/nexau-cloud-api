// Package tests — data_accumulation_test.go
//
// THE ACCUMULATED-HISTORY SCENARIOS (hermetic): llm_calls is the one table
// that grows by design — one row per call, forever (until the 400-day
// sweeper). These scenarios pin the WINDOW MATH at a scale an operator
// actually meets after months of traffic, where single-digit-row tests
// cannot lie: hundreds of thousands of rows, exact sums, exact recovery
// instants, and a parallel fan-out landing on top of a large history.
//
// The fake (windowedUsage) implements the production SQL one-for-one —
// sum(total_tokens) FILTER (recorded_at >= now()-window) and the
// oldest-first cumulative-prefix recovery — with a controllable clock. The
// REAL-SQL versions of these scenarios (1M rows, EXPLAIN, retention purge,
// vacuum, idempotent replay against the actual unique index) live in
// data_accumulation_pg_test.go, gated on NEXAU_TEST_DATABASE_URL.
//
// Scenario index (D = data accumulation, hermetic):
//
//	D01  150k-row history, exact windows     (seven days of calls: both
//	     window sums land on the exact token, and the recovery instant
//	     picks the exact row among 150,000 — oldest-first prefix logic,
//	     not an approximation)
//	D02  fan-out on top of a big history     (40 parallel admissions against
//	     a 100k-row history: exactly the calls that fit are admitted, the
//	     atomic reservation keeps the boundary exact under concurrency —
//	     the overshoot bug this design replaced scaled with fan-out)
//	D03  same-instant bursts and drip-open   (100 calls in one second all
//	     expire together five hours later — capacity re-opens as a step;
//	     spread calls re-open one row at a time — capacity re-opens as a
//	     drip; both behaviors fall straight out of "rows leave oldest-first")
package tests

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
)

// ---- D01: exact windows over a 150k-row history ------------------------------------

// TestScenarioD01_ExactWindowsOver150kHistory — a week of accumulated
// traffic (150,000 calls). The window sums must land on the exact token
// against independently-computed expectations, and a quota rejection must
// name the exact row whose expiry re-opens capacity: with (used − quota) = 7
// tokens to shed and the oldest in-window row carrying 9, resets_at is that
// row's recorded_at + 5h — picked out of 150,000 rows, to the second.
func TestScenarioD01_ExactWindowsOver150kHistory(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	tid := idn.Tenant.ID

	// 150k rows over the last 7 days: age_i = 1m + i·3.85s (deterministic;
	// no row sits on a window boundary), tokens_i = 5 + (i mod 5).
	const rows = 150_000
	base := sc.win.clock()
	type expectation struct {
		at     time.Time
		tokens int64
	}
	history := make([]expectation, 0, rows)
	var wantWeekly int64
	for i := 0; i < rows; i++ {
		age := time.Minute + time.Duration(i)*3850*time.Millisecond
		tok := int64(5 + i%5)
		history = append(history, expectation{at: base.Add(-age), tokens: tok})
		wantWeekly += tok
	}
	// Bulk-load the fake (same write path the scenario meter uses).
	sc.win.mu.Lock()
	sc.win.seq += int64(rows)
	for _, h := range history {
		sc.win.events[tid] = append(sc.win.events[tid], windowEvent{at: h.at, tokens: h.tokens})
	}
	sc.win.mu.Unlock()

	// Independent expectation for the 5h window.
	var want5h int64
	for _, h := range history {
		if h.at.After(base.Add(-5 * time.Hour)) {
			want5h += h.tokens
		}
	}

	pos, err := sc.env.proxy.QuotaPosition(context.Background(), tid, 1_000_000, 2_000_000)
	if err != nil {
		t.Fatalf("window usage: %v", err)
	}
	if pos.Used5h != want5h {
		t.Fatalf("5h window over %d rows: got %d, want %d", rows, pos.Used5h, want5h)
	}
	if pos.UsedWeekly != wantWeekly {
		t.Fatalf("weekly window: got %d, want %d", pos.UsedWeekly, wantWeekly)
	}

	// Recovery exactness: shed 7 tokens. Oldest-first, the first in-window
	// row carries 5 + (i mod 5) for its i — the independent replay below
	// finds the exact crossing row; the window must agree to the second.
	quota := want5h - 7
	rec, err := sc.env.proxy.Usage.WindowRecovery(context.Background(), tid, 5*time.Hour, quota, want5h)
	if err != nil || rec == nil {
		t.Fatalf("recovery: %v, %v", rec, err)
	}
	in5h := make([]expectation, 0, rows)
	for _, h := range history {
		if h.at.After(base.Add(-5*time.Hour)) && h.tokens > 0 {
			in5h = append(in5h, h)
		}
	}
	sort.Slice(in5h, func(i, j int) bool { return in5h[i].at.Before(in5h[j].at) })
	var cum int64
	var want time.Time
	for _, h := range in5h {
		cum += h.tokens
		if cum > 7 {
			want = h.at.Add(5 * time.Hour)
			break
		}
	}
	if rec.Sub(want) != 0 {
		t.Fatalf("recovery instant: got %v, want %v", rec.UTC(), want.UTC())
	}
}

// ---- D02: fan-out on top of a big history --------------------------------------------

// TestScenarioD02_FanOutOnBigHistoryIsExact — 100,000 accumulated rows and
// then 40 SIMULTANEOUS admissions, each reserving 250 tokens, with headroom
// for exactly 12. The atomic reservation ledger must admit exactly 12 and
// reject exactly 28 — not 13 (double-spend) and not 11 (over-hold) — no
// matter how the arrivals interleave.
func TestScenarioD02_FanOutOnBigHistoryIsExact(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.RequestsPerMinuteUser = 600 // keep RPM out of the picture
	idn.Limits.RequestsPerMinuteTenant = 6000
	idn.Limits.ConcurrentRequestsUser = 256
	idn.Limits.ConcurrentRequestsTenant = 256
	idn.Limits.Window5hTokens = 8000 // re-derived below from the real window
	tid := idn.Tenant.ID

	// A 100k-row history, one call every 3 seconds for ~3.5 days, 1 token
	// each: a long tail of old rows plus a dense recent head — the shape
	// months of agent traffic actually produces.
	sc.win.mu.Lock()
	sc.win.seq += 100_000
	evs := make([]windowEvent, 0, 100_000)
	for i := 0; i < 100_000; i++ {
		evs = append(evs, windowEvent{at: sc.win.now.Add(-time.Duration(i) * 3 * time.Second), tokens: 1})
	}
	sc.win.events[tid] = append(sc.win.events[tid], evs...)
	sc.win.mu.Unlock()

	// The real 5h position: rows with age ≤ 5h ⇔ i ≤ 6000 (the SQL cutoff
	// recorded_at >= now()-5h is INCLUSIVE, and i=6000 sits exactly on it)
	// → 6001 tokens. Read it, don't assume it — then leave headroom for
	// exactly 12×250.
	pos, err := sc.env.proxy.QuotaPosition(context.Background(), tid, 1, 1)
	if err != nil {
		t.Fatalf("window read: %v", err)
	}
	if pos.Used5h != 6001 {
		t.Fatalf("history 5h window = %d, want 6001 (inclusive cutoff)", pos.Used5h)
	}
	idn.Limits.Window5hTokens = pos.Used5h + 12*250
	// The weekly window matters too: 100k rows × 1 token = 100k weekly —
	// exactly the free plan's ceiling. Give it headroom so only the 5h
	// window is the binding dimension here.
	idn.Limits.WindowWeeklyTokens = pos.UsedWeekly + 100_000

	// 40 simultaneous admissions while all streams stay in flight.
	sc.env.fb.setScript(
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"working"}}]}`, delay: 800 * time.Millisecond},
		usageChunk("openai/gpt-6-luna", 100, 150), // actual 250 each
	)

	const total = 40
	results := make(chan *domain.Error, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, de := sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 249), &captureSink{})
			results <- de
		}()
	}
	wg.Wait()
	close(results)

	admitted, rejected := 0, 0
	for de := range results {
		if de == nil {
			admitted++
			continue
		}
		rejected++
		if de.Code != "WINDOW_QUOTA_EXCEEDED" {
			t.Fatalf("rejection must name the window, got %s", de.Code)
		}
	}
	if admitted != 12 || rejected != 28 {
		t.Fatalf("fan-out boundary: admitted %d, rejected %d — want exactly 12/28", admitted, rejected)
	}

	// Once the dust settles, usage is history + the 12 actuals — the
	// window lands exactly on the ceiling the ledger enforced.
	waitFor(t, "the 12 in-flight calls to land", 3*time.Second, func() bool {
		return sc.win.used5h(tid) == pos.Used5h+12*250
	})
}

// ---- D03: same-instant bursts and drip-open -------------------------------------------

// TestScenarioD03_BurstsExpireTogetherCallsDrip — how capacity re-opens as
// the window slides depends on how the binge was shaped:
//
//   - 60 calls spread 30s apart cross the 5h line one by one: capacity
//     re-opens as a DRIP (exactly one row's worth per interval, oldest
//     first), and a quota rejection during the drip names the next row's
//     exact expiry;
//   - 100 calls recorded in the SAME second all cross together: capacity
//     re-opens as a STEP (one jump of the whole burst).
//
// Both are the same rule — a row leaves the window exactly recorded_at+5h
// later — and the recovery instant reflects whichever shape the history has.
func TestScenarioD03_BurstsExpireTogetherCallsDrip(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000_000
	tid := idn.Tenant.ID
	base := sc.win.clock()

	// Shape A — a same-second burst: 100 calls at base−2h, 2 tokens each.
	// All 100 expire together at base+3h (recorded_at + 5h).
	burstAt := base.Add(-2 * time.Hour)
	// Shape B — a spread binge: 60 calls whose ages run 4h59m30s − i·30s
	// (i=0..59), 5 tokens each — i.e. the OLDEST expires at base+30s, then
	// one row every 30 seconds until base+30m. Observation instants are
	// placed mid-interval so no row ever sits exactly ON a cutoff.
	spreadAge := func(i int) time.Duration {
		return 5*time.Hour - 30*time.Second - time.Duration(i)*30*time.Second
	}

	sc.win.mu.Lock()
	sc.win.seq += 160
	for i := 0; i < 100; i++ {
		sc.win.events[tid] = append(sc.win.events[tid], windowEvent{at: burstAt, tokens: 2})
	}
	for i := 0; i < 60; i++ {
		sc.win.events[tid] = append(sc.win.events[tid], windowEvent{at: base.Add(-spreadAge(i)), tokens: 5})
	}
	sc.win.mu.Unlock()

	if got := sc.win.used5h(tid); got != 200+300 {
		t.Fatalf("initial window = %d, want 500", got)
	}

	// The spread rows drip out first — exactly one row per 30s, oldest
	// first (i=0 at base+30s, then 1, 2, …). Observe at 45s, 75s, 105s:
	// each is 15s past one expiry and 15s short of the next.
	sc.win.advance(45 * time.Second)
	for step := int64(1); step <= 2; step++ {
		if got := sc.win.used5h(tid); got != 500-5*step {
			t.Fatalf("drip observation %d: window = %d, want %d", step, got, 500-5*step)
		}
		sc.win.advance(30 * time.Second)
	}
	if got := sc.win.used5h(tid); got != 485 {
		t.Fatalf("drip observation 3: window = %d, want 485", got)
	}

	// A quota rejection during the drip names the NEXT row's expiry. At
	// clock = base+105s the remaining spread rows are i = 3..59 (57 rows,
	// 285 tokens) plus the burst (200). Shed 7 tokens: oldest-first, the
	// first row carries 5 (cum 5 ≤ 7), the second crosses — recovery is
	// the second-oldest remaining row's expiry, i.e. row i=4's
	// (base+30s+4·30s = base+150s).
	used := sc.win.used5h(tid)
	if used != 485 {
		t.Fatalf("mid-drip window = %d, want 485", used)
	}
	rec, err := sc.env.proxy.Usage.WindowRecovery(context.Background(), tid, 5*time.Hour, used-7, used)
	if err != nil || rec == nil {
		t.Fatalf("recovery: %v, %v", rec, err)
	}
	want := base.Add(-spreadAge(4)).Add(5 * time.Hour)
	if rec.Sub(want) != 0 {
		t.Fatalf("recovery instant during drip: got %v, want %v", rec.UTC(), want.UTC())
	}

	// The spread finishes dripping (last row i=59 expires at base+30m);
	// only the burst remains, and it leaves as ONE step at base+3h.
	sc.win.advance(31*time.Minute - 105*time.Second) // clock = base+31m
	if got := sc.win.used5h(tid); got != 200 {
		t.Fatalf("after the spread drips out: %d, want burst-only 200", got)
	}
	sc.win.advance(2*time.Hour + 29*time.Minute - time.Second) // clock = base+2h59m59s, just before base+3h
	if got := sc.win.used5h(tid); got != 200 {
		t.Fatalf("just before the burst expires: %d, want 200", got)
	}
	sc.win.advance(2 * time.Second) // cross base+3h
	if got := sc.win.used5h(tid); got != 0 {
		t.Fatalf("the same-second burst must leave as one step: %d, want 0", got)
	}
}
