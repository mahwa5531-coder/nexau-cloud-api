// Package tests — runtime_operation_test.go
//
// THE RUNNING-SERVER SCENARIOS: what the gateway does while it is up and
// serving — the temporal behaviors an operator meets on a normal Tuesday,
// none of which are visible in a single-request view:
//
//   - the stream lifetimes the SERVER enforces on its own (duration cap,
//     idle watchdog) and how each cut is metered and delivered in-band;
//   - the usage-position cache: what it shields (Postgres, from rapid
//     agentic turns), what it refuses to serve (≥95%-of-quota positions),
//     and what it refuses to store (at/over-quota positions) so recovery
//     detection stays immediate;
//   - the admission estimate: what a call reserves before the model has
//     produced a single token (default completion bound, tool bytes);
//   - the fast-state TTLs that make a running server self-healing:
//     leaked concurrency slots expire; the RPM window slides forward on
//     its own.
//
// Scenario index (R = runtime):
//
//	R01  stream exceeds MaxDuration            (the SERVER cuts it, not the
//	     client: metered as cancelled/MAX_DURATION with the usage already
//	     seen, and the client gets an in-band error event — a clean end,
//	     distinguishable from a network fault)
//	R02  stream stalls (no chunks)             (the idle watchdog aborts the
//	     upstream: metered as failed/MODEL_TIMEOUT, in-band error event —
//	     the "why do streams end?" dashboard tells the truth about who cut)
//	R03  usage cache shields Postgres          (two position reads within
//	     the TTL cost ONE llm_calls SUM — the whole point of the 5s cache
//	     under rapid agent turns)
//	R04  ≥95% positions bypass the cache       (near the edge, freshness
//	     beats cheapness: every read goes to PG, so the final approach to
//	     quota is exact)
//	R05  at/over-quota positions are never cached (the cache is DELeted,
//	     not SET: the moment the window drips below quota, the very next
//	     admission sees it — recovery detection is never delayed by TTL)
//	R06  estimate without max_tokens           (the reservation holds
//	     prompt/4 + 2048: a request that names no completion bound still
//	     reserves a sane worst case, reconciled to actual at call end)
//	R07  tool definitions widen the estimate   (tool bytes are prompt-side
//	     consumption: same request, one tool → the reservation grows by
//	     (description+parameters+128)/4)
//	R08  leaked concurrency slots expire       (a pod dies holding slots;
//	     SlotTTL is the backstop: the user's dimension re-opens by itself)
//	R09  the RPM window slides                 (a burst hits the per-minute
//	     cap and is told the honest retry-after; a minute later the same
//	     burst is welcome again — limits are temporal, not bans)
package tests

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/openrouter"
)

// countingUsage wraps windowedUsage with a query counter — proves which
// reads reached the (fake) llm_calls SUM and which were served from cache.
type countingUsage struct {
	*windowedUsage
	queries atomic.Int64
}

func (c *countingUsage) WindowUsage(ctx context.Context, tid string) (*domain.WindowUsage, error) {
	c.queries.Add(1)
	return c.windowedUsage.WindowUsage(ctx, tid)
}

// newRuntimeScenario is a quota scenario with a counting usage fake and a
// 300ms cache TTL (fast enough to watch expire, slow enough to stay alive
// across back-to-back reads).
func newRuntimeScenario(t *testing.T) (*scenario, *countingUsage) {
	t.Helper()
	sc := newScenario(t)
	cu := &countingUsage{windowedUsage: sc.win}
	sc.env.proxy.Usage = cu
	sc.env.proxy.UsageCacheTTL = 300 * time.Millisecond
	return sc, cu
}

// ---- R01: the server cuts an over-long stream -------------------------------------

// TestScenarioR01_StreamMaxDurationCut — a stream runs past
// Timing.MaxDuration (15 minutes in production). The SERVER ends it: the
// usage already seen is metered (the provider bills those tokens), the row
// carries cancelled/MAX_DURATION so the ops dashboard attributes the cut
// correctly, and the client receives an in-band error event — a clean,
// distinguishable end instead of a mid-chunk TCP severance.
func TestScenarioR01_StreamMaxDurationCut(t *testing.T) {
	sc := newScenario(t)
	sc.env.proxy.Timing.MaxDuration = 350 * time.Millisecond

	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000

	// Content flows immediately, usage arrives quickly, then the model just
	// keeps generating past the cap.
	sc.env.fb.setScript(
		contentChunk("openai/gpt-6-luna", "part one"),
		usageChunk("openai/gpt-6-luna", 100, 300), // total 400, seen before the cut
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"part two"}}]}`, delay: 2 * time.Second},
	)

	sink := &captureSink{}
	_, de := sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 5000), sink)
	if de != nil {
		t.Fatalf("an in-band cut is not an upstream error: %v", de)
	}
	if sink.errEvent != "MAX_DURATION" {
		t.Fatalf("client must receive the in-band MAX_DURATION event, got %q", sink.errEvent)
	}

	recs := sc.env.recorder.recorded()
	if len(recs) != 1 {
		t.Fatalf("exactly one metering row, got %d", len(recs))
	}
	r := recs[0]
	if r.Status != domain.CallCancelled || r.ErrorCode != "MAX_DURATION" {
		t.Fatalf("row must say cancelled/MAX_DURATION, got %s/%s", r.Status, r.ErrorCode)
	}
	if r.Usage.TotalTokens != 400 {
		t.Fatalf("usage seen before the cut must be metered: %d", r.Usage.TotalTokens)
	}
	// And the row feeds the window like any other consumption.
	if used := sc.win.used5h(idn.Tenant.ID); used != 400 {
		t.Fatalf("window must include the cut stream's usage: %d", used)
	}
}

// ---- R02: the idle watchdog cuts a stalled stream ----------------------------------

// TestScenarioR02_IdleStallCutByWatchdog — the model stops sending chunks
// (provider hang, network black hole). The idle watchdog aborts the
// upstream after Timing.IdleTimeout (300s in production; the abort also
// tells the provider to stop billing). The row says failed/MODEL_TIMEOUT —
// never a silent empty completion, never a misleading protocol error.
func TestScenarioR02_IdleStallCutByWatchdog(t *testing.T) {
	sc := newScenario(t)
	sc.env.proxy.Timing.IdleTimeout = 150 * time.Millisecond
	sc.env.proxy.Timing.MaxDuration = 5 * time.Second

	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000

	// One chunk, then silence far past the idle window.
	sc.env.fb.setScript(
		contentChunk("openai/gpt-6-luna", "hello"),
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"...eventually"}}]}`, delay: 3 * time.Second},
	)

	sink := &captureSink{}
	_, de := sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 5000), sink)
	if de == nil || de.Code != "MODEL_TIMEOUT" {
		t.Fatalf("an idle abort must surface as MODEL_TIMEOUT, got %v", de)
	}
	if sink.errEvent != "MODEL_TIMEOUT" {
		t.Fatalf("client must receive the in-band MODEL_TIMEOUT event, got %q", sink.errEvent)
	}

	recs := sc.env.recorder.recorded()
	if len(recs) != 1 {
		t.Fatalf("exactly one metering row, got %d", len(recs))
	}
	r := recs[0]
	if r.Status != domain.CallFailed || r.ErrorCode != "MODEL_TIMEOUT" {
		t.Fatalf("row must say failed/MODEL_TIMEOUT, got %s/%s", r.Status, r.ErrorCode)
	}
}

// ---- R03: the usage cache shields Postgres ------------------------------------------

// TestScenarioR03_CacheShieldsPostgresWithinTTL — an agent fires turns in
// quick succession; each admission and each /v1/me read needs the window
// position. Within the cache TTL those reads must cost ONE llm_calls SUM:
// the cache exists precisely so rapid agentic bursts do not translate into
// rapid full-table aggregates.
func TestScenarioR03_CacheShieldsPostgresWithinTTL(t *testing.T) {
	sc, cu := newRuntimeScenario(t)
	idn := freeIdentity() // 2000/100_000 windows
	tid := idn.Tenant.ID

	sc.win.addRow(tid, 1000) // 50% of the 5h budget — safely cacheable

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := sc.env.proxy.QuotaPosition(ctx, tid, 2000, 100_000); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if got := cu.queries.Load(); got != 1 {
		t.Fatalf("5 reads within the TTL must cost 1 PG query, cost %d", got)
	}

	// After the TTL the cache expires and the next read goes back to PG.
	// (miniredis expires TTLs on its virtual clock: FastForward IS the
	// TTL elapsing — the same mechanism, minus the wall-clock wait.)
	sc.env.mr.FastForward(350 * time.Millisecond)
	if _, err := sc.env.proxy.QuotaPosition(ctx, tid, 2000, 100_000); err != nil {
		t.Fatalf("post-TTL read: %v", err)
	}
	if got := cu.queries.Load(); got != 2 {
		t.Fatalf("post-TTL read must reach PG, queries = %d", got)
	}
}

// ---- R04: near-quota reads bypass the cache ------------------------------------------

// TestScenarioR04_NearQuotaReadsBypassCache — at ≥95% of a window's budget
// a cached position is a liability: the last admissions of a session must
// be decided on fresh numbers, not on a photograph taken up to 5s ago. The
// guard compares the CACHED value against the 95% line and re-reads PG on
// every call from there on.
func TestScenarioR04_NearQuotaReadsBypassCache(t *testing.T) {
	sc, cu := newRuntimeScenario(t)
	idn := freeIdentity()
	tid := idn.Tenant.ID
	const q5h = 10_000

	sc.win.addRow(tid, 9_600) // 96% — cached on write, but never served

	ctx := context.Background()
	if _, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if _, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if got := cu.queries.Load(); got != 2 {
		t.Fatalf("near-quota reads must bypass the cache and hit PG, queries = %d", got)
	}

	// Freshness, not just query count: mutate PG between reads; the second
	// read must reflect it (a served cache would still show the old value).
	sc.win.addRow(tid, 100)
	pos, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000)
	if err != nil {
		t.Fatalf("read 3: %v", err)
	}
	if pos.Used5h != 9_700 {
		t.Fatalf("near-quota read must be fresh, got %d", pos.Used5h)
	}
}

// ---- R05: over-quota positions are never cached ---------------------------------------

// TestScenarioR05_OverQuotaNeverCached_RecoveryIsImmediate — a position at
// or over quota is DELeted from the cache, never stored: if it were cached,
// a user whose window just dripped back under quota could stay locked out
// for up to the TTL. Because nothing over-quota is ever served from cache,
// the very next admission after capacity re-opens sees it.
func TestScenarioR05_OverQuotaNeverCached_RecoveryIsImmediate(t *testing.T) {
	sc, cu := newRuntimeScenario(t)
	idn := freeIdentity()
	tid := idn.Tenant.ID
	const q5h = 10_000

	sc.win.addRow(tid, 10_500) // over budget

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		pos, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if pos.Used5h != 10_500 {
			t.Fatalf("read %d must return PG truth", i)
		}
	}
	if got := cu.queries.Load(); got != 3 {
		t.Fatalf("over-quota reads must never be served from cache, queries = %d", got)
	}
	if sc.rdb.Exists(ctx, "usage:window:"+tid).Val() == 1 {
		t.Fatal("an over-quota position must not live in the cache")
	}

	// The window drips: the oldest binge row ages out of the 5h window.
	sc.win.advance(6 * time.Hour)
	sc.win.addRow(tid, 4_000)

	// The next read sees recovery IMMEDIATELY (no TTL wait)...
	pos, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000)
	if err != nil {
		t.Fatalf("recovery read: %v", err)
	}
	if pos.Used5h != 4_000 {
		t.Fatalf("recovery must be visible at once, got %d", pos.Used5h)
	}
	// ...and now that the position is under quota it is cacheable again.
	if _, err := sc.env.proxy.QuotaPosition(ctx, tid, q5h, 100_000); err != nil {
		t.Fatalf("post-recovery read: %v", err)
	}
	if got := cu.queries.Load(); got != 4 {
		t.Fatalf("under-quota position must be cached again, queries = %d", got)
	}
}

// ---- R06: the estimate without a completion bound ------------------------------------

// TestScenarioR06_EstimateDefaultsWhenNoMaxTokens — a request that names no
// max_tokens still reserves a bounded worst case: prompt bytes/4 plus the
// 2048 default completion estimate. The reservation is observable in Redis
// while the call runs, then reconciled to the metered actual.
func TestScenarioR06_EstimateDefaultsWhenNoMaxTokens(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000

	sc.env.fb.setScript(
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"working"}}]}`, delay: 700 * time.Millisecond},
		usageChunk("openai/gpt-6-luna", 100, 300), // actual: 400
	)
	req := &llm.Request{
		Model: "openai/gpt-6-luna", Stream: true, // NO max_tokens of any kind
		Messages: []openrouter.Message{{Role: "user", Content: json.RawMessage(`"hi"`)}},
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = sc.env.proxy.Stream(context.Background(), idn, req, &captureSink{}) }()

	// While in flight: prompt "hi" = 4 bytes / 4 = 1 token, plus the 2048
	// default completion bound.
	waitFor(t, "default-bound reservation", 2*time.Second, func() bool {
		_, sum := sc.reservationLedger(idn.Tenant.ID)
		return sum == 1+2048
	})

	<-done // the call finishes: 400 actual
	waitFor(t, "reservation reconciled to actual then released", 3*time.Second, func() bool {
		n, _ := sc.reservationLedger(idn.Tenant.ID)
		return n == 0
	})
	if used := sc.win.used5h(idn.Tenant.ID); used != 400 {
		t.Fatalf("metered usage = %d, want 400", used)
	}
}

// ---- R07: tool definitions widen the estimate ----------------------------------------

// TestScenarioR07_ToolsWidenTheEstimate — tool schemas travel with every
// request and are prompt-side consumption; the reservation must price them
// in: (description + parameters + 128) / 4 tokens per tool. Two otherwise
// identical in-flight calls, one with a tool: the ledger difference is
// exactly the tool's contribution.
func TestScenarioR07_ToolsWidenTheEstimate(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 100_000

	sc.env.fb.setScript(
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"working"}}]}`, delay: 700 * time.Millisecond},
		usageChunk("openai/gpt-6-luna", 10, 30),
	)

	// Call A: plain request, bound 100 → reservation 1 + 100.
	go func() {
		_, _ = sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	}()
	waitFor(t, "plain call's reservation", 2*time.Second, func() bool {
		_, sum := sc.reservationLedger(idn.Tenant.ID)
		return sum == 101
	})

	// Call B: same request plus one tool (400-byte description, 2-byte
	// parameters) → adds (400 + 2 + 128) / 4 = 132.
	req := reqBounded("openai/gpt-6-luna", 100)
	req.Tools = []openrouter.Tool{{
		Type: "function",
		Function: &openrouter.FunctionDef{
			Name:        "get_weather",
			Description: strings.Repeat("d", 400),
			Parameters:  json.RawMessage(`{}`),
		},
	}}
	go func() {
		_, _ = sc.env.proxy.Stream(context.Background(), idn, req, &captureSink{})
	}()
	waitFor(t, "both reservations: plain + tooled", 2*time.Second, func() bool {
		_, sum := sc.reservationLedger(idn.Tenant.ID)
		return sum == 101+233
	})
}

// ---- R08: leaked concurrency slots expire ---------------------------------------------

// TestScenarioR08_LeakedSlotsExpireByTTL — a pod dies mid-call holding
// concurrency slots; nothing will ever release them. SlotTTL is the
// backstop: after it elapses, the next admission's claim prunes the dead
// holders and the user's dimension re-opens. Without it, a crashed agent
// loop would lock its user out of LLM calls until an operator flushed
// Redis by hand.
func TestScenarioR08_LeakedSlotsExpireByTTL(t *testing.T) {
	sc := newScenario(t)
	sc.env.proxy.SlotTTL = 250 * time.Millisecond
	idn := freeIdentity() // ConcUser 8
	uid := idn.User.ID

	// The crash: eight slots claimed on the user's dimension, never
	// released (a process death — the release functions died with it).
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		if s, ok := sc.env.limiter.AcquireSlot(ctx, "rl:conc:user:"+uid, 8, 250*time.Millisecond); !ok || s == nil {
			t.Fatalf("seeding leaked slot %d failed", i)
		}
	}

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 10, 30))
	if _, de := sc.env.proxy.Stream(ctx, idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); de == nil || de.Code != "CONCURRENCY_LIMIT" {
		t.Fatalf("a fully leaked dimension must reject: %v", de)
	}

	// One SlotTTL later: the dead holders are pruned by the next claim —
	// no operator, no Redis flush.
	time.Sleep(300 * time.Millisecond)
	if _, de := sc.env.proxy.Stream(ctx, idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); de != nil {
		t.Fatalf("after SlotTTL the dimension must self-heal: %v", de)
	}
}

// ---- R09: the RPM window slides ---------------------------------------------------------

// TestScenarioR09_RPMWindowSlidesNotBans — a burst hits the per-minute
// request cap: the fourth call is refused with the HONEST retry-after (the
// time until the oldest event leaves the window — not the window's full
// length, a past inversion). A minute later the same burst pattern is
// welcome again: the sliding window is bookkeeping about the recent past,
// never a ban list.
func TestScenarioR09_RPMWindowSlidesNotBans(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.RequestsPerMinuteUser = 3
	idn.Limits.RequestsPerMinuteTenant = 100

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 10, 30))

	// Three quick calls fill the per-minute window.
	for i := 0; i < 3; i++ {
		if _, de := sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); de != nil {
			t.Fatalf("call %d must be admitted: %v", i, de)
		}
	}

	// The fourth is rate-limited and told when to come back.
	_, de := sc.env.proxy.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if de == nil || de.Code != "RATE_LIMITED" {
		t.Fatalf("the fourth call in a minute must be limited: %v", de)
	}
	if retry, ok := de.Details["retry_after_ms"]; !ok || retry == 0 {
		t.Fatalf("the refusal must carry a retry-after, got %v", de.Details)
	}

	// Time passes (the window slides — modeled by aging the recorded
	// events to one minute ago, exactly what ZREMRANGEBYSCORE prunes as
	// now()-60s advances past them).
	ctx := context.Background()
	key := "rl:rpm:user:" + idn.User.ID
	members := sc.rdb.ZRangeWithScores(ctx, key, 0, -1).Val()
	if len(members) != 3 {
		t.Fatalf("expected the 3 admitted events in the window, got %d", len(members))
	}
	for _, m := range members {
		sc.rdb.ZAdd(ctx, key, redis.Z{Score: m.Score - 61_000, Member: m.Member})
	}

	// The same burst pattern is welcome again.
	if _, de := sc.env.proxy.Stream(ctx, idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); de != nil {
		t.Fatalf("after the window slides the user must be re-admitted: %v", de)
	}
}
