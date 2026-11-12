// Package tests — quota_scenarios_test.go
//
// THE 13 USER SCENARIOS OF THE ROLLING QUOTA WINDOWS, end to end.
//
// What these tests exercise (production paths, real code):
//
//   - the rolling-window math itself: usage is NOT tracked anywhere, it is
//     DERIVED per admission from persisted llm_calls rows —
//     "sum(total_tokens) where recorded_at >= now()-5h" and "...now()-7d"
//     (repos usage.go WindowUsage), modelled here by windowedUsage with a
//     controllable clock;
//   - the admission-time reservation (quota.go): used(DB) + Σreserved(Redis,
//     live) + estimate(this call) ≤ quota, atomic in one Lua script;
//   - the reservation lifecycle: estimate → adjust-to-actual at call end →
//     release when the row is durably stored (QuotaPersisted);
//   - plan switching (free ⇄ pro): the ceiling moves, the history never does;
//   - degradation postures: Redis loss fails open to the PG truth, PG loss
//     skips the gate loudly;
//   - the normalization currency: same raw tokens burn the window at
//     model-specific rates, hot-changeable by the operator.
//
// Scenario index:
//
//	S01  fresh user, first-ever call              (no rows → sums are 0)
//	S02  normal usage, all limits fine            (rows accumulate; the usage
//	     bar and enforcement read the SAME cache — audit M7)
//	S03  5-hour burst window fills                (WINDOW_QUOTA_EXCEEDED with
//	     the exact drip-recovery instant; capacity re-opens the moment the
//	     oldest binge row ages out, not at a reset point)
//	S04  weekly window fills while 5h is fine     (sustained spend is bounded
//	     separately from bursts; recovery drips row by row)
//	S05  parallel fan-out burst                   (the atomic reservation
//	     stops N concurrent admissions from all spending the same budget —
//	     audit H2: the overshoot used to scale with the fan-out)
//	S05b quota rejections must not leak slots     (regression: the quota
//	     branch of admit() used to return without releaseHeld)
//	S06  user buys Pro while blocked              (ceiling rises, history is
//	     never deleted — the only llm_calls DELETE is the retention sweeper)
//	S07  downgrade to Free                        (existing usage is instantly
//	     re-evaluated against the smaller windows; downgrades have teeth)
//	S08  window fills WHILE a stream is running   (admission-time check only:
//	     a running stream is never killed mid-flight)
//	S09  client disconnects mid-stream            (cancelled row carries the
//	     PARTIAL usage already seen — the provider billed those tokens, so
//	     they count against the window)
//	S10  Redis down                               (quota gate fails OPEN to the
//	     plain PG check: under-quota passes, over-quota still blocks)
//	S11  PostgreSQL down at admission             (gate is skipped, call is
//	     admitted, nothing is lost — availability posture)
//	S12  model choice sets the burn rate          (luna 1×/5× vs sonnet
//	     20×/100× weights: identical raw work, 20× the window spend)
//	S13  operator changes weights mid-flight      (token_normalization UPDATE
//	     picked up within the RuleCache TTL, stale-serve-then-refresh; raw
//	     numbers stay reconstructible)
//
// Numbers are scaled down for arithmetic clarity; plan ratios follow the
// 000007 seed (free 60k/250k, pro 2M/10M) and model weights follow 0011.
package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/auth"
	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/openrouter"
	"github.com/mash-cloud/mash-api/internal/reqctx"
)

// ---- scenario fakes --------------------------------------------------------------

// windowEvent is one persisted llm_calls row's quota contribution: the
// (recorded_at, total_tokens) pair the window SQL sums over.
type windowEvent struct {
	at     time.Time
	tokens int64
	seq    int64 // stable tie-break: SQL orders by recorded_at ASC, id ASC
}

// windowedUsage mirrors the EXACT semantics of repos.LLMCallsRepo.WindowUsage
// and WindowRecovery — rolling-window sums over persisted rows — with a
// controllable "now()" so scenarios can time-travel past recovery instants
// without sleeping for hours.
type windowedUsage struct {
	mu     sync.Mutex
	now    time.Time
	seq    int64
	events map[string][]windowEvent
	fail   error // non-nil → WindowUsage/WindowRecovery fail (PG-down)
}

func newWindowedUsage() *windowedUsage {
	// Second-truncated base clock: resets_at is rendered RFC3339 (seconds),
	// so whole-hour offsets compare exactly against derr.Details["resets_at"].
	return &windowedUsage{now: time.Now().UTC().Truncate(time.Second), events: map[string][]windowEvent{}}
}

// addRow persists one row "now" (metering batcher lag is sub-second and
// irrelevant to window semantics).
func (w *windowedUsage) addRow(tid string, tokens int64) {
	w.addRowAt(tid, w.clock(), tokens)
}

// addRowAt persists one row at an arbitrary instant (scenario setup: the
// user's history before "now").
func (w *windowedUsage) addRowAt(tid string, at time.Time, tokens int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	w.events[tid] = append(w.events[tid], windowEvent{at: at.UTC(), tokens: tokens, seq: w.seq})
}

// advance moves the fake clock (the user waits; windows drip).
func (w *windowedUsage) advance(d time.Duration) {
	w.mu.Lock()
	w.now = w.now.Add(d)
	w.mu.Unlock()
}

func (w *windowedUsage) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *windowedUsage) failWith(err error) {
	w.mu.Lock()
	w.fail = err
	w.mu.Unlock()
}

func (w *windowedUsage) heal() {
	w.mu.Lock()
	w.fail = nil
	w.mu.Unlock()
}

func (w *windowedUsage) rows(tid string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events[tid])
}

// used5h reports the 5-hour position at the fake now (assertions).
func (w *windowedUsage) used5h(tid string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sumSince(tid, w.now.Add(-5*time.Hour))
}

func (w *windowedUsage) usedWeekly(tid string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sumSince(tid, w.now.Add(-7*24*time.Hour))
}

func (w *windowedUsage) sumSince(tid string, cutoff time.Time) int64 {
	var sum int64
	for _, e := range w.events[tid] {
		if !e.at.Before(cutoff) { // recorded_at >= cutoff (SQL is inclusive)
			sum += e.tokens
		}
	}
	return sum
}

// WindowUsage mirrors the production SQL one-for-one:
//
//	SELECT sum(total_tokens) FILTER (WHERE recorded_at >= now()-interval '5 hours'),
//	       sum(total_tokens) FILTER (WHERE recorded_at >= now()-interval '7 days')
//	FROM llm_calls WHERE tenant_id=$1 AND recorded_at >= now()-interval '7 days'
func (w *windowedUsage) WindowUsage(_ context.Context, tid string) (*domain.WindowUsage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail != nil {
		return nil, w.fail
	}
	u := &domain.WindowUsage{
		Used5h:     w.sumSince(tid, w.now.Add(-5*time.Hour)),
		UsedWeekly: w.sumSince(tid, w.now.Add(-7*24*time.Hour)),
	}
	return u, nil
}

// WindowRecovery mirrors the production SQL: the oldest-ordered prefix whose
// cumulative sum exceeds (used − quota) pins the recovery instant to that
// row's expiry (recorded_at + window). Capacity re-opens exactly then.
func (w *windowedUsage) WindowRecovery(_ context.Context, tid string, window time.Duration, quota, used int64) (*time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail != nil {
		return nil, w.fail
	}
	if used < quota {
		return nil, nil // precondition of the production query
	}
	rows := make([]windowEvent, 0, len(w.events[tid]))
	cutoff := w.now.Add(-window)
	for _, e := range w.events[tid] {
		if e.tokens > 0 && !e.at.Before(cutoff) {
			rows = append(rows, e)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].at.Equal(rows[j].at) {
			return rows[i].seq < rows[j].seq
		}
		return rows[i].at.Before(rows[j].at)
	})
	var cum int64
	for _, e := range rows {
		cum += e.tokens
		if cum > used-quota {
			t := e.at.Add(window)
			return &t, nil
		}
	}
	return nil, nil
}

// persistingMeter mirrors the OUTSIDE effects of the production metering
// pipeline (metering.Recorder): a completed/cancelled call's usage becomes a
// durable row (visible to WindowUsage), then the AfterPersist hook fires
// proxy.QuotaPersisted — releasing the call's reservation and invalidating
// the window cache so the next admission reads fresh numbers. Redis failures
// are ignored exactly as in production.
type persistingMeter struct {
	rec   *fakeRecorder
	win   *windowedUsage
	proxy *llm.Proxy
}

func (m *persistingMeter) Record(c domain.LLMCall) {
	m.rec.Record(c)
	if c.Usage.TotalTokens > 0 {
		m.win.addRow(c.TenantID, c.Usage.TotalTokens)
	}
	if m.proxy != nil {
		m.proxy.QuotaPersisted(c.TenantID, c.CallID, true)
	}
}

// mutableRules is a RuleSource the test (the "operator") can hot-change —
// the SQL-equivalent of UPDATE token_normalization ... with no redeploy.
type mutableRules struct {
	mu    sync.Mutex
	rules []domain.NormRule
}

func (m *mutableRules) All(context.Context) ([]domain.NormRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]domain.NormRule{}, m.rules...), nil
}

func (m *mutableRules) set(rules ...domain.NormRule) {
	m.mu.Lock()
	m.rules = rules
	m.mu.Unlock()
}

// ---- scenario environment ---------------------------------------------------------

type scenario struct {
	env *env
	win *windowedUsage
	pm  *persistingMeter
	rdb *redis.Client // direct miniredis client: reservation-ledger assertions
}

func newScenario(t *testing.T) *scenario {
	t.Helper()
	e := newEnv(t)
	win := newWindowedUsage()
	rdb := redis.NewClient(&redis.Options{Addr: e.mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	pm := &persistingMeter{rec: e.recorder, win: win}
	e.proxy.Usage = win
	e.proxy.Meter = pm
	pm.proxy = e.proxy
	// Short window-position cache: scenario phases mutate the fake between
	// admissions and rely on QuotaPersisted/manual invalidation for freshness.
	e.proxy.UsageCacheTTL = 20 * time.Millisecond
	return &scenario{env: e, win: win, pm: pm, rdb: rdb}
}

// flushUsageCache drops the tenant's cached window position — the exact
// invalidation the production QuotaPersisted hook performs when OTHER calls'
// rows land between two admissions of one test phase.
func (sc *scenario) flushUsageCache(tid string) {
	_ = sc.rdb.Del(context.Background(), "usage:window:"+tid).Err()
}

// reservationLedger reports (holders, sum) of the tenant's live token
// reservations — the Redis bridge between "call admitted" and "row durable".
func (sc *scenario) reservationLedger(tid string) (int64, int64) {
	ctx := context.Background()
	n, _ := sc.rdb.HLen(ctx, "usage:resv:amt:"+tid).Result()
	vals, _ := sc.rdb.HVals(ctx, "usage:resv:amt:"+tid).Result()
	var sum int64
	for _, v := range vals {
		var x int64
		if _, err := fmt.Sscanf(v, "%d", &x); err == nil {
			sum += x
		}
	}
	return n, sum
}

// freeIdentity / proIdentity: the SAME tenant (ten_t1) on two plans — plan
// switches move the ceiling, never the history. Ratios per the 000007 seed
// (free 60k/250k, pro 2M/10M), scaled for arithmetic clarity.
func freeIdentity() *auth.Identity {
	idn := testIdentity()
	idn.Plan = auth.PlanInfo{ID: "pln_free", Code: "free", Name: "Free"}
	idn.Limits = auth.Limits{
		RequestsPerMinuteUser:    20,
		RequestsPerMinuteTenant:  20,
		ConcurrentRequestsUser:   8,
		ConcurrentRequestsTenant: 8,
		Window5hTokens:           2000, // seed: 60_000
		WindowWeeklyTokens:       100_000,
	}
	return idn
}

func proIdentity() *auth.Identity {
	idn := testIdentity()
	idn.Plan = auth.PlanInfo{ID: "pln_pro", Code: "pro", Name: "Pro"}
	idn.Limits = auth.Limits{
		RequestsPerMinuteUser:    600,
		RequestsPerMinuteTenant:  6000,
		ConcurrentRequestsUser:   16,
		ConcurrentRequestsTenant: 256,
		Window5hTokens:           2_000_000,
		WindowWeeklyTokens:       10_000_000,
	}
	return idn
}

// reqBounded builds a stream request whose completion estimate is exact:
// prompt "hi" = 4 bytes / 4 = 1 token, plus maxOut → estimate = 1 + maxOut.
func reqBounded(model string, maxOut int64) *llm.Request {
	return &llm.Request{
		Model:               model,
		Stream:              true,
		MaxCompletionTokens: &maxOut,
		Messages:            []openrouter.Message{{Role: "user", Content: json.RawMessage(`"hi"`)}},
	}
}

func contentChunk(model, text string) scriptChunk {
	return chunk(fmt.Sprintf(`{"id":"c1","object":"chat.completion.chunk","model":%q,"choices":[{"index":0,"delta":{"content":%q}}]}`, model, text))
}

func usageChunk(model string, prompt, completion int64) scriptChunk {
	return scriptChunk{
		data: fmt.Sprintf(`{"id":"c1","object":"chat.completion.chunk","model":%q,"provider":"openai","choices":[{"index":0,"finish_reason":"stop"}],"usage":{"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d}}`,
			model, prompt, completion, prompt+completion),
		delay: 10 * time.Millisecond,
	}
}

func reqID(n int) context.Context {
	return reqctx.WithRequestID(context.Background(), fmt.Sprintf("req_scn_%d", n))
}

// ---- S01: fresh user --------------------------------------------------------------

// TestScenario01_FreshUserFirstEverCall — a brand-new tenant has NO llm_calls
// rows, so both window sums are 0 (no signup anchor, no "when did you join"
// — the windows are pure trailing intervals). The first call is admitted on
// an empty window, and once its metering row lands, the window position is
// exactly that call's normalized tokens.
func TestScenario01_FreshUserFirstEverCall(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000 // room for the estimate (101)

	if sc.win.rows("ten_t1") != 0 || sc.win.used5h("ten_t1") != 0 {
		t.Fatal("precondition: fresh tenant must have zero rows/usage")
	}

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))

	info, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr != nil {
		t.Fatalf("first-ever call must be admitted on an empty window: %v", derr)
	}
	// Position at admission: 0 used + 0 reserved − estimate 101.
	if info.Quota5hRemaining != 10_000-101 {
		t.Fatalf("5h remaining = %d, want %d", info.Quota5hRemaining, 10_000-101)
	}

	// The row landed; the window now reflects it (12 normalized tokens).
	if got := sc.win.used5h("ten_t1"); got != 12 {
		t.Fatalf("post-call used5h = %d, want 12", got)
	}
	if sc.win.rows("ten_t1") != 1 {
		t.Fatalf("rows = %d, want 1", sc.win.rows("ten_t1"))
	}
}

// ---- S02: normal usage ------------------------------------------------------------

// TestScenario02_NormalUsageAccumulatesMeteredRows — steady in-budget usage:
// every completed call appends its row; the next admission sees the fresh
// sum (QuotaPersisted invalidates the cache per row); and the usage bar the
// desktop renders (QuotaPosition — the SAME cache the gate reads, audit M7)
// never diverges from enforcement.
func TestScenario02_NormalUsageAccumulatesMeteredRows(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000
	idn.Limits.WindowWeeklyTokens = 100_000

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))

	var last *llm.RateInfo
	for i := 0; i < 3; i++ {
		info, derr := sc.env.proxy.Stream(reqID(i), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
		if derr != nil {
			t.Fatalf("call %d must be admitted: %v", i, derr)
		}
		last = info
	}
	// 3 rows × 12 tokens: the window is the SUM of persisted rows.
	if got := sc.win.used5h("ten_t1"); got != 36 {
		t.Fatalf("used5h = %d, want 36", got)
	}
	// The 3rd admission saw the first two rows: remaining = 10k − 24 − 101.
	if last.Quota5hRemaining != 10_000-24-101 {
		t.Fatalf("3rd call 5h remaining = %d, want %d", last.Quota5hRemaining, 10_000-24-101)
	}
	// The usage bar reads the exact same position enforcement used.
	pos, err := sc.env.proxy.QuotaPosition(context.Background(), "ten_t1", 10_000, 100_000)
	if err != nil || pos == nil {
		t.Fatalf("quota position: %v %v", pos, err)
	}
	if pos.Used5h != 36 || pos.UsedWeekly != 36 {
		t.Fatalf("usage-bar position = %+v, want 36/36", pos)
	}
	if n := len(sc.env.recorder.recorded()); n != 3 {
		t.Fatalf("metered rows = %d, want 3", n)
	}
}

// ---- S03: the 5-hour burst window --------------------------------------------------

// TestScenario03_FiveHourWindowFillsThenDripRecovers — the ChatGPT-style
// burst budget. History: a binge at T0−2h (700) and T0−1h (1400) puts the
// trailing 5h sum at 2100 ≥ quota 2000. The next admission is rejected with
// the EXACT recovery instant: the oldest row ages out at recorded_at+5h, so
// resets_at = T0+3h — not a reset point, a drip. One minute before it the
// window is still full; one minute after, capacity has re-opened.
func TestScenario03_FiveHourWindowFillsThenDripRecovers(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity() // 5h quota 2000
	T0 := sc.win.clock()
	sc.win.addRowAt("ten_t1", T0.Add(-2*time.Hour), 700)
	sc.win.addRowAt("ten_t1", T0.Add(-1*time.Hour), 1400)

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))
	sink := &captureSink{}
	_, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), sink)
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("want WINDOW_QUOTA_EXCEEDED, got %v", derr)
	}
	if derr.Details["window"] != "5h" || derr.Details["quota_tokens"] != int64(2000) || derr.Details["used_tokens"] != int64(2100) {
		t.Fatalf("rejection details: %+v", derr.Details)
	}
	// The recovery instant is the OLDEST row's expiry: (T0−2h) + 5h.
	if want := T0.Add(3 * time.Hour).Format(time.RFC3339); derr.Details["resets_at"] != want {
		t.Fatalf("resets_at = %v, want %v", derr.Details["resets_at"], want)
	}
	if sink.Started() {
		t.Fatal("quota rejection must happen before any byte is forwarded")
	}
	if n := len(sc.env.recorder.recorded()); n != 0 {
		t.Fatalf("blocked calls must not be metered, got %d rows", n)
	}

	// One minute BEFORE recovery: the binge row is still inside the window.
	sc.win.advance(2*time.Hour + 59*time.Minute)
	if _, derr := sc.env.proxy.Stream(reqID(2), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); derr == nil {
		t.Fatal("window must still be full one minute before the recovery instant")
	}

	// Past the recovery instant: the oldest row left the window (700 aged
	// out), leaving 1400 — the call fits (1400 + 101 ≤ 2000).
	sc.win.advance(2 * time.Minute)
	if _, derr := sc.env.proxy.Stream(reqID(3), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); derr != nil {
		t.Fatalf("capacity must have dripped back at resets_at: %v", derr)
	}
}

// ---- S04: the weekly window ---------------------------------------------------------

// TestScenario04_WeeklyWindowBindsIndependentlyOf5h — sustained spend is
// bounded separately from bursts. History: 300 at T0−3d, 4800 at T0−2d,
// 200 at T0−1h → 5h sum is only 200 (fine) but the weekly sum is 5300 over
// the 5000 quota. The rejection names "weekly". Recovery drips row by row:
// the 300-row leaving at T0+4d still leaves exactly 5000 (still blocked);
// only when the 4800-row leaves at T0+5d does usage drop to 200.
func TestScenario04_WeeklyWindowBindsIndependentlyOf5h(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 100_000 // 5h is deliberately fine
	idn.Limits.WindowWeeklyTokens = 5000
	T0 := sc.win.clock()
	sc.win.addRowAt("ten_t1", T0.Add(-72*time.Hour), 300)
	sc.win.addRowAt("ten_t1", T0.Add(-48*time.Hour), 4800)
	sc.win.addRowAt("ten_t1", T0.Add(-1*time.Hour), 200)

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))
	_, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("want WINDOW_QUOTA_EXCEEDED, got %v", derr)
	}
	if derr.Details["window"] != "weekly" {
		t.Fatalf("binding window = %v, want weekly (5h had room)", derr.Details["window"])
	}
	if derr.Details["used_tokens"] != int64(5300) || derr.Details["quota_tokens"] != int64(5000) {
		t.Fatalf("details: %+v", derr.Details)
	}
	// Recovery: overage is 300; the first row alone (cum 300) does NOT
	// exceed it, so recovery waits for the SECOND row's expiry: T0−2d + 7d.
	if want := T0.Add(5 * 24 * time.Hour).Format(time.RFC3339); derr.Details["resets_at"] != want {
		t.Fatalf("resets_at = %v, want %v", derr.Details["resets_at"], want)
	}

	// 4 hours pass: the 5h event (200) left the 5-hour window entirely, the
	// weekly sum is untouched → still blocked, and still by "weekly".
	sc.win.advance(4 * time.Hour)
	_, derr = sc.env.proxy.Stream(reqID(2), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr == nil || derr.Details["window"] != "weekly" {
		t.Fatalf("weekly must still bind after 5h usage drained: %v", derr)
	}

	// Drip step 1 (T0+4d+1m): the 300-row left the weekly window → exactly
	// 5000 remain → still blocked (5000 + 101 > 5000). The extra minute
	// matters: the window bound is INCLUSIVE (recorded_at >= now()-7d), so
	// at exactly T0+4d the row is still inside.
	sc.win.advance(4*24*time.Hour - 4*time.Hour + time.Minute)
	_, derr = sc.env.proxy.Stream(reqID(3), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr == nil {
		t.Fatal("at exactly-quota weekly usage the window must still bind")
	}

	// Drip step 2 (T0+5d+1m): the 4800-row left → 200 remain → admitted.
	sc.win.advance(24 * time.Hour)
	if _, derr := sc.env.proxy.Stream(reqID(4), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); derr != nil {
		t.Fatalf("weekly window must have dripped back open: %v", derr)
	}
}

// ---- S05: parallel fan-out ----------------------------------------------------------

// TestScenario05_ParallelBurstCannotDoubleSpendBudget — audit H2, the race
// that motivated the reservation ledger. A desktop agent fans out 20
// concurrent calls, each estimating 2001 tokens, against a 10_000 budget.
// Without the atomic reservation, every admission read the same stale
// position and they ALL passed (overshoot scaled with the fan-out: 40 calls
// spent 420k of a 100k budget). With it, exactly floor(10000/2001) = 4 are
// admitted, 16 are rejected, and the Redis ledger holds 4 live reservations
// while they stream — released only when their rows are durable.
func TestScenario05_ParallelBurstCannotDoubleSpendBudget(t *testing.T) {
	sc := newScenario(t)
	idn := testIdentity()
	idn.Limits.Window5hTokens = 10_000
	idn.Limits.WindowWeeklyTokens = 1_000_000
	idn.Limits.ConcurrentRequestsUser = 100 // keep concurrency out of the picture

	// Room for the gated hold: the idle watchdog and the total-duration cap
	// must not cut streams that are parked in SendChunk while the burst
	// settles (defaults in newEnv are 2s/5s).
	sc.env.proxy.Timing.IdleTimeout = 30 * time.Second
	sc.env.proxy.Timing.MaxDuration = 60 * time.Second

	// Admitted streams are GATED (not time-delayed): none can complete —
	// and therefore none can release its reservation — until the test closes
	// the gate. A time delay would race a late-scheduled admission goroutine
	// under CPU contention (a completion would re-open budget for it).
	gate := make(chan struct{})
	sc.env.fb.setScript(
		contentChunk("openai/gpt-6-luna", "hi"),
		usageChunk("openai/gpt-6-luna", 8, 4),
	)

	const n = 20
	var wg sync.WaitGroup
	var returned, admittedTotal atomic.Int32
	results := make([]*domain.Error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = sc.env.proxy.Stream(reqID(i), idn, reqBounded("openai/gpt-6-luna", 2000), &gatedSink{gate: gate, admitted: &admittedTotal})
			returned.Add(1)
		}(i)
	}

	// Wait until every goroutine is accounted for: 4 admitted (parked in
	// their sink) + 16 returned. While the gate is closed the 4 held
	// reservations make any further admission mathematically impossible
	// (0 + 4×2001 + 2001 > 10 000), so this state is stable — no straggler
	// can still be pre-admission once 20 goroutines are accounted for.
	waitFor(t, "all 20 admissions resolved (4 admitted + 16 rejected)", 5*time.Second, func() bool {
		return admittedTotal.Load() == 4 && returned.Load() == n-4
	})
	if got := sc.env.fb.requestCount(); got != 4 {
		t.Fatalf("upstream requests = %d, want exactly 4", got)
	}
	holders, sum := sc.reservationLedger("ten_t1")
	if holders != 4 {
		t.Fatalf("live reservations = %d, want 4 (the atomic budget split)", holders)
	}
	if sum != 4*2001 {
		t.Fatalf("reserved tokens = %d, want %d (4 × estimate 2001)", sum, 4*2001)
	}

	close(gate) // let the 4 admitted streams run to completion
	wg.Wait()

	admitted, rejected := 0, 0
	for _, de := range results {
		switch {
		case de == nil:
			admitted++
		case de.Code == "WINDOW_QUOTA_EXCEEDED":
			rejected++
		default:
			t.Fatalf("unexpected rejection code in the burst: %v", de)
		}
	}
	if admitted != 4 || rejected != 16 {
		t.Fatalf("admitted=%d rejected=%d, want 4/16 — the budget is atomic", admitted, rejected)
	}

	// After durable persist, every reservation was released: the ledger is
	// the bridge between "admitted" and "row landed", and it drains.
	holders, _ = sc.reservationLedger("ten_t1")
	if holders != 0 {
		t.Fatalf("post-persist reservations = %d, want 0 (QuotaPersisted released all)", holders)
	}
	// The 4 metered rows carry the actual consumption.
	if got := sc.win.used5h("ten_t1"); got != 4*12 {
		t.Fatalf("post-burst used5h = %d, want 48", got)
	}
}

// gatedSink holds its stream open (first chunk blocks) until the shared
// gate closes — the deterministic replacement for time-delayed scripts in
// concurrency-sensitive scenarios. The first chunk also marks the stream as
// admitted in the shared counter (rejected calls never reach a sink).
type gatedSink struct {
	gate     <-chan struct{}
	admitted *atomic.Int32
	mu       sync.Mutex
	n        int
}

func (s *gatedSink) SendChunk(raw []byte) error {
	s.mu.Lock()
	s.n++
	first := s.n == 1
	s.mu.Unlock()
	if first {
		s.admitted.Add(1)
	}
	<-s.gate
	return nil
}

func (s *gatedSink) SendError(errType, code, message string) error { return nil }

func (s *gatedSink) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n > 0
}

// TestScenario05b_QuotaRejectionDoesNotLeakConcurrencySlots — regression for
// the slot leak the scenario suite found in admit(): the quota branch used to
// return WITHOUT releaseHeld, so every WINDOW_QUOTA_EXCEEDED leaked one user
// + one tenant slot holder until SlotTTL (30 minutes in production). A
// quota-blocked agent retrying in a loop would exhaust its concurrency
// dimension and receive CONCURRENCY_LIMIT — the wrong error — for half an
// hour. All rejections here must stay quota errors.
func TestScenario05b_QuotaRejectionDoesNotLeakConcurrencySlots(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.ConcurrentRequestsUser = 3
	idn.Limits.ConcurrentRequestsTenant = 3
	// History already far over the 2000 5h budget: every attempt is a quota
	// rejection, none ever reaches the stream loop.
	T0 := sc.win.clock()
	sc.win.addRowAt("ten_t1", T0.Add(-30*time.Minute), 5000)

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))
	for i := 0; i < 8; i++ { // 8 rejections against a 3-slot dimension
		_, derr := sc.env.proxy.Stream(reqID(i), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
		if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
			t.Fatalf("attempt %d: want WINDOW_QUOTA_EXCEEDED (slots must not leak), got %v", i, derr)
		}
	}
}

// ---- S06: upgrade while blocked -----------------------------------------------------

// TestScenario06_UpgradeToProRaisesCeilingKeepsHistory — the user is blocked
// on the Free 5h window, pays, and the webhook activates Pro. NOTHING is
// deleted (the only llm_calls DELETE in the codebase is the retention
// sweeper): the same 1950-token history is instantly measured against the
// 2M Pro ceiling and the very next call goes through.
func TestScenario06_UpgradeToProRaisesCeilingKeepsHistory(t *testing.T) {
	sc := newScenario(t)
	free, pro := freeIdentity(), proIdentity() // same tenant ten_t1
	T0 := sc.win.clock()
	sc.win.addRowAt("ten_t1", T0.Add(-1*time.Hour), 1950) // 1950/2000 used

	sc.env.fb.setScript(contentChunk("anthropic/claude-sonnet-5.5", "hi"), usageChunk("anthropic/claude-sonnet-5.5", 8, 4))

	// Blocked as Free: 1950 + estimate 101 > 2000.
	_, derr := sc.env.proxy.Stream(reqID(1), free, reqBounded("anthropic/claude-sonnet-5.5", 100), &captureSink{})
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" || derr.Details["window"] != "5h" {
		t.Fatalf("free plan must be blocked: %v", derr)
	}

	// The payment lands; the identity cache expires; Pro applies to the SAME
	// history: 1950 + 101 ≤ 2_000_000 → admitted, first try.
	info, derr := sc.env.proxy.Stream(reqID(2), pro, reqBounded("anthropic/claude-sonnet-5.5", 100), &captureSink{})
	if derr != nil {
		t.Fatalf("pro plan must be admitted on the same history: %v", derr)
	}
	if info.Quota5hRemaining != 2_000_000-1950-101 {
		t.Fatalf("pro 5h remaining = %d, want %d", info.Quota5hRemaining, 2_000_000-1950-101)
	}

	// History preserved: the old binge row AND the new call's row.
	if n := sc.win.rows("ten_t1"); n != 2 {
		t.Fatalf("rows after upgrade = %d, want 2 (history kept + new row)", n)
	}
	if got := sc.win.used5h("ten_t1"); got != 1950+12 {
		t.Fatalf("used5h = %d, want 1962 (old usage + new row)", got)
	}
	// The blocked attempt was never metered; the Pro call was.
	if n := len(sc.env.recorder.recorded()); n != 1 {
		t.Fatalf("metered rows = %d, want 1", n)
	}
}

// ---- S07: downgrade ------------------------------------------------------------------

// TestScenario07_DowngradeReevaluatesHistoryAgainstFreeLimits — the reverse
// of S06, and it has teeth: 300.5k tokens of weekly history are nothing on
// Pro (10M) but instantly over the Free weekly window (250k here 100k
// scaled). The 5h window has plenty of room — the WEEKLY window is the one
// that binds, exactly as the error says.
func TestScenario07_DowngradeReevaluatesHistoryAgainstFreeLimits(t *testing.T) {
	sc := newScenario(t)
	free, pro := freeIdentity(), proIdentity() // free weekly = 100_000
	T0 := sc.win.clock()
	sc.win.addRowAt("ten_t1", T0.Add(-48*time.Hour), 150_000)
	sc.win.addRowAt("ten_t1", T0.Add(-24*time.Hour), 150_000)
	sc.win.addRowAt("ten_t1", T0.Add(-30*time.Minute), 500) // recent: 5h sum is small

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))

	// On Pro the same history is 3% of the weekly budget: fine.
	if _, derr := sc.env.proxy.Stream(reqID(1), pro, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); derr != nil {
		t.Fatalf("pro must be admitted: %v", derr)
	}

	// Subscription lapses → free limits apply to the SAME rows. The cached
	// position (under Pro quota) is bypassed: the 95%-of-free-quota guard
	// forces a fresh database read.
	_, derr := sc.env.proxy.Stream(reqID(2), free, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("free plan must be blocked on the same history: %v", derr)
	}
	if derr.Details["window"] != "weekly" {
		t.Fatalf("binding window = %v, want weekly (5h had room: 500 + 101 ≤ 2000)", derr.Details["window"])
	}
	if derr.Details["used_tokens"] != int64(300_512) {
		t.Fatalf("weekly used = %v, want 300512 (300500 history + the Pro call's 12-token row)", derr.Details["used_tokens"])
	}
}

// ---- S08: window fills mid-flight -----------------------------------------------------

// TestScenario08_RunningStreamSurvivesWindowFilling — the quota check is
// ADMISSION-TIME only. A stream already in flight when the tenant's window
// fills (other users' rows landing) runs to completion untouched; only the
// NEXT admission is rejected. The in-flight call's reservation is what kept
// the budget honest while it streamed.
func TestScenario08_RunningStreamSurvivesWindowFilling(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 3000

	sc.env.fb.setScript(
		contentChunk("openai/gpt-6-luna", "hi"),
		scriptChunk{data: fmt.Sprintf(`{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","provider":"openai","choices":[{"index":0,"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":20,"total_tokens":50}}`), delay: 800 * time.Millisecond},
	)

	done := make(chan *domain.Error, 1)
	go func() {
		_, de := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
		done <- de
	}()
	waitFor(t, "stream in flight", 3*time.Second, func() bool {
		return sc.env.proxy.ActiveCount() == 1
	})

	// Other calls in the tenant land 2950 tokens while our stream runs —
	// their persist hook would invalidate the window cache; model that here.
	sc.win.addRowAt("ten_t1", sc.win.clock(), 2950)
	sc.flushUsageCache("ten_t1")

	// Next admission: 2950 used + 101 live reservation + 101 estimate > 3000.
	_, derr := sc.env.proxy.Stream(reqID(2), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("post-fill admission must be rejected: %v", derr)
	}

	// The running stream was never touched: it completes cleanly and its
	// 50 tokens land on top of the 2950.
	select {
	case de := <-done:
		if de != nil {
			t.Fatalf("in-flight stream must survive the window filling: %v", de)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight stream did not complete")
	}
	calls := sc.env.recorder.recorded()
	if len(calls) != 1 || calls[0].Status != domain.CallCompleted || calls[0].Usage.TotalTokens != 50 {
		t.Fatalf("in-flight stream metering: %+v", calls)
	}
	if got := sc.win.used5h("ten_t1"); got != 3000 {
		t.Fatalf("used5h = %d, want 3000 (2950 other + 50 this)", got)
	}
}

// ---- S09: client disconnect -----------------------------------------------------------

// TestScenario09_ClientDisconnectMetersPartialUsage — the user closes the
// laptop mid-stream. The provider already billed the tokens that streamed,
// so the cancelled row carries the PARTIAL usage seen so far (700) and it
// COUNTS against the window: the next admission is evaluated against it.
func TestScenario09_ClientDisconnectMetersPartialUsage(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 750 // 700 partial + 101 estimate > 750 → next call blocked

	// Chunk 1: content. Chunk 2 (after 150ms): usage 700. Chunk 3: never
	// arrives for this client — it disconnects at 300ms.
	sc.env.fb.setScript(
		contentChunk("openai/gpt-6-luna", "hi"),
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[],"usage":{"prompt_tokens":500,"completion_tokens":200,"total_tokens":700}}`, delay: 150 * time.Millisecond},
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"finish_reason":"stop"}]}`, delay: 3 * time.Second},
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel() // the desktop app goes away
	}()

	sink := &captureSink{}
	_, derr := sc.env.proxy.Stream(reqctx.WithRequestID(ctx, "req_scn_disc"), idn, reqBounded("openai/gpt-6-luna", 100), sink)
	if derr != nil {
		t.Fatalf("client disconnect is not an upstream error: %v", derr)
	}

	calls := sc.env.recorder.recorded()
	if len(calls) != 1 {
		t.Fatalf("metered = %d, want 1", len(calls))
	}
	if calls[0].Status != domain.CallCancelled {
		t.Fatalf("status = %s, want cancelled", calls[0].Status)
	}
	if calls[0].Usage.TotalTokens != 700 {
		t.Fatalf("partial usage = %d, want 700 (tokens the provider billed)", calls[0].Usage.TotalTokens)
	}
	if got := sc.win.used5h("ten_t1"); got != 700 {
		t.Fatalf("window after disconnect = %d, want 700", got)
	}

	// The partial usage binds: the next admission is over the 750 budget.
	sc.flushUsageCache("ten_t1")
	if _, derr := sc.env.proxy.Stream(reqID(2), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{}); derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("partial usage must count against the window: %v", derr)
	}
}

// ---- S10: Redis down --------------------------------------------------------------------

// TestScenario10_RedisLossFailsOpenToDatabaseTruth — the quota gate's
// degraded posture. With Redis gone the reservation cannot be held, so the
// gate falls back to the plain used-vs-quota check straight from the
// (authoritative) row sums: under quota still passes, over quota still
// blocks. Availability over strictness — the quota is a plan-abuse bound,
// not a security boundary.
func TestScenario10_RedisLossFailsOpenToDatabaseTruth(t *testing.T) {
	t.Run("under quota still admitted", func(t *testing.T) {
		sc := newScenario(t)
		idn := freeIdentity() // 5h quota 2000
		T0 := sc.win.clock()
		sc.win.addRowAt("ten_t1", T0.Add(-1*time.Hour), 1900) // under

		sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))
		sc.env.mr.Close() // Redis is gone

		info, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
		if derr != nil {
			t.Fatalf("under-quota call must survive Redis loss: %v", derr)
		}
		if info.Quota5hRemaining != -1 {
			t.Fatalf("5h remaining = %d, want -1 (position unknown without the ledger)", info.Quota5hRemaining)
		}
	})

	t.Run("over quota still blocks from the row sums", func(t *testing.T) {
		sc := newScenario(t)
		idn := freeIdentity()
		T0 := sc.win.clock()
		sc.win.addRowAt("ten_t1", T0.Add(-1*time.Hour), 2100) // over

		sc.env.mr.Close()

		_, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
		if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
			t.Fatalf("over-quota must still block on the PG truth: %v", derr)
		}
		if derr.Details["window"] != "5h" || derr.Details["used_tokens"] != int64(2100) {
			t.Fatalf("details: %+v", derr.Details)
		}
	})
}

// ---- S11: PostgreSQL down at admission ----------------------------------------------------

// TestScenario11_DatabaseFailureAtAdmissionSkipsQuotaGate — the window query
// fails. The gate is skipped for that request (the fail-open posture; the
// quota.gate_fails counter makes it loud), the call is admitted, and nothing
// is lost: once the database answers again, the accumulated rows are fully
// visible to the next admission.
func TestScenario11_DatabaseFailureAtAdmissionSkipsQuotaGate(t *testing.T) {
	sc := newScenario(t)
	idn := freeIdentity()
	idn.Limits.Window5hTokens = 10_000

	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 8, 4))

	// The database refuses the window query mid-incident.
	sc.win.failWith(errors.New("postgres: connection refused during admission"))
	info, derr := sc.env.proxy.Stream(reqID(1), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr != nil {
		t.Fatalf("admission must survive the window query failing: %v", derr)
	}
	// The gate never ran: the position is unknown — remaining stays at its
	// zero default, which the transport renders as an OMITTED header (the
	// -1 marker belongs to the Redis-down path, where the position was read
	// but the ledger was not).
	if info.Quota5hRemaining != 0 {
		t.Fatalf("5h remaining = %d, want 0 (gate skipped, header omitted)", info.Quota5hRemaining)
	}

	// The database recovers: the call's row is now visible to the gate.
	sc.win.heal()
	sc.flushUsageCache("ten_t1")
	info2, derr := sc.env.proxy.Stream(reqID(2), idn, reqBounded("openai/gpt-6-luna", 100), &captureSink{})
	if derr != nil {
		t.Fatalf("post-recovery admission: %v", derr)
	}
	if info2.Quota5hRemaining != 10_000-12-101 {
		t.Fatalf("post-recovery 5h remaining = %d, want %d (the S11 row counts)", info2.Quota5hRemaining, 10_000-12-101)
	}
	if got := sc.win.used5h("ten_t1"); got != 24 {
		t.Fatalf("used5h = %d, want 24 (both rows)", got)
	}
}

// ---- S12: model choice sets the burn rate --------------------------------------------------

// TestScenario12_ModelChoiceSetsWindowBurnRate — the quota currency is
// NORMALIZED tokens, and the weights are price-proportional (migration
// 0011): luna 1×in/5×out, sonnet 20×in/100×out. Two tenants run IDENTICAL
// raw work (1000 prompt + 100 completion); the sonnet tenant's window burns
// 20× faster. Premium models do not cost more budget per plan — they consume
// the same budget faster, which is the same statement said honestly.
// (The admission ESTIMATE stays weight-agnostic by design; the burn
// difference lands in the metered rows.)
func TestScenario12_ModelChoiceSetsWindowBurnRate(t *testing.T) {
	sc := newScenario(t)
	sc.env.proxy.Norm = llm.NewRuleCache(&mutableRules{rules: []domain.NormRule{
		{Model: "openai/gpt-6-luna", InputWeight: 1, CachedReadWeight: 0.10, CachedWriteWeight: 1.25, OutputWeight: 5},
		{Model: "anthropic/claude-sonnet-5.5", InputWeight: 20, CachedReadWeight: 2, CachedWriteWeight: 25, OutputWeight: 100},
	}}, time.Hour)

	idnLuna := testIdentity() // tenant ten_t1
	idnLuna.Limits.Window5hTokens = 1_000_000
	idnSonnet := testIdentity()
	idnSonnet.Tenant.ID = "ten_sonnet"
	idnSonnet.Limits.Window5hTokens = 1_000_000

	// IDENTICAL raw usage on both: 1000 prompt, 100 completion.
	sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 1000, 100))
	if _, derr := sc.env.proxy.Stream(reqID(1), idnLuna, reqBounded("openai/gpt-6-luna", 200), &captureSink{}); derr != nil {
		t.Fatalf("luna call: %v", derr)
	}
	sc.env.fb.setScript(contentChunk("anthropic/claude-sonnet-5.5", "hi"), usageChunk("anthropic/claude-sonnet-5.5", 1000, 100))
	if _, derr := sc.env.proxy.Stream(reqID(2), idnSonnet, reqBounded("anthropic/claude-sonnet-5.5", 200), &captureSink{}); derr != nil {
		t.Fatalf("sonnet call: %v", derr)
	}

	luna, sonnet := sc.win.used5h("ten_t1"), sc.win.used5h("ten_sonnet")
	// luna:   in 1000×1 + out 100×5  = 1500 normalized
	// sonnet: in 1000×20 + out 100×100 = 30000 normalized
	if luna != 1500 {
		t.Fatalf("luna window burn = %d, want 1500", luna)
	}
	if sonnet != 30000 {
		t.Fatalf("sonnet window burn = %d, want 30000", sonnet)
	}
	if sonnet != 20*luna {
		t.Fatalf("burn ratio = %d/%d, want 20×", sonnet, luna)
	}
	// The raw provider numbers are identical and preserved on both rows —
	// the burn difference is purely the accounting currency.
	for _, c := range sc.env.recorder.recorded() {
		if c.Usage.RawPromptTokens != 1000 || c.Usage.RawCompletionTokens != 100 {
			t.Fatalf("raw usage must be preserved: %+v", c.Usage)
		}
	}
}

// ---- S13: operator changes the weights --------------------------------------------------------

// TestScenario13_OperatorWeightChangePickedUpWithinCacheTTL — the operator
// runs UPDATE token_normalization (no restart, no redeploy). The RuleCache
// serves the stale rule for at most one TTL window, refreshes in the
// background, and the next call is metered with the new weights. History
// stays reconstructible because every row keeps its raw numbers.
func TestScenario13_OperatorWeightChangePickedUpWithinCacheTTL(t *testing.T) {
	sc := newScenario(t)
	rules := &mutableRules{rules: []domain.NormRule{
		{Model: "openai/gpt-6-luna", InputWeight: 1, CachedReadWeight: 0.10, CachedWriteWeight: 1.25, OutputWeight: 5},
	}}
	sc.env.proxy.Norm = llm.NewRuleCache(rules, 150*time.Millisecond)
	idn := testIdentity()
	idn.Limits.Window5hTokens = 1_000_000

	run := func(i int) int64 {
		sc.env.fb.setScript(contentChunk("openai/gpt-6-luna", "hi"), usageChunk("openai/gpt-6-luna", 100, 100))
		if _, derr := sc.env.proxy.Stream(reqID(i), idn, reqBounded("openai/gpt-6-luna", 200), &captureSink{}); derr != nil {
			t.Fatalf("call %d: %v", i, derr)
		}
		return sc.env.recorder.recorded()[len(sc.env.recorder.recorded())-1].Usage.TotalTokens
	}

	// Before: out weight 5 → 100 + 500 = 600.
	if got := run(1); got != 600 {
		t.Fatalf("pre-change total = %d, want 600", got)
	}

	// The operator doubles the output weight (SQL UPDATE equivalent).
	rules.set(domain.NormRule{Model: "openai/gpt-6-luna", InputWeight: 1, CachedReadWeight: 0.10, CachedWriteWeight: 1.25, OutputWeight: 10})

	// Past the TTL: this call may still be served the stale rule (the cache
	// refreshes in the background, never blocking the hot path).
	time.Sleep(250 * time.Millisecond)
	stale := run(2)

	// Refresh has completed: the next call meters with the NEW weight.
	time.Sleep(300 * time.Millisecond)
	fresh := run(3)

	if stale != 600 && stale != 1100 {
		t.Fatalf("transition-window total = %d, want 600 (stale) or 1100 (fresh)", stale)
	}
	if fresh != 1100 {
		t.Fatalf("post-change total = %d, want 1100 (100 + 100×10)", fresh)
	}
	// Raw numbers unchanged across the weight change: history reconstructible.
	for i, c := range sc.env.recorder.recorded() {
		if c.Usage.RawPromptTokens != 100 || c.Usage.RawCompletionTokens != 100 {
			t.Fatalf("row %d raw usage drifted: %+v", i, c.Usage)
		}
	}
}
