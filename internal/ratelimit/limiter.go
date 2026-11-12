// Package ratelimit implements distributed rate limiting on Redis. All limits
// are enforced with atomic Lua scripts so that N API instances behind one load
// balancer share a single, consistent view. The limiter applies to logical
// requests (run creation, auth calls), never to individual stream frames.
//
// Posture: fail-open by configuration. When Redis is unreachable, requests are
// allowed (availability over strictness) and a fail-open counter is incremented
// so the incident is visible. Authorization never fails open — that is a
// PostgreSQL concern, fail-closed upstream.
package ratelimit

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/observability"
	"github.com/mash-cloud/mash-api/internal/store"
)

// Verdict is the outcome of a limit check.
type Verdict struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration // >0 when denied (sliding-window: full window minus head age)
}

// Limiter is safe for concurrent use.
type Limiter struct {
	rdb      redis.UniversalClient
	failOpen bool
	metrics  *observability.Metrics
}

func New(rdb redis.UniversalClient, failOpen bool, metrics *observability.Metrics) *Limiter {
	return &Limiter{rdb: rdb, failOpen: failOpen, metrics: metrics}
}

// --- sliding window ---------------------------------------------------------

// slidingWindowLua admits one request iff the number of events within the
// trailing window is below the limit. ZSET member is the unique request id.
// KEYS[1]=zset  ARGV: now_ms, window_ms, limit, member
var slidingWindowLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
local count = redis.call('ZCARD', KEYS[1])
if count < tonumber(ARGV[3]) then
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
        redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) + 1000)
        return {1, count + 1}
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry = tonumber(ARGV[2])
if oldest[2] then
        -- Remaining time until the oldest (head) entry leaves the window:
        --   window - (now - oldest)
        -- NOT now - oldest (that is the entry's AGE — the exact inversion
        -- that told throttled clients to wait ~the full window when 1s
        -- remained and to retry immediately when ~the whole window
        -- remained; audit finding 3).
        local age = tonumber(ARGV[1]) - tonumber(oldest[2])
        retry = tonumber(ARGV[2]) - age + 1
        if retry < 1 then retry = 1 end
        -- Clock-skew guard (multi-instance now_ms): never advertise more
        -- than the window itself.
        if retry > tonumber(ARGV[2]) then retry = tonumber(ARGV[2]) end
end
return {0, count, retry}
`)

// Allow checks one scope. key must already be namespaced by the caller.
func (l *Limiter) Allow(ctx context.Context, key string, limit int64, window time.Duration, member string) Verdict {
	if limit <= 0 {
		return Verdict{Allowed: true} // dimension disabled
	}
	now := time.Now()
	res, err := slidingWindowLua.Run(ctx, l.rdb, []string{key},
		now.UnixMilli(), window.Milliseconds(), limit, member).Result()
	if err != nil {
		return l.degraded()
	}
	return parseVerdict(res)
}

// AllowAll checks multiple scopes in ONE Redis round trip (pipelined scripts).
// The strictest denial wins. Each scope is independent (user, tenant, ip…).
type Scope struct {
	Key    string
	Limit  int64
	Window time.Duration
}

func (l *Limiter) AllowAll(ctx context.Context, member string, scopes ...Scope) Verdict {
	verdict, needLoad := l.allowAll(ctx, member, scopes)
	if needLoad {
		// Pipelined EVALSHA cannot fall back to EVAL (errors surface only
		// after Exec), so the script is loaded explicitly and the batch retried.
		if err := slidingWindowLua.Load(ctx, l.rdb).Err(); err == nil {
			verdict, _ = l.allowAll(ctx, member, scopes)
		}
	}
	return verdict
}

// AllowScopes runs the same pipelined admission as AllowAll but also returns
// the PER-SCOPE verdicts (aligned with the returned active scope list) so
// callers can report each dimension independently — e.g. the X-Ratelimit-*
// response headers need the user scope's own limit/remaining, not a merged
// minimum. Admission semantics are identical to AllowAll (same script, same
// single round trip, same degradation policy): use Merged for the decision
// and Verdicts/Active only for reporting.
type AllowScopesResult struct {
	Verdicts []Verdict // per-scope outcomes, aligned with Active
	Active   []Scope   // the scopes actually checked (limit > 0, key set)
	Merged   Verdict   // the admission decision — identical to AllowAll's return
}

func (l *Limiter) AllowScopes(ctx context.Context, member string, scopes ...Scope) AllowScopesResult {
	res := l.allowScopesDetail(ctx, member, scopes)
	if res.needLoad {
		// Pipelined EVALSHA cannot fall back to EVAL (errors surface only
		// after Exec), so the script is loaded explicitly and the batch retried.
		if err := slidingWindowLua.Load(ctx, l.rdb).Err(); err == nil {
			res = l.allowScopesDetail(ctx, member, scopes)
		}
	}
	if res.down {
		return AllowScopesResult{Merged: l.degraded()}
	}
	return AllowScopesResult{Verdicts: res.verdicts, Active: res.active, Merged: mergeVerdicts(res.verdicts, res.active)}
}

// allowAll merges per-scope verdicts into the strictest single verdict,
// preserving the degradation policy on Redis loss.
func (l *Limiter) allowAll(ctx context.Context, member string, scopes []Scope) (Verdict, bool) {
	r := l.allowScopesDetail(ctx, member, scopes)
	if r.needLoad {
		return Verdict{Allowed: true}, true
	}
	if r.down {
		return l.degraded(), false
	}
	return mergeVerdicts(r.verdicts, r.active), false
}

func mergeVerdicts(verdicts []Verdict, active []Scope) Verdict {
	verdict := Verdict{Allowed: true}
	for i, v := range verdicts {
		if !v.Allowed {
			if i < len(active) {
				v.Limit = active[i].Limit
			}
			return v
		}
		if v.Remaining < verdict.Remaining || i == 0 {
			verdict.Remaining = v.Remaining
		}
	}
	return verdict
}

type scopesRun struct {
	verdicts []Verdict
	active   []Scope
	needLoad bool
	down     bool
}

// allowScopesDetail pipelines one slidingWindowLua call per active scope and
// keeps the per-scope results.
func (l *Limiter) allowScopesDetail(ctx context.Context, member string, scopes []Scope) scopesRun {
	active := make([]Scope, 0, len(scopes))
	for _, s := range scopes {
		if s.Limit > 0 && s.Key != "" {
			active = append(active, s)
		}
	}
	if len(active) == 0 {
		return scopesRun{active: active}
	}

	now := time.Now().UnixMilli()
	pipe := l.rdb.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(active))
	for _, s := range active {
		cmds = append(cmds, slidingWindowLua.Run(ctx, pipe, []string{s.Key},
			now, s.Window.Milliseconds(), s.Limit, member))
	}
	if _, err := pipe.Exec(ctx); err != nil && !isScriptCmdErr(cmds) {
		return scopesRun{active: active, down: true}
	}

	verdicts := make([]Verdict, 0, len(active))
	needLoad := false
	for i, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil {
			if isNoScriptErr(err) {
				needLoad = true
				verdicts = append(verdicts, Verdict{Allowed: true})
				continue
			}
			if isRedisDownErr(err) {
				return scopesRun{active: active, down: true}
			}
			verdicts = append(verdicts, Verdict{Allowed: true}) // single-scope script fault: be lenient on that scope
			continue
		}
		v := parseVerdict(res)
		if !v.Allowed && i < len(active) {
			v.Limit = active[i].Limit
		}
		verdicts = append(verdicts, v)
	}
	return scopesRun{verdicts: verdicts, active: active, needLoad: needLoad}
}

// isNoScriptErr detects the NOSCRIPT server reply (pipeline path).
func isNoScriptErr(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}

// isScriptCmdErr reports whether any pipelined command already carried its own
// error (script-level, not connection-level) — Exec returns the first error.
func isScriptCmdErr(cmds []*redis.Cmd) bool {
	for _, c := range cmds {
		if c.Err() != nil {
			return true
		}
	}
	return false
}

func isRedisDownErr(err error) bool { return store.IsRedisDown(err) }

func parseVerdict(res any) Verdict {
	arr, ok := res.([]any)
	if !ok || len(arr) == 0 {
		return Verdict{Allowed: true}
	}
	allowed, _ := arr[0].(int64)
	v := Verdict{Allowed: allowed == 1}
	if len(arr) > 1 {
		if n, ok := arr[1].(int64); ok {
			v.Remaining = n
		}
	}
	if len(arr) > 2 {
		if ms, ok := arr[2].(int64); ok && ms > 0 {
			v.RetryAfter = time.Duration(ms) * time.Millisecond
		}
	}
	return v
}

// degraded is the fail-open verdict (metric + alarm).
func (l *Limiter) degraded() Verdict {
	if l.metrics != nil {
		l.metrics.RateLimitFailOpen.Add(context.Background(), 1)
	}
	if l.failOpen {
		return Verdict{Allowed: true}
	}
	return Verdict{Allowed: false, RetryAfter: time.Second}
}

// countLua prunes and reports the window count WITHOUT admitting.
// KEYS[1]=zset  ARGV: now_ms, window_ms
var countLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
return redis.call('ZCARD', KEYS[1])
`)

// hitLua records one event without any admission check (failure counting).
// KEYS[1]=zset  ARGV: now_ms, window_ms, member
var hitLua = redis.NewScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[3])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) + 1000)
return 1
`)

// admitLua atomically prunes the window, enforces the budget, and records
// the event in ONE round-trip. Replaces the old Count-read + Hit-write pair,
// which was a check-then-act race: a concurrent burst from one source could
// all read the pre-burst count and pass a budget un-counted (proven in
// real-infra E2E 2026-09-23: 60 concurrent signups sailed through a 30/15m
// per-IP budget). A limited request is NOT recorded — identical semantics
// to the sequential pair.
// KEYS[1]=zset  ARGV: now_ms, window_ms, member, budget
// Returns 1 = admitted (recorded), 0 = over budget (not recorded).
var admitLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
local n = redis.call('ZCARD', KEYS[1])
if n >= tonumber(ARGV[4]) then
        return 0
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[3])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) + 1000)
return 1
`)

// Admit atomically checks a counting-window budget and records the event
// when admitted. member MUST be unique per call (collision = undercount).
// Fail-open on Redis loss, mirroring Count's degradation.
func (l *Limiter) Admit(ctx context.Context, key string, window time.Duration, budget int, member string) bool {
	if budget <= 0 {
		return true
	}
	v, err := admitLua.Run(ctx, l.rdb, []string{key},
		time.Now().UnixMilli(), window.Milliseconds(), member, budget).Int64()
	if err != nil {
		if l.metrics != nil {
			l.metrics.RateLimitFailOpen.Add(ctx, 1)
		}
		return true
	}
	return v == 1
}

// Count reports the events currently inside the window (no admission).
func (l *Limiter) Count(ctx context.Context, key string, window time.Duration) int64 {
	n, err := countLua.Run(ctx, l.rdb, []string{key},
		time.Now().UnixMilli(), window.Milliseconds()).Int64()
	if err != nil {
		return 0 // fail-open
	}
	return n
}

// Hit records one event (e.g. a failed login) in the window.
func (l *Limiter) Hit(ctx context.Context, key string, window time.Duration, member string) {
	_, _ = hitLua.Run(ctx, l.rdb, []string{key},
		time.Now().UnixMilli(), window.Milliseconds(), member).Result()
}

// --- concurrency slots ------------------------------------------------------
//
// Design (audit H1, root fix): each dimension is a ZSET whose members are
// individual slot holders, scored with the slot's expiry instant (server
// time, from Redis TIME). Acquisition prunes expired holders, counts, and
// adds a fresh holder in one atomic script.
//
// Why per-holder expiry beats the old shared counter: the counter's key TTL
// was refreshed by EVERY acquire/release, so under sustained traffic the key
// never got a quiet TTL to expire — a slot leaked by a hard-killed pod (its
// Release never ran) stayed booked forever, and the only documented cure
// (Limiter.Reconcile) had zero production callers. With holders scored by
// expiry, a leaked slot disappears exactly one SlotTTL after it was CLAIMED,
// no matter how much other traffic touches the key. The key-level PEXPIRE is
// a pure garbage-collection backstop for idle dimensions: it is always set to
// ttl+1s at claim time, so it can never evict a live holder early.

// slotAcquireLua atomically prunes expired holders, enforces the limit, and
// claims one slot for ARGV[4] (holder id). KEYS[1]=holder zset
// ARGV: limit, now_ms, ttl_ms, holder — now_ms is caller-provided for the
// same clock-skew posture as the sliding-window scripts.
// Returns {1, holders_after_claim} on success, {0, holders_now} when full —
// the active-holder count feeds the X-Ratelimit concurrency headers.
var slotAcquireLua = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[2]))
local n = redis.call('ZCARD', KEYS[1])
if n >= tonumber(ARGV[1]) then
        return {0, n}
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[4])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) + 1000)
return {1, n + 1}
`)

// slotReleaseLua returns exactly one holder's slot (idempotent).
// KEYS[1]=holder zset  ARGV: holder
var slotReleaseLua = redis.NewScript(`
return redis.call('ZREM', KEYS[1], ARGV[1])
`)

// Slot is one claimed concurrency slot. Release is idempotent and safe to
// call from any goroutine, including a deferred cleanup after the request
// context has been cancelled (use a context.WithoutCancel wrapper there).
// Active reports the holder count observed at claim time (including this
// claim) — the transport surfaces it as X-Ratelimit-Remaining-Concurrency.
type Slot struct {
	lim    *Limiter
	key    string
	holder string
	active int64
	once   sync.Once
}

// Release returns the slot. Best-effort: a failed release only costs the
// holder its slot until the per-holder expiry reclaims it (bounded leak).
func (s *Slot) Release(ctx context.Context) {
	if s == nil {
		return
	}
	s.once.Do(func() {
		_, _ = slotReleaseLua.Run(ctx, s.lim.rdb, []string{s.key}, s.holder).Int()
	})
}

// Active reports the holder count observed at claim time (this claim
// included); 0 when the claim happened in a fail-open window.
func (s *Slot) Active() int64 {
	if s == nil {
		return 0
	}
	return s.active
}

// AcquireSlot claims one concurrency slot (bounded in-flight runs) and
// returns the handle used to release it. The second return is false when the
// dimension is at its limit. Fail-open on Redis loss, mirroring the limiter's
// posture; in that case the returned Slot is nil (nothing to release).
func (l *Limiter) AcquireSlot(ctx context.Context, key string, limit int64, ttl time.Duration) (*Slot, bool) {
	if limit <= 0 {
		return nil, true // dimension disabled — admitted, nothing to hold
	}
	holder := ids.New("slot")
	res, err := slotAcquireLua.Run(ctx, l.rdb, []string{key},
		limit, time.Now().UnixMilli(), ttl.Milliseconds(), holder).Slice()
	if err != nil || len(res) < 1 {
		return nil, l.degraded().Allowed
	}
	allowed, _ := res[0].(int64)
	if allowed != 1 {
		return nil, false
	}
	s := &Slot{lim: l, key: key, holder: holder, active: 1}
	if len(res) > 1 {
		if n, ok := res[1].(int64); ok {
			s.active = n
		}
	}
	return s, true
}

// --- client IP extraction ---------------------------------------------------

// The limiter itself is transport-agnostic; keys are composed by callers.
