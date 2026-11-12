package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T, failOpen bool) (*Limiter, *miniredis.Miniredis) {
	t.Helper()
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, failOpen, nil), mr
}

func TestSlidingWindowAllowsUpToLimit(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	const limit = 3
	for i := 0; i < limit; i++ {
		v := l.Allow(ctx, "k", limit, time.Minute, member(i))
		if !v.Allowed {
			t.Fatalf("request %d must be allowed", i)
		}
	}
	if v := l.Allow(ctx, "k", limit, time.Minute, "m_extra"); v.Allowed {
		t.Fatal("request beyond the limit must be denied")
	}
}

func TestSlidingWindowDenialCarriesRetryAfter(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		l.Allow(ctx, "k2", 2, time.Minute, member(i))
	}
	v := l.Allow(ctx, "k2", 2, time.Minute, "m")
	if v.Allowed {
		t.Fatal("must deny")
	}
	if v.RetryAfter <= 0 || v.RetryAfter > time.Minute {
		t.Fatalf("retry-after within the window: %v", v.RetryAfter)
	}
}

// TestSlidingWindowRetryAfterDirection guards audit finding 3: the denial
// Retry-After must be the REMAINING time until the oldest in-window entry
// expires (window − age), never the entry's age. The inverted formula told
// throttled clients to wait ~the full window when 1 s remained and to retry
// almost immediately when ~the whole window remained.
func TestSlidingWindowRetryAfterDirection(t *testing.T) {
	l, mr := newTestLimiter(t, true)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	// Case A: the in-window entry is 59 s old → it expires in ~1 s. A
	// correct limiter advertises ~1 s.
	kA := "rl:proof:direction:a"
	if err := rdb.ZAdd(ctx, kA, redis.Z{
		Score: float64(time.Now().UnixMilli() - 59_000), Member: "seed_59s_old",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	v := l.Allow(ctx, kA, 1, time.Minute, "req_new")
	if v.Allowed {
		t.Fatal("case A: must deny (window full)")
	}
	if v.RetryAfter > 3*time.Second {
		t.Fatalf("case A: RetryAfter=%v, want ~1s (oldest expires in 1s)", v.RetryAfter)
	}

	// Case B: the in-window entry is 1 s old → it expires in ~59 s. A
	// correct limiter advertises ~59 s.
	kB := "rl:proof:direction:b"
	if err := rdb.ZAdd(ctx, kB, redis.Z{
		Score: float64(time.Now().UnixMilli() - 1_000), Member: "seed_1s_old",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	v = l.Allow(ctx, kB, 1, time.Minute, "req_new")
	if v.Allowed {
		t.Fatal("case B: must deny (window full)")
	}
	if v.RetryAfter < 55*time.Second {
		t.Fatalf("case B: RetryAfter=%v, want ~59s (oldest expires in 59s)", v.RetryAfter)
	}
}

func TestSlidingWindowExpires(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	// The window is computed from the CLIENT clock (ARGV), so real time must
	// pass for expiry — a short window and sleep keep the test fast.
	l.Allow(ctx, "k3", 1, 40*time.Millisecond, "m1")
	if v := l.Allow(ctx, "k3", 1, 40*time.Millisecond, "m2"); v.Allowed {
		t.Fatal("second request in window must be denied")
	}
	time.Sleep(60 * time.Millisecond)
	if v := l.Allow(ctx, "k3", 1, 40*time.Millisecond, "m3"); !v.Allowed {
		t.Fatal("window expiry must re-allow")
	}
}

func TestZeroLimitDisablesDimension(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	v := l.Allow(context.Background(), "k", 0, time.Minute, "m")
	if !v.Allowed {
		t.Fatal("limit 0 = disabled")
	}
}

func TestAllowAllStrictestWins(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	// User scope: 10; tenant scope: 1.
	v := l.AllowAll(ctx, "m0",
		Scope{Key: "u1", Limit: 10, Window: time.Minute},
		Scope{Key: "t1", Limit: 1, Window: time.Minute},
	)
	if !v.Allowed {
		t.Fatal("first request passes all scopes")
	}
	v = l.AllowAll(ctx, "m1",
		Scope{Key: "u1", Limit: 10, Window: time.Minute},
		Scope{Key: "t1", Limit: 1, Window: time.Minute},
	)
	if v.Allowed {
		t.Fatal("tenant scope (limit 1) must deny the second request")
	}
}

func TestConcurrencyAcquireRelease(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	s1, ok := l.AcquireSlot(ctx, "conc", 2, time.Minute)
	if !ok {
		t.Fatal("first slot")
	}
	s2, ok := l.AcquireSlot(ctx, "conc", 2, time.Minute)
	if !ok {
		t.Fatal("second slot")
	}
	if _, ok := l.AcquireSlot(ctx, "conc", 2, time.Minute); ok {
		t.Fatal("third slot must fail")
	}
	s2.Release(ctx)
	if _, ok := l.AcquireSlot(ctx, "conc", 2, time.Minute); !ok {
		t.Fatal("released slot is claimable again")
	}
	// Idempotency: releasing the same handle twice must not free two slots.
	s2.Release(ctx)
	if _, ok := l.AcquireSlot(ctx, "conc", 2, time.Minute); ok {
		t.Fatal("double release must not over-free (limit 2, two live holders)")
	}
	s1.Release(ctx)
}

func TestConcurrencySlotExpires(t *testing.T) {
	l, mr := newTestLimiter(t, true)
	ctx := context.Background()
	// Seed a slot claimed 100ms ago with ttl 60ms: expired but never
	// released (owner crash). The score is the EXPIRY instant (claim+ttl),
	// exactly what slotAcquireLua writes. Per-holder expiry must prune it
	// on the next acquire — the holder's score, not key traffic, decides.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.ZAdd(ctx, "conc2", redis.Z{
		Score:  float64(time.Now().UnixMilli() - 100 + 60), // claim(−100ms)+ttl(60ms)
		Member: "crashed_holder",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok := l.AcquireSlot(ctx, "conc2", 1, 60*time.Millisecond); !ok {
		t.Fatal("expired slot must be reclaimable")
	}
}

// TestLeakedSlotExpiresDespiteTraffic is the H1 regression test. The old
// shared-counter design refreshed the key TTL on every successful acquire AND
// release, so a slot leaked by a dead pod stayed booked forever while healthy
// traffic kept churning the same dimension. Per-holder expiry must reclaim
// the leaked slot exactly one TTL after it was claimed, churn notwithstanding.
func TestLeakedSlotExpiresDespiteTraffic(t *testing.T) {
	l, mr := newTestLimiter(t, true)
	ctx := context.Background()
	const ttl = 120 * time.Millisecond

	// Slot A is claimed and never released (pod died mid-stream, limit 2):
	// seeded with its expiry instant (claim+ttl), matching the script's
	// own ZADD scoring.
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.ZAdd(ctx, "h1", redis.Z{
		Score:  float64(time.Now().UnixMilli() + ttl.Milliseconds()),
		Member: "leaked_holder",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	// Healthy traffic keeps churning the SAME dimension: successful
	// acquire + release pairs. Under the old design each of these
	// refreshed the key TTL — the eternal leak.
	for i := 0; i < 3; i++ {
		s, ok := l.AcquireSlot(ctx, "h1", 2, ttl)
		if !ok {
			t.Fatal("healthy slot must be claimable while one of two is leaked")
		}
		s.Release(ctx)
	}
	// The leaked holder still occupies one slot: the remaining free slot is
	// claimable, and while it is held nothing else is (1 leaked + 1 held = 2).
	s, ok := l.AcquireSlot(ctx, "h1", 2, ttl)
	if !ok {
		t.Fatal("the one free slot must be claimable while the leaked holder is live")
	}
	if _, ok := l.AcquireSlot(ctx, "h1", 2, ttl); ok {
		t.Fatal("precondition: leaked holder must still occupy a slot before its expiry")
	}
	s.Release(ctx)
	// A full slot-TTL passes with traffic continuing throughout.
	time.Sleep(ttl + 40*time.Millisecond)
	// A's ghost must be gone: BOTH remaining slots claimable at once.
	s1, ok := l.AcquireSlot(ctx, "h1", 2, ttl)
	if !ok {
		t.Fatal("first post-expiry slot")
	}
	s2, ok := l.AcquireSlot(ctx, "h1", 2, ttl)
	if !ok {
		s1.Release(ctx)
		t.Fatal("H1 regression: leaked slot must expire one TTL after claim despite ongoing traffic")
	}
	s1.Release(ctx)
	s2.Release(ctx)
}

func TestReleaseFloor(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	// Releasing an unknown holder must be a no-op (idempotent, no error).
	s := &Slot{lim: l, key: "never-acquired", holder: "ghost"}
	s.Release(ctx)
	if _, ok := l.AcquireSlot(ctx, "never-acquired", 1, time.Minute); !ok {
		t.Fatal("dimension must be usable after a ghost release")
	}
}

// TestReconcileResetsCounter was removed with the counter-based API: the
// per-holder ZSET design (H1 root fix) makes sweeper-based reconciliation
// unnecessary — every leaked slot self-expires exactly one SlotTTL after it
// was claimed, so there is no counter to reset.

func TestFailOpenOnRedisDown(t *testing.T) {
	l, mr := newTestLimiter(t, true)
	mr.Close() // Redis dies
	v := l.Allow(context.Background(), "k", 1, time.Minute, "m")
	if !v.Allowed {
		t.Fatal("fail-open must allow when Redis is down")
	}
}

func TestFailClosedOnRedisDown(t *testing.T) {
	l, mr := newTestLimiter(t, false)
	mr.Close()
	v := l.Allow(context.Background(), "k", 1, time.Minute, "m")
	if v.Allowed {
		t.Fatal("fail-closed must deny when Redis is down")
	}
}

func TestCountAndHit(t *testing.T) {
	l, _ := newTestLimiter(t, true)
	ctx := context.Background()
	if n := l.Count(ctx, "throttle", time.Minute); n != 0 {
		t.Fatalf("initial count: %d", n)
	}
	l.Hit(ctx, "throttle", time.Minute, "a")
	l.Hit(ctx, "throttle", time.Minute, "b")
	if n := l.Count(ctx, "throttle", time.Minute); n != 2 {
		t.Fatalf("count after hits: %d", n)
	}
	// Count must NOT admit.
	if n := l.Count(ctx, "throttle", time.Minute); n != 2 {
		t.Fatalf("count must not grow: %d", n)
	}
}

func member(i int) string {
	if i == 0 {
		return "m0"
	}
	return "m" + time.Now().Format("150405") + string(rune('a'+i))
}
