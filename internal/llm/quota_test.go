package llm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/openrouter"
)

func newQuotaTestProxy(t *testing.T) (*Proxy, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &Proxy{Redis: rdb, ReserveTTL: time.Minute}, mr
}

func tinyRequest() *Request {
	return &Request{
		Model: "openai/gpt-4o",
		Messages: []openrouter.Message{
			{Role: "user", Content: json.RawMessage(`"hi"`)},
		},
	}
}

// TestReservationBlocksBurst is the H2 regression test. Three sequential
// admissions against a 5000-token budget, each estimating ~2048 tokens: the
// third must be rejected because the first two hold live reservations. The
// old gate re-read the same stale usage sum and admitted all of them — the
// overshoot scaled with the fan-out.
func TestReservationBlocksBurst(t *testing.T) {
	p, _ := newQuotaTestProxy(t)
	ctx := context.Background()
	pos := &domain.WindowUsage{Used5h: 0, UsedWeekly: 0}
	const quota = 5000

	admitted, rejected := 0, 0
	for i := 0; i < 3; i++ {
		w, _, _ := p.reserveQuota(ctx, "ten_burst", ids.CallID(), tinyRequest(), pos, quota, 0)
		if w == "" {
			admitted++
		} else {
			rejected++
			if w != "5h" {
				t.Fatalf("expected the 5h window to bind, got %q", w)
			}
		}
	}
	if admitted != 2 || rejected != 1 {
		t.Fatalf("admitted=%d rejected=%d; want 2 admitted (2×2049 ≤ 5000), 1 rejected (3rd would cross)", admitted, rejected)
	}
	if got := p.totalReserved(ctx, "ten_burst"); got != 2*2049 {
		t.Fatalf("reserved=%d, want 4098 (two live estimates of 2049)", got)
	}
}

// TestReservationBlocksBurstWeekly verifies the weekly window binds too.
func TestReservationBlocksBurstWeekly(t *testing.T) {
	p, _ := newQuotaTestProxy(t)
	ctx := context.Background()
	pos := &domain.WindowUsage{Used5h: 0, UsedWeekly: 0}
	const quota = 5000
	for i := 0; i < 3; i++ {
		w, _, _ := p.reserveQuota(ctx, "ten_w", ids.CallID(), tinyRequest(), pos, 0, quota)
		if i == 2 {
			if w != "weekly" {
				t.Fatalf("third admission: want weekly rejection, got %q", w)
			}
		} else if w != "" {
			t.Fatalf("admission %d must pass, got %q", i, w)
		}
	}
}

// TestReservationReleasedOnPersist: QuotaPersisted (the metering completion
// hook) must drop the reservation AND invalidate the window cache so the next
// admission reads fresh database numbers.
func TestReservationReleasedOnPersist(t *testing.T) {
	p, _ := newQuotaTestProxy(t)
	ctx := context.Background()
	pos := &domain.WindowUsage{Used5h: 0, UsedWeekly: 0}

	callID := ids.CallID()
	if w, _, _ := p.reserveQuota(ctx, "ten_r", callID, tinyRequest(), pos, 100_000, 0); w != "" {
		t.Fatalf("admission failed: %q", w)
	}
	// Simulate a filled window cache.
	if err := p.Redis.Set(ctx, windowUsageKey("ten_r"), []byte(`{}`), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	p.QuotaPersisted("ten_r", callID, true)

	if got := p.totalReserved(ctx, "ten_r"); got != 0 {
		t.Fatalf("reservation must be released after persist, got %d", got)
	}
	if p.Redis.Exists(ctx, windowUsageKey("ten_r")).Val() != 0 {
		t.Fatal("window cache must be invalidated after persist")
	}
}

// TestReservationAdjustsToActual: when metering facts arrive, the reservation
// tracks the real consumption (up or down) until the row lands.
func TestReservationAdjustsToActual(t *testing.T) {
	p, _ := newQuotaTestProxy(t)
	ctx := context.Background()
	pos := &domain.WindowUsage{Used5h: 0, UsedWeekly: 0}

	callID := ids.CallID()
	if w, _, _ := p.reserveQuota(ctx, "ten_a", callID, tinyRequest(), pos, 100_000, 0); w != "" {
		t.Fatalf("admission failed: %q", w)
	}
	p.adjustReservation(ctx, "ten_a", callID, 7_500)
	if got := p.totalReserved(ctx, "ten_a"); got != 7_500 {
		t.Fatalf("reserved=%d, want the adjusted 7500", got)
	}

	// Adjusting an unknown (already released) call must not resurrect it.
	p.adjustReservation(ctx, "ten_a", ids.CallID(), 999)
	if got := p.totalReserved(ctx, "ten_a"); got != 7_500 {
		t.Fatalf("ghost adjust resurrected a reservation: %d", got)
	}

	// Releasing a released reservation is a no-op.
	p.releaseReservation(ctx, "ten_a", callID)
	p.releaseReservation(ctx, "ten_a", callID)
	if got := p.totalReserved(ctx, "ten_a"); got != 0 {
		t.Fatalf("reserved=%d, want 0 after release", got)
	}
}

// TestReservationExpiryPrunes: a leaked reservation (pod died mid-stream)
// must stop counting one ReserveTTL after it was claimed.
func TestReservationExpiryPrunes(t *testing.T) {
	p, _ := newQuotaTestProxy(t)
	ctx := context.Background()

	// Seed a reservation claimed 2 minutes ago with a 1-minute TTL.
	if err := p.Redis.ZAdd(ctx, resvExpKey("ten_e"), redis.Z{
		Score:  float64(time.Now().UnixMilli() - 120_000),
		Member: "crashed_call",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := p.Redis.HSet(ctx, resvAmtKey("ten_e"), "crashed_call", 99_000).Err(); err != nil {
		t.Fatal(err)
	}

	pos := &domain.WindowUsage{Used5h: 0, UsedWeekly: 0}
	// Budget 3000: the crashed reservation (99k) must NOT block a fresh call.
	if w, _, _ := p.reserveQuota(ctx, "ten_e", ids.CallID(), tinyRequest(), pos, 3000, 0); w != "" {
		t.Fatalf("expired reservation must be pruned, got rejection %q", w)
	}
}

// TestEstimateTokens pins the estimator's contract: prompt bytes/4 plus the
// request's own completion ceiling.
func TestEstimateTokens(t *testing.T) {
	req := tinyRequest()
	// prompt: `"hi"` = 4 bytes → 1 token; completion: default 2048.
	if got := estimateTokens(req); got != 2049 {
		t.Fatalf("default estimate: %d, want 2049 (1 prompt + 2048 completion)", got)
	}
	big := int64(50_000)
	req.MaxCompletionTokens = &big
	// prompt bytes: `"hi"` = 4 bytes → 1 token; plus the 50k completion ceiling.
	if got := estimateTokens(req); got != 50_001 {
		t.Fatalf("explicit ceiling: %d, want 50001", got)
	}
}
