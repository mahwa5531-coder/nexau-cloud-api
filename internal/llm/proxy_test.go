package llm

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/domain"
)

type mockUsageQuery struct {
	calls atomic.Int64
	usage *domain.WindowUsage
}

func (m *mockUsageQuery) WindowUsage(ctx context.Context, tenantID string) (*domain.WindowUsage, error) {
	m.calls.Add(1)
	return m.usage, nil
}

func (m *mockUsageQuery) WindowRecovery(ctx context.Context, tenantID string, window time.Duration, quota, used int64) (*time.Time, error) {
	t := time.Now().Add(time.Hour)
	return &t, nil
}

func TestProxyWindowUsage_RedisCache(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	mockUsage := &mockUsageQuery{
		usage: &domain.WindowUsage{Used5h: 1000, UsedWeekly: 5000},
	}

	p := &Proxy{
		Redis:         rdb,
		Usage:         mockUsage,
		UsageCacheTTL: 2 * time.Second,
	}

	ctx := context.Background()

	// 1. First call: Cache miss -> calls DB
	u1, err := p.windowUsage(ctx, "ten_test", 50000, 200000)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if u1.Used5h != 1000 || u1.UsedWeekly != 5000 {
		t.Fatalf("unexpected usage: %+v", u1)
	}
	if mockUsage.calls.Load() != 1 {
		t.Fatalf("expected 1 DB call on cache miss, got %d", mockUsage.calls.Load())
	}

	// 2. Second call within TTL: Cache hit -> does NOT call DB
	u2, err := p.windowUsage(ctx, "ten_test", 50000, 200000)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if u2.Used5h != 1000 || u2.UsedWeekly != 5000 {
		t.Fatalf("unexpected cached usage: %+v", u2)
	}
	if mockUsage.calls.Load() != 1 {
		t.Fatalf("expected DB call count to remain 1 on cache hit, got %d", mockUsage.calls.Load())
	}

	// 3. Fast-forward past TTL (2s): Cache expired -> calls DB
	mr.FastForward(3 * time.Second)
	mockUsage.usage = &domain.WindowUsage{Used5h: 48000, UsedWeekly: 5000} // 48000 / 50000 = 96%
	u3, err := p.windowUsage(ctx, "ten_test", 50000, 200000)
	if err != nil {
		t.Fatalf("post-expiry call failed: %v", err)
	}
	if u3.Used5h != 48000 {
		t.Fatalf("expected fresh near-quota value 48000, got %d", u3.Used5h)
	}
	if mockUsage.calls.Load() != 2 {
		t.Fatalf("expected post-expiry call to query DB, got %d", mockUsage.calls.Load())
	}

	// 4. While near-quota (48000 >= 95% of 50000): subsequent call bypasses cache and re-queries DB immediately
	mockUsage.usage = &domain.WindowUsage{Used5h: 48500, UsedWeekly: 5000}
	u4, err := p.windowUsage(ctx, "ten_test", 50000, 200000)
	if err != nil {
		t.Fatalf("near-quota bypass call failed: %v", err)
	}
	if u4.Used5h != 48500 {
		t.Fatalf("expected fresh near-quota value 48500, got %d", u4.Used5h)
	}
	if mockUsage.calls.Load() != 3 {
		t.Fatalf("expected near-quota call to bypass cache and query DB, got %d", mockUsage.calls.Load())
	}

	// 5. Over quota: does not cache
	mockUsage.usage = &domain.WindowUsage{Used5h: 51000, UsedWeekly: 5000}
	_, _ = p.windowUsage(ctx, "ten_test", 50000, 200000)
	if mockUsage.calls.Load() != 4 {
		t.Fatalf("expected DB call for over-quota check, got %d", mockUsage.calls.Load())
	}

	// Cache key should be deleted/absent after over quota
	exists := rdb.Exists(ctx, windowUsageKey("ten_test")).Val()
	if exists != 0 {
		t.Fatalf("expected over-quota usage key to not be cached in Redis, got exists=%d", exists)
	}
}

func TestProxyWindowUsage_FallbackWhenRedisNilOrDown(t *testing.T) {
	mockUsage := &mockUsageQuery{
		usage: &domain.WindowUsage{Used5h: 500, UsedWeekly: 2500},
	}

	// Redis is nil: graceful fallback to DB
	p := &Proxy{
		Redis: nil,
		Usage: mockUsage,
	}

	ctx := context.Background()
	u, err := p.windowUsage(ctx, "ten_nil", 50000, 200000)
	if err != nil {
		t.Fatalf("nil redis failed: %v", err)
	}
	if u.Used5h != 500 {
		t.Fatalf("expected 500, got %d", u.Used5h)
	}
	if mockUsage.calls.Load() != 1 {
		t.Fatalf("expected 1 DB call with nil redis, got %d", mockUsage.calls.Load())
	}
}
