// quota.go implements admission-time token reservation (audit H2, root fix).
//
// The old gate answered "how much has the tenant consumed?" from a cache up
// to 5s stale, while actual consumption was written asynchronously AFTER the
// stream finished. Under a parallel burst every admission decision was made
// against the same photograph of the past, so N concurrent requests could
// each pass a budget that only had room for one — an overshoot that scaled
// with the fan-out (40 parallel requests on a 100k-token budget could consume
// 420k).
//
// The fix holds the missing state where the decision happens: at admission
// the call RESERVES an estimate of its worst plausible consumption in Redis,
// atomically against the live sum of all in-flight reservations:
//
//	admit iff   used(DB, cached) + reserved(Redis, live) + estimate ≤ quota
//
// The reservation is then reconciled with reality:
//
//   - when the call finishes, its reservation is ADJUSTED to the actual
//     normalized token count (up or down — the metering fact is now pending);
//   - when metering durably persists the llm_calls row, the reservation is
//     RELEASED and the usage cache invalidated — the DB sum now covers it;
//   - if the record is finally abandoned (spill failed too), the reservation
//     is released as well (bounded under-count, loud) so a lost row can never
//     wedge a tenant's quota;
//   - a crashed pod's reservations self-expire one ReserveTTL after they were
//     claimed (same per-holder expiry pattern as concurrency slots).
//
// Redis layout per tenant:
//
//	usage:resv:exp:<tenant>  ZSET  member=callID  score=expiry instant (ms)
//	usage:resv:amt:<tenant>  HASH  field=callID   value=reserved tokens
//
// The ZSET is the expiry index (hashes have no field TTLs); both keys share
// a TTL backstop that always exceeds any live reservation's remaining life.
package llm

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/observability"
)

func resvExpKey(tid string) string { return "usage:resv:exp:" + tid }
func resvAmtKey(tid string) string { return "usage:resv:amt:" + tid }

// reserveLua prunes expired reservations, sums the live ones, enforces BOTH
// window budgets against (used + reserved + amount), and — when there is
// room — records the new reservation. One atomic script: two concurrent
// admissions cannot both spend the same remaining budget.
//
// KEYS[1]=exp zset  KEYS[2]=amt hash
// ARGV: now_ms, ttl_ms, call_id, amount, used5h, q5h, used_w, q_w
// Returns {1, total} when reserved, {0, 1} when the 5h window binds,
// {0, 2} when the weekly window binds.
var reserveLua = redis.NewScript(`
local cutoff = tonumber(ARGV[1])
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff)
if #expired > 0 then
        for i = 1, #expired do
                redis.call('HDEL', KEYS[2], expired[i])
        end
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
end
local total = 0
local vals = redis.call('HVALS', KEYS[2])
for i = 1, #vals do
        total = total + tonumber(vals[i])
end
local amt = tonumber(ARGV[4])
local q5h = tonumber(ARGV[6])
if q5h > 0 and tonumber(ARGV[5]) + total + amt > q5h then
        return {0, 1}
end
local qw = tonumber(ARGV[8])
if qw > 0 and tonumber(ARGV[7]) + total + amt > qw then
        return {0, 2}
end
redis.call('HSET', KEYS[2], ARGV[3], amt)
redis.call('ZADD', KEYS[1], cutoff + tonumber(ARGV[2]), ARGV[3])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) + 60000)
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) + 60000)
return {1, total + amt}
`)

// adjustReservationLua re-points a live reservation at the actual usage
// (only when the reservation still exists — a released/expired one must not
// be resurrected). KEYS[1]=exp  KEYS[2]=amt  ARGV: call_id, amount
var adjustReservationLua = redis.NewScript(`
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
        redis.call('HSET', KEYS[2], ARGV[1], tonumber(ARGV[2]))
        return 1
end
return 0
`)

// releaseReservationLua drops one reservation (idempotent).
// KEYS[1]=exp  KEYS[2]=amt  ARGV: call_id
var releaseReservationLua = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)

// reserveQuota runs the atomic window check. Returns the binding window
// ("" = admitted, "5h" | "weekly" = rejected), the projected used figure
// for the rejection message, and the live sum of in-flight reservations
// INCLUDING this call when admitted (−1 when unknown — no Redis / degraded;
// the transport omits the quota position headers in that case).
// Degrades to the legacy plain check when Redis is unavailable (fail-open
// posture of the quota gate is unchanged: a Redis outage must not lock users
// out).
func (p *Proxy) reserveQuota(ctx context.Context, tenantID, callID string, req *Request, pos *domain.WindowUsage, q5h, qWeekly int64) (string, int64, int64) {
	// Legacy behaviour when reservation is impossible (no Redis in this
	// deployment / test): plain used-vs-quota, no reservation held.
	if p.Redis == nil {
		if q5h > 0 && pos.Used5h >= q5h {
			return "5h", pos.Used5h, -1
		}
		if qWeekly > 0 && pos.UsedWeekly >= qWeekly {
			return "weekly", pos.UsedWeekly, -1
		}
		return "", 0, -1
	}

	amount := estimateTokens(req)
	ttl := p.ReserveTTL
	if ttl <= 0 {
		ttl = 20 * time.Minute
	}
	res, err := reserveLua.Run(ctx, p.Redis,
		[]string{resvExpKey(tenantID), resvAmtKey(tenantID)},
		time.Now().UnixMilli(), ttl.Milliseconds(), callID, amount,
		pos.Used5h, q5h, pos.UsedWeekly, qWeekly).Slice()
	if err != nil {
		// Redis error: the reservation cannot be held — fall back to the
		// plain check (visible via the gate-fails counter).
		if p.Metrics != nil {
			p.Metrics.QuotaGateFails.Add(ctx, 1)
		}
		if q5h > 0 && pos.Used5h >= q5h {
			return "5h", pos.Used5h, -1
		}
		if qWeekly > 0 && pos.UsedWeekly >= qWeekly {
			return "weekly", pos.UsedWeekly, -1
		}
		return "", 0, -1
	}

	if len(res) >= 2 {
		if allowed, _ := res[0].(int64); allowed == 1 {
			total, _ := res[1].(int64)
			return "", 0, total
		}
		switch w, _ := res[1].(int64); w {
		case 1:
			used := pos.Used5h
			if p.Metrics != nil {
				p.Metrics.QuotaRejected.Add(ctx, 1, observability.Attr("window", "5h"))
			}
			return "5h", used, 0
		case 2:
			used := pos.UsedWeekly
			if p.Metrics != nil {
				p.Metrics.QuotaRejected.Add(ctx, 1, observability.Attr("window", "weekly"))
			}
			return "weekly", used, 0
		}
	}
	return "", 0, -1
}

// adjustReservation re-points the call's reservation at its actual normalized
// consumption once metering facts are known (call finished). No-op when the
// reservation is gone. Runs on a context that survives request cancellation.
func (p *Proxy) adjustReservation(ctx context.Context, tenantID, callID string, actual int64) {
	if p.Redis == nil {
		return
	}
	_, _ = adjustReservationLua.Run(context.WithoutCancel(ctx), p.Redis,
		[]string{resvExpKey(tenantID), resvAmtKey(tenantID)},
		callID, actual).Int()
}

// releaseReservation drops the call's reservation (record abandoned without
// usage facts — nothing will be counted, so nothing may stay held).
func (p *Proxy) releaseReservation(ctx context.Context, tenantID, callID string) {
	if p.Redis == nil {
		return
	}
	_, _ = releaseReservationLua.Run(context.WithoutCancel(ctx), p.Redis,
		[]string{resvExpKey(tenantID), resvAmtKey(tenantID)},
		callID).Int()
}

// QuotaPersisted is the metering completion hook: the llm_calls row is
// durably stored (or finally abandoned — durable=false), so the call's
// reservation can be released: the authoritative DB sum now covers it. The
// window cache is invalidated so the very next admission reads fresh data
// instead of up-to-5s-stale numbers (closes the last visibility gap).
func (p *Proxy) QuotaPersisted(tenantID, callID string, durable bool) {
	if p.Redis == nil {
		return
	}
	ctx := context.Background()
	_, _ = releaseReservationLua.Run(ctx, p.Redis,
		[]string{resvExpKey(tenantID), resvAmtKey(tenantID)}, callID).Int()
	if durable {
		_ = p.Redis.Del(ctx, windowUsageKey(tenantID)).Err()
	}
}

// defaultCompletionEstimate bounds the un-known output side of a reservation.
// Requests that carry max_tokens / max_completion_tokens reserve their actual
// bound; without one, 2048 covers a typical agent turn without rejecting
// modest calls near the quota edge.
const defaultCompletionEstimate = 2048

// estimateTokens bounds what this request can plausibly consume, for the
// reservation ledger. The prompt side is measured from the payload (~4 bytes
// per token heuristic over message and tool-definition bytes); the completion
// side uses the request's own ceiling when present. It is an estimate, not a
// bill: the reservation is reconciled to the metered truth when the call ends.
func estimateTokens(req *Request) int64 {
	var bytes int64
	for i := range req.Messages {
		bytes += int64(len(req.Messages[i].Content))
		bytes += int64(len(req.Messages[i].Reasoning))
		for j := range req.Messages[i].ToolCalls {
			bytes += int64(len(req.Messages[i].ToolCalls[j].Function.Name)) +
				int64(len(req.Messages[i].ToolCalls[j].Function.Arguments))
		}
	}
	for i := range req.Tools {
		if fn := req.Tools[i].Function; fn != nil {
			bytes += int64(len(fn.Description)) + int64(len(fn.Parameters)) + 128
		}
	}
	prompt := bytes / 4

	out := int64(defaultCompletionEstimate)
	if req.MaxCompletionTokens != nil && *req.MaxCompletionTokens > 0 {
		out = *req.MaxCompletionTokens
	} else if req.MaxTokens != nil && *req.MaxTokens > 0 {
		out = *req.MaxTokens
	}
	return prompt + out
}

// totalReserved reports the live reservation sum for a tenant (diagnostics
// and tests).
func (p *Proxy) totalReserved(ctx context.Context, tenantID string) int64 {
	if p.Redis == nil {
		return 0
	}
	vals, err := p.Redis.HVals(ctx, resvAmtKey(tenantID)).Result()
	if err != nil {
		return 0
	}
	var total int64
	for _, v := range vals {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			total += n
		}
	}
	return total
}
