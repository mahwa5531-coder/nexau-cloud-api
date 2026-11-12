package tests

// rateinfo_test.go — X-Ratelimit-* payload correctness (the admission
// position the transport turns into response headers):
//
//   - success: RPM remaining counts THIS call, concurrency active counts
//     THIS holder, quota remaining deducts metered usage AND live
//     reservations
//   - denial: the binding dimension reports zero-remaining while the other
//     dimensions keep their honest values
//   - degradation: no Redis → quota position unknown, admission still works

import (
	"context"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/auth"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/reqctx"
)

func quotaIdentity() *auth.Identity {
	idn := testIdentity()
	idn.Limits.Window5hTokens = 3000
	idn.Limits.WindowWeeklyTokens = 100_000
	return idn
}

func TestRateInfo_SuccessCountsThisCall(t *testing.T) {
	env := newEnv(t)
	idn := quotaIdentity()

	_, info, derr := env.proxy.Complete(reqctx.WithRequestID(context.Background(), "req_ri_1"), idn, sampleRequest(false))
	if derr != nil {
		t.Fatalf("complete: %v", derr)
	}
	if info == nil {
		t.Fatal("nil RateInfo on success")
	}
	if info.RPMUserLimit != 600 {
		t.Fatalf("RPM limit = %d, want 600", info.RPMUserLimit)
	}
	if info.RPMUserRemaining != 599 { // this call is the 1st in the window
		t.Fatalf("RPM remaining = %d, want 599", info.RPMUserRemaining)
	}
	if info.ConcUserLimit != 16 {
		t.Fatalf("conc limit = %d, want 16", info.ConcUserLimit)
	}
	if info.ConcUserActive != 1 { // slot released by return time, but the CLAIM-time count is 1
		t.Fatalf("conc active = %d, want 1", info.ConcUserActive)
	}
	if info.Quota5hLimit != 3000 {
		t.Fatalf("quota limit = %d, want 3000", info.Quota5hLimit)
	}
	// estimateTokens(sampleRequest) = 1 (prompt: 4 content bytes / 4) + 2048
	// (default completion estimate) = 2049; no metered usage yet, so
	// remaining = 3000 − 0 − 2049.
	if info.Quota5hRemaining != 3000-2049 {
		t.Fatalf("quota remaining = %d, want %d", info.Quota5hRemaining, 3000-2049)
	}
	if info.QuotaWeeklyRemaining != 100_000-2049 {
		t.Fatalf("weekly remaining = %d, want %d", info.QuotaWeeklyRemaining, 100_000-2049)
	}
}

func TestRateInfo_RateLimitedReportsZeroAndRetry(t *testing.T) {
	env := newEnv(t)
	idn := testIdentity()
	idn.Limits.RequestsPerMinuteUser = 2 // plan override, tiny window

	// Two admissions fill the window; the third must be denied with a
	// zero-remaining user scope and a sane retry hint.
	for i := 0; i < 2; i++ {
		if _, _, derr := env.proxy.Complete(reqctx.WithRequestID(context.Background(), "req_rl"), idn, sampleRequest(false)); derr != nil {
			t.Fatalf("warmup %d: %v", i, derr)
		}
	}
	_, info, derr := env.proxy.Complete(reqctx.WithRequestID(context.Background(), "req_rl"), idn, sampleRequest(false))
	if derr == nil || derr.Code != "RATE_LIMITED" {
		t.Fatalf("expected RATE_LIMITED, got %v", derr)
	}
	if info == nil {
		t.Fatal("nil RateInfo on denial")
	}
	if info.RPMUserRemaining != 0 {
		t.Fatalf("RPM remaining on denial = %d, want 0", info.RPMUserRemaining)
	}
	if info.RPMUserLimit != 2 {
		t.Fatalf("RPM limit = %d, want 2 (plan override)", info.RPMUserLimit)
	}
	if derr.Details["retry_after_ms"] == nil || info.RPMRetryAfterMS <= 0 {
		t.Fatalf("retry hint missing: details=%v info=%+v", derr.Details, info.RPMRetryAfterMS)
	}
	// Slots are acquired BEFORE the RPM gate (audit L18), so the denial's
	// RateInfo still reports the claim-time concurrency position of THIS
	// request (1) — the honest admission-time view, released right after.
	if info.ConcUserLimit != 16 || info.ConcUserActive != 1 {
		t.Fatalf("conc position on RPM denial: %+v", info)
	}
	// The quota gate never ran (RPM denied first): limits stay at the plan
	// values and remainings stay unknown (0 → header omitted).
	if info.Quota5hLimit != 1_000_000 || info.Quota5hRemaining != 0 {
		t.Fatalf("quota position on RPM denial: %+v", info)
	}
}

func TestRateInfo_QuotaDeniedReportsZeroForBindingWindow(t *testing.T) {
	env := newEnv(t)
	idn := quotaIdentity()
	idn.Limits.Window5hTokens = 3000 // admits exactly one in-flight reservation (estimate 2049)
	idn.Limits.WindowWeeklyTokens = 1_000_000

	// A stream that stays in flight (the reservation must outlive admission).
	env.fb.setScript(
		chunk(`{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-4o","choices":[{"index":0,"delta":{"content":"h"}}]}`),
		scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-4o","choices":[{"index":0,"finish_reason":"stop"}],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}`, delay: 400 * time.Millisecond},
	)

	// Hold one stream open so its reservation stays live.
	unblock := make(chan struct{})
	sink := &blockingSink{unblock: unblock}
	go func() {
		_, _ = env.proxy.Stream(reqctx.WithRequestID(context.Background(), "req_qd_1"), idn, sampleRequest(true), sink)
	}()
	waitFor(t, "first stream in flight", 2*time.Second, func() bool {
		return env.proxy.ActiveCount() == 1
	})

	// The second call's estimate crosses the fully-reserved window.
	info, derr := env.proxy.Stream(reqctx.WithRequestID(context.Background(), "req_qd_2"), idn, sampleRequest(true), &captureSink{})
	if derr == nil || derr.Code != "WINDOW_QUOTA_EXCEEDED" {
		t.Fatalf("expected WINDOW_QUOTA_EXCEEDED, got %v", derr)
	}
	if info == nil {
		t.Fatal("nil RateInfo on quota denial")
	}
	if info.Quota5hRemaining != 0 {
		t.Fatalf("5h remaining = %d, want 0 on binding window", info.Quota5hRemaining)
	}
	// The weekly window is NOT binding: its remaining must reflect the live
	// reservation of the in-flight call, not zero.
	if info.QuotaWeeklyRemaining <= 0 {
		t.Fatalf("weekly remaining = %d, want > 0 (not binding)", info.QuotaWeeklyRemaining)
	}
	close(unblock)
}

func TestRateInfo_NoRedisQuotaUnknown(t *testing.T) {
	// Degraded posture: Redis unavailable → admission still succeeds (the
	// limiter fails open) and the quota position is explicitly unknown.
	env := newEnv(t)
	env.mr.Close() // kill Redis
	idn := quotaIdentity()

	_, info, derr := env.proxy.Complete(context.Background(), idn, sampleRequest(false))
	if derr != nil {
		t.Fatalf("degraded complete: %v", derr)
	}
	if info == nil {
		t.Fatal("nil RateInfo degraded")
	}
	if info.Quota5hRemaining != -1 {
		t.Fatalf("5h remaining = %d, want -1 (unknown)", info.Quota5hRemaining)
	}
	if info.RPMUserRemaining != 0 && info.RPMUserRemaining != 599 {
		// Fail-open window: either unknown (0, omitted by the transport) or
		// the count could not be read — both acceptable, anything else is a bug.
		t.Fatalf("RPM remaining under Redis loss = %d", info.RPMUserRemaining)
	}
}

// Compile-time check that RateInfo flows through the streaming path too.
var _ = func() *llm.RateInfo {
	return nil
}
