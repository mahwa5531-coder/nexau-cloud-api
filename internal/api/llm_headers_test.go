package api

// llm_headers_test.go — X-Ratelimit-* emission rules:
//
//   - every configured dimension gets a Limit header; Remaining headers ride
//     along only when the value is known (>= 0)
//   - disabled dimensions and unknown values are OMITTED, never guessed
//   - 429-class errors carry the exhausted dimension's zero-remaining hint
//   - the streaming sink applies the same set while headers are mutable

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/llm"
)

func headerSnapshot(h http.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if len(v) > 0 {
			out[http.CanonicalHeaderKey(k)] = v[0]
		}
	}
	return out
}

func TestApplyRateHeaders_FullPosition(t *testing.T) {
	h := http.Header{}
	applyRateHeaders(h, &llm.RateInfo{
		RPMUserLimit:         600,
		RPMUserRemaining:     595,
		ConcUserLimit:        16,
		ConcUserActive:       3,
		Quota5hLimit:         2_000_000,
		Quota5hRemaining:     1_234_567,
		QuotaWeeklyLimit:     10_000_000,
		QuotaWeeklyRemaining: 9_000_000,
	})
	s := headerSnapshot(h)
	want := map[string]string{
		"X-Ratelimit-Limit-Requests":          "600",
		"X-Ratelimit-Remaining-Requests":      "595",
		"X-Ratelimit-Limit-Concurrency":       "16",
		"X-Ratelimit-Active-Concurrency":      "3",
		"X-Ratelimit-Limit-Tokens":            "2000000",
		"X-Ratelimit-Remaining-Tokens":        "1234567",
		"X-Ratelimit-Limit-Tokens-Weekly":     "10000000",
		"X-Ratelimit-Remaining-Tokens-Weekly": "9000000",
	}
	for k, v := range want {
		if s[k] != v {
			t.Fatalf("header %s = %q, want %q (all: %v)", k, s[k], v, s)
		}
	}
	if len(s) != len(want) {
		t.Fatalf("unexpected extra headers: %v", s)
	}
}

func TestApplyRateHeaders_OmitsUnknownAndDisabled(t *testing.T) {
	h := http.Header{}
	// Degraded run (Redis loss): quota unknown, concurrency claim-time count
	// unknown, no plan windows configured.
	applyRateHeaders(h, &llm.RateInfo{
		RPMUserLimit:     600,
		RPMUserRemaining: 599,
		Quota5hRemaining: -1,
	})
	s := headerSnapshot(h)
	if _, ok := s["X-Ratelimit-Limit-Requests"]; !ok {
		t.Fatalf("RPM limit missing: %v", s)
	}
	for _, forbidden := range []string{
		"X-Ratelimit-Remaining-Tokens",
		"X-Ratelimit-Limit-Tokens",
		"X-Ratelimit-Limit-Concurrency",
		"X-Ratelimit-Active-Concurrency",
		"X-Ratelimit-Limit-Tokens-Weekly",
	} {
		if _, ok := s[forbidden]; ok {
			t.Fatalf("disabled/unknown dimension emitted %s: %v", forbidden, s)
		}
	}

	// nil info: nothing at all (gate never reached).
	h2 := http.Header{}
	applyRateHeaders(h2, nil)
	if len(h2) != 0 {
		t.Fatalf("nil info emitted headers: %v", headerSnapshot(h2))
	}
}

func TestApplyRateHintHeaders_ExhaustedDimensionZeroed(t *testing.T) {
	cases := []struct {
		name string
		derr *domain.Error
		want map[string]string
		not  []string
	}{
		{
			name: "rpm",
			derr: domain.ErrRateLimited(41000, "llm_rpm"),
			want: map[string]string{"X-Ratelimit-Remaining-Requests": "0"},
		},
		{
			name: "concurrency",
			derr: domain.ErrConcurrencyLimited("user"),
			want: map[string]string{"X-Ratelimit-Active-Concurrency": "0"},
		},
		{
			name: "quota 5h",
			derr: domain.ErrWindowQuotaExceeded("5h", 3000, 3100, nil),
			want: map[string]string{"X-Ratelimit-Remaining-Tokens": "0"},
		},
		{
			name: "quota weekly",
			derr: domain.ErrWindowQuotaExceeded("weekly", 10_000_000, 10_000_001, nil),
			want: map[string]string{"X-Ratelimit-Remaining-Tokens-Weekly": "0"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			applyRateHintHeaders(h, &llm.RateInfo{RPMUserLimit: 600, RPMUserRemaining: 0}, tc.derr)
			s := headerSnapshot(h)
			for k, v := range tc.want {
				if s[k] != v {
					t.Fatalf("%s: %s = %q, want %q (%v)", tc.name, k, s[k], v, s)
				}
			}
			for _, k := range tc.not {
				if _, ok := s[k]; ok {
					t.Fatalf("%s: unexpected %s (%v)", tc.name, k, s)
				}
			}
		})
	}

	// nil error must not zero anything.
	h := http.Header{}
	applyRateHintHeaders(h, &llm.RateInfo{RPMUserLimit: 600, RPMUserRemaining: 42}, nil)
	if got := headerSnapshot(h)["X-Ratelimit-Remaining-Requests"]; got != "42" {
		t.Fatalf("nil error clobbered remaining: %q", got)
	}
}

func TestSSERawSinkSetRateHeaders(t *testing.T) {
	// The sink implements llm.RateHeaderSink and forwards to the same
	// emission rules — the streaming 200 carries the identical set.
	rec := httptest.NewRecorder()
	sink := &sseRawSink{w: rec}
	var rhs llm.RateHeaderSink = sink
	rhs.SetRateHeaders(&llm.RateInfo{RPMUserLimit: 600, RPMUserRemaining: 1})
	got := headerSnapshot(rec.Header())
	if got["X-Ratelimit-Limit-Requests"] != "600" || got["X-Ratelimit-Remaining-Requests"] != "1" {
		t.Fatalf("sink did not apply headers: %v", got)
	}
	// Applied while still mutable: nothing written yet (the recorder's
	// Code field defaults to 200), so the header map is live and the
	// future SSE 200 will carry them.
	if rec.Body.Len() != 0 {
		t.Fatalf("response body already written: %q", rec.Body.String())
	}
}
