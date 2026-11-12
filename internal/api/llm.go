package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mash-cloud/mash-api/internal/auth"
	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/observability"
	"github.com/mash-cloud/mash-api/internal/reqctx"
)

// handleChatCompletions: POST /v1/chat/completions
//
// THE LLM endpoint of the cloud: an OpenAI-compatible pass-through tunnel to
// OpenRouter (direct upstream). The desktop agent runtime points its
// OpenAI-style client at this endpoint (model = "provider/model"); everything
// agent-related (sessions, transcripts, tools, retries) stays on the desktop.
//
// Full pipeline enforcement happens in the proxy (auth → account state →
// validation → model entitlement → rolling 5h/weekly token windows →
// RPM + concurrency limits → OpenRouter). Raw upstream SSE bytes are forwarded
// verbatim after validation — no re-serialization on the hot path. One
// llm_calls row meters the fact.
//
// Stream semantics (stream=true): SSE data: frames + [DONE], lazy headers —
// the HTTP 200 + SSE headers commit only when the first upstream byte is
// ready, so pre-stream upstream failures still answer with a real HTTP error
// (OpenAI SDKs observe status + code). A failure after bytes flowed surfaces
// as an in-band OpenAI-style error event before [DONE].
func (a *API) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	idn := auth.FromIdentity(r.Context())
	if idn == nil {
		writeError(w, r, domain.ErrUnauthorized(nil))
		return
	}

	// Count the body as it is consumed so the PLAN's request-size ceiling
	// can be enforced after decoding (audit L7: the value was resolved,
	// cached and advertised, but only the global 2 MiB cap was ever applied
	// — a plan promising 512 KB happily accepted anything under 2 MiB).
	cr := &countingReadCloser{rc: r.Body}
	r.Body = cr
	var req llm.Request
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, domain.ErrValidation("invalid chat payload: "+jsonErrText(err)))
		return
	}
	if idn.Limits.MaxRequestBytes > 0 && cr.n > idn.Limits.MaxRequestBytes {
		writeError(w, r, domain.ErrPayloadTooLarge(idn.Limits.MaxRequestBytes))
		return
	}

	if !req.Stream {
		// Synchronous path: gates → OpenRouter → the completion JSON body.
		body, info, derr := a.proxy.Complete(r.Context(), idn, &req)
		if derr != nil {
			applyRateHintHeaders(w.Header(), info, derr)
			writeError(w, r, derr.WithRequestID(reqctx.RequestID(r.Context())))
			return
		}
		applyRateHeaders(w.Header(), info)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}

	// Access log for THE endpoint (audit L10): every other route logs, but
	// the chat stream was skipped — support could never prove 'was user X
	// served at 02:59, on which model, for how long'. One line at start,
	// one at completion; never bodies.
	start := time.Now()
	observability.LogInfo("chat stream started",
		"user_id", idn.User.ID, "tenant_id", idn.Tenant.ID,
		"model", req.Model, "stream", req.Stream,
		"request_id", reqctx.RequestID(r.Context()))
	defer func() {
		observability.LogInfo("chat stream finished",
			"user_id", idn.User.ID, "model", req.Model,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", reqctx.RequestID(r.Context()))
	}()

	// Streaming path: raw SSE pass-through with a LAZY writer. The proxy
	// hands the admission position to the sink (RateHeaderSink) while the
	// headers are still mutable — the X-Ratelimit-* set rides the SSE 200.
	sink := &sseRawSink{
		w:          w,
		deadline:   a.cfg.StreamWriteDeadline,
		hbInterval: a.cfg.StreamHeartbeatInterval,
		once:       sync.Once{},
	}
	info, derr := a.proxy.Stream(r.Context(), idn, &req, sink)
	if !sink.Started() {
		// Nothing ever reached the client: upstream failed pre-first-byte or
		// the transport could not start. Either way the client receives a
		// real HTTP error, never a bare 200.
		if derr == nil {
			// Audit L5: an EMPTY upstream answer (zero data chunks,
			// straight to [DONE]) is a legitimate model response, not a
			// server fault — commit a clean, empty 200 SSE stream and
			// let the desktop decide how to present it. The old path
			// fabricated a 500 here, which clients dutifully retried,
			// burning RPM budget on a deterministic non-error.
			applyRateHeaders(w.Header(), info)
			if sink.ensureStarted() == nil {
				sink.Finish()
				return
			}
			derr = domain.ErrInternal(nil) // headers could not be committed
		}
		applyRateHintHeaders(w.Header(), info, derr)
		writeError(w, r, derr.WithRequestID(reqctx.RequestID(r.Context())))
		return
	}
	sink.Finish() // stop heartbeat + terminal [DONE] frame + close
}

// applyRateHeaders writes the X-Ratelimit-* client hints (the industry
// convention — OpenAI-style header names) from the admission position so a
// desktop agent can render a live meter without polling /v1/me. Dimensions
// the plan does not set and values the gates could not compute are omitted
// rather than guessed. On the wire the names arrive lowercased by HTTP
// convention (x-ratelimit-limit-requests …); Go canonicalizes the spelling.
func applyRateHeaders(h http.Header, info *llm.RateInfo) {
	if info == nil {
		return
	}
	if info.RPMUserLimit > 0 {
		h.Set("X-Ratelimit-Limit-Requests", strconv.FormatInt(info.RPMUserLimit, 10))
		if info.RPMUserRemaining >= 0 {
			h.Set("X-Ratelimit-Remaining-Requests", strconv.FormatInt(info.RPMUserRemaining, 10))
		}
	}
	if info.ConcUserLimit > 0 {
		h.Set("X-Ratelimit-Limit-Concurrency", strconv.FormatInt(info.ConcUserLimit, 10))
		if info.ConcUserActive > 0 {
			h.Set("X-Ratelimit-Active-Concurrency", strconv.FormatInt(info.ConcUserActive, 10))
		}
	}
	if info.Quota5hLimit > 0 {
		h.Set("X-Ratelimit-Limit-Tokens", strconv.FormatInt(info.Quota5hLimit, 10))
		if info.Quota5hRemaining >= 0 {
			h.Set("X-Ratelimit-Remaining-Tokens", strconv.FormatInt(info.Quota5hRemaining, 10))
		}
	}
	if info.QuotaWeeklyLimit > 0 {
		h.Set("X-Ratelimit-Limit-Tokens-Weekly", strconv.FormatInt(info.QuotaWeeklyLimit, 10))
		if info.QuotaWeeklyRemaining >= 0 {
			h.Set("X-Ratelimit-Remaining-Tokens-Weekly", strconv.FormatInt(info.QuotaWeeklyRemaining, 10))
		}
	}
}

// applyRateHintHeaders decorates 429-class errors with the exhausted
// dimension's zero-remaining header (plus the full position the gates did
// compute) — headers are still mutable because nothing has been written yet.
func applyRateHintHeaders(h http.Header, info *llm.RateInfo, derr *domain.Error) {
	if derr == nil {
		applyRateHeaders(h, info)
		return
	}
	applyRateHeaders(h, info)
	switch derr.Code {
	case "RATE_LIMITED":
		h.Set("X-Ratelimit-Remaining-Requests", "0")
	case "CONCURRENCY_LIMIT":
		h.Set("X-Ratelimit-Active-Concurrency", "0")
	case "WINDOW_QUOTA_EXCEEDED":
		if w, _ := derr.Details["window"].(string); w == "weekly" {
			h.Set("X-Ratelimit-Remaining-Tokens-Weekly", "0")
		} else {
			h.Set("X-Ratelimit-Remaining-Tokens", "0")
		}
	}
}

// sseRawSink writes raw chunk payloads inside SSE framing. The SSE writer —
// and therefore the HTTP 200 + stream headers — is created lazily on the
// first forwarded byte so pre-stream upstream failures can still be answered
// with a proper HTTP error.
type sseRawSink struct {
	w          http.ResponseWriter
	deadline   time.Duration
	hbInterval time.Duration
	once       sync.Once

	mu     sync.Mutex
	sse    *llm.SSEWriter
	hbStop context.CancelFunc
}

// SetRateHeaders implements llm.RateHeaderSink: the proxy calls it right
// after admission, before any byte commits the response — the X-Ratelimit-*
// set rides the SSE 200 itself.
func (s *sseRawSink) SetRateHeaders(info *llm.RateInfo) {
	applyRateHeaders(s.w.Header(), info)
}

// start commits the response (200 + SSE headers) on first use and arms the
// heartbeat. Idempotent.
func (s *sseRawSink) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sse == nil {
		sse, err := llm.NewSSEWriter(s.w, s.deadline)
		if err != nil {
			return err
		}
		s.sse = sse
		hbCtx, stop := context.WithCancel(context.Background())
		s.hbStop = stop
		go sse.Heartbeat(hbCtx, s.hbInterval)
	}
	return nil
}

// Started reports whether the response has been committed and bytes flowed.
func (s *sseRawSink) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sse != nil
}

// SendChunk forwards one raw payload inside SSE framing, committing the
// response lazily on first use.
func (s *sseRawSink) SendChunk(raw []byte) error {
	if err := s.start(); err != nil {
		return err
	}
	return s.sse.Event(raw)
}

// ensureStarted commits 200 + SSE headers without forwarding any payload —
// the empty-but-successful stream shape (audit L5). Exposed for the handler's
// empty-answer path only.
func (s *sseRawSink) ensureStarted() error { return s.start() }

// SendError writes one in-band OpenAI-style error object as an SSE event —
// the wire shape OpenAI SDKs treat as a streamed failure.
func (s *sseRawSink) SendError(errType, code, message string) error {
	blob, err := json.Marshal(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    errType,
			"code":    code,
		},
	})
	if err != nil {
		return err
	}
	return s.SendChunk(blob)
}

// Finish terminates the SSE stream: stops the heartbeat, writes the [DONE]
// sentinel, closes. No-op when nothing was ever started.
func (s *sseRawSink) Finish() {
	s.mu.Lock()
	sse := s.sse
	stop := s.hbStop
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	if sse == nil {
		return
	}
	_ = sse.Event([]byte("[DONE]"))
	sse.Close()
}

// countingReadCloser counts bytes as they are consumed (audit L7).
type countingReadCloser struct {
	rc io.ReadCloser
	n  int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error { return c.rc.Close() }
