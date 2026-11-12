package openrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/reqctx"
)

// domainError keeps the test file readable.
type domainError = domain.Error

// streamBody feeds an SSE body through the StreamReader.
func streamBody(t *testing.T, lines string) *StreamReader {
	t.Helper()
	body := io.NopCloser(strings.NewReader(lines))
	return newStreamReader(context.Background(), body, nil, nil)
}

func chunkLine(t *testing.T, c *ChatChunk) string {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}
	return "data: " + string(b) + "\n\n"
}

func TestStreamParsesChunks(t *testing.T) {
	sr := streamBody(t, "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n"+
		"data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Revenue\"}}]}\n\n"+
		"data: [DONE]\n\n")

	n := 0
	for {
		c, raw, done, err := sr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if done {
			break
		}
		n++
		if c == nil || len(raw) == 0 {
			t.Fatalf("chunk %d: missing payload", n)
		}
	}
	if n != 2 {
		t.Fatalf("expected 2 chunks, got %d", n)
	}
	sr.Close()
}

func TestStreamIgnoresComments(t *testing.T) {
	sr := streamBody(t, ": ping\n: keepalive\n\n"+
		"data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n"+
		"data: [DONE]\n\n")
	c, _, done, err := sr.Next()
	if err != nil || done || c == nil {
		t.Fatalf("Next: err=%v done=%v c=%v", err, done, c)
	}
	if _, _, done, err := sr.Next(); err != nil || !done {
		t.Fatalf("expected DONE, err=%v done=%v", err, done)
	}
	sr.Close()
}

// TestStreamSkipsOpenRouterKeepalives pins the exact keepalive comment
// OpenRouter emits mid-stream (docs: streaming) — the parser must skip it
// without treating it as data or a protocol fault.
func TestStreamSkipsOpenRouterKeepalives(t *testing.T) {
	sr := streamBody(t, ": OPENROUTER PROCESSING\n\n"+
		"data: {\"id\":\"gen_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n"+
		": OPENROUTER PROCESSING\n\n"+
		": OPENROUTER PROCESSING\n\n"+
		"data: {\"id\":\"gen_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"}}]}\n\n"+
		": OPENROUTER PROCESSING\n\n"+
		"data: {\"id\":\"gen_1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5,\"cost\":0.00001,\"is_byok\":true}}\n\n"+
		"data: [DONE]\n\n")

	var text strings.Builder
	var usage *Usage
	for {
		c, _, done, err := sr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if done {
			break
		}
		if len(c.Choices) > 0 {
			text.WriteString(c.Choices[0].Delta.Content)
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if text.String() != "ab" {
		t.Fatalf("content = %q, want ab", text.String())
	}
	if usage == nil {
		t.Fatal("final usage chunk missing")
	}
	if usage.TotalTokens != 5 || usage.Cost == nil || *usage.Cost != 0.00001 {
		t.Fatalf("usage = %+v", usage)
	}
	if usage.IsByok == nil || !*usage.IsByok {
		t.Fatalf("is_byok must decode: %+v", usage)
	}
	sr.Close()
}

func TestStreamInBandError(t *testing.T) {
	// OpenRouter mid-stream failure shape: top-level error object with a
	// numeric code and a terminal finish_reason "error".
	line := `data: {"id":"cmpl_1","object":"chat.completion.chunk","created":1,"model":"openai/gpt-4o","provider":"openai","error":{"code":429,"message":"Rate limit exceeded","metadata":{"error_type":"rate_limit_exceeded"}},"choices":[{"index":0,"delta":{"content":""},"finish_reason":"error"}]}` + "\n\n"
	sr := streamBody(t, line)
	_, _, _, err := sr.Next()
	if err == nil {
		t.Fatal("in-band error must surface")
	}
	de, ok := err.(*domainError)
	if !ok {
		t.Fatalf("error must be domain.Error, got %T", err)
	}
	if de.Code != "UPSTREAM_RATE_LIMITED" {
		t.Fatalf("code = %q, want UPSTREAM_RATE_LIMITED", de.Code)
	}
	if de.Details["retryable"] != true {
		t.Fatalf("429 in-band must be retryable: %v", de.Details)
	}
	if de.Details["error_type"] != "rate_limit_exceeded" {
		t.Fatalf("error_type detail missing: %v", de.Details)
	}
	sr.Close()
}

func TestStreamMalformedChunk(t *testing.T) {
	sr := streamBody(t, "data: {not json}\n\n")
	if _, _, _, err := sr.Next(); err == nil {
		t.Fatal("malformed chunk must error")
	}
	sr.Close()
}

func TestStreamUnexpectedEOF(t *testing.T) {
	sr := streamBody(t, "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
	// EOF without [DONE]: protocol termination.
	sr.Next()
	if _, _, _, err := sr.Next(); err == nil {
		t.Fatal("EOF without DONE must error")
	}
	sr.Close()
}

func TestStreamIdleWatchdogFires(t *testing.T) {
	// A body that never sends data: the idle watchdog (300ms here) cancels.
	ctx := WithIdleTimeout(context.Background(), 250*time.Millisecond)
	body := io.NopCloser(strings.NewReader(""))
	sr := newStreamReader(ctx, body, nil, nil)
	start := time.Now()
	_, _, _, err := sr.Next()
	if err == nil {
		t.Fatal("idle stream must abort")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("watchdog too slow: %v", time.Since(start))
	}
	sr.Close()
}

func TestStreamCloseIsIdempotent(t *testing.T) {
	sr := streamBody(t, "data: [DONE]\n\n")
	if err := sr.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	_ = sr.Close() // must not panic
}

func TestStreamSSEFieldSplit(t *testing.T) {
	cases := []struct {
		line  string
		field string
		value string
	}{
		{"data: x", "data", "x"},
		{"data:x", "data", "x"},
		{"event: foo", "event", "foo"},
		{"id: 42\r", "id", "42"},
	}
	for _, c := range cases {
		f, v, ok := splitSSELine([]byte(c.line))
		if !ok || string(f) != c.field || string(v) != c.value {
			t.Errorf("splitSSELine(%q) = (%q,%q,%v), want (%q,%q)", c.line, f, v, ok, c.field, c.value)
		}
	}
}

func TestChunkErrorDetection(t *testing.T) {
	cases := []struct {
		name  string
		chunk ChatChunk
		want  bool
	}{
		{"plain", ChatChunk{ID: "1"}, false},
		{"error-field-message", ChatChunk{Error: &ErrorField{Message: "x"}}, true},
		{"error-field-numeric-code", ChatChunk{Error: &ErrorField{Code: json.RawMessage("429")}}, true},
		{"empty-error-field", ChatChunk{Error: &ErrorField{}}, false},
	}
	for _, c := range cases {
		if got := c.chunk.IsError(); got != c.want {
			t.Errorf("%s: IsError=%v want %v", c.name, got, c.want)
		}
	}
}

// TestUsageDecodesOpenRouterShape pins the OpenRouter usage object: native
// token counts, cached/reasoning details, cost as a NUMBER, is_byok flag
// and cost_details.
func TestUsageDecodesOpenRouterShape(t *testing.T) {
	raw := `{
		"prompt_tokens": 1123,
		"completion_tokens": 287,
		"total_tokens": 1410,
		"prompt_tokens_details": {"cached_tokens": 768},
		"completion_tokens_details": {"reasoning_tokens": 132},
		"cost": 0.002381,
		"is_byok": true,
		"cost_details": {
			"upstream_inference_prompt_cost": 0.0015,
			"upstream_inference_completions_cost": 0.000881,
			"upstream_inference_cost": 0.002381
		}
	}`
	var u Usage
	if err := json.Unmarshal([]byte(raw), &u); err != nil {
		t.Fatalf("unmarshal usage: %v", err)
	}
	if u.PromptTokens != 1123 || u.CompletionTokens != 287 || u.TotalTokens != 1410 {
		t.Fatalf("tokens: %+v", u)
	}
	if u.PromptTokensDetails == nil || u.PromptTokensDetails.CachedTokens != 768 {
		t.Fatalf("cached tokens: %+v", u.PromptTokensDetails)
	}
	if u.CompletionTokensDetails == nil || u.CompletionTokensDetails.ReasoningTokens != 132 {
		t.Fatalf("reasoning tokens: %+v", u.CompletionTokensDetails)
	}
	if u.Cost == nil || *u.Cost != 0.002381 {
		t.Fatalf("cost: %+v", u.Cost)
	}
	if u.IsByok == nil || !*u.IsByok {
		t.Fatalf("is_byok: %+v", u.IsByok)
	}
	if u.CostDetails == nil || u.CostDetails.UpstreamInferencePromptCost != 0.0015 {
		t.Fatalf("cost_details: %+v", u.CostDetails)
	}
}

// --- error normalization ----------------------------------------------------

func TestErrorNormalization(t *testing.T) {
	be := &ErrorResponse{
		Error: ErrorField{
			Code:     json.RawMessage("500"),
			Message:  "slow down",
			Metadata: &ErrorMetadata{ErrorType: "server", ProviderCode: "500"},
		},
	}
	err := be.toDomain(500)
	de, ok := err.(*domainError)
	if !ok {
		t.Fatalf("must be domain error, got %T", err)
	}
	if de.Code != "UPSTREAM_UNAVAILABLE" {
		t.Fatalf("code: %q", de.Code)
	}
	if de.HTTP != http.StatusBadGateway {
		t.Fatalf("status: %d", de.HTTP)
	}
	if de.Details["retryable"] != true {
		t.Fatalf("500 must be retryable")
	}
	if de.Details["error_type"] != "server" || de.Details["provider_code"] != "500" {
		t.Fatalf("metadata details missing: %v", de.Details)
	}
	// Client message must not leak provider text.
	if strings.Contains(de.Message, "slow down") {
		t.Fatal("provider message must not reach the client payload")
	}
}

func TestClassification(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		md            *ErrorMetadata
		wantCode      string
		wantHTTP      int
		wantRetryable bool
	}{
		{"400", 400, nil, "UPSTREAM_BAD_REQUEST", 502, false},
		{"401", 401, nil, "UPSTREAM_AUTH", 502, false},
		{"402 credits", 402, nil, "UPSTREAM_CREDITS", 502, false},
		{"402 in-flight budget", 402, &ErrorMetadata{LimitSource: "openrouter_in_flight_budget"}, "UPSTREAM_RATE_LIMITED", 429, true},
		{"403 permission", 403, nil, "UPSTREAM_FORBIDDEN", 502, false},
		{"403 moderation reasons", 403, &ErrorMetadata{Reasons: []string{"hate"}}, "UPSTREAM_MODERATION", 403, false},
		{"403 content policy", 403, &ErrorMetadata{ErrorType: "content_policy_violation"}, "UPSTREAM_MODERATION", 403, false},
		{"404", 404, nil, "MODEL_NOT_FOUND", 404, false},
		{"408", 408, nil, "UPSTREAM_TIMEOUT", 504, true},
		{"429", 429, nil, "UPSTREAM_RATE_LIMITED", 429, true},
		{"500", 500, nil, "UPSTREAM_UNAVAILABLE", 502, true},
		{"502", 502, nil, "UPSTREAM_UNAVAILABLE", 502, true},
		{"503", 503, nil, "UPSTREAM_UNAVAILABLE", 502, true},
		{"524 edge timeout", 524, nil, "UPSTREAM_UNAVAILABLE", 502, true},
		{"529 overloaded", 529, nil, "UPSTREAM_UNAVAILABLE", 502, true},
	}
	for _, c := range cases {
		code, httpOut, retry := classify(c.status, c.md)
		if code != c.wantCode || httpOut != c.wantHTTP || retry != c.wantRetryable {
			t.Errorf("%s: classify() = (%q,%d,%v), want (%q,%d,%v)",
				c.name, code, httpOut, retry, c.wantCode, c.wantHTTP, c.wantRetryable)
		}
	}
}

func TestErrorFieldCodeShapes(t *testing.T) {
	num := &ErrorField{Code: json.RawMessage("429")}
	if num.HTTPStatus() != 429 || num.CodeString() != "429" {
		t.Fatalf("numeric code: %d %q", num.HTTPStatus(), num.CodeString())
	}
	str := &ErrorField{Code: json.RawMessage(`"rate_limited"`)}
	if str.HTTPStatus() != 0 || str.CodeString() != "rate_limited" {
		t.Fatalf("string code: %d %q", str.HTTPStatus(), str.CodeString())
	}
	empty := &ErrorField{}
	if empty.HTTPStatus() != 0 || empty.CodeString() != "" {
		t.Fatalf("empty code: %d %q", empty.HTTPStatus(), empty.CodeString())
	}
}

func TestClassifyTransportError(t *testing.T) {
	if classifyTransportError(context.Canceled) == nil {
		t.Fatal("canceled must classify")
	}
	if classifyTransportError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}

// --- client retry policy against a mock OpenRouter --------------------------

func TestClientRetriesPreFirstByte(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			// 503 twice, then success: retryable pre-first-byte failures.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":503,"message":"upstream down"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","created":1,"model":"openai/gpt-4o","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{
		BaseURL: srv.URL, MaxRetries: 3,
		RetryMinBackoff: time.Millisecond, RetryMaxBackoff: 5 * time.Millisecond,
	}, nil)
	resp, err := c.Completion(context.Background(), &ChatRequest{Model: "openai/gpt-4o"})
	if err != nil {
		t.Fatalf("Completion after retries: %v", err)
	}
	if resp.ID != "1" {
		t.Fatalf("bad response: %+v", resp)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("expected 3 attempts, got %d", n)
	}
}

func TestClientNoRetryOn4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":400,"message":"bad param"}}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL, MaxRetries: 3, RetryMinBackoff: time.Millisecond}, nil)
	_, err := c.Completion(context.Background(), &ChatRequest{Model: "openai/gpt-4o"})
	if err == nil {
		t.Fatal("400 must fail")
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("4xx must not retry, attempts=%d", n)
	}
}

// TestClientRetries200WithErrorBody pins the OpenRouter quirk where a
// provider failure lands in a committed 200 body: retryable classes re-run,
// terminal classes fail on the first attempt.
func TestClientRetries200WithErrorBody(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// 200 + provider failure: retryable per classification.
		_, _ = w.Write([]byte(`{"id":"gen_1","object":"chat.completion","created":1,"model":"openai/gpt-4o","error":{"code":503,"message":"no available provider"},"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL, MaxRetries: 2,
		RetryMinBackoff: time.Millisecond, RetryMaxBackoff: 2 * time.Millisecond}, nil)
	_, err := c.Completion(context.Background(), &ChatRequest{Model: "openai/gpt-4o"})
	if err == nil {
		t.Fatal("200-with-error must fail")
	}
	de, ok := err.(*domainError)
	if !ok || de.Code != "UPSTREAM_UNAVAILABLE" {
		t.Fatalf("error = %+v", err)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("retryable in-band error must exhaust attempts, got %d", n)
	}
}

func TestClient200WithErrorBodyTerminalNoRetry(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"gen_1","object":"chat.completion","created":1,"model":"openai/gpt-4o","error":{"code":400,"message":"context length exceeded"}}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL, MaxRetries: 2,
		RetryMinBackoff: time.Millisecond, RetryMaxBackoff: 2 * time.Millisecond}, nil)
	_, err := c.Completion(context.Background(), &ChatRequest{Model: "openai/gpt-4o"})
	if err == nil {
		t.Fatal("terminal in-band error must fail")
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("terminal class must not retry, attempts=%d", n)
	}
}

func TestClientStreamCancellation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		flusher.Flush()
		// Hold the stream open until the client cancels.
		<-r.Context().Done()
		close(release)
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	sr, err := c.CompletionStream(ctx, &ChatRequest{Model: "openai/gpt-4o", Stream: true})
	if err != nil {
		t.Fatalf("stream open: %v", err)
	}
	<-started
	cancel()
	// The reader must observe cancellation promptly (Next errors out).
	errCh := make(chan error, 1)
	go func() {
		_, _, _, err := sr.Next()
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("cancellation must terminate the stream read")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream read did not observe cancellation")
	}
	select {
	case <-release:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream did not observe cancellation")
	}
	sr.Close()
}

func TestClientStreamChunksIncrementally(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"c%d\"}}]}\n\n", i)
			flusher.Flush()
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL}, nil)
	sr, err := c.CompletionStream(context.Background(), &ChatRequest{Model: "openai/gpt-4o", Stream: true})
	if err != nil {
		t.Fatalf("stream open: %v", err)
	}
	defer sr.Close()

	var got []string
	for {
		chunk, _, done, err := sr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if done {
			break
		}
		if len(chunk.Choices) > 0 {
			got = append(got, chunk.Choices[0].Delta.Content)
		}
	}
	if strings.Join(got, "") != "c0c1c2" {
		t.Fatalf("chunks: %v", got)
	}
}

// TestClientPropagatesHeaders pins upstream correlation and attribution:
// x-request-id, x-mash-call-id, Authorization and the optional OpenRouter
// attribution headers must arrive on every upstream request when set.
func TestClientPropagatesHeaders(t *testing.T) {
	var gotReqID, gotCallID, gotAuth, gotReferer, gotTitle string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReqID = r.Header.Get("x-request-id")
		gotCallID = r.Header.Get("x-mash-call-id")
		gotAuth = r.Header.Get("Authorization")
		gotReferer = r.Header.Get("HTTP-Referer")
		gotTitle = r.Header.Get("X-OpenRouter-Title")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","object":"chat.completion","created":1,"model":"openai/gpt-4o","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}]}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{
		BaseURL: srv.URL, APIKey: "sk-or-v1-testkey",
		Referer: "https://mash.example.app", Title: "MASh",
	}, nil)
	ctx := reqctx.WithRequestID(context.Background(), "req_corr_1")
	ctx = reqctx.WithCallID(ctx, "call_corr_1")
	if _, err := c.Completion(ctx, &ChatRequest{Model: "openai/gpt-4o"}); err != nil {
		t.Fatalf("Completion: %v", err)
	}
	if gotReqID != "req_corr_1" {
		t.Fatalf("x-request-id = %q, want req_corr_1", gotReqID)
	}
	if gotCallID != "call_corr_1" {
		t.Fatalf("x-mash-call-id = %q, want call_corr_1", gotCallID)
	}
	if gotAuth != "Bearer sk-or-v1-testkey" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotReferer != "https://mash.example.app" {
		t.Fatalf("HTTP-Referer = %q", gotReferer)
	}
	if gotTitle != "MASh" {
		t.Fatalf("X-OpenRouter-Title = %q", gotTitle)
	}
}

// TestHealthProbesKeyEndpoint pins the readiness probe: GET {base}/v1/key
// with the service credential; 200 = healthy, anything else = error.
func TestHealthProbesKeyEndpoint(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if gotAuth != "Bearer sk-or-v1-testkey" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":401,"message":"User not found."}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"label":"sk-or-v1-au7...890","is_free_tier":false}}`))
	}))
	defer srv.Close()

	c := NewClient(TransportConfig{BaseURL: srv.URL, APIKey: "sk-or-v1-testkey"}, nil)
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health with valid key: %v", err)
	}
	if gotPath != "/v1/key" {
		t.Fatalf("probe path = %q, want /v1/key", gotPath)
	}

	bad := NewClient(TransportConfig{BaseURL: srv.URL, APIKey: "sk-or-v1-wrong"}, nil)
	if err := bad.Health(context.Background()); err == nil {
		t.Fatal("invalid key must fail health")
	}
	_ = gotAuth
}
