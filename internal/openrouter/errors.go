package openrouter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"go.opentelemetry.io/otel/trace"
)

// netDialer aliases net.Dialer for transport construction.
type netDialer = net.Dialer

func traceSpanContext(ctx context.Context) trace.SpanContext {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc
	}
	return trace.SpanContext{}
}

func traceFormat(sc trace.SpanContext) string {
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
}

// ---------------------------------------------------------------------------
// Error normalization
//
// OpenRouter wire format (docs: errors-and-debugging):
//
//	{ "error": { "code": <number>, "message": "...",
//	             "metadata": { "error_type": "...", "provider_code": "...",
//	                           "limit_source": "..." } } }
//
// The same error object can ride inside a 200-OK SSE chunk (mid-stream
// failures after the response is committed) and inside a 200-OK JSON body
// (non-streaming provider failures) — both are detected and surfaced.
// ---------------------------------------------------------------------------

// ErrorResponse is OpenRouter's error body.
type ErrorResponse struct {
	Error ErrorField `json:"error"`

	// headerRetryAfter carries the HTTP Retry-After header (parsed); it is
	// not part of the wire body. withRetry honors it, bounded by
	// RetryMaxBackoff, instead of guessing the 429/503 pacing.
	headerRetryAfter time.Duration `json:"-"`
}

// ErrorField is the nested error detail. Code is a JSON NUMBER in the
// canonical envelope (HTTP-ish status); string codes echoed by some
// providers are tolerated.
type ErrorField struct {
	Code     json.RawMessage `json:"code,omitempty"`
	Message  string          `json:"message,omitempty"`
	Metadata *ErrorMetadata  `json:"metadata,omitempty"`
}

// ErrorMetadata carries OpenRouter's typed error classification.
type ErrorMetadata struct {
	ErrorType    string   `json:"error_type,omitempty"`    // e.g. rate_limit_exceeded, provider_overloaded, content_policy_violation
	ProviderCode string   `json:"provider_code,omitempty"` // original upstream code
	LimitSource  string   `json:"limit_source,omitempty"`  // e.g. openrouter_in_flight_budget
	Reasons      []string `json:"reasons,omitempty"`       // moderation rejection reasons
}

// CodeString returns the error code as a string regardless of JSON shape
// (number or quoted string).
func (e *ErrorField) CodeString() string {
	if e == nil || len(e.Code) == 0 {
		return ""
	}
	raw := strings.TrimSpace(string(e.Code))
	if len(raw) >= 2 && raw[0] == '"' {
		var s string
		if json.Unmarshal([]byte(raw), &s) == nil {
			return s
		}
	}
	return raw
}

// HTTPStatus returns the numeric error code (0 when absent or unparseable).
func (e *ErrorField) HTTPStatus() int {
	n, err := strconv.Atoi(e.CodeString())
	if err != nil {
		return 0
	}
	return n
}

// classify maps an upstream status (plus OpenRouter error metadata) onto
// the client-facing code, safe HTTP status and retryability. Single source
// of truth for the HTTP-error path, in-band stream errors and 200-with-
// error bodies.
//
// Upstream credential problems (401/403) are OUR problem: they never
// forward as 401 (which would suggest the client's token was wrong).
// Moderation rejections (403 with reasons) are the one 403 the client can
// act on and surface as such.
func classify(status int, md *ErrorMetadata) (code string, httpOut int, retryable bool) {
	switch {
	case status == http.StatusTooManyRequests:
		return "UPSTREAM_RATE_LIMITED", http.StatusTooManyRequests, true
	case status == http.StatusPaymentRequired:
		// 402 is terminal (credits / key limit) EXCEPT the transient
		// in-flight spending budget, which OpenRouter paces via Retry-After.
		if md != nil && md.LimitSource == "openrouter_in_flight_budget" {
			return "UPSTREAM_RATE_LIMITED", http.StatusTooManyRequests, true
		}
		return "UPSTREAM_CREDITS", http.StatusBadGateway, false
	case status == http.StatusRequestTimeout:
		return "UPSTREAM_TIMEOUT", http.StatusGatewayTimeout, true
	case status == http.StatusNotFound:
		return "MODEL_NOT_FOUND", http.StatusNotFound, false
	case status == http.StatusUnauthorized:
		return "UPSTREAM_AUTH", http.StatusBadGateway, false
	case status == http.StatusForbidden:
		if md != nil && (len(md.Reasons) > 0 || md.ErrorType == "content_policy_violation" || md.ErrorType == "refusal") {
			return "UPSTREAM_MODERATION", http.StatusForbidden, false
		}
		return "UPSTREAM_FORBIDDEN", http.StatusBadGateway, false
	case status >= 500: // 500 / 502 / 503 / 524 (edge timeout) / 529 (overloaded)
		return "UPSTREAM_UNAVAILABLE", http.StatusBadGateway, true
	default:
		return "UPSTREAM_BAD_REQUEST", http.StatusBadGateway, false
	}
}

// clientMessage renders the client-safe message per classification.
// Provider text (which leaks model names, org ids, flagged input) never
// reaches the client payload; it rides only in Cause for logs.
func clientMessage(code string) string {
	switch code {
	case "UPSTREAM_RATE_LIMITED":
		return "The model provider is rate limiting; retry shortly."
	case "UPSTREAM_CREDITS":
		return "The gateway's model-provider account has insufficient credits."
	case "UPSTREAM_TIMEOUT":
		return "The model provider timed out."
	case "MODEL_NOT_FOUND":
		return "The requested model was not found upstream."
	case "UPSTREAM_AUTH", "UPSTREAM_FORBIDDEN":
		return "The gateway's upstream credential was rejected."
	case "UPSTREAM_MODERATION":
		return "The request was rejected by the provider's safety policy."
	case "UPSTREAM_UNAVAILABLE":
		return "The model provider is temporarily unavailable."
	default:
		return "The model provider rejected the request."
	}
}

// errorFieldToDomain converts an OpenRouter error object into the stable
// domain error model. status is the HTTP status when known; in-band
// payloads carry the true status in error.code, which wins when present.
func errorFieldToDomain(ef *ErrorField, status int, headerRetryAfter time.Duration) error {
	if ef == nil {
		ef = &ErrorField{}
	}
	if s := ef.HTTPStatus(); s != 0 {
		status = s
	}
	md := ef.Metadata
	code, httpOut, retry := classify(status, md)
	de := &domain.Error{
		Code:    code,
		Message: clientMessage(code),
		HTTP:    httpOut,
		Details: map[string]any{"retryable": retry},
		Cause:   errors.New(ef.CodeString() + ": " + ef.Message),
	}
	if md != nil {
		if md.ErrorType != "" {
			de.Details["error_type"] = md.ErrorType
		}
		if md.ProviderCode != "" {
			de.Details["provider_code"] = md.ProviderCode
		}
	}
	if headerRetryAfter > 0 {
		de.Details["retry_after_ms"] = headerRetryAfter.Milliseconds()
	}
	return de
}

// toDomain converts a parsed error body into the domain error model.
func (be *ErrorResponse) toDomain(status int) error {
	return errorFieldToDomain(&be.Error, status, be.headerRetryAfter)
}

// readErrorBody parses the OpenRouter error envelope from an error-status
// body. The bool reports whether the envelope was recognized.
func readErrorBody(r io.Reader, status int, headerRetryAfter time.Duration) (error, bool) {
	raw, err := io.ReadAll(io.LimitReader(r, 1<<20))
	if err != nil || len(raw) == 0 {
		return statusError(status, headerRetryAfter), false
	}
	var be ErrorResponse
	if jerr := json.Unmarshal(raw, &be); jerr != nil || (be.Error.Message == "" && len(be.Error.Code) == 0) {
		return statusError(status, headerRetryAfter), false
	}
	be.headerRetryAfter = headerRetryAfter
	return be.toDomain(status), true
}

// statusError builds the domain error for a status whose body could not be
// parsed into the envelope.
func statusError(status int, headerRetryAfter time.Duration) error {
	code, httpOut, retry := classify(status, nil)
	de := &domain.Error{
		Code:    code,
		Message: clientMessage(code),
		HTTP:    httpOut,
		Details: map[string]any{"retryable": retry},
	}
	if retry && headerRetryAfter > 0 {
		de.Details["retry_after_ms"] = headerRetryAfter.Milliseconds()
	}
	return de
}

// parseRetryAfter decodes the HTTP Retry-After header: delta-seconds or
// HTTP-date (RFC 7231 §7.1.3). Bounded to 10 minutes — larger hints are
// treated as "none" (the retry policy's own backoff bounds pacing anyway).
func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		if secs < 0 {
			return 0
		}
		return boundedRetryAfter(time.Duration(secs) * time.Second)
	}
	if at, err := http.ParseTime(h); err == nil {
		return boundedRetryAfter(time.Until(at))
	}
	return 0
}

func boundedRetryAfter(d time.Duration) time.Duration {
	if d <= 0 || d > 10*time.Minute {
		return 0
	}
	return d
}

// classifyTransportError maps net/http layer failures to domain errors,
// flagging retryability.
func classifyTransportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return domain.ErrCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.ErrUpstreamTimeout("connect")
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return domain.ErrUpstreamTimeout("transport")
	}
	return domain.ErrUpstreamUnavailable()
}

// retryAfterFrom extracts a Retry-After hint encoded in the domain error.
func retryAfterFrom(err error) time.Duration {
	if de := asDomainErr(err); de != nil {
		if v, ok := de.Details["retry_after_ms"].(int64); ok && v > 0 {
			return time.Duration(v) * time.Millisecond
		}
	}
	return 0
}

// retryable decides whether an error (pre-first-byte) may be retried safely.
func retryable(err error) bool {
	if de := asDomainErr(err); de != nil {
		if v, ok := de.Details["retryable"].(bool); ok {
			return v
		}
		switch de.Code {
		case "UPSTREAM_UNAVAILABLE", "UPSTREAM_TIMEOUT", "MODEL_TIMEOUT", "UPSTREAM_RATE_LIMITED":
			return true
		}
	}
	return false
}

func asDomainErr(err error) *domain.Error {
	if err == nil {
		return nil
	}
	var de *domain.Error
	if errors.As(err, &de) {
		return de
	}
	return nil
}

func backoffWithJitter(min, max time.Duration, attempt int) time.Duration {
	if max <= min {
		return min
	}
	base := min << uint(attempt) // exponential: 1x, 2x, 4x …
	if base > max || base <= 0 {
		base = max
	}
	// Full jitter over [base/2, base].
	half := base / 2
	return half + time.Duration(rand.Int63n(maxI64(int64(half))))
}

func maxI64(a int64) int64 { return maxOf(1, a) }

func maxOf(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
