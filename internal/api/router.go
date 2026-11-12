// Package api assembles the HTTP surface: route table, JSON encoding,
// auth-wrapped handlers and the SSE stream transport. Handlers are thin: they
// decode, delegate to services, and encode — business rules never live here.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mash-cloud/mash-api/internal/auth"
	"github.com/mash-cloud/mash-api/internal/config"
	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/llm"
	"github.com/mash-cloud/mash-api/internal/middleware"
	"github.com/mash-cloud/mash-api/internal/observability"
	"github.com/mash-cloud/mash-api/internal/openrouter"
	"github.com/mash-cloud/mash-api/internal/payment"
	"github.com/mash-cloud/mash-api/internal/ratelimit"
	"github.com/mash-cloud/mash-api/internal/store"
	"github.com/mash-cloud/mash-api/internal/store/repos"
)

// API holds the handler dependencies.
type API struct {
	cfg      *config.Config
	authSvc  *auth.Service
	authMW   auth.Middleware
	proxy    *llm.Proxy
	usage    *repos.LLMCallsRepo
	pg       *store.Postgres
	redis    *store.Redis
	upstream *openrouter.Client
	lim      *ratelimit.Limiter
	metrics  *observability.Metrics
	otel     *observability.OTel
	payments *payment.Service
}

// New builds the API.
func New(
	cfg *config.Config,
	authSvc *auth.Service,
	authMW auth.Middleware,
	proxy *llm.Proxy,
	usage *repos.LLMCallsRepo,
	pg *store.Postgres,
	rd *store.Redis,
	or *openrouter.Client,
	lim *ratelimit.Limiter,
	metrics *observability.Metrics,
	otel *observability.OTel,
	payments *payment.Service,
) *API {
	return &API{
		cfg: cfg, authSvc: authSvc, authMW: authMW, proxy: proxy,
		usage: usage, pg: pg, redis: rd, upstream: or, lim: lim,
		metrics: metrics, otel: otel, payments: payments,
	}
}

// Router builds the full route table (Go 1.22+ pattern routing).
//
// Surface (14 routes + health/metrics):
//
//	POST /v1/auth/refresh               rotate the desktop token pair
//	POST /v1/auth/logout                revoke one refresh token
//	POST /v1/auth/logout-all            revoke every session (lost device)
//	GET  /v1/auth/oauth/google          begin "Continue with Google" (302)
//	GET  /v1/auth/oauth/google/callback finish OAuth → web success page
//	POST /v1/auth/web/session           grant → short web access token
//	POST /v1/auth/desktop/code          (authed) mint single-use mcode
//	POST /v1/auth/desktop/exchange      mcode → full desktop TokenPair
//	GET  /v1/me                         identity + plan + quota position
//	GET  /v1/config                     client integration config
//	POST /v1/chat/completions           THE LLM endpoint (SSE tunnel to OpenRouter)
//	GET  /v1/usage                      usage aggregation window
//	POST /v1/payments/*                 plan purchases (PhonePe default)
func (a *API) Router() http.Handler {
	mux := http.NewServeMux()

	// routeMethods remembers every (method, path) pair registered below so
	// the 404 wrapper can answer a method mismatch with a real 405 + Allow
	// header (audit L1: the custom 404 used to swallow Go's built-in 405 —
	// every 'wrong verb first' debugger paid the phantom-404 tax, and
	// probing clients lost the Allow hint entirely).
	routeMethods := map[string]map[string]bool{}

	register := func(pattern string, h http.Handler) {
		mux.Handle(pattern, h)
		if method, path, ok := strings.Cut(pattern, " "); ok {
			if routeMethods[path] == nil {
				routeMethods[path] = map[string]bool{}
			}
			routeMethods[path][method] = true
		}
	}
	registerFunc := func(pattern string, h http.HandlerFunc) {
		register(pattern, h)
	}

	// Health (unauthenticated, cheap — never tied to LLM availability).
	registerFunc("GET /health/live", a.handleLiveness)
	registerFunc("GET /health/ready", a.handleReadiness)
	if a.cfg.OTel.Prometheus && a.otel != nil && a.otel.PromHandler != nil {
		register("GET /metrics", a.otel.PromHandler)
	}

	// Auth (Google OAuth is the only identity provider).
	registerFunc("POST /v1/auth/refresh", a.handleRefresh)
	register("POST /v1/auth/logout", a.authMW.Require(http.HandlerFunc(a.handleLogout)))
	register("POST /v1/auth/logout-all", a.authMW.Require(http.HandlerFunc(a.handleLogoutAll)))

	// Google OAuth + web-to-desktop handshake. The OAuth pair mounts only
	// when the provider is fully configured (main.go leaves Service.Google
	// nil otherwise); the JSON endpoints require the local signer.
	if a.authSvc != nil && a.authSvc.Google != nil {
		registerFunc("GET /v1/auth/oauth/google", a.handleGoogleOAuthBegin)
		registerFunc("GET /v1/auth/oauth/google/callback", a.handleGoogleOAuthCallback)
	}
	if a.authSvc != nil && a.authSvc.Signer != nil {
		registerFunc("POST /v1/auth/web/session", a.handleWebSession)
		register("POST /v1/auth/desktop/code", a.authMW.RequireDevice(http.HandlerFunc(a.handleDesktopCode)))
		registerFunc("POST /v1/auth/desktop/exchange", a.handleDesktopExchange)
	}

	// Identity & client config. /v1/me and /v1/usage are read-budgeted
	// (audit M7): both run llm_calls SUM aggregations, and an unthrottled
	// polling loop on either route turns the API into a query engine for
	// the giant table. Same sliding-window machinery as the LLM path,
	// separate budget (NEXAU_RATE_READ_RPM).
	register("GET /v1/me", a.authMW.Require(a.readLimited(http.HandlerFunc(a.handleMe))))
	register("GET /v1/config", a.authMW.Require(http.HandlerFunc(a.handleClientConfig)))

	// The LLM endpoint: OpenAI-compatible tunnel to OpenRouter
	// (agent runtime lives on the desktop; this is its model access).
	// DEVICE-ONLY (audit M3): a browser web-session token must never be an
	// LLM credential — the web origin's XSS blast radius stays a profile
	// view, not the tenant's entire token budget.
	register("POST /v1/chat/completions", a.authMW.RequireDevice(http.HandlerFunc(a.handleChatCompletions)))

	// Usage. DEVICE-ONLY (audit M3): the desktop's usage dashboard.
	register("GET /v1/usage", a.authMW.RequireDevice(a.readLimited(http.HandlerFunc(a.handleUsage))))

	// Payments (plan purchases — plans only, no credit packs). Routes
	// mount regardless of provider mode: disabled deployments answer
	// 503 PAYMENTS_DISABLED, keeping the client contract stable.
	registerFunc("POST /v1/payments/webhook", a.handlePaymentWebhook)
	register("GET /v1/payments/catalog", a.authMW.Require(http.HandlerFunc(a.handlePaymentCatalog)))
	register("POST /v1/payments/checkout", a.authMW.Require(http.HandlerFunc(a.handlePaymentCheckout)))
	register("POST /v1/payments/confirm", a.authMW.Require(http.HandlerFunc(a.handlePaymentConfirm)))
	register("GET /v1/payments/orders/{order_id}", a.authMW.Require(http.HandlerFunc(a.handlePaymentOrder)))
	register("GET /v1/payments/history", a.authMW.Require(http.HandlerFunc(a.handlePaymentHistory)))
	register("GET /v1/payments/subscription", a.authMW.Require(http.HandlerFunc(a.handlePaymentSubscription)))

	return a.wrapMux(mux, routeMethods)
}

// wrapMux adds the 404 fallback and the access-log + metrics shell.
func (a *API) wrapMux(mux *http.ServeMux, routeMethods map[string]map[string]bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &respStatus{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			dur := time.Since(start)
			if a.metrics != nil {
				a.metrics.HTTPRequests.Add(r.Context(), 1,
					observability.Attr("method", r.Method),
					observability.Attr("status", strconv.Itoa(sw.status)),
					observability.Attr("route", routeLabel(r)))
				a.metrics.HTTPDurationMS.Record(r.Context(), dur.Milliseconds(),
					observability.Attr("route", routeLabel(r)))
			}
			if !isStreamPath(r.URL.Path) {
				middleware.RequestLog(r, sw.status, dur)
			}
		}()

		if _, pattern := mux.Handler(r); pattern == "" {
			// Method mismatch? (path registered, but not for this verb) →
			// 405 with an Allow header, not a phantom 404 (audit L1).
			if methods, ok := routeMethods[r.URL.Path]; ok && len(methods) > 0 {
				allowed := make([]string, 0, len(methods))
				for m := range methods {
					allowed = append(allowed, m)
				}
				sort.Strings(allowed)
				sw.Header().Set("Allow", strings.Join(allowed, ", "))
				writeError(sw, r, domain.ErrMethodNotAllowed(strings.Join(allowed, ", ")))
				return
			}
			// Write through the status-capturing wrapper: 404s must be
			// logged and metered as 404s, not as the default 200.
			writeError(sw, r, domain.ErrNotFound("route"))
			return
		}
		mux.ServeHTTP(sw, r)
	})
}

func isStreamPath(p string) bool {
	return strings.HasSuffix(p, "/chat/completions")
}

// readLimited applies the per-user read RPM budget to a dashboard-style
// aggregation route. Runs INSIDE auth (the key needs the identity). Fail-open
// posture is inherited from the limiter; a nil limiter (some tests) skips the
// check entirely.
func (a *API) readLimited(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.lim != nil && a.cfg != nil && a.cfg.Rate.ReadRPM > 0 {
			if idn := auth.FromIdentity(r.Context()); idn != nil {
				v := a.lim.Allow(r.Context(), "rl:rpm:read:user:"+idn.User.ID,
					a.cfg.Rate.ReadRPM, time.Minute, ids.New("rd"))
				if !v.Allowed {
					retry := v.RetryAfter
					if retry <= 0 {
						retry = time.Second
					}
					writeError(w, r, domain.ErrRateLimited(retry.Milliseconds(), "read_rpm"))
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func routeLabel(r *http.Request) string {
	p := r.URL.Path
	if len(p) > 72 {
		p = p[:72]
	}
	return r.Method + " " + p
}

// --- shared plumbing --------------------------------------------------------

type respStatus struct {
	http.ResponseWriter
	status int
}

func (w *respStatus) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *respStatus) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController (SSE write deadlines) reach the real
// connection through this wrapper.
func (w *respStatus) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOK(w http.ResponseWriter, v any) { writeJSON(w, http.StatusOK, v) }

func writeError(w http.ResponseWriter, r *http.Request, de *domain.Error) {
	middleware.WriteDomainError(w, r, de)
}

type errTrailingJSON struct{}

func (errTrailingJSON) Error() string { return "trailing JSON content" }

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return err
	}
	// Audit L3: exactly ONE JSON document per body — uniformly. The old
	// policy rejected a trailing VALID document but silently ignored
	// trailing GARBAGE bytes, so behaviour changed with the shape of the
	// junk. Any non-whitespace tail is now the same loud 400.
	//
	// The tail is probed THROUGH the decoder, never by re-reading r.Body:
	// json.Decoder reads ahead into an internal buffer, so after the first
	// value decodes, trailing bytes are typically already inside the
	// decoder and r.Body reads as EMPTY (the live suite's H11 probe — a
	// valid body followed by "]}" — caught exactly that hole in the first
	// version of this fix, which let garbage through with a 200).
	var extra any
	switch err := dec.Decode(&extra); {
	case err == nil:
		return errTrailingJSON{} // a whole second JSON document
	case errors.Is(err, io.EOF):
		return nil // clean end (whitespace-only tail at most)
	default:
		return errTrailingJSON{} // invalid or truncated garbage after the value
	}
}

func jsonErrText(err error) string {
	if err == error(errTrailingJSON{}) {
		return "trailing content after the JSON body"
	}
	return err.Error()
}
