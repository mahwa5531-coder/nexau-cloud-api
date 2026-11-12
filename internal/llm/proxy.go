package llm

import (
        "context"
        "encoding/json"
        "math"
        "strings"
        "sync"
        "sync/atomic"
        "time"

        "github.com/mash-cloud/mash-api/internal/auth"
        "github.com/mash-cloud/mash-api/internal/domain"
        "github.com/mash-cloud/mash-api/internal/ids"
        "github.com/mash-cloud/mash-api/internal/observability"
        "github.com/mash-cloud/mash-api/internal/openrouter"
        "github.com/mash-cloud/mash-api/internal/ratelimit"
        "github.com/mash-cloud/mash-api/internal/reqctx"

        "github.com/redis/go-redis/v9"
)

// UpstreamClient is the upstream seam (implemented by openrouter.Client).
type UpstreamClient interface {
        Completion(ctx context.Context, req *openrouter.ChatRequest) (*openrouter.ChatResponse, error)
        CompletionStream(ctx context.Context, req *openrouter.ChatRequest) (*openrouter.StreamReader, error)
}

// UsageQuery reads the authoritative rolling-window token position
// (implemented by repos.LLMCallsRepo).
type UsageQuery interface {
        WindowUsage(ctx context.Context, tenantID string) (*domain.WindowUsage, error)
        WindowRecovery(ctx context.Context, tenantID string, window time.Duration, quota, used int64) (*time.Time, error)
}

// CallRecorder persists metering facts asynchronously (implemented by
// metering.Recorder).
type CallRecorder interface {
        Record(call domain.LLMCall)
}

// RateRules are the deployment-level defaults; plan limits override any
// non-zero dimension per identity.
type RateRules struct {
        RPMUser    int64
        RPMTenant  int64
        ConcUser   int64
        ConcTenant int64
}

// RateInfo is the admission outcome surfaced to the transport for
// X-Ratelimit-* response headers (the industry-standard client hints:
// the desktop renders a live usage meter from them without polling /v1/me).
// Zero values mean "dimension disabled"; negative Remaining means
// "unknown (degraded)" — the transport omits that header.
type RateInfo struct {
        RPMUserLimit         int64 // effective requests/min, user scope
        RPMUserRemaining     int64 // limit − events in the window (this call counted)
        RPMRetryAfterMS      int64 // >0 only when RPM denied
        ConcUserLimit        int64 // effective concurrent-stream ceiling, user scope
        ConcUserActive       int64 // holders observed at claim time, this call included
        Quota5hLimit         int64 // plan 5h window budget (normalized tokens)
        Quota5hRemaining     int64 // budget − metered − live reservations
        QuotaWeeklyLimit     int64
        QuotaWeeklyRemaining int64
}

// RateHeaderSink is optionally implemented by transports that can still
// mutate response headers — i.e. before the first byte commits them. The
// proxy calls it immediately after a successful admission so streaming
// responses carry the headers too (they would be too late at return time).
type RateHeaderSink interface {
        SetRateHeaders(info *RateInfo)
}

// Timing bounds one call's lifetimes.
type Timing struct {
        IdleTimeout      time.Duration // no upstream chunk → abort (default 300s)
        MaxDuration      time.Duration // hard cap of one stream
        NonStreamTimeout time.Duration // full budget of a non-streaming call
}

// Proxy is the LLM tunnel service. It is safe for concurrent use.
type Proxy struct {
        Upstream UpstreamClient
        Meter    CallRecorder
        Limiter  *ratelimit.Limiter
        Usage    UsageQuery
        Metrics  *observability.Metrics
        // Norm resolves the per-model token-accounting rule (dynamic via the
        // token_normalization table; nil = built-in identity accounting).
        Norm *RuleCache

        Redis         redis.UniversalClient
        UsageCacheTTL time.Duration // 5s default shields PG from rapid agentic bursts

        Limits  Limits
        Rate    RateRules
        Timing  Timing
        SlotTTL time.Duration // concurrency-slot TTL backstop (crash recovery)
        // ReserveTTL bounds how long an un-reconciled token reservation lives
        // (admission → durable metering); default 20m. Must exceed the maximum
        // stream duration (validated at boot).
        ReserveTTL time.Duration

        // Shutdown broadcast (audit M9): BeginShutdown cancels it; every active
        // stream then ends GRACEFULLY — in-band OpenAI-style error event plus a
        // clean [DONE] — instead of being severed by a bare TCP close when the
        // shutdown grace expires mid-stream. New admissions are refused with 503.
        sdOnce   sync.Once
        sdCtx    context.Context
        sdCancel context.CancelFunc

        active atomic.Int64 // live in-flight calls (metrics gauge source)
}

// shutdownContext lazily creates the shutdown broadcast context.
func (p *Proxy) shutdownContext() context.Context {
        p.sdOnce.Do(func() {
                p.sdCtx, p.sdCancel = context.WithCancel(context.Background())
        })
        return p.sdCtx
}

// BeginShutdown asks every in-flight stream to end gracefully and refuses
// new admissions. Call once, on SIGTERM, BEFORE http.Server.Shutdown so the
// connections close on their own instead of hitting the grace wall.
func (p *Proxy) BeginShutdown() {
        p.shutdownContext()
        p.sdCancel()
}

// IsShuttingDown reports whether BeginShutdown has been called.
func (p *Proxy) IsShuttingDown() bool {
        ctx := p.shutdownContext()
        return ctx.Err() != nil
}

// ActiveCount reports in-flight calls.
func (p *Proxy) ActiveCount() int64 { return p.active.Load() }

// Redis key vocabulary for the proxy's limiter dimensions.
func rpmUserKey(uid string) string     { return "rl:rpm:user:" + uid }
func rpmTenantKey(tid string) string   { return "rl:rpm:tenant:" + tid }
func concUserKey(uid string) string    { return "rl:conc:user:" + uid }
func concTenantKey(tid string) string  { return "rl:conc:tenant:" + tid }
func windowUsageKey(tid string) string { return "usage:window:" + tid }

// Sink forwards upstream SSE payloads verbatim to the client. One method per
// frame class; implemented by the HTTP transport (api/llm.go).
type Sink interface {
        // SendChunk forwards one SSE data payload (without the data:/[DONE]
        // framing — the transport writes framing).
        SendChunk(raw []byte) error
        // SendError delivers an in-band OpenAI-style error object once the
        // response has started streaming: standard OpenAI SDKs surface it as a
        // failure instead of a silent empty completion.
        SendError(errType, code, message string) error
        // Started reports whether any byte has been forwarded to the client.
        Started() bool
}

// Complete runs one non-streaming call end to end: gates → OpenRouter → raw
// completion body + metering. The returned bytes are the verbatim upstream
// JSON body (OpenAI-compatible contract: the client gets the completion
// object or an HTTP error, never a wrapper). The returned RateInfo carries
// the admission-time limit position for X-Ratelimit-* headers.
func (p *Proxy) Complete(ctx context.Context, idn *auth.Identity, req *Request) ([]byte, *RateInfo, *domain.Error) {
        callID := ids.CallID()
        release, info, de := p.admit(ctx, idn, req, callID)
        if de != nil {
                return nil, info, de
        }
        defer release()

        started := time.Now().UTC()
        cctx, cancel := context.WithTimeout(reqctx.WithCallID(ctx, callID), p.Timing.NonStreamTimeout)
        defer cancel()

        resp, err := p.Upstream.Completion(cctx, req.ToUpstream())
        if err != nil {
                de := domain.AsError(err)
                p.record(ctx, idn, req, callID, callMeta{status: domain.CallFailed, errCode: de.Code, errMsg: safeMessage(de), started: started})
                return nil, info, de
        }

        model, provider := "", resp.Provider
        if resp.Model != "" && model == "" {
                model = resp.Model
        }
        meta := callMeta{
                status:      domain.CallCompleted,
                usage:       NormalizeUsage(resp.Usage, p.normRule(ctx, model)),
                latency:     int(time.Since(started).Milliseconds()),
                resolved:    model,
                provider:    provider,
                started:     started,
                completedAt: time.Now().UTC(),
        }
        p.record(ctx, idn, req, callID, meta)
        // OpenAI-compatible contract: the VERBATIM upstream completion body
        // (audit M10) — unknown upstream fields survive exactly as they do
        // on the streaming path. The struct decode upstream of here is for
        // validation and metering only.
        if len(resp.Raw) > 0 {
                return resp.Raw, info, nil
        }
        // Defensive fallback (raw body absent — synthetic responses in
        // tests): re-marshal the decoded struct.
        blob, err := json.Marshal(resp)
        if err != nil {
                return nil, info, domain.ErrInternal(err)
        }
        return blob, info, nil
}

// Stream runs one streaming call end to end: gates → OpenRouter → verbatim SSE
// passthrough via sink → metering.
//
// Error contract: a non-nil return means the UPSTREAM failed. Callers use
// sink.Started() to pick the response shape — nothing flowed yet → real HTTP
// error; bytes already flowed → the in-band error event was emitted here and
// the caller closes the stream. Client disconnects (ctx cancelled / sink
// write failure) abort the upstream and return nil: there is nobody left to
// report to, but the usage seen so far is still metered.
func (p *Proxy) Stream(ctx context.Context, idn *auth.Identity, req *Request, sink Sink) (*RateInfo, *domain.Error) {
        callID := ids.CallID()
        release, info, de := p.admit(ctx, idn, req, callID)
        if de != nil {
                return info, de
        }
        defer release()

        // Surface the admission position while the response headers are still
        // mutable — before the first upstream byte commits them.
        if rs, ok := sink.(RateHeaderSink); ok {
                rs.SetRateHeaders(info)
        }

        started := time.Now().UTC()

        bctx := openrouter.WithIdleTimeout(ctx, p.Timing.IdleTimeout)
        bctx, durCancel := context.WithTimeout(reqctx.WithCallID(bctx, callID), p.Timing.MaxDuration)
        defer durCancel()

        // Shutdown-aware upstream context (audit M9): BeginShutdown cancels
        // it, the upstream read aborts, and the loop below ends the stream
        // with an in-band error event + clean [DONE] instead of letting the
        // process exit mid-chunk when the grace window expires.
        sdctx := p.shutdownContext()
        sctx, sCancel := context.WithCancel(bctx)
        stopWatch := context.AfterFunc(sdctx, sCancel)
        defer stopWatch()

        p.active.Add(1)
        defer p.active.Add(-1)

        reader, err := p.Upstream.CompletionStream(sctx, req.ToUpstream())
        if err != nil {
                de := domain.AsError(err)
                p.record(ctx, idn, req, callID, callMeta{status: domain.CallFailed, errCode: de.Code, errMsg: safeMessage(de), started: started})
                return info, de // pre-stream: caller answers with a real HTTP error
        }
        defer reader.Close()

        var usage *domain.TokenUsage
        var model, provider string
        forwarded := false

        for {
                chunk, raw, done, err := reader.Next()
                if err != nil {
                        if sdctx.Err() != nil {
                                // Server shutdown (audit M9): end GRACEFULLY. If bytes
                                // already flowed, the client gets an in-band OpenAI-style
                                // error event followed by the transport's clean [DONE] —
                                // distinguishable from a network failure, so the desktop
                                // can back off and retry instead of treating the answer
                                // as corrupt. If nothing flowed, a retryable 503 goes out.
                                p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                        status: domain.CallCancelled, errCode: "SERVER_SHUTDOWN", errMsg: "server shutting down",
                                        usage: usage, latency: int(time.Since(started).Milliseconds()),
                                        resolved: model, provider: provider, started: started,
                                })
                                if sink != nil && sink.Started() {
                                        _ = sink.SendError("server_error", "SERVER_SHUTDOWN",
                                                "The server is shutting down for maintenance; please retry shortly.")
                                        return info, nil // transport writes [DONE]
                                }
                                return info, domain.ErrServerShuttingDown()
                        }
                        if bctx.Err() != nil {
                                // The stream's OWN duration budget fired (audit L6): the server
                                // cut this stream at MaxDuration — meter it as MAX_DURATION, not
                                // as a flaky client, so the 'why do streams end?' dashboard
                                // tells the truth about who did the cutting.
                                p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                        status: domain.CallCancelled, errCode: "MAX_DURATION", errMsg: "stream exceeded the maximum duration",
                                        usage: usage, latency: int(time.Since(started).Milliseconds()),
                                        resolved: model, provider: provider, started: started,
                                })
                                if forwarded && sink != nil {
                                        _ = sink.SendError("server_error", "MAX_DURATION", "The stream exceeded the maximum duration.")
                                }
                                return info, nil
                        }
                        if ctx.Err() != nil || isClientGone(err) {
                                // Client disconnected: abort upstream, meter what was seen.
                                p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                        status: domain.CallCancelled, errCode: "CLIENT_DISCONNECTED", errMsg: "client disconnected",
                                        usage: usage, latency: int(time.Since(started).Milliseconds()),
                                        resolved: model, provider: provider, started: started,
                                })
                                return info, nil
                        }
                        de := domain.AsError(err)
                        if forwarded && sink != nil {
                                // Mid-stream failure: 200 + stream headers are already on the
                                // wire and cannot be taken back. Emit the in-band OpenAI-style
                                // error event so SDKs observe the failure instead of parsing
                                // an empty successful completion.
                                _ = sink.SendError(de.Code, de.Code, safeMessage(de))
                        }
                        p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                status: domain.CallFailed, errCode: de.Code, errMsg: safeMessage(de),
                                usage: usage, latency: int(time.Since(started).Milliseconds()),
                                resolved: model, provider: provider, started: started,
                        })
                        return info, de
                }
                if done {
                        p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                status: domain.CallCompleted,
                                usage:  usage, latency: int(time.Since(started).Milliseconds()),
                                resolved: model, provider: provider, started: started,
                        })
                        return info, nil
                }
                if chunk != nil {
                        // Resolve the model/provider from THIS chunk before
                        // touching its usage (audit M6): the old order
                        // normalized usage against the PREVIOUS chunk's model,
                        // so mid-stream fallbacks and early usage chunks were
                        // metered with the wrong token-accounting weights.
                        if chunk.Model != "" {
                                model = chunk.Model
                        }
                        if chunk.Provider != "" {
                                provider = chunk.Provider
                        }
                        if chunk.Usage != nil {
                                usage = NormalizeUsage(chunk.Usage, p.normRule(ctx, model))
                        }
                }
                if len(raw) > 0 {
                        if err := sink.SendChunk(raw); err != nil {
                                // Client gone: abort upstream, meter as cancelled.
                                _ = reader.Close()
                                p.record(context.WithoutCancel(ctx), idn, req, callID, callMeta{
                                        status: domain.CallCancelled, errCode: "CLIENT_DISCONNECTED", errMsg: "client disconnected",
                                        usage: usage, latency: int(time.Since(started).Milliseconds()),
                                        resolved: model, provider: provider, started: started,
                                })
                                return info, nil
                        }
                        forwarded = true
                        if p.Metrics != nil {
                                p.Metrics.EventsForwarded.Add(ctx, 1, observability.Attr("type", "raw_chunk"))
                        }
                }
        }
}

// callMeta is the metering payload accumulated during a call.
type callMeta struct {
        status      string
        errCode     string
        errMsg      string // client-safe failure reason (audit L19: llm_calls.error_message was always NULL)
        usage       *domain.TokenUsage
        latency     int
        resolved    string
        provider    string
        started     time.Time
        completedAt time.Time
}

func (p *Proxy) record(ctx context.Context, idn *auth.Identity, req *Request, callID string, m callMeta) {
        if p.Meter == nil {
                // No metering: nothing will ever persist this call's usage — the
                // reservation must not outlive the call (quota would over-hold).
                p.releaseReservation(ctx, idn.Tenant.ID, callID)
                return
        }
        // Reconcile the admission-time estimate with the metered truth: the
        // reservation now carries the actual consumption until the row lands
        // (audit H2). No usage facts → release; the row will count zero.
        if m.usage != nil {
                p.adjustReservation(ctx, idn.Tenant.ID, callID, m.usage.TotalTokens)
        } else {
                p.releaseReservation(ctx, idn.Tenant.ID, callID)
        }
        resolved := m.resolved
        if resolved == "" && m.status != domain.CallCompleted {
                // Audit L19: upstream-failed rows carried an empty resolved_model,
                // leaving 'which model failed?' unanswerable from the data. The
                // attempted model is the honest attribution for a failed call.
                resolved = req.Model
        }
        call := domain.LLMCall{
                CallID:         callID,
                TenantID:       idn.Tenant.ID,
                UserID:         idn.User.ID,
                RequestID:      reqctx.RequestID(ctx),
                RequestedModel: req.Model,
                ResolvedModel:  resolved,
                Provider:       m.provider,
                Stream:         req.Stream,
                Status:         m.status,
                ErrorCode:      m.errCode,
                ErrorMessage:   m.errMsg,
                LatencyMS:      m.latency,
                StartedAt:      m.started,
        }
        if !m.completedAt.IsZero() {
                t := m.completedAt
                call.CompletedAt = &t
        } else {
                t := time.Now().UTC()
                call.CompletedAt = &t
        }
        if m.usage != nil {
                call.Usage = *m.usage
        }
        p.Meter.Record(call)
}

// admit runs every pre-flight gate in order and returns the slot-release
// function and the admission-time rate position on success:
//
//      account/tenant/subscription state → payload validation → model
//      entitlement (primary + fallbacks) → concurrency slots (user + tenant)
//      → RPM sliding window (user + tenant) → rolling token windows with
//      admission-time reservation (5h + weekly).
//
// Cheapest gates run first; the quota gate runs last because it is the only
// one that touches PostgreSQL and the only one that holds durable state (the
// token reservation) — requests rejected by an earlier gate never reserve.
// The returned RateInfo is non-nil even on denial (filled as far as the gates
// reached) so the transport can emit honest X-Ratelimit-* headers on 429s.
func (p *Proxy) admit(ctx context.Context, idn *auth.Identity, req *Request, callID string) (func(), *RateInfo, *domain.Error) {
        info := &RateInfo{}
        // Refuse new work once shutdown has begun (audit M9): a stream
        // admitted during drain would be cut mid-flight by the grace wall.
        if p.IsShuttingDown() {
                return nil, info, domain.ErrServerShuttingDown()
        }

        // Account/tenant/subscription state (the auth middleware admits
        // SUBSCRIPTION_INACTIVE identities so /v1/me can render state; the LLM
        // path is where it becomes binding).
        if reason := idn.CanRun(); reason != "" {
                if reason == "SUBSCRIPTION_INACTIVE" {
                        return nil, info, domain.ErrSubscriptionInactive()
                }
                return nil, info, domain.ErrForbidden("Account state does not permit this request.")
        }

        // Payload validation.
        if de := req.Validate(p.Limits); de != nil {
                return nil, info, de
        }

        // Auto-resolve abstract model alias "mash/auto" to tenant's plan model:
        // Desktop client sends "mash/auto" so the server routes dynamically per plan tier.
        // This ensures MODEL_NOT_ENTITLED never fires for desktop users.
        // Pattern entries ("anthropic/*") are SKIPPED (audit L8): when a plan's
        // model list led with a pattern, the alias used to forward the pattern
        // STRING itself upstream as a literal model id — a model-not-found error
        // that sent everyone debugging provider status pages while our own alias
        // table was the one speaking pattern language.
        if req.Model == "mash/auto" || req.Model == "mash/default" {
                req.Model = "openai/gpt-6-luna"
                for _, m := range idn.Models {
                        if !strings.Contains(m, "*") {
                                req.Model = m
                                break
                        }
                }
        }

        // Model entitlement (allowlist with patterns). Every fallback must be
        // inside the allowlist too — listing an off-allowlist model as a fallback
        // must not bypass the primary check.
        if idn.Restricted {
                if !modelAllowed(idn.Models, req.Model) {
                        return nil, info, domain.ErrModelNotEntitled(req.Model)
                }
                for _, fb := range req.Fallbacks {
                        if !modelAllowed(idn.Models, fb) {
                                return nil, info, domain.ErrModelNotEntitled(fb)
                        }
                }
        }

        // Plan limits override deployment defaults per dimension.
        rpmUser, rpmTenant := p.Rate.RPMUser, p.Rate.RPMTenant
        concUser, concTenant := p.Rate.ConcUser, p.Rate.ConcTenant
        if idn.Limits.RequestsPerMinuteUser > 0 {
                rpmUser = idn.Limits.RequestsPerMinuteUser
        }
        if idn.Limits.RequestsPerMinuteTenant > 0 {
                rpmTenant = idn.Limits.RequestsPerMinuteTenant
        }
        if idn.Limits.ConcurrentRequestsUser > 0 {
                concUser = idn.Limits.ConcurrentRequestsUser
        }
        if idn.Limits.ConcurrentRequestsTenant > 0 {
                concTenant = idn.Limits.ConcurrentRequestsTenant
        }

        // Concurrency slots (Redis, per-holder expiry — see ratelimit.Slot).
        // Acquired BEFORE the RPM window (audit L18): a request rejected for
        // concurrency must not consume the user's requests-per-minute budget,
        // and the agent's retry of a slot rejection must not double-charge RPM.
        // The slots are held only for the microsecond it takes to run the RPM
        // check; an RPM denial releases them immediately.
        var held []*ratelimit.Slot
        info.ConcUserLimit = concUser
        info.Quota5hLimit = idn.Limits.Window5hTokens
        info.QuotaWeeklyLimit = idn.Limits.WindowWeeklyTokens
        if info.Quota5hLimit == 0 && info.QuotaWeeklyLimit == 0 {
                info.Quota5hRemaining, info.QuotaWeeklyRemaining = 0, 0
        }
        if concUser > 0 {
                s, ok := p.Limiter.AcquireSlot(ctx, concUserKey(idn.User.ID), concUser, p.SlotTTL)
                if !ok {
                        return nil, info, domain.ErrConcurrencyLimited("user")
                }
                if s != nil {
                        held = append(held, s)
                        info.ConcUserActive = s.Active()
                }
        }
        if concTenant > 0 {
                s, ok := p.Limiter.AcquireSlot(ctx, concTenantKey(idn.Tenant.ID), concTenant, p.SlotTTL)
                if !ok {
                        releaseHeld(ctx, held)
                        return nil, info, domain.ErrConcurrencyLimited("tenant")
                }
                if s != nil {
                        held = append(held, s)
                }
        }

        // RPM sliding window (one pipelined Redis round trip, both scopes).
        // ZSET member: SERVER-GENERATED and unique per admission — the inbound
        // X-Request-Id is client-controlled and reusing one fixed value would
        // dedupe every ZADD onto a single member, pinning the window count at 1.
        info.RPMUserLimit = rpmUser
        member := ids.New("adm")
        rpm := p.Limiter.AllowScopes(ctx, member,
                ratelimit.Scope{Key: rpmUserKey(idn.User.ID), Limit: rpmUser, Window: time.Minute},
                ratelimit.Scope{Key: rpmTenantKey(idn.Tenant.ID), Limit: rpmTenant, Window: time.Minute},
        )
        // User-scope position for the headers (Verdict.Remaining is the window's
        // event count, i.e. used — remaining is limit − used).
        for i, sc := range rpm.Active {
                if i < len(rpm.Verdicts) && sc.Key == rpmUserKey(idn.User.ID) {
                        v := rpm.Verdicts[i]
                        info.RPMUserRemaining = rpmUser - v.Remaining
                        if info.RPMUserRemaining < 0 {
                                info.RPMUserRemaining = 0
                        }
                        if !v.Allowed {
                                info.RPMRetryAfterMS = v.RetryAfter.Milliseconds()
                        }
                }
        }
        if !rpm.Merged.Allowed {
                releaseHeld(ctx, held)
                retry := rpm.Merged.RetryAfter
                if retry <= 0 {
                        retry = time.Second
                }
                return nil, info, domain.ErrRateLimited(retry.Milliseconds(), "llm_rpm")
        }

        // Rolling token windows with ADMISSION-TIME RESERVATION (audit H2):
        // a 5-hour burst budget and a 7-day weekly budget — either can bind.
        // The check is atomic against the live sum of in-flight reservations,
        // so a fan-out of parallel requests cannot all spend the same
        // remaining budget. Reservation failures fail OPEN (availability
        // posture; the quota gate is a plan-abuse bound, not a security
        // boundary) and are counted on quota.gate_fails.
        q5h, qWeekly := idn.Limits.Window5hTokens, idn.Limits.WindowWeeklyTokens
        if (q5h > 0 || qWeekly > 0) && p.Usage != nil {
                pos, err := p.windowUsage(ctx, idn.Tenant.ID, q5h, qWeekly)
                if err != nil && p.Metrics != nil {
                        p.Metrics.QuotaGateFails.Add(ctx, 1)
                }
                if err == nil && pos != nil {
                        w, projected, resv := p.reserveQuota(ctx, idn.Tenant.ID, callID, req, pos, q5h, qWeekly)
                        // Live quota position for the headers: metered usage plus every
                        // in-flight reservation (this call's included on success).
                        if resv >= 0 {
                                if q5h > 0 {
                                        info.Quota5hRemaining = q5h - pos.Used5h - resv
                                }
                                if qWeekly > 0 {
                                        info.QuotaWeeklyRemaining = qWeekly - pos.UsedWeekly - resv
                                }
                        } else {
                                info.Quota5hRemaining, info.QuotaWeeklyRemaining = -1, -1
                        }
                        if w != "" {
                                d, q := 5*time.Hour, q5h
                                if w == "weekly" {
                                        d, q = 7*24*time.Hour, qWeekly
                                }
                                if w == "weekly" {
                                        info.QuotaWeeklyRemaining = 0
                                } else {
                                        info.Quota5hRemaining = 0
                                }
                                // Quota rejection must return its concurrency slots — the
                                // request is over and will never reach the stream loop whose
                                // defer would have released them. Without this, every
                                // WINDOW_QUOTA_EXCEEDED leaks one user + one tenant holder
                                // until SlotTTL: a quota-blocked desktop agent retrying in a
                                // loop exhausts its concurrency dimension and starts seeing
                                // CONCURRENCY_LIMIT instead of the honest quota error for as
                                // long as the TTL lasts (mirrors the RPM branch above).
                                releaseHeld(ctx, held)
                                return nil, info, p.windowBlocked(ctx, idn, w, d, q, projected)
                        }
                }
        }

        var released bool
        return func() {
                if released {
                        return
                }
                released = true
                releaseHeld(ctx, held)
        }, info, nil
}

// releaseHeld returns every held slot exactly once, on a context that outlives
// request cancellation (a cancelled stream still owes its slots back).
func releaseHeld(ctx context.Context, held []*ratelimit.Slot) {
        if len(held) == 0 {
                return
        }
        rctx := context.WithoutCancel(ctx)
        for _, s := range held {
                s.Release(rctx)
        }
}

// windowBlocked renders the quota rejection with the exact recovery instant
// (rejection path only — one extra indexed query, never on the happy path).
func (p *Proxy) windowBlocked(ctx context.Context, idn *auth.Identity, window string, d time.Duration, quota, used int64) *domain.Error {
        var resetsAt *time.Time
        if p.Usage != nil {
                if t, err := p.Usage.WindowRecovery(ctx, idn.Tenant.ID, d, quota, used); err == nil {
                        resetsAt = t
                }
        }
        return domain.ErrWindowQuotaExceeded(window, quota, used, resetsAt)
}

// normRule resolves the accounting rule for the model actually served (the
// resolved model, falling back to the requested one).
func (p *Proxy) normRule(ctx context.Context, model string) domain.NormRule {
        if p.Norm == nil {
                return domain.DefaultNormRule
        }
        return p.Norm.Rule(ctx, model)
}

// modelAllowed matches "openai/gpt-4o" exactly or "anthropic/*" patterns.
func modelAllowed(allowlist []string, model string) bool {
        if len(allowlist) == 0 {
                return true
        }
        for _, pattern := range allowlist {
                if pattern == model {
                        return true
                }
                if strings.HasSuffix(pattern, "/*") {
                        if prefix := strings.TrimSuffix(pattern, "/*"); strings.HasPrefix(model, prefix+"/") {
                                return true
                        }
                }
        }
        return false
}

// NormalizeUsage converts raw OpenRouter usage into the canonical metering shape
// using the model's accounting rule. The result is the quota currency:
//
//      normalized_input  = max(0, round(prompt·iw) − round(cache_read·crw) − round(cache_write·cww))
//      normalized_output = round(completion·ow)
//      normalized_total  = normalized_input + normalized_output
//
// Raw provider numbers are preserved alongside (RawPromptTokens /
// RawCompletionTokens) so per-row accounting stays reconstructible whatever
// the weights were at record time. With the default rule (all weights 1) this
// is the classic accounting: cache hits/writes are free, everything else
// counts 1:1, total = input + output.
func NormalizeUsage(u *openrouter.Usage, rule domain.NormRule) *domain.TokenUsage {
        if u == nil {
                return nil
        }
        t := &domain.TokenUsage{
                RawPromptTokens:     u.PromptTokens,
                RawCompletionTokens: u.CompletionTokens,
                InputTokens:         applyWeight(u.PromptTokens, rule.InputWeight),
                OutputTokens:        applyWeight(u.CompletionTokens, rule.OutputWeight),
                ReasoningTokens:     0,
                CacheReadTokens:     0,
        }
        if u.PromptTokensDetails != nil {
                t.CacheReadTokens = u.PromptTokensDetails.CachedReadTokens
                if t.CacheReadTokens == 0 && u.PromptTokensDetails.CachedTokens > 0 {
                        t.CacheReadTokens = u.PromptTokensDetails.CachedTokens
                }
                t.CacheWriteTokens = u.PromptTokensDetails.CachedWriteTokens
        }
        if u.CompletionTokensDetails != nil {
                t.ReasoningTokens = u.CompletionTokensDetails.ReasoningTokens
        }
        // Cache hits/writes are discounted from the input, floored at zero — a
        // provider reporting more cached tokens than prompt tokens must never go
        // negative (and must never make the call free below zero).
        t.InputTokens -= applyWeight(t.CacheReadTokens, rule.CachedReadWeight)
        t.InputTokens -= applyWeight(t.CacheWriteTokens, rule.CachedWriteWeight)
        if t.InputTokens < 0 {
                t.InputTokens = 0
        }
        t.TotalTokens = t.InputTokens + t.OutputTokens
        if u.CostDetails != nil {
                t.InputCost = u.CostDetails.UpstreamInferencePromptCost
                t.OutputCost = u.CostDetails.UpstreamInferenceCompletionsCost
        }
        if u.Cost != nil {
                t.TotalCost = *u.Cost
                if t.TotalCost == 0 {
                        t.TotalCost = t.InputCost + t.OutputCost
                }
        }
        return t
}

// applyWeight scales a token count, with an exact fast path for weight 1.
func applyWeight(n int64, w float64) int64 {
        if n <= 0 {
                return 0
        }
        if w == 1 {
                return n
        }
        return int64(math.Round(float64(n) * w))
}

// isClientGone reports whether the error is a client-side disconnect rather
// than an upstream fault.
func isClientGone(err error) bool {
        if err == nil {
                return false
        }
        s := err.Error()
        return strings.Contains(s, "broken pipe") ||
                strings.Contains(s, "connection reset by peer") ||
                strings.Contains(s, "client disconnected")
}

// safeMessage renders a client-safe message for in-band error events.
func safeMessage(de *domain.Error) string {
        if de == nil {
                return "upstream error"
        }
        return de.Message
}

// QuotaPosition returns the tenant's rolling-window usage through the exact
// same short-TTL Redis cache the admission gate reads (audit M7: /v1/me must
// not re-run the llm_calls SUM per call, and the desktop's usage bar must
// never diverge from what enforcement sees).
func (p *Proxy) QuotaPosition(ctx context.Context, tenantID string, q5h, qWeekly int64) (*domain.WindowUsage, error) {
        return p.windowUsage(ctx, tenantID, q5h, qWeekly)
}

func (p *Proxy) windowUsage(ctx context.Context, tenantID string, q5h, qWeekly int64) (*domain.WindowUsage, error) {
        if p.Usage == nil {
                return nil, nil
        }
        cacheTTL := p.UsageCacheTTL
        if cacheTTL <= 0 {
                cacheTTL = 5 * time.Second // 5s default cache shields Postgres from rapid agent turns
        }

        // 1. Redis cache lookup (best-effort; Redis down or nil -> straight to PostgreSQL)
        if p.Redis != nil {
                if blob, err := p.Redis.Get(ctx, windowUsageKey(tenantID)).Bytes(); err == nil {
                        var u domain.WindowUsage
                        if json.Unmarshal(blob, &u) == nil {
                                // Safety: if cached value is near or over quota (>=95%), bypass and check fresh DB
                                near5h := q5h > 0 && u.Used5h >= q5h*95/100
                                nearWeekly := qWeekly > 0 && u.UsedWeekly >= qWeekly*95/100
                                if !near5h && !nearWeekly {
                                        return &u, nil
                                }
                        }
                }
        }

        // 2. Authoritative database query
        pos, err := p.Usage.WindowUsage(ctx, tenantID)
        if err != nil {
                return nil, err
        }

        // 3. Cache fill: only cache safe (under-quota) positions so recovery is immediate
        if p.Redis != nil && pos != nil {
                over5h := q5h > 0 && pos.Used5h >= q5h
                overWeekly := qWeekly > 0 && pos.UsedWeekly >= qWeekly
                if !over5h && !overWeekly {
                        if blob, err := json.Marshal(pos); err == nil {
                                _ = p.Redis.Set(ctx, windowUsageKey(tenantID), blob, cacheTTL).Err()
                        }
                } else {
                        _ = p.Redis.Del(ctx, windowUsageKey(tenantID)).Err()
                }
        }
        return pos, nil
}
