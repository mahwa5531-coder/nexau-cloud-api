# The Upstream — OpenRouter, Direct (Decision + Integration)

**Decision date:** 2026-11-03 · **Scope:** `internal/openrouter/`, `compose.yaml`,
all env knobs `NEXAU_OPENROUTER_*` · **Replaces:** the sidecar LLM gateway
container (removed the same day; see the note atop `CLEANUP-2026-10-05.md`).

---

## 1. The decision

The API now calls **OpenRouter directly over HTTPS** as its single LLM
upstream:

```
desktop ──HTTPS──▶ MASh Cloud API ──HTTPS──▶ openrouter.ai/api/v1 ──▶ providers (BYOK)
                     (this repo)              (no hop in between)
```

What was removed:

| Removed | Why it existed | Why it is not needed |
|---|---|---|
| The gateway container + its env keys (`BIFROST_PROVIDER_KEYS_*`), port 8081, `/health` probe | Multi-provider routing, provider-key isolation, per-provider failover | OpenRouter already IS that layer: 400+ models, provider failover on 429/5xx before the error reaches us, unified OpenAI-compatible surface, usage+cost in the final chunk |
| The extra network hop per LLM call | — | One less TLS path, one less moving part, one less container to run/monitor/patch |
| Bifrost's `fallbacks` request field | Per-request provider fallback chains | OpenRouter's `models` request field (ordered model fallback list); provider-level routing stays OpenRouter's job |
| `stream_options.include_usage` | Ask the upstream for usage in the last chunk | Deprecated upstream (no effect): OpenRouter **always** returns full usage — tokens, `cost`, `is_byok`, `cost_details` — in the final chunk |

What was **kept** (the production-hardening layer, now in
`internal/openrouter/`, ~1,670 production lines): pre-first-byte retry with
jittered backoff and Retry-After honoring, the circuit breaker
(closed/open/half-open), the SSE stream reader with idle watchdog and
keepalive-comment skipping, error classification into the domain model, the
verbatim passthrough contract, and usage/cost extraction. See §3 for why
this layer must exist even against a single managed upstream.

**Account setup (operator, one-time):**

1. Create the OpenRouter account; create ONE API key
   (`sk-or-v1-…`) → `NEXAU_OPENROUTER_API_KEY` (or `_FILE` secret mount).
2. Register provider keys as **BYOK** (OpenAI, Anthropic, Google…).
3. On every BYOK key, set **"Never use shared capacity"** — otherwise
   requests that exhaust/fail BYOK routing silently fall back to
   OpenRouter's own shared endpoints and are billed at list price into
   credits.
4. BYOK fee: 5% of list-price inference, free under the $25k/month
   list-price allowance (Standard/Business tiers). At our ≤100-client
   scale the worst case is a few hundred USD of list-price inference per
   month — the fee is **$0** in practice.

## 2. Wire contract (verified against OpenRouter's docs/OpenAPI)

- `POST {NEXAU_OPENROUTER_URL}/v1/chat/completions` (default base
  `https://openrouter.ai/api`), `Authorization: Bearer sk-or-…`, optional
  `HTTP-Referer` / `X-OpenRouter-Title` attribution headers
  (`NEXAU_OPENROUTER_REFERER` / `NEXAU_OPENROUTER_TITLE`).
- Model ids are native OpenRouter slugs: `"vendor/model"`
  (`openai/gpt-6-luna`, `anthropic/claude-sonnet-5.5`, …). The validated
  client `fallbacks` array maps to OpenRouter's `models` (ordered fallback
  list). Unknown request parameters pass through the Extras mechanism.
- Streaming: SSE `data: …` frames, keepalive comment lines
  (`: OPENROUTER PROCESSING`) — skipped per the SSE spec — and the terminal
  `data: [DONE]`. **Usage always arrives in the final chunk** (with a
  non-empty `choices` repeating `finish_reason`: treat it as an accounting
  frame, not a second terminal event).
- Mid-stream provider failures: HTTP stays 200 and the stream emits one
  chunk carrying a **top-level `error` object** (`{code: <number>,
  message, metadata}`) with `finish_reason:"error"` — surfaced as a domain
  error immediately (nothing is swallowed into an empty completion).
- Non-streaming failures after commit: **200 with an error-only body**
  ("check the body for an error field even on a 200") — detected and
  classified; retryable classes re-run pre-first-byte.
- Error envelope everywhere: `{"error":{"code":<number>,"message":"…",
  "metadata":{"error_type":"…","provider_code":"…","limit_source":"…"}}}`.
- Status → classification (single source of truth: `classify()` in
  `internal/openrouter/errors.go`):

| Upstream status | Client code | HTTP | Retry |
|---|---|---|---|
| 429 | `UPSTREAM_RATE_LIMITED` | 429 | yes (honor `Retry-After`) |
| 402 (credits/key limit) | `UPSTREAM_CREDITS` | 502 | no |
| 402 (`limit_source=openrouter_in_flight_budget`) | `UPSTREAM_RATE_LIMITED` | 429 | yes (honor `Retry-After`) |
| 408 | `UPSTREAM_TIMEOUT` | 504 | yes |
| 404 | `MODEL_NOT_FOUND` | 404 | no |
| 401 | `UPSTREAM_AUTH` | 502 | no (our credential problem) |
| 403 (permissions) | `UPSTREAM_FORBIDDEN` | 502 | no |
| 403 (moderation: `metadata.reasons` / `content_policy_violation` / `refusal`) | `UPSTREAM_MODERATION` | 403 | no |
| 500 / 502 / 503 / 524 / 529 | `UPSTREAM_UNAVAILABLE` | 502 | yes |
| other 4xx | `UPSTREAM_BAD_REQUEST` | 502 | no |

- `Retry-After` is honored (bounded by `NEXAU_OPENROUTER_RETRY_MAX_BACKOFF`).
- Readiness probe: `GET {base}/v1/key` — authenticated, un-billed,
  distinguishes 200 (key valid + service up) from 401. Reported on
  `/health/ready` as the `openrouter` dependency (`upstream_circuit` field
  carries the breaker state; neither ever gates readiness).
- Reconciliation: every response carries `X-Generation-Id`;
  `GET /api/v1/generation?id=gen-…` returns authoritative tokens, cost,
  latency and provider for post-hoc metering checks (not yet wired —
  reserved for the finance reconciliation tool).

## 3. Why the hardening layer stays (industry standard)

Every production LLM gateway keeps its own transport hardening even when
the upstream is a single managed router (LiteLLM and Portkey both support
OpenRouter as a first-class upstream and keep their retry/timeout/cooldown
layers on top). OpenRouter's own docs tell clients to retry with
exponential backoff on 429 and honor `Retry-After` themselves: provider
failover covers provider faults, not platform 429s, credit limits or a
dead TCP connection.

What our layer provides, and where it lives:

| Concern | Where | Behavior |
|---|---|---|
| Pre-first-byte retry | `client.go` `withRetry` | only failures that cannot have billed upstream work are retried (≤2 by default, jittered exponential backoff, Retry-After wins when present); anything after the first byte is final |
| Circuit breaker | `breaker.go` | consecutive + rate trips over a sliding window, exponential open cooldown (30s→5m), bounded half-open probes, per-attempt recording, 4xx/429 count as liveness (upstream alive), client cancels are neutral |
| SSE reader + idle watchdog | `stream.go` | spec-compliant parsing (comments skipped — OpenRouter keepalives), 1 MiB line bound, `[DONE]` sentinel, raw-termination detection, per-read idle deadline that both cancels and closes the body |
| Error normalization | `errors.go` | the classification table above; provider text never reaches the client payload (it rides in `Cause` for logs) |
| Usage/cost extraction | `types.go` + `llm.NormalizeUsage` | final-chunk usage → normalized quota tokens + raw tokens + USD cost (`usage.cost`, `cost_details.upstream_inference_*`) |
| Timeouts | `config.go` | dial 10s / TLS 10s / response-header 60s / non-stream 120s / stream idle 300s / stream max 15m — all `NEXAU_OPENROUTER_*` / `NEXAU_STREAM_*` knobs |
| Metrics | `observability/metrics.go` | `mash_upstream_requests_total{status}`, `mash_upstream_duration_ms`, `mash_upstream_stream_chunks_total`, `mash_upstream_breaker_{state,trips_total,rejected_total}` |

## 4. Environment (complete surface)

| Env | Default | Notes |
|---|---|---|
| `NEXAU_OPENROUTER_URL` | `https://openrouter.ai/api` | override only for mock-upstream tests |
| `NEXAU_OPENROUTER_API_KEY` | — | **required**; `NEXAU_OPENROUTER_API_KEY_FILE` for secret mounts |
| `NEXAU_OPENROUTER_REFERER` / `_TITLE` | — | optional attribution headers |
| `NEXAU_OPENROUTER_HEALTH_URL` | `{base}/v1/key` | readiness probe target |
| `NEXAU_OPENROUTER_DIAL_TIMEOUT` / `_TLS_TIMEOUT` / `_HEADER_TIMEOUT` | 10s / 10s / 60s | transport budgets |
| `NEXAU_OPENROUTER_REQUEST_TIMEOUT` | 120s | non-streaming call cap (≥5s validated) |
| `NEXAU_OPENROUTER_MAX_RETRIES` / `_RETRY_MIN_BACKOFF` / `_RETRY_MAX_BACKOFF` | 2 / 100ms / 2s | pre-first-byte retry policy |
| `NEXAU_OPENROUTER_MAX_IDLE_CONNS` / `_MAX_IDLE_CONNS_PER_HOST` / `_MAX_CONNS_PER_HOST` / `_IDLE_CONN_TIMEOUT` / `_DISABLE_HTTP2` | 512 / 256 / 0 / 90s / false | connection pool |
| `NEXAU_OPENROUTER_CB_*` | see `docs/CIRCUIT-BREAKER.md` §7 | breaker knobs (renamed from the old prefix; defaults unchanged) |

Migration note: former `NEXAU_BIFROST_*` variables are gone. Deployments
must set `NEXAU_OPENROUTER_API_KEY` (compose fails fast without it) and drop
the gateway container from the stack.

## 5. Test evidence

- `internal/openrouter/openrouter_test.go` — SSE parsing incl. the exact
  `: OPENROUTER PROCESSING` keepalive cadence, in-band error chunks with
  numeric codes, the OpenRouter usage shape (`cost` number, `is_byok`,
  `cost_details`, `cached_tokens` + both cache-write spellings), the
  classification table (15 status cases incl. 402 in-flight-budget and
  moderation 403), 200-with-error-body retry and no-retry paths, header
  propagation (auth + attribution + correlation), `/v1/key` health probe.
- `internal/openrouter/breaker_test.go` — 22 breaker tests (unchanged
  semantics, OpenRouter-shaped fixtures).
- `tests/llm_e2e_test.go` — the full pipeline against a scripted fake
  OpenRouter, with OpenRouter-shaped usage/cost fixtures.
- `go build ./...`, `go vet ./...`, `go test ./...` (incl. `-race` on the
  upstream, llm and tests packages) — all green.
