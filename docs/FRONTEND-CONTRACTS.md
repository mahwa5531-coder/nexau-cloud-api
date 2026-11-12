# MASh Cloud API — Frontend Contracts (v1)

The exact wire contracts for every endpoint a client (desktop app or web
frontend) can call. Field names, statuses, headers and error codes below are
taken verbatim from the handlers in `internal/api/` — this file is the
client-integration source of truth.

A machine-readable mirror of this catalog lives at
`docs/frontend-contracts.json` (same ids, same examples) for rendering the
contract list dynamically in a frontend.

---

## 1. Conventions that apply everywhere

| Rule | Value |
|---|---|
| Content type | `application/json` (all request/response bodies except the SSE stream) |
| Auth header | `Authorization: Bearer <access_token>` |
| Request id | Every response carries `X-Request-Id`. You may send your own (charset `[A-Za-z0-9._-]`, ≤128 chars); otherwise the server mints one. Quote it in bug reports. |
| Timestamps | RFC3339, UTC (e.g. `2026-11-07T09:15:00Z`) |
| Money | Integer **paise** (INR): `199900` = ₹1,999.00 |
| Token counts | "Normalized tokens": each model's usage weighted by its plan normalization rule; the same numbers the quota gate enforces |
| One JSON body | Exactly one JSON document per request — any trailing bytes/garbage ⇒ `400 INVALID_REQUEST` |
| 405 | Wrong verb on a known path ⇒ `405 METHOD_NOT_ALLOWED` + `Allow` header |
| 429 | Always carries `Retry-After` (seconds) + `details.retry_after_ms` |

### The error envelope (every non-2xx response)

```json
{
  "error": {
    "code": "WINDOW_QUOTA_EXCEEDED",
    "message": "The token quota for this plan window has been reached.",
    "request_id": "req_01JD2Z9K3M",
    "details": {
      "window": "5h",
      "quota_tokens": 60000,
      "used_tokens": 61250,
      "resets_at": "2026-11-07T12:41:03Z"
    }
  }
}
```

- `code` — the machine-readable switch (full catalog in §9)
- `message` — human-readable, safe to print
- `request_id` — present on every error
- `details` — optional, code-specific, documented per code in §9

### Session classes (which token may call what)

| Class | How obtained | Scope | Lifetime |
|---|---|---|---|
| Device token | `POST /v1/auth/desktop/exchange`, `POST /v1/auth/refresh` | `api` | 15 min (refreshable for 30 days) |
| Web session token | `POST /v1/auth/web/session` | `web` | 15 min, **no refresh token** |

Routes marked **device-only** answer a web token with `403 FORBIDDEN`
("This route requires a desktop session."). The browser session exists to
show a profile and run the payment flow — it is never an LLM credential.

### Token clocks (defaults; server-tunable)

| Credential | Default TTL |
|---|---|
| Access token | 15 min |
| Refresh token (rotating) | 30 days |
| Web session token | 15 min (no refresh) |
| OAuth state / web grant | 10 min / 5 min |
| Desktop pairing code (`mcode`) | **60 s**, single-use |
| Payment order (abandoned) | 15 min (late capture honored for 24 h) |

`expires_in` in every token response is **seconds**.

---

## 2. Endpoint index

| # | Method & path | Auth | What it does |
|---|---|---|---|
| 1 | `GET /health/live` | none | Process is up |
| 2 | `GET /health/ready` | none | Dependencies (PG/Redis/upstream) answer |
| 3 | `POST /v1/auth/refresh` | none | Rotate the desktop token pair |
| 4 | `POST /v1/auth/logout` | session | Revoke one refresh token |
| 5 | `POST /v1/auth/logout-all` | session | Revoke every session |
| 6 | `GET /v1/auth/oauth/google` | browser | Begin "Continue with Google" (302) |
| 7 | `GET /v1/auth/oauth/google/callback` | browser | Finish OAuth (302 with grant) |
| 8 | `POST /v1/auth/web/session` | none (grant) | Grant → short web session token |
| 9 | `POST /v1/auth/desktop/code` | session | Mint the 60 s single-use `mcode` |
| 10 | `POST /v1/auth/desktop/exchange` | none (mcode) | `mcode` → full desktop TokenPair |
| 11 | `GET /v1/me` | session | Identity + plan + live quota position |
| 12 | `GET /v1/config` | session | Client integration config |
| 13 | `POST /v1/chat/completions` | **device** | THE LLM endpoint (JSON or SSE) |
| 14 | `GET /v1/usage` | **device** | Usage aggregation over a window |
| 15 | `GET /v1/payments/catalog` | session | Purchasable plan SKUs |
| 16 | `POST /v1/payments/checkout` | session | Create a payment order |
| 17 | `POST /v1/payments/confirm` | session | Settle after paying |
| 18 | `GET /v1/payments/orders/{order_id}` | session | One order's status |
| 19 | `GET /v1/payments/history` | session | Order history |
| 20 | `GET /v1/payments/subscription` | session | Plan + paid-through date |
| 21 | `POST /v1/payments/webhook` | provider sig | **Not for your frontend** — provider → server |

`GET /metrics` (Prometheus) exists when the deployment enables it.

---

## 3. Health

### 3.1 `GET /health/live` — liveness

Unauthenticated, cheap, never touches dependencies.

```json
{ "status": "ok", "time": "2026-11-07T09:00:00Z" }
```

### 3.2 `GET /health/ready` — readiness

`200` when every checked dependency answers; `503` when any hard dependency
fails. `upstream_circuit` ("closed" | "open" | …) is informational only.

```json
{
  "status": "ok",
  "dependencies": [
    { "name": "postgres",  "ok": true, "latency_ms": 2 },
    { "name": "redis",     "ok": true, "latency_ms": 1 },
    { "name": "openrouter","ok": true, "latency_ms": 143 }
  ],
  "upstream_circuit": "closed",
  "time": "2026-11-07T09:00:00Z"
}
```

---

## 4. Auth & session

### 4.1 `POST /v1/auth/refresh` — rotate the desktop token pair

Public (per-IP throttled). The desktop calls this whenever the access token
is near expiry, and **must persist the new pair** — the old refresh token is
consumed by the rotation.

Request:

```json
{ "refresh_token": "rt_9f2c…" }
```

Response `200` — a full `TokenPair` (the same shape desktop/exchange returns):

```json
{
  "access_token": "eyJhbGciOi…",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "rt_51ab…",
  "user": {
    "id": "usr_2y8M",
    "email": "asha@example.com",
    "display_name": "Asha",
    "avatar_url": "https://lh3.googleusercontent.com/…",
    "status": "active",
    "is_platform_admin": false,
    "email_verified": true
  },
  "tenant": {
    "id": "tn_7Qk1",
    "slug": "asha-firm",
    "name": "Asha & Co",
    "status": "active"
  }
}
```

Rotation contract:

- Presenting the **current** token ⇒ new pair, old token dead.
- Presenting a **consumed/revoked** token ⇒ `401 REFRESH_TOKEN_REUSED` and the
  server revokes the whole token family (every device logged out — the theft
  response). Surface "please sign in again" when this arrives.
- Presenting a merely **expired** token ⇒ clean `401 REFRESH_TOKEN_EXPIRED`;
  just sign in again.

Errors: `INVALID_REQUEST` (400), `REFRESH_TOKEN_EXPIRED` / `REFRESH_TOKEN_REUSED`
/ `ACCOUNT_INACTIVE` (401), `RATE_LIMITED` (429, scope `refresh_ip`,
`Retry-After: 60`), `INTERNAL` (500).

### 4.2 `POST /v1/auth/logout` — sign out this device

Authenticated (either session class). Body optional:

```json
{ "refresh_token": "rt_51ab…" }
```

Response `200`: `{ "logged_out": true }`

### 4.3 `POST /v1/auth/logout-all` — sign out everywhere

Authenticated. Revokes every refresh-token family for the user; the presented
access token is blacklisted for its remaining lifetime.

Response `200`:

```json
{ "logged_out": true, "sessions_killed": 3 }
```

### 4.4 `GET /v1/auth/oauth/google` — begin "Continue with Google"

**Top-level browser navigation** (it sets the `mash_oauth_tx` HttpOnly
cookie on this hop). Answers `302` to Google's consent screen. Not a fetch()
target.

### 4.5 `GET /v1/auth/oauth/google/callback?code=…&state=…`

Google lands here. On success: `302` to the web success page with the
single-use grant as a query param — `…?grant=<code>`. On failure: `302` to
the web login page with `…?error=<code>` (e.g. `OAUTH_STATE_INVALID`,
`OAUTH_EMAIL_UNVERIFIED`). **No tokens ever appear in URLs.**

### 4.6 `POST /v1/auth/web/session` — grant → web session token

Called by the web page after the success redirect. Public (per-IP throttled).

Request: `{ "grant": "<single-use code from the redirect>" }`

Response `200` — a `WebSession` (note: **no refresh token**; keep it in page
memory only):

```json
{
  "access_token": "eyJhbGciOi…",
  "token_type": "Bearer",
  "expires_in": 900,
  "user":   { "id": "usr_2y8M", "email": "asha@example.com", "display_name": "Asha", "status": "active", "is_platform_admin": false },
  "tenant": { "id": "tn_7Qk1", "slug": "asha-firm", "name": "Asha & Co", "status": "active" }
}
```

Errors: `INVALID_OR_EXPIRED_CODE` (400), `RATE_LIMITED` (429).

### 4.7 `POST /v1/auth/desktop/code` — mint the pairing code

Authenticated (web session or an already-paired device). The web page shows
this code to the user / passes it via deep link or loopback. Per-user rate
limited.

Response `200`:

```json
{ "code": "mcode_8Kd0…", "expires_in": 60 }
```

### 4.8 `POST /v1/auth/desktop/exchange` — the desktop's login

Public (per-IP throttled). Single-use: the losing channel of the dual-channel
handshake gets `INVALID_OR_EXPIRED_CODE`.

Request (only `code` required; the rest label the device):

```json
{
  "code": "mcode_8Kd0…",
  "device_name": "Asha's MacBook",
  "platform": "macos",
  "device_id": "dev_3f9a2b"
}
```

Response `200` — the full `TokenPair` (shape in §4.1): 15-minute access
token + 30-day rotating refresh token. **This is the desktop's only login.**

Field limits: `code` ≤128, `device_name` ≤128, `platform` ≤32,
`device_id` ≤64 chars.

Errors: `INVALID_OR_EXPIRED_CODE` (400), `INVALID_REQUEST` (400),
`ACCOUNT_INACTIVE` (401), `RATE_LIMITED` (429, scope `desktop_exchange_ip`).

---

## 5. Identity & client config

### 5.1 `GET /v1/me` — who am I, what's my plan, where's my quota

Authenticated (either class). Read-budgeted (default 60 req/min/user —
`429 RATE_LIMITED`, scope `read_rpm`). This is the desktop dashboard's poll:
the quota numbers are the exact sums the enforcement gate reads (through the
same cache), so the usage bar can never disagree with a rejected call.

Response `200`:

```json
{
  "user": {
    "id": "usr_2y8M",
    "email": "asha@example.com",
    "display_name": "Asha",
    "avatar_url": "https://lh3.googleusercontent.com/…",
    "status": "active",
    "is_platform_admin": false,
    "email_verified": true
  },
  "tenant": {
    "id": "tn_7Qk1",
    "slug": "asha-firm",
    "name": "Asha & Co",
    "status": "active"
  },
  "membership": { "role": "owner", "status": "active" },
  "subscription_status": "active",
  "plan": { "id": "pln_pro", "code": "pro", "name": "Pro" },
  "limits": {
    "requests_per_minute_user": 60,
    "requests_per_minute_tenant": 300,
    "concurrent_requests_user": 8,
    "concurrent_requests_tenant": 32,
    "max_request_bytes": 2097152,
    "window_5h_tokens": 60000,
    "window_weekly_tokens": 500000
  },
  "models": ["openai/gpt-6-luna", "google/gemini-3-flash"],
  "restricted": true,
  "quota": {
    "currency": "normalized_tokens",
    "windows": [
      {
        "window": "5h",
        "used_tokens": 12400,
        "quota_tokens": 60000,
        "used_percent": 20,
        "resets_at": null
      },
      {
        "window": "weekly",
        "used_tokens": 503210,
        "quota_tokens": 500000,
        "used_percent": 100,
        "resets_at": "2026-11-08T04:12:55Z"
      }
    ]
  }
}
```

Field notes:

- `plan.code` is `"free"` for every tenant without an active subscription —
  always renderable.
- `models` is the allowlist when `restricted` is `true`; when `false` the
  tenant may call any configured model.
- `quota.windows[]` — one object per window the plan defines (`"5h"`,
  `"weekly"`).
- `resets_at` is `null` unless `used_tokens >= quota_tokens`; then it is the
  exact earliest instant usage drops back under quota (schedule the retry
  with it).
- `used_percent` is floor'd and capped at 100: a bar shows 99% while the
  next call may still be admitted; it shows 100% exactly when the gate
  rejects.
- The whole `quota` block is **omitted** if the usage aggregation fails
  (degraded, not wrong) — render "usage unavailable", never zeros.

### 5.2 `GET /v1/config` — integration config

Authenticated. No secrets. Example (defaults shown):

```json
{
  "version": "v1",
  "transports": {
    "llm": "POST /v1/chat/completions (OpenAI-compatible; model = \"provider/model\"; SSE when stream=true)",
    "events": "OpenAI chat-completions chunks, forwarded verbatim"
  },
  "limits": {
    "max_request_bytes": 2097152,
    "max_messages": 256,
    "max_tools": 128,
    "max_model_len": 256
  },
  "stream": {
    "idle_timeout_s": 300,
    "max_duration_s": 900,
    "heartbeat_s": 15,
    "terminal_event": "data: [DONE]"
  },
  "models": ["openai/gpt-6-luna", "google/gemini-3-flash"],
  "restricted": true,
  "server_time": "2026-11-07T09:00:00Z"
}
```

`models` is `null` when the tenant is unrestricted (any model allowed).

---

## 6. The LLM endpoint

### 6.1 `POST /v1/chat/completions` — **device-only**

OpenAI-compatible pass-through tunnel to OpenRouter. Point your OpenAI-style
client at it; `model` is `"provider/model"`.

Request (accepted fields — a validated subset, forwarded upstream):

```json
{
  "model": "openai/gpt-6-luna",
  "messages": [
    { "role": "system", "content": "You are a careful assistant." },
    { "role": "user", "content": "hello" }
  ],
  "stream": true,

  "tools": [ { "type": "function", "function": { "name": "get_weather", "parameters": {…} } } ],
  "tool_choice": "auto",

  "temperature": 0.7,
  "top_p": 0.9,
  "max_completion_tokens": 1024,
  "max_tokens": 1024,
  "stop": ["\n\n"],
  "seed": 42,
  "frequency_penalty": 0,
  "presence_penalty": 0,
  "logit_bias": { "50256": -100 },
  "logprobs": false,
  "top_logprobs": 5,
  "parallel_tool_calls": true,
  "response_format": { "type": "json_object" },
  "reasoning_effort": "medium",
  "fallbacks": ["anthropic/claude-sonnet-5.5"],
  "metadata": { "session": "abc" }
}
```

Unknown top-level keys pass through deliberately (bounded, JSON-typed) so
new provider parameters need no cloud release. Message `role` is
`system|user|assistant|tool|developer`; `content` may be a string or the
OpenAI content-parts array.

**Non-stream response `200`:** the upstream completion JSON, forwarded
verbatim (OpenAI shape: `id`, `object`, `choices[]`, `usage{…}`), plus the
`X-Ratelimit-*` headers of §8.

**Stream response `200` (`stream:true`):** `text/event-stream` —

```
data: {"id":"chatcmpl-…","choices":[{"delta":{"content":"Hel"}}],…}

: ping

data: {"id":"chatcmpl-…","choices":[{"delta":{"content":"lo"}}],…}

data: {"error":{"message":"stream exceeded the maximum duration","type":"…","code":"MAX_DURATION"}}

data: [DONE]
```

Stream semantics:

- Frames are raw OpenAI-style chunks: `data: {json}\n\n`, terminal
  `data: [DONE]\n\n`.
- Keepalives are SSE comments (`: ping`), one per `heartbeat_s` — ignore
  them, never treat as data.
- Usage, when the upstream includes it, rides the final data frame(s).
- The HTTP 200 + SSE headers commit only when the first upstream byte is
  ready: pre-stream failures answer as **real HTTP errors** (OpenAI SDKs
  observe status + code).
- A failure after bytes flowed arrives as one in-band
  `{"error":{message,type,code}}` event before `[DONE]`.
- The server cuts streams at `max_duration_s` (code `MAX_DURATION`) and
  aborts stalled ones at `idle_timeout_s` (code `MODEL_TIMEOUT`).

Errors: `INVALID_REQUEST` (400, incl. bad model format / too many messages /
oversized body), `PAYLOAD_TOO_LARGE` (413, `details.limit_bytes` — the
plan's byte ceiling), `MODEL_NOT_ENTITLED` (403, `details.model`), `FORBIDDEN`
(403 — web token), `TOKEN_EXPIRED`/`TOKEN_REVOKED`/`UNAUTHORIZED` (401),
`TENANT_SUSPENDED` (403), `RATE_LIMITED` (429), `CONCURRENCY_LIMIT` (429),
`WINDOW_QUOTA_EXCEEDED` (429), `UPSTREAM_UNAVAILABLE`/`UPSTREAM_PROTOCOL`
(502), `MODEL_TIMEOUT` (504), `SERVER_SHUTDOWN` (503).

---

## 7. Usage

### 7.1 `GET /v1/usage` — **device-only**, read-budgeted

Tenant-scoped aggregation of metered usage over a window (never
client-reported numbers).

Query params (all optional):

| Param | Meaning | Default / bounds |
|---|---|---|
| `days` | trailing window in days | 30; must be an integer in `[1,366]` |
| `from` | window start, RFC3339 | `now − 30d`; must precede `to` |
| `to` | window end, RFC3339 | `now` |
| `by_model` | `true` adds the per-model breakdown | absent |

Max span 366 days — larger ⇒ `400 INVALID_REQUEST`.

Response `200`:

```json
{
  "tenant_id": "tn_7Qk1",
  "from": "2026-10-08T09:00:00Z",
  "to": "2026-11-07T09:00:00Z",
  "calls": 214,
  "input_tokens": 812345,
  "output_tokens": 391204,
  "total_tokens": 1203549,
  "reasoning_tokens": 50211,
  "cache_read_tokens": 120433,
  "cache_write_tokens": 8801,
  "total_cost": 17.42,
  "by_model": [
    { "model": "openai/gpt-6-luna", "calls": 190, "input_tokens": 770001, "output_tokens": 350000, "total_tokens": 1120001, "total_cost": 15.9 },
    { "model": "google/gemini-3-flash", "calls": 24, "input_tokens": 42344, "output_tokens": 41204, "total_tokens": 83548, "total_cost": 1.52 }
  ]
}
```

`by_model[]` is sorted by `total_tokens` DESC and present only when
`by_model=true`. `total_cost` is USD as billed by the upstream.

---

## 8. Rate-limit response headers (chat endpoint)

Every chat response — success or 429 — carries the live admission position,
so the desktop can render meters without polling `/v1/me`. A header is
omitted when the plan doesn't set that dimension or the value is unknown.

| Header | Meaning |
|---|---|
| `X-Ratelimit-Limit-Requests` | Your per-minute request ceiling (user scope) |
| `X-Ratelimit-Remaining-Requests` | Requests left in this minute (this call counted) |
| `X-Ratelimit-Limit-Concurrency` | Your concurrent-stream ceiling |
| `X-Ratelimit-Active-Concurrency` | Holders observed at claim time (you included) |
| `X-Ratelimit-Limit-Tokens` | 5 h normalized-token budget |
| `X-Ratelimit-Remaining-Tokens` | 5 h budget left (metered + reservations) |
| `X-Ratelimit-Limit-Tokens-Weekly` | Weekly budget |
| `X-Ratelimit-Remaining-Tokens-Weekly` | Weekly budget left |

On a 429, the exhausted dimension's remaining-header is pinned to `0`
(e.g. `WINDOW_QUOTA_EXCEEDED` on the weekly window ⇒ 
`X-Ratelimit-Remaining-Tokens-Weekly: 0`).

---

## 9. Error-code catalog (the complete switch)

| Code | HTTP | `details` | When |
|---|---|---|---|
| `INVALID_REQUEST` | 400 | — | Malformed JSON, bad/missing fields, bad query params |
| `INVALID_OR_EXPIRED_CODE` | 400 | — | Bad/replayed/expired grant or `mcode` |
| `OAUTH_STATE_INVALID` | 400 | — | OAuth state replay / tx-cookie mismatch |
| `UNAUTHORIZED` | 401 | — | Missing/garbage Bearer token |
| `TOKEN_EXPIRED` | 401 | — | Access token past expiry — refresh and retry |
| `TOKEN_REVOKED` | 401 | — | Access token blacklisted (logout-all) |
| `REFRESH_TOKEN_EXPIRED` | 401 | — | Refresh token past 30 days — sign in again |
| `REFRESH_TOKEN_REUSED` | 401 | — | Reuse detected — **all sessions revoked** — sign in again |
| `ACCOUNT_INACTIVE` | 401 | — | User or account deactivated |
| `FORBIDDEN` | 403 | — | Web token on a device-only route; account state |
| `TENANT_SUSPENDED` | 403 | — | Tenant suspended |
| `SUBSCRIPTION_INACTIVE` | 403 | — | No active subscription where one is required |
| `MODEL_NOT_ENTITLED` | 403 | `model` | Model not in the tenant's allowlist |
| `OAUTH_EMAIL_UNVERIFIED` | 403 | — | Google email not verified |
| `NOT_FOUND` | 404 | — | Unknown route or unknown order id |
| `METHOD_NOT_ALLOWED` | 405 | `allow` | Wrong verb (see `Allow` header) |
| `IDEMPOTENCY_KEY_REUSE` | 422 | — | Same Idempotency-Key, different body |
| `PAYLOAD_TOO_LARGE` | 413 | `limit_bytes` | Body over the plan's byte ceiling |
| `IDENTITY_CONFLICT` | 409 | — | Email already linked to another identity |
| `RATE_LIMITED` | 429 | `scope`, `retry_after_ms` | RPM/read/checkout/webhook budget (+ `Retry-After`) |
| `CONCURRENCY_LIMIT` | 429 | `scope` | Concurrent-stream ceiling reached |
| `WINDOW_QUOTA_EXCEEDED` | 429 | `window`, `quota_tokens`, `used_tokens`, `resets_at?` | 5 h or weekly token budget exhausted — schedule with `resets_at` |
| `INTERNAL` | 500 | — | Server fault (quote `request_id`) |
| `UPSTREAM_UNAVAILABLE` | 502 | — | OpenRouter unreachable |
| `UPSTREAM_PROTOCOL` | 502 | — | Malformed upstream response |
| `OAUTH_PROVIDER_ERROR` | 502 | — | Google leg failed |
| `PAYMENT_PROVIDER_AUTH` | 502 | — | Provider rejected deployment credentials |
| `PAYMENT_PROVIDER_ERROR` | 502 | — | Provider rejected the request |
| `MODEL_TIMEOUT` | 504 | `scope` | Model request timed out |
| `SERVER_SHUTDOWN` | 503 | — | Server draining — retry with backoff |
| `OAUTH_DISABLED` | 503 | — | OAuth not configured on deployment |
| `PAYMENTS_DISABLED` | 503 | — | Payments not enabled on deployment |
| `DEPENDENCY_UNAVAILABLE` | 503 | `dependency` | PG/Redis down |
| `UNSUPPORTED_AUTH_MODE` | 501 | — | Deployment mints no tokens |
| `SKU_NOT_FOUND` | 404 | `sku_id` | Unknown `sku_id` at checkout |
| `PLAN_NOT_CONFIGURED` | 409 | `plan_code` | SKU's plan missing on deployment |
| `ORDER_NOT_OPEN` | 409 | `status` | Order already settled/expired |
| `PAYMENT_SIGNATURE_INVALID` | 400 | — | Confirm proof failed |
| `PAYMENT_AMOUNT_MISMATCH` | 409 | `expected_paise`, `received_paise` | Provider amount ≠ order |
| `DUPLICATE_IN_PROGRESS` | 409 | — | Identical checkout in flight — retry same key |
| `RECONCILE_RATE_LIMITED` | 429 | — | Too many order-status checks |
| `WEBHOOK_SIGNATURE_INVALID` | 401 | — | Webhook signature failed (server→server) |

In-band SSE error `code` values you can meet mid-stream: `MAX_DURATION`,
`MODEL_TIMEOUT`, `SERVER_SHUTDOWN`, `CLIENT_DISCONNECTED` (the last is
informational — you closed the stream).

---

## 10. Payments (plans only — no credit packs)

Every payments route answers `503 PAYMENTS_DISABLED` when the deployment
has payments off — render "not available here" and hide the purchase UI.

Order `status` state machine:
`pending` → `attempted` → `paid` (absorbing) | `failed`; `expired` when the
15-minute order TTL lapses (a later capture within 24 h still wins).

Every order response body is the same `order` object (`ClientView`):

```json
{
  "order": {
    "order_id": "ord_9c31",
    "provider_order_id": "order_NxK…",
    "provider_payment_id": "pay_Mb2…",
    "provider": "phonepe",
    "sku_id": "plan_pro_1m",
    "plan_code": "pro",
    "period_days": 30,
    "currency": "INR",
    "amount_paise": 199900,
    "status": "paid",
    "created_at": "2026-11-07T08:41:10Z",
    "paid_at": "2026-11-07T08:43:02Z",
    "expires_at": "2026-11-07T08:56:10Z"
  }
}
```

(`failure_reason` appears only on `failed`; `paid_at` only once paid.)

### 10.1 `GET /v1/payments/catalog`

```json
{
  "skus": [
    { "id": "plan_pro_1m", "label": "Pro — ₹1,999 / month",  "plan_code": "pro", "period_days": 30,  "amount_paise": 199900,  "badge": "Popular" },
    { "id": "plan_pro_1y", "label": "Pro — ₹19,999 / year",  "plan_code": "pro", "period_days": 365, "amount_paise": 1999900, "badge": "Best value" }
  ]
}
```

### 10.2 `POST /v1/payments/checkout` — create the order

**Send an `Idempotency-Key` header on every call** (any unique string you
keep for the retry) — network retries must never fork orders.

Request: `{ "sku_id": "plan_pro_1m" }`

Response `201` — provider handles for the browser:

```json
{
  "order": { "order_id": "ord_9c31", "status": "pending", … },
  "key_id": "rzp_live_…" ,
  "provider": "phonepe",
  "provider_order_id": "PAY-9c31-…",
  "redirect_url": "https://pay.phonepe.com/…"
}
```

Which handle is present depends on the active provider:
`redirect_url` (PhonePe — send the browser there wholesale), `client_secret`
(Stripe — for Stripe.js), `key_id` (Razorpay — for Checkout.js; public by
design).

Errors: `SKU_NOT_FOUND`, `PAYMENTS_DISABLED`, `RATE_LIMITED` (scope
`checkout_user`), `IDEMPOTENCY_KEY_REUSE` (422), `DUPLICATE_IN_PROGRESS`
(409), `PLAN_NOT_CONFIGURED`.

### 10.3 `POST /v1/payments/confirm` — settle after paying

Provider-shaped body; the generic core works for all providers:

```json
{
  "provider_order_id":   "PAY-9c31-…",
  "provider_payment_id": "T2311…",
  "provider_signature":  "…"
}
```

Legacy spellings also accepted: `razorpay_order_id` / `razorpay_payment_id`
/ `razorpay_signature`, `payment_intent_id` (+ optional `client_secret`),
`merchant_transaction_id`. Resolution order: generic → razorpay →
stripe/phonepe variants. For PhonePe/Stripe the server-side fetch is the
authority — pass the ids, not secrets.

Response `200`: `{ "order": { … "status": "paid" … } }`

Errors: `ORDER_NOT_OPEN`, `PAYMENT_SIGNATURE_INVALID`,
`PAYMENT_AMOUNT_MISMATCH`, `NOT_FOUND` (cross-tenant order), `RECONCILE_RATE_LIMITED`.

### 10.4 `GET /v1/payments/orders/{order_id}` — poll status

Reconciles with the provider when still `pending` (bounded rate). Response
`200`: `{ "order": {…} }`. Errors: `NOT_FOUND`, `RECONCILE_RATE_LIMITED`.

### 10.5 `GET /v1/payments/history?limit=50`

`limit` clamped to `[1,100]`, default 50, newest first:

```json
{ "orders": [ {…}, {…} ] }
```

### 10.6 `GET /v1/payments/subscription` — the entitlement position

```json
{
  "tenant_id": "tn_7Qk1",
  "plan_code": "pro",
  "plan_name": "Pro",
  "status": "active",
  "current_period_end": "2026-12-07T08:43:02Z",
  "days_remaining": 29
}
```

`status` ∈ `active | trialing | past_due | none`. When `none`: `plan_code`
is `""`, `current_period_end` absent, `days_remaining` 0 — the tenant is on
the Free default. When the paid period ends the read simply flips back to
`none`; there is no sweeper and no prorating.

### 10.7 `POST /v1/payments/webhook` — provider → server only

Unauthenticated by token **by design** (the provider signature over the raw
body is the auth). Your frontend never calls this.

---

## 11. The two journeys, end to end

**Desktop login (first time):**
1. Desktop opens `GET /v1/auth/oauth/google` in the system browser.
2. User consents; callback 302s to the web success page with `?grant=…`.
3. Web page POSTs the grant to `/v1/auth/web/session` → 15-min web token.
4. Web page POSTs `/v1/auth/desktop/code` → `{code, expires_in:60}`.
5. Code travels to the desktop (deep link or loopback).
6. Desktop POSTs `/v1/auth/desktop/exchange` → TokenPair. Done.
7. Before the access token's `expires_in` runs out: POST
   `/v1/auth/refresh` and **replace the stored pair**.

**Buy Pro (PhonePe shape):**
1. `GET /v1/payments/catalog` → render SKUs.
2. `POST /v1/payments/checkout` (with `Idempotency-Key`) → `redirect_url`.
3. Send the browser there; user pays.
4. `POST /v1/payments/confirm` (ids from checkout + provider) → order
   `paid`; or poll `GET /v1/payments/orders/{id}` until the webhook lands.
5. `GET /v1/payments/subscription` / `GET /v1/me` now show Pro ceilings.

---

Machine-readable mirror: `docs/frontend-contracts.json`.
Payment flows in depth: `docs/FRONTEND-PAYMENTS.md`. Pairing transport
(deep link vs loopback): `docs/DESKTOP-CONNECTION.md`.
