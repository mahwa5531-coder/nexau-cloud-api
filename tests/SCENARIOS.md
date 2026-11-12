# MASh Cloud API — The Complete User-Scenario Catalog

Phase 1 of the user-journey audit (2026-11-05): every scenario a real user
can perform against this system, researched from the source and written
down BEFORE testing. Phase 2 (`*_journeys_test.go`, `plan_lifecycle_pg_test.go`)
executes the ★ NEW rows against the real code.

Six families, one per hat a person wears:

| Family | Hat | Question it answers |
|---|---|---|
| **A** — Sign-in & session | the new/returning desktop user | "how do I get in, and what happens to my tokens?" |
| **P** — Payments | the buyer | "how do I pay, and what if something goes wrong?" |
| **T** — Tokens & quota | the caller | "how does my usage count, and what happens when I run out?" |
| **L** — Plan lifecycle | the subscriber over weeks | "what does buying time do, and what happens when I stop paying?" |
| **X** — Limits & attack | the hostile actor | "what stops me?" |
| **I** — Infrastructure | nobody (the server's own life) | already pinned by the B/R/D/P suites |

Coverage legend: `S03`/`B04`/`R06`/`D02`/`P05` = `tests/` scenario suites ·
`internal` = in-package unit/e2e tests next to the code · **★ NEW** = added by
phase 2 · ○ = not yet pinned anywhere (honest gaps).

Clocks worth memorizing (all defaults, all env-tunable):

| Clock | Default | Where |
|---|---|---|
| Access token (HS256, scope `api`) | 15 min | `config.go:293` |
| Refresh token (rotating, 30-day) | 30 days | `config.go:294` |
| Web session token (scope `web`, NO refresh) | 15 min | `config.go:308` |
| OAuth state / web grant / desktop code | 10 min / 5 min / 60 s | `config.go:309-311` |
| Identity cache (plan/limits staleness) | 30 s | `NormCacheTTL` family, `identity.go:27` |
| Usage cache on the quota hot path | 5 s | `main.go` wiring |
| Quota reservation (Redis) | 20 min | `ReserveTTL` |
| Payment order TTL / late-capture window | 15 min / 24 h | `config.go:357,359` |

---

## Family A — Sign-in & session

The only identity provider is Google OAuth (PKCE S256 + OIDC nonce, single-use
state bound to a tx cookie — `oauth.go:99-113, 375-444`). The browser NEVER
holds a long-lived credential: the web session is a 15-minute token with no
refresh. The desktop gets its full pair only through the 60-second single-use
`mcode` handshake (`desktop_code.go`), delivered over deep link or loopback.

| # | User journey | Expected behavior | Enforced at | Coverage |
|---|---|---|---|---|
| A01 | First-ever "Continue with Google" | User+tenant+membership provisioned, Free plan, web grant minted | `oauth.go:451` (`FindOrCreateGoogleUser`), `identity.go:124` free fallback | ★ NEW (journey), internal (provisioning cases) |
| A02 | Returning sign-in, same Google account | Same user/tenant resolved — never a duplicate account | `oauth.go:451` login case | ★ NEW, internal |
| A03 | Same email, different Google account (subject changed) | Identity conflict — refused, no silent merge | `oauth.go:462` `ErrIdentityConflict` | ★ NEW |
| A04 | Google account with unverified email | OAuth completes but web grant refused | `oauth.go:420` | ★ NEW |
| A05 | Replay/forge the OAuth `state` (CSRF attempt) | Single-use consume + constant-time tx-cookie compare → one generic error, no oracle | `oauth.go:402-414` | ★ NEW |
| A06 | Authorization code replayed / stolen (no verifier) | PKCE: token exchange fails — the server-held verifier never matches | `oauth.go:127` | ○ (needs a hostile fake Google; documented, not pinned) |
| A07 | ID token nonce mismatch (replayed token) | Exchange fails — nonce bound to this login attempt | `oauth.go:99-102` | ★ NEW (via fake Google) |
| A08 | Complete web grant → `POST /v1/auth/web/session` | Short web token minted; grant single-use | `oauth.go:484` | ★ NEW |
| A09 | Web grant reused / expired | Second exchange fails like any unknown code | `oauth.go:488` GETDEL | ★ NEW |
| A10 | Pair desktop: request `mcode` from the web page | `mcode_<64hex>`, 60 s TTL, bound to user+tenant | `desktop_code.go:53` | ★ NEW |
| A11 | Desktop exchanges the code (deep link wins, loopback also fires) | Exactly ONE full token pair; the losing channel gets INVALID_OR_EXPIRED_CODE | `desktop_code.go:91` atomic GETDEL | ★ NEW |
| A12 | Code used after 60 s expiry | Exchange fails; user re-requests | `desktop_code.go:59` TTL | ★ NEW |
| A13 | Garbage/malformed code, wrong platform field, 300-char device name | 400 with the field named; name truncated on rune boundary | `desktop_code.go:72-88` | internal (`desktop_code_test`, `batch4`) |
| A14 | Exchange for a deactivated account | 401 ACCOUNT_INACTIVE, no tokens | `desktop_code.go:104` | ★ NEW |
| A15 | Access token expires mid-session (15 min) | 401 token expired; client rotates via refresh | `jwt.go:39` | ★ NEW |
| A16 | Refresh rotation (normal path) | New pair; old refresh atomically consumed | `service.go:88` `Rotate` | ★ NEW, internal |
| A17 | REUSE a consumed refresh token (theft response) | Whole token family revoked — attacker AND victim re-login | `service.go` reuse→`RevokeFamily` | ★ NEW, internal |
| A18 | Refresh token merely EXPIRED (30 days idle) | Clean 401, family NOT purged (no theft signal) | `service.go` (audit M5) | ★ NEW |
| A19 | Logout one device | That refresh revoked; access JTI blacklisted for its remaining TTL | `service.go:174` | ★ NEW, internal |
| A20 | Logout-all (lost laptop) | Every session/family for the user revoked | `service.go:190` | internal (`logoutall_test`) |
| A21 | Web (browser) token used against `/v1/chat/completions` or `/v1/usage` | 403 — device scope required; XSS blast radius = profile view | `middleware.go:74` `RequireDevice` | ★ NEW |
| A22 | Tampered signature / wrong issuer / missing exp on any token | Classified 401, no oracle detail | `jwt.go:59-106` | internal (`auth_test`) |
| A23 | Redis down while revocation checking is on | Fail-open WITH counter+log (documented degraded window) | `service.go:243` | internal |
| A24 | Plan/limits changed in DB (or payment applied) | New limits visible within the 30 s identity cache TTL | `identity.go:47-67` | ★ NEW (see L-series) |

## Family P — Payments

Plans only — no credits, no wallet. Two SKUs: `plan_pro_1m` (₹1,999 / 30 days)
and `plan_pro_1y` (₹19,999 / 365 days) (`catalog.go:34`). **There is no weekly
SKU**; the catalog bounds allow an operator to add one (1–3660 days) via
`NEXAU_PAYMENT_CATALOG_JSON`. Order state machine: pending → attempted →
paid/failed/expired, with paid absorbing and late-capture-wins
(`domain.go:117-135`).

| # | User journey | Expected behavior | Enforced at | Coverage |
|---|---|---|---|---|
| P01 | Browse `/v1/payments/catalog` | Both SKUs, cheapest first, no secrets | `catalog.go:80` | ★ NEW |
| P02 | Checkout, happy path | Provider order + local pending row (15 min TTL) | `checkout.go:25` | ★ NEW, internal |
| P03 | Network retry duplicates the checkout (same idempotency key) | SAME order replayed — never a second row | `checkout.go:57-67` | ★ NEW, internal |
| P04 | Same idempotency key, DIFFERENT SKU | 422 — key is bound to the request hash | `checkout.go:59` | ★ NEW, internal |
| P05 | Unknown SKU / plan deleted mid-flight | 404 SKU / 422 plan-missing guard before anything is created | `checkout.go:29-43` | ★ NEW |
| P06 | Client retry-storm on checkout | Per-user hourly budget (default 20) → 429 | `checkout.go:47-52` | ★ NEW |
| P07 | Provider echoes a different amount | Loud failure, nothing created, nothing activatable | `checkout.go:87-92` | ★ NEW |
| P08 | Pay in browser, return, `POST /v1/payments/confirm` | 3-legged verify (signature + provider fetch + store tx) → paid + plan active | `confirm.go:30` | ★ NEW, internal |
| P09 | Confirm an order that is not mine (cross-tenant) | 404 identical to unknown — no existence oracle | `confirm.go:50` | ★ NEW |
| P10 | Webhook already applied the payment, THEN user confirms | Confirm returns the paid order (settled success, not conflict) | `confirm.go:56` | ★ NEW |
| P11 | Confirm with a forged/absent checkout signature | 403-class reject before any provider call | `confirm.go:66` | ★ NEW |
| P12 | Hand-crafted tiny payment against a big order | Amount mismatch alarm; order stays pending for truth | `confirm.go:82-88` | ★ NEW |
| P13 | Webhook arrives: payment.captured | Dedupe-recorded, amount cross-checked, applied exactly once | `webhook.go:28-148` | ★ NEW, internal |
| P14 | Duplicate webhook (provider retries) | Ack + converge: if recorded-but-unapplied, apply now (audit H4) | `webhook.go:78-98` | ★ NEW, internal |
| P15 | Out-of-order: payment.failed AFTER payment.captured | Failed is dropped — paid is absorbing | `webhook.go:142`, `apply.go:46` | ★ NEW, internal |
| P16 | Webhook with forged signature / wrong amount / unknown order | Reject / alarm-and-hold / record-and-ack (orphan) | `webhook.go:45,88,117-134` | ★ NEW, internal |
| P17 | Refund / chargeback event | RECORDED + loud warn; entitlement NOT auto-reversed (v2 manual) | `webhook.go:101-115` | ★ NEW |
| P18 | Webhook flood from one IP | Per-IP minute budget (default 120) → 429 before HMAC+DB | `webhook.go:34-39` | ★ NEW |
| P19 | User abandons checkout (closes tab) | Order expires via sweeper after TTL; nothing activated | `domain.go` `ExpireStale`, `housekeeping` | ★ NEW |
| P20 | User pays but the webhook AND the browser are lost | Reconcile sweep re-checks open + recently-expired orders (24 h window) → converges to paid | `config.go:359`, `reconcile.go` | internal (`reconcile_test`) |
| P21 | Late capture lands on an EXPIRED order | Expired → paid wins (money is real) | `domain.go:127` | ★ NEW |
| P22 | Payment simply fails | Order failed (terminal); user can check out again cleanly | `apply.go:46` | ★ NEW |
| P23 | Payments disabled deployment | All payment routes answer 503 PAYMENTS_DISABLED (contract stable) | `router.go:151-160`, `errors.go` | internal (`api/payments_test`) |

## Family T — Tokens & quota (our side of every call)

Admission order (`admit()`, cheapest first): shutdown → subscription → payload
shape → model permission → concurrency slots → RPM → PG quota windows. Two
rolling normalized-token windows — 5-hour burst + 7-day weekly — and **no
calendar-month reset anywhere** (migration 0008 removed it): capacity returns
call-by-call as rows age out.

| # | User journey | Expected behavior | Enforced at | Coverage |
|---|---|---|---|---|
| T01 | First call ever (empty history) | Admitted; zero sums on an empty window | `usage.go:135` | S01 |
| T02 | Normal daily usage | Weighted tokens accumulate; estimate reconciled to actual | `quota.go`, `metering` | S02, R06 |
| T03 | Burst: fill the 5-hour window | 429 `WINDOW_QUOTA_EXCEEDED` + exact `resets_at` | `quota.go` `WindowRecovery` | S03 ★ re-pinned |
| T04 | Sustained: fill the 7-day window while 5h is fine | Separate ceiling, same contract | | S04 |
| T05 | Wait for capacity | Old rows age out; recovery detected on the VERY next call (over-quota never cached) | R05 semantics | S03/S05, R04-R05 |
| T06 | Hit quota mid-stream | Running streams never killed; only admissions gated | admission-time only | S08 |
| T07 | Disconnect mid-stream | Partial usage still metered (provider billed it) | | S09 |
| T08 | Pick an expensive model | Normalization weights set the burn rate (e.g. sonnet 20×/100× vs luna 1×/5×) | `norm.go` | S12 |
| T09 | Operator changes weights | New calls burn at the new rate; stored history untouched | | S13, P05 |
| T10 | Fan out N parallel calls at the last of the budget | Atomic Lua reservation — exactly the budget's worth admitted | reserve script | S05 |
| T11 | Quota rejection under concurrency load | Rejected calls return their slots; no leak | | S05b |
| T12 | Call without `max_tokens` | Reservation = prompt/4 + 2048 default completion bound | | R06 |
| T13 | Send tools | Estimate widened per tool (description+parameters+128)/4 | | R07 |
| T14 | Exceed RPM (Free 20/min) | 429 with honest `retry-after`; a minute later, welcome again | `ratelimit` | R09 |
| T15 | Exceed concurrent requests (Free 3) | Admission blocked while in-flight; SlotTTL self-heals crashed holders | | R08 |
| T16 | Spam `/v1/me` + `/v1/usage` | Separate read-RPM budget | `router.go:216` | internal (`me_test`), ○ journey |

## Family L — Plan lifecycle (the "what happens when I don't pay" family)

A purchase buys a PLAN PERIOD, prepaid: 30 days (monthly SKU) or 365 days
(yearly). Renewals stack: `paid_through = GREATEST(now, paid_through) +
period` (`payments.go:246-256`) — paying twice back-to-back never loses days.
Expiry is READ-SIDE: `Effective()` simply stops matching a row whose
`current_period_end` has passed (`repos/identity.go:132`), and the identity
resolver falls back to the Free plan's limits. **Nothing is deleted, ever** —
usage history survives every plan change; only the ceilings move.

| # | User journey | Expected behavior | Enforced at | Coverage |
|---|---|---|---|---|
| L01 | Brand-new user | Free by default: 20 RPM, 8 concurrent (migration 0015 raised it from the seed's 3 for desktop-agent parallelism), 60k/5h, 250k/7d | seed 000007 + 0015, `identity.go:111-141` | ★ NEW (PG) |
| L02 | Buy monthly → limits change | Pro ceilings (600 RPM, 16 conc, 2M/5h, 10M/7d) within ≤30 s identity cache; usage history untouched | `identity.go` cache | S06 ★ re-pinned, ★ NEW (PG) |
| L03 | Buy yearly instead | Same mechanics, 365-day period | `catalog.go:37` | ★ NEW (PG) |
| L04 | Pay twice back-to-back (renew early) | Periods STACK: end = max(now, end) + 30d | `payments.go:246` | ★ NEW (PG) |
| L05 | **Stop paying at period end** | Row simply stops being effective → Free ceilings on the next identity resolution (≤30 s cache staleness); NO usage deleted; capacity keeps rolling, nothing "resets" | `repos/identity.go:132` | ★ NEW (PG) |
| L06 | "What about weekly?" | There is no weekly billing period. The 7-day thing is the ROLLING QUOTA WINDOW both plans share — it refills continuously. If an operator adds a weekly SKU (bounds allow 1–3660 days), it behaves exactly like a 7-day period | `catalog.go:23-29` | ★ NEW (PG, 7-day SKU) |
| L07 | Don't pay mid-period (idle for 2 weeks) | NOTHING happens — the period is prepaid; Pro ceilings hold until `current_period_end` | read-side expiry | ★ NEW (PG) |
| L08 | Re-subscribe after lapse | Fresh subscription row inserts cleanly (expired row does not hold the unique slot); old usage still counted in rolling windows | `payments.go:260+` partial index | ★ NEW (PG) |
| L09 | Downgrade mid-period (operator flip) | Existing usage instantly re-evaluated against smaller ceilings | `identity.go` | S07 |
| L10 | Upgrade while quota-blocked | Ceiling rises, history never deleted, next admission passes | | S06 |
| L11 | Corrupt plan limits JSON in DB | Hard load failure — NEVER silent unlimited (zero = ∞ here) | `repos/identity.go:144-153` (H10) | internal (`m12m13`), ○ journey |
| L12 | Free plan row missing entirely | Fail CLOSED — the bedrock fallback must exist | `identity.go:128-136` | ○ |
| L13 | Operator hot-changes plan limits via SQL | Live within the identity cache TTL | 000007 comment | ★ NEW (PG) |

## Family X — Limits, abuse & attack (what stops the hostile actor)

| # | Attack / abuse | Expected behavior | Enforced at | Coverage |
|---|---|---|---|---|
| X01 | Oversized payload (Free 2 MiB) | 413 before parse | `Limits.MaxRequestBytes` | internal (`proxy_test`) |
| X02 | 33 messages / 17 tools / giant model list | 422 with the field named | `Limits` family | internal |
| X03 | Model not on the plan allowlist | 403 before any upstream call | `admit()` model gate | internal |
| X04 | Stolen web token on device routes | 403 (A21) — scope, not just signature | `RequireDevice` | ★ NEW |
| X05 | Stolen refresh token (attacker+victim race) | Family revocation kills both (A17) | `service.go` | ★ NEW |
| X06 | Cross-tenant order probing | 404 = unknown (no oracle) (P09) | `confirm.go:50` | ★ NEW |
| X07 | Webhook forgery | HMAC over RAW bytes, fail closed (P16) | `webhook.go:45` | ★ NEW |
| X08 | `alg` confusion / none-alg JWT | HS256-only parser | `jwt.go:65,176` | internal |
| X09 | Trailing-garbage JSON body | Uniform loud 400 (audit L3) | `router.go:282-307` | internal (`decodejson_test`) |
| X10 | mock provider reaching production | Requires explicit `ALLOW_MOCK=true` (M13, double guard) | `config.go:574`, `main.go:283` | internal (`config` tests) |

## Family I — Infrastructure (already pinned)

Server lifecycle, outages and scale are NOT re-listed here: see
`tests/README.md` — B01–B09 (boot/restart/crash, incl. the 2026-11-05 boot
data race fix), R01–R09 (runtime), D01–D03 + P01–P05 (accumulation on real
PostgreSQL, 1M rows).

---

## Phase-2 execution plan (what the ★ rows above map to)

| Test file | Series | Drives | Against |
|---|---|---|---|
| `tests/signin_journeys_test.go` | `TestJourneyA*` | real `auth.Service` + real routes | in-memory store fakes + miniredis + a scripted fake Google (its own JWKS/token endpoints) |
| `tests/payment_journeys_test.go` | `TestJourneyP*` | real `payment.Service` end-to-end | `MockProvider` (real HMAC paths) + a fake Store mirroring the exact SQL semantics |
| `tests/plan_lifecycle_pg_test.go` | `TestJourneyL*` | real `repos` SQL (ApplyPaid, Effective, PlanByCode) | real PostgreSQL (gated on `NEXAU_TEST_DATABASE_URL`, like the P-suite) |

Hermetic limits, stated honestly: Google itself, the real payment gateways
and the real OpenRouter are external — we test OUR side of every handshake
against scripted counterparts, which is where all of our logic lives.
