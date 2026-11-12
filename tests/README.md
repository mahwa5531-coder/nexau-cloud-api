# MASh Cloud API — scenario test catalog

`tests/` holds the end-to-end scenario suites: every test drives the REAL
production code paths (real `llm.Proxy` admission with its Lua reservation
scripts, real `metering.Recorder` queue/batch/retry/spill/replay pipeline,
real `ratelimit.Limiter`, real `auth.Service`, real `payment.Service`) against
hermetic fakes (miniredis, a scripted OpenRouter, a scripted fake Google with
its own JWKS, an `llm_calls` table with the exact SQL semantics) — and, for
the PostgreSQL-gated suites, against a real database engine.

Unit tests for the individual packages live next to the code
(`internal/.../*_test.go`); this folder answers questions about BEHAVIOR
ACROSS components, in the shapes operators actually meet.

**`SCENARIOS.md` is the full user-journey catalog** (phase 1 of the
user-journey audit, 2026-11-05): every scenario a real user can perform,
organized by family — sign-in (A), payments (P), tokens & quota (T), plan
lifecycle (L), limits & attack (X) — with per-scenario expected behavior,
enforcement location, and coverage status. The `*_journeys_test.go` /
`plan_lifecycle_pg_test.go` files execute it (phase 2).

## Running

```bash
# Hermetic everything (no external dependencies):
go test ./tests/ -count=1

# One scenario family:
go test ./tests/ -run 'TestScenarioB' -count=1 -v   # boot/crash recovery
go test ./tests/ -run 'TestScenarioR' -count=1 -v   # runtime operation
go test ./tests/ -run 'TestScenarioD' -count=1 -v   # data accumulation (hermetic)
go test ./tests/ -run 'TestScenarioS' -count=1 -v   # the 13 quota scenarios
go test ./tests/ -run 'TestJourneyA'  -count=1 -v   # sign-in journeys (fake Google)
go test ./tests/ -run 'TestJourneyP'  -count=1 -v   # payment journeys

# Real-PostgreSQL scenarios (P and L families) — needs a disposable test DB:
NEXAU_TEST_DATABASE_URL=postgres://user:pass@host:5432/dbname?sslmode=disable \
    go test ./tests/ -run 'TestScenarioP|TestJourneyL' -count=1 -v
# (P01 loads ~1M rows via COPY; first run takes ~30s)

# Race detector (the B05/D02 fan-out paths are concurrency-sensitive):
go test ./tests/ -race -count=1
```

## The catalog

### Quota windows — `quota_scenarios_test.go` (S01–S13)

The rolling-window math itself, per user-visible situation:

| # | Scenario | What it pins |
|---|----------|--------------|
| S01 | Fresh user, first-ever call | Empty table ⇒ zero sums, admission on an empty window |
| S02 | Normal usage | Rows accumulate; the usage bar and enforcement read the same cache |
| S03 | 5-hour burst window fills | `WINDOW_QUOTA_EXCEEDED` with the exact drip-recovery instant |
| S04 | Weekly window fills while 5h is fine | Sustained spend bounded separately from bursts |
| S05 | Parallel fan-out burst | Atomic reservation stops N admissions spending one budget |
| S05b | Quota rejection leaks no slots | The quota branch of `admit()` returns its concurrency slots |
| S06 | Upgrade to Pro while blocked | Ceiling rises, history is never deleted |
| S07 | Downgrade to Free | Existing usage instantly re-evaluated against smaller windows |
| S08 | Window fills mid-stream | Admission-time check only; running streams are never killed |
| S09 | Client disconnects mid-stream | Partial usage still metered (the provider billed it) |
| S10 | Redis down | Quota gate fails OPEN to the PG truth |
| S11 | PostgreSQL down at admission | Gate skipped loudly; availability posture |
| S12 | Model choice sets burn rate | luna 1×/5× vs sonnet 20×/100× normalization weights |
| S13 | Operator changes weights | Picked up within the RuleCache TTL; history stays reconstructible |

### Boot, restart, crash — `boot_recovery_test.go` (B01–B09)

What happens when the gateway is **put on a server**, restarted, or crashed
mid-work. Three state classes with three fates: PG rows survive everything;
Redis keys survive gateway restarts but not Redis restarts; the metering
queue survives only via the fsync'd outbox.

| # | Scenario | What it pins |
|---|----------|--------------|
| B01 | Restart while calls in flight | New process honors old process's Redis reservations — no double-spend window |
| B02 | Hard crash orphans a reservation | Self-expires after one ReserveTTL; crash blast radius is bounded by construction |
| B03 | Crash lost queued rows | Boot replays the outbox, fires AfterPersist, releases the dead process's reservations, removes the file |
| B04 | Replay is exactly-once | Ambiguous commits and already-persisted replays insert zero duplicate rows |
| B05 | Queue overflow under wedged DB | Hot path never blocks, never drops; heal ⇒ flush + replay recover everything once |
| B06 | SIGTERM, healthy DB | Drain flushes the queue before exit; outbox untouched |
| B07 | SIGTERM, DB down | Exit bounded by the grace budget; unflushable records durable in the outbox; next boot replays |
| B08 | Redis restarts empty | Gate re-derives from PG immediately; fail-open cost is bounded and visible |
| B09 | Crash between commit and hook | Row durable + reservation lingering = conservative double-count for ≤ one TTL, then self-heal |

### Runtime operation — `runtime_operation_test.go` (R01–R09)

The temporal behaviors of a **running server**:

| # | Scenario | What it pins |
|---|----------|--------------|
| R01 | Stream exceeds MaxDuration | Server cuts it; metered cancelled/MAX_DURATION with partial usage; in-band error event |
| R02 | Stream stalls | Idle watchdog aborts; metered failed/MODEL_TIMEOUT; honest attribution |
| R03 | Usage cache shields PG | N reads within the TTL cost one llm_calls SUM |
| R04 | ≥95% positions bypass cache | Near the edge every read is fresh from PG |
| R05 | At/over-quota never cached | Recovery detected on the very next admission — no TTL delay |
| R06 | Estimate without max_tokens | Reservation = prompt/4 + 2048 default completion bound, reconciled to actual |
| R07 | Tools widen the estimate | (description + parameters + 128)/4 per tool added to the reservation |
| R08 | Leaked concurrency slots | SlotTTL backstop: a crashed pod's holders expire; the dimension self-heals |
| R09 | RPM window slides | Burst refused with honest retry-after; a minute later it is welcome again |

### Data accumulation — hermetic — `data_accumulation_test.go` (D01–D03)

The window math at months-of-traffic scale (fake implements the SQL
one-for-one with a controllable clock):

| # | Scenario | What it pins |
|---|----------|--------------|
| D01 | 150k-row history | Both window sums exact to the token; recovery instant picks the exact row among 150,000 |
| D02 | Fan-out on a big history | 40 parallel admissions against 100k rows: exactly 12 admitted / 28 rejected |
| D03 | Bursts vs spread binges | Same-second bursts expire as a step; spread calls drip out one by one |

### Data accumulation — real PostgreSQL — `data_accumulation_pg_test.go` (P01–P05)

The same questions against the real engine, real migrations, real indexes
(gated on `NEXAU_TEST_DATABASE_URL`):

| # | Scenario | What it pins |
|---|----------|--------------|
| P01 | One million accumulated rows | Exact sums over 1M rows (200k→50k-row whale tenant + 950k noise); index-driven plan; ~16ms admission read |
| P02 | 400-day retention sweeper | Bounded 5000-row batches to zero; recent rows and live windows untouched |
| P03 | Purged space is reclaimable | Dead tuples → 0 after VACUUM; relation shrinks (32.9 MB → 15.6 MB at 61k rows) |
| P04 | InsertCalls idempotent at scale | Same 2000-call batch twice ⇒ exactly 2000 rows (the at-least-once safety) |
| P05 | Weight change never rewrites history | Stored rows keep their totals; new calls burn at the new rate; mixed-currency windows are exact |

### Auth rotation — real PostgreSQL — `auth_pg_test.go` (P06)

The refresh-token rotation the desktop login depends on, on the real engine
(gated on `NEXAU_TEST_DATABASE_URL`; added by the 2026-11-06 live-boot
round — see Findings below):

| # | Scenario | What it pins |
|---|----------|--------------|
| P06 | Refresh rotation survives real PG | `Issue` → `Rotate` commits (successor inserted, consumed row linked); replay of the consumed token reads as reuse; the family revocation sweep works |

### User journeys — hermetic — `signin_journeys_test.go` / `payment_journeys_test.go`

Phase 2 of the user-journey audit (2026-11-05), executing the ★ NEW rows of
`SCENARIOS.md`. The sign-in family (A01–A24) drives the real `auth.Service`
state machine — single-use consumes, PKCE/nonce verification, family
revocation — against a scripted fake Google that serves its own JWKS and
signs real RS256 ID tokens. The payments family (P01–P23 in the catalog's
numbering) drives the real `payment.Service` — checkout idempotency, the
3-legged confirm verification, webhook dedupe/converge/ordering — against a
MockProvider running the REAL HMAC signature paths:

| Family | File | What it pins |
|---|------|--------------|
| A | `signin_journeys_test.go` | First/returning Google sign-in provisioning; unverified email refusal; state replay + tx-cookie mismatch (CSRF); ID-token nonce binding; single-use grants; desktop pairing issue/race/expiry; deactivation; token classification; refresh rotation + REUSE family revocation; expired-refresh non-purge; logout one/all; web-token blast radius |
| P | `payment_journeys_test.go` | Catalog; checkout happy path + idempotent replay + key reuse + unknown SKU; retry-storm budget; tampered amounts; confirm happy/cross-tenant/webhook-race/forged-signature/order-mismatch; webhook duplicate/out-of-order/forgery/orphan/refund-no-reversal; IP flood budget; abandoned-checkout expiry; late capture on EXPIRED; failed payment then clean retry |

### Plan lifecycle — real PostgreSQL — `plan_lifecycle_pg_test.go` (L01–L13)

What buying time does, what not paying does, and what "weekly" actually is
— pinned against real PostgreSQL with the REAL repos SQL (no store fakes):
real `ApplyPaid` extend-or-insert, real `Effective()` read-side expiry, real
plans seed, and the real identity resolver (gated on
`NEXAU_TEST_DATABASE_URL`). The single most important row is L05: a tenant
that stops paying simply stops matching the `Effective()` query — no
sweeper, no deletion — and the identity resolver hands back the Free plan's
ceilings on the very next call.

## Findings these tests surfaced while being written

- **A real boot data race (2026-11-05, B04 under `-race`)**: the harness's
  `newServerProcess` mirrored `cmd/server/main.go` a little too faithfully —
  both captured the not-yet-assigned `proxy` variable inside the
  `AfterPersist` closure, and the outbox replayer (a goroutine started the
  moment `metering.New` runs) can legitimately fire that hook DURING boot
  when a crashed predecessor left a spill file behind. Production wiring and
  harness were both fixed with `atomic.Pointer` publication;
  `go test ./tests/ -race -count=3` is the regression proof. The lesson the
  scenario catalog exists to enforce: the replay-at-boot path is a normal
  runtime path, not an error path, and it executes before "the rest of main"
  has finished.
- **Refresh rotation 500'd on every real PostgreSQL** (2026-11-06, fixed by
  the live-boot round): `RefreshTokensRepo.Rotate` wrote `replaced_by` BEFORE
  inserting the successor row — an immediate-FK violation (SQLSTATE 23503)
  that every hermetic fake was blind to; no PG-gated test had touched
  rotation. P06 now pins the fix on the real engine.
- **P01's plan threshold**: at ~20% tenant share of the table, PostgreSQL
  deliberately flips the window query to a parallel sequential scan (still
  ~130 ms at 1M rows) because index fetches for a fifth of the heap cost
  more. The index wins exactly where production lives — many tenants, each
  a small slice (5% whale ⇒ Bitmap Index Scan, ~16 ms).
- **miniredis TTLs run on its virtual clock**: `FastForward`, not wall-clock
  sleep, expires keys (R03 harness).
- **`time.Duration × time.Duration` overflows int64** at month-scale age
  ladders (ns×ns); P01's first version seeded rows 8 hours into the future
  until the ladder moved to plain-second arithmetic.

## Related suites

- `internal/metering/*_test.go` — recorder unit tests: batching, retry,
  outbox spill/replay, panic containment, close-drain.
- `internal/ratelimit/*_test.go` — sliding-window and slot unit tests,
  including the pinned-member and retry-after-direction audit regressions.
- `internal/store/repos/*_test.go` — PG-gated SQL truth: window sums,
  recovery instant, payment exactly-once, webhook dedupe.
- `tests/llm_e2e_test.go` — request-path e2e: SSE framing, auth, shutdown
  grace (M9), verbatim passthrough (M10), the original H2 reservation test.
- `tests/rateinfo_test.go` — X-Ratelimit-* header semantics.
