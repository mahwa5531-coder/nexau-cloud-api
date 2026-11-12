// Package tests — boot_recovery_test.go
//
// THE SERVER-LIFECYCLE SCENARIOS: what happens when the gateway is PUT on a
// server, when it is restarted, and when it crashes mid-work.
//
// The 13 quota scenarios (quota_scenarios_test.go) answer "is the window math
// right for one process". These tests answer the next question an operator
// asks: the process boundary. In production there are THREE kinds of state,
// with three different fates across a restart:
//
//   - PostgreSQL rows    — survive everything (that is the point of PG).
//   - Redis keys         — survive a gateway restart (Redis is a separate
//     container), but NOT a Redis restart (no volume in
//     the default compose: reservations, slots, RPM
//     windows and the usage cache all vanish).
//   - The metering queue — in-process memory; a crash loses whatever had not
//     been flushed, which is exactly what the JSONL
//     outbox exists to recover.
//
// Every test here drives the REAL production pipeline: the real
// metering.Recorder (bounded queue → batching → retry → spill → replay), the
// real proxy admission/reservation Lua scripts against real miniredis, and a
// fake llm_calls table that implements the exact SQL semantics of
// repos.LLMCallsRepo.InsertCalls (ON CONFLICT (call_id) DO NOTHING dedupe).
//
// Scenario index (B = boot/boot-order/crash):
//
//      B01  gateway restarts while calls are in flight   (reservations live in
//           Redis, not in the process: the NEW process immediately honors the
//           OLD process's in-flight reservations — no double-spend window across
//           a restart, no coordination needed)
//      B02  hard crash leaves a reservation orphaned     (no record, no release:
//           the reservation self-expires one ReserveTTL later — capacity
//           self-heals with no operator action; worst-case phantom over-count
//           is bounded by exactly one TTL)
//      B03  crash lost queued rows → boot replays        (records that never
//           reached PG are in the fsync'd outbox; a NEW process replays them at
//           startup, fires AfterPersist(durable=true), releases the dead
//           process's reservations, and deletes the outbox file)
//      B04  replay is exactly-once                        (a record that WAS
//           persisted but re-appears in the outbox — ambiguous commit, crashed
//           replay, un-removable file — inserts zero duplicate rows: the
//           call_id unique constraint makes replay idempotent)
//      B05  queue overflow under a wedged database        (the hot path never
//           blocks and never drops: overflow spills synchronously to the outbox;
//           when the writer heals, flush + replay recover every record once)
//      B06  SIGTERM with a healthy database               (the shutdown drain
//           flushes the queue before exit — the last completed calls' rows land
//           durably and the outbox is never needed)
//      B07  SIGTERM with a DOWN database                  (drain is bounded by
//           the operator's grace window: unflushable records spill to the outbox
//           and the next boot replays them — nothing is lost, exit is prompt)
//      B08  Redis restarts empty (no volume)              (reservations, slots
//           and the usage cache evaporate: the gate re-derives from PG truth
//           immediately; in-flight calls' un-persisted consumption is briefly
//           invisible — the bounded fail-open cost — and the durable history
//           still binds)
//      B09  crash between row-commit and hook             (row is in PG but
//           QuotaPersisted never ran: the reservation double-counts the row
//           until its TTL — conservative, never permanent, self-heals)
//
// ReserveTTL is scaled down to tens of milliseconds so "one TTL later" is a
// real sleep, not a 20-minute one; the mechanism is identical.
package tests

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "os"
        "path/filepath"
        "sync"
        "sync/atomic"
        "testing"
        "time"

        "github.com/alicebob/miniredis/v2"
        "github.com/redis/go-redis/v9"

        "github.com/mash-cloud/mash-api/internal/auth"
        "github.com/mash-cloud/mash-api/internal/domain"
        "github.com/mash-cloud/mash-api/internal/llm"
        "github.com/mash-cloud/mash-api/internal/metering"
        "github.com/mash-cloud/mash-api/internal/observability"
        "github.com/mash-cloud/mash-api/internal/openrouter"
        "github.com/mash-cloud/mash-api/internal/ratelimit"
)

// ---- the llm_calls table, with SQL semantics -------------------------------------
//
// llmCallsTable fakes repos.LLMCallsRepo for the metering pipeline: window
// math is inherited from windowedUsage (the exact production SQL semantics),
// and InsertCalls adds the exactly-once contract of the real SQL
// (ON CONFLICT (call_id) DO NOTHING) plus the failure modes a server actually
// meets: down, ambiguous commit, and wedged.
type insertMode int32

const (
        modeHealthy       insertMode = iota // every attempt succeeds
        modeDown                            // every attempt errors (PG down)
        modeAmbiguousOnce                   // rows ARE written, then an error is returned (commit lost)
        modeHang                            // block until released or the attempt budget elapses
)

type llmCallsTable struct {
        *windowedUsage

        mu       sync.Mutex
        byID     map[string]bool // call_id → exists (the UNIQUE constraint)
        mode     insertMode
        attempts int64
        hangGate chan struct{}
}

func newLLMCallsTable() *llmCallsTable {
        return &llmCallsTable{
                windowedUsage: newWindowedUsage(),
                byID:          map[string]bool{},
                hangGate:      make(chan struct{}),
        }
}

func (t *llmCallsTable) setMode(m insertMode) {
        t.mu.Lock()
        t.mode = m
        t.mu.Unlock()
}

func (t *llmCallsTable) uniqueRows() int64 {
        t.mu.Lock()
        defer t.mu.Unlock()
        return int64(len(t.byID))
}

func (t *llmCallsTable) attemptCount() int64 {
        t.mu.Lock()
        defer t.mu.Unlock()
        return t.attempts
}

// InsertCalls mirrors repos.LLMCallsRepo.InsertCalls: a batch write whose
// rows dedupe on call_id, classified failures, and per-attempt budgets.
func (t *llmCallsTable) InsertCalls(ctx context.Context, calls []domain.LLMCall) error {
        if len(calls) == 0 {
                return nil
        }
        t.mu.Lock()
        mode := t.mode
        t.attempts++
        t.mu.Unlock()

        store := func() {
                t.mu.Lock()
                defer t.mu.Unlock()
                for i := range calls {
                        if t.byID[calls[i].CallID] {
                                continue // ON CONFLICT (call_id) DO NOTHING
                        }
                        t.byID[calls[i].CallID] = true
                        if calls[i].Usage.TotalTokens > 0 {
                                t.addRowAtLocked(calls[i].TenantID, time.Now().UTC(), calls[i].Usage.TotalTokens)
                        }
                }
        }

        switch mode {
        case modeDown:
                return errors.New("pg: connection refused")
        case modeAmbiguousOnce:
                store()
                t.setMode(modeHealthy) // the retry (or replay) succeeds
                return errors.New("pg: commit succeeded but the response was lost")
        case modeHang:
                select {
                case <-t.hangGate:
                        store()
                        return nil
                case <-ctx.Done():
                        return ctx.Err()
                }
        default:
                store()
                return nil
        }
}

// addRowAtLocked is addRowAt without re-taking llmCallsTable.mu (callers
// already hold it; windowedUsage has its own lock, so nesting is safe).
func (t *llmCallsTable) addRowAtLocked(tid string, at time.Time, tokens int64) {
        t.windowedUsage.addRowAt(tid, at, tokens)
}

// seedRow inserts a row by hand (scenario setup: history that predates the
// server process under test).
func (t *llmCallsTable) seedRow(callID, tid string, at time.Time, tokens int64) {
        t.mu.Lock()
        defer t.mu.Unlock()
        t.byID[callID] = true
        t.addRowAtLocked(tid, at, tokens)
}

// ---- boot-scenario environment ----------------------------------------------------
//
// bootEnv wires the REAL production pieces: real metering.Recorder with a
// SpillPath (the durable outbox), real proxy reservation scripts, real
// miniredis — and lets the test kill/heal/restart each piece like an operator
// would. "Restarting the process" is modeled by building a second
// proxy+Recorder pair against the SAME Redis and the SAME table, which is
// precisely what a new pod does: everything that matters lives outside it.
type bootEnv struct {
        t      *testing.T
        mr     *miniredis.Miniredis
        rdb    *redis.Client
        fb     *fakeOpenRouter
        tbl    *llmCallsTable
        outbox string
        flush  time.Duration // recorder flush cadence (tests tune it)

        persisted chan domain.LLMCall // AfterPersist(durable=true) observations
        dropped   chan domain.LLMCall // AfterPersist(durable=false) observations
}

func newBootEnv(t *testing.T) *bootEnv {
        t.Helper()
        mr := miniredis.RunT(t)
        rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
        t.Cleanup(func() { _ = rdb.Close() })
        fb := newFakeOpenRouter(t)
        return &bootEnv{
                t:         t,
                mr:        mr,
                rdb:       rdb,
                fb:        fb,
                tbl:       newLLMCallsTable(),
                outbox:    filepath.Join(t.TempDir(), "outbox.jsonl"),
                flush:     20 * time.Millisecond,
                persisted: make(chan domain.LLMCall, 1024),
                dropped:   make(chan domain.LLMCall, 1024),
        }
}

// newServerProcess builds one "server process": a proxy wired to a fresh
// metering.Recorder sharing the deployment's Redis and llm_calls table —
// the wiring of cmd/server/main.go, including the hook closure that releases
// a reservation once its row is durable. wireHook=false severs the
// AfterPersist→QuotaPersisted link, which is the observable state of a
// process that died between the row commit and the hook.
func (be *bootEnv) newServerProcess(shortTTL time.Duration, wireHook bool) (*llm.Proxy, *metering.Recorder) {
        be.t.Helper()
        metrics := observability.NewMetrics()
        limiter := ratelimit.New(be.rdb, true, metrics)
        cfg := metering.Config{
                QueueSize:      64, // production floor
                BatchSize:      8,
                FlushInterval:  be.flush,
                RetryMax:       1, // attempt + 1 retry, then spill
                SpillPath:      be.outbox,
                ReplayInterval: 40 * time.Millisecond,
        }
        // PUBLISH PATTERN (same as cmd/server/main.go): the metering
        // batcher goroutine started by metering.New below can replay the
        // outbox and fire AfterPersist BEFORE this function assigns the
        // proxy — a plain closure capture of `var proxy` is a data race
        // (caught live by -race on 2026-11-06). Publish through an
        // atomic pointer instead.
        var proxyRef atomic.Pointer[llm.Proxy]
        if wireHook {
                persisted, dropped := be.persisted, be.dropped
                cfg.AfterPersist = func(rec domain.LLMCall, durable bool) {
                        if durable {
                                persisted <- rec
                        } else {
                                dropped <- rec
                        }
                        if p := proxyRef.Load(); p != nil {
                                p.QuotaPersisted(rec.TenantID, rec.CallID, durable)
                        }
                }
        }
        meter := metering.New(be.tbl, cfg, metrics)

        orClient := openrouter.NewClient(openrouter.TransportConfig{BaseURL: be.fb.srv.URL, APIKey: "sk-or-v1-test"}, metrics)
        proxy := &llm.Proxy{
                Upstream: orClient, Meter: meter, Limiter: limiter, Usage: be.tbl, Metrics: metrics,
                Redis: be.rdb, UsageCacheTTL: 15 * time.Millisecond,
                Limits:     llm.Limits{MaxMessages: 32, MaxTools: 16, MaxModelLen: 128},
                Rate:       llm.RateRules{RPMUser: 600, RPMTenant: 6000, ConcUser: 16, ConcTenant: 256},
                Timing:     llm.Timing{IdleTimeout: 2 * time.Second, MaxDuration: 5 * time.Second, NonStreamTimeout: 5 * time.Second},
                SlotTTL:    time.Minute,
                ReserveTTL: shortTTL, // scaled-down 20-minute default
        }
        proxyRef.Store(proxy)
        return proxy, meter
}

// bootStream runs one streaming call to completion on p (synchronous), with
// the default identity-normalization rule (1:1 weights: total = prompt +
// completion).
func bootStream(be *bootEnv, t *testing.T, p *llm.Proxy, idn *auth.Identity, maxOut, prompt, completion int64) {
        t.Helper()
        be.fb.setScript(
                contentChunk("openai/gpt-6-luna", "hi"),
                usageChunk("openai/gpt-6-luna", prompt, completion),
        )
        if _, de := p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", maxOut), &captureSink{}); de != nil {
                t.Fatalf("stream failed: %v", de)
        }
}

// outboxPending reports whether the durable outbox currently holds records
// ANYWHERE in its file family — the live path, or a claim file left by a
// replayer that is mid-cycle (the atomic rename IS the claim; a down DB
// leaves the claimed file for the next sweep, which is exactly the
// crashed-replay recovery path the production code documents).
func (be *bootEnv) outboxPending() bool {
        matches, _ := filepath.Glob(be.outbox + "*")
        return len(matches) > 0
}

// outboxAppend writes records straight into the outbox file — the state a
// crashed process leaves behind (or a replay that never got removed).
func (be *bootEnv) outboxAppend(calls ...domain.LLMCall) {
        be.t.Helper()
        f, err := os.OpenFile(be.outbox, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
        if err != nil {
                be.t.Fatalf("outbox open: %v", err)
        }
        defer f.Close()
        for i := range calls {
                b, err := json.Marshal(calls[i])
                if err != nil {
                        be.t.Fatalf("outbox marshal: %v", err)
                }
                if _, err := f.Write(append(b, '\n')); err != nil {
                        be.t.Fatalf("outbox write: %v", err)
                }
        }
        if err := f.Sync(); err != nil {
                be.t.Fatalf("outbox fsync: %v", err)
        }
}

// ledger reports (holders, token sum) of the tenant's live reservations,
// reading Redis directly (assertions only).
func (be *bootEnv) ledger(tid string) (int64, int64) {
        ctx := context.Background()
        n, _ := be.rdb.HLen(ctx, "usage:resv:amt:"+tid).Result()
        vals, _ := be.rdb.HVals(ctx, "usage:resv:amt:"+tid).Result()
        var sum int64
        for _, v := range vals {
                var x int64
                if _, err := fmt.Sscanf(v, "%d", &x); err == nil {
                        sum += x
                }
        }
        return n, sum
}

// mkCall builds a completed-call record exactly like proxy.record does.
func mkCall(tid, uid, callID string, total int64) domain.LLMCall {
        now := time.Now().UTC()
        return domain.LLMCall{
                CallID: callID, TenantID: tid, UserID: uid, RequestID: "req_test",
                RequestedModel: "openai/gpt-6-luna", ResolvedModel: "openai/gpt-6-luna",
                Status:    domain.CallCompleted,
                Usage:     domain.TokenUsage{InputTokens: total / 2, OutputTokens: total - total/2, TotalTokens: total},
                StartedAt: now, CompletedAt: &now,
        }
}

// ---- B01: restart while calls are in flight ---------------------------------------

// TestScenarioB01_RestartHonorsLiveReservations — the gateway pod restarts
// (deploy, OOM, node recycle) while user calls are mid-flight. The old
// process's token reservations live in REDIS, so the moment the new process
// serves its first admission it counts them: there is no window of time
// after a restart where the same budget can be double-spent, and no
// coordination protocol between processes is needed — the shared ledger IS
// the coordination.
func TestScenarioB01_RestartHonorsLiveReservations(t *testing.T) {
        be := newBootEnv(t)
        old, m1 := be.newServerProcess(time.Minute, true)
        defer m1.Close(500 * time.Millisecond)

        idn := freeIdentity()
        idn.Limits.Window5hTokens = 3000 // room for one reservation, not two

        // An in-flight call on the OLD process: its usage chunk arrives late,
        // so the call stays admitted-and-running across the "restart".
        be.fb.setScript(
                scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"..."}}]}`, delay: 400 * time.Millisecond},
                usageChunk("openai/gpt-6-luna", 100, 900),
        )
        go func() {
                _, _ = old.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 2500), &captureSink{})
        }()

        waitFor(t, "old process's reservation", 2*time.Second, func() bool {
                n, sum := be.ledger(idn.Tenant.ID)
                return n == 1 && sum == 2501 // 1 prompt token + 2500 completion bound
        })

        // The restart: a NEW server process against the same Redis and table.
        restarted, m2 := be.newServerProcess(time.Minute, true)
        defer m2.Close(500 * time.Millisecond)

        // Same tenant, a second call on the NEW process: the old process's
        // reservation must bind it — 2501 (old, live) + 1000 (this estimate)
        // exceeds the 3000 budget.
        _, de := restarted.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 999), &captureSink{})
        if de == nil || de.Code != "WINDOW_QUOTA_EXCEEDED" {
                t.Fatalf("new process must honor the old process's in-flight reservation: %v", de)
        }

        // The old call finishes; its row lands and releases, after which the
        // same request size fits (used=1000, reservation gone).
        waitFor(t, "old call's row to land", 3*time.Second, func() bool {
                return be.tbl.used5h(idn.Tenant.ID) == 1000
        })
        waitFor(t, "reservation released", 2*time.Second, func() bool {
                n, _ := be.ledger(idn.Tenant.ID)
                return n == 0
        })
        if _, de := restarted.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 1500), &captureSink{}); de != nil {
                t.Fatalf("after the old call settles, the budget re-opens: %v", de)
        }
}

// ---- B02: hard crash orphans a reservation ----------------------------------------

// TestScenarioB02_CrashedReservationSelfExpires — the pod dies hard
// (SIGKILL, kernel panic) between admission and metering: nobody will ever
// call adjust or release for that call. The reservation must not wedge the
// tenant forever: it self-expires one ReserveTTL after it was claimed, and
// the next admission's Lua run prunes it. The blast radius of a crash is
// therefore bounded by construction — at most ReserveTTL (20 minutes in
// production) of conservative over-counting, never a permanent lockout.
func TestScenarioB02_CrashedReservationSelfExpires(t *testing.T) {
        be := newBootEnv(t)
        const ttl = 80 * time.Millisecond
        crashed, m1 := be.newServerProcess(ttl, true)
        defer m1.Close(300 * time.Millisecond)

        idn := freeIdentity()
        idn.Limits.Window5hTokens = 3000

        // The call that never completes: its chunks hang long past the test's
        // assertions; the "crash" means no record() ever runs for it.
        be.fb.setScript(
                scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"..."}}]}`, delay: 5 * time.Second},
                usageChunk("openai/gpt-6-luna", 100, 900),
        )
        go func() {
                _, _ = crashed.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 2500), &captureSink{})
        }()

        waitFor(t, "crashed call's reservation", 2*time.Second, func() bool {
                _, sum := be.ledger(idn.Tenant.ID)
                return sum == 2501
        })

        // Immediately after the crash the budget is (correctly) conservative:
        // the phantom reservation still binds a second caller.
        probe, m2 := be.newServerProcess(ttl, true)
        defer m2.Close(300 * time.Millisecond)
        if _, de := probe.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 999), &captureSink{}); de == nil {
                t.Fatal("before ReserveTTL the orphaned reservation must still bind")
        }

        // One ReserveTTL later: pruned by the next admission's Lua run, budget
        // exactly back to the PG truth (which is still zero rows).
        time.Sleep(ttl + 60*time.Millisecond)
        be.fb.setScript( // a fast script: the probe's stream must complete
                contentChunk("openai/gpt-6-luna", "hi"),
                usageChunk("openai/gpt-6-luna", 100, 900),
        )
        be.rdb.Del(context.Background(), "usage:window:"+idn.Tenant.ID)
        if _, de := probe.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 2998), &captureSink{}); de != nil {
                t.Fatalf("after ReserveTTL the orphaned reservation must be pruned: %v", de)
        }
        // The phantom 2501-token reservation is gone; whatever remains is the
        // probe's own call reconciling (its row lands and releases moments
        // later), never the crashed call's.
        n, sum := be.ledger(idn.Tenant.ID)
        if n > 1 || sum == 2501 {
                t.Fatalf("expired reservation must be pruned: ledger (%d, %d)", n, sum)
        }
}

// ---- B03: boot replays the outbox of a crashed process -----------------------------

// TestScenarioB03_BootReplaysCrashedOutbox — the process died with rows
// still queued (or failed and spilled): the JSONL outbox is fsync'd, so the
// records are durable on local disk. A NEW process starts against the same
// volume, promptly replays the outbox at batcher startup, lands the rows in
// PG, fires AfterPersist(durable=true) — which releases the dead process's
// still-held reservations — and removes the file. The quota window converges
// to the truth with no operator action.
func TestScenarioB03_BootReplaysCrashedOutbox(t *testing.T) {
        be := newBootEnv(t)

        idn := freeIdentity()
        idn.Limits.Window5hTokens = 10_000

        // Process #1 with a dead database: completed calls spill to the outbox
        // while their reservations linger at the adjusted actual usage.
        p1, m1 := be.newServerProcess(time.Minute, true)
        be.tbl.setMode(modeDown)
        bootStream(be, t, p1, idn, 500, 100, 900) // → reservation adjusted to 1000
        bootStream(be, t, p1, idn, 500, 200, 800) // → reservation adjusted to 1000
        waitFor(t, "records to spill to the durable outbox", 3*time.Second, func() bool {
                return be.outboxPending()
        })
        waitFor(t, "both dead calls' reservations lingering", 3*time.Second, func() bool {
                n, sum := be.ledger(idn.Tenant.ID)
                return n == 2 && sum == 2000
        })
        if got := be.tbl.uniqueRows(); got != 0 {
                t.Fatalf("nothing can reach PG while it is down, rows = %d", got)
        }
        m1.Close(200 * time.Millisecond) // the process dies

        // A new pod boots: same table (now healed), same outbox file, and the
        // production hook wiring — replay's AfterPersist releases the dead
        // process's reservations through the NEW process.
        be.tbl.setMode(modeHealthy)
        _, m2 := be.newServerProcess(time.Minute, true)
        defer m2.Close(500 * time.Millisecond)

        waitFor(t, "boot replay to land the crashed records", 3*time.Second, func() bool {
                return be.tbl.uniqueRows() == 2
        })
        if used := be.tbl.used5h(idn.Tenant.ID); used != 2000 {
                t.Fatalf("replayed usage = %d, want 2000", used)
        }
        waitFor(t, "replay to release the dead process's reservations", 2*time.Second, func() bool {
                n, _ := be.ledger(idn.Tenant.ID)
                return n == 0
        })
        if be.outboxPending() {
                t.Fatal("outbox family must be empty after a successful replay")
        }
}

// ---- B04: replay is exactly-once ----------------------------------------------------

// TestScenarioB04_ReplayIsExactlyOnce — two ways the same record can be
// presented to the writer more than once, and neither may double-count:
//
//  1. ambiguous commit: the batch WAS committed but the connection died
//     before the acknowledgment — the metering retry re-sends the identical
//     batch;
//  2. replay of an already-persisted record: a crash after InsertCalls but
//     before the outbox file was removed leaves the record on disk.
//
// Both are absorbed by the same SQL contract the fake mirrors: ON CONFLICT
// (call_id) DO NOTHING on the llm_calls unique constraint. One call, one
// row, one window contribution — always.
func TestScenarioB04_ReplayIsExactlyOnce(t *testing.T) {
        be := newBootEnv(t)
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 10_000

        // Phase 1: ambiguous commit. The writer stores the rows AND errors; the
        // recorder's retry (same batch) must not duplicate them.
        be.tbl.setMode(modeAmbiguousOnce)
        p1, m1 := be.newServerProcess(time.Minute, true)
        bootStream(be, t, p1, idn, 500, 100, 900) // total 1000
        waitFor(t, "ambiguous batch to be re-presented and absorbed", 3*time.Second, func() bool {
                return be.tbl.attemptCount() >= 2 && be.tbl.uniqueRows() == 1
        })
        if used := be.tbl.used5h(idn.Tenant.ID); used != 1000 {
                t.Fatalf("ambiguous commit double-counted: window = %d, want 1000", used)
        }
        m1.Close(300 * time.Millisecond)

        // Phase 2: a record that is ALREADY in the table reappears in the
        // outbox (crash between persist and file removal). A fresh process
        // replays it: the unique constraint absorbs it.
        be.tbl.seedRow("llm_already_persisted", idn.Tenant.ID, time.Now().UTC(), 1000)
        be.outboxAppend(domain.LLMCall{
                CallID: "llm_already_persisted", TenantID: idn.Tenant.ID, UserID: idn.User.ID,
                RequestedModel: "openai/gpt-6-luna", ResolvedModel: "openai/gpt-6-luna", Status: domain.CallCompleted,
                Usage:     domain.TokenUsage{InputTokens: 100, OutputTokens: 900, TotalTokens: 1000},
                StartedAt: time.Now().UTC(),
        })

        _, m2 := be.newServerProcess(time.Minute, true)
        defer m2.Close(500 * time.Millisecond)

        waitFor(t, "replay of the duplicate to complete", 3*time.Second, func() bool {
                return !be.outboxPending()
        })
        if got := be.tbl.uniqueRows(); got != 2 {
                t.Fatalf("replayed duplicate must not add a row: unique = %d, want 2", got)
        }
        if used := be.tbl.used5h(idn.Tenant.ID); used != 2000 {
                t.Fatalf("replayed duplicate double-counted: window = %d, want 2000", used)
        }
}

// ---- B05: queue overflow under a wedged database ------------------------------------

// TestScenarioB05_QueueOverflowSpillsNeverDropsNorBlocks — PostgreSQL is
// wedged (network black hole, not a fast refusal) and a burst of completions
// exceeds the queue's capacity. Two properties must hold on the HOT PATH:
// Record never blocks (the streaming loop is never hostage to the database),
// and nothing is dropped (overflow spills synchronously to the fsync'd
// outbox). When the database heals, the normal flush plus the replay sweep
// recover every record exactly once.
func TestScenarioB05_QueueOverflowSpillsNeverDropsNorBlocks(t *testing.T) {
        be := newBootEnv(t)
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 1_000_000

        _, meter := be.newServerProcess(time.Minute, true)
        be.tbl.setMode(modeHang)

        // One record first: the batcher picks it up and wedges inside
        // InsertCalls, so the in-memory queue is all that is left to receive
        // the burst.
        meter.Record(mkCall(idn.Tenant.ID, idn.User.ID, "llm_b05_first", 100))
        time.Sleep(2 * be.flush) // let the tick hand it to the wedged writer

        // Burst past the queue capacity (64): the overflow must spill, fast.
        const extra = 80
        start := time.Now()
        for i := 0; i < extra; i++ {
                meter.Record(mkCall(idn.Tenant.ID, idn.User.ID, fmt.Sprintf("llm_b05_%03d", i), 10))
        }
        if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
                t.Fatalf("Record blocked on a full queue: %v for %d records", elapsed, extra)
        }

        // The database heals: release the wedged write.
        close(be.tbl.hangGate)

        waitFor(t, "flush + replay to recover every record", 5*time.Second, func() bool {
                return be.tbl.uniqueRows() == 1+int64(extra)
        })
        if used := be.tbl.used5h(idn.Tenant.ID); used != 100+10*int64(extra) {
                t.Fatalf("recovered usage = %d, want %d", used, 100+10*int64(extra))
        }
        if be.outboxPending() {
                t.Fatal("outbox must be consumed after recovery")
        }
}

// ---- B06: SIGTERM with a healthy database -------------------------------------------

// TestScenarioB06_ShutdownDrainFlushesTheQueue — the operator stops the
// server while calls that finished a moment ago are still sitting in the
// metering queue. The shutdown sequence's drain step (meter.Close with the
// remaining grace) must flush them durably before the process exits: the
// last completed calls' rows land and the outbox is never even touched.
func TestScenarioB06_ShutdownDrainFlushesTheQueue(t *testing.T) {
        be := newBootEnv(t)
        be.flush = 10 * time.Second // no tick: the drain is the only writer
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 1_000_000

        _, meter := be.newServerProcess(time.Minute, true)

        for i := 0; i < 6; i++ {
                meter.Record(mkCall(idn.Tenant.ID, idn.User.ID, fmt.Sprintf("llm_b06_%d", i), 100))
        }
        time.Sleep(50 * time.Millisecond)
        if got := be.tbl.uniqueRows(); got != 0 {
                t.Fatalf("precondition: records must still be queued, rows = %d", got)
        }

        meter.Close(2 * time.Second) // the drain

        if got := be.tbl.uniqueRows(); got != 6 {
                t.Fatalf("drain must flush every queued record: rows = %d, want 6", got)
        }
        if used := be.tbl.used5h(idn.Tenant.ID); used != 600 {
                t.Fatalf("drained usage = %d, want 600", used)
        }
        if be.outboxPending() {
                t.Fatal("healthy database: the outbox must not be needed at shutdown")
        }
}

// ---- B07: SIGTERM with a DOWN database ----------------------------------------------

// TestScenarioB07_ShutdownWedgedDatabaseIsBoundedAndDurable — the server is
// stopping while PostgreSQL is DOWN. Two guarantees: the process still exits
// within its drain budget (a dead database can never hold shutdown hostage),
// and the unflushable records spill to the outbox so the NEXT boot replays
// them. Nothing is lost; nothing hangs.
func TestScenarioB07_ShutdownWedgedDatabaseIsBoundedAndDurable(t *testing.T) {
        be := newBootEnv(t)
        be.flush = 10 * time.Second // records wait in the queue, not on a tick
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 1_000_000

        _, meter := be.newServerProcess(time.Minute, true)
        be.tbl.setMode(modeDown)

        for i := 0; i < 3; i++ {
                meter.Record(mkCall(idn.Tenant.ID, idn.User.ID, fmt.Sprintf("llm_b07_%d", i), 100))
        }

        start := time.Now()
        meter.Close(400 * time.Millisecond) // a tight grace budget
        if elapsed := time.Since(start); elapsed > 2*time.Second {
                t.Fatalf("shutdown must be bounded by its budget, took %v", elapsed)
        }
        if got := be.tbl.uniqueRows(); got != 0 {
                t.Fatalf("database is down: nothing can land, rows = %d", got)
        }
        if !be.outboxPending() {
                t.Fatal("unflushable records must be durable in the outbox")
        }

        // The next boot (B03's story, chained): heal + replay.
        be.tbl.setMode(modeHealthy)
        _, m2 := be.newServerProcess(time.Minute, true)
        defer m2.Close(500 * time.Millisecond)
        waitFor(t, "next boot to replay the shutdown's outbox", 3*time.Second, func() bool {
                return be.tbl.uniqueRows() == 3
        })
        if used := be.tbl.used5h(idn.Tenant.ID); used != 300 {
                t.Fatalf("replayed usage = %d, want 300", used)
        }
}

// ---- B08: Redis restarts empty ---------------------------------------------------------

// TestScenarioB08_RedisRestartGrandAmnesty — the default deployment runs
// Redis WITHOUT a volume: a Redis restart is a cold start, and every bit of
// fast state evaporates — reservations, concurrency slots, RPM windows, the
// usage cache. The system must converge, not collapse: the quota gate
// re-derives from PostgreSQL truth on the very next admission, and the
// durable history still binds. The transient cost is real and worth stating
// out loud: in-flight calls whose rows have not landed yet are briefly
// invisible (their reservations died with Redis), so a short over-admission
// window exists until they persist. That is the designed fail-open posture.
func TestScenarioB08_RedisRestartGrandAmnesty(t *testing.T) {
        be := newBootEnv(t)
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 6000

        // Durable history: 5000 tokens already in PG.
        be.tbl.seedRow("llm_b08_hist", idn.Tenant.ID, be.tbl.clock().Add(-10*time.Minute), 5000)

        p, meter := be.newServerProcess(time.Minute, true)
        defer meter.Close(500 * time.Millisecond)

        // A normal admission to warm the usage cache and hold a reservation.
        be.fb.setScript(
                scriptChunk{data: `{"id":"c1","object":"chat.completion.chunk","model":"openai/gpt-6-luna","choices":[{"index":0,"delta":{"content":"..."}}]}`, delay: time.Second},
                usageChunk("openai/gpt-6-luna", 50, 450),
        )
        go func() {
                _, _ = p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 500), &captureSink{})
        }()
        waitFor(t, "reservation held + cache warmed", 2*time.Second, func() bool {
                n, _ := be.ledger(idn.Tenant.ID)
                return n == 1
        })

        // Redis restarts empty (no volume, no AOF in the default compose).
        be.mr.FlushAll()

        // The next admission: cache miss → PG truth (5000) — and the dead
        // reservation is GONE, so this call is admitted against the durable
        // history alone: 5000 + 1000 ≤ 6000 fits. Had the reservation survived,
        // it would not.
        if _, de := p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 999), &captureSink{}); de != nil {
                t.Fatalf("post-restart admission must run on PG truth: %v", de)
        }

        // And the durable history still binds: once the in-flight rows land the
        // window is at quota, so a fresh 1000-token request is rejected.
        waitFor(t, "in-flight rows to land", 3*time.Second, func() bool {
                return be.tbl.used5h(idn.Tenant.ID) >= 6000
        })
        be.rdb.Del(context.Background(), "usage:window:"+idn.Tenant.ID)
        if _, de := p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 1000), &captureSink{}); de == nil || de.Code != "WINDOW_QUOTA_EXCEEDED" {
                t.Fatalf("durable history must still bind after the Redis restart: %v", de)
        }

        // The position is re-derived from PG, not from the lost snapshot: a
        // direct read (the same call /v1/me makes) returns the durable truth.
        // Note the cache key itself stays ABSENT by design — this position is
        // at quota, and at/over-quota positions are never cached (R05).
        be.rdb.Del(context.Background(), "usage:window:"+idn.Tenant.ID)
        pos, err := p.QuotaPosition(context.Background(), idn.Tenant.ID, 6000, 100_000)
        if err != nil {
                t.Fatalf("quota position: %v", err)
        }
        if pos == nil || pos.Used5h != 6000 {
                t.Fatalf("post-restart position must come from PG truth: %+v", pos)
        }
}

// ---- B09: crash between row-commit and the hook ----------------------------------------

// TestScenarioB09_CrashBetweenPersistAndHook — the narrowest crash window:
// InsertCalls committed, but the process died before QuotaPersisted ran.
// The row is durable (PG has it), yet the reservation is still held (Redis
// has it) — the same consumption is counted twice until the reservation's
// TTL elapses. This is CONSERVATIVE by design (never under-counts), bounded
// by ReserveTTL, and self-heals with no operator action. The test pins all
// three properties.
func TestScenarioB09_CrashBetweenPersistAndHook(t *testing.T) {
        be := newBootEnv(t)
        const ttl = 500 * time.Millisecond // generous vs the flush cadence
        idn := freeIdentity()
        idn.Limits.Window5hTokens = 2500

        // A server whose AfterPersist hook is deliberately severed — exactly
        // the observable state of "died right after the row landed".
        p, meter := be.newServerProcess(ttl, false)
        defer meter.Close(300 * time.Millisecond)

        bootStream(be, t, p, idn, 1000, 100, 900) // usage total 1000, row lands

        waitFor(t, "row durable + reservation still held", 3*time.Second, func() bool {
                return be.tbl.used5h(idn.Tenant.ID) == 1000
        })
        n, sum := be.ledger(idn.Tenant.ID)
        if n != 1 || sum != 1000 {
                t.Fatalf("severed hook: reservation must linger at the actual usage, got (%d, %d)", n, sum)
        }

        // Conservative double-count: used(1000) + reservation(1000) + est(600)
        // > 2500 → rejected. A crash may tighten budgets; it may never loosen
        // them. (Cache dropped first so the read is the fresh PG truth.)
        be.rdb.Del(context.Background(), "usage:window:"+idn.Tenant.ID)
        be.fb.setScript(
                contentChunk("openai/gpt-6-luna", "hi"),
                usageChunk("openai/gpt-6-luna", 10, 90),
        )
        if _, de := p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 599), &captureSink{}); de == nil || de.Code != "WINDOW_QUOTA_EXCEEDED" {
                t.Fatalf("post-crash budgets must stay conservative: %v", de)
        }

        // Self-heal: one ReserveTTL later the stale reservation is pruned and
        // the math is exact again (used 1000 + est 600 ≤ 2500).
        time.Sleep(ttl + 150*time.Millisecond)
        be.rdb.Del(context.Background(), "usage:window:"+idn.Tenant.ID)
        if _, de := p.Stream(context.Background(), idn, reqBounded("openai/gpt-6-luna", 599), &captureSink{}); de != nil {
                t.Fatalf("after ReserveTTL the double-count must self-heal: %v", de)
        }
}
