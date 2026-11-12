package metering

import (
        "context"
        "errors"
        "os"
        "path/filepath"
        "strings"
        "sync"
        "sync/atomic"
        "testing"
        "time"

        "github.com/mash-cloud/mash-api/internal/domain"
)

// flakyWriter fails while armed, then succeeds.
type outboxWriter struct {
        fail     atomic.Bool
        mu       sync.Mutex
        inserted []domain.LLMCall
}

func (w *outboxWriter) InsertCalls(_ context.Context, calls []domain.LLMCall) error {
        if w.fail.Load() {
                return errors.New("db down")
        }
        w.mu.Lock()
        w.inserted = append(w.inserted, calls...)
        w.mu.Unlock()
        return nil
}

func (w *outboxWriter) count() int {
        w.mu.Lock()
        defer w.mu.Unlock()
        return len(w.inserted)
}

func sampleCall(id string) domain.LLMCall {
        return domain.LLMCall{CallID: id, TenantID: "ten_1", Status: domain.CallCompleted}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
        t.Helper()
        // 10s: the outbox path fsyncs per batch — on a cold or loaded disk
        // (CI runners, first-run page cache) 5s was occasionally not enough.
        deadline := time.Now().Add(10 * time.Second)
        for time.Now().Before(deadline) {
                if cond() {
                        return
                }
                time.Sleep(20 * time.Millisecond)
        }
        t.Fatalf("timed out waiting for: %s", what)
}

// TestOutboxSpillAndReplay is the H3 regression test: a database outage
// longer than the retry budget must NOT lose usage records. The batch spills
// to the outbox file; once the database recovers, the replayer persists the
// records and fires AfterPersist(durable=true) for each.
func TestOutboxSpillAndReplay(t *testing.T) {
        w := &outboxWriter{}
        w.fail.Store(true)

        outbox := filepath.Join(t.TempDir(), "spill.jsonl")
        var persistedMu sync.Mutex
        var persisted []string
        var durableFlags []bool

        rec := New(w, Config{
                QueueSize: 128, BatchSize: 4, FlushInterval: 20 * time.Millisecond,
                RetryMax: 0, SpillPath: outbox, ReplayInterval: 50 * time.Millisecond,
                AfterPersist: func(r domain.LLMCall, durable bool) {
                        persistedMu.Lock()
                        persisted = append(persisted, r.CallID)
                        durableFlags = append(durableFlags, durable)
                        persistedMu.Unlock()
                },
        }, nil)
        defer rec.Close(2 * time.Second)

        rec.Record(sampleCall("call_a"))
        rec.Record(sampleCall("call_b"))
        rec.Record(sampleCall("call_c"))

        // The write fails (db down) → the batch lands in the outbox durably.
        // (The replayer may already have claimed/renamed the file, so the check
        // matches every pending outbox name, claims included.)
        waitUntil(t, "records spilled to outbox", func() bool {
                ms, _ := filepath.Glob(outbox + "*")
                for _, m := range ms {
                        if strings.HasSuffix(m, ".done") {
                                continue
                        }
                        if b, err := os.ReadFile(m); err == nil && len(b) > 0 {
                                return true
                        }
                }
                return false
        })

        // Database recovers: replay must persist everything and clean the file.
        w.fail.Store(false)
        waitUntil(t, "outbox replayed into the database", func() bool { return w.count() == 3 })
        waitUntil(t, "AfterPersist fired for every record", func() bool {
                persistedMu.Lock()
                defer persistedMu.Unlock()
                return len(persisted) == 3
        })
        persistedMu.Lock()
        for i, d := range durableFlags {
                if !d {
                        t.Fatalf("AfterPersist[%d] durable=%v, want true after replay", i, d)
                }
        }
        persistedMu.Unlock()

        if _, err := os.Stat(outbox); !os.IsNotExist(err) {
                t.Fatalf("outbox must be removed after successful replay, stat err=%v", err)
        }
}

// TestOutboxSurvivesRestart: records spilled by a dead process are replayed
// by the next one (crash recovery).
func TestOutboxSurvivesRestart(t *testing.T) {
        outbox := filepath.Join(t.TempDir(), "spill.jsonl")
        // A previous process spilled two records and died.
        if err := appendOutbox(outbox, []domain.LLMCall{sampleCall("r1"), sampleCall("r2")}); err != nil {
                t.Fatal(err)
        }

        w := &outboxWriter{}
        rec := New(w, Config{
                QueueSize: 128, BatchSize: 4, FlushInterval: 20 * time.Millisecond,
                RetryMax: 1, SpillPath: outbox, ReplayInterval: 50 * time.Millisecond,
        }, nil)
        defer rec.Close(2 * time.Second)

        waitUntil(t, "startup replay of a dead process's outbox", func() bool { return w.count() == 2 })
        if _, err := os.Stat(outbox); !os.IsNotExist(err) {
                t.Fatalf("outbox must be consumed, stat err=%v", err)
        }
}

// TestFinalDropFiresAfterPersistFalse: when BOTH the database and the outbox
// refuse, the record is finally lost and AfterPersist(durable=false) must
// fire so the caller can un-hold dependent state (quota reservations).
func TestFinalDropFiresAfterPersistFalse(t *testing.T) {
        w := &outboxWriter{}
        w.fail.Store(true)

        // Outbox path whose parent "directory" is actually a file → mkdir fails.
        roach := filepath.Join(t.TempDir(), "roach")
        if err := os.WriteFile(roach, []byte("x"), 0o644); err != nil {
                t.Fatal(err)
        }
        unusablePath := filepath.Join(roach, "spill.jsonl")

        var lostMu sync.Mutex
        var lost []string
        rec := New(w, Config{
                QueueSize: 128, BatchSize: 4, FlushInterval: 20 * time.Millisecond,
                RetryMax: 0, SpillPath: unusablePath,
                AfterPersist: func(r domain.LLMCall, durable bool) {
                        if !durable {
                                lostMu.Lock()
                                lost = append(lost, r.CallID)
                                lostMu.Unlock()
                        }
                },
        }, nil)
        defer rec.Close(2 * time.Second)

        rec.Record(sampleCall("call_lost"))
        waitUntil(t, "final drop signalled", func() bool {
                lostMu.Lock()
                defer lostMu.Unlock()
                return len(lost) == 1
        })
}

// TestQueueFullSpills: a wedged writer backs the queue up; overflow records
// spill durably instead of being dropped, and are recovered by replay.
func TestQueueFullSpills(t *testing.T) {
        w := &outboxWriter{}
        w.fail.Store(true)
        outbox := filepath.Join(t.TempDir(), "spill.jsonl")

        rec := New(w, Config{
                QueueSize: 64, BatchSize: 8, FlushInterval: 10 * time.Millisecond,
                RetryMax: 0, SpillPath: outbox, ReplayInterval: 50 * time.Millisecond,
        }, nil)
        defer rec.Close(3 * time.Second)

        // 200 records against a 64-slot queue and a failing writer: the overflow
        // must reach the outbox.
        for i := 0; i < 200; i++ {
                rec.Record(sampleCall("q" + time.Now().Format("150405.000000000") + "_" + itoa(i)))
        }

        waitUntil(t, "queue overflow reached the outbox", func() bool {
                b, err := os.ReadFile(outbox)
                return err == nil && len(b) > 100
        })

        // Recovery: everything (queued batches that spilled + queue overflow that
        // spilled) eventually lands exactly once and the outbox drains.
        w.fail.Store(false)
        waitUntil(t, "all records recovered", func() bool { return w.count() == 200 })
        waitUntil(t, "outbox drained", func() bool {
                _, err := os.Stat(outbox)
                return os.IsNotExist(err)
        })
}

func itoa(i int) string {
        if i == 0 {
                return "0"
        }
        var b [8]byte
        p := len(b)
        for i > 0 {
                p--
                b[p] = byte('0' + i%10)
                i /= 10
        }
        return string(b[p:])
}
