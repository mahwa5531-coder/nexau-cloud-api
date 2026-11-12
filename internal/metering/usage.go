// Package metering records authoritative llm_calls asynchronously. The
// streaming hot path must never block on PostgreSQL: records flow through a
// bounded queue to a single batcher goroutine that flushes in batches with
// retries.
//
// Durability (audit H3, root fix): records that cannot be persisted — DB
// outage longer than the retry budget, queue overflow, process death with a
// full queue — are no longer dropped. They are appended to a JSON-lines
// outbox file (fsync-per-batch) and replayed automatically once the database
// recovers or the process restarts. A record is only ever LOST when both
// PostgreSQL AND the local disk refuse it, and that case stays loud (metric +
// error log). Exactly-once still holds: replay goes through the same
// InsertCalls with ON CONFLICT DO NOTHING on llm_calls.call_id, so a record
// persisted-then-replayed cannot double-charge.
//
// Reconciliation hook: Config.AfterPersist fires per record once its fate is
// final — durable=true after the row is stored, durable=false when even the
// outbox refused it. The LLM proxy uses this to release the call's quota
// reservation (audit H2): released only once the authoritative sum covers it.
package metering

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/observability"
)

// CallWriter persists a batch of completed calls (implemented by
// repos.LLMCallsRepo.InsertCalls).
type CallWriter interface {
	InsertCalls(ctx context.Context, calls []domain.LLMCall) error
}

// Config bounds the recorder.
type Config struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	RetryMax      int

	// SpillPath is the durable outbox for records that exhausted their
	// write retries (or found the queue full). Empty disables persistence:
	// failures drop as before (tests and embedded deployments).
	SpillPath string

	// AfterPersist, when set, is invoked exactly once per record when its
	// fate is final: durable=true after the row is in PostgreSQL (including
	// via outbox replay), durable=false when the record is finally abandoned
	// (outbox write failed too). Never called on the hot path.
	AfterPersist func(rec domain.LLMCall, durable bool)

	// ReplayInterval bounds how often the outbox is retried (default 15s).
	ReplayInterval time.Duration
}

// Recorder is safe for concurrent use. Close must be called exactly once.
type Recorder struct {
	w   CallWriter
	cfg Config
	m   *observability.Metrics

	ch     chan domain.LLMCall
	wg     sync.WaitGroup
	cancel context.CancelFunc

	// dropCount tracks final losses (outbox refused too) between logs.
	dropMu   sync.Mutex
	dropped  int64
	lastDrop time.Time
}

// New starts the recorder, its batcher goroutine and (when configured) the
// outbox replayer.
func New(w CallWriter, cfg Config, m *observability.Metrics) *Recorder {
	if cfg.QueueSize < 64 {
		cfg.QueueSize = 64
	}
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if cfg.RetryMax < 0 {
		cfg.RetryMax = 0
	}
	if cfg.ReplayInterval <= 0 {
		cfg.ReplayInterval = 15 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Recorder{
		w:      w,
		cfg:    cfg,
		m:      m,
		ch:     make(chan domain.LLMCall, cfg.QueueSize),
		cancel: cancel,
	}
	r.wg.Add(1)
	go r.batcher(ctx)
	return r
}

// Record enqueues one authoritative call fact. It never blocks the streaming
// path: a full queue spills the record to the durable outbox instead of
// dropping it.
func (r *Recorder) Record(rec domain.LLMCall) {
	select {
	case r.ch <- rec:
		if r.m != nil {
			r.m.MeterQueueDepth.Set(int64(len(r.ch)))
		}
	default:
		r.spill([]domain.LLMCall{rec}, "queue_full")
	}
}

// spill persists records to the durable outbox; only a double failure (DB
// gone AND disk refusing) lands in the final-drop path.
func (r *Recorder) spill(records []domain.LLMCall, reason string) {
	if len(records) == 0 {
		return
	}
	if r.cfg.SpillPath == "" {
		// Outbox disabled: legacy loud-drop behaviour.
		for _, rec := range records {
			r.finalDrop(rec, reason)
		}
		return
	}
	if err := appendOutbox(r.cfg.SpillPath, records); err != nil {
		observability.LogError("metering: outbox write failed — records finally lost",
			"error", err, "records", len(records), "path", r.cfg.SpillPath)
		for _, rec := range records {
			r.finalDrop(rec, "spill_failed:"+reason)
		}
		return
	}
	if r.m != nil {
		r.m.UsageSpilled.Add(context.Background(), int64(len(records)))
	}
	// Records are durable in the outbox; they will be replayed. Do NOT fire
	// AfterPersist yet — the reservation must stay held until the row truly
	// lands (replay fires it).
}

// finalDrop accounts a record that can never be persisted.
func (r *Recorder) finalDrop(rec domain.LLMCall, reason string) {
	if r.m != nil {
		r.m.UsageDropped.Add(context.Background(), 1, observability.Attr("reason", reason))
	}
	if r.cfg.AfterPersist != nil {
		r.cfg.AfterPersist(rec, false)
	}
	r.dropMu.Lock()
	r.dropped++
	// Log at most once per second per process — a flood must not become a
	// log flood.
	if time.Since(r.lastDrop) > time.Second {
		r.lastDrop = time.Now()
		n := r.dropped
		r.dropped = 0
		r.dropMu.Unlock()
		observability.LogWarn("metering: call records finally lost (db and outbox both refused)",
			"lost_recent", n, "queue_size", r.cfg.QueueSize)
		return
	}
	r.dropMu.Unlock()
}

// batcher drains the queue into batches, persists with retry, and services
// the outbox replayer.
func (r *Recorder) batcher(ctx context.Context) {
	defer r.wg.Done()

	batch := make([]domain.LLMCall, 0, r.cfg.BatchSize)
	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()
	var replayC <-chan time.Time
	if r.cfg.SpillPath != "" {
		// First replay attempt happens promptly at startup, then on the
		// configured cadence (crashed-pod leftovers included).
		r.replayOutbox(ctx)
		rt := time.NewTicker(r.cfg.ReplayInterval)
		defer rt.Stop()
		replayC = rt.C
	}

	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		if err := safePersist(ctx, r.persistWithRetry, batch); err != nil {
			// Write failed after all retries: the outbox takes over. The
			// records survive; nothing is lost here.
			r.spill(batch, "db_error")
		} else {
			if r.m != nil {
				r.m.MeterBatchLatencyMS.Record(ctx, time.Since(start).Milliseconds())
				// Audit M8: these counters were registered but never
				// fired — dashboards scraped zeros for exactly the
				// behaviours they exist to reveal. Emit them at the
				// durable-persist decision point.
				for _, rec := range batch {
					r.m.CallsCompleted.Add(ctx, 1, observability.Attr("status", rec.Status))
					if rec.Usage.InputTokens > 0 || rec.Usage.OutputTokens > 0 {
						r.m.UsageTokensIn.Add(ctx, rec.Usage.InputTokens, observability.Attr("model", rec.ResolvedModel))
						r.m.UsageTokensOut.Add(ctx, rec.Usage.OutputTokens, observability.Attr("model", rec.ResolvedModel))
					}
				}
			}
			if r.cfg.AfterPersist != nil {
				for _, rec := range batch {
					r.cfg.AfterPersist(rec, true)
				}
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Shutdown: drain what is already queued (bounded), then stop.
			for {
				select {
				case rec := <-r.ch:
					batch = append(batch, rec)
					if len(batch) >= r.cfg.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case rec := <-r.ch:
			batch = append(batch, rec)
			if len(batch) >= r.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-replayC:
			r.replayOutbox(ctx)
		}
	}
}

// appendOutbox appends records as JSON lines, fsync-ing before returning so
// an immediately-following crash cannot lose them.
func appendOutbox(path string, records []domain.LLMCall) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("metering outbox mkdir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("metering outbox open: %w", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := range records {
		if err := enc.Encode(records[i]); err != nil {
			return fmt.Errorf("metering outbox encode: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("metering outbox flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("metering outbox fsync: %w", err)
	}
	return nil
}

// replayOutbox claims every pending outbox file and tries to persist its
// records. Claiming is an atomic rename: two processes sharing a volume never
// replay the same file. A failed persist puts the file back for the next
// cycle; a crashed replay leaves its claimed file behind, which the next
// startup sweep picks up.
func (r *Recorder) replayOutbox(ctx context.Context) {
	if r.cfg.SpillPath == "" {
		return
	}
	candidates := []string{r.cfg.SpillPath}
	if leftovers, err := filepath.Glob(r.cfg.SpillPath + ".claim-*"); err == nil {
		candidates = append(candidates, leftovers...)
	}
	for _, path := range candidates {
		r.replayFile(ctx, path)
	}
}

func (r *Recorder) replayFile(ctx context.Context, path string) {
	if _, err := os.Stat(path); err != nil {
		return // nothing pending
	}
	claimed := fmt.Sprintf("%s.claim-%d-%d", path, os.Getpid(), time.Now().UnixNano())
	if err := os.Rename(path, claimed); err != nil {
		return // raced with another replayer (or it vanished) — fine
	}

	f, err := os.Open(claimed)
	if err != nil {
		// Unreadable claim: leave it for the next sweep (it is still durable).
		return
	}
	var records []domain.LLMCall
	var badLines int
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec domain.LLMCall
		if err := json.Unmarshal(line, &rec); err != nil || rec.CallID == "" {
			badLines++
			continue
		}
		records = append(records, rec)
	}
	f.Close()
	if badLines > 0 {
		observability.LogWarn("metering: outbox contained unreadable lines",
			"file", claimed, "bad_lines", badLines)
	}

	if len(records) > 0 {
		// One attempt per cycle (no retry storm). On failure the claimed
		// file stays exactly where it is — the sweep globs .claim-* files
		// and retries them. It must NOT be renamed back over the original
		// path: records spilled in the meantime would be clobbered.
		if err := safePersist(ctx, r.persistOnce, records); err != nil {
			return
		}
	}
	if err := os.Remove(claimed); err != nil {
		// Persisted but not removable: rename out of the pending set so it
		// is not replayed forever; ON CONFLICT DO NOTHING would make a
		// duplicate harmless anyway.
		_ = os.Rename(claimed, claimed+".done")
	}
	if r.m != nil && len(records) > 0 {
		r.m.UsageReplayed.Add(ctx, int64(len(records)))
	}
	if r.cfg.AfterPersist != nil {
		for _, rec := range records {
			r.cfg.AfterPersist(rec, true)
		}
	}
}

// safePersist guards the batch write against panics (2026-09-18 post-
// mortem audit): the batcher runs on a detached goroutine where a panic
// would kill the process — and even if it only died silently, metering
// would stop for the pod's remaining lifetime (invisible billing gap). A
// contained panic is surfaced as a normal batch failure: spilled, counted,
// logged — and the loop keeps serving later records.
func safePersist(ctx context.Context, persist func(context.Context, []domain.LLMCall) error, batch []domain.LLMCall) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("usage writer panic contained: %v", rec)
		}
	}()
	return persist(ctx, batch)
}

// persistWithRetry retries only transient failures. Batch insert is
// idempotent (ON CONFLICT DO NOTHING), so a retry after a timeout can neither
// double-charge nor corrupt.
func (r *Recorder) persistWithRetry(ctx context.Context, batch []domain.LLMCall) error {
	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt <= r.cfg.RetryMax; attempt++ {
		// Per-attempt budget: the batcher must not wedge on a stuck query.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := r.w.InsertCalls(cctx, batch)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryableDB(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return lastErr
}

// persistOnce is the replay path: a single bounded attempt; the outbox file
// is the retry mechanism.
func (r *Recorder) persistOnce(ctx context.Context, batch []domain.LLMCall) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return r.w.InsertCalls(cctx, batch)
}

// retryableDB classifies storage failures. Context cancellation of the parent
// (shutdown) is NOT retryable here — the final drain runs on a live budget.
func retryableDB(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true // connection/timeout class: retry (idempotent insert)
}

// Close drains the queue, flushes remaining batches and stops the batcher,
// bounded by drain: a wedged PostgreSQL cannot extend process shutdown past
// the operator's grace window (records the drain cannot flush spill to the
// outbox and survive via replay).
func (r *Recorder) Close(drain time.Duration) {
	if drain <= 0 {
		drain = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		r.cancel()
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(drain):
		// Bounded: stop waiting. The batcher goroutine exits on its own when
		// the in-flight statement budget elapses; remaining records take the
		// outbox path and survive via replay.
	}
}

// QueueLen reports the current queue depth (diagnostics/metrics sampler).
func (r *Recorder) QueueLen() int { return len(r.ch) }

var _ = strconv.Itoa
