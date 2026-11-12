-- 000014_retention.sql (audit H9)
--
-- llm_calls and payment_webhook_events were append-only in practice: nothing
-- ever deleted, archived or partitioned them, and every interesting query
-- (quota windows, /v1/usage sums, reconciliation) scans them. This is
-- compound interest in the wrong direction: each month the same queries cost
-- more, backups take longer, migrations get scarier.
--
-- Retention model (pragmatic root fix):
--   * llm_calls keeps 400 days of raw rows (NEXAU_USAGE_RETENTION_DAYS) —
--     strictly more than the 366-day maximum /v1/usage window, so no API
--    -visible data ever disappears — and the housekeeping sweeper purges
--     older rows in bounded batches using the index below.
--   * payment_webhook_events keeps 90 days
--     (NEXAU_WEBHOOK_RETENTION_DAYS): the recorded payload is the recovery
--     tool for missed captures; after 90 days every referenced order is deep
--     in a terminal state.
--   * Time-based partitioning can replace the batch purge later, once the
--     table is actually large enough to need it; the indexes make the
--     interim cost predictable either way.

CREATE INDEX IF NOT EXISTS idx_llm_calls_recorded_at
    ON llm_calls (recorded_at);

CREATE INDEX IF NOT EXISTS idx_payment_webhook_events_created_at
    ON payment_webhook_events (received_at);
