-- 000013_sweeper_indexes.sql (audit M15)
--
-- The housekeeping sweeper runs this DELETE every 60 seconds:
--
--     DELETE FROM refresh_tokens
--     WHERE expires_at < now() - $1::interval
--        OR revoked_at < now() - $1::interval
--
-- It had NO supporting index: every sweep was a full-table scan whose cost
-- grew with the whole table (not the expired fraction), on a table that only
-- ever grows. Two plain indexes let PostgreSQL BitmapOr the two range
-- conditions; the revoked_at one is partial because most rows are NULL.
--
-- (A partial index on "expired" is impossible: index predicates must be
-- immutable, and now() is not.)

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_expires_at
    ON refresh_tokens (expires_at);

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_revoked_at
    ON refresh_tokens (revoked_at)
    WHERE revoked_at IS NOT NULL;
