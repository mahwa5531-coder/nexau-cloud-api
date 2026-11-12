-- 0015 concurrency headroom for desktop-agent parallelism (product tuning).
--
-- Decision: the per-user concurrency limit STAYS as an abuse backstop but is
-- raised above any legitimate desktop-agent usage, so paying customers never
-- feel it. Rationale:
--
--   * The product is a desktop agent (Cursor/Antigravity class). A user
--     legitimately works across 1-5 parallel things at a time, and each thing
--     may briefly hold 1-2 in-flight LLM streams (subagent/tool fan-out).
--     The old Free cap of 3 was BELOW that envelope — a real user with 3
--     composer panes could trip CONCURRENCY_LIMIT on a normal morning.
--   * LLM calls are short (typical agent turn < 30s; p95 ~ 1 min). With an
--     8-slot cap and ~30s median duration, a user sustains at most ~16
--     calls/min — still under the Free 20 RPM window, so the RPM/quota
--     economics remain the binding throttle. Concurrency only catches
--     runaway clients, cracked apps and scripted abuse — never humans.
--   * The limit must NOT be removed outright: Phase 1 upstream is one shared
--     OpenRouter account whose rate limits are per-account, never per our
--     end-user. Per-user/per-tenant slots are the ONLY queueing discipline
--     that stops one user's runaway fan-out from starving every other paying
--     customer on the same key.
--
-- Operators hot-tune this without a deploy:
--   UPDATE plans SET limits = limits || '{"concurrent_requests_per_user": N}'::jsonb WHERE code='free';
-- (picked up within the identity-cache TTL, ~30s).

UPDATE plans
   SET limits = limits || '{
         "concurrent_requests_per_user": 8,
         "concurrent_requests_per_tenant": 8
       }'::jsonb
 WHERE code = 'free';
