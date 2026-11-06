-- Schema change: store the brandAngle line next to the opener.
-- Apply in the Supabase SQL editor before this branch is deployed.
-- Idempotent. Does not rewrite existing openers or delete rows.
-- No new environment variables.
--
-- outbound_enrich.ensure_tables also adds this column when the runtime
-- role is allowed to ALTER. Vercel skips that DDL, so this migration is
-- required there. Render can apply it from ensure_tables on boot.

ALTER TABLE outbound_enriched_contacts
    ADD COLUMN IF NOT EXISTS brand_angle TEXT;
