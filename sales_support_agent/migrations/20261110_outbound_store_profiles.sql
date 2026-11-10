-- Compact StoreLeads facts for openers. Apply before turning on
-- OUTBOUND_RICH_PERSONALIZATION_ENABLED. Idempotent. Does not update
-- Instantly, and does not rewrite existing lead rows.
--
-- outbound_memory.ensure_profile_table creates this on SQLite. Vercel
-- Postgres skips runtime DDL, so this migration is required there.

CREATE TABLE IF NOT EXISTS outbound_store_profiles (
    domain TEXT PRIMARY KEY,
    profile TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
