-- Apollo fallback: which provider found the contact, and Apollo credits per domain.
-- Apply in the Supabase SQL editor before relying on per-contact provider history.
-- Idempotent. Does not rewrite openers, campaigns, recipients, or Trial Offer.
-- The hourly summary still counts providers when these columns are absent.
-- Until apollo_credits exists, the enrich job stores apollo_credits=N on reason
-- and reads that token for the daily cap.

ALTER TABLE outbound_enriched
    ADD COLUMN IF NOT EXISTS apollo_credits INTEGER;

ALTER TABLE outbound_enriched_contacts
    ADD COLUMN IF NOT EXISTS provider TEXT;

-- Spend recorded before this column existed lives in the reason as
-- apollo_credits=N. Copy the first token onto NULL rows so the daily cap
-- does not drop that spend. A row that already has a number is left alone.
UPDATE outbound_enriched
SET apollo_credits = CAST(
    substring(reason from 'apollo_credits=([0-9]+)') AS INTEGER
)
WHERE apollo_credits IS NULL
  AND reason ~ 'apollo_credits=[0-9]+';
