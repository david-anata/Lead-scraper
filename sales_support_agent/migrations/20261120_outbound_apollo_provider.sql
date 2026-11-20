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
