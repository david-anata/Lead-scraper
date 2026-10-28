-- Bounce tracking and the outbound suppression list.
-- Apply in the Supabase SQL editor for anata-agent-staging (vfcmljqakphwhslxtfzv)
-- before this branch is deployed. Idempotent. Does not delete or rewrite rows.
-- outbound_enriched and outbound_enriched_contacts already exist.

ALTER TABLE outbound_enriched_contacts
    ADD COLUMN IF NOT EXISTS email_status TEXT;

ALTER TABLE outbound_enriched_contacts
    ADD COLUMN IF NOT EXISTS skip_reason TEXT;

CREATE TABLE IF NOT EXISTS outbound_delivery_events (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    campaign_id TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    external_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS outbound_delivery_events_occurred_idx
    ON outbound_delivery_events (occurred_at, campaign_id, event_type);

CREATE TABLE IF NOT EXISTS outbound_suppressions (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kind, value)
);

CREATE TABLE IF NOT EXISTS outbound_sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE outbound_delivery_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbound_suppressions ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbound_sync_state ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE outbound_delivery_events FROM anon, authenticated;
REVOKE ALL ON TABLE outbound_suppressions FROM anon, authenticated;
REVOKE ALL ON TABLE outbound_sync_state FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_delivery_events TO agent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_suppressions TO agent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_sync_state TO agent_app;

GRANT USAGE, SELECT ON SEQUENCE outbound_delivery_events_id_seq TO agent_app;
GRANT USAGE, SELECT ON SEQUENCE outbound_suppressions_id_seq TO agent_app;

-- agent_app is the runtime role, not the table owner. RLS with no policy
-- would reject it, so each table has one policy for that role only.
DO $$
BEGIN
    CREATE POLICY outbound_delivery_events_agent_app
        ON outbound_delivery_events FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE POLICY outbound_suppressions_agent_app
        ON outbound_suppressions FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE POLICY outbound_sync_state_agent_app
        ON outbound_sync_state FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
