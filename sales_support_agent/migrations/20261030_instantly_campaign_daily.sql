-- Instantly daily campaign analytics, and the campaign on each Abby decision.
-- Apply for anata-agent-staging (vfcmljqakphwhslxtfzv). Idempotent.
-- Does not delete or rewrite existing abby_handled rows.

ALTER TABLE abby_handled
    ADD COLUMN IF NOT EXISTS campaign_id TEXT;

CREATE TABLE IF NOT EXISTS instantly_campaign_daily (
    date DATE NOT NULL,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT NOT NULL DEFAULT '',
    sent INTEGER NOT NULL DEFAULT 0,
    contacted INTEGER NOT NULL DEFAULT 0,
    new_leads_contacted INTEGER NOT NULL DEFAULT 0,
    replies INTEGER NOT NULL DEFAULT 0,
    unique_replies INTEGER NOT NULL DEFAULT 0,
    positive INTEGER NOT NULL DEFAULT 0,
    bounced INTEGER NOT NULL DEFAULT 0,
    unsubscribed INTEGER NOT NULL DEFAULT 0,
    opportunities INTEGER NOT NULL DEFAULT 0,
    meetings_booked INTEGER,
    pulled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, campaign_id)
);

ALTER TABLE instantly_campaign_daily ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE instantly_campaign_daily FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE instantly_campaign_daily TO agent_app;

DO $$
BEGIN
    CREATE POLICY instantly_campaign_daily_agent_app
        ON instantly_campaign_daily FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
