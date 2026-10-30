-- Scheduled Abby follow-ups (out of office, not now, has-solution second note).
-- Apply before the code that inserts these rows. Idempotent.
-- Does not add campaign_id to abby_handled and does not create instantly_campaign_daily.

CREATE TABLE IF NOT EXISTS abby_followups_scheduled (
    id BIGSERIAL PRIMARY KEY,
    lead TEXT,
    reply_to_id TEXT,
    eaccount TEXT,
    subject TEXT,
    first_name TEXT,
    brand TEXT,
    campaign_id TEXT,
    kind TEXT,
    due_at TIMESTAMP,
    status TEXT DEFAULT 'pending',
    body TEXT,
    source_email_id TEXT,
    detail TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP,
    UNIQUE (source_email_id, kind)
);

CREATE INDEX IF NOT EXISTS abby_followups_scheduled_due_idx
    ON abby_followups_scheduled (status, due_at);

ALTER TABLE abby_followups_scheduled ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE abby_followups_scheduled FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE abby_followups_scheduled TO agent_app;
GRANT USAGE, SELECT ON SEQUENCE abby_followups_scheduled_id_seq TO agent_app;

DO $$
BEGIN
    CREATE POLICY abby_followups_scheduled_agent_app
        ON abby_followups_scheduled FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
