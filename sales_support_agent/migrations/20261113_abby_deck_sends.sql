-- Deck sends and the hourly missed-reply log.
-- Apply before Abby sends a plan or the fixer runs. Idempotent.
-- Does not rewrite abby_handled.

CREATE TABLE IF NOT EXISTS abby_deck_sends (
    id BIGSERIAL PRIMARY KEY,
    lead TEXT NOT NULL,
    campaign_id TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    source_email_id TEXT NOT NULL,
    reply_id TEXT NOT NULL DEFAULT '',
    cc_address TEXT NOT NULL DEFAULT '',
    deck_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'sent',
    sent_at TIMESTAMP,
    founder_reply_id TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_email_id)
);

CREATE INDEX IF NOT EXISTS abby_deck_sends_sent_idx
    ON abby_deck_sends (status, sent_at);

CREATE TABLE IF NOT EXISTS abby_missed_reply_fixes (
    email_id TEXT PRIMARY KEY,
    lead TEXT NOT NULL DEFAULT '',
    campaign_id TEXT NOT NULL DEFAULT '',
    previous_action TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL DEFAULT '',
    reply_id TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS abby_missed_reply_fixes_sent_idx
    ON abby_missed_reply_fixes (outcome, sent_at);

CREATE TABLE IF NOT EXISTS abby_copy_applies (
    apply_key TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    report TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE abby_deck_sends ENABLE ROW LEVEL SECURITY;
ALTER TABLE abby_missed_reply_fixes ENABLE ROW LEVEL SECURITY;
ALTER TABLE abby_copy_applies ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE abby_deck_sends FROM anon, authenticated;
REVOKE ALL ON TABLE abby_missed_reply_fixes FROM anon, authenticated;
REVOKE ALL ON TABLE abby_copy_applies FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE abby_deck_sends TO agent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE abby_missed_reply_fixes TO agent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE abby_copy_applies TO agent_app;
GRANT USAGE, SELECT ON SEQUENCE abby_deck_sends_id_seq TO agent_app;

DO $$
BEGIN
    CREATE POLICY abby_deck_sends_agent_app
        ON abby_deck_sends FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE POLICY abby_missed_reply_fixes_agent_app
        ON abby_missed_reply_fixes FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE POLICY abby_copy_applies_agent_app
        ON abby_copy_applies FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
