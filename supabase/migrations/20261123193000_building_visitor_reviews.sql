-- Front-desk star taps and private notes for The Anata Building.
-- Additive only: creates one new table. Does not alter or rewrite existing tables.
-- Apply to the Agent Supabase project before using /review on Vercel.
-- Vercel skips boot-time DDL. Idempotent.

CREATE TABLE IF NOT EXISTS building_visitor_reviews (
    id VARCHAR(64) PRIMARY KEY,
    stars INTEGER NOT NULL,
    comment TEXT NOT NULL DEFAULT '',
    visitor_name VARCHAR(120) NOT NULL DEFAULT '',
    contact VARCHAR(160) NOT NULL DEFAULT '',
    visit_reasons_json JSON NOT NULL DEFAULT '[]',
    feedback_submitted BOOLEAN NOT NULL DEFAULT FALSE,
    notification_status VARCHAR(32) NOT NULL DEFAULT '',
    notification_json JSON NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_building_visitor_reviews_stars
    ON building_visitor_reviews (stars);

CREATE INDEX IF NOT EXISTS ix_building_visitor_reviews_feedback_submitted
    ON building_visitor_reviews (feedback_submitted);

CREATE INDEX IF NOT EXISTS ix_building_visitor_reviews_created_at
    ON building_visitor_reviews (created_at);

ALTER TABLE building_visitor_reviews ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE building_visitor_reviews FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE building_visitor_reviews TO agent_app;

DO $$
BEGIN
    CREATE POLICY building_visitor_reviews_agent_app
        ON building_visitor_reviews FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
