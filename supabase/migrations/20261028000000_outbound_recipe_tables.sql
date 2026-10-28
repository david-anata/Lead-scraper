-- Outbound recipe editor + daily batch tables.
--
-- Apply this by hand in the Supabase SQL editor. The app does not run it:
-- Postgres schema creation is skipped unless a force flag is set, which is why
-- these tables were missing and the recipe editor had nothing to read.
--
-- Idempotent. CREATE TABLE IF NOT EXISTS, and the seed uses ON CONFLICT DO
-- NOTHING so a second run does not overwrite operator edits.
--
-- The six seed rows match outbound_recipes.RECIPES (key, label, reason, tier,
-- weekday list, and the code cap). An empty table is also safe: the app falls
-- back to those same code defaults.

CREATE TABLE IF NOT EXISTS outbound_daily_batches (
    id SERIAL PRIMARY KEY,
    business_date TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'America/Denver',
    status TEXT NOT NULL,
    trigger TEXT NOT NULL,
    recipe_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    scanned INTEGER NOT NULL DEFAULT 0,
    matched INTEGER NOT NULL DEFAULT 0,
    skipped_seen INTEGER NOT NULL DEFAULT 0,
    unique_companies INTEGER NOT NULL DEFAULT 0,
    duplicates_removed INTEGER NOT NULL DEFAULT 0,
    artifact_csv TEXT NOT NULL DEFAULT '',
    artifact_filename TEXT NOT NULL DEFAULT '',
    artifact_sha256 TEXT NOT NULL DEFAULT '',
    email_status TEXT NOT NULL DEFAULT 'not_requested',
    slack_status TEXT NOT NULL DEFAULT 'not_requested',
    error TEXT NOT NULL DEFAULT '',
    correlation_id TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_date, trigger)
);

CREATE TABLE IF NOT EXISTS outbound_daily_batch_runs (
    batch_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL DEFAULT 0,
    recipe_key TEXT NOT NULL,
    recipe_label TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    fresh INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (batch_id, recipe_key)
);

CREATE TABLE IF NOT EXISTS outbound_daily_batch_companies (
    batch_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    lead_json TEXT NOT NULL,
    primary_recipe TEXT NOT NULL,
    matched_recipes TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (batch_id, domain)
);

CREATE TABLE IF NOT EXISTS outbound_recipe_definitions (
    recipe_key TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    template_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    tier TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    weekdays TEXT NOT NULL,
    cap INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    include_in_daily INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 1,
    updated_by TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO outbound_recipe_definitions
    (recipe_key, label, template_key, reason, tier, priority, weekdays, cap)
VALUES
    ('new_growth_app', 'Just installed a growth or CRO app', 'new_growth_app',
     'They added a growth or conversion tool in the last two weeks', 'A', 1, '1,3', 40),
    ('churned_tool', 'Just dropped a growth tool', 'churned_tool',
     'They removed a growth tool recently, so the problem is still open', 'A', 2, '1,4', 30),
    ('plan_upgrade', 'Upgraded to Shopify Plus', 'plan_upgrade',
     'They upgraded their store plan recently, which usually means new budget', 'B', 3, '2', 30),
    ('replatformed', 'Recently replatformed', 'replatformed',
     'They moved platforms recently, so vendors are being chosen right now', 'B', 4, '2,4', 25),
    ('social_surge', 'Social following spiking', 'social_surge',
     'Their social following jumped in the last month', 'B', 5, '1,3', 25),
    ('icp_baseline', 'Core ICP (volume filler)', 'icp_baseline',
     'They fit our ideal customer profile', 'C', 6, '0,1,2,3,4', 25)
ON CONFLICT (recipe_key) DO NOTHING;
