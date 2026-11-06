-- Signal campaign catalog and weekly scores.
-- Apply in the Supabase SQL editor for anata-agent-staging (vfcmljqakphwhslxtfzv)
-- before this branch is deployed. Idempotent. Does not delete or rewrite rows.
--
-- push_pending reads outbound_campaigns. An empty table falls back to
-- RECIPE_CAMPAIGN_MAP. Scoring writes outbound_campaign_scores and may
-- insert status='proposed' rows. It does not activate campaigns, change
-- daily_cap, or create Instantly campaigns.

CREATE TABLE IF NOT EXISTS outbound_campaigns (
    campaign_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    recipe_key TEXT NOT NULL,
    recipe_aliases TEXT NOT NULL DEFAULT '',
    storeleads_filters JSONB,
    status TEXT NOT NULL CHECK (status IN ('active', 'throttled', 'cut', 'proposed')),
    daily_cap INTEGER NOT NULL DEFAULT 0,
    template_ref TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS outbound_campaigns_recipe_idx
    ON outbound_campaigns (recipe_key, status);

CREATE TABLE IF NOT EXISTS outbound_campaign_scores (
    id BIGSERIAL PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    week_start TEXT NOT NULL,
    contacted INTEGER NOT NULL DEFAULT 0,
    positive_per_100 REAL,
    meetings_per_100 REAL,
    bounce_pct REAL,
    opt_out_pct REAL,
    judged INTEGER NOT NULL DEFAULT 0,
    action TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, week_start)
);

-- name is signal|offer|target. replatformed is an alias, not a sixth campaign.
-- Filters are confirmed List Domains parameters. _relative_days is resolved
-- in the app and is never sent to StoreLeads.
-- Campaign ids below are the v2 Instantly campaigns. ON CONFLICT does not
-- rewrite a row that is already there, so cut v1 rows stay cut.
INSERT INTO outbound_campaigns (
    campaign_id, name, recipe_key, recipe_aliases, storeleads_filters,
    status, daily_cap, template_ref
) VALUES
(
    '8e0df427-1f18-43ac-b0c6-e8ea787123d7',
    'amz_on|standard|us_amazon_10_20k',
    'amz_on',
    '',
    $json${"f:cc": "US", "f:ds": "Active", "f:ermax": 2000000, "f:ermin": 1000000, "f:it": "email", "f:p": "shopify", "f:scs": "Amazon"}$json$::jsonb,
    'active',
    40,
    'amz_on'
),
(
    '138a162d-6654-4916-9959-20c11e31214b',
    'amz_off|standard|us_not_amazon_10_20k',
    'amz_off',
    '',
    $json${"f:cc": "US", "f:ds": "Active", "f:ermax": 2000000, "f:ermin": 1000000, "f:it": "email", "f:p": "shopify", "f:scs": "Amazon", "f:scs:op": "not"}$json$::jsonb,
    'active',
    40,
    'amz_off'
),
(
    '4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72',
    'plan_upgrade|standard|us_plus_90d',
    'plan_upgrade',
    '',
    $json${"_relative_days": {"f:last_plan_change_at:min": 90}, "f:cc": "US", "f:ds": "Active", "f:ermax": 2000000, "f:ermin": 1000000, "f:it": "email", "f:p": "shopify", "f:plan": "Shopify Plus"}$json$::jsonb,
    'active',
    30,
    'plan_upgrade'
),
(
    'd1c197e3-bf1f-4f41-bcb9-e30e73975a2c',
    'replatform|standard|us_woo_magento_bc_120d',
    'replatform',
    'replatformed',
    $json${"_relative_days": {"f:last_platform_change_at:min": 120}, "f:cc": "US", "f:ds": "Active", "f:ermax": 2000000, "f:ermin": 1000000, "f:it": "email", "f:last_platform": "woocommerce,magento,bigcommerce", "f:last_platform:op": "or", "f:p": "shopify"}$json$::jsonb,
    'active',
    25,
    'replatformed'
),
(
    '907d32fd-d6db-4ce6-8edc-485b78cce120',
    'abby_reengage|standard|prior_thread',
    'abby_reengage',
    '',
    NULL,
    'active',
    0,
    'abby_reengage'
)
ON CONFLICT (campaign_id) DO NOTHING;

ALTER TABLE outbound_campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbound_campaign_scores ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE outbound_campaigns FROM anon, authenticated;
REVOKE ALL ON TABLE outbound_campaign_scores FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_campaigns TO agent_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_campaign_scores TO agent_app;
GRANT USAGE, SELECT ON SEQUENCE outbound_campaign_scores_id_seq TO agent_app;

-- agent_app is the runtime role, not the table owner. RLS with no policy
-- would reject it, so each table has one policy for that role only.
DO $$
BEGIN
    CREATE POLICY outbound_campaigns_agent_app
        ON outbound_campaigns FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    CREATE POLICY outbound_campaign_scores_agent_app
        ON outbound_campaign_scores FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
