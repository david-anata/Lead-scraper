-- Restart the daily icp_baseline feed into Instantly
-- "AGENT SDR // MARCH '26" (4f118329-c07e-4279-9c08-12059f2a49f4).
--
-- Apply in the Supabase SQL editor for anata-agent-staging (vfcmljqakphwhslxtfzv).
-- Data only. Idempotent. Does not change schema, and does not start, edit,
-- or limit the Instantly campaign. Existing contacts are not updated.
--
-- StoreLeads f:ermin and f:ermax are monthly sales in USD cents.
-- $1,000,000 a year = 1000000 * 100 / 12 = 8333333.
-- $20,000,000 a year = 20000000 * 100 / 12 = 166666666.

DELETE FROM outbound_campaigns
WHERE campaign_id = '71add6d5-0096-52df-a6e2-66135fbe22e4'
  AND recipe_key = 'icp_baseline'
  AND name = 'icp_baseline|hold|no_trial_offer_ingest';

INSERT INTO outbound_campaigns (
    campaign_id, name, recipe_key, recipe_aliases, storeleads_filters,
    status, daily_cap, template_ref
) VALUES (
    '4f118329-c07e-4279-9c08-12059f2a49f4',
    'icp_baseline|standard|us_shopify_1m_20m',
    'icp_baseline',
    '',
    $json${"f:cc": "US", "f:ds": "Active", "f:ermax": 166666666, "f:ermin": 8333333, "f:it": "email", "f:p": "shopify"}$json$::jsonb,
    'active',
    25,
    'icp_baseline'
)
ON CONFLICT (campaign_id) DO UPDATE SET
    name = EXCLUDED.name,
    recipe_key = EXCLUDED.recipe_key,
    recipe_aliases = EXCLUDED.recipe_aliases,
    storeleads_filters = EXCLUDED.storeleads_filters,
    status = EXCLUDED.status,
    daily_cap = EXCLUDED.daily_cap,
    template_ref = EXCLUDED.template_ref
WHERE outbound_campaigns.recipe_key = 'icp_baseline';

UPDATE outbound_recipe_definitions
SET include_in_daily = 1,
    cap = 25,
    updated_by = 'agent-sdr-restart-2026-11-20',
    updated_at = CURRENT_TIMESTAMP,
    version = outbound_recipe_definitions.version + 1
WHERE recipe_key = 'icp_baseline'
  AND (include_in_daily IS DISTINCT FROM 1 OR cap IS DISTINCT FROM 25);
