-- Mark stuck Prospeo INVALID_FILTERS rows finished without deleting them.
-- These are the active-campaign rows that kept returning to the front of
-- the enrich queue: amz_off platform subdomains, plan_upgrade, and replatform.
-- The original reason and enriched_at stay so the Prospeo error remains history.
-- Idempotent. A second run matches zero rows.

UPDATE outbound_enriched
SET status = 'invalid_filters'
WHERE reason ILIKE '%INVALID_FILTERS%'
  AND COALESCE(status, '') NOT IN ('found', 'in_instantly', 'invalid_filters')
  AND lower(domain) IN (
      SELECT lower(domain)
      FROM outbound_contacted_domains
      WHERE recipe IN ('amz_off', 'plan_upgrade', 'replatform', 'replatformed')
  );
