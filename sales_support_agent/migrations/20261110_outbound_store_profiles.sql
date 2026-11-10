-- Compact StoreLeads facts for openers.
-- Apply this file in the Supabase SQL editor before relying on stored
-- facts. Idempotent. Does not update Instantly and does not rewrite
-- existing lead rows. App startup on Vercel cannot create the table.
--
-- Until this is applied, opener code keeps the lead and does not send a
-- category filler. The next push retries once a profile row exists.

CREATE TABLE IF NOT EXISTS outbound_store_profiles (
    domain TEXT PRIMARY KEY,
    profile TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE outbound_store_profiles ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE outbound_store_profiles FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_store_profiles TO agent_app;

-- agent_app is the runtime role, not the table owner. RLS with no policy
-- would reject it, so this role has one policy.
DO $$
BEGIN
    CREATE POLICY outbound_store_profiles_agent_app
        ON outbound_store_profiles FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
