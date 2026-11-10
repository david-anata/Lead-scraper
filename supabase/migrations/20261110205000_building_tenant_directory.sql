-- Anata Building tenant directory.
-- Additive only: creates one new table. Does not alter or rewrite existing tables.
-- Apply to the shared Supabase project before using /admin/building/tenants.
-- Idempotent.

CREATE TABLE IF NOT EXISTS building_tenant_directory_contacts (
    id VARCHAR(64) PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL DEFAULT '',
    company_name VARCHAR(255) NOT NULL DEFAULT '',
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(128) NOT NULL DEFAULT '',
    office_suite VARCHAR(128) NOT NULL DEFAULT '',
    august_lock VARCHAR(255) NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    receives_notices BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(255) NOT NULL DEFAULT '',
    updated_by VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_building_tenant_directory_contacts_email UNIQUE (email)
);

CREATE INDEX IF NOT EXISTS ix_building_tenant_directory_company
    ON building_tenant_directory_contacts (company_name);

CREATE UNIQUE INDEX IF NOT EXISTS uq_building_tenant_directory_email_lower
    ON building_tenant_directory_contacts (lower(email));

CREATE INDEX IF NOT EXISTS ix_building_tenant_directory_notice_recipients
    ON building_tenant_directory_contacts (is_active, receives_notices);

ALTER TABLE building_tenant_directory_contacts ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE building_tenant_directory_contacts FROM anon, authenticated;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE building_tenant_directory_contacts TO agent_app;

DO $$
BEGIN
    CREATE POLICY building_tenant_directory_contacts_agent_app
        ON building_tenant_directory_contacts FOR ALL TO agent_app
        USING (true) WITH CHECK (true);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
