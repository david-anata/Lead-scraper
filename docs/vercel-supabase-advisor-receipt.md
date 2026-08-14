# Supabase advisor receipt

Reviewed: August 14, 2026

Project: `anata-agent-staging` (`vfcmljqakphwhslxtfzv`)

Status: no advisor finding blocks staging or the planned cutover

## Security

Supabase returned 172 informational `rls_enabled_no_policy` notices and no
warning- or error-level security notice. These tables intentionally have RLS
enabled without browser-facing policies. Agent does not use Supabase's public
Data API for application access; its server uses the dedicated restricted
`agent_app` Postgres role. With no `anon` or `authenticated` policy, Data API
access remains deny-by-default rather than accidentally public.

Reference: [Supabase RLS-without-policy advisor](https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy)

## Performance

Supabase returned 489 performance notices:

- 484 informational unused-index notices. The database was newly imported, so
  index-usage counters do not yet represent production traffic. No index will
  be removed from this signal before two business days of post-cutover traffic.
- One informational notice that Supabase Auth uses an absolute connection
  allocation. Agent authentication is application-managed and this is not on
  the request path. Revisit only if Supabase Auth is adopted or the instance is
  resized.
- Four warning-level duplicate indexes on `lead_mirrors`: `is_active`,
  `is_closed`, `status_key`, and `task_updated_at`. The duplicates come from
  SQLAlchemy's canonical `ix_lead_mirrors_*` indexes plus legacy compatibility
  indexes ending in `_idx`.

Reference: [Supabase duplicate-index advisor](https://supabase.com/docs/guides/database/database-linter?lint=0009_duplicate_index)

## Decision

Do not drop the four legacy indexes before cutover. The current Supabase schema
intentionally matches the Render rehearsal source; changing it now would weaken
that parity evidence for negligible benefit. After two business days of stable
production traffic, compare real index usage, keep the canonical
`ix_lead_mirrors_*` indexes, remove only proven duplicates through a reviewed
migration, and rerun both advisors.

This receipt contains no connection strings, keys, database values, or customer
data.
