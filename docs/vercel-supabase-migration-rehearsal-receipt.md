# Vercel to Supabase migration rehearsal receipt

Status: imported, secured, and staging-verified; production cutover is not authorized

Date: August 13, 2026 (America/Denver)

## Approved target

- Supabase organization: `anata`
- Project: `anata-agent-staging`
- Project reference: `vfcmljqakphwhslxtfzv`
- Region: `us-west-1`
- Plan impact approved by the owner: $10/month incremental project cost
- Vercel staging application: `https://agent-staging.anatainc.com`
- Render production and `https://agent.anatainc.com` were not changed.

The Vercel application connects through Supabase's IPv4 session pooler with a
dedicated `agent_app` login. The role can read and update Agent's application
tables but cannot administer the database. The database owner credential is
not present in Vercel.

## Source and rollback evidence

- Render export archive:
  `agent-render-export-2026-08-14.dir.tar.gz`
- Render export size: 151,670,231 bytes
- Render export SHA-256:
  `2BE5B16E7F0484C69A218BAFFF3C5A5C4984BB0BA28CD249D0548EADA8861F18`
- Source PostgreSQL version: 16.14
- Restore entries: 1,428
- Source tables/table-data sets/sequences: 171 / 171 / 61
- Supabase pre-import rollback archive:
  `agent-supabase-preimport-2026-08-14.dir.tar.gz`
- Rollback archive size: 751 bytes
- Rollback archive entries: 2
- Rollback archive SHA-256:
  `E605B1EF95C6A5A040C65F432C50D700EBCDA21F1D727BD4D1CB2CEDD90F1752`

Both archives remain outside the repository and contain no credentials.

Immediately before applying the current application schema, a second isolated
rollback archive was created:

- Archive: `agent-supabase-postimport-premigration-2026-08-14.dir.tar.gz`
- Size: 151,048,153 bytes
- Entries: 209
- SHA-256:
  `AB8B7FEDDB3CF808711891D42BC0CFDD5D614969EE4C26E37FB1D67908E523A8`

## Restore and parity result

The Render directory-format dump was restored with PostgreSQL 17 tooling using
`--exit-on-error`, `--no-owner`, `--no-privileges`, and four parallel jobs. The
restore completed without an error in approximately 212 seconds.

The post-restore audit proves:

- 171 of 171 imported tables match;
- 223,016 of 223,016 rows match;
- every table's row count matches;
- a full, order-independent fingerprint of every COPY row matches, with zero
  changed-table fingerprints;
- 61 of 61 sequence values and `is_called` states match;
- all 144 `website_ops_files` rows are present;
- staging reports 124,124,830 retained Website Ops bytes;
- retained `content_artifacts`, Finance report tables, Brand Analysis reports,
  and the other source-backed artifacts are included in the byte-level row
  fingerprint.

One empty Vercel-only table, `fulfillment_report_files`, was then created as an
additive application-support table. It does not exist in the Render snapshot
and is intentionally excluded from the 171-table source equality claim. The
production service has no persistent Fulfillment report disk to migrate, so
the canonical staging state is an explicit empty report library rather than a
fabricated report history.

The repository's normal `scripts/predeploy_agent.py` migration was then run
once under its PostgreSQL advisory lock with the Supabase owner. Its first
attempt was canceled safely while an old serverless connection held an idle
transaction lock. The transaction rolled back. The application role now has a
60-second idle-in-transaction timeout, the stale transaction cleared, and the
second migration completed in 36.7 seconds. The existing Arena agreement,
commercial draft, and tax decision were preserved unchanged.

Vercel sets `AGENT_RUNTIME_SCHEMA_MAINTENANCE=false`. This makes request-time
Finance helpers skip DDL after pre-deploy preparation, so the restricted
application role is never promoted to table owner merely to satisfy legacy
`CREATE INDEX IF NOT EXISTS` calls.

## Security result

Immediately after import:

- RLS was enabled on every imported public table;
- all table, sequence, and function privileges were revoked from Supabase's
  `anon` and `authenticated` roles;
- future default privileges for those browser-facing roles were revoked;
- the one mutable function search path was pinned to `pg_catalog, public`;
- `fulfillment_report_files` received the same RLS and revocation treatment;
- the dedicated `agent_app` role uses only explicit application grants and has
  no database-owner or superuser authority.

The post-DDL Supabase security advisor has no warning or error. Its 172
informational `RLS enabled, no policy` notices are intentional: the Data API
roles are denied and the server application uses its own restricted role. See
the [Supabase RLS advisor explanation](https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy).

Performance advisor notices are limited to fresh-database unused-index
information, four source-existing duplicate indexes, and the Auth connection
allocation recommendation. No index was removed during migration because that
would make the target schema diverge from the verified production snapshot.

## Exact staging release verification

Vercel deployment `dpl_BebN1HFP7MwHyLxJH1BSAqnMiLLZ` became READY and was
aliased to `agent-staging.anatainc.com`. After correcting the SQLAlchemy driver
to the repository-supported `psycopg2` dialect:

- `/health/live` passed;
- `/health/ready` passed with a live Supabase query;
- `/health/storage` passed with 144 files and 124,124,830 bytes;
- Workspace Home plus Sales, Fulfillment, Finance, HR, Website Ops, Content,
  Building, Advertising, Executive, Fulfillment CS Reports, Website Ops
  Reports, and HR Reports rendered through the authenticated staging session;
- the representative desktop pages retained nine canonical global navigation
  links, no global horizontal overflow, and no browser console errors;
- the Fulfillment CS report route's initial missing-table 500 was fixed and now
  renders its canonical empty state.

Render production remains the authority. This receipt does not authorize DNS,
callback, scheduler-writer, or production database cutover.

## Isolated rollback-restore proof

Repeated August 13, 2026 after candidate `8e5dcfb` passed its full release gate.
The PostgreSQL 17.11 client/server binaries and the saved archives were used
entirely on localhost; neither Supabase nor Render was modified.

- `agent-supabase-postimport-premigration-2026-08-14.dir.tar.gz` restored with
  `--exit-on-error --no-owner --no-privileges --schema=public --jobs=4` in
  12.890 seconds.
- The restored rollback state contained 172 public tables and 223,018 rows.
- Key recovered counts were: 12 application users, 6,738 cash events, 144
  Website Ops files, and 174 scheduled-job receipts.
- The saved Render base export independently restored in 17.267 seconds with
  171 tables and 223,016 rows.
- The two expected rollback-snapshot additions were one HR audit event and one
  key/value record; `fulfillment_report_files` was the expected empty additive
  target table.
- Both local databases shut down cleanly, port 55432 stopped listening, and the
  two temporary data directories were deleted and verified absent. The source
  archives remain intact outside the repository as the recoverable evidence.

This proves the base and rollback archives are readable and restorable. It does
not replace the final live-delta capture required inside the approved cutover
window.
