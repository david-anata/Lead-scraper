# Agent Vercel release-candidate sign-off

Status: engineering candidate ready for owner-approved production cutover; production cutover not approved

## Candidate identity

- Branch: `codex/vercel-agent-duplicate`
- Candidate application commit: `c6fabf1`
- Full GitHub release gate: run `31867676137`, passed
- Current immutable deployment: `dpl_A1gaRVTrmJVLvzio1YQfFis8kxKh`
- Immutable URL: `https://anata-agent-staging-luhwrkjpd-david-narayans-projects.vercel.app`
- Stable staging URL: `https://agent-staging.anatainc.com`
- Database: Supabase project `vfcmljqakphwhslxtfzv`, `us-west-1`, dedicated restricted `agent_app` role

## Passed engineering gates

- Full hosted regression gate passed for `ce72a0a`.
- Tracked-secret scan: 798 tracked files, zero findings. The scan reports only
  file, line, and pattern name and never prints a candidate value.
- Dependency audit: 46 declared/runtime dependencies, zero known vulnerabilities.
- Route inventory regenerated: 689 mounted FastAPI routes.
- Source/target database parity: 171 tables, 223,016 rows, full-row fingerprints,
  and 61 sequence states match the rehearsal snapshot.
- Artifact parity: 144 Website Ops files, 124,124,830 bytes; Render has no
  persistent disk outside database-backed retained state.
- Hosted Vercel scheduler authentication and read-only synthetic health pass.
- Hosted durable queue recovery passes failure, retry, overlap, and replay tests
  with no external effect.
- All 11 write schedules have successful Supabase shadow receipts with readable
  database inputs, configured declared dependencies, and no external writes.
- Three separate deployments of the final commit passed concurrent readiness;
  13 authenticated representative pages passed two complete desktop passes at
  1280px. Finance
  completed in 6.36–6.79 seconds cold and 252 ms warm after its query fix.
- Administrator and fresh-session passes found every expected heading, main
  landmark, skip target, and no horizontal overflow. A temporary restricted
  user could access Sales only and received a friendly denial for Finance,
  Executive, and HR. Its user and token records were deleted and verified zero.
- The saved rollback archive restored into isolated PostgreSQL 17 in 12.89
  seconds and recovered 172 tables / 223,018 rows, including all 144 retained
  Website Ops files. The isolated databases were stopped and securely removed.
- Vercel schedules remain globally write-disabled. Render production remains unchanged.
- Deal Board reads no longer launch an in-process HubSpot thread on Vercel.
  The explicit, CSRF-protected Sync action remains functional by completing in
  its request, while Render retains its existing background behavior. The final
  deployment log scan contains no HubSpot background error, error-level entry,
  or 5xx response.
- The current candidate also uses the configured QuickBooks callback in its
  setup help and recovers genuinely stale Website Ops runs before the first
  daily pulse. Focused tests and the complete hosted gate passed after both
  changes. Five concurrent stable-hostname readiness requests returned the
  Agent `{"status":"ready"}` response, and authenticated Website Ops,
  Finance/QuickBooks, and Deal Board reads returned 200. The exact deployment
  log scan contains no error-level, HubSpot-background, or 5xx entry.
- The final staging amendment removes Render-only operator guidance, pins deck
  links to the staging hostname, and identifies SQLAlchemy PostgreSQL driver
  URLs as Postgres on Settings. Google sign-in passed against the registered
  staging callback. Five concurrent readiness requests and an authenticated
  Settings inspection passed on the exact deployment; Settings reports
  `Postgres`, the staging deck origin, and provider-neutral deployment copy.
  Its exact log scan contains no error-level or 5xx application entry.
- The current candidate replaces the authentication marketing split-screen
  with one focused sign-in card. Email and Google authentication remain
  visible, while the break-glass password stays available behind a native
  `Admin recovery` disclosure. The live 1440px visual pass found no clipping or
  overflow; the semantic region, labeled fields, focus styles, error state,
  reduced-motion fallback, and collapsed/expanded recovery states pass. Five
  concurrent readiness calls returned 200 and the exact deployment logs have
  no error-level or 5xx application entry.
- The legacy Neon marketplace resource is disconnected from the Agent project,
  and all Neon-provided environment variables are absent. The resource itself
  remains undeleted as recoverable historical data until the owner separately
  authorizes destructive deletion.
- Supabase's August 14 advisor scan contains no warning- or error-level security
  finding. Its four duplicate-index warnings are documented for post-cutover
  cleanup; the indexes remain unchanged now to preserve source/target schema
  parity. Newly reset index-usage statistics will be reviewed only after real
  post-cutover traffic.
- A dedicated Resend staging webhook is enabled for delivery, bounce,
  complaint, delay, and failure events. Its signing secret is stored as a
  sensitive Vercel environment value. A controlled message to Resend's own
  `delivered@resend.dev` test address was provider-accepted, and its signed
  `email.delivered` webhook returned 200 on staging. The one-time fixed-recipient
  probe was removed after the receipt; no customer was contacted.
- The QuickBooks staging callback is saved alongside production in Intuit. A
  real OAuth authorization selected the verified Anata company, returned to
  staging, passed one-time state validation, stored fresh tokens, and rendered
  the authenticated workspace. The callback completed with 303, the workspace
  returned 200, and no QuickBooks accounting record was changed.
- Plaid now has both the production and staging OAuth return URIs registered,
  and its Sandbox webhook points at staging. Deployment
  `dpl_UVYBWGwk7Vzwpad9RK7Qvi6sSaBn` verified two provider-signed Sandbox
  deliveries with HTTP 200. Each receipt was authenticated against the
  environment-specific Plaid key, logged `processed=false`, executed zero
  database queries, and triggered no Finance sync or production-shaped data
  mutation. The primary Finance Plaid environment remains production.
- Finance's 30-second read cache is shared through the configured database so
  separate Vercel instances do not each rebuild the same brief. On the exact
  deployment, the first authenticated desktop load completed in 6.75 seconds
  and the next three completed in 0.30-0.68 seconds. HR Payroll completed in
  2.56 seconds. Both pages rendered their expected heading without horizontal
  overflow or browser errors. Five concurrent readiness requests returned 200,
  the removed Resend staging probe returned 404, and the exact deployment had
  no error-level log entries.
- The final database comparator now fingerprints every normalized row in every
  table, in addition to counts, schema, sequences, samples, and optional
  artifacts. A regression proves that it detects changed content beyond the
  five-row sample window. Vercel scheduled writers now require both the global
  cutover flag and an explicit per-job allowlist entry, so jobs can be enabled
  and receipted one at a time. Staging has the global flag false and an explicit
  no-job allowlist. The exact deployment passed five concurrent readiness
  requests, unauthenticated cron rejection, Finance and HR browser checks, and
  an error-level log scan with no findings.

## Required before asking for cutover approval

1. Keep optional user-connected Gmail outside the cutover unless the business
   separately chooses to launch it. The preserved system-managed inbox,
   Google sign-in, QuickBooks, Plaid, and Resend receipts are complete. Stripe
   is not Agent's billing rail, and the unused Instantly event webhook is not a
   cutover gate.
2. During the approved maintenance window, capture and apply the final
   production data-only refresh, rerun the every-row parity audit, and record the current source
   snapshot identifier. The base restore and rollback archive restore are timed
   and proven; only the live final refresh remains.
3. Name the go/no-go owner, rollback owner, migration operator, and business
   verification owner.
4. Obtain explicit approval for the production maintenance window, domain move,
   callback verification, and one-at-a-time scheduler enablement.

## Production safety statement

This record does not authorize changing `agent.anatainc.com`, disabling Render,
enabling Vercel writer schedules, moving callbacks, or running a final production
delta. Those actions require the explicit owner approval described above.
