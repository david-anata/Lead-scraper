# Vercel cutover and rollback runbook

Status: production cut over to Vercel on August 16, 2026; rollback monitoring active

## Current production identity

- `agent.anatainc.com` resolves by CNAME to
  `5d8f9dc770aa7bd0.vercel-dns-016.com` and is served by Vercel with a valid
  production certificate.
- `agent-staging.anatainc.com` remains attached to the same Vercel project.
- The Render web service remains healthy as the rollback target. Only the Agent
  Render schedulers are suspended; unrelated Render projects were untouched.
- The public hostname and every production callback path remain unchanged.
  Google, Gmail, QuickBooks, Plaid, Resend, and other providers should be
  verified after DNS moves; do not rewrite their already-correct
  `https://agent.anatainc.com/...` URLs merely because hosting changes.

## Named roles

- Go/no-go owner: David Narayan
- Rollback owner: David Narayan
- Migration operator: Codex
- Business verification owner: David Narayan

One person may fill more than one role, but cutover does not begin until the
names are explicitly confirmed and the rollback owner is available throughout
the maintenance window.

## Safety conditions

- Render stays authoritative until the go/no-go owner approves the domain move.
- Never enable Render and Vercel writers or schedules at the same time.
- Take no production write action from staging during rehearsal.
- Keep Render deployable and its database untouched for at least seven days and two complete business cycles after cutover.

## Rehearsal

The owner approved Supabase Pro as the durable Vercel database target. The final
Render snapshot was restored into the `anata-agent-staging` Supabase project.
All 175 source tables passed count and every-row fingerprint comparison after
the four source-only outbound tables were recreated from the authoritative
schema. The application uses a dedicated non-owner role and the
Supabase browser-facing roles have no table access. See
`vercel-supabase-migration-rehearsal-receipt.md`. The earlier Neon capacity
receipt remains historical evidence only; Neon is no longer the target.

1. Record the Render deployment identifier, Vercel deployment identifier, DNS state, provider callbacks, and scheduler state.
2. Export a read-only full Render database snapshot. The production service has no persistent disk; retained Website Ops, Content, Fulfillment, and report artifacts are database-backed and remain inside this snapshot.
3. Confirm the Supabase project has at least 2x the extracted source-snapshot size available before restoring.
4. Restore the snapshot into the isolated Supabase migration target with owner and privilege restoration disabled.
5. Run `python scripts/vercel_migration_audit.py` with source and target database URLs. Artifact-directory arguments remain optional and are not required for the current Render service because no persistent disk exists.
6. Correct every reported mismatch. Re-run the same restore to prove idempotency.
7. Capture a second source delta and apply it once. Confirm no duplicate records or artifacts.
8. Run authentication, one page per major section, shared reports, critical read paths, and dry-run job endpoints.
9. Simulate rollback by restoring the pre-rehearsal target snapshot and rerunning the smoke checks.
10. Record elapsed time for full snapshot, delta, audit, smoke test, and rollback.

## Go/no-go checklist

- Immutable Vercel deployment is READY and matches the approved commit.
- Full regression suite and all three QA passes are clean.
- Database and artifact audit returns `"ok": true`.
- Every migration-critical staging callback and signed webhook passes. A
  future-facing provider that is not active in Render may remain disabled when
  the go/no-go owner explicitly records it as outside parity scope.
- Render writers are still active and Vercel writers are still disabled before the maintenance window.
- A current backup has been restored successfully at least once.
- Go/no-go and rollback owners are present.

## Cutover sequence

1. Announce the short read-only or maintenance window.
2. Record final source counts and scheduler receipts.
3. Disable Render schedules and external writers; verify they are stopped.
   Use `docs/vercel-scheduler-cutover-map.md` as the exact allowlist; unrelated Render projects remain untouched.
4. Take fresh source and target rollback archives. Replace Supabase's public
   table data with one maintenance-window Render `pg_dump --data-only` refresh,
   rather than attempting an unproven row-by-row delta. Truncate public target
   tables with `RESTART IDENTITY CASCADE`, then restore with `pg_restore
   --exit-on-error --data-only --no-owner --no-privileges`. This preserves the
   already-verified Supabase schema, RLS, grants, restricted `agent_app` role,
   and target-only empty table while replacing operational data and sequence
   values from the stopped authoritative source.
5. Run the migration audit and stop immediately on any mismatch.
6. Promote the already-approved Vercel deployment without rebuilding it.
7. Attach `agent.anatainc.com` to the approved Vercel project and replace only
   its GoDaddy DNS record. Verify the existing production provider callbacks;
   their hostname and paths do not change.
8. Verify readiness, login/logout, one page per section, restricted permissions, and critical read workflows.
9. Set `VERCEL_CRON_WRITES_ENABLED=true` with an empty
   `VERCEL_CRON_ENABLED_JOBS` allowlist. Add one exact job name at a time in the
   order from `docs/vercel-scheduler-cutover-map.md`, deploy the environment
   revision, invoke that job once, and confirm its successful ledger receipt
   before appending the next name. Do not use the `*` wildcard during cutover.
10. End the maintenance window only after the business verification owner signs off.

## Rollback triggers

Rollback immediately for authentication failure, permission leakage, missing or inconsistent durable data, repeated 5xx responses, failed critical integrations, duplicate external writes, incorrect Finance evidence, or scheduler overlap.

## Rollback sequence

1. Empty `VERCEL_CRON_ENABLED_JOBS`, set
   `VERCEL_CRON_WRITES_ENABLED=false`, and deploy that environment revision.
   Verify every write route returns `status=disabled` before proceeding.
2. Point `agent.anatainc.com` and provider callbacks back to Render.
3. Re-enable Render writers only after confirming Vercel writers are stopped.
4. If any Vercel-era writes occurred, export and reconcile them before restoring service; never discard them silently.
5. Verify Render readiness, authentication, major sections, critical reads, and one scheduler receipt.
6. Record the trigger, timestamps, affected records, reconciliation result, and next decision.

## Two-business-day monitoring

Review Vercel 5xx and latency, Supabase connections and errors, durable-task backlog, cron/job failures, provider webhook receipts, authentication failures, data-count drift, report freshness, and operator feedback at opening, midday, and close. Keep Render rollback-ready until this monitoring period and the seven-day retention rule both pass.

## August 16 production receipt

- Final Render source archive: `/tmp/agent-source-final.dump`, 153,463,946
  bytes, with 175 table-data entries. The Supabase pre-cutover rollback archive
  is `/tmp/agent-supabase-precutover.dump`, 151,893,833 bytes.
- The ordered restore and direct copy of the three legacy
  `outbound_export_history` rows completed. All 175 tables then passed exact
  count and sorted row-hash comparison (`FINAL_PARITY_EXIT:0`).
- The temporary migration role was revoked and dropped. Supabase retained RLS,
  denied browser-role table grants, and the restricted `agent_app` grants.
- Production health, storage, Google sign-in, owner access, all nine major
  workspaces, Finance Review, Website Ops Reports, Fulfillment CS Reports, and
  HR Reports passed on the production hostname without desktop overflow.
  Plaid and QuickBooks both report August 16 source freshness.
- Vercel writer receipts passed for durable tasks, Gmail sync, Website Ops,
  Content orchestration, Sales operator, HR reminders, Building operations,
  stale-lead scan, daily digest, and the time-guarded outbound morning job.
- `daily-lead-build` remains intentionally absent from the Vercel allowlist.
  Its GitHub credential was repaired, but Apollo then rejected the configured
  API key as invalid. Keep the corresponding Render job suspended; add the job
  only after a replacement Apollo key produces a successful controlled receipt.
- Render stays rollback-ready during the monitoring period. Do not re-enable a
  Render scheduler while its Vercel counterpart remains allowlisted.
