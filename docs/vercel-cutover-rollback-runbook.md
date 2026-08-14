# Vercel cutover and rollback runbook

Status: rehearsal-ready; execution requires the owner-approved maintenance window and source access

## Named roles

- Go/no-go owner: pending
- Rollback owner: pending
- Migration operator: pending
- Business verification owner: pending

## Safety conditions

- Render stays authoritative until the go/no-go owner approves the domain move.
- Never enable Render and Vercel writers or schedules at the same time.
- Take no production write action from staging during rehearsal.
- Keep Render deployable and its database untouched for at least seven days and two complete business cycles after cutover.

## Rehearsal

1. Record the Render deployment identifier, Vercel deployment identifier, DNS state, provider callbacks, and scheduler state.
2. Export a read-only full Render database snapshot. The production service has no persistent disk; retained Website Ops, Content, Fulfillment, and report artifacts are database-backed and remain inside this snapshot.
3. Restore the snapshot into the isolated Neon migration target.
4. Run `python scripts/vercel_migration_audit.py` with source and target database URLs. Artifact-directory arguments remain optional and are not required for the current Render service because no persistent disk exists.
5. Correct every reported mismatch. Re-run the same restore to prove idempotency.
6. Capture a second source delta and apply it once. Confirm no duplicate records or artifacts.
7. Run authentication, one page per major section, shared reports, critical read paths, and dry-run job endpoints.
8. Simulate rollback by restoring the pre-rehearsal target snapshot and rerunning the smoke checks.
9. Record elapsed time for full snapshot, delta, audit, smoke test, and rollback.

## Go/no-go checklist

- Immutable Vercel deployment is READY and matches the approved commit.
- Full regression suite and all three QA passes are clean.
- Database and artifact audit returns `"ok": true`.
- Staging callbacks and signed webhooks pass.
- Render writers are still active and Vercel writers are still disabled before the maintenance window.
- A current backup has been restored successfully at least once.
- Go/no-go and rollback owners are present.

## Cutover sequence

1. Announce the short read-only or maintenance window.
2. Record final source counts and scheduler receipts.
3. Disable Render schedules and external writers; verify they are stopped.
   Use `docs/vercel-scheduler-cutover-map.md` as the exact allowlist; unrelated Render projects remain untouched.
4. Take and apply the final database and artifact delta.
5. Run the migration audit and stop immediately on any mismatch.
6. Promote the already-approved Vercel deployment without rebuilding it.
7. Move `agent.anatainc.com` DNS and exact provider callbacks to Vercel.
8. Verify readiness, login/logout, one page per section, restricted permissions, and critical read workflows.
9. Enable Vercel jobs one at a time. Confirm a successful ledger receipt before enabling the next job.
10. End the maintenance window only after the business verification owner signs off.

## Rollback triggers

Rollback immediately for authentication failure, permission leakage, missing or inconsistent durable data, repeated 5xx responses, failed critical integrations, duplicate external writes, incorrect Finance evidence, or scheduler overlap.

## Rollback sequence

1. Disable all Vercel schedules and external writers.
2. Point `agent.anatainc.com` and provider callbacks back to Render.
3. Re-enable Render writers only after confirming Vercel writers are stopped.
4. If any Vercel-era writes occurred, export and reconcile them before restoring service; never discard them silently.
5. Verify Render readiness, authentication, major sections, critical reads, and one scheduler receipt.
6. Record the trigger, timestamps, affected records, reconciliation result, and next decision.

## Two-business-day monitoring

Review Vercel 5xx and latency, Neon connections and errors, durable-task backlog, cron/job failures, provider webhook receipts, authentication failures, data-count drift, report freshness, and operator feedback at opening, midday, and close. Keep Render rollback-ready until this monitoring period and the seven-day retention rule both pass.
