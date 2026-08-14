# Vercel database migration rehearsal receipt

Date: August 13, 2026 (America/Denver)

Status: capacity gate failed safely; production and working staging database unchanged

## Source snapshot

- Render database: `sales-support-agent-db`
- Render database ID: `dpg-d740m8paae7s73b90mf0-a`
- Export timestamp: `2026-08-14T01:00Z`
- Export format: PostgreSQL directory dump inside a gzip-compressed tar archive
- Compressed bytes: `151670231`
- Compressed SHA-256: `2BE5B16E7F0484C69A218BAFFF3C5A5C4984BB0BA28CD249D0548EADA8861F18`
- Archive entries: `175`
- Extracted dump bytes: `871961458`
- Dumped PostgreSQL version: `16.14`
- Restore manifest: `1428` entries, `171` tables, `171` table-data sets, and `61` sequences
- Read-only source inventory: `171` tables and `223016` rows

The Render export remains protected by Render's backup retention and a temporary local migration copy. No signed download URL or database credential is recorded here.

## Target baseline and rollback

- Neon project: `divine-cell-12994442`
- Working database: `neondb`
- PostgreSQL version: `17.10`
- Plan at rehearsal time: Free, `0.5 GB` storage per project
- Read-only baseline: `167` tables, `233` rows, and `7` non-empty tables
- Baseline database size immediately before rehearsal: `20365312` bytes
- Logical rollback dump: `168` files and `1470` restore entries
- Compressed rollback bytes: `107941`
- Compressed rollback SHA-256: `012FA0E62EE221467225AEF0A2CB90A7E6F7873B22E60648DBAF277EEB1AE8EE`

The rollback snapshot completed and verified before any target write. Temporary environment files were deleted after use.

## Isolated restore attempt

The migration operator created a separate database named `agent_migration_rehearsal_20260814` in the existing isolated Neon project. The working `neondb` database remained online and was not overwritten.

The restore stopped while copying `mailbox_signals` because Neon's externally enforced project-size limit of `512 MB` was reached. At inspection time the partial rehearsal database occupied `350461952` bytes, in addition to the working database and project overhead.

The new read-only capacity preflight confirms the same condition before import: `536870912` project-limit bytes, `20365312` working-target bytes, and `1764288228` required bytes when applying the runbook's 2x source-snapshot headroom. The receipt returns `"ok": false` with exit code `1` on Free as designed.

This is a capacity failure, not a schema, credential, PostgreSQL-version, or application failure. The incomplete rehearsal database was force-removed after its exact name and size were verified. The stable staging readiness endpoint returned HTTP `200` after cleanup.

## Required resolution

Upgrade the Vercel Neon installation from **Free** to **Launch** before rerunning the restore. Launch is the smallest plan that removes the `0.5 GB` project ceiling and supplies enough room for the production snapshot, indexes, a migration database or branch, and growth. The plan is usage-based; no billable plan change may be confirmed without owner approval.

After approval:

1. Change the Neon installation plan to Launch in Vercel.
2. Confirm the new storage limit before importing.
3. Recreate the isolated rehearsal database.
4. Restore the same SHA-256-verified Render snapshot.
5. Run the full schema/count/sample/watermark/sequence comparator.
6. Prove rollback from the recorded pre-import dump.
7. Remove the rehearsal database after the receipt is complete, or retain it only for approved QA.
