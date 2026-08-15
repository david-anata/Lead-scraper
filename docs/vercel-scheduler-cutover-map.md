# Render-to-Vercel scheduler cutover map

Status: complete inventory; every retained job has a Vercel replacement and remains globally disabled before cutover

Reviewed against the live Render dashboard on August 13, 2026. Render stays authoritative until cutover approval.

| Render job | Live Render cadence | Vercel replacement | Vercel cadence | Cutover disposition |
| --- | --- | --- | --- | --- |
| `daily-lead-build` (`crn-d6pnlrn5gffc73dnm5a0`) | Weekdays 05:00 UTC, date in Denver, maximum 150 domains | `/api/vercel-cron/daily-lead-build` | Weekdays 05:00 UTC | Retain. Same date, limit, lead-builder service, and external handoff behavior; disabled before cutover. |
| `sales-support-stale-scan` (`crn-d6qdnrfpm1nc73b13mf0`) | Weekdays 15:00 UTC | `/api/vercel-cron/stale-leads` | Weekdays 15:00 UTC | Retain one-for-one. |
| `sales-support-operator-review` (`crn-d928ivq8qa3s73d0igu0`) | Hourly at :05; also runs HR reminders | `/api/vercel-cron/sales-operator` and `/api/vercel-cron/hr-reminders` | Hourly at :05 | Retain as two independently receipted steps so one failure cannot hide the other. |
| `website-ops-scheduler` (`crn-d9jc35navr4c73c7qmrg`) | Hourly at :00 | `/api/vercel-cron/website-ops` | Hourly at :00 | Retain one-for-one. |
| `content-engine-scheduler` (`crn-d9kdnf3m8hqs73c8fmng`) | Hourly at :00 | `/api/vercel-cron/content` | Hourly at :00 | Retain one-for-one. |
| `building-operations-hourly` (`crn-d9qjt6m417fc73ekbcrg`) | Hourly at :20 | `/api/vercel-cron/building-operations` | Hourly at :20 | Retain all four independent Building steps under one leased Vercel receipt. |
| `agent-outbound-morning` (`crn-d9js933eo5us73efo0g0`) | 13:00 and 14:00 UTC with Denver 07:00 guard | `/api/jobs/outbound-morning/run` | 13:00 and 14:00 UTC | Retain one-for-one. |
| `sales-support-gmail-sync` (`crn-d6qolo7kijhs73ba0pe0`) | Live dashboard still says hourly on weekdays; current `render.yaml` declares every 15 minutes | `/api/vercel-cron/gmail-sync` | Every 15 minutes | Use the current repository-declared cadence. Retire the legacy Render cadence at cutover. |
| `sales-support-daily-digest` (`crn-d6qomc450q8c73bmetu0`) | Legacy 23:00 UTC weekday job with no successful runs recorded | `/api/vercel-cron/daily-digest` | Weekdays 16:00 UTC | Use the working-day Vercel digest and retire the unsuccessful legacy job. |
| `building-synthetic-journey` (`crn-d9pd5je417fc73dlfl1g`) | Daily 13:30 UTC isolated pytest journey | Hosted release gate plus `/api/vercel-cron/synthetic-health` | Every push plus hourly at :30 | Retire at cutover. It never touches live provider data; hosted regression covers the golden path and Vercel synthetic health covers runtime/database readiness. |

`amazon-sp-api-platform-cron`, `tape`, and `amazon-sp-api-platform-staging` belong to other Render projects and are outside the Agent migration. They must not be disabled during Agent cutover.

## Cutover order

1. Record the last successful Render receipt for each retained job.
2. Disable only the Render jobs listed above; leave unrelated projects untouched.
3. Confirm `VERCEL_CRON_WRITES_ENABLED=false` and
   `VERCEL_CRON_ENABLED_JOBS` is empty, apply the final database delta, and pass
   the migration comparator.
4. Set the global flag true while the allowlist remains empty. Add one exact
   Vercel job name to `VERCEL_CRON_ENABLED_JOBS`, deploy that environment
   revision, invoke the matching route once, and verify its ledger receipt
   before appending the next name. Never use `*` during cutover.
5. On any duplicate or failed external effect, disable Vercel writes before re-enabling the matching Render job.
