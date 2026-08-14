# Vercel durable execution inventory

Status: hosted queue recovery proof complete; schedule shadow receipts and cutover enablement remain open

## Safety boundary

Every FastAPI `BackgroundTasks` call first records a complete task intent in PostgreSQL with a unique idempotency key. The request may attempt the task immediately, but `/api/vercel-cron/durable-tasks` repairs queued, failed, or lease-expired work. A repository test fails if a future route adds an ephemeral background task directly.

Current durable task types:

| Task type | Origin | External effect | Retry protection |
| --- | --- | --- | --- |
| `plaid_item_sync` | Signed Plaid webhook | Read Plaid item/transactions and persist Finance evidence | Webhook/item/type/code idempotency key; leased queue |
| `marketing_analysis` | Public marketing/advertising intake and operator retry | Generate analysis and deliver configured handoffs | Intake/run idempotency key; leased queue |
| `marketing_store_unlock` | Public store unlock and operator retry | Deliver store result and configured handoffs | Intake/run idempotency key; leased queue |
| `marketing_build_shelf` | Marketing result recovery | Build a missing comparison shelf | Run/ASIN idempotency key; leased queue |
| `fulfillment_finish_unlock` | Public Fulfillment intake and operator retry | Finish rate sheet and handoffs | Run/correlation idempotency key; leased queue |
| `fulfillment_retry_handoffs` | Sales operator retry | Retry incomplete Fulfillment handoffs | Run/time-specific operator key; leased queue |
| `advertising_audit` | Advertising upload | Generate the audit package | Audit-run idempotency key; leased queue |

Queue evidence includes status, attempts, maximum attempts, availability time, lease owner/expiry, start/completion times, last error, and result JSON. Failed tasks use bounded exponential retry timing. A worker can finish only a task owned by its lease token.

## Scheduled jobs

All Vercel write schedules require `CRON_SECRET` and remain inert while `VERCEL_CRON_WRITES_ENABLED=false`.

| Schedule | Vercel route | Cadence | State before cutover |
| --- | --- | --- | --- |
| Website Ops | `/api/vercel-cron/website-ops` | Hourly | Disabled |
| Content | `/api/vercel-cron/content` | Hourly | Disabled |
| Sales stale leads | `/api/vercel-cron/stale-leads` | Weekdays | Disabled |
| Gmail ingestion | `/api/vercel-cron/gmail-sync` | Every 15 minutes | Disabled |
| Sales operator | `/api/vercel-cron/sales-operator` | Hourly | Disabled |
| HR reminders | `/api/vercel-cron/hr-reminders` | Hourly | Disabled |
| Durable task repair | `/api/vercel-cron/durable-tasks` | Every 5 minutes | Disabled |
| Daily digest | `/api/vercel-cron/daily-digest` | Weekdays | Disabled |
| Building operations | `/api/vercel-cron/building-operations` | Hourly | Disabled |
| Original daily lead build | `/api/vercel-cron/daily-lead-build` | Weekdays at 05:00 UTC | Disabled |
| Outbound morning | `/api/jobs/outbound-morning/run` | Daily UTC windows with local-time guard | Disabled |
| Synthetic health | `/api/vercel-cron/synthetic-health` | Hourly | Active read-only |

The synthetic route checks application readiness, performs a bounded database query, and reports durable-queue backlog without invoking any external provider or write path. It requires the cron bearer secret even though it is read-only.

`/api/vercel-cron/preflight` is a second authenticated read-only receipt. It proves application readiness, database access, durable-queue schema availability, the full write-schedule inventory, and—critically—that the global write flag is still disabled. It fails closed if writes are enabled and never invokes a scheduled service.

## Cutover proof still required

- Authenticated synthetic-health and scheduler preflight receipts are complete.
- Hosted durable recovery receipt is complete. Vercel's scheduler invoked the
  staging-only recovery probe on August 13, 2026 at 21:57 Denver time against
  deployment `dpl_BWbYGD3D5rS4BpBg6NgrQY72opUT` (`895432a`). Supabase retained
  task `7f1005b8b36644ee8c6f5818d5fe1174`: the first attempt failed intentionally,
  an overlapping claim was refused, attempt two succeeded, replay was refused,
  and `external_writes` remained false.
- The first hosted attempt exposed request-time queue DDL by the restricted
  application role. The runtime now skips all schema maintenance when
  `AGENT_RUNTIME_SCHEMA_MAINTENANCE=false`; controlled predeploy execution is
  the only DDL owner. The repeated hosted probe passed.
- Hosted schedule shadow receipts are complete. On August 13, 2026 at 22:02
  Denver time, Vercel invoked the staging-only shadow matrix against deployment
  `dpl_6rhS5AkPouVLY3XVtt6HCJA8qRGU` (`d86f2d1`). All 11 write schedules
  recorded `succeeded` receipts under correlation ID
  `vercel-shadow-20260813T220231360265`. Every required database table was
  readable, every declared live configuration group was present, and every
  receipt recorded `external_writes=false`. The temporary trigger was removed
  immediately after evidence capture.
- Immediately before cutover, verify Render schedules are stopped before changing `VERCEL_CRON_WRITES_ENABLED`.
- Enable one Vercel schedule at a time and confirm its first successful ledger receipt.
