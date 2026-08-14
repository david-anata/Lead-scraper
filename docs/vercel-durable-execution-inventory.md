# Vercel durable execution inventory

Status: implementation complete; staging shadow receipts and cutover enablement remain open

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
| Outbound morning | `/api/jobs/outbound-morning/run` | Daily UTC windows with local-time guard | Disabled |
| Synthetic health | `/api/vercel-cron/synthetic-health` | Hourly | Active read-only |

The synthetic route checks application readiness, performs a bounded database query, and reports durable-queue backlog without invoking any external provider or write path. It requires the cron bearer secret even though it is read-only.

`/api/vercel-cron/preflight` is a second authenticated read-only receipt. It proves application readiness, database access, durable-queue schema availability, the full write-schedule inventory, and—critically—that the global write flag is still disabled. It fails closed if writes are enabled and never invokes a scheduled service.

## Cutover proof still required

- Capture one authenticated read-only synthetic receipt from Vercel.
- Invoke each write schedule in shadow/dry-run mode where supported and record its audit receipt.
- Force one isolated durable-task failure, verify retry state, then recover it without duplicating the effect.
- Immediately before cutover, verify Render schedules are stopped before changing `VERCEL_CRON_WRITES_ENABLED`.
- Enable one Vercel schedule at a time and confirm its first successful ledger receipt.
