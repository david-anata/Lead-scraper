# Sales Support Agent

This FastAPI app handles post-creation sales support inside your existing ClickUp CRM workflow. It does not create new leads in the normal flow. It starts after the ClickUp task already exists and focuses on follow-through, stale-lead prevention, append-only activity logging, owner-directed reminders, daily digests, and mailbox-based signal intake.

## What Phase 1 Includes

- Read-only ClickUp schema discovery against an existing CRM list
- Local mirror of ClickUp lead tasks for auditability and rule evaluation
- Manual communication event ingest for outbound, inbound, call, meeting, offer, and note events
- Native Instantly webhook ingest for email, reply, and meeting events
- Gmail mailbox polling for reply and lead-source signal intake
- Amazon-first sales deck generation from one target Amazon ASIN/URL plus Helium 10 Xray exports
- First-party HTML deck export with shared Anata branding, stable URLs, and basic deck view tracking
- Status-aware stale-lead scanning for active `new`, `contacted`, and `working` statuses
- Concise Slack alerts for high-signal events only
- Daily SDR email digest with grouped action items and draft replies
- SQLite-backed audit logs for every automation run and external write

## Folder Structure

```text
sales_support_agent/
  api/
  integrations/
  jobs/
  models/
  rules/
  services/
  config.py
  main.py
tests/
```

## Environment Variables

Required for ClickUp-backed execution:

- `CLICKUP_API_TOKEN`
- `CLICKUP_API_KEY` is also accepted as an alias
- `CLICKUP_LIST_ID`

Recommended for Slack alerts:

- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`
- `SLACK_AE_MAP_JSON`

Operational:

- `SALES_AGENT_INTERNAL_API_KEY`
- `SALES_AGENT_DB_URL`
- `CLICKUP_DISCOVERY_SAMPLE_SIZE`
- `CLICKUP_USE_DUE_DATE_FOR_FOLLOW_UP`
- `CLICKUP_DISCOVERY_SNAPSHOT_PATH`
- `STALE_LEAD_SCAN_MAX_TASKS`
- `STALE_LEAD_SCAN_SYNC_MAX_TASKS`
- `STALE_LEAD_SLACK_DIGEST_ENABLED`
- `STALE_LEAD_SLACK_DIGEST_MENTION_CHANNEL`
- `STALE_LEAD_SLACK_DIGEST_MAX_ITEMS`
- `STALE_LEAD_IMMEDIATE_ALERT_URGENCIES`
- `SLACK_IMMEDIATE_EVENT_TYPES`
- `DAILY_DIGEST_ENABLED`
- `DAILY_DIGEST_EMAIL_TO`
- `DAILY_DIGEST_EMAIL_CC`
- `DAILY_DIGEST_SUBJECT_PREFIX`
- `DAILY_DIGEST_MAX_ITEMS`
- `GMAIL_API_BASE_URL`
- `GMAIL_OAUTH_TOKEN_URL`
- `GMAIL_ACCESS_TOKEN`
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`
- `GMAIL_USER_ID`
- `GMAIL_POLL_QUERY`
- `GMAIL_POLL_MAX_MESSAGES`
- `GMAIL_SOURCE_DOMAINS`
- `INSTANTLY_WEBHOOK_SECRET`
- `INSTANTLY_WEBHOOK_SECRET_HEADER`

Deck generator:

- `SHARED_BRAND_PACKAGE_PATH`
- `DECK_PUBLIC_BASE_URL`

Amazon-first deck intake:

- one target product input
  - Amazon ASIN, or
  - Amazon product URL
- one Helium 10 Xray competitor CSV
- one optional Helium 10 Xray keyword CSV
- offering toggles
  - `amazon`
  - `shopify`
  - `tiktok_shop`

Optional deck generator tuning:

- `AMAZON_PROFIT_API_BASE_URL`
- `DECK_COMPETITOR_REQUIRED_COLUMNS`
- `DECK_COMPETITOR_ALLOWED_COLUMNS`
- `DECK_REQUIRED_TEMPLATE_FIELDS`
- `SHOPIFY_REQUEST_TIMEOUT_SECONDS`
- `SHOPIFY_USER_AGENT`
- `AMAZON_SP_API_BASE_URL`
- `AMAZON_SP_API_REGION`
- `AMAZON_SP_API_MARKETPLACE_ID`
- `AMAZON_SP_API_LWA_CLIENT_ID`
- `AMAZON_SP_API_LWA_CLIENT_SECRET`
- `AMAZON_SP_API_REFRESH_TOKEN`

Legacy / currently unused:

- `AMAZON_SP_API_AWS_ACCESS_KEY_ID`
- `AMAZON_SP_API_AWS_SECRET_ACCESS_KEY`
- `AMAZON_SP_API_AWS_SESSION_TOKEN`

Website ops:

- `WEBSITE_OPS_ROOT`
- `WEBSITE_OPS_URLS`
- `WEBSITE_OPS_EXECUTE_APPROVED`
- `WP_SITE_URL`
- `WP_USERNAME`
- `WP_APPLICATION_PASSWORD`

The agent admin now includes an internal website-ops section at:

- `/admin/website-ops`
- `/admin/website-ops/queue`
- `/admin/website-ops/reports`

This surface lets the team review SEO reports, submit page issues, approve safe actions, and optionally execute deterministic WordPress changes directly from the agent dashboard when `WEBSITE_OPS_EXECUTE_APPROVED=true`.

Optional existing-field overrides:

- `CLICKUP_NEXT_FOLLOW_UP_FIELD_ID`
- `CLICKUP_COMMUNICATION_SUMMARY_FIELD_ID`
- `CLICKUP_LAST_MEETING_OUTCOME_FIELD_ID`
- `CLICKUP_RECOMMENDED_NEXT_ACTION_FIELD_ID`
- `CLICKUP_LAST_MEANINGFUL_TOUCH_FIELD_ID`
- `CLICKUP_LAST_OUTBOUND_FIELD_ID`
- `CLICKUP_LAST_INBOUND_FIELD_ID`

## Local Setup

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn sales_support_agent.main:app --host 0.0.0.0 --port 8010 --reload
```

## Suggested Startup Order

1. Run schema discovery to capture the real ClickUp field layout.
2. Review `runtime/clickup_schema_snapshot.json`.
3. Set any explicit field IDs needed in `.env`.
4. Run a dry sync.
5. Run a dry stale-lead scan.
6. Turn on Slack alerts and scheduled execution.

## API Endpoints

- `GET /`
- `GET /health`
- `POST /api/discovery/clickup-schema`
- `POST /api/clickup/sync`
- `POST /api/jobs/stale-leads/run`
- `POST /api/jobs/gmail-sync/run`
- `POST /api/jobs/daily-digest/run`
- `POST /api/communications/events`
- `POST /api/integrations/instantly/webhook`
- `POST /admin/api/generate-deck`
- `GET /admin/api/deck-runs`
- `GET /decks/{deck_slug}/{run_id}/{token}`
- `GET /deck-exports/{run_id}/{token}`
- `GET /api/public/building/offerings`
- `GET /api/public/building/availability`
- `POST /api/public/building/inquiries`
- `GET /admin/building`
- `GET /api/internal/building/bookings`
- `POST /api/internal/building/bookings`
- `PUT /api/internal/building/billing/accounts/{account_id}`
- `PUT /api/internal/building/billing/schedules/{schedule_id}`
- `POST /api/internal/building/billing/schedules/{schedule_id}/approve`
- `POST /api/internal/building/billing/invoices`
- `POST /api/integrations/stripe/webhook`

Protected POST routes accept `X-Internal-Api-Key` when `SALES_AGENT_INTERNAL_API_KEY` is configured.

## Anata Building Operations

`/admin/building` is the internal Building Control Room. It brings together:

- sellable spaces, public offerings, and conservative availability;
- workspace, tour, and event inquiries;
- contacts with multiple relationships such as tenant, prospect, event host,
  and community member;
- marketing permission and suppression state;
- explainable audience segments;
- campaign draft, preview, test-send, approval, recipient snapshot, delivery,
  and unsubscribe state.
- workspace and event workflows with expiring holds, conflict checks, agreement
  evidence, deposit evidence, confirmation gates, and inventory release.
- native billing accounts and approved schedules, preview-first Stripe invoice
  creation, provider-confirmed payment evidence, and an explicit QBO accounting
  handoff state.

The public building website uses `BUILDING_SITE_INTAKE_KEY`, a dedicated
server-to-server secret. Campaign delivery additionally requires
`BUILDING_CAMPAIGN_TOKEN_SECRET` so unsubscribe links can be signed and verified.
Marketing messages only include currently subscribed, unsuppressed recipients.
Transactional tenant and booking messages remain a separate communication
class and are not disabled by a marketing unsubscribe.

An inquiry is not a booking. Event and workspace reservations begin in
`inquiry` and can move only through their approved state transitions. A
confirmed reservation requires signed-agreement evidence and, when configured,
verified deposit evidence. Cancelling, expiring, or completing the workflow
releases the linked availability block while retaining the audit history.

Billing schedules begin as editable drafts and become immutable after approval.
Invoice creation defaults to a no-write preview and requires an explicit
`execute: true` request plus an idempotency key. Stripe writes fail closed unless
`STRIPE_SECRET_KEY` is configured. Webhooks require
`STRIPE_WEBHOOK_SECRET`, reject stale or invalid signatures, and deduplicate
provider events. A Stripe-paid invoice is provider-confirmed evidence; it is not
described as bank-posted cash. Each invoice remains `pending_qbo` until the
accounting bridge records the QBO result.

## Example Requests

Discovery:

```bash
curl -X POST http://127.0.0.1:8010/api/discovery/clickup-schema \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{"sample_size": 5}'
```

Stale-lead dry run:

```bash
curl -X POST http://127.0.0.1:8010/api/jobs/stale-leads/run \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{"dry_run": true}'
```

Gmail mailbox sync:

```bash
curl -X POST http://127.0.0.1:8010/api/jobs/gmail-sync/run \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{"dry_run": true, "max_messages": 10}'
```

Daily digest:

```bash
curl -X POST http://127.0.0.1:8010/api/jobs/daily-digest/run \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{"include_stale": true, "include_mailbox": true}'
```

Communication event:

```bash
curl -X POST http://127.0.0.1:8010/api/communications/events \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{
    "task_id": "abc123",
    "event_type": "outbound_email_sent",
    "summary": "Sent first follow-up email after initial interest.",
    "recommended_next_action": "Check for reply tomorrow."
  }'
```

Instantly webhook:

```bash
curl -X POST http://127.0.0.1:8010/api/integrations/instantly/webhook \
  -H "Content-Type: application/json" \
  -H "X-Instantly-Webhook-Secret: $INSTANTLY_WEBHOOK_SECRET" \
  -d '{
    "event_type": "reply_received",
    "timestamp": "2026-03-13T16:00:00Z",
    "lead_email": "owner@example.com",
    "reply_text": "Interested. Can we speak next week?"
  }'
```

Deck generation:

```bash
curl -X POST http://127.0.0.1:8010/api/admin/generate-deck \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -F "target_product_input=https://www.amazon.com/dp/B0ABC12345" \
  -F "channels=amazon" \
  -F "channels=shopify" \
  -F "competitor_xray_csv=@/path/to/Helium_10_Xray.csv" \
  -F "keyword_xray_csv=@/path/to/Xray_Keyword.csv"
```

## Deck Output Notes

- The current deck workflow is Amazon-first.
- Canva and Google Sheets are no longer required for deck generation.
- Generated decks are fixed-layout HTML exports that reuse the shared Anata brand package.
- Public deck routes use the named slug format:
  - `GET /decks/{deck_slug}/{run_id}/{token}`
- Deck run metadata is stored in `automation_runs.summary_json`, including:
  - output type
  - stable deck URL
  - selected channels
  - total views
  - first viewed at
  - last viewed at

## System Diagram

```mermaid
flowchart LR
    ClickUp["ClickUp CRM List"] --> Sync["ClickUp Sync Service"]
    Sync --> Mirror["SQLite Mirror + Audit DB"]
    Instantly["Instantly Webhooks"] --> Updates["Activity Update Service"]
    Gmail["Gmail Mailbox Polling"] --> Updates
    Events["Manual Communication Event API"] --> Updates
    Updates --> ClickUp
    Updates --> Mirror
    Mirror --> Rules["Status + Meaningful-Touch Rules"]
    Rules --> Job["Stale Lead Job"]
    Job --> Slack["Urgent Slack Alerts"]
    Job --> Digest["Daily Email Digest"]
    Job --> ClickUp
```

## Active Status Logic

- Active enforcement: `NEW LEAD`, `CONTACTED COLD`, `CONTACTED WARM`, `WORKING QUALIFIED`, `WORKING NEEDS OFFER`, `WORKING OFFERED`, `WORKING NEGOTIATING`
- Excluded from enforcement: `WON - ACTIVE`, `LOST`, `LOST - NOT QUALIFIED`, `WON - CANCELED`

## Notes

- ClickUp remains the source of truth.
- The local database exists only for audit logs, dedupe, and automation memory.
- Phase 1 uses Monday-Friday business-day logic and does not implement holiday calendars.
- Instantly can push conversation events directly into the native webhook endpoint.
- Gmail polling is safe-triage-first: unmatched emails are surfaced in the daily digest and are not auto-created as leads.

## Team SOP

See the implementation and rollout playbook in [`sales_support_agent/TEAM_SOP.md`](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/TEAM_SOP.md).

For a click-by-click production launch guide using the same stack pattern as the lead builder app, see [`sales_support_agent/LIVE_ROLLOUT_GUIDE.md`](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/LIVE_ROLLOUT_GUIDE.md).

For the Amazon-first deck workflow and shared brand package usage, see [`sales_support_agent/docs/amazon_first_sales_deck.md`](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/docs/amazon_first_sales_deck.md).
# Anata HR and payroll control room

The `/admin/hr` section is a right-sized people and payroll operating system for
Anata's Utah team. It is intentionally provider-independent:

- Agent records employment setup, secure onboarding, W-4 elections, I-9 review,
  policy acknowledgements, exact time, corrections, PTO, and paid holidays.
- Payroll is semimonthly: the 1st–15th is paid on the 20th; the 16th–month end
  is paid on the following 5th. Saturday pay dates move to Friday and Sunday pay
  dates move to Monday. The overtime week is Sunday–Saturday.
- The calculation engine uses effective-dated 2026 IRS Publication 15/15-T and
  Utah Publication 14 rules. A qualified payroll/tax reviewer must confirm the
  setup before the application will prepare payroll.
- Preparation creates an immutable calculation version. Another authorized
  person must type the approval statement. Preparation and approval never move
  money or represent taxes as paid.
- Manual checks create employee-only pay statements. Tax liabilities remain
  due until payment and filing confirmations are recorded and reconciled.
- Wise contractor payments are prepared, approved, and reconciled separately
  from W-2 payroll. The current implementation records Wise evidence but does
  not call the Wise API or initiate transfers.
- Finance/Plaid is not part of this implementation. No Finance records are
  created or changed by HR.

There is currently no external payroll provider integration. Plaid is not a
payroll provider. A future provider adapter can receive approved payroll
snapshots, but it must not bypass the existing readiness and human-approval
controls.

Set `HR_PII_SECRET` to a long, production-only secret before collecting W-4
information. Without it, W-4 storage fails closed. Existing databases receive
additive HR tables and columns at startup.

`HR_PAYROLL_ADMIN_EMAILS` controls the recipients of the privacy-safe HR action
digest (David and Val by default). The existing operator cron invokes
`POST /api/jobs/hr-reminders/run`; one digest per recipient per day is sent only
when onboarding, time, contractor-document, I-9-expiration, or payroll-liability
items need review. The email contains no compensation, SSN, tax-election, or
pay-statement details.

Before the first live payroll, complete `/admin/hr/settings`:

1. enter reviewed 2026 opening balances for every W-2 employee;
2. enter the Utah unemployment rate from the employer notice;
3. verify EFTPS, Utah TAP, and Utah unemployment portal access;
4. have a qualified reviewer confirm the 2026 calculations;
5. complete each employee's employment setup and W-4;
6. resolve all open punches, time corrections, and payroll inputs.
