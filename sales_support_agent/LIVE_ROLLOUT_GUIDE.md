# Sales Support Agent Live Rollout Guide

## Goal

Launch the sales support agent using the same stack pattern as the current lead builder:

- GitHub repo for source control
- Render web service for the FastAPI app
- Render Cron for scheduled stale-lead scans
- GitHub Actions as a backup/manual trigger path

This guide is written as a click-by-click walkthrough for someone setting it up in production.

## Important Note Before You Start

The native Instantly webhook endpoint is now implemented at:

- `POST /api/integrations/instantly/webhook`

The remaining work is production setup:

- deploy the app
- configure the webhook secret
- point Instantly to the production endpoint
- validate email-based ClickUp matching with real leads

## Part 1: Prepare the Repo

### Step 1: Confirm the code is in GitHub

Where to click:

1. Open GitHub in your browser.
2. Open the `Lead-scraper` repository.
3. Click the `Code` tab.
4. Confirm you can see the `sales_support_agent/` folder in the repo.

What you should see:

- [sales_support_agent/main.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/main.py)
- [sales_support_agent/README.md](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/README.md)
- [sales_support_agent/TEAM_SOP.md](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/TEAM_SOP.md)

If not yet pushed:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
git status
git add .
git commit -m "Add sales support agent"
git push origin main
```

## Part 2: Create the Render Web Service

This follows the same pattern as the current lead builder app.

Render web services are configured in the Dashboard under `+ New > Web Service`. Source: [Render Web Services docs](https://render.com/docs/web-services).

### Fastest option: use the Render Blueprint

This repo now includes a Render blueprint at [render.yaml](/Users/davidnarayan/Documents/Playground/Lead-scraper/render.yaml).

If you want the fastest setup:

1. In Render Dashboard, click `+ New`.
2. Click `Blueprint`.
3. Select the `Lead-scraper` repo.
4. Render will detect `render.yaml`.
5. Review the proposed `sales-support-agent` web service and `sales-support-stale-scan` cron job.
6. Fill in the unsynced secret values.
7. Click `Apply`.

If you prefer to configure everything manually, continue with the steps below.

### Step 2: Open Render and create a new service

Where to click:

1. Go to [Render Dashboard](https://dashboard.render.com/).
2. In the top-right, click `+ New`.
3. Click `Web Service`.
4. If prompted, click `GitHub`.
5. Authorize Render to access the GitHub account if it is not already connected.
6. In the repo list, find and click `Lead-scraper`.
7. Click `Connect`.

### Step 3: Fill out the Render service form

Recommended values:

- Name: `sales-support-agent`
- Region: same region as your current lead builder service
- Branch: `main`
- Runtime: `Python 3`
- Root Directory: leave blank unless your Render setup already uses a subdirectory
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn sales_support_agent.main:app --host 0.0.0.0 --port $PORT`

Where to click:

1. In the `Name` field, type `sales-support-agent`.
2. In `Region`, choose the same region as the lead builder.
3. In `Branch`, choose `main`.
4. In `Runtime`, choose `Python 3`.
5. In `Build Command`, paste:

```bash
pip install -r requirements.txt
```

6. In `Start Command`, paste:

```bash
uvicorn sales_support_agent.main:app --host 0.0.0.0 --port $PORT
```

### Step 4: Configure health checks and deploy behavior

Where to click:

1. Scroll to `Advanced`.
2. Find `Health Check Path`.
3. Enter:

```txt
/health
```

4. Leave auto-deploy enabled so Render redeploys when you push to `main`.

### Step 5: Add environment variables

Render environment variables are managed under `Service > Environment`. Source: [Render Environment Variables docs](https://render.com/docs/configure-environment-variables).

You can add them during creation or after the service is created.

Minimum production env vars:

```env
CLICKUP_API_TOKEN=...
CLICKUP_API_KEY=...
CLICKUP_LIST_ID=...
SALES_AGENT_INTERNAL_API_KEY=...
SLACK_BOT_TOKEN=...
SLACK_CHANNEL_ID=...
SLACK_AE_MAP_JSON={"clickup-user-id":"slack-user-id"}
CLICKUP_DISCOVERY_SAMPLE_SIZE=10
CLICKUP_DISCOVERY_SNAPSHOT_PATH=runtime/clickup_schema_snapshot.json
CLICKUP_USE_DUE_DATE_FOR_FOLLOW_UP=false
STALE_LEAD_SCAN_MAX_TASKS=50
STALE_LEAD_SCAN_SYNC_MAX_TASKS=100
```

Database choice:

- temporary/dev: `SALES_AGENT_DB_URL=sqlite:///runtime/sales_support_agent.sqlite3`
- recommended production: Postgres URL

Where to click:

1. In the service setup form, find `Environment Variables`.
2. Click `+ Add Environment Variable` for each one.
3. Paste the key and value.
4. Repeat until all required vars are added.

Bulk-add option:

1. After service creation, click the service.
2. In the left sidebar, click `Environment`.
3. Click `Add from .env`.
4. Paste a clean production env block.
5. Click `Save and deploy`.

### Step 6: Create the web service

Where to click:

1. Review the form.
2. Click `Create Web Service`.
3. Wait for the build and deploy logs to complete.

### Step 7: Confirm the app is live

Where to click:

1. Open the service page in Render.
2. Click the generated service URL or `Open App`.
3. Visit:

```txt
https://<your-service>.onrender.com/health
```

You should see a healthy JSON response.

## Part 3: Choose Database Persistence

Render services have ephemeral filesystems unless you use a persistent disk or a database. Sources: [Render Persistent Disks docs](https://render.com/docs/disks), [Render Postgres docs](https://render.com/docs/databases).

### Recommended production choice: Render Postgres

SQLite is fine for local development, but for production the safest path is Postgres.

### Step 8: Create a Render Postgres database

Where to click:

1. In Render Dashboard, click `+ New`.
2. Click `Postgres`.
3. In `Name`, enter `sales-support-agent-db`.
4. Choose the same `Region` as the web service.
5. Pick the smallest paid plan that fits your budget.
6. Click `Create Database`.

### Step 9: Connect the app to Postgres

Where to click:

1. Open the new database in Render.
2. In the top-right, click `Connect`.
3. Copy the `Internal Database URL`.
4. Open the `sales-support-agent` web service.
5. Click `Environment`.
6. Add or update:

```env
SALES_AGENT_DB_URL=<internal-postgres-url>
```

7. Click `Save and deploy`.

## Part 4: Configure ClickUp

### Step 10: Create or confirm the ClickUp API token

Where to click:

1. Log in to ClickUp.
2. Click your avatar in the bottom-left.
3. Click `Apps` or `Integrations` depending on the current UI.
4. Find `API Token`.
5. Generate or copy the personal token.

Use that value for:

```env
CLICKUP_API_TOKEN=...
```

### Step 11: Get the ClickUp List ID

You need the exact CRM list the app should monitor.

Where to click:

1. Open the CRM list in ClickUp.
2. Look in the browser URL for the list identifier, or use ClickUp developer tools/API if needed.
3. Copy the list ID.

Set:

```env
CLICKUP_LIST_ID=...
```

### Step 12: Run ClickUp schema discovery

This safely inspects the real task schema without writing anything.

Where to click:

1. Open your API client of choice:
   - Postman
   - Insomnia
   - curl in terminal
2. Send a `POST` request to:

```txt
https://<sales-support-agent-url>/api/discovery/clickup-schema
```

Headers:

```txt
Content-Type: application/json
X-Internal-Api-Key: <your internal api key>
```

Body:

```json
{
  "sample_size": 10
}
```

### Step 13: Review the discovery output

What to review:

- actual status names
- custom field IDs
- assignee values
- sample comments
- whether follow-up-related fields already exist

The snapshot is saved by the app to:

- `runtime/clickup_schema_snapshot.json`

Use that snapshot to decide which existing field IDs should be added to env vars:

```env
CLICKUP_NEXT_FOLLOW_UP_FIELD_ID=
CLICKUP_COMMUNICATION_SUMMARY_FIELD_ID=
CLICKUP_LAST_MEETING_OUTCOME_FIELD_ID=
CLICKUP_RECOMMENDED_NEXT_ACTION_FIELD_ID=
CLICKUP_LAST_MEANINGFUL_TOUCH_FIELD_ID=
CLICKUP_LAST_OUTBOUND_FIELD_ID=
CLICKUP_LAST_INBOUND_FIELD_ID=
```

### Step 14: Add the confirmed field IDs in Render

Where to click:

1. In Render, open the `sales-support-agent` service.
2. Click `Environment`.
3. Add the field ID variables.
4. Click `Save and deploy`.

## Part 5: Configure Slack

### Step 15: Confirm the existing Slack bot token and channel

If you already use Slack with the lead builder, you may be able to reuse the same bot token and channel.

Required:

```env
SLACK_BOT_TOKEN=...
SLACK_CHANNEL_ID=...
```

### Step 16: Build the AE mapping

This app needs to know which Slack user corresponds to each ClickUp assignee.

Example:

```env
SLACK_AE_MAP_JSON={"123456":"U0123456789","john@company.com":"U0987654321"}
```

Where to click to get Slack user IDs:

1. In Slack, click a user profile.
2. Click the `More` or `three-dot` menu.
3. Click `Copy member ID` if available.

Where to click to get ClickUp assignee IDs:

1. Use the ClickUp schema discovery output.
2. Review task `assignees`.
3. Match each AE to their Slack member ID.

### Step 17: Test a Slack notification

Run a dry stale-lead scan first, then live once comfortable.

Dry run:

```bash
curl -X POST "https://<sales-support-agent-url>/api/jobs/stale-leads/run" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: <internal-key>" \
  -d '{"dry_run": true}'
```

Live run:

```bash
curl -X POST "https://<sales-support-agent-url>/api/jobs/stale-leads/run" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: <internal-key>" \
  -d '{"dry_run": false, "max_tasks": 10}'
```

## Part 6: Configure Instantly

Instantly webhooks are set up under `Integrations > Webhooks`. Sources: [Instantly Help: Webhooks](https://help.instantly.ai/en/articles/6261906-how-to-use-webhooks), [Instantly webhook event schema](https://developer.instantly.ai/webhook-events).

Important:

- Instantly webhooks are available on Hyper Growth plan or above per their help center.

### Step 18: Confirm the webhook endpoint URL

Use this production destination:

```txt
https://<sales-support-agent-url>/api/integrations/instantly/webhook
```

### Step 19: Open Instantly webhooks

Where to click:

1. Log in to Instantly.
2. Open `Settings`.
3. Click `Integrations`.
4. Click `Webhooks`.
5. Click `Add Webhook`.

### Step 20: Add the webhook destination

Use:

```txt
https://<sales-support-agent-url>/api/integrations/instantly/webhook
```

Where to click:

1. In `Destination URL`, paste the webhook URL.
2. In `Headers`, add the shared secret header that matches your app env vars.

Example header:

```txt
X-Instantly-Webhook-Secret: <your webhook secret>
```

### Step 21: Select the Instantly events

Recommended phase 1 events:

- `email_sent`
- `reply_received`
- `lead_meeting_completed`

Good optional additions:

- `lead_meeting_booked`
- `lead_interested`
- `lead_not_interested`
- `lead_neutral`

Where to click:

1. In the event selector, choose the desired event types.
2. In the campaign selector, choose either:
   - the specific campaign(s) that feed your sales CRM
   - all relevant campaigns
3. Click `Add Webhook` or `Save`.

### Step 22: Verify webhook delivery

Instantly now has webhook activity monitoring for recent webhook deliveries.

Where to click:

1. In Instantly, return to `Webhooks`.
2. Open `Webhook Activity`.
3. Check for:
   - successful `200` responses
   - failed attempts
   - retries
   - payload previews

If failures appear, compare:

- destination URL
- headers
- app logs in Render

## Part 7: Sync and Validate the App

### Step 23: Run a ClickUp sync

Use:

```bash
curl -X POST "https://<sales-support-agent-url>/api/clickup/sync" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: <internal-key>" \
  -d '{"include_closed": true, "max_tasks": 100}'
```

Expected result:

- tasks synced into the local audit mirror
- no ClickUp task creation
- no CRM structure changes

### Step 24: Run a stale-lead dry run

Use:

```bash
curl -X POST "https://<sales-support-agent-url>/api/jobs/stale-leads/run" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: <internal-key>" \
  -d '{"dry_run": true}'
```

Review:

- how many tasks were inspected
- which leads would be alerted
- whether due/overdue counts look sensible

### Step 25: Test one event end-to-end

Best first test:

1. choose one real or test lead in ClickUp
2. trigger an email send in Instantly
3. confirm the event is received
4. confirm the ClickUp task gets:
   - append-only comment
   - last touch update if mapped
   - next follow-up date if mapped

Then test:

1. reply received in Instantly
2. app updates ClickUp
3. app posts Slack reply notification

## Part 8: Create Render Cron

The current lead builder already uses Render Cron as the primary scheduler. Source: [Render Cron Jobs docs](https://render.com/docs/cronjobs).

### Step 26: Create the stale-lead Render Cron

Where to click:

1. In Render Dashboard, click `+ New`.
2. Click `Cron Job`.
3. Select the same `Lead-scraper` GitHub repo.
4. Click `Connect`.

### Step 27: Configure the Cron Job

Recommended config:

- Name: `sales-support-stale-scan`
- Branch: `main`
- Runtime: same as the web service
- Build Command: `pip install -r requirements.txt`
- Schedule:
  - `0 15 * * MON-FRI` for 8:00 AM America/Denver during standard time
  - or adjust based on your desired UTC schedule
- Command:

```bash
curl -X POST "$SALES_SUPPORT_AGENT_URL/api/jobs/stale-leads/run" \
  -H "Content-Type: application/json" \
  -H "X-Internal-Api-Key: $SALES_AGENT_INTERNAL_API_KEY" \
  -d '{"dry_run": false}'
```

Important:

- Render cron schedules use UTC

### Step 28: Add cron environment variables

Where to click:

1. Open the cron job.
2. Click `Environment`.
3. Add:

```env
SALES_SUPPORT_AGENT_URL=https://<sales-support-agent-url>
SALES_AGENT_INTERNAL_API_KEY=...
```

4. Click `Save and deploy`.

### Step 29: Manually trigger a cron run

Where to click:

1. Open the cron job in Render.
2. Click `Trigger Run`.
3. Watch the logs.

Confirm:

- request hits the web service
- stale-lead job completes
- Slack alerts appear only when expected

## Part 9: Add GitHub Actions Backup

This mirrors the backup approach already used by the lead builder workflow in [`.github/workflows/daily-lead-build.yml`](/Users/davidnarayan/Documents/Playground/Lead-scraper/.github/workflows/daily-lead-build.yml).

### Step 30: Confirm the backup GitHub Actions workflow exists

The repo now includes:

- [`.github/workflows/sales-support-stale-scan.yml`](/Users/davidnarayan/Documents/Playground/Lead-scraper/.github/workflows/sales-support-stale-scan.yml)

### Step 31: Add the GitHub secret

Where to click:

1. In GitHub, open the repo.
2. Click `Settings`.
3. In the left sidebar, click `Secrets and variables`.
4. Click `Actions`.
5. Click `New repository secret`.

Add:

- `SALES_SUPPORT_AGENT_URL`
- `SALES_AGENT_INTERNAL_API_KEY`

### Step 32: Run the workflow manually

Where to click:

1. In GitHub, click `Actions`.
2. Click the stale-scan workflow.
3. Click `Run workflow`.
4. Choose the branch.
5. Click the green `Run workflow` button.

## Part 10: Launch Sequence

### Step 33: Pilot with one AE

Recommended rollout:

1. limit testing to one AE or one small lead segment
2. monitor Slack noise
3. verify ClickUp comments are useful
4. verify follow-up dates are correct

### Step 34: Review after 3 to 5 business days

Review:

- false positive reminders
- missed reminders
- incorrect task matching from Instantly
- missing field mappings
- AE feedback on Slack wording

### Step 35: Roll out to the full team

Once pilot looks good:

1. keep Render Cron live
2. keep GitHub Actions backup ready
3. share the SOP with the team
4. define who owns monitoring and fixes

## What Still Needs Engineering Work

Before this is truly production-ready, engineering should complete or validate:

1. production persistence decision:
   - SQLite with disk
   - or preferably Postgres
2. exact email-based Instantly event-to-ClickUp task matching on real data
3. optional webhook signature verification if available in your Instantly setup

## Recommended Final Production Stack

- GitHub repo: `Lead-scraper`
- Render web service: `sales-support-agent`
- Render Postgres: `sales-support-agent-db`
- Render Cron: `sales-support-stale-scan`
- Instantly Webhooks: push email/reply/meeting events into the app
- ClickUp: CRM source of truth
- Slack: AE notification layer

## References

- [Render Web Services](https://render.com/docs/web-services)
- [Render Environment Variables](https://render.com/docs/configure-environment-variables)
- [Render Cron Jobs](https://render.com/docs/cronjobs)
- [Render Postgres](https://render.com/docs/databases)
- [Render Persistent Disks](https://render.com/docs/disks)
- [Instantly Webhooks Help](https://help.instantly.ai/en/articles/6261906-how-to-use-webhooks)
- [Instantly Webhook Events](https://developer.instantly.ai/webhook-events)
- [Instantly Webhook API](https://developer.instantly.ai/api/v2/webhook)
- [Instantly Webhook Activity Monitoring](https://help.instantly.ai/en/articles/12299464-webhook-activity-monitoring)
