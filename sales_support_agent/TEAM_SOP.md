# Sales Support Agent Team SOP

## Purpose

This app exists to make sure no ClickUp lead goes untouched after it is created.

It does not create leads.

It starts working after:

1. a lead is created upstream in ClickUp
2. the initial Slack notification has already been sent by the upstream workflow

From that point forward, this app is responsible for:

- tracking meaningful sales activity
- updating the existing ClickUp task safely
- setting or refreshing next follow-up timing
- alerting the assigned AE when a lead is untouched, due, or overdue
- preserving an audit trail of every automated action

## Recommended Architecture

### Source of truth

- ClickUp is the CRM source of truth
- Instantly is the upstream source for email conversation events
- Slack is the action/notification layer for the AE
- SQLite is the local audit and dedupe store for the app

### Preferred event flow

The recommended production flow is:

1. Instantly creates or updates lead engagement upstream
2. Instantly sends webhook events to this app
3. this app normalizes those events
4. this app updates the matching ClickUp task
5. this app runs stale-lead enforcement on a schedule
6. this app notifies the AE in Slack when follow-through is missing

### Why webhooks are preferred over polling

Instantly supports webhook delivery for events such as `email_sent`, `reply_received`, `lead_meeting_booked`, and `lead_meeting_completed`. Webhooks are the best fit because they are near real-time and reduce lag between a conversation change and the ClickUp update.

Polling Instantly conversations is possible through the email APIs, but it is a worse first implementation because it adds:

- higher API volume
- dedupe complexity
- more state tracking
- slower CRM updates

For phase 1, the best production path is:

- use Instantly webhooks for email and meeting events
- use the stale-lead scheduled job for follow-up enforcement

## Current App Capabilities

The app currently provides:

- read-only ClickUp schema discovery
- ClickUp list sync into a local mirror
- communication event ingest at `POST /api/communications/events`
- stale-lead scan at `POST /api/jobs/stale-leads/run`
- AE Slack reminders
- append-only ClickUp comments
- SQLite audit logging

## Native Instantly Webhook Endpoint

The app now includes a native Instantly webhook receiver at:

- `POST /api/integrations/instantly/webhook`

That endpoint:

- accepts raw Instantly webhook payloads
- validates a shared secret header when configured
- matches the lead to an existing ClickUp task by exact email
- normalizes the event into the internal communication event format
- updates ClickUp through the same audited communication service used by the manual API

## What Needs To Happen To Go Live

### 1. Environment and deployment

- choose the host for the app
- create production environment variables
- install dependencies
- deploy the FastAPI service
- expose a stable HTTPS URL for webhook delivery

Minimum production environment variables:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `SALES_AGENT_INTERNAL_API_KEY`
- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`
- `SLACK_AE_MAP_JSON`
- `SALES_AGENT_DB_URL`

Recommended:

- use Postgres in production instead of SQLite if you want durable multi-instance safety
- keep SQLite only for local/dev or single-instance low-risk rollout

### 2. ClickUp discovery and field mapping

- run ClickUp schema discovery
- inspect the generated snapshot
- confirm which existing custom fields should be managed by the agent
- set the field ID env vars only for fields that already exist

Field IDs to confirm if available:

- next follow-up date
- communication summary
- last meeting outcome
- recommended next action
- last meaningful touch
- last outbound
- last inbound

### 3. Slack assignee mapping

- map ClickUp assignee IDs to Slack user IDs in `SLACK_AE_MAP_JSON`
- test at least one AE mapping end-to-end

### 4. Instantly integration completion

- confirm your Instantly plan supports webhooks
- create an Instantly API key with the needed scopes
- configure Instantly webhooks for the required event types
- point those webhooks to the production app URL
- set the webhook secret header to match the app configuration

Recommended Instantly event types to ingest:

- `email_sent`
- `reply_received`
- `lead_meeting_booked`
- `lead_meeting_completed`
- `lead_interested`
- `lead_not_interested`
- `lead_neutral`

For phase 1, the most important are:

- `email_sent`
- `reply_received`
- `lead_meeting_completed`

### 5. Native Instantly webhook adapter

This is already implemented.

The operational work is now:

- set `INSTANTLY_WEBHOOK_SECRET`
- set `INSTANTLY_WEBHOOK_SECRET_HEADER`
- add the matching header in Instantly webhook configuration
- verify successful delivery in Instantly webhook activity monitoring

### 6. Lead/task matching logic

You need a reliable way to map an Instantly event back to the correct ClickUp task.

Recommended matching order:

1. exact email match
2. exact Instantly lead ID stored in a ClickUp field or local mapping table
3. fallback on company + contact name if email is missing

Strong recommendation:

- add or confirm one stable identifier for matching, ideally email

### 7. Scheduled stale-lead execution

Run the stale-lead job on a weekday schedule.

Recommended cadence:

- 8:00 AM local time
- 1:00 PM local time
- optional 4:30 PM local time

The schedule should trigger:

- `POST /api/jobs/stale-leads/run`

### 8. Production testing

Run a staged rollout with a small subset of leads or one AE first.

Test cases:

1. new ClickUp lead with no activity
2. outbound email sent from Instantly
3. inbound reply received in Instantly
4. meeting completed
5. offer sent
6. follow-up due reminder
7. overdue reminder
8. excluded status does not alert

### 9. Team onboarding

- train AEs on what Slack reminders mean
- define when an AE must take action
- define when a manager escalates overdue leads
- define who owns field mapping and webhook health

## SOP: Daily Operational Use

### For AEs

1. Watch the sales support Slack alerts.
2. When alerted, open the ClickUp task immediately.
3. Take the recommended next action.
4. Make sure the outcome is reflected in ClickUp.
5. If a lead moves meaningfully, update the status if appropriate.
6. If a lead is in `FOLLOW UP`, always ensure the next follow-up date is set.

### For Sales Ops

1. Review overdue alerts daily.
2. Spot check whether ClickUp fields are being updated correctly.
3. Monitor whether Instantly webhooks are arriving.
4. Review audit logs when there is uncertainty about an automated change.
5. Re-run discovery if ClickUp fields/statuses change.

### For Engineering

1. Monitor app uptime and webhook delivery health.
2. Review logs for failed ClickUp writes or Slack posts.
3. Monitor stale-lead job execution on schedule.
4. Keep Instantly and ClickUp credentials valid.
5. Review rate limits and failed retries.

## SOP: Event Handling Rules

### When Instantly reports `email_sent`

The app should:

- append an activity note to ClickUp
- update last outbound and last meaningful touch if fields exist
- compute the next follow-up date
- clear untouched/new-lead enforcement

### When Instantly reports `reply_received`

The app should:

- append a reply summary to ClickUp
- update last inbound and last meaningful touch if fields exist
- clear due/overdue state
- notify the AE in Slack

### When Instantly reports `lead_meeting_completed`

The app should:

- append meeting activity to ClickUp
- update last meaningful touch
- update meeting outcome if available
- set next follow-up timing
- notify Slack if notes are missing

### When a lead remains untouched after creation

The app should:

- wait through the grace window
- send an untouched reminder at 1 business day
- escalate to due at 2 business days
- escalate to overdue at 3 business days

### When a lead becomes stale after first touch

The app should enforce status-based timing:

- `CONTACTED COLD`: due at 2 business days, overdue at 3
- `CONTACTED WARM`: due at 1 business day, overdue at 2
- `WORKING QUALIFIED`: due at 2 business days, overdue at 3
- `WORKING NEEDS OFFER`: due at 1 business day, overdue at 2
- `WORKING OFFERED`: due at 4 business days, overdue at 5
- `WORKING NEGOTIATING`: due at 2 business days, overdue at 3
- `FOLLOW UP`: due on next follow-up date, overdue after 1 missed business day

## SOP: Rollout Plan

### Phase A: Safe setup

- deploy the app
- run discovery
- verify field mapping
- verify Slack mapping
- do not enable live reminders yet

### Phase B: Read-only confidence

- run sync
- run stale-lead job in dry-run mode
- inspect outputs
- validate which leads would have been alerted

### Phase C: Event ingestion

- connect Instantly webhook events
- validate one outbound and one reply flow
- verify ClickUp task updates

### Phase D: Controlled live rollout

- enable live reminders for one AE or one segment
- monitor for 3 to 5 business days
- tune thresholds or field mapping if needed

### Phase E: Full launch

- enable for the full active CRM list
- publish team SOP
- start manager review cadence

## Engineering Build Checklist

- deploy FastAPI app to a stable host
- install requirements from `requirements.txt`
- configure all production env vars
- run discovery and confirm field IDs
- decide SQLite vs Postgres for production persistence
- confirm exact email-based event-to-ClickUp task matching on real leads
- test Slack notifications
- configure scheduled stale-lead execution
- run dry-run validation
- launch to a pilot group
- launch fully

## Exact Next Steps

If the goal is to get this live as quickly as possible, do these next:

1. Install dependencies and deploy the current app.
2. Run ClickUp discovery and lock the field map.
3. Configure Slack assignee mapping.
4. Decide whether to use SQLite temporarily or Postgres immediately.
5. Configure Instantly webhooks to send `email_sent`, `reply_received`, and `lead_meeting_completed`.
6. Test end-to-end with one real lead.
7. Turn on the stale-lead scheduled job.
8. Pilot with one AE.
9. Roll out to the full team.

## References

- Instantly webhooks help article: https://help.instantly.ai/en/articles/6261906-how-to-use-webhooks
- Instantly webhook event guide: https://developer.instantly.ai/webhook-events
- Instantly webhook API docs: https://developer.instantly.ai/api/v2/webhook
- Instantly email API docs: https://developer.instantly.ai/api/v2/email/getemail
