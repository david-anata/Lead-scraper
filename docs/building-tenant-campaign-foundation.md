# Building tenant CRM and campaign preparation

This phase extends the existing Building CRM and campaign system. It does not
import contacts, change consent, schedule messages, or call an email provider.

## Reused foundations

- `BuildingContact`, `BuildingRelationship`, and `BuildingBillingAccount`
- marketing preferences, signed unsubscribe, and durable suppression
- inquiry-created prospect and event-host relationships
- reviewed tenant/community roster imports
- segments, campaigns, recipients, delivery feedback, HubSpot recovery, and
  Building audit events

Relationships may now reference a billing account. Operational-contact
authority records its source, evidence reference, and timestamp separately from
marketing consent. Changing either one never changes the other.
Legacy or partially migrated contacts without both an operational source and
evidence reference are excluded from operational audiences until staff records
that authority explicitly.

## Canonical audiences

`POST /api/internal/building/crm/segments/bootstrap` creates definitions only;
it imports zero contacts and is safe to replay.

| ID | Membership | Purpose |
| --- | --- | --- |
| `current-tenants` | active tenants and governed tenant employees | both |
| `former-tenants` | former tenants | marketing |
| `workspace-prospects` | workspace/tour inquiries | marketing |
| `event-prospects-hosts` | event inquiries and event hosts | marketing |

Manual approved lists require explicit contact IDs and approval evidence.
Audience preview remains deterministic, explains every inclusion/exclusion, and
reports truthful empty counts.

## Preparation lifecycle

`draft -> previewed -> reviewed -> approved (schedule-ready)`

1. Draft content stores its content version, template reference,
   classification, and checksum.
2. Preview resolves and hashes the current eligible audience.
3. Review requires `REVIEW CAMPAIGN {id}` and makes no provider call.
4. Approval requires `APPROVE CAMPAIGN {id}`. It freezes deduplicated recipient,
   content, and permission/suppression evidence in the existing recipient
   outbox. Approval means schedule-ready, not scheduled or sent.

Exact draft, review, and approval replays are idempotent. Audience/content
changes invalidate the prior preview.

`tenant_private` content is allowed only for an operational campaign aimed at
active tenants/tenant employees with explicit approval evidence. It is rejected
for marketing and is never included in public/default marketing content.

## Routes and permissions

- `building.crm.manage`: contact, relationship, account, consent, and
  suppression maintenance
- `building.campaigns.prepare`: draft, preview, and review
- `building.campaigns.approve`: freeze the schedule-ready recipient snapshot

Legacy `building.manage` operators retain compatibility. All transitions write
Building audit evidence.

Key internal routes:

- `PUT /api/internal/building/crm/contacts/{id}/operational-preference`
- `PUT /api/internal/building/crm/segments/{id}`
- `POST /api/internal/building/crm/segments/bootstrap`
- `GET /api/internal/building/crm/segments/{id}/preview`
- `PUT /api/internal/building/crm/campaigns/{id}`
- `POST /api/internal/building/crm/campaigns/{id}/preview`
- `POST /api/internal/building/crm/campaigns/{id}/review`
- `POST /api/internal/building/crm/campaigns/{id}/approve`

The authenticated Building Control page exposes the draft, preview, provider-
free review, and schedule-ready approval controls. Existing send/schedule paths
remain separate and are not part of this rollout.

## Rollout

1. Deploy additive columns.
2. Grant the narrow CRM/preparer/approver permissions.
3. Bootstrap canonical audience definitions.
4. Review existing relationship and consent evidence; do not infer consent.
5. Configure and verify a sender/provider separately before enabling schedule or
   send operations.

Open decisions: production email provider, authenticated sending domain/from
address, reply-to/owner routing, legal footer/company address, consent wording
and retention policy, approval separation-of-duties, send-window/timezone
policy, bounce/complaint thresholds, and whether HubSpot remains a projection or
becomes an upstream source for selected fields.
