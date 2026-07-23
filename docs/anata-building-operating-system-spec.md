# Anata Building Operating System

Status: Product specification  
Prepared: 2026-07-23  
Primary application: `agent.anatainc.com`  
Public storefront: `anata-building.vercel.app`

## Plain-English Summary

Anata Building needs one connected system for marketing spaces, receiving leads,
tracking availability, booking events, managing tenants, issuing contracts,
collecting money, and running the daily building operation.

The public website becomes the storefront. It explains what Anata offers,
shows trustworthy availability and starting prices, and starts the correct
workspace, tour, or event journey.

Agent becomes the operating system. It owns the building inventory, holds,
reservations, occupancy, contracts, billing schedules, operational tasks, and
audit history. HubSpot remains the sales/contact CRM during the initial
release. Stripe is the recommended payment rail. Google Calendar is a schedule
projection, not the booking source of truth. QuickBooks remains connected
during a controlled accounting transition rather than being removed in one
step.

The system must also support a tenant CRM and email program. Staff can create
audiences such as current tenants, event hosts, office prospects, former
tenants, and conference-room customers; send appropriate campaigns; and keep
marketing preferences separate from required operational communication.

## Verified Current State

### Building website

- The Next.js website has tour, workspace, and event inquiry forms.
- Inquiries are sent to a configured webhook or fall back to email.
- Forms collect contact details and a small amount of qualification data.
- The site does not currently own inventory, real availability, pricing,
  reservations, contracts, invoices, or payments.

### Agent

- Agent already provides sales pipelines, contact/deal workflows, follow-up,
  Gmail, Resend, HubSpot integration, quote references, audit concepts, and
  finance workflows.
- Agent can create and update HubSpot contacts, companies, deals, notes, and
  tasks.
- Agent has QuickBooks and bank-data foundations and distinguishes expected,
  confirmed, and posted money.
- The existing public marketing intake pattern uses a server-to-server shared
  secret. The browser does not receive the integration secret.
- Agent does not yet have a verified building-space, tenant, lease, event
  reservation, contract-signature, or native invoice-collection domain.

### External lead and scheduling sources

- The building receives leads from Facebook Marketplace.
- The event lead source is believed to be Eventective and must be confirmed.
- Eventective may contain its own availability, messaging, contract, and
  payment state.
- Google Calendar can represent busy times but must not independently decide
  whether a commercial booking is confirmed.

## Problem

The current tools represent pieces of the customer journey but do not share one
authoritative operational record.

This creates predictable failure modes:

- a space can appear available after it has been held or contracted;
- a lead can remain trapped in email, Messenger, or an external venue inbox;
- staff can mistake an inquiry for a reservation;
- event and workspace workflows can be mixed together;
- pricing, images, features, and occupancy can disagree across channels;
- contracts, deposits, invoices, and bank evidence can require duplicate entry;
- tenant communications can rely on manually maintained email lists;
- reporting cannot reliably connect a marketing source to a signed agreement
  and collected revenue.

## Product Goal

Create one operating workflow from first impression to completed relationship:

```text
Marketing source
  -> Website or external inquiry
  -> Contact and opportunity
  -> Tour or availability check
  -> Quote and temporary hold
  -> Contract and required payment
  -> Occupancy or confirmed event
  -> Service, communication, renewal, or closeout
```

An operator should be able to answer these questions without checking several
systems:

1. What can we sell today?
2. Who needs a response?
3. What tours and events are coming up?
4. Which holds, proposals, or contracts are expiring?
5. What money is due, late, paid, or still unverified?
6. Who currently belongs to the building community?
7. Which audience should receive this announcement or campaign?
8. What is the next action that protects revenue or customer experience?

## Users and Permissions

### Public prospect

- Browses spaces, offerings, features, photographs, videos, and pricing.
- Checks or requests availability.
- Requests a tour or begins an event inquiry.
- Reviews a proposal, signs an agreement, and pays through a secure link.

### Tenant or event customer

- Receives transactional notices, invoices, receipts, and required updates.
- Manages contact and communication preferences.
- In a later phase, views agreements, balances, reservations, and requests.

### Sales and community operator

- Qualifies leads, schedules tours, manages opportunities, and communicates
  with tenants.
- Creates approved holds, proposals, and campaign audiences.
- Cannot silently alter posted payment evidence or restricted contract terms.

### Event operator

- Reviews event requirements and conflicts.
- Manages holds, packages, contracts, deposits, checklists, and closeout.

### Finance operator

- Manages billing schedules, invoices, refunds, collections, reconciliation,
  and accounting exports.

### Administrator

- Manages spaces, rate plans, permissions, integrations, templates, and
  consequential overrides.

## System Ownership

| Domain | Initial source of truth | Notes |
| --- | --- | --- |
| Public content and presentation | Building website | Reads structured offering and availability data from Agent. |
| Contacts, companies, sales activity | HubSpot | Agent provides the operator experience and stores stable external IDs. |
| Space inventory and availability | Agent | Never owned independently by the website or calendar. |
| Holds, bookings, occupancy, tenant lifecycle | Agent | Audited state transitions. |
| Contracts | Agent plus selected signature provider | Agent owns status and document linkage; provider owns signing evidence. |
| Invoice and collection workflow | Agent | Agent owns why and when money is due. |
| Payment credentials and processing | Stripe | Agent stores provider IDs and status, never raw payment credentials. |
| Posted bank evidence | Plaid or approved bank source | Pending and posted remain distinct. |
| Formal accounting during transition | QuickBooks | Removed only after accounting readiness is proven. |
| Calendar projection | Google Calendar | Derived from confirmed Agent records and approved holds. |
| Marketing audience rules and suppression | Agent | Delivery may use HubSpot or a campaign provider adapter. |
| Transactional email | Agent via Resend/provider | Kept separate from optional marketing campaigns. |

## Core Data Model

### Property and space

- property;
- space;
- space type;
- capacity by layout;
- location/floor;
- public and internal descriptions;
- features and accessibility attributes;
- media assets with approved placement and alt text;
- active, unavailable, maintenance, or retired status.

### Offering and rate plan

- workspace, meeting, event, storage, parking, or add-on;
- booking unit: hour, day, month, term, or custom;
- public starting price or range;
- internal price and discount rules;
- minimum duration or term;
- deposit and cancellation rules;
- included items and optional add-ons;
- approval requirement;
- effective dates.

### Availability block

- space;
- start and end;
- state;
- source;
- linked opportunity, hold, reservation, occupancy, or maintenance item;
- expiration for temporary holds;
- override actor and reason.

Availability states:

```text
Available
  -> Soft hold
  -> Contract pending
  -> Booked or Occupied
  -> Turnover
  -> Available

Any state -> Maintenance or Unavailable -> prior approved state
```

### CRM identity

- person/contact;
- company;
- normalized email and phone;
- HubSpot contact/company IDs;
- relationship types such as prospect, tenant, event host, vendor, former
  tenant, and community member;
- acquisition source and source detail;
- communication preferences and suppression state;
- duplicate and merge history.

One person may have several relationships and opportunities. A tenant who later
books an event must not become a duplicate contact.

### Opportunity

- workspace or event type;
- contact and company;
- source and campaign;
- requested space or offering;
- requirements;
- assigned owner;
- stage;
- expected value and probability;
- next action and due date;
- loss or disqualification reason.

### Agreement and occupancy

- proposal/quote;
- contract template and version;
- signature status and evidence;
- tenant, event host, or responsible company;
- effective dates;
- linked spaces and offerings;
- deposit and billing schedule;
- amendments, renewal, cancellation, and closeout.

### Billing

- customer account;
- charge schedule;
- invoice;
- invoice line;
- deposit;
- payment;
- refund;
- credit;
- balance;
- collection state;
- Stripe and QuickBooks references;
- bank evidence and reconciliation state.

### Campaign and communication

- audience definition;
- immutable recipient snapshot for each send;
- campaign;
- message/template version;
- sender identity;
- send schedule and status;
- delivery, bounce, complaint, open, and click events where available;
- unsubscribe/suppression event;
- transactional communication history.

### Operations

- tour;
- event checklist;
- move-in/move-out checklist;
- maintenance or service request;
- assigned task;
- attachment;
- activity and audit event.

## Target Workflows

### Workspace sales

```text
New inquiry
  -> Qualified
  -> Tour scheduled
  -> Tour completed
  -> Proposal sent
  -> Contract pending
  -> Deposit or first payment due
  -> Won / Move-in scheduled
  -> Active tenant
  -> Renewal, expansion, transfer, or move-out
```

Required behavior:

- A tour is not a hold unless an operator explicitly creates one.
- A proposal may create an expiring soft hold when policy permits.
- A space becomes occupied only from an effective signed agreement or audited
  override.
- Losing or cancelling an opportunity releases its hold.

### Event booking

```text
Inquiry
  -> Requirements review
  -> Availability confirmed
  -> Expiring soft hold
  -> Quote sent
  -> Contract pending
  -> Deposit due
  -> Confirmed
  -> Pre-event readiness
  -> Event completed
  -> Final balance and closeout
```

Required behavior:

- Date, time, setup/teardown, attendance, event type, and key requirements are
  collected before confirmation.
- Conflicts are checked against Agent availability.
- Confirmation requires the configured combination of approval, signed
  agreement, and cleared or accepted deposit.
- Calendar events are created or updated from Agent state.
- Cancellation and refund policies are applied from the signed rate plan and
  agreement version.

### External lead intake

All sources normalize into the same intake contract:

- source;
- source reference;
- received timestamp;
- contact/company details;
- requested offering, dates, capacity, budget, and notes;
- original message or secure source link;
- consent evidence where provided.

Processing:

1. Validate and normalize.
2. Deduplicate the contact/company.
3. Create or update the HubSpot record.
4. Create an Agent opportunity.
5. Assign an owner and response deadline.
6. Preserve the original source.
7. Record success, partial success, or failure for retry.

Website intake uses the existing server-to-server secret pattern. Marketplace
and Eventective begin with supported email parsing or assisted manual intake
until a reliable official integration is verified.

### Tenant CRM and campaigns

Default audience segments:

- current tenants;
- primary billing contacts;
- tenant employees/community members;
- prospects by offering and stage;
- upcoming event hosts;
- past event hosts;
- former tenants;
- waitlist members;
- vendors and partners;
- opted-in building community.

Campaign examples:

- building announcements;
- community events;
- conference-room and event-space promotions;
- available office announcements;
- tenant spotlights;
- renewal and expansion campaigns;
- post-event follow-up;
- re-engagement for lost or former prospects.

Rules:

- Required operational messages and optional marketing messages are separate
  communication classes.
- Unsubscribing from marketing does not suppress rent notices, safety notices,
  booking changes, receipts, or other required transactional messages.
- Every marketing send evaluates current consent and the global suppression
  list immediately before delivery.
- Audience membership is explainable: an operator can see why a person is in a
  segment.
- A campaign stores the exact recipient snapshot and template version.
- Contacts with missing or ambiguous permission are excluded from promotional
  campaigns by default.
- Tenant employee lists must have an owner and periodic review so former
  employees do not remain active indefinitely.

### Billing and collection

1. A signed agreement produces an approved billing schedule.
2. Agent creates the appropriate one-time or recurring Stripe invoice.
3. Stripe securely collects card or ACH payment.
4. Webhooks update invoice/payment state idempotently.
5. Agent sends or coordinates reminders and assigns collection work.
6. Posted bank evidence is matched without treating pending money as cash.
7. During transition, accounting entries or invoice references synchronize to
   QuickBooks.
8. Refunds, credits, write-offs, and manual overrides require permissions,
   reasons, and audit history.

## Public Website Requirements

### Offerings

- Provide structured pages for available offices/suites, coworking, meeting
  rooms, and event spaces.
- Each page uses media assigned to that actual space or offering.
- Display capacity, features, included items, price presentation, availability
  language, and the correct conversion action.
- Offerings without approved content remain unpublished.

### Availability

- The website reads a public, redacted availability projection from Agent.
- Public data never includes tenant identity, internal pricing, hold owner,
  contract details, or internal notes.
- Stale data displays a safe fallback such as “Contact us for availability”
  rather than a false open date.
- Availability responses are cacheable for performance but are invalidated
  after consequential state changes.

### Conversion actions

- Available workspace: `Schedule a tour` or `Ask about this space`.
- Future availability: `Join the waitlist`.
- Event space: `Check your date`.
- Meeting room: `Request a booking` until instant-booking policy is approved.
- Every submission carries the offering, page, campaign, and attribution data.

## Agent Operator Experience

### Building Control

One default page answers:

- spaces available now and soon;
- active and expiring holds;
- new and overdue leads;
- upcoming tours;
- contracts awaiting signature;
- deposits and invoices requiring action;
- move-ins, move-outs, renewals, and upcoming events;
- operational blockers and maintenance;
- one prioritized next-action queue.

### Inventory

- Space list and visual availability calendar.
- Filters by type, status, capacity, date, and offering.
- Bulk-safe media and feature management.
- Clear conflict and stale-source states.

### CRM and campaigns

- Contact/company profile with relationships, opportunities, agreements,
  invoices, communication preferences, and activity.
- Deduplication queue.
- Segment builder using approved fields.
- Campaign draft, preview, test send, approval, schedule, send, and report.
- Suppression and consent review.

### Bookings and tenants

- Separate workspace and event pipelines.
- Calendar/timeline view.
- Agreement, payment, and readiness state visible together without conflation.

## API and Integration Contracts

### Public catalog API

Read-only endpoints:

- `GET /api/public/building/offerings`
- `GET /api/public/building/offerings/{slug}`
- `GET /api/public/building/availability`

Responses contain only approved public fields and include `updated_at`.

### Public intake API

- `POST /api/public/building/inquiries`
- Authenticated server-to-server using a building-specific intake secret.
- Idempotency key required.
- Returns a public-safe tracking ID, not internal CRM details.

### Internal operational API

Resource families:

- properties and spaces;
- offerings and rate plans;
- availability and holds;
- contacts, relationships, and opportunities;
- tours, reservations, occupancies, and events;
- agreements;
- billing schedules, invoices, and payments;
- segments, campaigns, preferences, and suppression;
- tasks, checklists, media, and audit history.

Consequential writes support preview where practical and always record actor,
source, previous state, new state, timestamp, and reason.

### Webhooks

- Stripe invoice and payment events;
- signature provider agreement events;
- email delivery, bounce, complaint, and unsubscribe events;
- optional calendar and external-source events where supported.

Every webhook:

- verifies authenticity;
- is idempotent;
- stores the provider event ID;
- handles out-of-order delivery;
- retries safely;
- exposes failures in an operator queue.

## Important States

Every main workspace supports:

- loading;
- empty;
- filtered empty;
- stale;
- partially synchronized;
- validation failure;
- integration unavailable;
- conflict;
- duplicate;
- permission denied;
- success;
- consequential action pending;
- retryable and terminal error.

The public site fails safely. If Agent is unavailable, existing content remains
visible and the user can submit a fallback inquiry, but availability is not
presented as live.

## Analytics

Preserve first-touch and latest-touch attribution:

- source;
- medium;
- campaign;
- listing/source reference;
- landing page;
- offering;
- initial timestamp;
- conversion timestamps.

Primary funnels:

### Workspace

```text
Visitor -> Inquiry -> Qualified -> Tour -> Proposal -> Signed -> Paid -> Occupied
```

### Event

```text
Visitor/lead -> Inquiry -> Hold -> Quote -> Signed -> Deposit -> Confirmed -> Completed
```

Measures:

- response time by source;
- inquiry-to-tour and tour-to-contract conversion;
- inquiry-to-event-confirmation conversion;
- revenue and collected cash by source/offering;
- space utilization and vacancy days;
- hold expiration rate;
- contract and deposit cycle time;
- invoice aging and collection time;
- campaign delivery, engagement, unsubscribe, and attributed inquiry;
- tenant renewal, expansion, and churn.

## Security, Privacy, and Accessibility

- Secrets remain server-side.
- Role-based access protects financial, contract, tenant, and campaign data.
- Payment credentials remain with the payment provider.
- Public APIs expose only allow-listed fields.
- Sensitive changes and external sends are audited.
- Data export, correction, suppression, and retention workflows are defined
  before broad campaign use.
- Campaign previews show sender, audience count, exclusions, and unsubscribe
  behavior.
- Website and Agent controls are keyboard accessible and have visible focus.
- Status is communicated with text, not color alone.
- Public forms include labels, useful validation, error recovery, and mobile
  behavior.

## Scope

### Included

- Building inventory and public offerings
- Availability, holds, occupancy, and event booking
- Connected lead intake and attribution
- HubSpot-backed CRM workflow
- Tenant and community contact relationships
- Segments, preferences, suppression, and campaigns
- Tours, quotes, agreements, deposits, invoices, and payments
- Google Calendar projection
- Eventective/Marketplace assisted intake
- Operator queues and audit history
- Controlled QuickBooks transition

### Not included in the first release

- Replacing HubSpot with a fully native general-purpose CRM
- Fully autonomous pricing or contract approval
- Public instant booking for complex events
- Storing card or bank credentials
- Autonomous payment execution outside approved billing schedules
- A general ledger or immediate QuickBooks shutdown
- Payroll replacement
- Access-control hardware, door locks, or network provisioning
- A full tenant social network
- Native mobile applications

## Delivery Plan

### Phase 0: operating decisions and data cleanup

- Confirm inventory, space names, capacities, current occupants, lease dates,
  prices, event rules, and responsible operators.
- Confirm Eventective and map all current lead inboxes.
- Select signature and payment providers.
- Define communication classes, consent rules, and initial tenant list owner.
- Establish stable IDs and deduplicate contacts.

Exit condition: every active space and current tenant has one reviewed record.

### Phase 1: inventory, CRM intake, and public catalog

- Add property, space, offering, rate-plan, availability, relationship, and
  opportunity models.
- Add Building Control and Inventory operator pages.
- Connect building website intake to Agent.
- Synchronize contacts/companies/deals with HubSpot.
- Publish offerings and safe availability to the website.
- Add source attribution and failure queues.

Exit condition: website availability is generated from Agent and every website
inquiry enters one assigned pipeline.

### Phase 2: tours, holds, event booking, and calendar

- Add workspace and event state machines.
- Add tours, holds, conflict checks, expiration, event requirements, and
  operational calendars.
- Project confirmed events and approved holds to Google Calendar.
- Add Eventective/Marketplace assisted intake.

Exit condition: operators cannot double-book a space through normal workflows,
and expiring holds are visible and actionable.

### Phase 3: agreements and payments

- Add versioned proposal and agreement records.
- Integrate the selected signature provider.
- Add billing schedules and Stripe invoices, deposits, ACH/card collection,
  webhooks, reminders, refunds, and reconciliation.
- Continue required QuickBooks synchronization.

Exit condition: a signed agreement can produce a traceable invoice and payment
without duplicate manual entry.

### Phase 4: tenant CRM and campaigns

- Import and review tenants, tenant employees, event hosts, prospects, and
  former customers.
- Add preferences, suppression, explainable segments, campaign approvals,
  delivery events, and analytics.
- Launch with one operational tenant announcement and one optional promotional
  campaign after test sends and list review.

Exit condition: staff can send to a reviewed audience, exclusions are
explainable, and unsubscribe/bounce events suppress future marketing sends.

### Phase 5: tenant operations and QBO readiness

- Add move-in/out, renewal, maintenance, service, and event checklists.
- Add a limited customer portal if operator workflows are stable.
- Run parallel financial closes and document accounting gaps.
- Decide whether Agent will export to an accountant, retain QBO as the ledger,
  or pursue a native accounting phase.

Exit condition: QBO retirement is a documented accounting decision, not merely
an integration shutdown.

## Acceptance Criteria

### Inventory and website

- An authorized operator can create a space and offering without editing code.
- Publishing an offering exposes only approved public fields.
- Changing a space from Available to Soft hold removes public “available now”
  language within the defined cache window.
- Stale or unavailable Agent data never produces a false availability claim.
- Every offering page sends its stable offering ID with an inquiry.

### CRM and lead intake

- Submitting a valid website inquiry creates one Agent opportunity and creates
  or updates the correct HubSpot contact.
- Repeated submissions from the same person do not create uncontrolled contact
  duplicates.
- The source, original reference, received time, and first response time are
  retained.
- Partial HubSpot or email failure appears in a retryable operator queue without
  losing the inquiry.

### Holds and bookings

- Overlapping confirmed availability blocks for the same exclusive space are
  rejected.
- Soft holds expire according to policy and release availability.
- A tour does not block inventory unless explicitly converted to a hold.
- An event cannot become Confirmed without its configured approval, agreement,
  and deposit conditions.
- Calendar deletion or modification does not silently cancel the Agent booking.

### Agreements and billing

- The signed agreement version and evidence are retained.
- An approved billing schedule can create a provider invoice idempotently.
- Duplicate or out-of-order payment webhooks do not double-apply money.
- Pending ACH is not shown as posted cash.
- Refunds, credits, write-offs, and overrides require permission and an audit
  reason.
- QuickBooks remains reconcilable during the transition.

### Tenant CRM and campaigns

- A contact may be a tenant, prospect, and event host without duplication.
- Operators can preview why each contact is included or excluded from a segment.
- Marketing-unsubscribed contacts are excluded immediately from promotional
  sends.
- Required transactional messages remain independently deliverable when
  appropriate.
- Bounce, complaint, and unsubscribe events update suppression state
  idempotently.
- A campaign records its recipient snapshot, content version, approver, sender,
  send time, and delivery outcome.
- Test send and final audience preview are required before a bulk send.

### Permissions and audit

- Public users cannot access internal availability or tenant details.
- Sales users cannot mark a bank payment as posted without the required finance
  permission and evidence.
- Every consequential state change and external send records actor, time,
  source, before/after state, and result.

## Validation Plan

### Automated

- Model and state-transition tests
- Availability overlap and hold-expiration tests
- Public-field allow-list tests
- Intake validation, deduplication, and idempotency tests
- HubSpot partial-failure and retry tests
- Stripe webhook signature, ordering, and duplication tests
- Campaign suppression and audience-snapshot tests
- Permission and audit tests
- Website contract tests against Agent public APIs

### Operator walkthroughs

1. Convert a website office inquiry into a tour, hold, signed agreement,
   invoice, payment, and occupied space.
2. Convert an Eventective or manually entered event lead into a hold, agreement,
   deposit, calendar event, checklist, and closeout.
3. Cancel each workflow at every consequential stage and verify availability,
   money, calendar, and communication effects.
4. Send a tenant operational notice and an optional promotional campaign to
   different reviewed audiences.
5. Unsubscribe, bounce, merge, and remove contacts and verify future behavior.
6. Simulate HubSpot, Stripe, Google, and Agent outages and verify safe recovery.
7. Complete a parallel month-end reconciliation with QuickBooks.

## Unresolved Decisions and Recommended Defaults

| Decision | Recommended default |
| --- | --- |
| Event lead platform identity | Confirm Eventective before integration work. |
| CRM source of truth | Keep HubSpot for contacts, companies, and sales activity initially. |
| Operational source of truth | Agent owns spaces, holds, bookings, occupancy, contracts, and billing workflow. |
| Campaign delivery provider | Use a provider adapter; start with existing HubSpot capability if licensed, otherwise a dedicated campaign-capable provider. Do not use ad hoc bulk transactional email. |
| Transactional email | Continue through Agent's existing Resend integration unless deliverability requirements indicate otherwise. |
| Payment provider | Stripe Invoicing with ACH and card support. |
| Signature provider | Select after comparing current contract workflow, API/webhook support, templates, and cost. |
| Event booking mode | Request, approve, contract, and deposit before confirmation; no complex-event instant booking initially. |
| Public availability precision | Show status and available date for offices; use date-request flow for events until operating rules are proven. |
| Public pricing | Publish starting prices or ranges, with operator-approved exceptions. |
| Calendar ownership | Agent is authoritative; Google Calendar is a synchronized projection. |
| QBO transition | Retain as formal accounting source through parallel closes and accountant review. |
| Tenant portal | Defer until internal operator workflows and data quality are stable. |

## Required Owner Decisions Before Phase 1

1. Confirm the complete rentable inventory and current occupancy.
2. Confirm public price presentation by offering.
3. Confirm Eventective and identify every lead inbox and owner.
4. Define event types, capacity, hours, deposits, insurance, alcohol, security,
   cleaning, cancellation, and approval rules.
5. Identify the current contract/signature process.
6. Identify billing methods, billing dates, late rules, and refund authority.
7. Name the owner of tenant contact data and campaign approval.
8. Define which tenant employees may receive community marketing.
9. Confirm the accountant/bookkeeper and QBO retirement requirements.
10. Select the first pilot: one office offering and one event offering.
