# Finance Native Operations Upgrade

Status: Build-ready product specification  
Prepared: 2026-07-22

## Plain-English Summary

Anata Finance should replace ClickUp with its own money-commitment workflow and replace routine bank CSV uploads with read-only Plaid bank connections. QuickBooks remains the accounting and invoice source. An LLM makes the system easier to use by turning plain-English requests and documents into proposed records, explaining problems, matching evidence, and drafting follow-ups. It does not become the financial database and cannot move money or silently change records.

The upgraded product should let a non-technical operator answer five questions in one short daily review:

1. How much cash is actually available now?
2. What money is expected, and how reliable is it?
3. What must be paid next?
4. What is broken or unverified?
5. What single action should happen first?

## Verified Current State

The live `/admin/finances` page already provides a strong foundation:

- cash on hand, confirmed and expected income, required outflow, and a cash floor;
- collections, cash trajectory, savings analysis, and Smart CFO advice;
- a unified Money queue;
- bank CSV ingestion;
- QuickBooks receivables and payment-evidence synchronization;
- ClickUp AP/AR plans and recurring items;
- partial-payment, installment, settlement, reconciliation, and audit concepts;
- conservative source rules that prevent expected income from being presented as posted cash.

The live review on 2026-07-22 also verified the core usability problem:

- 423 records were in Needs action;
- 321 records appeared as payables and 107 as incoming;
- 89 active obligations had missing or conflicting evidence;
- 180 records were excluded;
- the bank CSV snapshot was stale;
- repeated historical payroll, rent, loan, and other ClickUp occurrences dominated the queue;
- the Update money dialog mixed uploads, source refreshes, reconciliation, settings, and manual exceptions.

These counts are operational evidence, not permanent requirements. They demonstrate that ClickUp history and source reconciliation are overwhelming the daily decision workflow.

## Problem

Finance has good financial logic but too many sources and too much historical residue. The page promises one next action while presenting multiple source errors, hundreds of queue items, and several overlapping AI summaries. Operators must understand ClickUp, QuickBooks, CSV imports, reconciliation, and internal source precedence before they can act.

The root problem is not visual styling. It is the absence of one native place to create, own, approve, schedule, and close money commitments.

## Product Goal

Make Anata the operational source of truth for financial commitments while preserving external evidence:

- **Anata** owns bills, expected receipts, payroll commitments, recurring commitments, assignments, approvals, exceptions, and workflow state.
- **Plaid** provides read-only bank accounts, balances, and posted or pending transactions.
- **QuickBooks Online** provides invoices and formal accounting context.
- **The LLM** assists with intake, classification, matching, explanations, drafting, and prioritization.

## Users

### Primary user

An owner or finance operator performing a short daily cash review without needing technical or accounting-system expertise.

### Secondary users

- department owners responsible for a bill or collection;
- approvers who review a commitment or future payment;
- administrators who manage integrations, permissions, and exceptions.

## Product Principles

1. Posted bank cash is never confused with forecast cash.
2. A commitment is not a payment; a plan is not bank evidence.
3. AI proposes and explains. Deterministic rules calculate and enforce.
4. Financial writes require preview, confirmation, attribution, and audit history.
5. Historical records remain searchable but do not overwhelm today's queue.
6. When evidence conflicts, the system fails closed and asks for review.
7. No autonomous payment execution is included in the initial release.

## Target Information Architecture

`/admin/finances` remains the main Finance destination. Its primary scan path becomes:

### 1. Today

- verified bank cash;
- bills due before the next likely deposit;
- confirmed and expected collections shown separately;
- gap using cash today;
- gap if confirmed collections arrive;
- one primary next action.

When a required source is stale, lower-priority recommendations collapse and the primary action resolves that source.

### 2. Commitments

Anata-native bills, incoming money, payroll, debt, taxes, rent, and recurring commitments. The default view shows current unresolved work only.

Filters:

- Needs review
- Incoming
- Bills
- Payroll
- Recurring
- Completed
- History

### 3. Transactions

Plaid bank accounts and activity:

- current and available balances;
- posted and pending transactions;
- matched and unmatched activity;
- connection freshness and reconnect state;
- account-level visibility and permissions.

### 4. Ask Finance

A plain-English assistant for creating proposals, finding records, explaining numbers, and preparing follow-up. It is visibly labeled as assistance, not bank or accounting truth.

### 5. Sources

A simple Source Center replaces the overloaded Update money dialog:

- Bank accounts — Plaid status and last update
- QuickBooks — invoice/accounting status and last update
- Anata commitments — health and unresolved exceptions
- Payroll provider — status when added
- Manual import — fallback only

Each source has one clear status and one primary recovery action.

## Native Commitment Model

### Commitment types

- payable;
- receivable;
- payroll;
- tax;
- debt payment;
- recurring operating expense;
- transfer or reserve movement;
- manual exception.

### Required fields

- commitment type;
- counterparty or employee/payroll group label;
- amount or explicitly marked unknown amount;
- due or expected date;
- status;
- priority;
- owner;
- evidence class;
- source and source reference;
- created/updated actor and timestamps.

### Optional fields

- recurrence rule;
- minimum payment;
- installment plan;
- flexibility;
- confidence;
- notes and attachments;
- approval requirement;
- linked QuickBooks invoice;
- linked Plaid transactions.

### Payable lifecycle

```text
Draft -> Needs review -> Approved -> Scheduled -> Partially paid -> Bank verified
                                  \-> Cancelled
```

`Paid` is not accepted as final proof unless supported by a posted transaction, a documented accounting settlement, or an explicit audited exception.

### Incoming lifecycle

```text
Expected -> Confirmed -> Due -> Overdue -> Received -> Bank verified
                     \-> Cancelled or written off
```

Expected income never enters posted cash. Confirmed income may enter the committed forecast only when it has a documented amount and date.

### Recurrence behavior

A recurring template generates only the next necessary occurrence or a bounded forward window. Superseded historical occurrences move to History and do not remain in today's Needs action queue. The system never produces hundreds of active instances from old schedules.

## Plaid Integration

### Initial products

- Transactions for bank activity and incremental updates;
- Balance for current balance checks where available;
- Plaid Link for institution connection and reconnect flows.

### Initial behavior

1. An authorized admin selects Connect bank.
2. The server creates a short-lived Link token.
3. The user completes Plaid Link.
4. The server exchanges the public token and encrypts the resulting access token.
5. A background job imports accounts and transactions using stable Plaid identities and cursors.
6. Webhooks mark data ready, updated, removed, or requiring reconnection.
7. Finance recalculates source freshness, matches, cash posture, and recommendations.

### Safety requirements

- Plaid credentials and access tokens never appear in page source, logs, LLM prompts, or analytics.
- Tokens are encrypted at rest and redacted from errors.
- Webhooks are verified and idempotent.
- Account removal stops future synchronization without deleting audit history.
- Pending and posted transactions remain distinguishable.
- Modified or removed Plaid transactions create auditable corrections rather than destructive silent changes.
- Only authorized Finance admins may connect or disconnect accounts.

### CSV fallback

Bank CSV remains available as an emergency fallback and for unsupported accounts. It is no longer the routine primary action after Plaid is connected. The Source Center clearly states which source currently owns actual cash for each account to prevent double counting.

## QuickBooks Role

QuickBooks remains the source for:

- open invoices;
- invoice balance and due-date evidence;
- customer accounting context;
- formal accounting records.

QuickBooks does not own Anata workflow state and does not replace posted bank evidence. Cross-source duplicates are quarantined for review.

## Payroll

Plaid Payroll Income is not used to run Anata payroll; it is designed mainly to verify an individual's employment and income.

The Finance roadmap for payroll is:

1. Create native payroll commitments and recurrence rules in Anata.
2. Use Plaid transactions to verify payroll withdrawals after posting.
3. Add a direct integration with the actual payroll provider for upcoming payroll totals and dates.
4. Protect payroll as a must-pay category that AI cannot recommend cancelling or deferring without explicit policy and human approval.

The initial provider is an open decision. Recommended default: integrate the provider Anata actually uses rather than building a generic payroll abstraction prematurely.

## LLM Responsibilities

### Allowed

- turn plain English into a proposed commitment;
- extract draft fields from invoices, statements, and uploaded documents;
- suggest categories, owners, recurrence, and priority;
- summarize why Finance is blocked;
- propose likely transaction-to-commitment matches;
- detect potential duplicates for review;
- rank collections using amount, age, likelihood, previous attempts, and cash impact;
- draft collection or vendor messages;
- answer questions using authorized Finance records;
- explain calculations produced by deterministic Finance services.

### Prohibited

- create or change a financial record without confirmation;
- calculate canonical cash, balances, open amounts, or forecast paths;
- mark a commitment paid without accepted evidence;
- approve, schedule, or send money;
- delete or hide financial history;
- place sensitive bank tokens or raw credentials in a prompt;
- treat a model inference as source evidence;
- contact a customer, employee, vendor, bank, or payroll provider without an explicit approved action.

### Conversational confirmation example

User:

> Add payroll of $10,000 for August 5 and mark it critical.

Preview:

> Create a $10,000 Payroll commitment due August 5, marked Must pay. This reduces the forecast by $10,000. No bank payment will be initiated.

The user must confirm Save commitment.

## Recommendation and Collection Ranking

Deterministic rules select eligible actions. The LLM may explain them.

Priority order:

1. Restore stale or missing bank evidence.
2. Resolve conflicting or duplicated records.
3. Collect confirmed receivables that materially reduce a cash gap.
4. Protect payroll, tax, debt, rent, insurance, and critical utilities.
5. Review flexible commitments that cause a floor breach.
6. Schedule funded required payments.
7. Surface savings opportunities only after source trust is ready.

Collection priority considers amount, days overdue, payment likelihood, previous attempts, relationship risk, and funding-gap impact. Age alone does not make a very small invoice the first action.

## Page and Copy Changes

### Funding-gap labels

Replace an ambiguous single funding-gap presentation with:

- **Gap using bank cash today**
- **Gap if confirmed invoices are collected**

### Trust and AI sections

When trust is blocked, combine Decision Trust, Smart Brief, Smart CFO, and Savings warnings into one authoritative blocker. Lower-priority AI panels collapse until the blocker is resolved.

### Primary action

The page always exposes one primary action. Examples:

- Refresh connected bank accounts
- Reconnect bank
- Review two unmatched withdrawals
- Confirm upcoming payroll
- Collect Divi Energy invoices

### Source Center

Do not mix source administration, recurring cleanup, cash-floor settings, and manual entry in one long modal. Cash-floor settings belong in Finance settings. Reconciliation has its own focused review. Add commitment is available directly from Commitments and Ask Finance.

## Important States

### Empty

Show a guided setup sequence: connect bank, connect QuickBooks, set cash floor, add or import the first commitment. Never display `$0` as though it were verified cash.

### Loading

Keep the page shell visible. Show per-source progress and preserve the last verified values with an updating label.

### Stale

Keep prior values visible with their date, mark them stale, pause payment recommendations, and provide one refresh or reconnect action.

### Partial source failure

One failed institution does not erase healthy accounts. Show exactly which totals are incomplete and suppress only decisions affected by that missing evidence.

### Conflict

Show both source claims, the financial effect, and the actions needed to resolve the conflict. Do not silently choose the more convenient value.

### Permission denied

Users without Finance write permission may view authorized summaries but cannot connect sources, reveal bank details, approve commitments, or invoke write previews.

### Responsive behavior

Desktop remains primary. At narrower widths, the Today metrics stack, filters wrap, and tables use a deliberate horizontal container or row-detail pattern without hiding amounts, dates, confidence, or actions.

## Migration From ClickUp

### Migration rules

- Do not bulk-import every ClickUp Finance record into the active queue.
- Profile all records first and classify them as active, historical, duplicate, ambiguous, or excluded.
- Import active commitments with stable source references.
- Preserve the original ClickUp URL and payload hash for auditability.
- Move historical occurrences into a read-only archive.
- Quarantine ambiguous and duplicate records for a bounded migration review.
- Continue read-only ClickUp comparison during the transition.
- Disable ClickUp writes before disabling reads.
- Remove ClickUp from Finance source readiness only after reconciliation passes.

### Cutover phases

1. Add native commitments without changing current Finance reads.
2. Import and reconcile a reviewed active subset.
3. Dual-read native and ClickUp values in shadow mode.
4. Make Anata the workflow source for new commitments.
5. Freeze ClickUp Finance writes.
6. Switch forecasts and queue reads to native commitments.
7. Archive ClickUp history and remove its daily Finance controls.

No destructive deletion is part of the cutover.

## Data and Service Impact

Reuse the existing canonical `cash_events`, `settlement_allocations`, `payment_installments`, source identity, reconciliation, forecast, and audit concepts where safe. Add native workflow fields or a dedicated commitment workflow table only where the existing event model cannot represent ownership, approval, recurrence, and lifecycle cleanly.

Add Plaid-specific storage for:

```text
plaid_items
- id / scope_key / institution_id / display_name
- encrypted_access_token
- status / consent_expiration / last_success_at / last_error_code
- created_by / created_at / updated_at

plaid_accounts
- id / plaid_item_id / external_account_id
- name / mask / type / subtype / currency
- current_balance_cents / available_balance_cents / balance_as_of
- active / created_at / updated_at

plaid_sync_state
- plaid_item_id / transactions_cursor / last_webhook_at / updated_at
```

Plaid transactions should enter the existing canonical transaction and source-record system using stable external IDs. Do not create a second ledger or forecast implementation.

## API and Route Impact

Keep:

- `GET /admin/finances`
- existing QuickBooks OAuth and synchronization routes;
- manual AP/AR routes temporarily as compatibility paths;
- existing settlement and installment services after authority review.

Add or replace with routes equivalent to:

```text
GET  /admin/finances/commitments
POST /admin/finances/commitments/preview
POST /admin/finances/commitments/confirm
POST /admin/finances/commitments/{id}/transition/preview
POST /admin/finances/commitments/{id}/transition/confirm

POST /admin/finances/plaid/link-token
POST /admin/finances/plaid/exchange
POST /api/integrations/plaid/webhook
POST /admin/finances/plaid/items/{id}/refresh
POST /admin/finances/plaid/items/{id}/disconnect/preview
POST /admin/finances/plaid/items/{id}/disconnect/confirm

POST /admin/finances/assistant/preview
POST /admin/finances/assistant/confirm
```

Exact route names may follow repository conventions, but preview/confirm separation, authorization, CSRF protection, idempotency, and audit logging are required.

## Security and Permissions

- Bank connection management requires Finance admin permission.
- Commitment creation and editing require Finance operator permission.
- Approval uses a distinct approver permission where policy requires it.
- Sensitive account identifiers are masked by default.
- LLM context is minimized to the records necessary for the request.
- Uploaded financial documents follow a documented retention policy.
- Every write records actor, timestamp, before/after values, source evidence, and idempotency key.
- Payment execution, if ever approved later, requires a separate specification, risk review, limits, dual approval, webhook reconciliation, and emergency shutoff.

## Analytics and Operational Monitoring

Track product health without sending sensitive transaction descriptions or bank details to analytics:

- source connection success and reconnect rate;
- time since last successful bank and QuickBooks update;
- active Needs review count;
- percentage of transactions auto-matched and manually corrected;
- commitment creation source: manual, LLM-assisted, imported, or integration;
- time from commitment due to verified settlement;
- daily-review completion and primary-action completion;
- LLM proposal acceptance, edit, and rejection rates;
- webhook failures, sync lag, and duplicate prevention events.

## Rollout Plan

### Phase 0: Trust cleanup

- profile the live ClickUp backlog;
- correct recurrence and historical-queue behavior;
- reconcile the conflicting obligation counts shown by different Finance sections;
- ensure one canonical trust calculation drives every warning;
- add migration reporting and rollback checkpoints.

### Phase 1: Native commitments

- build native commitment lifecycle, ownership, recurrence, history, and approvals;
- import a reviewed active ClickUp subset;
- run shadow comparisons;
- stop creating new Finance work in ClickUp.

### Phase 2: Plaid read-only bank truth

- implement Sandbox first;
- connect test institutions and sync balances/transactions;
- add webhooks, reconnect, source freshness, and canonical matching;
- move CSV to fallback status;
- enable limited production only after reconciliation passes.

### Phase 3: LLM assistance

- add plain-English commitment preview;
- add explanation and search;
- add match suggestions and document extraction;
- add collection ranking and draft communications;
- measure edits and false-positive rates before expanding autonomy.

### Phase 4: Payroll-provider connection

- integrate the provider Anata uses;
- import upcoming payroll dates and totals as protected commitments;
- verify settlements with Plaid transactions.

### Phase 5: Payment evaluation

Payments remain out of scope until the preceding phases are stable. Evaluate Plaid Auth plus a processor or Plaid Transfer in a separate decision and security review.

## Acceptance Criteria

### Daily Finance experience

- A non-technical operator can identify verified cash, required payments, expected receipts, and the single next action without opening another system.
- The default queue contains only current unresolved work; historical ClickUp recurrence residue is absent.
- No page shows conflicting counts for the same source-trust condition.
- Gap using cash today and gap after confirmed collections are clearly distinguished.
- When a bank source is stale, the primary action directly starts refresh or reconnect.

### Native commitments

- A user can create, review, approve, schedule, partially settle, cancel, and archive a commitment with a complete audit trail.
- Recurrence generates bounded current work and does not flood the queue.
- Completed operational status cannot fabricate bank settlement.
- Every transition is authorized, idempotent, and previewed where it changes financial meaning.

### Plaid

- Sandbox Link connects successfully without exposing credentials to the browser or logs.
- Incremental synchronization handles added, modified, and removed transactions idempotently.
- Pending transactions cannot be treated as final settlement.
- Reconnecting or disconnecting preserves historical audit evidence.
- Plaid and CSV transactions cannot be double-counted for the same account and period.
- A failed institution clearly limits only the affected totals and recommendations.

### LLM

- An LLM-created commitment is always a preview until a user confirms it.
- The model cannot directly change cash, settlement, approval, payment, or source authority.
- Calculations shown in LLM explanations match deterministic Finance outputs.
- Sensitive credentials and full bank tokens never enter prompts.
- Rejected or edited proposals are measurable for quality improvement.

### Accessibility and usability

- All dialogs trap and restore focus and close with Escape.
- Status is never conveyed by color alone.
- Tables, filters, input errors, loading, and success messages are screen-reader understandable.
- Keyboard-only operation covers the full daily review and commitment-confirmation path.
- Desktop QA passes at 1280, 1440, and 1920 pixels without page-level horizontal overflow.

### Regression protection

- Existing cash, QBO, settlement, forecast, reconciliation, savings, permissions, and audit tests remain green.
- Migration reconciliation proves that active face amount, open amount, settled amount, and posted cash totals are preserved or explicitly quarantined.
- A rollback can restore pre-cutover reads without losing writes or audit events.

## Validation Plan

1. Run a read-only production profile and classify the ClickUp backlog.
2. Test native commitment lifecycles using a copied production database.
3. Compare old and new queue, forecast, and trust calculations in shadow mode.
4. Exercise Plaid Sandbox scenarios: success, MFA, delayed transactions, removed transactions, duplicate webhooks, reconnect required, and institution outage.
5. Test LLM intake with valid, incomplete, ambiguous, duplicate, and malicious document content.
6. Perform keyboard and screen-reader review of Today, Commitments, Source Center, and confirmation flows.
7. Conduct a one-week operator trial before freezing ClickUp writes.
8. Review audit logs and reconciliation reports before every cutover step.

## Non-Goals

- autonomous payments;
- autonomous payroll execution;
- replacing QuickBooks as the accounting system;
- allowing an LLM to become the ledger or calculation engine;
- importing all ClickUp history into active work;
- deleting historical evidence during migration;
- building a general project-management product outside Finance;
- adding multiple payroll providers before the real provider is selected.

## Open Decisions and Recommended Defaults

1. **Which payroll provider is authoritative?**  
   Default: integrate the provider currently used by Anata.

2. **Who can approve protected commitments?**  
   Default: Finance admin plus a separate approver for payroll, tax, debt, and payments above a configurable threshold.

3. **How much transaction history should Plaid request initially?**  
   Default: enough to support existing recurrence and matching rules, starting conservatively in Sandbox and validating cost before production.

4. **Should Plaid current or available balance drive cash on hand?**  
   Default: display both when supplied; use the conservative available balance for spend decisions and label the selected evidence explicitly.

5. **How long should ClickUp remain readable?**  
   Default: retain a read-only archive through at least one full reconciliation and reporting cycle before removing the integration.

6. **When should payments be considered?**  
   Default: only after native commitments, Plaid read-only sync, and payroll planning have operated reliably in production and a separate payment-control specification is approved.

## Recommended First Build Slice

Do not begin with Plaid UI or an AI chat box. Begin with the smallest change that removes the root confusion:

1. introduce native commitments and bounded recurrence;
2. migrate a reviewed set of current ClickUp Finance records;
3. make the Money queue read native commitments in shadow mode;
4. reconcile counts and cash impact against the existing page;
5. then connect Plaid Sandbox as the actuals source.

This sequence prevents Plaid and the LLM from being layered on top of the current 423-item source problem.
