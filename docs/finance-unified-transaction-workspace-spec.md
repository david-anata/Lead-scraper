# Unified Finance Transaction Workspace Specification

Status: Build-ready proposal  
Owner: Anata Finance Control  
Scope: Finance only  
Prepared: 2026-08-04

## 1. Outcome

An owner can encounter the same expense in Calendar, Budget & Savings, Review,
Bookkeeping, or a cash plan and always receive the same facts, available actions,
draft state, confirmation language, and audit history.

The work replaces page-specific transaction behavior with one shared transaction
workspace. It does not collapse every Finance page into one screen. Each page
keeps its purpose, while transaction decisions are owned by shared components,
shared APIs, and one evidence-aware state model.

Success means David can review a large set of expenses, stage decisions without
each click saving, safely leave and return, preview the combined consequences,
save once, undo the batch, and see the results consistently everywhere.

## 2. Verified current behavior

- Finance currently has six owner-facing destinations: Today, Cash plan,
  Calendar, Budget & savings, Review, and Accounts & setup.
- Calendar shows posted, planned, and historically inferred expenses but is
  primarily read-only.
- Budget & savings supports staged vendor decisions, local draft recovery,
  `Save all changes`, discard, and a `beforeunload` warning.
- Review supports checkbox selection, bulk preview, a required reason for
  consequential actions, protected items, batch audit records, and undo.
- What is coming has a separate selection bar, confirmation dialogs, JSON saves,
  in-place row removal, and its own undo behavior.
- Bookkeeping supports transaction and merchant-level filing decisions.
- Savings review decisions are durable but use a separate opportunity workflow.

These are useful capabilities. The problem is duplicated ownership and
inconsistent interaction, not a complete absence of functionality.

## 3. User and primary job

Primary user: David, the owner/operator.

Primary job:

> Show me what money left, what will probably leave next, what I can cut, and let
> me make many safe decisions without losing work or creating false financial
> facts.

Secondary jobs:

- categorize transactions for bookkeeping;
- identify and manage recurring costs;
- match posted payments to planned obligations;
- investigate ambiguous transactions;
- request and verify cancellations;
- understand budget and end-of-month cash effects;
- audit and reverse prior decisions.

## 4. Product principles

1. One transaction, one identity, one history.
2. Views may differ; facts and actions may not.
3. Posted cash, planned obligations, recurring predictions, and savings
   opportunities remain distinct objects.
4. A user decision is not automatically a financial fact.
5. Paid requires posted settlement evidence.
6. Cancelled service and cancelled ledger obligation are different states.
7. Potential savings are not realized savings.
8. Selection never implies saving.
9. All edits are staged until `Save all changes`.
10. A bulk save is one atomic, idempotent, auditable operation.
11. Protected categories never inherit risky bulk actions.
12. AI proposes; deterministic rules validate; the owner confirms.

## 5. Exact vocabulary

The following words have one meaning throughout Finance.

### 5.1 Evidence status

| Label | Exact meaning | Required evidence |
|---|---|---|
| Posted | A bank or authoritative accounting source says the transaction occurred. | Source transaction identifier and posted date. |
| Paid | Posted outflow has been allocated to an obligation, or the row itself is the posted purchase being discussed. | Posted bank/QBO evidence. |
| Partially paid | Some posted value is allocated and a positive obligation balance remains. | Allocation records and open balance. |
| Unpaid | A known obligation has a positive balance and no complete posted settlement. | Obligation plus settlement calculation. |
| Unconfirmed | History suggests a future charge, but no obligation or posted payment proves it. | Recurring-pattern evidence only. |
| Matched | A posted transaction is allocated to a specific obligation. | Settlement allocation. |
| Needs review | Evidence conflicts, is incomplete, or does not permit a safe decision. | Explicit reason code. |

`Completed`, `approved`, or `closed` in ClickUp never means `Paid`.

### 5.2 Planning status

| Label | Exact meaning |
|---|---|
| Planned | A known obligation or confirmed recurring schedule exists. |
| Unplanned | Posted cash has no linked obligation or confirmed schedule. |
| Expected from history | A high-confidence recurring pattern recognizes a posted charge. This is classification, not settlement. |
| Possible from history | A future recurring prediction that has not been confirmed as a plan. |

### 5.3 Savings status

| State | Meaning |
|---|---|
| Unknown | No owner decision exists. |
| Needed | The cost should continue. |
| Waste | The owner believes the cost should stop or shrink. |
| Investigating | More evidence or vendor contact is required. |
| Cancellation recommended | The system recommends review; no vendor action is claimed. |
| Cancellation requested | The owner records that a request was sent. |
| Vendor confirmed cancellation | A vendor confirmation and effective date were recorded. |
| Waiting for final charge | Cancellation is confirmed but the expected final billing window remains open. |
| Bank-verified stopped | The verification window passed with no qualifying new charge. |
| Realized savings | Bank evidence confirms the cost stopped or decreased for the defined verification period. |

### 5.4 Save language

- `Draft`: staged locally/server-side but not applied to finance records.
- `Saved`: the server committed the complete batch and returned its batch ID.
- `Synced`: a named external system confirmed its write.
- `Failed`: no claim is made that the affected operation succeeded.
- `Partially synced`: the Agent batch saved, but one or more external writes need
  retry. This is never described as a partially saved Agent batch.

## 6. Information architecture

The six Finance destinations remain:

1. **Today** — cash position and the next owner action.
2. **Cash plan** — known upcoming income, obligations, and coverage.
3. **Calendar** — timing of posted, unpaid, and possible items.
4. **Budget & savings** — spending control, cuts, trends, and realized savings.
5. **Review** — all ambiguous, conflicting, overdue, or protected decisions.
6. **Accounts & setup** — sources, freshness, connections, and rules.

Bookkeeping becomes a mode inside the shared transaction workspace and a saved
view reachable from Budget & savings and Review. It does not need a competing
transaction interaction model.

Each page can filter and summarize transactions differently. Clicking or acting
on a transaction always invokes the shared workspace.

## 7. Object model and ambiguity prevention

Finance contains related but non-interchangeable objects.

### 7.1 Posted transaction

Money that actually entered or left an account. Canonical identifier:
`transaction_id`. Safe actions include categorize, classify savings value,
match, split, mark transfer, add note, and attach evidence. It cannot be
retroactively cancelled.

### 7.2 Obligation

A known amount expected to be paid or received. Canonical identifier:
`obligation_id`. Safe actions include edit plan, defer, cancel the obligation,
match a posted transaction, or mark a non-cash resolution with a reason. It
cannot be marked paid without settlement evidence.

### 7.3 Recurring pattern

A historical inference. Canonical identifier: `pattern_key`. Safe actions
include track, not recurring/not a bill, snooze, combine histories, and adjust
the proposed schedule. Tracking creates or updates a schedule; it does not
create posted cash.

### 7.4 Savings opportunity

A computed review packet referencing one or more transactions or merchants.
Canonical identifier: `opportunity_key`. Safe actions classify the opportunity
and progress cancellation verification. They do not mutate source transactions.

### 7.5 Merchant rule

A reusable instruction created from a confirmed decision. Canonical identifier:
`rule_id`. It may propose or automatically apply low-risk classifications within
its allowed scope. Every application records the rule version.

### 7.6 Relationship rules

- A transaction may match zero or more obligations only through allocations.
- An obligation may be partially settled by multiple transactions.
- A recurring pattern may produce a schedule only after confirmation.
- A savings opportunity may reference many transactions but never owns them.
- A merchant rule may affect future rows but never rewrites posted amounts,
  dates, source identifiers, or settlement evidence.

## 8. Shared transaction workspace

The shared workspace is a right-side drawer on desktop and a full-screen sheet
on phones. Opening it does not navigate away or discard page state.

### 8.1 Header

- cleaned merchant name;
- amount and direction;
- posted/planned date;
- account label;
- evidence status;
- planning status;
- close control that restores focus to the originating row.

### 8.2 Sections

1. **What happened** — raw description, source, account, identifiers, date,
   amount, pending/posted state.
2. **How it is treated** — category, budget treatment, savings classification,
   protected status.
3. **Payment evidence** — obligation, allocations, paid/open amounts, match
   confidence.
4. **Pattern** — cadence, occurrence history, next likely charge, confidence.
5. **Savings** — six-month spend, next likely charge, potential and realized
   savings, cancellation state.
6. **Notes and evidence** — owner notes, confirmation numbers, attachments.
7. **Activity** — prior value, new value, actor, time, source page, batch/rule.

### 8.3 Footer

- `Stage change` for the current item;
- `Discard item changes` when the item has a draft;
- previous/next selected item navigation;
- no direct permanent save button.

## 9. Action eligibility registry

One server-owned registry defines action availability. Pages do not invent
their own eligibility rules.

| Action | Transaction | Obligation | Pattern | Savings opportunity | Protected restriction |
|---|---:|---:|---:|---:|---|
| Needed/Waste/Unknown/Investigating | Yes | No | No | Yes | Waste blocked for payroll/tax/debt without individual review. |
| Change category | Yes | Limited | Proposed only | No | Payroll/tax/debt categories require individual review. |
| Match payment | Yes | Yes | No | No | Protected obligations require individual confirmation. |
| Split transaction | Yes | No | No | No | Must not exceed unallocated amount. |
| Mark internal transfer | Yes | No | No | No | Requires paired-account or explicit reason evidence. |
| Track recurring bill | No | Creates obligation | Yes | No | Protected category confirmation required. |
| Not a bill | No | No | Yes | No | Does not delete posted history. |
| Defer/snooze | No | Yes | Yes | Yes | Deferral count recorded. |
| Cancel obligation | No | Yes | No | No | Individual reason for protected items. |
| Cancellation requested | No | No | No | Yes | Requires date; vendor evidence optional at this stage. |
| Vendor confirmed cancellation | No | No | No | Yes | Requires effective date and evidence note/reference. |
| Realized savings | No | No | No | Yes | System-computed only after bank verification. |
| Add note/evidence | Yes | Yes | Yes | Yes | Always available. |

When a mixed selection cannot support an action, the action remains visible but
shows the eligible count, skipped count, and reason. It never silently applies
to only some rows without preview disclosure.

## 10. Selection model

1. Checkboxes select objects, not visual rows.
2. Selection is scoped to the current object type unless the chosen action
   explicitly supports a mixed selection.
3. `Select all visible` selects only loaded rows matching the current filters.
4. A separate `Select all N matching` action is required for off-page results.
5. Header checkbox uses checked, unchecked, and indeterminate states.
6. Selected count and selected dollar total are always visible.
7. Selection survives sort, filter, drawer open/close, and page pagination.
8. Selection does not survive switching Finance destinations by default.
9. If a draft exists, navigation may preserve the draft but clears active
   selection after an explicit warning.
10. Locked rows explain why they cannot be selected.
11. During batch mode, conflicting row actions are disabled.
12. `Clear selection` never discards staged changes without warning.

## 11. Draft and save architecture

### 11.1 Draft scope

A Finance draft belongs to `(user, finance_workspace, dataset_revision)` and
contains ordered item changes. A draft can include transactions, obligations,
patterns, savings opportunities, and rule proposals, but every entry declares
its object type and action.

### 11.2 Draft durability

- Mirror the draft in browser storage for immediate crash recovery.
- Persist an encrypted server draft after a short idle interval and before
  deliberate Finance navigation.
- Browser and server drafts carry a revision and last-updated time.
- If both exist, merge only non-conflicting item changes. Present conflicts.
- Never put raw Plaid secrets, full account numbers, or attachments in browser
  storage.

### 11.3 Unsaved-change behavior

- Persistent save bar: `N unsaved changes` and dollar scope.
- `Save all changes` is disabled at zero changes.
- `Discard draft` requires confirmation when more than one change exists.
- Browser exit uses the standard unsaved-change warning.
- Internal navigation uses an application dialog with `Stay`, `Save and go`,
  and `Keep draft and go`.
- `Keep draft and go` is allowed only after server-draft persistence succeeds.
- Returning shows `Recovered N unsaved changes from TIME`.

### 11.4 Save transaction

1. Client requests a server preview using draft ID and revision.
2. Server reloads authoritative records and action eligibility.
3. Preview groups changes into safe, protected, conflicting, and invalid.
4. User confirms the exact preview; consequential actions require a reason.
5. Client submits preview token plus idempotency key.
6. Server applies all Agent database changes in one database transaction.
7. Server writes audit events and one batch record in that transaction.
8. Server returns the batch ID and queues external sync work.
9. Client clears only successfully committed draft entries.
10. External QBO/vendor tasks report separate sync results.

If authoritative data changed after preview, save stops and identifies the
conflicting rows. The user can refresh those rows without losing the rest of the
draft.

## 12. Bulk preview and confirmation

The preview answers:

- How many selected?
- How much money is represented?
- What exact fields or states will change?
- Which items are protected or skipped, and why?
- What happens to cash-plan, budget, savings, and bookkeeping views?
- Will a reusable rule be created?
- Are any external systems affected?
- Can the batch be undone, and for how long?

Preview sections are `Will change`, `Needs individual review`, `Will be skipped`,
and `External follow-up`. Confirmation buttons use concrete labels such as
`Save 18 classifications`, never generic `Confirm` for consequential actions.

## 13. Undo and recovery

- Each saved batch has one batch ID.
- Undo reverses only Agent-owned reversible decisions.
- Undo does not pretend to reverse an external email, vendor cancellation, QBO
  write, bank transaction, payroll event, or tax action.
- The confirmation page states exactly what undo can and cannot reverse.
- Undo is offered in the success message and Activity view.
- A superseded change cannot be blindly undone; show a conflict preview.
- Failed external sync remains retryable after an Agent decision is saved.

## 14. Page responsibilities

### 14.1 Today

Shows cash position, source freshness, decision blockers, and the single most
important next action. It may open the shared workspace but does not host a long
transaction queue.

### 14.2 Cash plan

Shows known income, required obligations, payment coverage, and end-of-month
trajectory. Actions focus on obligations and matches. Predicted expenses remain
separate from required cash.

### 14.3 Calendar

Shows past seven days, today, next fourteen days, and weekly roll-ups. Every
expense opens the shared workspace. Users may select by item, day, or week. Day
and week selection uses object IDs and the shared action registry.

### 14.4 Budget & savings

Owns six-month trends, category budgets, merchant roll-ups, savings queues,
potential/realized savings, and cancellation verification. It uses the shared
draft and no longer owns a separate save implementation.

### 14.5 Review

Owns ambiguous, conflicting, stale, overdue, protected, and failed-sync work.
It uses the shared batch bar, preview, save, and undo components. Review reason
codes come from the server.

### 14.6 Accounts & setup

Owns source connections, freshness, account roles, merchant rules, automation
thresholds, QuickBooks status, and data-health diagnostics. It does not edit
individual transactions.

## 15. Search, filter, sort, and saved views

The shared command bar supports:

- merchant and raw-description search;
- date range;
- amount range;
- account;
- direction;
- evidence status;
- planning status;
- savings state;
- category;
- recurring confidence;
- protected state;
- source and sync status;
- sort by date, amount, merchant, next charge, savings impact, or confidence.

Filters are encoded in the URL for reload/share continuity. User-named saved
views store filter/sort/column settings, not transaction data. The result count
and total value update together.

## 16. Cancellation workflow

Cancellation applies to a merchant service or future obligation, never to a
posted transaction.

1. Owner marks a savings opportunity `Waste` or `Cancellation recommended`.
2. Owner stages `Cancellation requested` with request date and optional note.
3. Vendor confirmation records confirmation reference and effective/final date.
4. System predicts the final permissible billing window.
5. Any later qualifying charge creates a `Charge after cancellation` review item.
6. After the verification window, the system marks `Bank-verified stopped` if
   no qualifying charge exists.
7. Realized savings are calculated against an explicit baseline and period.

The system never sends cancellation messages, cancels services, or changes real
payments without a separately authorized integration and confirmation flow.

## 17. Bookkeeping and QuickBooks

- Agent classification and QuickBooks synchronization are separate statuses.
- A category decision may optionally propose a merchant rule.
- Preview shows current QBO category, proposed category, historical reach, and
  conflicts with existing rules.
- Agent batch commits before external sync is queued.
- QBO success stores its authoritative reference.
- QBO failure creates a retryable Review item without rolling back the owner's
  Agent decision.
- A bulk retry includes only failed external writes.
- No category rule may alter payroll, tax, debt, transfer, or owner-draw
  treatment without individual confirmation.

## 18. AI behavior

AI may:

- summarize six-month merchant behavior;
- explain recurring evidence;
- propose savings classifications and categories;
- identify likely duplicates or overlapping tools;
- explain price increases and cash impact;
- draft vendor-contact language for the owner to use elsewhere.

AI may not:

- mark paid;
- claim a cancellation occurred;
- create settlement evidence;
- move money;
- execute payroll, tax, debt, vendor, or QBO writes;
- silently override deterministic protections;
- invent an amount, date, merchant identity, or confidence.

Every AI proposal includes evidence links, confidence, limitations, and a
non-AI fallback. AI output is never required to load or operate Finance.

## 19. API contract

Recommended endpoints:

- `GET /admin/finances/api/objects/{type}/{id}` — shared workspace detail.
- `GET /admin/finances/api/actions?type=&ids=` — eligibility registry result.
- `POST /admin/finances/api/drafts` — create/update encrypted server draft.
- `GET /admin/finances/api/drafts/current` — recover current draft.
- `DELETE /admin/finances/api/drafts/{id}` — discard draft.
- `POST /admin/finances/api/batches/preview` — authoritative preview.
- `POST /admin/finances/api/batches/apply` — atomic commit using preview token.
- `POST /admin/finances/api/batches/{id}/undo` — guarded undo.
- `POST /admin/finances/api/batches/{id}/retry-sync` — retry external failures.
- `GET /admin/finances/api/activity` — filterable audit history.

All write requests require authenticated actor, CSRF protection, idempotency
key, object revision, action reason when applicable, and allow-listed return
paths. Responses use safe user-facing errors plus trace/request IDs.

## 20. Data additions

Recommended additive tables:

### `finance_drafts`

`id`, `scope_key`, `actor_id`, `dataset_revision`, `draft_revision`,
`encrypted_payload`, `created_at`, `updated_at`, `expires_at`.

### `finance_action_batches`

`id`, `actor_id`, `preview_token_hash`, `idempotency_key`, `reason`, `status`,
`item_count`, `amount_cents`, `source_page`, `created_at`, `committed_at`,
`undone_at`.

### `finance_action_batch_items`

`id`, `batch_id`, `object_type`, `object_id`, `action`, `prior_state_json`,
`new_state_json`, `eligibility_result`, `skip_reason`, `external_sync_status`.

### `finance_cancellation_cases`

`id`, `merchant_key`, `opportunity_key`, `state`, `request_date`,
`vendor_confirmed_at`, `effective_date`, `expected_final_charge_date`,
`verification_window_days`, `confirmation_reference`, `baseline_cents`,
`verified_savings_cents`, `created_at`, `updated_at`.

Existing settlement allocations, source transactions, obligations, recurring
decisions, savings reviews, rules, and audit rows remain authoritative. Migrate
by reference; do not duplicate source cash facts.

## 21. Component contract

Implement shared server-rendered primitives with progressive enhancement:

- `FinanceTransactionRow`
- `FinanceTransactionCard`
- `FinanceObjectDrawer`
- `FinanceCommandBar`
- `FinanceSelectionCell`
- `FinanceBatchBar`
- `FinanceDraftBar`
- `FinanceBatchPreview`
- `FinanceEligibilityNotice`
- `FinanceEvidenceBadge`
- `FinanceActivityTimeline`
- `FinanceSaveConfirmation`
- `FinanceConflictRecovery`

Without JavaScript, forms navigate to preview and confirmation pages. JavaScript
adds drawers, in-place staging, draft persistence, stable rows, and live regions.

## 22. Important UI states

### Empty

Explain why no rows appear, retain active filters, and offer `Clear filters`.
Do not show success language if a source is missing.

### Loading

Keep headers, filters, column widths, and draft bar stable. Use skeleton rows for
predictable delayed data. Never replace the entire Finance shell with a spinner.

### Saving

Disable only the batch being saved, show exact progress, and keep unrelated rows
stable. A double submission reuses the idempotency key.

### Error

Preserve the draft and selections. Show one summary plus row-level reasons. Do
not expose stack traces, SQL, secrets, or raw provider payloads.

### Conflict

Show server value, draft value, and recommended resolution. Permit `Keep server`,
`Keep my draft` when still eligible, or `Review individually`.

### Stale source

Display the source and age. Allow classification work where safe, but block
evidence-dependent payment or realized-savings claims.

### Permission/protected

Show the row, lock icon, exact reason, and required next step. Do not merely
disable a control without explanation.

### Partial external sync

State that Agent decisions were saved and name the external system failures.
Offer retry; do not ask the user to repeat the full decision batch.

## 23. Accessibility

- Prefer native tables for tabular reading and native checkboxes for selection.
- Use an interactive grid only if spreadsheet-like keyboard navigation is
  deliberately implemented and tested.
- Selected rows expose `aria-selected`; the checkbox remains the operable
  control.
- Selection counts, draft changes, saves, failures, and undo results use polite
  live regions.
- Dialogs trap focus, have accessible names, close with Escape, and restore
  focus.
- Sorting exposes `aria-sort` and works with Enter/Space.
- Visible focus is never obscured by sticky bars.
- Status uses text plus color.
- Touch targets meet the shared control size.
- Reduced motion disables drawer/confirmation transitions without hiding state.

## 24. Responsive behavior

Desktop uses table rows, a sticky command/batch bar, and a side drawer. Phone
uses transaction cards, persistent checkboxes, a bottom batch tray, and a
full-screen workspace. Tables that remain tabular scroll inside their own
container and never widen the document.

The phone draft bar shows count plus `Review & save`; secondary actions are in a
menu. Selection, draft, filters, and scroll position survive rotation and
temporary app backgrounding.

## 25. Performance and reliability

- One canonical paginated query powers each transaction collection.
- Drawer details load on demand.
- Filters use indexed fields and return count plus amount totals.
- Selection stores IDs, not cloned row payloads.
- Draft writes are debounced and revisioned.
- Batch limits default to 200 items; larger result sets use server-side
  `Select all matching` jobs with preview.
- Every external sync is queued, retryable, and observable.
- Page operation never depends on AI availability.
- Metrics and logs include request, draft, batch, object, and provider IDs but no
  secrets or full account numbers.

## 26. Analytics

Track:

- time to review 25/100 transactions;
- draft created, recovered, discarded, and abandoned;
- selection size and action;
- preview conflicts and protected skips;
- batch success, failure, and undo;
- potential versus bank-verified realized savings;
- cancellation leakage charges;
- rule proposal acceptance and later correction;
- QBO sync success, failure, retry, and correction;
- keyboard and phone completion rates;
- page-specific entry into the shared workspace.

No analytics event includes raw descriptions, customer names, account numbers,
notes, or financial-provider secrets.

## 27. Rollout plan

### Phase 0 — contracts and safeguards

- Freeze vocabulary and action eligibility.
- Add shared read model, draft, batch, audit, and cancellation schemas.
- Add feature flags and dual-read comparison diagnostics.

### Phase 1 — shared workspace foundation

- Shared row/card, drawer, command bar, selection bar, draft bar, preview, save,
  and undo.
- Low-risk actions: Needed, Waste, Unknown, Investigating, category, note.
- Launch first in Budget & savings behind a flag.

### Phase 2 — Calendar and cross-page continuity

- Open shared workspace from Calendar.
- Add day/week selection and shared filters.
- Preserve source page state and show cash impacts.

### Phase 3 — Savings and cancellation

- Migrate savings opportunity decisions.
- Add cancellation case state machine and verification alerts.
- Keep potential and realized savings separate.

### Phase 4 — Review and evidence actions

- Migrate Review bulk actions, matching, defer, protected handling, and conflict
  recovery to the shared batch system.

### Phase 5 — Bookkeeping and QuickBooks

- Migrate category/rule decisions.
- Add queued QBO sync, retry, and external status visibility.

### Phase 6 — automation and consolidation

- Enable validated merchant rules and high-confidence suggestions.
- Retire page-specific save, bulk, drawer, confirmation, and undo code only after
  parity and production telemetry prove the replacement.

## 28. Acceptance criteria

1. The same transaction opened from Calendar, Budget, Savings, or Review shows
   identical facts and eligible actions.
2. Ten staged decisions cause zero permanent writes before `Save all changes`.
3. Refresh, crash, and accidental navigation recover the complete safe draft.
4. Internal navigation never silently discards a nonempty draft.
5. Preview identifies every changed, skipped, protected, and conflicting item.
6. A confirmed batch is atomic and returns one audit batch ID.
7. Retrying the same idempotency key cannot duplicate changes.
8. Undo accurately restores reversible Agent state and explains external limits.
9. A posted transaction is never changed to cancelled.
10. An obligation is never marked paid without posted settlement evidence.
11. A cancellation request never counts as realized savings.
12. Payroll, tax, and debt cannot receive risky bulk decisions.
13. AI output cannot perform or manufacture financial actions or evidence.
14. Page filters, sorting, selection, and drawer usage work by keyboard.
15. Desktop at 1280/1440 and phone at 375/390 have no page-level overflow.
16. Saving errors preserve draft and selection state.
17. QBO failure does not erase the owner's saved Agent classification.
18. All important state changes have actor, time, prior/new value, source page,
   and batch/rule identity.
19. Existing posted amounts, balances, source IDs, and settlement allocations are
   unchanged by migration.
20. Old page-specific action systems are removed only after their replacement
   passes functional and production visual parity.

## 29. Validation plan

### Automated

- unit tests for vocabulary and action eligibility;
- state-machine tests for savings/cancellation/payment statuses;
- draft merge, revision, expiry, and recovery tests;
- atomic batch, idempotency, protected-skip, conflict, and undo tests;
- allocation invariants and no-false-payment tests;
- QBO queue/retry tests;
- accessibility structure and progressive-enhancement tests;
- responsive HTML/CSS contract tests;
- migration comparison tests proving source facts are unchanged.

### Production/sandbox visual gate

- open one transaction from every Finance destination;
- stage several mixed decisions and navigate among Finance pages;
- refresh and recover the draft;
- verify selection, preview, protected skips, save, confirmation, and undo;
- exercise empty, loading, validation error, stale, conflict, permission, and
  partial-sync states using safe fixtures or read-only production checks;
- test 1280, 1440, 390, and 375 widths;
- verify keyboard order, focus return, live announcements, overflow, clipping,
  and console errors.

## 30. Traceability of the 200 requested upgrades

All 200 upgrades are included through the following requirement groups:

| Original range | Covered by |
|---|---|
| 1–10 Connected product | Sections 6, 7, 14, 21 |
| 11–20 Shared row | Sections 8 and 21 |
| 21–30 Selection | Section 10 |
| 31–40 Saving | Section 11 |
| 41–50 Confirmation | Section 12 |
| 51–60 Universal actions | Sections 9 and 19 |
| 61–70 Cancellation | Sections 5.3 and 16 |
| 71–80 Savings | Sections 5.3, 14.4, and 16 |
| 81–90 Budget | Sections 14.4 and 18 |
| 91–100 Calendar | Sections 14.3 and 24 |
| 101–110 Detail | Section 8 |
| 111–120 Search/filter | Section 15 |
| 121–130 Bookkeeping/QBO | Section 17 |
| 131–140 AI | Section 18 |
| 141–150 Automation/rules | Sections 7.5, 9, 18, and 27 |
| 151–160 Accessibility | Section 23 |
| 161–170 Mobile | Section 24 |
| 171–180 Reliability | Sections 11, 19, and 25 |
| 181–190 Audit/recovery | Sections 13, 19, and 20 |
| 191–200 Measurement/rollout | Sections 26 and 27 |

## 31. Decisions and recommended defaults

1. **Should selections persist across Finance pages?** No by default. Drafts
   persist; active selection clears after a warning. This prevents invisible
   cross-page batches.
2. **Should every classification autosave?** No. Stage all owner decisions and
   save once. Background persistence protects the draft, not the ledger.
3. **Should a transaction drawer include every possible action?** Show all
   relevant actions, but disabled actions must explain evidence or permission
   requirements.
4. **Should batch saves be partially successful?** Agent database decisions are
   atomic. External sync occurs afterward and may be partially synced with
   explicit retry.
5. **Should AI classify automatically?** Suggestions first. Later automation is
   allowed only for low-risk actions after measured accuracy and per-rule owner
   control.
6. **Should cancellation send vendor messages?** Not in this scope. Record and
   verify the case; any future sending integration requires separate authority.
7. **Should Bookkeeping remain a separate top-level page?** Recommended no after
   parity. Keep it as a saved transaction view/mode during migration.
8. **How long should drafts remain?** Recommended 30 days, with clear age and
   manual discard. Security review may shorten this.
9. **How long should undo remain available?** Recommended for the session and
   until a later dependent decision supersedes the batch.
10. **What is the first production slice?** Budget & savings low-risk decisions,
    because it already has staged saving and provides the safest migration path.

## 32. Non-goals

- Moving money.
- Running payroll.
- Paying taxes or debt.
- Automatically cancelling vendor services.
- Treating predictions as bills.
- Replacing QuickBooks as the accounting ledger.
- Rewriting historical posted bank facts.
- Building a spreadsheet clone.
- Adding a new frontend framework or second design system.
- Removing legacy flows before replacement parity is proven.

## Appendix A — Complete numbered requirement inventory

This appendix is the delivery checklist. The sections above define how these
requirements interact and take precedence if a short label below is unclear.

1. One shared transaction contract.
2. One permanent transaction ID.
3. One transaction workspace across pages.
4. One status vocabulary.
5. One action vocabulary.
6. One evidence vocabulary.
7. One selection behavior.
8. One confirmation behavior.
9. One undo behavior.
10. Preserve page context between transaction views.
11. Standardize merchant names.
12. Standardize amount placement.
13. Always show the transaction date.
14. Separate posted and planned dates.
15. Show a safely masked account label.
16. Show category consistently.
17. Show Paid, Unpaid, Partial, or Unconfirmed.
18. Show Planned, Unplanned, or Expected.
19. Show recurring confidence.
20. Put row actions in one location.
21. Add selection to every eligible row.
22. Select all visible rows.
23. Support indeterminate select-all.
24. Show selected count and value.
25. Use a sticky batch-action bar.
26. Preserve selection while opening details.
27. Preserve selection while sorting.
28. Preserve selection while filtering.
29. Provide Clear selection.
30. Explain ineligible rows.
31. Do not save classification clicks immediately.
32. Stage changes first.
33. Show an unsaved-change count.
34. Provide Save all changes.
35. Provide Discard draft.
36. Recover after refresh.
37. Recover after a crash.
38. Warn before unsafe exit.
39. Show draft recovery time.
40. Clear drafts only after server confirmation.
41. Preview every bulk change.
42. Show affected count.
43. Show affected value.
44. Show changed fields.
45. Distinguish reversible actions.
46. Require destructive-action reasons.
47. Require individual protected-cost confirmation.
48. Block incompatible mixed actions.
49. Revalidate immediately before save.
50. Explain every skipped or rejected item.
51. Mark Needed.
52. Mark Waste.
53. Mark Unknown.
54. Mark Investigating.
55. Change category.
56. Match an existing bill.
57. Split a transaction.
58. Mark an internal transfer.
59. Add a note.
60. Add supporting evidence.
61. Record Cancellation recommended.
62. Record Cancellation requested.
63. Record Vendor confirmed cancellation.
64. Record Waiting for final charge.
65. Record Bank-verified stopped.
66. Store cancellation request date.
67. Store expected final billing date.
68. Store confirmation references.
69. Flag charges after cancellation.
70. Calculate realized savings only after verification.
71. Use the shared workspace for Savings.
72. Group savings by merchant.
73. Show six-month merchant spend.
74. Show last-charge date.
75. Show average monthly cost.
76. Show next likely charge.
77. Show annualized potential savings.
78. Separate potential and realized savings.
79. Rank savings by impact and confidence.
80. Provide highest-savings-first review.
81. Batch-categorize transactions.
82. Set merchant budget treatment.
83. Apply confirmed decisions to matching history.
84. Propose rules for matching future transactions.
85. Compare actual spending to budget.
86. Show remaining category budget.
87. Warn before budget overrun.
88. Suggest budgets from six-month averages.
89. Explain unusual budget changes.
90. Show selected cuts' cash impact.
91. Open the shared workspace from Calendar.
92. Select a day's expenses.
93. Select across days.
94. Select a week.
95. Show daily paid/unpaid totals.
96. Show weekly paid/unpaid totals.
97. Show projected end-of-month cash after staged cuts.
98. Show planned expenses without payment evidence.
99. Flag unexpected posted transactions.
100. Provide review-before-next-charge queue.
101. Show raw bank description.
102. Show cleaned merchant.
103. Show source account.
104. Show diagnostic transaction identifiers safely.
105. Show QuickBooks linkage.
106. Show related recurring pattern.
107. Show related obligation.
108. Show prior merchant decisions.
109. Show similar transactions.
110. Show complete activity history.
111. Provide global Finance search.
112. Search raw descriptions.
113. Search cleaned merchants.
114. Filter payment evidence status.
115. Filter planning status.
116. Filter savings state.
117. Filter category.
118. Filter account.
119. Filter recurring confidence.
120. Save named views.
121. Open bookkeeping from any transaction.
122. Show current QBO category.
123. Show proposed QBO category.
124. Batch-approve bookkeeping suggestions.
125. Create merchant categorization rules.
126. Preview historical rule reach.
127. Detect QBO rule conflicts.
128. Separate Agent classification and QBO sync.
129. Retry failed QBO writes individually.
130. Show QBO sync audit history.
131. Explain recurring-charge evidence.
132. Summarize merchant behavior.
133. Suggest Needed, Waste, or Unknown.
134. Suggest bookkeeping category.
135. Identify likely duplicate subscriptions.
136. Identify price increases.
137. Identify overlapping tools.
138. Draft cancellation language without sending it.
139. Explain cash impact.
140. Keep every AI suggestion reviewable.
141. Create rules from confirmed decisions.
142. Auto-classify only above configured confidence.
143. Never auto-cancel.
144. Never auto-classify payroll as waste.
145. Never auto-write-off tax or debt.
146. Auto-flag post-cancellation charges.
147. Detect new recurring merchants.
148. Detect changed recurring amounts.
149. Surface upcoming renewals.
150. Let the owner disable rules.
151. Make selection keyboard accessible.
152. Expose selected state semantically.
153. Announce selection changes.
154. Announce save success and failure.
155. Preserve or restore useful focus.
156. Support conventional keyboard row selection.
157. Support Escape for drawers and dialogs.
158. Restore trigger focus after close.
159. Never use color alone for state.
160. Prefer native tables unless a grid is justified.
161. Use cards for complex phone rows.
162. Keep phone selection visible.
163. Use a phone bottom batch tray.
164. Keep Review & save accessible on phones.
165. Show selected count on phones.
166. Use a full-screen phone workspace.
167. Use accessible touch targets.
168. Avoid hover-only actions.
169. Preserve safe drafts while backgrounded.
170. Contain weekly-table scrolling.
171. Reuse canonical transaction queries.
172. Load drawer details on demand.
173. Paginate large histories.
174. Use stable skeleton loading.
175. Remember filter and sort preferences.
176. Use idempotency keys for batches.
177. Reject stale saves clearly.
178. Retry only failed external records.
179. Preserve successful Agent decisions when external sync fails.
180. Monitor action and save failures.
181. Record actor.
182. Record decision time.
183. Record prior value.
184. Record new value.
185. Record reason.
186. Record source page.
187. Give each batch an ID.
188. Provide guarded undo.
189. Provide filterable Activity history.
190. Provide an audit-history export with appropriate access controls.
191. Measure time to review 100 transactions.
192. Measure draft recovery.
193. Measure batch-save success.
194. Measure undo usage.
195. Measure reviewed cancellation candidates.
196. Measure potential versus realized savings.
197. Measure automated recurring recognition.
198. Measure QBO classification accuracy.
199. Release the shared workspace page by page.
200. Retire legacy action systems only after verified parity.
