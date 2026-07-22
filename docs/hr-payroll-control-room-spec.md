# Anata HR and Payroll Control Room

Status: discovery and operator interview in progress  
Scope: `agent.anatainc.com/admin/hr` and the Lead-scraper repository  
Out of scope: Finance/Plaid implementation and automatic bank movement

## July 22 operator decision update

This decision record supersedes the provider-dependent recommendations later in this
draft; those sections will be consolidated after the remaining payroll interview.

- Anata will not add or reactivate a monthly payroll subscription.
- Agent will become the system of record for payroll preparation, filing-grade federal
  and Utah calculations, explicit approval, printed-check issuance, tax-liability
  tracking, filing preparation, and reconciliation.
- David or Val may prepare a run. David is the required final approver. The approval
  must be recorded in Agent even when the business conversation happens verbally.
- Government payments and filings remain explicit human actions in their official
  portals. Agent records the receipt/confirmation and reconciles it to the liability;
  it must never label a payment or filing complete merely because it was calculated.
- Launch uses printed checks. A later bank-transfer option may record a transfer made
  through Anata's bank, but no Finance/Plaid work is authorized in this scope.
- The first live run covers August 1-15, 2026 and is paid August 20, 2026.
- The recurring semimonthly schedule is: 1st-15th paid on the 20th; 16th-month end
  paid on the 5th of the following month. Saturday paydays move to Friday and Sunday
  paydays move to Monday.
- The overtime workweek is Sunday through Saturday. All US employees work in Utah.
- Anata currently has Utah TAP access. EFTPS and Utah unemployment employer-portal
  access are unverified and are required launch-readiness checks.
- Overseas VAs paid through Wise remain a separate contractor-payment workflow and
  are not included in US employee withholding or unemployment calculations.
- The salaried employee's regular compensation is a fixed $1,000 per semimonthly
  paycheck rather than an amount dynamically derived from hours.
- Hourly employees are paid from exact clocked time; payroll must not apply a hidden
  rounding rule. Breaks and corrections must remain visible in the time record.
- The current salaried employee is treated as overtime-exempt. The exemption decision
  must be recorded on the employment record and cannot be inferred merely from being
  paid a salary.
- Hourly employees do not clock out for lunch or routine breaks; that time is paid.
- David and Val may correct a missed or inaccurate time entry. Each correction requires
  a reason and preserves before/after values, actor, and timestamp. David remains the
  final payroll approver even when he entered a correction.
- Overtime requires advance approval. Agent warns David and Val as an employee nears
  40 hours and creates a blocking review issue for unapproved overtime. However, a
  nonexempt employee who actually works more than 40 hours in the Sunday-Saturday
  workweek must still receive the legally required overtime premium; approval controls
  scheduling and policy compliance, not whether earned wages are paid.
- Reimbursements require a receipt and business purpose, are reviewed separately, and
  are not silently combined with taxable wages.
- David or Val may enter bonuses and commissions. They are included in the immutable
  payroll snapshot and David confirms them during final payroll approval.
- Accurate 2026 year-to-date wages, employee withholding, employer tax, and tax-deposit
  balances will be recovered from prior payroll records before the August 20 run.
  Starting the application database fresh does not reset statutory year-to-date totals.
- EFTPS access recovery is deferred and does not block product discovery or building.
  It remains a dated readiness item before the first federal deposit is due. Agent may
  show another IRS-authorized electronic payment route as a contingency, but it may not
  mark the federal liability paid without a real confirmation.
- Launch does not require preprinted payroll-check stock. David may handwrite an
  ordinary business check; Agent records the check number, issue date, employee, net
  amount, status, and any void/reissue history.
- Employee time entry must work on both phones and computers. The first app experience
  should be a responsive installable web app with secure employee invitations, not a
  separate app-store release.
- Do not collect GPS location for time punches by default. Location is unnecessary for
  the stated operating need and adds privacy and support cost.
- Time entry is deliberately simple: one clock-in and one clock-out for the workday,
  with additional punch pairs only when correcting an interruption. Do not add clients,
  projects, job codes, or allocation screens. Agent calculates paid duration and flags
  missing, reversed, overlapping, or implausibly long punches for David or Val to fix.
- Secure employee invitation onboarding collects personal/contact information,
  emergency contact, federal W-4 elections, required acknowledgements, and employee
  attestations. David or Val completes the employer-side review and approval, including
  the separate I-9 document-verification workflow. Emergency contacts and identity/tax
  documents are restricted to specifically authorized HR administrators and never
  appear in ordinary manager or payroll-summary views.

### Calculation and compliance boundary

Agent may calculate payroll, but only through an effective-dated, tested rules engine
based on the official IRS and Utah publications for the applicable pay date. Each run
must preserve the rule/table version, employee elections, year-to-date values, inputs,
rounding, results, and approval snapshot used. Updates to tax rules are reviewed and
activated as controlled configuration changes, never silently inferred.

Agent will prepare and track, but not silently submit:

- federal payroll-tax deposits through EFTPS;
- federal Forms 941 and 940;
- Utah withholding returns/reconciliation through TAP;
- Utah unemployment wage reports and contributions;
- W-2/W-3 preparation and submission through SSA Business Services Online.

The first production run requires a documented parallel review by a qualified payroll
accountant or tax professional. This is a one-time launch control, not a requirement to
buy a recurring payroll subscription.

## Executive decision

Anata should build an HR and payroll **control room**, not a payroll engine.

The control room should collect employee and time inputs, show whether the next payroll is ready, obtain exact totals from Anata's payroll provider, explain the expected cash requirement, flag exceptions, and record explicit human approvals. The selected payroll provider remains responsible for gross-to-net calculation, withholding, filing, payslips, and moving money.

No payroll provider is currently integrated in this repository. Discovery with the
operator confirmed that Anata **previously used QuickBooks Online Payroll but later
downgraded and no longer has an active payroll subscription**. QBO Payroll is the
historical provider and the leading reactivation candidate, not the current payroll
authority. Filing-grade payroll and direct deposit must remain disabled until QBO
Payroll is reactivated or another provider is selected and configured. Plaid is not a
payroll provider. Stripe-related columns exist only in the inherited Base44 data
model; there is no Stripe SDK, configuration, service, route, or execution code in
Agent. They must not be treated as proof of a live payroll integration.

The operator chose a clean start and does not want to reactivate the former QBO
Payroll subscription. Build the three-person HR/time/check workflow first. Before
enabling production tax results or bank transfer, name either a payroll service or a
qualified accountant/payroll professional responsible for calculation, deposits, and
filings. Do not build a generic multi-provider layer or make QBO a dependency.

## Discovery findings

### What exists today

- The HR section is mounted under `/admin/hr` and uses the same signed-session authentication as the rest of the admin application.
- Working pages exist for the HR dashboard, employees, employee add/edit, and teams.
- Time, payroll, reports, and payroll settings are visible navigation destinations but are “coming soon” placeholders.
- Employee editing currently covers email, name, HR role, employee type, team, active/inactive status, hourly rate, annual salary, and phone.
- The database already contains 12 HR-oriented tables for employees, teams, settings, schedules, pay periods, time entries, payroll runs, payroll line items, paychecks, printed checks, handbooks, and acknowledgements.
- Money is stored as integer cents, which is appropriate for exact totals.
- Existing tests verify table creation, basic employee/team operations, duplicate-email handling, authentication, and the split between general HR access and payroll access.
- The public production URL redirects an unauthenticated request to `/admin/login`, as expected. Authenticated production records were not inspected during this pass.

### Important gaps and risks

1. **No provider connection.** There is no source for authoritative gross pay, employer taxes, deductions, net pay, debit date, or provider run status.
2. **The current copy overpromises.** The payroll placeholder says Anata will “compute gross/taxes/net and pay employees.” That is the wrong ownership model and should be replaced when the control room is built.
3. **Permissions are too broad.** `hr.access` permits both reading and changing employees/teams. `hr.payroll` combines viewing sensitive payroll, editing settings, and eventually running payroll. Preparation and final approval are not separated.
4. **No approval ledger.** Existing payroll rows have an initiator but no immutable prepare/review/approve/submit event history, approval comments, version/hash of what was approved, or dual-control policy.
5. **No write audit for HR changes.** Employee and team edits are not written to the application's audit service.
6. **Sensitive data needs stronger boundaries.** The schema includes date of birth, partial SSN, W-4 fields, address, I-9 notes, bank labels, and compensation. The current UI exposes only a subset, but the database has no field-level access model, encryption marker, retention policy, or access log for these records.
7. **Weak data relationships.** Several relationships are strings rather than foreign keys: employee email, team ID, pay schedule ID, payroll run ID, and pay period ID. Email changes and imports can orphan records.
8. **Input validation is minimal.** Invalid money becomes zero, both hourly and salary fields can be populated, team managers need not be employees, team names can duplicate, and no employment dates or provider IDs are captured.
9. **Status models do not match a control room.** Payroll statuses such as `processing/completed/failed` are not enough to represent data collection, issue resolution, readiness, approval, provider submission, cancellation, and reconciliation.
10. **Mobile usability is not implemented.** The fixed 232px side navigation, six-column employee table, two-column forms, and side-by-side actions have no HR-specific small-screen behavior. The pages will be cramped or overflow on phones.
11. **Accessibility coverage is thin.** Forms lack explicit `for`/`id` associations, flash messages are not live regions, statuses use emoji/color, tables have no mobile alternative, and there are no HR accessibility or viewport tests.
12. **Migration is unfinished.** The models retain `base44_id`, but no importer was found. Production data completeness and ownership must be confirmed before the new UI becomes authoritative.

## Product definition

### Primary users

- **HR preparer:** maintains worker records, collects changes, fixes missing information, and prepares a run.
- **Manager/time approver:** confirms only the time and variable pay for assigned workers.
- **Payroll approver:** reviews the exact provider-backed totals and gives explicit final approval.
- **Executive/read-only:** sees readiness, cash requirement, issues, and history without seeing unnecessary personal details.
- **Employee:** future self-service access to their own profile, time, and documents; not required for the first payroll-control release.

### Core rule

Anata may prepare, compare, explain, and request approval. It must not calculate statutory payroll, invent tax values, file taxes, or distribute wages. Provider data is authoritative for gross-to-net and payroll execution.

### One-screen questions

The payroll control room must answer, in one scan:

1. When is the next payroll, and what is the provider cutoff?
2. Are all people, hours, reimbursements, commissions, and changes ready?
3. What exact amount will leave the business, on what date, and what is included?
4. What is missing, inconsistent, or blocking submission?
5. Who prepared, reviewed, and approved this exact version?
6. What is the single next action?

## Target workflow

1. **Open next run.** Import or refresh the next payroll run and deadlines from the chosen provider.
2. **Collect inputs.** Gather approved hours, salary changes, bonuses, commissions, reimbursements, deductions, new hires, and terminations. Each item has an owner and source.
3. **Validate readiness.** Run deterministic checks: missing provider worker ID, unapproved time, conflicting pay types, unexpected change from prior run, incomplete onboarding, invalid effective dates, and provider sync errors.
4. **Review cash impact.** Show the provider's exact debit total when available: employee net pay, employee taxes withheld, employer taxes, benefits/deductions, fees, and total cash debit. If totals are still preliminary, label them “estimate” and block final approval.
5. **Prepare run.** A preparer freezes an input version. Any later edit invalidates approvals and creates a new version.
6. **Independent review.** A second authorized person reviews the exceptions, totals, differences from the previous run, and source freshness.
7. **Explicit approval.** The approver sees a final confirmation stating the amount, debit date, employee count, provider, and unresolved warnings. Approval requires a deliberate action and optional comment; critical blockers cannot be overridden.
8. **Submit to provider.** Only after approval, Anata may call the provider's supported submit/approve endpoint if available. If the provider requires its own UI, Anata records “approved in Anata” and deep-links to the provider; it must not claim submission occurred until provider confirmation is received.
9. **Track outcome.** Provider webhooks/polling update submitted, accepted, processing, paid, failed, or cancelled. Store provider IDs and raw-event references for auditability.
10. **Close run.** Mark complete only from provider evidence. Bank verification through Finance/Plaid is a separate future concern and is not part of this build.

## Screen plan

### HR home

Replace generic counts with an action-oriented overview:

- next payroll date and cutoff;
- readiness state and blocking issue count;
- expected payroll cash requirement with source and freshness;
- onboarding/offboarding tasks due;
- one primary next action;
- recent approved changes.

### Payroll control room

- Run header: period, pay date, cutoff, provider, last sync, status.
- Readiness checklist: people, time, variable pay, changes, provider connection.
- Cash impact: exact provider totals by evidence class and change versus prior run.
- Issue queue: severity, affected person, reason, owner, resolution route.
- Employee summary: name, pay type, hours/changes, gross, employer cost, net, readiness; sensitive details hidden by default.
- Approval timeline: prepared, reviewed, approved, submitted, confirmed, including actor and timestamp.
- Sticky next-action bar on desktop; compact bottom action bar on mobile.

### People

- Search, status/type/team filters, stable result count, and pagination.
- Separate profile, employment, compensation, compliance, provider connection, and history sections.
- Compensation visible only to roles that need it.
- New hire/offboarding checklists with ownership and due dates.
- Deactivate rather than delete; preserve payroll history.

### Time and variable pay

- Pay-period view with missing/late/approved states.
- Manager-scoped approval for assigned workers only.
- Bulk approval requires a review summary and audit event.
- Import option for the provider or time source if Anata does not become the time clock.

### Settings and connections

- Provider connection status, company/provider identifiers, scopes, last sync, webhook health, and reconnect action.
- Pay schedules and cutoff rules imported from the provider where possible.
- Approval policy: preparer and approver separation, threshold rules, emergency override owners.
- Never expose tax-rate fields as if Anata owns statutory calculation.

## Data and integration changes

Keep the existing tables for historical compatibility, but add provider-native and audit-safe records rather than forcing new behavior into inherited fields.

### Add

- `hr_provider_connections`: provider, company ID, encrypted credential reference, scopes, status, last sync, last error.
- `hr_provider_workers`: employee ID, provider worker ID, sync status, last verified time.
- `hr_payroll_runs`: add provider run ID, cutoff, debit date, currency, provider status, readiness status, input version, source freshness, submitted/confirmed timestamps.
- `hr_payroll_run_versions`: immutable snapshot and content hash of employees, inputs, totals, and issues.
- `hr_payroll_issues`: rule key, severity, affected entity, owner, status, resolution, timestamps.
- `hr_payroll_approvals`: run/version, stage, decision, actor, role, comment, timestamp, snapshot hash.
- `hr_payroll_events`: append-only provider and user event ledger with idempotency key and redacted payload reference.
- `hr_employee_changes`: effective-dated, approval-aware compensation and employment changes.

### Correct

- Use numeric employee/team/run/period foreign keys for new records. Keep `base44_id` only as migration provenance.
- Treat provider-returned totals as snapshots, not locally editable calculations.
- Store OAuth/API secrets encrypted and outside ordinary rows; show only connection metadata in the UI.
- Do not persist full bank account numbers or full SSNs. Prefer provider-hosted onboarding for sensitive identity, tax, and payment details.

### Provider service contract

After the provider is chosen, implement one concrete service with:

- connection/authentication;
- company and worker mapping;
- upcoming run and deadline retrieval;
- run totals and employee-line retrieval;
- supported input updates, if authorized;
- submit/approve action, if the provider supports it;
- webhook verification and idempotent event handling;
- clear errors and retry behavior.

Do not create multiple unused provider adapters in phase one.

## Permissions and security

Replace the two broad permissions with least-privilege capabilities:

- `hr.people.view`
- `hr.people.manage`
- `hr.compensation.view`
- `hr.compensation.manage`
- `hr.time.manage_own`
- `hr.time.approve_team`
- `hr.payroll.view`
- `hr.payroll.prepare`
- `hr.payroll.approve`
- `hr.payroll.submit`
- `hr.settings.manage`
- `hr.audit.view`

Required controls:

- Default deny; existing super-admin remains break-glass only.
- A preparer cannot be the sole final approver for the same run.
- Manager access is limited to assigned workers and approved fields.
- Employee access, when added, is limited to the employee's own record.
- Every HR write and sensitive read is auditable; payroll events are append-only.
- CSRF protection for browser forms, secure/same-site cookies, re-authentication for final approval, and rate limiting for sensitive actions.
- Encrypt provider tokens and high-risk PII at rest; redact logs and error messages.
- Define retention and deletion rules with legal/payroll counsel before collecting more PII.
- Critical actions use idempotency keys and fail closed when provider state is uncertain.

## Mobile and accessibility requirements

- At 768px and below, replace the fixed left rail with a labeled section menu; do not consume 232px of the viewport.
- Employee and issue tables become stacked summaries with a details disclosure; no page-level horizontal scrolling.
- Two-column forms collapse to one column; action buttons become full-width with at least 44px touch targets.
- Keep the approval summary visible without covering content; require a confirmation screen before final approval.
- Use explicit labels, field help, error summaries, keyboard focus, semantic tables/forms/dialogs, and `aria-live` for sync and validation results.
- Status must use text plus icon/color. Respect reduced motion and maintain usable contrast.
- Test 320px, 375px, 768px, 1280px, and 1440px widths, keyboard-only use, and screen-reader names.

## Delivery phases

### Phase 0 — provider and production-data confirmation

1. Name the provider currently used, account owner, plan/API access, payroll states, pay schedule, and required approval behavior.
2. Inventory live employee/payroll records and any Base44 export without copying sensitive values into tickets or logs.
3. Confirm whether the provider supports read totals, write inputs, approve/submit, and webhooks.
4. Choose the first release boundary: provider deep-link approval or API submission.

Exit: provider and source-of-truth decision is signed off.

### Phase 1 — safe HR foundation

1. Add granular RBAC and preserve backward-compatible role migration.
2. Add audit/event tables, proper relationships, effective-dated changes, and provider IDs.
3. Strengthen validation, CSRF, secret handling, redaction, and HR write logging.
4. Improve People/Teams mobile and accessibility behavior.

Exit: people changes are validated, permissioned, and fully auditable.

### Phase 2 — read-only provider-backed control room

1. Connect the chosen provider with least-privilege read scopes.
2. Display upcoming run, cutoff, exact/preliminary totals, readiness checks, issues, and prior-run comparison.
3. Add refresh/webhook health and reliable idempotent synchronization.
4. Keep approval and submission disabled.

Exit: Anata accurately mirrors provider state and labels stale or incomplete data.

### Phase 3 — preparation and approvals

1. Add input collection, run versioning, issue resolution, prepare/review/approve stages, and approval invalidation after edits.
2. Require independent approval and re-authentication.
3. Add provider deep-link completion first unless a well-supported submit API is available.

Exit: a complete payroll packet can be prepared and explicitly approved with an immutable record.

### Phase 4 — controlled provider submission

1. Enable submission only if the provider offers a stable supported endpoint.
2. Add idempotency, timeout recovery, webhook confirmation, failure handling, and emergency stop.
3. Roll out to one payroll cycle in shadow mode, then one controlled live run with manual provider verification.

Exit: Anata can submit an already approved version without calculating taxes or moving money itself.

## Acceptance criteria

- An unauthenticated user is redirected to sign-in; an unauthorized user receives no HR or payroll data.
- A people viewer cannot see compensation unless separately granted.
- A manager can approve only assigned workers' time.
- The next payroll shows provider, cutoff, pay date, source freshness, and whether totals are exact or preliminary.
- Final approval is impossible with stale provider totals, critical issues, or unapproved required inputs.
- Final approval shows total debit, debit date, employee count, provider, version, and approver identity.
- Any edit after preparation creates a new version and invalidates previous approvals.
- A preparer cannot be the only approver.
- Duplicate clicks/retries cannot create duplicate provider submissions or audit events.
- Provider failure never changes a run to submitted/paid without provider evidence.
- Every HR write, approval, submission attempt, and provider state change is attributable and timestamped.
- No full SSN or bank account is stored or logged by Anata.
- All HR workflows are usable at 320px width and with keyboard-only navigation.
- Finance/Plaid files and behavior remain unchanged.

## Validation plan

- Unit tests for permissions, state transitions, readiness rules, version hashes, approval invalidation, money formatting, and provider payload mapping.
- Integration tests with provider sandbox/fixtures for refresh, webhooks, duplicate events, timeouts, stale data, and submit idempotency.
- Route tests for unauthenticated, unauthorized, preparer, manager, approver, and super-admin paths.
- Security tests for CSRF, horizontal/vertical privilege escalation, sensitive logging, token encryption, webhook signatures, and replay attacks.
- Visual tests at the required mobile/desktop widths plus keyboard and screen-reader checks.
- Shadow comparison against the provider for at least one full payroll cycle before enabling any submit action.

## Open decisions

1. Which payroll provider is authoritative today? **Recommended default: the provider Anata already uses.**
2. Does that provider's current plan expose API/webhook access?
3. Who may prepare, independently approve, and submit payroll?
4. Is the first release read-only plus provider deep link, or direct provider submission?
5. Where do hours, commissions, reimbursements, benefits, and deductions originate today?
6. Which Base44 records must be migrated, and has their completeness been verified?
7. Which roles may view compensation, tax-document status, and compliance details?

No implementation beyond this specification should begin until decisions 1–4 are answered.

## Base44 source-repository migration assessment

Source reviewed: `anatavon/anata-hr`, default branch at commit
`0dd8e4c1a14065db57bef0a63d48d4c393d56748` (June 16, 2026).

### Confirmed source architecture

- React/Vite frontend using the Base44 SDK and Base44-hosted entities/functions.
- Multi-organization data model with Base44 users, organizations, teams, time entries,
  pay periods, schedules, settings, payroll runs/lines, paychecks, printed checks, and
  handbook acknowledgements.
- Employee clock-in/time entry, manager timesheet review, freeze/unfreeze, schedules,
  employee onboarding, W-4/I-9 fields, reporting, payroll preview, printed checks, and
  payroll history are implemented.
- Stripe Connect Express onboarding creates a connected account for each employee.
- The `runPayroll` Base44 function calculates gross pay, federal withholding, Social
  Security, Medicare, state withholding, deductions, and net pay inside the app.
- The same function can send net-pay transfers directly to employee Stripe accounts.

### Critical migration boundary

The Base44 payroll calculation should be retained as a **planning and reconciliation
calculator**, but it must not be the statutory source used to decide withholding or
tax filings. Payment execution should be rebuilt behind approval and provider-backed
payroll results rather than copied unchanged.

The source code itself says its US tax model is intentionally simplified and not
tax-accurate. Specific omissions include accurate bonus withholding, year-to-date
Social Security wage-base tracking, progressive state rules, Additional Medicare,
FUTA, and SUTA. It also does not file taxes or remit withheld amounts. A Stripe
transfer only distributes money; it does not make this a compliant payroll system.

Until a proper payroll provider is chosen and connected:

- disable/omit `execute` and `send_single` behavior;
- do not migrate `STRIPE_SECRET_KEY` or employee Stripe account credentials;
- preserve the calculator for estimates, scenario planning, regression tests, and
  discrepancy alerts, but label every result as non-filing-grade;
- do not present locally calculated tax/net figures as authoritative;
- preserve historical Base44 results only as clearly labeled legacy records;
- make all new payroll screens preparation/read-only controls with no money movement.

After a provider is connected, Anata may compare its planning calculation with the
provider result and flag unexplained differences. The provider result controls the
check/direct-deposit amount, tax liabilities, filings, and pay statement.

### Safe-to-migrate capability map

| Base44 capability | Agent destination | Treatment |
|---|---|---|
| Employee and team records | Existing HR People/Teams | Import after validation and deduplication |
| Employee type and compensation | HR employment/compensation records | Import with restricted access and effective dates |
| Time entries | HR Time | Port rules and history; replace email links with employee IDs |
| Pay-period freeze | HR Time approval | Port as manager approval with immutable audit event |
| Pay schedules | HR Settings | Port scheduling data, but provider remains authoritative |
| W-4/I-9 status | HR compliance | Import status/metadata only after access and retention review |
| Handbook/acknowledgements | HR Documents | Port content links and acknowledgement evidence |
| Payroll run/line history | Legacy payroll history | Read-only import; label calculations as Base44 legacy estimates |
| Payroll preview calculation | Payroll control room | Keep as estimate; compare against provider-returned totals |
| Stripe onboarding/transfers | Payment history only | Do not copy unchanged; replace with provider-backed, approved payment flow |
| Printed checks | Legacy documents | Preserve only if legally/operationally required |
| Reports | HR Reports | Rebuild from permission-filtered Agent data |

### Complete payroll capability target

The intended product is mini-BambooHR parity for Anata, including:

- employee directory, employment history, compensation history, teams, onboarding,
  offboarding, documents, handbook acknowledgements, PTO/leave, and permissions;
- employee time entry, manager correction, overtime visibility, pay-period approval,
  and frozen payroll inputs;
- bonuses, commissions, reimbursements, deductions, benefits, garnishments, and
  effective-dated pay changes;
- local payroll estimate plus authoritative provider calculation and variance review;
- printed checks with controlled check numbers and void/reissue flow;
- direct deposit when enabled through the payroll provider;
- secure employee pay statements and email notifications (email should link to an
  authenticated statement rather than attach sensitive payroll data);
- tax-liability schedule, filing/deposit status, W-2/1099 availability, and exceptions;
- explicit prepare, independent review, approve, and submit stages;
- reconciliation by employee and liability: approved amount, check/direct deposit,
  provider result, tax deposit, reversal/void, and final status;
- immutable audit history and exports for the accountant.

Reconciliation must distinguish `calculated`, `provider-confirmed`, `approved`,
`issued`, `cleared/confirmed`, `tax deposited`, `filed`, `voided`, and `exception`.
Bank/Plaid evidence can be connected later through a narrow read-only interface, but
Finance/Plaid implementation remains outside this HR migration.

### Data migration sequence

1. Export Base44 entities through an authorized, read-only export mechanism.
2. Save an encrypted, access-limited source archive and record export time/counts.
3. Run a dry-run mapper that validates required fields, duplicates, orphaned references,
   currencies, dates, organization ownership, and money conversion to integer cents.
4. Import teams and employees first, recording `base44_id` on every mapped row.
5. Import schedules, pay periods, time, handbooks, and acknowledgements.
6. Import payroll runs, lines, paychecks, and checks into a read-only legacy namespace.
7. Reconcile entity counts and sampled totals; produce an exceptions report.
8. Obtain human sign-off before the Agent records become the operational source.
9. Keep Base44 read-only during a defined verification window; do not delete it as part
   of the migration.

The repository contains schemas and code, not the live Base44 records. Actual data
migration therefore requires an authorized Base44 export or API credential with
read access; GitHub collaborator access alone cannot retrieve production entity data.

## Recovered Base44 export inventory - July 22, 2026

The supplied `Anata HR data table export.zip` contains real, non-sample Base44 data
and is usable as a partial migration source.

### Recovered

- 1 organization
- 2 teams
- 133 time entries spanning March 23 through July 22, 2026
- 17 pay periods: 10 frozen and 7 pending
- 10 completed payroll runs
- 25 payroll line items
- 23 printed checks
- 1 standalone paycheck record
- 1 payroll-settings record
- 6 distinct employee email identities referenced across operational records

### Integrity results

- Every payroll line item references an exported payroll run.
- Every printed check references an exported payroll run and line item.
- No payroll line has more than one exported printed check.
- Exported run gross, net, deduction, and employee-count totals reconcile to the
  exported payroll line items within one cent.
- Payroll lines total $18,752.36 gross, $2,632.48 deductions, and $16,119.88 net.
- Printed checks total $15,888.22 net. The difference must remain an explicit
  migration exception until matched to the standalone paycheck, failed line, or
  another payment record.
- Payroll line statuses are 24 pending and 1 failed; printed-check statuses are all
  `ready`. These statuses prove record creation, not that checks cleared or taxes
  were deposited/filed.
- All 133 time entries lack a `pay_period_id`; period membership must be derived from
  employee identity plus date, then reviewed.
- Three time entries lack a stored hours value and require exception handling.

### Still needed from Base44

Export these tables if they are available:

1. `User` - critical employee master, role, compensation, tax-state, onboarding,
   Stripe reference, and compliance metadata
2. `PaySchedule`
3. `EmployeeHandbook`
4. `HandbookAcknowledgement`

Also export any audit/activity history Base44 exposes. Do not export secrets or full
bank credentials into a ZIP.

### W-9 review

The supplied W-9 visually confirms company identity, address, EIN presence, and a
signature/date. Despite the filename saying “Unsigned,” the rendered form appears
signed. Treat it as highly sensitive and keep it out of Git and application logs.
The LLC tax-classification entry should be confirmed by the accountant before using
the form for provider onboarding. A W-9 supports identity setup but does not replace
payroll registrations, deposit schedules, state/local tax accounts, or year-to-date
employee payroll records.
