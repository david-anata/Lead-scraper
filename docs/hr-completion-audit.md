# Anata HR completion audit

**Scope:** HR and payroll only; Finance/Plaid files and behavior are excluded

**Launch mode:** manual controlled payroll with handwritten checks

**First live period:** August 1–15, 2026, paid August 20, 2026

## Operating boundary

Agent prepares employee records, time, PTO, payroll inputs, calculation estimates,
cash impact, immutable approval packets, checks, statements, liability schedules,
and evidence. David gives final payroll approval. David or Val completes checks,
government payments, and filings outside Agent and records confirmation evidence.

There is no connected payroll provider. Plaid is not payroll. Square, bank transfer,
automatic tax payment, automatic filing, and automatic wage distribution are
disabled. A qualified payroll professional must independently review the 2026
calculation package and opening balances before the first live run.

## Requirement evidence

| Area | Current evidence | Status |
|---|---|---|
| Secure access | Authenticated HR routes, granular people/compensation/time/payroll/settings/audit permissions, session-bound CSRF, same-origin checks, scoped employee statements, sensitive-read audit events | Implemented |
| Employee setup | Employer-owned profile, hire date, classification, title, manager, one pay basis, effective-dated compensation history, status, team assignment | Implemented |
| Invitations and onboarding | Expiring app invitations, employee-only profile and W-4 ownership, sealed SSN, safe W-4 redisplay, I-9 employer review without document uploads, correction requests preserving signed history | Implemented |
| Teams | Unique names, active-employee manager requirement, roster drill-down, membership updates from team and employee workflows | Implemented |
| Time | One daily clock-in/out, exact elapsed seconds, duplicate protection, missed-punch requests, original/proposed/final correction history, independent review, period submission and approval | Implemented |
| PTO and holidays | Hire-date accrual, 90-day eligibility, 40-hour cap, no negative balance, weekday/holiday/overlap safeguards, observed holiday proposals separated from overtime | Implemented |
| Payroll inputs | Bonuses, commissions, reimbursements with evidence, deductions, garnishments, holiday adjustments, manual corrections, recurrence controls, independent review | Implemented |
| Payroll calendar | Semimonthly periods, Sunday–Saturday workweek, Saturday-to-Friday and Sunday-to-Monday pay-date rules, approved August launch default | Implemented |
| Calculation and readiness | Versioned 2026 federal/Utah rules, W-4 and opening-balance gates, overtime and employer tax estimates, negative-net protection, source hashes, immutable prepared versions | Implemented as planning estimate; qualified review required |
| Approval | Required named final approver, preparer/approver separation, deliberate typed approval, stale-version invalidation, critical blockers cannot be overridden | Implemented |
| Cash impact | Employee check cash, employee taxes, employer taxes, deductions, reimbursements, and total employer cost shown before approval | Implemented as planning estimate |
| Checks and statements | Unique manual check numbers, employee-only statements, clearing evidence, idempotency, void/reissue history | Implemented |
| Tax and filing evidence | Liability due dates, separate payment and filing confirmation, exact-amount reconciliation, mismatch exceptions, close gate | Implemented; government action remains human |
| Contractors | Separate engagement/tax-form/Wise readiness, flat-fee obligation, independent approval, Wise evidence, reconciliation outside W-2 totals | Implemented |
| Offboarding | Final-pay and evidence checklist, access suspension only at completion, retained employment/payroll history | Implemented |
| Reporting and recovery | Accountant CSVs, provider handoff CSV, private checksum backup, controlled Base44 preview/commit, append-only audit evidence | Implemented |
| Notifications | Privacy-safe daily action digest with deduplication and no compensation or tax-election detail | Implemented |
| Provider readiness | Versioned capability contract, forbidden sensitive payload fields, immutable request/version matching, authority/cutover gates | Implemented for a future service; submission disabled |
| Mobile/accessibility | 320px responsive forms/tables/navigation, textual status, touch targets, focus styles, reduced motion, semantic labels | Implemented; production visual verification required after each UI release |

## Human and external operational gates

These are not missing product features and Agent must not invent their outcomes:

1. Employees complete and sign their own W-4s.
2. A different person reviews each opening balance and timesheet.
3. A qualified payroll professional reviews the calculation package.
4. David or Val confirms EFTPS, Utah TAP, and Utah unemployment portal access.
5. David approves the exact frozen payroll version.
6. David or Val issues checks and records clearing evidence.
7. David or Val completes tax payments and filings and records official confirmations.

## Isolated pending decision

Valeria's older time correction remains a human-only decision. It is shown separately
from August payroll readiness because it cannot change the August 1–15 period. Another
authorized person must compare the original and proposed time and either approve it
with a review reason or deny it with a review reason. Agent does not choose.

## Completion standard

The HR product build is complete when all implemented rows above remain proven by
tests and deployed visual checks. The first real payroll is operationally ready only
after the human and external gates are evidenced for the August period.
