# HR data recovery findings

**Reviewed:** July 26, 2026

**Source:** `Anata HR data table export.zip`
**Archive SHA-256:** `e75acf73023278d11260295630467c786f63e9ab4438951bd58fc349537e4f75`

## What is recoverable

The Base44 export is a real data-table export, not just application source. It contains:

| Source table | Rows |
|---|---:|
| Organization | 1 |
| Pay periods | 17 |
| Paychecks | 1 |
| Payroll line items | 25 |
| Payroll runs | 10 |
| Payroll settings | 1 |
| Printed checks | 23 |
| Teams | 2 |
| Time entries | 133 |

The recovery preview identifies five non-sample employee email identities:

- four have names in issued-check/payroll history;
- one unnamed identity appears only in two time entries and requires alias review;
- three named identities show hourly-rate history;
- one named identity shows fixed-pay history;
- five sample/test rows are excluded automatically.

The history covers time from March 23 through July 22, 2026, and payroll pay dates
from April 10 through July 20, 2026.

## Reconciliation decision

Opening-balance proposals use the 23 non-voided printed checks, not all 25 payroll
line items. Two payroll calculations lack corresponding issued-check evidence and
must not inflate year-to-date wages.

The recovery preserves all calculation rows as history, including failed or duplicate
attempts, while using issued checks as the stronger paid-wage signal.

## QuickBooks status

The connected QuickBooks app currently requires reauthentication for payroll scopes.
The attempted read-only calls for company payroll data, employees, the latest payroll
run, and deductions all returned `UNAUTHORIZED` with a reauthentication requirement.

QuickBooks is therefore a later reconciliation source, not a blocker to recovering the
Base44 history. After reconnection, it should be used to confirm:

- current active employees and current compensation;
- 2026 year-to-date taxable wage bases and withholding;
- company payroll/tax account setup;
- deductions and the last authoritative payroll run.

## Import controls

The Anata recovery tool:

- previews before writing;
- requires the same archive fingerprint at commit;
- rejects unexpected, encrypted, oversized, or path-containing ZIP files;
- excludes sample rows;
- imports idempotently using Base44 record IDs;
- creates newly discovered employees as inactive;
- does not send invitations;
- does not activate Base44 tax settings;
- creates unapproved opening-balance candidates from issued-check evidence;
- records one redacted audit event;
- never imports full SSNs or bank details, files taxes, or moves money.
