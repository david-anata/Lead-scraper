# Anata HR record retention policy

**Owner:** David or Val  
**Applies to:** Anata LLC HR, timekeeping, payroll-preparation, contractor-payment, and employment-compliance records  
**Policy date:** July 23, 2026  
**Review cadence:** Annually and whenever Anata changes payroll providers, employs people outside Utah, or receives a legal/accountant instruction

## Plain-English rule

Anata keeps the records needed to explain who worked, what they were paid, what was withheld, what was filed, who approved it, and how the payment was completed.

For normal HR and payroll records, Anata's operating standard is **seven years after the later of the record date, payment date, filing date, or employee separation date**. This is a conservative company policy that is longer than several federal and Utah minimums. It is not a claim that every record is legally required for seven years.

Form I-9 follows its own rule. A legal hold, audit, agency request, dispute, or accountant instruction always pauses deletion.

## Retention schedule

| Record | Anata operating period | Important handling rule |
|---|---:|---|
| Payroll runs, pay statements, earnings, bonuses, commissions, reimbursements, deductions, taxes, employer costs, checks, transfers, and payment evidence | 7 years | Keep the approved version, corrections, provider result, payment reference, and approver history together. |
| Time punches, corrections, approvals, schedules, holiday-pay records, and wage-rate tables | 7 years | Preserve the original entry and the correction trail; never overwrite history silently. |
| Federal and Utah payroll returns, W-2/W-3 records, 1099 records, tax deposits, unemployment reports, notices, and reconciliation evidence | 7 years | Keep filing confirmations and payment references with the related period. |
| Forms W-4 and replacement versions | 7 years after the later of replacement, last related filing/payment, or separation | Store sealed; restrict access; retain each version used to calculate withholding. |
| Employee profile, employment dates, job and team history, compensation changes, acknowledgements, and offboarding records | Employment plus 7 years | Deactivate access at separation; do not delete the employment history. |
| Form I-9 and any retained supporting copies | While employed, then until the later of 3 years after hire or 1 year after employment ends | Store separately and securely. Do not retain identity-document copies unless Anata adopts and consistently follows a reviewed copying policy. |
| Contractor agreements, approved fees, Wise/payment references, W-9s, and 1099 support | 7 years after the later of payment, filing, or contract end | Keep contractor records separate from employee wage records. |
| Payroll approvals, sensitive-data access logs, change history, exports, and recovery manifests | 7 years | Audit history is append-only. A backup manifest excludes sealed tax forms and full SSNs. |
| Invitation and password-reset records | 1 year after expiry or revocation | Keep token hashes and audit events only; never retain a reusable plaintext token. |
| Unsuccessful drafts that never became an employment, payroll, tax, or compliance record | Up to 1 year | A manager must confirm the draft has no legal, audit, dispute, or payment value before removal. |

## Official minimums that informed this policy

- The [IRS employment-tax recordkeeping guidance](https://www.irs.gov/businesses/small-businesses-self-employed/employment-tax-recordkeeping) says to keep employment-tax records for at least four years and includes wage payments, employee identity, employment dates, W-4s, deposits, returns, reimbursements, and supporting documentation.
- The U.S. Department of Labor's [FLSA Recordkeeping Fact Sheet #21](https://www.dol.gov/agencies/whd/fact-sheets/21-flsa-recordkeeping) states that payroll records must generally be preserved for at least three years and records supporting wage computations, such as timecards and wage-rate tables, for two years.
- Utah's [Minimum Wage Act recordkeeping rule](https://le.utah.gov/xcode/Title34/Chapter40/34-40-S201.html) calls for three years of covered payroll records. Utah's [unemployment insurance employer guidance](https://jobs.utah.gov/UI/Employer/Public/Questions/QuarterlyReporting.aspx) calls for four calendar years of the listed employee, pay-period, wage, bonus, commission, and separation records.
- The [USCIS Form I-9 instructions](https://www.uscis.gov/sites/default/files/document/forms/i-9instr.pdf) require an employer to retain Form I-9 while the employee works for the employer and, after employment ends, until the later of one year after termination or three years after the first day of employment.

These sources are operational guidance, not legal advice. David or Val should confirm unusual cases with the payroll provider, accountant, or employment counsel.

## Storage and access

1. Full Social Security numbers, W-4 data, and any Form I-9 are sealed sensitive records. Only people with the specific work need and permission may access them.
2. Employee-facing accounts may see only their own authorized records. Managers may see only assigned employees and only the fields needed for their role.
3. Every sensitive read, material change, approval, export, and provider handoff must leave an audit event.
4. HR exports must be stored only in an approved company location, not a personal email account or unapproved device.
5. The verified HR recovery ZIP is an operational backup, not a complete legal archive: it intentionally excludes sealed tax forms and full SSNs.
6. If a payroll provider becomes the official tax and wage authority, Anata must retain the provider's payroll results, filing confirmations, payment references, and reconciliation evidence alongside the Anata run.

## Offboarding and deletion

Offboarding deactivates the person's access and closes active assignments. It does not delete their records.

There is **no automatic deletion** in the HR app under this policy. Before any deletion, David or Val must:

1. calculate the applicable retention deadline;
2. check for a legal hold, audit, tax notice, wage dispute, correction, open filing, or accountant instruction;
3. verify a usable backup of records that must remain;
4. obtain a second human approval;
5. record what was removed, why, when, by whom, and under which retention rule.

If the correct deadline is uncertain, retain the record and ask the payroll provider, accountant, or employment counsel.

## Annual review checklist

- Confirm all workers and work locations; this policy currently assumes Utah W-2 employees and separately managed overseas contractors.
- Confirm the active payroll provider and which system is the statutory source of truth.
- Review the prior year's payroll, tax, unemployment, W-2/1099, payment, approval, and reconciliation evidence.
- Calculate terminated employees' Form I-9 retention dates.
- Review sensitive-access and export logs.
- Test an HR recovery download and checksum manifest.
- Record any legal holds or approved policy changes.
