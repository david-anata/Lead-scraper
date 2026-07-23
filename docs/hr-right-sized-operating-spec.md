# Anata HR — Right-Sized Operating Specification

Status: build-ready product specification; no implementation authorized by this document  
Audience: David, Val, employees, implementation team, payroll reviewer/accountant  
Scope: three Utah W-2 employees and overseas contractors paid through Wise  
Target: a simple employee experience and a controlled employer payroll workflow without a recurring payroll subscription

## 1. Outcome

Anata HR should let:

- an employee securely join Anata, maintain their own information, clock in and out,
  request PTO, acknowledge policies, and retrieve pay statements;
- Val or David review employee setup, correct time, approve PTO, prepare variable pay,
  and assemble a payroll run;
- David review the exact employee pay, tax liability, employer cost, and required cash,
  then explicitly approve the frozen payroll version;
- David or Val issue handwritten checks, complete required government payments and
  filings, and attach confirmation evidence;
- the system prove what was calculated, approved, issued, deposited, filed, corrected,
  or left unresolved.

The product is not intended to copy every BambooHR module. It is a small-company HR,
time, payroll-preparation, and compliance control system for Anata's actual workforce.

## 2. Verified starting point

The live Agent HR section currently provides:

- authenticated HR pages;
- employee and team records;
- a simple phone/desktop daily clock;
- basic PTO balance, request, approval, and denial records;
- append-only HR audit events for selected actions;
- a settings page displaying approved policy decisions;
- a payroll page that correctly blocks final approval while opening payroll data and
  tax setup are incomplete.

It does not yet provide:

- secure employee invitations or self-service onboarding;
- hire dates, employment history, secure tax identity storage, or complete W-4/I-9
  workflows;
- correct salaried PTO accrual or 90-day eligibility enforcement;
- missed-punch requests, time corrections, pay-period approval, or overtime review;
- real payroll run inputs, versioning, statutory calculations, approval, checks, pay
  statements, tax liabilities, filing records, or reconciliation;
- complete HR permissions, sensitive-read auditing, retention rules, or employee-only
  access boundaries.

Historical Base44 exports are partial and must not be assumed to contain the complete
employee master or filing-grade year-to-date payroll values.

## 3. Approved operating rules

### Workforce

- One overtime-exempt salaried employee receives a fixed $1,000 each semimonthly check.
- Two nonexempt hourly employees are paid exact clocked time.
- Routine lunches and breaks are paid; employees do not clock out for them.
- Overseas VAs are contractors paid separately through Wise and are not included in
  US employee withholding, unemployment, PTO, or holiday calculations.
- All current W-2 employees work in Utah.

### Payroll calendar

- Pay period 1: 1st through 15th, paid on the 20th.
- Pay period 2: 16th through the last day of the month, paid on the 5th of the following
  month.
- A Saturday payday moves to Friday.
- A Sunday payday moves to Monday.
- The workweek is Sunday through Saturday.
- The first intended live Agent run covers August 1–15, 2026 and pays August 20, 2026.

### Time and overtime

- Time entry is one clock-in and one clock-out for the workday.
- No projects, clients, job codes, or GPS location are collected.
- David or Val may correct time, but every correction requires a reason and retains the
  original value, new value, actor, and timestamp.
- Overtime requires advance approval.
- Agent warns as a nonexempt employee approaches 40 worked hours.
- If overtime is actually worked, it remains payable even when it was not approved.
  The run records a policy exception; it does not suppress earned wages.

### Paid time off

- One combined PTO bank covers vacation, illness, and personal time.
- W-2 employees accrue one PTO hour per 52 paid hours, up to 40 hours per policy year.
- Accrual begins on the hire date; use begins after 90 days.
- The balance cap is 40 hours. Accrual pauses at the cap.
- No negative balance or borrowing future PTO.
- PTO is paid at the employee's base rate.
- PTO and holiday hours are paid hours but not hours worked for overtime.
- Unused PTO is not paid at separation unless a written agreement requires it.

### Paid holidays

- New Year's Day
- Memorial Day
- Independence Day
- Labor Day
- Thanksgiving
- Christmas

Eligibility begins after 90 days for W-2 employees. Hourly employees receive their
normally scheduled hours up to eight; salaried employees receive normal salary. If an
employee works on a holiday, regular worked time and holiday pay remain separate lines.
There is no holiday accrual or payout balance.

### Other pay

Payroll supports:

- regular hourly wages;
- fixed semimonthly salary;
- overtime;
- holiday pay;
- bonuses;
- commissions;
- accountable reimbursements;
- contractor flat fees;
- mandatory and configured voluntary deductions.

Reimbursements require a receipt and business purpose and remain separate from taxable
wages unless reviewed and deliberately reclassified. David or Val may enter bonuses
and commissions; David confirms them in final payroll approval.

### Authority

- David or Val may prepare payroll.
- David is the required final approver.
- A verbal business conversation is not sufficient evidence by itself; Agent records
  David's deliberate approval of the exact frozen version.
- Launch uses handwritten business checks.
- Bank transfer may be added later, but Finance/Plaid implementation is outside scope.

## 4. Product principles

1. **Ask each person only for information they own.** Employees provide personal,
   withholding, emergency, and attestation data. David or Val provides employment,
   compensation, classification, and employer-verification data.
2. **Prefill facts, never choices.** Agent may prefill company name, Utah work state,
   payroll schedule, employee name/email, and previously confirmed contact information.
   It must not choose W-4 elections, attest to work authorization, invent deductions, or
   sign for a person.
3. **One clear next action.** Every screen identifies what is complete, what is blocked,
   who owns the blocker, and the next action.
4. **Sensitive by default.** Tax identity, compensation, emergency contacts, I-9
   evidence, and pay statements are hidden unless the role and purpose require access.
5. **Approval is tied to a version.** Any change to time, compensation, variable pay,
   tax elections, deduction inputs, or calculation rules creates a new payroll version
   and invalidates prior approval.
6. **Calculation is not completion.** A liability is not paid, a check is not issued,
   and a return is not filed without recorded external evidence.
7. **No silent automation.** Agent may generate periods, deadlines, calculations,
   reminders, and draft filing packets. It may not silently change employee records,
   submit payroll, move money, or claim a government action succeeded.
8. **Mobile first for employees; decision first for employers.** Employee tasks must be
   easy on a phone. Employer payroll review must make totals, blockers, evidence, and
   approval consequences easy to scan.

## 5. Roles and permission boundaries

### Employee

May:

- view and update permitted fields on their own profile;
- complete their own onboarding forms and attestations;
- clock in/out and see their own time;
- request a time correction;
- request/cancel eligible PTO;
- view their own PTO balance, policy documents, acknowledgements, and pay statements.

May not:

- view another employee;
- change compensation, classification, hire date, tax-table configuration, approved
  time, payroll inputs, tax liabilities, or check status;
- approve their own time correction or PTO request.

### HR/payroll preparer — David or Val

May:

- invite employees;
- maintain employer-owned employment fields;
- review onboarding;
- correct time with a reason;
- approve PTO;
- prepare payroll inputs and resolve ordinary readiness issues;
- record checks, government confirmations, and filing evidence;
- view necessary compensation and payroll detail.

May not:

- alter employee attestations;
- overwrite the original value of a corrected record;
- make a payroll version appear approved without David's approval event;
- mark payment or filing complete without evidence.

### Final payroll approver — David

May:

- approve or reject a frozen payroll version;
- explicitly accept noncritical warnings;
- void or authorize reissue of checks;
- approve corrected/amended runs.

Critical blockers cannot be overridden, including missing employee identity, invalid
calculation configuration, incomplete year-to-date opening balances, negative net pay,
unresolved duplicate payment, or unbalanced payroll totals.

### Accountant/reviewer

Recommended read/export role:

- can inspect run inputs, calculations, year-to-date totals, liabilities, filings, and
  audit evidence;
- cannot edit employee self-service fields or issue payments;
- may mark a review complete and leave findings.

### Super-admin

Break-glass only. Use is logged and reviewed. Super-admin status does not erase
separation between employee attestations, payroll approval, and external evidence.

## 6. End-to-end journey A — company setup

### A1. Open setup checklist

Owner: David or Val  
Entry: HR → Setup  
State: `not_started`, `in_progress`, `ready_for_review`, `complete`, `needs_update`

Agent creates a guided checklist for:

1. legal company identity;
2. payroll calendar;
3. Utah work/tax setup;
4. federal tax deposit setup;
5. Utah unemployment setup;
6. PTO and holiday policies;
7. approval responsibilities;
8. prior-year/current-year opening balances;
9. check issuance;
10. employee invitations.

### A2. Company payroll profile form

Employer provides:

- legal name and business address;
- federal EIN through a masked, encrypted field;
- Utah withholding account identifier;
- Utah unemployment account identifier and current contribution rate;
- federal deposit schedule;
- default work state and workweek;
- payroll contact and final approver;
- check payment method;
- effective date of configuration.

Agent prefills:

- legal name/address only from a previously verified company profile;
- Utah as the current employee work state;
- Sunday–Saturday workweek;
- approved semimonthly calendar;
- David as required final approver;
- semiweekly federal deposit schedule;
- TAP access as confirmed;
- EFTPS and Utah unemployment portal access as unverified until confirmed.

Security:

- credentials, PINs, passwords, and full bank account data are never entered here;
- EIN and account identifiers are masked after save;
- reads and changes are audited;
- changes after a payroll run require an effective date and review.

### A3. Opening payroll balance import

Owner: David/Val; reviewer: accountant or qualified payroll professional

Accepted input:

- structured CSV/template or guided manual entry;
- source document reference for each imported total;
- employee mapping;
- through-date.

Required per employee:

- gross wages;
- federal taxable wages;
- Social Security wages/tax;
- Medicare wages/tax;
- federal income tax withheld;
- Utah taxable wages/tax;
- FUTA taxable wages;
- Utah unemployment taxable wages;
- bonuses and other supplemental wages;
- deductions;
- net pay;
- voids/corrections.

Required employer/run totals:

- filed Forms 941 and balances;
- federal deposits and confirmations;
- Utah withholding returns/payments;
- Utah unemployment reports/payments;
- outstanding liabilities.

Automation:

- map known employee email/name aliases;
- detect duplicates, missing periods, impossible negatives, wage-base conflicts, and
  totals that do not reconcile;
- calculate but do not silently fix differences;
- produce an exception queue and accountant export.

Exit gate:

- every W-2 employee has opening values through July 31, 2026;
- employee totals reconcile to employer totals or have a signed exception;
- reviewer records approval;
- statutory calculation remains disabled until this gate passes.

## 7. End-to-end journey B — secure employee invitation and onboarding

### B1. Create employee invitation

Owner: David or Val

Employer form:

- employee legal/preferred name;
- work email or personal invitation email;
- hire date;
- title and manager;
- employee type;
- hourly rate or fixed semimonthly amount;
- exempt/nonexempt classification;
- work state;
- expected/scheduled hours for holiday and PTO treatment;
- PTO/holiday policy assignment;
- invitation expiration.

Agent prefills:

- Utah work state;
- semimonthly payroll schedule;
- Sunday–Saturday workweek;
- standard PTO and holiday policies;
- nonexempt for hourly employees;
- fixed semimonthly basis when the approved $1,000 salary option is selected.

Employer must deliberately confirm:

- compensation;
- exemption classification;
- hire date;
- manager;
- policy eligibility.

Invitation:

- one-time, expiring, revocable token;
- delivered by email as a secure link;
- no SSN, tax election, compensation, or sensitive data in the email;
- rate-limited and protected against token reuse;
- first use requires identity verification and account setup.

States:

`draft`, `sent`, `opened`, `employee_in_progress`, `employer_review`,
`changes_requested`, `complete`, `expired`, `revoked`

### B2. Employee profile form

Employee enters:

- legal name;
- preferred name;
- home address;
- phone;
- emergency contact name, relationship, phone, and optional email;
- communication preference;
- required attestations.

Agent may prefill:

- invitation email;
- employer-entered name for confirmation;
- existing verified contact fields.

Agent must not:

- expose compensation on a shared/public screen;
- copy emergency contact data from another source without confirmation;
- require unrelated demographic information.

### B3. Federal W-4 guided form

Employee-owned and employee-signed.

Workflow:

1. explain that Agent can guide but cannot choose tax elections;
2. prefill verified identity and address;
3. employee selects filing status;
4. employee completes multiple-jobs, dependents, other income, deductions, and extra
   withholding fields when applicable;
5. show a plain-English review mirroring the official form structure;
6. employee attests and electronically signs;
7. generate an immutable form snapshot and effective date;
8. notify David/Val that a W-4 is complete without revealing unnecessary choices in
   general task lists;
9. apply to the next eligible unapproved payroll run.

Security:

- full SSN is collected only in a dedicated encrypted tax-identity field;
- full SSN is never returned in ordinary HTML after initial confirmation;
- display last four only;
- no tax elections in URLs, email, analytics, or logs;
- re-authentication required to view or change sensitive tax fields;
- every sensitive read and write is audited.

Exception states:

- incomplete signature;
- invalid/missing SSN;
- address missing;
- employee requests help;
- replacement W-4 effective after a run is frozen;
- duplicate submission;
- revoked invitation.

### B4. I-9 workflow

Employee:

- completes employee-owned information and attestation;
- receives a clear list/link to acceptable document choices;
- does not email identity documents.

David or Val:

- performs the employer verification step;
- records document category/type, issuing authority, document number/expiration only
  when required;
- records verification date and verifier;
- records whether reverification is required.

Agent:

- separates employee and employer sections;
- prevents the employer from signing for the employee;
- creates deadline reminders;
- masks document identifiers;
- does not store document images by default;
- supports a documented exception if counsel-approved remote verification is used.

### B5. Handbook and policy acknowledgements

Agent assigns the current:

- employee handbook;
- timekeeping policy;
- overtime approval policy;
- PTO/holiday policy;
- payroll/check delivery policy;
- privacy/security notice.

Employee:

- opens each document;
- confirms version and receipt;
- signs/acknowledges;
- may download a copy.

Agent retains:

- document version/hash;
- signer;
- timestamp;
- IP/security metadata appropriate for audit;
- superseded versions and re-acknowledgement requirements.

### B6. Employer review and activation

Readiness checklist:

- identity/profile complete;
- W-4 signed;
- I-9 employer step complete or valid pending state;
- employment/compensation confirmed;
- policies acknowledged;
- hire date and eligibility set;
- account access working.

David or Val:

- approves;
- requests a correction with a plain-English reason; or
- saves a documented allowed exception.

Activation creates:

- active employee record;
- pay schedule assignment;
- PTO/holiday eligibility dates;
- time-clock access;
- payroll readiness status;
- immutable onboarding completion event.

## 8. End-to-end journey C — daily time and correction

### C1. Clock in

Employee sees:

- current local date/time;
- `Clock in` primary action;
- last completed shift;
- current weekly worked hours;
- overtime warning when applicable.

Action:

- one tap/click;
- Agent stores UTC timestamp and Utah display time;
- duplicate open shifts are blocked;
- success state is announced accessibly.

### C2. Clock out

Employee sees elapsed time and confirms `Clock out`.

Agent:

- closes the open shift;
- calculates exact elapsed hours without hidden rounding;
- flags negative, overlapping, duplicate, or implausibly long shifts;
- updates workweek and pay-period totals;
- does not deduct paid breaks.

### C3. Missed-punch correction request

Employee form:

- affected date;
- requested clock-in/out;
- reason;
- optional supporting note.

Agent prefills:

- existing entry;
- work date;
- detected issue;
- pay-period context.

State:

`requested`, `approved`, `denied`, `superseded`, `withdrawn`

David or Val:

- compares old/new values;
- approves, edits with a reason, or denies;
- sees payroll and overtime impact before saving.

Audit:

- immutable original entry;
- proposed value;
- final value;
- employee request;
- reviewer and reason;
- affected payroll version.

### C4. Overtime control

Automation:

- warning at a configurable pre-40-hour threshold;
- urgent warning at or above 40 worked hours;
- advance overtime request/approval option;
- unapproved worked overtime becomes a payroll issue, not unpaid time;
- regular-rate calculation includes legally required remuneration categories.

## 9. End-to-end journey D — PTO and holidays

### D1. Accrual

Agent accrues from eligible paid hours:

- hourly: eligible paid hours from approved payroll inputs;
- fixed salary: configured standard paid hours per period, not clock punches;
- cap and policy-year rules applied deterministically;
- every accrual/use/adjustment posts to an immutable PTO ledger.

The balance shown is:

`prior balance + earned + approved adjustments - used - pending reservations`

### D2. Request PTO

Employee selects:

- date(s);
- hours by date;
- optional note.

Agent displays:

- current available balance;
- pending requests;
- eligibility date;
- resulting balance;
- overlapping company holidays;
- whether any requested time exceeds the balance.

No negative request is approvable.

### D3. Review

David or Val sees:

- employee/date/hours;
- available balance;
- payroll-period impact;
- overlapping approved leave;
- policy exceptions.

Approval reserves the balance. Payroll use finalizes it. Denial/revocation releases it.

### D4. Holiday automation

Agent generates holiday events annually from the approved list.

For each employee/payroll period it:

- checks 90-day eligibility;
- checks employee type and expected scheduled hours;
- proposes holiday pay as a separate payroll line;
- flags holiday worked time;
- never converts holiday pay into overtime hours worked.

David/Val confirms exceptions before payroll freeze.

## 10. End-to-end journey E — pay-period close and payroll

### E1. Generate period

Agent automatically creates future periods and adjusted pay dates.

State model:

`collecting` → `time_review` → `inputs_ready` → `calculated` → `prepared` →
`approved` → `checks_issued` → `taxes_due` → `filed_and_reconciled` → `closed`

Additional states:

`blocked`, `rejected`, `voided`, `corrected`, `reopened`

### E2. Close time

David or Val reviews:

- all active employees expected in the period;
- missing/open/invalid punches;
- corrections;
- hours by Sunday–Saturday workweek;
- overtime and approval status;
- PTO/holiday hours;
- employee/time approver.

Freeze:

- creates an immutable time snapshot;
- later time changes reopen readiness and invalidate downstream approval.

### E3. Enter variable payroll items

Form supports:

- bonus;
- commission;
- accountable reimbursement;
- holiday adjustment;
- deduction;
- garnishment;
- manual correction;
- contractor flat fee in the separate contractor workflow.

Each item requires:

- employee;
- type;
- amount;
- effective pay period;
- taxable/non-taxable classification from a controlled list;
- business reason;
- source/receipt where applicable;
- creator;
- reviewer status.

Automation:

- carry forward only explicitly recurring deductions;
- never repeat a one-time bonus/reimbursement;
- warn on unusual change versus prior period;
- prevent duplicate receipt/amount/date combinations;
- reimbursements require receipt and business purpose.

### E4. Calculate payroll

Inputs:

- frozen time;
- fixed salary;
- approved PTO/holiday pay;
- approved variable items;
- effective W-4;
- Utah configuration;
- opening/year-to-date balances;
- effective tax-rule package;
- deductions.

Required calculation output per employee:

- regular hours/pay;
- overtime hours/pay;
- PTO and holiday hours/pay;
- bonus and commission;
- reimbursement;
- gross pay;
- federal taxable wages;
- federal income-tax withholding;
- Social Security wages/tax;
- Medicare wages/tax;
- Utah taxable wages/withholding;
- deductions;
- net pay;
- employer Social Security/Medicare;
- FUTA and Utah unemployment;
- total employer cost.

Required run output:

- employee net checks;
- employee taxes withheld;
- employer payroll taxes;
- reimbursements;
- total check cash required;
- total tax liability;
- total employer cash impact;
- comparison with prior run;
- calculation-rule version and source publication effective dates.

Safety:

- use integer cents and explicit rounding rules;
- retain year-to-date wage-base state;
- distinguish regular and supplemental wages;
- no negative net pay without an explicit correction workflow;
- calculations are deterministic and reproducible;
- statutory tables are effective-dated and activated only after review;
- the first production run requires qualified parallel review.

### E5. Resolve readiness issues

Critical blockers:

- missing/invalid employee tax identity;
- incomplete opening balances;
- missing W-4 required for the configured method;
- unbalanced run totals;
- missing tax-rule version;
- duplicate check/payment;
- negative net;
- missing David approval;
- changed input after approval.

Warnings requiring acknowledgment:

- unapproved overtime that was nevertheless worked and included;
- unusual hours/pay change;
- first paycheck;
- bonus/commission;
- large reimbursement;
- expiring I-9 document;
- portal access deadline risk.

### E6. Prepare and freeze

Val or David selects `Prepare payroll`.

Agent:

- reruns validations;
- saves an immutable input/result snapshot and hash;
- records preparer and timestamp;
- blocks ordinary editing;
- shows David the exact version requiring approval.

### E7. David approval

Approval screen must show:

- pay period and pay date;
- employee count;
- each employee gross, taxes/deductions, reimbursement, and net;
- total check cash required;
- total tax liability and employer cost;
- changed amounts versus prior run;
- unresolved warnings;
- evidence/source freshness;
- version/hash and preparer.

David must:

- re-authenticate;
- deliberately choose approve or reject;
- optionally comment;
- acknowledge any allowed warnings.

Any changed input invalidates approval and returns to `calculated`.

## 11. End-to-end journey F — checks and pay statements

### F1. Issue handwritten checks

For each approved employee line, David or Val records:

- check number;
- issue date;
- check amount;
- payment method;
- optional memo;
- issuer.

Agent validates:

- amount equals approved net;
- check number is not duplicated;
- issue date is appropriate;
- payroll version remains approved.

States:

`not_issued`, `issued`, `delivered`, `voided`, `reissued`, `exception`

### F2. Pay statement

After check issuance, Agent creates a secure statement showing:

- employer/employee;
- pay period/pay date;
- hours and pay types;
- gross;
- each tax/deduction;
- reimbursement;
- net;
- year-to-date totals;
- check number masked as appropriate.

Delivery:

- email contains only a sign-in link and neutral notification;
- no sensitive attachment;
- employee can view/download their own statement;
- access is logged.

### F3. Void/reissue

Requires:

- original check;
- reason;
- David authorization;
- whether funds were delivered/cashed;
- replacement check number, if any;
- payroll/tax impact review.

Original evidence remains immutable. Reissue never overwrites the original check.

## 12. End-to-end journey G — tax payment, filing, and reconciliation

### G1. Liability schedule

Agent creates liabilities from approved payroll:

- federal withheld income tax;
- employee/employer Social Security;
- employee/employer Medicare;
- FUTA;
- Utah withholding;
- Utah unemployment.

Each liability includes:

- source payroll runs;
- amount;
- due date;
- government portal;
- deposit/return type;
- owner;
- status;
- evidence.

Statuses:

`calculated`, `due`, `scheduled`, `paid_unverified`, `confirmed`,
`filed`, `rejected`, `late`, `corrected`

### G2. Human government action

Agent presents a guided payment/filing packet and deep link.

David or Val:

- signs into the official portal outside Agent;
- enters/verifies the payment or return;
- explicitly submits;
- returns with confirmation number/date and receipt.

Agent:

- cannot store portal passwords/PINs;
- cannot mark complete from task completion alone;
- verifies entered amount against liability;
- flags partial, duplicate, late, or mismatched evidence.

### G3. Required operational coverage

- federal electronic payroll-tax deposits;
- quarterly Form 941;
- annual Form 940;
- Utah withholding returns/reconciliation through TAP;
- Utah unemployment wage report/contribution;
- W-2/W-3 year-end packet and submission tracking.

### G4. Reconciliation

Per employee:

`approved net = issued checks - voided checks + valid reissues`

Per liability:

`approved liability = confirmed payments + unresolved difference`

Per filing:

`filed reported totals = included payroll totals + documented amendments`

Run closure requires:

- every employee payment resolved;
- every due liability linked to confirmed evidence or an explicit open exception;
- filing status recorded when applicable;
- difference equals zero or has a signed corrective action.

## 13. End-to-end journey H — contractor/Wise workflow

This is intentionally separate from US payroll.

Contractor record:

- legal/display name;
- country;
- email;
- engagement start/end;
- flat-fee terms and currency;
- Wise recipient reference (not full bank credentials);
- contract/W-8/W-9 status as applicable;
- active/inactive status.

Payment request:

- contractor;
- service period;
- amount/currency;
- invoice or source;
- business purpose;
- preparer;
- David approval;
- Wise transfer reference and confirmation.

Agent does not:

- classify overseas contractors as W-2 employees;
- calculate Utah/FICA withholding for them;
- store Wise passwords or full bank details;
- mix contractor totals into employee payroll-tax liabilities.

## 14. End-to-end journey I — offboarding

### I1. Start separation

David or Val records:

- employee;
- last day worked;
- employer-initiated or resignation;
- reason category;
- final-pay deadline;
- outstanding time/PTO/expense/check issues;
- access/equipment/tasks.

### I2. Final payroll

Agent:

- freezes final worked time;
- includes earned pay and approved reimbursements;
- applies written PTO non-payout policy unless an agreement overrides it;
- warns about Utah final-pay timing;
- creates final statement and tax-liability effects;
- requires David approval.

### I3. Close access and records

Checklist:

- deactivate employee access after required retrieval window;
- preserve pay statements and statutory records;
- revoke outstanding invitation/tokens;
- record equipment/access completion;
- provide employee document access instructions;
- retain audit evidence under the approved schedule.

No employee/payroll history is deleted as part of ordinary deactivation.

## 15. Secure form catalog

| Form | Primary owner | Safe prefill | Must remain deliberate | Approval/review |
|---|---|---|---|---|
| Company payroll profile | David/Val | approved schedule, Utah defaults | EIN/account IDs, rates, effective date | David |
| Opening balance import | David/Val | employee mappings | source totals/exceptions | qualified reviewer |
| Employee invitation | David/Val | policies, Utah, schedule | hire date, pay, classification | David/Val |
| Personal profile | employee | invitation identity | legal/contact confirmation | employee; HR review |
| Emergency contact | employee | prior confirmed value | relationship/contact consent | employee |
| W-4 | employee | verified identity/address | elections, attestation, signature | employee |
| I-9 employee section | employee | verified profile fields where allowed | attestation/signature | employee |
| I-9 employer review | David/Val | employee reference | document review/verification | verifier |
| Handbook acknowledgement | employee | assigned version | receipt/signature | employee |
| Time correction | employee or David/Val | original punch | requested/final values and reason | David/Val |
| PTO request | employee | balance/eligibility | dates/hours | David/Val |
| Variable pay item | David/Val | employee/period | type, amount, tax treatment, source | David |
| Payroll preparation | David/Val | all approved inputs | freeze exact version | preparer |
| Payroll approval | David | frozen results | approval/rejection | David only |
| Check issue/void | David/Val | approved net | check number/status/reason | David for void/reissue |
| Tax evidence | David/Val | liability/amount/due date | portal submission and confirmation | reconciler |
| Offboarding | David/Val | employee/employment data | dates/reason/final-pay facts | David/Val |

## 16. Automation map

### Agent should automate

- pay-period and pay-date generation;
- weekend pay-date adjustment;
- onboarding/PTO/holiday eligibility dates;
- reminders and expired invitation handling;
- time anomaly and overtime warnings;
- PTO accrual proposals and ledger posting from approved payroll inputs;
- holiday-pay proposals;
- opening-balance validation;
- deterministic payroll calculations;
- prior-run variance checks;
- immutable versioning and approval invalidation;
- liability due dates and checklists;
- pay-statement generation;
- reconciliation math;
- employee/employer notifications;
- accountant-ready exports.

### Agent should assist but require confirmation

- employee/profile prefill;
- employee alias mapping during import;
- classification reminders;
- holiday scheduled-hour proposals;
- taxable/non-taxable pay-type choice;
- W-4 guided explanations;
- I-9 deadline and document-choice guidance;
- government payment/filing packets;
- correction recommendations.

### Agent must not automate

- W-4 elections or signatures;
- employee or employer I-9 attestations;
- exempt/nonexempt classification choice without employer confirmation;
- government credentials;
- tax/payment submission without explicit authority and supported integration;
- money movement;
- fabricated confirmation numbers;
- overriding critical payroll blockers;
- deletion of payroll/audit history.

## 17. Data and audit requirements

Use stable numeric relationships for new records and preserve Base44 identifiers only
as migration provenance.

Required record groups:

- employee identity/profile;
- employment and effective-dated compensation;
- tax identity and effective-dated withholding elections;
- I-9/compliance status;
- invitations/onboarding tasks;
- documents/acknowledgements;
- time entries/corrections/approvals;
- PTO policy/ledger/requests;
- holiday calendar/employee proposals;
- payroll periods/runs/versions/issues/approvals;
- payroll earnings/deductions/taxes/employer liabilities;
- checks/pay statements/voids;
- tax liabilities/payments/filings/evidence;
- contractor engagements/payment requests;
- append-only audit events.

Every sensitive write and material sensitive read records:

- actor;
- action;
- object and stable identifier;
- timestamp;
- prior/new value reference where appropriate;
- reason;
- request/session context;
- payroll version affected;
- redacted metadata only.

## 18. Security and privacy requirements

- Default deny and least privilege.
- Employee can access only self-owned data.
- Compensation, tax identity, I-9, emergency contacts, and pay statements have separate
  access capabilities.
- Full SSN/EIN/document identifiers use field-level encryption and masked display.
- Do not store full bank account information for handwritten-check launch.
- Secrets and government portal credentials are never stored in HR tables.
- Secure, HTTP-only, same-site cookies; CSRF protection for writes.
- Re-authentication for tax-field access, payroll approval, void/reissue, and sensitive
  exports.
- Rate limiting and token replay protection for invitations and authentication.
- Logs/errors/analytics redact PII and payroll values unless strictly required.
- Secure document storage with short-lived authorized download links.
- Retention schedule documented before collecting document images.
- Backup/restore and audit-integrity checks included in launch validation.

## 19. Notifications

Employee notifications:

- invitation and reminder;
- onboarding correction requested;
- successful account activation;
- missed/open punch reminder;
- time correction decision;
- PTO decision;
- pay statement ready;
- changed personal/tax information confirmation;
- document/policy acknowledgement due.

Employer notifications:

- onboarding blocker;
- expiring invitation;
- time anomaly/overtime warning;
- pending correction/PTO;
- payroll readiness blocker;
- payroll prepared for David;
- approval invalidated by a change;
- check not issued/duplicate;
- tax liability approaching due;
- missing/mismatched payment or filing evidence;
- document expiration.

Email contains no SSN, tax elections, detailed compensation, or pay-statement attachment.
Notifications deep-link to the authorized screen.

## 20. Required UI states

Every workflow defines:

- empty state with the correct first action;
- loading/processing state that prevents duplicate submission;
- success confirmation;
- field validation with preserved non-sensitive input;
- permission-denied state without data leakage;
- expired/revoked invitation state;
- stale-version conflict state;
- calculation unavailable state;
- external portal unavailable state;
- partial evidence/reconciliation state;
- mobile layout;
- keyboard/focus behavior;
- screen-reader labels/live status;
- print/download treatment where applicable.

At 320px and above:

- employee tasks require no page-level horizontal scrolling;
- primary actions have at least 44px touch targets;
- tables become labeled stacked records;
- approval totals remain visible without covering content;
- sensitive values are masked by default.

## 21. Right-sized delivery plan

### Release 1 — safe employee and time foundation

Build:

- employment profile with hire date/classification/fixed-pay support;
- granular permissions and sensitive audit;
- secure invitation and account activation;
- personal/emergency profile;
- W-4 guided workflow and secure tax identity;
- I-9 checklist/status workflow;
- handbook/policy acknowledgements;
- missed-punch corrections and employer review;
- correct salaried/hourly PTO accrual and eligibility;
- holiday calendar and proposals.

Exit:

- all three W-2 employees can complete onboarding and daily HR tasks;
- David/Val can resolve time/PTO issues;
- employee data boundaries pass security tests.

### Release 2 — opening balances and payroll preparation

Build:

- opening-balance template/import/reconciliation;
- real pay periods;
- time close/freeze;
- variable pay/reimbursement/deductions;
- versioned federal/Utah payroll calculator;
- employee/employer totals and issue queue;
- prepared/frozen run state.

Exit:

- August run can be calculated reproducibly;
- qualified reviewer matches results against an independent calculation;
- no final approval or checks until all critical blockers pass.

### Release 3 — approval, checks, statements, and liabilities

Build:

- David approval/re-authentication;
- approval invalidation;
- handwritten check issuance/void/reissue;
- secure pay statements;
- tax-liability schedule;
- payment/filing evidence and reconciliation.

Exit:

- one complete payroll travels from approved inputs through checks and confirmed
  liabilities with an immutable audit trail.

### Release 4 — contractor and operational completion

Build:

- contractor/Wise records and approved payment requests;
- offboarding/final-pay workflow;
- accountant exports;
- small set of reports and compliance reminders.

Exit:

- W-2 payroll and contractor payments remain distinct and reconcilable;
- onboarding-to-offboarding is operationally complete.

### Deferred

- recruiting/ATS;
- job boards;
- benefits enrollment;
- performance reviews/360s;
- engagement surveys;
- recognition feed;
- training-management system;
- multi-state/multi-entity payroll;
- native iOS/Android applications;
- automatic money movement or Finance/Plaid implementation.

## 22. Acceptance criteria

### Identity and onboarding

- An expired, revoked, reused, or guessed invitation cannot expose employee data.
- An employee can complete only their own employee-owned fields and signatures.
- David/Val can request correction without overwriting the submitted form.
- W-4 choices are never automatically selected.
- Full SSN/document identifiers never appear in ordinary pages, URLs, emails, logs, or
  analytics.
- Employee activation clearly identifies incomplete or allowed-pending steps.

### Time and leave

- Duplicate clock-in is blocked and duplicate clicks do not create duplicate entries.
- Exact elapsed time is retained without hidden rounding.
- Correction preserves original/proposed/final values and reviewer reason.
- Worked overtime is included even when advance approval is missing.
- Salaried PTO accrues from configured eligible hours, not time punches.
- PTO cannot be approved before eligibility or beyond available balance.
- Holiday pay is proposed separately and does not increase overtime hours worked.

### Payroll

- The semimonthly calendar and weekend rules generate the expected dates.
- Every approved run references frozen inputs, effective elections, opening/YTD values,
  and a calculation-rule version.
- Any relevant edit invalidates approval.
- David sees check cash, tax liabilities, employer taxes, reimbursements, and total
  employer cash impact before approval.
- Critical blockers cannot be overridden.
- Repeated approval/check actions are idempotent.
- The first live calculation matches an independent qualified review.

### Checks, statements, and reconciliation

- Check number cannot be duplicated.
- Issued check amount must match approved net.
- Void/reissue preserves original evidence and requires a reason.
- Employee can access only their own statement through authentication.
- No statement is emailed as a sensitive attachment.
- A liability cannot become confirmed without confirmation evidence.
- A run cannot close with an unexplained employee-payment or liability difference.

### Security and mobile

- Horizontal and vertical privilege-escalation tests pass for every role.
- CSRF, invitation replay, sensitive logging, export authorization, and direct-object
  reference tests pass.
- Core employee journeys work at 320px and with keyboard-only navigation.
- Status is conveyed with text, not color alone.
- Finance/Plaid files and behavior remain unchanged.

## 23. End-to-end test scenarios

1. **New hourly employee:** invite → profile → W-4 → I-9 → acknowledgements →
   activation → punches → correction → PTO → payroll → check → statement → liability.
2. **Existing salaried employee:** opening balances → fixed $1,000 check → salary PTO
   accrual → holiday → approval → check/statement.
3. **Unapproved overtime:** employee crosses 40 → warning → payable overtime included →
   policy exception acknowledged → approved run.
4. **Changed W-4:** employee submits replacement after period calculation → next
   eligible run uses new effective record → prior closed run unchanged.
5. **Payroll changed after approval:** Val changes bonus → David approval invalidated →
   new version calculated → reapproval required.
6. **Bad check:** issued check voided → David authorizes reissue → original retained →
   net payment reconciles once.
7. **Tax mismatch:** entered confirmation amount differs from liability → status remains
   exception → correction/partial evidence resolves it.
8. **PTO edge:** request before 90 days or beyond balance is blocked with explanation.
9. **Invitation attack:** expired/reused token and cross-employee URL access reveal no
   private data.
10. **Contractor payment:** approved Wise flat fee remains outside W-2 payroll tax
    totals and records the Wise confirmation.
11. **Offboarding:** final time/pay, access removal, statement availability, and retained
    audit complete without deleting history.
12. **Mobile employee:** onboarding, clock, correction, PTO, and statement work on a
    320px viewport with keyboard/screen-reader names.

## 24. Rollout gates

### Before inviting employees

- permission and invitation security tests pass;
- privacy/retention policy is approved;
- company profile and policies are reviewed;
- production email links use the correct domain;
- support/recovery path is documented.

### Before calculating live payroll

- all three employee masters are complete;
- opening values through July 31 are reconciled;
- current W-4s are effective;
- Utah unemployment rate is entered from evidence;
- statutory rule package is independently reviewed;
- time/PTO/holiday inputs are approved.

### Before David approval

- calculation comparison passes;
- all critical issues are resolved;
- tax liabilities and cash impact are visible;
- exact version and source data are frozen.

### Before closing the run

- checks are issued or resolved;
- statements are available;
- required tax payments have confirmation evidence or explicit open exceptions;
- applicable filing status is recorded;
- reconciliation differences are zero or assigned corrective action.

## 25. Decisions still needed

Recommended defaults are included so implementation does not stall.

1. **Standard paid hours for salaried PTO/holiday treatment:** use 40 hours per workweek
   and 86.67 hours per semimonthly period unless the employment agreement says otherwise.
2. **PTO policy year:** use employee anniversary year to align the 90-day start and avoid
   a special first-year proration.
3. **PTO request increment:** use quarter-hour increments.
4. **Overtime warning threshold:** notify at 36 hours and escalate at 40.
5. **Implausibly long shift threshold:** flag after 16 elapsed hours; never silently cap.
6. **Employee statement retention/access:** retain required payroll records and keep
   former-employee statement access through a secure recovery process.
7. **Sensitive document images:** do not store I-9 identity document images unless a
   written compliance decision requires consistent retention.
8. **Payroll reviewer:** use a qualified accountant/payroll professional for opening
   balances and the first production comparison, without requiring a monthly provider.
9. **Employee correction approvals:** David or Val may approve; the requester cannot
   approve their own request.
10. **Contractor tax-document workflow:** track status and expiry first; add country-
    specific form automation only when a real contractor requires it.

## 26. Definition of success

For Anata, right-sized HR parity is achieved when:

> Employees can securely onboard, clock time, request PTO, keep their information
> current, and retrieve pay statements. Val can prepare a complete payroll from trusted
> inputs. David can see the exact employee, tax, and cash impact, approve one immutable
> version, issue checks, and prove every required payment and filing outcome. Contractors
> remain separate, and every correction is understandable after the fact.

