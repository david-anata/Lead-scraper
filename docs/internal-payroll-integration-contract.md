# Internal payroll integration contract

**Status:** Ready for the internal payroll team to implement  
**Contract version:** 2026-07-23  
**System of engagement:** Anata HR control room  
**Future authority:** Not yet decided

## Purpose

Anata HR prepares time, earnings inputs, deductions, reimbursements, cash-impact estimates, and explicit human approval. A future internal payroll service may become the authority for final calculation, wage distribution, tax payments, and tax filings—but only for capabilities it explicitly owns and can prove.

This contract lets the two systems connect without deciding prematurely who carries statutory or money-movement responsibility.

## Non-negotiable boundary

- Anata sends one approved, immutable payroll version.
- The payroll service returns its own run ID, the matching Anata version hash, timestamped state, evidence reference, and authoritative totals.
- Anata never infers `paid`, `filed`, or `completed` from a successful HTTP response.
- The service may claim only the capabilities declared by its production manifest.
- SSNs, bank accounts, identity documents, and W-4 elections do not travel in a payroll-run request. The payroll service must maintain those in a separately secured employee profile and use opaque employee IDs in the run.
- Plaid, a bank transaction, a printed check, an approval, or a calculated liability is not proof of payroll filing.

The executable contract lives in `sales_support_agent/services/hr/provider_contract.py`.

## Required service manifest

The internal service must provide a versioned manifest containing:

```json
{
  "service_name": "Internal payroll",
  "environment": "sandbox",
  "authority_owner": "Named person or licensed partner",
  "contract_version": "2026-07-23",
  "capabilities": {
    "authoritative_calculation": false,
    "wage_distribution": false,
    "tax_payment": false,
    "tax_filing": false,
    "pay_statements": false,
    "new_hire_reporting": false,
    "signed_webhooks": false
  }
}
```

`sandbox` and `production` manifests are separate. Production submission stays disabled until the production manifest, authority owner, credentials, and operational review are approved.

## Run submission

Recommended endpoint:

`POST /v1/payroll-runs`

Required request fields:

- idempotency key;
- Anata run ID;
- immutable Anata version hash;
- pay-period start and end;
- pay date;
- approver identity and timezone-aware approval time;
- one opaque provider employee ID per person;
- approved regular, overtime, holiday, and PTO minutes;
- approved taxable additions, reimbursements, and deductions.

The same idempotency key and version must always return the same provider run. A changed Anata version requires a new key and new provider run.

The submission response means only `accepted`. It is not evidence of calculation, payment, or filing.

## Status and confirmations

Recommended endpoint:

`GET /v1/payroll-runs/{provider_run_id}`

Supported states:

1. `accepted`
2. `calculated`
3. `approved`
4. `wages_distributed`
5. `taxes_paid`
6. `taxes_filed`
7. `completed`
8. `failed`
9. `canceled`

Every confirmation includes:

- provider run ID;
- Anata run ID and exact version hash;
- timezone-aware occurrence time;
- immutable evidence or event reference;
- authoritative gross, employee tax, employer tax, deductions, reimbursements, net, and total debit whenever the state depends on calculated money;
- structured failure code and safe operator message on failure.

`completed` is valid only when the manifest declares authoritative calculation, wage distribution, tax payment, and tax filing.

## Webhooks

Recommended endpoint in Agent:

`POST /api/integrations/internal-payroll/webhooks`

Before enabling it, implement:

- asymmetric signatures or HMAC with a dedicated rotating secret;
- timestamp tolerance and replay protection;
- unique event IDs;
- constant-time signature comparison;
- raw-body verification before JSON parsing;
- idempotent event storage;
- strict run ID and version-hash matching;
- audit records for accepted and rejected events;
- no secrets or payroll values in error logs.

Webhook delivery is evidence only after signature, replay, state-transition, capability, and version checks pass.

## Employee-profile boundary

The payroll service needs a separate secure employee-profile flow for:

- legal identity and tax jurisdiction;
- W-4 elections and effective dates;
- state withholding elections;
- bank instructions;
- deduction authorizations;
- provider employee ID;
- status and effective dates.

Anata may show completion status and the opaque provider employee ID. It should not copy full SSNs or bank-account numbers into ordinary HR pages, logs, URLs, exports, or run payloads.

## Error and recovery rules

- Timeout after submission: query by idempotency key; do not resubmit blindly.
- Duplicate event: acknowledge without repeating the state change or audit event.
- Hash mismatch: quarantine the confirmation and block the run.
- Calculation variance: keep the run in review; do not issue payment from the estimate.
- Partial payment or filing: record the precise confirmed state and open exception; never mark completed.
- Provider outage: preserve the approved Anata version and follow the documented manual contingency.
- Credentials revoked: disable submission immediately; read-only historical reconciliation may remain.

## Cutover gates

The internal service cannot become production authority until all are true:

1. The user decides who legally and operationally owns calculation, wage distribution, tax payment, and tax filing.
2. Production capabilities and authority owner are recorded.
3. Secure employee tax and bank profiles exist outside the run payload.
4. Idempotency, authentication, signatures, replay protection, and audit behavior pass tests.
5. Calculation rules receive independent qualified review.
6. Opening balances and year-to-date totals are reconciled.
7. At least one complete payroll runs in shadow mode with zero unexplained differences.
8. David approves the production cutover.
9. A rollback and manual contingency are documented and tested.

Until then, Anata remains a preparation, approval, evidence, and reconciliation control room.
