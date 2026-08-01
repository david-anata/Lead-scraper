# Building signature-request readiness

Agent separates contract approval from e-sign delivery.

## Staff workflow

1. Prepare and approve the immutable agreement package.
2. On the customer contract page, choose **Prepare signature request**.
3. Agent freezes the active customer name and email, agreement ID/version,
   agreement checksum, reservation, and a provider-neutral `not_sent` state.
4. A contract approver moves the record through **prepared → in review →
   approved** using the exact typed confirmations shown by the page.
5. The approved record is ready for a future provider adapter. It is not a
   signature request, delivery receipt, signature, or booking confirmation.

The operation is idempotent for one agreement version. A changed checksum
requires a new agreement version instead of silently replacing the frozen
handoff.

## Failure and recovery

- A missing or inactive contact, missing customer email, unapproved agreement,
  failed checksum, missing reservation, or inactive temporary hold fails
  closed.
- If the hold expires during review, Agent marks the readiness record expired
  and does not call a provider.
- Retrying preparation with the identical snapshot reuses the existing record.
- Every preparation, review, approval, and expiry writes audit evidence with
  `provider_write=false` and `message_sent=false`.

## Provider activation still required

The production e-sign provider, account, restricted credentials, callback
verification, signature-event mapping, document retention policy, and
controlled delivery rehearsal remain external activation work. Until a
provider adapter is implemented and verified, the Arena launch checklist
correctly keeps electronic signatures blocked.
