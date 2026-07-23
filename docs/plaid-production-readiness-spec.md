# Plaid Production Readiness Specification

## Outcome

Anata Finance can connect a real United States business bank through Plaid Production, keep the connection healthy, explain failures clearly, and disconnect it safely without exposing credentials or creating uncontrolled Plaid charges.

## Current verified behavior

The existing integration already:

- opens Plaid Link and creates new Items with the Transactions product;
- exchanges public tokens on the server;
- encrypts Plaid access tokens before database storage;
- stores accounts, cached balances, and synchronized transactions;
- verifies Plaid webhook signatures before processing them;
- refreshes connected Items and isolates failures between banks;
- supports Plaid update mode when a bank needs the user to sign in again;
- restricts Finance actions to authenticated, authorized users; and
- provides a CSV fallback when a bank connection is unavailable.

The last verified live configuration used Plaid Sandbox. Production credentials have not yet been activated.

## Problem

The current integration is sufficient for Sandbox testing but is not yet ready for real financial data. It does not have a verified OAuth return flow for redirect-based institutions, a complete user-controlled offboarding flow, sufficiently structured Plaid diagnostic logging, or a controlled Production migration procedure. The application also needs a visible privacy-policy link before Production Link is launched.

## Users

- **Finance administrator:** connects, refreshes, repairs, and disconnects company bank accounts.
- **Authorized Finance viewer:** views approved balances and transactions but cannot manage bank connections unless granted the required permission.
- **System operator:** diagnoses Plaid failures without accessing bank credentials or full account numbers.

The initial launch is an internal Anata workflow. It is not a public consumer banking product, payment processor, lender, or payroll system.

## Launch scope

### 1. Production OAuth support

- Define a stable HTTPS OAuth redirect address on `agent.anatainc.com`.
- Register the exact redirect address in the Plaid Dashboard.
- Include the approved redirect address when creating applicable Link tokens.
- Restore the user to Finance after an OAuth bank redirect and resume the same Link session.
- Reject invalid, expired, or mismatched return attempts without creating an Item.
- Show a plain-English retry message when the return cannot be completed.

### 2. Bank offboarding

- Add a Finance action named **Disconnect bank** for users with bank-management permission.
- Require an explicit confirmation that states the bank will stop refreshing and that disconnection cannot be undone without reconnecting.
- Call Plaid `/item/remove` on the server before treating the Item as disconnected.
- Never send an access token to the browser.
- Mark the local Item disconnected, destroy its stored access token, stop refresh jobs, and exclude its accounts from current-cash calculations.
- Retain or delete imported transaction history according to the approved retention procedure. Until that procedure is approved, default to preserving minimal audit history while removing the reusable Plaid credential.
- Record who disconnected the bank, when it occurred, Plaid's result, and any cleanup result.
- If Plaid removal fails, do not falsely show the bank as fully disconnected; show an actionable retry state.

### 3. Safe diagnostic logging

- Create structured events for Link-token creation, Link completion, token exchange, account sync, transaction sync, webhook receipt, update-mode launch, OAuth return, and disconnection.
- Where available, record `item_id`, `request_id`, `account_id`, and `link_session_id`, along with timestamps, environment, operation, result, and Plaid error code.
- Redact or omit Plaid secrets, access tokens, public tokens, full account numbers, routing numbers, credentials, identity details, and raw financial payloads.
- Use identifiers only for troubleshooting and support correlation.
- Ensure error messages shown to users do not expose server exceptions or sensitive identifiers unnecessarily.

### 4. Privacy and consent access

- Add a visible link to Anata's published privacy policy from the Finance connection experience.
- Keep Plaid Link user-initiated; never connect a bank automatically.
- Explain before launch that Plaid will retrieve account, balance, and transaction information for internal cash-flow management and reconciliation.
- Preserve Plaid's own disclosure and consent screens without obscuring or bypassing them.

### 5. Controlled Production migration

- Require MFA on Plaid, Render, GitHub, and the identity-provider account used for production administration before connecting a real bank.
- Store the Plaid Production secret only in protected Render environment variables.
- Set `PLAID_ENV=production` only during the approved release.
- Keep Sandbox and Production Items separate; never attempt to reuse a Sandbox access token in Production.
- Confirm the production webhook and OAuth redirect addresses in Plaid before deployment.
- Deploy with a rollback path that restores the previous application version without exposing or deleting credentials.
- Connect one Anata business bank as the pilot and validate it before enabling additional connections.

## Required interface states

- **Ready:** user can start a secure bank connection.
- **Preparing:** button is disabled while a Link token is created; the state must time out into a useful error.
- **Link open:** Plaid controls the institution and consent flow.
- **OAuth redirect:** the app preserves enough short-lived state to resume securely.
- **Connected:** institution, masked account information, last successful refresh, and account count are visible.
- **Refreshing:** repeat clicks are blocked and the existing verified data remains visible.
- **Needs attention:** update mode is offered when Plaid reports a recoverable login or consent problem.
- **Unavailable:** the user sees a retry option and CSV fallback without losing existing verified history.
- **Disconnect confirmation:** consequences are explicit and cancellation is safe.
- **Disconnect pending/error:** the UI does not claim success until Plaid and local credential cleanup have completed.
- **Disconnected:** the bank no longer refreshes or contributes to current cash.
- **Permission denied:** viewers without bank-management permission cannot connect, repair, or disconnect Items.

All states must remain usable on desktop and iPhone-sized screens, with keyboard-accessible controls, visible focus, and status messages that are not conveyed by color alone.

## Data and security requirements

- Continue encrypting stored Plaid access tokens with `PLAID_TOKEN_SECRET`.
- Do not place credentials, secrets, or raw Plaid payloads in application logs.
- Use server-side authorization for every connection-management route.
- Use short-lived, single-purpose state for OAuth returns and protect it against replay.
- Preserve verified webhook signature and body-hash checks.
- Store only the account fields and transaction fields required by Finance.
- Make disconnect and credential-destruction actions auditable.
- Do not implement money movement, ACH initiation, payroll, lending, or underwriting in this release.

## Product boundaries

Plaid approval may include Auth, Balance, Identity, Identity Match, Statements, Recurring Transactions, and Transactions Refresh. Approval does not mean those products must be called at launch.

This release will launch the existing **Transactions** workflow. Additional products require separate implementation, pricing controls, user disclosures, and tests:

- **Balance:** use only for intentionally requested real-time balance checks, with rate limits and cost tracking.
- **Transactions Refresh:** use only through a rate-limited explicit refresh policy.
- **Recurring Transactions:** add after the base transaction feed is stable.
- **Auth, Identity, and Identity Match:** defer until a payment processor and exact payment workflow are approved.
- **Statements:** add only with explicit download, access, storage, and retention rules.
- **Income and Signal:** out of scope for current Finance and payroll requirements.

## Cost controls

- Normal `/transactions/sync` runs may follow webhook and staleness rules.
- No paid Balance or Transactions Refresh endpoint may run on ordinary page loads.
- Each paid operation must have a server-side cooldown, idempotency protection where applicable, and an audit event.
- Add a clear monthly usage review and an initial Plaid budget alert target of $25.

## Acceptance criteria

The release is ready when all of the following are true:

1. A permitted user can connect a Production test/pilot institution through Link, including an OAuth redirect institution.
2. Link cannot be launched by an unauthenticated or unauthorized user.
3. No Plaid secret or access token appears in browser responses, page source, logs, or error messages.
4. A completed connection creates one encrypted Item and imports its selected accounts and Transactions data without duplicates.
5. Plaid webhooks with valid signatures are accepted; invalid, expired, or altered webhooks are rejected.
6. A stale connection refreshes successfully without requiring the page to remain open.
7. A login-required Item offers update mode and returns to a healthy connected state after repair.
8. The UI exits the Preparing state with a useful error when Link-token creation fails or times out.
9. A permitted administrator can disconnect a bank; Plaid removal succeeds, the reusable local token is destroyed, refresh stops, and current cash excludes the Item.
10. A failed Plaid removal remains visible as an unresolved retry state and is not reported as complete.
11. Safe diagnostic records include available Plaid correlation identifiers but contain no secrets, account numbers, or raw payloads.
12. The Finance connection experience links to Anata's public privacy policy and explains the data purpose before Link opens.
13. The workflow passes desktop and iPhone-sized responsive checks plus keyboard-only navigation.
14. Existing Finance calculations, QBO behavior, CSV fallback, and non-Plaid tools continue to pass their regression tests.
15. MFA is confirmed for Plaid, Render, GitHub, and production identity-provider administration before the real bank is connected.

## Validation plan

### Automated

- Unit tests for OAuth state creation, expiry, mismatch, replay, and safe return handling.
- Route and permission tests for Link, refresh, update mode, and disconnect.
- Tests that `/item/remove` success destroys the stored credential and failure preserves an actionable state.
- Tests that disconnected Items cannot refresh or contribute balances.
- Logging-redaction tests covering every Plaid token and sensitive account field.
- Existing Finance and Plaid regression suite.

### Sandbox

- Complete standard Link, OAuth redirect, webhook, transaction sync, update mode, Item error, and disconnect scenarios.
- Confirm masked account display, mobile layout, retry behavior, and CSV fallback.
- Search application and Render logs for token or account-number leakage.

### Production pilot

- Confirm environment, approved products, webhook address, OAuth redirect, and MFA before Link launch.
- Connect one authorized Anata business bank account.
- Verify institution, selected accounts, balances, transaction history, webhook delivery, refresh timestamps, and reconnect behavior.
- Perform and document a controlled disconnect test with a nonessential pilot Item if feasible.
- Monitor errors and Plaid usage for at least one business day before adding more banks.

## Rollout sequence

1. Implement and test OAuth return handling, offboarding, logging, and the privacy link in Sandbox.
2. Enable and verify MFA on all production administrative systems.
3. Complete the security-policy and data-retention procedures committed to Plaid.
4. Receive Plaid Production approval and enter the Production secret directly in Render.
5. Register production webhook and OAuth addresses, deploy, and run the pilot connection.
6. Monitor the pilot, resolve any Item or institution errors, then approve wider internal use.
7. Implement paid add-on products individually after the Transactions launch is stable.

## Recommended defaults and open decisions

- **OAuth redirect:** use a dedicated callback on `https://agent.anatainc.com`, not a generic Finance page.
- **Disconnect history:** remove the credential immediately and preserve only the minimum audit/transaction history permitted by the forthcoming retention policy.
- **Production access:** Finance administrators can manage connections; Finance viewers remain read-only.
- **Initial product:** Transactions only, even if additional products are approved.
- **Paid refreshes:** disabled until their endpoint, cooldown, budget control, and UI language are separately approved.
- **Open decision:** the formal retention period for imported Plaid transactions must be approved in the data-retention procedure before automated deletion is implemented.

