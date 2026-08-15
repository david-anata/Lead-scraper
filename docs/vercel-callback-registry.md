# Vercel staging callback registry

Status: Google, QuickBooks, and Plaid receipts verified; remaining provider receipts in progress

Stable staging origin: `https://agent-staging.anatainc.com`

Never place credentials, tokens, webhook signing secrets, or customer payloads in this file.

| Provider | Purpose | Staging callback or webhook | Production callback currently in use | Staging verification |
| --- | --- | --- | --- | --- |
| Google | Agent sign-in | `https://agent-staging.anatainc.com/admin/auth/callback` | `https://agent.anatainc.com/admin/auth/callback` | August 14: David added the staging URI alongside production in the existing Agent OAuth client. A real Google sign-in returned to staging, created the expected Agent session, and landed on David's authorized workspace. A fabricated callback still returns `state_mismatch`. No production URI was removed. |
| Gmail | Connected inbox OAuth | `https://agent-staging.anatainc.com/admin/settings/inboxes/callback` | Same path on `agent.anatainc.com` | August 14: David added the staging inbox callback alongside the production and staging sign-in callbacks. Authorization now reaches Google's account/consent flow instead of `redirect_uri_mismatch`. Production and staging both show the same connected system-managed legacy inbox; user-connected Gmail consent is therefore an enhancement receipt, not a blocker for preserving the current inbox path. |
| QuickBooks | Finance OAuth | `https://agent-staging.anatainc.com/admin/finances/qbo/callback` | Same path on `agent.anatainc.com` | August 14: the staging callback is saved alongside production in the Anata Agent Intuit application. A real OAuth authorization selected the verified Anata company, returned to staging, consumed its one-time state, stored fresh tokens, and completed with 303 before the authenticated workspace returned 200. The callback log recorded the approved realm and a one-hour access-token lifetime; no callback error or 5xx occurred. |
| Plaid | Link OAuth return | `https://agent-staging.anatainc.com/admin/finances/plaid/oauth-return` | Same path on `agent.anatainc.com` | August 14: production and staging returns are both registered. Plaid Sandbox Link completed with the documented mock credentials and created a Sandbox Item; no real bank was connected. |
| Plaid | Signed webhook | `https://agent-staging.anatainc.com/api/integrations/plaid/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 401. After the isolated Sandbox verifier was deployed, two provider-signed Sandbox deliveries returned 200, logged `environment=sandbox processed=false`, executed zero database queries, and triggered no Finance sync or data mutation. |
| Instantly | Sales event webhook | `https://agent-staging.anatainc.com/api/integrations/instantly/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 401 before processing. A dedicated signing secret and provider delivery remain pending. |
| Stripe | Building billing webhook | `https://agent-staging.anatainc.com/api/integrations/stripe/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 400 and confirms Stripe verification is not configured. Owner must select the Stripe environment and register its secret before a signed test event. |
| Resend | Building email webhook | `https://agent-staging.anatainc.com/api/integrations/resend/webhook` | Same path on `agent.anatainc.com` | August 14: a dedicated enabled staging webhook was registered for delivery, bounce, complaint, delay, and failure events. Its signing secret was stored as Vercel's sensitive `RESEND_WEBHOOK_SECRET`, a fresh immutable deployment was promoted, and five concurrent readiness checks passed. Unsigned payloads reject with 401. A controlled provider-generated signed event remains pending. |

## Verification receipt requirements

For every row, record the test date, operator, provider environment, result, source-system event identifier, Agent audit identifier, and whether the test produced any external write. Staging schedules and external writes remain disabled until cutover approval.

## Owner action queue

The following actions require the account owner because they grant account
access, select a money environment, or change a provider-owned allowlist. Agent
code and Vercel cannot complete them silently.

1. In staging Settings, choose **Connect your inbox**, select the intended
   Google account, and approve the requested Gmail access. Then capture a
   read-only sync receipt; do not send an email.
2. QuickBooks callback registration and real OAuth connection are complete.
   Preserve both production and staging redirect URIs until cutover is closed.
3. Plaid staging redirect, Sandbox Link, and provider-signed webhook receipts
   are complete. Preserve the production and staging return URIs until cutover
   is closed; keep the isolated Sandbox verifier unset in the live Render app.
4. Resend registration and secret deployment are complete. Send one controlled
   Resend test email to `delivered@resend.dev`, then record the signed webhook
   receipt. Do not use a customer address.
5. Decide whether Stripe billing is already required for production parity. If
   yes, explicitly select test mode for staging, register the staging webhook,
   and add its test keys. If no, keep payment execution disabled and track it
   as a separate Building launch dependency.
6. Decide whether the Instantly event webhook is already required for
   production parity. If yes, create one shared secret in the provider and
   Vercel and run a non-customer test event. If no, keep the current API/read
   integration and track webhook enablement separately.

## Pass criteria

- Every OAuth authorization returns to `agent-staging.anatainc.com`, never Render.
- Invalid, expired, and replayed OAuth state is rejected.
- Valid signed webhook fixtures are accepted exactly once.
- Missing or invalid signatures are rejected without changing application data.
- No staging notification or generated link points at `agent.anatainc.com` unless explicitly testing the production system.

## August 14 negative-path receipt

The checks above used fabricated identifiers and invalid or missing signatures;
they did not contain provider credentials, customer data, or valid event IDs.
Every callback failed before an external exchange or application write. The
exact staging deployment log scan after the checks contained no error-level or
5xx entry. These negative-path receipts prove fail-closed behavior only; they do
not replace the provider-generated signed happy-path receipts still marked
pending.

## August 14 Google happy-path receipt

Operator: David Narayan. Provider environment: Google OAuth client in the
`anata inc` Google Cloud project. The existing production sign-in callback was
preserved. The staging sign-in callback and staging Gmail inbox callback were
added as separate authorized redirect URIs. Real Google sign-in returned to
`agent-staging.anatainc.com`, established David's authorized Agent session, and
rendered the workspace home. The only application write was the expected
staging authentication/session audit state; no email, customer communication,
Gmail mailbox mutation, or production change occurred.

## August 14 QuickBooks happy-path receipt

Operator: David Narayan. Provider environment: the production-mode Anata Agent
application in Intuit Developer. The production callback remained registered
while the staging callback was added and verified after a fresh settings reload.
Agent generated a one-time OAuth state, Intuit authorized the verified Anata
company, and Intuit returned to the staging callback. Agent consumed the state,
stored the encrypted tokens and approved realm, logged a one-hour access-token
lifetime, redirected with 303, and rendered the authenticated staging workspace
with 200. No invoice, payment, category, customer, or other QuickBooks record was
created or changed by this connection receipt.

## August 14 Plaid happy-path receipt

Operator: Codex, with David Narayan supplying the Sandbox verification secret
directly to Vercel. Provider environment: Plaid Sandbox. Production and staging
OAuth returns remained registered, while the Sandbox configuration delivered
webhooks to the staging hostname. A fresh Sandbox Link flow used Plaid's mock
`user_good` profile and connected three mock accounts. Two provider-signed
webhooks reached deployment `dpl_UVYBWGwk7Vzwpad9RK7Qvi6sSaBn`, returned 200,
and logged `environment=sandbox processed=false`. Both requests executed zero
database queries; neither recorded an Item, enqueued a sync, changed Finance
data, connected a real bank, or affected Render production. Finance continues
to use the primary production Plaid environment for ordinary reads.

## August 14 production-parity observation

Authenticated Settings reads on Render production and Vercel staging each show
one connected, system-managed legacy Gmail inbox with zero connection errors.
The two Finance Accounts & setup reads each show the same five Plaid-backed
accounts, account roles, reserve totals, and credit-card liability. The visible
checking balance differs by $79.79 because staging and production are separate
database snapshots while Render remains authoritative. Both Finance briefs show
the same $40,812 confirmed-money-in total attributed to QuickBooks and confirmed
Anata receivables. This proves rendered source-data continuity, not a live OAuth
refresh or signed-webhook receipt; those narrower provider checks remain listed
above where required.
