# Vercel staging callback registry

Status: registration and provider verification pending owner-console access

Stable staging origin: `https://agent-staging.anatainc.com`

Never place credentials, tokens, webhook signing secrets, or customer payloads in this file.

| Provider | Purpose | Staging callback or webhook | Production callback currently in use | Staging verification |
| --- | --- | --- | --- | --- |
| Google | Agent sign-in | `https://agent-staging.anatainc.com/admin/auth/callback` | `https://agent.anatainc.com/admin/auth/callback` | August 14: authorization starts at Google with the exact staging callback, a signed state, and the expected identity scopes; a fabricated callback returns `state_mismatch`. Google still returns `redirect_uri_mismatch` until the owner adds the staging URI alongside production in the OAuth client. |
| Gmail | Connected inbox OAuth | `https://agent-staging.anatainc.com/admin/settings/inboxes/callback` | Same path on `agent.anatainc.com` | August 14: a callback without an authenticated session returns to login without exchanging a code. Console registration and read-only inbox test pending. |
| QuickBooks | Finance OAuth | `https://agent-staging.anatainc.com/admin/finances/qbo/callback` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; August 14: missing parameters and fabricated state both reject with 400 before token exchange. Console registration and read-only company test pending. |
| Plaid | Link OAuth return | `https://agent-staging.anatainc.com/admin/finances/plaid/oauth-return` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; authenticated return renders Accounts & setup so Plaid Link can resume; dashboard registration and sandbox Link test pending |
| Plaid | Signed webhook | `https://agent-staging.anatainc.com/api/integrations/plaid/webhook` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; August 14: unsigned sandbox-shaped payload rejects with 401 and `verification_missing`. Signed sandbox delivery pending. |
| Instantly | Sales event webhook | `https://agent-staging.anatainc.com/api/integrations/instantly/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 401 before processing. A dedicated signing secret and provider delivery remain pending. |
| Stripe | Building billing webhook | `https://agent-staging.anatainc.com/api/integrations/stripe/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 400 and confirms Stripe verification is not configured. Owner must select the Stripe environment and register its secret before a signed test event. |
| Resend | Building email webhook | `https://agent-staging.anatainc.com/api/integrations/resend/webhook` | Same path on `agent.anatainc.com` | August 14: unsigned payload rejects with 401 for a missing signature. Signed provider test event pending. |

## Verification receipt requirements

For every row, record the test date, operator, provider environment, result, source-system event identifier, Agent audit identifier, and whether the test produced any external write. Staging schedules and external writes remain disabled until cutover approval.

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
