# Vercel staging callback registry

Status: registration and provider verification pending owner-console access

Stable staging origin: `https://agent-staging.anatainc.com`

Never place credentials, tokens, webhook signing secrets, or customer payloads in this file.

| Provider | Purpose | Staging callback or webhook | Production callback currently in use | Staging verification |
| --- | --- | --- | --- | --- |
| Google | Agent sign-in | `https://agent-staging.anatainc.com/admin/auth/callback` | `https://agent.anatainc.com/admin/auth/callback` | Live authorization reaches Google with the correct staging callback but Google returns `redirect_uri_mismatch`; add the staging URI alongside production in the Google OAuth client, then repeat sign-in. Fallback login passes; invalid state returns `state_mismatch`. |
| Gmail | Connected inbox OAuth | `https://agent-staging.anatainc.com/admin/settings/inboxes/callback` | Same path on `agent.anatainc.com` | Console registration and read-only inbox test pending |
| QuickBooks | Finance OAuth | `https://agent-staging.anatainc.com/admin/finances/qbo/callback` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; invalid/expired state rejected; console registration and read-only company test pending |
| Plaid | Link OAuth return | `https://agent-staging.anatainc.com/admin/finances/plaid/oauth-return` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; dashboard registration and sandbox Link test pending |
| Plaid | Signed webhook | `https://agent-staging.anatainc.com/api/integrations/plaid/webhook` | Same path on `agent.anatainc.com` | Explicit staging environment variable set; valid and invalid signature tests pending |
| Instantly | Sales event webhook | `https://agent-staging.anatainc.com/api/integrations/instantly/webhook` | Same path on `agent.anatainc.com` | Missing/invalid credential rejected; signing secret and provider delivery pending |
| Stripe | Building billing webhook | `https://agent-staging.anatainc.com/api/integrations/stripe/webhook` | Same path on `agent.anatainc.com` | Live negative test confirms Stripe verification is not configured; owner credential and endpoint setup required |
| Resend | Building email webhook | `https://agent-staging.anatainc.com/api/integrations/resend/webhook` | Same path on `agent.anatainc.com` | Missing signature rejected; provider test event pending |

## Verification receipt requirements

For every row, record the test date, operator, provider environment, result, source-system event identifier, Agent audit identifier, and whether the test produced any external write. Staging schedules and external writes remain disabled until cutover approval.

## Pass criteria

- Every OAuth authorization returns to `agent-staging.anatainc.com`, never Render.
- Invalid, expired, and replayed OAuth state is rejected.
- Valid signed webhook fixtures are accepted exactly once.
- Missing or invalid signatures are rejected without changing application data.
- No staging notification or generated link points at `agent.anatainc.com` unless explicitly testing the production system.
