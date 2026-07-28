# Building customer event status projection

The completion audit found that Agent already held authoritative lifecycle
evidence from inquiry through calendar projection, but the redacted customer
status was available only through an internal endpoint. This slice exposes that
existing truth through a signed, expiring, read-only contract for the website.

## Routes

- `POST /api/internal/building/bookings/{reservation_id}/customer-status-access`
  prepares an access URL. It requires the internal API key, an active linked
  contact, an expiry of 1–90 days, and records an audit event. It never sends the
  URL.
- `GET /api/public/building/bookings/status?token=...` returns the current
  redacted projection. No internal key is required because the signed URL is the
  bearer credential.

Tokens use the already-configured Building HMAC secret with a separate audience
and signing prefix. Tampered, mismatched, inactive-contact, and unknown links
return the same generic not-found response. Expired links return HTTP 410.

## Customer-safe response

The projection includes:

- event start/end window;
- truthful reservation label, message, booking flag, and hold expiry;
- quote status/version;
- agreement preparation and verified-signature state;
- payment-request, deposit, verified-payment, and invoice state;
- calendar projection readiness without a provider event ID;
- an explicit statement that no email or text delivery is claimed.

It excludes contact data, attendance, operator ownership/notes, requirements,
commercial amounts, line items, contract/payment provider references, audit
actors, internal errors, calendar IDs, and provider URLs.

## Rollout

1. Deploy the read-only endpoint and internal URL-preparation route.
2. Verify the Building HMAC secret is configured.
3. Have the website render the JSON contract on its customer status page.
4. Decide separately how staff deliver links; this PR sends no email or SMS.
5. Later add explicit token revocation only if business policy requires it.

Rollback is application-only. This slice adds no tables or columns and changes
no reservation, agreement, payment, calendar, or communication state.
