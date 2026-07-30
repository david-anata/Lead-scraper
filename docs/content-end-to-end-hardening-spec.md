# Content System end-to-end hardening specification

Status: approved for production implementation
Date: July 30, 2026

## Problem

Agent can record Content operations and call publishing relays, but the current
release does not yet prove that every ready Riverside recording becomes
publishable native content, enters a durable daily calendar, and reaches every
eligible destination without loss.

## Required outcome

1. Riverside is polled through the supported Business API v3 with complete
   pagination. Every ready recording and transcript is ingested idempotently.
2. Every transcript-backed episode produces original, publishable treatments
   for personal LinkedIn, company LinkedIn, YouTube, Instagram, and staging-only
   X. Prompts and writing instructions are not artifacts.
3. Every treatment applies the Six C's:
   Channel-native structure, source-backed Credibility, approved Category,
   substantive Content, measurable Calibration metadata, and an appropriate
   Collection CTA.
4. The portfolio has a publication opportunity every day. David's personal
   LinkedIn remains limited to 2–3 verified posts weekly. Other destinations use
   their approved native cadence; X never publishes.
5. The strongest eligible source is selected using comparable observed
   performance, Six C quality, freshness, fatigue, and delivery history.
6. One disconnected or failing channel cannot stop ingestion, transformation,
   staging, or other ready destinations.
7. Every source-to-destination pair has a durable state and audit event. A
   source is not complete while an eligible channel is missing its artifact or
   final publication evidence.
8. Video destinations receive a retrievable media reference, not only a
   database identifier.

## Scope

- Riverside ingestion and source normalization
- transcript-to-native-copy generation
- Six C quality contracts
- durable per-episode transformation coverage
- daily portfolio scheduling and channel-isolated publication
- provider receipts, public URLs, failures, retries, analytics, and learning
- operator readiness and backlog evidence in `/admin/content`

## Non-goals

- X live publishing
- fabricating clips, quotes, results, or experiences
- publishing to an unverified account
- bypassing first-live channel activation
- weakening David's personal 2–3/week policy to fill a quota

## Target workflow

`Poll all Riverside pages → ingest every ready episode → verify transcript and
media → generate native copy → apply Six C gates → record per-channel coverage
→ schedule daily portfolio → publish each ready channel independently → verify
receipt and public URL → ingest analytics → rank future source patterns`

## Important states

- **Ready:** transcript, media, native copy, CTA, lineage, and destination are
  available.
- **Needs review:** copy exists but a first-live or quality approval is pending.
- **Blocked:** the named provider or account is unavailable; unrelated channels
  continue.
- **Failed:** a bounded attempt failed and retains the same idempotency key.
- **Missing coverage:** a ready episode lacks an artifact for an eligible
  channel.
- **Backlog low:** fewer than seven daily portfolio opportunities remain.
- **Delivered:** the provider returned a receipt and public URL, and Agent
  verified the post.

## Acceptance criteria

1. A paginated two-page Riverside fixture ingests every ready recording once,
   ignores processing recordings, and never persists bearer credentials.
2. Two newly ingested episodes in one run each create five distinct artifacts.
3. Generated bodies contain publishable copy, not imperative writing
   instructions; all bodies retain transcript lineage.
4. LinkedIn personal, LinkedIn company, YouTube, Instagram, and X bodies are
   structurally different and each contains its approved CTA behavior.
5. Six C quality fails closed when source credibility, category, native
   structure, or CTA is absent.
6. A daily scheduler run creates or advances the portfolio calendar without
   exceeding channel cadence or publishing X.
7. A failed Instagram connector does not prevent an eligible LinkedIn or
   YouTube attempt in the same daily cycle.
8. Every external attempt records actor, idempotency, attempts, safe failure,
   receipt, public URL, and verification state.
9. The Control Room shows episode coverage, daily backlog, David's weekly
   personal cadence, and the strongest eligible score.
10. Focused tests, broader regressions, production health, exact deployed
    commit, desktop view, and phone view pass before completion.

## Rollout

Deploy code in shadow mode. Configure Riverside and one destination at a time.
For each channel: verify identity, run a shadow fixture, approve first live,
publish safe source-backed test content, verify the public URL, then enable its
automatic switch. X remains staging-only.
