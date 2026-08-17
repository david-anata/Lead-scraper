# Anata Outbound System — Build Kit

Anata's own outbound machine for booking performance-marketing sales calls, run parallel to
the side agency. This folder is the setup kit. Follow the docs in order.

## The flow
StoreLeads finds the brands  ->  Clay finds the person + verified email + writes the personal
line (uses Clay enrichment)  ->  Instantly sends from your warmed domains  ->  the app orchestrates
and keeps the scoreboard. Sales/HubSpot come next.

## The offer
Performance marketing, direct. Proof: ~40% sales lift in month one by finding wasted ad spend,
reallocating it, and closing conversion-rate holes.

## The target
DTC ecommerce brands ~$1M-$15M/yr, 2-80 people, US/UK/CA/AU, six niches (beauty/wellness,
food & bev, apparel, home, pets, baby-kids-toys). Start broad, then double down on the niche
that replies best. Never contact agencies, wholesalers, dropshippers, POD, B2B, manufacturers.

## Docs
- `01-email-sequence.md` — the 3 emails (spintax, no links in email 1). READY.
- `02-clay-table-blueprint.md` — the Clay table + the qualify and personalize prompts. READY.
- `03-storeleads-filter-recipe.md` — exact StoreLeads filters. READY.
- `04-instantly-campaign-setup.md` — exact Instantly settings. READY.

## Who does what
- **You:** get StoreLeads, build the Clay table from doc 02, set up the Instantly campaign from
  doc 04, and set all keys yourself (I never touch raw keys).
- **Me (next):** build the app scoreboard that reads Instantly's numbers, then the dry-run
  preview, then repoint the app to feed Clay. Sales/HubSpot after that.

## Guardrails
One lead offer. 3 emails max, no links/unsubscribe in email 1, spintax always. Verified emails
only. Never email a brand twice. Size sending to reply capacity. Nothing sends until you
approve the copy and see a no-send test list first.
