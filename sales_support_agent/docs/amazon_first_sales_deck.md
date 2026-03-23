# Amazon-First Sales Deck

## Purpose

This workflow generates a first-party HTML sales deck for Amazon opportunities using:

- a target Amazon ASIN or product URL
- a Helium 10 Xray CSV for competitor and niche data
- an optional Helium 10 Xray keyword CSV for SEO/search behavior

The output is an Anata-branded HTML deck with a stable share URL and basic view tracking.

## Intake

The admin deck form accepts:

- `target_product_input`
- `competitor_xray_csv`
- `keyword_xray_csv`
- `channels`

`channels` controls which service-offering slides are included:

- `amazon`
- `shopify`
- `tiktok_shop`

Amazon is always treated as the primary data-backed channel in v1.

## Data sources

### Target product

The target product must be an Amazon ASIN or a supported Amazon product URL.

The generator enriches the target listing through the existing Amazon research path and falls back to parsed target metadata if the richer enrichment path is unavailable.

### Competitor Xray CSV

The Xray CSV is the canonical v1 source for:

- niche summary cards
- competitor table rows
- price / revenue / BSR / review comparisons
- seller country, size tier, and fulfillment distributions
- competitor listing images

The current parser reads the Helium 10 export headers directly and normalizes the core fields into `Helium10XrayReport`.

### Keyword CSV

The keyword CSV is optional, but it is the preferred source for:

- top keyword opportunities
- search volume summaries
- title density and competing-products context
- SEO/search-behavior slide copy

## Output model

The generator produces a fixed-layout HTML deck. It does not rely on Canva or Google Sheets in the active workflow.

Each deck is persisted as a `deck_generation` automation run and stores:

- deck title
- slug
- target product identifier
- enabled channels
- generated HTML
- public deck URL
- view count
- first viewed timestamp
- last viewed timestamp

## Public routes

Decks are served from:

- `/decks/{deck_slug}/{run_id}/{token}`

Legacy tokenized exports remain supported at:

- `/deck-exports/{run_id}/{token}`

Both routes render the same stored HTML and increment the same view counters.

## Shared brand package

Projects should pull brand assets from:

- `/Users/davidnarayan/Documents/Playground/shared/anata_brand`

Repo-local fallback:

- `/Users/davidnarayan/Documents/Playground/Lead-scraper/shared/anata_brand`

The shared brand package contains:

- `style.css`
- `tokens.json`
- `components.md`
- `assets/wordmark.svg`
- `assets/monogram.svg`

Use `SHARED_BRAND_PACKAGE_PATH` to override the brand root when needed.

## Slide structure

The current deck is built from these sections:

1. Cover
2. Keyword niche summary
3. Target listing snapshot
4. Competitor landscape
5. Search behavior
6. Conversion and PDP
7. Channel offering slides
8. Recommended plan / closing

## Implementation notes

- The admin workflow should not show Google Sheets or Canva as active requirements.
- The HTML deck is intended to be editable through code and component updates, not through a post-generation editor in v1.
- If a future project needs new deck styling, extend the shared brand package first and then consume it from the deck renderer instead of creating one-off CSS inside the project.
