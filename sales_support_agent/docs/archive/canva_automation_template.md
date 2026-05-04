# Canva Automation-First Template Guide

This template is designed for the v1 autofill deck flow:

- one brand
- one target product
- up to five competitor Amazon links or ASINs
- text fields + chart fields only
- no image autofill in v1

Target product intake can be either:

- a Shopify product URL
- an Amazon ASIN
- an Amazon product URL

## Slide Flow

1. Cover
2. Executive summary
3. Market opportunity
4. Hero product snapshot
5. Competitor comparison
6. Top competitors by BSR / sales estimate
7. Listing / CRO audit
8. SEO / content opportunity
9. Creative / graphics opportunity
10. Advertising opportunity
11. Recommended plan
12. Expected impact / next steps
13. Why anata
14. CTA / closing

## Slide Contract

### 1. Cover

- headline: `brand_name`
- subhead: `hero_product_name`
- context line: `reporting_period`

### 2. Executive summary

- summary block: `executive_summary`
- support line: `hero_product_snapshot`

### 3. Market opportunity

- summary block: `market_summary`
- table/chart: `top_products_by_bsr`

### 4. Hero product snapshot

- product name: `hero_product_name`
- source url: `hero_product_source_url`
- price: `hero_product_price`
- dimensions: `hero_product_dimensions`
- description: `hero_product_description`
- type: `hero_product_type`
- tags: `hero_product_tags`

### 5. Competitor comparison

- chart: `competitor_table`
- optional supporting fields:
  - `competitor_1_name`
  - `competitor_1_bsr`
  - `competitor_1_estimated_sales`
  - `competitor_1_units`
  - repeat through slot `5`

### 6. Top competitors by BSR / sales estimate

- chart: `top_products_by_bsr`

### 7. Listing / CRO audit

- summary block: `cro_summary`
- competitor detail fields:
  - `competitor_1_strength`
  - `competitor_1_gap`
  - repeat through slot `5`

### 8. SEO / content opportunity

- summary block: `seo_summary`

### 9. Creative / graphics opportunity

- summary block: `creative_summary`

### 10. Advertising opportunity

- summary block: `advertising_summary`

### 11. Recommended plan

- summary block: `recommended_plan_summary`

### 12. Expected impact / next steps

- summary block: `expected_impact_summary`

### 13. Why anata

- summary block: `why_anata_summary`

### 14. CTA / closing

- summary block: `cta_summary`

## Naming Rules

- lowercase only
- underscore-separated
- no spaces
- no punctuation beyond underscores

## Core Text Fields

- `brand_name`
- `brand_domain`
- `brand_shopify_url`
- `hero_product_name`
- `hero_product_handle`
- `hero_product_source_url`
- `hero_product_price`
- `hero_product_bsr`
- `hero_product_dimensions`
- `hero_product_snapshot`
- `report_generated_date`
- `reporting_period`
- `executive_summary`
- `market_summary`
- `cro_summary`
- `seo_summary`
- `creative_summary`
- `advertising_summary`
- `recommended_plan_summary`
- `expected_impact_summary`
- `why_anata_summary`
- `cta_summary`

## Competitor Slot Fields

Repeat these for slots `1` through `5`:

- `competitor_1_name`
- `competitor_1_identifier`
- `competitor_1_source_url`
- `competitor_1_asin`
- `competitor_1_bsr`
- `competitor_1_estimated_sales`
- `competitor_1_units`
- `competitor_1_strength`
- `competitor_1_gap`

## Chart Fields

### `competitor_table`

Columns:

- `competitor`
- `bsr`
- `estimated_sales`
- `estimated_units`
- `price`
- `review_count`

### `top_products_by_bsr`

Columns:

- `product_name`
- `bsr`
- `sales`
- `units`
- `change_from_previous_period`

## Important Constraints

- Keep each slide modular:
  - one headline
  - one summary block
  - one main chart/table or comparison area
  - one takeaway block
- Do not add Canva image autofill fields in this version.
- The backend keys must match the Canva field names exactly.
- If you need static product imagery, place it in the template manually and keep it non-required for automation.

## Current Backend Behavior

- The target product is used for hero-product and brand context.
- Competitor Amazon links and ASINs are normalized into deck-ready fields.
- SP-API and BSR enrichment are separate follow-on workstreams; the current template should tolerate empty metric values without breaking layout.
