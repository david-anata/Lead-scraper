# Anata Brand Package

This package is the shared deck and dashboard branding source for Anata projects.

## Contents

- `style.css`
  - Shared first-party deck stylesheet
- `tokens.json`
  - Reusable color and typography tokens
- `components.md`
  - Component inventory and naming guidance
- `assets/wordmark.svg`
  - Inline-safe wordmark asset
- `assets/monogram.svg`
  - Inline-safe monogram asset

## Intended usage

- Repo fallback path:
  - `Lead-scraper/shared/anata_brand`
- Cross-project override path:
  - set `SHARED_BRAND_PACKAGE_PATH` to a shared workspace location

## Naming rules

- Global brand assets live here.
- Deck-specific assets should stay in the consuming project unless they are promoted into the shared package.
- Reusable CSS classes should be generic and presentation-oriented, not campaign-specific.

## Import guidance

- Python services should read assets from `SHARED_BRAND_PACKAGE_PATH` first, then fall back to the repo package.
- Frontend projects should import `style.css` directly or copy the relevant primitives into their build pipeline.
