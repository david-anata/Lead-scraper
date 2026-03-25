Anata brand assets for shared deck/UI use.

Source of truth for deployed deck assets:
- `/Users/davidnarayan/Documents/Playground/Lead-scraper/shared/anata_brand/assets`

Only update these approved files if you want a new deploy to use them:
- `1.png`
  - deck header icon
- `anata wordmark logo - black.png`
  - shared wordmark source
- `no-product-image-available.png`
  - fallback image when the target product image cannot be resolved

Canonical filenames used by code:
- `assets/monogram.png`
- `assets/wordmark.png`
- `assets/no-product-image-available.png`

Current loading behavior:
- the deck generator first checks the original approved filenames above
- then falls back to the canonical filenames
- this allows you to update the approved files directly without changing code

Important:
- Render only serves files committed in this repo
- updating files outside this repo does not change the deployed deck until those files are copied or committed here

`style.css` should contain only reusable brand and layout primitives, not deck-specific one-off hacks.
