Anata brand assets for shared deck/UI use.

Canonical asset paths:
- `assets/wordmark.svg`
- `assets/monogram.svg`
- `style.css`

Usage rules:
- Keep reusable brand files here and mirror them to `/Users/davidnarayan/Documents/Playground/shared/anata_brand/` for cross-project use.
- `wordmark.svg` should be the primary Anata wordmark used in deck headers.
- `monogram.svg` should be the compact badge used for avatars, placeholders, and hero fallbacks.
- `style.css` should contain only reusable brand and layout primitives, not deck-specific one-off hacks.

Deck loading behavior:
- The deck generator reads `SHARED_BRAND_PACKAGE_PATH` first.
- If that is unset, it falls back to this repo-local package.

Recommended setup:
- Point `SHARED_BRAND_PACKAGE_PATH` at `/Users/davidnarayan/Documents/Playground/shared/anata_brand`
- Replace the SVG assets above whenever the official logo package changes.
