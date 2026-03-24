Anata brand assets for shared deck/UI use.

Canonical asset paths:
- `assets/wordmark.png`
- `assets/monogram.png`
- `style.css`

Usage rules:
- Keep reusable brand files here and mirror them to `/Users/davidnarayan/Documents/Playground/shared/anata_brand/` for cross-project use.
- `wordmark.png` should be the provided primary Anata wordmark asset.
- `monogram.png` should be the provided compact logo icon used in the deck header and fallbacks.
- `style.css` should contain only reusable brand and layout primitives, not deck-specific one-off hacks.

Deck loading behavior:
- The deck generator reads `SHARED_BRAND_PACKAGE_PATH` first.
- If that is unset, it falls back to this repo-local package.

Recommended setup:
- Point `SHARED_BRAND_PACKAGE_PATH` at `/Users/davidnarayan/Documents/Playground/shared/anata_brand`
- Replace the SVG assets above whenever the official logo package changes.
