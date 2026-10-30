# Anata Agent Brand Book

One page to keep every Agent surface (admin pages, Call Script, public
deliverables) looking like the same product. The values come from
`shared/anata_brand/tokens.json`; `sales_support_agent/static/admin.css` maps
them to `--agent-*` CSS variables. `DESIGN.md` has the full interaction rules.

## Colors

| Token | Value | Use |
| --- | --- | --- |
| `--agent-ink` | `#2b3644` | Text, dark sidebar, primary buttons |
| `--agent-ink-muted` | `#5d6977` | Secondary text and labels |
| `--agent-page` | `#f9f7f3` | Page background |
| `--agent-surface` | `#ffffff` | Cards and panels |
| `--agent-surface-soft` | `#f2ece3` | Quiet highlight, marked text |
| `--agent-blue` / `-strong` / `-soft` | `#85bbda` / `#5e9fc4` / `#e7f3f9` | Accent, progress, neutral info |
| `--agent-support` | `#bfa889` | Warm support accent |
| `--agent-border` | `#dfe3e6` | Dividers and card borders |
| `--agent-good` | `#477a59` | Verified, done |
| `--agent-warn` | `#986817` | Caution |
| `--agent-danger` | `#8b4c42` | Blocked, needs attention |

Status colors carry meaning only, and always come with a word (never color
alone).

## Type

- Headings, labels and buttons: `Montserrat` (600 to 800).
- Reading text and data: `Inter`, falling back to `Segoe UI`.
- Sizes: 12, 14, 16, 20, 28 and 36px. Body line height 1.5.

## Spacing, shape and motion

- Spacing steps: 4, 8, 12, 16, 20, 24, 32px.
- Radius: 10px controls, 16px panels, 20px cards, pill for badges.
- Controls are 42px tall (44px on touch).
- Motion: 120 to 240ms with `cubic-bezier(0.2, 0.8, 0.2, 1)`. Respect
  reduced motion. Live content never jumps: reserve space for text that
  changes, and warn before it switches (the Call Script shows a 5-second fill
  bar before a new live move replaces the current one).

## Components

Use the shared primitives listed in `DESIGN.md` (PageHeader, CommandBar,
StatusBadge, Alert, ProgressStatus and the table contract). New pages that
ship their own stylesheet, like the Call Script, must define their colors as
the `--agent-*` tokens above rather than new raw values.
