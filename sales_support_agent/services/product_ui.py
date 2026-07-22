"""Shared visual primitives for authenticated Anata Agent pages.

This module is deliberately CSS-only: it lets the existing server-rendered pages
share one product design system without changing their routes, forms, or business
logic.  Page-specific styles may extend these primitives, but canonical colors,
type, focus, motion, and controls live here.
"""

from functools import lru_cache


@lru_cache(maxsize=1)
def render_product_ui_styles() -> str:
    """Return the canonical Agent product tokens and global interaction rules."""
    return r"""
      :root {
        --agent-background: #f9f7f3;
        --agent-surface: #ffffff;
        --agent-surface-soft: #f2ece3;
        --agent-ink: #2b3644;
        --agent-ink-muted: #5d6977;
        --agent-navy: #34445c;
        --agent-border: rgba(43, 54, 68, 0.12);
        --agent-accent: #85bbda;
        --agent-accent-strong: #5e9fc4;
        --agent-accent-soft: rgba(133, 187, 218, 0.18);
        --agent-support: #bfa889;
        --agent-support-soft: rgba(191, 168, 137, 0.18);
        --agent-success: #6ea480;
        --agent-success-soft: rgba(110, 164, 128, 0.16);
        --agent-warning: #c28b2c;
        --agent-danger: #9a5a4e;
        --agent-radius-xs: 6px;
        --agent-radius-sm: 8px;
        --agent-radius-md: 12px;
        --agent-radius-lg: 14px;
        --agent-radius-xl: 22px;
        --agent-radius-2xl: 28px;
        --agent-shadow-resting: 0 1px 0 rgba(20,39,63,.04), 0 4px 12px rgba(20,39,63,.04);
        --agent-shadow-lifted: 0 20px 45px rgba(43,54,68,.10);
        --agent-focus: 0 0 0 3px rgba(133,187,218,.48);
        --agent-duration-fast: 140ms;
        --agent-duration-base: 200ms;
        --agent-font-body: "Roboto", system-ui, -apple-system, "Segoe UI", Arial, sans-serif;
        --agent-font-heading: "Montserrat", system-ui, -apple-system, "Segoe UI", Arial, sans-serif;
      }
      *, *::before, *::after { box-sizing: border-box; }
      html { color-scheme: light; }
      body {
        margin: 0;
        background: var(--agent-background);
        color: var(--agent-ink);
        font-family: var(--agent-font-body);
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
      }
      button, input, select, textarea { font: inherit; }
      button, a, input, select, textarea, summary, [tabindex] {
        -webkit-tap-highlight-color: transparent;
      }
      :where(a, button, input, select, textarea, summary, [tabindex]):focus-visible {
        outline: 2px solid var(--agent-ink);
        outline-offset: 2px;
        box-shadow: var(--agent-focus);
      }
      :where(h1, h2, h3, h4, h5, h6) {
        font-family: var(--agent-font-heading);
        color: var(--agent-ink);
      }
      :where(input, select, textarea) {
        min-height: 42px;
        border: 1px solid var(--agent-border);
        border-radius: var(--agent-radius-md);
        background: var(--agent-surface);
        color: var(--agent-ink);
        transition: border-color var(--agent-duration-fast) ease, box-shadow var(--agent-duration-fast) ease;
      }
      :where(input, select, textarea):hover { border-color: rgba(43,54,68,.26); }
      :where(input, select, textarea):disabled {
        background: var(--agent-surface-soft);
        color: var(--agent-ink-muted);
        cursor: not-allowed;
      }
      ::placeholder { color: rgba(93,105,119,.72); }
      .agent-page {
        width: min(100%, 1180px);
        margin-inline: auto;
        padding: 28px 24px 64px;
      }
      .agent-page--wide { width: min(100%, 1320px); }
      .agent-page--compact { width: min(100%, 760px); }
      .agent-page-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 20px;
        margin-bottom: 24px;
      }
      .agent-page-title { margin: 0; font-size: clamp(1.625rem, 3vw, 2rem); line-height: 1.1; font-weight: 800; letter-spacing: -.025em; }
      .agent-page-description { margin: 7px 0 0; max-width: 72ch; color: var(--agent-ink-muted); line-height: 1.55; }
      .agent-eyebrow { margin: 0 0 7px; font: 800 .6875rem/1.2 var(--agent-font-heading); letter-spacing: .08em; text-transform: uppercase; color: var(--agent-accent-strong); }
      .agent-panel {
        background: var(--agent-surface);
        border: 1px solid var(--agent-border);
        border-radius: var(--agent-radius-lg);
        box-shadow: var(--agent-shadow-resting);
      }
      .agent-empty, .agent-error, .agent-loading {
        padding: 32px 24px;
        border: 1px dashed var(--agent-border);
        border-radius: var(--agent-radius-lg);
        background: var(--agent-surface);
        color: var(--agent-ink-muted);
        text-align: center;
      }
      .agent-error { border-style: solid; border-color: rgba(154,90,78,.35); background: rgba(154,90,78,.07); color: var(--agent-danger); }
      @media (max-width: 720px) {
        .agent-page { padding: 22px 16px 44px; }
        .agent-page-header { flex-direction: column; align-items: stretch; }
        .agent-page-header > :where(a, button, .agent-actions) { width: 100%; }
      }
      @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after { scroll-behavior: auto !important; transition-duration: .01ms !important; animation-duration: .01ms !important; animation-iteration-count: 1 !important; }
      }
    """
