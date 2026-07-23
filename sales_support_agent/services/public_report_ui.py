"""Shared presentation primitives for recipient-facing Agent deliverables.

This module owns only visual and accessibility scaffolding. It deliberately
does not inspect records, validate tokens, mutate tracking, or interpret report
data; those responsibilities stay with each route and renderer.
"""

from __future__ import annotations

from html import escape


PUBLIC_REPORT_DESIGN_VERSION = "anata-public-report-v1"


def public_report_foundation_css() -> str:
    """Return the canonical, dependency-free public report foundation."""

    return """
:root {
  --anata-background: #f9f7f3;
  --anata-surface: #ffffff;
  --anata-surface-soft: #f2ece3;
  --anata-ink: #2b3644;
  --anata-ink-muted: #5d6977;
  --anata-border: rgba(43, 54, 68, 0.12);
  --anata-accent: #85bbda;
  --anata-accent-strong: #5e9fc4;
  --anata-accent-soft: rgba(133, 187, 218, 0.18);
  --anata-support: #bfa889;
  --anata-success: #477a59;
  --anata-warning: #986817;
  --anata-danger: #8b4c42;
  --anata-shadow: 0 20px 54px -40px rgba(43, 54, 68, 0.42);
  --anata-radius-sm: 8px;
  --anata-radius-md: 14px;
  --anata-radius-lg: 22px;
  --anata-radius-pill: 999px;
}
*, *::before, *::after { box-sizing: border-box; }
html { color-scheme: light; }
body {
  margin: 0;
  background: var(--anata-background);
  color: var(--anata-ink);
  font-family: "Inter", "Segoe UI", system-ui, sans-serif;
  line-height: 1.55;
}
.public-report-skip {
  position: fixed;
  z-index: 9999;
  top: 12px;
  left: 12px;
  transform: translateY(-160%);
  padding: 10px 14px;
  border-radius: var(--anata-radius-sm);
  background: var(--anata-ink);
  color: #fff;
  font-family: "Montserrat", "Inter", sans-serif;
  font-weight: 800;
}
.public-report-skip:focus { transform: translateY(0); }
.public-report-wordmark {
  margin: 0;
  font-family: "Montserrat", "Inter", sans-serif;
  font-size: 21px;
  font-weight: 900;
  letter-spacing: -0.04em;
}
.public-report-wordmark span { color: var(--anata-accent); }
.public-report-eyebrow {
  margin: 0;
  color: var(--anata-accent-strong);
  font-family: "Montserrat", "Inter", sans-serif;
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.public-report-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
}
.public-report-action {
  display: inline-flex;
  min-height: 44px;
  align-items: center;
  justify-content: center;
  padding: 0 16px;
  border: 1px solid var(--anata-border);
  border-radius: var(--anata-radius-pill);
  background: var(--anata-surface);
  color: var(--anata-ink);
  font-family: "Montserrat", "Inter", sans-serif;
  font-size: 12px;
  font-weight: 800;
  text-decoration: none;
  cursor: pointer;
}
.public-report-action:hover { border-color: var(--anata-accent-strong); }
.public-report-action--primary {
  border-color: var(--anata-ink);
  background: var(--anata-ink);
  color: #fff;
}
.public-report-action:focus-visible,
.public-report-toolbar button:focus-visible,
.public-report-toolbar a:focus-visible {
  outline: 3px solid var(--anata-accent);
  outline-offset: 3px;
}
.public-report-live[aria-live] {
  min-height: 1.25em;
  color: var(--anata-ink-muted);
  font-size: 13px;
}
.public-report-recovery {
  width: min(680px, calc(100% - 40px));
  margin: clamp(40px, 10vh, 110px) auto;
  padding: clamp(28px, 5vw, 52px);
  border: 1px solid var(--anata-border);
  border-radius: var(--anata-radius-lg);
  background: var(--anata-surface);
  box-shadow: var(--anata-shadow);
}
.public-report-recovery h1 {
  margin: 10px 0 12px;
  font-family: "Montserrat", "Inter", sans-serif;
  font-size: clamp(28px, 5vw, 42px);
  line-height: 1.05;
  letter-spacing: -0.035em;
}
.public-report-recovery p {
  max-width: 58ch;
  color: var(--anata-ink-muted);
}
.public-report-recovery__steps {
  margin: 24px 0 0;
  padding: 22px;
  border-radius: var(--anata-radius-md);
  background: var(--anata-surface-soft);
}
.public-report-recovery__steps strong {
  font-family: "Montserrat", "Inter", sans-serif;
  font-size: 13px;
}
.public-report-recovery__steps ol { margin-bottom: 0; padding-left: 20px; }
.public-report-footer {
  margin-top: 32px;
  padding-top: 18px;
  border-top: 1px solid var(--anata-border);
  color: var(--anata-ink-muted);
  font-size: 12px;
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    scroll-behavior: auto !important;
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
@media (max-width: 520px) {
  .public-report-toolbar { align-items: stretch; flex-direction: column; }
  .public-report-action { width: 100%; }
}
@media print {
  .public-report-skip,
  .public-report-toolbar,
  .public-report-live { display: none !important; }
  body { background: #fff; }
}
"""


def render_public_recovery_page(
    *,
    report_kind: str = "report",
    title: str = "This report is unavailable",
) -> str:
    """Render a neutral, non-enumerating public 404 page."""

    kind = escape(report_kind.strip() or "report")
    safe_title = escape(title.strip() or "This report is unavailable")
    return f"""<!doctype html>
<html lang="en" data-design-system="{PUBLIC_REPORT_DESIGN_VERSION}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex, nofollow">
  <title>{safe_title} | Anata</title>
  <style>{public_report_foundation_css()}</style>
</head>
<body>
  <a class="public-report-skip" href="#report-content">Skip to report status</a>
  <main id="report-content" class="public-report-recovery">
    <p class="public-report-wordmark" aria-label="Anata">anata<span>.</span></p>
    <p class="public-report-eyebrow">{kind} access</p>
    <h1>{safe_title}</h1>
    <p>The requested report was not found at this link. It may be incomplete, replaced, or no longer available. For privacy, we cannot provide additional details about this shared {kind}.</p>
    <section class="public-report-recovery__steps" aria-labelledby="recovery-title">
      <strong id="recovery-title">What you can do</strong>
      <ol>
        <li>Check that the complete link was copied into your browser.</li>
        <li>Ask the person who shared it to send a current link.</li>
      </ol>
    </section>
    <footer class="public-report-footer">Shared securely through Anata Agent.</footer>
  </main>
</body>
</html>"""
