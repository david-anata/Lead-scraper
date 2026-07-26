"""Canonical document shell helpers for Agent HTML pages."""

from __future__ import annotations

import html

from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_stylesheet_links,
)


def render_document_head(
    title: str,
    *,
    include_operator_styles: bool = True,
    extra_head: str = "",
) -> str:
    """Render shared metadata, fonts, favicon, and cacheable shell styles."""
    styles = render_agent_stylesheet_links() if include_operator_styles else (
        '<link rel="stylesheet" href="/static/admin.css?v=2">'
    )
    return f"""<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
{render_agent_favicon_links()}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
{styles}
{extra_head}"""


def render_operator_document(
    *,
    title: str,
    navigation: str,
    body: str,
    page_class: str = "",
    extra_head: str = "",
) -> str:
    """Render an authenticated page with the canonical landmarks."""
    class_name = f"app app--operator {page_class}".strip()
    return f"""<!doctype html>
<html lang="en">
<head>{render_document_head(title, extra_head=extra_head)}</head>
<body class="{html.escape(class_name)}">
  {navigation}
  <main id="agent-main-content" class="app-container app-page">{body}</main>
</body>
</html>"""


def render_transition_document(
    *,
    title: str,
    body: str,
    extra_head: str = "",
) -> str:
    """Render signed-out, pending, permission, and recovery states."""
    return f"""<!doctype html>
<html lang="en">
<head>{render_document_head(title, include_operator_styles=False, extra_head=extra_head)}</head>
<body class="app app--transition">
  <main id="agent-main-content" class="app-container app-container--focused app-page">{body}</main>
</body>
</html>"""
