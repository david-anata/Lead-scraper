"""Structural guardrails for the authenticated Agent application shell."""

from __future__ import annotations

from pathlib import Path

from sales_support_agent.services.admin_nav import (
    render_agent_nav,
    render_agent_nav_styles,
)


ROOT = Path(__file__).resolve().parents[1]


def test_section_navigation_uses_full_width_band_and_constrained_row() -> None:
    styles = render_agent_nav_styles()
    nav = render_agent_nav(
        "sales",
        sales_section="sales_deals",
        permissions={"sales.deals", "sales.priorities"},
    )

    assert ".topbar-section-band" in styles
    assert ".topbar-section-row" in styles
    assert "max-width: 1320px" in styles
    assert '<div class="topbar-section-band">' in nav
    assert 'href="/admin/sales/fix-queue"' in nav


def test_authenticated_page_families_use_canonical_canvas_width() -> None:
    sources = (
        "sales_support_agent/static/admin.css",
        "sales_support_agent/static/finance.css",
        "sales_support_agent/api/sales_router.py",
        "sales_support_agent/services/admin_dashboard.py",
        "sales_support_agent/services/advertising/audit_page.py",
        "sales_support_agent/services/fulfillment_dashboard.py",
        "sales_support_agent/services/sales/deal_detail.py",
        "sales_support_agent/services/sales/rep_dashboard.py",
        "sales_support_agent/services/website_ops.py",
    )

    for relative_path in sources:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "max-width: 1320px" in text or "max-width:1320px" in text


def test_legacy_operator_canvas_widths_are_not_reintroduced() -> None:
    sources = (
        "sales_support_agent/static/admin.css",
        "sales_support_agent/static/finance.css",
        "sales_support_agent/services/admin_dashboard.py",
        "sales_support_agent/services/advertising/audit_page.py",
        "sales_support_agent/services/fulfillment_dashboard.py",
        "sales_support_agent/services/sales/deal_detail.py",
        "sales_support_agent/services/sales/rep_dashboard.py",
        "sales_support_agent/services/website_ops.py",
    )
    legacy_declarations = (
        "max-width: 1160px;",
        "max-width:1160px;",
        "max-width: 1180px;",
        "max-width:1180px;",
        "max-width: 1280px;",
        "max-width:1280px;",
        "max-width: 1480px;",
        "max-width:1480px;",
    )

    for relative_path in sources:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        for declaration in legacy_declarations:
            assert declaration not in text
