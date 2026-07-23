from sales_support_agent.services.public_report_ui import (
    PUBLIC_REPORT_DESIGN_VERSION,
    public_report_foundation_css,
    render_public_recovery_page,
)
from sales_support_agent.services.fulfillment_dashboard import render_fulfillment_report_detail_page
from sales_support_agent.services.hr.pages import render_hr_reports


def test_public_report_foundation_has_accessibility_and_print_contracts() -> None:
    css = public_report_foundation_css()
    assert ".public-report-skip" in css
    assert ":focus-visible" in css
    assert "prefers-reduced-motion" in css
    assert "@media print" in css
    assert "--anata-ink: #2b3644" in css


def test_public_recovery_page_is_branded_neutral_and_non_enumerating() -> None:
    html = render_public_recovery_page(report_kind="rate sheet")
    assert PUBLIC_REPORT_DESIGN_VERSION in html
    assert "This report is unavailable" in html
    assert "noindex, nofollow" in html
    assert "Ask the person who shared it" in html
    assert "record exists" not in html
    assert "exception" not in html


def test_fulfillment_report_detail_exposes_all_saved_artifacts() -> None:
    html = render_fulfillment_report_detail_page(
        {"summary": {}, "warnings": [], "escalations": []},
        report_slug="daily-2026-07-23",
    )
    assert "/admin/fulfillment/cs/reports/daily-2026-07-23.html" in html
    assert "/admin/fulfillment/cs/reports/daily-2026-07-23.md" in html
    assert "/admin/fulfillment/cs/reports/daily-2026-07-23.json" in html
    assert 'aria-label="Report downloads"' in html


def test_hr_reports_describe_freshness_and_name_each_export() -> None:
    html = render_hr_reports(user={"name": "QA Operator", "email": "qa@anatainc.com"})
    assert "Available now:" in html
    assert "Files reflect approved records available when you download them" in html
    assert 'aria-label="Download Employee directory as CSV"' in html
