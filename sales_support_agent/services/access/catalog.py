"""The tool catalog — single source of truth for per-tool permissions.

Every individual tool a role can be granted is listed here exactly once. The
catalog drives three things so they never drift:
  * the role editor UI (the checkbox grid),
  * the route guards (`require_tool` / `has_tool` map a request path -> tool key),
  * nav filtering (which sections/sub-links a user may see).

Add a tool here and all three stay in sync.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tool:
    key: str            # stable permission key stored in role.permissions_json
    label: str          # human label in the role editor + nav
    section: str        # primary nav section this tool belongs to
    url_prefixes: tuple  # request paths this tool guards
    exact: bool = False  # match prefix exactly (avoid e.g. /admin matching everything)


# Ordered; grouped by section. `access.manage` gates the RBAC admin UI itself.
TOOL_CATALOG: tuple[Tool, ...] = (
    Tool("sales.priorities", "Sales Priorities", "Sales Priorities", ("/admin",), exact=True),
    Tool("sales.decks", "Generate sales deck", "Sales Priorities", ("/admin/sales-decks", "/admin/api/generate-deck", "/admin/api/deck-runs")),
    Tool("website_ops.seo", "SEO Dashboard", "Website Ops", ("/admin/website-ops",), exact=True),
    Tool("website_ops.queue", "Queue", "Website Ops", ("/admin/website-ops/queue",)),
    Tool("website_ops.reports", "Reports", "Website Ops", ("/admin/website-ops/reports",)),
    Tool("finance", "Finance", "Finance", ("/admin/finances",)),
    Tool("advertising.audit", "Advertising Audit", "Advertising", ("/admin/advertising",)),
    Tool("executive.summary", "Executive Summary", "Executive", ("/admin/executive",), exact=True),
    Tool("executive.brand_analysis", "Brand Analysis", "Executive", ("/admin/executive/brand-analysis",)),
    Tool("fulfillment.rate_sheets", "Fulfillment Sales Deck", "Fulfillment", ("/admin/fulfillment/sales",)),
    Tool("fulfillment.dashboard", "CS Dashboard", "Fulfillment", ("/admin/fulfillment/cs",), exact=True),
    Tool("fulfillment.reports", "CS Reports", "Fulfillment", ("/admin/fulfillment/cs/reports",)),
    # HR — employees/time/reports under hr.access; the sensitive money + config
    # (payroll runs, pay schedules, tax settings) gated separately by hr.payroll.
    # Most-specific prefix wins, so /admin/hr/payroll resolves to hr.payroll even
    # though hr.access covers the broader /admin/hr.
    Tool("hr.access", "HR — people, time & reports", "HR", ("/admin/hr",)),
    Tool("hr.payroll", "HR — run payroll & settings", "HR", ("/admin/hr/payroll", "/admin/hr/settings")),
    Tool("access.manage", "Access admin (users & roles)", "Access", ("/admin/access",)),
)

ALL_TOOL_KEYS: frozenset = frozenset(t.key for t in TOOL_CATALOG)
_BY_KEY = {t.key: t for t in TOOL_CATALOG}

# Section -> ordered list of its tools, for nav filtering / role-editor grouping.
SECTIONS: dict = {}
for _t in TOOL_CATALOG:
    SECTIONS.setdefault(_t.section, []).append(_t)


def tool(key: str):
    return _BY_KEY.get(key)


def label_for(key: str) -> str:
    t = _BY_KEY.get(key)
    return t.label if t else key


def valid_keys(keys) -> list:
    """Filter an arbitrary iterable down to known catalog keys (defensive — a
    stale key in a stored role never grants access or crashes the editor)."""
    return [k for k in (keys or []) if k in ALL_TOOL_KEYS]


def section_has_any(section: str, granted: set) -> bool:
    return any(t.key in granted for t in SECTIONS.get(section, ()))
