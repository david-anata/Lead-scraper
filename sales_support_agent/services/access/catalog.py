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
    any_of: tuple = ()   # narrower capabilities that may also satisfy this route


# Ordered; grouped by section. `access.manage` gates the RBAC admin UI itself.
TOOL_CATALOG: tuple[Tool, ...] = (
    Tool("sales.deals", "Control Room & Deal Board", "Sales", ("/admin/sales",)),
    Tool("sales.priorities", "Fix Queue", "Sales", ("/admin",), exact=True),
    Tool("sales.decks", "Sales Decks", "Sales", ("/admin/sales-decks", "/admin/sales/decks", "/admin/api/generate-deck", "/admin/api/deck-runs")),
    Tool("website_ops.seo", "Overview", "Website Ops", ("/admin/website-ops", "/admin/api/website-ops/run", "/admin/api/website-ops/status"), exact=True),
    Tool("website_ops.queue", "Queue", "Website Ops", ("/admin/website-ops/queue", "/admin/website-ops/feedback", "/admin/api/website-ops/feedback")),
    Tool("website_ops.reports", "Reports", "Website Ops", ("/admin/website-ops/reports",)),
    Tool("finance", "Finance", "Finance", ("/admin/finances",)),
    Tool("building.manage", "Building operations", "Building", ("/admin/building",)),
    Tool("advertising.audit", "Advertising Audit", "Advertising", ("/admin/advertising",)),
    Tool("executive.summary", "Executive Summary", "Executive", ("/admin/executive",), exact=True),
    Tool("executive.brand_analysis", "Brand Analysis", "Executive", ("/admin/executive/brand-analysis",)),
    Tool("fulfillment.rate_sheets", "Sales Pipeline", "Fulfillment", ("/admin/fulfillment/sales",)),
    Tool("fulfillment.dashboard", "CS Action Queue", "Fulfillment", ("/admin/fulfillment/cs",), exact=True),
    Tool("fulfillment.reports", "CS Reports", "Fulfillment", ("/admin/fulfillment/cs/reports",)),
    # HR — employees/time/reports under hr.access; the sensitive money + config
    # (payroll runs, pay schedules, tax settings) gated separately by hr.payroll.
    # Most-specific prefix wins, so /admin/hr/payroll resolves to hr.payroll even
    # though hr.access covers the broader /admin/hr.
    Tool("hr.access", "HR — employee self-service", "HR", ("/admin/hr",)),
    Tool(
        "hr.payroll", "HR — legacy full payroll administration", "HR",
        ("/admin/hr/payroll", "/admin/hr/settings"),
        any_of=(
            "hr.payroll.view", "hr.payroll.prepare", "hr.payroll.approve",
            "hr.payroll.submit", "hr.settings.manage",
        ),
    ),
    Tool("hr.people.view", "HR — view people", "HR", ()),
    Tool("hr.people.manage", "HR — manage people", "HR", ()),
    Tool("hr.compensation.view", "HR — view compensation", "HR", ()),
    Tool("hr.compensation.manage", "HR — manage compensation", "HR", ()),
    Tool("hr.time.approve_team", "HR — approve team time", "HR", ()),
    Tool("hr.payroll.view", "HR — view payroll", "HR", ()),
    Tool("hr.payroll.prepare", "HR — prepare payroll", "HR", ()),
    Tool("hr.payroll.approve", "HR — approve payroll", "HR", ()),
    Tool("hr.payroll.submit", "HR — record payment/provider actions", "HR", ()),
    Tool("hr.settings.manage", "HR — manage payroll settings", "HR", ()),
    Tool("hr.audit.view", "HR — view audit and exports", "HR", ()),
    Tool("access.manage", "People and access", "Access", ("/admin/access",)),
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


def grants_tool(granted: set, key: str) -> bool:
    """Return whether a permission set satisfies a catalog route capability."""
    selected = _BY_KEY.get(key)
    return bool(
        key in granted
        or (selected and any(candidate in granted for candidate in selected.any_of))
    )
