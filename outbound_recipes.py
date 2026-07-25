"""Named StoreLeads pull recipes: what we pull, and when.

Each recipe is a saved StoreLeads query plus the timing that makes it a genuine
"why now" trigger rather than a static list. StoreLeads refreshes its data weekly
(normally Monday), so trigger recipes are pulled early in the week when the
newly-changed rows appear; the baseline recipe runs daily to keep volume steady.

Every recipe carries the reason it fired. That reason is what makes the opening
line defensible and it is also what we measure: the scoreboard reports booked
calls by recipe, so recipes that do not earn their place get retired.

Pure functions of a clock, so the exact query for any date is unit-testable
without a live key.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

# ---- ICP floor, applied to EVERY recipe --------------------------------------
# StoreLeads' revenue filters are MONTHLY sales in cents; our band is yearly.
_YEARLY_MIN_CENTS = 1_000_000_00      # $1M/yr
_YEARLY_MAX_CENTS = 15_000_000_00     # $15M/yr
MONTHLY_MIN_CENTS = _YEARLY_MIN_CENTS // 12
MONTHLY_MAX_CENTS = _YEARLY_MAX_CENTS // 12

BASE_FILTERS: dict[str, Any] = {
    "f:p": "shopify",
    "f:cc": "US,GB,CA,AU",
    "f:cc:op": "or",
    "f:ds": "Active",                       # skip password-protected / inactive
    "f:it": "email",                        # must have a contact route
    "f:ermin": MONTHLY_MIN_CENTS,
    "f:ermax": MONTHLY_MAX_CENTS,
    "f:tags": "Dropshipper,Print on Demand",
    "f:tags:op": "not",                     # brief 2: these are never a fit
}

# High-signal apps. Installing one means budget just moved; uninstalling one
# means they had the problem, tried a tool, and still have the problem.
GROWTH_APP_TOKENS = (
    "shopify.triplewhale-1",                # attribution/analytics
    "shopify.intelligems",                  # A/B testing
    "shopify.polar-analytics",              # analytics
    "shopify.lucky-orange",                 # heatmaps / CRO
    "shopify.microsoft-clarity",            # session replay
    "shopify.knocommerce",                  # post-purchase surveys / attribution
    "shopify.octane-ai-quiz-personalization",
    "shopify.rebuy",                        # personalisation / AOV
    "shopify.product-ads-by-criteo",        # paid ads
    "shopify.impact-1",                     # affiliate / partnerships
)


def _iso(dt: datetime) -> str:
    """StoreLeads wants JSON-style timestamps, e.g. 2026-04-23T18:25:43.511Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ---- tunables ----------------------------------------------------------------
# Everything an operator should be able to change without a code deploy. Stored
# in the database and edited on the Lead Ops page; these are only the fallbacks.
DEFAULT_SETTINGS: dict[str, Any] = {
    "new_growth_app.window_days": 14,     # how recent an install still counts
    "churned_tool.tools_per_day": 1,      # how many tools we check for churn daily
    "new_growth_app.max_per_run": 40,
    "churned_tool.max_per_run": 30,
    "plan_upgrade.max_per_run": 30,
    "replatformed.max_per_run": 25,
    "social_surge.max_per_run": 25,
    "icp_baseline.max_per_run": 25,
    "social_surge.min_growth_pct": 25,
    "plan_upgrade.window_days": 60,
    "replatformed.window_days": 90,
    "churned_tool.window_days": 30,
}

# Reasons that mention a time window must be regenerated when that window is
# retuned, because this text is what the prospect actually reads.
_REASON_TEMPLATES: dict[str, str] = {
    "new_growth_app": "They added a growth or conversion tool in the last {days} days",
    "churned_tool": "They removed a growth tool in the last {days} days, so the problem is still open",
    "plan_upgrade": "They upgraded their store plan in the last {days} days, which usually means new budget",
    "replatformed": "They moved platforms in the last {days} days, so vendors are being chosen right now",
}

TUNABLE_LABELS: dict[str, str] = {
    "new_growth_app.window_days": "Just installed: how many days back still counts",
    "churned_tool.tools_per_day": "Just dropped: how many tools to check each day",
    "churned_tool.window_days": "Just dropped: how many days back still counts",
    "plan_upgrade.window_days": "Plan upgrade: how many days back still counts",
    "replatformed.window_days": "Replatformed: how many days back still counts",
    "social_surge.min_growth_pct": "Social surge: minimum follower growth percent",
}


def setting(settings: Optional[dict[str, Any]], key: str) -> Any:
    """Read a tunable, falling back to the shipped default."""
    if settings and key in settings and settings[key] not in (None, ""):
        return settings[key]
    return DEFAULT_SETTINGS.get(key)


def _int(settings: Optional[dict[str, Any]], key: str) -> int:
    try:
        return int(setting(settings, key))
    except (TypeError, ValueError):
        return int(DEFAULT_SETTINGS.get(key) or 0)


@dataclass(frozen=True)
class Recipe:
    key: str
    label: str
    reason: str                 # the human "why now", used for the opener
    tier: str                   # expected heat: A hottest
    cadence: str                # "weekly" (trigger) or "daily" (baseline)
    max_per_run: int
    build: Callable[..., dict[str, Any]] = field(repr=False)
    # Some triggers cannot be expressed as a single StoreLeads filter, so the
    # server narrows and this re-checks each row. Verified live 25 Jul: the
    # app install/uninstall filters accept ONE app id, not a list, so a list
    # silently returns zero rows.
    client_filter: Optional[Callable[..., bool]] = field(default=None, repr=False)

    def params(self, now: Optional[datetime] = None,
               settings: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Full StoreLeads query for this recipe at a point in time."""
        now = now or datetime.now(timezone.utc)
        return {**BASE_FILTERS, **self.build(now, settings)}

    def keeps(self, store: dict[str, Any], now: Optional[datetime] = None,
              settings: Optional[dict[str, Any]] = None) -> bool:
        if self.client_filter is None:
            return True
        return self.client_filter(store, now or datetime.now(timezone.utc), settings)

    def reason_for(self, settings: Optional[dict[str, Any]] = None) -> str:
        """The 'why now' line, kept truthful when a window is retuned. This text
        reaches the prospect, so it must never claim a window we did not use."""
        tmpl = _REASON_TEMPLATES.get(self.key)
        if not tmpl:
            return self.reason
        days = _int(settings, f"{self.key}.window_days")
        return tmpl.format(days=days, weeks=max(1, round(days / 7)))

    def cap(self, settings: Optional[dict[str, Any]] = None) -> int:
        return _int(settings, f"{self.key}.max_per_run") or self.max_per_run

    def enabled(self, settings: Optional[dict[str, Any]] = None) -> bool:
        val = setting(settings, f"{self.key}.enabled")
        return True if val is None else str(val).lower() not in ("0", "false", "no", "off")


def _installed_recently(store: dict[str, Any], now: datetime,
                        settings: Optional[dict[str, Any]] = None) -> bool:
    """True if the store installed one of our growth apps inside the window."""
    days = _int(settings, "new_growth_app.window_days")
    for app in store.get("apps") or []:
        if not isinstance(app, dict):
            continue
        token = f"{app.get('platform', 'shopify')}.{app.get('token', '')}"
        if token not in GROWTH_APP_TOKENS:
            continue
        raw = str(app.get("installed_at") or "").strip().replace("Z", "+00:00")
        if not raw:
            continue
        try:
            installed = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if installed.tzinfo is None:
            installed = installed.replace(tzinfo=timezone.utc)
        age = (now - installed).days
        if 0 <= age <= days:
            return True
    return False


def _new_growth_app(now: datetime, settings=None) -> dict[str, Any]:
    # Server narrows to "has one of these apps"; the install window is applied
    # client-side because f:app_installed_at only accepts a single app id.
    return {
        "f:an": ",".join(GROWTH_APP_TOKENS),
        "f:an:op": "or",
        "sort": "-er,rank",
    }


def churn_tokens_for(now: datetime, settings=None) -> list[str]:
    """Which tools we check for churn today. f:app_uninstalled_at takes ONE app
    id, so we rotate; checking more per day widens coverage at the cost of more
    calls."""
    per_day = max(1, _int(settings, "churned_tool.tools_per_day"))
    start = (now.timetuple().tm_yday * per_day) % len(GROWTH_APP_TOKENS)
    return [GROWTH_APP_TOKENS[(start + i) % len(GROWTH_APP_TOKENS)]
            for i in range(min(per_day, len(GROWTH_APP_TOKENS)))]


def _churned_tool(now: datetime, settings=None) -> dict[str, Any]:
    return {
        "f:app_uninstalled_at": churn_tokens_for(now, settings)[0],
        "f:app_uninstalled_at:min": _iso(
            now - timedelta(days=_int(settings, "churned_tool.window_days"))),
        "sort": "-er,rank",
    }


def _plan_upgrade(now: datetime, settings=None) -> dict[str, Any]:
    return {
        "f:plan": "Shopify Plus",
        "f:last_plan_change_at:min": _iso(
            now - timedelta(days=_int(settings, "plan_upgrade.window_days"))),
        "sort": "-lplancat,rank",
    }


def _replatformed(now: datetime, settings=None) -> dict[str, Any]:
    return {
        "f:last_platform_change_at:min": _iso(
            now - timedelta(days=_int(settings, "replatformed.window_days"))),
        "sort": "-er,rank",
    }


def _social_surge(now: datetime, settings=None) -> dict[str, Any]:
    return {
        "f:tiktokfollowers30dpmin": _int(settings, "social_surge.min_growth_pct"),
        "sort": "-fc30dp_10,rank",
    }


def _icp_baseline(now: datetime, settings=None) -> dict[str, Any]:
    return {"sort": "-er,rank"}


RECIPES: tuple[Recipe, ...] = (
    Recipe(
        key="new_growth_app",
        label="Just installed a growth or CRO app",
        reason="They added a growth or conversion tool in the last two weeks",
        tier="A", cadence="weekly", max_per_run=40, build=_new_growth_app,
        client_filter=_installed_recently,
    ),
    Recipe(
        key="churned_tool",
        label="Just dropped a growth tool",
        reason="They removed a growth tool recently, so the problem is still open",
        tier="A", cadence="weekly", max_per_run=30, build=_churned_tool,
    ),
    Recipe(
        key="plan_upgrade",
        label="Upgraded to Shopify Plus",
        reason="They upgraded their store plan recently, which usually means new budget",
        tier="B", cadence="weekly", max_per_run=30, build=_plan_upgrade,
    ),
    Recipe(
        key="replatformed",
        label="Recently replatformed",
        reason="They moved platforms recently, so vendors are being chosen right now",
        tier="B", cadence="weekly", max_per_run=25, build=_replatformed,
    ),
    Recipe(
        key="social_surge",
        label="Social following spiking",
        reason="Their social following jumped in the last month",
        tier="B", cadence="weekly", max_per_run=25, build=_social_surge,
    ),
    Recipe(
        key="icp_baseline",
        label="Core ICP (volume filler)",
        reason="They fit our ideal customer profile",
        tier="C", cadence="daily", max_per_run=25, build=_icp_baseline,
    ),
)

_BY_KEY = {r.key: r for r in RECIPES}


def recipe(key: str) -> Optional[Recipe]:
    return _BY_KEY.get(key)


def recipes_for_day(weekday: int, settings: Optional[dict[str, Any]] = None) -> list[Recipe]:
    """Which recipes run on a given weekday (Mon=0). StoreLeads refreshes weekly
    on Monday, so triggers are pulled Tue/Wed once the new rows have landed;
    the baseline runs every weekday to keep volume steady. Recipes switched off
    in settings never run."""
    if weekday >= 5:            # weekends: no sending, so no pulling
        return []
    live = [r for r in RECIPES if r.enabled(settings)]
    out = [r for r in live if r.cadence == "daily"]
    if weekday in (1, 2):       # Tuesday, Wednesday
        out = [r for r in live if r.cadence == "weekly"] + out
    return out


def daily_plan(now: Optional[datetime] = None,
               settings: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """What we intend to pull today, and why. Drives the app's Lead Ops page."""
    now = now or datetime.now(timezone.utc)
    todays = recipes_for_day(now.weekday(), settings)
    return {
        "date": now.date().isoformat(),
        "weekday": now.strftime("%A"),
        "recipes": [
            {"key": r.key, "label": r.label, "tier": r.tier,
             "reason": r.reason_for(settings), "max_per_run": r.cap(settings)}
            for r in todays
        ],
        "planned_total": sum(r.cap(settings) for r in todays),
    }
