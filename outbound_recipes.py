"""Named StoreLeads pull recipes: what we pull, and when.

Each recipe is a saved StoreLeads query plus the timing that makes it a genuine
"why now" trigger rather than a static list. StoreLeads refreshes its data weekly
(normally Monday), so trigger recipes start Tuesday, once that refresh has landed,
and are spread across the week so the next morning does not re-scan the same pool.
The baseline recipe runs every weekday to keep volume steady.

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
# The revenue band lives in DEFAULT_SETTINGS so it can be retuned in the app.
# There are deliberately no module-level copies of it: a second definition is
# how a "we changed the target" edit silently fails to take effect.

def base_filters(settings: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """The ICP floor applied to EVERY recipe. The revenue band is tunable on the
    Lead Ops page and takes effect on the very next pull.

    StoreLeads filters on MONTHLY sales in cents, while we think in dollars per
    year, so the conversion happens here in one place.
    """
    lo = _int(settings, "icp.revenue_min_usd") * 100 // 12
    hi = _int(settings, "icp.revenue_max_usd") * 100 // 12
    # Revenue alone is a weak size gate: StoreLeads call their sales figures
    # "directionally useful, not perfectly accurate", so an understated estimate
    # lets a much bigger company through. Headcount is the harder cross-check.
    emp_lo = _int(settings, "icp.employees_min")
    emp_hi = _int(settings, "icp.employees_max")
    return {
        "f:p": "shopify",
        "f:cc": "US,GB,CA,AU",
        "f:cc:op": "or",
        "f:ds": "Active",                       # skip password-protected / inactive
        "f:it": "email",                        # must have a contact route
        "f:ermin": lo,
        "f:ermax": hi,
        "f:empcmin": emp_lo,
        "f:empcmax": emp_hi,
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
    """StoreLeads wants JSON-style timestamps, e.g. 2012-04-23T18:25:43.511Z."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _parse_app_date(value: Any) -> Optional[datetime]:
    """Parse an app install or removal timestamp, including nanosecond fractions."""
    raw = str(value or "").strip()
    if not raw:
        return None
    text = raw.replace("Z", "+00:00")
    if "." in text:
        head, tail = text.split(".", 1)
        digits = []
        rest = ""
        for index, char in enumerate(tail):
            if char.isdigit():
                digits.append(char)
            else:
                rest = tail[index:]
                break
        text = f"{head}.{(''.join(digits) + '000000')[:6]}{rest}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _app_id(app: dict[str, Any]) -> str:
    """StoreLeads app id: platform + token, e.g. shopify.rebuy."""
    token = str(app.get("token") or "").strip()
    if not token:
        return ""
    if token.startswith("shopify."):
        return token
    platform = str(app.get("platform") or "shopify").strip() or "shopify"
    return f"{platform}.{token}"


# ---- tunables ----------------------------------------------------------------
# Everything an operator should be able to change without a code deploy. Stored
# in the database and edited on the Lead Ops page; these are only the fallbacks.
DEFAULT_SETTINGS: dict[str, Any] = {
    "icp.revenue_min_usd": 1_000_000,     # smallest brand worth our time
    "icp.revenue_max_usd": 20_000_000,    # largest before they have an agency
    "icp.employees_min": 2,               # a real business, not a side project
    "icp.employees_max": 80,              # the stated ICP, now actually enforced
    "new_growth_app.window_days": 14,     # how recent an install still counts
    # One call per app. The uninstall filter accepts a single id, so a default
    # of 1 left most of the list unchecked and a miss looked like an empty recipe.
    "churned_tool.tools_per_day": len(GROWTH_APP_TOKENS),
    "new_growth_app.max_per_run": 100,
    "churned_tool.max_per_run": 60,
    "plan_upgrade.max_per_run": 100,
    "replatformed.max_per_run": 100,
    "social_surge.max_per_run": 100,
    "icp_baseline.max_per_run": 200,
    "social_surge.min_growth_pct": 25,
    "plan_upgrade.window_days": 180,
    "replatformed.window_days": 180,
    "churned_tool.window_days": 30,
    # Amazon marketplace check. A brand losing control of its own listings is a
    # real "why now", but a wrong claim is worse than no claim, so the gates are
    # deliberately conservative and every one of them is tunable in the app.
    "amazon.enabled": True,
    "amazon.min_unknown_sellers": 3,
    "amazon.max_listings_checked": 1,
    "amazon.max_name_variants": 2,
    "amazon.min_confidence": "high",
    "amazon.follow_country_marketplace": False,
    "amazon.price_erosion_min_pct": 5,
    "amazon.finding_max_age_days": 7,
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
    "icp.revenue_min_usd": "Smallest brand we want (dollars a year)",
    "icp.revenue_max_usd": "Largest brand we want (dollars a year)",
    "icp.employees_min": "Fewest employees",
    "icp.employees_max": "Most employees (a harder size check than revenue)",
    "new_growth_app.window_days": "Just installed: how many days back still counts",
    "churned_tool.tools_per_day": "Just dropped: how many tools to check each day",
    "churned_tool.window_days": "Just dropped: how many days back still counts",
    "plan_upgrade.window_days": "Plan upgrade: how many days back still counts",
    "replatformed.window_days": "Replatformed: how many days back still counts",
    "social_surge.min_growth_pct": "Social surge: minimum follower growth percent",
    "amazon.enabled": "Amazon check: use it at all",
    "amazon.min_unknown_sellers": "Amazon check: how many unknown sellers before we call it a problem",
    "amazon.max_listings_checked": "Amazon check: how many listings we look at per brand",
    "amazon.max_name_variants": "Amazon check: how many spellings of the brand name we try",
    "amazon.min_confidence": "Amazon check: how sure we must be before we mention it",
    "amazon.follow_country_marketplace": "Amazon check: look at a brand's home store instead of the US one",
    "amazon.price_erosion_min_pct": "Amazon check: how far the price must slip to count",
    "amazon.finding_max_age_days": "Amazon check: how many days back a finding still counts",
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
    # server narrows and this re-checks each row. The app install and uninstall
    # filters accept ONE app id. A comma list silently returns zero rows.
    client_filter: Optional[Callable[..., bool]] = field(default=None, repr=False)
    # Mon=0 .. Fri=4. The morning job and the recipe table both read this.
    weekdays: tuple[int, ...] = ()
    # When one filter cannot carry a list, expand returns one partial query
    # per StoreLeads call. None means a single build() query.
    expand: Optional[Callable[..., list]] = field(default=None, repr=False)

    def param_sets(self, now: Optional[datetime] = None,
                   settings: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Every StoreLeads query this recipe should run, ICP floor included.

        A value of None in an expanded query removes that ICP filter for this
        call (the client gate still applies it). Keys starting with '_' are
        internal and are not sent.
        """
        now = now or datetime.now(timezone.utc)
        extras = self.expand(now, settings) if self.expand else [self.build(now, settings)]
        sets = []
        for extra in extras:
            merged = {**base_filters(settings), **(extra or {})}
            sets.append({
                key: value for key, value in merged.items()
                if value is not None and not str(key).startswith("_")
            })
        return sets or [{key: value for key, value in base_filters(settings).items() if value is not None}]

    def params(self, now: Optional[datetime] = None,
               settings: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Full StoreLeads query for this recipe at a point in time."""
        return self.param_sets(now, settings)[0]

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
        if _app_id(app) not in GROWTH_APP_TOKENS:
            continue
        installed = _parse_app_date(app.get("installed_at"))
        if installed is None:
            continue
        age = (now - installed).days
        if 0 <= age <= days:
            return True
    return False


def growth_app_tokens_for(now: datetime, settings=None) -> list[str]:
    """Every growth app, rotated so the cap does not always fill from the same one.

    f:app_installed_at accepts one app id. A comma list returns zero rows and no
    error, so each app is its own call.
    """
    tokens = list(GROWTH_APP_TOKENS)
    start = now.toordinal() % len(tokens)
    return tokens[start:] + tokens[:start]


def _growth_queries(now: datetime, settings=None) -> list[dict[str, Any]]:
    window = _iso(now - timedelta(days=_int(settings, "new_growth_app.window_days")))
    return [
        {
            "f:app_installed_at": token,
            "f:app_installed_at:min": window,
            "sort": "-er,rank",
        }
        for token in growth_app_tokens_for(now, settings)
    ]


def _new_growth_app(now: datetime, settings=None) -> dict[str, Any]:
    return _growth_queries(now, settings)[0]


def churn_tokens_for(now: datetime, settings=None) -> list[str]:
    """Which tools we check for churn today.

    f:app_uninstalled_at takes ONE app id. The list is rotated so the first call
    (and a cap that stops early) does not always start on the same tool.
    """
    per_day = max(1, _int(settings, "churned_tool.tools_per_day"))
    count = min(per_day, len(GROWTH_APP_TOKENS))
    start = now.toordinal() % len(GROWTH_APP_TOKENS)
    return [GROWTH_APP_TOKENS[(start + i) % len(GROWTH_APP_TOKENS)] for i in range(count)]


def _churn_queries(now: datetime, settings=None) -> list[dict[str, Any]]:
    """One uninstall query per tool.

    The date format and the shopify. prefix already match the docs (the same
    timestamp helper is what plan_upgrade and replatformed use successfully).
    What zeroed this recipe is stacking f:it=email plus the employee bounds on
    an already single-app, 30-day filter: that intersection returned an empty
    200. Email and headcount are still required by store_matches_icp.
    """
    window = _iso(now - timedelta(days=_int(settings, "churned_tool.window_days")))
    return [
        {
            "f:app_uninstalled_at": token,
            "f:app_uninstalled_at:min": window,
            "f:it": None,
            "f:empcmin": None,
            "f:empcmax": None,
            "sort": "-er,rank",
        }
        for token in churn_tokens_for(now, settings)
    ]


def _churned_tool(now: datetime, settings=None) -> dict[str, Any]:
    return _churn_queries(now, settings)[0]


def _removed_in_window(store: dict[str, Any], now: datetime,
                       settings: Optional[dict[str, Any]] = None,
                       *, require_removal: bool = False) -> bool:
    """True when apps[].removed_at shows one of today's tools dropped in window.

    require_removal is the fallback path, used when the uninstall filter itself
    returned no domains. The normal path trusts the server if the payload has
    no removal timestamps to check.
    """
    wanted = set(churn_tokens_for(now, settings))
    days = _int(settings, "churned_tool.window_days")
    apps = [app for app in (store.get("apps") or []) if isinstance(app, dict)]
    saw_removal = False
    for app in apps:
        if app.get("removed_at"):
            saw_removal = True
        if _app_id(app) not in wanted:
            continue
        removed = _parse_app_date(app.get("removed_at"))
        if removed is None:
            continue
        age = (now - removed).days
        if 0 <= age <= days:
            return True
    if require_removal:
        return False
    return not apps or not saw_removal


def _churn_keeps(store: dict[str, Any], now: datetime,
                 settings: Optional[dict[str, Any]] = None) -> bool:
    return _removed_in_window(store, now, settings, require_removal=False)


def churn_fallback_keeps(store: dict[str, Any], now: datetime,
                          settings: Optional[dict[str, Any]] = None) -> bool:
    """Client-side churn check for a pull that did not use the uninstall filter."""
    return _removed_in_window(store, now, settings, require_removal=True)


# Rotated each calendar day so the same brands are not always first.
PLAN_UPGRADE_SORTS = ("-lplancat,rank", "-er,rank", "-prank,rank")


def _plan_upgrade(now: datetime, settings=None) -> dict[str, Any]:
    # Left tight on purpose. This is the highest-scoring recipe (93 of 104 leads
    # at 8 or above). It runs once a week so Wednesday does not re-scan Tuesday.
    return {
        "f:plan": "Shopify Plus",
        "f:last_plan_change_at:min": _iso(
            now - timedelta(days=_int(settings, "plan_upgrade.window_days"))),
        "sort": PLAN_UPGRADE_SORTS[now.toordinal() % 3],
    }


# Previous platforms a real brand migrates off of. Values are the ones
# f:last_platform documents (lowercase). The field is mutually exclusive, so
# multiple values require f:last_platform:op=or or the query matches nothing.
PREVIOUS_PLATFORMS = (
    "woocommerce",
    "bigcommerce",
    "magento",
    "wix",
    "squarespace",
    "prestashop",
    "shopware",
    "volusion",
    "opencart",
    "3dcart",
    "demandware",
    "bigcartel",
)
REPLATFORM_SORTS = ("-er,rank", "-prank,rank", "-mas,rank")


def _replatformed(now: datetime, settings=None) -> dict[str, Any]:
    return {
        "f:last_platform": ",".join(PREVIOUS_PLATFORMS),
        "f:last_platform:op": "or",
        "f:last_platform_change_at:min": _iso(
            now - timedelta(days=_int(settings, "replatformed.window_days"))),
        "sort": REPLATFORM_SORTS[now.toordinal() % len(REPLATFORM_SORTS)],
    }


# TikTok has a documented sort (-fc30dp_10) and no documented 30-day filter.
# f:tiktokfollowers30dpmin was ignored, so the recipe was only a sort of the
# whole ICP. YouTube, Pinterest and Twitter have real percent filters.
SOCIAL_GROWTH_SIGNALS = (
    ("tiktok", "", "-fc30dp_10,rank"),
    ("youtube", "f:youtubefollowers30dpmin", "-fc30dp_8,rank"),
    ("pinterest", "f:pinterestfollowers30dpmin", "-fc30dp_6,rank"),
    ("twitter", "f:twitterfollowers30dpmin", "-fc30dp_1,rank"),
)
_SOCIAL_CONTACT_TYPES = ("tiktok", "youtube", "pinterest", "twitter")


def _social_queries(now: datetime, settings=None) -> list[dict[str, Any]]:
    """TikTok first (the signal that scores well), then each documented filter.

    The three documented networks rotate so the second pull of the week is not
    the same head of the list. TikTok stays first so it still fills the cap.
    """
    pct = _int(settings, "social_surge.min_growth_pct")
    supported = list(SOCIAL_GROWTH_SIGNALS[1:])
    start = now.toordinal() % len(supported)
    ordered = (SOCIAL_GROWTH_SIGNALS[0], *(supported[start:] + supported[:start]))
    queries = []
    for _kind, filt, sort in ordered:
        extra: dict[str, Any] = {"sort": sort}
        if filt:
            extra[filt] = pct
        queries.append(extra)
    return queries


def _social_surge(now: datetime, settings=None) -> dict[str, Any]:
    return _social_queries(now, settings)[0]


def _follower_growth_pct(contact: dict[str, Any]) -> Optional[float]:
    try:
        followers = float(contact.get("followers"))
        delta = float(contact.get("followers_30d"))
    except (TypeError, ValueError):
        return None
    base = followers - delta
    if base <= 0:
        return 100.0 if delta > 0 else 0.0
    return delta / base * 100.0


def _social_grew(store: dict[str, Any], now: datetime,
                 settings: Optional[dict[str, Any]] = None) -> bool:
    """Keep a store with measured 30-day growth at or above the setting.

    Rows with no follower counts are kept: a documented server filter already
    applied the threshold, and the list payload sometimes omits the counts.
    """
    minimum = _int(settings, "social_surge.min_growth_pct")
    measured = False
    for item in store.get("contact_info") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").lower() not in _SOCIAL_CONTACT_TYPES:
            continue
        pct = _follower_growth_pct(item)
        if pct is None:
            continue
        measured = True
        if pct >= minimum:
            return True
    return not measured


# Documented sorts. Revenue stays in the rotation; the others change which
# stores sit in the first pages once the top of -er,rank has all been seen.
BASELINE_SORTS = ("-er,rank", "-mas,rank", "-prank,rank")


def baseline_sort_for(now: datetime) -> str:
    return BASELINE_SORTS[now.toordinal() % len(BASELINE_SORTS)]


def _icp_baseline(now: datetime, settings=None) -> dict[str, Any]:
    return {"sort": baseline_sort_for(now)}


# List Domains documents cursor pagination, not page numbers. The page parameter
# stops at about 11 pages (550 stores at page_size 50). Slices still matter:
# each revenue band is its own walk, and the sort rotates so a dry head of
# -er,rank is not the only window we ever see. Cursor walking happens in
# outbound_pipeline, so a slice can continue past that 550.
BASELINE_SLICES = 12


def revenue_slices(settings: Optional[dict[str, Any]] = None,
                   n: int = BASELINE_SLICES) -> list[tuple[int, int]]:
    """Log-spaced (min, max) monthly-cents slices covering the ICP revenue band."""
    lo = max(1, _int(settings, "icp.revenue_min_usd") * 100 // 12)
    hi = max(lo + 1, _int(settings, "icp.revenue_max_usd") * 100 // 12)
    ratio = (hi / lo) ** (1.0 / n)
    edges = [int(round(lo * ratio ** i)) for i in range(n)] + [hi]
    return [(edges[i], edges[i + 1]) for i in range(n) if edges[i + 1] > edges[i]]


def baseline_slices_for(now: datetime, settings: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    """Today's ordered list of StoreLeads param overrides, one per revenue slice."""
    slices = revenue_slices(settings)
    start = now.toordinal() % len(slices) if slices else 0
    ordered = slices[start:] + slices[:start]
    sort = baseline_sort_for(now)
    return [{"f:ermin": a, "f:ermax": b, "sort": sort} for a, b in ordered]


# Weekdays are Mon=0 .. Fri=4. Triggers start Tuesday, after the Monday refresh.
# plan_upgrade is the one query that does not change between days, so it runs
# once. The others run twice, on non-consecutive days, with a rotated query.
RECIPES: tuple[Recipe, ...] = (
    Recipe(
        key="new_growth_app",
        label="Just installed a growth or CRO app",
        reason="They added a growth or conversion tool in the last two weeks",
        tier="A", cadence="weekly", max_per_run=40, build=_new_growth_app,
        client_filter=_installed_recently, weekdays=(1, 3), expand=_growth_queries,
    ),
    Recipe(
        key="churned_tool",
        label="Just dropped a growth tool",
        reason="They removed a growth tool recently, so the problem is still open",
        tier="A", cadence="weekly", max_per_run=30, build=_churned_tool,
        client_filter=_churn_keeps, weekdays=(1, 4), expand=_churn_queries,
    ),
    Recipe(
        key="plan_upgrade",
        label="Upgraded to Shopify Plus",
        reason="They upgraded their store plan recently, which usually means new budget",
        tier="B", cadence="weekly", max_per_run=30, build=_plan_upgrade,
        weekdays=(2,),
    ),
    Recipe(
        key="replatformed",
        label="Recently replatformed",
        reason="They moved platforms recently, so vendors are being chosen right now",
        tier="B", cadence="weekly", max_per_run=25, build=_replatformed,
        weekdays=(2, 4),
    ),
    Recipe(
        key="social_surge",
        label="Social following spiking",
        reason="Their social following jumped in the last month",
        tier="B", cadence="weekly", max_per_run=25, build=_social_surge,
        client_filter=_social_grew, weekdays=(1, 3), expand=_social_queries,
    ),
    Recipe(
        key="icp_baseline",
        label="Core ICP (volume filler)",
        reason="They fit our ideal customer profile",
        tier="C", cadence="daily", max_per_run=25, build=_icp_baseline,
        weekdays=(0, 1, 2, 3, 4),
    ),
)

_BY_KEY = {r.key: r for r in RECIPES}


def recipe(key: str) -> Optional[Recipe]:
    return _BY_KEY.get(key)


_WEEKDAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri")


def weekday_label(item: Recipe) -> str:
    """Operator-facing schedule, e.g. 'Tue, Thu'."""
    return ", ".join(_WEEKDAY_NAMES[day] for day in item.weekdays if 0 <= day < 5)


def recipes_for_day(weekday: int, settings: Optional[dict[str, Any]] = None) -> list[Recipe]:
    """Which recipes run on a given weekday (Mon=0).

    StoreLeads refreshes weekly on Monday, so nothing but the baseline runs that
    day. Triggers are spread Tue-Fri. Each recipe's weekdays tuple is the same
    list the recipe table is seeded with. Recipes switched off in settings never
    run. Weekends: no sending, so no pulling.
    """
    if weekday >= 5:
        return []
    return [r for r in RECIPES if r.enabled(settings) and weekday in r.weekdays]


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
