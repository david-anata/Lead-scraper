"""The Clay replacement, end to end: brands in, ready-to-send leads out.

For each sourced brand that has not been enriched before:
    find up to N verified contacts  ->  write one opening line each  ->  remember
    them  ->  (optionally) push to a NEW Instantly campaign.

Guarantees:
  * A domain is enriched once. Credits are never spent on the same brand twice.
  * An email address is never queued twice.
  * A hard credit budget per run stops spend before it runs away.
  * Dry run by default: contacts are found and stored (that is the point of a
    test) but nothing is pushed to Instantly unless the caller opts in.
  * Fails open: a provider or database problem yields a report with a reason,
    never an exception into the daily job.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from sqlalchemy import text

import outbound_contacts as _contacts
import outbound_instantly_push as _push
import outbound_openers as _openers

logger = logging.getLogger(__name__)

_DOMAINS = "outbound_enriched"
_PEOPLE = "outbound_enriched_contacts"

_CREATE = (
    f"""CREATE TABLE IF NOT EXISTS {_DOMAINS} (
        domain TEXT PRIMARY KEY,
        status TEXT,
        contacts_found INTEGER,
        credits_used INTEGER,
        reason TEXT,
        enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    f"""CREATE TABLE IF NOT EXISTS {_PEOPLE} (
        email TEXT PRIMARY KEY,
        domain TEXT,
        first_name TEXT,
        last_name TEXT,
        title TEXT,
        linkedin_url TEXT,
        opener TEXT,
        opener_source TEXT,
        brand_angle TEXT,
        brand TEXT,
        category TEXT,
        recipe TEXT,
        tier TEXT,
        campaign_id TEXT,
        pushed_at TIMESTAMP,
        email_status TEXT,
        skip_reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
)

_PEOPLE_ALTERS = (
    f"ALTER TABLE {_PEOPLE} ADD COLUMN email_status TEXT",
    f"ALTER TABLE {_PEOPLE} ADD COLUMN skip_reason TEXT",
    f"ALTER TABLE {_PEOPLE} ADD COLUMN brand_angle TEXT",
)


def ensure_tables(engine) -> None:
    """Create the two tables. Runs in the Render pre-deploy migration (schema
    owner). Vercel's runtime role cannot run DDL, so it skips this there."""
    import os
    if os.getenv("VERCEL") and engine.dialect.name == "postgresql":
        return
    from sales_support_agent.services.outbound_ddl import create_table_if_absent

    for table, sql in zip((_DOMAINS, _PEOPLE), _CREATE, strict=True):
        create_table_if_absent(engine, sql, table)
    for stmt in _PEOPLE_ALTERS:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 — column already present
            pass


def enriched_domains(engine) -> set[str]:
    """Domains we will not search again.

    A failed INVALID_FILTERS row is not done: the subdomain bug stored those
    failures, and a later run has to be allowed to search the brand again.
    A domain we already found contacts for, or already saw inside Instantly,
    stays done even if the reason text mentions the filter error.
    """
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT domain, COALESCE(status, ''), COALESCE(reason, '') FROM {_DOMAINS}")).fetchall()
    done = set()
    for domain, status, reason in rows:
        if "INVALID_FILTERS" in str(reason).upper() and status not in {"found", "in_instantly"}:
            continue
        done.add(str(domain).lower())
    return done


def known_emails(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT email FROM {_PEOPLE}")).fetchall()
    return {str(r[0]).lower() for r in rows}


def _record_domain(engine, domain: str, status: str, found: int, credits: int, reason: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {_DOMAINS} (domain, status, contacts_found, credits_used, reason) "
            "VALUES (:d, :s, :f, :c, :r) ON CONFLICT (domain) DO UPDATE SET "
            "status = :s, contacts_found = :f, credits_used = :c, reason = :r"),
            {"d": domain, "s": status, "f": found, "c": credits, "r": reason[:500]})


def _record_person(engine, c: _contacts.Contact, opener: str, source: str,
                   lead: dict[str, Any], brand_angle: str = "") -> None:
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(c) for c in cats)
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {_PEOPLE} (email, domain, first_name, last_name, title, linkedin_url, "
            "opener, opener_source, brand_angle, brand, category, recipe, tier, email_status) "
            "VALUES (:e, :d, :f, :l, :t, :u, :o, :s, :angle, :b, :cat, :r, :tier, :status) "
            "ON CONFLICT (email) DO NOTHING"),
            {"e": c.email, "d": c.domain, "f": c.first_name, "l": c.last_name, "t": c.title,
             "u": c.linkedin_url, "o": opener, "s": source, "angle": brand_angle or "",
             "b": str(lead.get("brand") or c.domain), "cat": str(cats or lead.get("niche") or ""),
             "r": str(lead.get("recipe") or ""), "tier": str(lead.get("tier") or ""),
             "status": c.email_status or ""})


def unpushed(engine, limit: int = 500, *,
             exclude_recipes: Iterable[str] = ()) -> list[dict[str, Any]]:
    """Contacts waiting for a push, oldest first.

    ``exclude_recipes`` keeps those recipe values out of this page so a large
    held batch cannot crowd the limit. They stay unpushed.
    """
    excluded = [str(recipe).strip() for recipe in exclude_recipes if str(recipe).strip()]
    params: dict[str, Any] = {"n": limit}
    sql = (
        "SELECT email, domain, first_name, last_name, title, opener, brand, category, "
        f"recipe, tier, brand_angle FROM {_PEOPLE} WHERE pushed_at IS NULL "
        "AND COALESCE(skip_reason, '') = '' "
    )
    if excluded:
        holders = []
        for index, recipe in enumerate(excluded):
            key = f"x{index}"
            holders.append(f":{key}")
            params[key] = recipe
        sql += f"AND COALESCE(recipe, '') NOT IN ({', '.join(holders)}) "
    sql += "ORDER BY created_at LIMIT :n"
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [dict(zip(("email", "domain", "first_name", "last_name", "title", "opener", "brand",
                      "category", "recipe", "tier", "brand_angle"), r)) for r in rows]


def _count_unpushed_recipes(engine, recipes: Iterable[str]) -> int:
    names = [str(recipe).strip() for recipe in recipes if str(recipe).strip()]
    if not names:
        return 0
    params: dict[str, Any] = {}
    holders = []
    for index, recipe in enumerate(names):
        key = f"r{index}"
        holders.append(f":{key}")
        params[key] = recipe
    sql = (
        f"SELECT COUNT(*) FROM {_PEOPLE} WHERE pushed_at IS NULL "
        "AND COALESCE(skip_reason, '') = '' "
        f"AND COALESCE(recipe, '') IN ({', '.join(holders)})"
    )
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar() or 0)


def _mark_pushed(engine, emails: Iterable[str], campaign_id: str) -> None:
    with engine.begin() as conn:
        for e in emails:
            conn.execute(text(
                f"UPDATE {_PEOPLE} SET pushed_at = CURRENT_TIMESTAMP, campaign_id = :c WHERE email = :e"),
                {"c": campaign_id, "e": e})


@dataclass
class EnrichReport:
    dry_run: bool = True
    companies_tried: int = 0
    companies_with_contacts: int = 0
    contacts_found: int = 0
    credits_used: int = 0
    skipped_already_enriched: int = 0
    skipped_in_instantly: int = 0
    skipped_check_failed: int = 0
    skipped_not_us: int = 0
    stopped_reason: str = ""
    pushed: int = 0
    suppressed: int = 0
    queued: int = 0
    push_note: str = ""
    samples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def find_rate(self) -> float:
        return round(self.companies_with_contacts / self.companies_tried, 3) if self.companies_tried else 0.0

    @property
    def credits_per_contact(self) -> float:
        return round(self.credits_used / self.contacts_found, 2) if self.contacts_found else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run, "companies_tried": self.companies_tried,
            "companies_with_contacts": self.companies_with_contacts,
            "find_rate": self.find_rate, "contacts_found": self.contacts_found,
            "credits_used": self.credits_used, "credits_per_contact": self.credits_per_contact,
            "skipped_already_enriched": self.skipped_already_enriched,
            "skipped_in_instantly": self.skipped_in_instantly,
            "skipped_check_failed": self.skipped_check_failed,
            "skipped_not_us": self.skipped_not_us,
            "stopped_reason": self.stopped_reason, "pushed": self.pushed,
            "suppressed": self.suppressed, "queued": self.queued,
            "push_note": self.push_note, "samples": self.samples[:10],
        }


def companies_enriched_by_recipe(engine, since_utc, recipes: Iterable[str]) -> dict[str, int]:
    """Companies enriched since ``since_utc`` whose stored recipe is in ``recipes``.

    A failed ``INVALID_FILTERS`` row does not count. The same domain can be
    searched again, and it should not use up a campaign's daily cap.
    """

    wanted = {str(recipe).strip() for recipe in recipes if str(recipe).strip()}
    if engine is None or not wanted:
        return {}
    try:
        ensure_tables(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(
                """
                SELECT d.recipe, COALESCE(e.status, ''), COALESCE(e.reason, '')
                FROM outbound_enriched e
                JOIN outbound_contacted_domains d ON d.domain = e.domain
                WHERE e.enriched_at >= :t
                """
            ), {"t": since_utc}).fetchall()
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not count enriched recipes")
        return {}
    counts: dict[str, int] = {}
    for recipe, status, reason in rows:
        name = str(recipe or "").strip()
        if name not in wanted:
            continue
        if "INVALID_FILTERS" in str(reason).upper() and str(status) not in {"found", "in_instantly"}:
            continue
        counts[name] = counts.get(name, 0) + 1
    return counts


def _recipe_names(row: dict[str, Any]) -> list[str]:
    names = [str(row.get("recipe_key") or "").strip()]
    names.extend(
        part.strip() for part in str(row.get("recipe_aliases") or "").split(",") if part.strip()
    )
    return [name for name in names if name]


def _interleave(buckets: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """One lead from each bucket, in turn, until every bucket is empty."""

    rows = [list(bucket) for bucket in buckets if bucket]
    cursors = [0] * len(rows)
    ordered: list[dict[str, Any]] = []
    while True:
        advanced = False
        for index, bucket in enumerate(rows):
            cursor = cursors[index]
            if cursor >= len(bucket):
                continue
            ordered.append(bucket[cursor])
            cursors[index] = cursor + 1
            advanced = True
        if not advanced:
            return ordered


def order_leads_for_enrich(engine, leads: list[dict[str, Any]], *, since_utc) -> list[dict[str, Any]]:
    """Active campaign recipes first, then the rest of ``leads``.

    Each active ``outbound_campaigns`` row with a positive ``daily_cap``
    contributes its oldest not-yet-enriched brands, up to the cap minus
    companies of that recipe (and its aliases) already enriched since
    ``since_utc``. Campaigns are interleaved so one recipe cannot spend
    the whole run. Baseline, social, and every other recipe keep the
    order they arrived in. ``leads`` from ``load_leads`` is newest first.

    Older signal brands are loaded from the lead table. They are not
    limited to the newest page, which is where ``icp_baseline`` and
    ``social_surge`` were crowding them out.

    This does not change which Instantly campaign a recipe is pushed to,
    and it does not update or move a contact already in Instantly.
    """

    general = list(leads or [])
    try:
        return _order_leads_for_enrich(engine, general, since_utc=since_utc)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not put campaign recipes first")
        return general


def _order_leads_for_enrich(engine, general: list[dict[str, Any]], *, since_utc) -> list[dict[str, Any]]:
    import outbound_campaign_source as source
    import outbound_campaigns as campaigns

    rows = [
        row for row in campaigns.list_campaigns(engine)
        if row.get("status") == "active" and int(row.get("daily_cap") or 0) > 0
    ]
    if not rows:
        return general
    ordered_rows = source._ordered(rows)
    owner: dict[str, str] = {}
    names_by_campaign: dict[str, list[str]] = {}
    for row in ordered_rows:
        names = _recipe_names(row)
        names_by_campaign[str(row["recipe_key"])] = names
        for name in names:
            owner.setdefault(name, str(row["recipe_key"]))
    if not owner:
        return general

    from sales_support_agent.services import outbound_memory

    counts = companies_enriched_by_recipe(engine, since_utc, owner.keys())
    try:
        ensure_tables(engine)
        done = enriched_domains(engine)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not read enriched domains for ordering")
        done = set()
    stored = outbound_memory.load_leads_by_recipe(engine, owner.keys(), oldest_first=True)
    merged: dict[str, dict[str, Any]] = {}
    for lead in stored:
        domain = _contacts.normalize_domain(str(lead.get("domain") or ""))
        if domain:
            merged.setdefault(domain, lead)
    for lead in general:
        domain = _contacts.normalize_domain(str(lead.get("domain") or ""))
        recipe = str(lead.get("recipe") or "").strip()
        if domain and recipe in owner:
            merged.setdefault(domain, lead)

    buckets: dict[str, list[dict[str, Any]]] = {str(row["recipe_key"]): [] for row in ordered_rows}
    candidates = sorted(
        merged.values(),
        key=lambda lead: (str(lead.get("first_seen_at") or "9999"), str(lead.get("domain") or "")),
    )
    for lead in candidates:
        domain = _contacts.normalize_domain(str(lead.get("domain") or ""))
        if not domain or domain in done:
            continue
        campaign_key = owner.get(str(lead.get("recipe") or "").strip())
        if campaign_key and campaign_key in buckets:
            buckets[campaign_key].append(lead)
    for row in ordered_rows:
        key = str(row["recipe_key"])
        used = sum(counts.get(name, 0) for name in names_by_campaign.get(key, []))
        remaining = max(0, int(row.get("daily_cap") or 0) - used)
        buckets[key] = buckets[key][:remaining]

    import outbound_campaign_source as _priority_source

    first = [str(row["recipe_key"]) for row in ordered_rows if str(row["recipe_key"]) in _priority_source._ENRICH_FIRST]
    first.sort(key=lambda key: _priority_source._ENRICH_FIRST.index(key))
    later = [str(row["recipe_key"]) for row in ordered_rows if str(row["recipe_key"]) not in _priority_source._ENRICH_FIRST]
    priority = _interleave([buckets[key] for key in first]) + _interleave([buckets[key] for key in later])
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for lead in priority:
        domain = _contacts.normalize_domain(str(lead.get("domain") or ""))
        if not domain or domain in seen:
            continue
        seen.add(domain)
        ordered.append(lead)
    campaign_count = len(ordered)
    for lead in general:
        domain = _contacts.normalize_domain(str(lead.get("domain") or ""))
        if domain and domain in seen:
            continue
        if domain:
            seen.add(domain)
        ordered.append(lead)
    logger.info(
        "[outbound-enrich] campaign recipes first: %s, then %s other brands",
        campaign_count, len(ordered) - campaign_count,
    )
    return ordered


def run_enrichment(
    engine,
    leads: list[dict[str, Any]],
    *,
    api_key: str,
    max_companies: int = 10,
    max_contacts: int = 2,
    credit_budget: int = 150,
    campaign_id: str = "",
    dry_run: bool = True,
    find: Optional[Callable[..., _contacts.FindResult]] = None,
    write: Optional[Callable[..., tuple[str, str]]] = None,
    write_angle: Optional[Callable[..., tuple[str, str]]] = None,
    push: Optional[Callable[..., _push.PushOutcome]] = None,
    deadline: Optional[float] = None,
    clock: Optional[Callable[[], float]] = None,
    already_in_instantly: Optional[Callable[[str], Optional[bool]]] = None,
) -> EnrichReport:
    import time as _time
    clock = clock or _time.monotonic
    find = find or _contacts.find_contacts
    write = write or _openers.write_opener
    write_angle = write_angle or _openers.write_brand_angle
    push = push or _push.push_leads
    _openers._OPENERS_BY_DOMAIN.clear()
    _openers._ANGLES_BY_DOMAIN.clear()
    report = EnrichReport(dry_run=dry_run)

    if engine is None:
        report.stopped_reason = "No database."
        return report
    if not api_key:
        report.stopped_reason = "PROSPEO_API_KEY is not set."
        return report
    try:
        ensure_tables(engine)
        done = enriched_domains(engine)
        emails_seen = known_emails(engine)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not read state")
        report.stopped_reason = "Could not read enrichment state."
        return report

    leads_by_domain: dict[str, dict[str, Any]] = {}
    for lead in leads:
        d = _contacts.normalize_domain(str(lead.get("domain") or ""))
        if not d:
            continue
        if d in done:
            report.skipped_already_enriched += 1
            continue
        leads_by_domain.setdefault(d, lead)

    for domain, lead in list(leads_by_domain.items()):
        if report.companies_tried >= max_companies:
            break
        if report.credits_used >= credit_budget:
            report.stopped_reason = f"Credit budget of {credit_budget} reached."
            break
        if deadline is not None and clock() >= deadline:
            report.stopped_reason = "Time budget for this run reached; the next run continues."
            break
        if already_in_instantly is not None:
            seen = already_in_instantly(domain)
            if seen is None:
                report.skipped_check_failed += 1
                if report.skipped_check_failed >= 5 and report.companies_tried == 0:
                    report.stopped_reason = "Could not check Instantly for existing leads."
                    break
                continue
            if seen:
                report.skipped_in_instantly += 1
                try:
                    _record_domain(engine, domain, "in_instantly", 0, 0,
                                   "Already a lead in Instantly; not contacted again.")
                except Exception:  # noqa: BLE001
                    logger.exception("[outbound-enrich] could not mark %s", domain)
                continue
        country = str(lead.get("country") or lead.get("country_code") or "").strip().upper()
        if country == "UK":
            country = "GB"
        if country and country != "US":
            report.skipped_not_us += 1
            try:
                _record_domain(engine, domain, "not_us", 0, 0, "StoreLeads country is not US.")
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-enrich] could not mark %s", domain)
            continue
        report.companies_tried += 1
        find_kwargs = {"api_key": api_key, "max_contacts": max_contacts}
        if str(lead.get("recipe") or "").strip() == "sales_org":
            find_kwargs["rank_title"] = _contacts.sales_title_rank
        res = find(domain, **find_kwargs)
        report.credits_used += res.credits_used
        fresh = [c for c in res.contacts if c.email not in emails_seen]
        status = "found" if fresh else ("duplicate" if res.contacts else "none")
        try:
            _record_domain(engine, domain, status, len(fresh), res.credits_used, res.reason)
            for c in fresh:
                profile: dict[str, Any] = {}
                try:
                    from sales_support_agent.services.outbound_memory import load_store_profiles

                    profile = load_store_profiles(engine, [domain]).get(domain) or {}
                except Exception:  # noqa: BLE001
                    profile = {}
                if not profile and isinstance(lead.get("store_profile"), dict):
                    profile = lead["store_profile"]
                opener_lead = {**lead, "store_profile": profile}
                if profile and _openers.rich_personalization_enabled():
                    opener_lead["rich_personalization"] = True
                line, src = write(opener_lead, {"title": c.title})
                angle, angle_src = write_angle(opener_lead, {"title": c.title})
                shop_line, shop_angle = _openers.shopper_lines(opener_lead)
                if shop_line and _openers.opener_hold_reason(line, opener_lead):
                    line, src = shop_line, "shopper"
                    angle, angle_src = shop_angle, "shopper" if shop_angle else "none"
                if _openers.angle_is_filler(angle):
                    angle, angle_src = "", "none"
                hold = _openers.opener_hold_reason(line, opener_lead)
                if hold:
                    logger.info(
                        "[outbound-enrich] holding %s at %s: %s",
                        c.email, domain, hold,
                    )
                _record_person(engine, c, line, src, lead, brand_angle=angle)
                if hold:
                    _set_opener_hold(engine, c.email, hold)
                emails_seen.add(c.email)
                report.contacts_found += 1
                if len(report.samples) < 10:
                    report.samples.append({"domain": domain, "email": c.email, "title": c.title,
                                           "opener": line, "opener_source": src,
                                           "brand_angle": angle, "brand_angle_source": angle_src})
            if fresh:
                report.companies_with_contacts += 1
                try:
                    from sales_support_agent.services import outbound_memory
                    outbound_memory.record_contacts(engine, [
                        {"email": c.email, "linkedin_url": c.linkedin_url, "first_name": c.first_name,
                         "last_name": c.last_name, "company": str(lead.get("brand") or domain)}
                        for c in fresh])
                except Exception:  # noqa: BLE001
                    logger.exception("[outbound-enrich] could not mirror contacts")
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-enrich] could not save %s", domain)
            report.stopped_reason = "Database write failed."
            break
        if res.reason.startswith(("Out of contact-finder credits", "Rate limit")):
            report.stopped_reason = res.reason
            break

    if not dry_run:
        # campaign_id is only the default for recipes that are not in the map.
        # An empty id still pushes mapped recipes.
        out = push_pending(engine, campaign_id, dry_run=False, push=push)
        report.push_note = out["note"]
        report.pushed = out["pushed"]
        report.suppressed = int(out.get("suppressed") or 0)
        report.queued = int(out.get("queued") or 0)
    else:
        report.push_note = "Dry run: nothing sent to Instantly."
    return report


def rewrite_openers(engine, leads: list[dict[str, Any]], *,
                    write: Optional[Callable[..., tuple[str, str]]] = None,
                    limit: int = 200) -> dict[str, Any]:
    """Rewrite openers for contacts not yet pushed (after a prompt change).
    Costs only AI calls, never contact-finder credits."""
    write = write or _openers.write_opener
    by_domain = {_contacts.normalize_domain(str(l.get("domain") or "")): l for l in leads}
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT email, domain, title, brand, category, recipe, tier FROM {_PEOPLE} "
            "WHERE pushed_at IS NULL ORDER BY created_at LIMIT :n"), {"n": limit}).fetchall()
    done, ai, samples = 0, 0, []
    for email, domain, title, brand, category, recipe, tier in rows:
        lead = by_domain.get(domain) or {"domain": domain, "brand": brand,
                                         "categories": [category], "recipe": recipe, "tier": tier}
        domain_key = str(domain or "").strip().lower()
        _openers._OPENERS_BY_DOMAIN.pop(domain_key, None)
        _openers._ANGLES_BY_DOMAIN.pop(domain_key, None)
        line, src = write(lead, {"title": title})
        angle, _angle_src = _openers.write_brand_angle(lead, {"title": title})
        with engine.begin() as conn:
            conn.execute(text(
                f"UPDATE {_PEOPLE} SET opener = :o, opener_source = :s, brand_angle = :a "
                "WHERE email = :e"),
                {"o": line, "s": src, "a": angle, "e": email})
        done += 1
        ai += src == "ai"
        if len(samples) < 12:
            samples.append({"brand": brand, "title": title, "opener": line, "source": src})
    return {"rewritten": done, "ai": ai, "fallback": done - ai, "samples": samples}


def _count_opener_holds(engine) -> int:
    """Contacts still waiting because the opener is not shopper-visible."""

    if engine is None:
        return 0
    try:
        with engine.connect() as conn:
            found = conn.execute(text(
                f"SELECT COUNT(*) FROM {_PEOPLE} WHERE pushed_at IS NULL "
                "AND COALESCE(skip_reason, '') LIKE :prefix"
            ), {"prefix": _openers.OPENER_HOLD_PREFIX + "%"}).scalar()
    except Exception:  # noqa: BLE001
        return 0
    return int(found or 0)


def _set_opener_hold(engine, email: str, reason: str) -> None:
    """Leave the contact unpushed, with a reason the next run can read."""

    stored = f"{_openers.OPENER_HOLD_PREFIX} {reason}"[:200]
    with engine.begin() as conn:
        conn.execute(text(
            f"UPDATE {_PEOPLE} SET skip_reason = :r WHERE email = :e AND pushed_at IS NULL"
        ), {"r": stored, "e": email})


def release_held_openers(engine) -> int:
    """Send a held contact forward once a shopper-visible fact exists.

    A missing profile table leaves the hold in place. Nothing is pushed here.
    """

    if engine is None:
        return 0
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT email, domain, brand, category, recipe, tier FROM {_PEOPLE} "
                "WHERE pushed_at IS NULL AND COALESCE(skip_reason, '') LIKE :prefix"
            ), {"prefix": _openers.OPENER_HOLD_PREFIX + "%"}).fetchall()
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not read held openers")
        return 0
    if not rows:
        return 0
    profiles: dict[str, dict[str, Any]] = {}
    try:
        from sales_support_agent.services.outbound_memory import load_store_profiles

        profiles = load_store_profiles(engine, [str(row[1] or "") for row in rows])
    except Exception:  # noqa: BLE001
        logger.info("[outbound-enrich] store profiles could not be read; opener holds stay")
        profiles = {}
    released = 0
    for email, domain, brand, category, recipe, tier in rows:
        domain_key = str(domain or "").strip().lower()
        lead = {
            "domain": domain_key, "brand": brand, "categories": category,
            "recipe": recipe, "tier": tier,
            "store_profile": profiles.get(domain_key) or {},
        }
        opener, angle = _openers.shopper_lines(lead)
        if _openers.opener_hold_reason(opener, lead):
            continue
        if _openers.angle_is_filler(angle):
            angle = ""
        with engine.begin() as conn:
            conn.execute(text(
                f"UPDATE {_PEOPLE} SET opener = :o, opener_source = 'shopper', "
                "brand_angle = :a, skip_reason = '' "
                "WHERE email = :e AND pushed_at IS NULL"
            ), {"o": opener, "a": angle, "e": email})
        logger.info(
            "[outbound-enrich] %s at %s retrying: shopper-visible opener is ready",
            email, domain_key,
        )
        released += 1
    return released


def _mark_skipped(engine, people: list[dict[str, Any]]) -> None:
    """Keep a suppressed contact out of the unpushed queue without deleting it."""
    with engine.begin() as conn:
        for person in people:
            conn.execute(text(
                f"UPDATE {_PEOPLE} SET skip_reason = :r WHERE email = :e AND pushed_at IS NULL"),
                {"r": f"suppressed:{person.get('suppressed_match') or 'match'}"[:200],
                 "e": person.get("email")})


def _empty_push(*, dry_run: bool, suppressed: int = 0, excluded: int = 0,
                held: int = 0, queued: int = 0, note: str = "Nothing to push.",
                errors: Optional[list[str]] = None) -> dict[str, Any]:
    return {
        "pending": 0, "pushed": 0, "created": 0, "skipped": 0,
        "suppressed": suppressed, "excluded_recipes": excluded, "held": held,
        "queued": queued, "dry_run": dry_run, "errors": errors or [], "note": note,
        "preview": [], "campaigns": [],
    }


def _signals_for_domains(engine, domains: Iterable[str]) -> dict[str, list[str]]:
    """Stored lead signals keyed by the contact domain and its registrable root.

    A missing table means there is no evidence. That queues a filler. It does
    not send the contact to the default campaign.
    """

    wanted = {str(domain or "").strip().lower() for domain in domains if str(domain or "").strip()}
    if not wanted or engine is None:
        return {}
    roots = {_contacts.registrable_domain(domain) for domain in wanted}
    roots.discard("")
    keys = wanted | roots
    holders = []
    params: dict[str, Any] = {}
    for index, domain in enumerate(sorted(keys)):
        key = f"d{index}"
        holders.append(f":{key}")
        params[key] = domain
    sql = (
        "SELECT domain, signals FROM outbound_contacted_domains "
        f"WHERE domain IN ({', '.join(holders)})"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
    except Exception:  # noqa: BLE001
        logger.debug("[outbound-enrich] lead signals could not be read")
        return {}
    found: dict[str, list[str]] = {domain: [] for domain in wanted}
    for domain, raw in rows:
        key = str(domain or "").strip().lower()
        try:
            parsed = json.loads(raw or "[]")
        except (TypeError, ValueError):
            parsed = []
        if not isinstance(parsed, list):
            continue
        tokens = [str(item).strip() for item in parsed if str(item or "").strip()]
        root = _contacts.registrable_domain(key)
        for candidate in wanted:
            if candidate == key or candidate == root or _contacts.registrable_domain(candidate) == root:
                found.setdefault(candidate, []).extend(tokens)
    return found


def _campaign_routing(engine):
    """Table routing, or a fallback marker when the table cannot be used."""

    class _Fallback:
        available = False
        by_recipe: dict[str, str] = {}
        blocked: frozenset[str] = frozenset()

    try:
        import outbound_campaigns as campaigns
        return campaigns.load_routing(engine)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] campaign table could not be read; using the hardcoded map")
        return _Fallback()


def departed_emails(engine) -> set[str]:
    """Emails Abby already classified as having left the company.

    A missing table means Abby has not recorded anyone here. That is not a
    reason to stop the push. A read error is the same: callers still apply
    the suppression list, and this set is empty.
    """

    sql = (
        "SELECT lower(lead) FROM abby_handled "
        "WHERE lower(COALESCE(action, '')) = 'ignore' AND ("
        "lower(COALESCE(detail, '')) LIKE '%departed contact%' "
        "OR lower(COALESCE(detail, '')) LIKE '%departed employee%'"
        ")"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
    except Exception:  # noqa: BLE001 - table missing, or Abby is not on this database
        logger.debug("[outbound-enrich] departed contacts could not be read")
        return set()
    found: set[str] = set()
    for row in rows:
        email = str(row[0] or "").strip().lower()
        if "@" in email:
            found.add(email)
    return found


def push_pending(engine, campaign_id: str, *, dry_run: bool = True, limit: int = 500,
                 push: Optional[Callable[..., _push.PushOutcome]] = None) -> dict[str, Any]:
    """Add not-yet-pushed contacts to Instantly, one campaign per recipe group.

    A recipe on ``outbound_campaigns`` with status active or throttled goes
    to that campaign when it is one of the v2 recipes. ``cut`` and
    ``proposed`` v2 recipes stay unpushed. Filler recipes stay queued unless
    stored signals prove a v2 recipe. ``campaign_id`` and
    ``OUTBOUND_INSTANTLY_CAMPAIGN_ID`` are not used, including when that
    variable still names Trial Offer Oct. ``redo_partner`` is left unpushed.
    Suppressed emails and domains, departed contacts, and role inboxes are left
    unpushed and, on a live push, marked so they do not clog the queue. A
    contact is marked pushed only when
    Instantly accepted that campaign's batch with no errors. If the suppression
    list cannot be read, nothing is pushed.
    """
    push = push or _push.push_leads
    try:
        release_held_openers(engine)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] could not retry held openers")
    try:
        excluded = _count_unpushed_recipes(engine, _push.PUSH_EXCLUDED_RECIPES)
        pending = unpushed(engine, limit, exclude_recipes=_push.PUSH_EXCLUDED_RECIPES)
        import outbound_suppression as suppression
        allowed, blocked = suppression.partition(engine, pending)
        left = departed_emails(engine)
        still: list[dict[str, Any]] = []
        domains = [str(person.get("domain") or "") for person in allowed]
        try:
            from sales_support_agent.services.outbound_memory import countries_for

            countries = countries_for(engine, domains)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-enrich] country lookup failed")
            countries = {}
        for person in allowed:
            email = str(person.get("email") or "").strip().lower()
            domain = str(person.get("domain") or "").strip().lower()
            quality = _contacts.lead_quality_reason(
                email=email,
                first_name=str(person.get("first_name") or ""),
                title=str(person.get("title") or ""),
                require_title=True,
                recipe=str(person.get("recipe") or ""),
            )
            country = countries.get(domain, "")
            if quality == "role mailbox":
                blocked.append({**person, "suppressed_match": f"role:{email}"})
            elif quality:
                blocked.append({**person, "suppressed_match": quality})
            elif country and country != "US":
                blocked.append({**person, "suppressed_match": f"country:{country}"})
            elif email and email in left:
                blocked.append({**person, "suppressed_match": f"departed:{email}"})
            else:
                still.append(person)
        allowed = still
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-enrich] suppression check failed")
        return _empty_push(
            dry_run=dry_run, errors=["suppression list could not be read"],
            note="Suppression list could not be read; nothing pushed.",
        )
    if blocked and not dry_run:
        _mark_skipped(engine, blocked)
    # The caller's campaign id is intentionally unused. A stale Trial Offer
    # default must not become the destination for whatever is left over.
    del campaign_id
    routing = _campaign_routing(engine)
    campaign_map = routing.by_recipe if routing.available else None
    signals_by_domain = _signals_for_domains(engine, (person.get("domain") for person in allowed))
    groups: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    held = 0
    held_copy = 0
    queued = 0
    for person in allowed:
        copy_lead = {
            "brand": person.get("brand"),
            "categories": person.get("category"),
            "recipe": person.get("recipe"),
        }
        copy_reason = _openers.opener_hold_reason(str(person.get("opener") or ""), copy_lead)
        if copy_reason:
            held_copy += 1
            logger.info(
                "[outbound-enrich] holding %s: %s",
                person.get("email"), copy_reason,
            )
            if not dry_run:
                _set_opener_hold(engine, str(person.get("email") or ""), copy_reason)
            continue
        if _openers.angle_is_filler(str(person.get("brand_angle") or "")):
            person = {**person, "brand_angle": ""}
            logger.info(
                "[outbound-enrich] cleared filler brand angle for %s",
                person.get("email"),
            )
        recipe_name = str(person.get("recipe") or "").strip()
        domain_key = str(person.get("domain") or "").strip().lower()
        signals = list(signals_by_domain.get(domain_key) or [])
        extra = person.get("signals") or []
        if isinstance(extra, str):
            extra = [extra]
        signals.extend(str(item) for item in extra if str(item or "").strip())
        target, status, route_name = _push.resolve_recipe_campaign(
            recipe_name, signals, campaign_map=campaign_map, blocked=routing.blocked,
        )
        if status == "blocked":
            held += 1
            continue
        if status != "routed" or not target or _push._closed_to_new_leads(target):
            queued += 1
            continue
        lead = {"domain": person["domain"], "brand": person["brand"], "categories": person["category"],
                "recipe": route_name or recipe_name, "tier": person["tier"]}
        body = _push.build_lead(person, lead, person["opener"])
        if route_name and route_name != recipe_name:
            body["custom_variables"]["source_recipe"] = recipe_name
        groups.setdefault(target, []).append((person, body))
    if not groups:
        bits = []
        if excluded:
            bits.append(f"Excluded {excluded} redo_partner contacts from the regular push.")
        if blocked:
            bits.append(f"Skipped {len(blocked)} suppressed contacts.")
        if queued:
            bits.append(f"Queued {queued} contacts. {_push.QUEUED_NOTE}")
        if held:
            bits.append(f"Held {held} contacts whose campaign is cut or proposed.")
        waiting = max(held_copy, _count_opener_holds(engine))
        if waiting:
            bits.append(f"Held {waiting} contacts with no shopper-visible opener.")
        return _empty_push(
            dry_run=dry_run, suppressed=len(blocked), excluded=excluded,
            held=held + waiting,
            queued=queued, note=" ".join(bits) or "Nothing to push.",
        )

    created = skipped = pushed = 0
    errors: list[str] = []
    notes: list[str] = []
    preview: list[dict[str, Any]] = []
    campaigns: list[dict[str, Any]] = []
    stayed_dry = True
    for target, items in groups.items():
        payload = [body for _person, body in items]
        emails = [str(person.get("email") or "") for person, _body in items]
        outcome = push(payload, target, dry_run=dry_run)
        stayed_dry = stayed_dry and outcome.dry_run
        group_pushed = 0
        if not outcome.dry_run and not outcome.errors and payload:
            _mark_pushed(engine, emails, target)
            group_pushed = len(emails)
            pushed += group_pushed
        created += outcome.created
        skipped += outcome.skipped
        errors.extend(outcome.errors)
        if outcome.reason or outcome.summary:
            notes.append(outcome.reason or outcome.summary)
        for body in payload:
            if len(preview) >= 3:
                break
            shown = {key: value for key, value in body.items() if key != "email"}
            shown["campaign_id"] = target
            preview.append(shown)
        campaigns.append({
            "campaign_id": target,
            "pending": len(payload),
            "pushed": group_pushed,
            "dry_run": outcome.dry_run,
            "recipes": sorted({str(person.get("recipe") or "") for person, _body in items}),
        })
    note = " ".join(notes).strip()
    if excluded:
        note = f"{note} Excluded {excluded} redo_partner contacts from the regular push.".strip()
    if blocked:
        note = f"{note} Skipped {len(blocked)} suppressed contacts.".strip()
    if queued:
        note = f"{note} Queued {queued} contacts. {_push.QUEUED_NOTE}".strip()
    if held:
        note = f"{note} Held {held} contacts whose campaign is cut or proposed.".strip()
    waiting = max(held_copy, _count_opener_holds(engine))
    if waiting:
        note = f"{note} Held {waiting} contacts with no shopper-visible opener.".strip()
    return {
        "pending": sum(len(items) for items in groups.values()),
        "pushed": pushed, "created": created, "skipped": skipped,
        "suppressed": len(blocked), "excluded_recipes": excluded,
        "held": held + waiting,
        "queued": queued, "dry_run": stayed_dry, "errors": errors, "note": note,
        "preview": preview, "campaigns": campaigns,
    }


def enriched_since(engine, since_utc) -> dict[str, int]:
    """Companies tried and credits spent since a UTC timestamp (daily caps)."""
    ensure_tables(engine)
    with engine.connect() as conn:
        row = conn.execute(text(
            f"SELECT COUNT(*), COALESCE(SUM(credits_used), 0) FROM {_DOMAINS} WHERE enriched_at >= :t"),
            {"t": since_utc}).fetchone()
    return {"companies": int(row[0] or 0), "credits": int(row[1] or 0)}


MAX_ENRICH_LIST_DOMAINS = 200


def coerce_dry_run(value: Any) -> bool:
    """Missing or unrecognized values stay on the safe side: dry run."""
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def domain_list_from(raw: Any) -> list[str]:
    """Normalize a domain list from a JSON array or a comma/newline string.

    Order is preserved. Duplicates and blanks are dropped. Hosts are reduced
    to the registrable domain the rest of enrichment already uses.
    """
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.replace(",", "\n").splitlines()]
    elif isinstance(raw, (list, tuple)):
        parts = []
        for item in raw:
            parts.extend(part.strip() for part in str(item).replace(",", "\n").splitlines())
    else:
        parts = []
    seen: set[str] = set()
    domains: list[str] = []
    for part in parts:
        domain = _contacts.normalize_domain(part)
        if not domain or domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return domains


def enrich_domain_list(
    engine,
    domains: Any,
    *,
    api_key: str,
    dry_run: bool = True,
    find: Optional[Callable[..., _contacts.FindResult]] = None,
    write: Optional[Callable[..., tuple[str, str]]] = None,
    already_in_instantly: Optional[Callable[[str], Optional[bool]]] = None,
) -> dict[str, Any]:
    """Enrich an explicit domain list and tag every new contact ``redo_partner``.

    ``dry_run`` defaults to true and does not call the contact finder or write
    rows. A live run stores contacts and does not push them. It also does not
    flush the regular unpushed queue: ``redo_partner`` is excluded from
    ``push_pending``, and this call leaves Instantly alone.
    """
    cleaned = domain_list_from(domains)
    base = {
        "ok": False,
        "dry_run": dry_run,
        "recipe": _push.REDO_PARTNER_RECIPE,
        "domains": cleaned,
        "companies": len(cleaned),
        "pushed": 0,
    }
    if len(cleaned) > MAX_ENRICH_LIST_DOMAINS:
        return {**base, "domains": cleaned[:MAX_ENRICH_LIST_DOMAINS],
                "reason": f"At most {MAX_ENRICH_LIST_DOMAINS} domains per call."}
    if not cleaned:
        return {**base, "reason": "No domains to enrich."}
    if dry_run:
        return {**base, "ok": True, "contacts_found": 0,
                "note": "Dry run: nothing enriched and nothing sent to Instantly."}
    leads = [{
        "domain": domain,
        "brand": domain.split(".")[0],
        "recipe": _push.REDO_PARTNER_RECIPE,
        "tier": "",
        "categories": "",
    } for domain in cleaned]
    # Instantly stays off even on a live enrich. Passing dry_run through to
    # run_enrichment would also push every other waiting contact.
    report = run_enrichment(
        engine, leads, api_key=api_key,
        max_companies=len(leads), max_contacts=2,
        credit_budget=max(len(leads) * 6, 1),
        campaign_id="", dry_run=True,
        find=find, write=write, already_in_instantly=already_in_instantly,
    )
    hard_fail = bool(report.stopped_reason) and report.companies_tried == 0 and report.contacts_found == 0
    return {
        **base,
        "ok": not hard_fail,
        "contacts_found": report.contacts_found,
        "reason": report.stopped_reason,
        "note": "Contacts are tagged redo_partner and left out of the regular Instantly push.",
        "report": report.as_dict(),
    }
