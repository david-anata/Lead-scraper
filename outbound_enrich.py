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
        brand TEXT,
        category TEXT,
        recipe TEXT,
        tier TEXT,
        campaign_id TEXT,
        pushed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
)


def ensure_tables(engine) -> None:
    """Create the two tables. Runs in the Render pre-deploy migration (schema
    owner). Vercel's runtime role cannot run DDL, so it skips this there."""
    import os
    if os.getenv("VERCEL") and engine.dialect.name == "postgresql":
        return
    with engine.begin() as conn:
        for sql in _CREATE:
            conn.execute(text(sql))


def enriched_domains(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT domain FROM {_DOMAINS}")).fetchall()
    return {str(r[0]).lower() for r in rows}


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
            {"d": domain, "s": status, "f": found, "c": credits, "r": reason[:300]})


def _record_person(engine, c: _contacts.Contact, opener: str, source: str,
                   lead: dict[str, Any]) -> None:
    cats = lead.get("categories") or ""
    if isinstance(cats, (list, tuple)):
        cats = ", ".join(str(c) for c in cats)
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {_PEOPLE} (email, domain, first_name, last_name, title, linkedin_url, "
            "opener, opener_source, brand, category, recipe, tier) "
            "VALUES (:e, :d, :f, :l, :t, :u, :o, :s, :b, :cat, :r, :tier) "
            "ON CONFLICT (email) DO NOTHING"),
            {"e": c.email, "d": c.domain, "f": c.first_name, "l": c.last_name, "t": c.title,
             "u": c.linkedin_url, "o": opener, "s": source,
             "b": str(lead.get("brand") or c.domain), "cat": str(cats or lead.get("niche") or ""),
             "r": str(lead.get("recipe") or ""), "tier": str(lead.get("tier") or "")})


def unpushed(engine, limit: int = 500) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT email, domain, first_name, last_name, title, opener, brand, category, "
            f"recipe, tier FROM {_PEOPLE} WHERE pushed_at IS NULL ORDER BY created_at LIMIT :n"),
            {"n": limit}).fetchall()
    return [dict(zip(("email", "domain", "first_name", "last_name", "title", "opener", "brand",
                      "category", "recipe", "tier"), r)) for r in rows]


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
    stopped_reason: str = ""
    pushed: int = 0
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
            "stopped_reason": self.stopped_reason, "pushed": self.pushed,
            "push_note": self.push_note, "samples": self.samples[:10],
        }


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
    push: Optional[Callable[..., _push.PushOutcome]] = None,
    deadline: Optional[float] = None,
    clock: Optional[Callable[[], float]] = None,
) -> EnrichReport:
    import time as _time
    clock = clock or _time.monotonic
    find = find or _contacts.find_contacts
    write = write or _openers.write_opener
    push = push or _push.push_leads
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

    for domain, lead in list(leads_by_domain.items())[:max_companies]:
        if report.credits_used >= credit_budget:
            report.stopped_reason = f"Credit budget of {credit_budget} reached."
            break
        if deadline is not None and clock() >= deadline:
            report.stopped_reason = "Time budget for this run reached; the next run continues."
            break
        report.companies_tried += 1
        res = find(domain, api_key=api_key, max_contacts=max_contacts)
        report.credits_used += res.credits_used
        fresh = [c for c in res.contacts if c.email not in emails_seen]
        status = "found" if fresh else ("duplicate" if res.contacts else "none")
        try:
            _record_domain(engine, domain, status, len(fresh), res.credits_used, res.reason)
            for c in fresh:
                line, src = write(lead, {"title": c.title})
                _record_person(engine, c, line, src, lead)
                emails_seen.add(c.email)
                report.contacts_found += 1
                if len(report.samples) < 10:
                    report.samples.append({"domain": domain, "email": c.email, "title": c.title,
                                           "opener": line, "opener_source": src})
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

    if campaign_id and not dry_run:
        out = push_pending(engine, campaign_id, dry_run=False, push=push)
        report.push_note = out["note"]
        report.pushed = out["pushed"]
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
        line, src = write(lead, {"title": title})
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE {_PEOPLE} SET opener = :o, opener_source = :s WHERE email = :e"),
                         {"o": line, "s": src, "e": email})
        done += 1
        ai += src == "ai"
        if len(samples) < 12:
            samples.append({"brand": brand, "title": title, "opener": line, "source": src})
    return {"rewritten": done, "ai": ai, "fallback": done - ai, "samples": samples}


def push_pending(engine, campaign_id: str, *, dry_run: bool = True, limit: int = 500,
                 push: Optional[Callable[..., _push.PushOutcome]] = None) -> dict[str, Any]:
    """Add every not-yet-pushed contact to an Instantly campaign. Marks them
    pushed only when Instantly accepted the batch with no errors."""
    push = push or _push.push_leads
    pending = unpushed(engine, limit)
    payload, emails = [], []
    for p in pending:
        lead = {"domain": p["domain"], "brand": p["brand"], "categories": p["category"],
                "recipe": p["recipe"], "tier": p["tier"]}
        payload.append(_push.build_lead(p, lead, p["opener"]))
        emails.append(p["email"])
    outcome = push(payload, campaign_id, dry_run=dry_run)
    pushed = 0
    if not outcome.dry_run and not outcome.errors and payload:
        _mark_pushed(engine, emails, campaign_id)
        pushed = len(emails)
    preview = [{k: v for k, v in lead.items() if k != "email"} for lead in payload[:3]]
    return {"pending": len(payload), "pushed": pushed, "created": outcome.created,
            "skipped": outcome.skipped, "dry_run": outcome.dry_run,
            "errors": outcome.errors, "note": outcome.reason or outcome.summary,
            "preview": preview}


def enriched_since(engine, since_utc) -> dict[str, int]:
    """Companies tried and credits spent since a UTC timestamp (daily caps)."""
    ensure_tables(engine)
    with engine.connect() as conn:
        row = conn.execute(text(
            f"SELECT COUNT(*), COALESCE(SUM(credits_used), 0) FROM {_DOMAINS} WHERE enriched_at >= :t"),
            {"t": since_utc}).fetchone()
    return {"companies": int(row[0] or 0), "credits": int(row[1] or 0)}
