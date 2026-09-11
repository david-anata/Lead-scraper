"""Persistence + read-back for Fulfillment Rate Sheets.

Mirrors brand_analysis/storage.py: short-lived ORM Sessions on the shared
global engine. Rate sheets ride on the existing `automation_runs` table
(run_type="fulfillment_rate_sheet") with the full result — including the
rendered HTML — in summary_json, exactly like sales decks. Engagement reuses
the deck tables (DeckVisitSession / DeckSectionView key only on run_id).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from . import workflow
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import (
    AutomationAction,
    AutomationRun,
    DeckSectionView,
    DeckVisitSession,
)

logger = logging.getLogger(__name__)

RUN_TYPE = "fulfillment_rate_sheet"
PUBLIC_CORRELATION_ACTION = "public_correlation"
PUBLIC_CORRELATION_PREFIX = "fulfillment-public:"


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_run(*, trigger: str, metadata: Optional[dict] = None) -> int:
    with _session() as s:
        run = AutomationRun(
            run_type=RUN_TYPE,
            status="running",
            trigger=trigger,
            started_at=datetime.now(timezone.utc),
            metadata_json=metadata or {},
            summary_json={},
        )
        s.add(run)
        s.flush()
        return int(run.id)


def _correlation_dedupe_key(correlation_id: str) -> str:
    return f"{PUBLIC_CORRELATION_PREFIX}{str(correlation_id or '').strip()}"


def _bind_public_correlation_session(
    session: Session,
    *,
    run_id: int,
    correlation_id: str,
) -> None:
    correlation = str(correlation_id or "").strip()
    if not correlation:
        return
    dedupe_key = _correlation_dedupe_key(correlation)
    action = session.execute(
        select(AutomationAction)
        .where(
            AutomationAction.dedupe_key == dedupe_key,
            AutomationAction.action_type == PUBLIC_CORRELATION_ACTION,
        )
        .order_by(AutomationAction.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if action is None:
        action = AutomationAction(
            run_id=run_id,
            clickup_task_id="",
            system="fulfillment",
            action_type=PUBLIC_CORRELATION_ACTION,
            dedupe_key=dedupe_key,
            success=True,
            error_message="",
            before_json={},
            after_json={},
        )
    else:
        action.run_id = run_id
        action.success = True
    session.add(action)


def bind_public_correlation(run_id: int, correlation_id: str) -> None:
    with _session() as session:
        _bind_public_correlation_session(
            session,
            run_id=run_id,
            correlation_id=correlation_id,
        )


def save_draft(run_id: int, summary: dict) -> None:
    """Persist a finished generation as a reviewable DRAFT (not yet public)."""
    with _session() as s:
        run = s.get(AutomationRun, run_id)
        if run is None:
            return
        run.status = "draft"
        run.completed_at = datetime.now(timezone.utc)
        run.summary_json = workflow.migrate(summary, published=False)
        s.add(run)
        _bind_public_correlation_session(
            s,
            run_id=run_id,
            correlation_id=str((summary or {}).get("public_correlation_id") or ""),
        )


# Backward-compatible alias: "completing" a generation now lands in draft.
complete_run = save_draft


def _locked_run(session: Session, run_id: int):
    """Serialize JSON updates on Postgres so publication cannot race edits."""
    return session.execute(select(AutomationRun).where(
        AutomationRun.id == run_id, AutomationRun.run_type == RUN_TYPE,
    ).with_for_update()).scalar_one_or_none()


def publish_run(run_id: int, *, expected_revision: int | None = None, actor: str = "") -> bool:
    """Publish exactly the reviewed rendered draft, without external calls."""
    with _session() as session:
        run = _locked_run(session, run_id)
        if run is None or run.status not in ("draft", "completed"):
            return False
        summary = workflow.migrate(dict(run.summary_json or {}), published=run.status == "completed")
        revision = summary["draft_revision"]
        if expected_revision is not None and revision != expected_revision:
            raise workflow.RevisionConflict("This draft changed. Reload and review the latest version.")
        errors = workflow.publication_errors(summary)
        if errors:
            raise ValueError(errors[0])
        summary["published_at"] = datetime.now(timezone.utc).isoformat()
        summary["published_snapshot"] = workflow.customer_snapshot(summary)
        summary["published_calculation"] = {"quote_margin_override": summary.get("quote_margin_override")}
        summary["owner_email"] = actor or summary.get("owner_email", "")
        if summary.get("pipeline_stage") not in ("won", "lost"):
            summary["pipeline_stage"] = "published"
        history = list(summary.get("negotiation_history") or [])
        history.append(workflow.history_event("Proposal published", actor, revision))
        summary["negotiation_history"] = history[-25:]
        run.status = "completed"
        run.summary_json = summary
        return True


def update_summary(run_id: int, patch: dict, *, expected_revision: int | None = None,
                   rendered: bool = False, actor: str = "", event: str = "") -> bool:
    """Merge under lock, preserving the live snapshot and invalidating review."""
    with _session() as session:
        run = _locked_run(session, run_id)
        if run is None:
            return False
        summary = workflow.migrate(dict(run.summary_json or {}), published=run.status == "completed")
        if expected_revision is not None and summary["draft_revision"] != expected_revision:
            raise workflow.RevisionConflict("This draft changed. Reload and review the latest version.")
        changed = any(key in patch and patch[key] != summary.get(key) for key in workflow.INPUT_KEYS)
        # Never accept a caller's stale copy of lifecycle metadata.
        safe_patch = {key: value for key, value in patch.items() if key not in {
            "draft_revision", "rendered_revision", "published_snapshot", "published_calculation", "pricing_review",
        }}
        summary.update(safe_patch)
        if changed:
            summary["draft_revision"] += 1
            summary.pop("pricing_review", None)
            pricing = dict(summary.get("sales_pricing") or {})
            pricing["reviewed"] = False
            if "fulfillment_actual_costs" in patch and "sales_pricing" not in patch:
                pricing["margin_approved"] = False
            summary["sales_pricing"] = pricing
        summary["suppress_fulfillment_pricing"] = workflow.document_kind(summary) == "shipping_teaser"
        if rendered:
            summary["rendered_revision"] = summary["draft_revision"]
        if actor:
            history = list(summary.get("negotiation_history") or [])
            history.append(workflow.history_event(event or ("Draft saved" if not rendered else "Preview rebuilt"), actor, summary["draft_revision"]))
            summary["negotiation_history"] = history[-25:]
        run.summary_json = summary
        return True


def approve_pricing(run_id: int, *, expected_revision: int, actor: str) -> None:
    """Approve the visible saved version, never implicitly approve new edits."""
    with _session() as session:
        run = _locked_run(session, run_id)
        if run is None:
            raise ValueError("Proposal not found.")
        summary = workflow.migrate(dict(run.summary_json or {}), published=run.status == "completed")
        if summary["draft_revision"] != expected_revision:
            raise workflow.RevisionConflict("This draft changed. Reload and review the latest version.")
        if workflow.document_kind(summary) != "fulfillment_proposal":
            raise ValueError("Prepare a fulfillment proposal before reviewing pricing.")
        errors = workflow.publication_errors(summary, require_review=False)
        if errors:
            raise ValueError(errors[0])
        event = workflow.history_event("Pricing reviewed", actor, expected_revision)
        summary["pricing_review"] = {"revision": expected_revision, "actor": actor, "at": event["at"]}
        summary["sales_pricing"] = {**(summary.get("sales_pricing") or {}), "reviewed": True}
        summary["negotiation_history"] = [*(summary.get("negotiation_history") or []), event][-25:]
        run.summary_json = summary


def fail_run(run_id: int, error: str) -> None:
    with _session() as s:
        run = s.get(AutomationRun, run_id)
        if run is None:
            return
        run.status = "failed"
        run.completed_at = datetime.now(timezone.utc)
        summary = dict(run.summary_json or {})
        summary["error"] = str(error)[:2000]
        run.summary_json = summary
        s.add(run)


def get_run(run_id: int) -> Optional[AutomationRun]:
    with _session() as s:
        run = s.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == RUN_TYPE,
            )
        ).scalar_one_or_none()
        return run


def get_run_by_public_correlation(correlation_id: str) -> Optional[AutomationRun]:
    """Find a public-funnel run by its high-entropy, non-PII correlation ID."""
    needle = str(correlation_id or "").strip()
    if not needle:
        return None
    with _session() as s:
        run = s.execute(
            select(AutomationRun)
            .join(AutomationAction, AutomationAction.run_id == AutomationRun.id)
            .where(
                AutomationAction.dedupe_key == _correlation_dedupe_key(needle),
                AutomationAction.action_type == PUBLIC_CORRELATION_ACTION,
                AutomationAction.success.is_(True),
                AutomationRun.run_type == RUN_TYPE,
            )
            .order_by(AutomationAction.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if run is not None:
            return run

        # Bounded compatibility path for public runs created before indexed
        # correlation actions were introduced. New and updated runs never use
        # this path; the fixed cap prevents historical growth from increasing
        # memory or query cost without bound.
        legacy_runs = (
            s.execute(
                select(AutomationRun)
                .where(AutomationRun.run_type == RUN_TYPE)
                .order_by(AutomationRun.id.desc())
                .limit(100)
            )
            .scalars()
            .all()
        )
        for legacy_run in legacy_runs:
            if str((legacy_run.summary_json or {}).get("public_correlation_id") or "") == needle:
                _bind_public_correlation_session(
                    s,
                    run_id=int(legacy_run.id),
                    correlation_id=needle,
                )
                return legacy_run
    return None


def list_runs(limit: int = 100) -> list[dict]:
    """Slim rows for the pipeline table, newest first."""
    with _session() as s:
        rows = (
            s.execute(
                select(AutomationRun)
                .where(AutomationRun.run_type == RUN_TYPE)
                .order_by(AutomationRun.started_at.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        out: list[dict] = []
        for r in rows:
            summary = dict(r.summary_json or {})
            prospect_profile = dict(summary.get("prospect_profile") or {})
            fulfillment_quote = dict(summary.get("fulfillment_quote") or {})
            pitched_monthly = float(fulfillment_quote.get("monthly_total") or 0) or None
            pass_through_monthly = 0.0
            for line in fulfillment_quote.get("lines") or []:
                if not isinstance(line, dict):
                    continue
                if str(line.get("key") or "") == "shipping":
                    try:
                        pass_through_monthly += float(line.get("monthly") or 0)
                    except (TypeError, ValueError):
                        pass
            monthly_order_volume = None
            vol_raw = prospect_profile.get("monthly_order_volume")
            if vol_raw is not None:
                try:
                    monthly_order_volume = int(vol_raw)
                except (TypeError, ValueError):
                    pass
            out.append(
                {
                    "id": int(r.id),
                    "draft_revision": int(summary.get("draft_revision") or 1),
                    "status": r.status,
                    # Existing rows predating the draft flow were published on
                    # completion, so status=="completed" IS the published bit.
                    "published": r.status == "completed",
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "design_title": str(summary.get("design_title") or ""),
                    "prospect": str(summary.get("prospect") or ""),
                    "needs_proposal_conversion": workflow.document_kind(summary) == "shipping_teaser" and summary.get("segment") != "diy" and bool(summary.get("rate_overrides")),
                    "origin_zip": str(summary.get("origin_zip") or ""),
                    "rates_source": str(summary.get("rates_source") or ""),
                    "sections_included": list(summary.get("sections_included") or []),
                    "view_path": str(summary.get("view_path") or ""),
                    "export_token": str(summary.get("export_token") or ""),
                    "warnings": list(summary.get("warnings") or []),
                    "error": str(summary.get("error") or ""),
                    # Pipeline fields
                    "pipeline_stage": str(summary.get("pipeline_stage") or "intake"),
                    "pipeline_notes": str(summary.get("pipeline_notes") or ""),
                    "fulfillment_actual_costs": dict(
                        summary.get("fulfillment_actual_costs") or {}
                    ),
                    "monthly_order_volume": monthly_order_volume,
                    "pitched_monthly": pitched_monthly,
                    "pass_through_monthly": round(pass_through_monthly, 2),
                    # Raw profile stored for the expand panel brief + margin calc
                    "prospect_profile": prospect_profile,
                    # HubSpot integration
                    "hubspot_quote_url": str(summary.get("hubspot_quote_url") or ""),
                    "hubspot_deal_url": str(summary.get("hubspot_deal_url") or ""),
                    "published_at": str(summary.get("published_at") or ""),
                }
            )
        return out


def update_stage(run_id: int, stage: str) -> bool:
    return update_summary(run_id, {"pipeline_stage": stage})


def update_costs(run_id: int, costs: dict, *, expected_revision: int | None = None) -> bool:
    return update_summary(run_id, {"fulfillment_actual_costs": costs}, expected_revision=expected_revision)


def update_notes(run_id: int, notes: str) -> bool:
    return update_summary(run_id, {"pipeline_notes": notes})


def append_history(run_id: int, event: str, detail: str = "", *, user_email: str = "") -> bool:
    """Append a compact operator-visible history event to summary_json."""
    with _session() as session:
        run = _locked_run(session, run_id)
        if run is None:
            return False
        summary = workflow.migrate(dict(run.summary_json or {}), published=run.status == "completed")
        history = list(summary.get("negotiation_history") or [])
        history.append({"at": datetime.now(timezone.utc).isoformat(),
            "event": str(event or "Updated").strip()[:80],
            "detail": str(detail or "").strip()[:220],
            "user_email": str(user_email or "").strip()[:120]})
        summary["negotiation_history"] = history[-25:]
        run.summary_json = summary
        return True


def delete_run(run_id: int) -> bool:
    """Delete a run plus its engagement rows. Returns True if a row existed."""
    with _session() as s:
        run = s.execute(
            select(AutomationRun).where(
                AutomationRun.id == run_id,
                AutomationRun.run_type == RUN_TYPE,
            )
        ).scalar_one_or_none()
        if run is None:
            return False
        session_rows = (
            s.execute(select(DeckVisitSession).where(DeckVisitSession.run_id == run_id))
            .scalars()
            .all()
        )
        for visit in session_rows:
            for sec in (
                s.execute(select(DeckSectionView).where(DeckSectionView.session_id == visit.id))
                .scalars()
                .all()
            ):
                s.delete(sec)
            s.delete(visit)
        s.delete(run)
        return True


def engagement_for(run_ids: list[int]) -> dict[int, dict]:
    """Per-run engagement rollup for the History table.

    Returns {run_id: {external_sessions, internal_sessions, total_seconds,
    max_scroll_pct, last_viewed_at}} — external-only for the time/scroll
    numbers so admin previews don't inflate prospect engagement.
    """
    if not run_ids:
        return {}
    with _session() as s:
        rows = (
            s.execute(select(DeckVisitSession).where(DeckVisitSession.run_id.in_(run_ids)))
            .scalars()
            .all()
        )
    out: dict[int, dict] = {}
    for row in rows:
        bucket = out.setdefault(
            int(row.run_id),
            {
                "external_sessions": 0,
                "internal_sessions": 0,
                "total_seconds": 0,
                "max_scroll_pct": 0,
                "last_viewed_at": None,
            },
        )
        if row.is_internal:
            bucket["internal_sessions"] += 1
        else:
            bucket["external_sessions"] += 1
            bucket["total_seconds"] += int(row.total_seconds or 0)
            bucket["max_scroll_pct"] = max(bucket["max_scroll_pct"], int(row.max_scroll_pct or 0))
        hb = row.last_heartbeat_at.isoformat() if row.last_heartbeat_at else None
        if hb and (bucket["last_viewed_at"] is None or hb > bucket["last_viewed_at"]):
            bucket["last_viewed_at"] = hb
    return out
