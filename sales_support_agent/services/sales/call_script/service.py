"""Live call script: one tracker per lead, call reviews, and page rendering.

The call script itself is a self-contained page (assets/call_script.html). It
was built for the claude.ai artifact runtime, so we serve it unchanged and
inject `assets/runtime_adapter.js`, which provides the same `window.claude.use`
surface backed by this module's endpoints.
"""

from __future__ import annotations

import html
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import SalesCallReview, SalesCallTracker

ASSETS_DIR = Path(__file__).resolve().parent / "assets"
SLIDES_DIR = ASSETS_DIR / "slides"
BASE_PATH = "/admin/sales/call"

# The script's per-call storage keys. Only these are persisted per lead; UI
# preferences (open tab, deck size) stay in the rep's browser.
PER_CALL_KEYS = (
    "ss-beliefs",
    "ss-gap",
    "ss-nums",
    "ss-sel",
    "ss-asks",
    "ss-ai",
    "ss-callmeta",
    "ss-last",
    "ss-live",
)
MAX_STATE_BYTES = 512 * 1024
MAX_REVIEW_BYTES = 256 * 1024
MAX_FIELD_LEN = 255
BELIEFS = ("Pain", "Doubt", "Cost", "Desire", "Support", "Trust", "Money")

STATUS_OPEN = "open"
STATUS_ARCHIVED = "archived"


class CallScriptError(ValueError):
    """Raised for invalid tracker or review input."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clean(value: Any, limit: int = MAX_FIELD_LEN) -> str:
    return " ".join(str(value or "").split())[:limit]


def _email(user: Optional[dict]) -> str:
    return str((user or {}).get("email") or "").strip().lower()


def can_access(user: Optional[dict], tracker: SalesCallTracker) -> bool:
    """Reps see their own trackers; super-admins see every tracker."""
    if not user:
        return False
    if user.get("is_superadmin"):
        return True
    return tracker.owner_email == _email(user)


# ---------------------------------------------------------------- trackers


def create_tracker(
    session: Session,
    user: dict,
    *,
    prospect_name: str,
    company: str = "",
    lead_link: str = "",
) -> SalesCallTracker:
    prospect_name = _clean(prospect_name)
    company = _clean(company)
    lead_link = _clean(lead_link, 1024)
    if not prospect_name and not company:
        raise CallScriptError("Add the prospect's name or company to start a call.")
    if lead_link and not lead_link.lower().startswith(("https://", "http://")):
        raise CallScriptError("The lead link must start with https://.")
    owner = _email(user)
    if not owner:
        raise CallScriptError("Sign in again to start a call.")
    now = _now()
    tracker = SalesCallTracker(
        id=str(uuid.uuid4()),
        owner_email=owner,
        prospect_name=prospect_name,
        company=company,
        lead_link=lead_link,
        status=STATUS_OPEN,
        state_json={},
        created_at=now,
        updated_at=now,
    )
    session.add(tracker)
    session.flush()
    return tracker


def list_trackers(
    session: Session, user: dict, *, include_archived: bool = False
) -> list[SalesCallTracker]:
    stmt = select(SalesCallTracker)
    if not user.get("is_superadmin"):
        stmt = stmt.where(SalesCallTracker.owner_email == _email(user))
    if not include_archived:
        stmt = stmt.where(SalesCallTracker.status == STATUS_OPEN)
    stmt = stmt.order_by(SalesCallTracker.updated_at.desc()).limit(200)
    return list(session.scalars(stmt))


def get_tracker(session: Session, user: dict, tracker_id: str) -> Optional[SalesCallTracker]:
    tracker = session.get(SalesCallTracker, str(tracker_id or "")[:36])
    if tracker is None or not can_access(user, tracker):
        return None
    return tracker


def sanitize_state(state: Any) -> dict:
    """Keep only the script's per-call keys and enforce a size ceiling."""
    if not isinstance(state, dict):
        raise CallScriptError("Tracker state must be an object.")
    clean = {k: state[k] for k in PER_CALL_KEYS if k in state}
    if len(json.dumps(clean, default=str)) > MAX_STATE_BYTES:
        raise CallScriptError("Tracker state is too large to save.")
    return clean


def save_state(session: Session, tracker: SalesCallTracker, state: Any) -> SalesCallTracker:
    tracker.state_json = sanitize_state(state)
    tracker.updated_at = _now()
    session.flush()
    return tracker


def set_status(session: Session, tracker: SalesCallTracker, status: str) -> SalesCallTracker:
    if status not in (STATUS_OPEN, STATUS_ARCHIVED):
        raise CallScriptError("Unknown tracker status.")
    tracker.status = status
    tracker.updated_at = _now()
    session.flush()
    return tracker


@dataclass(frozen=True)
class TrackerSummary:
    id: str
    label: str
    company: str
    owner_email: str
    beliefs_said: int
    stage: str
    updated_at: datetime
    status: str


def summarize(tracker: SalesCallTracker) -> TrackerSummary:
    state = tracker.state_json or {}
    beliefs = state.get("ss-beliefs") if isinstance(state.get("ss-beliefs"), dict) else {}
    said = sum(1 for b in BELIEFS if beliefs.get(b))
    meta = state.get("ss-callmeta") if isinstance(state.get("ss-callmeta"), dict) else {}
    if meta.get("reviewed"):
        stage = "Reviewed"
    elif meta.get("pitchAt"):
        stage = "Pitched"
    elif meta.get("start"):
        stage = "In discovery"
    else:
        stage = "Not started"
    return TrackerSummary(
        id=tracker.id,
        label=tracker.prospect_name or tracker.company,
        company=tracker.company,
        owner_email=tracker.owner_email,
        beliefs_said=said,
        stage=stage,
        updated_at=tracker.updated_at,
        status=tracker.status,
    )


# ---------------------------------------------------------------- reviews


def list_reviews(session: Session, user: dict) -> list[dict]:
    stmt = (
        select(SalesCallReview)
        .where(SalesCallReview.owner_email == _email(user))
        .order_by(SalesCallReview.updated_at.desc())
        .limit(500)
    )
    return [dict(r.review_json or {}, id=r.id) for r in session.scalars(stmt)]


def get_review(session: Session, user: dict, review_id: str) -> Optional[dict]:
    row = session.get(SalesCallReview, (_clean(review_id, 64), _email(user)))
    return dict(row.review_json or {}, id=row.id) if row else None


def upsert_review(
    session: Session, user: dict, review_id: str, review: Any, tracker_id: str = ""
) -> dict:
    review_id = _clean(review_id, 64)
    if not review_id:
        raise CallScriptError("Review id is required.")
    if not isinstance(review, dict):
        raise CallScriptError("Review must be an object.")
    if len(json.dumps(review, default=str)) > MAX_REVIEW_BYTES:
        raise CallScriptError("Review is too large to save.")
    owner = _email(user)
    now = _now()
    row = session.get(SalesCallReview, (review_id, owner))
    if row is None:
        row = SalesCallReview(
            id=review_id, owner_email=owner, created_at=now, review_json={}, tracker_id=""
        )
        session.add(row)
    row.review_json = review
    row.tracker_id = _clean(tracker_id, 36)
    row.updated_at = now
    session.flush()
    return dict(review, id=review_id)


def delete_review(session: Session, user: dict, review_id: str) -> bool:
    row = session.get(SalesCallReview, (_clean(review_id, 64), _email(user)))
    if row is None:
        return False
    session.delete(row)
    session.flush()
    return True


# ---------------------------------------------------------------- rendering


@lru_cache(maxsize=1)
def _script_html() -> str:
    return (ASSETS_DIR / "call_script.html").read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def _adapter_js() -> str:
    return (ASSETS_DIR / "runtime_adapter.js").read_text(encoding="utf-8")


def _json_for_script(value: Any) -> str:
    """JSON safe to embed inside a <script> element."""
    return (
        json.dumps(value, default=str)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
    )


def render_call_page(
    tracker: SalesCallTracker, user: dict, *, csrf: str, ai_enabled: bool
) -> str:
    """The call script with this lead's state and the runtime adapter injected."""
    label = tracker.prospect_name or tracker.company
    if tracker.prospect_name and tracker.company:
        label = f"{tracker.prospect_name}, {tracker.company}"
    config = {
        "base": BASE_PATH,
        "trackerId": tracker.id,
        "label": label,
        "csrf": csrf,
        "aiEnabled": bool(ai_enabled),
        "user": {"email": _email(user), "name": str(user.get("name") or "")},
        "state": tracker.state_json or {},
    }
    injection = (
        f"<script>window.__ANATA_CALL__ = {_json_for_script(config)};</script>\n"
        f"<script>\n{_adapter_js()}\n</script>\n"
    )
    page = _script_html()
    marker = "<script>"
    idx = page.find(marker)
    if idx < 0:
        raise RuntimeError("Call script asset is missing its script element.")
    page = page[:idx] + injection + page[idx:]
    title = f"<title>Call: {html.escape(label)}</title>"
    return page.replace("<title>Anata Call Script</title>", title, 1)


def slide_path(name: str) -> Optional[Path]:
    """Resolve a deck slide file, refusing anything outside the slides folder."""
    if not name or "/" in name or "\\" in name or not name.endswith(".jpg"):
        return None
    candidate = (SLIDES_DIR / name).resolve()
    if candidate.parent != SLIDES_DIR.resolve() or not candidate.is_file():
        return None
    return candidate
