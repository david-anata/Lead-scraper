"""Outbound scoreboard — one page showing how the machine is performing.

Reads sending numbers back from Instantly (and later Clay and the sales team),
computes the rates that matter, and renders a simple internal page. When a tool
is not connected it says so plainly rather than showing a fake number.

The math and rendering are pure and fully tested. The Instantly reader is thin
and defensive; its live response is confirmed once INSTANTLY_API_KEY is set on
the server.
"""

from __future__ import annotations

import html
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import requests

logger = logging.getLogger(__name__)

INSTANTLY_ANALYTICS_URL = "https://api.instantly.ai/api/v2/campaigns/analytics"


def _rate(part: int, whole: int) -> Optional[float]:
    if not whole:
        return None
    return round(100.0 * part / whole, 1)


@dataclass
class NicheRow:
    niche: str
    sent: int = 0
    positive: int = 0

    @property
    def positive_rate(self) -> Optional[float]:
        return _rate(self.positive, self.sent)


@dataclass
class Scoreboard:
    connected: bool = False
    reason: str = ""
    sent: int = 0
    replies: int = 0
    positive: int = 0
    bounces: int = 0
    booked_calls: Optional[int] = None
    niches: list[NicheRow] = field(default_factory=list)

    @property
    def reply_rate(self) -> Optional[float]:
        return _rate(self.replies, self.sent)

    @property
    def positive_rate(self) -> Optional[float]:
        return _rate(self.positive, self.sent)

    @property
    def bounce_rate(self) -> Optional[float]:
        return _rate(self.bounces, self.sent)

    @property
    def emails_per_booked_call(self) -> Optional[int]:
        if not self.booked_calls or not self.sent:
            return None
        return int(round(self.sent / self.booked_calls))


def compute_scoreboard(
    instantly_stats: Optional[dict[str, Any]],
    *,
    booked_calls: Optional[int] = None,
) -> Scoreboard:
    """Turn raw Instantly numbers into the scoreboard. None stats = not connected."""
    if not instantly_stats:
        return Scoreboard(connected=False, reason="Instantly not connected")

    board = Scoreboard(
        connected=True,
        sent=int(instantly_stats.get("sent") or 0),
        replies=int(instantly_stats.get("replies") or 0),
        positive=int(instantly_stats.get("positive_replies") or 0),
        bounces=int(instantly_stats.get("bounces") or 0),
        booked_calls=booked_calls,
    )
    for row in instantly_stats.get("by_niche") or []:
        board.niches.append(NicheRow(
            niche=str(row.get("niche") or "other"),
            sent=int(row.get("sent") or 0),
            positive=int(row.get("positive") or 0),
        ))
    board.niches.sort(key=lambda n: (-(n.positive_rate or -1), n.niche))
    return board


# ---- thin Instantly reader (injectable) --------------------------------------

def fetch_instantly_analytics(api_key: str, *, timeout: int = 30) -> Optional[dict[str, Any]]:
    """Read campaign analytics from Instantly. Returns normalized stats or None
    if not configured. Field names are mapped defensively and confirmed live."""
    if not api_key:
        return None
    resp = requests.get(
        INSTANTLY_ANALYTICS_URL,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    rows = payload if isinstance(payload, list) else payload.get("data") or payload.get("campaigns") or []

    sent = replies = positive = bounces = 0
    for row in rows:
        sent += int(row.get("emails_sent_count") or row.get("sent") or 0)
        replies += int(row.get("reply_count") or row.get("replies") or 0)
        positive += int(row.get("total_opportunities") or row.get("positive_replies") or 0)
        bounces += int(row.get("bounced_count") or row.get("bounces") or 0)
    return {"sent": sent, "replies": replies, "positive_replies": positive, "bounces": bounces}


def get_scoreboard(
    api_key: str,
    *,
    booked_calls: Optional[int] = None,
    fetch: Optional[Callable[..., Optional[dict[str, Any]]]] = None,
) -> Scoreboard:
    fetch = fetch or fetch_instantly_analytics
    try:
        stats = fetch(api_key)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[scoreboard] Instantly read failed")
        return Scoreboard(connected=False, reason=f"Instantly read failed: {exc}")
    return compute_scoreboard(stats, booked_calls=booked_calls)


# ---- render ------------------------------------------------------------------

def _tile(label: str, value: str, note: str = "") -> str:
    note_html = f"<small>{html.escape(note)}</small>" if note else ""
    return (
        f'<div class="ob-tile"><span class="ob-tile-label">{html.escape(label)}</span>'
        f'<strong>{html.escape(value)}</strong>{note_html}</div>'
    )


def _fmt_pct(v: Optional[float]) -> str:
    return f"{v}%" if v is not None else "not connected"


# Scoped CSS for the scoreboard body. Shared by the standalone page and the
# shell-wrapped page on agent.anatainc.com, so the tiles look identical in both.
SCOREBOARD_CSS = """
  .ob-tiles { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; max-width:900px; }
  .ob-tile { background:#fff; border:1px solid #e5e7eb; border-radius:14px; padding:16px 18px; display:grid; gap:4px; }
  .ob-tile-label { font-size:11px; letter-spacing:.08em; text-transform:uppercase; color:#6b7280; font-weight:700; }
  .ob-tile strong { font-size:24px; }
  .ob-tile small { color:#6b7280; }
  .ob-headline { margin:20px 0; font-size:16px; font-weight:600; }
  .ob-table { border-collapse:collapse; width:100%; max-width:900px; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .ob-table th, .ob-table td { text-align:left; padding:10px 14px; border-bottom:1px solid #f0f0f3; font-size:14px; }
  .ob-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  @media (max-width:640px) { .ob-tiles { grid-template-columns:repeat(2,1fr); } }
"""


def render_scoreboard_body(board: Scoreboard) -> str:
    """Just the tiles + headline + niche table — no <html> wrapper. The router
    drops this into the app shell; the standalone page wraps it below."""
    if not board.connected:
        tiles = (
            _tile("Sent", "not connected") + _tile("Reply rate", "not connected")
            + _tile("Positive", "not connected") + _tile("Bounce", "not connected")
        )
        headline = (
            '<p class="ob-headline">Connect Instantly to see your numbers. '
            f'{html.escape(board.reason)}</p>'
        )
    else:
        tiles = (
            _tile("Sent", f"{board.sent:,}")
            + _tile("Reply rate", _fmt_pct(board.reply_rate))
            + _tile("Positive", _fmt_pct(board.positive_rate))
            + _tile("Bounce", _fmt_pct(board.bounce_rate))
        )
        epc = board.emails_per_booked_call
        epc_text = f"~{epc:,} emails per booked call" if epc is not None else (
            "emails per booked call: fills in once sales calls are connected"
        )
        headline = f'<p class="ob-headline">{html.escape(epc_text)}</p>'

    niche_rows = "".join(
        f"<tr><td>{html.escape(n.niche)}</td><td>{n.sent:,}</td><td>{_fmt_pct(n.positive_rate)}</td></tr>"
        for n in board.niches
    ) or '<tr><td colspan="3">No niche data yet.</td></tr>'

    return f"""
  <div class="ob-tiles">{tiles}</div>
  {headline}
  <h2 style="font-size:15px; margin:24px 0 8px;">By niche (which to double down on)</h2>
  <table class="ob-table"><thead><tr><th>Niche</th><th>Sent</th><th>Positive %</th></tr></thead>
  <tbody>{niche_rows}</tbody></table>"""


def render_scoreboard_html(board: Scoreboard) -> str:
    """Standalone page (used by the root app). agent.anatainc.com wraps
    render_scoreboard_body in the shell instead."""
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Outbound Scoreboard</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, sans-serif; background:#f7f7f9; color:#1c2230; margin:0; padding:32px; }}
  h1 {{ font-size:22px; margin:0 0 4px; }}
  .ob-sub {{ color:#6b7280; margin:0 0 24px; }}
  {SCOREBOARD_CSS}
</style></head>
<body>
  <h1>Outbound scoreboard</h1>
  <p class="ob-sub">Your machine, and how it is performing.</p>
  {render_scoreboard_body(board)}
</body></html>"""


def load_instantly_key() -> str:
    return (os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip()
