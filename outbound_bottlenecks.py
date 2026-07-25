"""Capacity + bottleneck read for the outbound machine (docs/outbound/09, B1).

Answers "where is the machine jammed": not enough emails going out, not enough
people to work replies, or not enough Clay to enrich. Pure math from a few
capacity inputs and the booked-call target, so it is fully unit-testable. The
scoreboard reads the inputs from env and joins the live reply rate.
"""

from __future__ import annotations

import html
import math
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BottleneckRow:
    stage: str
    have: Optional[float]
    need: Optional[float]
    unit: str = ""

    def known(self) -> bool:
        return self.have is not None and self.need is not None

    @property
    def ok(self) -> bool:
        return self.known() and self.have >= self.need  # type: ignore[operator]

    @property
    def shortfall_ratio(self) -> float:
        """How badly short: need / have. Higher is worse. Inf if we have zero
        but need some; 0 when unknown so it never wins 'biggest jam'."""
        if not self.known():
            return 0.0
        if self.have and self.have > 0:  # type: ignore[operator]
            return float(self.need) / float(self.have)  # type: ignore[arg-type]
        return math.inf if (self.need or 0) > 0 else 0.0

    @property
    def status(self) -> str:
        if not self.known():
            return "Set your numbers"
        return "OK" if self.ok else "Under target"


@dataclass
class Bottlenecks:
    rows: list[BottleneckRow] = field(default_factory=list)

    @property
    def biggest(self) -> Optional[BottleneckRow]:
        under = [r for r in self.rows if r.known() and not r.ok]
        if not under:
            return None
        return max(under, key=lambda r: r.shortfall_ratio)

    @property
    def headline(self) -> str:
        b = self.biggest
        if b is None:
            if any(not r.known() for r in self.rows):
                return "Add your capacity numbers to see where the machine is jammed."
            return "No bottleneck right now: capacity is keeping up."
        return f"Biggest bottleneck right now: {b.stage}."


def compute_bottlenecks(
    *,
    emails_have: Optional[float],
    emails_need: Optional[float],
    members_have: Optional[float],
    members_need: Optional[float],
    clay_have: Optional[float],
    clay_need: Optional[float],
) -> Bottlenecks:
    return Bottlenecks(rows=[
        BottleneckRow("Emails per day", emails_have, emails_need, "emails"),
        BottleneckRow("Reply capacity", members_have, members_need, "people"),
        BottleneckRow("Clay enrichment", clay_have, clay_need, "leads/day"),
    ])


def _env_float(name: str) -> Optional[float]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def get_bottlenecks(reply_rate_pct: Optional[float], emails_per_booked_call: Optional[float]) -> Bottlenecks:
    """Derive the three capacity rows from env inputs + the live reply rate.

    Env (all optional; missing ones show 'Set your numbers'):
      OUTBOUND_EMAILS_PER_DAY_CAPACITY   how many we can safely send/day
      OUTBOUND_SALES_MEMBERS             people working replies (default 2)
      OUTBOUND_REPLIES_PER_PERSON_PER_DAY how many replies one person clears (default 25)
      OUTBOUND_CLAY_CREDITS_REMAINING    Clay credits left
      OUTBOUND_CLAY_CREDITS_PER_LEAD     credits one enriched lead costs (default 3)
      OUTBOUND_TARGET_BOOKED_CALLS_PER_WEEK  goal (default 15)
      OUTBOUND_SENDING_DAYS_PER_WEEK         (default 5)
      OUTBOUND_EMAILS_PER_BOOKED_CALL_ASSUMED fallback if we have no live number (default 2000)
    """
    target_calls = _env_float("OUTBOUND_TARGET_BOOKED_CALLS_PER_WEEK") or 15.0
    send_days = _env_float("OUTBOUND_SENDING_DAYS_PER_WEEK") or 5.0
    epc = emails_per_booked_call or _env_float("OUTBOUND_EMAILS_PER_BOOKED_CALL_ASSUMED") or 2000.0

    # Emails/day needed to hit the weekly booked-call goal.
    emails_need = (target_calls * epc) / max(send_days, 1)
    emails_have = _env_float("OUTBOUND_EMAILS_PER_DAY_CAPACITY")

    # Reply capacity: replies/day = reply_rate * emails/day (uses capacity if set,
    # else the need, so the estimate exists before we hit full volume).
    emails_for_replies = emails_have if emails_have is not None else emails_need
    rr = (reply_rate_pct or 0) / 100.0
    replies_per_day = emails_for_replies * rr
    per_person = _env_float("OUTBOUND_REPLIES_PER_PERSON_PER_DAY") or 25.0
    members_need = math.ceil(replies_per_day / per_person) if replies_per_day > 0 else 0.0
    members_have = _env_float("OUTBOUND_SALES_MEMBERS")
    if members_have is None:
        members_have = 2.0  # David + Gabe, the locked operating roles

    # Clay: leads/day we can enrich vs leads/day we need (1 lead per email).
    credits_remaining = _env_float("OUTBOUND_CLAY_CREDITS_REMAINING")
    credits_per_lead = _env_float("OUTBOUND_CLAY_CREDITS_PER_LEAD") or 3.0
    # Capacity expressed as a per-day rate over a 20-working-day month.
    clay_have = (credits_remaining / credits_per_lead / 20.0) if credits_remaining is not None else None
    clay_need = emails_need  # need one enriched lead per email sent

    return compute_bottlenecks(
        emails_have=emails_have, emails_need=round(emails_need),
        members_have=members_have, members_need=members_need,
        clay_have=round(clay_have) if clay_have is not None else None,
        clay_need=round(clay_need),
    )


BOTTLENECK_CSS = """
  .bn-wrap { max-width:900px; margin:22px 0 0; }
  .bn-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:0 0 8px; }
  .bn-table { border-collapse:collapse; width:100%; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .bn-table th, .bn-table td { text-align:left; padding:10px 14px; border-bottom:1px solid #f0f0f3; font-size:14px; }
  .bn-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .bn-ok { color:#0a7d33; font-weight:700; }
  .bn-under { color:#b42318; font-weight:700; }
  .bn-unknown { color:#6b7280; }
  .bn-line { margin:12px 0 0; font-size:15px; font-weight:600; }
"""


_SINGULAR = {"people": "person", "emails": "email"}


def _fmt_num(v: Optional[float], unit: str) -> str:
    if v is None:
        return "not set"
    n = int(v) if float(v).is_integer() else v
    if n == 1 and unit in _SINGULAR:
        unit = _SINGULAR[unit]
    return f"{n:,} {unit}".strip()


def render_bottlenecks_html(bn: Bottlenecks) -> str:
    rows = []
    for r in bn.rows:
        if not r.known():
            cls, label = "bn-unknown", r.status
        elif r.ok:
            cls, label = "bn-ok", "OK"
        else:
            cls, label = "bn-under", "Under target"
        rows.append(
            f"<tr><td>{html.escape(r.stage)}</td>"
            f"<td>{html.escape(_fmt_num(r.have, r.unit))}</td>"
            f"<td>{html.escape(_fmt_num(r.need, r.unit))}</td>"
            f'<td class="{cls}">{html.escape(label)}</td></tr>'
        )
    return f"""
    <div class="bn-wrap">
      <p class="bn-h">Capacity and bottlenecks</p>
      <table class="bn-table">
        <thead><tr><th>Stage</th><th>Have</th><th>Need</th><th>Status</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      <p class="bn-line">{html.escape(bn.headline)}</p>
    </div>"""
