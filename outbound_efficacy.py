"""Per-signal efficacy (docs/outbound/08 Part 3, docs/outbound/09 B1).

Which selection signal actually books calls. We record the signals that fired on
every pushed brand; when per-reply outcomes are available (positive reply / booked
call, keyed by domain), this joins them to show sent/positive/rate per signal and
each signal's lift over the overall baseline. Pure + testable; the outcome source
is wired later, so with no outcomes it honestly shows counts and 'no outcomes yet'.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class SignalStat:
    signal: str
    sent: int
    positive: int

    @property
    def positive_rate(self) -> Optional[float]:
        return round(100.0 * self.positive / self.sent, 2) if self.sent else None


@dataclass
class Efficacy:
    stats: list[SignalStat]
    baseline_rate: Optional[float]
    has_outcomes: bool

    def lift(self, stat: SignalStat) -> Optional[float]:
        if not self.baseline_rate or stat.positive_rate is None:
            return None
        return round(stat.positive_rate / self.baseline_rate, 2)


def compute_signal_efficacy(
    pushed: list[dict[str, Any]],
    outcomes: Optional[dict[str, dict[str, Any]]] = None,
) -> Efficacy:
    """pushed: [{domain, signals: [...]}]. outcomes: {domain: {positive: bool}}."""
    outcomes = outcomes or {}
    has_outcomes = len(outcomes) > 0

    counts: dict[str, list[int]] = {}  # signal -> [sent, positive]
    total_sent = 0
    total_pos = 0
    for lead in pushed:
        dom = str(lead.get("domain") or "").strip().lower()
        sigs = lead.get("signals") or []
        pos = bool(outcomes.get(dom, {}).get("positive"))
        total_sent += 1
        total_pos += 1 if pos else 0
        for sig in sigs:
            row = counts.setdefault(str(sig), [0, 0])
            row[0] += 1
            row[1] += 1 if pos else 0

    stats = [SignalStat(sig, c[0], c[1]) for sig, c in counts.items()]
    stats.sort(key=lambda s: s.sent, reverse=True)
    baseline = round(100.0 * total_pos / total_sent, 2) if (has_outcomes and total_sent) else None
    return Efficacy(stats=stats, baseline_rate=baseline, has_outcomes=has_outcomes)


EFFICACY_CSS = """
  .ef-wrap { max-width:900px; margin:22px 0 0; }
  .ef-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:0 0 8px; }
  .ef-table { border-collapse:collapse; width:100%; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .ef-table th, .ef-table td { text-align:left; padding:10px 14px; border-bottom:1px solid #f0f0f3; font-size:14px; }
  .ef-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .ef-empty { color:#6b7280; margin:8px 0 0; font-size:14px; }
"""


def _fmt_pct(v: Optional[float]) -> str:
    return f"{v}%" if v is not None else "-"


def render_efficacy_html(eff: Efficacy) -> str:
    if not eff.stats:
        return (
            '<div class="ef-wrap"><p class="ef-h">By signal (which to double down on)</p>'
            '<p class="ef-empty">Numbers fill in once brands are tagged and pushed.</p></div>'
        )
    rows = []
    for s in eff.stats:
        lift = eff.lift(s)
        lift_txt = f"{lift}x" if lift is not None else ("-" if eff.has_outcomes else "waiting on replies")
        rows.append(
            f"<tr><td>{html.escape(s.signal)}</td><td>{s.sent:,}</td>"
            f"<td>{_fmt_pct(s.positive_rate)}</td><td>{html.escape(lift_txt)}</td></tr>"
        )
    note = "" if eff.has_outcomes else (
        '<p class="ef-empty">Positive rates fill in once per-reply outcomes are connected. '
        'Counts show how many brands carried each signal.</p>'
    )
    return f"""
    <div class="ef-wrap">
      <p class="ef-h">By signal (which to double down on)</p>
      <table class="ef-table">
        <thead><tr><th>Signal</th><th>Sent</th><th>Positive %</th><th>vs baseline</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      {note}
    </div>"""
