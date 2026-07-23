"""Rep Accountability Dashboard — per-rep pipeline health metrics.

``build_rep_dashboard`` is pure data (testable without a request);
``render_rep_dashboard_page`` wraps it in the shared admin chrome.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import HubSpotDeal
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


def _esc(v: object) -> str:
    return html.escape(str(v or ""))


def _fmt_dollars(cents: int) -> str:
    if cents <= 0:
        return "$0"
    dollars = cents / 100
    if dollars >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if dollars >= 1_000:
        return f"${dollars / 1_000:.0f}k"
    return f"${dollars:,.0f}"


def _age_label(dt: Optional[datetime], as_of: datetime) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    days = (as_of - dt).days
    if days == 0:
        return "today"
    if days == 1:
        return "1d ago"
    if days < 30:
        return f"{days}d ago"
    months = days // 30
    return f"{months}mo ago"


@dataclass
class RepMetrics:
    owner_email: str
    owner_name: str
    open_deal_count: int = 0
    pipeline_cents: int = 0
    overdue_count: int = 0
    stale_count: int = 0
    no_amount_count: int = 0
    no_contact_count: int = 0
    last_touch_at: Optional[datetime] = None
    deal_ids: list[str] = field(default_factory=list)

    @property
    def health_score(self) -> int:
        """0-100 score: 100 = perfectly healthy pipeline."""
        if self.open_deal_count == 0:
            return 100
        issues = self.overdue_count + self.stale_count + self.no_amount_count
        ratio = issues / self.open_deal_count
        return max(0, int((1 - ratio) * 100))


@dataclass
class RepDashboard:
    reps: list[RepMetrics] = field(default_factory=list)
    total_open: int = 0
    total_pipeline_cents: int = 0
    stale_days: int = 14


def build_rep_dashboard(
    session: Session,
    *,
    as_of: datetime | None = None,
    stale_days: int = 14,
) -> RepDashboard:
    as_of = as_of or datetime.now(timezone.utc)
    stale_cutoff = as_of - timedelta(days=stale_days)

    deals = (
        session.scalars(
            select(HubSpotDeal).where(HubSpotDeal.is_closed.is_(False))
        ).all()
    )

    rep_map: dict[str, RepMetrics] = {}
    for d in deals:
        email = (d.owner_email or "").strip() or "unassigned"
        if email not in rep_map:
            parts = email.split("@")[0].split(".")
            name = " ".join(p.capitalize() for p in parts) if len(parts) > 1 else parts[0].capitalize()
            rep_map[email] = RepMetrics(owner_email=email, owner_name=name)
        m = rep_map[email]
        m.open_deal_count += 1
        m.pipeline_cents += d.amount_cents or 0
        m.deal_ids.append(d.hubspot_deal_id)

        cd = d.close_date
        if cd is not None:
            if cd.tzinfo is None:
                cd = cd.replace(tzinfo=timezone.utc)
            if cd < as_of:
                m.overdue_count += 1

        touch = d.last_meaningful_touch_at
        if touch is None or (touch.replace(tzinfo=timezone.utc) if touch.tzinfo is None else touch) < stale_cutoff:
            m.stale_count += 1

        if (d.amount_cents or 0) <= 0:
            m.no_amount_count += 1

        # Track most recent touch across this rep's deals.
        if d.last_meaningful_touch_at is not None:
            t = d.last_meaningful_touch_at
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            if m.last_touch_at is None or t > m.last_touch_at:
                m.last_touch_at = t

    reps = sorted(rep_map.values(), key=lambda r: (-r.overdue_count, -r.stale_count, r.owner_email))
    total_cents = sum(r.pipeline_cents for r in reps)

    return RepDashboard(
        reps=reps,
        total_open=len(deals),
        total_pipeline_cents=total_cents,
        stale_days=stale_days,
    )


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _score_badge(score: int) -> str:
    if score >= 80:
        color = "#1a7f4b"
        bg = "#e8f9ef"
    elif score >= 50:
        color = "#a06800"
        bg = "#fff8e1"
    else:
        color = "#c0392b"
        bg = "#fdf0ef"
    return (
        f'<span style="display:inline-block;padding:2px 10px;border-radius:999px;'
        f'background:{bg};color:{color};font-weight:800;font-size:12px;">{score}</span>'
    )


def render_rep_dashboard_page(
    dashboard: RepDashboard,
    *,
    user: dict | None = None,
    as_of: datetime | None = None,
) -> str:
    now = as_of or datetime.now(timezone.utc)

    rows_html = ""
    for rep in dashboard.reps:
        score = rep.health_score
        badge = _score_badge(score)
        overdue_cell = (
            f'<span style="color:#c0392b;font-weight:700">{rep.overdue_count}</span>'
            if rep.overdue_count else "0"
        )
        stale_cell = (
            f'<span style="color:#e07b00;font-weight:700">{rep.stale_count}</span>'
            if rep.stale_count else "0"
        )
        board_link = f'/admin/sales/deals?owner={_esc(rep.owner_email)}'
        rows_html += f"""
        <tr>
          <td><a href="{board_link}" style="color:#2B3644;font-weight:700;text-decoration:none">{_esc(rep.owner_name)}</a>
              <div style="font-size:11px;color:#888;margin-top:2px">{_esc(rep.owner_email)}</div></td>
          <td style="text-align:right">{rep.open_deal_count}</td>
          <td style="text-align:right">{_fmt_dollars(rep.pipeline_cents)}</td>
          <td style="text-align:center">{overdue_cell}</td>
          <td style="text-align:center">{stale_cell}</td>
          <td style="text-align:center">{rep.no_amount_count or "0"}</td>
          <td style="text-align:center">{_age_label(rep.last_touch_at, now)}</td>
          <td style="text-align:center">{badge}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Rep Accountability — Sales</title>
{render_agent_favicon_links()}
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Montserrat:wght@700;800;900&display=swap" rel="stylesheet">
<style>
{render_agent_nav_styles()}
body {{ margin:0; font-family:"Inter",sans-serif; background:#f9f7f3; color:#2B3644; }}
.page {{ max-width:1320px; margin:0 auto; padding:32px 24px; }}
h1 {{ font-family:"Montserrat",sans-serif; font-size:24px; font-weight:900; margin:0 0 4px; }}
.subtitle {{ font-size:13px; color:#888; margin:0 0 24px; }}
.stats {{ display:flex; gap:16px; flex-wrap:wrap; margin-bottom:28px; }}
.stat-card {{ background:#fff; border:1px solid rgba(43,54,68,.1); border-radius:14px; padding:16px 22px; }}
.stat-card .val {{ font-family:"Montserrat",sans-serif; font-size:26px; font-weight:900; }}
.stat-card .lbl {{ font-size:12px; color:#888; margin-top:2px; }}
.card {{ background:#fff; border:1px solid rgba(43,54,68,.1); border-radius:16px; overflow:hidden; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ padding:10px 14px; text-align:left; background:#f5f4f1; font-family:"Montserrat",sans-serif; font-size:11px; font-weight:800; letter-spacing:.05em; text-transform:uppercase; color:#888; border-bottom:1px solid rgba(43,54,68,.08); }}
th:not(:first-child) {{ text-align:center; }}
td {{ padding:12px 14px; border-bottom:1px solid rgba(43,54,68,.06); vertical-align:middle; }}
tr:last-child td {{ border-bottom:none; }}
tr:hover td {{ background:rgba(43,54,68,.02); }}
</style>
</head>
<body>
{render_agent_nav("sales_reps", sales_section="sales_reps", user=user)}
<div class="page">
  <h1>Rep Accountability</h1>
  <p class="subtitle">Open pipeline health per sales rep — updated on each HubSpot sync. Stale = no inbound touch in {dashboard.stale_days}+ days.</p>

  <div class="stats">
    <div class="stat-card"><div class="val">{len(dashboard.reps)}</div><div class="lbl">Active reps</div></div>
    <div class="stat-card"><div class="val">{dashboard.total_open}</div><div class="lbl">Open deals</div></div>
    <div class="stat-card"><div class="val">{_fmt_dollars(dashboard.total_pipeline_cents)}</div><div class="lbl">Total pipeline</div></div>
  </div>

  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Rep</th>
          <th>Open deals</th>
          <th>Pipeline</th>
          <th>Overdue</th>
          <th>Stale</th>
          <th>No amount</th>
          <th>Last touch</th>
          <th>Health</th>
        </tr>
      </thead>
      <tbody>
        {rows_html or '<tr><td colspan="8" style="text-align:center;color:#aaa;padding:32px">No open deals found.</td></tr>'}
      </tbody>
    </table>
  </div>
</div>
</body>
</html>"""
