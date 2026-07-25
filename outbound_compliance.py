"""System compliance against the Anata outbound briefs.

Checks the running machine against the non-negotiable rules in
docs/outbound/Anata_Cold_Outbound_System_Brief (sections 5.1-5.4, 6, 7) and the
Cold Outbound Copywriting Playbook (sections 5-7, 13). Each check is either

  pass    - provably true from live numbers or from our own code/config
  fail    - provably violated, fix before sending
  confirm - a setting that lives in Instantly or Clay, so a human confirms it
            once and records it here via an env flag (we never guess "pass")

The #1 KPI is POSITIVE reply rate, per the briefs: a high raw reply rate full of
"no thanks" is a false win. Everything here is pure and unit-tested.
"""

from __future__ import annotations

import html
import os
from dataclasses import dataclass
from typing import Optional

# Defaults come straight from the briefs.
DEFAULT_PER_MAILBOX_DAILY_CAP = 25      # brief 5.1 "safe per-mailbox daily send rate, e.g. 25/day"
DEFAULT_MAX_SEQUENCE_EMAILS = 3         # brief 5.3 / playbook 7 (2 default, 3 max)
DEFAULT_BOUNCE_LIMIT_PCT = 3.0          # brief 6: bounces matter as much as complaints
DEFAULT_POSITIVE_TARGET_PCT = 1.0       # our KPI target; tune as real data lands
DEFAULT_RECYCLE_MONTHS = 3              # brief 6: recycle no sooner than ~3 months

PASS, FAIL, CONFIRM = "pass", "fail", "confirm"


@dataclass
class Check:
    name: str
    status: str
    detail: str
    source: str


def _env_flag(name: str) -> Optional[bool]:
    """Tri-state: True/False if explicitly set, None if never confirmed."""
    raw = (os.getenv(name) or "").strip().lower()
    if raw in ("1", "true", "yes", "on", "done", "confirmed"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return None


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _confirm_check(name: str, env: str, detail_yes: str, detail_no: str, source: str) -> Check:
    flag = _env_flag(env)
    if flag is True:
        return Check(name, PASS, detail_yes, source)
    if flag is False:
        return Check(name, FAIL, detail_no, source)
    return Check(name, CONFIRM, f"Not confirmed yet. {detail_no}", source)


def compute_compliance(
    *,
    positive_rate: Optional[float],
    bounce_rate: Optional[float],
    connected: bool,
) -> list[Check]:
    """Build the full guardrail checklist. Live rates come from Instantly."""
    checks: list[Check] = []

    # ---- The KPI itself -----------------------------------------------------
    target = _env_float("OUTBOUND_POSITIVE_REPLY_TARGET_PCT", DEFAULT_POSITIVE_TARGET_PCT)
    if not connected or positive_rate is None:
        checks.append(Check(
            "Positive reply rate (our #1 KPI)", CONFIRM,
            "Waiting on live sending data.", "Brief 5.3 / Playbook 9",
        ))
    elif positive_rate >= target:
        checks.append(Check(
            "Positive reply rate (our #1 KPI)", PASS,
            f"{positive_rate}% is at or above our {target}% target.", "Brief 5.3 / Playbook 9",
        ))
    else:
        checks.append(Check(
            "Positive reply rate (our #1 KPI)", FAIL,
            f"{positive_rate}% is below our {target}% target. Change one thing at a time "
            f"(subject, then offer, then CTA) and judge on this number only.",
            "Brief 5.3 / Playbook 9",
        ))

    # ---- Deliverability guardrails -----------------------------------------
    limit = _env_float("OUTBOUND_BOUNCE_LIMIT_PCT", DEFAULT_BOUNCE_LIMIT_PCT)
    if not connected or bounce_rate is None:
        checks.append(Check("Bounce rate under control", CONFIRM,
                            "Waiting on live sending data.", "Brief 6"))
    elif bounce_rate <= limit:
        checks.append(Check("Bounce rate under control", PASS,
                            f"{bounce_rate}% is under the {limit}% ceiling.", "Brief 6"))
    else:
        checks.append(Check("Bounce rate under control", FAIL,
                            f"{bounce_rate}% is over the {limit}% ceiling. Pause and re-verify "
                            "the list before sending more.", "Brief 6"))

    cap = _env_float("OUTBOUND_EMAILS_PER_MAILBOX_PER_DAY", DEFAULT_PER_MAILBOX_DAILY_CAP)
    if cap <= DEFAULT_PER_MAILBOX_DAILY_CAP:
        checks.append(Check("Per-mailbox daily volume", PASS,
                            f"{int(cap)} a day per mailbox is within the safe rate. "
                            "Scale by adding mailboxes, not by sending more per mailbox.",
                            "Brief 5.1"))
    else:
        checks.append(Check("Per-mailbox daily volume", FAIL,
                            f"{int(cap)} a day per mailbox is above the safe rate of "
                            f"{DEFAULT_PER_MAILBOX_DAILY_CAP}. Add mailboxes instead.", "Brief 5.1"))

    seq = _env_float("OUTBOUND_SEQUENCE_EMAILS", DEFAULT_MAX_SEQUENCE_EMAILS)
    if seq <= 2:
        checks.append(Check("Sequence length", PASS,
                            f"{int(seq)} emails. This is the recommended default.", "Playbook 7"))
    elif seq <= DEFAULT_MAX_SEQUENCE_EMAILS:
        checks.append(Check("Sequence length", PASS,
                            f"{int(seq)} emails, at the ceiling. Only keep the third once this "
                            "campaign beats your baseline.", "Brief 5.3 / Playbook 7"))
    else:
        checks.append(Check("Sequence length", FAIL,
                            f"{int(seq)} emails is over the 3 email ceiling. More touches raise "
                            "spam risk without lifting replies.", "Brief 5.3"))

    # ---- Settings that live in Instantly (human confirms once) --------------
    checks.append(_confirm_check(
        "Open and click tracking off", "OUTBOUND_TRACKING_DISABLED",
        "Tracking is off, which protects inbox placement.",
        "Turn open and click tracking OFF in the campaign. Tracking pixels are a top "
        "deliverability killer.", "Brief 5.4"))
    checks.append(_confirm_check(
        "Warmup on for every mailbox", "OUTBOUND_WARMUP_ON",
        "Warmup is on for all sending mailboxes.",
        "Leave warmup permanently on. It offsets the spam complaints you will inevitably get.",
        "Brief 5.2"))
    checks.append(_confirm_check(
        "Sending from dedicated domains", "OUTBOUND_DEDICATED_DOMAINS",
        "Sending from dedicated domains, not the primary domain.",
        "Never bulk send from anatainc.com. That reputation is sacred.", "Brief 5.1"))
    checks.append(_confirm_check(
        "Email 1 is text only, no links or opt-out", "OUTBOUND_EMAIL1_TEXT_ONLY",
        "Email 1 carries no links, images, or unsubscribe language.",
        "Strip links, images, and opt-out language from email 1.", "Brief 5.3 / Playbook 11"))
    checks.append(_confirm_check(
        "Spintax on every send", "OUTBOUND_SPINTAX_ON",
        "Spintax is in place and the variations were previewed.",
        "Spintax is mandatory now, not optional. Preview the variations for nonsense.",
        "Brief 5.3"))
    checks.append(_confirm_check(
        "Verified emails only", "OUTBOUND_VERIFIED_ONLY",
        "Only verified addresses are sent to (bulk verify, then catch-all check).",
        "Gate sending on verified status in Clay. Two step: bulk verifier, then catch-all.",
        "Brief 3"))
    checks.append(_confirm_check(
        "Copy reviewed against the playbook", "OUTBOUND_COPY_APPROVED",
        "Copy passed the playbook audit and David approved it.",
        "Run the copy audit and get David's sign off before any real send.",
        "Playbook 13"))

    # ---- Rules our own code already enforces --------------------------------
    checks.append(Check("Dropshippers and print-on-demand excluded", PASS,
                        "Our sourcing filter drops these before they ever reach Clay.",
                        "Brief 2"))
    checks.append(Check("Never email the same brand twice", PASS,
                        "Every downloaded brand is remembered and skipped on later pulls.",
                        "Brief 6"))
    months = int(_env_float("OUTBOUND_RECYCLE_MONTHS", DEFAULT_RECYCLE_MONTHS))
    checks.append(Check("List recycling window", PASS,
                        f"Un-replied brands are held for {months} months, and are re-approached "
                        "with a fresh angle rather than the same copy.", "Brief 6"))
    return checks


def summarize(checks: list[Check]) -> dict[str, int]:
    out = {PASS: 0, FAIL: 0, CONFIRM: 0}
    for c in checks:
        out[c.status] = out.get(c.status, 0) + 1
    return out


COMPLIANCE_CSS = """
  .cp-wrap { max-width:900px; margin:22px 0 0; }
  .cp-h { font-size:13px; font-weight:800; letter-spacing:.06em; text-transform:uppercase; color:#6b7280; margin:0 0 8px; }
  .cp-table { border-collapse:collapse; width:100%; background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; }
  .cp-table th, .cp-table td { text-align:left; padding:10px 14px; border-bottom:1px solid #f0f0f3; font-size:14px; vertical-align:top; }
  .cp-table th { background:#fafafa; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:#6b7280; }
  .cp-pass { color:#0a7d33; font-weight:700; white-space:nowrap; }
  .cp-fail { color:#b42318; font-weight:700; white-space:nowrap; }
  .cp-confirm { color:#b54708; font-weight:700; white-space:nowrap; }
  .cp-src { color:#9ca3af; font-size:12px; }
  .cp-line { margin:12px 0 0; font-size:15px; font-weight:600; }
"""

_LABEL = {PASS: "OK", FAIL: "Fix this", CONFIRM: "Confirm"}


def render_compliance_html(checks: list[Check]) -> str:
    rows = []
    for c in checks:
        rows.append(
            f"<tr><td>{html.escape(c.name)}<div class='cp-src'>{html.escape(c.source)}</div></td>"
            f"<td class='cp-{c.status}'>{_LABEL[c.status]}</td>"
            f"<td>{html.escape(c.detail)}</td></tr>"
        )
    s = summarize(checks)
    if s[FAIL]:
        line = f"{s[FAIL]} thing(s) to fix before sending. {s[CONFIRM]} still to confirm."
    elif s[CONFIRM]:
        line = f"Nothing broken. {s[CONFIRM]} setting(s) still need a one-time confirmation."
    else:
        line = "Fully compliant with the outbound rules."
    return f"""
    <div class="cp-wrap">
      <p class="cp-h">System compliance (our rules, from the briefs)</p>
      <table class="cp-table">
        <thead><tr><th>Rule</th><th>Status</th><th>What it means</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
      <p class="cp-line">{html.escape(line)}</p>
    </div>"""
