"""The replacement Finance experience: a small, traceable daily money brief.

This module deliberately depends on the established deterministic Finance
control builder.  It changes presentation and provenance, not Plaid,
QuickBooks, settlement, or commitment storage.
"""

from __future__ import annotations

import hashlib
import html
import json
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from sales_support_agent.services.cashflow.accounts_view import load_accounts_overview
from sales_support_agent.services.cashflow.cashflow_helpers import _page_shell
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.overview import (
    _build_renderer_state,
    _load_finance_control_inputs,
    _load_settlement_context,
    _normalise_renderer_state,
    _resolve_current_balance,
)


def _money(cents: int | None, *, exact: bool = False) -> str:
    if cents is None:
        return "Unavailable"
    value = int(cents) / 100
    return f"${value:,.2f}" if exact else f"${value:,.0f}"


@dataclass(frozen=True)
class EvidenceAmount:
    key: str
    label: str
    cents: int
    evidence_class: str
    source: str
    as_of: str
    formula: str


@dataclass(frozen=True)
class Outlook:
    key: str
    label: str
    cents: int
    formula: str
    explanation: str


@dataclass(frozen=True)
class AttentionCase:
    key: str
    title: str
    explanation: str
    href: str
    action_label: str
    severity: str = "review"


@dataclass(frozen=True)
class FinanceBrief:
    calculation_id: str
    as_of: str
    source_label: str
    balance_available: bool
    trust_ready: bool | None
    review_count: int
    amounts: tuple[EvidenceAmount, ...]
    outlooks: tuple[Outlook, ...]
    attention: tuple[AttentionCase, ...]
    excluded_summary: str

    def amount(self, key: str) -> EvidenceAmount:
        return next(item for item in self.amounts if item.key == key)


def _attention_cases(
    state: Mapping[str, Any], *, balance_available: bool, review_count: int
) -> tuple[AttentionCase, ...]:
    cases: list[AttentionCase] = []
    trust = state["trust_gate"]
    if not balance_available:
        cases.append(
            AttentionCase(
                key="bank-balance",
                title="Update the bank balance",
                explanation="A current bank balance is required before Finance can describe cash safely.",
                href="/admin/finances/accounts",
                action_label="Check accounts",
                severity="blocked",
            )
        )
    for index, issue in enumerate(
        list(trust.get("issues") or [])[:2] if review_count else []
    ):
        cases.append(
            AttentionCase(
                key=f"trust-{index}",
                title="Resolve uncertain financial evidence",
                explanation=str(issue),
                href="/admin/finances/review",
                action_label="Review this",
                severity="blocked",
            )
        )
    collection = state.get("collections", {}).get("next_collection")
    if collection and len(cases) < 3:
        party = str(
            collection.get("party")
            if isinstance(collection, Mapping)
            else getattr(collection, "party", "")
        ) or "the oldest overdue invoice"
        cases.append(
            AttentionCase(
                key="oldest-collection",
                title=f"Follow up with {party}",
                explanation="This is the oldest confirmed open receivable in the collection list.",
                href="/admin/finances/collections",
                action_label="Open collections",
            )
        )
    if not cases:
        cases.append(
            AttentionCase(
                key="all-clear",
                title="No urgent review is blocking the brief",
                explanation="The connected sources have not reported a blocking exception.",
                href="/admin/finances/plan",
                action_label="View cash plan",
                severity="ready",
            )
        )
    return tuple(cases[:3])


def build_finance_brief(
    *,
    rows: Sequence[Mapping[str, Any]],
    balance_cents: int,
    balance_as_of: str,
    balance_source: str,
    settlement_annotations: Sequence[Mapping[str, Any]] | None = None,
    income_decisions: Any = None,
    source_connections: Any = None,
    as_of: date | None = None,
) -> FinanceBrief:
    """Build the small UI contract from the canonical deterministic state."""
    today = as_of or date.today()
    control, fallback, _ = _build_renderer_state(
        list(rows),
        int(balance_cents),
        balance_as_of,
        today,
        list(settlement_annotations) if settlement_annotations is not None else None,
        income_decisions,
        source_connections,
        balance_source,
    )
    state = _normalise_renderer_state(control, fallback)
    cash = state["cash"]
    actual = int(cash["cash_on_hand_cents"])
    confirmed_in = int(cash["incoming_confirmed_cents"])
    expected_in = int(cash["incoming_expected_cents"])
    confirmed_out = int(cash["required_out_cents"])
    expected_out = int(cash["expected_out_cents"])
    source_label = {
        "plaid": "Plaid connected bank",
        "csv": "Bank CSV",
        "qbo": "QuickBooks bank",
    }.get(str(balance_source).lower(), "Bank source")
    amounts = (
        EvidenceAmount(
            "cash",
            "Verified cash now",
            actual,
            "verified",
            source_label,
            balance_as_of,
            "Spendable connected-account balances selected for Finance.",
        ),
        EvidenceAmount(
            "confirmed_in",
            "Confirmed money in",
            confirmed_in,
            "confirmed",
            "QuickBooks and confirmed Anata receivables",
            today.isoformat(),
            "Open, dated receivables with no contradictory settlement evidence.",
        ),
        EvidenceAmount(
            "expected_in",
            "Expected money in",
            expected_in,
            "expected",
            "Approved historical patterns and modeled income",
            today.isoformat(),
            "Expected deposits are kept separate and are not available cash.",
        ),
        EvidenceAmount(
            "confirmed_out",
            "Confirmed money out",
            confirmed_out,
            "confirmed",
            "Anata commitments and accounting evidence",
            today.isoformat(),
            "Unsettled confirmed commitments due in the next 14 days.",
        ),
        EvidenceAmount(
            "expected_out",
            "Expected money out",
            expected_out,
            "expected",
            "Approved recurring bank patterns",
            today.isoformat(),
            "Predicted costs are shown separately from confirmed commitments.",
        ),
    )
    conservative = actual - confirmed_out - expected_out
    likely = conservative + confirmed_in
    optimistic = likely + expected_in
    outlooks = (
        Outlook(
            "conservative",
            "Conservative",
            conservative,
            "Verified cash − confirmed out − expected out",
            "Assumes incoming money is late and all known or expected costs occur.",
        ),
        Outlook(
            "likely",
            "Likely",
            likely,
            "Conservative + confirmed incoming",
            "Adds dated, confirmed receivables but excludes unconfirmed income.",
        ),
        Outlook(
            "optimistic",
            "Optimistic",
            optimistic,
            "Likely + expected incoming",
            "Also includes expected income that has not been confirmed.",
        ),
    )
    proof = {
        "as_of": today.isoformat(),
        "balance_as_of": balance_as_of,
        "source": balance_source,
        "amounts": [asdict(item) for item in amounts],
        "outlooks": [asdict(item) for item in outlooks],
    }
    calculation_id = hashlib.sha256(
        json.dumps(proof, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    try:
        from sales_support_agent.services.cashflow.bulk_resolve import list_review_items

        review_count = int(list_review_items().get("total") or 0)
    except Exception:
        review_count = 0
    return FinanceBrief(
        calculation_id=calculation_id,
        as_of=today.isoformat(),
        source_label=source_label,
        balance_available=bool(cash["balance_available"]),
        trust_ready=state["trust_gate"]["ready"],
        review_count=review_count,
        amounts=amounts,
        outlooks=outlooks,
        attention=_attention_cases(
            state,
            balance_available=bool(cash["balance_available"]),
            review_count=review_count,
        ),
        excluded_summary=state["data_quality"]["summary"],
    )


def load_finance_brief(settings: Any) -> FinanceBrief:
    """Load current evidence without mutating bank or accounting records."""
    try:
        rows = list_obligations(limit=10_000)
        rows, settlement_annotations = _load_settlement_context(rows)
        balance_cents, balance_as_of, balance_source = _resolve_current_balance(
            rows,
            plaid_environment=str(getattr(settings, "plaid_environment", "sandbox") or "sandbox"),
        )
        if str(balance_source).lower() == "plaid":
            accounts = load_accounts_overview()
            if int(accounts.get("account_count") or 0) > 0:
                # The headline and Accounts page must use the exact same role
                # selection. Savings/reserve balances stay visible but are not
                # silently added to spendable cash.
                balance_cents = int(accounts.get("spendable_cents") or 0)
                balance_as_of = str(accounts.get("as_of") or balance_as_of)
        income_decisions, source_connections = _load_finance_control_inputs(settings)
        return build_finance_brief(
            rows=rows,
            balance_cents=balance_cents,
            balance_as_of=balance_as_of,
            balance_source=balance_source,
            settlement_annotations=settlement_annotations,
            income_decisions=income_decisions,
            source_connections=source_connections,
        )
    except Exception:
        # Authentication and transition states must still render a safe Finance
        # shell when the database is unavailable. Zero is never presented as a
        # verified balance: the unavailable state explicitly blocks decisions.
        today = date.today().isoformat()
        amounts = (
            EvidenceAmount("cash", "Verified cash now", 0, "unavailable", "Bank source unavailable", today, "No current bank evidence is available."),
            EvidenceAmount("confirmed_in", "Confirmed money in", 0, "unavailable", "Accounting source unavailable", today, "No confirmed receivables are available."),
            EvidenceAmount("expected_in", "Expected money in", 0, "unavailable", "Forecast source unavailable", today, "No expected income is available."),
            EvidenceAmount("confirmed_out", "Confirmed money out", 0, "unavailable", "Commitment source unavailable", today, "No confirmed commitments are available."),
            EvidenceAmount("expected_out", "Expected money out", 0, "unavailable", "Forecast source unavailable", today, "No expected costs are available."),
        )
        outlooks = (
            Outlook("conservative", "Conservative", 0, "Unavailable until sources reconnect", "Finance cannot calculate this scenario safely."),
            Outlook("likely", "Likely", 0, "Unavailable until sources reconnect", "Finance cannot calculate this scenario safely."),
            Outlook("optimistic", "Optimistic", 0, "Unavailable until sources reconnect", "Finance cannot calculate this scenario safely."),
        )
        return FinanceBrief(
            calculation_id="sources-unavailable",
            as_of=today,
            source_label="Bank source unavailable",
            balance_available=False,
            trust_ready=False,
            review_count=0,
            amounts=amounts,
            outlooks=outlooks,
            attention=(
                AttentionCase(
                    "source-unavailable",
                    "Reconnect the Finance sources",
                    "Finance could not read current bank and accounting evidence.",
                    "/admin/finances/accounts",
                    "Check accounts",
                    "blocked",
                ),
            ),
            excluded_summary="No source records were counted while Finance was unavailable.",
        )


def _brief_nav(active: str) -> str:
    return render_finance_nav(active, counts={})


def _metric_html(item: EvidenceAmount, *, exact: bool = False) -> str:
    as_of = html.escape(item.as_of or "Update unavailable")
    return f"""
      <article class="money-brief-metric money-brief-metric--{html.escape(item.evidence_class)}">
        <div class="money-brief-metric__head">
          <span>{html.escape(item.label)}</span>
          <span class="money-evidence">{html.escape(item.evidence_class.title())}</span>
        </div>
        <strong>{_money(item.cents, exact=exact)}</strong>
        <p>{html.escape(item.source)} · {as_of}</p>
      </article>"""


def render_money_brief_page(
    brief: FinanceBrief, *, flash: str = ""
) -> str:
    amounts = {item.key: item for item in brief.amounts}
    attention_html = "".join(
        f"""
        <li class="money-attention money-attention--{html.escape(item.severity)}">
          <div><strong>{html.escape(item.title)}</strong>
          <p>{html.escape(item.explanation)}</p></div>
          <a class="btn btn-secondary btn-sm" href="{html.escape(item.href, quote=True)}">{html.escape(item.action_label)}</a>
        </li>"""
        for item in brief.attention
    )
    outlook_html = "".join(
        f"""
        <article class="money-outlook money-outlook--{html.escape(item.key)}">
          <span>{html.escape(item.label)}</span>
          <strong>{_money(item.cents, exact=True)}</strong>
          <p>{html.escape(item.explanation)}</p>
        </article>"""
        for item in brief.outlooks
    )
    status = (
        '<span class="money-status money-status--ready">Sources ready</span>'
        if brief.trust_ready is True
        else '<span class="money-status money-status--review">Calculated with exclusions</span>'
    )
    review_link = (
        '<a href="/admin/finances/review">Open Review</a>'
        if brief.review_count
        else '<span class="money-section-state">No daily review cases</span>'
    )
    body = f"""
    <div class="money-brief">
      {_brief_nav("today")}
      <header class="money-page-header">
        <div>
          <p class="finance-eyebrow">Finance</p>
          <h1>Your money brief</h1>
          <p class="money-page-subtitle">What is true now, what may happen over the next 14 days, and what needs your attention.</p>
        </div>
        <div class="money-page-status">
          {status}
          <span>Calculation {html.escape(brief.calculation_id)}</span>
        </div>
      </header>

      <section class="money-now" aria-labelledby="money-now-title">
        <div class="money-section-heading">
          <div><p class="finance-eyebrow">Right now</p><h2 id="money-now-title">The numbers that matter</h2></div>
          <a href="/admin/finances/accounts">Check accounts</a>
        </div>
        <div class="money-metrics">
          {_metric_html(amounts["cash"], exact=True)}
          <div class="money-flow-pair">
            {_metric_html(amounts["confirmed_in"])}
            {_metric_html(amounts["expected_in"])}
          </div>
          <div class="money-flow-pair">
            {_metric_html(amounts["confirmed_out"])}
            {_metric_html(amounts["expected_out"])}
          </div>
        </div>
      </section>

      <section class="money-outlook-section" aria-labelledby="money-outlook-title">
        <div class="money-section-heading">
          <div><p class="finance-eyebrow">14-day outlook</p><h2 id="money-outlook-title">Three honest possibilities</h2></div>
          <a href="/admin/finances/calculations/{html.escape(brief.calculation_id)}">See the math</a>
        </div>
        <div class="money-outlooks">{outlook_html}</div>
      </section>

      <section class="money-attention-section" aria-labelledby="money-attention-title">
        <div class="money-section-heading">
          <div><p class="finance-eyebrow">Needs attention</p><h2 id="money-attention-title">Handle these next</h2></div>
          {review_link}
        </div>
        <ol class="money-attention-list">{attention_html}</ol>
      </section>

      <footer class="money-proof-note">
        <strong>What was left out</strong>
        <p>{html.escape(brief.excluded_summary)} Excluded records remain in source history and are never silently counted as verified cash.</p>
      </footer>
    </div>"""
    return _page_shell("Your money brief", "today", body, flash=flash)


def render_calculation_page(brief: FinanceBrief, *, flash: str = "") -> str:
    amount_rows = "".join(
        f"<tr><td><strong>{html.escape(item.label)}</strong><span>{html.escape(item.evidence_class.title())}</span></td>"
        f"<td>{_money(item.cents, exact=True)}</td><td>{html.escape(item.source)}</td>"
        f"<td>{html.escape(item.formula)}</td></tr>"
        for item in brief.amounts
    )
    outlook_rows = "".join(
        f"<article class='money-formula'><div><span>{html.escape(item.label)}</span>"
        f"<strong>{_money(item.cents, exact=True)}</strong></div>"
        f"<code>{html.escape(item.formula)}</code><p>{html.escape(item.explanation)}</p></article>"
        for item in brief.outlooks
    )
    body = f"""
    <div class="money-brief">
      {_brief_nav("plan")}
      <header class="money-page-header">
        <div><p class="finance-eyebrow">Calculation proof</p><h1>See exactly how it was calculated</h1>
        <p class="money-page-subtitle">Nothing on this page changes your bank, books, or forecast.</p></div>
        <a class="btn btn-secondary" href="/admin/finances">Back to money brief</a>
      </header>
      <section class="money-proof-card">
        <div class="money-proof-meta"><span>Calculation</span><strong>{html.escape(brief.calculation_id)}</strong>
        <span>As of</span><strong>{html.escape(brief.as_of)}</strong></div>
        <div class="money-table-wrap"><table class="money-proof-table"><thead><tr>
        <th>Number</th><th>Amount</th><th>Source</th><th>Rule</th></tr></thead>
        <tbody>{amount_rows}</tbody></table></div>
      </section>
      <section class="money-formulas" aria-label="Outlook formulas">{outlook_rows}</section>
    </div>"""
    return _page_shell("Calculation proof", "plan", body, flash=flash)


def render_cash_plan_page(brief: FinanceBrief) -> str:
    outlook_html = "".join(
        f"<article class='money-plan-scenario money-plan-scenario--{html.escape(item.key)}'>"
        f"<span>{html.escape(item.label)}</span><strong>{_money(item.cents, exact=True)}</strong>"
        f"<p>{html.escape(item.explanation)}</p><code>{html.escape(item.formula)}</code></article>"
        for item in brief.outlooks
    )
    body = f"""
    <div class="money-brief">
      {_brief_nav("plan")}
      <header class="money-page-header">
        <div><p class="finance-eyebrow">Cash plan</p><h1>Plan without changing your books</h1>
        <p class="money-page-subtitle">Compare the next 14 days using the same source numbers shown on Today.</p></div>
        <a class="btn btn-secondary" href="/admin/finances/calculations/{html.escape(brief.calculation_id)}">See the math</a>
      </header>
      <section class="money-plan-lead">
        <p>Starting verified cash</p><strong>{_money(brief.amount("cash").cents, exact=True)}</strong>
        <span>{html.escape(brief.source_label)} · {html.escape(brief.amount("cash").as_of or "Update unavailable")}</span>
      </section>
      <section class="money-plan-grid">{outlook_html}</section>
      <div class="money-state-note"><strong>Read-only planning</strong>
      <p>These scenarios do not edit Plaid, QuickBooks, schedules, bills, or invoices.</p></div>
    </div>"""
    return _page_shell("Cash plan", "plan", body)


def render_accounts_page(brief: FinanceBrief, settings: Any) -> str:
    try:
        accounts = load_accounts_overview()
    except Exception:
        accounts = {
            "spendable_cents": 0,
            "reserve_cents": 0,
            "liability_cents": 0,
            "as_of": "",
            "account_count": 0,
            "banks": [],
        }
    bank_html: list[str] = []
    for bank in accounts.get("banks") or []:
        rows = "".join(
            f"<li><div><strong>{html.escape(str(account.get('name') or 'Account'))}</strong>"
            f"<span>{html.escape(str(account.get('subtype') or account.get('account_type') or '').title())}"
            f"{' · ••' + html.escape(str(account.get('mask'))) if account.get('mask') else ''}</span></div>"
            f"<div><strong>{_money(int(account.get('balance_cents') or 0), exact=True)}</strong>"
            f"<span>{html.escape(str(account.get('cash_role') or 'excluded').replace('_', ' ').title())}</span></div></li>"
            for account in bank.get("accounts") or []
        )
        bank_html.append(
            f"<article class='money-bank'><h2>{html.escape(str(bank.get('display_name') or 'Connected bank'))}</h2>"
            f"<ul>{rows}</ul></article>"
        )
    if not bank_html:
        bank_html.append(
            "<div class='money-empty'><h2>No connected accounts are available</h2>"
            "<p>Connect or repair a bank connection before relying on current cash.</p>"
            "<button class='btn btn-primary' type='button' data-open-plaid>Connect a bank</button></div>"
        )
    body = f"""
    <div class="money-brief">
      {_brief_nav("accounts")}
      <header class="money-page-header">
        <div><p class="finance-eyebrow">Accounts &amp; setup</p><h1>Know where cash comes from</h1>
        <p class="money-page-subtitle">Connected accounts stay in Plaid. This page explains which balances Finance counts.</p></div>
        <div class="money-account-actions">
          <button class="btn btn-secondary" type="button" data-open-plaid>Connect another bank</button>
          <form method="post" action="/admin/finances/accounts/refresh">
            <button class="btn btn-primary" type="submit">Refresh connected banks</button>
          </form>
        </div>
      </header>
      <section class="money-account-summary">
        <article><span>Spendable cash</span><strong>{_money(int(accounts.get("spendable_cents") or 0), exact=True)}</strong></article>
        <article><span>Savings &amp; reserves</span><strong>{_money(int(accounts.get("reserve_cents") or 0), exact=True)}</strong></article>
        <article><span>Credit cards owed</span><strong>{_money(int(accounts.get("liability_cents") or 0), exact=True)}</strong></article>
        <article><span>Connected accounts</span><strong>{int(accounts.get("account_count") or 0)}</strong></article>
      </section>
      <div class="money-bank-list">{''.join(bank_html)}</div>
      <div class="money-state-note"><strong>Balance freshness</strong>
      <p>Last available account evidence: {html.escape(str(accounts.get("as_of") or brief.amount("cash").as_of or "Unavailable"))}. A cached balance is labeled by date; it is not described as real-time.</p></div>
      <p class="money-connection-error" role="alert" data-plaid-error hidden></p>
    </div>
    <script src="/api/integrations/plaid/link-initialize.js"></script>
    <script>
    (() => {{
      const tokenKey = 'anata_plaid_link_token';
      const errorBox = document.querySelector('[data-plaid-error]');
      const showError = message => {{
        errorBox.textContent = message;
        errorBox.hidden = false;
      }};
      const exchange = async (publicToken, metadata) => {{
        const response = await fetch('/admin/finances/plaid/exchange', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json', 'Accept': 'application/json'}},
          body: JSON.stringify({{
            public_token: publicToken,
            institution_id: metadata?.institution?.institution_id || '',
            institution_name: metadata?.institution?.name || '',
            link_session_id: metadata?.link_session_id || ''
          }})
        }});
        if (!response.ok) throw new Error('The bank connected, but its first update did not finish.');
        sessionStorage.removeItem(tokenKey);
        window.location.assign('/admin/finances/accounts?flash=ok:Bank+connected+and+refreshed');
      }};
      const openLink = async button => {{
        if (!window.Plaid) {{ showError('The secure bank connection could not load. Reload this page and try again.'); return; }}
        const original = button.textContent;
        button.disabled = true;
        button.textContent = 'Preparing secure connection…';
        errorBox.hidden = true;
        try {{
          const response = await fetch('/admin/finances/plaid/link-token', {{method: 'POST', headers: {{'Accept': 'application/json'}}}});
          if (!response.ok) throw new Error('Plaid could not prepare a secure bank connection.');
          const data = await response.json();
          sessionStorage.setItem(tokenKey, data.link_token);
          window.Plaid.create({{
            token: data.link_token,
            onSuccess: exchange,
            onExit: () => {{ button.disabled = false; button.textContent = original; }}
          }}).open();
        }} catch (error) {{
          button.disabled = false;
          button.textContent = original;
          showError(error.message || 'The bank connection could not start.');
        }}
      }};
      document.querySelectorAll('[data-open-plaid]').forEach(button => button.addEventListener('click', () => openLink(button)));
      const oauthState = new URLSearchParams(window.location.search).get('oauth_state_id');
      const oauthToken = sessionStorage.getItem(tokenKey);
      if (oauthState && oauthToken && window.Plaid) {{
        window.Plaid.create({{
          token: oauthToken,
          receivedRedirectUri: window.location.href,
          onSuccess: exchange,
          onExit: () => showError('The bank authorization was not completed. Try connecting again.')
        }}).open();
      }}
    }})();
    </script>"""
    return _page_shell("Accounts & setup", "accounts", body)
