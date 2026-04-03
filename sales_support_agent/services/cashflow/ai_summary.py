"""Plain-language cashflow summary via Claude API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sales_support_agent.services.cashflow.engine import RiskAlert, WeekBucket


@dataclass
class SummaryResult:
    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cached: bool = False


_SYSTEM = (
    "You are a concise, direct financial advisor. "
    "The user is a small business owner reviewing their cashflow forecast. "
    "Respond in plain English — no markdown headers, no bullet points, no jargon. "
    "Keep your response under 150 words."
)


def _build_prompt(
    weeks: list[WeekBucket],
    alerts: list[RiskAlert],
    current_balance_cents: int,
) -> str:
    balance_dollars = current_balance_cents / 100

    # Summarise weeks
    week_lines: list[str] = []
    for w in weeks[:8]:  # cap at 8 weeks to stay within token budget
        sign = "+" if w.net_cents >= 0 else ""
        week_lines.append(
            f"  {w.label}: in ${w.inflow_cents/100:,.0f}  out ${w.outflow_cents/100:,.0f}"
            f"  net {sign}${w.net_cents/100:,.0f}  ending ${w.ending_cash_cents/100:,.0f}"
        )

    # Summarise alerts
    alert_lines: list[str] = []
    for a in alerts[:5]:
        alert_lines.append(f"  [{a.severity.upper()}] {a.title}: {a.detail}")

    weeks_block = "\n".join(week_lines) if week_lines else "  (no data)"
    alerts_block = "\n".join(alert_lines) if alert_lines else "  None"

    return (
        f"Current bank balance: ${balance_dollars:,.2f}\n\n"
        f"12-week cashflow forecast:\n{weeks_block}\n\n"
        f"Risk alerts:\n{alerts_block}\n\n"
        "Give me a brief plain-English summary of the cash position, "
        "any important risks in the next 4 weeks, and one or two specific actions I should consider."
    )


def generate_cashflow_summary(
    weeks: list[WeekBucket],
    alerts: list[RiskAlert],
    current_balance_cents: int,
    *,
    api_key: str | None = None,
    model: str = "claude-haiku-4-5-20251001",
) -> SummaryResult:
    """
    Call the Anthropic API to produce a plain-language cashflow summary.

    Falls back to a static summary if the API key is missing or the call fails,
    so the page never hard-errors.
    """
    import os

    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return SummaryResult(
            text=(
                "AI summary unavailable — set ANTHROPIC_API_KEY to enable. "
                "Review the forecast table and alerts above for your cash position."
            ),
            model="none",
            input_tokens=0,
            output_tokens=0,
        )

    prompt = _build_prompt(weeks, alerts, current_balance_cents)

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=key)
        message = client.messages.create(
            model=model,
            max_tokens=256,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text if message.content else ""
        return SummaryResult(
            text=text.strip(),
            model=message.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
        )

    except Exception as exc:  # noqa: BLE001
        return SummaryResult(
            text=f"AI summary temporarily unavailable ({exc}). Check the forecast and alerts above.",
            model="error",
            input_tokens=0,
            output_tokens=0,
        )
