"""Merchant/description → internal category mapping.

Rules are evaluated top-to-bottom; first match wins.  Each rule is a
(compiled_regex, category) pair.  Patterns are matched case-insensitively
against the full Description/Extended Description field from the bank CSV.

Categories
----------
    revenue         Client payments, deposits, inflows from customers
    payroll         Salary, wages, contractor payments
    tax             Tax payments, payroll tax remittances
    debt            Loan repayments, lines of credit, merchant cash advances
    rent            Lease / rent payments
    utilities       Power, gas, water, internet, cable, phone
    software        SaaS subscriptions, AI tools, developer tools
    insurance       Business or personal insurance premiums
    credit_card     Credit card payments (not purchases)
    transfer        Internal bank transfers, Wise, owner draws
    owner_draw      Payments to owners / personal accounts
    equipment       Hardware, audio/visual, physical equipment
    supplies        Office supplies, Amazon, retail, Costco, Walmart
    meals           Restaurants, food, dining (rarely a business expense)
    fees            Bank fees, payment processing fees, analysis fees
    uncategorized   No pattern matched
"""

from __future__ import annotations

import re
from typing import NamedTuple


class CategoryRule(NamedTuple):
    pattern: re.Pattern[str]
    category: str


# ---------------------------------------------------------------------------
# Rule list — order matters: more specific rules first
# ---------------------------------------------------------------------------

_RAW_RULES: list[tuple[str, str]] = [
    # ── Revenue / inflows ──────────────────────────────────────────────────
    (r"INTU.*DEPOSIT|INTUIT.*DEPOSIT|Intuit Deposit", "revenue"),
    (r"PAYPAL|VENMO", "revenue"),
    (r"MOBILE CHECK DEPOSIT|Remote Deposit", "revenue"),
    (r"UPS.*EDI PAYMTS|UPS.*GENERAL SERV", "revenue"),
    (r"CORP PAY|COSWAY|Cosway", "revenue"),
    (r"BILL PAYMT.*DAVID NARAYAN.*Credit|Deposit.*DAVID NARAYAN", "revenue"),

    # ── Payroll ────────────────────────────────────────────────────────────
    (r"INTUIT.*PAYROLL|INTU.*PAYROLL|TYPE:\s*PAYROLL", "payroll"),

    # ── Tax ────────────────────────────────────────────────────────────────
    (r"INTUIT.*TAX|INTU.*TAX|TYPE:\s*TAX", "tax"),

    # ── Debt / loan repayments ─────────────────────────────────────────────
    (r"FORAFINANCIAL|FORA FINANCIAL", "debt"),
    (r"Stripe Cap|STRIPE CAP|TYPE:\s*Stripe Cap", "debt"),
    (r"Payment to Fora|Transfer to Cap", "debt"),

    # ── Rent / lease ──────────────────────────────────────────────────────
    (r"BOULDER RANCH|Boulder Ranch", "rent"),

    # ── Utilities ─────────────────────────────────────────────────────────
    (r"LEHI CITY|LEHI.*UTIL", "utilities"),
    (r"QuestarGas|QUESTAR|Questar Gas", "utilities"),
    (r"CITY OF SARATOGA|SARATOGA.*CITY", "utilities"),
    (r"ROCKY.*MTN.*POWER|ROCKYMTN|PACIFIC POWER", "utilities"),
    (r"COMCAST|XFINITY|CABLE SVCS", "utilities"),
    (r"SPI\*ENB GAS|ENB GAS", "utilities"),
    (r"Payment to Questar|Payment to xfinity|Payment to Rocky Mountain", "utilities"),
    (r"LEHI CITY.*Von Hill", "utilities"),

    # ── Software / SaaS ───────────────────────────────────────────────────
    (r"OPENAI|CHATGPT", "software"),
    (r"CLAUDE\.AI|ANTHROPIC", "software"),
    (r"ZAPIER", "software"),
    (r"CLICKUP", "software"),
    (r"INSTANTLY", "software"),
    (r"HELIUM10|HELIUM 10", "software"),
    (r"SUPABASE", "software"),
    (r"LOOM|LOOM\.COM", "software"),
    (r"MICROSOFT|MICROSOFT#", "software"),
    (r"GODADDY|DNH\*GODADDY", "software"),
    (r"APOLLO\.IO", "software"),
    (r"HEYREACH", "software"),
    (r"HUNTER\.IO", "software"),
    (r"APPFOLIO", "software"),
    (r"BREVO|WWW\.BREVO", "software"),
    (r"WAREMATCH", "software"),
    (r"AMAZON PRIME", "software"),
    (r"PRINTPERFECT", "software"),
    (r"WYZE", "software"),
    (r"CANVA", "software"),

    # ── Insurance ─────────────────────────────────────────────────────────
    (r"CINCINNATI INSUR|Cincinnati Insur", "insurance"),
    (r"BEAR RIVER.*INS|Ins\. Paymt.*BEAR RIVER", "insurance"),

    # ── Credit card payments ───────────────────────────────────────────────
    (r"CITI AUTOPAY|CITIBANK|Payment to Citibank", "credit_card"),

    # ── Owner draws / personal transfers ──────────────────────────────────
    (r"CANYON VIEW", "owner_draw"),
    (r"CHARLENE NARAYAN", "owner_draw"),
    (r"DAVID NARAYAN", "owner_draw"),

    # ── Transfers ─────────────────────────────────────────────────────────
    (r"WISE", "transfer"),
    (r"From Share|To Share", "transfer"),

    # ── Equipment ─────────────────────────────────────────────────────────
    (r"SWEETWATER", "equipment"),

    # ── Supplies / retail ─────────────────────────────────────────────────
    (r"COSTCO", "supplies"),
    (r"WAL-MART|WALMART", "supplies"),
    (r"AMAZON MKTPL|Amazon\.com\*", "supplies"),
    (r"AMAZON(?!.*PRIME)", "supplies"),

    # ── Fees ──────────────────────────────────────────────────────────────
    (r"Analysis Fee|ANALYSIS FEE", "fees"),
    (r"INTUIT.*TRAN FEE|INTU.*TRAN FEE|Intuit Service Charges", "fees"),
    (r"VISA INTERNATIONAL SERVICE", "fees"),
    (r"Overd|OVERDRAFT", "fees"),

    # ── Meals (rarely business — flag for review) ──────────────────────────
    (r"RESTAURANT|DINING|DOORDASH|UBER EATS|GRUBHUB", "meals"),
]

# Compile all patterns once at import time
RULES: list[CategoryRule] = [
    CategoryRule(pattern=re.compile(pattern, re.IGNORECASE), category=category)
    for pattern, category in _RAW_RULES
]


def categorize(description: str, bank_category: str = "") -> str:
    """Return the internal category for a transaction description.

    Our pattern rules take priority over the bank's own category because the
    bank frequently miscategorises software tools as 'Restaurants & Dining' or
    'Entertainment'.  If no pattern matches, we fall back to normalising the
    bank's category; if that is also empty we return 'uncategorized'.

    Args:
        description:   The raw Description or Extended Description column from
                       the bank CSV (may include ACH memo text).
        bank_category: The 'Transaction Category' column value from the bank
                       CSV (may be empty).

    Returns:
        A lowercase internal category string.
    """
    text = (description or "").strip()
    for rule in RULES:
        if rule.pattern.search(text):
            return rule.category

    # Fall back to a normalised bank category when no rule matches
    return _normalise_bank_category(bank_category)


# Map of bank category strings → our internal categories
_BANK_CATEGORY_MAP: dict[str, str] = {
    "deposits": "revenue",
    "loan payments": "debt",
    "transfers": "transfer",
    "service charges & fees": "fees",
    "utilities": "utilities",
    "online services": "software",
    "shopping": "supplies",
    "home supplies": "supplies",
    "credit card payments": "credit_card",
    "cable & satellite": "utilities",
    "restaurants & dining": "meals",
    "entertainment": "other",
    "hobbies": "other",
    "other expenses": "other",
}


def _normalise_bank_category(bank_category: str) -> str:
    """Map a bank-supplied category label to our internal category."""
    key = (bank_category or "").strip().lower()
    return _BANK_CATEGORY_MAP.get(key, "other")
