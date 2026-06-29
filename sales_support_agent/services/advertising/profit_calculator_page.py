"""Advertising > Profit Calculator pages.

Keep the calculator first-party in the server-rendered app, but isolate its CSS
and JS from the shared admin shell by rendering the interactive runtime on a
dedicated page that the admin shell embeds in an iframe.
"""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.advertising.audit_page import _page


def _esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


_CATEGORY_OPTIONS = (
    ("everythingElse", "Everything Else"),
    ("appliancesCompact", "Appliances - Compact"),
    ("appliancesFull", "Appliances - Full-size"),
    ("automotive", "Automotive and Powersports"),
    ("babyProducts", "Baby Products"),
    ("backpacksLuggage", "Backpacks, Handbags, and Luggage"),
    ("beauty", "Beauty, Health, and Personal Care"),
    ("businessIndustrial", "Business, Industrial, and Scientific Supplies"),
    ("clothing", "Clothing and Accessories"),
    ("computers", "Computers"),
    ("consumerElectronics", "Consumer Electronics"),
    ("electronicsAccessories", "Electronics Accessories"),
    ("eyewear", "Eyewear"),
    ("fineArt", "Fine Art"),
    ("footwear", "Footwear"),
    ("furniture", "Furniture"),
    ("groceryGourmet", "Grocery and Gourmet"),
    ("homeKitchen", "Home and Kitchen"),
    ("jewelry", "Jewelry"),
    ("lawnGarden", "Lawn and Garden"),
    ("mattresses", "Mattresses"),
    ("musicalInstruments", "Musical Instruments"),
    ("officeProducts", "Office Products"),
    ("outdoors", "Outdoors"),
    ("petProducts", "Pet Products"),
    ("sports", "Sports and Outdoors"),
    ("toolsHomeImprovement", "Tools and Home Improvement"),
    ("toysGames", "Toys and Games"),
    ("videoGameConsoles", "Video Game Consoles"),
    ("videoGamesAccessories", "Video Games and Accessories"),
    ("watches", "Watches"),
)


_APP_STYLES = """
<style>
  @import url("https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Roboto:wght@300;400;500&display=swap");

  :root {
    color-scheme: light;
  }

  html,
  body {
    margin: 0;
    background: transparent;
    color: #222222;
    font-family: "Roboto", sans-serif;
  }

  body {
    padding: 0;
  }

  .apc-calculator {
    --apc-bg: #85bbda;
    --apc-card: #ffffff;
    --apc-border: #d9d2c3;
    --apc-text: #222222;
    --apc-muted: #6b6b6b;
    --apc-accent: #111111;
    --apc-button-text: #ffffff;
    --apc-radius: 18px;
    --apc-gap: 24px;
    color: var(--apc-text);
    background: transparent;
    border: 0;
    border-radius: 0;
    padding: 0;
    box-shadow: none;
  }

  .apc-calculator,
  .apc-calculator * {
    box-sizing: border-box;
  }

  .apc-header {
    display: flex;
    align-items: end;
    justify-content: space-between;
    gap: 18px;
    margin-bottom: 22px;
  }

  .apc-header-copy {
    max-width: 780px;
  }

  .apc-eyebrow {
    display: inline-block;
    margin-bottom: 10px;
    padding: 8px 12px;
    border-radius: 999px;
    background: rgba(17, 17, 17, 0.88);
    color: #ffffff;
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .apc-header h1 {
    margin: 0;
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    font-size: clamp(30px, 4vw, 48px);
    font-weight: 800;
    letter-spacing: -0.04em;
    line-height: 0.98;
  }

  .apc-header p {
    margin: 10px 0 0;
    color: rgba(43, 54, 68, 0.86);
    font-size: 16px;
    line-height: 1.55;
  }

  .apc-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    justify-content: flex-end;
    align-items: center;
    padding: 8px;
    border: 1px solid rgba(43, 54, 68, 0.12);
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.58);
    backdrop-filter: blur(10px);
  }

  .apc-calculator button,
  .apc-button-link {
    appearance: none;
    border: 0;
    border-radius: 999px;
    background: #2b3644;
    color: var(--apc-button-text);
    min-height: 46px;
    padding: 12px 18px;
    font-family: "Montserrat", sans-serif;
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 0.06em;
    line-height: 1;
    text-transform: uppercase;
    cursor: pointer;
    transition: background 0.2s ease, color 0.2s ease, transform 0.2s ease, opacity 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
    box-shadow: 0 10px 22px rgba(43, 54, 68, 0.14);
  }

  .apc-calculator button:hover,
  .apc-calculator button:focus,
  .apc-button-link:hover,
  .apc-button-link:focus {
    background: #85bbda;
    color: #ffffff;
    border-color: #85bbda;
    opacity: 1;
    transform: translateY(-1px);
    outline: none;
  }

  .apc-button-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    text-decoration: none;
  }

  .apc-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.2fr) minmax(360px, 0.8fr);
    gap: var(--apc-gap);
    align-items: start;
  }

  .apc-section {
    background: var(--apc-card);
    border: 1px solid rgba(217, 210, 195, 0.72);
    border-radius: 22px;
    padding: 22px;
    box-shadow: 0 12px 30px rgba(17, 17, 17, 0.045);
  }

  .apc-section h2,
  .apc-section h3 {
    margin: 0 0 14px;
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    line-height: 1.15;
  }

  .apc-section h2 {
    font-size: 24px;
  }

  .apc-section h3 {
    font-size: 18px;
  }

  .apc-section-head {
    display: flex;
    align-items: end;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 16px;
  }

  .apc-section-head p {
    margin: 8px 0 0;
    color: var(--apc-muted);
    font-size: 14px;
    line-height: 1.45;
  }

  .apc-tag {
    color: rgba(17, 17, 17, 0.62);
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    white-space: nowrap;
  }

  .apc-field {
    margin-bottom: 16px;
  }

  .apc-field:last-child {
    margin-bottom: 0;
  }

  .apc-field label,
  .apc-check-label {
    display: block;
    margin-bottom: 8px;
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .apc-check-label {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    letter-spacing: 0;
    text-transform: none;
  }

  .apc-field input[type="number"],
  .apc-field input[type="text"],
  .apc-field select {
    width: 100%;
    min-height: 48px;
    padding: 12px 14px;
    border: 1px solid var(--apc-border);
    border-radius: 12px;
    background: #fff;
    color: var(--apc-text);
    font-size: 16px;
    line-height: 1.2;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.72);
  }

  .apc-field input[type="checkbox"] {
    margin: 0;
    accent-color: #85bbda;
    transform: translateY(1px);
  }

  .apc-field input:focus,
  .apc-field select:focus {
    outline: none;
    border-color: #85bbda;
    box-shadow: 0 0 0 3px rgba(133, 187, 218, 0.24);
  }

  .apc-row {
    display: grid;
    gap: 12px;
  }

  .apc-row-2 {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .apc-row-3 {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .apc-inline-action {
    display: grid;
    gap: 10px;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: end;
  }

  .apc-meta-card {
    display: grid;
    gap: 8px;
    padding: 14px;
    border: 1px solid #ece7dc;
    border-radius: 14px;
    background: linear-gradient(180deg, #fffefa, #f8f5ef);
  }

  .apc-thumb {
    width: min(100%, 220px);
    aspect-ratio: 1 / 1;
    justify-self: center;
    border-radius: 14px;
    background: #dfe8ef center / cover no-repeat;
  }

  .apc-meta-card span,
  .apc-kpi span,
  .apc-mini-card span {
    display: block;
    color: rgba(17, 17, 17, 0.58);
    font-family: "Montserrat", sans-serif;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .apc-meta-card strong,
  .apc-kpi strong,
  .apc-mini-card strong {
    display: block;
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    font-size: 18px;
    font-weight: 700;
    line-height: 1.2;
  }

  .apc-meta-card p {
    margin: 0;
    color: var(--apc-muted);
    font-size: 14px;
    line-height: 1.45;
  }

  .apc-section button {
    border-radius: 12px;
    box-shadow: none;
  }

  .apc-button-secondary {
    background: rgba(255, 255, 255, 0.86) !important;
    color: #2b3644 !important;
    border: 1px solid rgba(43, 54, 68, 0.12) !important;
    box-shadow: 0 8px 20px rgba(43, 54, 68, 0.06) !important;
  }

  .apc-button-secondary:hover,
  .apc-button-secondary:focus {
    background: rgba(133, 187, 218, 0.16) !important;
    color: #2b3644 !important;
  }

  .apc-status {
    margin-top: 16px;
    padding: 12px 14px;
    border: 1px solid #e0d9cc;
    border-radius: 12px;
    background: #f8f4eb;
    color: #5b554c;
    font-size: 14px;
    line-height: 1.45;
  }

  .apc-status.is-success {
    border-color: rgba(110, 164, 128, 0.38);
    background: rgba(110, 164, 128, 0.12);
    color: #456b51;
  }

  .apc-status.is-danger {
    border-color: rgba(154, 90, 78, 0.36);
    background: rgba(154, 90, 78, 0.10);
    color: #8e4d42;
  }

  .apc-helper-card {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    margin-top: 14px;
    padding: 16px 18px;
    border: 1px solid #ece7dc;
    border-radius: 16px;
    background: linear-gradient(180deg, rgba(133, 187, 218, 0.12), rgba(255, 255, 255, 0.98));
  }

  .apc-helper-copy strong {
    display: block;
    margin-bottom: 6px;
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    font-size: 15px;
    font-weight: 700;
    letter-spacing: -0.02em;
  }

  .apc-helper-copy p {
    margin: 0;
    color: var(--apc-muted);
    font-size: 14px;
    line-height: 1.45;
  }

  .apc-results-stack {
    display: grid;
    gap: 18px;
  }

  .apc-kpis {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
  }

  .apc-kpi,
  .apc-mini-card {
    padding: 14px;
    border: 1px solid #ece7dc;
    border-radius: 14px;
    background: linear-gradient(180deg, #fffdfa, #f8f4ed);
  }

  .apc-kpi strong {
    margin-top: 8px;
    font-size: 26px;
    letter-spacing: -0.03em;
  }

  .apc-kpi small,
  .apc-mini-card small {
    display: block;
    margin-top: 6px;
    color: var(--apc-muted);
    font-size: 12px;
    line-height: 1.45;
  }

  .apc-kpi.is-negative strong {
    color: #9a5a4e;
  }

  .apc-mini-grid {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .apc-report-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .apc-report-list li {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    align-items: baseline;
    padding: 10px 0;
    border-bottom: 1px solid #ece7dc;
    color: var(--apc-muted);
    font-size: 14px;
  }

  .apc-report-list li:last-child {
    border-bottom: 0;
    padding-bottom: 0;
  }

  .apc-report-list strong {
    color: var(--apc-text);
    font-family: "Montserrat", sans-serif;
    font-size: 14px;
    font-weight: 700;
    text-align: right;
  }

  .apc-compare {
    display: grid;
    gap: 10px;
  }

  .apc-compare-head,
  .apc-compare-row {
    display: grid;
    gap: 10px;
    grid-template-columns: minmax(0, 1.1fr) repeat(3, minmax(0, 0.55fr));
    align-items: center;
  }

  .apc-compare-head span {
    color: rgba(17, 17, 17, 0.56);
    font-family: "Montserrat", sans-serif;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .apc-compare-row {
    padding-top: 12px;
    border-top: 1px solid #ece7dc;
  }

  .apc-compare-label strong {
    font-size: 15px;
  }

  .apc-compare-label span {
    margin-top: 4px;
    color: var(--apc-muted);
    font-size: 12px;
    letter-spacing: 0;
    text-transform: none;
  }

  .apc-compare-pill {
    padding: 10px 12px;
    border-radius: 12px;
    background: #f7f4ee;
    border: 1px solid #ece7dc;
  }

  .apc-compare-delta {
    justify-self: end;
    min-width: 90px;
    padding: 10px 12px;
    border-radius: 999px;
    background: rgba(133, 187, 218, 0.16);
    color: var(--apc-accent);
    font-family: "Montserrat", sans-serif;
    font-size: 12px;
    font-weight: 700;
    text-align: center;
  }

  .apc-note {
    margin-top: 16px;
    color: var(--apc-muted);
    font-size: 13px;
    line-height: 1.5;
  }

  @media (max-width: 1100px) {
    .apc-grid {
      grid-template-columns: 1fr;
    }

    .apc-header {
      flex-direction: column;
      align-items: stretch;
    }

    .apc-header-copy {
      max-width: none;
    }

    .apc-actions {
      width: 100%;
      justify-content: flex-start;
      border-radius: 20px;
    }
  }

  @media (max-width: 920px) {
    .apc-compare-head,
    .apc-compare-row {
      grid-template-columns: 1fr;
    }

    .apc-compare-delta {
      justify-self: start;
    }
  }

  @media (max-width: 760px) {
    .apc-calculator {
      padding: 0;
    }

    .apc-row-2,
    .apc-row-3,
    .apc-kpis,
    .apc-mini-grid,
    .apc-compare-head,
    .apc-compare-row {
      grid-template-columns: 1fr;
    }

    .apc-inline-action {
      grid-template-columns: 1fr;
    }

    .apc-actions {
      padding: 10px;
      gap: 8px;
    }

    .apc-actions > * {
      width: 100%;
    }

    .apc-helper-card {
      flex-direction: column;
      align-items: stretch;
    }

    .apc-compare-delta {
      justify-self: start;
    }
  }
</style>
"""


_APP_SCRIPT = """
<script>
(function () {
  var root = document.querySelector("[data-apc-root]");
  if (!root) return;

  var calcTimer = null;
  var currentState = {};

  function field(name) {
    return root.querySelector('[data-field="' + name + '"]');
  }

  function value(name) {
    var input = field(name);
    return input ? parseFloat(input.value) || 0 : 0;
  }

  function text(name) {
    var input = field(name);
    return input ? String(input.value || "") : "";
  }

  function checked(name) {
    var input = field(name);
    return !!(input && input.checked);
  }

  function setOutput(name, content) {
    root.querySelectorAll('[data-output="' + name + '"]').forEach(function (node) {
      node.textContent = content;
    });
  }

  function setOutputBackground(name, imageUrl) {
    root.querySelectorAll('[data-output-bg="' + name + '"]').forEach(function (node) {
      node.style.backgroundImage = imageUrl ? 'url("' + imageUrl.replace(/"/g, '\\"') + '")' : "none";
    });
  }

  function money(amount) {
    return "$" + (Number(amount) || 0).toFixed(2);
  }

  function percent(amount) {
    return (Number(amount) || 0).toFixed(2) + "%";
  }

  function roundToNearestHalf(amount) {
    return Math.round((Number(amount) || 0) * 2) / 2;
  }

  function roundToNearestQuarter(amount) {
    return Math.round((Number(amount) || 0) * 4) / 4;
  }

  function roundToNearestNickel(amount) {
    return Math.round((Number(amount) || 0) * 20) / 20;
  }

  function pluralizeMonths(months) {
    var rounded = Number(months) || 0;
    return rounded === 1 ? "1 month" : rounded + " months";
  }

  function seasonLabel(key) {
    return key === "peak" ? "Oct-Dec" : "Jan-Sep";
  }

  function trimTitle(value, maxLength) {
    var textValue = String(value || "");
    if (textValue.length <= maxLength) return textValue;
    return textValue.slice(0, Math.max(maxLength - 3, 0)).trim() + "...";
  }

  function formatMonthDayYear(date) {
    return date.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function apiBase() {
    return String(root.getAttribute("data-api-base") || "").replace(/\\/+$/, "");
  }

  function setStatus(message, tone) {
    var node = root.querySelector('[data-output="apiStatus"]');
    if (!node) return;
    node.textContent = message;
    node.className = "apc-status" + (tone ? " is-" + tone : "");
  }

  function autoFillField(name, nextValue) {
    var input = field(name);
    if (!input || input.dataset.userEdited === "true") return;
    input.value = nextValue;
  }

  function getDimensionalWeight(length, width, height) {
    if (!length || !width || !height) return 0;
    return (length * width * height) / 139;
  }

  function getVolume(length, width, height) {
    if (!length || !width || !height) return 0;
    return (length * width * height) / 1728;
  }

  function getEstimatedWeight(weight, length, width, height) {
    var direct = Number(weight) || 0;
    if (direct > 0) return direct;
    var dimensional = getDimensionalWeight(length, width, height);
    if (!dimensional) return 0;
    return Math.max(0.25, roundToNearestQuarter(dimensional));
  }

  function estimateCogs(price) {
    if (!price) return 0;
    return roundToNearestHalf(price / 3);
  }

  function estimateFbmDefaults(weight, length, width, height) {
    var shippingWeight = getEstimatedWeight(weight, length, width, height);
    var volume = getVolume(length, width, height);
    return {
      pickPack: roundToNearestHalf(2.5 + shippingWeight * 0.25),
      outbound: roundToNearestHalf(3.5 + shippingWeight * 0.65),
      storage: Math.max(0.15, roundToNearestNickel(volume * 0.6)),
      other: 0
    };
  }

  function formatDeltaPct(base, nextValue) {
    var baseNumber = Number(base) || 0;
    var nextNumber = Number(nextValue) || 0;
    if (!baseNumber) return "0.00%";
    var delta = ((nextNumber - baseNumber) / Math.abs(baseNumber)) * 100;
    return (delta > 0 ? "+" : "") + delta.toFixed(2) + "%";
  }

  function applyDerivedAutofills() {
    var length = value("length");
    var width = value("width");
    var height = value("height");
    var weight = value("weight");
    var resolvedWeight = getEstimatedWeight(weight, length, width, height);

    if (resolvedWeight > 0) {
      autoFillField("weight", resolvedWeight.toFixed(2));
    }

    var price = value("price");
    if (price > 0) {
      autoFillField("cogs", estimateCogs(price).toFixed(2));
    }

    var fbmDefaults = estimateFbmDefaults(resolvedWeight, length, width, height);
    autoFillField("fbmPickPack", fbmDefaults.pickPack.toFixed(2));
    autoFillField("fbmOutbound", fbmDefaults.outbound.toFixed(2));
    autoFillField("fbmStorage", fbmDefaults.storage.toFixed(2));
    autoFillField("fbmOther", fbmDefaults.other.toFixed(2));
  }

  function reportState(state) {
    currentState = state;

    setOutput("volume", Number(state.volume_cubic_feet || 0).toFixed(2) + " cu ft");
    setOutput("shippingWeight", Number(state.shipping_weight_lb || 0).toFixed(2) + " lb");
    setOutput("sizeTier", state.size_tier || "Unknown");
    setOutput("profit", money(state.net_profit));
    setOutput("margin", percent(state.net_margin_pct));
    setOutput("roi", percent(state.roi_on_cogs_pct));
    setOutput("revenue", money(state.total_revenue));
    setOutput("totalCost", money(state.total_cost));
    setOutput("fbaFee", money(state.fba_fee));
    setOutput("fbmProfit", money(state.fbm_profit));
    setOutput("fbmMargin", percent(state.fbm_margin_pct));
    setOutput("referralFee", money(state.referral_fee));
    setOutput("storageTotal", money(state.storage_total));
    setOutput("amazonFees", money(state.amazon_fees_total));
    setOutput("marketingCost", money(state.marketing_cost));
    setOutput("agencyCost", money(state.agency_cost));
    setOutput("fbmCost", money(state.fbm_cost));
    setOutput("compareCostFba", money(state.total_cost));
    setOutput("compareCostFbm", money(state.fbm_cost));
    setOutput("compareCostDelta", formatDeltaPct(state.total_cost, state.fbm_cost));
    setOutput("compareProfitFba", money(state.net_profit));
    setOutput("compareProfitFbm", money(state.fbm_profit));
    setOutput("compareProfitDelta", formatDeltaPct(state.net_profit, state.fbm_profit));
    setOutput("compareMarginFba", percent(state.net_margin_pct));
    setOutput("compareMarginFbm", percent(state.fbm_margin_pct));
    setOutput("compareMarginDelta", formatDeltaPct(state.net_margin_pct, state.fbm_margin_pct));

    var negative = Number(state.net_profit || 0) < 0;
    root.querySelectorAll('[data-kpi="profit"]').forEach(function (node) {
      node.classList.toggle("is-negative", negative);
    });
  }

  function buildPayload() {
    return {
      category_key: text("category") || "everythingElse",
      price: value("price"),
      buyer_shipping: value("shipping"),
      length: value("length"),
      width: value("width"),
      height: value("height"),
      weight_lb: value("weight"),
      is_apparel: checked("isApparel"),
      months_stored: value("monthsStored"),
      season: text("season") || "offpeak",
      inbound_fee: value("inbound"),
      other_amazon_fees: value("otherAmazonFees"),
      cogs: value("cogs"),
      prep_cost: value("prepCost"),
      misc_cost: value("miscCost"),
      marketing_pct: value("marketingPct"),
      agency_pct: value("agencyPct"),
      fbm_pick_pack: value("fbmPickPack"),
      fbm_outbound: value("fbmOutbound"),
      fbm_storage: value("fbmStorage"),
      fbm_other: value("fbmOther")
    };
  }

  function resetOutputs() {
    reportState({
      total_revenue: 0,
      referral_fee: 0,
      fba_fee: 0,
      storage_total: 0,
      amazon_fees_total: 0,
      marketing_cost: 0,
      agency_cost: 0,
      total_cost: 0,
      net_profit: 0,
      net_margin_pct: 0,
      roi_on_cogs_pct: 0,
      size_tier: "Unknown",
      volume_cubic_feet: 0,
      shipping_weight_lb: 0,
      fbm_cost: 0,
      fbm_profit: 0,
      fbm_margin_pct: 0
    });
  }

  async function fetchJson(url, options) {
    var response = await fetch(url, options || {});
    var payload = null;
    try {
      payload = await response.json();
    } catch (error) {}
    if (!response.ok) {
      var message = response.status + " " + response.statusText;
      if (payload && payload.detail) message = payload.detail;
      throw new Error(message);
    }
    return payload || {};
  }

  async function lookupAsin() {
    var asin = text("asin").trim();
    if (!asin) {
      setStatus("Enter an ASIN to look up product details.", "danger");
      return;
    }

    setStatus("Looking up ASIN " + asin + "...", "");

    try {
      ["weight", "cogs", "fbmPickPack", "fbmOutbound", "fbmStorage", "fbmOther"].forEach(function (name) {
        var input = field(name);
        if (input) delete input.dataset.userEdited;
      });

      var item = await fetchJson(apiBase() + "/catalog/" + encodeURIComponent(asin));
      if (field("category")) field("category").value = item.category_key || "everythingElse";
      if (field("price")) field("price").value = item.price || 0;
      if (field("length")) field("length").value = item.dimensions ? item.dimensions.length || 0 : 0;
      if (field("width")) field("width").value = item.dimensions ? item.dimensions.width || 0 : 0;
      if (field("height")) field("height").value = item.dimensions ? item.dimensions.height || 0 : 0;
      if (field("weight")) {
        field("weight").value = getEstimatedWeight(
          item.weight_lb || 0,
          item.dimensions ? item.dimensions.length || 0 : 0,
          item.dimensions ? item.dimensions.width || 0 : 0,
          item.dimensions ? item.dimensions.height || 0 : 0
        ).toFixed(2);
      }
      if (field("isApparel")) field("isApparel").checked = item.category_key === "clothing";

      applyDerivedAutofills();

      setOutput("lookupTitle", trimTitle(item.title || "Lookup complete", 72));
      setOutput("lookupMeta", (item.category_label || "Unknown category") + " | Marketplace " + (item.marketplace_id || "-"));
      setOutput("lookupBrand", item.brand || "Unknown brand");
      setOutput("lookupAsin", "ASIN " + (item.asin || asin));
      setOutputBackground("lookupImage", item.images && item.images[0] ? item.images[0] : "");

      setStatus("ASIN loaded from the backend. Recalculating profitability...", "success");
      await calculateViaApi();
    } catch (error) {
      setStatus("ASIN lookup failed: " + error.message, "danger");
    }
  }

  async function calculateViaApi() {
    applyDerivedAutofills();
    var payload = buildPayload();
    if (!payload.price && !payload.length && !payload.width && !payload.height && !payload.weight_lb) {
      resetOutputs();
      setStatus("Enter product details or use ASIN lookup to calculate.", "");
      return;
    }

    try {
      var result = await fetchJson(apiBase() + "/profitability/estimate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      reportState(result);
      setStatus("Connected to the backend profitability engine.", "success");
    } catch (error) {
      setStatus("Backend estimate failed: " + error.message, "danger");
    }
  }

  function scheduleCalculate() {
    if (calcTimer) window.clearTimeout(calcTimer);
    calcTimer = window.setTimeout(function () {
      calculateViaApi();
    }, 250);
  }

  function resetFields() {
    root.querySelectorAll("input[type='number']").forEach(function (input) {
      if (input.hasAttribute("value")) input.value = input.getAttribute("value");
      else input.value = "";
      delete input.dataset.userEdited;
    });
    root.querySelectorAll("input[type='text']").forEach(function (input) {
      input.value = "";
    });
    root.querySelectorAll("select").forEach(function (select) {
      select.selectedIndex = 0;
    });
    root.querySelectorAll("input[type='checkbox']").forEach(function (input) {
      input.checked = false;
    });
    setOutput("lookupTitle", "No ASIN loaded");
    setOutput("lookupMeta", "Use the live backend to pull product details.");
    setOutput("lookupBrand", "-");
    setOutput("lookupAsin", "ASIN not loaded");
    setOutputBackground("lookupImage", "");
    resetOutputs();
    setStatus("Enter product details or use ASIN lookup to calculate.", "");
  }

  function exportPdf() {
    var exportWindow = window.open("", "_blank", "width=1100,height=900");
    if (!exportWindow) return;

    var generatedOn = formatMonthDayYear(new Date());
    var title = root.querySelector('[data-output="lookupTitle"]');
    var brand = root.querySelector('[data-output="lookupBrand"]');

    exportWindow.document.open();
    exportWindow.document.write(
      "<!doctype html><html><head><meta charset='utf-8'><title>Amazon Profit Estimate</title>" +
      "<style>body{font-family:Arial,sans-serif;padding:24px;color:#222}h1{margin:0 0 8px}h2{margin:24px 0 12px}ul{list-style:none;padding:0}li{padding:8px 0;border-bottom:1px solid #ddd;display:flex;justify-content:space-between;gap:12px}small{color:#666}</style>" +
      "</head><body>" +
      "<h1>Amazon Profit Estimate</h1><small>Generated " + escapeHtml(generatedOn) + "</small>" +
      "<h2>" + escapeHtml((title && title.textContent) || "Manual model") + "</h2>" +
      "<p>Brand: " + escapeHtml((brand && brand.textContent) || "-") + "</p>" +
      "<ul>" +
      "<li><span>Revenue</span><strong>" + escapeHtml(root.querySelector('[data-output=\"revenue\"]').textContent) + "</strong></li>" +
      "<li><span>Net Profit</span><strong>" + escapeHtml(root.querySelector('[data-output=\"profit\"]').textContent) + "</strong></li>" +
      "<li><span>Net Margin</span><strong>" + escapeHtml(root.querySelector('[data-output=\"margin\"]').textContent) + "</strong></li>" +
      "<li><span>ROI on COGS</span><strong>" + escapeHtml(root.querySelector('[data-output=\"roi\"]').textContent) + "</strong></li>" +
      "<li><span>Total Cost</span><strong>" + escapeHtml(root.querySelector('[data-output=\"totalCost\"]').textContent) + "</strong></li>" +
      "<li><span>FBM Profit</span><strong>" + escapeHtml(root.querySelector('[data-output=\"fbmProfit\"]').textContent) + "</strong></li>" +
      "</ul>" +
      "</body></html>"
    );
    exportWindow.document.close();
    exportWindow.focus();
    setTimeout(function () {
      exportWindow.print();
    }, 250);
  }

  root.querySelectorAll("input, select").forEach(function (input) {
    var eventName = input.type === "checkbox" || input.tagName === "SELECT" ? "change" : "input";
    input.addEventListener(eventName, function () {
      if (["weight", "cogs", "fbmPickPack", "fbmOutbound", "fbmStorage", "fbmOther"].indexOf(input.getAttribute("data-field")) !== -1) {
        input.dataset.userEdited = "true";
      }
      if (input.getAttribute("data-field") === "asin") return;
      scheduleCalculate();
    });
  });

  root.querySelectorAll('[data-action="lookup"]').forEach(function (button) {
    button.addEventListener("click", lookupAsin);
  });

  root.querySelectorAll('[data-action="pdf"]').forEach(function (button) {
    button.addEventListener("click", exportPdf);
  });

  root.querySelectorAll('[data-action="reset"]').forEach(function (button) {
    button.addEventListener("click", resetFields);
  });

  resetOutputs();
  resetFields();
})();
</script>
"""


def render_profit_calculator_host_page(*, app_src: str, user: Optional[dict] = None) -> str:
    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Profit Calculator<span class="highlight">.</span></h1>
        <p class="page-copy">This stays in the server-rendered admin app, but the interactive calculator runs in an isolated first-party frame so its CSS,
        print flow, and runtime logic cannot break the rest of the HTML admin experience.</p>
      </section>
      <div class="card plan-card">
        <h2>Runtime isolation <small>· keep HTML, avoid shared-shell collisions</small></h2>
        <p style="margin:0;color:rgba(43,54,68,0.82);font-size:15px;line-height:1.55;">
          The calculator uses a dedicated runtime page plus same-origin API proxying. That keeps it compatible with the existing FastAPI HTML stack without
          turning the shared admin shell into a pasted widget host.
        </p>
        <div class="strip" style="margin-top:16px;">
          <div class="strip-info">Open the full calculator directly if you need a clean print target or a shareable internal link.</div>
          <div class="strip-actions">
            <a class="btn secondary" href="{_esc(app_src)}" target="_blank" rel="noopener">Open standalone</a>
          </div>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:hidden;">
        <iframe
          src="{_esc(app_src)}"
          title="Amazon Profit Calculator"
          loading="lazy"
          style="width:100%;min-height:1760px;border:0;display:block;background:#f3eee5;"
        ></iframe>
      </div>
    """
    return _page(
        "agent | Advertising Profit Calculator",
        body,
        user=user,
        advertising_section="advertising_profit_calculator",
    )


def render_profit_calculator_app_page(*, api_base: str, bulk_app_src: str = "/amazon-bulk-profitability/runtime") -> str:
    category_options = "".join(
        f'<option value="{_esc(key)}">{_esc(label)}</option>'
        for key, label in _CATEGORY_OPTIONS
    )
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Amazon Profit Calculator</title>
    {_APP_STYLES}
  </head>
  <body>
    <div class="apc-calculator" data-apc-root data-api-base="{_esc(api_base)}">
      <div class="apc-header">
        <div class="apc-header-copy">
          <span class="apc-eyebrow">Profitability Planner</span>
          <h1>Amazon profit calculator.</h1>
          <p>Auto-fill the product from an ASIN or model it manually. The calculator uses the backend profitability engine and keeps the interface isolated from the shared admin shell.</p>
        </div>
        <div class="apc-actions">
          <a class="apc-button-link apc-button-secondary" href="{_esc(bulk_app_src)}" target="_blank" rel="noopener">Bulk Planner</a>
          <button class="apc-button-secondary" type="button" data-action="reset">Reset</button>
          <button type="button" data-action="pdf">Download PDF</button>
        </div>
      </div>

      <div class="apc-grid">
        <div class="apc-results-stack">
          <section class="apc-section">
            <div class="apc-section-head">
              <div>
                <h2>ASIN lookup</h2>
                <p>Use the live backend to pull product details and auto-fill the model.</p>
              </div>
              <div class="apc-tag">API</div>
            </div>

            <div class="apc-row apc-row-2">
              <div class="apc-field">
                <label>ASIN</label>
                <div class="apc-inline-action">
                  <input type="text" data-field="asin" placeholder="B08N5WRWNW">
                  <button class="apc-button-secondary" type="button" data-action="lookup">Lookup ASIN</button>
                </div>
              </div>
              <div class="apc-meta-card">
                <span>Brand</span>
                <strong data-output="lookupBrand">-</strong>
                <p data-output="lookupAsin">ASIN not loaded</p>
              </div>
            </div>

            <div class="apc-row apc-row-2">
              <div class="apc-meta-card">
                <div class="apc-thumb" data-output-bg="lookupImage"></div>
              </div>
              <div class="apc-meta-card">
                <span>Lookup Title</span>
                <strong data-output="lookupTitle">No ASIN loaded</strong>
                <p data-output="lookupMeta">Use the live backend to pull product details.</p>
              </div>
            </div>

            <div class="apc-status" data-output="apiStatus">Enter product details or use ASIN lookup to calculate.</div>
            <div class="apc-helper-card">
              <div class="apc-helper-copy">
                <strong>Running multiple ASINs?</strong>
                <p>Open the bulk planner to paste or upload a list of ASINs and export a CSV with profitability outputs.</p>
              </div>
              <a class="apc-button-link" href="{_esc(bulk_app_src)}" target="_blank" rel="noopener">Open Bulk Planner</a>
            </div>
          </section>

          <section class="apc-section">
            <div class="apc-section-head">
              <div>
                <h2>Product setup</h2>
                <p>Capture the item inputs Amazon uses to classify the offer and calculate fees.</p>
              </div>
              <div class="apc-tag">Product</div>
            </div>

            <div class="apc-row apc-row-3">
              <div class="apc-field">
                <label>Category</label>
                <select data-field="category">{category_options}</select>
              </div>
              <div class="apc-field">
                <label>Price</label>
                <input type="number" step="0.01" data-field="price" value="0">
              </div>
              <div class="apc-field">
                <label>Buyer shipping</label>
                <input type="number" step="0.01" data-field="shipping" value="0">
              </div>
            </div>

            <div class="apc-row apc-row-2">
              <div class="apc-field">
                <label>Weight (lb)</label>
                <input type="number" step="0.01" data-field="weight" value="0">
              </div>
              <div class="apc-field">
                <label class="apc-check-label">
                  <input type="checkbox" data-field="isApparel">
                  Apparel sizing rules
                </label>
              </div>
            </div>

            <div class="apc-row apc-row-3">
              <div class="apc-field">
                <label>Length (in)</label>
                <input type="number" step="0.01" data-field="length" value="0">
              </div>
              <div class="apc-field">
                <label>Width (in)</label>
                <input type="number" step="0.01" data-field="width" value="0">
              </div>
              <div class="apc-field">
                <label>Height (in)</label>
                <input type="number" step="0.01" data-field="height" value="0">
              </div>
            </div>

            <div class="apc-mini-grid" style="margin-top:16px;">
              <div class="apc-mini-card">
                <span>Volume</span>
                <strong data-output="volume">0.00 cu ft</strong>
                <small>Auto-derived from package dimensions.</small>
              </div>
              <div class="apc-mini-card">
                <span>Shipping Weight</span>
                <strong data-output="shippingWeight">0.00 lb</strong>
                <small>Uses entered weight or dimensional fallback.</small>
              </div>
              <div class="apc-mini-card">
                <span>Size Tier</span>
                <strong data-output="sizeTier">Unknown</strong>
                <small>Returned by the estimate engine.</small>
              </div>
              <div class="apc-mini-card">
                <span>Revenue</span>
                <strong data-output="revenue">$0.00</strong>
                <small>Price plus buyer shipping.</small>
              </div>
            </div>
          </section>

          <section class="apc-section">
            <div class="apc-section-head">
              <div>
                <h2>Fees and costs</h2>
                <p>Model the Amazon cost stack, business costs, and FBM alternative.</p>
              </div>
              <div class="apc-tag">Costs</div>
            </div>

            <div class="apc-row apc-row-3">
              <div class="apc-field">
                <label>Months stored</label>
                <input type="number" step="1" data-field="monthsStored" value="1">
              </div>
              <div class="apc-field">
                <label>Season</label>
                <select data-field="season">
                  <option value="offpeak">Jan-Sep</option>
                  <option value="peak">Oct-Dec</option>
                </select>
              </div>
              <div class="apc-field">
                <label>Inbound fee</label>
                <input type="number" step="0.01" data-field="inbound" value="0">
              </div>
            </div>

            <div class="apc-row apc-row-3">
              <div class="apc-field">
                <label>Other Amazon fees</label>
                <input type="number" step="0.01" data-field="otherAmazonFees" value="0">
              </div>
              <div class="apc-field">
                <label>COGS</label>
                <input type="number" step="0.01" data-field="cogs" value="0">
              </div>
              <div class="apc-field">
                <label>Prep cost</label>
                <input type="number" step="0.01" data-field="prepCost" value="0">
              </div>
            </div>

            <div class="apc-row apc-row-3">
              <div class="apc-field">
                <label>Misc cost</label>
                <input type="number" step="0.01" data-field="miscCost" value="0">
              </div>
              <div class="apc-field">
                <label>Marketing %</label>
                <input type="number" step="0.01" data-field="marketingPct" value="0">
              </div>
              <div class="apc-field">
                <label>Agency %</label>
                <input type="number" step="0.01" data-field="agencyPct" value="0">
              </div>
            </div>

            <div class="apc-row apc-row-2" style="margin-top:18px;">
              <div>
                <h3>FBM scenario</h3>
                <div class="apc-row apc-row-2">
                  <div class="apc-field">
                    <label>Pick + pack</label>
                    <input type="number" step="0.01" data-field="fbmPickPack" value="0">
                  </div>
                  <div class="apc-field">
                    <label>Outbound</label>
                    <input type="number" step="0.01" data-field="fbmOutbound" value="0">
                  </div>
                  <div class="apc-field">
                    <label>Storage</label>
                    <input type="number" step="0.01" data-field="fbmStorage" value="0">
                  </div>
                  <div class="apc-field">
                    <label>Other</label>
                    <input type="number" step="0.01" data-field="fbmOther" value="0">
                  </div>
                </div>
              </div>
              <div class="apc-mini-grid" style="grid-template-columns:repeat(2,minmax(0,1fr));align-content:start;">
                <div class="apc-mini-card">
                  <span>Referral Fee</span>
                  <strong data-output="referralFee">$0.00</strong>
                  <small>Returned by Amazon rate logic.</small>
                </div>
                <div class="apc-mini-card">
                  <span>FBA Fee</span>
                  <strong data-output="fbaFee">$0.00</strong>
                  <small>Fulfillment fee from the backend model.</small>
                </div>
                <div class="apc-mini-card">
                  <span>Storage Total</span>
                  <strong data-output="storageTotal">$0.00</strong>
                  <small>Season and duration aware.</small>
                </div>
                <div class="apc-mini-card">
                  <span>Amazon Fees</span>
                  <strong data-output="amazonFees">$0.00</strong>
                  <small>Total marketplace cost stack.</small>
                </div>
              </div>
            </div>
          </section>
        </div>

        <section class="apc-section">
          <div class="apc-section-head">
            <div>
              <h2>Results</h2>
              <p>Primary economics for the FBA model plus a direct FBM comparison.</p>
            </div>
            <div class="apc-tag">Output</div>
          </div>

          <div class="apc-kpis">
            <div class="apc-kpi" data-kpi="profit">
              <span>Net Profit</span>
              <strong data-output="profit">$0.00</strong>
              <small>Per-unit take-home after fees and costs.</small>
            </div>
            <div class="apc-kpi">
              <span>Net Margin</span>
              <strong data-output="margin">0.00%</strong>
              <small>Profit rate against revenue.</small>
            </div>
            <div class="apc-kpi">
              <span>ROI on COGS</span>
              <strong data-output="roi">0.00%</strong>
              <small>Return relative to cost of goods sold.</small>
            </div>
          </div>

          <div class="apc-results-stack" style="margin-top:18px;">
            <div class="apc-section" style="padding:18px;">
              <h3>FBA summary</h3>
              <ul class="apc-report-list">
                <li><span>Revenue</span><strong data-output="revenue">$0.00</strong></li>
                <li><span>Total Cost</span><strong data-output="totalCost">$0.00</strong></li>
                <li><span>Marketing Cost</span><strong data-output="marketingCost">$0.00</strong></li>
                <li><span>Agency Cost</span><strong data-output="agencyCost">$0.00</strong></li>
                <li><span>FBA Fulfillment</span><strong data-output="fbaFee">$0.00</strong></li>
              </ul>
            </div>

            <div class="apc-section" style="padding:18px;">
              <h3>FBM summary</h3>
              <ul class="apc-report-list">
                <li><span>FBM Cost</span><strong data-output="fbmCost">$0.00</strong></li>
                <li><span>FBM Profit</span><strong data-output="fbmProfit">$0.00</strong></li>
                <li><span>FBM Margin</span><strong data-output="fbmMargin">0.00%</strong></li>
              </ul>
            </div>

            <div class="apc-section" style="padding:18px;">
              <h3>FBA vs FBM</h3>
              <div class="apc-compare">
                <div class="apc-compare-head">
                  <span>Metric</span>
                  <span>FBA</span>
                  <span>FBM</span>
                  <span>Delta</span>
                </div>

                <div class="apc-compare-row">
                  <div class="apc-compare-label">
                    <strong>Cost per unit</strong>
                    <span>Total modeled landed cost.</span>
                  </div>
                  <div class="apc-compare-pill"><strong data-output="compareCostFba">$0.00</strong></div>
                  <div class="apc-compare-pill"><strong data-output="compareCostFbm">$0.00</strong></div>
                  <div class="apc-compare-delta" data-output="compareCostDelta">0.00%</div>
                </div>

                <div class="apc-compare-row">
                  <div class="apc-compare-label">
                    <strong>Net profit</strong>
                    <span>Per-unit take-home after costs.</span>
                  </div>
                  <div class="apc-compare-pill"><strong data-output="compareProfitFba">$0.00</strong></div>
                  <div class="apc-compare-pill"><strong data-output="compareProfitFbm">$0.00</strong></div>
                  <div class="apc-compare-delta" data-output="compareProfitDelta">0.00%</div>
                </div>

                <div class="apc-compare-row">
                  <div class="apc-compare-label">
                    <strong>Net margin</strong>
                    <span>Profit rate against revenue.</span>
                  </div>
                  <div class="apc-compare-pill"><strong data-output="compareMarginFba">0.00%</strong></div>
                  <div class="apc-compare-pill"><strong data-output="compareMarginFbm">0.00%</strong></div>
                  <div class="apc-compare-delta" data-output="compareMarginDelta">0.00%</div>
                </div>
              </div>
            </div>
          </div>

          <p class="apc-note">This route stays in the HTML FastAPI app. The calculator runtime is isolated so shared admin styles, print flows, and scripts do not bleed into other operator surfaces.</p>
        </section>
      </div>
    </div>
    {_APP_SCRIPT}
  </body>
</html>"""
