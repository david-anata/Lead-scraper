"""Advertising > Bulk ASIN profitability pages."""

from __future__ import annotations

import html
from typing import Optional

from sales_support_agent.services.advertising.audit_page import _page


def _esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


_APP_STYLES = """
<style>
  @import url("https://fonts.googleapis.com/css2?family=Montserrat:wght@700;800&family=Roboto:wght@300;400;500&display=swap");

  html,
  body {
    margin: 0;
    background: #f9f7f3;
    color: #2b3644;
    font-family: "Roboto", sans-serif;
  }

  body { padding: 18px; }

  .abu-dashboard {
    --abu-dark: #2b3644;
    --abu-dark-alt: #33445c;
    --abu-light: #85bbda;
    --abu-bg: #f9f7f3;
    --abu-card: #ffffff;
    --abu-result: #e7eef5;
    --abu-border: rgba(43, 54, 68, 0.12);
    --abu-shadow: rgba(43, 54, 68, 0.1);
    --abu-danger: #9a5a4e;
    --abu-success: #5c8a6e;
    background: linear-gradient(180deg, rgba(133, 187, 218, 0.12), rgba(249, 247, 243, 0.9)), var(--abu-bg);
    border: 1px solid var(--abu-border);
    border-radius: 30px;
    box-shadow: 0 18px 40px var(--abu-shadow);
    color: var(--abu-dark);
    font-family: "Roboto", sans-serif;
  }

  .abu-dashboard,
  .abu-dashboard * { box-sizing: border-box; }

  .abu-shell {
    padding: 28px;
    overflow: hidden;
  }

  .abu-header {
    display: grid;
    gap: 18px;
    grid-template-columns: minmax(0, 1.3fr) auto;
    align-items: end;
    margin-bottom: 22px;
  }

  .abu-eyebrow {
    display: inline-block;
    margin-bottom: 14px;
    padding: 10px 16px;
    border-radius: 8px;
    background: var(--abu-dark);
    color: #fff;
    font-family: "Montserrat", sans-serif;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .abu-header h1 {
    margin: 0;
    font-family: "Montserrat", sans-serif;
    font-size: clamp(30px, 4vw, 52px);
    font-weight: 800;
    letter-spacing: -0.05em;
    line-height: 0.95;
  }

  .abu-header p {
    margin: 12px 0 0;
    max-width: 820px;
    color: rgba(43, 54, 68, 0.76);
    font-size: 16px;
    line-height: 1.55;
  }

  .abu-accent { color: var(--abu-light); }

  .abu-actions {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    justify-content: flex-end;
    align-items: center;
    padding: 8px;
    border: 1px solid rgba(255, 255, 255, 0.34);
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.18);
    backdrop-filter: blur(10px);
  }

  .abu-button {
    appearance: none;
    border: 0;
    border-radius: 999px;
    cursor: pointer;
    font-family: "Montserrat", sans-serif;
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 0.06em;
    min-height: 48px;
    padding: 12px 20px;
    text-transform: uppercase;
    transition: transform 0.2s ease, opacity 0.2s ease, background 0.2s ease, box-shadow 0.2s ease;
    box-shadow: 0 10px 22px rgba(43, 54, 68, 0.12);
  }

  .abu-button:hover,
  .abu-button:focus { transform: translateY(-1px); outline: none; }

  .abu-button-primary { background: var(--abu-light); color: #fff; }
  .abu-button-primary:hover,
  .abu-button-primary:focus { background: var(--abu-dark) !important; color: #fff !important; }

  .abu-button-secondary {
    background: transparent;
    border: 1px solid var(--abu-border);
    color: var(--abu-dark);
    box-shadow: none;
  }

  .abu-button-secondary:hover,
  .abu-button-secondary:focus {
    background: rgba(133, 187, 218, 0.12) !important;
    border-color: var(--abu-light) !important;
    color: var(--abu-dark) !important;
  }

  .abu-button:disabled {
    opacity: 0.45;
    cursor: not-allowed;
    transform: none !important;
  }

  .abu-layout {
    display: grid;
    gap: 20px;
    min-width: 0;
  }

  .abu-panel,
  .abu-results {
    min-width: 0;
    border: 1px solid var(--abu-border);
    border-radius: 28px;
    padding: 18px;
    box-shadow: 0 14px 32px rgba(43, 54, 68, 0.06);
  }

  .abu-panel {
    background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(249,247,243,0.92)), var(--abu-card);
  }

  .abu-results {
    background: linear-gradient(180deg, rgba(233, 239, 246, 0.98), rgba(226, 233, 241, 0.94)), var(--abu-result);
  }

  .abu-section {
    min-width: 0;
    border: 1px solid rgba(43, 54, 68, 0.08);
    border-radius: 24px;
    background: linear-gradient(180deg, rgba(255,255,255,0.88), rgba(250, 248, 243, 0.78));
    padding: 18px;
    margin-bottom: 16px;
  }

  .abu-section:last-child { margin-bottom: 0; }

  .abu-section-head {
    display: flex;
    justify-content: space-between;
    align-items: end;
    gap: 16px;
    margin-bottom: 14px;
  }

  .abu-section-head h2 {
    margin: 0 0 6px;
    font-family: "Montserrat", sans-serif;
    font-size: 24px;
    font-weight: 700;
    letter-spacing: -0.04em;
  }

  .abu-section-head p {
    margin: 0;
    color: rgba(43, 54, 68, 0.72);
    font-size: 14px;
    line-height: 1.5;
    max-width: 700px;
  }

  .abu-tag {
    color: rgba(43, 54, 68, 0.58);
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    white-space: nowrap;
  }

  .abu-grid {
    display: grid;
    gap: 14px;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    min-width: 0;
  }

  .abu-grid-2 {
    display: grid;
    gap: 14px;
    grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr);
    min-width: 0;
  }

  .abu-grid > *,
  .abu-grid-2 > *,
  .abu-kpis > * {
    min-width: 0;
  }

  .abu-field label {
    display: block;
    margin-bottom: 8px;
    color: rgba(43, 54, 68, 0.64);
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .abu-field input[type="number"],
  .abu-field input[type="file"],
  .abu-field textarea,
  .abu-field select {
    width: 100%;
    appearance: none;
    min-height: 48px;
    padding: 11px 14px;
    border: 1px solid rgba(43, 54, 68, 0.14);
    border-bottom: 2px solid rgba(43, 54, 68, 0.18);
    border-radius: 12px;
    background: rgba(255, 255, 255, 0.92);
    color: var(--abu-dark);
    font-family: "Roboto", sans-serif;
    font-size: 16px;
    line-height: 1.2;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.6);
    transition: border-color 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
  }

  .abu-field input[type="file"] {
    display: none;
  }

  .abu-file-picker {
    display: flex;
    align-items: center;
    gap: 12px;
    min-height: 56px;
    padding: 10px 12px;
    border: 1px solid rgba(43, 54, 68, 0.14);
    border-bottom-width: 2px;
    border-radius: 12px;
    background: rgba(255, 255, 255, 0.92);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.6);
  }

  .abu-file-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 40px;
    padding: 10px 16px;
    border-radius: 999px;
    background: var(--abu-dark);
    color: #fff;
    border: 0;
    cursor: pointer;
    font-family: "Montserrat", sans-serif;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    white-space: nowrap;
    transition: transform 0.2s ease, background 0.2s ease;
  }

  .abu-file-button:hover,
  .abu-file-button:focus {
    background: var(--abu-light);
    transform: translateY(-1px);
  }

  .abu-file-name {
    color: rgba(43, 54, 68, 0.7);
    font-size: 14px;
    line-height: 1.4;
  }

  .abu-field textarea {
    min-height: 220px;
    resize: vertical;
  }

  .abu-field select {
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14'%3E%3Cpath d='M3 5.25 7 9l4-3.75' fill='none' stroke='%2333445c' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
    background-position: right 14px center;
    background-repeat: no-repeat;
    background-size: 14px 14px;
    padding-right: 40px;
  }

  .abu-field input:focus,
  .abu-field textarea:focus,
  .abu-field select:focus {
    border-color: var(--abu-light);
    box-shadow: 0 0 0 3px rgba(133, 187, 218, 0.16);
    outline: none;
    background: #fff;
  }

  .abu-status {
    margin-top: 14px;
    padding: 12px 14px;
    border-radius: 14px;
    border: 1px solid rgba(43, 54, 68, 0.08);
    background: rgba(255,255,255,0.64);
    color: rgba(43, 54, 68, 0.78);
    font-size: 14px;
    line-height: 1.5;
  }

  .abu-status.is-success { border-color: rgba(92,138,110,0.32); background: rgba(92,138,110,0.12); color: var(--abu-success); }
  .abu-status.is-danger { border-color: rgba(154,90,78,0.32); background: rgba(154,90,78,0.1); color: var(--abu-danger); }

  .abu-kpis {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    margin-bottom: 16px;
    min-width: 0;
  }

  .abu-results-actions {
    display: flex;
    justify-content: flex-end;
    margin-bottom: 16px;
  }

  .abu-kpi {
    padding: 14px 16px;
    border-radius: 18px;
    border: 1px solid rgba(43, 54, 68, 0.08);
    background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(247, 249, 252, 0.82));
  }

  .abu-kpi span {
    display: block;
    color: rgba(43, 54, 68, 0.62);
    font-family: "Montserrat", sans-serif;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .abu-kpi strong {
    display: block;
    margin-top: 8px;
    color: var(--abu-dark);
    font-family: "Montserrat", sans-serif;
    font-size: 28px;
    font-weight: 800;
    letter-spacing: -0.05em;
    line-height: 0.95;
  }

  .abu-table-wrap {
    max-width: 100%;
    min-width: 0;
    overflow: auto;
    border-radius: 18px;
    border: 1px solid rgba(43, 54, 68, 0.08);
    background: rgba(255,255,255,0.78);
  }

  .abu-table {
    width: 100%;
    min-width: 980px;
    border-collapse: collapse;
  }

  .abu-table th,
  .abu-table td {
    padding: 12px 14px;
    border-bottom: 1px solid rgba(43, 54, 68, 0.08);
    text-align: left;
    color: rgba(43, 54, 68, 0.82);
    font-size: 13px;
    line-height: 1.4;
    vertical-align: top;
  }

  .abu-table th {
    position: sticky;
    top: 0;
    background: #eef3f8;
    color: rgba(43, 54, 68, 0.62);
    font-family: "Montserrat", sans-serif;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .abu-table td strong {
    color: var(--abu-dark);
    font-family: "Montserrat", sans-serif;
    font-size: 13px;
    font-weight: 700;
  }

  .abu-note {
    margin-top: 14px;
    color: rgba(43, 54, 68, 0.66);
    font-size: 13px;
    line-height: 1.5;
  }

  @media (max-width: 1180px) {
    .abu-header,
    .abu-grid,
    .abu-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .abu-grid-2 { grid-template-columns: 1fr; }
    .abu-actions { justify-content: flex-start; }
  }

  @media (max-width: 720px) {
    .abu-shell { padding: 18px; }
    .abu-header,
    .abu-grid,
    .abu-grid-2,
    .abu-kpis { grid-template-columns: 1fr; }
    .abu-section-head { display: block; }
    .abu-actions { width: 100%; border-radius: 20px; }
    .abu-file-picker { align-items: stretch; flex-direction: column; }
    .abu-results-actions { justify-content: stretch; }
    .abu-results-actions .abu-button { width: 100%; }
  }
</style>
"""


_APP_SCRIPT = """
<script>
(function () {
  var root = document.querySelector("[data-amazon-bulk-upload]");
  if (!root) return;

  var csvRows = [];
  var activeBatch = false;

  function field(name) {
    return root.querySelector('[data-field="' + name + '"]');
  }

  function value(name) {
    var input = field(name);
    return input ? parseFloat(input.value) || 0 : 0;
  }

  function text(name) {
    var input = field(name);
    return input ? input.value : "";
  }

  function syncFileName() {
    var fileInput = field("asinFile");
    var label = root.querySelector('[data-output="fileName"]');
    if (!label) return;
    var fileName = fileInput && fileInput.files && fileInput.files[0] ? fileInput.files[0].name : "No file selected";
    label.textContent = fileName;
  }

  function setOutput(name, content) {
    root.querySelectorAll('[data-output="' + name + '"]').forEach(function (node) {
      node.textContent = content;
    });
  }

  function setStatus(message, tone) {
    var node = root.querySelector('[data-output="status"]');
    if (!node) return;
    node.textContent = message;
    node.className = "abu-status" + (tone ? " is-" + tone : "");
  }

  function apiBase() {
    return String(root.getAttribute("data-api-base") || "").replace(/\\/+$/, "");
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function trimTitle(value, maxLength) {
    var textValue = String(value || "");
    if (textValue.length <= maxLength) return textValue;
    return textValue.slice(0, Math.max(maxLength - 3, 0)).trim() + "...";
  }

  function fileToText(file) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onload = function () { resolve(String(reader.result || "")); };
      reader.onerror = reject;
      reader.readAsText(file);
    });
  }

  function extractAsins(raw) {
    var matches = String(raw || "").toUpperCase().match(/[A-Z0-9]{10}/g) || [];
    var seen = {};
    return matches.filter(function (asin) {
      if (seen[asin]) return false;
      seen[asin] = true;
      return true;
    });
  }

  async function fetchJson(url, options) {
    var response = await fetch(url, options || {});
    if (!response.ok) {
      var message = response.status + " " + response.statusText;
      try {
        var payload = await response.json();
        if (payload && payload.detail) message = payload.detail;
      } catch (error) {}
      throw new Error(message);
    }
    return response.json();
  }

  function buildAssumptions() {
    return {
      buyer_shipping: value("buyerShipping"),
      months_stored: value("monthsStored"),
      season: text("season") || "offpeak",
      inbound_fee: value("inboundFee"),
      other_amazon_fees: value("otherAmazonFees"),
      prep_cost: value("prepCost"),
      misc_cost: value("miscCost"),
      marketing_pct: value("marketingPct"),
      agency_pct: value("agencyPct"),
      fbm_pick_pack: value("fbmPickPack"),
      fbm_outbound: value("fbmOutbound"),
      fbm_storage: value("fbmStorage"),
      fbm_other: 0,
      cogs: 0
    };
  }

  function setButtonsState() {
    var runButton = root.querySelector('[data-action="run"]');
    var downloadButton = root.querySelector('[data-action="download"]');
    if (runButton) runButton.disabled = activeBatch;
    if (downloadButton) downloadButton.disabled = activeBatch || !csvRows.length;
  }

  function renderRows(rows) {
    var tbody = root.querySelector('[data-output="rows"]');
    if (!tbody) return;
    if (!rows.length) {
      tbody.innerHTML = "<tr><td colspan='11'>No batch has been run yet.</td></tr>";
      return;
    }
    tbody.innerHTML = rows.slice(0, 25).map(function (row) {
      return "<tr>" +
        "<td><strong>" + escapeHtml(row.asin) + "</strong></td>" +
        "<td>" + escapeHtml(trimTitle(row.title, 56)) + "</td>" +
        "<td>" + escapeHtml(row.brand) + "</td>" +
        "<td>" + escapeHtml(row.category_label) + "</td>" +
        "<td>" + escapeHtml(row.price) + "</td>" +
        "<td>" + escapeHtml(row.size_tier) + "</td>" +
        "<td>" + escapeHtml(row.fba_fee) + "</td>" +
        "<td>" + escapeHtml(row.amazon_fees_total) + "</td>" +
        "<td>" + escapeHtml(row.profit_before_cogs) + "</td>" +
        "<td>" + escapeHtml(row.max_cogs_at_breakeven) + "</td>" +
        "<td>" + escapeHtml(row.status) + "</td>" +
      "</tr>";
    }).join("");
  }

  function csvEscape(value) {
    var stringValue = String(value == null ? "" : value);
    if (/[",\\n]/.test(stringValue)) {
      return '"' + stringValue.replace(/"/g, '""') + '"';
    }
    return stringValue;
  }

  function downloadCsv() {
    if (!csvRows.length) return;
    var headers = Object.keys(csvRows[0]);
    var body = csvRows.map(function (row) {
      return headers.map(function (key) { return csvEscape(row[key]); }).join(",");
    });
    var csv = headers.join(",") + "\\n" + body.join("\\n");
    var blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    var url = URL.createObjectURL(blob);
    var link = document.createElement("a");
    link.href = url;
    link.download = "amazon-bulk-profitability-output.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  async function loadSourceText() {
    var textValue = text("asinText").trim();
    var fileInput = field("asinFile");
    if (fileInput && fileInput.files && fileInput.files[0]) {
      var fileText = await fileToText(fileInput.files[0]);
      return textValue ? textValue + "\\n" + fileText : fileText;
    }
    return textValue;
  }

  async function processAsin(asin, assumptions) {
    var catalog = await fetchJson(apiBase() + "/catalog/" + encodeURIComponent(asin));
    var payload = {
      category_key: catalog.category_key || "everythingElse",
      price: Number(catalog.price || 0),
      buyer_shipping: assumptions.buyer_shipping,
      length: Number((catalog.dimensions && catalog.dimensions.length) || 0),
      width: Number((catalog.dimensions && catalog.dimensions.width) || 0),
      height: Number((catalog.dimensions && catalog.dimensions.height) || 0),
      weight_lb: Number(catalog.weight_lb || 0),
      is_apparel: catalog.category_key === "clothing",
      months_stored: assumptions.months_stored,
      season: assumptions.season,
      inbound_fee: assumptions.inbound_fee,
      other_amazon_fees: assumptions.other_amazon_fees,
      cogs: 0,
      prep_cost: assumptions.prep_cost,
      misc_cost: assumptions.misc_cost,
      marketing_pct: assumptions.marketing_pct,
      agency_pct: assumptions.agency_pct,
      fbm_pick_pack: assumptions.fbm_pick_pack,
      fbm_outbound: assumptions.fbm_outbound,
      fbm_storage: assumptions.fbm_storage,
      fbm_other: assumptions.fbm_other
    };
    var estimate = await fetchJson(apiBase() + "/profitability/estimate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

    return {
      asin: asin,
      title: catalog.title || "",
      brand: catalog.brand || "",
      category_key: catalog.category_key || "",
      category_label: catalog.category_label || "",
      price: Number(catalog.price || 0).toFixed(2),
      buyer_shipping: assumptions.buyer_shipping.toFixed(2),
      length_in: payload.length.toFixed(2),
      width_in: payload.width.toFixed(2),
      height_in: payload.height.toFixed(2),
      weight_lb: Number(estimate.shipping_weight_lb || 0).toFixed(2),
      size_tier: estimate.size_tier || "",
      total_revenue: Number(estimate.total_revenue || 0).toFixed(2),
      referral_fee: Number(estimate.referral_fee || 0).toFixed(2),
      fba_fee: Number(estimate.fba_fee || 0).toFixed(2),
      storage_total: Number(estimate.storage_total || 0).toFixed(2),
      amazon_fees_total: Number(estimate.amazon_fees_total || 0).toFixed(2),
      marketing_cost: Number(estimate.marketing_cost || 0).toFixed(2),
      agency_cost: Number(estimate.agency_cost || 0).toFixed(2),
      prep_cost: assumptions.prep_cost.toFixed(2),
      misc_cost: assumptions.misc_cost.toFixed(2),
      cogs: "",
      profit_before_cogs: Number(estimate.net_profit || 0).toFixed(2),
      max_cogs_at_breakeven: Number(estimate.net_profit || 0).toFixed(2),
      net_margin_before_cogs_pct: Number(estimate.net_margin_pct || 0).toFixed(2),
      fbm_cost_no_cogs: Number(estimate.fbm_cost || 0).toFixed(2),
      fbm_profit_before_cogs: Number(estimate.fbm_profit || 0).toFixed(2),
      fbm_margin_before_cogs_pct: Number(estimate.fbm_margin_pct || 0).toFixed(2),
      status: "Processed"
    };
  }

  async function runBatch() {
    if (activeBatch) return;
    if (!apiBase()) {
      setStatus("Backend URL is missing from the widget configuration.", "danger");
      return;
    }

    activeBatch = true;
    csvRows = [];
    renderRows([]);
    setButtonsState();

    try {
      var sourceText = await loadSourceText();
      var asins = extractAsins(sourceText);
      setOutput("asinCount", asins.length);
      setOutput("processedCount", "0");
      setOutput("errorCount", "0");
      setOutput("csvReady", "No");

      if (!asins.length) {
        setStatus("No valid ASINs were found in the pasted text or uploaded file.", "danger");
        activeBatch = false;
        setButtonsState();
        return;
      }

      var assumptions = buildAssumptions();
      var processed = 0;
      var errors = 0;

      setStatus("Running " + asins.length + " ASINs through lookup and estimate...", "");

      for (var index = 0; index < asins.length; index += 1) {
        var asin = asins[index];
        try {
          var row = await processAsin(asin, assumptions);
          csvRows.push(row);
        } catch (error) {
          errors += 1;
          csvRows.push({
            asin: asin,
            title: "",
            brand: "",
            category_key: "",
            category_label: "",
            price: "",
            buyer_shipping: assumptions.buyer_shipping.toFixed(2),
            length_in: "",
            width_in: "",
            height_in: "",
            weight_lb: "",
            size_tier: "",
            total_revenue: "",
            referral_fee: "",
            fba_fee: "",
            storage_total: "",
            amazon_fees_total: "",
            marketing_cost: "",
            agency_cost: "",
            prep_cost: assumptions.prep_cost.toFixed(2),
            misc_cost: assumptions.misc_cost.toFixed(2),
            cogs: "",
            profit_before_cogs: "",
            max_cogs_at_breakeven: "",
            net_margin_before_cogs_pct: "",
            fbm_cost_no_cogs: "",
            fbm_profit_before_cogs: "",
            fbm_margin_before_cogs_pct: "",
            status: "Error: " + error.message
          });
        }

        processed += 1;
        setOutput("processedCount", processed);
        setOutput("errorCount", errors);
        renderRows(csvRows);
        setStatus("Processed " + processed + " of " + asins.length + " ASINs.", errors ? "danger" : "");
      }

      setOutput("csvReady", "Yes");
      setStatus("Batch complete. " + processed + " rows processed" + (errors ? " with " + errors + " errors." : "."), errors ? "danger" : "success");
    } catch (error) {
      setStatus("Batch failed: " + error.message, "danger");
    }

    activeBatch = false;
    setButtonsState();
  }

  function resetForm() {
    root.querySelectorAll("input[type='number']").forEach(function (input) {
      if (input.hasAttribute("value")) input.value = input.getAttribute("value");
      else input.value = "";
    });
    root.querySelectorAll("select").forEach(function (select) {
      select.selectedIndex = 0;
    });
    root.querySelectorAll("textarea").forEach(function (area) {
      area.value = "";
    });
    var fileInput = field("asinFile");
    if (fileInput) fileInput.value = "";
    syncFileName();
    csvRows = [];
    setOutput("asinCount", "0");
    setOutput("processedCount", "0");
    setOutput("errorCount", "0");
    setOutput("csvReady", "No");
    renderRows([]);
    setStatus("Ready. Add ASINs, confirm the shared assumptions, then run the batch.", "");
    setButtonsState();
  }

  root.querySelectorAll('[data-action="run"]').forEach(function (button) {
    button.addEventListener("click", runBatch);
  });

  root.querySelectorAll('[data-action="download"]').forEach(function (button) {
    button.addEventListener("click", downloadCsv);
  });

  root.querySelectorAll('[data-action="reset"]').forEach(function (button) {
    button.addEventListener("click", resetForm);
  });

  root.querySelectorAll('[data-action="pickFile"]').forEach(function (button) {
    button.addEventListener("click", function () {
      var picker = field("asinFile");
      if (picker) picker.click();
    });
  });

  var fileInput = field("asinFile");
  if (fileInput) {
    fileInput.addEventListener("change", syncFileName);
  }

  resetForm();
})();
</script>
"""


def render_bulk_profitability_host_page(*, app_src: str, user: Optional[dict] = None) -> str:
    body = f"""
      <section class="page-header">
        <span class="eyebrow">Advertising</span>
        <h1 class="page-title">Bulk Planner<span class="highlight">.</span></h1>
        <p class="page-copy">The bulk planner is hosted as a first-party isolated runtime just like the single-item calculator, so you can use it in admin and on the public site without pasted widget drift.</p>
      </section>
      <div class="card plan-card">
        <h2>Shared runtime <small>· public and admin use the same app</small></h2>
        <p style="margin:0;color:rgba(43,54,68,0.82);font-size:15px;line-height:1.55;">
          This planner accepts pasted ASINs or a small upload, runs catalog lookup plus profitability estimates, and exports a CSV for follow-up.
        </p>
        <div class="strip" style="margin-top:16px;">
          <div class="strip-info">Open the runtime directly if you want a clean public test target.</div>
          <div class="strip-actions">
            <a class="btn secondary" href="{_esc(app_src)}" target="_blank" rel="noopener">Open standalone</a>
          </div>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:hidden;">
        <iframe
          src="{_esc(app_src)}"
          title="Bulk ASIN Profitability Upload"
          loading="lazy"
          style="width:100%;min-height:1650px;border:0;display:block;background:#f9f7f3;"
        ></iframe>
      </div>
    """
    return _page(
        "agent | Advertising Bulk Planner",
        body,
        user=user,
        advertising_section="advertising_bulk_profitability",
    )


def render_bulk_profitability_app_page(*, api_base: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Bulk ASIN Profitability Upload</title>
    {_APP_STYLES}
  </head>
  <body>
    <div class="abu-dashboard" data-amazon-bulk-upload data-api-base="{_esc(api_base)}">
      <div class="abu-shell">
        <div class="abu-header">
          <div>
            <span class="abu-eyebrow">Bulk Planner</span>
            <h1>Bulk ASIN <span class="abu-accent">profitability upload</span>.</h1>
            <p>Upload or paste a list of ASINs, apply one set of shared assumptions, and export a CSV with fee and profitability outputs. `COGS` is left blank intentionally for manual follow-up.</p>
          </div>

          <div class="abu-actions">
            <button class="abu-button abu-button-secondary" type="button" data-action="reset">Reset</button>
            <button class="abu-button abu-button-primary" type="button" data-action="run">Run Batch</button>
          </div>
        </div>

        <div class="abu-layout">
          <section class="abu-panel">
            <div class="abu-section">
              <div class="abu-section-head">
                <div>
                  <h2>ASIN Intake</h2>
                  <p>Paste ASINs, URLs, or a one-column CSV. The parser extracts 10-character ASINs automatically and de-duplicates them.</p>
                </div>
                <div class="abu-tag">Input</div>
              </div>

              <div class="abu-grid-2">
                <div class="abu-field">
                  <label>Paste ASINs or CSV rows</label>
                  <textarea data-field="asinText" placeholder="B08N5WRWNW&#10;B0G2XJY5M2&#10;https://www.amazon.com/dp/B09..."></textarea>
                </div>
                <div>
                  <div class="abu-field">
                    <label>Optional CSV / TXT Upload</label>
                    <div class="abu-file-picker">
                      <button class="abu-file-button" type="button" data-action="pickFile">Choose File</button>
                      <span class="abu-file-name" data-output="fileName">No file selected</span>
                      <input id="abu-asin-file" type="file" accept=".csv,.txt" data-field="asinFile">
                    </div>
                  </div>
                  <div class="abu-field" style="margin-top: 14px;">
                    <label>Marketplace</label>
                    <select data-field="marketplace">
                      <option value="ATVPDKIKX0DER">United States</option>
                    </select>
                  </div>
                  <div class="abu-note">
                    Output CSV columns include scraped product details, fee outputs, `profit_before_cogs`, `max_cogs_at_breakeven`, and a blank `cogs` column for manual completion.
                  </div>
                </div>
              </div>
            </div>

            <div class="abu-section">
              <div class="abu-section-head">
                <div>
                  <h2>Shared Assumptions</h2>
                  <p>These inputs apply to every ASIN in the batch. They should reflect realistic planning assumptions that can be reused across the sheet.</p>
                </div>
                <div class="abu-tag">Assumptions</div>
              </div>

              <div class="abu-grid">
                <div class="abu-field">
                  <label>Buyer Shipping ($)</label>
                  <input type="number" min="0" step="0.01" value="0" data-field="buyerShipping">
                </div>
                <div class="abu-field">
                  <label>Months Stored</label>
                  <input type="number" min="0" step="0.25" value="1" data-field="monthsStored">
                </div>
                <div class="abu-field">
                  <label>Storage Season</label>
                  <select data-field="season">
                    <option value="offpeak">January to September</option>
                    <option value="peak">October to December</option>
                  </select>
                </div>
                <div class="abu-field">
                  <label>Inbound Fee ($)</label>
                  <input type="number" min="0" step="0.01" value="0" data-field="inboundFee">
                </div>
                <div class="abu-field">
                  <label>Other Amazon Fees ($)</label>
                  <input type="number" min="0" step="0.01" value="0" data-field="otherAmazonFees">
                </div>
                <div class="abu-field">
                  <label>Prep Cost ($)</label>
                  <input type="number" min="0" step="0.01" value="0" data-field="prepCost">
                </div>
                <div class="abu-field">
                  <label>Misc Cost ($)</label>
                  <input type="number" min="0" step="0.01" value="0" data-field="miscCost">
                </div>
                <div class="abu-field">
                  <label>Marketing %</label>
                  <input type="number" min="0" step="0.01" value="15" data-field="marketingPct">
                </div>
                <div class="abu-field">
                  <label>Agency Fee %</label>
                  <input type="number" min="0" step="0.01" value="10" data-field="agencyPct">
                </div>
                <div class="abu-field">
                  <label>FBM Pick / Pack ($)</label>
                  <input type="number" min="0" step="0.01" value="3.00" data-field="fbmPickPack">
                </div>
                <div class="abu-field">
                  <label>FBM Outbound ($)</label>
                  <input type="number" min="0" step="0.01" value="4.50" data-field="fbmOutbound">
                </div>
                <div class="abu-field">
                  <label>FBM Storage ($)</label>
                  <input type="number" min="0" step="0.01" value="0.15" data-field="fbmStorage">
                </div>
              </div>
            </div>

            <div class="abu-status" data-output="status">Ready. Add ASINs, confirm the shared assumptions, then run the batch.</div>
          </section>

          <section class="abu-results">
            <div class="abu-section">
              <div class="abu-section-head">
                <div>
                  <h2>Batch Results</h2>
                  <p>The preview table shows the first processed rows. Download the full enriched CSV when the batch completes.</p>
                </div>
                <div class="abu-tag">Output</div>
              </div>

              <div class="abu-kpis">
                <div class="abu-kpi">
                  <span>ASINs Parsed</span>
                  <strong data-output="asinCount">0</strong>
                </div>
                <div class="abu-kpi">
                  <span>Rows Processed</span>
                  <strong data-output="processedCount">0</strong>
                </div>
                <div class="abu-kpi">
                  <span>Rows with Errors</span>
                  <strong data-output="errorCount">0</strong>
                </div>
                <div class="abu-kpi">
                  <span>CSV Ready</span>
                  <strong data-output="csvReady">No</strong>
                </div>
              </div>

              <div class="abu-results-actions">
                <button class="abu-button abu-button-primary" type="button" data-action="download" disabled>Download CSV</button>
              </div>

              <div class="abu-table-wrap">
                <table class="abu-table">
                  <thead>
                    <tr>
                      <th>ASIN</th>
                      <th>Title</th>
                      <th>Brand</th>
                      <th>Category</th>
                      <th>Price</th>
                      <th>Size Tier</th>
                      <th>FBA Fee</th>
                      <th>Amazon Fees</th>
                      <th>Profit Before COGS</th>
                      <th>Max COGS at Breakeven</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody data-output="rows">
                    <tr>
                      <td colspan="11">No batch has been run yet.</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
    {_APP_SCRIPT}
  </body>
</html>"""
