(() => {
  "use strict";

  const state = {
    csrf: "",
    draft: null,
    changes: [],
    saving: false,
    ready: false,
    timer: null,
    allowNavigation: false,
    unprotected: false,
    saveFailed: false,
  };
  const status = document.querySelector("[data-finance-draft-status]");
  const statusMessage = document.querySelector("[data-finance-draft-message]");
  const reviewLink = document.querySelector("[data-finance-draft-review]");
  const discardButton = document.querySelector("[data-finance-draft-discard]");
  const panel = document.querySelector("[data-finance-object-panel]");
  const panelTitle = document.querySelector("[data-finance-object-title]");
  const panelBody = document.querySelector("[data-finance-object-body]");
  const globalSearch = document.querySelector("[data-finance-global-search]");
  const searchResults = document.querySelector("[data-finance-search-results]");
  const searchCount = document.querySelector("[data-finance-search-count]");
  const savedViews = document.querySelector("[data-finance-saved-views]");
  const saveViewButton = document.querySelector("[data-finance-save-view]");
  const esc = value => String(value ?? "").replace(/[&<>'"]/g, character => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"})[character]);
  const financeDate = value => {
    const raw = String(value || "").slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return "Unavailable";
    const [year, month, day] = raw.split("-").map(Number);
    return new Intl.DateTimeFormat("en-US", {month: "short", day: "numeric", year: "numeric"})
      .format(new Date(year, month - 1, day));
  };

  const announce = (message, tone = "neutral") => {
    if (!status) return;
    status.hidden = !message;
    document.body.classList.toggle("has-finance-draft", Boolean(message));
    if (statusMessage) statusMessage.textContent = message;
    else status.textContent = message;
    status.dataset.tone = tone;
    if (reviewLink) reviewLink.hidden = !state.changes.length;
    if (discardButton) discardButton.hidden = !state.changes.length;
  };

  const request = async (path, options = {}) => {
    const headers = {Accept: "application/json", ...(options.headers || {})};
    if (options.body) headers["Content-Type"] = "application/json";
    if (state.csrf && !["GET", "HEAD"].includes(options.method || "GET")) {
      headers["X-CSRF-Token"] = state.csrf;
    }
    const response = await fetch(path, {...options, headers, credentials: "same-origin"});
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "Finance could not complete that request.");
    return payload;
  };

  const normalize = change => ({
    object_type: String(change.object_type || ""),
    object_id: String(change.object_id || ""),
    action: String(change.action || ""),
    value: change.value,
    label: String(change.label || ""),
    amount_cents: Number(change.amount_cents || 0),
    expected_revision: Number(change.expected_revision || 0),
  });

  const persist = async () => {
    clearTimeout(state.timer);
    if (!state.ready) return;
    if (state.saving) {
      while (state.saving) await new Promise(resolve => setTimeout(resolve, 25));
      return;
    }
    state.saving = true;
    announce("Saving");
    try {
      const result = await request("/admin/finances/api/workspace/draft", {
        method: "PUT",
        body: JSON.stringify({changes: state.changes, dataset_revision: document.body.dataset.financeRevision || ""}),
      });
      state.draft = result;
      state.unprotected = false;
      state.saveFailed = false;
      announce(`${result.change_count} change${result.change_count === 1 ? "" : "s"} saved securely`, "success");
      document.dispatchEvent(new CustomEvent("finance:draft-saved", {detail: result}));
    } catch (error) {
      state.saveFailed = true;
      announce(`Save failed: ${error.message}`, "error");
      document.dispatchEvent(new CustomEvent("finance:draft-error", {detail: {message: error.message}}));
    } finally {
      state.saving = false;
    }
  };

  const stage = change => {
    const next = normalize(change);
    const index = state.changes.findIndex(item =>
      item.object_type === next.object_type && item.object_id === next.object_id && item.action === next.action
    );
    if (index >= 0) state.changes[index] = next;
    else state.changes.push(next);
    state.unprotected = true;
    state.saveFailed = false;
    announce(`${state.changes.length} unsaved change${state.changes.length === 1 ? "" : "s"}`, "warning");
    clearTimeout(state.timer);
    state.timer = setTimeout(persist, 450);
    document.dispatchEvent(new CustomEvent("finance:draft-changed", {detail: {changes: [...state.changes]}}));
  };

  const replaceScope = (objectType, changes) => {
    state.changes = [
      ...state.changes.filter(item => item.object_type !== objectType),
      ...changes.map(normalize),
    ];
    state.unprotected = true;
    state.saveFailed = false;
    announce(`${state.changes.length} unsaved change${state.changes.length === 1 ? "" : "s"}`, "warning");
    clearTimeout(state.timer);
    state.timer = setTimeout(persist, 450);
    document.dispatchEvent(new CustomEvent("finance:draft-changed", {detail: {changes: [...state.changes]}}));
  };

  const discard = async () => {
    await request("/admin/finances/api/workspace/draft", {method: "DELETE"});
    state.changes = [];
    state.draft = null;
    state.unprotected = false;
    state.saveFailed = false;
    announce("Draft discarded", "success");
    document.dispatchEvent(new CustomEvent("finance:draft-discarded"));
  };

  const reviewAndSave = async () => {
    if (!state.changes.length) return;
    await persist();
    state.allowNavigation = true;
    window.location.assign("/admin/finances/workspace/review");
  };

  discardButton?.addEventListener("click", async () => {
    try {
      await discard();
    } catch (error) {
      state.saveFailed = true;
      announce(`Save failed: ${error.message}`, "error");
    }
  });

  const openObject = async (type, id) => {
    if (!panel || !panelTitle || !panelBody) return;
    panel.dataset.objectType = type;
    panel.dataset.objectId = id;
    panelTitle.textContent = "Loading…";
    panelBody.innerHTML = '<div class="finance-object-state">Loading current evidence…</div>';
    panel.showModal();
    try {
      const item = await request(`/admin/finances/api/objects/${encodeURIComponent(type)}/${encodeURIComponent(id)}`);
      panelTitle.textContent = item.name || item.vendor_or_customer || "Finance item";
      const amount = new Intl.NumberFormat("en-US", {style: "currency", currency: "USD"}).format((item.amount_cents || 0) / 100);
      const protectedCopy = item.protected ? '<p class="finance-object-warning"><strong>Protected item.</strong> Payroll, tax, and debt changes require individual review.</p>' : "";
      const identifiers = (item.source_identifiers || []).map(source => `<li>${esc(source.source_system)} · ${esc(source.masked_external_id)}</li>`).join("");
      const related = (item.related_items || []).map(relatedItem => `<li>${esc(relatedItem.name || relatedItem.vendor_or_customer || relatedItem.id)} · ${esc(relatedItem.status)}</li>`).join("");
      const activity = (item.activity || []).map(event => `<li><strong>${esc(String(event.action_type || "").replaceAll("_", " "))}</strong><span>${esc(event.actor)} · ${esc(event.created_at)}</span></li>`).join("");
      const isPostedTransaction = item.record_kind === "transaction";
      const dateFacts = isPostedTransaction
        ? `<div><dt>Posted date</dt><dd>${esc(financeDate(item.effective_date || item.due_date))}</dd></div>`
        : `<div><dt>Due date</dt><dd>${esc(financeDate(item.due_date || item.effective_date) || "Not scheduled")}</dd></div>`;
      const cleanup = isPostedTransaction && !item.protected ? `<section class="finance-object-cleanup"><h3>Clean up this transaction</h3>
        <p>Choose any answers you know. They stay in one protected draft until you review and save everything together.</p>
        <div class="finance-object-actions" role="group" aria-label="Transaction cleanup actions">
          <button type="button" data-finance-stage-action="set_savings_state" data-finance-stage-value="needed">Needed</button>
          <button type="button" data-finance-stage-action="set_savings_state" data-finance-stage-value="waste">Waste</button>
          <button type="button" data-finance-stage-action="set_savings_state" data-finance-stage-value="investigate">Investigate</button>
          <button type="button" data-finance-stage-action="mark_duplicate" data-finance-stage-value="true">Duplicate</button>
          <button type="button" data-finance-stage-action="mark_internal_transfer" data-finance-stage-value="true">Internal transfer</button>
        </div><button type="button" class="btn btn-primary btn-sm" data-finance-review-draft>Review and save all changes</button></section>` : "";
      const paymentEvidence = isPostedTransaction && !Number(item.payment_evidence?.allocated_cents || 0)
        ? "Not matched to a planned bill. The bank withdrawal itself is still verified."
        : `${new Intl.NumberFormat("en-US", {style: "currency", currency: "USD"}).format(Number(item.payment_evidence?.allocated_cents || 0) / 100)} matched to payment evidence.`;
      panelBody.innerHTML = `${protectedCopy}<section><h3>What happened</h3><dl class="finance-object-facts">
        <div><dt>Amount</dt><dd>${amount}</dd></div>
        <div><dt>Evidence</dt><dd>${esc(item.confidence || "Unconfirmed")}</dd></div>
        <div><dt>Payment status</dt><dd>${esc(item.status || item.state || "Needs review")}</dd></div>
        <div><dt>Category</dt><dd>${esc(item.category || "Uncategorized")}</dd></div>
        ${dateFacts}
      </dl><p class="finance-object-description">${esc(item.description || "No raw bank description is available.")}</p></section>
      <section><h3>How it is treated</h3><dl class="finance-object-facts"><div><dt>Source</dt><dd>${esc(item.source || "Operator review")}</dd></div><div><dt>Saved decision</dt><dd>${item.revision ? `Revision ${Number(item.revision)}` : "None"}</dd></div></dl></section>
      ${cleanup}<section><h3>Payment evidence</h3><p>${paymentEvidence}</p>${related ? `<ul class="finance-object-list">${related}</ul>` : "<p>No related obligation or transaction is linked.</p>"}</section>
      <section><h3>Source identifiers</h3>${identifiers ? `<ul class="finance-object-list">${identifiers}</ul>` : "<p>No diagnostic identifiers are available.</p>"}</section>
      <section><h3>Similar transactions</h3><p>${Number(item.similar_transactions?.length || 0)} similar posted item(s) found.</p></section>
      <section><h3>Activity</h3>${activity ? `<ul class="finance-object-list">${activity}</ul>` : "<p>No prior operator changes.</p>"}</section>
      <p class="finance-object-note">Changes are staged here and saved together through Review. No money moves from this panel.</p>`;
    } catch (error) {
      panelTitle.textContent = "Could not load this item";
      panelBody.innerHTML = `<div class="finance-object-state finance-object-state--error">${error.message}</div>`;
    }
  };

  document.addEventListener("click", event => {
    const patternButton = event.target.closest("[data-finance-pattern-cadence]");
    if (patternButton) {
      event.preventDefault();
      const form = patternButton.closest("form");
      stage({
        object_type: "pattern",
        object_id: form?.querySelector('[name="pattern_key"]')?.value || "",
        action: "set_pattern_cadence",
        value: patternButton.dataset.financePatternCadence,
        label: form?.querySelector('[name="vendor"]')?.value || "Possible charge",
        amount_cents: Number(form?.querySelector('[name="amount_cents"]')?.value || 0),
      });
      form?.querySelectorAll("[data-finance-pattern-cadence]").forEach(button => button.classList.toggle("is-selected", button === patternButton));
      return;
    }
    const opener = event.target.closest("[data-finance-object-open]");
    if (opener) {
      event.preventDefault();
      openObject(opener.dataset.financeObjectType, opener.dataset.financeObjectId);
    }
    if (event.target.closest("[data-finance-object-close]")) panel?.close();
    const stageButton = event.target.closest("[data-finance-stage-action]");
    if (stageButton && panel) {
      const value = stageButton.dataset.financeStageValue === "true" ? true : stageButton.dataset.financeStageValue;
      stage({object_type: panel.dataset.objectType, object_id: panel.dataset.objectId, action: stageButton.dataset.financeStageAction, value});
      stageButton.classList.add("is-selected");
    }
    if (event.target.closest("[data-finance-review-draft]")) reviewAndSave();
  });

  let searchTimer = null;
  globalSearch?.addEventListener("input", () => {
    clearTimeout(searchTimer);
    const query = globalSearch.value.trim();
    if (query.length < 2) {
      if (searchResults) { searchResults.hidden = true; searchResults.innerHTML = ""; }
      if (searchCount) searchCount.textContent = "";
      return;
    }
    searchTimer = setTimeout(async () => {
      try {
        const result = await request(`/admin/finances/api/workspace/search?q=${encodeURIComponent(query)}`);
        if (searchCount) searchCount.textContent = `${result.items.length} result${result.items.length === 1 ? "" : "s"}`;
        if (!searchResults) return;
        searchResults.innerHTML = result.items.length ? result.items.map(item => {
          const amount = new Intl.NumberFormat("en-US", {style: "currency", currency: "USD"}).format(Number(item.amount_cents || 0) / 100);
          return `<button type="button" data-finance-object-open data-finance-object-type="${esc(item.object_type)}" data-finance-object-id="${esc(item.id)}"><span><strong>${esc(item.name || item.vendor_or_customer || "Finance item")}</strong><small>${esc(item.description || item.status || "")}</small></span><em>${amount}</em></button>`;
        }).join("") : '<p>No matching Finance items.</p>';
        searchResults.hidden = false;
      } catch (error) {
        if (searchResults) { searchResults.hidden = false; searchResults.innerHTML = `<p>${esc(error.message)}</p>`; }
      }
    }, 220);
  });
  globalSearch?.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      globalSearch.value = "";
      if (searchResults) searchResults.hidden = true;
      if (searchCount) searchCount.textContent = "";
    }
  });

  const renderSavedViews = views => {
    if (!savedViews) return;
    savedViews.innerHTML = (views || []).map(view => `<button type="button" data-finance-saved-view data-query="${esc(view.definition?.query || "")}">${esc(view.name)}</button>`).join("");
  };
  savedViews?.addEventListener("click", event => {
    const button = event.target.closest("[data-finance-saved-view]");
    if (!button || !globalSearch) return;
    globalSearch.value = button.dataset.query || "";
    globalSearch.dispatchEvent(new Event("input", {bubbles: true}));
  });
  saveViewButton?.addEventListener("click", async () => {
    const query = globalSearch?.value.trim() || "";
    if (query.length < 2) { announce("Enter a Finance search before saving the view.", "warning"); return; }
    const name = window.prompt("Name this Finance view", query);
    if (!name) return;
    try {
      await request("/admin/finances/api/workspace/views", {method: "POST", body: JSON.stringify({name, definition: {query}})});
      const refreshed = await request("/admin/finances/api/workspace/bootstrap");
      renderSavedViews(refreshed.saved_views);
      announce(`Saved view “${name}”`, "success");
    } catch (error) { announce(error.message, "error"); }
  });

  window.addEventListener("beforeunload", event => {
    if (state.allowNavigation || (!state.unprotected && !state.saving && !state.saveFailed)) return;
    event.preventDefault();
    event.returnValue = "";
  });

  window.FinanceWorkspace = {stage, replaceScope, persist, discard, reviewAndSave, openObject, getState: () => ({...state, changes: [...state.changes]})};

  request("/admin/finances/api/workspace/bootstrap")
    .then(result => {
      state.csrf = result.csrf_token || "";
      state.draft = result.draft;
      state.changes = result.draft?.changes || [];
      state.ready = true;
      renderSavedViews(result.saved_views);
      if (state.changes.length) announce(`${state.changes.length} recovered change${state.changes.length === 1 ? "" : "s"} saved securely`, "success");
      document.dispatchEvent(new CustomEvent("finance:workspace-ready", {detail: result}));
    })
    .catch(error => announce(`Draft protection unavailable: ${error.message}`, "error"));
})();
