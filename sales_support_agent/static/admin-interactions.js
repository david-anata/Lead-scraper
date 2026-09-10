(() => {
  "use strict";

  const peopleInput = document.getElementById("people-search");
  if (peopleInput) {
    const rows = Array.from(document.querySelectorAll("#people-rows > tr"))
      .filter(row => row.cells.length === 6);
    const empty = document.getElementById("people-empty");
    const count = document.getElementById("people-count");
    const renderPeople = () => {
      const query = peopleInput.value.trim().toLocaleLowerCase();
      let visible = 0;
      rows.forEach(row => {
        const nameAndEmail = `${row.cells[0].textContent} ${row.cells[1].textContent}`.toLocaleLowerCase();
        row.hidden = !nameAndEmail.includes(query);
        if (!row.hidden) visible += 1;
      });
      empty.hidden = !rows.length || visible > 0;
      count.textContent = `${visible} of ${rows.length} people`;
    };
    document.querySelector("[data-people-controls]").hidden = false;
    peopleInput.addEventListener("input", renderPeople);
    document.getElementById("people-clear").addEventListener("click", () => {
      peopleInput.value = "";
      renderPeople();
      peopleInput.focus();
    });
    renderPeople();
  }

  document.querySelectorAll(".top-actions--secondary").forEach(row => {
    const updateEdges = () => {
      row.classList.toggle("has-more-left", row.scrollLeft > 1);
      row.classList.toggle("has-more-right", row.scrollWidth - row.clientWidth - row.scrollLeft > 1);
    };
    row.addEventListener("scroll", updateEdges, {passive: true});
    window.addEventListener("resize", updateEdges);
    if (typeof ResizeObserver !== "undefined") new ResizeObserver(updateEdges).observe(row);
    if (document.fonts) document.fonts.ready.then(updateEdges);
    updateEdges();
  });

  const announce = (message) => {
    let region = document.querySelector(".app-live-region");
    if (!region) {
      region = document.createElement("div");
      region.className = "app-live-region";
      region.setAttribute("role", "status");
      region.setAttribute("aria-live", "polite");
      document.body.appendChild(region);
    }
    region.textContent = message;
  };

  document.querySelectorAll("form[method='post']").forEach((form) => {
    form.addEventListener("submit", (event) => {
      if (event.defaultPrevented || !form.checkValidity()) return;
      const submitter = event.submitter;
      if (submitter && submitter.dataset.confirmMessage) {
        if (!window.confirm(submitter.dataset.confirmMessage)) {
          event.preventDefault();
          return;
        }
      }
      form.setAttribute("aria-busy", "true");
      if (submitter) {
        submitter.classList.add("app-is-submitting");
        submitter.setAttribute("aria-disabled", "true");
      }
      announce((submitter && submitter.dataset.progressMessage) || "Saving…");
    });
  });

  document.querySelectorAll("[data-paginate]").forEach((container) => {
    const items = Array.from(container.querySelectorAll(":scope > [data-page-item]"));
    const pageSize = Math.max(1, Number(container.dataset.pageSize || 10));
    if (items.length <= pageSize) return;

    let page = 0;
    const controls = document.createElement("nav");
    controls.className = "app-pagination";
    controls.setAttribute("aria-label", container.dataset.paginationLabel || "Results pages");
    controls.innerHTML = '<span class="app-muted" data-page-status></span><div class="app-pagination__controls"><button type="button" data-page-previous>Previous</button><button type="button" data-page-next>Next</button></div>';
    container.after(controls);

    const previous = controls.querySelector("[data-page-previous]");
    const next = controls.querySelector("[data-page-next]");
    const status = controls.querySelector("[data-page-status]");
    const render = () => {
      const start = page * pageSize;
      const end = Math.min(start + pageSize, items.length);
      items.forEach((item, index) => { item.hidden = index < start || index >= end; });
      status.textContent = `Showing ${start + 1}–${end} of ${items.length}`;
      previous.disabled = page === 0;
      next.disabled = end >= items.length;
    };
    previous.addEventListener("click", () => { page -= 1; render(); container.scrollIntoView({behavior: "smooth", block: "start"}); });
    next.addEventListener("click", () => { page += 1; render(); container.scrollIntoView({behavior: "smooth", block: "start"}); });
    render();
  });
})();
