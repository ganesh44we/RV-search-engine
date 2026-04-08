const API_BASE = "http://127.0.0.1:8000";

const state = {
  backendReady: false,
  searchAbort: null
};

const els = {
  searchInput: document.getElementById("search-input"),
  suggestions: document.getElementById("suggestions"),
  results: document.getElementById("results")
};

function debounce(fn, wait = 180) {
  let timeout = null;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), wait);
  };
}

async function ensureDemoData() {
  const statsResponse = await fetch(`${API_BASE}/stats`);
  const stats = await statsResponse.json();
  if (!stats.documents) {
    await fetch(`${API_BASE}/bootstrap-demo`, { method: "POST" });
  }
}

async function pollBackendStatus() {
  if (!window.desktopApp) {
    state.backendReady = true;
    await ensureDemoData();
    return;
  }
  const status = await window.desktopApp.getBackendStatus();
  state.backendReady = Boolean(status.ready);
  if (state.backendReady) {
    await ensureDemoData();
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderSuggestions(items) {
  if (!items.length) {
    els.suggestions.classList.add("hidden");
    els.suggestions.innerHTML = "";
    return;
  }
  els.suggestions.classList.remove("hidden");
  els.suggestions.innerHTML = items
    .map(
      (item) => `<button class="suggestion-item" type="button" data-value="${escapeHtml(item)}">${escapeHtml(item)}</button>`
    )
    .join("");

  els.suggestions.querySelectorAll(".suggestion-item").forEach((button) => {
    button.addEventListener("click", () => {
      els.searchInput.value = button.dataset.value;
      els.suggestions.classList.add("hidden");
      runSearch(button.dataset.value);
    });
  });
}

function renderResults(payload) {
  const results = payload.results || [];
  if (!results.length) {
    els.results.innerHTML = `
      <div class="empty-state">
        <p>No results found.</p>
      </div>
    `;
    return;
  }

  const didYouMean = payload.did_you_mean
    ? `<div class="aux-line">Did you mean <button type="button" data-query="${escapeHtml(payload.did_you_mean)}">${escapeHtml(payload.did_you_mean)}</button>?</div>`
    : "";

  const relatedQueries = payload.related_queries?.length
    ? `<div class="related">${payload.related_queries
        .map((query) => `<button type="button" data-query="${escapeHtml(query)}">${escapeHtml(query)}</button>`)
        .join("")}</div>`
    : "";

  els.results.innerHTML = `
    ${didYouMean}
    ${relatedQueries}
    ${results
      .map((result) => {
        const doc = result.document || {};
        const snippet = result.explanation?.snippet || doc.description || doc.content || "";
        return `
          <article class="result-card">
            ${doc.url ? `<a class="result-link" href="${escapeHtml(doc.url)}">${escapeHtml(doc.url)}</a>` : ""}
            <h2 class="result-title">${escapeHtml(doc.title || result.id)}</h2>
            <p class="result-snippet">${escapeHtml(snippet)}</p>
          </article>
        `;
      })
      .join("")}
  `;

  els.results.querySelectorAll("[data-query]").forEach((button) => {
    button.addEventListener("click", () => {
      els.searchInput.value = button.dataset.query;
      runSearch(button.dataset.query);
    });
  });
}

async function fetchSuggestions(query) {
  if (!state.backendReady || !query.trim()) {
    renderSuggestions([]);
    return;
  }
  try {
    const response = await fetch(`${API_BASE}/suggest?q=${encodeURIComponent(query)}`);
    if (!response.ok) {
      return;
    }
    const payload = await response.json();
    renderSuggestions(payload.suggestions || []);
  } catch {
    renderSuggestions([]);
  }
}

async function runSearch(query) {
  if (!state.backendReady) {
    return;
  }
  const trimmed = query.trim();
  if (!trimmed) {
    els.results.innerHTML = `<div class="empty-state"><p>Search the web index.</p></div>`;
    renderSuggestions([]);
    return;
  }

  if (state.searchAbort) {
    state.searchAbort.abort();
  }
  const controller = new AbortController();
  state.searchAbort = controller;
  renderSuggestions([]);
  els.results.innerHTML = `<div class="empty-state"><p>Searching...</p></div>`;

  try {
    const response = await fetch(`${API_BASE}/search?q=${encodeURIComponent(trimmed)}`, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(`Search failed with status ${response.status}`);
    }
    const payload = await response.json();
    renderResults(payload);
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }
    els.results.innerHTML = `<div class="empty-state"><p>${escapeHtml(error.message)}</p></div>`;
  }
}

const debouncedSuggest = debounce(fetchSuggestions, 120);
const debouncedSearch = debounce(runSearch, 220);

els.searchInput.addEventListener("input", (event) => {
  const value = event.target.value;
  debouncedSuggest(value);
  debouncedSearch(value);
});

els.searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    runSearch(els.searchInput.value);
  }
});

document.addEventListener("click", (event) => {
  if (!els.suggestions.contains(event.target) && event.target !== els.searchInput) {
    els.suggestions.classList.add("hidden");
  }
});

window.desktopApp?.onBackendStatus(async (payload) => {
  state.backendReady = Boolean(payload.ready);
  if (state.backendReady) {
    await ensureDemoData();
  }
});

pollBackendStatus();
