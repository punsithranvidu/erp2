let IA_ME = null;
let IA_EDIT_ID = null;
let IA_FORM_OWNER_ID = null;
let IA_ROWS = [];
let IA_USERS = [];

function qs(id) {
  return document.getElementById(id);
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showMsg(text, ok = true) {
  const el = qs("iaMsg");
  if (!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return {};
  }
}

async function api(url, opts = {}) {
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    ...opts
  });
  const out = await safeJson(res);
  if (!res.ok || out.ok === false) {
    throw new Error(out.error || "Request failed");
  }
  return out;
}

function isAdmin() {
  return (document.body.dataset.role || "") === "ADMIN";
}

function currentUserId() {
  const username = IA_ME?.user || document.body.dataset.user || "";
  const row = IA_USERS.find((item) => item.username === username);
  return Number(row?.id || 0);
}

function fmtDateTime(value) {
  const text = String(value || "").trim();
  return text ? text.replace("T", " ") : "-";
}

function visibilityClass(value) {
  if (value === "ADMINS") return "admins";
  if (value === "ONLY_ME") return "only-me";
  return "everyone";
}

function multiline(value) {
  return esc(value || "").replaceAll("\n", "<br>");
}

function queryString(params) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([key, val]) => {
    if (val !== undefined && val !== null && String(val) !== "") {
      sp.set(key, val);
    }
  });
  return sp.toString();
}

function getFilters() {
  return {
    q: qs("iaSearch")?.value.trim() || "",
    user_id: isAdmin() ? (qs("iaUserFilter")?.value || "") : ""
  };
}

function setFormOwner(ownerUserId) {
  IA_FORM_OWNER_ID = Number(ownerUserId || 0);
  document.querySelectorAll("[data-ia-contributor]").forEach((input) => {
    const label = input.closest("label");
    const inputUserId = Number(input.value || 0);
    const isOwner = inputUserId === IA_FORM_OWNER_ID;
    input.disabled = isOwner;
    if (isOwner) input.checked = false;
    if (label) label.classList.toggle("is-disabled", isOwner);
  });
}

function selectedContributorIds() {
  return [...document.querySelectorAll("[data-ia-contributor]:checked")]
    .map((input) => Number(input.value || 0))
    .filter((id) => Number.isFinite(id) && id > 0);
}

function setSelectedContributors(ids) {
  const selected = new Set((ids || []).map((id) => Number(id)));
  document.querySelectorAll("[data-ia-contributor]").forEach((input) => {
    const inputId = Number(input.value || 0);
    input.checked = !input.disabled && selected.has(inputId);
  });
}

function syncFormToggleState() {
  const page = document.querySelector(".ia-page");
  const btn = qs("iaFormToggle");
  if (!page || !btn) return;

  const collapsed = page.classList.contains("form-collapsed");
  btn.textContent = "";
  btn.classList.toggle("is-collapsed", collapsed);
  btn.setAttribute("aria-label", collapsed ? "Expand achievement form" : "Collapse achievement form");
}

function ensureFormVisible() {
  const page = document.querySelector(".ia-page");
  if (!page) return;
  page.classList.remove("form-collapsed");
  syncFormToggleState();
}

function clearForm() {
  IA_EDIT_ID = null;
  qs("iaFormTitle").textContent = "Add Achievement";
  qs("iaTimePeriod").value = "";
  qs("iaAchievedWork").value = "";
  qs("iaTimeSpent").value = "";
  qs("iaResultsGot").value = "";
  qs("iaVisibility").value = "EVERYONE";
  qs("iaSaveBtn").textContent = "Save";
  setFormOwner(currentUserId());
  setSelectedContributors([]);
  showMsg("", true);
}

function fillForm(row) {
  IA_EDIT_ID = Number(row.id || 0);
  qs("iaFormTitle").textContent = `Edit Achievement #${row.id}`;
  qs("iaTimePeriod").value = row.time_period || "";
  qs("iaAchievedWork").value = row.achieved_work || "";
  qs("iaTimeSpent").value = row.time_spent || "";
  qs("iaResultsGot").value = row.results_got || "";
  qs("iaVisibility").value = row.visibility || "EVERYONE";
  qs("iaSaveBtn").textContent = "Update";
  setFormOwner(Number(row.user_id || 0));
  setSelectedContributors(row.contributor_ids || []);
  ensureFormVisible();
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function userLabel(row) {
  const name = row.full_name || row.username || "";
  return row.full_name ? `${row.full_name} (${row.username})` : row.username;
}

function renderContributorPicker() {
  const box = qs("iaContributorBox");
  if (!box) return;

  if (!IA_USERS.length) {
    box.innerHTML = `<div class="ia-picker-empty">No active users available.</div>`;
    return;
  }

  box.innerHTML = IA_USERS.map((row) => `
    <label>
      <input type="checkbox" data-ia-contributor value="${row.id}">
      <span>${esc(userLabel(row))} <span class="tiny muted">(${esc(row.role || "")})</span></span>
    </label>
  `).join("");

  setFormOwner(IA_FORM_OWNER_ID || currentUserId());
}

function renderUserFilter() {
  const wrap = qs("iaUserFilterWrap");
  const select = qs("iaUserFilter");
  if (!wrap || !select) return;

  if (!isAdmin()) {
    wrap.style.display = "none";
    return;
  }

  wrap.style.display = "";
  const current = select.value;
  const options = [`<option value="">All records</option>`];
  IA_USERS.forEach((row) => {
    options.push(`<option value="${row.id}">${esc(userLabel(row))}</option>`);
  });
  select.innerHTML = options.join("");
  select.value = IA_USERS.some((row) => String(row.id) === String(current)) ? current : "";
}

function renderMemberList(contributors) {
  if (!contributors || !contributors.length) {
    return `<span class="ia-meta">No contributors</span>`;
  }

  return `
    <div class="ia-member-list">
      ${contributors.map((item) => `<span class="ia-member-pill">${esc(item.display_name || item.full_name || item.username || "")}</span>`).join("")}
    </div>
  `;
}

function accessPill(row) {
  if (Number(row.can_edit) === 1) {
    return `<span class="ia-access-pill edit">Editable</span>`;
  }

  if (Number(row.is_contributor) === 1) {
    return `<span class="ia-access-pill locked">Contributor view</span>`;
  }

  return `<span class="ia-access-pill locked">View only</span>`;
}

function actionButtons(row) {
  const parts = [
    `<button class="btn ghost mini" type="button" data-act="details" data-id="${row.id}">Details</button>`
  ];

  if (Number(row.can_edit) === 1) {
    parts.push(`<button class="btn ghost mini" type="button" data-act="edit" data-id="${row.id}">Edit</button>`);
  }
  if (Number(row.can_delete) === 1) {
    parts.push(`<button class="btn ghost mini" type="button" data-act="delete" data-id="${row.id}">Delete</button>`);
  }

  return `<div class="ia-actions">${parts.join("")}</div>`;
}

function renderTable(rows, meta = {}) {
  const body = qs("iaTableBody");
  const count = qs("iaTableCount");
  const title = qs("iaTableTitle");
  const scopeText = qs("iaScopeText");

  if (count) {
    count.textContent = `${rows.length} record${rows.length === 1 ? "" : "s"}`;
  }
  if (title) {
    title.textContent = meta.table_title || "Achievements";
  }
  if (scopeText) {
    scopeText.textContent = meta.scope_text || "";
  }

  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="10" class="ia-empty">No achievements found for the current filters.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td>
        <div class="ia-cell-stack ia-owner">
          <strong>${esc(row.owner_name || "-")}</strong>
          <span class="ia-meta">${esc(row.owner_username || "")}</span>
        </div>
      </td>
      <td><strong>${esc(row.time_period || "-")}</strong></td>
      <td class="ia-copy">${multiline(row.achieved_work)}</td>
      <td>${esc(row.time_spent || "-")}</td>
      <td class="ia-copy">${multiline(row.results_got)}</td>
      <td>${renderMemberList(row.contributors || [])}</td>
      <td><span class="ia-visibility ${visibilityClass(row.visibility)}">${esc(row.visibility_label)}</span></td>
      <td>
        <div class="ia-cell-stack">
          <strong>${esc(row.edited_by || row.created_by || "-")}</strong>
          <span class="ia-meta">${esc(fmtDateTime(row.edited_at))}</span>
        </div>
      </td>
      <td>${accessPill(row)}</td>
      <td>${actionButtons(row)}</td>
    </tr>
  `).join("");
}

function detailCard(label, value) {
  return `
    <div class="ia-detail-card">
      <h4>${esc(label)}</h4>
      <p>${value}</p>
    </div>
  `;
}

function openDetails(row) {
  const modal = qs("iaDetailModal");
  if (!modal || !row) return;

  const contributorText = (row.contributors || []).length
    ? (row.contributors || []).map((item) => esc(item.display_name || item.full_name || item.username || "")).join(", ")
    : "No contributors";

  const accessText = Number(row.can_edit) === 1
    ? "You can edit this achievement."
    : Number(row.is_contributor) === 1
      ? "You were added as a contributor. Editing is locked unless the owner or an admin manages it."
      : "You can view this achievement, but editing is locked."
;

  qs("iaDetailTitle").textContent = row.time_period || `Achievement #${row.id}`;
  qs("iaDetailSub").textContent = `${row.owner_name || row.created_by || "Unknown"} • ${row.visibility_label || row.visibility || ""}`;
  qs("iaDetailGrid").innerHTML = [
    detailCard("Owner", esc(row.owner_name || "-")),
    detailCard("Created By", esc(row.created_by || "-")),
    detailCard("Created At", esc(fmtDateTime(row.created_at))),
    detailCard("Last Edited By", esc(row.edited_by || row.updated_by || row.created_by || "-")),
    detailCard("Last Edited At", esc(fmtDateTime(row.edited_at))),
    detailCard("Contributors", contributorText),
    detailCard("Visibility", esc(row.visibility_label || row.visibility || "-")),
    detailCard("Access", esc(accessText))
  ].join("");

  qs("iaDetailContentGrid").innerHTML = [
    detailCard("Achieved Work", multiline(row.achieved_work)),
    detailCard("Results Achieved", multiline(row.results_got)),
    detailCard("Time Period", esc(row.time_period || "-")),
    detailCard("Time Spent", esc(row.time_spent || "-"))
  ].join("");

  modal.classList.add("show");
}

function closeDetails() {
  qs("iaDetailModal")?.classList.remove("show");
}

async function loadMe() {
  IA_ME = await api("/api/me");
}

async function loadUsers() {
  const out = await api("/api/individual-achievement/users");
  IA_USERS = out.data || [];
  renderUserFilter();
  renderContributorPicker();
}

async function loadAchievements() {
  const filters = getFilters();
  const out = await api(`/api/individual-achievement/list?${queryString(filters)}`);
  IA_ROWS = out.data || [];
  renderTable(IA_ROWS, out.meta || {});
}

async function saveAchievement() {
  try {
    const payload = {
      time_period: qs("iaTimePeriod").value.trim(),
      achieved_work: qs("iaAchievedWork").value.trim(),
      time_spent: qs("iaTimeSpent").value.trim(),
      results_got: qs("iaResultsGot").value.trim(),
      visibility: qs("iaVisibility").value,
      contributor_ids: selectedContributorIds()
    };

    if (IA_EDIT_ID) {
      const out = await api(`/api/individual-achievement/${IA_EDIT_ID}`, {
        method: "PUT",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Achievement updated successfully", true);
    } else {
      const out = await api("/api/individual-achievement", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Achievement added successfully", true);
    }

    clearForm();
    await loadAchievements();
  } catch (err) {
    showMsg(err.message, false);
  }
}

async function deleteAchievement(id) {
  const row = IA_ROWS.find((item) => Number(item.id) === Number(id));
  const label = row ? (row.time_period || `#${id}`) : `#${id}`;
  if (!window.confirm(`Delete achievement ${label}?`)) return;

  try {
    const out = await api(`/api/individual-achievement/${id}`, {
      method: "DELETE"
    });
    if (Number(IA_EDIT_ID) === Number(id)) {
      clearForm();
    }
    showMsg(out.message || "Achievement deleted successfully", true);
    await loadAchievements();
  } catch (err) {
    showMsg(err.message, false);
  }
}

function setupFormToggle() {
  const page = document.querySelector(".ia-page");
  const btn = qs("iaFormToggle");
  if (!page || !btn) return;

  btn.addEventListener("click", () => {
    page.classList.toggle("form-collapsed");
    syncFormToggleState();
  });

  syncFormToggleState();
}

function setupTableActions() {
  const body = qs("iaTableBody");
  if (!body) return;

  body.addEventListener("click", async (event) => {
    const btn = event.target.closest("[data-act]");
    if (!btn) return;

    const id = Number(btn.getAttribute("data-id") || 0);
    const row = IA_ROWS.find((item) => Number(item.id) === id);
    if (!id || !row) return;

    const action = btn.getAttribute("data-act");
    if (action === "details") {
      openDetails(row);
      return;
    }
    if (action === "edit") {
      fillForm(row);
      return;
    }
    if (action === "delete") {
      await deleteAchievement(id);
    }
  });
}

function setupDetailModal() {
  qs("iaDetailCloseBtn")?.addEventListener("click", closeDetails);
  qs("iaDetailModal")?.addEventListener("click", (event) => {
    if (event.target === qs("iaDetailModal")) {
      closeDetails();
    }
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  try {
    setupFormToggle();
    setupTableActions();
    setupDetailModal();

    await loadMe();
    await loadUsers();
    clearForm();
    await loadAchievements();

    qs("iaSaveBtn").addEventListener("click", saveAchievement);
    qs("iaClearBtn").addEventListener("click", clearForm);
    qs("iaRefreshBtn").addEventListener("click", loadAchievements);
    qs("iaApplyFiltersBtn").addEventListener("click", loadAchievements);
    qs("iaResetFiltersBtn").addEventListener("click", () => {
      qs("iaSearch").value = "";
      if (qs("iaUserFilter")) {
        qs("iaUserFilter").value = "";
      }
      loadAchievements();
    });
    qs("iaSearch").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadAchievements();
      }
    });
    qs("iaUserFilter")?.addEventListener("change", () => {
      if (isAdmin()) {
        loadAchievements();
      }
    });
  } catch (err) {
    showMsg(err.message || "Failed to load achievements.", false);
  }
});
