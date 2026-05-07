let IA_ME = null;
let IA_EDIT_ID = null;
let IA_MY_ROWS = [];
let IA_VISIBLE_ROWS = [];
let IA_USERS = [];

function qs(id){
  return document.getElementById(id);
}

function esc(value){
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showMsg(text, ok=true){
  const el = qs("iaMsg");
  if(!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

async function safeJson(res){
  try { return await res.json(); }
  catch { return {}; }
}

async function api(url, opts={}){
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    ...opts
  });
  const out = await safeJson(res);
  if(!res.ok || out.ok === false){
    throw new Error(out.error || "Request failed");
  }
  return out;
}

function isAdmin(){
  return (document.body.dataset.role || "") === "ADMIN";
}

function fmtDateTime(value){
  const text = String(value || "").trim();
  return text ? text.replace("T", " ") : "-";
}

function visibilityClass(value){
  if(value === "ADMINS") return "admins";
  if(value === "ONLY_ME") return "only-me";
  return "everyone";
}

function multiline(value){
  return esc(value || "").replaceAll("\n", "<br>");
}

function getFilters(){
  return {
    q: qs("iaSearch").value.trim(),
    user_id: qs("iaUserFilter").value
  };
}

function queryString(params){
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([key, val]) => {
    if(val !== undefined && val !== null && String(val) !== ""){
      sp.set(key, val);
    }
  });
  return sp.toString();
}

function clearForm(){
  IA_EDIT_ID = null;
  qs("iaFormTitle").textContent = "Add Achievement";
  qs("iaTimePeriod").value = "";
  qs("iaAchievedWork").value = "";
  qs("iaTimeSpent").value = "";
  qs("iaResultsGot").value = "";
  qs("iaVisibility").value = "EVERYONE";
  qs("iaSaveBtn").textContent = "Save";
  showMsg("", true);
}

function fillForm(row){
  IA_EDIT_ID = Number(row.id || 0);
  qs("iaFormTitle").textContent = `Edit Achievement #${row.id}`;
  qs("iaTimePeriod").value = row.time_period || "";
  qs("iaAchievedWork").value = row.achieved_work || "";
  qs("iaTimeSpent").value = row.time_spent || "";
  qs("iaResultsGot").value = row.results_got || "";
  qs("iaVisibility").value = row.visibility || "EVERYONE";
  qs("iaSaveBtn").textContent = "Update";
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function renderUserFilter(users){
  const select = qs("iaUserFilter");
  if(!select) return;

  const current = select.value;
  const options = [`<option value="">${isAdmin() ? "All users" : "All visible users"}</option>`];
  users.forEach((row) => {
    const label = row.full_name || row.username;
    options.push(`<option value="${row.id}">${esc(label)}</option>`);
  });
  select.innerHTML = options.join("");
  select.value = users.some((row) => String(row.id) === String(current)) ? current : "";
}

function actionButtons(row){
  if(Number(row.can_edit) !== 1 && Number(row.can_delete) !== 1){
    return `<span class="ia-meta">-</span>`;
  }

  return `
    <div class="ia-actions">
      ${Number(row.can_edit) === 1 ? `<button class="btn ghost mini" type="button" data-act="edit" data-id="${row.id}">Edit</button>` : ""}
      ${Number(row.can_delete) === 1 ? `<button class="btn ghost mini" type="button" data-act="delete" data-id="${row.id}">Delete</button>` : ""}
    </div>
  `;
}

function attachActionHandlers(scope){
  scope.querySelectorAll("[data-act='edit']").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = Number(btn.getAttribute("data-id") || 0);
      const row = [...IA_MY_ROWS, ...IA_VISIBLE_ROWS].find((item) => Number(item.id) === id);
      if(row) fillForm(row);
    });
  });

  scope.querySelectorAll("[data-act='delete']").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.getAttribute("data-id") || 0);
      if(!id) return;
      await deleteAchievement(id);
    });
  });
}

function renderMyTable(rows){
  const body = qs("iaMyBody");
  const count = qs("iaMyCount");
  if(count) count.textContent = `${rows.length} record${rows.length === 1 ? "" : "s"}`;

  if(!rows.length){
    body.innerHTML = `<tr><td colspan="7" class="ia-empty">No achievements found.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td><b>${esc(row.time_period)}</b></td>
      <td class="ia-copy">${multiline(row.achieved_work)}</td>
      <td>${esc(row.time_spent)}</td>
      <td class="ia-copy">${multiline(row.results_got)}</td>
      <td><span class="ia-visibility ${visibilityClass(row.visibility)}">${esc(row.visibility_label)}</span></td>
      <td class="ia-meta">${esc(fmtDateTime(row.updated_at || row.created_at))}</td>
      <td>${actionButtons(row)}</td>
    </tr>
  `).join("");

  attachActionHandlers(body);
}

function selectedUserLabel(){
  const selected = qs("iaUserFilter").value;
  if(!selected) return isAdmin() ? "Visible Achievements" : "Shared Achievements";

  const row = IA_USERS.find((item) => String(item.id) === String(selected));
  if(!row) return isAdmin() ? "Visible Achievements" : "Shared Achievements";

  const name = row.full_name || row.username;
  return `${name} Achievements`;
}

function renderVisibleTable(rows){
  const body = qs("iaVisibleBody");
  const count = qs("iaVisibleCount");
  const title = qs("iaVisibleTitle");
  if(count) count.textContent = `${rows.length} record${rows.length === 1 ? "" : "s"}`;
  if(title) title.textContent = selectedUserLabel();

  if(!rows.length){
    body.innerHTML = `<tr><td colspan="8" class="ia-empty">No visible achievements found.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td>
        <b>${esc(row.owner_name)}</b>
        <div class="ia-meta">${esc(row.owner_username || "")}</div>
      </td>
      <td><b>${esc(row.time_period)}</b></td>
      <td class="ia-copy">${multiline(row.achieved_work)}</td>
      <td>${esc(row.time_spent)}</td>
      <td class="ia-copy">${multiline(row.results_got)}</td>
      <td><span class="ia-visibility ${visibilityClass(row.visibility)}">${esc(row.visibility_label)}</span></td>
      <td class="ia-meta">${esc(fmtDateTime(row.updated_at || row.created_at))}</td>
      <td>${actionButtons(row)}</td>
    </tr>
  `).join("");

  attachActionHandlers(body);
}

async function loadMe(){
  IA_ME = await api("/api/me");
}

async function loadUsers(){
  const out = await api("/api/individual-achievement/users");
  IA_USERS = out.data || [];
  renderUserFilter(IA_USERS);
}

async function loadMyAchievements(){
  const filters = getFilters();
  const out = await api(`/api/individual-achievement/my?${queryString({ q: filters.q })}`);
  IA_MY_ROWS = out.data || [];
  renderMyTable(IA_MY_ROWS);
}

async function loadVisibleAchievements(){
  const filters = getFilters();
  const out = await api(`/api/individual-achievement/visible?${queryString(filters)}`);
  IA_VISIBLE_ROWS = out.data || [];
  renderVisibleTable(IA_VISIBLE_ROWS);
}

async function loadAll(){
  await Promise.all([
    loadMyAchievements(),
    loadVisibleAchievements()
  ]);
}

async function saveAchievement(){
  try{
    const payload = {
      time_period: qs("iaTimePeriod").value.trim(),
      achieved_work: qs("iaAchievedWork").value.trim(),
      time_spent: qs("iaTimeSpent").value.trim(),
      results_got: qs("iaResultsGot").value.trim(),
      visibility: qs("iaVisibility").value
    };

    if(IA_EDIT_ID){
      const out = await api(`/api/individual-achievement/${IA_EDIT_ID}`, {
        method: "PUT",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Achievement updated successfully", true);
    }else{
      const out = await api("/api/individual-achievement", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Achievement added successfully", true);
    }

    clearForm();
    await loadAll();
  }catch(err){
    showMsg(err.message, false);
  }
}

async function deleteAchievement(id){
  const row = [...IA_MY_ROWS, ...IA_VISIBLE_ROWS].find((item) => Number(item.id) === Number(id));
  const label = row ? (row.time_period || `#${id}`) : `#${id}`;
  if(!window.confirm(`Delete achievement ${label}?`)) return;

  try{
    const out = await api(`/api/individual-achievement/${id}`, {
      method: "DELETE"
    });
    if(Number(IA_EDIT_ID) === Number(id)){
      clearForm();
    }
    showMsg(out.message || "Achievement deleted successfully", true);
    await loadAll();
  }catch(err){
    showMsg(err.message, false);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  try{
    await loadMe();
    await loadUsers();
    await loadAll();

    qs("iaSaveBtn").addEventListener("click", saveAchievement);
    qs("iaClearBtn").addEventListener("click", clearForm);
    qs("iaRefreshBtn").addEventListener("click", loadAll);
    qs("iaApplyFiltersBtn").addEventListener("click", loadAll);
    qs("iaResetFiltersBtn").addEventListener("click", () => {
      qs("iaSearch").value = "";
      qs("iaUserFilter").value = "";
      loadAll();
    });
    qs("iaUserFilter").addEventListener("change", loadVisibleAchievements);
    qs("iaSearch").addEventListener("keydown", (e) => {
      if(e.key === "Enter"){
        e.preventDefault();
        loadAll();
      }
    });
  }catch(err){
    showMsg(err.message || "Failed to load achievements.", false);
  }
});
