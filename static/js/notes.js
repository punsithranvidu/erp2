let NOTES_TAB = "active";
let NOTES_PAGE = 1;
let NOTES_TOTAL_PAGES = 1;
let EDIT_NOTE_ID = null;
let ME = null;

const NOTE_STATUSES = ["Active", "Done", "Cancelled"];

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
  const el = qs("noteMsg");
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

function toInputDateTime(value){
  const v = String(value || "");
  if(!v) return "";
  return v.slice(0, 16);
}

function fmt(value){
  return value || "-";
}

function statusClass(status){
  return String(status || "Active").toLowerCase();
}

function getFilters(){
  return {
    q: qs("filterSearch").value.trim(),
    status: qs("filterStatus").value,
    user_id: qs("filterUser") ? qs("filterUser").value : "",
    expected_date: qs("filterExpected").value
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
  EDIT_NOTE_ID = null;
  qs("noteFormTitle").textContent = "Add Note";
  qs("noteText").value = "";
  qs("expectedEndDate").value = "";
  qs("noteStatus").value = "Active";
  qs("noteStatus").disabled = false;
  qs("saveNoteBtn").textContent = "Save";
  showMsg("", true);
}

function fillForm(row){
  EDIT_NOTE_ID = row.id;
  qs("noteFormTitle").textContent = `Edit Note #${row.id}`;
  qs("noteText").value = row.note_text || "";
  qs("expectedEndDate").value = toInputDateTime(row.expected_end_date);
  qs("noteStatus").value = NOTE_STATUSES.includes(row.status) ? row.status : "Active";
  qs("noteStatus").disabled = false;
  qs("saveNoteBtn").textContent = "Update";
  window.scrollTo({ top: 0, behavior: "smooth" });
}

async function saveNote(){
  try{
    const payload = {
      note_text: qs("noteText").value.trim(),
      expected_end_date: qs("expectedEndDate").value,
      status: qs("noteStatus").value
    };

    if(!payload.note_text){
      showMsg("Note text is required", false);
      return;
    }

    if(EDIT_NOTE_ID){
      const out = await api(`/api/notes/${EDIT_NOTE_ID}`, {
        method: "PUT",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Note updated successfully", true);
    }else{
      const out = await api("/api/notes", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      showMsg(out.message || "Note saved successfully", true);
    }

    clearForm();
    await loadNotes();
  }catch(err){
    showMsg(err.message, false);
  }
}

function setTab(tab){
  NOTES_TAB = tab;
  NOTES_PAGE = 1;
  document.querySelectorAll(".notes-tab").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  });

  const sub = qs("notesSub");
  if(tab === "active") sub.textContent = "Active shared notes.";
  if(tab === "mine") sub.textContent = "Notes related to the selected user.";
  if(tab === "trash") sub.textContent = "Deleted notes. Admin can recover or permanently delete.";

  qs("pageWarning").style.display = "none";
  qs("historyPanel").style.display = "none";
  loadNotes();
}

function renderWarning(pages){
  const el = qs("pageWarning");
  pages = Array.isArray(pages) ? pages : [];
  if(!pages.length){
    el.style.display = "none";
    el.textContent = "";
    return;
  }

  if(pages.length === 1){
    el.textContent = `Previous page ${pages[0]} still has unmarked active notes.`;
  }else{
    el.textContent = `Previous pages ${pages.join(", ")} also have active notes.`;
  }
  el.style.display = "block";
}

function noteTooltip(row){
  return `
    <div><b>Created:</b> ${esc(fmt(row.created_at))}</div>
    <div><b>Created by:</b> ${esc(fmt(row.created_by))}</div>
    <div><b>Expected due:</b> ${esc(fmt(row.expected_end_date))}</div>
    <div><b>Status:</b> ${esc(fmt(row.status))}</div>
    <div><b>Done date:</b> ${esc(fmt(row.done_at))}</div>
    <div><b>Cancelled date:</b> ${esc(fmt(row.cancelled_at))}</div>
  `;
}

function actionButton(label, className="ghost"){
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = `btn mini ${className}`;
  btn.textContent = label;
  return btn;
}

function renderNoteCard(row){
  const card = document.createElement("div");
  card.className = `note-card ${statusClass(row.status)}`;

  card.innerHTML = `
    <div class="note-tip">${noteTooltip(row)}</div>
    <div class="note-text">${esc(row.note_text)}</div>
    <div class="note-meta">
      <span class="note-pill status-${statusClass(row.status)}">${esc(row.status)}</span>
      <span class="note-pill">By ${esc(row.created_by || "-")}</span>
      <span class="note-pill">Due ${esc(row.expected_end_date || "-")}</span>
    </div>
    <div class="note-actions"></div>
  `;

  const actions = card.querySelector(".note-actions");

  if(Number(row.can_status) === 1 && row.status !== "Done"){
    const doneBtn = actionButton("Done");
    doneBtn.addEventListener("click", () => setStatus(row.id, "Done"));
    actions.appendChild(doneBtn);
  }

  if(Number(row.can_status) === 1 && row.status !== "Cancelled"){
    const cancelBtn = actionButton("Cancel");
    cancelBtn.addEventListener("click", () => setStatus(row.id, "Cancelled"));
    actions.appendChild(cancelBtn);
  }

  if(Number(row.can_status) === 1 && row.status !== "Active"){
    const activeBtn = actionButton("Active");
    activeBtn.addEventListener("click", () => setStatus(row.id, "Active"));
    actions.appendChild(activeBtn);
  }

  const historyBtn = actionButton("History");
  historyBtn.addEventListener("click", () => loadHistory(row.id));
  actions.appendChild(historyBtn);

  if(Number(row.can_edit) === 1){
    const editBtn = actionButton("Edit");
    editBtn.addEventListener("click", () => fillForm(row));
    actions.appendChild(editBtn);
  }

  if(Number(row.can_delete) === 1){
    const deleteBtn = actionButton("Delete");
    deleteBtn.addEventListener("click", () => deleteNote(row.id));
    actions.appendChild(deleteBtn);
  }

  return card;
}

function renderTrashCard(row){
  const card = document.createElement("div");
  card.className = `note-card ${statusClass(row.status)}`;
  card.innerHTML = `
    <div class="note-tip">${noteTooltip(row)}<div><b>Deleted:</b> ${esc(fmt(row.deleted_at))}</div><div><b>Deleted by:</b> ${esc(fmt(row.deleted_by))}</div></div>
    <div class="note-text">${esc(row.note_text)}</div>
    <div class="note-meta">
      <span class="note-pill status-${statusClass(row.status)}">${esc(row.status)}</span>
      <span class="note-pill">Deleted ${esc(row.deleted_at || "-")}</span>
      <span class="note-pill">By ${esc(row.created_by || "-")}</span>
    </div>
    <div class="note-actions"></div>
  `;

  const actions = card.querySelector(".note-actions");
  const recoverBtn = actionButton("Recover");
  recoverBtn.addEventListener("click", () => recoverNote(row.id));
  actions.appendChild(recoverBtn);

  const permBtn = actionButton("Permanent Delete");
  permBtn.addEventListener("click", () => permanentDelete(row.id));
  actions.appendChild(permBtn);

  const historyBtn = actionButton("History");
  historyBtn.addEventListener("click", () => loadHistory(row.id));
  actions.appendChild(historyBtn);

  return card;
}

function renderNotes(rows){
  const board = qs("notesBoard");
  board.innerHTML = "";

  if(!rows.length){
    board.innerHTML = `<div class="notes-empty">No notes found.</div>`;
    return;
  }

  rows.forEach(row => {
    board.appendChild(NOTES_TAB === "trash" ? renderTrashCard(row) : renderNoteCard(row));
  });
}

function renderPager(out){
  NOTES_TOTAL_PAGES = Number(out.total_pages || 1);
  qs("pageLabel").textContent = `Page ${Number(out.page || NOTES_PAGE)} of ${NOTES_TOTAL_PAGES}`;
  qs("totalLabel").textContent = `${Number(out.total || 0)} notes shown`;
  qs("prevPageBtn").disabled = NOTES_PAGE <= 1;
  qs("nextPageBtn").disabled = NOTES_PAGE >= NOTES_TOTAL_PAGES;
  qs("notesPager").style.display = NOTES_TAB === "trash" ? "none" : "flex";
}

async function loadNotes(){
  try{
    if(NOTES_TAB === "trash"){
      await loadTrash();
      return;
    }

    const filters = getFilters();
    const qsText = queryString({
      tab: NOTES_TAB,
      page: NOTES_PAGE,
      per_page: 12,
      q: filters.q,
      status: filters.status,
      user_id: filters.user_id,
      expected_date: filters.expected_date
    });
    const out = await api(`/api/notes?${qsText}`);
    renderNotes(out.data || []);
    renderPager(out);
    renderWarning(out.previous_active_pages || []);
  }catch(err){
    showMsg(err.message, false);
  }
}

async function loadTrash(){
  const filters = getFilters();
  const qsText = queryString({
    tab: "trash",
    q: filters.q,
    status: filters.status,
    user_id: filters.user_id,
    expected_date: filters.expected_date
  });
  const out = await api(`/api/notes/trash?${qsText}`);
  renderNotes(out.data || []);
  qs("totalLabel").textContent = `${(out.data || []).length} trash notes`;
  qs("notesPager").style.display = "none";
  renderWarning([]);
}

async function setStatus(id, status){
  try{
    await api(`/api/notes/${id}/status`, {
      method: "POST",
      body: JSON.stringify({ status })
    });
    await loadNotes();
  }catch(err){
    showMsg(err.message, false);
  }
}

async function deleteNote(id){
  if(!confirm("Move this note to trash?")) return;
  try{
    const out = await api(`/api/notes/${id}`, { method: "DELETE" });
    showMsg(out.message || "Note moved to trash", true);
    await loadNotes();
  }catch(err){
    showMsg(err.message, false);
  }
}

async function recoverNote(id){
  try{
    const out = await api(`/api/notes/trash/${id}/recover`, { method: "POST" });
    showMsg(out.message || "Note recovered", true);
    await loadNotes();
  }catch(err){
    showMsg(err.message, false);
  }
}

async function permanentDelete(id){
  if(!confirm("Permanently delete this note? This cannot be undone.")) return;
  try{
    const out = await api(`/api/notes/trash/${id}/permanent`, { method: "DELETE" });
    showMsg(out.message || "Note permanently deleted", true);
    await loadNotes();
  }catch(err){
    showMsg(err.message, false);
  }
}

async function loadHistory(id){
  try{
    const out = await api(`/api/notes/${id}/history`);
    const rows = out.data || [];
    const panel = qs("historyPanel");
    const body = qs("historyRows");

    if(!rows.length){
      body.innerHTML = `<p class="sub">No expected date history yet.</p>`;
    }else{
      body.innerHTML = rows.map(r => `
        <div class="notes-history-row">
          <div><b>Old</b><br>${esc(fmt(r.old_expected_end_date))}</div>
          <div><b>New</b><br>${esc(fmt(r.new_expected_end_date))}</div>
          <div><b>Changed</b><br>${esc(fmt(r.changed_at))}</div>
          <div><b>By</b><br>${esc(fmt(r.changed_by))}</div>
        </div>
      `).join("");
    }

    panel.style.display = "block";
  }catch(err){
    showMsg(err.message, false);
  }
}

async function loadUsers(){
  const role = document.body.dataset.role || "";
  ME = await api("/api/me");
  if(role !== "ADMIN"){
    return;
  }

  document.querySelectorAll(".admin-only").forEach(el => {
    el.style.display = "";
  });

  const out = await api("/api/notes/users");
  const users = out.data || [];
  const select = qs("filterUser");
  select.innerHTML = `
    <option value="">Me</option>
    <option value="ALL">All Users</option>
  ` + users.map(u => `<option value="${u.id}">${esc(u.username)} (${esc(u.role)})</option>`).join("");
}

function bindEvents(){
  qs("saveNoteBtn").addEventListener("click", saveNote);
  qs("clearNoteBtn").addEventListener("click", clearForm);
  qs("refreshNotesBtn").addEventListener("click", loadNotes);
  qs("applyFiltersBtn").addEventListener("click", () => {
    NOTES_PAGE = 1;
    loadNotes();
  });
  qs("prevPageBtn").addEventListener("click", () => {
    if(NOTES_PAGE > 1){
      NOTES_PAGE -= 1;
      loadNotes();
    }
  });
  qs("nextPageBtn").addEventListener("click", () => {
    if(NOTES_PAGE < NOTES_TOTAL_PAGES){
      NOTES_PAGE += 1;
      loadNotes();
    }
  });
  qs("closeHistoryBtn").addEventListener("click", () => {
    qs("historyPanel").style.display = "none";
  });
  document.querySelectorAll(".notes-tab").forEach(btn => {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  });
  qs("filterSearch").addEventListener("keydown", (e) => {
    if(e.key === "Enter"){
      NOTES_PAGE = 1;
      loadNotes();
    }
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  bindEvents();
  await loadUsers();
  await loadNotes();
});
