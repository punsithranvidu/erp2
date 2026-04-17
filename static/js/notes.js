let NOTES_TAB = "active";
let NOTES_PAGE = 1;
let NOTES_TOTAL_PAGES = 1;
let EDIT_NOTE_ID = null;
let ME = null;
let SELECTED_NOTE = null;

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

function clearSelection(){
  SELECTED_NOTE = null;
  document.querySelectorAll(".note-card.selected").forEach(card => card.classList.remove("selected"));
  syncActionBar();
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
  closeHistory();
  clearSelection();
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
    <span class="note-tip-line"><b>Created:</b> ${esc(fmt(row.created_at))}</span>
    <span class="note-tip-line"><b>Created by:</b> ${esc(fmt(row.created_by))}</span>
    <span class="note-tip-line"><b>Expected due:</b> ${esc(fmt(row.expected_end_date))}</span>
    <span class="note-tip-line"><b>Status:</b> ${esc(fmt(row.status))}</span>
    <span class="note-tip-line"><b>Last edited:</b> ${esc(fmt(row.edited_at))}</span>
    <span class="note-tip-line"><b>Edited by:</b> ${esc(fmt(row.edited_by))}</span>
    <span class="note-tip-line"><b>Done date:</b> ${esc(fmt(row.done_at))}</span>
    <span class="note-tip-line"><b>Cancelled date:</b> ${esc(fmt(row.cancelled_at))}</span>
  `;
}

function syncActionBar(){
  const bar = qs("noteActionBar");
  const label = qs("selectedNoteLabel");
  const row = SELECTED_NOTE;
  if(!bar || !label) return;

  bar.classList.toggle("active", !!row);
  if(!row){
    label.textContent = "No note selected";
    return;
  }

  label.textContent = `Selected: ${row.note_text || ""}`;
  const isTrash = NOTES_TAB === "trash";
  const canStatus = Number(row.can_status) === 1;

  qs("markDoneBtn").style.display = isTrash || !canStatus || row.status === "Done" ? "none" : "";
  qs("markCancelBtn").style.display = isTrash || !canStatus || row.status === "Cancelled" ? "none" : "";
  qs("markActiveBtn").style.display = isTrash || !canStatus || row.status === "Active" ? "none" : "";
  qs("editSelectedBtn").style.display = !isTrash && Number(row.can_edit) === 1 ? "" : "none";
  qs("deleteSelectedBtn").style.display = !isTrash && Number(row.can_delete) === 1 ? "" : "none";
  qs("recoverSelectedBtn").style.display = isTrash ? "" : "none";
  qs("permanentSelectedBtn").style.display = isTrash ? "" : "none";
}

function selectNote(row, card){
  SELECTED_NOTE = row;
  document.querySelectorAll(".note-card.selected").forEach(el => el.classList.remove("selected"));
  if(card) card.classList.add("selected");
  syncActionBar();
}

function renderNoteCard(row){
  const card = document.createElement("div");
  card.className = `note-card ${statusClass(row.status)}`;
  card.tabIndex = 0;
  card.setAttribute("role", "button");
  card.setAttribute("aria-label", `Select note ${row.id}`);

  card.innerHTML = `
    <div class="note-text">${esc(row.note_text)}</div>
    <span class="note-pill status-${statusClass(row.status)}">${esc(row.status)}</span>
    <button class="note-info-btn" type="button" aria-label="Note details">!
      <span class="note-tip">${noteTooltip(row)}</span>
    </button>
  `;

  card.addEventListener("click", () => selectNote(row, card));
  card.addEventListener("keydown", (e) => {
    if(e.key === "Enter" || e.key === " "){
      e.preventDefault();
      selectNote(row, card);
    }
  });
  card.querySelector(".note-info-btn").addEventListener("click", (e) => e.stopPropagation());

  return card;
}

function renderTrashCard(row){
  const card = document.createElement("div");
  card.className = `note-card ${statusClass(row.status)}`;
  card.tabIndex = 0;
  card.setAttribute("role", "button");
  card.setAttribute("aria-label", `Select trash note ${row.id}`);
  card.innerHTML = `
    <div class="note-text">${esc(row.note_text)}</div>
    <span class="note-pill status-${statusClass(row.status)}">${esc(row.status)}</span>
    <button class="note-info-btn" type="button" aria-label="Note details">!
      <span class="note-tip">${noteTooltip(row)}<span class="note-tip-line"><b>Deleted:</b> ${esc(fmt(row.deleted_at))}</span><span class="note-tip-line"><b>Deleted by:</b> ${esc(fmt(row.deleted_by))}</span></span>
    </button>
  `;

  card.addEventListener("click", () => selectNote(row, card));
  card.addEventListener("keydown", (e) => {
    if(e.key === "Enter" || e.key === " "){
      e.preventDefault();
      selectNote(row, card);
    }
  });
  card.querySelector(".note-info-btn").addEventListener("click", (e) => e.stopPropagation());

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
  syncActionBar();
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
      per_page: 30,
      q: filters.q,
      status: filters.status,
      user_id: filters.user_id,
      expected_date: filters.expected_date
    });
    const out = await api(`/api/notes?${qsText}`);
    clearSelection();
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
  clearSelection();
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
    const data = out.data || {};
    const editRows = data.edit_history || [];
    const dueRows = data.due_date_history || [];
    const modal = qs("historyModal");
    const body = qs("historyRows");

    const editHtml = editRows.length
      ? editRows.map((r, idx) => `
          <div class="notes-history-line">
            <span><b>edited_by_${idx + 1}</b>: ${esc(fmt(r.edited_by))}</span>
            <span><b>edited_date_${idx + 1}</b>: ${esc(fmt(r.edited_at))}</span>
          </div>
        `).join("")
      : `<p class="sub">No edit history yet.</p>`;

    const dueHtml = dueRows.length
      ? dueRows.map(r => `
          <div class="notes-history-line">
            <span><b>Due</b>: ${esc(fmt(r.old_expected_end_date))} to ${esc(fmt(r.new_expected_end_date))}</span>
            <span>${esc(fmt(r.changed_at))} by ${esc(fmt(r.changed_by))}</span>
          </div>
        `).join("")
      : "";

    body.innerHTML = `
      <div>
        <h4 style="margin:0 0 8px;">Edit History</h4>
        ${editHtml}
      </div>
      ${dueHtml ? `<div style="margin-top:12px;"><h4 style="margin:0 0 8px;">Expected Date Changes</h4>${dueHtml}</div>` : ""}
    `;

    modal.classList.add("active");
    modal.setAttribute("aria-hidden", "false");
  }catch(err){
    showMsg(err.message, false);
  }
}

function closeHistory(){
  const modal = qs("historyModal");
  if(!modal) return;
  modal.classList.remove("active");
  modal.setAttribute("aria-hidden", "true");
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
  qs("closeHistoryBtn").addEventListener("click", closeHistory);
  qs("historyModal").addEventListener("click", (e) => {
    if(e.target === qs("historyModal")) closeHistory();
  });
  document.querySelectorAll(".notes-tab").forEach(btn => {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  });
  qs("markDoneBtn").addEventListener("click", () => SELECTED_NOTE && setStatus(SELECTED_NOTE.id, "Done"));
  qs("markCancelBtn").addEventListener("click", () => SELECTED_NOTE && setStatus(SELECTED_NOTE.id, "Cancelled"));
  qs("markActiveBtn").addEventListener("click", () => SELECTED_NOTE && setStatus(SELECTED_NOTE.id, "Active"));
  qs("editSelectedBtn").addEventListener("click", () => SELECTED_NOTE && fillForm(SELECTED_NOTE));
  qs("deleteSelectedBtn").addEventListener("click", () => SELECTED_NOTE && deleteNote(SELECTED_NOTE.id));
  qs("recoverSelectedBtn").addEventListener("click", () => SELECTED_NOTE && recoverNote(SELECTED_NOTE.id));
  qs("permanentSelectedBtn").addEventListener("click", () => SELECTED_NOTE && permanentDelete(SELECTED_NOTE.id));
  qs("historySelectedBtn").addEventListener("click", () => SELECTED_NOTE && loadHistory(SELECTED_NOTE.id));
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
