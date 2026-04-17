let WT_TAB = "board";
let WT_USERS = [];
let WT_OWNER_ID = "";

function $(id){
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

function isAdmin(){
  return (document.body.dataset.role || "") === "ADMIN";
}

function showMsg(id, text, ok=true){
  const el = $(id);
  if(!el) return;
  el.textContent = text || "";
  el.className = "msg wt-msg " + (ok ? "ok" : "bad");
}

async function safeJson(res){
  try { return await res.json(); } catch { return {}; }
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

function monthParts(){
  const raw = $("wtMonth").value || `${document.body.dataset.year}-${document.body.dataset.month}`;
  const [year, month] = raw.split("-").map(Number);
  return { year, month };
}

function queryString(params){
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if(v !== undefined && v !== null && String(v) !== "") sp.set(k, v);
  });
  return sp.toString();
}

function statusClass(status){
  return String(status || "").toLowerCase();
}

function ownerParam(){
  if(!isAdmin()) return "";
  return $("wtEmployee") ? $("wtEmployee").value : WT_OWNER_ID;
}

function renderTabs(tab){
  WT_TAB = tab;
  document.querySelectorAll(".wt-tab").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  });
  document.querySelectorAll(".wt-section").forEach(section => {
    section.classList.toggle("active", section.id === `wt-tab-${tab}`);
  });
  if(tab === "board") loadBoard();
  if(tab === "review") loadReview();
  if(tab === "trash") loadTrash();
}

function weekAddHtml(week){
  return `
    <div class="wt-add">
      <label for="wtAdd${week}">Add Task</label>
      <input id="wtAdd${week}" type="text" placeholder="Task for week ${week}" />
      <button class="btn mini" type="button" data-add-week="${week}">Add Task</button>
    </div>
  `;
}

function badges(task){
  const confirm = task.confirmation_status === "PENDING" ? "Pending Admin Confirmation" : task.confirmation_status;
  const edit = Number(task.pending_edit_count || 0) > 0 ? `<span class="wt-badge">Edit pending</span>` : "";
  const carry = Number(task.carry_forward_count || 0) > 0 ? `<span class="wt-badge">Moved ${task.carry_forward_count}x</span>` : "";
  return `
    <div class="wt-badges">
      <span class="wt-badge ${statusClass(task.status)}">${esc(task.status)}</span>
      <span class="wt-badge">${esc(confirm)}</span>
      ${edit}
      ${carry}
    </div>
  `;
}

function taskActions(task){
  const confirmed = task.confirmation_status === "CONFIRMED";
  const admin = isAdmin();
  const parts = [];

  if(admin && task.confirmation_status === "PENDING"){
    parts.push(`<button class="btn ghost mini" data-act="confirm" data-id="${task.id}" type="button">Confirm</button>`);
  }
  if(confirmed && task.status !== "Done"){
    parts.push(`<button class="btn ghost mini" data-act="done" data-id="${task.id}" type="button">Done</button>`);
  }
  if(confirmed && task.status !== "Cancelled"){
    parts.push(`<button class="btn ghost mini" data-act="cancel" data-id="${task.id}" type="button">Cancel</button>`);
  }
  if(confirmed && task.status !== "Active"){
    parts.push(`<button class="btn ghost mini" data-act="active" data-id="${task.id}" type="button">Active</button>`);
  }
  if(confirmed && task.status !== "Done" && task.status !== "Cancelled"){
    parts.push(`<button class="btn ghost mini" data-act="carry" data-id="${task.id}" type="button">Move Next Week</button>`);
  }
  if(admin){
    parts.push(`<button class="btn ghost mini" data-act="edit" data-id="${task.id}" type="button">Edit</button>`);
  }else if(Number(task.can_request_edit) === 1){
    parts.push(`<button class="btn ghost mini" data-act="request-edit" data-id="${task.id}" type="button">Request Edit</button>`);
  }
  parts.push(`<button class="btn ghost mini" data-act="delete" data-id="${task.id}" type="button">Delete</button>`);
  return `<div class="wt-actions">${parts.join("")}</div>`;
}

function renderTask(task){
  return `
    <div class="wt-task ${statusClass(task.status)} ${task.confirmation_status === "PENDING" ? "pending" : ""}" data-task-id="${task.id}">
      <div class="wt-task-text">${esc(task.task_text)}</div>
      ${badges(task)}
      ${taskActions(task)}
    </div>
  `;
}

function renderBoard(data){
  const board = $("wtBoard");
  const owner = data.owner;
  $("wtScopeText").textContent = owner
    ? `Viewing ${owner.full_name || owner.username} - ${data.year}-${String(data.month).padStart(2, "0")}`
    : `Viewing ${data.year}-${String(data.month).padStart(2, "0")}`;

  board.innerHTML = (data.weeks || []).map(week => {
    const healthText = week.health === "healthy" ? "Healthy" : `${week.carry_forward_total || 0} moved`;
    const tasks = week.tasks || [];
    return `
      <div class="wt-card health-${esc(week.health)}">
        <div class="wt-card-head">
          <div>
            <h4>Week ${week.week}</h4>
            <p class="sub tiny">${tasks.length} task${tasks.length === 1 ? "" : "s"}</p>
          </div>
          <span class="wt-health">${esc(healthText)}</span>
        </div>
        ${weekAddHtml(week.week)}
        <div class="wt-task-list">
          ${tasks.length ? tasks.map(renderTask).join("") : `<div class="wt-empty">No tasks yet.</div>`}
        </div>
      </div>
    `;
  }).join("");
}

async function loadBoard(){
  try{
    const { year, month } = monthParts();
    const qs = queryString({
      year,
      month,
      week: $("wtWeek").value,
      status: $("wtStatus").value,
      q: $("wtSearch").value.trim(),
      owner_user_id: ownerParam()
    });
    const out = await api(`/api/weekly-tasks?${qs}`);
    renderBoard(out.data || {});
    showMsg("wtBoardMsg", "", true);
  }catch(err){
    showMsg("wtBoardMsg", err.message, false);
  }
}

async function addTask(week){
  const input = $(`wtAdd${week}`);
  const text = (input?.value || "").trim();
  if(!text){
    showMsg("wtBoardMsg", "Task text is required", false);
    return;
  }
  try{
    const { year, month } = monthParts();
    await api("/api/weekly-tasks", {
      method: "POST",
      body: JSON.stringify({
        task_text: text,
        year,
        month,
        week_number: week,
        owner_user_id: ownerParam()
      })
    });
    input.value = "";
    await loadBoard();
    showMsg("wtBoardMsg", isAdmin() ? "Task added as active." : "Task submitted for admin confirmation.", true);
  }catch(err){
    showMsg("wtBoardMsg", err.message, false);
  }
}

async function setStatus(id, status){
  await api(`/api/weekly-tasks/${id}/status`, {
    method: "POST",
    body: JSON.stringify({ status })
  });
}

async function confirmTask(id){
  await api(`/api/weekly-tasks/${id}/confirm`, { method: "POST" });
}

async function carryTask(id){
  await api(`/api/weekly-tasks/${id}/carry-forward`, { method: "POST" });
}

async function deleteTask(id){
  if(!confirm("Move this task to trash?")) return;
  await api(`/api/weekly-tasks/${id}`, { method: "DELETE" });
}

async function editTask(id){
  const card = document.querySelector(`[data-task-id="${id}"] .wt-task-text`);
  const current = card ? card.textContent.trim() : "";
  const text = prompt("Edit task text", current);
  if(text === null) return;
  const clean = text.trim();
  if(!clean) return;
  const status = prompt("Status: Pending, Active, Done, Cancelled", "Active") || "Active";
  await api(`/api/weekly-tasks/${id}`, {
    method: "PUT",
    body: JSON.stringify({ task_text: clean, status })
  });
}

async function requestEdit(id){
  const text = prompt("Requested new task text");
  if(text === null) return;
  const clean = text.trim();
  if(!clean) return;
  await api(`/api/weekly-tasks/${id}/edit-request`, {
    method: "POST",
    body: JSON.stringify({ requested_text: clean })
  });
}

async function handleBoardClick(e){
  const add = e.target.closest("[data-add-week]");
  if(add){
    await addTask(Number(add.dataset.addWeek));
    return;
  }

  const btn = e.target.closest("[data-act]");
  if(!btn) return;
  const id = Number(btn.dataset.id);
  try{
    if(btn.dataset.act === "confirm") await confirmTask(id);
    if(btn.dataset.act === "done") await setStatus(id, "Done");
    if(btn.dataset.act === "cancel") await setStatus(id, "Cancelled");
    if(btn.dataset.act === "active") await setStatus(id, "Active");
    if(btn.dataset.act === "carry") await carryTask(id);
    if(btn.dataset.act === "delete") await deleteTask(id);
    if(btn.dataset.act === "edit") await editTask(id);
    if(btn.dataset.act === "request-edit") await requestEdit(id);
    await loadBoard();
    showMsg("wtBoardMsg", "Task updated.", true);
  }catch(err){
    showMsg("wtBoardMsg", err.message, false);
  }
}

function reviewItem(row, type){
  if(type === "task"){
    return `
      <div class="wt-review-item">
        <div><b>${esc(row.task_text)}</b></div>
        <p class="sub">Owner: ${esc(row.owner_full_name || row.owner_username || "-")} | Week ${row.week_number} | Created by ${esc(row.created_by || "-")}</p>
        <div class="wt-actions">
          <button class="btn mini" data-review-task="${row.id}" type="button">Confirm</button>
        </div>
      </div>
    `;
  }
  return `
    <div class="wt-review-item">
      <div><b>Current:</b> ${esc(row.task_text)}</div>
      <div><b>Requested:</b> ${esc(row.requested_text)}</div>
      <p class="sub">Owner: ${esc(row.owner_full_name || row.owner_username || "-")} | Requested by ${esc(row.requested_by || "-")}</p>
      <div class="wt-actions">
        <button class="btn mini" data-approve-edit="${row.id}" type="button">Approve</button>
        <button class="btn ghost mini" data-deny-edit="${row.id}" type="button">Deny</button>
      </div>
    </div>
  `;
}

async function loadReview(){
  if(!isAdmin()) return;
  try{
    const out = await api("/api/weekly-tasks/review");
    const pendingTasks = out.data?.pending_tasks || [];
    const edits = out.data?.edit_requests || [];
    $("wtPendingTasks").innerHTML = pendingTasks.length ? pendingTasks.map(r => reviewItem(r, "task")).join("") : `<div class="wt-empty">No pending tasks.</div>`;
    $("wtEditRequests").innerHTML = edits.length ? edits.map(r => reviewItem(r, "edit")).join("") : `<div class="wt-empty">No edit requests.</div>`;
    showMsg("wtReviewMsg", "", true);
  }catch(err){
    showMsg("wtReviewMsg", err.message, false);
  }
}

async function handleReviewClick(e){
  const task = e.target.closest("[data-review-task]");
  const approve = e.target.closest("[data-approve-edit]");
  const deny = e.target.closest("[data-deny-edit]");
  try{
    if(task) await confirmTask(Number(task.dataset.reviewTask));
    if(approve) await api(`/api/weekly-tasks/edit-requests/${approve.dataset.approveEdit}/approve`, { method: "POST" });
    if(deny) await api(`/api/weekly-tasks/edit-requests/${deny.dataset.denyEdit}/deny`, { method: "POST" });
    await loadReview();
    await loadBoard();
    showMsg("wtReviewMsg", "Review updated.", true);
  }catch(err){
    showMsg("wtReviewMsg", err.message, false);
  }
}

function trashItem(row){
  return `
    <div class="wt-review-item">
      <div><b>${esc(row.task_text)}</b></div>
      <p class="sub">Owner: ${esc(row.owner_full_name || row.owner_username || "-")} | ${row.task_year}-${String(row.task_month).padStart(2, "0")} Week ${row.week_number} | Deleted by ${esc(row.deleted_by || "-")}</p>
      <div class="wt-actions">
        <button class="btn mini" data-recover="${row.id}" type="button">Recover</button>
        <button class="btn ghost mini" data-permanent="${row.id}" type="button">Permanent Delete</button>
      </div>
    </div>
  `;
}

async function loadTrash(){
  if(!isAdmin()) return;
  try{
    const out = await api("/api/weekly-tasks/trash");
    const rows = out.data || [];
    $("wtTrashList").innerHTML = rows.length ? rows.map(trashItem).join("") : `<div class="wt-empty">Trash is empty.</div>`;
    showMsg("wtTrashMsg", "", true);
  }catch(err){
    showMsg("wtTrashMsg", err.message, false);
  }
}

async function handleTrashClick(e){
  const recover = e.target.closest("[data-recover]");
  const permanent = e.target.closest("[data-permanent]");
  try{
    if(recover){
      await api(`/api/weekly-tasks/trash/${recover.dataset.recover}/recover`, { method: "POST" });
    }
    if(permanent){
      if(!confirm("Permanently delete this task?")) return;
      await api(`/api/weekly-tasks/trash/${permanent.dataset.permanent}/permanent`, { method: "DELETE" });
    }
    await loadTrash();
    showMsg("wtTrashMsg", "Trash updated.", true);
  }catch(err){
    showMsg("wtTrashMsg", err.message, false);
  }
}

async function loadUsers(){
  if(!isAdmin()) return;
  document.querySelectorAll(".admin-only").forEach(el => { el.style.display = ""; });
  const out = await api("/api/weekly-tasks/users");
  WT_USERS = out.data || [];
  const select = $("wtEmployee");
  select.innerHTML = WT_USERS.map(u => `<option value="${u.id}">${esc(u.full_name || u.username)} (${esc(u.role)})</option>`).join("");
  WT_OWNER_ID = select.value || "";
}

function bindEvents(){
  document.querySelectorAll(".wt-tab").forEach(btn => {
    btn.addEventListener("click", () => renderTabs(btn.dataset.tab));
  });
  $("wtRefreshBtn").addEventListener("click", () => {
    if(WT_TAB === "board") loadBoard();
    if(WT_TAB === "review") loadReview();
    if(WT_TAB === "trash") loadTrash();
  });
  $("wtApplyBtn").addEventListener("click", loadBoard);
  $("wtSearch").addEventListener("keydown", (e) => {
    if(e.key === "Enter") loadBoard();
  });
  $("wtBoard").addEventListener("click", handleBoardClick);
  if(isAdmin()){
    $("wtEmployee").addEventListener("change", () => {
      WT_OWNER_ID = $("wtEmployee").value;
      loadBoard();
    });
    $("wtPendingTasks").addEventListener("click", handleReviewClick);
    $("wtEditRequests").addEventListener("click", handleReviewClick);
    $("wtTrashList").addEventListener("click", handleTrashClick);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const year = document.body.dataset.year;
  const month = document.body.dataset.month;
  $("wtMonth").value = `${year}-${month}`;
  bindEvents();
  await loadUsers();
  await loadBoard();
});
